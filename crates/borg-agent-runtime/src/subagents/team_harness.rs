use super::*;
use crate::plugin_store::{CommitScope, PluginScope, PluginWrite};

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct SnapshotArgs {
    after: Option<Uuid>,
    limit: Option<usize>,
    recent_messages: Option<usize>,
    max_bytes: Option<usize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InspectArgs {
    session_id: Uuid,
    after_sequence: Option<u64>,
    limit: Option<usize>,
    max_bytes: Option<usize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentGoalArgs {
    target: String,
    goal_action: Option<crate::GoalAction>,
}

// Deliberate allowlist: no provider events, nested child events, reasoning,
// tool payloads, attachments, environment or interaction payloads cross this API.
fn public_event(event: &SessionEvent) -> Option<Value> {
    let body = match &event.kind {
        SessionEventKind::Message {
            message_id,
            actor,
            text,
            status,
            ..
        } if matches!(
            actor,
            EventActor::User | EventActor::Assistant | EventActor::System
        ) =>
        {
            json!({
            "type": "message", "message_id": message_id, "actor": actor,
            "text": crate::secret_scrub::scrub_secrets(text), "status": status})
        }
        SessionEventKind::AgentMessageReceived {
            message_id,
            sender_id,
            sender_name,
            text,
        } => json!({
            "type": "agent_message_received", "message_id": message_id, "sender_id": sender_id,
            "sender_name": crate::secret_scrub::scrub_secrets(sender_name),
            "text": crate::secret_scrub::scrub_secrets(text)}),
        SessionEventKind::GoalUpdated { goal } => {
            json!({"type": "goal_updated", "goal": public_goal(Some(goal))})
        }
        SessionEventKind::GoalCleared { goal_id } => {
            json!({"type": "goal_cleared", "goal_id": goal_id})
        }
        SessionEventKind::StatusChanged { status, detail } => {
            json!({"type": "status_changed", "status": status,
            "detail": detail.as_deref().map(crate::secret_scrub::scrub_secrets)})
        }
        SessionEventKind::UserStopChanged { engaged } => {
            json!({"type": "user_stop_changed", "engaged": engaged})
        }
        _ => return None,
    };
    Some(
        json!({"event_id": event.id, "session_id": event.session_id, "sequence": event.sequence,
        "created_at": event.created_at, "body": body}),
    )
}

fn public_goal(goal: Option<&crate::SessionGoal>) -> Value {
    let mut goal = serde_json::to_value(goal).expect("goal is serializable");
    if let Some(objective) = goal["objective"].as_str() {
        goal["objective"] = json!(crate::secret_scrub::scrub_secrets(objective));
    }
    goal
}

fn assignment_goal_observation(state: &crate::SessionState) -> Value {
    let unfinished = state
        .todos
        .iter()
        .filter(|item| item.status != crate::PlanItemStatus::Completed)
        .count();
    let warning = if unfinished == 0 {
        None
    } else if state.goal.is_none() {
        Some("unfinished_assignments_without_goal")
    } else if state
        .goal
        .as_ref()
        .is_some_and(|goal| goal.status == crate::GoalStatus::Complete)
    {
        Some("unfinished_assignments_with_completed_goal")
    } else {
        None
    };
    json!({"source": "session_store.state", "plan_revision": state.plan_workspace_revision,
        "unfinished_assigned_items": unfinished, "warning": warning,
        "caveat": "Assignment and goal lifecycles are independent. This warning is not proof of stale work or authorization to change or resume a goal."})
}

fn execution_goal_warning(
    goal: Option<&crate::SessionGoal>,
    executing: bool,
) -> Option<&'static str> {
    if executing
        && goal.is_some_and(|goal| {
            matches!(
                goal.status,
                crate::GoalStatus::Blocked | crate::GoalStatus::Paused
            )
        })
    {
        Some("executing_with_blocked_or_paused_goal")
    } else {
        None
    }
}

fn canonical_summary(id: Uuid, state: &crate::SessionState) -> Value {
    let mut plan = serde_json::to_value(&state.todos).expect("plan is serializable");
    scrub_value(&mut plan);
    json!({"session_id": id, "source": "session_store.state", "revision": state.latest_sequence,
        "observed_at": Utc::now(), "activity_at": state.activity_at,
        "execution_status": state.status, "status_detail": state.status_detail.as_deref().map(crate::secret_scrub::scrub_secrets),
        "goal": public_goal(state.goal.as_ref()),
        "execution_goal_warning": execution_goal_warning(state.goal.as_ref(), matches!(state.status, Some(crate::SessionStatus::Starting | crate::SessionStatus::Running))),
        "assignment_goal_observation": assignment_goal_observation(state),
        "remaining_tokens": state.goal.as_ref().and_then(|goal| goal.token_budget.map(|budget| budget.saturating_sub(goal.tokens_used))), "usage": state.usage, "assigned_plan": plan,
        "plan_revision": state.plan_workspace_revision, "user_stopped": state.user_stopped,
        "pending_approval_id": state.pending_approval_id,
        "pending_decision_id": state.pending_provider_interaction_id,
        "pending_decision_kind": state.pending_provider_interaction_kind,
        "wait_reason": if state.user_stopped { Some("explicit_user_stop") }
            else if state.pending_approval_id.is_some() { Some("pending_approval") }
            else if state.pending_provider_interaction_id.is_some() { Some("pending_decision") }
            else if state.usage_limit_retry.is_some() { Some("provider_usage_limit_retry") }
            else { None },
        "caveat": "Recorded execution status/detail is not a liveness probe or proof of working, goal completion or authority; absent wait details remain unknown."})
}

pub(super) fn scrub_value(value: &mut Value) {
    match value {
        Value::String(text) => *text = crate::secret_scrub::scrub_secrets(text).into_owned(),
        Value::Array(values) => values.iter_mut().for_each(scrub_value),
        Value::Object(values) => values.values_mut().for_each(scrub_value),
        _ => (),
    }
}

fn validate_goal_action(
    action: &crate::GoalAction,
    state: Option<&crate::SessionState>,
) -> Result<()> {
    if let crate::GoalAction::Set {
        objective,
        token_budget,
    } = action
    {
        ensure!(
            !objective.trim().is_empty() && objective.chars().count() <= 4096,
            "goal objective must contain 1..4096 characters"
        );
        ensure!(
            token_budget.is_none_or(|budget| budget > 0),
            "token budget must be positive"
        );
    }
    if let Some(state) = state {
        match action {
            crate::GoalAction::Pause => {
                ensure!(state.goal.is_some(), "child has no goal");
            }
            crate::GoalAction::Resume => {
                let goal = state.goal.as_ref().context("child has no goal")?;
                ensure!(
                    !state.user_stopped,
                    "explicit human stop requires human resume authorization"
                );
                ensure!(
                    state.pending_approval_id.is_none()
                        && state.pending_provider_interaction_id.is_none(),
                    "child has a pending approval or decision"
                );
                ensure!(
                    !matches!(
                        goal.status,
                        crate::GoalStatus::Complete | crate::GoalStatus::BudgetLimited
                    ),
                    "completed or budget-limited goals cannot be resumed"
                );
            }
            crate::GoalAction::Set { .. } => {
                ensure!(
                    !state.user_stopped,
                    "explicit human stop requires human resume authorization"
                );
                ensure!(
                    state.pending_approval_id.is_none()
                        && state.pending_provider_interaction_id.is_none(),
                    "child has a pending approval or decision"
                );
            }
            crate::GoalAction::Clear => (),
        }
    }
    Ok(())
}

impl SubagentCoordinator {
    async fn authorize_inspection(&self, actor: Uuid, target: Uuid) -> Result<()> {
        ensure!(
            self.store.is_descendant(actor, target).await?,
            "session is not an owned descendant of the caller"
        );
        Ok(())
    }

    pub(super) async fn agent_goal(
        &self,
        actor: Uuid,
        arguments: Value,
        change: bool,
    ) -> Result<Value> {
        let args: AgentGoalArgs = serde_json::from_value(arguments)?;
        let id = self.table.lock().await.resolve(&args.target)?;
        self.authorize_inspection(actor, id).await?;
        if !change {
            ensure!(
                args.goal_action.is_none(),
                "get_agent_goal does not accept goal_action"
            );
            let state = self.store.state(id).await?;
            return Ok(
                json!({"session_id": id, "goal": public_goal(state.goal.as_ref()),
                "revision": state.latest_sequence, "user_stopped": state.user_stopped,
                "assignment_goal_observation": assignment_goal_observation(&state),
                "execution_status": state.status,
                "execution_goal_warning": execution_goal_warning(state.goal.as_ref(), matches!(state.status, Some(crate::SessionStatus::Starting | crate::SessionStatus::Running))),
                "source": "canonical session store", "observed_at": Utc::now()}),
            );
        }
        ensure!(
            actor == self.root_session_id,
            "only the director may control child goals"
        );
        let action = args.goal_action.context("goal_action is required")?;
        let state = self.store.state(id).await?;
        validate_goal_action(&action, Some(&state))?;
        self.send_command(&id.to_string(), |session_id| HostCommand::AgentGoal {
            session_id,
            action,
        })
        .await?;
        Ok(json!({"session_id": id, "accepted": true, "applied": null,
            "before_revision": state.latest_sequence,
            "caveat": "Queued to the owning child actor, not proof of application. Read get_agent_goal to verify. Pause/clear do not interrupt a running turn; interrupt_agent is separate. Use only for explicit human-authorized goal management."}))
    }

    pub(super) async fn team_snapshot(&self, actor: Uuid, arguments: Value) -> Result<Value> {
        let args: SnapshotArgs = serde_json::from_value(arguments)?;
        let started_at = Utc::now();
        let limit = args.limit.unwrap_or(50).clamp(1, 100);
        let mut ids = self
            .store
            .descendant_sessions(actor, args.after, limit + 1)
            .await?;
        let more = ids.len() > limit;
        ids.truncate(limit);
        let mut more = more;
        let max_bytes = args
            .max_bytes
            .unwrap_or(256 * 1024)
            .clamp(1024, 1024 * 1024);
        let mut bytes = 0;
        let mut next = args.after;
        let mut bounded = false;
        let mut agents = Vec::new();
        let mut partial = false;
        for id in ids {
            match self.store.state(id).await {
                Ok(state) => {
                    let mut summary = canonical_summary(id, &state);
                    if let Some(live) = self.get(id).await {
                        summary["execution_observation"] = json!({"source": "coordinator", "status": live.status,
                            "updated_at": live.updated_at, "task_name": live.task_name, "cwd": live.cwd,
                            "execution_goal_warning": execution_goal_warning(state.goal.as_ref(), matches!(live.status, SubagentStatus::Starting | SubagentStatus::Running)),
                            "caveat": "Coordinator projection, not a liveness probe. task_name is session identity, not the current assignment or goal. Goal and execution lifecycles are independent; a warning does not authorize resume."});
                    } else {
                        summary["execution_observation"] = json!({"source": "coordinator", "status": null, "caveat": "No live observation in this coordinator"});
                    }
                    let recent_limit = args.recent_messages.unwrap_or(2).min(10);
                    let recent = if recent_limit == 0 {
                        Ok(Vec::new())
                    } else {
                        self.store.recent_messages(id, recent_limit).await
                    };
                    match recent {
                        Ok(events) => {
                            summary["recent_messages"] = json!(
                                events
                                    .iter()
                                    .filter_map(public_event)
                                    .map(bounded_recent_message)
                                    .collect::<Vec<_>>()
                            )
                        }
                        Err(error) => {
                            partial = true;
                            summary["messages_error"] =
                                json!(crate::secret_scrub::scrub_secrets(&error.to_string()));
                        }
                    }
                    let size = serde_json::to_vec(&summary)?.len();
                    if bytes + size > max_bytes {
                        ensure!(
                            !agents.is_empty(),
                            "one agent exceeds max_bytes; increase the bound or use inspect_agent"
                        );
                        more = true;
                        bounded = true;
                        break;
                    }
                    bytes += size;
                    next = Some(id);
                    agents.push(summary);
                }
                Err(error) => {
                    partial = true;
                    next = Some(id);
                    agents.push(json!({"session_id": id, "error": crate::secret_scrub::scrub_secrets(&error.to_string())}));
                }
            }
        }
        Ok(
            json!({"started_at": started_at, "observed_at": Utc::now(), "source": "canonical ownership/session store plus coordinator observation",
            "agents": agents, "next_after": if more { next } else { None }, "has_more": more, "partial": partial, "byte_bounded": bounded,
            "caveat": "Non-atomic bounded snapshot; sources can advance independently. Roster presence does not prove liveness or permission."}),
        )
    }

    pub(super) async fn inspect_agent(&self, actor: Uuid, arguments: Value) -> Result<Value> {
        let args: InspectArgs = serde_json::from_value(arguments)?;
        self.authorize_inspection(actor, args.session_id).await?;
        let state = self.store.state(args.session_id).await?;
        let after = args.after_sequence.unwrap_or(0);
        let limit = args.limit.unwrap_or(50).clamp(1, 200);
        let max_bytes = args
            .max_bytes
            .unwrap_or(256 * 1024)
            .clamp(1024, 1024 * 1024);
        let events = self
            .store
            .events_after(args.session_id, after, limit)
            .await?;
        let mut cursor = after;
        let mut bytes = 0;
        let mut exposed = Vec::new();
        for event in events
            .iter()
            .filter(|event| event.sequence <= state.latest_sequence)
        {
            if let Some(value) = public_event(event) {
                let size = serde_json::to_vec(&value)?.len();
                if bytes + size > max_bytes {
                    ensure!(
                        cursor > after,
                        "event exceeds max_bytes; increase max_bytes to read this event without truncating its text"
                    );
                    break;
                }
                bytes += size;
                exposed.push(value);
            }
            cursor = event.sequence;
        }
        Ok(
            json!({"canonical": canonical_summary(args.session_id, &state), "events": exposed,
            "scanned_events": events.iter().filter(|event| event.sequence <= cursor).count(),
            "next_after_sequence": cursor, "has_more": cursor < state.latest_sequence,
            "snapshot_revision": state.latest_sequence, "tail_can_advance": true,
            "caveat": "Page is bounded at the reported canonical revision; later events require another read. Canonical and workspace projections can advance independently.",
            "source": "canonical session journal", "observed_at": Utc::now(),
            "projection_lag": "Workspace delivery projection is independent; this read does not repair it.",
            "excluded": "Private reasoning, provider payloads, tool payloads, nested events, attachments and interaction payloads."}),
        )
    }
}

const BATCH_NAMESPACE: &str = "borg-team-operations";
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BatchItem {
    session_id: Uuid,
    operation: String,
    #[serde(default)]
    message: String,
    configuration: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    goal_action: Option<crate::GoalAction>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BatchArgs {
    idempotency_key: String,
    operations: Vec<BatchItem>,
    #[serde(default)]
    cancel: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OperationArgs {
    operation_id: Uuid,
}

impl SubagentCoordinator {
    async fn batch_entry(
        &self,
        actor: Uuid,
        id: Uuid,
    ) -> Result<Option<crate::plugin_store::PluginStateEntry>> {
        self.store
            .plugin_backend()
            .get_entry(
                BATCH_NAMESPACE,
                PluginScope::Session,
                &actor.to_string(),
                &id.to_string(),
            )
            .await
    }

    async fn save_batch(&self, actor: Uuid, id: Uuid, revision: u64, state: Value) -> Result<()> {
        let scope = actor.to_string();
        let request = serde_json::to_vec(&state)?;
        ensure!(
            request.len() <= 512 * 1024,
            "batch receipt exceeds durable state limit"
        );
        let hash = format!("sha256:{}", hex::encode(Sha256::digest(&request)));
        self.store
            .plugin_backend()
            .commit(
                CommitScope {
                    extension_id: BATCH_NAMESPACE,
                    scope: PluginScope::Session,
                    scope_id: &scope,
                },
                &format!("{id}:{revision}:{hash}"),
                &hash,
                &[PluginWrite::Put {
                    key: id.to_string(),
                    value: state,
                    expected_revision: Some(revision),
                }],
                &[],
                &json!({"source": "team_batch", "actor_session_id": actor}),
            )
            .await?;
        Ok(())
    }

    pub(super) async fn get_team_operation(&self, actor: Uuid, arguments: Value) -> Result<Value> {
        let args: OperationArgs = serde_json::from_value(arguments)?;
        let entry = self
            .batch_entry(actor, args.operation_id)
            .await?
            .context("unknown operation for this caller")?;
        let mut receipt = entry.value.context("operation receipt removed")?;
        // Responses are sanitized, not the request body used to check idempotency.
        scrub_value(&mut receipt);
        let mut counts = BTreeMap::<String, usize>::new();
        for result in receipt["results"]
            .as_array()
            .context("invalid batch results")?
        {
            *counts
                .entry(result["state"].as_str().unwrap_or("unknown").to_string())
                .or_default() += 1;
        }
        let total = receipt["results"].as_array().unwrap().len();
        let submitted = counts.get("submitted").copied().unwrap_or(0)
            + counts.get("submitted_recovered").copied().unwrap_or(0);

        Ok(
            json!({"operation_id": args.operation_id, "revision": entry.revision, "updated_at": entry.updated_at,
            "observed_at": Utc::now(), "source": "durable session-scoped operation receipt", "receipt": receipt,
            "progress": {"total": total, "submitted": submitted, "all_submitted": submitted == total, "by_state": counts},
            "caveat": "sending means outcome unknown if execution was interrupted; it is never automatically replayed. Results prove submission only, not read/approval/completion."}),
        )
    }

    pub(super) async fn team_batch(&self, actor: Uuid, arguments: Value) -> Result<Value> {
        let args: BatchArgs = serde_json::from_value(arguments)?;
        let key = required_idempotency_key(&args.idempotency_key)?;
        ensure!(
            (1..=64).contains(&args.operations.len()),
            "batch requires 1..64 operations"
        );
        ensure!(
            self.root_launch.capabilities.multiplayer,
            "durable batches require multiplayer storage"
        );
        // Validate every target before writing intent or producing any side effect.
        for item in &args.operations {
            ensure!(
                item.operation == "control_agent_goal" || item.goal_action.is_none(),
                "goal_action is only accepted for control_agent_goal"
            );
            match item.operation.as_str() {
                "send_message" | "followup_task" => {
                    ensure!(
                        !item.message.trim().is_empty() && item.message.len() <= 8192,
                        "message requires 1..8192 bytes"
                    );
                    ensure!(
                        item.configuration.is_none(),
                        "message operations do not accept configuration"
                    );
                }
                "configure_agent" => {
                    ensure!(
                        actor == self.root_session_id,
                        "only the director may configure child agents"
                    );
                    ensure!(
                        item.message.is_empty(),
                        "configure_agent does not accept message"
                    );
                    let mut configuration = item
                        .configuration
                        .clone()
                        .context("configure_agent requires configuration")?;
                    let fields = configuration
                        .as_object_mut()
                        .context("configuration must be an object")?;
                    ensure!(
                        !fields.contains_key("target"),
                        "configuration target comes from session_id"
                    );
                    fields.insert("target".into(), json!(item.session_id));
                    let _: ConfigureAgentArgs = serde_json::from_value(configuration)?;
                }
                "control_agent_goal" => {
                    ensure!(
                        actor == self.root_session_id,
                        "only the director may control child goals"
                    );
                    ensure!(
                        item.message.is_empty() && item.configuration.is_none(),
                        "control_agent_goal accepts only session_id and goal_action"
                    );
                    let action = item
                        .goal_action
                        .as_ref()
                        .context("goal_action is required")?;
                    validate_goal_action(action, None)?;
                }
                "interrupt_agent" => ensure!(
                    item.message.is_empty() && item.configuration.is_none(),
                    "interrupt_agent accepts only session_id"
                ),
                _ => bail!("unsupported batch operation"),
            }
            self.authorize_inspection(actor, item.session_id).await?;
        }
        let id = Uuid::new_v5(&actor, format!("team-batch:{key}").as_bytes());
        let request = json!(args.operations);
        if let Some(entry) = self.batch_entry(actor, id).await? {
            ensure!(
                entry
                    .value
                    .as_ref()
                    .and_then(|value| value.get("operations"))
                    == Some(&request),
                "idempotency key reused with different operations"
            );
        } else {
            self.save_batch(actor, id, 0, json!({"operations": request, "created_at": Utc::now(), "cancelled": false,
                "results": args.operations.iter().map(|item| json!({"session_id": item.session_id, "state": "pending"})).collect::<Vec<_>>() })).await?;
        }
        if args.cancel {
            let entry = self
                .batch_entry(actor, id)
                .await?
                .context("batch disappeared")?;
            let mut state = entry.value.context("batch disappeared")?;
            state["cancelled"] = json!(true);
            self.save_batch(actor, id, entry.revision, state).await?;
            return self
                .get_team_operation(actor, json!({"operation_id": id}))
                .await;
        }
        for (index, item) in args.operations.iter().enumerate() {
            let entry = self
                .batch_entry(actor, id)
                .await?
                .context("batch disappeared")?;
            let mut state = entry.value.context("batch disappeared")?;
            if state["cancelled"] == true {
                break;
            }
            if args
                .operations
                .iter()
                .enumerate()
                .take(index)
                .any(|(prior, operation)| {
                    operation.operation == "control_agent_goal"
                        && state["results"][prior]["state"] != "submitted"
                })
            {
                state["gate"] = json!(
                    "earlier goal command has no confirmed submission; later operations remain unsent"
                );
                self.save_batch(actor, id, entry.revision, state).await?;
                break;
            }
            // Fence concurrent retries durably BEFORE the effect. A dropped
            // future leaves an explicit uncertain target, never a replay.
            if state["results"][index]["state"] == "sending" {
                let binding = self
                    .store
                    .workspace_binding(actor)
                    .await?
                    .context("author has no workspace")?;
                if let Some(event) = self
                    .workspace_store()
                    .await?
                    .event_by_idempotency_key(
                        binding.workspace_id,
                        binding.participant_id,
                        &format!("team-batch:{actor}:{id}:{index}"),
                    )
                    .await?
                    && let WorkspaceEventKind::Message { message, .. } = event.kind
                {
                    state["results"][index] = json!({"session_id": item.session_id, "state": "submitted_recovered",
                        "message_id": message.id, "dispatched_locally": null, "source": "canonical workspace message", "finished_at": Utc::now()});
                    self.save_batch(actor, id, entry.revision, state).await?;
                }
                continue;
            }
            if state["results"][index]["state"] != "pending" {
                continue;
            }
            let actor_state = self.store.state(actor).await?;
            let gated = actor_state.user_stopped
                || actor_state.pending_approval_id.is_some()
                || actor_state.pending_provider_interaction_id.is_some()
                || actor_state.goal.as_ref().is_some_and(|goal| {
                    goal.token_budget
                        .is_some_and(|budget| goal.tokens_used >= budget)
                });
            if gated {
                state["gate"] =
                    json!("caller stop/approval/decision/budget gate prevents further submission");
                self.save_batch(actor, id, entry.revision, state).await?;
                break;
            }
            state["results"][index]["state"] = json!("sending");
            state["results"][index]["started_at"] = json!(Utc::now());
            self.save_batch(actor, id, entry.revision, state).await?;
            let target = item.session_id.to_string();
            let result: Result<Value> = if item.operation == "control_agent_goal" {
                self.agent_goal(
                    actor,
                    json!({"target": target, "goal_action": item.goal_action}),
                    true,
                )
                .await
            } else if matches!(
                item.operation.as_str(),
                "configure_agent" | "interrupt_agent"
            ) {
                let mut arguments = item.configuration.clone().unwrap_or_else(|| json!({}));
                arguments["target"] = json!(target);
                let result = Box::pin(self.call_tool_as(actor, &item.operation, arguments)).await;
                result.map(|value| {
                    if item.operation == "configure_agent" {
                        let agent = &value["agent"];
                        json!({"session_id": agent["session_id"], "status": agent["status"], "provider": agent["provider"],
                            "model": agent["model"], "effort": agent["effort"], "fast": agent["fast"], "ultrafast": agent["ultrafast"], "updated_at": agent["updated_at"]})
                    } else { value }
                })
            } else {
                let options = TeamMessageOptions {
                    idempotency_key: Some(format!("team-batch:{actor}:{id}:{index}")),
                    ..Default::default()
                };
                let routed = if item.operation == "followup_task" {
                    self.route_followup_task_with_options_as(actor, &target, &item.message, options)
                        .await
                } else {
                    self.route_message_with_options_as(actor, &target, &item.message, options)
                        .await
                };
                routed.map(|routed| json!({"message_id": routed.receipt.as_ref().map(|receipt| receipt.message_id),
                    "dispatched_locally": routed.dispatched_locally, "relay_pending": routed.relay_pending, "awaiting_wake": routed.awaiting_wake}))
            };
            let entry = self
                .batch_entry(actor, id)
                .await?
                .context("batch disappeared")?;
            let mut state = entry.value.context("batch disappeared")?;
            state["results"][index] = match result {
                Ok(mut result) => {
                    result["session_id"] = json!(item.session_id);
                    result["state"] = json!("submitted");
                    result["finished_at"] = json!(Utc::now());
                    scrub_value(&mut result);
                    result
                }
                Err(error) => {
                    json!({"session_id": item.session_id, "state": "failed_or_uncertain", "error": crate::secret_scrub::scrub_secrets(&error.to_string()), "finished_at": Utc::now()})
                }
            };
            self.save_batch(actor, id, entry.revision, state).await?;
        }
        self.get_team_operation(actor, json!({"operation_id": id}))
            .await
    }
}

fn bounded_recent_message(mut event: Value) -> Value {
    if let Some(text) = event["body"]["text"].as_str() {
        let shortened: String = text.chars().take(4096).collect();
        let truncated = shortened.len() < text.len();
        event["body"]["text"] = json!(shortened);
        event["text_truncated"] = json!(truncated);
        if truncated {
            event["instruction"] =
                json!("Use inspect_agent at sequence - 1 for full canonical text.");
        }
    }
    event
}

#[cfg(test)]
mod observation_tests {
    use super::*;

    #[test]
    fn batch_goal_extension_preserves_legacy_request_serialization() {
        let legacy = json!({"session_id": Uuid::nil(), "operation": "interrupt_agent", "message": "", "configuration": null});
        let item: BatchItem = serde_json::from_value(legacy.clone()).unwrap();
        assert_eq!(serde_json::to_value(item).unwrap(), legacy);
        let invalid = json!({"session_id": Uuid::nil(), "operation": "control_agent_goal", "goal_action": {"type": "unknown"}});
        assert!(serde_json::from_value::<BatchItem>(invalid).is_err());
    }

    #[test]
    fn executing_with_blocked_or_paused_goal_is_visible_without_resuming() {
        let mut state = crate::SessionState::default();
        state.goal = Some(crate::SessionGoal::new("work".into(), None));
        for status in [
            crate::SessionStatus::Starting,
            crate::SessionStatus::Running,
        ] {
            state.status = Some(status);
            for goal_status in [crate::GoalStatus::Blocked, crate::GoalStatus::Paused] {
                state.goal.as_mut().unwrap().status = goal_status;
                let summary = canonical_summary(Uuid::nil(), &state);
                assert_eq!(
                    summary["execution_goal_warning"],
                    "executing_with_blocked_or_paused_goal"
                );
                assert_eq!(state.goal.as_ref().unwrap().status, goal_status);
            }
        }
        state.status = Some(crate::SessionStatus::Ready);
        assert!(canonical_summary(Uuid::nil(), &state)["execution_goal_warning"].is_null());
        state.status = Some(crate::SessionStatus::Running);
        state.goal.as_mut().unwrap().status = crate::GoalStatus::Active;
        assert!(canonical_summary(Uuid::nil(), &state)["execution_goal_warning"].is_null());
    }

    #[test]
    fn unfinished_assignments_warn_without_an_unfinished_goal() {
        let mut state = crate::SessionState::default();
        state.todos.push(crate::PlanItem {
            id: Uuid::new_v4(),
            content: "assigned work".into(),
            status: crate::PlanItemStatus::Pending,
        });
        assert_eq!(
            canonical_summary(Uuid::nil(), &state)["assignment_goal_observation"]["warning"],
            "unfinished_assignments_without_goal"
        );
        state.goal = Some(crate::SessionGoal::new("work".into(), None));
        assert!(
            canonical_summary(Uuid::nil(), &state)["assignment_goal_observation"]["warning"]
                .is_null()
        );
        state.goal.as_mut().unwrap().status = crate::GoalStatus::Complete;
        assert_eq!(
            canonical_summary(Uuid::nil(), &state)["assignment_goal_observation"]["warning"],
            "unfinished_assignments_with_completed_goal"
        );
        state.todos[0].status = crate::PlanItemStatus::Completed;
        assert!(
            canonical_summary(Uuid::nil(), &state)["assignment_goal_observation"]["warning"]
                .is_null()
        );
    }
}
