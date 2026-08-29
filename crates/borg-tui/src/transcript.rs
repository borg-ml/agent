const USER_INTERRUPT_ACTIVITY: &str = "agent interrupted by user";
const TOOL_ELAPSED_REFRESH_MILLIS: i64 = 100;

fn user_message_has_structured_whitespace(text: &str) -> bool {
    text.contains('\n')
        && text.lines().any(|line| {
            line.contains('\t')
                || line.starts_with(' ')
                || line.contains("  ")
                || line.chars().any(|character| "│┃┌┐└┘┬┴┼╭╮╰╯".contains(character))
        })
}

fn structured_user_message_lines(
    text: &str,
    width: usize,
    text_color: Option<Color>,
) -> Vec<Line<'static>> {
    let style = text_color.map_or_else(Style::default, |color| Style::default().fg(color));
    display_ranges(text, width.max(1), false)
        .into_iter()
        .map(|(start, end)| Line::from(Span::styled(text[start..end].to_string(), style)))
        .collect()
}

fn goal_status_label(status: GoalStatus) -> &'static str {
    match status {
        GoalStatus::Active => "▶ active",
        GoalStatus::Paused => "▮▮ paused",
        GoalStatus::Blocked => "■ blocked",
        GoalStatus::UsageLimited => "■ usage limit reached",
        GoalStatus::BudgetLimited => "■ token budget reached",
        GoalStatus::Complete => "complete",
    }
}

fn line_is_blank(line: &Line<'static>) -> bool {
    line.spans
        .iter()
        .all(|span| span.content.trim().is_empty())
}

fn line_is_unstyled_blank(line: &Line<'static>) -> bool {
    line.spans.is_empty()
}

fn extend_tool_lifecycle_spans(
    spans: &mut Vec<Span<'static>>,
    text: &str,
    lifecycle: Option<&str>,
    style: Style,
    backgrounded: bool,
) {
    let lifecycle_match = backgrounded
        .then_some(lifecycle)
        .flatten()
        .and_then(|lifecycle| text.rfind(lifecycle).map(|start| (lifecycle, start)));
    let Some((lifecycle, lifecycle_start)) = lifecycle_match else {
        spans.push(Span::styled(text.to_string(), style));
        return;
    };
    let lifecycle_end = lifecycle_start + lifecycle.len();
    if lifecycle_start > 0 {
        spans.push(Span::styled(text[..lifecycle_start].to_string(), style));
    }
    spans.push(Span::styled(
        text[lifecycle_start..lifecycle_end].to_string(),
        Style::default().fg(BACKGROUND_RUNNING_TEXT),
    ));
    if lifecycle_end < text.len() {
        spans.push(Span::styled(text[lifecycle_end..].to_string(), style));
    }
}

struct Transcript {
    order: Vec<TranscriptEntry>,
    messages: HashMap<Uuid, usize>,
    tools: HashMap<String, usize>,
    foreground_tool: Option<String>,
    preparing_tool: Option<String>,
    goal: Option<SessionGoal>,
    todos: Vec<PlanItem>,
    config: Option<SessionDisplayConfig>,
    active_turn: Option<ActiveTurnDisplayConfig>,
    live_turn_closed: bool,
    subagents: HashMap<Uuid, SubagentStatus>,
    subagent_snapshots: HashMap<Uuid, SubagentSnapshot>,
    subagent_entries: HashMap<Uuid, usize>,
    runtime_processes: HashMap<Uuid, RuntimeProcessProjection>,
    provider_backgrounds: HashMap<String, ProviderBackgroundProjection>,
    provider_followups: HashMap<String, String>,
    queued_messages: HashSet<Uuid>,
    queued_message_sequences: HashMap<Uuid, u64>,
    follow_tail: bool,
    selected: Option<usize>,
    auto_expand_edits: bool,
    auto_expand_tools: bool,
    user_label: String,
    assistant_label: String,
    user_label_color: Color,
    user_message_color: Color,
    assistant_label_color: Color,
    assistant_message_color: Color,
    context_remaining_percent: u8,
    context_known: bool,
    context_tokens: Option<u64>,
    context_window_tokens: Option<u64>,
    cache_diagnostics: CacheDiagnostics,
    tool_run_offsets: HashMap<usize, usize>,
    expanded_tool_runs: HashSet<usize>,
    active_reasoning: Option<usize>,
    last_edit: Option<usize>,
    next_image_number: usize,
    message_markdown_cache: RefCell<MessageMarkdownCache>,
    tool_body_cache: RefCell<ToolBodyCache>,
}

#[derive(Clone, Debug)]
struct RuntimeProcessProjection {
    command: String,
    pid: u32,
    tool_index: Option<usize>,
    running: bool,
}

#[derive(Clone, Debug)]
struct ProviderBackgroundProjection {
    command: String,
    tool_index: usize,
}

#[derive(Default)]
struct MessageMarkdownCache {
    width: usize,
    messages: HashMap<usize, MarkdownRender>,
    #[cfg(test)]
    misses: usize,
}

#[derive(Clone, Default)]
struct MarkdownRender {
    lines: Vec<Line<'static>>,
    links: Vec<LinkRowRange>,
}

#[derive(Default)]
struct ToolBodyCache {
    width: usize,
    lines: HashMap<(usize, bool, bool), Vec<Line<'static>>>,
    #[cfg(test)]
    misses: usize,
}

impl Default for Transcript {
    fn default() -> Self {
        Self {
            order: Vec::new(),
            messages: HashMap::new(),
            tools: HashMap::new(),
            foreground_tool: None,
            preparing_tool: None,
            goal: None,
            todos: Vec::new(),
            config: None,
            active_turn: None,
            live_turn_closed: false,
            subagents: HashMap::new(),
            subagent_snapshots: HashMap::new(),
            subagent_entries: HashMap::new(),
            runtime_processes: HashMap::new(),
            provider_backgrounds: HashMap::new(),
            provider_followups: HashMap::new(),
            queued_messages: HashSet::new(),
            queued_message_sequences: HashMap::new(),
            follow_tail: true,
            selected: None,
            auto_expand_edits: true,
            auto_expand_tools: false,
            user_label: "user".to_string(),
            assistant_label: "borg".to_string(),
            user_label_color: USER_LABEL_BLUE,
            user_message_color: USER_TEXT,
            assistant_label_color: BORG_ORANGE,
            assistant_message_color: Color::White,
            context_remaining_percent: 100,
            context_known: false,
            context_tokens: None,
            context_window_tokens: None,
            cache_diagnostics: CacheDiagnostics::default(),
            tool_run_offsets: HashMap::new(),
            expanded_tool_runs: HashSet::new(),
            active_reasoning: None,
            last_edit: None,
            next_image_number: 1,
            message_markdown_cache: RefCell::new(MessageMarkdownCache::default()),
            tool_body_cache: RefCell::new(ToolBodyCache::default()),
        }
    }
}

#[derive(Clone, Copy)]
struct ToolRunWindow {
    start: usize,
    end: usize,
    total: usize,
}

#[derive(Clone)]
struct SessionDisplayConfig {
    cwd: PathBuf,
    provider: CodingProvider,
    model: Option<String>,
    effort: Option<String>,
    response_language: ResponseLanguage,
    fast: bool,
    permission_mode: PermissionMode,
}

impl SessionDisplayConfig {
    fn cache_signature(&self) -> CacheSignature {
        CacheSignature::new(self.provider, self.model.as_deref(), self.effort.as_deref())
    }
}

struct ActiveTurnDisplayConfig {
    message_id: Uuid,
    provider: CodingProvider,
    model: Option<String>,
    effort: Option<String>,
}

impl ActiveTurnDisplayConfig {
    fn cache_signature(&self) -> CacheSignature {
        CacheSignature::new(self.provider, self.model.as_deref(), self.effort.as_deref())
    }
}

enum TranscriptEntry {
    Message {
        actor: EventActor,
        text: String,
        attachments: Vec<(usize, PathBuf)>,
        model: Option<String>,
        effort: Option<String>,
        time: String,
        status: MessageStatus,
        complete: bool,
    },
    Activity {
        text: String,
        time: String,
    },
    /// A durable, typed lifecycle row.  Agent/approval/provider-interaction
    /// rows used to be flattened into strings and later reparsed to decide
    /// colour, grouping, and state.  Keep the semantic fields together so a
    /// reconnect/update can replace one row without manufacturing duplicate
    /// transcript text.
    Action {
        kind: TranscriptActionKind,
        label: String,
        detail: String,
        body: Option<String>,
        time: String,
        state: TranscriptActionState,
        expanded: bool,
    },
    Plan {
        items: Vec<PlanItem>,
        time: String,
        expanded: bool,
    },
    Goal {
        goal: SessionGoal,
        time: String,
    },
    Info {
        title: String,
        text: String,
        time: String,
    },
    Compaction {
        summary: String,
        time: String,
        sequence: u64,
        expanded: bool,
        complete: bool,
    },
    Tool {
        source_name: String,
        name: String,
        detail: String,
        code_view: Option<(String, String)>,
        output_view: Option<(String, String)>,
        payload_refs: Vec<SessionPayloadRef>,
        time: String,
        started_at: DateTime<Utc>,
        completed_at: Option<DateTime<Utc>>,
        complete: bool,
        error: bool,
        user_interrupted: bool,
        backgrounded: bool,
        expanded: bool,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TranscriptActionKind {
    Agent,
    Approval,
    ProviderInteraction,
    Error,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TranscriptActionState {
    Running,
    Waiting,
    Complete,
    Stopped,
    Failed,
}

fn compaction_has_expandable_detail(summary: &str) -> bool {
    let detail = summary
        .strip_prefix("Compacted context: ")
        .unwrap_or(summary);
    let normalized = detail
        .split(|character: char| !character.is_alphanumeric())
        .filter(|word| !word.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();

    !matches!(
        normalized.as_str(),
        ""
            | "context compacted"
            | "context was compacted"
            | "no summary"
            | "no details"
    )
}

fn tool_has_expandable_body(
    source_name: &str,
    code_view: Option<&(String, String)>,
    output_view: Option<&(String, String)>,
) -> bool {
    !is_mcp_resource_probe(source_name)
        && [code_view, output_view]
            .into_iter()
            .flatten()
            .any(|(_, body)| !body.trim().is_empty())
}

fn transcript_entry_is_turn_output(entry: &TranscriptEntry) -> bool {
    matches!(
        entry,
        TranscriptEntry::Message {
            actor: EventActor::Assistant,
            ..
        } | TranscriptEntry::Tool { .. }
    )
}

impl Transcript {
    fn message_id_at(&self, index: usize) -> Option<Uuid> {
        self.messages
            .iter()
            .find_map(|(message_id, message_index)| (*message_index == index).then_some(*message_id))
    }

    fn upsert_subagent_snapshot(&mut self, agent: &SubagentSnapshot) {
        self.upsert_subagent_snapshot_with_status(agent, agent.status);
    }

    fn upsert_subagent_snapshot_with_status(
        &mut self,
        agent: &SubagentSnapshot,
        status: SubagentStatus,
    ) {
        self.subagents.insert(agent.session_id, status);
        let mut snapshot = agent.clone();
        snapshot.status = status;
        self.subagent_snapshots.insert(agent.session_id, snapshot);
    }

    fn project_optimistic_message(&mut self, event: &SessionEvent) {
        let _ = self.apply(event);
        self.live_turn_closed = false;
        if event.sequence == 0
            && let SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                status: MessageStatus::Complete,
                ..
            } = &event.kind
            && let Some(config) = self.config.as_ref()
        {
            // A submitted prompt is already a live turn from the user's
            // perspective. Suppress stale cold-cache guidance immediately
            // instead of waiting for the actor's TurnStarted round trip.
            self.active_turn = Some(ActiveTurnDisplayConfig {
                message_id: *message_id,
                provider: config.provider,
                model: config.model.clone(),
                effort: config.effort.clone(),
            });
        }
    }

    fn reserve_history(&mut self, event_count: usize) {
        self.order.reserve(event_count);
        self.messages.reserve(event_count / 4);
        self.tools.reserve(event_count / 4);
        self.subagents.reserve(event_count / 16);
        self.subagent_snapshots.reserve(event_count / 16);
        self.subagent_entries.reserve(event_count / 16);
    }

    fn seed_session_state(&mut self, state: &SessionState) {
        self.goal = state.goal.clone();
        self.todos = state.todos.clone();
        self.config = state
            .configuration
            .as_ref()
            .map(|configuration| SessionDisplayConfig {
                cwd: configuration.cwd.clone(),
                provider: configuration.provider,
                model: configuration.model.clone(),
                effort: configuration.effort.clone(),
                response_language: configuration.response_language,
                fast: configuration.fast,
                permission_mode: configuration.permission_mode,
            });
        if let (Some(context_tokens), Some(context_window_tokens)) = (
            state.usage.context_tokens,
            state.usage.context_window_tokens,
        ) {
            self.context_known = true;
            self.context_tokens = Some(context_tokens);
            self.context_window_tokens = Some(context_window_tokens);
            self.context_remaining_percent =
                context_remaining_percent(context_tokens, context_window_tokens);
        }
        self.live_turn_closed = matches!(
            state.status,
            Some(
                SessionStatus::Ready
                    | SessionStatus::Completed
                    | SessionStatus::Failed
                    | SessionStatus::Stopped
            )
        );
    }

    fn reconcile_session_status(&mut self, state: &SessionState) {
        if !matches!(
            state.status,
            Some(
                SessionStatus::Starting
                    | SessionStatus::Running
                    | SessionStatus::WaitingForApproval
            )
        ) {
            self.finish_running_tools(state.activity_at.unwrap_or_else(Utc::now), false, "");
        }
    }

    fn clear_visible_entries(&mut self) {
        self.order.clear();
        self.messages.clear();
        self.tools.clear();
        self.subagent_entries.clear();
        self.queued_messages.clear();
        self.queued_message_sequences.clear();
        self.tool_run_offsets.clear();
        self.expanded_tool_runs.clear();
        self.active_reasoning = None;
        self.last_edit = None;
        self.message_markdown_cache.get_mut().messages.clear();
        self.tool_body_cache.get_mut().lines.clear();
        self.selected = None;
        self.follow_tail = true;
    }

    fn show_goal(&mut self, goal: Option<&SessionGoal>) -> Option<usize> {
        let time = canonical_local_time(Local::now());
        match goal {
            Some(goal) => self.upsert_goal(goal.clone(), time),
            None => {
                self.order.push(TranscriptEntry::Activity {
                    text: "No durable goal is set. Use /goal OBJECTIVE to start one.".to_string(),
                    time,
                });
                None
            }
        }
    }

    fn optimistically_apply_goal_action(&mut self, action: &GoalAction) -> bool {
        if matches!(action, GoalAction::Clear) {
            return self.goal.take().is_some();
        }
        let updated_goal = {
            let Some(goal) = self.goal.as_mut() else {
                return false;
            };
            let next_status = match action {
                GoalAction::Pause if goal.status == GoalStatus::Active => GoalStatus::Paused,
                GoalAction::Resume
                    if matches!(
                        goal.status,
                        GoalStatus::Paused | GoalStatus::Blocked | GoalStatus::UsageLimited
                    ) => GoalStatus::Active,
                _ => return false,
            };
            goal.status = next_status;
            goal.updated_at = Utc::now();
            goal.clone()
        };
        self.upsert_goal(updated_goal, canonical_local_time(Local::now()));
        true
    }

    fn show_director_context_boundary(&mut self) {
        self.order.push(TranscriptEntry::Activity {
            text: DIRECTOR_CONTEXT_BOUNDARY.to_string(),
            time: canonical_local_time(Local::now()),
        });
    }

    fn show_plan(&mut self, items: &[PlanItem]) -> Option<usize> {
        let time = canonical_local_time(Local::now());
        self.upsert_plan(items.to_vec(), time)
    }

    fn upsert_goal(&mut self, goal: SessionGoal, time: String) -> Option<usize> {
        let removed = self
            .order
            .iter()
            .rposition(|entry| matches!(entry, TranscriptEntry::Goal { .. }));
        if let Some(index) = removed {
            self.order.remove(index);
            self.reindex_after_removal(index);
        }
        self.order.push(TranscriptEntry::Goal { goal, time });
        removed
    }

    fn upsert_plan(&mut self, items: Vec<PlanItem>, time: String) -> Option<usize> {
        let mut expanded = false;
        let removed = self
            .order
            .iter()
            .rposition(|entry| matches!(entry, TranscriptEntry::Plan { .. }));
        if let Some(index) = removed {
            if let Some(TranscriptEntry::Plan {
                expanded: previous, ..
            }) = self.order.get(index)
            {
                expanded = *previous;
            }
            self.order.remove(index);
            self.reindex_after_removal(index);
        }
        self.order.push(TranscriptEntry::Plan {
            items,
            time,
            expanded,
        });
        removed
    }

    fn toggle_plan_expansion(&mut self, index: usize) {
        if let Some(TranscriptEntry::Plan { expanded, .. }) = self.order.get_mut(index) {
            *expanded = !*expanded;
        }
    }

    fn toggle_compaction_expansion(&mut self, index: usize) {
        if let Some(TranscriptEntry::Compaction {
            expanded,
            summary,
            complete,
            ..
        }) = self.order.get_mut(index)
            && *complete
            && compaction_has_expandable_detail(summary)
        {
            *expanded = !*expanded;
        }
    }

    fn compaction_is_expandable(&self, index: usize) -> bool {
        matches!(
            self.order.get(index),
            Some(TranscriptEntry::Compaction {
                summary,
                complete: true,
                ..
            }) if compaction_has_expandable_detail(summary)
        )
    }

    fn action_is_expandable(&self, index: usize) -> bool {
        matches!(
            self.order.get(index),
            Some(TranscriptEntry::Action {
                body: Some(body), ..
            }) if !body.trim().is_empty()
        )
    }

    fn toggle_action_expansion(&mut self, index: usize) {
        if let Some(TranscriptEntry::Action { expanded, body, .. }) = self.order.get_mut(index)
            && body.as_deref().is_some_and(|body| !body.trim().is_empty())
        {
            *expanded = !*expanded;
        }
    }

    fn compaction_revert_sequence(&self, index: usize) -> Option<u64> {
        let TranscriptEntry::Compaction {
            summary, sequence, complete, ..
        } = self.order.get(index)?
        else {
            return None;
        };
        (*complete && *sequence > 0 && compaction_has_expandable_detail(summary))
            .then(|| sequence.checked_add(1))
            .flatten()
    }

    fn plan_is_clippable(&self, index: usize) -> bool {
        matches!(
            self.order.get(index),
            Some(TranscriptEntry::Plan { items, .. }) if items.len() > MAX_COLLAPSED_PLAN_ITEMS
        )
    }

    #[cfg(test)]
    fn tool_is_expandable(&self, index: usize) -> bool {
        matches!(
            self.order.get(index),
            Some(TranscriptEntry::Tool {
                source_name,
                code_view,
                output_view,
                ..
            }) if tool_has_expandable_body(source_name, code_view.as_ref(), output_view.as_ref())
        )
    }

    #[cfg(test)]
    fn toggle_tool(&mut self, index: usize) -> Vec<SessionPayloadRef> {
        if !self.tool_is_expandable(index) {
            return Vec::new();
        }
        if let Some(TranscriptEntry::Tool {
            expanded,
            payload_refs,
            ..
        }) = self.order.get_mut(index)
        {
            *expanded = !*expanded;
            if *expanded {
                return payload_refs.clone();
            }
        }
        Vec::new()
    }

    #[cfg(test)]
    fn tool_is_expanded(&self, index: usize) -> bool {
        matches!(
            self.order.get(index),
            Some(TranscriptEntry::Tool { expanded: true, .. })
        ) && self.tool_is_expandable(index)
    }

    fn tool_payloads(&self, index: usize) -> Vec<SessionPayloadRef> {
        match self.order.get(index) {
            Some(TranscriptEntry::Tool { payload_refs, .. }) => payload_refs.clone(),
            _ => Vec::new(),
        }
    }

    /// Return the footer affordance for a tool while the pointer is over its
    /// rendered row. Edit tools expose their diff as the call body, while
    /// other tools expose their completed response as the output body.
    fn tool_copy_hint(&self, index: usize) -> Option<&'static str> {
        let TranscriptEntry::Tool {
            code_view,
            output_view,
            detail,
            ..
        } = self.order.get(index)?
        else {
            return None;
        };
        if code_view
            .as_ref()
            .is_some_and(|(language, body)| is_diff_language(language) && !body.trim().is_empty())
        {
            Some("left click inspect · right click copy diff")
        } else if output_view
            .as_ref()
            .is_some_and(|(_, body)| !body.trim().is_empty())
        {
            Some("left click inspect · right click copy output")
        } else if code_view
            .as_ref()
            .is_some_and(|(language, body)| language == "reasoning" && !body.trim().is_empty())
        {
            Some("left click inspect · right click copy thinking")
        } else if code_view
            .as_ref()
            .is_some_and(|(_, body)| !body.trim().is_empty())
        {
            Some("left click inspect · right click copy tool call")
        } else if !detail.trim().is_empty() {
            Some("right click copy tool details")
        } else {
            None
        }
    }

    fn hydrate_payload(&mut self, payload: &SessionPayloadRef, bytes: Vec<u8>) -> Result<()> {
        let Some(TranscriptEntry::Tool {
            source_name,
            name,
            detail,
            code_view,
            output_view,
            error,
            backgrounded,
            payload_refs,
            ..
        }) = self.order.iter_mut().find(|entry| {
            matches!(
                entry,
                TranscriptEntry::Tool { payload_refs, .. }
                    if payload_refs.iter().any(|candidate| candidate.id == payload.id)
            )
        })
        else {
            return Ok(());
        };
        match payload.kind {
            SessionPayloadKind::ToolInput => {
                let input: serde_json::Value = serde_json::from_slice(&bytes)
                    .context("stored tool input is not valid JSON")?;
                let presentation = project_tool_presentation(source_name, &input, None, false);
                *name = presentation.label;
                *detail = presentation.detail;
                *code_view = presentation.input.map(|body| (body.language, body.text));
            }
            SessionPayloadKind::ToolOutput => {
                let output =
                    String::from_utf8(bytes).context("stored tool output is not valid UTF-8")?;
                *backgrounded = !*error && tool_output_is_backgrounded(&output);
                let hydrated_presentation = project_tool_presentation(
                    source_name,
                    &serde_json::Value::Null,
                    Some(&output),
                    *error,
                );
                let edit_diff = (!*error)
                    .then(|| hydrated_presentation.output.clone())
                    .flatten()
                    .filter(|body| is_diff_language(&body.language));
                if let Some(body) = edit_diff {
                    *name = hydrated_presentation.label;
                    *detail = hydrated_presentation.detail;
                    *code_view = Some((body.language, body.text));
                    *output_view = None;
                } else {
                    *output_view = if is_mcp_resource_probe(source_name) {
                        None
                    } else if *error && !output.trim().is_empty() {
                        Some(("text".to_string(), output.trim_end().to_string()))
                    } else {
                        tool_output_code_view(name, &output)
                    };
                }
            }
            SessionPayloadKind::ToolResultInput => {
                let input: serde_json::Value = serde_json::from_slice(&bytes)
                    .context("stored tool result input is not valid JSON")?;
                if name == "Search web"
                    && let Some(query) = web_search_query(&input)
                {
                    *detail = format!("“{}”", compact_text(&query, 120));
                }
            }
        }
        payload_refs.retain(|candidate| candidate.id != payload.id);
        Ok(())
    }

    fn anchor_tool_run(&mut self, start: usize, max_offset: usize) {
        let current = self
            .tool_run_offsets
            .get(&start)
            .copied()
            .unwrap_or(max_offset)
            .min(max_offset);
        self.tool_run_offsets.insert(start, current);
    }

    fn scroll_tool_run(&mut self, start: usize, max_offset: usize, delta: isize) -> bool {
        if max_offset == 0 {
            return false;
        }
        let current = self.tool_run_offset(start, max_offset);
        let next = current.saturating_add_signed(delta).min(max_offset);
        if next == current {
            return false;
        }
        if next == max_offset {
            self.tool_run_offsets.remove(&start);
        } else {
            self.tool_run_offsets.insert(start, next);
        }
        true
    }

    fn tool_run_offset(&self, start: usize, max_offset: usize) -> usize {
        self.tool_run_offsets
            .get(&start)
            .copied()
            .unwrap_or(max_offset)
            .min(max_offset)
    }

    fn toggle_tool_run_expansion(&mut self, start: usize) -> bool {
        if self.expanded_tool_runs.contains(&start) {
            self.expanded_tool_runs.remove(&start);
            false
        } else {
            self.expanded_tool_runs.insert(start);
            true
        }
    }

    fn tool_run_expanded(&self, start: usize) -> bool {
        self.expanded_tool_runs.contains(&start)
    }

    fn tool_run_start_containing(&self, index: usize) -> Option<usize> {
        if !matches!(self.order.get(index), Some(TranscriptEntry::Tool { .. })) {
            return None;
        }
        self.tool_run_windows()[index].map(|window| window.start)
    }

    fn apply(&mut self, event: &SessionEvent) -> Option<usize> {
        self.apply_event(event, true)
    }

    fn apply_history(&mut self, event: &SessionEvent) -> Option<usize> {
        self.apply_event(event, false)
    }

    fn apply_event(
        &mut self,
        event: &SessionEvent,
        reorder_late_user_messages: bool,
    ) -> Option<usize> {
        let provider_advanced = matches!(
            &event.kind,
            SessionEventKind::Message {
                actor: EventActor::Assistant,
                ..
            } | SessionEventKind::ReasoningDelta { .. }
                | SessionEventKind::ReasoningCompleted
                | SessionEventKind::ToolStarted { .. }
        ) || matches!(
            &event.kind,
            SessionEventKind::ProviderEvent { kind, payload, .. }
                if Self::provider_reasoning_lifecycle(kind, payload).is_some()
        );
        let starts_prepared_action = matches!(event.kind, SessionEventKind::ToolStarted { .. })
            && self.preparing_tool.is_some();
        if provider_advanced && !starts_prepared_action {
            self.mark_running_tools_backgrounded();
        }
        let completed_turn = match &event.kind {
            SessionEventKind::TurnCompleted { message_id, .. }
            | SessionEventKind::PromptRecalled { message_id, .. } => Some(*message_id),
            _ => None,
        };
        if let Some(message_id) = completed_turn
            && self
                .active_turn
                .as_ref()
                .is_some_and(|turn| turn.message_id == message_id)
        {
            self.active_turn = None;
        }
        match &event.kind {
            SessionEventKind::TurnStarted { .. }
            | SessionEventKind::StatusChanged {
                status:
                    SessionStatus::Starting | SessionStatus::Running | SessionStatus::WaitingForApproval,
                ..
            } => self.live_turn_closed = false,
            SessionEventKind::StatusChanged {
                status:
                    SessionStatus::Ready
                    | SessionStatus::Completed
                    | SessionStatus::Failed
                    | SessionStatus::Stopped,
                ..
            } => {
                self.live_turn_closed = true;
                self.finish_live_assistant_messages(event.created_at);
            }
            SessionEventKind::TurnCompleted { .. } => {
                self.live_turn_closed = true;
                self.finish_live_assistant_messages(event.created_at);
            }
            _ => {}
        }
        let mut removed_entry = None;
        match &event.kind {
            SessionEventKind::SessionConfigured {
                cwd,
                provider,
                model,
                effort,
                response_language,
                fast,
                permission_mode,
                ..
            } => {
                let context_identity_changed = self.config.as_ref().is_some_and(|old| {
                    old.provider != *provider || old.model.as_ref() != model.as_ref()
                });
                self.config = Some(SessionDisplayConfig {
                    cwd: cwd.clone(),
                    provider: *provider,
                    model: model.clone(),
                    effort: effort.clone(),
                    response_language: *response_language,
                    fast: *fast,
                    permission_mode: *permission_mode,
                });
                if context_identity_changed {
                    // Context usage belongs to the provider/model identity that
                    // produced it. Do not carry the old model's percentage over
                    // while the new provider is preparing its first report.
                    self.context_known = false;
                    self.context_remaining_percent = 100;
                    self.context_tokens = None;
                    self.context_window_tokens = None;
                }
            }
            SessionEventKind::TurnStarted {
                message_id,
                provider,
                model,
                effort,
                ..
            } => {
                self.active_turn = Some(ActiveTurnDisplayConfig {
                    message_id: *message_id,
                    provider: *provider,
                    model: model.clone(),
                    effort: effort.clone(),
                });
            }
            SessionEventKind::UsageUpdated {
                turn_id,
                provider_context_reused,
                input_tokens,
                cached_input_tokens,
                cache_creation_input_tokens,
                cost_microusd,
                cost_basis,
                context_tokens,
                context_window_tokens,
                ..
            } => {
                if let (Some(context_tokens), Some(context_window_tokens)) =
                    (context_tokens, context_window_tokens)
                {
                    self.context_known = true;
                    self.context_tokens = Some(*context_tokens);
                    self.context_window_tokens = Some(*context_window_tokens);
                    self.context_remaining_percent =
                        context_remaining_percent(*context_tokens, *context_window_tokens);
                }
                let usage_belongs_to_active_turn = turn_id.is_none_or(|turn_id| {
                    self.active_turn
                        .as_ref()
                        .is_some_and(|active| active.message_id == turn_id)
                });
                if usage_belongs_to_active_turn
                    && let Some(signature) = self
                        .active_turn
                        .as_ref()
                        .map(ActiveTurnDisplayConfig::cache_signature)
                        .or_else(|| {
                            self.config
                                .as_ref()
                                .map(SessionDisplayConfig::cache_signature)
                        })
                    && let Some(notice) = self.cache_diagnostics.observe(
                        event.created_at,
                        signature,
                        CacheUsage {
                            input_tokens: *input_tokens,
                            cached_input_tokens: *cached_input_tokens,
                            cache_creation_input_tokens: *cache_creation_input_tokens,
                            context_tokens: *context_tokens,
                            cost_microusd: *cost_microusd,
                            cost_basis,
                            provider_context_reused: *provider_context_reused,
                        },
                    )
                {
                    self.order.push(TranscriptEntry::Info {
                        title: "Prompt cache miss".to_string(),
                        text: notice.text(),
                        time: local_event_time(event),
                    });
                }
            }
            SessionEventKind::ContextWindowUpdated {
                context_tokens,
                context_window_tokens,
            } => {
                self.context_known = true;
                self.context_tokens = Some(*context_tokens);
                self.context_window_tokens = Some(*context_window_tokens);
                self.context_remaining_percent =
                    context_remaining_percent(*context_tokens, *context_window_tokens);
            }
            SessionEventKind::ContextCleared => {
                self.clear_visible_entries();
                self.context_remaining_percent = 100;
                self.context_known = true;
                self.context_tokens = self.context_window_tokens.map(|_| 0);
                self.cache_diagnostics.reset();
            }
            SessionEventKind::PromptRecalled { message_id, .. } => {
                self.queued_messages.remove(message_id);
                self.queued_message_sequences.remove(message_id);
                removed_entry = self.remove_message(*message_id);
            }
            SessionEventKind::Message {
                message_id,
                actor,
                text,
                status,
                attachments,
                delivery: _,
            } => {
                // A coalesced live snapshot can arrive after its durable
                // terminal boundary during reconnect. Never resurrect a
                // responding row once the active turn has been closed.
                if *actor == EventActor::Assistant
                    && *status == MessageStatus::InProgress
                    && self.live_turn_closed
                {
                    return removed_entry;
                }
                // Team delivery is provider input, not a human-authored chat
                // message. Its child-authored report is projected separately
                // through SubagentActivity with the correct agent identity.
                // System delivery is provider input, not a chat row. The
                // child-authored report is rendered through SubagentActivity.
                if *actor == EventActor::System {
                    removed_entry = self.remove_message(*message_id);
                    return removed_entry;
                }
                // Queued prompts belong to the pending-input projection only.
                // Materializing an invisible transcript row here would pin the
                // eventual admitted message to its enqueue position instead of
                // the real provider-boundary chronology.
                if *status == MessageStatus::Queued {
                    self.queued_messages.insert(*message_id);
                    if event.sequence > 0 {
                        self.queued_message_sequences
                            .insert(*message_id, event.sequence);
                    }
                    // An accepted steer may have briefly materialized as an
                    // in-progress transcript row. If the turn is interrupted,
                    // its later queue transition must withdraw that row again.
                    removed_entry = self.remove_message(*message_id);
                    return removed_entry;
                }
                if *actor == EventActor::Assistant && text.trim().is_empty() {
                    removed_entry = self.remove_message(*message_id);
                    return removed_entry;
                }
                if *actor == EventActor::Assistant {
                    self.finish_reasoning(event.created_at);
                }
                if let Some(index) = self.messages.get(message_id).copied() {
                    self.message_markdown_cache
                        .get_mut()
                        .messages
                        .remove(&index);
                    let numbered_attachments = matches!(
                        &self.order[index],
                        TranscriptEntry::Message {
                            attachments: stored,
                            ..
                        } if stored.is_empty() && !attachments.is_empty()
                    )
                    .then(|| {
                        number_message_attachments(text, attachments, &mut self.next_image_number)
                    });
                    if let TranscriptEntry::Message {
                        actor: stored_actor,
                        text: stored_text,
                        attachments: stored_attachments,
                        status: stored_status,
                        complete,
                        ..
                    } = &mut self.order[index]
                    {
                        *stored_actor = *actor;
                        *stored_text = text.clone();
                        *stored_status = *status;
                        *complete =
                            matches!(*status, MessageStatus::Complete | MessageStatus::Failed);
                        if let Some(attachments) = numbered_attachments {
                            *stored_attachments = attachments;
                        }
                    }
                } else {
                    let attachments =
                        number_message_attachments(text, attachments, &mut self.next_image_number);
                    let (model, effort) = if *actor == EventActor::Assistant {
                        self.active_turn
                            .as_ref()
                            .map(|turn| (turn.model.clone(), turn.effort.clone()))
                            .or_else(|| {
                                self.config
                                    .as_ref()
                                    .map(|config| (config.model.clone(), config.effort.clone()))
                            })
                            .unwrap_or_default()
                    } else {
                        (None, None)
                    };
                    if *status != MessageStatus::Queued
                        && matches!(actor, EventActor::User | EventActor::Assistant)
                    {
                        self.collapse_previous_edit();
                    }
                    let queued = self.queued_messages.remove(message_id);
                    let queued_sequence = self.queued_message_sequences.get(message_id).copied();
                    let insertion_index = if *actor == EventActor::User
                        && matches!(status, MessageStatus::Complete | MessageStatus::Failed)
                        && event.sequence > 0
                        && !self.live_turn_closed
                        && reorder_late_user_messages
                        && (!queued || queued_sequence.is_some())
                    {
                        queued_sequence.map_or_else(
                            || self.late_user_message_insertion_index(),
                            |sequence| self.queued_user_message_insertion_index(sequence),
                        )
                    } else {
                        self.order.len()
                    };
                    if insertion_index < self.order.len() {
                        self.reindex_after_insertion(insertion_index);
                    }
                    self.messages.insert(*message_id, insertion_index);
                    self.order.insert(insertion_index, TranscriptEntry::Message {
                        actor: *actor,
                        text: text.clone(),
                        attachments,
                        model,
                        effort,
                        time: local_event_time(event),
                        status: *status,
                        complete: matches!(
                            *status,
                            MessageStatus::Complete | MessageStatus::Failed
                        ),
                    });
                }
            }
            SessionEventKind::ReasoningDelta { text } => {
                self.append_reasoning(text, event.created_at, local_event_time(event));
            }
            SessionEventKind::ReasoningCompleted => {
                self.finish_reasoning(event.created_at);
            }
            SessionEventKind::ProviderEvent { kind, payload, .. }
                if kind == "action/preparing" =>
            {
                let Some(label) = payload.get("label").and_then(serde_json::Value::as_str) else {
                    return removed_entry;
                };
                if self.preparing_tool.is_none() {
                    self.mark_running_tools_backgrounded();
                }
                let tool_call_id = self.preparing_tool.clone().unwrap_or_else(|| {
                    format!("action-preparing:{}", event.id)
                });
                self.preparing_tool = Some(tool_call_id.clone());
                let input = serde_json::json!({"label": label});
                self.upsert_running_tool(
                    event,
                    &tool_call_id,
                    "action_preparing",
                    &input,
                    None,
                );
            }
            SessionEventKind::ProviderEvent { kind, payload, .. }
                if is_live_tool_call_event(kind) =>
            {
                let Some(tool_call_id) = payload
                    .get("tool_call_id")
                    .and_then(serde_json::Value::as_str)
                else {
                    return removed_entry;
                };
                let Some(raw_name) = payload.get("name").and_then(serde_json::Value::as_str)
                else {
                    return removed_entry;
                };
                let input = payload
                    .get("input")
                    .unwrap_or(&serde_json::Value::Null);
                self.promote_preparing_tool(tool_call_id);
                self.upsert_running_tool(event, tool_call_id, raw_name, input, None);
            }
            SessionEventKind::ToolStarted {
                tool_call_id,
                name,
                input,
                input_ref,
            } => {
                self.promote_preparing_tool(tool_call_id);
                self.upsert_running_tool(
                    event,
                    tool_call_id,
                    name,
                    input,
                    input_ref.as_ref(),
                );
            }
            SessionEventKind::ToolUpdated {
                tool_call_id,
                name,
                input,
            } => {
                self.promote_preparing_tool(tool_call_id);
                self.upsert_running_tool(event, tool_call_id, name, input, None);
            }
            SessionEventKind::ToolCompleted {
                tool_call_id,
                output,
                output_ref,
                is_error,
                input,
                input_ref,
            } => {
                self.complete_preparing_tool(tool_call_id);
                if self.foreground_tool.as_deref() == Some(tool_call_id) {
                    self.foreground_tool = None;
                }
                let auto_expand_edits = self.auto_expand_edits;
                let tool_index = self.tools.get(tool_call_id).copied();
                let (source_name_for_process, command_for_process) = tool_index
                    .and_then(|index| self.order.get(index))
                    .and_then(|entry| match entry {
                        TranscriptEntry::Tool {
                            source_name,
                            detail,
                            ..
                        } => Some((source_name.clone(), detail.clone())),
                        _ => None,
                    })
                    .unwrap_or_default();
                let stored_followup_handle = self.provider_followups.remove(tool_call_id);
                let followup_handle =
                    tool_process_followup_handle(&source_name_for_process, input.as_ref())
                        .or(stored_followup_handle);
                let reported_background_handle =
                    (!*is_error).then(|| tool_output_background_handle(output)).flatten();
                let output_handle = (tool_can_start_background_process(&source_name_for_process))
                    .then(|| reported_background_handle.clone())
                    .flatten();
                if let Some(index) = tool_index
                    && let TranscriptEntry::Tool {
                        source_name,
                        name,
                        detail,
                        code_view,
                        output_view,
                        completed_at,
                        complete,
                        error,
                        backgrounded,
                        expanded,
                        payload_refs,
                        ..
                    } = &mut self.order[index]
                {
                    self.tool_body_cache
                        .get_mut()
                        .lines
                        .retain(|(tool_index, _, _), _| *tool_index != index);
                    payload_refs.extend(output_ref.iter().cloned());
                    payload_refs.extend(input_ref.iter().cloned());
                    if name == "Search web"
                        && let Some(query) = input.as_ref().and_then(web_search_query)
                    {
                        *detail = format!("“{}”", compact_text(&query, 120));
                    }
                    if *is_error && !output.trim().is_empty() {
                        let message = output.lines().next().unwrap_or_default();
                        let message = compact_text(message, 120);
                        if detail.trim().is_empty() {
                            *detail = message;
                        } else {
                            *detail = format!("{detail} · {message}");
                        }
                    }
                    *complete = true;
                    *completed_at = Some(event.created_at);
                    *error = *is_error;
                    *backgrounded = !*is_error && tool_output_is_backgrounded(output);
                    let completion_presentation = project_tool_presentation(
                        source_name,
                        input.as_ref().unwrap_or(&serde_json::Value::Null),
                        Some(output),
                        *is_error,
                    );
                    if completion_presentation.category == ToolPresentationCategory::Read
                        && !completion_presentation.detail.is_empty()
                    {
                        *detail = completion_presentation.detail.clone();
                    }
                    // Keep the result summary on the tool header when the
                    // input had no useful detail (for example `git status`),
                    // while leaving the full output in the expandable tool
                    // body. This keeps command output out of assistant
                    // message bubbles without making completed tools
                    // anonymous.
                    if detail.trim().is_empty()
                        && let Some(result) = completion_presentation.result.as_deref()
                    {
                        *detail = compact_text(result, 120);
                    }
                    let input_is_diff = code_view
                        .as_ref()
                        .is_some_and(|(language, _)| is_diff_language(language));
                    if completion_presentation.category == ToolPresentationCategory::Edit {
                        if let Some(body) = completion_presentation
                            .output
                            .filter(|body| is_diff_language(&body.language))
                        {
                            *name = completion_presentation.label;
                            *detail = completion_presentation.detail;
                            *code_view = Some((body.language, body.text));
                            *output_view = None;
                            *expanded = auto_expand_edits;
                        } else if let Some(body) = completion_presentation
                            .input
                            .filter(|body| is_diff_language(&body.language))
                        {
                            // Some providers start a tool before its input is
                            // complete, then attach the authoritative edit
                            // envelope to ToolCompleted. Replace the stale
                            // JSON preview with the parsed diff.
                            *name = completion_presentation.label;
                            *detail = completion_presentation.detail;
                            *code_view = Some((body.language, body.text));
                            *output_view = None;
                            *expanded = auto_expand_edits;
                        } else if input_is_diff && !*is_error {
                            // Native edit tools return a mutation receipt rather
                            // than a second diff. Keep the useful proposed diff
                            // as the sole expanded body instead of appending an
                            // opaque JSON receipt below it.
                            *output_view = None;
                            *expanded = auto_expand_edits;
                        } else {
                            *output_view = if is_mcp_resource_probe(source_name) {
                                None
                            } else if *is_error && !output.trim().is_empty() {
                                Some(("text".to_string(), output.trim_end().to_string()))
                            } else {
                                tool_output_code_view(name, output)
                            };
                        }
                    } else {
                        *output_view = if is_mcp_resource_probe(source_name) {
                            None
                        } else if *is_error && !output.trim().is_empty() {
                            Some(("text".to_string(), output.trim_end().to_string()))
                        } else {
                            borg_control_tool_output_view(source_name, input.as_ref(), output)
                                .or_else(|| {
                                    borg_lsp_diagnostics_view(source_name, input.as_ref(), output)
                                })
                                .map(|text| {
                                    (
                                        if is_subagent_tool(source_name) {
                                            "subagent"
                                        } else {
                                            "command"
                                        }
                                        .to_string(),
                                        text,
                                    )
                                })
                                .or_else(|| tool_output_code_view(name, output))
                        };
                    }
                    let _ = name;
                }
                if let (Some(index), Some(handle)) = (tool_index, output_handle.as_ref()) {
                    let is_native_process = Uuid::parse_str(handle)
                        .ok()
                        .is_some_and(|process_id| self.runtime_processes.contains_key(&process_id));
                    if !is_native_process {
                        self.provider_backgrounds
                            .entry(handle.clone())
                            .or_insert_with(|| ProviderBackgroundProjection {
                                command: command_for_process.clone(),
                                tool_index: index,
                            });
                    }
                }
                if reported_background_handle.is_none()
                    && let Some(handle) = followup_handle
                    && let Some(process) = self.provider_backgrounds.remove(&handle)
                    && let Some(TranscriptEntry::Tool {
                        output_view,
                        backgrounded,
                        ..
                    }) = self.order.get_mut(process.tool_index)
                {
                    self.tool_body_cache
                        .get_mut()
                        .lines
                        .retain(|(tool_index, _, _), _| *tool_index != process.tool_index);
                    let output = tool_process_output_text(output);
                    if !output.trim().is_empty() {
                        *output_view = Some(("text".to_string(), output));
                    }
                    *backgrounded = false;
                }
            }
            SessionEventKind::StatusChanged {
                status: SessionStatus::Ready,
                detail: Some(detail),
            } if detail.eq_ignore_ascii_case("interrupted") => {
                self.mark_running_tools_user_interrupted(event.created_at);
            }
            SessionEventKind::TurnCompleted {
                error: Some(error), ..
            } if error.to_ascii_lowercase().contains("interrupted") => {
                self.mark_running_tools_user_interrupted(event.created_at);
            }
            SessionEventKind::TurnCompleted {
                error: Some(error), ..
            } => {
                self.finish_running_tools(event.created_at, true, error);
            }
            SessionEventKind::TurnCompleted { error: None, .. } => {
                self.finish_running_tools(event.created_at, false, "");
            }
            SessionEventKind::ApprovalRequested { title, detail, .. } => {
                self.finish_reasoning(event.created_at);
                self.order.push(TranscriptEntry::Action {
                    kind: TranscriptActionKind::Approval,
                    label: "Approval".to_string(),
                    detail: format_action_detail(title, detail),
                    body: (!detail.trim().is_empty()).then(|| detail.clone()),
                    time: local_event_time(event),
                    state: TranscriptActionState::Waiting,
                    expanded: false,
                })
            }
            SessionEventKind::ProviderInteractionRequested {
                title,
                detail,
                payload,
                ..
            } => {
                self.finish_reasoning(event.created_at);
                let options = provider_interaction_options(payload);
                self.order.push(TranscriptEntry::Action {
                    kind: TranscriptActionKind::ProviderInteraction,
                    label: "Input needed".to_string(),
                    detail: if options.is_empty() {
                        format_action_detail(title, detail)
                    } else {
                        format!("{} · {options}", format_action_detail(title, detail))
                    },
                    body: (!detail.trim().is_empty()).then(|| detail.clone()),
                    time: local_event_time(event),
                    state: TranscriptActionState::Waiting,
                    expanded: false,
                })
            }
            SessionEventKind::GoalUpdated { goal } => {
                self.goal = Some(goal.clone());
                self.upsert_goal(goal.clone(), local_event_time(event));
            }
            SessionEventKind::GoalCleared { .. } => {
                self.goal = None;
                self.order.push(TranscriptEntry::Activity {
                    text: "goal cleared".to_string(),
                    time: local_event_time(event),
                });
            }
            SessionEventKind::PlanUpdated { items } => {
                self.todos = items.clone();
                self.upsert_plan(items.clone(), local_event_time(event));
            }
            SessionEventKind::RuntimeProcessStarted {
                process_id,
                pid,
                command,
                ..
            } => {
                let tool_index = self
                    .foreground_tool
                    .as_ref()
                    .and_then(|tool_call_id| self.tools.get(tool_call_id))
                    .copied();
                self.runtime_processes.insert(
                    *process_id,
                    RuntimeProcessProjection {
                        command: command.clone(),
                        pid: *pid,
                        tool_index,
                        running: true,
                    },
                );
            }
            SessionEventKind::RuntimeProcessCompleted {
                process_id,
                stdout,
                stderr,
                ..
            } => {
                let tool_index = self.runtime_processes.get(process_id).and_then(|process| process.tool_index);
                if let Some(process) = self.runtime_processes.get_mut(process_id) {
                    process.running = false;
                }
                if let Some(tool_index) = tool_index
                    && !self.runtime_processes.values().any(|process| {
                        process.running && process.tool_index == Some(tool_index)
                    })
                    && let Some(TranscriptEntry::Tool {
                        output_view,
                        backgrounded,
                        ..
                    }) = self.order.get_mut(tool_index)
                {
                    self.tool_body_cache
                        .get_mut()
                        .lines
                        .retain(|(index, _, _), _| *index != tool_index);
                    let output = [stdout.as_str(), stderr.as_str()]
                        .into_iter()
                        .filter(|text| !text.is_empty())
                        .collect::<Vec<_>>()
                        .join("\n");
                    if !output.trim().is_empty() {
                        *output_view = Some(("text".to_string(), output));
                    }
                    *backgrounded = false;
                }
            }
            SessionEventKind::ProviderEvent { kind, payload, .. }
                if Self::provider_reasoning_lifecycle(kind, payload) == Some(true) =>
            {
                self.start_reasoning(event.created_at, local_event_time(event));
            }
            SessionEventKind::ProviderEvent { kind, payload, .. }
                if Self::provider_reasoning_lifecycle(kind, payload) == Some(false) =>
            {
                self.finish_reasoning(event.created_at);
            }
            SessionEventKind::ProviderEvent { kind, .. }
                if kind == "context_compaction_failed" =>
            {
                self.finish_reasoning(event.created_at);
                self.cache_diagnostics.reset();
                if matches!(
                    self.order.last(),
                    Some(TranscriptEntry::Compaction {
                        complete: false,
                        ..
                    })
                ) {
                    removed_entry = self.order.len().checked_sub(1);
                    self.order.pop();
                }
            }
            SessionEventKind::ProviderEvent { kind, payload, .. }
                if is_context_compaction(kind) =>
            {
                self.finish_reasoning(event.created_at);
                self.cache_diagnostics.reset();
                let started = context_compaction_started(kind, payload);
                if started {
                    if let Some(TranscriptEntry::Compaction {
                        summary,
                        time,
                        sequence,
                        expanded,
                        complete,
                    }) = self.order.last_mut()
                        && !*complete
                    {
                        *summary = "Compacting context…".to_string();
                        *time = local_event_time(event);
                        *sequence = event.sequence;
                        *expanded = false;
                    } else {
                        self.order.push(TranscriptEntry::Compaction {
                            summary: "Compacting context…".to_string(),
                            time: local_event_time(event),
                            sequence: event.sequence,
                            expanded: false,
                            complete: false,
                        });
                    }
                } else {
                    let summary = context_compaction_card_summary(payload);
                    if let Some(TranscriptEntry::Compaction {
                        summary: previous,
                        time,
                        sequence,
                        complete,
                        expanded,
                    }) = self.order.last_mut()
                        && !*complete
                    {
                        *previous = summary;
                        *time = local_event_time(event);
                        *sequence = event.sequence;
                        *complete = true;
                        *expanded = false;
                    } else if !matches!(
                        self.order.last(),
                        Some(TranscriptEntry::Compaction {
                            summary: previous,
                            complete: true,
                            ..
                        }) if previous == &summary
                    ) {
                        self.order.push(TranscriptEntry::Compaction {
                            summary,
                            time: local_event_time(event),
                            sequence: event.sequence,
                            expanded: false,
                            complete: true,
                        });
                    }
                }
            }
            SessionEventKind::SubagentActivity {
                activity,
                agent,
                event: child_event,
            } => {
                let status = effective_subagent_status(
                    *activity,
                    agent.status,
                    child_event.as_deref(),
                );
                self.upsert_subagent_snapshot_with_status(agent, status);
                if let Some((label, detail, body, state)) =
                    subagent_action_projection(*activity, agent, child_event.as_deref())
                {
                    let time = local_event_time(event);
                    if let Some(index) = self.subagent_entries.get(&agent.session_id).copied() {
                        match self.order.get_mut(index) {
                            Some(TranscriptEntry::Action {
                                label: existing_label,
                                detail: existing_detail,
                                body: existing_body,
                                time: existing_time,
                                state: existing_state,
                                expanded,
                                ..
                            }) => {
                                *existing_label = label;
                                *existing_detail = detail;
                                *existing_body = body;
                                *existing_time = time;
                                *existing_state = state;
                                if existing_body.is_none() {
                                    *expanded = false;
                                }
                            }
                            Some(TranscriptEntry::Activity {
                                text: existing,
                                time: existing_time,
                            }) => {
                                *existing = format_action_text(&label, &detail, body.as_deref());
                                *existing_time = time;
                            }
                            _ => {}
                        }
                    } else {
                        self.subagent_entries
                            .insert(agent.session_id, self.order.len());
                        self.order.push(TranscriptEntry::Action {
                            kind: TranscriptActionKind::Agent,
                            label,
                            detail,
                            body,
                            time,
                            state,
                            expanded: false,
                        });
                    }
                }
            }
            SessionEventKind::Error { message } => {
                self.finish_reasoning(event.created_at);
                self.order.push(TranscriptEntry::Action {
                    kind: TranscriptActionKind::Error,
                    label: "Error".to_string(),
                    detail: compact_text(message, 180),
                    body: Some(message.clone()),
                    time: local_event_time(event),
                    state: TranscriptActionState::Failed,
                    expanded: false,
                })
            }
            _ => {}
        }
        removed_entry
    }

    fn remove_message(&mut self, message_id: Uuid) -> Option<usize> {
        let index = self.messages.remove(&message_id)?;
        self.order.remove(index);
        self.reindex_after_removal(index);
        Some(index)
    }

    fn reindex_after_removal(&mut self, index: usize) {
        self.message_markdown_cache.get_mut().messages.clear();
        self.tool_body_cache.get_mut().lines.clear();
        for stored_index in self
            .messages
            .values_mut()
            .chain(self.tools.values_mut())
            .chain(self.subagent_entries.values_mut())
        {
            if *stored_index > index {
                *stored_index -= 1;
            }
        }
        for process in self.runtime_processes.values_mut() {
            process.tool_index = process.tool_index.and_then(|tool_index| {
                (tool_index != index)
                    .then_some(tool_index - usize::from(tool_index > index))
            });
        }
        self.provider_backgrounds.retain(|_, process| {
            if process.tool_index == index {
                false
            } else {
                process.tool_index -= usize::from(process.tool_index > index);
                true
            }
        });
        self.selected = self.selected.and_then(|selected| {
            if selected == index {
                None
            } else {
                Some(selected - usize::from(selected > index))
            }
        });
        self.tool_run_offsets = self
            .tool_run_offsets
            .drain()
            .filter_map(|(start, offset)| {
                (start != index).then_some((start - usize::from(start > index), offset))
            })
            .collect();
        self.expanded_tool_runs = self
            .expanded_tool_runs
            .drain()
            .filter_map(|start| (start != index).then_some(start - usize::from(start > index)))
            .collect();
        self.active_reasoning = self.active_reasoning.and_then(|reasoning| {
            if reasoning == index {
                None
            } else {
                Some(reasoning - usize::from(reasoning > index))
            }
        });
        self.last_edit = self.last_edit.and_then(|edit| {
            if edit == index {
                None
            } else {
                Some(edit - usize::from(edit > index))
            }
        });
    }

    fn late_user_message_insertion_index(&self) -> usize {
        let output_start = self
            .order
            .iter()
            .rposition(|entry| {
                matches!(
                    entry,
                    TranscriptEntry::Message {
                        actor: EventActor::User,
                        ..
                    }
                )
            })
            .and_then(|last_user| {
                self.order
                    .iter()
                    .skip(last_user + 1)
                    .position(transcript_entry_is_turn_output)
                    .map(|offset| last_user + 1 + offset)
            });
        output_start.unwrap_or_else(|| {
            self.order
                .iter()
                .position(transcript_entry_is_turn_output)
                .unwrap_or(self.order.len())
        })
    }

    fn queued_user_message_insertion_index(&self, sequence: u64) -> usize {
        self.order
            .iter()
            .enumerate()
            .find_map(|(index, entry)| {
                if !matches!(entry, TranscriptEntry::Message { .. }) {
                    return None;
                }
                let message_id = self
                    .messages
                    .iter()
                    .find_map(|(message_id, stored_index)| {
                        (*stored_index == index).then_some(message_id)
                    })?;
                (self
                    .queued_message_sequences
                    .get(message_id)
                .is_some_and(|existing| *existing > sequence))
                .then_some(index)
            })
            .unwrap_or(self.order.len())
    }

    fn reindex_after_insertion(&mut self, index: usize) {
        self.message_markdown_cache.get_mut().messages.clear();
        self.tool_body_cache.get_mut().lines.clear();
        for stored_index in self
            .messages
            .values_mut()
            .chain(self.tools.values_mut())
            .chain(self.subagent_entries.values_mut())
        {
            if *stored_index >= index {
                *stored_index += 1;
            }
        }
        for process in self.runtime_processes.values_mut() {
            if let Some(tool_index) = process.tool_index.as_mut()
                && *tool_index >= index
            {
                *tool_index += 1;
            }
        }
        for process in self.provider_backgrounds.values_mut() {
            if process.tool_index >= index {
                process.tool_index += 1;
            }
        }
        self.selected = self.selected.map(|selected| {
            selected + usize::from(selected >= index)
        });
        self.tool_run_offsets = self
            .tool_run_offsets
            .drain()
            .map(|(start, offset)| (start + usize::from(start >= index), offset))
            .collect();
        self.expanded_tool_runs = self
            .expanded_tool_runs
            .drain()
            .map(|start| start + usize::from(start >= index))
            .collect();
        self.active_reasoning = self
            .active_reasoning
            .map(|reasoning| reasoning + usize::from(reasoning >= index));
        self.last_edit = self
            .last_edit
            .map(|edit| edit + usize::from(edit >= index));
    }

    fn append_reasoning(&mut self, text: &str, started_at: DateTime<Utc>, time: String) {
        if let Some(index) = self.active_reasoning
            && let Some(TranscriptEntry::Tool {
                code_view: Some((language, source)),
                complete,
                ..
            }) = self.order.get_mut(index)
            && language == "reasoning"
            && !*complete
        {
            Self::merge_reasoning_snapshot(source, text);
            return;
        }
        if text.trim().is_empty() {
            return;
        }
        self.start_reasoning(started_at, time);
        if let Some(index) = self.active_reasoning
            && let Some(TranscriptEntry::Tool {
                code_view: Some((language, source)),
                ..
            }) = self.order.get_mut(index)
            && language == "reasoning"
        {
            Self::merge_reasoning_snapshot(source, text);
        }
    }

    fn start_reasoning(&mut self, started_at: DateTime<Utc>, time: String) {
        if let Some(index) = self.active_reasoning {
            if matches!(
                self.order.get(index),
                Some(TranscriptEntry::Tool {
                    code_view: Some((language, _)),
                    complete: false,
                    ..
                }) if language == "reasoning"
            ) {
                return;
            }
            self.active_reasoning = None;
        }
        let index = self.order.len();
        self.order.push(TranscriptEntry::Tool {
            source_name: "reasoning".to_string(),
            name: "Thinking".to_string(),
            detail: String::new(),
            code_view: Some(("reasoning".to_string(), String::new())),
            output_view: None,
            payload_refs: Vec::new(),
            time,
            started_at,
            completed_at: None,
            complete: false,
            error: false,
            user_interrupted: false,
            backgrounded: false,
            expanded: true,
        });
        self.active_reasoning = Some(index);
    }

    fn merge_reasoning_snapshot(source: &mut String, incoming: &str) {
        if incoming.is_empty() || incoming == source {
            return;
        }
        // Most providers emit deltas, but subscription adapters may replay a
        // cumulative reasoning snapshot at a lifecycle boundary. Reconcile
        // that snapshot instead of appending the already-rendered prefix.
        if incoming.starts_with(source.as_str()) {
            source.clear();
            source.push_str(incoming);
        } else if source.starts_with(incoming) {
            // An older snapshot arrived after a newer one during reconnect.
        } else {
            let overlap = longest_suffix_prefix_overlap(source, incoming);
            source.push_str(&incoming[overlap..]);
        }
    }

    fn finish_reasoning(&mut self, completed_at: DateTime<Utc>) {
        let Some(index) = self.active_reasoning.take() else {
            return;
        };
        if let Some(TranscriptEntry::Tool {
            complete,
            expanded,
            completed_at: stored_completed_at,
            ..
        }) = self.order.get_mut(index)
        {
            *complete = true;
            *expanded = false;
            *stored_completed_at = Some(completed_at);
        }
    }

    fn provider_reasoning_lifecycle(
        kind: &str,
        payload: &serde_json::Value,
    ) -> Option<bool> {
        let (method, suffix) = kind
            .rsplit_once(':')
            .map_or((kind, None), |(method, suffix)| (method, Some(suffix)));
        let item_type = suffix
            .or_else(|| payload.pointer("/item/type").and_then(serde_json::Value::as_str))
            .or_else(|| {
                payload
                    .pointer("/params/item/type")
                    .and_then(serde_json::Value::as_str)
            });
        let method = method
            .to_ascii_lowercase()
            .replace(['.', '_', '-'], "/");
        let is_reasoning = item_type
            .is_some_and(|item_type| item_type.to_ascii_lowercase().contains("reasoning"))
            || method.contains("reasoning");
        if !is_reasoning {
            return None;
        }
        if method.ends_with("/started") || method.ends_with("/added") {
            Some(true)
        } else if method.ends_with("/completed") || method.ends_with("/done") {
            Some(false)
        } else {
            None
        }
    }

    fn upsert_running_tool(
        &mut self,
        event: &SessionEvent,
        tool_call_id: &str,
        name: &str,
        input: &serde_json::Value,
        input_ref: Option<&SessionPayloadRef>,
    ) {
        self.finish_reasoning(event.created_at);
        if let Some(handle) = tool_process_followup_handle(name, Some(input)) {
            self.provider_followups
                .insert(tool_call_id.to_string(), handle);
        }
        let presentation = project_tool_presentation(name, input, None, false);
        let display_name = presentation.label;
        let detail = presentation.detail;
        let code_view = presentation.input.map(|body| (body.language, body.text));
        let is_edit_diff = matches!(
            code_view.as_ref(),
            Some((language, _)) if is_diff_language(language)
        );
        let rich_ui = tool_has_rich_ui(
            &display_name,
            code_view.as_ref().map(|(language, _)| language.as_str()),
        );
        if is_edit_diff {
            self.collapse_previous_edit();
        }
        let expanded = input_ref.is_none()
            && ((is_edit_diff && self.auto_expand_edits)
                || (!is_edit_diff
                    && code_view.is_some()
                    && (rich_ui || self.auto_expand_tools)));
        if let Some(tool_index) = self.tools.get(tool_call_id).copied()
            && let Some(TranscriptEntry::Tool {
                source_name: stored_source_name,
                name: stored_name,
                detail: stored_detail,
                code_view: stored_code_view,
                output_view: stored_output_view,
                payload_refs: stored_payload_refs,
                complete: stored_complete,
                error: stored_error,
                user_interrupted: stored_user_interrupted,
                backgrounded: stored_backgrounded,
                expanded: stored_expanded,
                completed_at: stored_completed_at,
                ..
            }) = self.order.get_mut(tool_index)
            && !*stored_complete
        {
            if !*stored_backgrounded {
                self.foreground_tool = Some(tool_call_id.to_string());
            }
            *stored_source_name = name.to_string();
            *stored_name = display_name;
            *stored_detail = detail;
            *stored_code_view = code_view;
            *stored_output_view = None;
            *stored_payload_refs = input_ref.cloned().into_iter().collect();
            *stored_error = false;
            *stored_user_interrupted = false;
            *stored_expanded = expanded;
            *stored_completed_at = None;
            if is_edit_diff {
                self.last_edit = Some(tool_index);
            }
            return;
        }
        self.foreground_tool = Some(tool_call_id.to_string());
        let tool_index = self.order.len();
        self.tools.insert(tool_call_id.to_string(), tool_index);
        self.order.push(TranscriptEntry::Tool {
            source_name: name.to_string(),
            name: display_name,
            detail,
            code_view,
            output_view: None,
            payload_refs: input_ref.cloned().into_iter().collect(),
            time: local_event_time(event),
            started_at: event.created_at,
            completed_at: None,
            complete: false,
            error: false,
            user_interrupted: false,
            backgrounded: false,
            expanded,
        });
        if is_edit_diff {
            self.last_edit = Some(tool_index);
        }
    }

    fn promote_preparing_tool(&mut self, tool_call_id: &str) {
        let Some(preparing_id) = self.preparing_tool.take() else {
            return;
        };
        if preparing_id != tool_call_id && self.tools.contains_key(tool_call_id) {
            let Some(preparing_index) = self.tools.remove(&preparing_id) else {
                return;
            };
            self.order.remove(preparing_index);
            self.reindex_after_removal(preparing_index);
            if self.foreground_tool.as_deref() == Some(preparing_id.as_str()) {
                self.foreground_tool = Some(tool_call_id.to_string());
            }
            return;
        }
        let Some(tool_index) = self.tools.remove(&preparing_id) else {
            return;
        };
        self.tools.insert(tool_call_id.to_string(), tool_index);
        if self.foreground_tool.as_deref() == Some(preparing_id.as_str()) {
            self.foreground_tool = Some(tool_call_id.to_string());
        }
    }

    fn complete_preparing_tool(&mut self, tool_call_id: &str) {
        self.promote_preparing_tool(tool_call_id);
        let Some(tool_index) = self.tools.get(tool_call_id).copied() else {
            return;
        };
        if let Some(TranscriptEntry::Tool {
            source_name,
            name,
            detail,
            ..
        }) = self.order.get_mut(tool_index)
            && source_name == "action_preparing"
        {
            *source_name = "action".to_string();
            if !detail.is_empty() {
                *name = format!("Run {detail}");
                detail.clear();
            } else if let Some(label) = name.strip_prefix("Prepare ") {
                *name = format!("Run {label}");
            }
        }
    }

    fn mark_running_tools_user_interrupted(&mut self, completed_at: DateTime<Utc>) {
        self.finish_reasoning(completed_at);
        self.foreground_tool = None;
        self.preparing_tool = None;
        for entry in &mut self.order {
            if let TranscriptEntry::Tool {
                complete,
                user_interrupted,
                completed_at: stored_completed_at,
                ..
            } = entry
                && !*complete
            {
                *complete = true;
                *user_interrupted = true;
                *stored_completed_at = Some(completed_at);
            }
        }
    }

    fn mark_running_tools_backgrounded(&mut self) {
        let Some(tool_call_id) = self.foreground_tool.take() else {
            return;
        };
        let Some(index) = self.tools.get(&tool_call_id).copied() else {
            return;
        };
        if let Some(TranscriptEntry::Tool {
            complete,
            error,
            user_interrupted,
            backgrounded,
            ..
        }) = self.order.get_mut(index)
            && !*complete
            && !*error
            && !*user_interrupted
        {
            *backgrounded = true;
        }
    }

    fn finish_running_tools(
        &mut self,
        completed_at: DateTime<Utc>,
        failed: bool,
        error_detail: &str,
    ) {
        self.finish_reasoning(completed_at);
        self.foreground_tool = None;
        self.preparing_tool = None;
        let active_background_tools = self
            .runtime_processes
            .values()
            .filter(|process| process.running)
            .filter_map(|process| process.tool_index)
            .chain(
                self.provider_backgrounds
                    .values()
                    .map(|process| process.tool_index),
            )
            .collect::<HashSet<_>>();
        for (index, entry) in self.order.iter_mut().enumerate() {
            if let TranscriptEntry::Tool {
                detail,
                complete,
                error,
                backgrounded,
                completed_at: stored_completed_at,
                ..
            } = entry
                && !*complete
            {
                *complete = true;
                *error = failed;
                *backgrounded = active_background_tools.contains(&index);
                *stored_completed_at = Some(completed_at);
                if failed && !error_detail.trim().is_empty() {
                    *detail = format!(
                        "{detail} · {}",
                        compact_text(error_detail.lines().next().unwrap_or_default(), 120)
                    );
                }
            }
        }
    }

    fn finish_live_assistant_messages(&mut self, completed_at: DateTime<Utc>) {
        for (index, entry) in self.order.iter_mut().enumerate() {
            let TranscriptEntry::Message {
                actor: EventActor::Assistant,
                status,
                complete,
                ..
            } = entry
            else {
                continue;
            };
            if *status != MessageStatus::InProgress {
                continue;
            }
            *status = MessageStatus::Complete;
            *complete = true;
            self.message_markdown_cache
                .get_mut()
                .messages
                .remove(&index);
        }
        self.finish_reasoning(completed_at);
    }

    fn set_auto_expand_edits(&mut self, enabled: bool) {
        self.auto_expand_edits = enabled;
        for entry in &mut self.order {
            if let TranscriptEntry::Tool {
                code_view: Some((language, _)),
                expanded,
                ..
            } = entry
                && is_diff_language(language)
            {
                *expanded = enabled;
            }
        }
    }

    fn collapse_previous_edit(&mut self) {
        if let Some(TranscriptEntry::Tool { expanded, .. }) =
            self.last_edit.and_then(|index| self.order.get_mut(index))
        {
            *expanded = false;
        }
    }

    fn set_auto_expand_tools(&mut self, enabled: bool) {
        self.auto_expand_tools = enabled;
        for entry in &mut self.order {
            if let TranscriptEntry::Tool {
                name,
                code_view: Some((language, _)),
                expanded,
                ..
            } = entry
                && !is_diff_language(language)
                && language != "reasoning"
                && !tool_has_rich_ui(name, Some(language))
            {
                *expanded = enabled;
            }
        }
    }

    fn config_statuses(
        &self,
    ) -> (
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        String,
    ) {
        let Some(config) = self.config.as_ref() else {
            return (None, None, None, None, String::new());
        };
        let model = config.model.clone();
        (
            model,
            config.effort.clone(),
            config.fast.then(|| "fast".to_string()),
            Some(permission_mode_label(config.permission_mode).to_string()),
            fish_style_path(&config.cwd),
        )
    }

    fn context_status(&self) -> (String, bool) {
        if !self.context_known {
            return (String::new(), false);
        }
        let imminent = self.context_remaining_percent <= 20;
        let status = if imminent {
            format!(
                "compaction imminent ({}% left)",
                self.context_remaining_percent
            )
        } else {
            format!("{}% context left", self.context_remaining_percent)
        };
        (status, imminent)
    }

    fn context_limit_label(&self) -> String {
        let (status, _) = self.context_status();
        if !self
            .config
            .as_ref()
            .is_some_and(|config| config.provider == CodingProvider::OpenAiCompatible)
        {
            return status;
        }
        self.context_window_tokens.map_or(status.clone(), |window| {
            format!("{status} · {}", format_context_tokens(window))
        })
    }

    fn context_tooltip(&self) -> String {
        if !self.context_known {
            return String::new();
        }
        match (self.context_tokens, self.context_window_tokens) {
            (Some(tokens), Some(window)) => format!(
                "{} used of {} window · {}",
                format_context_tokens(tokens),
                format_context_tokens(window),
                self.context_status().0
            ),
            _ => self.context_status().0,
        }
    }

    fn cache_status(&self, now: DateTime<Utc>) -> Option<CacheStatus> {
        if self.active_turn.is_some() {
            return None;
        }
        let signature = self.config.as_ref()?.cache_signature();
        self.cache_diagnostics.status(now, &signature)
    }

    fn active_subagent_count(&self) -> usize {
        self.subagents
            .values()
            .filter(|status| subagent_is_working(**status))
            .count()
    }

    fn agent_roster_entries(&self) -> Vec<(String, Option<Uuid>)> {
        let mut rows = Vec::new();
        if let Some(config) = self.config.as_ref() {
            let model = config
                .model
                .as_deref()
                .unwrap_or_else(|| config.provider.catalog_backend());
            rows.push((
                format!(
                    "director  {model}  {}  main thread",
                    config.effort.as_deref().unwrap_or("default")
                ),
                None,
            ));
        } else {
            rows.push(("director  model pending  main thread".to_string(), None));
        }
        let mut agents = self
            .subagent_snapshots
            .values()
            .filter(|agent| subagent_is_working(agent.status))
            .collect::<Vec<_>>();
        agents.sort_by(|left, right| left.task_name.cmp(&right.task_name));
        rows.extend(agents.into_iter().map(|agent| {
            let name = display_agent_name(&agent.task_name);
            let model = agent
                .model
                .as_deref()
                .unwrap_or_else(|| agent.provider.catalog_backend());
            let effort = agent.effort.as_deref().unwrap_or("default");
            let usage = format_subagent_usage(&agent.usage);
            (
                format!(
                    "{name}  {model}  {effort}  {}{usage}",
                    subagent_status_label(agent.status)
                ),
                Some(agent.session_id),
            )
        }));
        rows
    }

    fn goal_status(&self) -> Option<String> {
        let goal = self.goal.as_ref()?;
        let label = match goal.status {
            GoalStatus::Active => "active",
            GoalStatus::Paused => "paused",
            GoalStatus::Blocked => "blocked",
            GoalStatus::UsageLimited => "usage limit reached",
            GoalStatus::BudgetLimited => "token budget reached",
            GoalStatus::Complete => return None,
        };
        let live_time = goal
            .time_used_seconds
            .saturating_add(if goal.status.is_active() {
                Utc::now()
                    .signed_duration_since(goal.updated_at)
                    .num_seconds()
                    .max(0) as u64
            } else {
                0
            });
        Some(format_elapsed_duration(live_time).map_or_else(
            || format!("{label} /goal"),
            |duration| format!("{label} /goal {duration}"),
        ))
    }

    fn todo_status(&self) -> Option<String> {
        let open = self
            .todos
            .iter()
            .filter(|item| item.status != PlanItemStatus::Completed)
            .count();
        (open > 0).then(|| format!("{open} to-do{}", if open == 1 { "" } else { "s" }))
    }

    fn shell_status(&self) -> Option<String> {
        let active = self.active_shell_rows().len();
        (active > 0).then(|| format!("{active} shell{}", if active == 1 { "" } else { "s" }))
    }

    fn active_shell_rows(&self) -> Vec<(String, Option<usize>)> {
        let mut claimed_tools = HashSet::new();
        let mut rows = self
            .order
            .iter()
            .enumerate()
            .filter_map(|(index, entry)| match entry {
                TranscriptEntry::Tool {
                    source_name,
                    name,
                    detail,
                    complete: false,
                    error: false,
                    user_interrupted: false,
                    backgrounded: true,
                    ..
                } if tool_can_start_background_process(source_name) => {
                    claimed_tools.insert(index);
                    Some((
                        compact_text(
                            if detail.trim().is_empty() { name } else { detail },
                            120,
                        ),
                        Some(index),
                    ))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        rows.extend(self
            .runtime_processes
            .values()
            .filter(|process| {
                process.running
                    && process
                        .tool_index
                        .is_none_or(|tool_index| !claimed_tools.contains(&tool_index))
            })
            .map(|process| {
                (
                    format!("pid {}  {}", process.pid, compact_text(&process.command, 100)),
                    process.tool_index,
                )
            })
        );
        rows.extend(
            self.provider_backgrounds
                .iter()
                .filter(|(_, process)| !claimed_tools.contains(&process.tool_index))
                .map(|(handle, process)| {
                    (
                        format!(
                            "{}  {}",
                            compact_text(handle, 18),
                            compact_text(&process.command, 100)
                        ),
                        Some(process.tool_index),
                    )
                }),
        );
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        rows
    }

    #[cfg(test)]
    fn todo_tooltip_rows(&self, expanded: bool) -> Vec<String> {
        self.todo_tooltip_rows_with_status(expanded)
            .into_iter()
            .map(|(row, _)| row)
            .collect()
    }

    fn todo_tooltip_rows_with_status(&self, expanded: bool) -> Vec<(String, bool)> {
        if self.todos.is_empty() {
            return vec![("No to-dos in the current plan".to_string(), false)];
        }
        let ordered = ordered_plan_items(&self.todos);
        let clipped = !expanded && ordered.len() > MAX_COLLAPSED_PLAN_ITEMS;
        let mut rows = ordered
            .iter()
            .take(if clipped {
                MAX_COLLAPSED_PLAN_ITEMS
            } else {
                ordered.len()
            })
            .map(|item| {
                let glyph = match item.status {
                    PlanItemStatus::Completed => "✓",
                    PlanItemStatus::InProgress => "●",
                    PlanItemStatus::Pending => "○",
                };
                (
                    format!("{glyph}  {}", item.content),
                    item.status == PlanItemStatus::Completed,
                )
            })
            .collect::<Vec<_>>();
        if clipped {
            rows.push((
                format!(
                    "    + {} more · click to expand",
                    ordered.len() - MAX_COLLAPSED_PLAN_ITEMS
                ),
                false,
            ));
        } else if expanded && ordered.len() > MAX_COLLAPSED_PLAN_ITEMS {
            rows.push(("    − click to collapse".to_string(), false));
        }
        rows
    }

    fn active_goal_cache_tick(&self) -> Option<i64> {
        self.active_goal_cache_tick_at(Utc::now())
    }

    fn active_goal_cache_tick_at(&self, now: DateTime<Utc>) -> Option<i64> {
        self.goal
            .as_ref()
            .filter(|goal| goal.status.is_active())
            .map(|goal| {
                goal.time_used_seconds.saturating_add(
                    now.signed_duration_since(goal.updated_at)
                        .num_seconds()
                        .max(0) as u64,
                ) as i64
                    / 60
            })
    }

    fn tool_elapsed_cache_tick(&self) -> Option<i64> {
        self.tool_elapsed_cache_tick_at(Utc::now())
    }

    fn tool_elapsed_cache_tick_at(&self, now: DateTime<Utc>) -> Option<i64> {
        self.has_running_tool()
            .then(|| now.timestamp_millis().div_euclid(TOOL_ELAPSED_REFRESH_MILLIS))
    }

    fn has_running_tool(&self) -> bool {
        self.order.iter().any(|entry| {
            matches!(
                entry,
                TranscriptEntry::Tool {
                    complete: false,
                    ..
                }
            )
        })
    }

    fn tool_activity_is_running(&self, index: usize) -> bool {
        matches!(
            self.order.get(index),
            Some(TranscriptEntry::Tool {
                name,
                code_view,
                complete: false,
                error: false,
                user_interrupted: false,
                ..
            }) if !tool_action_is_instant(
                name,
                code_view.as_ref().map(|(language, _)| language.as_str()),
            )
        )
    }

    fn first_prompt(&self) -> Option<&str> {
        self.order.iter().find_map(|entry| match entry {
            TranscriptEntry::Message {
                actor: EventActor::User,
                text,
                status: MessageStatus::Complete,
                ..
            } => Some(text.as_str()),
            _ => None,
        })
    }

    #[cfg(test)]
    fn lines(&self, width: usize) -> Vec<Line<'static>> {
        self.render(width, None, None, None).0
    }

    #[cfg(test)]
    fn render(
        &self,
        width: usize,
        hovered_tool: Option<usize>,
        hovered_message: Option<usize>,
        hovered_entry: Option<usize>,
    ) -> TranscriptRender {
        self.render_with_tool_run_viewport(
            width,
            DEFAULT_TOOL_RUN_VIEWPORT_HEIGHT,
            hovered_tool,
            hovered_message,
            hovered_entry,
        )
    }

    #[cfg(test)]
    fn render_with_tool_run_viewport(
        &self,
        width: usize,
        tool_run_viewport_height: usize,
        hovered_tool: Option<usize>,
        hovered_message: Option<usize>,
        hovered_entry: Option<usize>,
    ) -> TranscriptRender {
        self.render_with_tool_run_viewport_mode(
            width,
            tool_run_viewport_height,
            hovered_tool,
            hovered_message,
            hovered_entry,
            false,
            None,
        )
    }

    fn render_for_cache(
        &self,
        width: usize,
        tool_run_viewport_height: usize,
    ) -> TranscriptRender {
        self.render_with_tool_run_viewport_mode(
            width,
            tool_run_viewport_height,
            None,
            None,
            None,
            true,
            None,
        )
    }

    fn render_tool_for_cache(
        &self,
        index: usize,
        width: usize,
        tool_run_viewport_height: usize,
    ) -> TranscriptRender {
        self.render_with_tool_run_viewport_mode(
            width,
            tool_run_viewport_height,
            None,
            None,
            None,
            true,
            Some(index),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn render_with_tool_run_viewport_mode(
        &self,
        width: usize,
        tool_run_viewport_height: usize,
        hovered_tool: Option<usize>,
        hovered_message: Option<usize>,
        hovered_entry: Option<usize>,
        defer_completed_message_backgrounds: bool,
        focused_tool: Option<usize>,
    ) -> TranscriptRender {
        let today = Local::now().date_naive();
        let today_prefix = today.format("%Y-%m-%d ").to_string();
        if focused_tool.is_none() {
            self.prepare_message_markdown_cache(width);
        }
        {
            let mut cache = self.tool_body_cache.borrow_mut();
            if cache.width != width {
                cache.width = width;
                cache.lines.clear();
            }
        }
        let mut lines = Vec::new();
        let mut tool_rows = Vec::new();
        let mut tool_run_rows = Vec::new();
        let mut message_rows = Vec::new();
        let mut entry_rows = Vec::new();
        let mut link_rows = Vec::new();
        let mut selection_rows: Vec<SelectionRowRange> = Vec::new();
        let mut tool_run_starts = HashMap::new();
        let tool_run_windows = self.tool_run_windows();
        let running_tool = self.has_running_tool();
        if let Some(index) = focused_tool
            && let Some(TranscriptEntry::Tool { name, complete, .. }) = self.order.get(index)
        {
            lines.push(Line::from(vec![
                Span::styled(
                    "  Tool output",
                    Style::default()
                        .fg(BORG_ORANGE)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    format!(" · {name} · {}", if *complete { "complete" } else { "live" }),
                    Style::default().fg(Color::Gray),
                ),
                Span::styled(" · Esc to return", Style::default().fg(Color::DarkGray)),
            ]));
            lines.push(Line::default());
        }
        for (index, entry) in self.order.iter().enumerate() {
            if focused_tool.is_some_and(|focused| focused != index) {
                continue;
            }
            let tool_window = focused_tool.is_none().then_some(tool_run_windows[index]).flatten();
            if let Some(window) = tool_window.filter(|window| index == window.start) {
                let row = lines.len();
                lines.push(Line::from(Span::styled(
                    format!("┌─ actions · {}", window.total),
                    Style::default().fg(Color::DarkGray),
                )));
                tool_run_starts.insert(
                    window.start,
                    (
                        row,
                        tool_rows.len(),
                        entry_rows.len(),
                        selection_rows.len(),
                    ),
                );
            }
            let visible_message = matches!(
                entry,
                TranscriptEntry::Message { status, .. } if *status != MessageStatus::Queued
            );
            let starts_labeled_group = visible_message
                || matches!(
                    entry,
                    TranscriptEntry::Plan { .. }
                        | TranscriptEntry::Goal { .. }
                        | TranscriptEntry::Info { .. }
                        | TranscriptEntry::Action { .. }
                        | TranscriptEntry::Compaction { .. }
                );
            let is_chat_message = matches!(
                entry,
                TranscriptEntry::Message {
                    actor: EventActor::User | EventActor::Assistant,
                    status,
                    ..
                } if *status != MessageStatus::Queued
            );
            let entry_start = if is_chat_message {
                if !lines.is_empty()
                    && lines
                        .last()
                        .is_none_or(|line| !line_is_unstyled_blank(line))
                {
                    lines.push(Line::default());
                }
                lines.len()
            } else {
                if starts_labeled_group
                    && lines
                        .last()
                        .is_none_or(|line| !line_is_unstyled_blank(line))
                {
                    lines.push(Line::default());
                }
                lines.len()
            };
            match entry {
                TranscriptEntry::Message {
                    actor,
                    text,
                    attachments,
                    model,
                    effort,
                    time,
                    status,
                    complete,
                } => {
                    if *status == MessageStatus::Queued {
                        continue;
                    }
                    let (label, color) = match actor {
                        EventActor::User => (self.user_label.clone(), self.user_label_color),
                        EventActor::Assistant => {
                            (self.assistant_label.clone(), self.assistant_label_color)
                        }
                        EventActor::Tool => ("Tool".to_string(), Color::Blue),
                        EventActor::System => ("System".to_string(), Color::DarkGray),
                    };
                    let message_background_start = if is_chat_message {
                        lines.push(Line::default());
                        lines.len().saturating_sub(1)
                    } else {
                        lines.len()
                    };
                    let message_content_start = lines.len();
                    let time = display_local_time(time, &today_prefix);
                    let mut header = vec![Span::styled(
                        format!("  ▌ {label}"),
                        Style::default().fg(color).add_modifier(Modifier::BOLD),
                    )];
                    if *actor == EventActor::Assistant {
                        let runtime = [model.as_deref(), effort.as_deref()]
                            .into_iter()
                            .flatten()
                            .collect::<Vec<_>>()
                            .join(" ");
                        if !runtime.is_empty() {
                            header.push(Span::styled(
                                format!("  {runtime}"),
                                Style::default().fg(Color::DarkGray),
                            ));
                        }
                    }
                    header.push(Span::styled(
                        format!("  {time}"),
                        Style::default().fg(Color::DarkGray),
                    ));
                    lines.push(Line::from(header));
                    let text_color = match actor {
                        EventActor::User => Some(self.user_message_color),
                        EventActor::Assistant => Some(self.assistant_message_color),
                        _ => None,
                    };
                    let mut message_lines = {
                        let mut cache = self.message_markdown_cache.borrow_mut();
                        #[cfg(test)]
                        let mut missed = false;
                        let rendered = match cache.messages.entry(index) {
                            std::collections::hash_map::Entry::Occupied(entry) => {
                                entry.get().clone()
                            }
                            std::collections::hash_map::Entry::Vacant(entry) => {
                                #[cfg(test)]
                                {
                                    missed = true;
                                }
                                let content_width =
                                    width.saturating_sub(MESSAGE_HORIZONTAL_PADDING * 2);
                                let preserve_structure = *actor == EventActor::User
                                    && user_message_has_structured_whitespace(text);
                                let mut lines = if preserve_structure {
                                    structured_user_message_lines(text, content_width, text_color)
                                } else {
                                    markdown_lines(text, content_width, text_color)
                                };
                                let mut links = if preserve_structure {
                                    Vec::new()
                                } else {
                                    markdown_link_ranges(text, &lines)
                                };
                                for link in &mut links {
                                    link.start += MESSAGE_HORIZONTAL_PADDING;
                                    link.end += MESSAGE_HORIZONTAL_PADDING;
                                }
                                for line in &mut lines {
                                    line.spans.insert(0, Span::raw("  "));
                                }
                                entry.insert(MarkdownRender { lines, links }).clone()
                            }
                        };
                        #[cfg(test)]
                        if missed {
                            cache.misses += 1;
                        }
                        rendered
                    };
                    for link in &mut message_lines.links {
                        link.row += lines.len();
                    }
                    link_rows.extend(message_lines.links);
                    lines.extend(message_lines.lines);
                    for (number, path) in attachments {
                        let token = format!("[Image {number}]");
                        if !text.contains(&token) {
                            lines.push(Line::from(Span::styled(
                                format!("  {token}"),
                                Style::default()
                                    .fg(Color::LightCyan)
                                    .add_modifier(Modifier::BOLD),
                            )));
                        }
                        let attachment_line = Line::from(vec![
                            Span::styled("    ▣ ", Style::default().fg(Color::LightCyan)),
                            Span::styled(
                                format!("Image {number}"),
                                Style::default().add_modifier(Modifier::BOLD),
                            ),
                            Span::styled(
                                format!("  {}", display_name(path)),
                                Style::default().fg(Color::DarkGray),
                            ),
                        ]);
                        if let Ok(url) = url::Url::from_file_path(path) {
                            link_rows.push(LinkRowRange {
                                row: lines.len(),
                                start: 4,
                                end: attachment_line.width(),
                                url: url.to_string(),
                            });
                        }
                        lines.push(attachment_line);
                    }
                    if *actor == EventActor::Assistant && !complete && !running_tool {
                        lines.push(Line::from(Span::styled(
                            "    ◌ responding",
                            Style::default().fg(Color::Cyan),
                        )));
                    }
                    while lines.len() > message_content_start
                        && lines.last().is_some_and(line_is_blank)
                    {
                        lines.pop();
                    }
                    let message_content_end = lines.len();
                    let message_end = if is_chat_message {
                        lines.push(Line::default());
                        let end = lines.len();
                        if !defer_completed_message_backgrounds || !*complete {
                            let background = if hovered_message == Some(index) {
                                MESSAGE_HOVER_BG
                            } else {
                                MESSAGE_BG
                            };
                            for line in &mut lines[message_background_start..end] {
                                apply_line_background(line, width, background);
                            }
                        }
                        end
                    } else {
                        lines.len()
                    };
                    if is_chat_message && *complete {
                        message_rows.push((index, message_background_start, message_end));
                    }
                    selection_rows.push(SelectionRowRange::transcript_entry(
                        index,
                        if is_chat_message {
                            message_content_start
                        } else {
                            entry_start
                        },
                        message_content_end,
                    ));
                    if is_chat_message {
                        lines.push(Line::default());
                    }
                }
                TranscriptEntry::Activity { text, time } => {
                    let time = display_local_time(time, &today_prefix);
                    let prefix = if tool_window.is_some() { "│ " } else { "  " };
                    let activity_color = if text == USER_INTERRUPT_ACTIVITY {
                        Color::LightRed
                    } else if is_subagent_activity_text(text) {
                        SUBAGENT_PINK
                    } else {
                        Color::DarkGray
                    };
                    for line in wrap_display(text, width.saturating_sub(8)) {
                        lines.push(Line::from(vec![
                            Span::styled(
                                format!("{prefix}{time}  "),
                                Style::default().fg(Color::DarkGray),
                            ),
                            Span::styled(line, Style::default().fg(activity_color)),
                        ]));
                    }
                    selection_rows.push(if tool_window.is_some() {
                        SelectionRowRange::nested_entry(index, entry_start, lines.len(), 0)
                    } else {
                        SelectionRowRange::transcript_entry(index, entry_start, lines.len())
                    });
                }
                TranscriptEntry::Action {
                    kind,
                    label,
                    detail,
                    body,
                    time,
                    state,
                    expanded,
                } => {
                    let time = display_local_time(time, &today_prefix);
                    let prefix = if tool_window.is_some() { "│ " } else { "  " };
                    let glyph = transcript_action_glyph(*state);
                    let color = transcript_action_color(*kind, *state);
                    let mut summary = if detail.is_empty() {
                        format!("{time}  {glyph} {label}")
                    } else {
                        format!("{time}  {glyph} {label}  {detail}")
                    };
                    if body.as_deref().is_some_and(|body| !body.trim().is_empty()) {
                        summary.push_str(if *expanded {
                            " · click to collapse"
                        } else {
                            " · click to expand"
                        });
                    }
                    let action_start = lines.len();
                    for line in wrap_display(&summary, width.saturating_sub(prefix.len() + 2)) {
                        lines.push(Line::from(vec![
                            Span::styled(prefix, Style::default().fg(Color::DarkGray)),
                            Span::styled(line, Style::default().fg(color)),
                        ]));
                    }
                    if *expanded
                        && let Some(body) = body.as_deref().filter(|body| !body.trim().is_empty())
                    {
                        for line in wrap_display(body, width.saturating_sub(prefix.len() + 6)) {
                            lines.push(Line::from(vec![
                                Span::styled(
                                    if tool_window.is_some() {
                                        "│   │ "
                                    } else {
                                        "  │ "
                                    },
                                    Style::default().fg(Color::DarkGray),
                                ),
                                Span::styled(line, Style::default().fg(Color::Gray)),
                            ]));
                        }
                    }
                    if hovered_entry == Some(index) {
                        for line in &mut lines[action_start..] {
                            apply_line_background(line, width, MESSAGE_HOVER_BG);
                        }
                    }
                    entry_rows.push((index, entry_start, lines.len()));
                    selection_rows.push(if tool_window.is_some() {
                        SelectionRowRange::nested_entry(index, entry_start, lines.len(), 0)
                    } else {
                        SelectionRowRange::transcript_entry(index, entry_start, lines.len())
                    });
                }
                TranscriptEntry::Plan {
                    items,
                    time,
                    expanded,
                } => {
                    let time = display_local_time(time, &today_prefix);
                    let done = items
                        .iter()
                        .filter(|item| item.status == PlanItemStatus::Completed)
                        .count();
                    lines.push(Line::from(vec![
                        Span::styled(
                            "▌ Plan",
                            Style::default()
                                .fg(Color::Yellow)
                                .add_modifier(Modifier::BOLD),
                        ),
                        Span::styled(
                            format!("  {time}  {done}/{} completed", items.len()),
                            Style::default().fg(Color::DarkGray),
                        ),
                    ]));
                    let display_items = ordered_plan_items(items);
                    let clipped = !*expanded && items.len() > MAX_COLLAPSED_PLAN_ITEMS;
                    let display_limit = if clipped {
                        MAX_COLLAPSED_PLAN_ITEMS
                    } else {
                        usize::MAX
                    };
                    for item in display_items.into_iter().take(display_limit) {
                        let (glyph, marker_style, text_style) = match item.status {
                            PlanItemStatus::Completed => (
                                "✓",
                                Style::default().fg(Color::DarkGray),
                                Style::default()
                                    .fg(Color::DarkGray)
                                    .add_modifier(Modifier::CROSSED_OUT),
                            ),
                            PlanItemStatus::InProgress => (
                                "●",
                                Style::default()
                                    .fg(Color::LightGreen)
                                    .add_modifier(Modifier::BOLD),
                                Style::default()
                                    .fg(Color::LightGreen)
                                    .add_modifier(Modifier::BOLD),
                            ),
                            PlanItemStatus::Pending => (
                                "○",
                                Style::default()
                                    .fg(Color::White)
                                    .add_modifier(Modifier::BOLD),
                                Style::default()
                                    .fg(Color::White)
                                    .add_modifier(Modifier::BOLD),
                            ),
                        };
                        for (line_index, line) in
                            wrap_display(&item.content, width.saturating_sub(5))
                                .into_iter()
                                .enumerate()
                        {
                            let marker = if line_index == 0 { glyph } else { " " };
                            lines.push(Line::from(vec![
                                Span::styled(format!("  {marker}  "), marker_style),
                                Span::styled(line, text_style),
                            ]));
                        }
                    }
                    if clipped {
                        lines.push(Line::from(Span::styled(
                            format!(
                                "    + {} more · click to expand",
                                items.len() - MAX_COLLAPSED_PLAN_ITEMS
                            ),
                            Style::default().fg(Color::DarkGray),
                        )));
                    } else if *expanded && items.len() > MAX_COLLAPSED_PLAN_ITEMS {
                        lines.push(Line::from(Span::styled(
                            "    − show less",
                            Style::default().fg(Color::DarkGray),
                        )));
                    }
                    if hovered_entry == Some(index) {
                        for line in &mut lines[entry_start..] {
                            apply_line_background(line, width, MESSAGE_HOVER_BG);
                        }
                    }
                    entry_rows.push((index, entry_start, lines.len()));
                    selection_rows.push(SelectionRowRange::transcript_entry(
                        index,
                        entry_start,
                        lines.len(),
                    ));
                    lines.push(Line::default());
                }
                TranscriptEntry::Goal { goal, time } => {
                    let time = display_local_time(time, &today_prefix);
                    let live_time =
                        goal.time_used_seconds
                            .saturating_add(if goal.status.is_active() {
                                Utc::now()
                                    .signed_duration_since(goal.updated_at)
                                    .num_seconds()
                                    .max(0) as u64
                            } else {
                                0
                            });
                    lines.push(Line::from(vec![
                        Span::styled(
                            "▌ Goal",
                            Style::default()
                                .fg(Color::Yellow)
                                .add_modifier(Modifier::BOLD),
                        ),
                        Span::styled(
                            format_elapsed_duration(live_time).map_or_else(
                                || format!("  {time}  {}", goal_status_label(goal.status)),
                                |duration| {
                                    format!(
                                        "  {time}  {} · {duration}",
                                        goal_status_label(goal.status)
                                    )
                                },
                            ),
                            Style::default().fg(Color::DarkGray),
                        ),
                    ]));
                    let objective_style = if goal.status == GoalStatus::Complete {
                        Style::default()
                            .fg(Color::DarkGray)
                            .add_modifier(Modifier::CROSSED_OUT)
                    } else {
                        Style::default().fg(Color::White)
                    };
                    for line in wrap_display(&goal.objective, width.saturating_sub(4)) {
                        lines.push(Line::from(vec![
                            Span::raw("  "),
                            Span::styled(line, objective_style),
                        ]));
                    }
                    if hovered_entry == Some(index) {
                        for line in &mut lines[entry_start..] {
                            apply_line_background(line, width, MESSAGE_HOVER_BG);
                        }
                    }
                    entry_rows.push((index, entry_start, lines.len()));
                    selection_rows.push(SelectionRowRange::transcript_entry(
                        index,
                        entry_start,
                        lines.len(),
                    ));
                    lines.push(Line::default());
                }
                TranscriptEntry::Info { title, text, time } => {
                    let time = display_local_time(time, &today_prefix);
                    lines.push(Line::from(vec![
                        Span::styled(
                            format!("▌ {}", title.to_ascii_uppercase()),
                            Style::default()
                                .fg(Color::LightCyan)
                                .add_modifier(Modifier::BOLD),
                        ),
                        Span::styled(format!("  {time}"), Style::default().fg(Color::DarkGray)),
                    ]));
                    for line in wrap_display(text, width.saturating_sub(4)) {
                        lines.push(Line::from(vec![
                            Span::raw("  "),
                            Span::styled(line, Style::default().fg(Color::Gray)),
                        ]));
                    }
                    if hovered_entry == Some(index) {
                        for line in &mut lines[entry_start..] {
                            apply_line_background(line, width, MESSAGE_HOVER_BG);
                        }
                    }
                    entry_rows.push((index, entry_start, lines.len()));
                    selection_rows.push(SelectionRowRange::transcript_entry(
                        index,
                        entry_start,
                        lines.len(),
                    ));
                    lines.push(Line::default());
                }
                TranscriptEntry::Compaction {
                    summary,
                    time,
                    expanded,
                    complete,
                    ..
                } => {
                    let time = display_local_time(time, &today_prefix);
                    let expandable = *complete && compaction_has_expandable_detail(summary);
                    let action_hint = if expandable {
                        if *expanded {
                            " · click to collapse · right-click for actions"
                        } else {
                            " · click to expand · right-click for actions"
                        }
                    } else {
                        ""
                    };
                    lines.push(Line::from(vec![
                        Span::styled(
                            format!("▌ {}", compact_text(summary, 180)),
                            Style::default()
                                .fg(BORG_ORANGE)
                                .add_modifier(Modifier::BOLD),
                        ),
                        Span::styled(
                            format!("  {time}{action_hint}"),
                            Style::default().fg(Color::DarkGray),
                        ),
                    ]));
                    if *expanded && expandable {
                        let detail = summary
                            .strip_prefix("Compacted context: ")
                            .unwrap_or(summary);
                        for line in wrap_display(detail, width.saturating_sub(6)) {
                            lines.push(Line::from(vec![
                                Span::raw("  │ "),
                                Span::styled(line, Style::default().fg(Color::Gray)),
                            ]));
                        }
                    }
                    if hovered_entry == Some(index) {
                        for line in &mut lines[entry_start..] {
                            apply_line_background(line, width, MESSAGE_HOVER_BG);
                        }
                    }
                    entry_rows.push((index, entry_start, lines.len()));
                    selection_rows.push(SelectionRowRange::transcript_entry(
                        index,
                        entry_start,
                        lines.len(),
                    ));
                    lines.push(Line::default());
                }
                TranscriptEntry::Tool {
                    source_name,
                    name,
                    detail,
                    code_view,
                    output_view,
                    time,
                    started_at,
                    completed_at,
                    complete,
                    error,
                    user_interrupted,
                    backgrounded,
                    expanded,
                    ..
                } => {
                    let next_is_tool = focused_tool.is_none()
                        && matches!(
                            self.order.get(index + 1),
                            Some(TranscriptEntry::Tool { .. })
                        );
                    let time = display_local_time(time, &today_prefix);
                    let is_reasoning = matches!(
                        code_view.as_ref(),
                        Some((language, _)) if language == "reasoning"
                    );
                    let expandable = tool_has_expandable_body(
                        source_name,
                        code_view.as_ref(),
                        output_view.as_ref(),
                    );
                    let is_instant = tool_action_is_instant(
                        name,
                        code_view.as_ref().map(|(language, _)| language.as_str()),
                    );
                    let summary_start = lines.len();
                    let glyph = if *error {
                        "!"
                    } else if *user_interrupted {
                        "■"
                    } else if *backgrounded {
                        "↗"
                    } else if *complete && !is_instant {
                        "✓"
                    } else {
                        "◇"
                    };
                    let gutter_style = if hovered_tool == Some(index) {
                        Style::default().fg(Color::Gray)
                    } else {
                        Style::default().fg(Color::DarkGray)
                    };
                    let style = if *error || *user_interrupted {
                        Style::default().fg(Color::Red)
                    } else if *backgrounded {
                        Style::default().fg(USER_LABEL_BLUE)
                    } else {
                        gutter_style
                    };
                    let name_style = if is_reasoning {
                        Style::default()
                            .fg(Color::Gray)
                            .add_modifier(Modifier::BOLD | Modifier::ITALIC)
                    } else if *error || *user_interrupted {
                        Style::default()
                            .fg(Color::LightRed)
                            .add_modifier(Modifier::BOLD)
                    } else if *backgrounded {
                        Style::default()
                            .fg(USER_LABEL_BLUE)
                            .add_modifier(Modifier::BOLD)
                    } else if is_subagent_tool(name) {
                        Style::default()
                            .fg(SUBAGENT_PINK)
                            .add_modifier(Modifier::BOLD)
                    } else {
                        Style::default()
                            .fg(Color::White)
                            .add_modifier(Modifier::BOLD)
                    };
                    let is_edit = is_edit_tool(source_name, name);
                    let lifecycle = if *user_interrupted {
                        Some("user interrupted")
                    } else if *backgrounded {
                        Some("Running in background")
                    } else {
                        None
                    };
                    let lifecycle_complete = *complete && !*backgrounded;
                    let display_name = if is_reasoning {
                        Cow::Borrowed(name.as_str())
                    } else {
                        tool_lifecycle_label(name, lifecycle_complete)
                    };
                    let show_tool_body = *complete
                        || !is_edit
                        || code_view
                            .as_ref()
                            .is_some_and(|(language, _)| is_diff_language(language));
                    let mut summary = if detail.is_empty() {
                        format!("{time}  {glyph} {display_name}")
                    } else {
                        format!("{time}  {glyph} {display_name}  {detail}")
                    };
                    if let Some(lifecycle) = lifecycle {
                        summary.push_str(&format!(" · {lifecycle}"));
                    }
                    let prefix = if tool_window.is_some() { "│ " } else { "  " };
                    let elapsed = format_tool_elapsed(*started_at, *completed_at);
                    for (line_index, line) in
                        tool_summary_lines(&summary, elapsed.as_deref(), prefix, width)
                            .into_iter()
                            .enumerate()
                    {
                        if line_index == 0
                            && let Some(name_start) = line.find(display_name.as_ref())
                        {
                            let name_end = name_start + display_name.len();
                            let mut spans = vec![
                                Span::styled(prefix.to_string(), gutter_style),
                                Span::styled(line[..name_start].to_string(), style),
                            ];
                            if *backgrounded && !is_reasoning {
                                let verb_end = display_name.find(' ').unwrap_or(display_name.len());
                                let (verb, rest) = display_name.split_at(verb_end);
                                spans.push(Span::styled(
                                    verb.to_string(),
                                    Style::default()
                                        .fg(BACKGROUND_RUNNING_TEXT)
                                        .add_modifier(Modifier::BOLD),
                                ));
                                if !rest.is_empty() {
                                    spans.push(Span::styled(rest.to_string(), name_style));
                                }
                            } else {
                                spans.push(Span::styled(display_name.to_string(), name_style));
                            }
                            extend_tool_lifecycle_spans(
                                &mut spans,
                                &line[name_end..],
                                lifecycle,
                                style,
                                *backgrounded,
                            );
                            lines.push(Line::from(spans));
                        } else {
                            let mut spans = vec![Span::styled(prefix.to_string(), gutter_style)];
                            extend_tool_lifecycle_spans(
                                &mut spans,
                                &line,
                                lifecycle,
                                style,
                                *backgrounded,
                            );
                            lines.push(Line::from(spans));
                        }
                    }
                    if hovered_tool == Some(index) {
                        for line in &mut lines[summary_start..] {
                            apply_line_background(line, width, MESSAGE_HOVER_BG);
                        }
                    }
                    let body_expanded = *expanded || focused_tool == Some(index);
                    if show_tool_body
                        && body_expanded
                        && expandable
                        && let Some((language, source)) = code_view
                    {
                        let body_prefix = if tool_window.is_some() {
                            "│   │ "
                        } else {
                            "  │ "
                        };
                        if *complete {
                            let key = (index, false, tool_window.is_some());
                            let mut cache = self.tool_body_cache.borrow_mut();
                            #[cfg(test)]
                            let mut missed = false;
                            let rendered = match cache.lines.entry(key) {
                                std::collections::hash_map::Entry::Occupied(entry) => {
                                    entry.get().clone()
                                }
                                std::collections::hash_map::Entry::Vacant(entry) => {
                                    #[cfg(test)]
                                    {
                                        missed = true;
                                    }
                                    entry
                                        .insert(rendering::tool_body_lines(
                                            language,
                                            source,
                                            width,
                                            body_prefix,
                                        ))
                                        .clone()
                                }
                            };
                            #[cfg(test)]
                            if missed {
                                cache.misses += 1;
                            }
                            lines.extend(rendered);
                        } else {
                            lines.extend(rendering::tool_body_lines(
                                language,
                                source,
                                width,
                                body_prefix,
                            ));
                        }
                    }
                    if show_tool_body
                        && body_expanded
                        && expandable
                        && let Some((language, source)) = output_view
                    {
                        let body_prefix = if tool_window.is_some() {
                            "│   │ "
                        } else {
                            "  │ "
                        };
                        if *complete {
                            let key = (index, true, tool_window.is_some());
                            let mut cache = self.tool_body_cache.borrow_mut();
                            #[cfg(test)]
                            let mut missed = false;
                            let rendered = match cache.lines.entry(key) {
                                std::collections::hash_map::Entry::Occupied(entry) => {
                                    entry.get().clone()
                                }
                                std::collections::hash_map::Entry::Vacant(entry) => {
                                    #[cfg(test)]
                                    {
                                        missed = true;
                                    }
                                    entry
                                        .insert(rendering::tool_body_lines(
                                            language,
                                            source,
                                            width,
                                            body_prefix,
                                        ))
                                        .clone()
                                }
                            };
                            #[cfg(test)]
                            if missed {
                                cache.misses += 1;
                            }
                            lines.extend(rendered);
                        } else {
                            lines.extend(rendering::tool_body_lines(
                                language,
                                source,
                                width,
                                body_prefix,
                            ));
                        }
                    }
                    tool_rows.push((index, summary_start, lines.len()));
                    selection_rows.push(if tool_window.is_some() {
                        SelectionRowRange::nested_entry(index, summary_start, lines.len(), 0)
                    } else {
                        SelectionRowRange::transcript_entry(index, summary_start, lines.len())
                    });
                    if tool_window.is_none()
                        && !next_is_tool
                        && lines
                            .last()
                            .is_none_or(|line| !line_is_unstyled_blank(line))
                    {
                        lines.push(Line::default());
                    }
                    if let Some(window) = tool_window
                        && index + 1 == window.end
                    {
                        let (
                            header_row,
                            first_tool_row,
                            first_entry_row,
                            first_selection_row,
                        ) = tool_run_starts
                            .get(&window.start)
                            .copied()
                            .expect("tool run header was recorded");
                        let content_start = header_row + 1;
                        let content_end = lines.len();
                        let total_lines = content_end.saturating_sub(content_start);
                        let expandable = total_lines > tool_run_viewport_height;
                        let expanded = expandable && self.tool_run_expanded(window.start);
                        let viewport_height = if expanded {
                            total_lines
                        } else {
                            tool_run_viewport_height
                        };
                        let max_offset = total_lines.saturating_sub(viewport_height);
                        let offset = self
                            .tool_run_offsets
                            .get(&window.start)
                            .copied()
                            .unwrap_or(max_offset)
                            .min(max_offset);
                        let visible_end = offset.saturating_add(viewport_height).min(total_lines);
                        let viewport_start = content_start + offset;
                        let sticky_tool_header = tool_rows[first_tool_row..]
                            .iter()
                            .find(|(_, start, end)| {
                                *start < viewport_start && *end > viewport_start
                            })
                            .map(|(entry, start, _)| (*entry, lines[*start].clone()));
                        let mut visible_lines =
                            lines[content_start + offset..content_start + visible_end].to_vec();
                        if let Some((_, header)) = sticky_tool_header.as_ref()
                            && let Some(first) = visible_lines.first_mut()
                        {
                            *first = header.clone();
                        }

                        lines.truncate(content_start);
                        lines.extend(visible_lines);
                        let action_hint = if !expandable {
                            ""
                        } else if expanded {
                            if offset > 0 {
                                " · click to collapse · ↑ scroll"
                            } else {
                                " · click to collapse"
                            }
                        } else if offset > 0 {
                            " · click to expand · ↑ scroll"
                        } else {
                            " · click to expand"
                        };
                        lines[header_row] = Line::from(Span::styled(
                            format!("┌─ actions · {}{}", window.total, action_hint),
                            Style::default().fg(Color::DarkGray),
                        ));
                        lines.push(Line::from(Span::styled(
                            if visible_end < total_lines {
                                "└─ ↓ more"
                            } else {
                                "└─"
                            },
                            Style::default().fg(Color::DarkGray),
                        )));

                        let run_tool_rows = tool_rows.split_off(first_tool_row);
                        for (tool_index, start, end) in run_tool_rows {
                            let start = start.saturating_sub(content_start);
                            let end = end.saturating_sub(content_start);
                            let visible_start = start.max(offset);
                            let visible_tool_end = end.min(visible_end);
                            if visible_start < visible_tool_end {
                                tool_rows.push((
                                    tool_index,
                                    content_start + visible_start - offset,
                                    content_start + visible_tool_end - offset,
                                ));
                            }
                        }
                        let run_entry_rows = entry_rows.split_off(first_entry_row);
                        for (entry_index, start, end) in run_entry_rows {
                            let start = start.saturating_sub(content_start);
                            let end = end.saturating_sub(content_start);
                            let visible_start = start.max(offset);
                            let visible_entry_end = end.min(visible_end);
                            if visible_start < visible_entry_end {
                                entry_rows.push((
                                    entry_index,
                                    content_start + visible_start - offset,
                                    content_start + visible_entry_end - offset,
                                ));
                            }
                        }
                        let run_selection_rows =
                            selection_rows.split_off(first_selection_row);
                        for range in run_selection_rows {
                            let start = range.start.saturating_sub(content_start);
                            let end = range.end.saturating_sub(content_start);
                            let visible_start = start.max(offset);
                            let visible_selection_end = end.min(visible_end);
                            if visible_start < visible_selection_end {
                                let screen_start = content_start + visible_start - offset;
                                let screen_end =
                                    content_start + visible_selection_end - offset;
                                let body_start = range
                                    .body_start
                                    .saturating_add(visible_start.saturating_sub(start));
                                let has_sticky_header = sticky_tool_header
                                    .as_ref()
                                    .is_some_and(|(entry, _)| *entry == range.entry)
                                    && visible_start == offset;
                                if has_sticky_header {
                                    selection_rows.push(SelectionRowRange::nested_entry(
                                        range.entry,
                                        screen_start,
                                        screen_start + 1,
                                        0,
                                    ));
                                    if screen_start + 1 < screen_end {
                                        selection_rows.push(SelectionRowRange::nested_entry(
                                            range.entry,
                                            screen_start + 1,
                                            screen_end,
                                            body_start.saturating_add(1),
                                        ));
                                    }
                                } else {
                                    selection_rows.push(SelectionRowRange::nested_entry(
                                        range.entry,
                                        screen_start,
                                        screen_end,
                                        body_start,
                                    ));
                                }
                            }
                        }
                        tool_run_rows.push((
                            window.start,
                            header_row,
                            lines.len(),
                            max_offset,
                            expandable,
                        ));
                    }
                }
            }
        }
        (
            lines,
            tool_rows,
            tool_run_rows,
            message_rows,
            entry_rows,
            link_rows,
            selection_rows,
        )
    }

    fn prepare_message_markdown_cache(&self, width: usize) {
        {
            let mut cache = self.message_markdown_cache.borrow_mut();
            if cache.width == width {
                return;
            }
            cache.width = width;
            cache.messages.clear();
        }

        let message_count = self
            .order
            .iter()
            .filter(|entry| {
                matches!(
                    entry,
                    TranscriptEntry::Message { status, .. }
                        if *status != MessageStatus::Queued
                )
            })
            .count();
        if message_count < PARALLEL_MARKDOWN_RENDER_MIN_MESSAGES {
            return;
        }

        let worker_count = thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1)
            .min(MAX_PARALLEL_MARKDOWN_RENDER_WORKERS)
            .min(message_count.div_ceil(128));
        if worker_count < 2 {
            return;
        }

        let chunk_size = self.order.len().div_ceil(worker_count);
        let content_width = width.saturating_sub(MESSAGE_HORIZONTAL_PADDING * 2);
        let user_message_color = self.user_message_color;
        let assistant_message_color = self.assistant_message_color;
        let messages = thread::scope(|scope| {
            let handles = self
                .order
                .chunks(chunk_size)
                .enumerate()
                .map(|(chunk_index, entries)| {
                    scope.spawn(move || {
                        entries
                            .iter()
                            .enumerate()
                            .filter_map(|(entry_index, entry)| {
                                let TranscriptEntry::Message {
                                    actor,
                                    text,
                                    status,
                                    ..
                                } = entry
                                else {
                                    return None;
                                };
                                if *status == MessageStatus::Queued {
                                    return None;
                                }
                                let text_color = match actor {
                                    EventActor::User => Some(user_message_color),
                                    EventActor::Assistant => Some(assistant_message_color),
                                    _ => None,
                                };
                                let mut lines = markdown_lines(text, content_width, text_color);
                                let mut links = markdown_link_ranges(text, &lines);
                                for link in &mut links {
                                    link.start += MESSAGE_HORIZONTAL_PADDING;
                                    link.end += MESSAGE_HORIZONTAL_PADDING;
                                }
                                for line in &mut lines {
                                    line.spans.insert(0, Span::raw("  "));
                                }
                                Some((
                                    chunk_index * chunk_size + entry_index,
                                    MarkdownRender { lines, links },
                                ))
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .collect::<Vec<_>>();

            handles
                .into_iter()
                .flat_map(|handle| handle.join().expect("Markdown renderer panicked"))
                .collect::<HashMap<_, _>>()
        });

        let mut cache = self.message_markdown_cache.borrow_mut();
        #[cfg(test)]
        {
            cache.misses += messages.len();
        }
        cache.messages = messages;
    }

    fn tool_run_windows(&self) -> Vec<Option<ToolRunWindow>> {
        let mut windows = vec![None; self.order.len()];
        let mut index = 0;
        while index < self.order.len() {
            if !matches!(self.order[index], TranscriptEntry::Tool { .. }) {
                index += 1;
                continue;
            }
            let start = index;
            while index < self.order.len() {
                if matches!(self.order[index], TranscriptEntry::Tool { .. }) {
                    index += 1;
                    continue;
                }
                if action_run_bridge(&self.order[index])
                    && matches!(
                        self.order.get(index + 1),
                        Some(TranscriptEntry::Tool { .. })
                    )
                {
                    index += 1;
                    continue;
                }
                break;
            }
            let total = index - start;
            if total <= TOOL_RUN_BOX_THRESHOLD {
                continue;
            }
            let window = ToolRunWindow {
                start,
                end: index,
                total,
            };
            for slot in &mut windows[start..index] {
                *slot = Some(window);
            }
        }
        windows
    }

    fn copy_text(&self) -> Option<String> {
        if let Some(index) = self.selected {
            self.order
                .get(index)
                .and_then(TranscriptEntry::copy_text_owned)
        } else {
            self.last_assistant_message_text()
        }
    }

    fn last_assistant_message_text(&self) -> Option<String> {
        self.order.iter().rev().find_map(|entry| match entry {
            TranscriptEntry::Message {
                actor: EventActor::Assistant,
                text,
                ..
            } => Some(markdown_plain_text(text)),
            _ => None,
        })
    }

    fn select_previous(&mut self) {
        if self.order.is_empty() {
            return;
        }
        self.selected = Some(match self.selected {
            Some(index) => index.saturating_sub(1),
            None => self.order.len() - 1,
        });
    }

    fn select_next(&mut self) {
        if self.order.is_empty() {
            return;
        }
        self.selected = Some(match self.selected {
            Some(index) if index + 1 < self.order.len() => index + 1,
            Some(_) => self.order.len() - 1,
            None => self.order.len() - 1,
        });
    }

    fn selection_notice(&self, keymap: &KeyMap) -> String {
        match self.selected {
            Some(index) => format!(
                "Selection {}/{} · choose {}/{} · copy {}",
                index + 1,
                self.order.len(),
                keymap.label(KeyAction::SelectPrevious),
                keymap.label(KeyAction::SelectNext),
                keymap.label(KeyAction::Copy)
            ),
            None => "No transcript entries to select".to_string(),
        }
    }

    fn copy_notice(&self) -> String {
        if self.selected.is_some() {
            "Copied selected transcript entry".to_string()
        } else {
            "Copied last response".to_string()
        }
    }
}
