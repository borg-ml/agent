use borg_view::core::*;
#[cfg(test)]
use borg_view::entry::Execution;
use borg_view::entry::ExecutionState;
use borg_view::entry::{
    ToolRunWindow, TranscriptActionKind, TranscriptActionState, TranscriptEntry,
};
use borg_view::grouping::{is_reasoning_entry, same_action_type};
use borg_view::semantics::command_output_without_changes;
const USER_INTERRUPT_ACTIVITY: &str = "agent interrupted by user";
/// The readout counts whole seconds, so the timer is repainted once per
/// second rather than ten times for a digit nobody can read.
const TOOL_ELAPSED_REFRESH_MILLIS: i64 = 1_000;

fn user_message_has_structured_whitespace(text: &str) -> bool {
    text.contains('\n')
        && text.lines().any(|line| {
            line.contains('\t')
                || line.starts_with(' ')
                || line.contains("  ")
                || line
                    .chars()
                    .any(|character| "│┃┌┐└┘┬┴┼╭╮╰╯".contains(character))
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

/// The body of a chat message. Both the frame and the parallel warm-up fill
/// the same cache, so they must render a message identically.
fn message_markdown_render(
    actor: EventActor,
    text: &str,
    width: usize,
    text_color: Option<Color>,
) -> MarkdownRender {
    if actor != EventActor::User {
        let lines = markdown_lines(text, width, text_color);
        let links = markdown_link_ranges(text, &lines);
        return MarkdownRender { lines, links };
    }
    if user_message_has_structured_whitespace(text) {
        return MarkdownRender {
            lines: structured_user_message_lines(text, width, text_color),
            links: Vec::new(),
        };
    }
    let lines = crate::markdown::user_markdown_lines(text, width, text_color);
    let links = markdown_link_ranges(text, &lines);
    MarkdownRender { lines, links }
}

fn goal_status_label(status: GoalStatus) -> &'static str {
    match status {
        GoalStatus::Active => "▶ active",
        GoalStatus::Paused => "▮▮ paused",
        GoalStatus::Blocked => "▮▮ blocked",
        GoalStatus::UsageLimited => "▮▮ usage limit reached",
        GoalStatus::BudgetLimited => "▮▮ token budget reached",
        GoalStatus::Complete => "complete",
    }
}

fn line_is_blank(line: &Line<'static>) -> bool {
    line.spans.iter().all(|span| span.content.trim().is_empty())
}

fn line_is_unstyled_blank(line: &Line<'static>) -> bool {
    line.spans.is_empty()
}

fn message_badge_colors(color: Color) -> (Color, Color) {
    match color {
        Color::Rgb(red, green, blue) => {
            let mix = |base: u8, accent: u8, accent_weight: u16| {
                ((u16::from(base) * (100 - accent_weight) + u16::from(accent) * accent_weight)
                    / 100) as u8
            };
            let background = match MESSAGE_BG {
                Color::Rgb(base_red, base_green, base_blue) => Color::Rgb(
                    mix(base_red, red, 16),
                    mix(base_green, green, 16),
                    mix(base_blue, blue, 16),
                ),
                _ => MESSAGE_HOVER_BG,
            };
            let text = Color::Rgb(
                mix(red, u8::MAX, 62),
                mix(green, u8::MAX, 62),
                mix(blue, u8::MAX, 62),
            );
            (text, background)
        }
        _ => (Color::White, MESSAGE_HOVER_BG),
    }
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
    core: TranscriptCore,
    assignment_sender: String,
    local_minute_cache: HashMap<i64, String>,
    pub(crate) wrap_action_rows: bool,
    pub(crate) show_action_done_at: bool,
    tool_click_behavior: ToolClickBehavior,
    user_label: String,
    assistant_label: String,
    user_label_color: Color,
    user_message_color: Color,
    assistant_label_color: Color,
    assistant_message_color: Color,
    image_cell: Option<(u16, u16)>,
    message_markdown_cache: RefCell<MessageMarkdownCache>,
    tool_body_cache: RefCell<ToolBodyCache>,
    render_resumes: RefCell<Vec<RenderResume>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AgentRosterEntry {
    name: String,
    model: String,
    effort: String,
    state: String,
    lifecycle: Option<borg_remote::SubagentLifecycle>,
    usage: String,
    child_id: Option<Uuid>,
}

#[derive(Default)]
struct MessageMarkdownCache {
    previews: HashMap<(PathBuf, usize, usize), Vec<Line<'static>>>,
    messages: HashMap<(usize, usize), MarkdownRender>,
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
    lines: HashMap<(usize, usize, bool, bool), Vec<Line<'static>>>,
    #[cfg(test)]
    misses: usize,
}

/// Glyph previews carry two pixels per cell, so past this width the extra
/// columns show nothing a reader can use.
const GLYPH_PREVIEW_TILE_WIDTH: usize = 48;
const GLYPH_PREVIEW_TILE_ROWS: usize = 24;
/// Rows a graphics preview may reserve. A larger image is scaled down to fit,
/// so one screenshot cannot bury the transcript; its link opens it full size.
const MAX_GRAPHICS_PREVIEW_ROWS: usize = 24;

impl Default for Transcript {
    fn default() -> Self {
        Self {
            core: TranscriptCore::default(),
            assignment_sender: "director".to_string(),
            local_minute_cache: HashMap::new(),
            wrap_action_rows: false,
            show_action_done_at: false,
            tool_click_behavior: ToolClickBehavior::Fullscreen,
            user_label: "user".to_string(),
            assistant_label: "borg".to_string(),
            user_label_color: USER_LABEL_BLUE,
            user_message_color: USER_TEXT,
            assistant_label_color: BORG_ORANGE,
            assistant_message_color: Color::White,
            image_cell: None,
            message_markdown_cache: RefCell::new(MessageMarkdownCache::default()),
            tool_body_cache: RefCell::new(ToolBodyCache::default()),
            render_resumes: RefCell::new(Vec::new()),
        }
    }
}
impl std::ops::Deref for Transcript {
    type Target = TranscriptCore;
    fn deref(&self) -> &Self::Target {
        &self.core
    }
}
impl std::ops::DerefMut for Transcript {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.core
    }
}

/// Output lengths of a `TranscriptRender`, in tuple order.
type RenderLengths = [usize; 9];

fn render_lengths(render: &TranscriptRender) -> RenderLengths {
    [
        render.0.len(),
        render.1.len(),
        render.2.len(),
        render.3.len(),
        render.4.len(),
        render.5.len(),
        render.6.len(),
        render.7.len(),
        render.8.len(),
    ]
}

fn truncate_render(render: &mut TranscriptRender, lengths: RenderLengths) {
    render.0.truncate(lengths[0]);
    render.1.truncate(lengths[1]);
    render.2.truncate(lengths[2]);
    render.3.truncate(lengths[3]);
    render.4.truncate(lengths[4]);
    render.5.truncate(lengths[5]);
    render.6.truncate(lengths[6]);
    render.7.truncate(lengths[7]);
    render.8.truncate(lengths[8]);
}

/// What a render pass recorded about the entries it drew.
#[derive(Default)]
struct RenderTrace {
    /// Output lengths before each entry, then the final lengths.
    checkpoints: Vec<RenderLengths>,
    /// Entries whose rows show the render clock.
    clock_rows: Vec<usize>,
    /// Group headers span all their timed entries, not an individual clock.
    clock_groups: Vec<ToolRunWindow>,
    /// Live assistant messages, whose rows depend on whether a tool runs.
    running_tool_rows: Vec<usize>,
}

impl RenderTrace {
    fn truncate(&mut self, start: usize) {
        self.checkpoints.truncate(start);
        self.clock_rows.retain(|row| *row < start);
        self.clock_groups.retain(|window| window.start < start);
        self.running_tool_rows.retain(|row| *row < start);
    }
}

/// Render inputs that, when changed, invalidate every row.
#[derive(PartialEq)]
struct RenderInputs {
    width: usize,
    tool_run_viewport_height: usize,
    today: NaiveDate,
    user_label: String,
    assistant_label: String,
    assignment_sender: String,
    colors: [Color; 4],
    image_cell: Option<(u16, u16)>,
    tool_click_behavior: ToolClickBehavior,
    show_action_done_at: bool,
}

/// A cache-mode render plus what the next render must compare to redraw only
/// from the first entry whose rows can differ.
struct RenderResume {
    inputs: RenderInputs,
    render_time: DateTime<Utc>,
    changed_from: usize,
    running_tool: bool,
    director_prompt_row: Option<usize>,
    tool_run_offsets: HashMap<usize, usize>,
    expanded_tool_runs: HashSet<usize>,
    action_run_expansions: HashMap<usize, bool>,
    open_tool_run: Option<usize>,
    windows: Vec<Option<ToolRunWindow>>,
    trace: RenderTrace,
    render: Arc<TranscriptRender>,
    #[cfg(test)]
    redrawn_from: usize,
}

/// The part of an entry's rows that follows the render clock.
fn render_clock_key(entry: &TranscriptEntry, now: DateTime<Utc>) -> Option<String> {
    match entry {
        TranscriptEntry::Tool { execution, .. } => {
            format_tool_elapsed_at(execution.started_at(), execution.completed_at(), now)
        }
        TranscriptEntry::Goal { goal, .. } => format_elapsed_duration(goal_live_seconds(goal, now)),
        _ => None,
    }
}

fn format_tool_group_elapsed_at(entries: &[TranscriptEntry], now: DateTime<Utc>) -> Option<String> {
    let times = entries.iter().filter_map(|entry| match entry {
        TranscriptEntry::Tool { execution, .. } => Some((
            execution.started_at(),
            execution.completed_at().unwrap_or(now),
        )),
        _ => None,
    });
    let started_at = times.clone().map(|(start, _)| start).min()?;
    let ended_at = times.map(|(_, end)| end).max()?;
    format_tool_elapsed_at(started_at, Some(ended_at), now)
}

fn goal_live_seconds(goal: &SessionGoal, now: DateTime<Utc>) -> u64 {
    goal.time_used_seconds
        .saturating_add(if goal.status.is_active() {
            now.signed_duration_since(goal.updated_at)
                .num_seconds()
                .max(0) as u64
        } else {
            0
        })
}

/// Status-row values derived from the session configuration.
#[derive(Default)]
pub(crate) struct ConfigStatuses {
    pub(crate) model: Option<String>,
    pub(crate) effort: Option<String>,
    pub(crate) fast: Option<String>,
    pub(crate) permission: Option<String>,
    pub(crate) billing: Option<String>,
    pub(crate) cwd: String,
}

/// The billing label for the configured provider, from the durable capability
/// snapshot: `api`, the subscription tier (`pro`, `max`, …), `sub`, or
/// `endpoint`. `None` until the host has classified the lane.
fn billing_status_for(
    capabilities: &[borg_remote::ProviderCapability],
    provider: CodingProvider,
) -> Option<String> {
    capabilities
        .iter()
        .find(|capability| capability.provider == provider)
        .and_then(borg_remote::ProviderCapability::billing_label)
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
        "" | "context compacted" | "context was compacted" | "no summary" | "no details"
    )
}

/// A group's header: when it started, how many actions, and where commands ran.
fn tool_window_summary(entries: &[TranscriptEntry], total: usize, today_prefix: &str) -> String {
    let mut time = None;
    let mut cwd = None;
    for entry in entries {
        let TranscriptEntry::Tool {
            time: row_time,
            cwd: row_cwd,
            ..
        } = entry
        else {
            continue;
        };
        time.get_or_insert(row_time.as_str());
        if row_cwd.is_some() {
            cwd = row_cwd.as_deref();
        }
    }
    let mut parts: Vec<String> = Vec::new();
    if let Some(time) = time {
        parts.push(display_local_time(time, today_prefix).to_string());
    }
    parts.push(format!(
        "{total} action{}",
        if total == 1 { "" } else { "s" }
    ));
    if let Some(cwd) = cwd {
        let home = std::env::var("HOME").unwrap_or_default();
        parts.push(match cwd.strip_prefix(&home) {
            Some(rest) if !home.is_empty() => format!("~{rest}"),
            _ => cwd.to_string(),
        });
    }
    parts.join(" · ")
}

/// An expanded group uses a down triangle; the timestamp starts in the same
/// column as the collapsed `▸ ` header.
const TOOL_WINDOW_HEADER_INDENT: &str = "▾ ";

fn action_label_style(entry: &TranscriptEntry) -> Style {
    match entry {
        TranscriptEntry::Tool {
            name, execution, ..
        } => {
            if is_reasoning_entry(entry) {
                Style::default()
                    .fg(Color::Gray)
                    .add_modifier(Modifier::BOLD | Modifier::ITALIC)
            } else if matches!(
                execution.lifecycle(),
                borg_view::wire::Lifecycle::Failed | borg_view::wire::Lifecycle::Stopped
            ) {
                Style::default()
                    .fg(Color::LightRed)
                    .add_modifier(Modifier::BOLD)
            } else if execution.is_backgrounded() {
                Style::default()
                    .fg(USER_LABEL_BLUE)
                    .add_modifier(Modifier::BOLD)
            } else if is_subagent_tool(name)
                || matches!(entry, TranscriptEntry::Tool { source_name, .. } if is_subagent_tool(source_name))
            {
                Style::default()
                    .fg(SUBAGENT_PURPLE)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD)
            }
        }
        TranscriptEntry::Action {
            kind, execution, ..
        } => Style::default().fg(transcript_action_color(*kind, execution.state())),
        _ => Style::default()
            .fg(Color::White)
            .add_modifier(Modifier::BOLD),
    }
}

/// Group labels use the same emphasis as solo tools; metadata stays muted.
fn tool_window_header(
    prefix: &'static str,
    text: String,
    show_time: bool,
    label_style: Style,
) -> Line<'static> {
    let grey = Style::default().fg(Color::DarkGray);
    let mut spans = vec![Span::styled(prefix, grey)];
    let start = if show_time {
        text.find(" · ").map_or(0, |index| index + " · ".len())
    } else {
        0
    };
    let end = start + text[start..].find(['(', '·']).unwrap_or(text.len() - start);
    spans.push(Span::styled(text[..start].to_string(), grey));
    spans.push(Span::styled(text[start..end].to_string(), label_style));
    spans.push(Span::styled(text[end..].to_string(), grey));
    Line::from(spans)
}

/// A finished row's marker: what kind of work it was, at a glance.
fn tool_kind_marker(name: &str, is_reasoning: bool) -> &'static str {
    if is_reasoning {
        return "◦";
    }
    if is_subagent_tool(name) {
        return "◆";
    }
    match name.split(' ').next().unwrap_or(name) {
        "Read" | "List" | "View" | "Open" | "Inspect" => "≡",
        "Search" | "Find" | "Grep" => "⌕",
        "Write" | "Edit" | "Update" | "Create" | "Delete" | "Apply" | "Rename" | "Move" => "◈",
        "Run" | "Wait" | "Send" | "Stop" | "Build" | "Test" => "›",
        _ => "✓",
    }
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

/// Queued prompts and a reply whose first block is still held have no row.
fn message_row_hidden(entry: &TranscriptEntry) -> bool {
    matches!(
        entry,
        TranscriptEntry::Message {
            status: MessageStatus::Queued,
            ..
        }
    ) || matches!(entry, TranscriptEntry::Message {
            actor: EventActor::Assistant,
            status: MessageStatus::InProgress,
            text,
            ..
        } if text.trim().is_empty())
}

impl Transcript {
    fn apply_child_meta(
        &mut self,
        child_id: Uuid,
        cursor: borg_view::wire::Cursor,
        meta: &borg_view::wire::Meta,
    ) -> anyhow::Result<bool> {
        let changed = self.core.apply_child_meta(child_id, cursor, meta)?;
        self.apply_render_invalidations();
        Ok(changed)
    }

    fn apply_render_invalidations(&mut self) {
        let changes = std::mem::take(self.core.order.invalidations.get_mut());
        for change in changes {
            match change {
                RenderInvalidation::Changed(index) => {
                    self.message_markdown_cache
                        .get_mut()
                        .messages
                        .retain(|(i, _), _| *i != index);
                    self.tool_body_cache
                        .get_mut()
                        .lines
                        .retain(|(i, _, _, _), _| *i != index);
                }
                RenderInvalidation::Inserted(index) | RenderInvalidation::Removed(index) => {
                    let inserting = matches!(change, RenderInvalidation::Inserted(_));
                    let cache = self.message_markdown_cache.get_mut();
                    cache.messages = cache
                        .messages
                        .drain()
                        .filter_map(|((i, width), body)| {
                            if !inserting && i == index {
                                return None;
                            }
                            let shifted = if inserting {
                                i + usize::from(i >= index)
                            } else {
                                i - usize::from(i > index)
                            };
                            Some(((shifted, width), body))
                        })
                        .collect();
                    let cache = self.tool_body_cache.get_mut();
                    cache.lines = cache
                        .lines
                        .drain()
                        .filter_map(|((i, width, output, boxed), body)| {
                            if !inserting && i == index {
                                return None;
                            }
                            let shifted = if inserting {
                                i + usize::from(i >= index)
                            } else {
                                i - usize::from(i > index)
                            };
                            Some(((shifted, width, output, boxed), body))
                        })
                        .collect();
                }
                RenderInvalidation::Cleared => {
                    self.message_markdown_cache.get_mut().messages.clear();
                    self.tool_body_cache.get_mut().lines.clear();
                    self.render_resumes.get_mut().clear();
                }
            }
        }
        self.core.order.dirty.get_mut().clear();
        self.core.order.removed.clear();
    }

    fn apply(&mut self, event: &SessionEvent) -> Option<usize> {
        let removed = self.core.apply(event);
        self.apply_render_invalidations();
        removed
    }

    fn apply_history(&mut self, event: &SessionEvent) -> Option<usize> {
        let removed = self.core.apply_history(event);
        self.apply_render_invalidations();
        removed
    }

    fn set_image_cell(&mut self, cell: Option<(u16, u16)>) {
        self.image_cell = cell;
    }

    fn image_cell(&self) -> Option<(u16, u16)> {
        self.image_cell
    }

    /// The line under a preview tile. On a terminal without graphics the reader
    /// is told the tile cannot show the text rather than left to squint at it.
    fn preview_caption(&self, number: usize, path: &std::path::Path) -> String {
        if self.image_cell.is_some() {
            format!("Image {number} · {}", display_name(path))
        } else {
            // The limitation goes before the name so truncation cannot
            // swallow it.
            format!(
                "Image {number} · text unreadable here · {}",
                display_name(path)
            )
        }
    }

    fn message_id_at(&self, index: usize) -> Option<Uuid> {
        self.messages
            .iter()
            .find_map(|(message_id, message_index)| {
                (*message_index == index).then_some(*message_id)
            })
    }

    fn sync_subagent_config(
        &mut self,
        agent: &SubagentSnapshot,
        inherited: Option<&SessionDisplayConfig>,
    ) {
        if self
            .config_updated_at
            .is_some_and(|updated| updated > agent.updated_at)
        {
            return;
        }
        let identity_changed = self
            .config
            .as_ref()
            .is_some_and(|config| config.provider != agent.provider || config.model != agent.model);
        let config = self.config.get_or_insert_with(|| {
            inherited.cloned().unwrap_or(SessionDisplayConfig {
                cwd: agent.cwd.clone(),
                provider: agent.provider,
                model: agent.model.clone(),
                effort: agent.effort.clone(),
                response_language: ResponseLanguage::default(),
                fast: agent.fast,
                ultrafast: agent.ultrafast,
                permission_mode: PermissionMode::Manual,
                speed_support: Default::default(),
            })
        });
        if config.provider != agent.provider || config.model != agent.model {
            config.speed_support = Default::default();
        }
        config.cwd = agent.cwd.clone();
        config.provider = agent.provider;
        config.model = agent.model.clone();
        config.effort = agent.effort.clone();
        config.fast = agent.fast;
        config.ultrafast = agent.ultrafast;
        self.config_updated_at = Some(agent.updated_at);
        if identity_changed {
            self.context_known = false;
            self.context_remaining_percent = 100;
            self.context_tokens = None;
            self.context_window_tokens = None;
        }
    }

    fn upsert_subagent_snapshot(&mut self, agent: &SubagentSnapshot) {
        self.upsert_subagent_snapshot_with_status(agent, agent.status);
    }

    fn project_optimistic_message(&mut self, event: &SessionEvent) {
        let mut placeholder = event.clone();
        if event.sequence == 0
            && let SessionEventKind::Message {
                actor: EventActor::User,
                status,
                ..
            } = &mut placeholder.kind
        {
            *status = MessageStatus::InProgress;
        }
        let _ = self.apply(&placeholder);
        if event.sequence == 0
            && let SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                status,
                ..
            } = &event.kind
            && let Some(index) = self.messages.get(message_id).copied()
            && let Some(TranscriptEntry::Message {
                execution,
                status: row_status,
                ..
            }) = self.order.get_mut(index)
        {
            execution.transition(ExecutionState::Waiting, event.created_at);
            *row_status = *status;
        }
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
                fast: config.fast,
                ultrafast: config.ultrafast,
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
        self.local_minute_cache.reserve(event_count.min(4_096) / 32);
    }

    fn apply_projected_plan(
        &mut self,
        items: &[PlanItem],
        participant_id: Uuid,
        revision: u64,
        time: String,
    ) -> bool {
        if !self.seed_plan(items, Some(participant_id), Some(revision)) {
            return false;
        }
        // An empty workspace plan is still authoritative, but not a card on a new chat.
        if items.is_empty()
            && !self
                .order
                .iter()
                .any(|entry| matches!(entry, TranscriptEntry::Plan { .. }))
        {
            return true;
        }
        // A workspace snapshot updates the existing card without shifting tool/message indices.
        if let Some(TranscriptEntry::Plan {
            items: current,
            previous,
            time: current_time,
            ..
        }) = self
            .order
            .iter_mut()
            .rev()
            .find(|entry| matches!(entry, TranscriptEntry::Plan { .. }))
        {
            *previous = std::mem::replace(current, items.to_vec());
            *current_time = time;
        } else {
            self.order.push(TranscriptEntry::Plan {
                items: items.to_vec(),
                previous: Vec::new(),
                time,
                expanded: false,
            });
        }
        true
    }

    fn accepts_plan_history(&self, kind: &SessionEventKind) -> bool {
        match kind {
            SessionEventKind::PlanProjected { participant_id, .. } => self
                .plan_participant_id
                .is_none_or(|current| current == *participant_id),
            SessionEventKind::AgentPlanProjected {
                session_id,
                participant_id,
                ..
            } => self
                .agent_plans
                .get(session_id)
                .is_none_or(|current| current.participant_id == *participant_id),
            _ => true,
        }
    }

    fn seed_session_state(&mut self, state: &SessionState) {
        self.meta.director = state.team_director.as_ref().map(Into::into);
        self.goal = state.goal.clone();
        if let (Some(participant_id), Some(revision)) =
            (state.plan_participant_id, state.plan_workspace_revision)
        {
            self.apply_projected_plan(&state.todos, participant_id, revision, String::new());
        } else {
            self.seed_plan(&state.todos, None, None);
        }
        for (id, plan) in &state.agent_plans {
            if self.agent_plans.get(id).is_none_or(|old| {
                plan.participant_id != old.participant_id
                    || plan.workspace_revision >= old.workspace_revision
            }) {
                self.agent_plans.insert(*id, plan.clone());
            }
        }
        self.watches = state.watches.clone();
        self.provider_capabilities = state.provider_capabilities.clone();
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
                ultrafast: configuration.ultrafast,
                permission_mode: configuration.permission_mode,
                speed_support: configuration.speed_support,
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
        self.session_usage = borg_remote::SubagentUsage {
            input_tokens: state.usage.input_tokens,
            cached_input_tokens: state.usage.cached_input_tokens,
            cache_creation_input_tokens: state.usage.cache_creation_input_tokens,
            output_tokens: state.usage.output_tokens,
            total_tokens: state.usage.total_tokens,
            context_tokens: state.usage.context_tokens,
            cost_microusd: state.usage.cost_microusd,
            cost_basis: state.usage.cost_basis.clone(),
            cost_complete: state.usage.cost_complete,
        };
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

    fn show_goal(&mut self, goal: Option<&SessionGoal>) -> Option<usize> {
        let time = Utc::now().to_rfc3339();
        match goal {
            Some(goal) => self.upsert_goal(goal.clone(), time, false),
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
                    ) =>
                {
                    GoalStatus::Active
                }
                _ => return false,
            };
            goal.status = next_status;
            goal.updated_at = Utc::now();
            goal.clone()
        };
        self.upsert_goal(updated_goal, Utc::now().to_rfc3339(), false);
        true
    }

    fn show_director_context_boundary(&mut self) {
        self.child_transcript = true;
        self.order.push(TranscriptEntry::Activity {
            text: DIRECTOR_CONTEXT_BOUNDARY.to_string(),
            time: Utc::now().to_rfc3339(),
        });
    }

    fn show_plan(&mut self, items: &[PlanItem]) -> Option<usize> {
        let time = Utc::now().to_rfc3339();
        self.upsert_plan(items.to_vec(), time)
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
            execution,
            ..
        }) = self.order.get_mut(index)
            && execution.is_terminal()
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
                execution,
                ..
            }) if execution.is_terminal() && compaction_has_expandable_detail(summary)
        )
    }

    fn inspector_heading(&self, index: usize) -> Option<(&str, bool)> {
        match self.order.get(index)? {
            TranscriptEntry::Tool {
                name, execution, ..
            } => Some((name, execution.is_terminal())),
            TranscriptEntry::Action {
                label, execution, ..
            } => Some((label, execution.is_terminal())),
            TranscriptEntry::Plan { .. } => Some(("Plan", true)),
            TranscriptEntry::Compaction { execution, .. } => {
                Some(("Compaction", execution.is_terminal()))
            }
            _ => None,
        }
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
            summary,
            sequence,
            execution,
            ..
        } = self.order.get(index)?
        else {
            return None;
        };
        (execution.is_terminal() && *sequence > 0 && compaction_has_expandable_detail(summary))
            .then(|| sequence.checked_add(1))
            .flatten()
    }

    fn plan_is_clippable(&self, index: usize) -> bool {
        matches!(
            self.order.get(index),
            Some(TranscriptEntry::Plan { items, previous, .. })
                if plan_card_rows(items, previous, true).1 > 0
        )
    }

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
    /// Footer hint for a clickable non-tool row; rows carry no inline hint.
    fn entry_click_hint(&self, index: usize) -> Option<&'static str> {
        match self.order.get(index)? {
            TranscriptEntry::Action { body, expanded, .. }
                if body.as_deref().is_some_and(|body| !body.trim().is_empty()) =>
            {
                Some(
                    if self.tool_click_behavior == ToolClickBehavior::Fullscreen {
                        "click open full screen"
                    } else if *expanded {
                        "click collapse"
                    } else {
                        "click expand"
                    },
                )
            }
            TranscriptEntry::Compaction {
                summary,
                expanded,
                execution,
                ..
            } if execution.is_terminal() && compaction_has_expandable_detail(summary) => {
                Some(if *expanded {
                    "click collapse · right-click actions"
                } else {
                    "click expand · right-click actions"
                })
            }
            TranscriptEntry::Plan {
                items, expanded, ..
            } if items.len() > MAX_COLLAPSED_PLAN_ITEMS => Some(if *expanded {
                "click collapse"
            } else {
                "click expand"
            }),
            _ => None,
        }
    }

    fn tool_run_header_hint(&self, start: usize) -> &'static str {
        if self.tool_run_expanded(start) {
            "click collapse"
        } else {
            "click expand"
        }
    }

    fn foreground_action_id(&self, index: usize) -> Option<String> {
        let TranscriptEntry::Tool {
            execution,
            code_view,
            ..
        } = self.order.get(index)?
        else {
            return None;
        };
        if execution.is_terminal() || execution.is_backgrounded() {
            return None;
        }
        if code_view
            .as_ref()
            .is_some_and(|(language, _)| language == "reasoning")
        {
            return None;
        }
        let id = self
            .tools
            .iter()
            .find_map(|(id, row)| (*row == index).then_some(id))?;
        if self
            .preparing_tools
            .values()
            .chain(&self.unkeyed_preparing_tools)
            .any(|preparing| preparing == id)
        {
            return None;
        }
        Some(id.clone())
    }

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
            Some("click inspect · right-click copy diff")
        } else if output_view
            .as_ref()
            .is_some_and(|(_, body)| !body.trim().is_empty())
        {
            Some("click inspect · right-click copy output")
        } else if code_view
            .as_ref()
            .is_some_and(|(language, body)| language == "reasoning" && !body.trim().is_empty())
        {
            Some("click inspect · right-click copy thinking")
        } else if code_view
            .as_ref()
            .is_some_and(|(_, body)| !body.trim().is_empty())
        {
            Some("click inspect · right-click copy tool call")
        } else if !detail.trim().is_empty() {
            Some("right-click copy tool details")
        } else {
            None
        }
    }

    fn hydrate_payload(&mut self, payload: &SessionPayloadRef, bytes: Vec<u8>) -> Result<()> {
        let Some(index) = self.order.iter().position(|entry| {
            matches!(
                entry,
                TranscriptEntry::Tool { payload_refs, .. }
                    if payload_refs.iter().any(|candidate| candidate.id == payload.id)
            )
        }) else {
            return Ok(());
        };
        let Some(TranscriptEntry::Tool {
            source_name,
            name,
            detail,
            code_view,
            output_view,
            execution,
            payload_refs,
            ..
        }) = self.order.get_mut(index)
        else {
            return Ok(());
        };
        let mut command_edit = None;
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
                command_edit = command_edit_presentation(source_name, &output);
                let run_output = command_edit
                    .as_ref()
                    .map(|_| command_output_without_changes(&output))
                    .unwrap_or(output);
                let hydrated_presentation = project_tool_presentation(
                    source_name,
                    &serde_json::Value::Null,
                    Some(&run_output),
                    execution.is_failed(),
                );
                // Loading an old receipt must not revive a settled background job.
                let edit_diff = (!execution.is_failed())
                    .then(|| hydrated_presentation.output.clone())
                    .flatten()
                    .filter(|body| is_diff_language(&body.language));
                if let Some(body) = edit_diff {
                    *name = hydrated_presentation.label;
                    *detail = hydrated_presentation.detail;
                    *code_view = Some((body.language, body.text));
                    *output_view = None;
                } else {
                    if let Some(body) = hydrated_presentation
                        .input
                        .filter(|body| body.language == "command")
                    {
                        *code_view = Some((body.language, body.text));
                    }
                    *output_view = if is_mcp_resource_probe(source_name) {
                        None
                    } else if execution.is_failed()
                        && !run_output.trim().is_empty()
                        && hydrated_presentation
                            .output
                            .as_ref()
                            .is_none_or(|body| body.language != "text")
                    {
                        Some(("text".to_string(), run_output.trim_end().to_string()))
                    } else {
                        tool_output_code_view(source_name, &run_output)
                    };
                }
            }
            SessionPayloadKind::ToolResultInput => {
                let input: serde_json::Value = serde_json::from_slice(&bytes)
                    .context("stored tool result input is not valid JSON")?;
                let presentation =
                    project_tool_presentation(source_name, &input, None, execution.is_failed());
                if presentation.category != ToolPresentationCategory::Edit
                    && let Some(body) = presentation.input
                {
                    *code_view = Some((body.language, body.text));
                }
                if name == "Search web"
                    && let Some(query) = web_search_query(&input)
                {
                    *detail = format!("“{}”", compact_text(&query, 120));
                }
            }
            // Provider prompts and native provider model messages are attached
            // to their own provider event, never to a tool row, so there is
            // nothing to hydrate here.
            SessionPayloadKind::ProviderPrompt | SessionPayloadKind::ProviderModelMessage => {}
        }
        payload_refs.retain(|candidate| candidate.id != payload.id);
        if let Some(edit) = command_edit
            && let Some(tool_call_id) = self.tools.iter().find_map(|(tool_call_id, tool_index)| {
                (*tool_index == index).then(|| tool_call_id.clone())
            })
            && let Some(TranscriptEntry::Tool {
                time, execution, ..
            }) = self.order.get(index)
            && let Some(completed_at) = execution.completed_at()
        {
            let time = time.clone();
            if !self.hydrate_command_edit(&tool_call_id, &edit) {
                self.upsert_command_edit(&tool_call_id, edit, time, completed_at, None);
            }
        }
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

    fn action_run_expanded(&self, start: usize) -> bool {
        self.action_run_expansions
            .get(&start)
            .copied()
            .unwrap_or_else(|| self.action_run_open_by_default(start))
    }

    /// A repeated run folds once a different action follows it, or once it
    /// reaches five actions, unless one of its actions is still live.
    fn action_run_open_by_default(&self, start: usize) -> bool {
        let Some(first) = self.order.get(start) else {
            return false;
        };
        let mut end = start;
        let mut live = false;
        while let Some(entry) = self.order.get(end)
            && same_action_type(first, entry)
        {
            live |= matches!(entry, TranscriptEntry::Tool { execution, .. } | TranscriptEntry::Action { execution, .. } if !execution.is_terminal());
            end += 1;
        }
        let next_is_action = matches!(
            self.order.get(end),
            Some(TranscriptEntry::Tool { .. } | TranscriptEntry::Action { .. })
        );
        if end - start >= 5 {
            return live;
        }
        live || !next_is_action
    }

    fn toggle_action_run_expansion(&mut self, start: usize) -> bool {
        let expanded = !self.action_run_expanded(start);
        self.action_run_expansions.insert(start, expanded);
        expanded
    }

    fn tool_run_start_containing(&self, index: usize) -> Option<usize> {
        if !matches!(self.order.get(index), Some(TranscriptEntry::Tool { .. })) {
            return None;
        }
        self.tool_run_windows()[index].map(|window| window.start)
    }

    fn status_label(&self, status: SessionStatus) -> &'static str {
        if status == SessionStatus::Ready && self.waiting_on_watchers {
            "waiting"
        } else {
            status_label(status)
        }
    }

    /// The row carrying the director's assignment, if this window shows it.
    /// Both the renderer and the markdown prefill resolve it here so a cached
    /// body can never disagree with the badge above it.
    fn director_prompt_row(&self) -> Option<usize> {
        match self.director_prompt {
            DirectorPrompt::Row(prompt) => self.messages.get(&prompt).copied(),
            DirectorPrompt::Unknown | DirectorPrompt::Expected => None,
        }
    }

    /// Drain the entry indices inserted ahead of existing rows by the most
    /// recent applied event, oldest first. Callers holding their own entry
    /// indices must shift them by these, in order, exactly as
    /// `reindex_after_insertion` shifted the transcript's own bookkeeping.
    fn take_entry_insertions(&mut self) -> Vec<usize> {
        std::mem::take(&mut self.pending_entry_insertions)
    }

    pub(crate) fn set_response_streaming(
        &mut self,
        streaming: borg_ui::preferences::ResponseStreaming,
    ) {
        if self.response_streaming == streaming {
            return;
        }
        self.response_streaming = streaming;
        if streaming == borg_ui::preferences::ResponseStreaming::Token {
            self.flush_streaming_tails();
        } else {
            let live: Vec<Uuid> = self.messages.keys().copied().collect();
            for message_id in live {
                self.hold_back_unfinished(message_id);
            }
        }
    }

    fn set_diff_expansion(&mut self, policy: DiffExpansionPolicy) {
        self.diff_expansion = policy;
        for entry in self.order.iter_mut() {
            if let TranscriptEntry::Tool {
                code_view: Some((language, _)),
                expanded,
                ..
            } = entry
                && is_diff_language(language)
            {
                *expanded = policy != DiffExpansionPolicy::Collapsed;
            }
        }
    }

    fn set_auto_expand_tools(&mut self, enabled: bool) {
        self.auto_expand_tools = enabled;
        for entry in self.order.iter_mut() {
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

    fn set_auto_expand_thinking(&mut self, enabled: bool) {
        self.auto_expand_thinking = enabled;
        if let Some(index) = self.active_reasoning
            && let Some(TranscriptEntry::Tool { expanded, .. }) = self.order.get_mut(index)
        {
            *expanded = enabled;
        }
    }

    fn set_action_descriptors(&mut self, enabled: bool) {
        self.action_descriptors = enabled;
    }

    fn config_statuses(&self) -> ConfigStatuses {
        let Some(config) = self.config.as_ref() else {
            return ConfigStatuses::default();
        };
        ConfigStatuses {
            model: config.model.clone(),
            effort: config.effort.clone(),
            fast: if config.ultrafast {
                Some("ultrafast".to_string())
            } else {
                config.fast.then(|| "fast".to_string())
            },
            permission: Some(permission_mode_label(config.permission_mode).to_string()),
            billing: if config.provider == CodingProvider::OpenCode
                && config
                    .model
                    .as_deref()
                    .is_some_and(|model| model.starts_with("opencode-go/"))
            {
                Some("Go sub".to_string())
            } else {
                billing_status_for(&self.provider_capabilities, config.provider)
            },
            cwd: fish_style_path(&config.cwd),
        }
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
            .filter(|status| matches!(status, SubagentStatus::Starting | SubagentStatus::Running))
            .count()
    }

    fn agent_roster_entries(&self) -> Vec<AgentRosterEntry> {
        let mut rows = Vec::new();
        let host_is_director = self
            .meta
            .director
            .as_ref()
            .is_none_or(|director| director.path == "/root");
        let host_name = if host_is_director {
            "director"
        } else {
            "/root"
        };
        let host_role = if host_is_director {
            "main thread"
        } else {
            "member"
        };
        if let Some(config) = self.config.as_ref() {
            let model = config
                .model
                .as_deref()
                .unwrap_or_else(|| config.provider.catalog_backend());
            rows.push(AgentRosterEntry {
                name: host_name.to_string(),
                model: model.to_string(),
                effort: config.effort.as_deref().unwrap_or("—").to_string(),
                state: host_role.to_string(),
                lifecycle: None,
                usage: format_subagent_usage(&self.session_usage)
                    .trim_start()
                    .to_string(),
                child_id: None,
            });
        } else {
            rows.push(AgentRosterEntry {
                name: host_name.to_string(),
                model: "model pending".to_string(),
                effort: "—".to_string(),
                state: host_role.to_string(),
                lifecycle: None,
                usage: "—".to_string(),
                child_id: None,
            });
        }
        let mut agents = self
            .subagent_snapshots
            .keys()
            .filter_map(|id| self.core.effective_subagent_snapshot(*id))
            .collect::<Vec<_>>();
        let rank = |agent: &SubagentSnapshot| match agent.lifecycle() {
            borg_remote::SubagentLifecycle::Starting | borg_remote::SubagentLifecycle::Running => 0,
            borg_remote::SubagentLifecycle::WaitingOnWatchers
            | borg_remote::SubagentLifecycle::WaitingForApproval => 1,
            _ => 2,
        };
        agents.sort_by(|left, right| {
            (rank(left), &left.task_name).cmp(&(rank(right), &right.task_name))
        });
        rows.extend(agents.into_iter().map(|agent| {
            let name = if self
                .meta
                .director
                .as_ref()
                .is_some_and(|director| director.session_id == agent.session_id.to_string())
            {
                format!("{} · director", agent.task_name)
            } else {
                display_agent_name(&agent.task_name)
            };
            let model = display_subagent_model(&agent);
            let effort = agent.effort.as_deref().unwrap_or("—");
            let usage = format_subagent_usage(&agent.usage);
            let usage = usage
                .trim_start()
                .strip_prefix("usage ")
                .unwrap_or(usage.trim_start());
            AgentRosterEntry {
                name,
                model,
                effort: effort.to_string(),
                state: borg_view::projection_helpers::subagent_lifecycle_detail(&agent),
                lifecycle: Some(agent.lifecycle()),
                usage: usage.to_string(),
                child_id: Some(agent.session_id),
            }
        }));
        rows
    }

    fn goal_status(&self) -> Option<String> {
        let goal = self.goal.as_ref()?;
        let label = match goal.status {
            GoalStatus::Active => "▶ active",
            GoalStatus::Paused => "▮▮ paused",
            GoalStatus::Blocked => "▮▮ blocked",
            GoalStatus::UsageLimited => "▮▮ usage limit reached",
            GoalStatus::BudgetLimited => "▮▮ token budget reached",
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

    /// Footer token for armed watches: only running ones count.
    pub(crate) fn watch_status(&self) -> Option<String> {
        let running = self.watches.iter().filter(|watch| watch.running).count();
        (running > 0).then(|| format!("{running} watcher{}", if running == 1 { "" } else { "s" }))
    }

    /// One row per watch for the watches panel, newest first.
    pub(crate) fn watch_rows(&self) -> Vec<(String, Uuid)> {
        let now = Utc::now();
        let mut watches: Vec<&WatchSummary> = self.watches.iter().collect();
        watches.sort_by_key(|watch| std::cmp::Reverse(watch.started_at));
        watches
            .into_iter()
            .map(|watch| {
                let state = if watch.running { "running" } else { "exited" };
                let events = format!(
                    "{} event{}",
                    watch.event_count,
                    if watch.event_count == 1 { "" } else { "s" }
                );
                let last = watch
                    .last_event_at
                    .map(|at| format!("last {} ago", format_short_age(now - at)))
                    .unwrap_or_else(|| "no events yet".to_string());
                let age = format_short_age(now - watch.started_at);
                (
                    format!("{} · {state} {age} · {events} · {last}", watch.label),
                    watch.watch_id,
                )
            })
            .collect()
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
        let mut rows =
            self.order
                .iter()
                .enumerate()
                .filter_map(|(index, entry)| match entry {
                    TranscriptEntry::Tool {
                        source_name,
                        name,
                        detail,
                        execution,
                        ..
                    } if execution.is_backgrounded()
                        && tool_can_start_background_process(source_name)
                        && !self
                            .provider_backgrounds
                            .values()
                            .any(|process| process.tool_index == index)
                        && !self.runtime_processes.values().any(|process| {
                            process.running && process.tool_index == Some(index)
                        }) =>
                    {
                        claimed_tools.insert(index);
                        Some((
                            compact_text(
                                if detail.trim().is_empty() {
                                    name
                                } else {
                                    detail
                                },
                                120,
                            ),
                            Some(index),
                        ))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
        let watch_ids: HashSet<Uuid> = self.watches.iter().map(|watch| watch.watch_id).collect();
        rows.extend(
            self.runtime_processes
                .iter()
                .filter(|(process_id, process)| {
                    !watch_ids.contains(process_id)
                        && process.running
                        && process
                            .tool_index
                            .is_none_or(|tool_index| !claimed_tools.contains(&tool_index))
                })
                .map(|(_, process)| {
                    (
                        format!(
                            "pid {}  {}",
                            process.pid,
                            compact_text(&process.command, 100)
                        ),
                        process.tool_index,
                    )
                }),
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
        let mut ordered = ordered_plan_items(&self.todos);
        if !expanded {
            ordered.sort_by_key(|item| item.status == PlanItemStatus::Completed);
        }
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
                    PlanItemStatus::Blocked => "⊘ blocked ·",
                    PlanItemStatus::AwaitingReview => "◇ awaiting review ·",
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
        self.has_running_tool().then(|| {
            now.timestamp_millis()
                .div_euclid(TOOL_ELAPSED_REFRESH_MILLIS)
        })
    }

    /// A running tool repaints on whole-second boundaries, so a long action
    /// costs the same as a short one: one repaint a second, which is exactly
    /// when its readout changes.
    fn running_tool_timer_tick_at(&self, now: DateTime<Utc>) -> Option<i64> {
        self.has_running_tool().then(|| {
            now.timestamp_millis()
                .div_euclid(TOOL_ELAPSED_REFRESH_MILLIS)
        })
    }

    fn running_tool_elapsed_labels_at(&self, now: DateTime<Utc>) -> Vec<(usize, Option<String>)> {
        let mut indices = self.tools.values().copied().collect::<Vec<_>>();
        indices.extend(self.active_reasoning);
        indices.sort_unstable();
        indices.dedup();
        // A group header and its individual rows have different clocks. The
        // single-entry viewport refresh cannot update both; use the renderer.
        let windows = self.tool_run_windows();
        if indices.iter().any(|index| {
            matches!(
                self.order.get(*index),
                Some(TranscriptEntry::Tool {
                    execution,
                    ..
                }) if !execution.is_terminal()
            ) && windows.get(*index).is_some_and(Option::is_some)
        }) {
            return Vec::new();
        }
        indices
            .into_iter()
            .filter_map(|index| match self.order.get(index) {
                Some(TranscriptEntry::Tool { execution, .. }) if !execution.is_terminal() => {
                    Some((
                        index,
                        format_tool_elapsed_at(
                            execution.started_at(),
                            execution.completed_at(),
                            now,
                        ),
                    ))
                }
                _ => None,
            })
            .collect()
    }

    fn has_running_tool(&self) -> bool {
        self.order.iter().any(|entry| {
            matches!(entry,
                TranscriptEntry::Tool { execution,  .. } if !execution.is_terminal()
            )
        })
    }

    fn tool_activity_is_running(&self, index: usize) -> bool {
        matches!(
            self.order.get(index),
            Some(TranscriptEntry::Tool {
                name,
                code_view,
                execution,
                ..
            }) if !execution.is_terminal() && (execution.is_backgrounded() || !tool_action_is_instant(
                name,
                code_view.as_ref().map(|(language, _)| language.as_str()),
            ))
        )
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
            Utc::now(),
        )
    }

    #[cfg(test)]
    fn render_for_cache(&self, width: usize, tool_run_viewport_height: usize) -> TranscriptRender {
        Arc::unwrap_or_clone(self.render_for_cache_at(width, tool_run_viewport_height, Utc::now()))
    }

    /// The draw's transcript layout. It equals a full cache-mode render, but
    /// keeps the previous render's rows up to the first entry that can differ
    /// and redraws only from there. The previous render is reused in place
    /// when nothing else still holds it.
    fn render_for_cache_at(
        &self,
        width: usize,
        tool_run_viewport_height: usize,
        render_time: DateTime<Utc>,
    ) -> Arc<TranscriptRender> {
        let inputs = RenderInputs {
            width,
            tool_run_viewport_height,
            today: Local::now().date_naive(),
            user_label: self.user_label.clone(),
            assistant_label: self.assistant_label.clone(),
            assignment_sender: self.assignment_sender.clone(),
            colors: [
                self.user_label_color,
                self.user_message_color,
                self.assistant_label_color,
                self.assistant_message_color,
            ],
            image_cell: self.image_cell,
            tool_click_behavior: self.tool_click_behavior,
            show_action_done_at: self.show_action_done_at,
        };
        let windows = self.tool_run_windows();
        let running_tool = self.has_running_tool();
        let director_prompt_row = self.director_prompt_row();
        let mut resumes = self.render_resumes.borrow_mut();
        let changed_from = self.order.take_changed_from();
        for resume in resumes.iter_mut() {
            resume.changed_from = resume.changed_from.min(changed_from);
        }
        let mut resume = resumes
            .iter()
            .position(|resume| resume.inputs == inputs)
            .map(|position| resumes.remove(position));
        let start = resume.as_mut().map_or(0, |resume| {
            self.render_resume_start(
                resume,
                &windows,
                running_tool,
                director_prompt_row,
                render_time,
            )
        });
        let (mut render, mut trace) = match resume {
            Some(resume) if start > 0 => {
                let mut render = resume.render;
                let mut trace = resume.trace;
                truncate_render(Arc::make_mut(&mut render), trace.checkpoints[start]);
                trace.truncate(start);
                (render, trace)
            }
            _ => (Arc::default(), RenderTrace::default()),
        };
        self.render_rows(
            Arc::make_mut(&mut render),
            &mut trace,
            start,
            &windows,
            inputs.today,
            width,
            tool_run_viewport_height,
            None,
            None,
            None,
            true,
            None,
            render_time,
        );
        if resumes.len() == 2 {
            resumes.remove(0);
        }
        resumes.push(RenderResume {
            inputs,
            render_time,
            changed_from: usize::MAX,
            running_tool,
            director_prompt_row,
            tool_run_offsets: self.tool_run_offsets.clone(),
            expanded_tool_runs: self.expanded_tool_runs.clone(),
            action_run_expansions: self.action_run_expansions.clone(),
            open_tool_run: self.open_tool_run(&windows),
            windows,
            trace,
            render: Arc::clone(&render),
            #[cfg(test)]
            redrawn_from: start,
        });
        render
    }

    /// The first entry whose rows may differ from `resume`'s render.
    /// The action group still being worked in. It stays open; finished groups
    /// fold to their summary header until clicked. The last group is open
    /// until the next assistant message completes, or while nothing follows it.
    /// Whether a group still contains work that has not finished.
    ///
    /// Covers a tool call that has not reported a result, and a process that
    /// started and is still running. Folding either would hide the only place
    /// that work is visible.
    fn window_has_unfinished_work(&self, window: &ToolRunWindow) -> bool {
        if self.order[window.start..window.end].iter().any(|entry| {
            matches!(
                entry,
                TranscriptEntry::Tool { execution,  .. } if !execution.is_terminal()
            ) || matches!(
                entry,
                TranscriptEntry::Action {
                    execution,
                    ..
                }
             if !execution.is_terminal())
        }) {
            return true;
        }
        self.runtime_processes.values().any(|process| {
            process.running
                && process
                    .tool_index
                    .is_some_and(|index| index >= window.start && index < window.end)
        })
    }

    fn open_tool_run(&self, windows: &[Option<ToolRunWindow>]) -> Option<usize> {
        windows
            .iter()
            .flatten()
            .max_by_key(|window| window.start)
            .filter(|window| {
                window.end >= self.order.len()
                    || (self.active_turn.is_some()
                        && !self.order[window.end..].iter().any(|entry| {
                            matches!(
                                entry,
                                TranscriptEntry::Message {
                                    actor: EventActor::Assistant | EventActor::User,
                                    execution,
                                    ..
                                } if execution.is_terminal()
                            )
                        }))
            })
            .map(|window| window.start)
    }

    fn render_resume_start(
        &self,
        resume: &mut RenderResume,
        windows: &[Option<ToolRunWindow>],
        running_tool: bool,
        director_prompt_row: Option<usize>,
        render_time: DateTime<Utc>,
    ) -> usize {
        let rendered_entries = resume.trace.checkpoints.len() - 1;
        // A tool row reads the kind of the entry after it.
        let mut start = resume.changed_from.saturating_sub(1).min(rendered_entries);
        // Whether a run is boxed depends on entries after it: its length and
        // any bridging agent rows.
        if let Some(index) = windows
            .iter()
            .zip(&resume.windows)
            .position(|(window, rendered)| window != rendered)
        {
            start = start.min(index);
        }
        if running_tool != resume.running_tool
            && let Some(row) = resume.trace.running_tool_rows.first()
        {
            start = start.min(*row);
        }
        // A group folds or unfolds when it stops or starts being the open one.
        let open = self.open_tool_run(windows);
        if open != resume.open_tool_run
            && let Some(run) = open.into_iter().chain(resume.open_tool_run).min()
        {
            start = start.min(run);
        }
        if director_prompt_row != resume.director_prompt_row
            && let Some(row) = director_prompt_row
                .into_iter()
                .chain(resume.director_prompt_row)
                .min()
        {
            start = start.min(row);
        }
        if let Some(run) = self
            .tool_run_offsets
            .keys()
            .chain(resume.tool_run_offsets.keys())
            .filter(|run| self.tool_run_offsets.get(run) != resume.tool_run_offsets.get(run))
            .chain(
                self.expanded_tool_runs
                    .symmetric_difference(&resume.expanded_tool_runs),
            )
            .chain(
                self.action_run_expansions
                    .keys()
                    .chain(resume.action_run_expansions.keys())
                    .filter(|run| {
                        self.action_run_expansions.get(run) != resume.action_run_expansions.get(run)
                    }),
            )
            .min()
        {
            start = start.min(*run);
        }
        let action_windows = self.action_run_windows(windows);
        let mut clock_groups = resume.trace.clock_groups.clone();
        clock_groups.sort_unstable_by_key(|window| (window.start, window.end));
        clock_groups.dedup();
        for window in clock_groups {
            if window.start < start
                && !self.refresh_render_group_clock(
                    &mut resume.render,
                    window,
                    windows,
                    &action_windows,
                    resume.render_time,
                    render_time,
                )
            {
                start = window.start;
            }
        }
        // Ticking a timer must not relayout the unrelated history after it.
        let mut clock_rows = resume.trace.clock_rows.clone();
        clock_rows.sort_unstable();
        clock_rows.dedup();
        for row in clock_rows {
            if row >= start {
                break;
            }
            if !self.refresh_render_clock(
                &mut resume.render,
                row,
                resume.inputs.width,
                resume.render_time,
                render_time,
            ) {
                start = row;
            }
        }
        // A tool run rewrites its rows once its last entry is drawn, so it is
        // redrawn from its header.
        while let Some(run_start) = [windows, resume.windows.as_slice()]
            .into_iter()
            .filter_map(|windows| windows.get(start).copied().flatten())
            .map(|window| window.start)
            .filter(|run_start| *run_start < start)
            .min()
        {
            start = run_start;
        }
        start
    }

    fn refresh_render_group_clock(
        &self,
        render: &mut Arc<TranscriptRender>,
        window: ToolRunWindow,
        outer: &[Option<ToolRunWindow>],
        consecutive: &[Option<ToolRunWindow>],
        cached_time: DateTime<Utc>,
        render_time: DateTime<Utc>,
    ) -> bool {
        let Some(entries) = self.order.get(window.start..window.end) else {
            return false;
        };
        let cached = format_tool_group_elapsed_at(entries, cached_time);
        let current = format_tool_group_elapsed_at(entries, render_time);
        if cached == current {
            return true;
        }
        let (Some(cached), Some(current)) = (cached, current) else {
            return false;
        };
        if cached.width() != current.width() {
            return false;
        }
        let rows = render
            .2
            .iter()
            .filter_map(|(index, row, ..)| {
                (*index == window.start && outer[*index] == Some(window)).then_some(*row)
            })
            .chain(render.8.iter().filter_map(|(index, row, ..)| {
                (*index == window.start && consecutive[*index] == Some(window)).then_some(*row)
            }))
            .collect::<Vec<_>>();
        if rows.iter().any(|row| {
            !render.0[*row]
                .spans
                .iter()
                .any(|span| span.content.ends_with(&cached))
        }) {
            return false;
        }
        let cached = [(window.start, Some(cached))];
        let current = [(window.start, Some(current))];
        let render = Arc::make_mut(render);
        for row in rows {
            refresh_tool_elapsed_line(&mut render.0[row], window.start, &cached, &current);
        }
        true
    }

    /// Updates only timer headers; changed widths or wrapped labels need layout.
    fn refresh_render_clock(
        &self,
        render: &mut Arc<TranscriptRender>,
        index: usize,
        width: usize,
        cached_time: DateTime<Utc>,
        render_time: DateTime<Utc>,
    ) -> bool {
        let cached = render_clock_key(&self.order[index], cached_time);
        let current = render_clock_key(&self.order[index], render_time);
        if cached == current {
            return true;
        }
        let (Some(cached), Some(current)) = (cached, current) else {
            return false;
        };
        if cached.width() != current.width()
            || (matches!(&self.order[index], TranscriptEntry::Tool { .. })
                && width <= current.width().max(8) + 6)
        {
            return false;
        }
        let mut rows = render
            .1
            .iter()
            .chain(&render.4)
            .filter_map(|(entry, start, _)| (*entry == index).then_some(*start))
            .collect::<Vec<_>>();
        rows.sort_unstable();
        rows.dedup();
        if rows.iter().any(|row| {
            !render.0[*row]
                .spans
                .iter()
                .any(|span| span.content.ends_with(&cached))
        }) {
            return false;
        }
        let cached = [(index, Some(cached))];
        let current = [(index, Some(current))];
        let render = Arc::make_mut(render);
        for row in rows {
            refresh_tool_elapsed_line(&mut render.0[row], index, &cached, &current);
        }
        for (entry, elapsed) in &mut render.7 {
            if *entry == index {
                *elapsed = current[0].1.clone();
            }
        }
        true
    }

    #[cfg(test)]
    fn render_tool_for_cache(
        &self,
        index: usize,
        width: usize,
        tool_run_viewport_height: usize,
    ) -> TranscriptRender {
        self.render_tool_for_cache_at(index, width, tool_run_viewport_height, Utc::now())
    }

    fn render_tool_for_cache_at(
        &self,
        index: usize,
        width: usize,
        tool_run_viewport_height: usize,
        render_time: DateTime<Utc>,
    ) -> TranscriptRender {
        self.render_with_tool_run_viewport_mode(
            width,
            tool_run_viewport_height,
            None,
            None,
            None,
            true,
            Some(index),
            render_time,
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
        render_time: DateTime<Utc>,
    ) -> TranscriptRender {
        let mut render = TranscriptRender::default();
        self.render_rows(
            &mut render,
            &mut RenderTrace::default(),
            0,
            &self.tool_run_windows(),
            Local::now().date_naive(),
            width,
            tool_run_viewport_height,
            hovered_tool,
            hovered_message,
            hovered_entry,
            defer_completed_message_backgrounds,
            focused_tool,
            render_time,
        );
        render
    }

    fn subagent_display_text(&self, text: &str) -> String {
        static IDS: OnceLock<Regex> = OnceLock::new();
        let ids = IDS.get_or_init(|| {
            Regex::new(
                r"(?i)(?:session:|participant:)?[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
            )
            .unwrap()
        });
        ids.replace_all(text, |capture: &regex::Captures<'_>| {
            let target = capture[0].rsplit(':').next().unwrap();
            Uuid::parse_str(target)
                .ok()
                .and_then(|id| self.agent_names.get(&id).cloned())
                .unwrap_or_else(|| "agent".to_string())
        })
        .into_owned()
    }

    /// Draws entries from `start` onward into `render`, which must hold exactly
    /// the rows drawn before `start` (a checkpoint that is not inside a run).
    #[allow(clippy::too_many_arguments)]
    fn render_rows(
        &self,
        render: &mut TranscriptRender,
        trace: &mut RenderTrace,
        start: usize,
        tool_run_windows: &[Option<ToolRunWindow>],
        today: NaiveDate,
        width: usize,
        tool_run_viewport_height: usize,
        hovered_tool: Option<usize>,
        hovered_message: Option<usize>,
        hovered_entry: Option<usize>,
        defer_completed_message_backgrounds: bool,
        focused_tool: Option<usize>,
        render_time: DateTime<Utc>,
    ) {
        let today_prefix = today.format("%Y-%m-%d ").to_string();
        let open_window_start = self.open_tool_run(tool_run_windows);
        if focused_tool.is_none() && start == 0 {
            self.prepare_message_markdown_cache(width);
        }
        let (
            lines,
            tool_rows,
            tool_run_rows,
            message_rows,
            entry_rows,
            link_rows,
            selection_rows,
            running_tool_elapsed,
            action_run_rows,
        ) = &mut *render;
        let mut tool_run_starts = HashMap::new();
        let action_windows = self.action_run_windows(tool_run_windows);
        let running_tool = self.has_running_tool();
        if let Some(index) = focused_tool
            && let Some((name, complete)) = self.inspector_heading(index)
        {
            lines.extend(markdown::wrap_markdown_spans(
                &[
                    Span::styled(
                        "  Action details",
                        Style::default()
                            .fg(BORG_ORANGE)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::styled(
                        format!(" · {name} · {}", if complete { "complete" } else { "live" }),
                        Style::default().fg(Color::Gray),
                    ),
                    Span::styled(" · Esc to return", Style::default().fg(Color::DarkGray)),
                ],
                width,
            ));
            lines.push(Line::default());
        }
        let director_prompt_row = self.director_prompt_row();
        for (index, entry) in self.order.iter().enumerate().skip(start) {
            trace.checkpoints.push([
                lines.len(),
                tool_rows.len(),
                tool_run_rows.len(),
                message_rows.len(),
                entry_rows.len(),
                link_rows.len(),
                selection_rows.len(),
                running_tool_elapsed.len(),
                action_run_rows.len(),
            ]);
            if focused_tool.is_some_and(|focused| focused != index) {
                continue;
            }
            let mut display_entry = Cow::Borrowed(entry);
            if let TranscriptEntry::Tool {
                source_name, name, ..
            } = entry
                && (is_subagent_tool(source_name) || is_subagent_tool(name))
                && let TranscriptEntry::Tool {
                    detail,
                    code_view,
                    output_view,
                    ..
                } = display_entry.to_mut()
            {
                *detail = self.subagent_display_text(detail);
                for (_, body) in code_view.iter_mut().chain(output_view.iter_mut()) {
                    *body = self.subagent_display_text(body);
                }
            }
            if let TranscriptEntry::Action {
                kind: TranscriptActionKind::Agent,
                ..
            } = entry
                && let TranscriptEntry::Action { detail, body, .. } = display_entry.to_mut()
            {
                *detail = self.subagent_display_text(detail);
                if let Some(body) = body {
                    *body = self.subagent_display_text(body);
                }
            }
            let entry = display_entry.as_ref();
            let tool_window = focused_tool
                .is_none()
                .then_some(tool_run_windows[index])
                .flatten();
            if let Some(window) = tool_window.filter(|window| index == window.start) {
                let row = lines.len();
                lines.push(Line::from(Span::styled(
                    format!(
                        "    {}",
                        tool_window_summary(
                            &self.order[window.start..window.end],
                            window.total,
                            &today_prefix,
                        )
                    ),
                    Style::default().fg(Color::DarkGray),
                )));
                tool_run_starts.insert(
                    window.start,
                    (
                        row,
                        tool_rows.len(),
                        entry_rows.len(),
                        selection_rows.len(),
                        action_run_rows.len(),
                    ),
                );
            }
            let action_window = focused_tool
                .is_none()
                .then_some(action_windows[index])
                .flatten();
            if let Some(window) = action_window.filter(|window| index == window.start) {
                let summary_entry = self.order[window.start..window.end]
                    .iter()
                    .find(|entry| {
                        matches!(entry, TranscriptEntry::Tool { execution, .. } | TranscriptEntry::Action { execution, .. } if !execution.is_terminal())
                    })
                    .unwrap_or(entry);
                let label = match summary_entry {
                    TranscriptEntry::Tool { name, .. } => name,
                    TranscriptEntry::Action { label, .. } => label,
                    _ => unreachable!(),
                };
                let summary = format!("{label} ({})", window.total);
                let row = lines.len();
                lines.push(tool_window_header(
                    if self.action_run_expanded(window.start) {
                        "  ▾ "
                    } else {
                        "  ▸ "
                    },
                    summary,
                    false,
                    action_label_style(summary_entry),
                ));
                let timer_entry = (window.start..window.end)
                    .find(|index| {
                        matches!(
                            &self.order[*index],
                            TranscriptEntry::Tool {
                                execution,
                                ..
                            } if execution.completed_at().is_none()
                        )
                    })
                    .or_else(|| {
                        (window.start..window.end).rev().find(|index| {
                            matches!(&self.order[*index], TranscriptEntry::Tool { .. })
                        })
                    });
                if let Some(timer) = timer_entry
                    && let TranscriptEntry::Tool { execution, .. } = &self.order[timer]
                {
                    if execution.completed_at().is_none() {
                        trace.clock_rows.push(timer);
                        trace.clock_groups.push(window);
                        running_tool_elapsed.push((
                            timer,
                            format_tool_elapsed_at(
                                execution.started_at(),
                                execution.completed_at(),
                                render_time,
                            ),
                        ));
                    }
                    if let Some(elapsed) = format_tool_group_elapsed_at(
                        &self.order[window.start..window.end],
                        render_time,
                    ) {
                        let header = &mut lines[row];
                        let padding = width.saturating_sub(header.width() + elapsed.width());
                        header.spans.push(Span::styled(
                            format!("{}{}", " ".repeat(padding.max(2)), elapsed),
                            Style::default().fg(Color::DarkGray),
                        ));
                    }
                }
                action_run_rows.push((window.start, row, row + 1, timer_entry));
            }
            let visible_message =
                matches!(entry, TranscriptEntry::Message { .. }) && !message_row_hidden(entry);
            let starts_labeled_group = visible_message
                || matches!(
                    entry,
                    TranscriptEntry::Plan { .. }
                        | TranscriptEntry::Goal { .. }
                        | TranscriptEntry::Info { .. }
                        | TranscriptEntry::Compaction { .. }
                );
            let is_chat_message = matches!(
                entry,
                TranscriptEntry::Message {
                    actor: EventActor::User | EventActor::Assistant,
                    ..
                }
            ) && !message_row_hidden(entry);
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
                    && tool_window.is_none()
                    && lines
                        .last()
                        .is_none_or(|line| !line_is_unstyled_blank(line))
                {
                    lines.push(Line::default());
                }
                lines.len()
            };
            let outer_width = width;
            let width = if action_window.is_some() {
                width.saturating_sub(2)
            } else {
                width
            };
            match entry {
                _ if action_window
                    .is_some_and(|window| !self.action_run_expanded(window.start)) => {}
                TranscriptEntry::Message {
                    actor,
                    text,
                    attachments,
                    model,
                    effort,
                    time,
                    status: _,
                    execution,
                    redirected,
                } => {
                    if message_row_hidden(entry) {
                        continue;
                    }
                    let from_director = director_prompt_row == Some(index);
                    let (label, color) = match actor {
                        EventActor::User if from_director => {
                            (self.assignment_sender.clone(), SUBAGENT_PURPLE)
                        }
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
                    let mut header = if matches!(actor, EventActor::User | EventActor::Assistant) {
                        let (badge_text, badge_background) = message_badge_colors(color);
                        vec![
                            Span::raw("  "),
                            Span::styled("▌", Style::default().fg(color).bg(badge_background)),
                            Span::styled(
                                format!(" {label} "),
                                Style::default()
                                    .fg(badge_text)
                                    .bg(badge_background)
                                    .add_modifier(Modifier::BOLD),
                            ),
                            Span::styled("▐", Style::default().fg(color).bg(badge_background)),
                        ]
                    } else {
                        vec![Span::styled(
                            format!("  {label}"),
                            Style::default().fg(color).add_modifier(Modifier::BOLD),
                        )]
                    };
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
                        EventActor::User if from_director => Some(SUBAGENT_PURPLE),
                        EventActor::User => Some(self.user_message_color),
                        EventActor::Assistant => Some(self.assistant_message_color),
                        _ => None,
                    };
                    let mut message_lines = {
                        let mut cache = self.message_markdown_cache.borrow_mut();
                        #[cfg(test)]
                        let mut missed = false;
                        let rendered = match cache.messages.entry((index, width)) {
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
                                let MarkdownRender {
                                    mut lines,
                                    mut links,
                                } = message_markdown_render(
                                    *actor,
                                    text,
                                    content_width,
                                    text_color,
                                );
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
                    let available = width.saturating_sub(MESSAGE_HORIZONTAL_PADDING).max(1);
                    // A graphics terminal draws the image at the tile pixel
                    // size: the tile takes the transcript width and the rows the
                    // image needs at that width, up to a cap that scales a large
                    // image down. A glyph tile packs two pixels into one cell,
                    // so it stays small - and its caption says the text cannot
                    // be read there.
                    //
                    // A graphics tile also carries a gutter bar down its left
                    // edge: a screenshot of this interface drawn at its own
                    // resolution is otherwise indistinguishable from the live
                    // transcript around it.
                    let gutter = self.image_cell.is_some();
                    let (tile_width, tile_rows) = match self.image_cell {
                        Some(_) => {
                            let tile_width = available.saturating_sub(2).max(1);
                            (tile_width, GLYPH_PREVIEW_TILE_ROWS.min(tile_width))
                        }
                        None => {
                            let tile_width = available.min(GLYPH_PREVIEW_TILE_WIDTH);
                            (tile_width, tile_width.min(GLYPH_PREVIEW_TILE_ROWS))
                        }
                    };
                    let columns = ((available + 2) / (tile_width + 2)).max(1);
                    for group in attachments.chunks(columns) {
                        let mut cache = self.message_markdown_cache.borrow_mut();
                        if cache.previews.len() > 128 {
                            cache.previews.clear();
                        }
                        let previews = group
                            .iter()
                            .map(|(_, path)| {
                                cache
                                    .previews
                                    .entry((path.clone(), tile_width, tile_rows))
                                    .or_insert_with(|| {
                                        match self.image_cell {
                                            Some(cell) => {
                                                if let Some(rows) =
                                                    attachments::graphics_preview_rows(
                                                        path,
                                                        tile_width,
                                                        cell,
                                                        MAX_GRAPHICS_PREVIEW_ROWS,
                                                    )
                                                {
                                                    // The graphics protocol draws
                                                    // over these rows; reserving
                                                    // them is what sizes the image.
                                                    vec![Line::default(); rows]
                                                } else {
                                                    // Unreadable size: a glyph
                                                    // tile at least shows shape.
                                                    attachments::preview(
                                                        path,
                                                        available.min(GLYPH_PREVIEW_TILE_WIDTH),
                                                        GLYPH_PREVIEW_TILE_ROWS,
                                                    )
                                                    .unwrap_or_default()
                                                }
                                            }
                                            None => {
                                                attachments::preview(path, tile_width, tile_rows)
                                                    .unwrap_or_default()
                                            }
                                        }
                                    })
                                    .clone()
                            })
                            .collect::<Vec<_>>();
                        let height = previews.iter().map(Vec::len).max().unwrap_or(0);
                        for row in 0..=height {
                            let mut line = Line::from("  ");
                            for (column, (number, path)) in group.iter().enumerate() {
                                if gutter {
                                    line.spans.push(Span::styled(
                                        "▌ ",
                                        Style::default().fg(Color::LightCyan),
                                    ));
                                }
                                let start = line.width();
                                let tile = if row == height {
                                    Line::from(Span::styled(
                                        truncate_table_cell(
                                            &self.preview_caption(*number, path),
                                            tile_width,
                                        ),
                                        Style::default().fg(Color::LightCyan),
                                    ))
                                } else {
                                    previews[column].get(row).cloned().unwrap_or_default()
                                };
                                let padding = tile_width.saturating_sub(tile.width());
                                line.spans.extend(tile.spans);
                                if let Ok(url) = url::Url::from_file_path(path) {
                                    link_rows.push(LinkRowRange {
                                        row: lines.len(),
                                        start,
                                        end: start + tile_width,
                                        url: url.to_string(),
                                    });
                                }
                                if column + 1 < group.len() {
                                    line.spans.push(Span::raw(" ".repeat(padding + 2)));
                                }
                            }
                            lines.push(line);
                        }
                    }
                    if *actor == EventActor::Assistant && !execution.is_terminal() {
                        trace.running_tool_rows.push(index);
                    }
                    if *actor == EventActor::Assistant && !execution.is_terminal() && !running_tool
                    {
                        lines.push(Line::from(Span::styled(
                            "    ◌ responding",
                            Style::default().fg(Color::Cyan),
                        )));
                    } else if *actor == EventActor::Assistant
                        && (execution.is_interrupted() || *redirected)
                    {
                        lines.push(Line::from(Span::styled(
                            if *redirected {
                                "    ↪ redirected by follow-up"
                            } else {
                                "    ■ user interrupted"
                            },
                            Style::default().fg(Color::Red),
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
                        if !defer_completed_message_backgrounds || !execution.is_terminal() {
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
                    // A sent message waiting behind a running tool is not
                    // complete yet, but its text is final and copyable.
                    if is_chat_message && (execution.is_terminal() || *actor == EventActor::User) {
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
                    let prefix = "  ";
                    let activity_color = if text == USER_INTERRUPT_ACTIVITY {
                        Color::LightRed
                    } else if is_subagent_activity_text(text) {
                        SUBAGENT_PURPLE
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
                    execution,
                    expanded,
                    ..
                } => {
                    let time = if tool_window.is_some() {
                        String::new()
                    } else {
                        format!("{}  ", display_local_time(time, &today_prefix))
                    };
                    let prefix = "  ";
                    let glyph = transcript_action_glyph(execution.state());
                    let color = transcript_action_color(*kind, execution.state());
                    let mut summary = if detail.is_empty() {
                        format!("{time}{glyph} {label}")
                    } else {
                        format!("{time}{glyph} {label:<9}  {detail}")
                    };
                    // A single-line row still says what the message is about.
                    if !self.wrap_action_rows
                        && *kind == TranscriptActionKind::Agent
                        && label == "Peer"
                        && !*expanded
                        && focused_tool.is_none()
                        && let Some(first) = body
                            .as_deref()
                            .and_then(|body| body.lines().find(|line| !line.trim().is_empty()))
                    {
                        summary.push_str(" · ");
                        summary.push_str(first.trim());
                    }
                    let action_start = lines.len();
                    for line in tool_summary_lines(
                        &summary,
                        None,
                        prefix,
                        width.saturating_sub(2),
                        self.wrap_action_rows,
                    ) {
                        lines.push(Line::from(vec![
                            Span::styled(prefix, Style::default().fg(Color::DarkGray)),
                            Span::styled(line, Style::default().fg(color)),
                        ]));
                    }
                    if (*expanded
                        || focused_tool == Some(index)
                        || (self.wrap_action_rows
                            && *kind == TranscriptActionKind::Agent
                            && label == "Peer"))
                        && let Some(body) = body.as_deref().filter(|body| !body.trim().is_empty())
                    {
                        let mut body_lines = wrap_display(
                            body,
                            width.saturating_sub(UnicodeWidthStr::width(prefix) + 6),
                        );
                        if !expanded && focused_tool.is_none() && body_lines.len() > 3 {
                            body_lines.truncate(3);
                            body_lines[2] = "…".into();
                        }
                        for line in body_lines {
                            lines.push(Line::from(vec![
                                Span::styled(
                                    if tool_window.is_some() {
                                        "    │ "
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
                    // Close a run of tool and action rows with the same gap a tool
                    // row leaves, and none between rows of the run.
                    let next_in_run = focused_tool.is_none()
                        && matches!(
                            self.order.get(index + 1),
                            Some(TranscriptEntry::Tool { .. } | TranscriptEntry::Action { .. })
                        );
                    if tool_window.is_none()
                        && !next_in_run
                        && lines
                            .last()
                            .is_none_or(|line| !line_is_unstyled_blank(line))
                    {
                        lines.push(Line::default());
                    }
                }
                TranscriptEntry::Plan {
                    items,
                    previous,
                    time,
                    expanded,
                } => {
                    let time = display_local_time(time, &today_prefix);
                    let done = items
                        .iter()
                        .filter(|item| item.status == PlanItemStatus::Completed)
                        .count();
                    let dropped = removed_plan_items(items, previous);
                    let dropped = if dropped == 0 {
                        String::new()
                    } else {
                        format!(" · {dropped} removed")
                    };
                    lines.push(Line::from(vec![
                        Span::styled(
                            "▌ Plan",
                            Style::default()
                                .fg(TODO_ORANGE)
                                .add_modifier(Modifier::BOLD),
                        ),
                        Span::styled(
                            format!("  {time}  {done}/{} completed{dropped}", items.len()),
                            Style::default().fg(Color::DarkGray),
                        ),
                    ]));
                    // Collapsed, the card is a change log: a long plan whose
                    // newest row is its most important one must not hide that
                    // row behind unchanged leading steps.
                    let collapsed = !*expanded && focused_tool != Some(index);
                    let previous = if self.plan_workspace_revision.is_some() {
                        &[][..]
                    } else {
                        previous.as_slice()
                    };
                    let (display_items, hidden) = plan_card_rows(items, previous, collapsed);
                    for item in display_items {
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
                                    .fg(TODO_ORANGE)
                                    .add_modifier(Modifier::BOLD),
                                Style::default()
                                    .fg(TODO_ORANGE)
                                    .add_modifier(Modifier::BOLD),
                            ),
                            PlanItemStatus::Blocked => (
                                "⊘ blocked ·",
                                Style::default().fg(Color::Yellow),
                                Style::default().fg(Color::Gray),
                            ),
                            PlanItemStatus::AwaitingReview => (
                                "◇ awaiting review ·",
                                Style::default().fg(SUBAGENT_PURPLE),
                                Style::default().fg(Color::Gray),
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
                    if collapsed && hidden > 0 {
                        lines.push(Line::from(Span::styled(
                            format!("    + {hidden} more"),
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
                    if goal.status.is_active() {
                        trace.clock_rows.push(index);
                    }
                    let live_time = goal_live_seconds(goal, render_time);
                    lines.push(Line::from(vec![
                        Span::styled(
                            "▌ Goal",
                            Style::default()
                                .fg(TODO_ORANGE)
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
                    execution,
                    ..
                } => {
                    let time = display_local_time(time, &today_prefix);
                    let expandable =
                        execution.is_terminal() && compaction_has_expandable_detail(summary);
                    let title = if execution.is_terminal() || focused_tool == Some(index) {
                        "Compacted context".to_string()
                    } else {
                        compact_text(summary, 180)
                    };
                    let detail = if execution.is_terminal() && focused_tool != Some(index) {
                        let detail = summary
                            .strip_prefix("Compacted context")
                            .unwrap_or(summary)
                            .trim_start_matches(':')
                            .trim_start();
                        format!("   {}", compact_text(&markdown_plain_text(detail), 162))
                    } else {
                        String::new()
                    };
                    lines.push(Line::from(vec![
                        Span::styled("▌ ", Style::default().fg(BORG_ORANGE)),
                        Span::styled(
                            title,
                            Style::default()
                                .fg(BORG_ORANGE)
                                .add_modifier(Modifier::BOLD),
                        ),
                        Span::styled(detail, Style::default().fg(Color::DarkGray)),
                        Span::styled(format!("  {time}"), Style::default().fg(Color::DarkGray)),
                    ]));
                    if (*expanded || focused_tool == Some(index)) && expandable {
                        let detail = summary
                            .strip_prefix("Compacted context: ")
                            .unwrap_or(summary);
                        for line in
                            markdown_lines(detail, width.saturating_sub(6), Some(Color::Gray))
                        {
                            let mut spans = vec![Span::raw("  │ ")];
                            spans.extend(line.spans);
                            lines.push(Line::from(spans));
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
                    execution,
                    expanded,
                    outcome,
                    ..
                } => {
                    let agent_action = is_subagent_tool(source_name) || is_subagent_tool(name);
                    // Tool and action rows (Watch, Peer) form one uniform run.
                    let next_is_tool = focused_tool.is_none()
                        && matches!(
                            self.order.get(index + 1),
                            Some(TranscriptEntry::Tool { .. } | TranscriptEntry::Action { .. })
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
                    let failed = execution.is_failed();
                    let glyph = if failed {
                        "!"
                    } else if execution.is_interrupted() {
                        "■"
                    } else if execution.is_backgrounded() {
                        "↗"
                    } else if execution.is_terminal()
                        && (!is_instant || is_command_tool(source_name))
                    {
                        tool_kind_marker(name, is_reasoning)
                    } else {
                        "◇"
                    };
                    let gutter_style = if hovered_tool == Some(index) {
                        Style::default().fg(Color::Gray)
                    } else {
                        Style::default().fg(Color::DarkGray)
                    };
                    let style = if failed || execution.is_interrupted() {
                        Style::default().fg(Color::Red)
                    } else if execution.is_backgrounded() {
                        Style::default().fg(USER_LABEL_BLUE)
                    } else {
                        gutter_style
                    };
                    let name_style = action_label_style(entry);
                    let is_edit = is_edit_tool(source_name, name);
                    let lifecycle = if execution.is_interrupted() {
                        Some("user interrupted")
                    } else if execution.is_backgrounded() {
                        Some("Running in background")
                    } else {
                        None
                    };
                    let lifecycle_complete =
                        execution.is_terminal() && !execution.is_backgrounded();
                    let display_name = if is_reasoning {
                        Cow::Borrowed(name.as_str())
                    } else {
                        tool_lifecycle_label(name, lifecycle_complete)
                    };
                    let show_tool_body = execution.is_terminal()
                        || !is_edit
                        || code_view
                            .as_ref()
                            .is_some_and(|(language, _)| is_diff_language(language));
                    let display_detail = detail;
                    // Inside a group the header carries the time.
                    let time = if tool_window.is_some() {
                        String::new()
                    } else {
                        format!("{time}  ")
                    };
                    // The detail view's body already spells out a command or a diff; for
                    // anything else the header is the only place that says what ran.
                    let body_states_action = code_view.as_ref().is_some_and(|(language, _)| {
                        language == "command" || is_diff_language(language)
                    });
                    let mut summary = if display_detail.is_empty()
                        || (focused_tool == Some(index) && body_states_action)
                    {
                        format!("{time}{glyph} {display_name}")
                    } else {
                        format!("{time}{glyph} {display_name:<9}  {display_detail}")
                    };
                    if let Some(lifecycle) = lifecycle {
                        summary.push_str(&format!(" · {lifecycle}"));
                    }
                    let prefix = "  ";
                    let elapsed = format_tool_elapsed_at(
                        execution.started_at(),
                        execution.completed_at(),
                        render_time,
                    );
                    let right = match (outcome.as_deref(), elapsed.as_deref()) {
                        (Some(outcome), Some(elapsed)) => Some(format!("{outcome}  {elapsed}")),
                        (Some(outcome), None) => Some(outcome.to_string()),
                        (None, elapsed) => elapsed.map(str::to_string),
                    };
                    if execution.completed_at().is_none() {
                        trace.clock_rows.push(index);
                    }
                    if !execution.is_terminal() {
                        running_tool_elapsed.push((index, elapsed.clone()));
                    }
                    for (line_index, line) in tool_summary_lines(
                        &summary,
                        right.as_deref(),
                        prefix,
                        width,
                        self.wrap_action_rows,
                    )
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
                            if execution.is_backgrounded() && !is_reasoning {
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
                                execution.is_backgrounded(),
                            );
                            lines.push(Line::from(spans));
                        } else {
                            let mut spans = vec![Span::styled(prefix.to_string(), gutter_style)];
                            extend_tool_lifecycle_spans(
                                &mut spans,
                                &line,
                                lifecycle,
                                style,
                                execution.is_backgrounded(),
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
                        let body_start = lines.len();
                        let body_prefix = if tool_window.is_some() {
                            "    │ "
                        } else {
                            "  │ "
                        };
                        if focused_tool == Some(index) {
                            lines.extend(rendering::tool_detail_lines(
                                language,
                                source,
                                width,
                                body_prefix,
                            ));
                        } else if execution.is_terminal() && !agent_action {
                            let key = (index, width, false, tool_window.is_some());
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
                        if focused_tool == Some(index) && language == "command" {
                            for line in &mut lines[body_start..] {
                                for span in line.spans.iter_mut().skip(1) {
                                    span.style = span.style.fg(USER_LABEL_BLUE);
                                }
                            }
                        }
                    }
                    if show_tool_body
                        && body_expanded
                        && expandable
                        && let Some((language, source)) = output_view
                    {
                        let body_prefix = if tool_window.is_some() {
                            "    │ "
                        } else {
                            "  │ "
                        };
                        if focused_tool == Some(index) {
                            lines.extend(rendering::tool_detail_lines(
                                language,
                                source,
                                width,
                                body_prefix,
                            ));
                        } else if execution.is_terminal() && !agent_action {
                            let key = (index, width, true, tool_window.is_some());
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
                }
            }
            if action_window.is_some() {
                for line in &mut lines[entry_start..] {
                    line.spans.insert(0, Span::raw("  "));
                }
            }
            let width = outer_width;
            if let Some(window) = tool_window
                && index + 1 == window.end
            {
                let (
                    header_row,
                    first_tool_row,
                    first_entry_row,
                    first_selection_row,
                    first_action_run_row,
                ) = tool_run_starts
                    .get(&window.start)
                    .copied()
                    .expect("tool run header was recorded");
                let content_start = header_row + 1;
                let content_end = lines.len();
                let total_lines = content_end.saturating_sub(content_start);
                // A group with work still in flight stays unfolded. Only
                // the latest group is ever "open", so a long command
                // still running would otherwise be folded away as soon
                // as the next action started, taking its live row with it.
                // That also keeps a group open once its live work finishes:
                // it stays expanded until a new message ends the group,
                // rather than collapsing the moment the last running
                // action stops.
                let foldable = total_lines > 0
                    && !self.window_has_unfinished_work(&window)
                    && open_window_start != Some(window.start);
                let folded = foldable && !self.tool_run_expanded(window.start);
                let expandable = foldable || total_lines > tool_run_viewport_height;
                let expanded = expandable && self.tool_run_expanded(window.start);
                let viewport_height = if expanded {
                    total_lines
                } else if folded {
                    0
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
                    .find(|(_, start, end)| *start < viewport_start && *end > viewport_start)
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
                let action_hint = if !folded && expandable && offset > 0 {
                    " · ↑ scroll"
                } else {
                    ""
                };
                lines[header_row] = tool_window_header(
                    if folded {
                        "▸ "
                    } else {
                        TOOL_WINDOW_HEADER_INDENT
                    },
                    format!(
                        "{}{}",
                        tool_window_summary(
                            &self.order[window.start..window.end],
                            window.total,
                            &today_prefix,
                        ),
                        action_hint
                    ),
                    true,
                    Style::default()
                        .fg(Color::White)
                        .add_modifier(Modifier::BOLD),
                );
                if !folded {
                    lines.push(Line::from(Span::styled(
                        if visible_end < total_lines {
                            "  ↓ more"
                        } else {
                            ""
                        },
                        Style::default().fg(Color::DarkGray),
                    )));
                }

                let run_action_rows = action_run_rows.split_off(first_action_run_row);
                for (run, row, _, timer) in run_action_rows {
                    let relative = row.saturating_sub(content_start);
                    if relative >= offset
                        && relative < visible_end
                        && sticky_tool_header
                            .as_ref()
                            .is_none_or(|_| relative != offset)
                    {
                        let row = content_start + relative - offset;
                        action_run_rows.push((run, row, row + 1, timer));
                    }
                }
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
                let mut header_right = (self.show_action_done_at
                    && !self.window_has_unfinished_work(&window))
                .then(|| {
                    self.order[window.start..window.end]
                        .iter()
                        .filter_map(|entry| match entry {
                            TranscriptEntry::Tool { execution, .. } => execution
                                .completed_at()
                                .map(|time| canonical_local_time(time.with_timezone(&Local))),
                            TranscriptEntry::Action { time, .. } => Some(time.clone()),
                            _ => None,
                        })
                        .max()
                })
                .flatten()
                .map(|time| format!("done at {}", display_local_time(&time, &today_prefix)));
                if let Some(timer) = (window.start..window.end).find(|index| {
                    matches!(
                        &self.order[*index],
                        TranscriptEntry::Tool {
                            execution,
                            ..
                        } if execution.completed_at().is_none()
                    )
                }) {
                    trace.clock_rows.push(timer);
                    trace.clock_groups.push(window);
                    running_tool_elapsed
                        .push((timer, render_clock_key(&self.order[timer], render_time)));
                }
                if let Some(elapsed) =
                    format_tool_group_elapsed_at(&self.order[window.start..window.end], render_time)
                {
                    header_right = Some(match &header_right {
                        Some(done_at) => format!("{elapsed} · {done_at}"),
                        None => elapsed,
                    });
                }
                if let Some(right) = header_right {
                    let header = &mut lines[header_row];
                    header.spans = truncate_status_spans(
                        std::mem::take(&mut header.spans),
                        width.saturating_sub(right.width() + 2),
                    );
                    let padding = width.saturating_sub(header.width() + right.width());
                    header.spans.push(Span::styled(
                        format!("{}{}", " ".repeat(padding), right),
                        Style::default().fg(Color::DarkGray),
                    ));
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
                let run_selection_rows = selection_rows.split_off(first_selection_row);
                for range in run_selection_rows {
                    let start = range.start.saturating_sub(content_start);
                    let end = range.end.saturating_sub(content_start);
                    let visible_start = start.max(offset);
                    let visible_selection_end = end.min(visible_end);
                    if visible_start < visible_selection_end {
                        let screen_start = content_start + visible_start - offset;
                        let screen_end = content_start + visible_selection_end - offset;
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
        trace.checkpoints.push(render_lengths(render));
    }

    fn prepare_message_markdown_cache(&self, width: usize) {
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
        let cached_indices = self
            .message_markdown_cache
            .borrow()
            .messages
            .keys()
            .filter_map(|(index, cached_width)| (*cached_width == width).then_some(*index))
            .collect::<HashSet<_>>();
        if cached_indices.len() == message_count {
            return;
        }
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
        let director_prompt_row = self.director_prompt_row();
        let assistant_message_color = self.assistant_message_color;
        let messages = thread::scope(|scope| {
            let cached_indices = &cached_indices;
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
                                let index = chunk_index * chunk_size + entry_index;
                                if cached_indices.contains(&index) {
                                    return None;
                                }
                                let text_color = match actor {
                                    EventActor::User if Some(index) == director_prompt_row => {
                                        Some(SUBAGENT_PURPLE)
                                    }
                                    EventActor::User => Some(user_message_color),
                                    EventActor::Assistant => Some(assistant_message_color),
                                    _ => None,
                                };
                                let MarkdownRender {
                                    mut lines,
                                    mut links,
                                } = message_markdown_render(
                                    *actor,
                                    text,
                                    content_width,
                                    text_color,
                                );
                                for link in &mut links {
                                    link.start += MESSAGE_HORIZONTAL_PADDING;
                                    link.end += MESSAGE_HORIZONTAL_PADDING;
                                }
                                for line in &mut lines {
                                    line.spans.insert(0, Span::raw("  "));
                                }
                                Some(((index, width), MarkdownRender { lines, links }))
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
        cache.messages.extend(messages);
    }

    fn tool_run_windows(&self) -> Vec<Option<ToolRunWindow>> {
        borg_view::grouping::tool_run_windows(&self.order)
    }

    fn action_run_windows(&self, outer: &[Option<ToolRunWindow>]) -> Vec<Option<ToolRunWindow>> {
        borg_view::grouping::action_run_windows(&self.order, outer)
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
                ..
            } => entry.copy_text_owned(),
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

#[cfg(test)]
mod parallel_preparation_tests {
    use super::*;

    #[test]
    fn active_goal_returns_to_the_tail_at_turn_completion_in_live_and_replay() {
        let session_id = Uuid::new_v4();
        let message_id = Uuid::new_v4();
        let goal = SessionGoal::new("Keep checking replay".into(), None);
        let events = [
            SessionEvent::new(
                session_id,
                1,
                SessionEventKind::GoalUpdated { goal: goal.clone() },
            ),
            SessionEvent::new(
                session_id,
                2,
                SessionEventKind::Message {
                    message_id,
                    actor: EventActor::Assistant,
                    text: "One turn finished".into(),
                    attachments: Vec::new(),
                    status: MessageStatus::Complete,
                    delivery: None,
                },
            ),
            SessionEvent::new(session_id, 3, SessionEventKind::GoalUpdated { goal }),
            SessionEvent::new(
                session_id,
                4,
                SessionEventKind::TurnCompleted {
                    message_id,
                    provider_session_id: None,
                    final_text: "One turn finished".into(),
                    error: None,
                },
            ),
        ];
        for replay in [false, true] {
            let mut transcript = Transcript::default();
            for event in &events[..3] {
                if replay {
                    transcript.apply_history(event);
                } else {
                    transcript.apply(event);
                }
            }
            assert!(matches!(&transcript.order[0], TranscriptEntry::Goal { .. }));
            if replay {
                transcript.apply_history(&events[3]);
            } else {
                transcript.apply(&events[3]);
            }
            assert_eq!(transcript.order.len(), 2);
            assert!(matches!(
                &transcript.order[0],
                TranscriptEntry::Message { .. }
            ));
            assert!(
                matches!(&transcript.order[1], TranscriptEntry::Goal { goal, .. }
                if goal.objective == "Keep checking replay")
            );
        }
    }

    #[test]
    fn finalized_reasoning_reuses_a_live_lifecycle_row() {
        let session_id = Uuid::new_v4();
        let mut transcript = Transcript::default();
        transcript.apply(&SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "item/started:reasoning".into(),
                payload: serde_json::json!({"item":{"type":"reasoning","id":"provider-reason"}}),
            },
        ));
        let first = SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ReasoningTextDelta {
                delta: "Checking replay".into(),
            },
        );
        transcript.apply(&first);
        transcript.apply(&SessionEvent::new(
            session_id,
            1,
            SessionEventKind::ReasoningCompleted,
        ));
        transcript.apply(&SessionEvent::new(
            session_id,
            2,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "reasoning_snapshot".into(),
                payload: serde_json::json!({"reasoning_id":"provider-reason","started_at":first.created_at,
                "text":"Checking replay safely","complete":true}),
            },
        ));
        assert_eq!(transcript.order.len(), 1);
        assert!(matches!(&transcript.order[0], TranscriptEntry::Tool {
            code_view: Some((_, text)), execution, ..
        } if execution.is_terminal() && text == "Checking replay safely"));
    }

    #[test]
    fn archived_reasoning_updates_its_original_row_after_intervening_output() {
        let session_id = Uuid::new_v4();
        let reasoning_id = Uuid::new_v4();
        let started_at = Utc::now();
        let snapshot = |sequence, text, complete| {
            SessionEvent::new(
                session_id,
                sequence,
                SessionEventKind::ProviderEvent {
                    provider: CodingProvider::Codex,
                    kind: "reasoning_snapshot".into(),
                    payload: serde_json::json!({"reasoning_id":reasoning_id,
                    "started_at":started_at,"text":text,"complete":complete}),
                },
            )
        };
        let mut transcript = Transcript::default();
        transcript.apply_history(&snapshot(1, "Checking", false));
        transcript.apply_history(&SessionEvent::new(
            session_id,
            2,
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::Assistant,
                text: "I found the issue.".into(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        ));
        transcript.apply_history(&snapshot(3, "Checking the persisted order.", true));
        assert_eq!(transcript.order.len(), 2);
        assert!(matches!(&transcript.order[0], TranscriptEntry::Tool {
            code_view: Some((language, text)), execution, ..
        } if execution.is_terminal() && language == "reasoning" && text == "Checking the persisted order."));
        assert!(matches!(
            &transcript.order[1],
            TranscriptEntry::Message { .. }
        ));
    }

    #[test]
    fn action_group_duration_spans_all_actions_and_live_ticks() {
        let base = Utc::now();
        let at = |millis| base + chrono::Duration::milliseconds(millis);
        for kind in ["reasoning", "exec", "mixed"] {
            for nested in [false, true] {
                for expanded in [false, true] {
                    let mut transcript = Transcript::default();
                    for (start, end) in [(100, Some(2_100)), (5_100, Some(12_100)), (10_100, None)]
                    {
                        transcript.start_reasoning(at(start), "19:38".into());
                        if let Some(end) = end {
                            transcript.finish_reasoning(at(end));
                        }
                    }
                    for index in 0..3 {
                        if kind == "exec" || (kind == "mixed" && index < 2) {
                            let TranscriptEntry::Tool {
                                source_name,
                                name,
                                code_view,
                                ..
                            } = &mut transcript.order[index]
                            else {
                                unreachable!()
                            };
                            *source_name = "exec".into();
                            *name = "Run".into();
                            *code_view = None;
                        }
                    }
                    if kind == "exec" {
                        transcript.active_reasoning = None;
                        transcript.tools.insert("live".into(), 2);
                    }
                    if nested {
                        transcript.order.push(TranscriptEntry::Action {
                            source_session_id: None,
                            kind: TranscriptActionKind::Watch,
                            label: "Watch".into(),
                            detail: "finished".into(),
                            body: None,
                            time: "19:38".into(),
                            execution: Execution::new(Utc::now(), TranscriptActionState::Complete),
                            expanded: false,
                        });
                    }
                    transcript.action_run_expansions.insert(0, expanded);
                    transcript.order.push(TranscriptEntry::Info {
                        title: "History".into(),
                        text: "Unrelated completed output".into(),
                        time: "19:38".into(),
                    });
                    let mut cache = None;
                    for (millis, expected) in [
                        (13_100, "13s"),
                        (14_100, "14s"),
                        (59_100, "59s"),
                        (60_100, "1m 00s"),
                    ] {
                        let now = at(millis);
                        let labels = transcript.running_tool_elapsed_labels_at(now);
                        let render = cached_transcript_render(
                            &transcript,
                            &mut cache,
                            100,
                            30,
                            None,
                            &labels,
                            Local::now().date_naive(),
                            now,
                        );
                        assert!(render.0[0].to_string().ends_with(expected));
                        if millis == 14_100 {
                            assert_eq!(
                                transcript
                                    .render_resumes
                                    .borrow()
                                    .last()
                                    .unwrap()
                                    .redrawn_from,
                                transcript.order.len(),
                                "same-width group ticks must reuse unrelated history",
                            );
                        }
                        let subgroup = render.8[0].1;
                        assert!(
                            render.0[subgroup]
                                .to_string()
                                .ends_with(if kind == "mixed" { "12s" } else { expected })
                        );
                        assert!(!committed_viewport_is_reusable(
                            cache.as_ref().unwrap(),
                            100,
                            30,
                            &labels,
                        ));
                        assert_eq!(
                            *render,
                            transcript.render_with_tool_run_viewport_mode(
                                100, 30, None, None, None, true, None, now,
                            ),
                        );
                    }
                    // Completion delivery can arrive after the clock advanced;
                    // the latest completion timestamp, not the last row, wins.
                    transcript.active_reasoning = None;
                    let TranscriptEntry::Tool {
                        name, execution, ..
                    } = &mut transcript.order[2]
                    else {
                        unreachable!()
                    };
                    *name = if kind == "exec" { "Run" } else { "Reasoned" }.into();
                    execution.transition(ExecutionState::Complete, at(11_100));
                    transcript.show_action_done_at = true;
                    let done_at = format!(
                        "done at {}",
                        at(12_100).with_timezone(&Local).format("%H:%M")
                    );
                    for millis in [61_100, 120_100] {
                        let render = transcript.render_for_cache_at(100, 30, at(millis));
                        assert!(
                            render.0[0]
                                .to_string()
                                .ends_with(&format!("12s · {done_at}"))
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn completed_reasoning_summary_is_static_and_keeps_full_expandable_text() {
        let session_id = Uuid::new_v4();
        let completed_at = Utc::now();
        let source = "**Waiting on external session.**\n**Awaiting session expiry.**\n**Awaiting session expiry.**\n**Running candidate tests.**";
        let mut transcript = Transcript::default();
        transcript.apply(&SessionEvent::new(
            session_id,
            1,
            SessionEventKind::ReasoningDelta {
                text: source.to_string(),
            },
        ));
        let mut completed = SessionEvent::new(session_id, 2, SessionEventKind::ReasoningCompleted);
        completed.created_at = completed_at;
        transcript.apply(&completed);

        let mut first_render = None;
        for seconds in [0, 2, 4, 6, 60] {
            let rendered = transcript
                .render_for_cache_at(100, 30, completed_at + chrono::Duration::seconds(seconds))
                .0
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("\n");
            assert!(rendered.contains("Running candidate tests."), "{rendered}");
            assert!(!rendered.contains("Awaiting session expiry."));
            if let Some(first) = &first_render {
                assert_eq!(&rendered, first, "completed reasoning must not animate");
            } else {
                first_render = Some(rendered);
            }
        }

        transcript.toggle_tool(0);
        let expanded = transcript
            .render_for_cache_at(100, 30, completed_at + chrono::Duration::seconds(8))
            .0
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        assert_eq!(expanded.matches("Awaiting session expiry.").count(), 2);
        assert!(matches!(
            &transcript.order[0],
            TranscriptEntry::Tool {
                code_view: Some((_, body)),
                ..
            } if body == source
        ));
    }

    #[test]
    fn streamed_thinking_keeps_a_bounded_live_summary_and_full_expandable_text() {
        let session_id = Uuid::new_v4();
        let mut transcript = Transcript::default();
        let initial = "a".repeat(100_000);
        transcript.apply(&SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ReasoningTextDelta {
                delta: initial.clone(),
            },
        ));
        transcript.apply(&SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ReasoningTextDelta {
                delta: "recent thinking".to_string(),
            },
        ));

        let Some(TranscriptEntry::Tool {
            detail,
            code_view: Some((_, source)),
            ..
        }) = transcript.order.first()
        else {
            panic!("expected Thinking row");
        };
        assert!(detail.len() <= 164);
        assert!(detail.ends_with("recent thinking"));
        assert_eq!(source, &format!("{initial}recent thinking"));
    }

    #[test]
    fn a_reasoning_row_shows_a_whole_sentence_from_its_start() {
        let finished = "I checked the decals. They project straight down with pitch -90, set before RegisterComponent.";
        assert_eq!(
            reasoning_preview(finished),
            "They project straight down with pitch -90, set before RegisterComponent."
        );
        // Two words of a new sentence say less than the finished one before it.
        assert_eq!(
            reasoning_preview(&format!("{finished} Next I")),
            "They project straight down with pitch -90, set before RegisterComponent."
        );
        assert_eq!(
            reasoning_preview("**Tracing ore cues**\n\nLooking at"),
            "Tracing ore cues"
        );
    }

    #[test]
    fn parallel_action_preparations_promote_without_orphan_rows() {
        let session_id = Uuid::new_v4();
        let mut transcript = Transcript::default();
        for (sequence, tool_call_id, label) in [
            (1, "tool-a", ""),
            (2, "tool-b", ""),
            (3, "tool-a", "read"),
            (4, "tool-b", "run tests"),
        ] {
            transcript.apply(&SessionEvent::new(
                session_id,
                sequence,
                SessionEventKind::ProviderEvent {
                    provider: CodingProvider::Codex,
                    kind: "action/preparing".to_string(),
                    payload: serde_json::json!({
                        "label": label,
                        "tool_call_id": tool_call_id,
                    }),
                },
            ));
        }

        assert_eq!(transcript.order.len(), 2);
        assert!(transcript.order.iter().all(|entry| matches!(
            entry,
            TranscriptEntry::Tool {
                source_name,
                execution,
                ..
            } if !execution.is_terminal() && source_name == "action_preparing"
        )));

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
            transcript.apply(&SessionEvent::new(
                session_id,
                sequence,
                SessionEventKind::ToolStarted {
                    tool_call_id: tool_call_id.to_string(),
                    name: name.to_string(),
                    input,
                    input_ref: None,
                    parent_tool_call_id: None,
                },
            ));
        }

        assert_eq!(transcript.order.len(), 2);
        assert_ne!(transcript.tools["tool-a"], transcript.tools["tool-b"]);
        assert!(transcript.order.iter().all(|entry| matches!(
            entry,
            TranscriptEntry::Tool {
                source_name,
                execution,
                ..
            } if !execution.is_terminal() && source_name != "action_preparing"
        )));
    }
}

fn format_short_age(age: chrono::Duration) -> String {
    let seconds = age.num_seconds().max(0);
    if seconds < 60 {
        format!("{seconds}s")
    } else if seconds < 3600 {
        format!("{}m", seconds / 60)
    } else if seconds < 86_400 {
        format!("{}h", seconds / 3600)
    } else {
        format!("{}d", seconds / 86_400)
    }
}

#[cfg(test)]
mod model_request_tests {
    use super::*;

    #[test]
    fn model_request_survives_summaries_and_replay_does_not_claim_receipt() {
        let mut transcript = Transcript::default();
        let started = Utc::now() - chrono::Duration::seconds(222);
        let event = |kind: &str, payload: serde_json::Value| {
            let mut event = SessionEvent::new(
                Uuid::nil(),
                0,
                SessionEventKind::ProviderEvent {
                    provider: CodingProvider::Codex,
                    kind: kind.into(),
                    payload,
                },
            );
            event.created_at = started;
            event
        };
        let request = event(
            "native_model_request",
            serde_json::json!({"request_id": "request"}),
        );
        transcript.apply_history(&request);
        let summary = event("reasoning_snapshot", serde_json::json!({"text": "summary"}));
        for _ in 0..18 {
            transcript.apply_history(&summary);
        }
        assert_eq!(
            transcript.model_request.as_ref().unwrap().last_received_at,
            None
        );
        assert_eq!(
            transcript.model_request.as_ref().unwrap().output_tokens,
            None
        );
        let received_after = Utc::now();
        transcript.apply(&summary);
        transcript.apply(&request);
        let progress = transcript.model_request.as_ref().unwrap();
        assert_eq!(progress.id, "request");
        assert!(progress.last_received_at.unwrap() >= received_after);
        transcript.apply(&event(
            "native_model_usage",
            serde_json::json!({"request_id": "other", "complete": true}),
        ));
        assert!(transcript.model_request.is_some());
        transcript.apply(&event("native_model_usage", serde_json::json!({"request_id": "request", "complete": false, "usage": {"output_tokens": 7}})));
        assert_eq!(
            transcript.model_request.as_ref().unwrap().output_tokens,
            Some(7)
        );
        transcript.apply(&event(
            "native_model_usage",
            serde_json::json!({"request_id": "request", "complete": true}),
        ));
        assert!(transcript.model_request.is_none());
    }

    #[test]
    fn process_started_before_tool_is_bound_by_exec_result() {
        check_process_result_binding(true);
    }

    #[test]
    fn diff_like_test_stdout_without_changes_does_not_invent_edit() {
        check_process_result_binding(false);
    }

    fn check_process_result_binding(has_changes: bool) {
        let session = Uuid::new_v4();
        let process = Uuid::new_v4();
        let mut transcript = Transcript::default();
        let kinds = [
            SessionEventKind::ToolStarted {
                tool_call_id: "prior".into(),
                name: "read_file".into(),
                input: serde_json::json!({"path": "prior.rs"}),
                input_ref: None,
                parent_tool_call_id: None,
            },
            SessionEventKind::RuntimeProcessStarted {
                process_id: process,
                pid: 42,
                command: "python3 rewrite.py; cargo test".into(),
                cwd: PathBuf::from("/workspace"),
            },
            SessionEventKind::ToolStarted {
                tool_call_id: "edit".into(),
                name: "exec".into(),
                input: serde_json::json!({"cmd": "python3 rewrite.py; cargo test"}),
                input_ref: None,
                parent_tool_call_id: None,
            },
            SessionEventKind::ToolCompleted {
                tool_call_id: "edit".into(),
                output: serde_json::json!({"session_id": process, "running": true}).to_string(),
                output_ref: None,
                is_error: false,
                input: None,
                input_ref: None,
                parent_tool_call_id: None,
            },
            SessionEventKind::RuntimeProcessCompleted {
                process_id: process,
                pid: 42,
                status: borg_remote::RuntimeProcessStatus::TimedOut,
                exit_code: None,
                timed_out: true,
                stdout:
                    "test output\ndiff --git a/unowned.rs b/unowned.rs\n@@ -1 +1 @@\n-old\n+unowned"
                        .into(),
                stderr: String::new(),
                stdout_omitted_bytes: 0,
                stderr_omitted_bytes: 0,
                error: None,
                changes: if has_changes {
                    vec![borg_remote::CommandChange {
                        path: "target.rs".into(),
                        added: 1,
                        removed: 1,
                        diff: "@@ -1 +1 @@\n-old\n+new".into(),
                    }]
                } else {
                    Vec::new()
                },
            },
        ];
        for (sequence, kind) in kinds.into_iter().enumerate() {
            transcript.apply(&SessionEvent::new(session, sequence as u64 + 1, kind));
        }
        assert_eq!(
            transcript.originating_tool_call_id(process).as_deref(),
            Some("edit")
        );
        assert!(!transcript.command_edit_rows.contains_key("prior"));
        if !has_changes {
            assert!(transcript.command_edit_rows.is_empty());
            assert_eq!(transcript.order.len(), 2);
            assert!(matches!(&transcript.order[0], TranscriptEntry::Tool {
                source_name, output_view: None, ..
            } if source_name == "read_file"));
            assert!(matches!(&transcript.order[1], TranscriptEntry::Tool {
                output_view: Some((_, output)), ..
            } if output.contains("+unowned")));
            return;
        }
        let index = transcript.command_edit_rows["edit"];
        assert!(matches!(&transcript.order[index], TranscriptEntry::Tool {
            code_view: Some((language, diff)), ..
        } if language == "diff:rs" && diff.contains("+new") && !diff.contains("test output")));
        assert_eq!(transcript.command_edit_rows.len(), 1);
    }
}
