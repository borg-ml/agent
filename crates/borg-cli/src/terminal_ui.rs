mod attachments;
mod cache_diagnostics;
mod clipboard;
mod markdown;
mod rendering;
mod terminal_input;
#[cfg(test)]
mod tests;

use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::io::{self, Stdout};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::editor_preferences::{TranscriptPreferences, parse_hex_color};
use anyhow::{Context, Result};
use attachments::{AttachmentStore, PasteOutcome};
use borg_remote::{
    ApprovalDecision, CodingProvider, EventActor, GoalStatus, MessageStatus, PermissionMode,
    PlanItem, PlanItemStatus, PromptDelivery, ResponseLanguage, SessionEvent, SessionEventKind,
    SessionGoal, SessionPayloadKind, SessionPayloadRef, SessionState, SessionStatus,
    SubagentActivityKind, SubagentSnapshot, SubagentStatus, ToolPresentationCategory, compact_text,
    is_diff_language, is_subagent_tool, project_tool_presentation, tool_has_rich_ui,
    tool_output_code_view, tool_output_is_backgrounded, web_search_query,
};
#[cfg(test)]
use borg_remote::{tool_call_summary, tool_code_view};
use chrono::{DateTime, Local, NaiveDate, Utc};
use crossterm::cursor::SetCursorStyle;
use crossterm::event::{
    DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture, Event,
    KeyCode, KeyEvent, KeyEventKind, KeyModifiers, KeyboardEnhancementFlags, MouseButton,
    MouseEventKind, PopKeyboardEnhancementFlags, PushKeyboardEnhancementFlags,
};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, SetTitle, disable_raw_mode, enable_raw_mode,
    supports_keyboard_enhancement,
};
use pulldown_cmark::{
    Alignment as MarkdownAlignment, CodeBlockKind, Event as MarkdownEvent, HeadingLevel, Options,
    Parser, Tag, TagEnd,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Position, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use ratatui::{TerminalOptions, Viewport};
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;
use uuid::Uuid;

use self::cache_diagnostics::{CacheDiagnostics, CacheSignature, CacheUsage};
use self::markdown::{markdown_lines, markdown_link_ranges, open_http_link, truncate_table_cell};
use self::terminal_input::TerminalInput;
pub(crate) use self::terminal_input::TerminalInputEvent;
use crate::agent_config::KeybindingConfig;

const INLINE_VIEWPORT_HEIGHT: u16 = 24;
const HORIZONTAL_MARGIN: u16 = 0;
const BORG_ORANGE: Color = Color::Rgb(255, 142, 36);
const BORG_ORANGE_HOVER: Color = Color::Rgb(255, 184, 92);
const RUNNING_STATUS_PEACH: Color = Color::Rgb(255, 132, 112);
const SUBAGENT_PINK: Color = Color::Rgb(255, 105, 180);
const USER_LABEL_BLUE: Color = Color::Rgb(74, 163, 255);
const USER_TEXT: Color = Color::Rgb(198, 228, 255);
const MESSAGE_BG: Color = Color::Rgb(33, 25, 29);
const MESSAGE_HOVER_BG: Color = Color::Rgb(48, 36, 41);
const MESSAGE_HORIZONTAL_PADDING: usize = 2;
const COMMAND_PANEL_BG: Color = Color::Rgb(31, 24, 27);
const DOUBLE_CTRL_C_WINDOW: Duration = Duration::from_secs(1);
const COPY_NOTICE_DURATION: Duration = Duration::from_secs(5);
const NESTED_SCROLL_GESTURE_GAP: Duration = Duration::from_millis(200);
const WHEEL_SCROLL_VIEWPORT_DIVISOR: usize = 6;
const MIN_WHEEL_SCROLL_LINES_PER_EVENT: usize = 3;
const MAX_WHEEL_SCROLL_LINES_PER_EVENT: usize = 12;
const MAX_WHEEL_SCROLL_LINES_PER_FRAME: isize = 8;
const WHEEL_SCROLL_EASING_DIVISOR: usize = 8;
const MIN_NESTED_SCROLL_LINES_PER_FRAME: usize = 2;
const MAX_NESTED_SCROLL_LINES_PER_FRAME: usize = 12;
const MAX_PENDING_WHEEL_SCROLL_LINES: isize = 160;
const TOOL_RUN_BOX_THRESHOLD: usize = 8;
#[cfg(test)]
const DEFAULT_TOOL_RUN_VIEWPORT_HEIGHT: usize = 8;
const MIN_TOOL_RUN_VIEWPORT_HEIGHT: usize = 6;
const MAX_TOOL_RUN_VIEWPORT_HEIGHT: usize = 30;
const TOOL_RUN_CHROME_HEIGHT: usize = 2;
const MIN_SCROLLBAR_THUMB_ROWS: u16 = 5;
const LARGE_PASTE_CHAR_THRESHOLD: usize = 1000;
const SELECTION_AUTOSCROLL_LINES_PER_FRAME: usize = 2;
type RowRange = (usize, usize, usize);
type ToolRunRowRange = (usize, usize, usize, usize);
#[derive(Clone, Debug, PartialEq, Eq)]
struct LinkRowRange {
    row: usize,
    start: usize,
    end: usize,
    url: String,
}
type TranscriptRender = (
    Vec<Line<'static>>,
    Vec<RowRange>,
    Vec<ToolRunRowRange>,
    Vec<RowRange>,
    Vec<RowRange>,
    Vec<LinkRowRange>,
);
type CachedTranscriptRender = (
    usize,
    usize,
    Option<i64>,
    Option<usize>,
    NaiveDate,
    Arc<TranscriptRender>,
);

/// A semantic viewport position that survives transcript reflow.  Tool bodies
/// are special: after they collapse their header is the nearest durable row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TranscriptViewportAnchor {
    entry_index: usize,
    entry_row_offset: usize,
    viewport_row: usize,
    collapsed_tool_header: Option<usize>,
}

#[derive(Clone, Copy)]
enum KeyAction {
    Send,
    Queue,
    Newline,
    Keybindings,
    Interrupt,
    ClearOrExit,
    Exit,
    AttachImage,
    Copy,
    ScrollUp,
    ScrollDown,
    SelectPrevious,
    SelectNext,
    Approve,
    Deny,
}

#[derive(Clone)]
struct KeyChord {
    code: KeyCode,
    modifiers: KeyModifiers,
    label: String,
}

#[derive(Clone)]
struct KeyMap {
    send: Vec<KeyChord>,
    queue: Vec<KeyChord>,
    newline: Vec<KeyChord>,
    keybindings: Vec<KeyChord>,
    interrupt: Vec<KeyChord>,
    clear_or_exit: Vec<KeyChord>,
    exit: Vec<KeyChord>,
    attach_image: Vec<KeyChord>,
    copy: Vec<KeyChord>,
    scroll_up: Vec<KeyChord>,
    scroll_down: Vec<KeyChord>,
    select_previous: Vec<KeyChord>,
    select_next: Vec<KeyChord>,
    approve: Vec<KeyChord>,
    deny: Vec<KeyChord>,
}

impl KeyMap {
    fn from_config(config: &KeybindingConfig) -> Result<Self> {
        Ok(Self {
            send: parse_key_chords(&config.send)?,
            queue: parse_key_chords(&config.queue)?,
            newline: parse_key_chords(&config.newline)?,
            keybindings: parse_key_chords(&config.keybindings)?,
            interrupt: parse_key_chords(&config.interrupt)?,
            clear_or_exit: parse_key_chords(&config.clear_or_exit)?,
            exit: parse_key_chords(&config.exit)?,
            attach_image: parse_key_chords(&config.attach_image)?,
            copy: parse_key_chords(&config.copy)?,
            scroll_up: parse_key_chords(&config.scroll_up)?,
            scroll_down: parse_key_chords(&config.scroll_down)?,
            select_previous: parse_key_chords(&config.select_previous)?,
            select_next: parse_key_chords(&config.select_next)?,
            approve: parse_key_chords(&config.approve)?,
            deny: parse_key_chords(&config.deny)?,
        })
    }

    fn chords(&self, action: KeyAction) -> &[KeyChord] {
        match action {
            KeyAction::Send => &self.send,
            KeyAction::Queue => &self.queue,
            KeyAction::Newline => &self.newline,
            KeyAction::Keybindings => &self.keybindings,
            KeyAction::Interrupt => &self.interrupt,
            KeyAction::ClearOrExit => &self.clear_or_exit,
            KeyAction::Exit => &self.exit,
            KeyAction::AttachImage => &self.attach_image,
            KeyAction::Copy => &self.copy,
            KeyAction::ScrollUp => &self.scroll_up,
            KeyAction::ScrollDown => &self.scroll_down,
            KeyAction::SelectPrevious => &self.select_previous,
            KeyAction::SelectNext => &self.select_next,
            KeyAction::Approve => &self.approve,
            KeyAction::Deny => &self.deny,
        }
    }

    fn matches(&self, action: KeyAction, key: &KeyEvent) -> bool {
        let modifiers =
            key.modifiers & (KeyModifiers::CONTROL | KeyModifiers::ALT | KeyModifiers::SHIFT);
        self.chords(action)
            .iter()
            .any(|chord| key_codes_match(chord.code, key.code) && chord.modifiers == modifiers)
    }

    fn label(&self, action: KeyAction) -> String {
        self.chords(action)
            .iter()
            .map(|chord| chord.label.as_str())
            .collect::<Vec<_>>()
            .join("/")
    }
}

fn key_codes_match(left: KeyCode, right: KeyCode) -> bool {
    match (left, right) {
        (KeyCode::Char(left), KeyCode::Char(right)) => left.eq_ignore_ascii_case(&right),
        _ => left == right,
    }
}

fn parse_key_chords(values: &[String]) -> Result<Vec<KeyChord>> {
    values.iter().map(|value| parse_key_chord(value)).collect()
}

fn parse_key_chord(value: &str) -> Result<KeyChord> {
    let mut modifiers = KeyModifiers::NONE;
    let mut code = None;
    for part in value.split('+') {
        let part = part.trim().to_ascii_lowercase();
        match part.as_str() {
            "ctrl" => modifiers.insert(KeyModifiers::CONTROL),
            "alt" => modifiers.insert(KeyModifiers::ALT),
            "shift" => modifiers.insert(KeyModifiers::SHIFT),
            "enter" => code = Some(KeyCode::Enter),
            "esc" => code = Some(KeyCode::Esc),
            "tab" => code = Some(KeyCode::Tab),
            "backspace" => code = Some(KeyCode::Backspace),
            "delete" => code = Some(KeyCode::Delete),
            "up" => code = Some(KeyCode::Up),
            "down" => code = Some(KeyCode::Down),
            "left" => code = Some(KeyCode::Left),
            "right" => code = Some(KeyCode::Right),
            "pageup" => code = Some(KeyCode::PageUp),
            "pagedown" => code = Some(KeyCode::PageDown),
            "home" => code = Some(KeyCode::Home),
            "end" => code = Some(KeyCode::End),
            "space" => code = Some(KeyCode::Char(' ')),
            character if character.chars().count() == 1 => {
                code = character.chars().next().map(KeyCode::Char);
            }
            _ => anyhow::bail!("unsupported key chord `{value}`"),
        }
    }
    Ok(KeyChord {
        code: code.with_context(|| format!("key chord `{value}` has no key"))?,
        modifiers,
        label: value.to_ascii_lowercase(),
    })
}

#[derive(Clone, Copy)]
struct NestedScrollCapture {
    tool_run_start: usize,
    direction: isize,
    last_event: Instant,
}

struct NestedScrollMotion {
    tool_run_start: usize,
    max_offset: usize,
    motion: ScrollMotion,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PendingPromptProjection {
    message_id: Uuid,
    text: String,
    delivery: PromptDelivery,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct TranscriptPoint {
    row: usize,
    column: usize,
}

#[derive(Clone, Copy, Debug)]
struct TextSelection {
    anchor: TranscriptPoint,
    focus: TranscriptPoint,
    dragging: bool,
    autoscroll: isize,
    pointer: Position,
}

impl TextSelection {
    fn ordered(self) -> (TranscriptPoint, TranscriptPoint) {
        if self.anchor <= self.focus {
            (self.anchor, self.focus)
        } else {
            (self.focus, self.anchor)
        }
    }

    fn is_empty(self) -> bool {
        self.anchor == self.focus
    }
}

const SLASH_COMMANDS: &[(&str, &str)] = &[
    ("/help", "show commands"),
    ("/settings", "view interactive session settings"),
    ("/model", "choose the model"),
    ("/effort", "choose reasoning effort"),
    ("/language", "choose response and drafting language"),
    ("/fast", "toggle provider priority/fast mode"),
    ("/followups", "choose steer current turn or queue next turn"),
    ("/refresh", "choose terminal refresh rate"),
    ("/sleep", "prevent sleep during active turns"),
    ("/expand-edits", "auto-expand edit diffs"),
    ("/expand-tools", "auto-expand other tool details"),
    ("/colors", "view configurable transcript colours"),
    ("/color", "set a transcript colour"),
    ("/usage", "view real Codex weekly limit and session usage"),
    ("/clear", "clear conversation context"),
    ("/compact", "compact the current conversation context"),
    ("/resume", "resume a saved Borg session"),
    ("/goal", "view or update the durable goal"),
    ("/todo", "view or update the durable todo list"),
    ("/todos", "alias for /todo"),
    ("/queue", "run after the current turn"),
    ("/steer", "steer the current Codex turn"),
    ("/interrupt", "interrupt the current turn"),
    ("/stop", "alias for /interrupt"),
    ("/login", "reconnect the current provider"),
    ("/remote", "connect this machine to your Borg account"),
    ("/quit", "end the session"),
    ("/exit", "alias for /quit"),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScreenMode {
    /// Draw in a bounded inline viewport. Shell scrollback and native terminal
    /// selection remain useful, including in recovery consoles.
    Inline,
    /// Full-screen opt-in for users who prefer an application-style viewport.
    Alternate,
}

impl ScreenMode {
    pub fn from_environment() -> Self {
        match std::env::var("BORG_TUI_SCREEN").ok().as_deref() {
            Some("inline") => Self::Inline,
            _ => Self::Alternate,
        }
    }
}

fn rich_terminal_supported(term: Option<&str>, borg_tui: Option<&str>) -> bool {
    if borg_tui.is_some_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "plain" | "off" | "false" | "0"
        )
    }) {
        return false;
    }
    !term.is_some_and(|value| matches!(value.trim(), "" | "dumb" | "unknown"))
}

#[derive(Debug)]
pub enum UiAction {
    None,
    Submit {
        text: String,
        attachments: Vec<PathBuf>,
    },
    Queue {
        message_id: Uuid,
        text: String,
        attachments: Vec<PathBuf>,
    },
    Approve(ApprovalDecision),
    RecallQueuedPrompt,
    Rewind {
        sequence: u64,
        text: String,
        attachments: Vec<PathBuf>,
    },
    SetModel(String),
    SetEffort(String),
    SetResponseLanguage(ResponseLanguage),
    SetFast(bool),
    SetRefreshRate(u64),
    SetPreventSleep(bool),
    SetSteerActive(bool),
    SetAutoExpandEdits(bool),
    SetAutoExpandTools(bool),
    LoadPayloads(Vec<SessionPayloadRef>),
    Interrupt,
    Quit,
}

pub struct BorgTerminal {
    terminal: Terminal<CrosstermBackend<Stdout>>,
    input: TerminalInput,
    mode: ScreenMode,
    transcript: Transcript,
    director_transcript: Option<Box<Transcript>>,
    child_transcripts: HashMap<Uuid, Transcript>,
    focused_child: Option<Uuid>,
    team_switcher_open: bool,
    team_roster_hit_areas: Vec<(Rect, Option<Uuid>)>,
    back_to_director_area: Option<Rect>,
    composer: Composer,
    attachment_store: AttachmentStore,
    keymap: KeyMap,
    cwd: PathBuf,
    status: SessionStatus,
    pending_approval: bool,
    pending_provider_interaction: bool,
    pending_provider_interaction_secret: bool,
    scroll_from_bottom: usize,
    scroll_motion: ScrollMotion,
    scrollbar_area: Option<Rect>,
    scrollbar_thumb_area: Option<Rect>,
    scrollbar_drag_offset: u16,
    transcript_viewport_area: Option<Rect>,
    transcript_scroll_max: usize,
    dragging_scrollbar: bool,
    scrollbar_hovered: bool,
    jump_to_bottom_area: Option<Rect>,
    jump_to_bottom_hovered: bool,
    keybindings_hint_area: Option<Rect>,
    keybindings_hovered: bool,
    tool_hit_areas: Vec<(Rect, usize)>,
    tool_run_hit_areas: Vec<(Rect, usize, usize)>,
    entry_hit_areas: Vec<(Rect, usize)>,
    message_hit_areas: Vec<(Rect, usize)>,
    link_hit_areas: Vec<(Rect, String)>,
    picker_hit_areas: Vec<(Rect, usize)>,
    hovered_tool: Option<usize>,
    hovered_tool_run: Option<(usize, usize)>,
    hovered_entry: Option<usize>,
    hovered_message: Option<usize>,
    hovered_link: Option<String>,
    hovered_picker_option: Option<usize>,
    goal_status_area: Option<Rect>,
    goal_status_hovered: bool,
    agents_status_area: Option<Rect>,
    agents_status_hovered: bool,
    nested_scroll_capture: Option<NestedScrollCapture>,
    nested_scroll_motion: Option<NestedScrollMotion>,
    text_selection: Option<TextSelection>,
    active_since: Option<DateTime<Utc>>,
    notice: Option<String>,
    copy_notice_expires_at: Option<Instant>,
    clipboard_lease: Option<clipboard::ClipboardLease>,
    keyboard_enhancement: bool,
    last_ctrl_c: Option<Instant>,
    queued_prompts: Vec<PendingPromptProjection>,
    replaying_history: bool,
    picker: Option<Picker>,
    keybindings_open: bool,
    slash_selection: usize,
    rewind_targets: Vec<RewindTarget>,
    rewind_primed: bool,
    borging_this_run: bool,
    last_terminal_title: Option<String>,
    transcript_render_cache: Option<CachedTranscriptRender>,
    rendered_transcript_height: usize,
    pending_scroll_anchor_height: Option<usize>,
    pending_transcript_anchor: Option<TranscriptViewportAnchor>,
    event_redraw_needed: bool,
    cursor_blink_started_at: Instant,
    terminal_restored: bool,
}

#[derive(Clone)]
struct RewindTarget {
    sequence: u64,
    text: String,
    attachments: Vec<PathBuf>,
}

struct Picker {
    kind: PickerKind,
    title: &'static str,
    options: Vec<PickerOption>,
    selected: usize,
}

struct PickerOption {
    label: String,
    value: String,
    preview: Option<String>,
    section: Option<String>,
}

impl PickerOption {
    fn new(label: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            value: value.into(),
            preview: None,
            section: None,
        }
    }
}

pub struct ResumeSessionOption {
    pub id: Uuid,
    pub label: String,
    pub preview: String,
    pub current_directory: bool,
}

#[derive(Clone, Copy)]
enum PickerKind {
    Settings,
    Resume,
    Model,
    Effort,
    Language,
    Fast,
    RefreshRate,
    PreventSleep,
    ActiveMessages,
    AutoExpandEdits,
    AutoExpandTools,
    Rewind,
    MessageActions,
}

impl Picker {
    fn new<'a>(
        kind: PickerKind,
        title: &'static str,
        options: impl IntoIterator<Item = &'a str>,
        current: Option<&str>,
    ) -> Self {
        let options = options
            .into_iter()
            .map(|option| PickerOption::new(option, option))
            .collect::<Vec<_>>();
        let selected = current
            .and_then(|current| options.iter().position(|option| option.label == current))
            .unwrap_or(0);
        Self {
            kind,
            title,
            options,
            selected,
        }
    }

    fn previous(&mut self) {
        self.selected = self
            .selected
            .checked_sub(1)
            .unwrap_or(self.options.len() - 1);
    }

    fn next(&mut self) {
        self.selected = (self.selected + 1) % self.options.len();
    }

    fn scroll(&mut self, delta: isize) -> bool {
        let next = self
            .selected
            .saturating_add_signed(delta)
            .min(self.options.len().saturating_sub(1));
        if next == self.selected {
            return false;
        }
        self.selected = next;
        true
    }

    fn selected_value(self) -> String {
        self.options[self.selected].value.clone()
    }

    fn select_number(&mut self, number: char) -> bool {
        let Some(index) = number
            .to_digit(10)
            .and_then(|number| usize::try_from(number).ok())
            .and_then(|number| number.checked_sub(1))
        else {
            return false;
        };
        if index >= self.options.len() {
            return false;
        }
        self.selected = index;
        true
    }

    fn styled_option_rows(&self) -> Vec<(String, Style)> {
        let mut rows = Vec::with_capacity(self.options.len() + 2);
        for (index, option) in self.options.iter().enumerate() {
            if let Some(section) = option.section.as_deref() {
                rows.push((
                    format!("  {}", section.to_ascii_uppercase()),
                    Style::default()
                        .fg(Color::DarkGray)
                        .add_modifier(Modifier::BOLD),
                ));
            }
            let selected = index == self.selected;
            rows.push((
                format!(
                    "  {} {}",
                    if selected { "›" } else { " " },
                    numbered_picker_option(index, &option.label),
                ),
                if selected {
                    Style::default()
                        .fg(BORG_ORANGE)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(Color::Gray)
                },
            ));
        }
        rows
    }

    fn styled_lines(
        &self,
        width: usize,
        preview_label_color: Color,
        preview_message_color: Color,
    ) -> Vec<Line<'static>> {
        if !matches!(self.kind, PickerKind::Resume) || width < 60 {
            return std::iter::once(Line::from(Span::styled(
                format!("> {}", self.title),
                Style::default()
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            )))
            .chain(
                self.styled_option_rows()
                    .into_iter()
                    .map(|(row, style)| Line::from(Span::styled(row, style))),
            )
            .collect();
        }

        self.styled_resume_lines(width, preview_label_color, preview_message_color)
    }

    fn styled_resume_lines(
        &self,
        width: usize,
        preview_label_color: Color,
        preview_message_color: Color,
    ) -> Vec<Line<'static>> {
        let left_width = (width * 2 / 5).clamp(28, 44);
        let left_content_width = left_width.saturating_sub(2);
        let right_width = width.saturating_sub(left_width + 3).max(1);
        let preview = self
            .options
            .get(self.selected)
            .and_then(|option| option.preview.as_deref())
            .unwrap_or("No session preview available");
        let preview_lines = markdown_lines(preview, right_width, Some(preview_message_color));
        let option_rows = self.styled_option_rows();
        let row_count = option_rows.len().max(preview_lines.len());
        let mut lines = vec![Line::from(vec![
            Span::styled(
                format!("> {}", pad_display(self.title, left_content_width)),
                Style::default()
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" │ ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                "Latest response",
                Style::default()
                    .fg(preview_label_color)
                    .add_modifier(Modifier::BOLD),
            ),
        ])];
        for row in 0..row_count {
            let option = option_rows
                .get(row)
                .map(|(option, style)| (truncate_table_cell(option, left_content_width), *style))
                .unwrap_or_else(|| (String::new(), Style::default()));
            let mut spans = vec![
                Span::styled(pad_display(&option.0, left_content_width), option.1),
                Span::styled(" │ ", Style::default().fg(Color::DarkGray)),
            ];
            if let Some(preview) = preview_lines.get(row) {
                spans.extend(preview.spans.clone());
            }
            lines.push(Line::from(spans));
        }
        lines
    }

    #[cfg(test)]
    fn display(&self, width: usize) -> String {
        self.styled_lines(width, BORG_ORANGE, Color::White)
            .into_iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n")
    }
}

fn numbered_picker_option(index: usize, option: &str) -> String {
    if index < 9 {
        format!("{}. {option}", index + 1)
    } else {
        format!("   {option}")
    }
}

fn pad_display(value: &str, width: usize) -> String {
    let used = UnicodeWidthStr::width(value);
    format!("{value}{}", " ".repeat(width.saturating_sub(used)))
}

fn model_picker_options(provider: Option<CodingProvider>, current: Option<&str>) -> Vec<&str> {
    if let Some(catalog) = provider.and_then(CodingProvider::model_catalog) {
        return catalog
            .selectable_models
            .iter()
            .map(|(id, _)| *id)
            .collect();
    }
    match provider {
        Some(CodingProvider::OpenRouter) => {
            let mut options = vec!["moonshotai/kimi-k3"];
            if let Some(current) = current
                && !options.contains(&current)
            {
                options.insert(0, current);
            }
            options
        }
        Some(CodingProvider::OpenAiCompatible | CodingProvider::OpenCode) | None => {
            vec![current.unwrap_or("model-id")]
        }
        Some(CodingProvider::Codex | CodingProvider::Claude | CodingProvider::Kimi) => {
            unreachable!("catalog-backed providers have a model catalog")
        }
    }
}

fn effort_picker_options(provider: Option<CodingProvider>) -> &'static [&'static str] {
    provider
        .and_then(CodingProvider::model_catalog)
        .map(|catalog| catalog.effort_levels)
        .filter(|efforts| !efforts.is_empty())
        .unwrap_or(&borg_provider::CODEX_EFFORT_LEVELS)
}

impl BorgTerminal {
    pub fn fallback_requested() -> bool {
        !rich_terminal_supported(
            std::env::var("TERM").ok().as_deref(),
            std::env::var("BORG_TUI").ok().as_deref(),
        )
    }

    pub fn enter(
        sessions_dir: &Path,
        session_id: Uuid,
        cwd: PathBuf,
        keybindings: &KeybindingConfig,
    ) -> Result<Self> {
        anyhow::ensure!(
            rich_terminal_supported(
                std::env::var("TERM").ok().as_deref(),
                std::env::var("BORG_TUI").ok().as_deref(),
            ),
            "terminal does not support the rich TUI"
        );
        let mode = ScreenMode::from_environment();
        let attachment_store = AttachmentStore::for_session(sessions_dir, session_id)?;
        let keymap = KeyMap::from_config(keybindings)?;
        enable_raw_mode().context("failed to enable terminal raw mode")?;
        let mut stdout = io::stdout();
        let keyboard_enhancement = supports_keyboard_enhancement().unwrap_or(false);
        if keyboard_enhancement
            && let Err(error) = execute!(
                stdout,
                PushKeyboardEnhancementFlags(
                    KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES
                        | KeyboardEnhancementFlags::REPORT_ALL_KEYS_AS_ESCAPE_CODES
                        | KeyboardEnhancementFlags::REPORT_EVENT_TYPES
                )
            )
        {
            let _ = disable_raw_mode();
            return Err(error).context("failed to enable enhanced keyboard input");
        }
        if let Err(error) = execute!(stdout, EnableBracketedPaste) {
            if keyboard_enhancement {
                let _ = execute!(stdout, PopKeyboardEnhancementFlags);
            }
            let _ = disable_raw_mode();
            return Err(error.into());
        }
        if mode == ScreenMode::Alternate
            && let Err(error) = execute!(stdout, EnterAlternateScreen)
        {
            let _ = execute!(stdout, DisableBracketedPaste);
            if keyboard_enhancement {
                let _ = execute!(stdout, PopKeyboardEnhancementFlags);
            }
            let _ = disable_raw_mode();
            return Err(error.into());
        }
        if let Err(error) = execute!(stdout, EnableMouseCapture, SetCursorStyle::BlinkingBar) {
            let _ = execute!(stdout, DisableBracketedPaste);
            if mode == ScreenMode::Alternate {
                let _ = execute!(stdout, LeaveAlternateScreen);
            }
            if keyboard_enhancement {
                let _ = execute!(stdout, PopKeyboardEnhancementFlags);
            }
            let _ = disable_raw_mode();
            return Err(error.into());
        }
        let backend = CrosstermBackend::new(stdout);
        let terminal = match Terminal::with_options(
            backend,
            TerminalOptions {
                viewport: match mode {
                    ScreenMode::Inline => Viewport::Inline(INLINE_VIEWPORT_HEIGHT),
                    ScreenMode::Alternate => Viewport::Fullscreen,
                },
            },
        ) {
            Ok(terminal) => terminal,
            Err(error) => {
                let mut stdout = io::stdout();
                let _ = execute!(stdout, SetCursorStyle::DefaultUserShape);
                let _ = execute!(stdout, DisableBracketedPaste);
                if mode == ScreenMode::Alternate {
                    let _ = execute!(stdout, LeaveAlternateScreen);
                }
                if keyboard_enhancement {
                    let _ = execute!(stdout, PopKeyboardEnhancementFlags);
                }
                let _ = disable_raw_mode();
                return Err(error).context("failed to initialize terminal renderer");
            }
        };
        Ok(Self {
            terminal,
            input: TerminalInput::spawn(),
            mode,
            transcript: Transcript::default(),
            director_transcript: None,
            child_transcripts: HashMap::new(),
            focused_child: None,
            team_switcher_open: false,
            team_roster_hit_areas: Vec::new(),
            back_to_director_area: None,
            composer: Composer::default(),
            attachment_store,
            keymap,
            cwd,
            status: SessionStatus::Starting,
            pending_approval: false,
            pending_provider_interaction: false,
            pending_provider_interaction_secret: false,
            scroll_from_bottom: 0,
            scroll_motion: ScrollMotion::default(),
            scrollbar_area: None,
            scrollbar_thumb_area: None,
            scrollbar_drag_offset: 0,
            transcript_viewport_area: None,
            transcript_scroll_max: 0,
            dragging_scrollbar: false,
            scrollbar_hovered: false,
            jump_to_bottom_area: None,
            jump_to_bottom_hovered: false,
            keybindings_hint_area: None,
            keybindings_hovered: false,
            tool_hit_areas: Vec::new(),
            tool_run_hit_areas: Vec::new(),
            entry_hit_areas: Vec::new(),
            message_hit_areas: Vec::new(),
            link_hit_areas: Vec::new(),
            picker_hit_areas: Vec::new(),
            hovered_tool: None,
            hovered_tool_run: None,
            hovered_entry: None,
            hovered_message: None,
            hovered_link: None,
            hovered_picker_option: None,
            goal_status_area: None,
            goal_status_hovered: false,
            agents_status_area: None,
            agents_status_hovered: false,
            nested_scroll_capture: None,
            nested_scroll_motion: None,
            text_selection: None,
            active_since: None,
            notice: None,
            copy_notice_expires_at: None,
            clipboard_lease: None,
            keyboard_enhancement,
            last_ctrl_c: None,
            queued_prompts: Vec::new(),
            replaying_history: false,
            picker: None,
            keybindings_open: false,
            slash_selection: 0,
            rewind_targets: Vec::new(),
            rewind_primed: false,
            borging_this_run: false,
            last_terminal_title: None,
            transcript_render_cache: None,
            rendered_transcript_height: 0,
            pending_scroll_anchor_height: None,
            pending_transcript_anchor: None,
            event_redraw_needed: false,
            cursor_blink_started_at: Instant::now(),
            terminal_restored: false,
        })
    }

    pub async fn next_event(&mut self) -> Option<io::Result<TerminalInputEvent>> {
        self.input.next_event().await
    }

    pub async fn restart_input(&mut self, notice: impl Into<String>) {
        self.input.shutdown().await;
        self.input = TerminalInput::spawn();
        self.notice = Some(notice.into());
        self.event_redraw_needed = true;
    }

    pub fn handle_external_interrupt(&mut self) {
        self.composer.clear();
        self.notice = Some("Prompt cleared · press Ctrl-C again to exit".to_string());
        self.event_redraw_needed = true;
    }

    pub async fn shutdown(mut self) {
        self.restore_terminal();
        self.input.shutdown().await;
    }

    pub fn seed_history(&mut self, events: &[SessionEvent]) {
        self.transcript.reserve_history(events.len());
        self.rewind_targets.reserve(events.len() / 4);
        self.composer.seed_session_events(events);
        self.replaying_history = true;
        for event in events {
            let _ = self.apply_session_event(event);
        }
        self.replaying_history = false;
    }

    pub fn seed_session_state(&mut self, state: &SessionState) {
        self.transcript.seed_session_state(state);
        if let Some(status) = state.status {
            self.status = status;
        }
        self.pending_approval = state.pending_approval_id.is_some();
        self.pending_provider_interaction = state.pending_provider_interaction_id.is_some();
        self.pending_provider_interaction_secret = state
            .pending_provider_interaction_payload
            .as_ref()
            .is_some_and(provider_interaction_contains_secret);
    }

    pub fn restore_composer(&mut self, text: String, attachments: Vec<PathBuf>) {
        self.composer.restore(text, attachments);
    }

    pub fn project_pending_prompt(
        &mut self,
        message_id: Uuid,
        text: String,
        delivery: PromptDelivery,
    ) {
        push_queued_prompt(&mut self.queued_prompts, message_id, text, delivery);
    }

    pub fn reject_optimistic_prompt(
        &mut self,
        message_id: Uuid,
        text: String,
        attachments: Vec<PathBuf>,
    ) {
        self.queued_prompts
            .retain(|queued| queued.message_id != message_id);
        self.composer.restore(text, attachments);
        self.notice = Some("Could not send the prompt; it was returned to the composer".into());
    }

    pub fn is_launch_screen(&self) -> bool {
        self.transcript.order.is_empty()
            && self.queued_prompts.is_empty()
            && self.composer.text.is_empty()
            && self.composer.attachments.is_empty()
    }

    pub fn has_expiring_notice(&self) -> bool {
        self.copy_notice_expires_at.is_some()
    }

    pub fn has_cache_idle_timer(&self) -> bool {
        self.transcript.active_turn.is_none()
            && self.transcript.cache_diagnostics.needs_idle_timer()
    }

    pub fn has_blinking_cursor(&self) -> bool {
        self.picker.is_none()
    }

    pub fn apply_session_event(&mut self, event: &SessionEvent) -> bool {
        self.record_child_event(event);
        let projection_changed = match &event.kind {
            SessionEventKind::SessionStarted
            | SessionEventKind::ProviderSessionLinked { .. }
            | SessionEventKind::SubagentControl { .. } => false,
            SessionEventKind::ProviderEvent { kind, .. } => is_context_compaction(kind),
            _ => true,
        };
        if let SessionEventKind::StatusChanged { status, .. } = event.kind {
            let was_active = matches!(
                self.status,
                SessionStatus::Starting | SessionStatus::Running
            );
            let is_active = matches!(status, SessionStatus::Starting | SessionStatus::Running);
            if is_active && !was_active {
                self.borging_this_run = borging_for_run(Uuid::new_v4());
            }
            self.status = status;
            if matches!(status, SessionStatus::Starting | SessionStatus::Running) {
                self.active_since.get_or_insert(event.created_at);
            } else {
                self.active_since = None;
            }
        }
        update_queued_prompts(&mut self.queued_prompts, &event.kind);
        match &event.kind {
            SessionEventKind::Message {
                actor: EventActor::User,
                text,
                attachments,
                status: MessageStatus::Complete,
                ..
            } => {
                if self
                    .rewind_targets
                    .last()
                    .is_none_or(|target| target.sequence != event.sequence)
                {
                    self.rewind_targets.push(RewindTarget {
                        sequence: event.sequence,
                        text: text.clone(),
                        attachments: attachments.clone(),
                    });
                }
            }
            SessionEventKind::ApprovalRequested { .. } => self.pending_approval = true,
            SessionEventKind::ApprovalResolved { .. } => self.pending_approval = false,
            SessionEventKind::ProviderInteractionRequested { payload, .. } => {
                self.pending_provider_interaction = true;
                self.pending_provider_interaction_secret =
                    provider_interaction_contains_secret(payload);
            }
            SessionEventKind::ProviderInteractionResolved { .. } => {
                self.pending_provider_interaction = false;
                self.pending_provider_interaction_secret = false;
            }
            SessionEventKind::Error { message } => self.notice = Some(message.clone()),
            SessionEventKind::ContextCleared if !self.replaying_history => {
                self.notice = Some("Conversation context cleared".to_string());
                self.scroll_from_bottom = 0;
                self.hovered_tool = None;
                self.hovered_tool_run = None;
                self.hovered_entry = None;
                self.hovered_message = None;
            }
            SessionEventKind::PromptRecalled {
                text, attachments, ..
            } if !self.replaying_history => {
                self.composer
                    .append_recalled(text.clone(), attachments.clone());
                self.notice = Some("Latest queued prompt returned to composer".to_string());
            }
            _ => {}
        }
        let transcript = self
            .director_transcript
            .as_deref_mut()
            .unwrap_or(&mut self.transcript);
        let transcript_entries_before = transcript.order.len();
        let transcript_changed = session_event_changes_transcript(&event.kind);
        transcript.apply(event);
        let transcript_changed =
            transcript_changed || transcript.order.len() != transcript_entries_before;
        if transcript_changed {
            if (self.scroll_from_bottom > 0 || self.text_selection.is_some())
                && self.pending_scroll_anchor_height.is_none()
            {
                self.pending_scroll_anchor_height = Some(self.rendered_transcript_height);
            }
            self.transcript_render_cache = None;
        }
        if self.scroll_from_bottom == 0 {
            self.transcript.follow_tail = true;
            self.pending_scroll_anchor_height = None;
        }
        projection_changed
    }

    fn record_child_event(&mut self, event: &SessionEvent) {
        let SessionEventKind::SubagentActivity {
            agent,
            event: Some(child_event),
            ..
        } = &event.kind
        else {
            return;
        };
        if self.focused_child == Some(agent.session_id) {
            self.transcript.apply(child_event);
        } else {
            self.child_transcripts
                .entry(agent.session_id)
                .or_default()
                .apply(child_event);
        }
    }

    fn focus_child_transcript(&mut self, child_id: Uuid) {
        if self.focused_child.is_some() {
            return;
        }
        switch_to_child_transcript(
            &mut self.transcript,
            &mut self.director_transcript,
            &mut self.child_transcripts,
            child_id,
        );
        self.focused_child = Some(child_id);
        self.team_switcher_open = false;
        self.reset_transcript_focus();
        self.notice = Some("Viewing child transcript · click Director to return".to_string());
    }

    fn focus_director_transcript(&mut self) {
        let Some(child_id) = self.focused_child.take() else {
            return;
        };
        switch_to_director_transcript(
            &mut self.transcript,
            &mut self.director_transcript,
            &mut self.child_transcripts,
            child_id,
        );
        self.team_switcher_open = false;
        self.reset_transcript_focus();
        self.notice = Some("Viewing director transcript".to_string());
    }

    fn reset_transcript_focus(&mut self) {
        self.scroll_from_bottom = 0;
        self.transcript_render_cache = None;
        self.text_selection = None;
        self.hovered_entry = None;
        self.hovered_message = None;
        self.hovered_tool = None;
        self.hovered_tool_run = None;
    }

    pub fn set_notice(&mut self, notice: impl Into<String>) {
        self.notice = Some(notice.into());
    }

    pub fn hydrate_payload(&mut self, payload: &SessionPayloadRef, bytes: Vec<u8>) -> Result<()> {
        self.transcript.hydrate_payload(payload, bytes)?;
        self.transcript.tool_body_cache.get_mut().lines.clear();
        self.transcript_render_cache = None;
        Ok(())
    }

    pub fn show_goal(&mut self, goal: Option<&SessionGoal>) {
        self.notice = None;
        self.transcript.show_goal(goal);
        self.transcript_render_cache = None;
    }

    pub fn show_plan(&mut self, items: &[PlanItem]) {
        self.notice = None;
        self.transcript.show_plan(items);
        self.transcript_render_cache = None;
    }

    pub fn show_info(&mut self, title: impl Into<String>, text: impl Into<String>) {
        self.notice = None;
        self.transcript.order.push(TranscriptEntry::Info {
            title: title.into(),
            text: text.into(),
            time: canonical_local_time(Local::now()),
        });
        self.transcript_render_cache = None;
    }

    pub fn open_model_picker(&mut self) {
        let provider = self
            .transcript
            .config
            .as_ref()
            .map(|config| config.provider);
        let current = self
            .transcript
            .config
            .as_ref()
            .and_then(|config| config.model.clone());
        let options = model_picker_options(provider, current.as_deref());
        self.picker = Some(Picker::new(
            PickerKind::Model,
            "Choose model",
            options,
            current.as_deref(),
        ));
    }

    pub fn open_settings_picker(&mut self, user_label: &str, assistant_label: &str) {
        let options = vec![
            "Model".to_string(),
            "Reasoning effort".to_string(),
            "Response language".to_string(),
            "Provider fast mode".to_string(),
            "Active messages".to_string(),
            "Refresh rate".to_string(),
            "Prevent sleep".to_string(),
            "Auto-expand edits".to_string(),
            "Auto-expand tools".to_string(),
            "Transcript colours".to_string(),
            format!("User label · {user_label}"),
            format!("Assistant label · {assistant_label}"),
        ];
        let values = [
            "/model",
            "/effort",
            "/language",
            "/fast",
            "/followups",
            "/refresh",
            "/sleep",
            "/expand-edits",
            "/expand-tools",
            "/colors",
            "/user-label",
            "/assistant-label",
        ];
        self.picker = Some(Picker {
            kind: PickerKind::Settings,
            title: "Settings",
            options: options
                .into_iter()
                .zip(values)
                .map(|(label, value)| PickerOption::new(label, value))
                .collect(),
            selected: 0,
        });
    }

    pub fn open_resume_picker(&mut self, sessions: &[ResumeSessionOption]) {
        let mut saw_current_directory = false;
        let mut saw_all_directories = false;
        self.picker = Some(Picker {
            kind: PickerKind::Resume,
            title: "Resume session",
            options: sessions
                .iter()
                .map(|session| {
                    let section = if session.current_directory && !saw_current_directory {
                        saw_current_directory = true;
                        Some("Current directory".to_string())
                    } else if !session.current_directory && !saw_all_directories {
                        saw_all_directories = true;
                        Some("All directories".to_string())
                    } else {
                        None
                    };
                    PickerOption {
                        label: session.label.clone(),
                        value: session.id.to_string(),
                        preview: Some(session.preview.clone()),
                        section,
                    }
                })
                .collect(),
            selected: 0,
        });
    }

    pub fn open_effort_picker(&mut self) {
        let provider = self
            .transcript
            .config
            .as_ref()
            .map(|config| config.provider);
        let current = self
            .transcript
            .config
            .as_ref()
            .and_then(|config| config.effort.clone());
        let options = effort_picker_options(provider);
        self.picker = Some(Picker::new(
            PickerKind::Effort,
            "Choose effort",
            options.iter().copied(),
            current.as_deref(),
        ));
    }

    pub fn open_language_picker(&mut self) {
        let current = self
            .transcript
            .config
            .as_ref()
            .map(|config| config.response_language.code());
        let options = ResponseLanguage::ALL
            .map(|language| format!("{} ({})", language.name(), language.code()));
        self.picker = Some(Picker {
            kind: PickerKind::Language,
            title: "Response and drafting language",
            options: options
                .into_iter()
                .zip(ResponseLanguage::ALL)
                .map(|(label, language)| PickerOption::new(label, language.code()))
                .collect(),
            selected: current
                .and_then(|current| {
                    ResponseLanguage::ALL
                        .iter()
                        .position(|language| language.code() == current)
                })
                .unwrap_or(0),
        });
    }

    pub fn open_fast_picker(&mut self, enabled: bool) {
        self.picker = Some(Picker::new(
            PickerKind::Fast,
            "Provider fast mode",
            ["On", "Off"],
            Some(if enabled { "On" } else { "Off" }),
        ));
    }

    pub fn open_refresh_rate_picker(&mut self, current: u64) {
        let current = current.to_string();
        self.picker = Some(Picker::new(
            PickerKind::RefreshRate,
            "Choose refresh rate",
            ["30", "60", "90", "120", "144", "165", "240"],
            Some(&current),
        ));
    }

    pub fn open_prevent_sleep_picker(&mut self, enabled: bool) {
        self.picker = Some(Picker::new(
            PickerKind::PreventSleep,
            "Prevent sleep during active turns",
            ["On", "Off"],
            Some(if enabled { "On" } else { "Off" }),
        ));
    }

    pub fn open_active_messages_picker(&mut self, steer_active: bool) {
        self.picker = Some(Picker::new(
            PickerKind::ActiveMessages,
            "Messages sent while Codex is working",
            ["Steer current turn", "Queue next turn"],
            Some(if steer_active {
                "Steer current turn"
            } else {
                "Queue next turn"
            }),
        ));
    }

    pub fn open_auto_expand_edits_picker(&mut self) {
        self.picker = Some(Picker::new(
            PickerKind::AutoExpandEdits,
            "Auto-expand edit diffs",
            ["On", "Off"],
            Some(if self.transcript.auto_expand_edits {
                "On"
            } else {
                "Off"
            }),
        ));
    }

    pub fn open_auto_expand_tools_picker(&mut self) {
        self.picker = Some(Picker::new(
            PickerKind::AutoExpandTools,
            "Auto-expand other tool details",
            ["On", "Off"],
            Some(if self.transcript.auto_expand_tools {
                "On"
            } else {
                "Off"
            }),
        ));
    }

    pub fn set_auto_expand_edits(&mut self, enabled: bool) {
        if !enabled {
            self.capture_transcript_anchor_for_collapse();
        }
        self.transcript.set_auto_expand_edits(enabled);
        self.transcript_render_cache = None;
    }

    pub fn set_auto_expand_tools(&mut self, enabled: bool) {
        if !enabled {
            self.capture_transcript_anchor_for_collapse();
        }
        self.transcript.set_auto_expand_tools(enabled);
        self.transcript_render_cache = None;
    }

    pub fn set_transcript_labels(&mut self, user: String, assistant: String) {
        self.transcript.user_label = user;
        self.transcript.assistant_label = assistant;
        self.transcript_render_cache = None;
    }

    pub fn set_transcript_colors(&mut self, preferences: &TranscriptPreferences) {
        self.transcript.user_label_color = terminal_color(&preferences.user_label_color);
        self.transcript.user_message_color = terminal_color(&preferences.user_message_color);
        self.transcript.assistant_label_color = terminal_color(&preferences.assistant_label_color);
        self.transcript.assistant_message_color =
            terminal_color(&preferences.assistant_message_color);
        self.transcript
            .message_markdown_cache
            .get_mut()
            .messages
            .clear();
        self.transcript_render_cache = None;
    }

    fn open_rewind_picker(&mut self) {
        let options = self
            .rewind_targets
            .iter()
            .rev()
            .map(|target| {
                let compact = target.text.split_whitespace().collect::<Vec<_>>().join(" ");
                if compact.chars().count() > 72 {
                    format!("{}…", compact.chars().take(72).collect::<String>())
                } else {
                    compact
                }
            })
            .collect::<Vec<_>>();
        if options.is_empty() {
            self.notice = Some("No previous message to edit".to_string());
            self.rewind_primed = false;
            return;
        }
        self.picker = Some(Picker {
            kind: PickerKind::Rewind,
            title: "Edit a previous message",
            options: options
                .into_iter()
                .map(|option| PickerOption::new(option.clone(), option))
                .collect(),
            selected: 0,
        });
    }

    fn open_entry_actions(&mut self, index: usize) {
        let (title, options) = match self.transcript.order.get(index) {
            Some(TranscriptEntry::Message {
                actor: EventActor::User,
                ..
            }) => ("Message actions", vec!["Revert to here", "Copy message"]),
            Some(TranscriptEntry::Message {
                actor: EventActor::Assistant,
                ..
            }) => ("Message actions", vec!["Copy response"]),
            Some(TranscriptEntry::Goal { .. }) => ("Goal actions", vec!["Copy goal"]),
            Some(TranscriptEntry::Plan { .. }) => ("Plan actions", vec!["Copy todo list"]),
            Some(TranscriptEntry::Info { .. }) => ("Card actions", vec!["Copy details"]),
            _ => return,
        };
        self.transcript.selected = Some(index);
        self.picker = Some(Picker::new(
            PickerKind::MessageActions,
            title,
            options,
            None,
        ));
    }

    pub fn handle_event(&mut self, input: TerminalInputEvent) -> Result<UiAction> {
        let TerminalInputEvent {
            event,
            scroll_repetitions,
        } = input;
        if matches!(&event, Event::Paste(_))
            || matches!(&event, Event::Key(key) if key.kind != KeyEventKind::Release)
        {
            self.cursor_blink_started_at = Instant::now();
        }
        self.event_redraw_needed = true;
        match event {
            Event::Resize(width, height) => {
                let area = Rect::new(0, 0, width, height);
                if self.terminal.size()? == area.into() {
                    self.event_redraw_needed = false;
                } else {
                    self.terminal.resize(area)?;
                }
                Ok(UiAction::None)
            }
            Event::Paste(value) => {
                self.last_ctrl_c = None;
                let value = normalize_terminal_capture_paste(&value);
                let PasteOutcome { text, attachments } =
                    self.attachment_store.stage_paste(&value, &self.cwd)?;
                let pasted_text_label = if text.chars().count() > LARGE_PASTE_CHAR_THRESHOLD {
                    Some(self.composer.insert_pasted_text(text))
                } else {
                    self.composer.insert(&text);
                    None
                };
                for path in attachments {
                    self.composer.insert_attachment(path);
                }
                self.notice = self
                    .composer
                    .attachments
                    .last()
                    .map(|attachment| format!("Attached {}", attachment.label))
                    .or_else(|| pasted_text_label.map(|label| format!("Added {label}")));
                Ok(UiAction::None)
            }
            Event::Mouse(mouse) => {
                self.last_ctrl_c = None;
                let pointer = Position::new(mouse.column, mouse.row);
                self.hovered_tool = self
                    .tool_hit_areas
                    .iter()
                    .find_map(|(area, index)| area.contains(pointer).then_some(*index));
                self.hovered_tool_run =
                    self.tool_run_hit_areas
                        .iter()
                        .find_map(|(area, start, max_offset)| {
                            area.contains(pointer).then_some((*start, *max_offset))
                        });
                self.hovered_entry = self
                    .entry_hit_areas
                    .iter()
                    .find_map(|(area, index)| area.contains(pointer).then_some(*index));
                self.hovered_message = self
                    .message_hit_areas
                    .iter()
                    .find_map(|(area, index)| area.contains(pointer).then_some(*index));
                self.hovered_link = self
                    .link_hit_areas
                    .iter()
                    .find_map(|(area, url)| area.contains(pointer).then(|| url.clone()));
                self.hovered_picker_option = self
                    .picker_hit_areas
                    .iter()
                    .find_map(|(area, index)| area.contains(pointer).then_some(*index));
                self.goal_status_hovered = self
                    .goal_status_area
                    .is_some_and(|area| area.contains(pointer));
                self.agents_status_hovered = self
                    .agents_status_area
                    .is_some_and(|area| area.contains(pointer));
                self.scrollbar_hovered = self
                    .scrollbar_area
                    .is_some_and(|area| area.contains(pointer));
                self.jump_to_bottom_hovered = self
                    .jump_to_bottom_area
                    .is_some_and(|area| area.contains(pointer));
                self.keybindings_hovered = self
                    .keybindings_hint_area
                    .is_some_and(|area| area.contains(pointer));
                if matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left)) {
                    if self
                        .back_to_director_area
                        .is_some_and(|area| area.contains(pointer))
                    {
                        self.focus_director_transcript();
                        return Ok(UiAction::None);
                    }
                    if let Some((_, child_id)) = self
                        .team_roster_hit_areas
                        .iter()
                        .find(|(area, _)| area.contains(pointer))
                    {
                        if let Some(child_id) = child_id {
                            self.focus_child_transcript(*child_id);
                        } else {
                            self.focus_director_transcript();
                        }
                        return Ok(UiAction::None);
                    }
                    if self
                        .agents_status_area
                        .is_some_and(|area| area.contains(pointer))
                    {
                        self.team_switcher_open = !self.team_switcher_open;
                        return Ok(UiAction::None);
                    }
                }
                if let Some(picker) = self.picker.as_mut() {
                    let consumed = match mouse.kind {
                        MouseEventKind::ScrollUp => picker.scroll(-(scroll_repetitions as isize)),
                        MouseEventKind::ScrollDown => picker.scroll(scroll_repetitions as isize),
                        _ => false,
                    };
                    if consumed {
                        return Ok(UiAction::None);
                    }
                }
                if matches!(
                    self.picker.as_ref().map(|picker| picker.kind),
                    Some(PickerKind::MessageActions)
                ) {
                    if matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left)) {
                        if let Some(option) = self.hovered_picker_option {
                            self.picker.as_mut().expect("checked above").selected = option;
                            return Ok(self.run_selected_message_action());
                        }
                        self.picker = None;
                        self.transcript.selected = None;
                    }
                    if !matches!(
                        mouse.kind,
                        MouseEventKind::ScrollUp | MouseEventKind::ScrollDown
                    ) {
                        return Ok(UiAction::None);
                    }
                }
                let hovered_tool_run = self
                    .tool_run_hit_areas
                    .iter()
                    .find_map(|(area, start, max_offset)| {
                        area.contains(pointer)
                            .then_some((*start, *max_offset, area.height))
                    })
                    .or_else(|| {
                        self.hovered_tool
                            .and_then(|index| self.transcript.tool_run_start_containing(index))
                            .and_then(|start| {
                                self.tool_run_hit_areas.iter().find_map(
                                    |(area, candidate, max_offset)| {
                                        (*candidate == start).then_some((
                                            start,
                                            *max_offset,
                                            area.height,
                                        ))
                                    },
                                )
                            })
                    });
                if matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left))
                    && !mouse.modifiers.contains(KeyModifiers::SHIFT)
                {
                    self.text_selection = None;
                }
                match mouse.kind {
                    MouseEventKind::Down(MouseButton::Left)
                        if self.scrollbar_area.is_some_and(|area| {
                            area.contains(Position::new(mouse.column, mouse.row))
                        }) =>
                    {
                        self.cancel_scroll_motion();
                        self.dragging_scrollbar = true;
                        self.scrollbar_drag_offset = self
                            .scrollbar_thumb_area
                            .filter(|thumb| thumb.contains(Position::new(mouse.column, mouse.row)))
                            .map_or_else(
                                || {
                                    self.scrollbar_thumb_area
                                        .map_or(0, |thumb| thumb.height.saturating_sub(1) / 2)
                                },
                                |thumb| mouse.row.saturating_sub(thumb.y),
                            );
                        self.scroll_to_scrollbar_row(mouse.row);
                    }
                    MouseEventKind::Down(MouseButton::Left)
                        if self
                            .jump_to_bottom_area
                            .is_some_and(|area| area.contains(pointer)) =>
                    {
                        self.cancel_scroll_motion();
                        self.scroll_from_bottom = 0;
                        self.text_selection = None;
                        self.transcript.follow_tail = true;
                    }
                    MouseEventKind::Down(MouseButton::Left)
                        if mouse.modifiers.contains(KeyModifiers::SHIFT) =>
                    {
                        if let Some(point) = self.transcript_point_at(pointer) {
                            self.text_selection = Some(TextSelection {
                                anchor: point,
                                focus: point,
                                dragging: true,
                                autoscroll: 0,
                                pointer,
                            });
                        }
                        self.pending_scroll_anchor_height = None;
                    }
                    MouseEventKind::Down(MouseButton::Left) if self.hovered_link.is_some() => {
                        if let Err(error) =
                            open_http_link(self.hovered_link.as_deref().expect("checked above"))
                        {
                            self.notice = Some(format!("Could not open link: {error}"));
                        }
                    }
                    MouseEventKind::Down(MouseButton::Left) if self.hovered_tool.is_some() => {
                        self.nested_scroll_motion = None;
                        let tool_index = self.hovered_tool.expect("checked above");
                        if self.transcript.tool_is_expanded(tool_index) {
                            self.capture_transcript_anchor_for_collapse();
                        }
                        if let Some((start, max_offset, _)) = hovered_tool_run {
                            self.transcript.anchor_tool_run(start, max_offset);
                        }
                        let payloads = self.transcript.toggle_tool(tool_index);
                        self.transcript_render_cache = None;
                        if !payloads.is_empty() {
                            return Ok(UiAction::LoadPayloads(payloads));
                        }
                    }
                    MouseEventKind::Down(MouseButton::Left) if self.hovered_message.is_some() => {
                        self.open_entry_actions(self.hovered_message.expect("checked above"));
                    }
                    MouseEventKind::Down(MouseButton::Left) if self.hovered_entry.is_some() => {
                        self.open_entry_actions(self.hovered_entry.expect("checked above"));
                    }
                    MouseEventKind::Down(MouseButton::Left) => {
                        self.transcript.selected = None;
                    }
                    MouseEventKind::Drag(MouseButton::Left) if self.dragging_scrollbar => {
                        self.cancel_scroll_motion();
                        self.scroll_to_scrollbar_row(mouse.row);
                    }
                    MouseEventKind::Drag(MouseButton::Left)
                        if self
                            .text_selection
                            .is_some_and(|selection| selection.dragging) =>
                    {
                        self.pending_scroll_anchor_height = None;
                        self.update_text_selection_drag(pointer);
                    }
                    MouseEventKind::Up(MouseButton::Left) => {
                        self.dragging_scrollbar = false;
                        if let Some(selection) = self.text_selection.as_mut() {
                            selection.dragging = false;
                            selection.autoscroll = 0;
                        }
                    }
                    MouseEventKind::ScrollUp => {
                        let consumed = if let Some((start, max_offset, viewport_height)) =
                            hovered_tool_run
                        {
                            let can_move = self.transcript.tool_run_offset(start, max_offset) > 0;
                            if can_move {
                                self.queue_nested_wheel_scroll(
                                    start,
                                    max_offset,
                                    -wheel_scroll_lines(viewport_height)
                                        * scroll_repetitions as isize,
                                );
                            }
                            nested_scroll_consumed(
                                &mut self.nested_scroll_capture,
                                start,
                                -1,
                                can_move,
                                Instant::now(),
                            )
                        } else {
                            self.nested_scroll_capture = None;
                            false
                        };
                        if !consumed {
                            let viewport_height =
                                self.transcript_viewport_area.map_or(1, |area| area.height);
                            self.queue_wheel_scroll(
                                wheel_scroll_lines(viewport_height) * scroll_repetitions as isize,
                            );
                        }
                    }
                    MouseEventKind::ScrollDown => {
                        let consumed =
                            if let Some((start, max_offset, viewport_height)) = hovered_tool_run {
                                let can_move =
                                    self.transcript.tool_run_offset(start, max_offset) < max_offset;
                                if can_move {
                                    self.queue_nested_wheel_scroll(
                                        start,
                                        max_offset,
                                        wheel_scroll_lines(viewport_height)
                                            * scroll_repetitions as isize,
                                    );
                                }
                                nested_scroll_consumed(
                                    &mut self.nested_scroll_capture,
                                    start,
                                    1,
                                    can_move,
                                    Instant::now(),
                                )
                            } else {
                                self.nested_scroll_capture = None;
                                false
                            };
                        if !consumed {
                            let viewport_height =
                                self.transcript_viewport_area.map_or(1, |area| area.height);
                            self.queue_wheel_scroll(
                                -wheel_scroll_lines(viewport_height) * scroll_repetitions as isize,
                            );
                        }
                    }
                    _ => {}
                }
                Ok(UiAction::None)
            }
            Event::Key(key) if key.kind != KeyEventKind::Release => self.handle_key(key),
            _ => {
                self.event_redraw_needed = false;
                Ok(UiAction::None)
            }
        }
    }

    pub fn take_event_redraw_needed(&mut self) -> bool {
        std::mem::take(&mut self.event_redraw_needed)
    }

    pub fn advance_scroll_frame(&mut self) {
        let nested_update = self.nested_scroll_motion.as_mut().map(|nested| {
            let current = self
                .transcript
                .tool_run_offset(nested.tool_run_start, nested.max_offset);
            let next = nested.motion.advance_with_limits(
                current,
                nested.max_offset,
                MIN_NESTED_SCROLL_LINES_PER_FRAME,
                MAX_NESTED_SCROLL_LINES_PER_FRAME,
            );
            (nested.tool_run_start, nested.max_offset, current, next)
        });
        if let Some((start, max_offset, current, next)) = nested_update
            && next != current
        {
            let delta = if next >= current {
                isize::try_from(next - current).unwrap_or(isize::MAX)
            } else {
                -isize::try_from(current - next).unwrap_or(isize::MAX)
            };
            self.transcript.scroll_tool_run(start, max_offset, delta);
            self.transcript_render_cache = None;
        }
        if self
            .nested_scroll_motion
            .as_ref()
            .is_some_and(|nested| !nested.motion.is_active())
        {
            self.nested_scroll_motion = None;
        }
        self.scroll_from_bottom = self
            .scroll_motion
            .advance(self.scroll_from_bottom, self.transcript_scroll_max);
        let selection_autoscroll = self
            .text_selection
            .filter(|selection| selection.dragging)
            .map_or(0, |selection| selection.autoscroll);
        if selection_autoscroll > 0 {
            self.scroll_from_bottom = self
                .scroll_from_bottom
                .saturating_add(SELECTION_AUTOSCROLL_LINES_PER_FRAME)
                .min(self.transcript_scroll_max);
        } else if selection_autoscroll < 0 {
            self.scroll_from_bottom = self
                .scroll_from_bottom
                .saturating_sub(SELECTION_AUTOSCROLL_LINES_PER_FRAME);
        }
        if selection_autoscroll != 0
            && let Some(pointer) = self.text_selection.map(|selection| selection.pointer)
        {
            self.update_text_selection_focus(pointer);
        }
    }

    pub fn has_pending_scroll_frame(&self) -> bool {
        self.scroll_motion.is_active()
            || self
                .nested_scroll_motion
                .as_ref()
                .is_some_and(|nested| nested.motion.is_active())
            || self
                .text_selection
                .is_some_and(|selection| selection.dragging && selection.autoscroll != 0)
    }

    fn queue_wheel_scroll(&mut self, lines: isize) {
        self.nested_scroll_motion = None;
        self.scroll_motion.push(lines);
    }

    fn queue_nested_wheel_scroll(&mut self, start: usize, max_offset: usize, lines: isize) {
        self.scroll_motion.cancel();
        let nested = self
            .nested_scroll_motion
            .get_or_insert_with(|| NestedScrollMotion {
                tool_run_start: start,
                max_offset,
                motion: ScrollMotion::default(),
            });
        if nested.tool_run_start != start {
            *nested = NestedScrollMotion {
                tool_run_start: start,
                max_offset,
                motion: ScrollMotion::default(),
            };
        } else {
            nested.max_offset = max_offset;
        }
        nested.motion.push(lines);
    }

    fn cancel_scroll_motion(&mut self) {
        self.scroll_motion.cancel();
        self.nested_scroll_motion = None;
    }

    fn transcript_point_at(&self, pointer: Position) -> Option<TranscriptPoint> {
        let area = self.transcript_viewport_area?;
        area.contains(pointer)
            .then(|| self.transcript_point_for_pointer(area, pointer))
    }

    fn transcript_point_for_pointer(&self, area: Rect, pointer: Position) -> TranscriptPoint {
        let scroll_start = self
            .transcript_scroll_max
            .saturating_sub(self.scroll_from_bottom.min(self.transcript_scroll_max));
        let viewport_row = pointer
            .y
            .saturating_sub(area.y)
            .min(area.height.saturating_sub(1));
        let column = pointer
            .x
            .saturating_sub(area.x)
            .min(area.width.saturating_sub(1));
        TranscriptPoint {
            row: scroll_start.saturating_add(usize::from(viewport_row)),
            column: usize::from(column),
        }
    }

    fn update_text_selection_drag(&mut self, pointer: Position) {
        let Some(area) = self.transcript_viewport_area else {
            return;
        };
        let autoscroll = if pointer.y <= area.y {
            1
        } else if pointer.y >= area.bottom().saturating_sub(1) {
            -1
        } else {
            0
        };
        if let Some(selection) = self.text_selection.as_mut() {
            selection.pointer = pointer;
            selection.autoscroll = autoscroll;
        }
        self.update_text_selection_focus(pointer);
    }

    fn update_text_selection_focus(&mut self, pointer: Position) {
        let Some(area) = self.transcript_viewport_area else {
            return;
        };
        let clamped = Position::new(
            pointer.x.clamp(area.x, area.right().saturating_sub(1)),
            pointer.y.clamp(area.y, area.bottom().saturating_sub(1)),
        );
        let point = self.transcript_point_for_pointer(area, clamped);
        if let Some(selection) = self.text_selection.as_mut() {
            selection.focus = point;
        }
    }

    fn copy_text_selection(&mut self) -> bool {
        let Some(selection) = self
            .text_selection
            .filter(|selection| !selection.is_empty())
        else {
            return false;
        };
        let Some((_, _, _, _, _, render)) = self.transcript_render_cache.as_ref() else {
            return false;
        };
        let Some(text) = selected_transcript_text(&render.0, selection) else {
            return false;
        };
        match clipboard::copy(&text) {
            Ok(lease) => {
                self.clipboard_lease = lease;
                self.show_copy_notice("✓ Copied selection to clipboard");
            }
            Err(error) => self.notice = Some(format!("Copy failed: {error}")),
        }
        true
    }

    fn scroll_to_scrollbar_row(&mut self, row: u16) {
        let (Some(area), Some(thumb)) = (self.scrollbar_area, self.scrollbar_thumb_area) else {
            return;
        };
        let pointer_offset = row
            .saturating_sub(area.y)
            .min(area.height.saturating_sub(1));
        let thumb_travel = area.height.saturating_sub(thumb.height);
        let thumb_top = pointer_offset
            .saturating_sub(self.scrollbar_drag_offset)
            .min(thumb_travel);
        let scroll_from_top = if thumb_travel == 0 {
            0
        } else {
            self.transcript_scroll_max
                .saturating_mul(usize::from(thumb_top))
                / usize::from(thumb_travel)
        };
        self.scroll_from_bottom = self.transcript_scroll_max.saturating_sub(scroll_from_top);
    }

    fn capture_transcript_anchor_for_collapse(&mut self) {
        if self.scroll_from_bottom == 0 || self.pending_transcript_anchor.is_some() {
            return;
        }
        let Some(area) = self.transcript_viewport_area else {
            return;
        };
        let Some((_, _, _, _, _, render)) = self.transcript_render_cache.as_ref() else {
            return;
        };
        self.pending_transcript_anchor = transcript_viewport_anchor(
            &render.1,
            &render.4,
            self.transcript_scroll_max,
            self.scroll_from_bottom,
            usize::from(area.height),
            true,
        );
        if self.pending_transcript_anchor.is_some() {
            self.pending_scroll_anchor_height = None;
        }
    }

    fn copy_transcript_entry(&mut self, index: usize) {
        let Some(text) = self
            .transcript
            .order
            .get(index)
            .and_then(TranscriptEntry::copy_text_owned)
        else {
            return;
        };
        match clipboard::copy(&text) {
            Ok(lease) => {
                self.clipboard_lease = lease;
                self.show_copy_notice("✓ Copied to clipboard");
            }
            Err(error) => self.notice = Some(format!("Copy failed: {error}")),
        }
    }

    fn show_copy_notice(&mut self, notice: impl Into<String>) {
        self.notice = Some(notice.into());
        self.copy_notice_expires_at = Some(Instant::now() + COPY_NOTICE_DURATION);
    }

    fn rewind_action_for_output(&mut self, index: usize) -> UiAction {
        let user_message_count = self
            .transcript
            .order
            .iter()
            .take(index.saturating_add(1))
            .filter(|entry| {
                matches!(
                    entry,
                    TranscriptEntry::Message {
                        actor: EventActor::User,
                        status: MessageStatus::Complete,
                        ..
                    }
                )
            })
            .count();
        let Some(target) = user_message_count
            .checked_sub(1)
            .and_then(|target| self.rewind_targets.get(target))
            .cloned()
        else {
            self.notice = Some("No user message precedes this response".to_string());
            return UiAction::None;
        };
        UiAction::Rewind {
            sequence: target.sequence,
            text: target.text,
            attachments: target.attachments,
        }
    }

    fn run_selected_message_action(&mut self) -> UiAction {
        let Some(index) = self.transcript.selected else {
            self.picker = None;
            return UiAction::None;
        };
        let selected = self.picker.take().map(Picker::selected_value);
        self.transcript.selected = None;
        match selected.as_deref() {
            Some("Revert to here") => self.rewind_action_for_output(index),
            Some(selected) if selected.starts_with("Copy ") => {
                self.copy_transcript_entry(index);
                UiAction::None
            }
            _ => UiAction::None,
        }
    }

    fn run_selected_picker(&mut self) -> Result<UiAction> {
        let picker = self.picker.take().expect("picker exists");
        if matches!(picker.kind, PickerKind::Rewind) {
            let target = self
                .rewind_targets
                .iter()
                .rev()
                .nth(picker.selected)
                .expect("rewind picker mirrors targets")
                .clone();
            self.rewind_primed = false;
            return Ok(UiAction::Rewind {
                sequence: target.sequence,
                text: target.text,
                attachments: target.attachments,
            });
        }
        Ok(match picker.kind {
            PickerKind::Settings => UiAction::Submit {
                text: picker.selected_value(),
                attachments: Vec::new(),
            },
            PickerKind::Resume => UiAction::Submit {
                text: format!("/resume {}", picker.selected_value()),
                attachments: Vec::new(),
            },
            PickerKind::Model => UiAction::SetModel(picker.selected_value()),
            PickerKind::Effort => UiAction::SetEffort(picker.selected_value()),
            PickerKind::Language => UiAction::SetResponseLanguage(
                ResponseLanguage::parse(&picker.selected_value())
                    .expect("language picker values are canonical"),
            ),
            PickerKind::Fast => UiAction::SetFast(picker.selected_value() == "On"),
            PickerKind::RefreshRate => UiAction::SetRefreshRate(
                picker
                    .selected_value()
                    .parse()
                    .expect("FPS options are numeric"),
            ),
            PickerKind::PreventSleep => UiAction::SetPreventSleep(picker.selected_value() == "On"),
            PickerKind::ActiveMessages => {
                UiAction::SetSteerActive(picker.selected_value() == "Steer current turn")
            }
            PickerKind::AutoExpandEdits => {
                UiAction::SetAutoExpandEdits(picker.selected_value() == "On")
            }
            PickerKind::AutoExpandTools => {
                UiAction::SetAutoExpandTools(picker.selected_value() == "On")
            }
            PickerKind::Rewind => unreachable!("handled above"),
            PickerKind::MessageActions => unreachable!("handled separately"),
        })
    }

    pub fn draw(&mut self) -> Result<()> {
        if self
            .copy_notice_expires_at
            .is_some_and(|expires_at| Instant::now() >= expires_at)
        {
            if self
                .notice
                .as_deref()
                .is_some_and(|notice| notice.to_ascii_lowercase().contains("copied"))
            {
                self.notice = None;
            }
            self.copy_notice_expires_at = None;
        }
        let title = terminal_title(self.status, self.transcript.first_prompt());
        if self.last_terminal_title.as_deref() != Some(&title) {
            execute!(self.terminal.backend_mut(), SetTitle(&title))?;
            self.last_terminal_title = Some(title);
        }
        let terminal_size = self.terminal.size()?;
        let content_width = terminal_content_width(terminal_size.width);
        let tool_run_viewport_height = tool_run_viewport_height(terminal_size.height as usize);
        // Reserve the transcript's scrollbar gutter before wrapping. Rendering
        // at full width and then narrowing the widget clips the final cells
        // instead of moving the wrap point.
        let transcript_width = content_width.saturating_sub(3).max(1) as usize;
        let goal_tick = self.transcript.active_goal_cache_tick();
        let tool_spinner_tick = self.transcript.tool_spinner_cache_tick();
        let local_date = Local::now().date_naive();
        let transcript_render = self
            .transcript_render_cache
            .as_ref()
            .filter(
                |(
                    width,
                    cached_tool_run_viewport_height,
                    cached_goal_tick,
                    cached_tool_spinner_tick,
                    cached_date,
                    _,
                )| {
                    *width == transcript_width
                        && *cached_tool_run_viewport_height == tool_run_viewport_height
                        && *cached_goal_tick == goal_tick
                        && *cached_tool_spinner_tick == tool_spinner_tick
                        && *cached_date == local_date
                },
            )
            .map(|(_, _, _, _, _, render)| Arc::clone(render))
            .unwrap_or_else(|| {
                let render = Arc::new(self.transcript.render_with_tool_run_viewport(
                    transcript_width,
                    tool_run_viewport_height,
                    None,
                    None,
                    None,
                ));
                self.transcript_render_cache = Some((
                    transcript_width,
                    tool_run_viewport_height,
                    goal_tick,
                    tool_spinner_tick,
                    local_date,
                    Arc::clone(&render),
                ));
                render
            });
        let (transcript, tool_rows, tool_run_rows, message_rows, entry_rows, link_rows) =
            transcript_render.as_ref();
        let queued_prompts = &self.queued_prompts;
        // Keep the first draft anchored in the splash composition area. Moving
        // it to the chat footer on the first keystroke makes the whole screen
        // jump before the user has actually submitted anything.
        let is_launch_screen = transcript.is_empty() && queued_prompts.is_empty();
        let transcript_height = transcript.len();
        if let Some(previous_height) = self.pending_scroll_anchor_height.take() {
            self.scroll_from_bottom =
                preserve_scroll_anchor(self.scroll_from_bottom, previous_height, transcript_height);
        }
        self.rendered_transcript_height = transcript_height;
        let message_actions_open = matches!(
            self.picker.as_ref().map(|picker| picker.kind),
            Some(PickerKind::MessageActions)
        );
        let resume_picker_open = self
            .picker
            .as_ref()
            .is_some_and(|picker| matches!(picker.kind, PickerKind::Resume));
        let composer_area_width = if is_launch_screen {
            if resume_picker_open {
                content_width.saturating_sub(6).clamp(1, 140)
            } else {
                responsive_launch_width(content_width)
            }
        } else {
            content_width
        };
        let pending_approval = self.pending_approval;
        let status = self.status;
        let status_label = if self.borging_this_run
            && matches!(status, SessionStatus::Starting | SessionStatus::Running)
        {
            "borging"
        } else {
            status_label(status)
        };
        let status_glyph = activity_glyph(status);
        let (config_primary, config_secondary) = self.transcript.config_lines();
        let cache_status = self.transcript.cache_status(Utc::now());
        let (context_status, context_imminent) = self.transcript.context_status();
        let team_transcript = self
            .director_transcript
            .as_deref()
            .unwrap_or(&self.transcript);
        let active_subagents = team_transcript.active_subagent_count();
        let working_agents = active_subagents + 1;
        let agent_roster_rows = team_transcript.active_agent_roster_rows();
        let agent_roster_entries = team_transcript.agent_roster_entries();
        let session_is_active = matches!(status, SessionStatus::Starting | SessionStatus::Running);
        let goal_status = self.transcript.goal_status();
        let slash_suggestions = (self.picker.is_none())
            .then(|| slash_suggestion_lines(&self.composer.text, self.slash_selection))
            .filter(|lines| !lines.is_empty());
        let showing_slash_suggestions = slash_suggestions.is_some();
        let notice = self.notice.clone();
        let cold_cache_guidance = cache_status
            .as_ref()
            .filter(|(_, warning)| *warning)
            .filter(|_| {
                status == SessionStatus::Ready
                    && self.picker.is_none()
                    && !self.composer.text.trim().is_empty()
                    && !showing_slash_suggestions
                    && notice.is_none()
            })
            .map(|(label, _)| {
                let reason = label
                    .strip_prefix("cache cold · ")
                    .or_else(|| label.strip_prefix("cache may be cold · "))
                    .unwrap_or(label);
                format!(
                    "Cold cache: {reason}; the next turn may reprocess earlier context. \
                     Run /clear first if that context is no longer useful."
                )
            });
        let showing_primary_controls =
            !showing_slash_suggestions && notice.is_none() && cold_cache_guidance.is_none();
        let primary_controls = primary_controls_line(&self.keymap);
        let keybindings_hint = format!("keybindings {}", self.keymap.label(KeyAction::Keybindings));
        let notice_style = Style::default().fg(
            if notice
                .as_deref()
                .is_some_and(|message| message.starts_with("✓ Copied"))
            {
                Color::LightGreen
            } else if self.picker.is_none() && self.composer.text.trim_start().starts_with('/') {
                Color::White
            } else {
                Color::DarkGray
            },
        );
        let controls = slash_suggestions.unwrap_or_else(|| {
            if let Some(notice) = notice {
                vec![Line::from(Span::styled(notice, notice_style))]
            } else if let Some(guidance) = cold_cache_guidance {
                wrap_display(&guidance, content_width.saturating_sub(2).max(1) as usize)
                    .into_iter()
                    .map(|line| Line::from(Span::styled(line, Style::default().fg(Color::Yellow))))
                    .collect()
            } else {
                vec![Line::from(primary_controls.clone())]
            }
        });
        let controls = if is_launch_screen {
            controls
        } else {
            inset_control_lines(controls)
        };
        let controls_height = controls.len().min(u16::MAX as usize) as u16;
        let footer_height = if is_launch_screen {
            1
        } else {
            controls_height + 1
        };
        let composer_text_width = composer_area_width
            .saturating_sub(if is_launch_screen { 5 } else { 4 })
            .max(1) as usize;
        let (composer_display_text, composer_display_cursor) =
            if self.pending_provider_interaction_secret {
                mask_secret_composer_text(&self.composer.text, self.composer.cursor)
            } else {
                (self.composer.text.clone(), self.composer.cursor)
            };
        let composer_ranges = display_ranges(&composer_display_text, composer_text_width, true);
        let composer_cursor = composer_cursor_position_in_ranges(
            &composer_display_text,
            composer_display_cursor,
            &composer_ranges,
        );
        let picker_lines = self
            .picker
            .as_ref()
            .filter(|_| !message_actions_open)
            .map(|picker| {
                picker.styled_lines(
                    composer_area_width.saturating_sub(4).max(1) as usize,
                    self.transcript.assistant_label_color,
                    self.transcript.assistant_message_color,
                )
            });
        let composer_line_count = picker_lines
            .as_ref()
            .map_or_else(|| composer_ranges.len(), Vec::len);
        let prompt_marker = if pending_approval || self.pending_provider_interaction {
            " ! "
        } else {
            " > "
        };
        let composer_render_lines = if self.picker.as_ref().is_some_and(|_| !message_actions_open) {
            Vec::new()
        } else if self.composer.text.is_empty() {
            let placeholder = if self.pending_provider_interaction {
                "Answer the provider request…"
            } else {
                match status {
                    SessionStatus::Running | SessionStatus::Starting => {
                        "Ask a follow-up or steer the current turn…"
                    }
                    SessionStatus::WaitingForApproval => "Allow · Y   Deny · N",
                    _ => "Describe a task…",
                }
            };
            vec![Line::from(vec![
                Span::styled(prompt_marker, Style::default().fg(Color::DarkGray)),
                Span::styled(placeholder, Style::default().fg(Color::DarkGray)),
            ])]
        } else if self.pending_provider_interaction_secret {
            styled_plain_composer_lines(&composer_display_text, &composer_ranges, prompt_marker)
        } else {
            self.composer
                .styled_lines_for_ranges(&composer_ranges, prompt_marker)
        };
        let composer_max_height = if resume_picker_open { 18 } else { 8 };
        let composer_height = composer_line_count
            .max(composer_cursor.0.saturating_add(1))
            .clamp(1, composer_max_height) as u16
            + 2;
        let composer_scroll =
            (composer_cursor.0 as u16).saturating_sub(composer_height.saturating_sub(3));
        let mut next_scrollbar_area = None;
        let mut next_scrollbar_thumb_area = None;
        let mut next_transcript_viewport_area = None;
        let mut next_scroll_max = 0;
        let mut next_tool_hit_areas = Vec::new();
        let mut next_tool_run_hit_areas = Vec::new();
        let mut next_message_hit_areas = Vec::new();
        let mut next_link_hit_areas = Vec::new();
        let mut next_entry_hit_areas = Vec::new();
        let mut next_picker_hit_areas = Vec::new();
        let mut next_jump_to_bottom_area = None;
        let mut next_goal_status_area = None;
        let mut next_agents_status_area = None;
        let mut next_team_roster_hit_areas = Vec::new();
        let mut next_back_to_director_area = None;
        let mut next_keybindings_hint_area = None;
        let pending_transcript_anchor = self.pending_transcript_anchor.take();
        let mut restored_scroll_from_bottom = None;
        let cursor_visible = cursor_blink_visible(self.cursor_blink_started_at.elapsed());
        self.terminal.draw(|frame| {
            let area = centered_content_area(frame.area());
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Min(3),
                    Constraint::Length(queued_prompt_panel_height(queued_prompts)),
                    Constraint::Length(2 * u16::from(!is_launch_screen)),
                    Constraint::Length(composer_height),
                    Constraint::Length(footer_height),
                ])
                .split(area);
            let status_color = session_status_color(status);
            let (status_area, transcript_area, composer_area, footer_area) = if is_launch_screen {
                let launch_width = composer_area_width.min(chunks[0].width);
                let launch_height = composer_height
                    .saturating_add(5)
                    .saturating_add(controls_height)
                    .min(chunks[0].height);
                let launch = Rect {
                    x: chunks[0].x + chunks[0].width.saturating_sub(launch_width) / 2,
                    y: chunks[0].y + chunks[0].height.saturating_sub(launch_height) / 2,
                    width: launch_width,
                    height: launch_height,
                };
                let launch_chunks = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([
                        Constraint::Length(4),
                        Constraint::Length(composer_height),
                        Constraint::Length(controls_height),
                    ])
                    .split(launch);
                frame.render_widget(
                    Paragraph::new(vec![
                        Line::from(Span::styled(
                            "B O R G",
                            Style::default()
                                .fg(Color::White)
                                .add_modifier(Modifier::BOLD),
                        )),
                        Line::from(""),
                        Line::from(Span::styled(
                            "What are we working on?",
                            Style::default().fg(Color::Gray),
                        )),
                    ])
                    .alignment(Alignment::Center),
                    launch_chunks[0],
                );
                frame.render_widget(
                    Paragraph::new(controls.clone())
                        .style(Style::default().fg(Color::DarkGray))
                        .alignment(if showing_slash_suggestions {
                            Alignment::Left
                        } else {
                            Alignment::Center
                        }),
                    launch_chunks[2],
                );
                (
                    chunks[4],
                    Rect::default(),
                    launch_chunks[1],
                    Rect::default(),
                )
            } else {
                (
                    Rect {
                        y: chunks[2].y.saturating_add(1),
                        height: chunks[2].height.saturating_sub(1),
                        ..chunks[2]
                    },
                    Rect {
                        height: chunks[0].height.saturating_sub(1),
                        ..chunks[0]
                    },
                    chunks[3],
                    chunks[4],
                )
            };
            if !is_launch_screen {
                frame.render_widget(
                    Block::default().style(Style::default().bg(COMMAND_PANEL_BG)),
                    chunks[2],
                );
            }
            if !transcript_area.is_empty() {
                let visible_height = transcript_area.height as usize;
                let scroll_from_bottom =
                    pending_transcript_anchor.map_or(self.scroll_from_bottom, |anchor| {
                        restore_transcript_viewport_anchor(
                            anchor,
                            tool_rows,
                            entry_rows,
                            transcript_height,
                            visible_height,
                            self.scroll_from_bottom,
                        )
                    });
                restored_scroll_from_bottom = Some(scroll_from_bottom);
                let scroll_max = transcript_height.saturating_sub(transcript_area.height as usize);
                next_scroll_max = scroll_max;
                let scroll = scroll_max.saturating_sub(scroll_from_bottom.min(scroll_max));
                let content_area = if transcript_area.width > 4 {
                    Rect {
                        width: transcript_area.width - 3,
                        ..transcript_area
                    }
                } else {
                    transcript_area
                };
                next_transcript_viewport_area = Some(content_area);
                let scrollbar_area = if scroll_max > 0 && transcript_area.width > 4 {
                    Some(Rect {
                        x: transcript_area.right() - 2,
                        width: 2,
                        ..transcript_area
                    })
                } else {
                    None
                };
                let scroll_start = scroll;
                let visible_height = content_area.height as usize;
                let sticky_index = tool_rows.partition_point(|(_, start, _)| *start < scroll_start);
                let sticky_tool_header = sticky_index
                    .checked_sub(1)
                    .and_then(|index| tool_rows.get(index))
                    .filter(|(_, _, end)| *end > scroll_start)
                    .map(|(index, start, _)| (*index, transcript[*start].clone()));
                let visible_transcript = transcript
                    .iter()
                    .skip(scroll_start)
                    .take(visible_height)
                    .cloned()
                    .collect::<Vec<_>>();
                let mut visible_transcript = visible_transcript;
                for (index, start, end) in
                    visible_row_ranges(tool_rows, scroll_start, visible_height)
                {
                    if self.hovered_tool == Some(*index) {
                        apply_viewport_background(
                            &mut visible_transcript,
                            *start,
                            start.saturating_add(1),
                            scroll_start,
                            content_area.width as usize,
                            MESSAGE_HOVER_BG,
                        );
                    }
                    next_tool_hit_areas.push((
                        viewport_hit_area(content_area, scroll_start, *start, *end),
                        *index,
                    ));
                }
                let visible_end = scroll_start.saturating_add(visible_height);
                for (start_index, start, end, max_offset) in tool_run_rows
                    .iter()
                    .filter(|(_, start, end, _)| *end > scroll_start && *start < visible_end)
                {
                    next_tool_run_hit_areas.push((
                        viewport_hit_area(content_area, scroll_start, *start, *end),
                        *start_index,
                        *max_offset,
                    ));
                }
                for (index, start, end) in
                    visible_row_ranges(message_rows, scroll_start, visible_height)
                {
                    if self.hovered_message == Some(*index) {
                        apply_viewport_background(
                            &mut visible_transcript,
                            *start,
                            *end,
                            scroll_start,
                            content_area.width as usize,
                            MESSAGE_HOVER_BG,
                        );
                    }
                    next_message_hit_areas.push((
                        viewport_hit_area(content_area, scroll_start, *start, *end),
                        *index,
                    ));
                }
                for (index, start, end) in
                    visible_row_ranges(entry_rows, scroll_start, visible_height)
                {
                    if self.hovered_entry == Some(*index) {
                        apply_viewport_background(
                            &mut visible_transcript,
                            *start,
                            *end,
                            scroll_start,
                            content_area.width as usize,
                            MESSAGE_HOVER_BG,
                        );
                    }
                    next_entry_hit_areas.push((
                        viewport_hit_area(content_area, scroll_start, *start, *end),
                        *index,
                    ));
                }
                if let Some(selection) = self.text_selection {
                    apply_text_selection(&mut visible_transcript, scroll_start, selection);
                }
                frame.render_widget(Paragraph::new(visible_transcript), content_area);
                for link in link_rows.iter().filter(|link| {
                    link.row >= scroll_start
                        && link.row < scroll_start.saturating_add(visible_height)
                }) {
                    let x = content_area.x.saturating_add(link.start as u16);
                    let width = link.end.saturating_sub(link.start) as u16;
                    if width > 0 && x < content_area.right() {
                        next_link_hit_areas.push((
                            Rect {
                                x,
                                y: content_area.y + (link.row - scroll_start) as u16,
                                width: width.min(content_area.right().saturating_sub(x)),
                                height: 1,
                            },
                            link.url.clone(),
                        ));
                    }
                }
                if let Some((index, mut header)) = sticky_tool_header {
                    apply_line_background(
                        &mut header,
                        content_area.width as usize,
                        if self.hovered_tool == Some(index) {
                            MESSAGE_HOVER_BG
                        } else {
                            MESSAGE_BG
                        },
                    );
                    let sticky_area = Rect {
                        height: 1,
                        ..content_area
                    };
                    frame.render_widget(Paragraph::new(header), sticky_area);
                    next_tool_hit_areas.push((sticky_area, index));
                }
                if let Some(area) = scrollbar_area {
                    let minimum_thumb_height = MIN_SCROLLBAR_THUMB_ROWS.min(area.height);
                    let thumb_height = ((u64::from(area.height) * u64::from(area.height))
                        / transcript_height.max(1) as u64)
                        .clamp(u64::from(minimum_thumb_height), u64::from(area.height))
                        as u16;
                    let thumb_travel = area.height.saturating_sub(thumb_height);
                    let thumb_top = if scroll_max == 0 {
                        0
                    } else {
                        (scroll as u64 * u64::from(thumb_travel) / scroll_max as u64) as u16
                    };
                    let rows = (0..area.height)
                        .map(|row| {
                            let in_thumb = row >= thumb_top && row < thumb_top + thumb_height;
                            Line::from(Span::styled(
                                " ▊",
                                Style::default().fg(if in_thumb {
                                    if self.scrollbar_hovered || self.dragging_scrollbar {
                                        BORG_ORANGE_HOVER
                                    } else {
                                        BORG_ORANGE
                                    }
                                } else {
                                    Color::DarkGray
                                }),
                            ))
                        })
                        .collect::<Vec<_>>();
                    frame.render_widget(Paragraph::new(rows), area);
                    next_scrollbar_area = Some(area);
                    next_scrollbar_thumb_area = Some(Rect {
                        y: area.y.saturating_add(thumb_top),
                        height: thumb_height,
                        ..area
                    });
                }
            }
            if !is_launch_screen && self.scroll_from_bottom > 0 {
                let label = " ↓ Jump to bottom ";
                let button_width = label.width() as u16;
                let button = Rect {
                    x: chunks[2].right().saturating_sub(button_width + 1),
                    y: chunks[2].y,
                    width: button_width,
                    height: 1,
                };
                frame.render_widget(
                    Paragraph::new(label).style(
                        Style::default()
                            .fg(if self.jump_to_bottom_hovered {
                                Color::White
                            } else {
                                Color::Gray
                            })
                            .bg(if self.jump_to_bottom_hovered {
                                MESSAGE_HOVER_BG
                            } else {
                                COMMAND_PANEL_BG
                            }),
                    ),
                    button,
                );
                next_jump_to_bottom_area = Some(button);
            }
            if !queued_prompts.is_empty() {
                frame.render_widget(
                    Paragraph::new(queued_prompt_lines(
                        queued_prompts.as_slice(),
                        chunks[1].width,
                    ))
                    .block(
                        Block::default()
                            .borders(Borders::TOP | Borders::LEFT)
                            .border_style(Style::default().fg(Color::DarkGray))
                            .title(Span::styled(
                                format!(" PENDING INPUT · {} ", queued_prompts.len()),
                                Style::default()
                                    .fg(BORG_ORANGE)
                                    .add_modifier(Modifier::BOLD),
                            )),
                    ),
                    chunks[1],
                );
            }
            if !is_launch_screen {
                frame.render_widget(
                    Block::default().style(Style::default().bg(COMMAND_PANEL_BG)),
                    Rect {
                        x: area.x,
                        y: composer_area.y,
                        width: area.width,
                        height: footer_area.bottom().saturating_sub(composer_area.y),
                    },
                );
            }
            let composer_block = Block::default()
                .style(Style::default().bg(if !is_launch_screen {
                    COMMAND_PANEL_BG
                } else {
                    Color::Reset
                }))
                .borders(if is_launch_screen {
                    Borders::LEFT
                } else {
                    Borders::TOP
                })
                .border_style(Style::default().fg(if is_launch_screen {
                    BORG_ORANGE
                } else {
                    Color::DarkGray
                }));
            if let Some(lines) = picker_lines.clone() {
                frame.render_widget(
                    Paragraph::new(lines)
                        .block(composer_block)
                        .scroll((composer_scroll, 0)),
                    composer_area,
                );
            } else {
                frame.render_widget(
                    Paragraph::new(composer_render_lines.clone())
                        .block(composer_block)
                        .scroll((composer_scroll, 0)),
                    composer_area,
                );
            }
            if self.picker.is_none() && cursor_visible {
                let (cursor_row, cursor_column) = composer_cursor;
                frame.set_cursor_position(Position {
                    x: composer_area
                        .x
                        .saturating_add(composer_cursor_x_offset(is_launch_screen))
                        .saturating_add(cursor_column as u16),
                    y: composer_area
                        .y
                        .saturating_add(if is_launch_screen { 0 } else { 1 })
                        .saturating_add((cursor_row as u16).saturating_sub(composer_scroll))
                        .min(composer_area.bottom().saturating_sub(1)),
                });
            }
            let mut status_spans = vec![Span::styled(
                format!(" {status_glyph} {status_label}"),
                Style::default().fg(status_color),
            )];
            if session_is_active
                && let Some(duration) =
                    format_elapsed_duration(self.active_since.map_or(0, |started| {
                        Utc::now()
                            .signed_duration_since(started)
                            .num_seconds()
                            .max(0) as u64
                    }))
            {
                status_spans.push(Span::styled(
                    format!(" {duration}"),
                    Style::default().fg(status_color),
                ));
            }
            let agents_status_start = status_spans.iter().map(|span| span.width()).sum::<usize>();
            let agents_status_width =
                (active_subagents > 0).then(|| format!(" · {working_agents} agents").width());
            if active_subagents > 0 {
                push_status_segment(
                    &mut status_spans,
                    format!("{working_agents} agents"),
                    SUBAGENT_PINK,
                );
            }
            let goal_status_start = status_spans.iter().map(|span| span.width()).sum::<usize>();
            let goal_status_width = goal_status
                .as_ref()
                .map(|status| format!(" · {status}").width());
            if let Some(goal_status) = goal_status.clone() {
                status_spans.push(Span::styled(
                    format!(" · {goal_status}"),
                    Style::default().fg(Color::Yellow),
                ));
            }
            push_status_segment(&mut status_spans, config_primary, Color::Gray);
            push_status_segment(
                &mut status_spans,
                context_status,
                if context_imminent {
                    Color::Yellow
                } else {
                    Color::Gray
                },
            );
            push_status_segment(&mut status_spans, config_secondary, Color::Gray);
            let status_line = Line::from(status_spans);
            let alignment_offset = if is_launch_screen {
                status_area.width.saturating_sub(status_line.width() as u16) / 2
            } else {
                0
            };
            if let Some(agents_status_width) = agents_status_width {
                next_agents_status_area = Some(Rect {
                    x: status_area
                        .x
                        .saturating_add(alignment_offset)
                        .saturating_add(agents_status_start as u16),
                    y: status_area.y,
                    width: (agents_status_width as u16).min(status_area.width),
                    height: 1,
                });
            }
            if let Some(goal_status_width) = goal_status_width {
                next_goal_status_area = Some(Rect {
                    x: status_area
                        .x
                        .saturating_add(alignment_offset)
                        .saturating_add(goal_status_start as u16),
                    y: status_area.y,
                    width: (goal_status_width as u16).min(status_area.width),
                    height: 1,
                });
            }
            frame.render_widget(
                Paragraph::new(status_line)
                    .style(
                        Style::default()
                            .fg(Color::DarkGray)
                            .bg(if is_launch_screen {
                                Color::Reset
                            } else {
                                COMMAND_PANEL_BG
                            }),
                    )
                    .alignment(if is_launch_screen {
                        Alignment::Center
                    } else {
                        Alignment::Left
                    }),
                status_area,
            );
            if (self.agents_status_hovered || self.team_switcher_open) && active_subagents > 0 {
                let tooltip_width = agent_roster_rows
                    .iter()
                    .map(|row| row.width() as u16)
                    .max()
                    .unwrap_or(24)
                    .saturating_add(4)
                    .clamp(30, status_area.width.min(96));
                let tooltip_height = (agent_roster_rows.len() as u16)
                    .saturating_add(2)
                    .min(status_area.y.saturating_sub(area.y).max(1));
                let tooltip = Rect {
                    x: next_agents_status_area
                        .map(|agents_area| agents_area.x)
                        .unwrap_or(status_area.x)
                        .min(area.right().saturating_sub(tooltip_width)),
                    y: status_area.y.saturating_sub(tooltip_height),
                    width: tooltip_width,
                    height: tooltip_height,
                };
                frame.render_widget(Clear, tooltip);
                frame.render_widget(
                    Paragraph::new(agent_roster_rows.join("\n"))
                        .style(Style::default().fg(Color::White).bg(COMMAND_PANEL_BG))
                        .block(
                            Block::default()
                                .borders(Borders::ALL)
                                .border_style(Style::default().fg(SUBAGENT_PINK))
                                .title(" Team "),
                        ),
                    tooltip,
                );
                if self.team_switcher_open {
                    for (index, (_, child_id)) in agent_roster_entries.iter().enumerate() {
                        next_team_roster_hit_areas.push((
                            Rect {
                                x: tooltip.x.saturating_add(1),
                                y: tooltip.y.saturating_add(1 + index as u16),
                                width: tooltip.width.saturating_sub(2),
                                height: 1,
                            },
                            *child_id,
                        ));
                    }
                }
            }
            if self.focused_child.is_some() {
                let label = " ← Director ";
                let button = Rect {
                    x: status_area.right().saturating_sub(label.width() as u16),
                    y: status_area.y,
                    width: label.width() as u16,
                    height: 1,
                };
                frame.render_widget(
                    Paragraph::new(label).style(Style::default().fg(Color::White).bg(
                        if self.back_to_director_area == Some(button) {
                            MESSAGE_HOVER_BG
                        } else {
                            COMMAND_PANEL_BG
                        },
                    )),
                    button,
                );
                next_back_to_director_area = Some(button);
            }
            if self.goal_status_hovered
                && let Some(goal) = self.transcript.goal.as_ref()
            {
                let tooltip_width = (goal.objective.width() as u16)
                    .saturating_add(4)
                    .clamp(24, status_area.width.min(80));
                let tooltip_lines =
                    wrap_display(&goal.objective, tooltip_width.saturating_sub(4) as usize);
                let tooltip_height = (tooltip_lines.len() as u16)
                    .saturating_add(2)
                    .min(status_area.y.saturating_sub(area.y).max(1));
                let tooltip = Rect {
                    x: next_goal_status_area
                        .map(|goal_area| goal_area.x)
                        .unwrap_or(status_area.x)
                        .min(area.right().saturating_sub(tooltip_width)),
                    y: status_area.y.saturating_sub(tooltip_height),
                    width: tooltip_width,
                    height: tooltip_height,
                };
                frame.render_widget(Clear, tooltip);
                frame.render_widget(
                    Paragraph::new(goal.objective.as_str())
                        .wrap(ratatui::widgets::Wrap { trim: true })
                        .style(Style::default().fg(Color::White).bg(COMMAND_PANEL_BG))
                        .block(
                            Block::default()
                                .borders(Borders::ALL)
                                .border_style(Style::default().fg(Color::Yellow))
                                .title(" Goal "),
                        ),
                    tooltip,
                );
            }
            if !is_launch_screen {
                frame.render_widget(
                    Paragraph::new(controls)
                        .style(Style::default().fg(Color::DarkGray).bg(COMMAND_PANEL_BG)),
                    footer_area,
                );
            }
            if showing_primary_controls {
                let (controls_x, controls_y, controls_width) = if is_launch_screen {
                    let width = primary_controls.width() as u16;
                    (
                        composer_area
                            .x
                            .saturating_add(composer_area.width.saturating_sub(width) / 2),
                        composer_area.bottom(),
                        width,
                    )
                } else {
                    (
                        footer_area.x.saturating_add(1),
                        footer_area.y,
                        primary_controls.width() as u16,
                    )
                };
                let hint_width = keybindings_hint.width() as u16;
                next_keybindings_hint_area = Some(Rect {
                    x: controls_x.saturating_add(controls_width.saturating_sub(hint_width)),
                    y: controls_y,
                    width: hint_width.min(controls_width),
                    height: 1,
                });
            }
            if showing_primary_controls
                && (self.keybindings_open || self.keybindings_hovered)
                && let Some(hint_area) = next_keybindings_hint_area.or(self.keybindings_hint_area)
            {
                let tooltip_width = area.width.min(82);
                let tooltip_lines = keybinding_lines(
                    &self.keymap,
                    tooltip_width.saturating_sub(2).max(1) as usize,
                );
                let tooltip_height = (tooltip_lines.len() as u16)
                    .saturating_add(2)
                    .min(hint_area.y.saturating_sub(area.y).max(1));
                let tooltip = Rect {
                    x: hint_area
                        .right()
                        .saturating_sub(tooltip_width)
                        .clamp(area.x, area.right().saturating_sub(tooltip_width)),
                    y: hint_area.y.saturating_sub(tooltip_height),
                    width: tooltip_width,
                    height: tooltip_height,
                };
                frame.render_widget(Clear, tooltip);
                frame.render_widget(
                    Paragraph::new(tooltip_lines)
                        .style(Style::default().fg(Color::Gray).bg(COMMAND_PANEL_BG))
                        .block(
                            Block::default()
                                .borders(Borders::ALL)
                                .border_style(Style::default().fg(BORG_ORANGE))
                                .title(Span::styled(
                                    format!(
                                        " Keybindings · close {} or {} ",
                                        self.keymap.label(KeyAction::Keybindings),
                                        self.keymap.label(KeyAction::Interrupt)
                                    ),
                                    Style::default()
                                        .fg(Color::White)
                                        .add_modifier(Modifier::BOLD),
                                )),
                        ),
                    tooltip,
                );
            }
            if let Some(picker) = self
                .picker
                .as_ref()
                .filter(|picker| matches!(picker.kind, PickerKind::MessageActions))
            {
                let popup = centered_popup(frame.area(), 48, 6);
                frame.render_widget(Clear, popup);
                frame.render_widget(
                    Block::default()
                        .style(Style::default().bg(Color::Rgb(20, 20, 22)))
                        .borders(Borders::ALL)
                        .border_style(Style::default().fg(Color::DarkGray))
                        .title(Span::styled(
                            format!(" {} ", picker.title),
                            Style::default()
                                .fg(Color::White)
                                .add_modifier(Modifier::BOLD),
                        )),
                    popup,
                );
                frame.render_widget(
                    Paragraph::new("esc").style(Style::default().fg(Color::DarkGray)),
                    Rect {
                        x: popup.right().saturating_sub(5),
                        y: popup.y,
                        width: 3,
                        height: 1,
                    },
                );
                for (index, option) in picker.options.iter().enumerate() {
                    let row = Rect {
                        x: popup.x + 1,
                        y: popup.y + 2 + index as u16,
                        width: popup.width.saturating_sub(2),
                        height: 1,
                    };
                    let selected = index == picker.selected;
                    frame.render_widget(
                        Paragraph::new(format!(
                            " {}  {}",
                            if option.label.starts_with("Revert") {
                                "↶"
                            } else {
                                "⧉"
                            },
                            option.label,
                        ))
                        .style(if selected {
                            Style::default()
                                .fg(Color::Rgb(0, 0, 0))
                                .bg(BORG_ORANGE)
                                .add_modifier(Modifier::BOLD)
                        } else {
                            Style::default().fg(Color::Gray)
                        }),
                        row,
                    );
                    next_picker_hit_areas.push((row, index));
                }
            }
        })?;
        self.scrollbar_area = next_scrollbar_area;
        self.scrollbar_thumb_area = next_scrollbar_thumb_area;
        self.transcript_viewport_area = next_transcript_viewport_area;
        self.transcript_scroll_max = next_scroll_max;
        self.tool_hit_areas = next_tool_hit_areas;
        self.tool_run_hit_areas = next_tool_run_hit_areas;
        self.message_hit_areas = next_message_hit_areas;
        self.link_hit_areas = next_link_hit_areas;
        self.entry_hit_areas = next_entry_hit_areas;
        self.picker_hit_areas = next_picker_hit_areas;
        self.jump_to_bottom_area = next_jump_to_bottom_area;
        self.goal_status_area = next_goal_status_area;
        self.agents_status_area = next_agents_status_area;
        self.team_roster_hit_areas = next_team_roster_hit_areas;
        self.back_to_director_area = next_back_to_director_area;
        self.keybindings_hint_area = next_keybindings_hint_area;
        self.scroll_from_bottom = restored_scroll_from_bottom
            .unwrap_or(self.scroll_from_bottom)
            .min(next_scroll_max);
        Ok(())
    }

    fn handle_key(&mut self, key: KeyEvent) -> Result<UiAction> {
        if matches!(
            self.picker.as_ref().map(|picker| picker.kind),
            Some(PickerKind::MessageActions)
        ) {
            return Ok(match key.code {
                KeyCode::Up | KeyCode::Left => {
                    self.picker.as_mut().expect("checked above").previous();
                    UiAction::None
                }
                KeyCode::Down | KeyCode::Right | KeyCode::Tab => {
                    self.picker.as_mut().expect("checked above").next();
                    UiAction::None
                }
                KeyCode::Esc => {
                    self.picker = None;
                    self.transcript.selected = None;
                    UiAction::None
                }
                KeyCode::Enter => self.run_selected_message_action(),
                _ => UiAction::None,
            });
        }
        let selected_by_number = if key.modifiers == KeyModifiers::NONE
            && let KeyCode::Char(number) = key.code
        {
            self.picker
                .as_mut()
                .is_some_and(|picker| picker.select_number(number))
        } else {
            false
        };
        if selected_by_number {
            return self.run_selected_picker();
        }
        if key.code == KeyCode::Enter && self.picker.is_some() {
            return self.run_selected_picker();
        }
        if let Some(picker) = self.picker.as_mut() {
            return Ok(match key.code {
                KeyCode::Up | KeyCode::Left => {
                    picker.previous();
                    UiAction::None
                }
                KeyCode::Down | KeyCode::Right => {
                    picker.next();
                    UiAction::None
                }
                KeyCode::Esc => {
                    self.picker = None;
                    UiAction::None
                }
                _ => UiAction::None,
            });
        }
        if self.keymap.matches(KeyAction::Keybindings, &key) && self.composer.text.is_empty() {
            self.keybindings_open = !self.keybindings_open;
            self.notice = None;
            return Ok(UiAction::None);
        }
        if self.keybindings_open && self.keymap.matches(KeyAction::Interrupt, &key) {
            self.keybindings_open = false;
            return Ok(UiAction::None);
        }
        if self.keymap.matches(KeyAction::Interrupt, &key)
            && self.composer.text.is_empty()
            && !self.pending_approval
            && !matches!(
                self.status,
                SessionStatus::Starting | SessionStatus::Running
            )
        {
            if self.rewind_primed {
                self.open_rewind_picker();
            } else {
                self.rewind_primed = true;
                self.notice = Some(format!(
                    "Edit a previous message · press {} again",
                    self.keymap.label(KeyAction::Interrupt)
                ));
            }
            return Ok(UiAction::None);
        }
        if self.rewind_primed {
            self.rewind_primed = false;
        }
        if self.keymap.matches(KeyAction::ClearOrExit, &key) {
            if self.copy_text_selection() {
                return Ok(UiAction::None);
            }
            if repeated_ctrl_c(&mut self.last_ctrl_c, Instant::now()) {
                return Ok(UiAction::Quit);
            }
            self.composer.clear();
            self.notice = Some(format!(
                "Prompt cleared · press {} again to exit",
                self.keymap.label(KeyAction::ClearOrExit)
            ));
            return Ok(UiAction::None);
        }
        self.last_ctrl_c = None;
        if self.pending_approval {
            return Ok(if self.keymap.matches(KeyAction::Approve, &key) {
                UiAction::Approve(ApprovalDecision::AllowOnce)
            } else if self.keymap.matches(KeyAction::Deny, &key) {
                UiAction::Approve(ApprovalDecision::Deny)
            } else {
                UiAction::None
            });
        }
        if deletes_previous_word(&key) {
            self.composer.backspace_word();
            self.update_slash_notice();
            return Ok(UiAction::None);
        }
        if self.keymap.matches(KeyAction::Exit, &key) {
            return Ok(UiAction::Quit);
        }
        if self.keymap.matches(KeyAction::AttachImage, &key) {
            match self.attachment_store.capture_clipboard_image() {
                Ok(path) => {
                    let label = self.composer.insert_attachment(path);
                    self.notice = Some(format!("Attached {label}"));
                }
                Err(error) => self.notice = Some(format!("Image paste failed: {error:#}")),
            }
            return Ok(UiAction::None);
        }
        if self.keymap.matches(KeyAction::Copy, &key) {
            if let Some(text) = self.transcript.copy_text() {
                match clipboard::copy(text) {
                    Ok(lease) => {
                        self.clipboard_lease = lease;
                        self.show_copy_notice(self.transcript.copy_notice());
                    }
                    Err(error) => self.notice = Some(format!("Copy failed: {error}")),
                }
            }
            return Ok(UiAction::None);
        }
        if key.modifiers.contains(KeyModifiers::CONTROL) {
            return match key.code {
                KeyCode::Left => {
                    self.composer.move_word_left();
                    Ok(UiAction::None)
                }
                KeyCode::Right => {
                    self.composer.move_word_right();
                    Ok(UiAction::None)
                }
                _ => Ok(UiAction::None),
            };
        }
        if self.keymap.matches(KeyAction::SelectPrevious, &key) {
            self.transcript.select_previous();
            self.notice = Some(self.transcript.selection_notice(&self.keymap));
            return Ok(UiAction::None);
        }
        if self.keymap.matches(KeyAction::SelectNext, &key) {
            self.transcript.select_next();
            self.notice = Some(self.transcript.selection_notice(&self.keymap));
            return Ok(UiAction::None);
        }
        if self.keymap.matches(KeyAction::Newline, &key) {
            self.composer.insert("\n");
            return Ok(UiAction::None);
        }
        if self.keymap.matches(KeyAction::Queue, &key)
            && slash_matches(&self.composer.text).is_empty()
        {
            let (text, attachments) = self.composer.take();
            if text.trim().is_empty() && attachments.is_empty() {
                return Ok(UiAction::None);
            }
            self.notice = None;
            return Ok(
                if matches!(
                    self.status,
                    SessionStatus::Starting
                        | SessionStatus::Running
                        | SessionStatus::WaitingForApproval
                ) {
                    let message_id = Uuid::new_v4();
                    push_queued_prompt(
                        &mut self.queued_prompts,
                        message_id,
                        text.clone(),
                        PromptDelivery::Queue,
                    );
                    UiAction::Queue {
                        message_id,
                        text,
                        attachments,
                    }
                } else {
                    UiAction::Submit { text, attachments }
                },
            );
        }
        if self.keymap.matches(KeyAction::Send, &key) {
            if self.composer.attachments.is_empty()
                && let Some(command) =
                    slash_selected_command(&self.composer.text, self.slash_selection)
            {
                self.composer.replace_text(command);
            }
            let (text, attachments) = self.composer.take();
            if text.trim().is_empty() && attachments.is_empty() {
                return Ok(UiAction::None);
            }
            self.notice = None;
            return Ok(UiAction::Submit { text, attachments });
        }
        if self.keymap.matches(KeyAction::ScrollUp, &key) {
            self.scroll_from_bottom = self.scroll_from_bottom.saturating_add(8);
            return Ok(UiAction::None);
        }
        if self.keymap.matches(KeyAction::ScrollDown, &key) {
            self.scroll_from_bottom = self.scroll_from_bottom.saturating_sub(8);
            return Ok(UiAction::None);
        }
        if self.keymap.matches(KeyAction::Interrupt, &key) {
            return Ok(UiAction::Interrupt);
        }
        match key.code {
            KeyCode::Char(character) => {
                self.keybindings_open = false;
                self.composer.insert(&character.to_string());
                self.slash_selection = 0;
                self.update_slash_notice();
                Ok(UiAction::None)
            }
            KeyCode::Backspace => {
                self.composer.backspace();
                self.slash_selection = 0;
                self.update_slash_notice();
                Ok(UiAction::None)
            }
            KeyCode::Delete => {
                self.composer.delete();
                self.slash_selection = 0;
                self.update_slash_notice();
                Ok(UiAction::None)
            }
            KeyCode::Left => {
                self.composer.move_left();
                Ok(UiAction::None)
            }
            KeyCode::Right => {
                self.composer.move_right();
                Ok(UiAction::None)
            }
            KeyCode::Home => {
                self.composer.cursor = 0;
                self.composer.preferred_column = None;
                Ok(UiAction::None)
            }
            KeyCode::End => {
                self.composer.cursor = self.composer.text.len();
                self.composer.preferred_column = None;
                Ok(UiAction::None)
            }
            KeyCode::Up => {
                let slash_matches = slash_matches(&self.composer.text).len();
                if slash_matches > 0 {
                    self.slash_selection = self
                        .slash_selection
                        .checked_sub(1)
                        .unwrap_or(slash_matches - 1);
                    return Ok(UiAction::None);
                }
                if should_recall_latest_queued_prompt(&self.composer.text, &self.queued_prompts) {
                    return Ok(UiAction::RecallQueuedPrompt);
                }
                if self.composer.history_index.is_some() || self.composer.text.is_empty() {
                    self.composer.history_previous();
                } else {
                    let width = terminal_content_width(self.terminal.size()?.width).max(1) as usize;
                    self.composer.move_vertical(-1, width);
                }
                self.update_slash_notice();
                Ok(UiAction::None)
            }
            KeyCode::Down => {
                let slash_matches = slash_matches(&self.composer.text).len();
                if slash_matches > 0 {
                    self.slash_selection = (self.slash_selection + 1) % slash_matches;
                    return Ok(UiAction::None);
                }
                if self.composer.history_index.is_some() {
                    self.composer.history_next();
                } else if !self.composer.text.is_empty() {
                    let width = terminal_content_width(self.terminal.size()?.width).max(1) as usize;
                    self.composer.move_vertical(1, width);
                }
                self.update_slash_notice();
                Ok(UiAction::None)
            }
            KeyCode::Tab => {
                let matches = slash_matches(&self.composer.text);
                if let Some((command, help)) = matches.get(self.slash_selection) {
                    self.composer.replace_text(*command);
                    self.notice = Some(format!("{command} · {help}"));
                } else if !matches.is_empty() {
                    self.notice = Some(slash_help(&matches));
                }
                Ok(UiAction::None)
            }
            _ => Ok(UiAction::None),
        }
    }

    fn update_slash_notice(&mut self) {
        let matches = slash_matches(&self.composer.text);
        self.slash_selection = self.slash_selection.min(matches.len().saturating_sub(1));
        self.notice = if matches.is_empty() {
            None
        } else {
            Some(slash_help(&matches))
        };
    }
}

#[cfg(test)]
fn is_composer_newline(key: &KeyEvent) -> bool {
    key.code == KeyCode::Enter
        && key
            .modifiers
            .intersects(KeyModifiers::SHIFT | KeyModifiers::ALT)
}

impl Drop for BorgTerminal {
    fn drop(&mut self) {
        self.restore_terminal();
    }
}

impl BorgTerminal {
    fn restore_terminal(&mut self) {
        if self.terminal_restored {
            return;
        }
        self.terminal_restored = true;
        self.input.abort();
        let _ = execute!(self.terminal.backend_mut(), SetTitle("Borg CLI"));
        let _ = execute!(self.terminal.backend_mut(), DisableMouseCapture);
        let _ = execute!(self.terminal.backend_mut(), DisableBracketedPaste);
        if self.keyboard_enhancement {
            let _ = execute!(self.terminal.backend_mut(), PopKeyboardEnhancementFlags);
        }
        if self.mode == ScreenMode::Alternate {
            let _ = execute!(self.terminal.backend_mut(), LeaveAlternateScreen);
        }
        let _ = execute!(
            self.terminal.backend_mut(),
            SetCursorStyle::DefaultUserShape
        );
        let _ = disable_raw_mode();
        let _ = self.terminal.show_cursor();
    }
}

fn switch_to_child_transcript(
    transcript: &mut Transcript,
    director_transcript: &mut Option<Box<Transcript>>,
    child_transcripts: &mut HashMap<Uuid, Transcript>,
    child_id: Uuid,
) {
    let child = child_transcripts.remove(&child_id).unwrap_or_default();
    *director_transcript = Some(Box::new(std::mem::replace(transcript, child)));
}

fn switch_to_director_transcript(
    transcript: &mut Transcript,
    director_transcript: &mut Option<Box<Transcript>>,
    child_transcripts: &mut HashMap<Uuid, Transcript>,
    child_id: Uuid,
) {
    let director = *director_transcript
        .take()
        .expect("child focus has director transcript");
    child_transcripts.insert(child_id, std::mem::replace(transcript, director));
}

#[derive(Default)]
struct Composer {
    text: String,
    cursor: usize,
    preferred_column: Option<usize>,
    attachments: Vec<ComposerAttachment>,
    pasted_texts: Vec<ComposerPastedText>,
    next_image_number: usize,
    next_pasted_text_number: usize,
    history: Vec<String>,
    history_index: Option<usize>,
    history_draft: String,
}

struct ComposerAttachment {
    path: PathBuf,
    label: String,
    start: usize,
    end: usize,
}

struct ComposerPastedText {
    content: String,
    start: usize,
    end: usize,
}

impl Composer {
    fn seed_session_events(&mut self, events: &[SessionEvent]) {
        let mut seen = HashSet::new();
        for event in events {
            let SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text,
                attachments,
                status: MessageStatus::Complete,
                ..
            } = &event.kind
            else {
                continue;
            };
            if seen.insert(*message_id) {
                self.next_image_number += attachments.len();
                if !text.trim().is_empty()
                    && self.history.last().is_none_or(|previous| previous != text)
                {
                    self.history.push(text.clone());
                }
            }
        }
    }

    fn insert(&mut self, text: &str) {
        self.shift_inline_tokens(self.cursor, text.len() as isize);
        self.text.insert_str(self.cursor, text);
        self.cursor += text.len();
        self.preferred_column = None;
    }

    fn insert_attachment(&mut self, path: PathBuf) -> String {
        self.next_image_number += 1;
        let label = format!("Image {}", self.next_image_number);
        let token = format!("[{label}]");
        let start = self.cursor;
        self.insert(&token);
        self.attachments.push(ComposerAttachment {
            path,
            label: label.clone(),
            start,
            end: self.cursor,
        });
        label
    }

    fn insert_pasted_text(&mut self, content: String) -> String {
        self.next_pasted_text_number += 1;
        let label = format!("Pasted Text {}", self.next_pasted_text_number);
        let token = format!("[{label}]");
        let start = self.cursor;
        self.insert(&token);
        self.pasted_texts.push(ComposerPastedText {
            content,
            start,
            end: self.cursor,
        });
        label
    }

    fn backspace(&mut self) {
        if let Some(index) = self
            .attachments
            .iter()
            .position(|attachment| attachment.end == self.cursor)
        {
            self.remove_attachment(index);
            return;
        }
        if let Some(index) = self
            .pasted_texts
            .iter()
            .position(|pasted| pasted.end == self.cursor)
        {
            self.remove_pasted_text(index);
            return;
        }
        let end = self.cursor;
        let Some(previous) = self.text[..self.cursor]
            .grapheme_indices(true)
            .next_back()
            .map(|(index, _)| index)
        else {
            return;
        };
        self.text.drain(previous..self.cursor);
        self.cursor = previous;
        self.shift_inline_tokens(end, -((end - previous) as isize));
        self.preferred_column = None;
    }

    fn backspace_word(&mut self) {
        let end = self.cursor;
        self.move_word_left();
        let mut start = self.cursor;
        for attachment in &self.attachments {
            if attachment.start < end && attachment.end > start {
                start = start.min(attachment.start);
            }
        }
        for pasted in &self.pasted_texts {
            if pasted.start < end && pasted.end > start {
                start = start.min(pasted.start);
            }
        }
        if start == end {
            return;
        }
        self.attachments
            .retain(|attachment| attachment.end <= start || attachment.start >= end);
        self.pasted_texts
            .retain(|pasted| pasted.end <= start || pasted.start >= end);
        self.shift_inline_tokens(end, -((end - start) as isize));
        self.text.drain(start..end);
        self.cursor = start;
        self.preferred_column = None;
    }

    fn delete(&mut self) {
        if let Some(index) = self
            .attachments
            .iter()
            .position(|attachment| attachment.start == self.cursor)
        {
            self.remove_attachment(index);
            return;
        }
        if let Some(index) = self
            .pasted_texts
            .iter()
            .position(|pasted| pasted.start == self.cursor)
        {
            self.remove_pasted_text(index);
            return;
        }
        let Some(next) = self.text[self.cursor..]
            .grapheme_indices(true)
            .nth(1)
            .map(|(index, _)| self.cursor + index)
        else {
            self.text.truncate(self.cursor);
            self.preferred_column = None;
            return;
        };
        self.text.drain(self.cursor..next);
        self.shift_inline_tokens(next, -((next - self.cursor) as isize));
        self.preferred_column = None;
    }

    fn move_left(&mut self) {
        if let Some(attachment) = self
            .attachments
            .iter()
            .find(|attachment| attachment.end == self.cursor)
        {
            self.cursor = attachment.start;
            self.preferred_column = None;
            return;
        }
        if let Some(pasted) = self
            .pasted_texts
            .iter()
            .find(|pasted| pasted.end == self.cursor)
        {
            self.cursor = pasted.start;
            self.preferred_column = None;
            return;
        }
        if let Some((index, _)) = self.text[..self.cursor].grapheme_indices(true).next_back() {
            self.cursor = index;
            self.preferred_column = None;
        }
    }

    fn move_right(&mut self) {
        if let Some(attachment) = self
            .attachments
            .iter()
            .find(|attachment| attachment.start == self.cursor)
        {
            self.cursor = attachment.end;
            self.preferred_column = None;
            return;
        }
        if let Some(pasted) = self
            .pasted_texts
            .iter()
            .find(|pasted| pasted.start == self.cursor)
        {
            self.cursor = pasted.end;
            self.preferred_column = None;
            return;
        }
        if let Some((index, grapheme)) = self.text[self.cursor..].grapheme_indices(true).next() {
            self.cursor += index + grapheme.len();
            self.preferred_column = None;
        }
    }

    fn move_word_left(&mut self) {
        let mut target = 0;
        for (start, word) in self.text.unicode_word_indices() {
            if start >= self.cursor {
                break;
            }
            target = start;
            if self.cursor <= start + word.len() {
                break;
            }
        }
        self.cursor = target;
        self.preferred_column = None;
    }

    fn move_word_right(&mut self) {
        self.cursor = self
            .text
            .unicode_word_indices()
            .map(|(start, _)| start)
            .find(|start| *start > self.cursor)
            .unwrap_or(self.text.len());
        self.preferred_column = None;
    }

    fn move_vertical(&mut self, direction: isize, width: usize) {
        let ranges = display_ranges(&self.text, width, true);
        let (row, column) = composer_cursor_position(&self.text, self.cursor, width);
        let target_row = if direction < 0 {
            row.checked_sub(1)
        } else {
            row.checked_add(1).filter(|row| *row < ranges.len())
        };
        let Some(target_row) = target_row else {
            return;
        };
        let desired = self.preferred_column.unwrap_or(column);
        let (start, end) = ranges[target_row];
        self.cursor = cursor_at_column(&self.text, start, end, desired);
        self.preferred_column = Some(desired);
    }

    fn expanded_text(&self) -> String {
        let mut expanded = self.text.clone();
        let mut pasted = self.pasted_texts.iter().collect::<Vec<_>>();
        pasted.sort_by_key(|item| item.start);
        for item in pasted.into_iter().rev() {
            expanded.replace_range(item.start..item.end, &item.content);
        }
        expanded
    }

    #[cfg(test)]
    fn styled_lines(&self, width: usize, prompt_marker: &str) -> Vec<Line<'static>> {
        let ranges = display_ranges(&self.text, width, false);
        self.styled_lines_for_ranges(&ranges, prompt_marker)
    }

    fn styled_lines_for_ranges(
        &self,
        ranges: &[(usize, usize)],
        prompt_marker: &str,
    ) -> Vec<Line<'static>> {
        let mut tokens = self
            .attachments
            .iter()
            .map(|attachment| (attachment.start, attachment.end))
            .chain(
                self.pasted_texts
                    .iter()
                    .map(|pasted| (pasted.start, pasted.end)),
            )
            .collect::<Vec<_>>();
        tokens.sort_unstable();
        ranges
            .iter()
            .copied()
            .enumerate()
            .map(|(row, (start, end))| {
                let mut spans = Vec::new();
                if row == 0 {
                    spans.push(Span::styled(
                        prompt_marker.to_string(),
                        Style::default().fg(Color::White),
                    ));
                } else {
                    spans.push(Span::raw(" ".repeat(UnicodeWidthStr::width(prompt_marker))));
                }
                let mut cursor = start;
                for (token_start, token_end) in tokens
                    .iter()
                    .copied()
                    .filter(|(token_start, token_end)| *token_end > start && *token_start < end)
                {
                    let token_start = token_start.max(start);
                    let token_end = token_end.min(end);
                    if cursor < token_start {
                        spans.push(Span::styled(
                            self.text[cursor..token_start].to_string(),
                            Style::default().fg(Color::White),
                        ));
                    }
                    spans.push(Span::styled(
                        self.text[token_start..token_end].to_string(),
                        Style::default()
                            .fg(Color::LightYellow)
                            .add_modifier(Modifier::BOLD),
                    ));
                    cursor = token_end;
                }
                if cursor < end {
                    spans.push(Span::styled(
                        self.text[cursor..end].to_string(),
                        Style::default().fg(Color::White),
                    ));
                }
                Line::from(spans)
            })
            .collect()
    }

    fn take(&mut self) -> (String, Vec<PathBuf>) {
        let text = self.expanded_text();
        if !text.trim().is_empty() && self.history.last().is_none_or(|previous| previous != &text) {
            self.history.push(text.clone());
        }
        self.text.clear();
        self.pasted_texts.clear();
        self.cursor = 0;
        self.preferred_column = None;
        self.history_index = None;
        self.history_draft.clear();
        (
            text,
            std::mem::take(&mut self.attachments)
                .into_iter()
                .map(|attachment| attachment.path)
                .collect(),
        )
    }

    fn clear(&mut self) {
        self.text.clear();
        self.cursor = 0;
        self.preferred_column = None;
        self.attachments.clear();
        self.pasted_texts.clear();
        self.history_index = None;
        self.history_draft.clear();
    }

    fn restore(&mut self, text: String, attachments: Vec<PathBuf>) {
        self.clear();
        self.text = text;
        self.cursor = self.text.len();
        let mut search_from = 0;
        for path in attachments {
            let token = self.text[search_from..]
                .find("[Image ")
                .map(|offset| search_from + offset)
                .and_then(|start| {
                    self.text[start..]
                        .find(']')
                        .map(|offset| (start, start + offset + 1))
                });
            if let Some((start, end)) = token {
                let label = self.text[start + 1..end - 1].to_string();
                self.attachments.push(ComposerAttachment {
                    path,
                    label,
                    start,
                    end,
                });
                search_from = end;
            } else {
                self.insert_attachment(path);
                search_from = self.cursor;
            }
        }
    }

    fn append_recalled(&mut self, text: String, attachments: Vec<PathBuf>) {
        if self.text.is_empty() && self.attachments.is_empty() && self.pasted_texts.is_empty() {
            self.restore(text, attachments);
            return;
        }
        let existing_text = self.expanded_text();
        self.text.clear();
        self.pasted_texts.clear();
        let mut existing_attachments = std::mem::take(&mut self.attachments)
            .into_iter()
            .map(|attachment| attachment.path)
            .collect::<Vec<_>>();
        existing_attachments.extend(attachments);
        self.restore(format!("{existing_text}\n\n{text}"), existing_attachments);
    }

    fn remove_attachment(&mut self, index: usize) {
        let attachment = self.attachments.remove(index);
        let removed = attachment.end - attachment.start;
        self.text.drain(attachment.start..attachment.end);
        self.cursor = attachment.start;
        self.shift_inline_tokens(attachment.end, -(removed as isize));
        self.preferred_column = None;
    }

    fn remove_pasted_text(&mut self, index: usize) {
        let pasted = self.pasted_texts.remove(index);
        let removed = pasted.end - pasted.start;
        self.text.drain(pasted.start..pasted.end);
        self.cursor = pasted.start;
        self.shift_inline_tokens(pasted.end, -(removed as isize));
        self.preferred_column = None;
    }

    fn shift_inline_tokens(&mut self, at: usize, delta: isize) {
        for attachment in &mut self.attachments {
            if attachment.start >= at {
                attachment.start = attachment.start.saturating_add_signed(delta);
                attachment.end = attachment.end.saturating_add_signed(delta);
            }
        }
        for pasted in &mut self.pasted_texts {
            if pasted.start >= at {
                pasted.start = pasted.start.saturating_add_signed(delta);
                pasted.end = pasted.end.saturating_add_signed(delta);
            }
        }
    }

    fn replace_text(&mut self, text: impl Into<String>) {
        self.pasted_texts.clear();
        self.text = text.into();
        self.cursor = self.text.len();
        self.preferred_column = None;
    }

    fn history_previous(&mut self) {
        if self.history.is_empty() {
            return;
        }
        let next = match self.history_index {
            Some(index) => index.saturating_sub(1),
            None => {
                self.history_draft = self.expanded_text();
                self.history.len() - 1
            }
        };
        self.history_index = Some(next);
        self.replace_text(self.history[next].clone());
    }

    fn history_next(&mut self) {
        let Some(index) = self.history_index else {
            return;
        };
        if index + 1 < self.history.len() {
            let next = index + 1;
            self.history_index = Some(next);
            self.replace_text(self.history[next].clone());
        } else {
            self.history_index = None;
            let draft = std::mem::take(&mut self.history_draft);
            self.replace_text(draft);
        }
    }
}

fn repeated_ctrl_c(last: &mut Option<Instant>, now: Instant) -> bool {
    let repeated = last
        .is_some_and(|previous| now.saturating_duration_since(previous) <= DOUBLE_CTRL_C_WINDOW);
    *last = (!repeated).then_some(now);
    repeated
}

fn deletes_previous_word(key: &KeyEvent) -> bool {
    (key.modifiers.contains(KeyModifiers::CONTROL)
        && matches!(
            key.code,
            KeyCode::Backspace | KeyCode::Char('h' | 'w') | KeyCode::Char('\u{8}' | '\u{17}')
        ))
        || (key.modifiers.contains(KeyModifiers::ALT) && key.code == KeyCode::Backspace)
}

struct Transcript {
    order: Vec<TranscriptEntry>,
    messages: HashMap<Uuid, usize>,
    tools: HashMap<String, usize>,
    goal: Option<SessionGoal>,
    todos: Vec<PlanItem>,
    config: Option<SessionDisplayConfig>,
    active_turn: Option<ActiveTurnDisplayConfig>,
    subagents: HashMap<Uuid, SubagentStatus>,
    subagent_snapshots: HashMap<Uuid, SubagentSnapshot>,
    subagent_entries: HashMap<Uuid, usize>,
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
    cache_diagnostics: CacheDiagnostics,
    tool_run_offsets: HashMap<usize, usize>,
    active_reasoning: Option<usize>,
    last_edit: Option<usize>,
    next_image_number: usize,
    message_markdown_cache: RefCell<MessageMarkdownCache>,
    tool_body_cache: RefCell<ToolBodyCache>,
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
            goal: None,
            todos: Vec::new(),
            config: None,
            active_turn: None,
            subagents: HashMap::new(),
            subagent_snapshots: HashMap::new(),
            subagent_entries: HashMap::new(),
            follow_tail: false,
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
            cache_diagnostics: CacheDiagnostics::default(),
            tool_run_offsets: HashMap::new(),
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
    Plan {
        items: Vec<PlanItem>,
        time: String,
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

impl Transcript {
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
            self.context_remaining_percent =
                context_remaining_percent(context_tokens, context_window_tokens);
        }
    }

    fn clear_visible_entries(&mut self) {
        self.order.clear();
        self.messages.clear();
        self.tools.clear();
        self.subagent_entries.clear();
        self.tool_run_offsets.clear();
        self.active_reasoning = None;
        self.last_edit = None;
        self.message_markdown_cache.get_mut().messages.clear();
        self.tool_body_cache.get_mut().lines.clear();
        self.selected = None;
        self.follow_tail = true;
    }

    fn show_goal(&mut self, goal: Option<&SessionGoal>) {
        let time = canonical_local_time(Local::now());
        match goal {
            Some(goal) => self.upsert_goal(goal.clone(), time),
            None => self.order.push(TranscriptEntry::Activity {
                text: "No durable goal is set. Use /goal OBJECTIVE to start one.".to_string(),
                time,
            }),
        }
    }

    fn show_plan(&mut self, items: &[PlanItem]) {
        let time = canonical_local_time(Local::now());
        self.upsert_plan(items.to_vec(), time);
    }

    fn upsert_goal(&mut self, goal: SessionGoal, time: String) {
        if let Some(index) = self
            .order
            .iter()
            .rposition(|entry| matches!(entry, TranscriptEntry::Goal { .. }))
        {
            self.order.remove(index);
            self.reindex_after_removal(index);
        }
        self.order.push(TranscriptEntry::Goal { goal, time });
    }

    fn upsert_plan(&mut self, items: Vec<PlanItem>, time: String) {
        if let Some(index) = self
            .order
            .iter()
            .rposition(|entry| matches!(entry, TranscriptEntry::Plan { .. }))
        {
            self.order.remove(index);
            self.reindex_after_removal(index);
        }
        self.order.push(TranscriptEntry::Plan { items, time });
    }

    fn toggle_tool(&mut self, index: usize) -> Vec<SessionPayloadRef> {
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
        )
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
                *output_view = if *error && !output.trim().is_empty() {
                    Some(("text".to_string(), output.trim_end().to_string()))
                } else {
                    tool_output_code_view(name, &output)
                };
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

    fn tool_run_start_containing(&self, index: usize) -> Option<usize> {
        if !matches!(self.order.get(index), Some(TranscriptEntry::Tool { .. })) {
            return None;
        }
        let mut start = index;
        while start > 0 && matches!(self.order[start - 1], TranscriptEntry::Tool { .. }) {
            start -= 1;
        }
        let total = self.order[start..]
            .iter()
            .take_while(|entry| matches!(entry, TranscriptEntry::Tool { .. }))
            .count();
        (total > TOOL_RUN_BOX_THRESHOLD).then_some(start)
    }

    fn apply(&mut self, event: &SessionEvent) {
        if let SessionEventKind::TurnCompleted { message_id, .. } = &event.kind
            && self
                .active_turn
                .as_ref()
                .is_some_and(|turn| turn.message_id == *message_id)
        {
            self.active_turn = None;
        }
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
                self.config = Some(SessionDisplayConfig {
                    cwd: cwd.clone(),
                    provider: *provider,
                    model: model.clone(),
                    effort: effort.clone(),
                    response_language: *response_language,
                    fast: *fast,
                    permission_mode: *permission_mode,
                });
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
                    self.context_remaining_percent =
                        context_remaining_percent(*context_tokens, *context_window_tokens);
                }
                if let Some(signature) = self
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
                            cost_microusd: *cost_microusd,
                            cost_basis,
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
                self.context_remaining_percent =
                    context_remaining_percent(*context_tokens, *context_window_tokens);
            }
            SessionEventKind::ContextCleared => {
                self.clear_visible_entries();
                self.context_remaining_percent = 100;
                self.cache_diagnostics.reset();
            }
            SessionEventKind::PromptRecalled { message_id, .. } => {
                self.remove_message(*message_id);
            }
            SessionEventKind::Message {
                message_id,
                actor,
                text,
                status,
                attachments,
                ..
            } => {
                // Queued prompts belong to the pending-input projection only.
                // Materializing an invisible transcript row here would pin the
                // eventual admitted message to its enqueue position instead of
                // the real provider-boundary chronology.
                if *status == MessageStatus::Queued {
                    return;
                }
                if *actor == EventActor::Assistant && text.trim().is_empty() {
                    self.remove_message(*message_id);
                    return;
                }
                if *actor == EventActor::Assistant {
                    self.finish_reasoning(event.created_at);
                }
                if let Some(index) = self.messages.get(message_id).copied() {
                    self.message_markdown_cache
                        .get_mut()
                        .messages
                        .remove(&index);
                    if let TranscriptEntry::Message {
                        actor: stored_actor,
                        text: stored_text,
                        status: stored_status,
                        complete,
                        ..
                    } = &mut self.order[index]
                    {
                        *stored_actor = *actor;
                        *stored_text = text.clone();
                        *stored_status = *status;
                        *complete = *status == MessageStatus::Complete;
                    }
                } else {
                    let first_image = self.next_image_number;
                    self.next_image_number =
                        self.next_image_number.saturating_add(attachments.len());
                    let attachments = attachments
                        .iter()
                        .cloned()
                        .enumerate()
                        .map(|(offset, path)| (first_image + offset, path))
                        .collect();
                    self.messages.insert(*message_id, self.order.len());
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
                    self.order.push(TranscriptEntry::Message {
                        actor: *actor,
                        text: text.clone(),
                        attachments,
                        model,
                        effort,
                        time: local_event_time(event),
                        status: *status,
                        complete: *status == MessageStatus::Complete,
                    });
                }
            }
            SessionEventKind::ReasoningDelta { text } => {
                self.append_reasoning(text, event.created_at, local_event_time(event));
            }
            SessionEventKind::ToolStarted {
                tool_call_id,
                name,
                input,
                input_ref,
            } => {
                self.finish_reasoning(event.created_at);
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
                let tool_index = self.order.len();
                self.tools.insert(tool_call_id.clone(), tool_index);
                self.order.push(TranscriptEntry::Tool {
                    source_name: name.clone(),
                    name: display_name,
                    detail,
                    code_view,
                    output_view: None,
                    payload_refs: input_ref.iter().cloned().collect(),
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
            SessionEventKind::ToolCompleted {
                tool_call_id,
                output,
                output_ref,
                is_error,
                input,
                input_ref,
            } => {
                let auto_expand_edits = self.auto_expand_edits;
                if let Some(index) = self.tools.get(tool_call_id).copied()
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
                        *detail = format!("{detail} · {}", compact_text(message, 120));
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
                    if completion_presentation.category == ToolPresentationCategory::Edit
                        && let Some(body) = completion_presentation
                            .output
                            .filter(|body| is_diff_language(&body.language))
                    {
                        *name = completion_presentation.label;
                        *detail = completion_presentation.detail;
                        *code_view = Some((body.language, body.text));
                        *output_view = None;
                        *expanded = auto_expand_edits;
                    } else {
                        *output_view = if *is_error && !output.trim().is_empty() {
                            Some(("text".to_string(), output.trim_end().to_string()))
                        } else {
                            tool_output_code_view(name, output)
                        };
                    }
                    let _ = name;
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
                self.order.push(TranscriptEntry::Activity {
                    text: format!("approval · {title} · {detail}"),
                    time: local_event_time(event),
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
                self.order.push(TranscriptEntry::Activity {
                    text: if options.is_empty() {
                        format!("input needed · {title} · {detail}")
                    } else {
                        format!("input needed · {title} · {detail} · {options}")
                    },
                    time: local_event_time(event),
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
            SessionEventKind::ProviderEvent { kind, payload, .. }
                if is_context_compaction(kind) =>
            {
                self.finish_reasoning(event.created_at);
                self.cache_diagnostics.reset();
                let summary = context_compaction_summary(payload);
                if matches!(
                    self.order.last(),
                    Some(TranscriptEntry::Compaction {
                        summary: previous,
                        ..
                    }) if previous == &summary
                ) {
                    return;
                }
                self.order.push(TranscriptEntry::Compaction {
                    summary,
                    time: local_event_time(event),
                });
            }
            SessionEventKind::SubagentActivity {
                activity,
                agent,
                event: child_event,
            } => {
                self.subagents.insert(agent.session_id, agent.status);
                self.subagent_snapshots
                    .insert(agent.session_id, agent.clone());
                if let Some(text) =
                    subagent_activity_summary(*activity, agent, child_event.as_deref())
                {
                    if let Some(index) = self.subagent_entries.get(&agent.session_id).copied()
                        && let Some(TranscriptEntry::Activity {
                            text: existing,
                            time,
                        }) = self.order.get_mut(index)
                    {
                        *existing = text;
                        *time = local_event_time(event);
                    } else {
                        self.subagent_entries
                            .insert(agent.session_id, self.order.len());
                        self.order.push(TranscriptEntry::Activity {
                            text,
                            time: local_event_time(event),
                        });
                    }
                }
            }
            SessionEventKind::Error { message } => {
                self.finish_reasoning(event.created_at);
                self.order.push(TranscriptEntry::Activity {
                    text: format!("error · {message}"),
                    time: local_event_time(event),
                })
            }
            _ => {}
        }
    }

    fn remove_message(&mut self, message_id: Uuid) {
        let Some(index) = self.messages.remove(&message_id) else {
            return;
        };
        self.order.remove(index);
        self.reindex_after_removal(index);
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
            source.push_str(text);
            return;
        }
        if text.trim().is_empty() {
            return;
        }
        let index = self.order.len();
        self.order.push(TranscriptEntry::Tool {
            source_name: "reasoning".to_string(),
            name: "Thinking".to_string(),
            detail: String::new(),
            code_view: Some(("reasoning".to_string(), text.to_string())),
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

    fn mark_running_tools_user_interrupted(&mut self, completed_at: DateTime<Utc>) {
        self.finish_reasoning(completed_at);
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

    fn finish_running_tools(
        &mut self,
        completed_at: DateTime<Utc>,
        failed: bool,
        error_detail: &str,
    ) {
        self.finish_reasoning(completed_at);
        for entry in &mut self.order {
            if let TranscriptEntry::Tool {
                detail,
                complete,
                error,
                completed_at: stored_completed_at,
                ..
            } = entry
                && !*complete
            {
                *complete = true;
                *error = failed;
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

    fn config_lines(&self) -> (String, String) {
        let Some(config) = self.config.as_ref() else {
            return (String::new(), String::new());
        };
        let mut model_parts = Vec::new();
        if let Some(model) = config.model.as_deref() {
            model_parts.push(model.to_string());
        }
        if let Some(effort) = config.effort.as_deref() {
            model_parts.push(effort.to_string());
        }
        if config.fast {
            model_parts.push("fast".to_string());
        }
        let mut primary = Vec::new();
        if !model_parts.is_empty() {
            primary.push(model_parts.join(" "));
        }
        let secondary = [
            match config.permission_mode {
                PermissionMode::FullAccess => "full access",
                PermissionMode::Auto => "auto approvals",
                PermissionMode::Manual => "manual approvals",
            }
            .to_string(),
            fish_style_path(&config.cwd),
        ];
        (primary.join(" · "), secondary.join(" · "))
    }

    fn context_status(&self) -> (String, bool) {
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

    fn cache_status(&self, now: DateTime<Utc>) -> Option<(String, bool)> {
        if self.active_turn.is_some() {
            return None;
        }
        let signature = self.config.as_ref()?.cache_signature();
        self.cache_diagnostics
            .status(now, &signature)
            .map(|status| (status.label, status.warning))
    }

    fn active_subagent_count(&self) -> usize {
        self.subagents
            .values()
            .filter(|status| {
                matches!(
                    status,
                    SubagentStatus::Starting
                        | SubagentStatus::Running
                        | SubagentStatus::WaitingForApproval
                )
            })
            .count()
    }

    fn active_agent_roster_rows(&self) -> Vec<String> {
        self.agent_roster_entries()
            .into_iter()
            .map(|(row, _)| row)
            .collect()
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
        let mut agents = self.subagent_snapshots.values().collect::<Vec<_>>();
        agents.sort_by(|left, right| left.task_name.cmp(&right.task_name));
        rows.extend(agents.into_iter().map(|agent| {
            let name = agent
                .task_name
                .strip_prefix("/root/")
                .unwrap_or(&agent.task_name);
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
            GoalStatus::Active => "pursuing",
            GoalStatus::Paused => "paused",
            GoalStatus::Blocked => "blocked",
            GoalStatus::UsageLimited => "usage limited",
            GoalStatus::BudgetLimited => "budget limited",
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

    fn tool_spinner_cache_tick(&self) -> Option<usize> {
        self.order
            .iter()
            .any(|entry| {
                matches!(
                    entry,
                    TranscriptEntry::Tool {
                        complete: false,
                        ..
                    }
                )
            })
            .then(spinner_frame_index)
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

    fn render_with_tool_run_viewport(
        &self,
        width: usize,
        tool_run_viewport_height: usize,
        hovered_tool: Option<usize>,
        hovered_message: Option<usize>,
        hovered_entry: Option<usize>,
    ) -> TranscriptRender {
        let today = Local::now().date_naive();
        {
            let mut cache = self.message_markdown_cache.borrow_mut();
            if cache.width != width {
                cache.width = width;
                cache.messages.clear();
            }
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
        let mut tool_run_starts = HashMap::new();
        let tool_run_windows = self.tool_run_windows();
        for (index, entry) in self.order.iter().enumerate() {
            let tool_window = tool_run_windows[index];
            if let Some(window) = tool_window.filter(|window| index == window.start) {
                let row = lines.len();
                lines.push(Line::from(Span::styled(
                    format!("┌─ actions · {}", window.total),
                    Style::default().fg(Color::DarkGray),
                )));
                tool_run_starts.insert(window.start, (row, tool_rows.len()));
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
                        | TranscriptEntry::Compaction { .. }
                );
            if starts_labeled_group
                && lines
                    .last()
                    .is_none_or(|line: &Line<'static>| !line.spans.is_empty())
            {
                lines.push(Line::default());
            }
            let entry_start = lines.len();
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
                    if matches!(actor, EventActor::User | EventActor::Assistant) {
                        lines.push(Line::default());
                    }
                    let time = display_local_time(time, today);
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
                                let lines = markdown_lines(
                                    text,
                                    width.saturating_sub(MESSAGE_HORIZONTAL_PADDING * 2),
                                    text_color,
                                );
                                let links = markdown_link_ranges(text, &lines);
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
                        link.start += MESSAGE_HORIZONTAL_PADDING;
                        link.end += MESSAGE_HORIZONTAL_PADDING;
                    }
                    link_rows.extend(message_lines.links);
                    for line in &mut message_lines.lines {
                        line.spans.insert(0, Span::raw("  "));
                    }
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
                        lines.push(Line::from(vec![
                            Span::styled("    ▣ ", Style::default().fg(Color::LightCyan)),
                            Span::styled(
                                format!("Image {number}"),
                                Style::default().add_modifier(Modifier::BOLD),
                            ),
                            Span::styled(
                                format!("  {}", display_name(path)),
                                Style::default().fg(Color::DarkGray),
                            ),
                        ]));
                    }
                    if !complete {
                        lines.push(Line::from(Span::styled(
                            "    ◌ responding",
                            Style::default().fg(Color::Cyan),
                        )));
                    }
                    if matches!(actor, EventActor::User | EventActor::Assistant) {
                        let background = if hovered_message == Some(index) {
                            MESSAGE_HOVER_BG
                        } else {
                            MESSAGE_BG
                        };
                        for line in &mut lines[entry_start..] {
                            apply_line_background(line, width, background);
                        }
                    }
                    if matches!(actor, EventActor::User | EventActor::Assistant) && *complete {
                        message_rows.push((index, entry_start, lines.len()));
                    }
                    lines.push(Line::default());
                }
                TranscriptEntry::Activity { text, time } => {
                    let time = display_local_time(time, today);
                    for line in wrap_display(text, width.saturating_sub(8)) {
                        lines.push(Line::from(Span::styled(
                            format!("  {time}  {line}"),
                            Style::default().fg(Color::DarkGray),
                        )));
                    }
                }
                TranscriptEntry::Plan { items, time } => {
                    let time = display_local_time(time, today);
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
                    let display_items = [PlanItemStatus::InProgress, PlanItemStatus::Pending]
                        .into_iter()
                        .flat_map(|status| items.iter().filter(move |item| item.status == status))
                        .chain(
                            items
                                .iter()
                                .filter(|item| item.status == PlanItemStatus::Completed),
                        );
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
                                "◌",
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
                    if hovered_entry == Some(index) {
                        for line in &mut lines[entry_start..] {
                            apply_line_background(line, width, MESSAGE_HOVER_BG);
                        }
                    }
                    entry_rows.push((index, entry_start, lines.len()));
                    lines.push(Line::default());
                }
                TranscriptEntry::Goal { goal, time } => {
                    let time = display_local_time(time, today);
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
                                || format!("  {time}  {:?}", goal.status),
                                |duration| format!("  {time}  {:?} · {duration}", goal.status),
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
                    lines.push(Line::default());
                }
                TranscriptEntry::Info { title, text, time } => {
                    let time = display_local_time(time, today);
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
                    lines.push(Line::default());
                }
                TranscriptEntry::Compaction { summary, time } => {
                    let time = display_local_time(time, today);
                    lines.push(Line::from(vec![
                        Span::styled(
                            format!("▌ {summary}"),
                            Style::default()
                                .fg(BORG_ORANGE)
                                .add_modifier(Modifier::BOLD),
                        ),
                        Span::styled(format!("  {time}"), Style::default().fg(Color::DarkGray)),
                    ]));
                    lines.push(Line::default());
                }
                TranscriptEntry::Tool {
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
                    let time = display_local_time(time, today);
                    let is_reasoning = matches!(
                        code_view.as_ref(),
                        Some((language, _)) if language == "reasoning"
                    );
                    let rich_ui = tool_has_rich_ui(
                        name,
                        code_view.as_ref().map(|(language, _)| language.as_str()),
                    );
                    if *expanded
                        && rich_ui
                        && lines
                            .last()
                            .is_some_and(|line: &Line<'static>| !line.spans.is_empty())
                    {
                        lines.push(tool_run_separator(tool_window.is_some()));
                    }
                    let summary_start = lines.len();
                    let glyph = if *error {
                        "!"
                    } else if *user_interrupted {
                        "■"
                    } else if *backgrounded {
                        "↗"
                    } else if *complete {
                        if is_reasoning { "◇" } else { "✓" }
                    } else {
                        activity_glyph(SessionStatus::Running)
                    };
                    let style = if *error || *user_interrupted {
                        Style::default().fg(Color::Red)
                    } else if hovered_tool == Some(index) {
                        Style::default().fg(Color::Gray)
                    } else {
                        Style::default().fg(Color::DarkGray)
                    };
                    let name_style = if is_reasoning {
                        Style::default()
                            .fg(Color::Gray)
                            .add_modifier(Modifier::BOLD | Modifier::ITALIC)
                    } else if *error || *user_interrupted {
                        Style::default()
                            .fg(Color::LightRed)
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
                    let lifecycle = if *user_interrupted {
                        Some("user interrupted")
                    } else if *backgrounded {
                        Some("backgrounded")
                    } else if !*complete && !is_reasoning {
                        Some("running")
                    } else {
                        None
                    };
                    let mut summary = if detail.is_empty() {
                        format!("{time}  {glyph} {name}")
                    } else {
                        format!("{time}  {glyph} {name}  {detail}")
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
                            && let Some(name_start) = line.find(name.as_str())
                        {
                            let name_end = name_start + name.len();
                            lines.push(Line::from(vec![
                                Span::styled(format!("{prefix}{}", &line[..name_start]), style),
                                Span::styled(name.clone(), name_style),
                                Span::styled(line[name_end..].to_string(), style),
                            ]));
                        } else {
                            lines.push(Line::from(Span::styled(format!("{prefix}{line}"), style)));
                        }
                    }
                    if hovered_tool == Some(index) {
                        for line in &mut lines[summary_start..] {
                            apply_line_background(line, width, MESSAGE_HOVER_BG);
                        }
                    }
                    if *expanded && let Some((language, source)) = code_view {
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
                    if *expanded && let Some((language, source)) = output_view {
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
                    if *expanded
                        && rich_ui
                        && lines
                            .last()
                            .is_some_and(|line: &Line<'static>| !line.spans.is_empty())
                    {
                        lines.push(tool_run_separator(tool_window.is_some()));
                    }
                    tool_rows.push((index, summary_start, lines.len()));
                    if let Some(window) = tool_window
                        && index + 1 == window.end
                    {
                        let (header_row, first_tool_row) = tool_run_starts
                            .get(&window.start)
                            .copied()
                            .expect("tool run header was recorded");
                        let content_start = header_row + 1;
                        let content_end = lines.len();
                        let total_lines = content_end.saturating_sub(content_start);
                        let max_offset = total_lines.saturating_sub(tool_run_viewport_height);
                        let offset = self
                            .tool_run_offsets
                            .get(&window.start)
                            .copied()
                            .unwrap_or(max_offset)
                            .min(max_offset);
                        let visible_end = offset
                            .saturating_add(tool_run_viewport_height)
                            .min(total_lines);
                        let viewport_start = content_start + offset;
                        let sticky_tool_header = tool_rows[first_tool_row..]
                            .iter()
                            .find(|(_, start, end)| {
                                *start < viewport_start && *end > viewport_start
                            })
                            .map(|(_, start, _)| lines[*start].clone());
                        let mut visible_lines =
                            lines[content_start + offset..content_start + visible_end].to_vec();
                        if let Some(header) = sticky_tool_header
                            && let Some(first) = visible_lines.first_mut()
                        {
                            *first = header;
                        }

                        lines.truncate(content_start);
                        lines.extend(visible_lines);
                        lines[header_row] = Line::from(Span::styled(
                            format!(
                                "┌─ actions · {}{}",
                                window.total,
                                if offset > 0 { " · ↑ more" } else { "" }
                            ),
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
                        tool_run_rows.push((window.start, header_row, lines.len(), max_offset));
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
        )
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
            while index < self.order.len()
                && matches!(self.order[index], TranscriptEntry::Tool { .. })
            {
                index += 1;
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

    fn copy_text(&self) -> Option<&str> {
        self.selected
            .and_then(|index| self.order.get(index))
            .and_then(TranscriptEntry::copy_text)
            .or_else(|| {
                self.order.iter().rev().find_map(|entry| match entry {
                    TranscriptEntry::Message {
                        actor: EventActor::Assistant,
                        text,
                        ..
                    } => Some(text.as_str()),
                    _ => None,
                })
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

fn subagent_status_label(status: SubagentStatus) -> &'static str {
    match status {
        SubagentStatus::Starting => "starting",
        SubagentStatus::Running => "running",
        SubagentStatus::Ready => "ready",
        SubagentStatus::WaitingForApproval => "awaiting approval",
        SubagentStatus::Stopped => "stopped",
        SubagentStatus::Failed => "failed",
    }
}

fn format_subagent_usage(usage: &borg_remote::SubagentUsage) -> String {
    if usage.total_tokens == 0 && usage.cost_microusd.is_none() {
        return "  usage —".to_string();
    }
    let mut parts = Vec::new();
    if usage.total_tokens > 0 {
        parts.push(format_compact_count(usage.total_tokens));
    }
    if let Some(cost_microusd) = usage.cost_microusd {
        parts.push(format!("${:.4}", cost_microusd as f64 / 1_000_000.0));
    }
    format!("  {}", parts.join(" · "))
}

fn format_compact_count(value: u64) -> String {
    if value >= 1_000_000 {
        format!("{:.1}m tok", value as f64 / 1_000_000.0)
    } else if value >= 1_000 {
        format!("{:.1}k tok", value as f64 / 1_000.0)
    } else {
        format!("{value} tok")
    }
}

fn is_context_compaction(kind: &str) -> bool {
    matches!(
        kind.rsplit(['.', ':', '/'])
            .next()
            .unwrap_or(kind)
            .to_ascii_lowercase()
            .replace(['-', '_'], "")
            .as_str(),
        "contextcompaction"
    )
}

#[derive(Default)]
struct ScrollMotion {
    remaining_lines: isize,
}

impl ScrollMotion {
    fn push(&mut self, lines: isize) {
        if lines == 0 {
            return;
        }
        self.remaining_lines = if self.remaining_lines.signum() == lines.signum() {
            self.remaining_lines.saturating_add(lines)
        } else {
            lines
        }
        .clamp(
            -MAX_PENDING_WHEEL_SCROLL_LINES,
            MAX_PENDING_WHEEL_SCROLL_LINES,
        );
    }

    fn cancel(&mut self) {
        self.remaining_lines = 0;
    }

    fn is_active(&self) -> bool {
        self.remaining_lines != 0
    }

    fn advance(&mut self, scroll_from_bottom: usize, scroll_max: usize) -> usize {
        self.advance_with_limits(
            scroll_from_bottom,
            scroll_max,
            1,
            MAX_WHEEL_SCROLL_LINES_PER_FRAME as usize,
        )
    }

    fn advance_with_limits(
        &mut self,
        scroll_from_bottom: usize,
        scroll_max: usize,
        minimum_step: usize,
        maximum_step: usize,
    ) -> usize {
        if self.remaining_lines == 0 {
            return scroll_from_bottom;
        }
        let magnitude = self
            .remaining_lines
            .unsigned_abs()
            .div_ceil(WHEEL_SCROLL_EASING_DIVISOR)
            .clamp(
                minimum_step.min(self.remaining_lines.unsigned_abs()).max(1),
                maximum_step.max(1),
            );
        let requested = if self.remaining_lines > 0 {
            magnitude as isize
        } else {
            -(magnitude as isize)
        };
        let next = if requested > 0 {
            scroll_from_bottom
                .saturating_add(requested as usize)
                .min(scroll_max)
        } else {
            scroll_from_bottom.saturating_sub(requested.unsigned_abs())
        };
        let applied = if next >= scroll_from_bottom {
            isize::try_from(next - scroll_from_bottom).unwrap_or(isize::MAX)
        } else {
            -isize::try_from(scroll_from_bottom - next).unwrap_or(isize::MAX)
        };
        if applied == 0 || applied.unsigned_abs() < requested.unsigned_abs() {
            self.cancel();
        } else {
            self.remaining_lines = self.remaining_lines.saturating_sub(applied);
        }
        next
    }
}

fn session_status_color(status: SessionStatus) -> Color {
    match status {
        SessionStatus::WaitingForApproval => Color::Yellow,
        SessionStatus::Running | SessionStatus::Starting => RUNNING_STATUS_PEACH,
        SessionStatus::Failed => Color::LightRed,
        _ => Color::Gray,
    }
}

/// Whether applying an event can change the cached transcript layout.
///
/// Footer/projection and provider-lifecycle updates still trigger a frame when
/// appropriate, but must not make scrolling re-render the complete history.
fn session_event_changes_transcript(kind: &SessionEventKind) -> bool {
    match kind {
        SessionEventKind::SessionStarted
        | SessionEventKind::SessionConfigured { .. }
        | SessionEventKind::ApprovalResolved { .. }
        | SessionEventKind::ProviderInteractionResolved { .. }
        | SessionEventKind::UsageUpdated { .. }
        | SessionEventKind::ContextWindowUpdated { .. }
        | SessionEventKind::SubagentControl { .. }
        | SessionEventKind::ProviderSessionLinked { .. }
        | SessionEventKind::TurnStarted { .. } => false,
        SessionEventKind::StatusChanged { status, detail } => {
            *status == SessionStatus::Ready
                && detail
                    .as_deref()
                    .is_some_and(|value| value.eq_ignore_ascii_case("interrupted"))
        }
        SessionEventKind::ProviderEvent { kind, .. } => is_context_compaction(kind),
        SessionEventKind::Message { .. }
        | SessionEventKind::ReasoningDelta { .. }
        | SessionEventKind::ToolStarted { .. }
        | SessionEventKind::ToolCompleted { .. }
        | SessionEventKind::ApprovalRequested { .. }
        | SessionEventKind::ProviderInteractionRequested { .. }
        | SessionEventKind::PlanUpdated { .. }
        | SessionEventKind::GoalUpdated { .. }
        | SessionEventKind::GoalCleared { .. }
        | SessionEventKind::ContextCleared
        | SessionEventKind::SubagentActivity { .. }
        | SessionEventKind::PromptRecalled { .. }
        | SessionEventKind::TurnCompleted { .. }
        | SessionEventKind::Error { .. } => true,
    }
}

fn context_compaction_summary(payload: &serde_json::Value) -> String {
    ["summary", "message", "detail"]
        .into_iter()
        .find_map(|field| {
            payload
                .get(field)
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
        })
        .map(|summary| compact_text(summary, 180))
        .unwrap_or_else(|| "Compacting context…".to_string())
}

pub(crate) fn subagent_activity_summary(
    activity: SubagentActivityKind,
    agent: &SubagentSnapshot,
    child_event: Option<&SessionEvent>,
) -> Option<String> {
    let task = &agent.task_name;
    match activity {
        SubagentActivityKind::Started => Some(format!("agent · {task} · started")),
        SubagentActivityKind::Updated => match child_event.map(|event| &event.kind) {
            Some(SessionEventKind::ApprovalRequested { title, detail, .. }) => Some(format!(
                "agent · {task} · needs approval · {}",
                compact_text(
                    if title.trim().is_empty() {
                        detail
                    } else {
                        title
                    },
                    120
                )
            )),
            Some(SessionEventKind::Error { message }) => Some(format!(
                "agent · {task} · error · {}",
                compact_text(message, 120)
            )),
            _ => None,
        },
        SubagentActivityKind::Completed => Some(terminal_agent_summary(
            task,
            "completed",
            agent.final_text.as_deref(),
        )),
        SubagentActivityKind::Stopped => Some(terminal_agent_summary(
            task,
            "stopped",
            agent.final_text.as_deref(),
        )),
        SubagentActivityKind::Failed => Some(format!(
            "agent · {task} · failed{}",
            agent
                .detail
                .as_deref()
                .filter(|detail| !detail.trim().is_empty())
                .map(|detail| format!(" · {}", compact_text(detail, 120)))
                .unwrap_or_default()
        )),
    }
}

fn terminal_agent_summary(task: &str, outcome: &str, final_text: Option<&str>) -> String {
    let result = final_text
        .and_then(|text| text.lines().find(|line| !line.trim().is_empty()))
        .map(|text| format!(" · {}", compact_text(text, 120)))
        .unwrap_or_default();
    format!("agent · {task} · {outcome}{result}")
}

fn normalize_terminal_capture_paste(value: &str) -> Cow<'_, str> {
    let lines = value.lines().collect::<Vec<_>>();
    let nonempty = lines.iter().filter(|line| !line.trim().is_empty()).count();
    let gutter_lines = lines
        .iter()
        .filter(|line| line.trim_end().ends_with('▊'))
        .count();
    if gutter_lines < 3 || gutter_lines * 2 < nonempty {
        return Cow::Borrowed(value);
    }

    Cow::Owned(
        lines
            .into_iter()
            .map(|line| {
                let mut line = line.trim_end();
                while let Some(without_gutter) = line.strip_suffix('▊') {
                    line = without_gutter.trim_end();
                }
                line.trim_start()
            })
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

fn update_queued_prompts(
    queued_prompts: &mut Vec<PendingPromptProjection>,
    event: &SessionEventKind,
) {
    match event {
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text,
            status: MessageStatus::Queued,
            delivery: Some(delivery),
            ..
        } => {
            push_queued_prompt(queued_prompts, *message_id, text.clone(), *delivery);
        }
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            status: MessageStatus::Complete,
            delivery,
            ..
        } => {
            if let Some(admitted) = queued_prompts
                .iter()
                .position(|queued| queued.message_id == *message_id)
            {
                if *delivery == Some(PromptDelivery::Queue) {
                    let mut index = 0;
                    queued_prompts.retain(|queued| {
                        let retain = index > admitted || queued.delivery != PromptDelivery::Queue;
                        index += 1;
                        retain
                    });
                } else {
                    queued_prompts.remove(admitted);
                }
            } else if *delivery == Some(PromptDelivery::Queue) {
                // A later user prompt was admitted while older projected queue
                // entries remained. FIFO admission makes older queued entries
                // stale, while active-turn steers are independent.
                queued_prompts.retain(|queued| queued.delivery != PromptDelivery::Queue);
            }
        }
        SessionEventKind::Message { message_id, .. }
        | SessionEventKind::PromptRecalled { message_id, .. } => {
            queued_prompts.retain(|queued| queued.message_id != *message_id);
        }
        _ => {}
    }
}

fn push_queued_prompt(
    queued_prompts: &mut Vec<PendingPromptProjection>,
    message_id: Uuid,
    text: String,
    delivery: PromptDelivery,
) {
    if let Some(queued) = queued_prompts
        .iter_mut()
        .find(|queued| queued.message_id == message_id)
    {
        queued.text = text;
        queued.delivery = delivery;
    } else {
        queued_prompts.push(PendingPromptProjection {
            message_id,
            text,
            delivery,
        });
    }
}

fn should_recall_latest_queued_prompt(
    composer_text: &str,
    queued_prompts: &[PendingPromptProjection],
) -> bool {
    composer_text.is_empty()
        && queued_prompts
            .iter()
            .any(|prompt| prompt.delivery == PromptDelivery::Queue)
}

fn queued_prompt_panel_height(queued_prompts: &[PendingPromptProjection]) -> u16 {
    if queued_prompts.is_empty() {
        return 0;
    }
    queued_prompts
        .len()
        .min(6)
        .saturating_add(usize::from(queued_prompts.len() > 6))
        // One top-border/title row plus one contextual shortcut row.
        .saturating_add(2)
        .min(u16::MAX as usize) as u16
}

fn queued_prompt_lines(
    queued_prompts: &[PendingPromptProjection],
    panel_width: u16,
) -> Vec<Line<'static>> {
    let visible = queued_prompts.len().min(6);
    let queue_width = panel_width.saturating_sub(26).max(1) as usize;
    let mut lines = queued_prompts
        .iter()
        .take(visible)
        .map(|prompt| {
            let (label, label_color) = match prompt.delivery {
                PromptDelivery::Steer => ("NEXT TOOL", BORG_ORANGE),
                PromptDelivery::Queue => ("NEXT TURN", Color::Gray),
            };
            Line::from(vec![
                Span::styled(" ↳ ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!("{label:<9} "),
                    Style::default()
                        .fg(label_color)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    compact_text(&prompt.text, queue_width),
                    Style::default().fg(Color::Gray),
                ),
            ])
        })
        .collect::<Vec<_>>();
    if queued_prompts.len() > visible {
        lines.push(Line::from(Span::styled(
            format!("   +{} more pending", queued_prompts.len() - visible),
            Style::default().fg(Color::DarkGray),
        )));
    }
    let has_steers = queued_prompts
        .iter()
        .any(|prompt| prompt.delivery == PromptDelivery::Steer);
    let has_queue = queued_prompts
        .iter()
        .any(|prompt| prompt.delivery == PromptDelivery::Queue);
    let mut hints = Vec::new();
    if has_steers {
        hints.push("esc interrupt + send now");
    }
    if has_queue {
        hints.push("↑ edit latest queued");
    }
    lines.push(Line::from(Span::styled(
        format!("   {}", hints.join("  ·  ")),
        Style::default().fg(Color::DarkGray),
    )));
    lines
}

fn local_event_time(event: &SessionEvent) -> String {
    canonical_local_time(event.created_at.with_timezone(&Local))
}

fn canonical_local_time(time: DateTime<Local>) -> String {
    time.format("%Y-%m-%d %H:%M").to_string()
}

fn display_local_time(time: &str, today: NaiveDate) -> Cow<'_, str> {
    let today_prefix = today.format("%Y-%m-%d ").to_string();
    time.strip_prefix(&today_prefix)
        .map(Cow::Borrowed)
        .unwrap_or_else(|| Cow::Borrowed(time))
}

fn terminal_color(value: &str) -> Color {
    let (red, green, blue) =
        parse_hex_color(value).expect("validated editor colour must remain valid");
    Color::Rgb(red, green, blue)
}

fn context_remaining_percent(tokens: u64, window: u64) -> u8 {
    const BASELINE_TOKENS: u64 = 12_000;
    if window <= BASELINE_TOKENS {
        return 0;
    }
    let effective_window = window - BASELINE_TOKENS;
    let used = tokens.saturating_sub(BASELINE_TOKENS);
    let remaining = effective_window.saturating_sub(used);
    ((remaining as f64 / effective_window as f64) * 100.0)
        .clamp(0.0, 100.0)
        .round() as u8
}

impl TranscriptEntry {
    fn copy_text(&self) -> Option<&str> {
        match self {
            Self::Message { text, .. } | Self::Activity { text, .. } | Self::Info { text, .. } => {
                Some(text)
            }
            Self::Compaction { .. } | Self::Plan { .. } | Self::Goal { .. } => None,
            Self::Tool { detail, .. } => Some(detail),
        }
    }

    fn copy_text_owned(&self) -> Option<String> {
        match self {
            Self::Plan { items, .. } => Some(
                items
                    .iter()
                    .map(|item| {
                        let marker = match item.status {
                            PlanItemStatus::Completed => "✓",
                            PlanItemStatus::InProgress => "◌",
                            PlanItemStatus::Pending => "○",
                        };
                        format!("{marker} {}", item.content)
                    })
                    .collect::<Vec<_>>()
                    .join("\n"),
            ),
            Self::Goal { goal, .. } => Some(goal.objective.clone()),
            Self::Tool {
                detail, code_view, ..
            } => Some(
                code_view
                    .as_ref()
                    .map(|(_, source)| source.clone())
                    .unwrap_or_else(|| detail.clone()),
            ),
            _ => self.copy_text().map(str::to_string),
        }
    }
}

fn apply_line_background(line: &mut Line<'static>, width: usize, background: Color) {
    for span in &mut line.spans {
        span.style = span.style.bg(background);
    }
    let fill = width.saturating_sub(line.width());
    line.spans.push(Span::styled(
        " ".repeat(fill),
        Style::default().bg(background),
    ));
}

fn apply_text_selection(
    visible_lines: &mut [Line<'static>],
    scroll_start: usize,
    selection: TextSelection,
) {
    for (viewport_row, line) in visible_lines.iter_mut().enumerate() {
        let row = scroll_start.saturating_add(viewport_row);
        let Some((start, end)) = selection_columns_for_row(selection, row) else {
            continue;
        };
        let mut column = 0usize;
        let mut spans = Vec::new();
        for span in &line.spans {
            for grapheme in span.content.graphemes(true) {
                let grapheme_width = grapheme.width();
                let selected = column < end && column.saturating_add(grapheme_width) > start;
                let style = if selected {
                    span.style.fg(Color::White).bg(Color::Rgb(45, 83, 120))
                } else {
                    span.style
                };
                spans.push(Span::styled(grapheme.to_string(), style));
                column = column.saturating_add(grapheme_width);
            }
        }
        line.spans = spans;
    }
}

fn selected_transcript_text(lines: &[Line<'static>], selection: TextSelection) -> Option<String> {
    let (start, end) = selection.ordered();
    if start == end || start.row >= lines.len() {
        return None;
    }
    let last_row = end.row.min(lines.len().saturating_sub(1));
    let mut selected = Vec::new();
    for (row, line) in lines.iter().enumerate().take(last_row + 1).skip(start.row) {
        let line_width = line
            .spans
            .iter()
            .map(|span| span.content.width())
            .sum::<usize>();
        let from = if row == start.row {
            start.column.min(line_width)
        } else {
            0
        };
        let to = if row == end.row {
            end.column.min(line_width)
        } else {
            line_width
        };
        let mut column = 0usize;
        let mut text = String::new();
        for span in &line.spans {
            for grapheme in span.content.graphemes(true) {
                let grapheme_width = grapheme.width();
                if column < to && column.saturating_add(grapheme_width) > from {
                    text.push_str(grapheme);
                }
                column = column.saturating_add(grapheme_width);
            }
        }
        selected.push(text.trim_end().to_string());
    }
    let text = selected.join("\n");
    (!text.is_empty()).then_some(text)
}

fn selection_columns_for_row(selection: TextSelection, row: usize) -> Option<(usize, usize)> {
    let (start, end) = selection.ordered();
    if row < start.row || row > end.row {
        return None;
    }
    Some(if start.row == end.row {
        (start.column, end.column)
    } else if row == start.row {
        (start.column, usize::MAX)
    } else if row == end.row {
        (0, end.column)
    } else {
        (0, usize::MAX)
    })
}

fn tool_run_separator(in_tool_run: bool) -> Line<'static> {
    if in_tool_run {
        Line::from(Span::styled("│", Style::default().fg(Color::DarkGray)))
    } else {
        Line::default()
    }
}

fn preserve_scroll_anchor(
    scroll_from_bottom: usize,
    previous_height: usize,
    next_height: usize,
) -> usize {
    if next_height >= previous_height {
        scroll_from_bottom.saturating_add(next_height - previous_height)
    } else {
        scroll_from_bottom.saturating_sub(previous_height - next_height)
    }
}

fn transcript_viewport_anchor(
    tool_rows: &[RowRange],
    entry_rows: &[RowRange],
    scroll_max: usize,
    scroll_from_bottom: usize,
    viewport_height: usize,
    collapsing: bool,
) -> Option<TranscriptViewportAnchor> {
    let scroll_start = scroll_max.saturating_sub(scroll_from_bottom.min(scroll_max));
    let viewport_row = viewport_height.saturating_sub(1) / 2;
    let row = scroll_start.saturating_add(viewport_row);
    let entry = entry_rows
        .iter()
        .find(|(_, start, end)| *start <= row && row < *end)
        .copied()
        .or_else(|| {
            tool_rows
                .iter()
                .find(|(_, start, end)| *start <= row && row < *end)
                .copied()
        })?;
    let (entry_index, entry_start, _) = entry;
    let collapsed_tool_header = if collapsing {
        tool_rows
            .iter()
            .find(|(_, start, end)| *start < row && row < *end)
            .map(|(index, _, _)| *index)
    } else {
        None
    };
    Some(TranscriptViewportAnchor {
        entry_index: *entry_index,
        entry_row_offset: row.saturating_sub(*entry_start),
        viewport_row,
        collapsed_tool_header,
    })
}

fn restore_transcript_viewport_anchor(
    anchor: TranscriptViewportAnchor,
    tool_rows: &[RowRange],
    entry_rows: &[RowRange],
    transcript_height: usize,
    viewport_height: usize,
    current_scroll_from_bottom: usize,
) -> usize {
    let scroll_max = transcript_height.saturating_sub(viewport_height);
    let target_row = anchor
        .collapsed_tool_header
        .and_then(|index| {
            tool_rows
                .iter()
                .find(|(candidate, _, _)| *candidate == index)
                .map(|(_, start, _)| *start)
        })
        .or_else(|| {
            entry_rows
                .iter()
                .find(|(index, _, _)| *index == anchor.entry_index)
                .map(|(_, start, end)| {
                    start
                        .saturating_add(anchor.entry_row_offset.min(end.saturating_sub(*start + 1)))
                })
        });
    let Some(target_row) = target_row else {
        return current_scroll_from_bottom.min(scroll_max);
    };
    let scroll_start = target_row
        .saturating_sub(anchor.viewport_row)
        .min(scroll_max);
    scroll_max.saturating_sub(scroll_start)
}

fn fish_style_path(path: &Path) -> String {
    let components = path.components().collect::<Vec<_>>();
    let final_directory = components
        .iter()
        .rposition(|component| matches!(component, std::path::Component::Normal(_)));
    let mut shortened = PathBuf::new();
    for (index, component) in components.into_iter().enumerate() {
        match component {
            std::path::Component::Normal(name) if Some(index) != final_directory => {
                let name = name.to_string_lossy();
                let grapheme_count = if name.starts_with('.') { 2 } else { 1 };
                let abbreviation = name
                    .graphemes(true)
                    .take(grapheme_count)
                    .collect::<String>();
                shortened.push(abbreviation);
            }
            std::path::Component::Normal(name) => shortened.push(name),
            std::path::Component::Prefix(prefix) => shortened.push(prefix.as_os_str()),
            std::path::Component::RootDir => shortened.push(std::path::MAIN_SEPARATOR.to_string()),
            std::path::Component::CurDir => shortened.push("."),
            std::path::Component::ParentDir => shortened.push(".."),
        }
    }
    shortened.display().to_string()
}

fn slash_matches(value: &str) -> Vec<&'static (&'static str, &'static str)> {
    let value = value.trim();
    if !value.starts_with('/') || value.contains(char::is_whitespace) {
        return Vec::new();
    }
    SLASH_COMMANDS
        .iter()
        .filter(|(command, _)| command.starts_with(value))
        .collect()
}

fn slash_selected_command(value: &str, selected: usize) -> Option<&'static str> {
    slash_matches(value)
        .get(selected)
        .map(|(command, _)| *command)
}

fn slash_help(matches: &[&(&str, &str)]) -> String {
    matches
        .iter()
        .take(5)
        .map(|(command, help)| format!("{command} {help}"))
        .collect::<Vec<_>>()
        .join(" · ")
}

fn primary_controls_line(keymap: &KeyMap) -> String {
    format!(
        "send {} · queue {} · commands / · keybindings {}",
        keymap.label(KeyAction::Send),
        keymap.label(KeyAction::Queue),
        keymap.label(KeyAction::Keybindings)
    )
}

fn inset_control_lines(mut lines: Vec<Line<'static>>) -> Vec<Line<'static>> {
    for line in &mut lines {
        line.spans.insert(0, Span::raw(" "));
    }
    lines
}

fn keybinding_lines(keymap: &KeyMap, width: usize) -> Vec<Line<'static>> {
    let bindings = [
        ("send", keymap.label(KeyAction::Send)),
        ("queue next turn", keymap.label(KeyAction::Queue)),
        ("newline", keymap.label(KeyAction::Newline)),
        ("commands", "/".to_string()),
        ("interrupt or close", keymap.label(KeyAction::Interrupt)),
        ("clear · twice exit", keymap.label(KeyAction::ClearOrExit)),
        ("exit", keymap.label(KeyAction::Exit)),
        ("attach image", keymap.label(KeyAction::AttachImage)),
        ("copy selection/response", keymap.label(KeyAction::Copy)),
        (
            "scroll transcript",
            format!(
                "{}/{}",
                keymap.label(KeyAction::ScrollUp),
                keymap.label(KeyAction::ScrollDown)
            ),
        ),
        (
            "select transcript entry",
            format!(
                "{}/{}",
                keymap.label(KeyAction::SelectPrevious),
                keymap.label(KeyAction::SelectNext)
            ),
        ),
        ("select terminal text", "shift+drag".to_string()),
    ];
    let binding = |(action, key): &(&str, String)| format!("{action:<26} {key}");
    let mut lines = Vec::new();
    if width >= 76 {
        for pair in bindings.chunks(2) {
            let left = binding(&pair[0]);
            let text = pair.get(1).map_or(left.clone(), |right| {
                format!(" {left:<43} {}", binding(right))
            });
            lines.push(Line::from(text));
        }
    } else {
        for binding_pair in &bindings {
            lines.extend(
                wrap_display(&format!(" {}", binding(binding_pair)), width.max(1))
                    .into_iter()
                    .map(Line::from),
            );
        }
    }
    lines
}

fn slash_suggestion_lines(value: &str, selected: usize) -> Vec<Line<'static>> {
    let matches = slash_matches(value);
    const VISIBLE_SUGGESTIONS: usize = 5;
    let selected = selected.min(matches.len().saturating_sub(1));
    let start = selected
        .saturating_sub(VISIBLE_SUGGESTIONS - 1)
        .min(matches.len().saturating_sub(VISIBLE_SUGGESTIONS));
    let visible = &matches[start..matches.len().min(start + VISIBLE_SUGGESTIONS)];
    let command_width = visible
        .iter()
        .map(|(command, _)| command.len())
        .max()
        .unwrap_or(0);

    visible
        .iter()
        .enumerate()
        .map(|(index, (command, help))| {
            let is_selected = start + index == selected;
            let row_style = if is_selected {
                Style::default().fg(Color::White).bg(MESSAGE_HOVER_BG)
            } else {
                Style::default().fg(Color::Gray)
            };
            let marker_style = Style::default()
                .fg(if is_selected {
                    BORG_ORANGE
                } else {
                    Color::DarkGray
                })
                .bg(if is_selected {
                    MESSAGE_HOVER_BG
                } else {
                    Color::Reset
                });
            Line::from(vec![
                Span::styled(if is_selected { " › " } else { "   " }, marker_style),
                Span::styled(
                    format!("{command:<command_width$}"),
                    row_style.add_modifier(Modifier::BOLD),
                ),
                Span::styled("   ", row_style),
                Span::styled(*help, row_style),
            ])
        })
        .collect()
}

fn terminal_content_width(terminal_width: u16) -> u16 {
    terminal_width
        .saturating_sub(HORIZONTAL_MARGIN.saturating_mul(2))
        .max(1)
}

fn responsive_launch_width(available: u16) -> u16 {
    if available < 70 {
        available
    } else {
        available.saturating_mul(3) / 5
    }
    .max(1)
}

fn centered_content_area(area: Rect) -> Rect {
    let width = terminal_content_width(area.width);
    Rect {
        x: area.x.saturating_add(area.width.saturating_sub(width) / 2),
        y: area.y,
        width,
        height: area.height,
    }
}

fn tool_run_viewport_height(viewport_height: usize) -> usize {
    (viewport_height / 3)
        .saturating_sub(TOOL_RUN_CHROME_HEIGHT)
        .clamp(MIN_TOOL_RUN_VIEWPORT_HEIGHT, MAX_TOOL_RUN_VIEWPORT_HEIGHT)
}

fn wheel_scroll_lines(viewport_height: u16) -> isize {
    usize::from(viewport_height)
        .div_ceil(WHEEL_SCROLL_VIEWPORT_DIVISOR)
        .clamp(
            MIN_WHEEL_SCROLL_LINES_PER_EVENT,
            MAX_WHEEL_SCROLL_LINES_PER_EVENT,
        ) as isize
}

fn visible_row_ranges(
    rows: &[(usize, usize, usize)],
    scroll_start: usize,
    visible_height: usize,
) -> &[(usize, usize, usize)] {
    let visible_end = scroll_start.saturating_add(visible_height);
    let first = rows.partition_point(|(_, _, end)| *end <= scroll_start);
    let count = rows[first..].partition_point(|(_, start, _)| *start < visible_end);
    &rows[first..first + count]
}

fn nested_scroll_consumed(
    capture: &mut Option<NestedScrollCapture>,
    tool_run_start: usize,
    direction: isize,
    moved: bool,
    now: Instant,
) -> bool {
    let continues_captured_gesture = capture.is_some_and(|capture| {
        capture.tool_run_start == tool_run_start
            && capture.direction == direction
            && now.saturating_duration_since(capture.last_event) <= NESTED_SCROLL_GESTURE_GAP
    });
    if moved || continues_captured_gesture {
        *capture = Some(NestedScrollCapture {
            tool_run_start,
            direction,
            last_event: now,
        });
        true
    } else {
        *capture = None;
        false
    }
}

fn viewport_hit_area(
    area: Rect,
    scroll_start: usize,
    entry_start: usize,
    entry_end: usize,
) -> Rect {
    let visible_start = entry_start.saturating_sub(scroll_start);
    let visible_end = entry_end.saturating_sub(scroll_start);
    let row = visible_start.min(area.height.saturating_sub(1) as usize) as u16;
    let height = visible_end
        .min(area.height as usize)
        .saturating_sub(visible_start)
        .max(1) as u16;
    Rect {
        x: area.x,
        y: area.y + row,
        width: area.width,
        height,
    }
}

fn apply_viewport_background(
    lines: &mut [Line<'static>],
    entry_start: usize,
    entry_end: usize,
    scroll_start: usize,
    width: usize,
    background: Color,
) {
    let start = entry_start.saturating_sub(scroll_start).min(lines.len());
    let end = entry_end.saturating_sub(scroll_start).min(lines.len());
    for line in &mut lines[start..end] {
        apply_line_background(line, width, background);
    }
}

fn centered_popup(area: Rect, preferred_width: u16, preferred_height: u16) -> Rect {
    let width = preferred_width.min(area.width.saturating_sub(2)).max(1);
    let height = preferred_height.min(area.height.saturating_sub(2)).max(1);
    Rect {
        x: area.x + area.width.saturating_sub(width) / 2,
        y: area.y + area.height.saturating_sub(height) / 2,
        width,
        height,
    }
}

fn wrap_display(value: &str, width: usize) -> Vec<String> {
    display_ranges(value, width, false)
        .into_iter()
        .map(|(start, end)| value[start..end].to_string())
        .collect()
}

fn tool_summary_lines(
    summary: &str,
    elapsed: Option<&str>,
    prefix: &str,
    width: usize,
) -> Vec<String> {
    let content_width = width.saturating_sub(UnicodeWidthStr::width(prefix));
    let Some(elapsed) = elapsed else {
        return wrap_display(summary, content_width.max(1));
    };
    let elapsed_width = UnicodeWidthStr::width(elapsed);
    let reserved_width = elapsed_width.saturating_add(2);
    if content_width <= reserved_width {
        return wrap_display(&format!("{summary} · {elapsed}"), content_width.max(1));
    }

    let first_width = content_width - reserved_width;
    let Some((first_start, first_end)) = display_ranges(summary, first_width, false)
        .into_iter()
        .next()
    else {
        return vec![format!("{:>content_width$}", elapsed)];
    };
    let mut lines = vec![summary[first_start..first_end].to_string()];
    let remaining = summary[first_end..].trim_start();
    if !remaining.is_empty() {
        lines.extend(wrap_display(remaining, content_width));
    }
    if let Some(first) = lines.first_mut() {
        let padding = content_width
            .saturating_sub(UnicodeWidthStr::width(first.as_str()))
            .saturating_sub(elapsed_width);
        first.push_str(&" ".repeat(padding));
        first.push_str(elapsed);
    }
    lines
}

fn format_tool_elapsed(
    started_at: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,
) -> Option<String> {
    let elapsed_ms = completed_at
        .unwrap_or_else(Utc::now)
        .signed_duration_since(started_at)
        .num_milliseconds()
        .max(0) as u64;
    if elapsed_ms < 100 {
        return None;
    }
    if elapsed_ms < 60_000 {
        return Some(format!("{:.1}s", elapsed_ms as f64 / 1_000.0));
    }

    let total_seconds = elapsed_ms / 1_000;
    let seconds = total_seconds % 60;
    let total_minutes = total_seconds / 60;
    if total_minutes < 60 {
        return Some(format!("{total_minutes}m {seconds:02}s"));
    }

    let minutes = total_minutes % 60;
    let total_hours = total_minutes / 60;
    if total_hours < 24 {
        return Some(format!("{total_hours}h {minutes:02}m"));
    }

    let days = total_hours / 24;
    let hours = total_hours % 24;
    Some(format!("{days}d {hours:02}h"))
}

fn composer_cursor_position(value: &str, cursor: usize, width: usize) -> (usize, usize) {
    let ranges = display_ranges(value, width, true);
    composer_cursor_position_in_ranges(value, cursor, &ranges)
}

fn composer_cursor_position_in_ranges(
    value: &str,
    cursor: usize,
    ranges: &[(usize, usize)],
) -> (usize, usize) {
    let mut position = (0, 0);
    for (row, (start, end)) in ranges.iter().copied().enumerate() {
        if cursor < start || cursor > end {
            continue;
        }
        // At a soft-wrap boundary, the later range is the visual authority.
        position = (row, UnicodeWidthStr::width(&value[start..cursor.min(end)]));
    }
    position
}

fn composer_cursor_x_offset(is_launch_screen: bool) -> u16 {
    u16::from(is_launch_screen) + 3
}

fn provider_interaction_options(payload: &serde_json::Value) -> String {
    payload
        .get("questions")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .flat_map(|question| {
            question
                .get("options")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|option| option.get("label").and_then(serde_json::Value::as_str))
        })
        .collect::<Vec<_>>()
        .join(" · ")
}

fn provider_interaction_contains_secret(payload: &serde_json::Value) -> bool {
    payload
        .get("questions")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|questions| {
            questions.iter().any(|question| {
                question
                    .get("isSecret")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false)
            })
        })
}

fn mask_secret_composer_text(value: &str, cursor: usize) -> (String, usize) {
    let mut masked = String::new();
    let mut masked_cursor = 0;
    for (start, grapheme) in value.grapheme_indices(true) {
        if start == cursor {
            masked_cursor = masked.len();
        }
        if grapheme == "\n" {
            masked.push('\n');
        } else {
            masked.push('•');
        }
        if start + grapheme.len() == cursor {
            masked_cursor = masked.len();
        }
    }
    if cursor == value.len() {
        masked_cursor = masked.len();
    }
    (masked, masked_cursor)
}

fn styled_plain_composer_lines(
    value: &str,
    ranges: &[(usize, usize)],
    prompt_marker: &str,
) -> Vec<Line<'static>> {
    ranges
        .iter()
        .copied()
        .enumerate()
        .map(|(row, (start, end))| {
            let prefix = if row == 0 {
                prompt_marker.to_string()
            } else {
                " ".repeat(UnicodeWidthStr::width(prompt_marker))
            };
            Line::from(vec![
                Span::styled(prefix, Style::default().fg(Color::White)),
                Span::styled(
                    value[start..end].to_string(),
                    Style::default().fg(Color::White),
                ),
            ])
        })
        .collect()
}

fn cursor_at_column(value: &str, start: usize, end: usize, column: usize) -> usize {
    let mut cursor = start;
    let mut cells = 0usize;
    for (offset, grapheme) in value[start..end].grapheme_indices(true) {
        let width = UnicodeWidthStr::width(grapheme);
        if cells.saturating_add(width) > column {
            break;
        }
        cells = cells.saturating_add(width);
        cursor = start + offset + grapheme.len();
    }
    cursor
}

fn display_ranges(value: &str, width: usize, reserve_final_caret: bool) -> Vec<(usize, usize)> {
    let width = width.max(1);
    let mut ranges = Vec::new();
    let mut offset = 0;
    for source in value.split_inclusive('\n') {
        let line = source.strip_suffix('\n').unwrap_or(source);
        ranges.extend(line_ranges(line, offset, width));
        offset = offset.saturating_add(source.len());
        if source.ends_with('\n') && offset == value.len() {
            ranges.push((offset, offset));
        }
    }
    if value.is_empty() {
        ranges.push((0, 0));
    } else if reserve_final_caret && !value.ends_with('\n') {
        let last = ranges.last().copied().unwrap_or((0, 0));
        if last.1 == value.len() && UnicodeWidthStr::width(&value[last.0..last.1]) >= width {
            // A caret after a completely full final row belongs on the next
            // visual row. Reserving it now prevents the prompt box jumping
            // only after the next character is entered.
            ranges.push((value.len(), value.len()));
        }
    }
    ranges
}

fn line_ranges(line: &str, offset: usize, width: usize) -> Vec<(usize, usize)> {
    let graphemes = line
        .grapheme_indices(true)
        .map(|(start, value)| {
            (
                start,
                start + value.len(),
                UnicodeWidthStr::width(value),
                value.chars().all(char::is_whitespace),
            )
        })
        .collect::<Vec<_>>();
    if graphemes.is_empty() {
        return vec![(offset, offset)];
    }
    let mut output = Vec::new();
    let mut start = 0;
    while start < graphemes.len() {
        let mut end = start;
        let mut cells = 0usize;
        while end < graphemes.len() {
            let next = cells.saturating_add(graphemes[end].2);
            if end > start && next > width {
                break;
            }
            cells = next;
            end += 1;
            if cells >= width {
                break;
            }
        }
        if end < graphemes.len()
            && let Some(space) = (start..end)
                .rev()
                .find(|index| graphemes[*index].3 && *index > start)
        {
            end = space + 1;
        }
        let byte_start = graphemes[start].0;
        let byte_end = graphemes[end - 1].1;
        output.push((offset + byte_start, offset + byte_end));
        start = end;
    }
    output
}

fn display_name(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("image")
        .to_string()
}

fn status_label(status: SessionStatus) -> &'static str {
    match status {
        SessionStatus::Starting => "starting",
        SessionStatus::Ready => "ready",
        SessionStatus::Running => "running",
        SessionStatus::WaitingForApproval => "approval",
        SessionStatus::Completed => "complete",
        SessionStatus::Failed => "failed",
        SessionStatus::Stopped => "stopped",
    }
}

fn push_status_segment(spans: &mut Vec<Span<'static>>, value: String, color: Color) {
    if !value.is_empty() {
        spans.push(Span::styled(
            format!(" · {value}"),
            Style::default().fg(color),
        ));
    }
}

fn terminal_title(status: SessionStatus, first_prompt: Option<&str>) -> String {
    let prompt = first_prompt
        .map(|prompt| prompt.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|prompt| !prompt.is_empty())
        .map(|prompt| prompt.chars().take(48).collect::<String>());
    let prefix = if matches!(status, SessionStatus::Starting | SessionStatus::Running) {
        format!("{} Borg CLI", activity_glyph(status))
    } else {
        "Borg CLI".to_string()
    };
    prompt.map_or(prefix.clone(), |prompt| format!("{prefix} - {prompt}..."))
}

fn borging_for_run(seed: Uuid) -> bool {
    seed.as_u128().is_multiple_of(100)
}

fn format_elapsed_duration(total_seconds: u64) -> Option<String> {
    if total_seconds < 60 {
        return None;
    }
    let days = total_seconds / 86_400;
    let hours = total_seconds % 86_400 / 3_600;
    let minutes = total_seconds % 3_600 / 60;
    let mut parts = Vec::new();
    if days > 0 {
        parts.push(format!("{days}d"));
    }
    if hours > 0 || days > 0 {
        parts.push(format!("{hours}h"));
    }
    if minutes > 0 || hours > 0 || days > 0 {
        parts.push(format!("{minutes}m"));
    }
    Some(parts.join(" "))
}

fn activity_glyph(status: SessionStatus) -> &'static str {
    if !matches!(status, SessionStatus::Starting | SessionStatus::Running) {
        return "●";
    }
    const FRAMES: [&str; 8] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧"];
    FRAMES[spinner_frame_index()]
}

fn spinner_frame_index() -> usize {
    let frame = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() / 120);
    frame as usize % 8
}

fn cursor_blink_visible(elapsed: Duration) -> bool {
    (elapsed.as_millis() / 500).is_multiple_of(2)
}
