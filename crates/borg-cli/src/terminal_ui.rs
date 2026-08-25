mod attachments;
mod cache_diagnostics;
mod clipboard;
mod markdown;
mod rendering;
mod terminal_input;
#[cfg(test)]
mod tests;

use std::borrow::Cow;
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::io::{self, Stdout};
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use crate::editor_preferences::{DictationIconStyle, TranscriptPreferences, parse_hex_color};
use anyhow::{Context, Result};
use attachments::{AttachmentStore, PasteOutcome};
use borg_remote::{
    ApprovalDecision, CodingProvider, EventActor, GoalAction, GoalStatus, MessageStatus,
    PermissionMode, PlanItem, PlanItemStatus, PromptDelivery, ResponseLanguage, SessionEvent,
    SessionEventKind, SessionGoal, SessionPayloadKind, SessionPayloadRef, SessionState,
    SessionStatus, SubagentActivityKind, SubagentSnapshot, SubagentStatus,
    ToolPresentationCategory, compact_text, is_diff_language, is_edit_tool, is_mcp_resource_probe,
    is_subagent_tool, project_tool_presentation, tool_action_is_instant, tool_has_rich_ui,
    tool_output_code_view, tool_output_is_backgrounded, web_search_query,
};
#[cfg(test)]
use borg_remote::{tool_call_summary, tool_code_view};
use chrono::{DateTime, Local, NaiveDate, Utc};
use crossterm::cursor::SetCursorStyle;
use crossterm::event::{
    DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture, Event,
    KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, SetTitle, disable_raw_mode, enable_raw_mode,
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
use ratatui::widgets::{Block, Borders, Clear, Padding, Paragraph};
use ratatui::{TerminalOptions, Viewport};
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;
use uuid::Uuid;

use self::cache_diagnostics::{CacheDiagnostics, CacheSignature, CacheStatus, CacheUsage};
use self::markdown::{
    markdown_lines, markdown_link_ranges, markdown_plain_text, open_http_link, truncate_table_cell,
};
use self::terminal_input::TerminalInput;
pub(crate) use self::terminal_input::TerminalInputEvent;
use crate::agent_config::KeybindingConfig;

const INLINE_VIEWPORT_HEIGHT: u16 = 24;
const HORIZONTAL_MARGIN: u16 = 0;
const BORG_ORANGE: Color = Color::Rgb(255, 142, 36);
const BORG_ORANGE_HOVER: Color = Color::Rgb(255, 184, 92);
const RUNNING_STATUS_PEACH: Color = Color::Rgb(255, 132, 112);
const SUBAGENT_PINK: Color = Color::Rgb(255, 105, 180);
const SUBAGENT_PINK_HOVER: Color = Color::Rgb(255, 170, 215);
const USER_LABEL_BLUE: Color = Color::Rgb(74, 163, 255);
const USER_TEXT: Color = Color::Rgb(198, 228, 255);
const MESSAGE_BG: Color = Color::Rgb(33, 25, 29);
const MESSAGE_HOVER_BG: Color = Color::Rgb(48, 36, 41);
const MESSAGE_HORIZONTAL_PADDING: usize = 2;
const PARALLEL_MARKDOWN_RENDER_MIN_MESSAGES: usize = 512;
const MAX_PARALLEL_MARKDOWN_RENDER_WORKERS: usize = 16;
const COMMAND_PANEL_BG: Color = Color::Rgb(31, 24, 27);
/// Divider between status-line segments. It is its own span so a hovered
/// segment underlines its own text only.
const STATUS_SEPARATOR: &str = " · ";
const GIT_STATUS_REFRESH_INTERVAL: Duration = Duration::from_secs(2);
const GOAL_CLEAR_COMMAND: &str = "/goal clear";
const DIRECTOR_CONTEXT_BOUNDARY: &str = "— context provided by director agent —";
const DOUBLE_CTRL_C_WINDOW: Duration = Duration::from_secs(1);
const COPY_NOTICE_DURATION: Duration = Duration::from_secs(5);
const SPLASH_ANIMATION_DURATION: Duration = Duration::from_millis(1_500);
const NESTED_SCROLL_GESTURE_GAP: Duration = Duration::from_millis(200);
const WHEEL_SCROLL_VIEWPORT_DIVISOR: usize = 6;
const MIN_WHEEL_SCROLL_LINES_PER_EVENT: usize = 1;
const MAX_WHEEL_SCROLL_LINES_PER_EVENT: usize = 12;
const MAX_WHEEL_SCROLL_LINES_PER_FRAME: isize = 8;
const WHEEL_SCROLL_EASING_DIVISOR: usize = 8;
const NESTED_WHEEL_SCROLL_FULL_HEIGHT_ROWS: usize = 72;
const MAX_PENDING_WHEEL_SCROLL_LINES: isize = 160;
const TOOL_RUN_BOX_THRESHOLD: usize = 8;
const MAX_COLLAPSED_PLAN_ITEMS: usize = 5;

#[cfg(test)]
const DEFAULT_TOOL_RUN_VIEWPORT_HEIGHT: usize = 8;
const MIN_TOOL_RUN_VIEWPORT_HEIGHT: usize = 6;
const MAX_TOOL_RUN_VIEWPORT_HEIGHT: usize = 30;
const TOOL_RUN_CHROME_HEIGHT: usize = 2;
const MIN_SCROLLBAR_THUMB_ROWS: u16 = 5;
const TRANSCRIPT_SCROLLBAR_GUTTER_WIDTH: u16 = 3;
const LARGE_PASTE_CHAR_THRESHOLD: usize = 1000;
const DICTATION_BUTTON_WIDTH: u16 = 6;
const DICTATION_EMOJI_ICON: &str = "🎤";
const DICTATION_NERD_FONT_ICON: &str = "󰍬";
const SELECTION_AUTOSCROLL_LINES_PER_FRAME: usize = 2;
type RowRange = (usize, usize, usize);
type ToolRunRowRange = (usize, usize, usize, usize, bool);
/// The visible slice of one selectable transcript entry.
///
/// `body_start` is the entry-relative row shown at `start`. Nested action
/// accordions track that row directly because clipping and sticky headers can
/// expose several disjoint slices of one entry. Ordinary transcript entries
/// additionally use text offsets so selections survive wrapping and streaming.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SelectionRowRange {
    entry: usize,
    start: usize,
    end: usize,
    body_start: usize,
    uses_logical_offsets: bool,
}

impl SelectionRowRange {
    const fn new(
        entry: usize,
        start: usize,
        end: usize,
        body_start: usize,
        uses_logical_offsets: bool,
    ) -> Self {
        Self {
            entry,
            start,
            end,
            body_start,
            uses_logical_offsets,
        }
    }

    const fn transcript_entry(entry: usize, start: usize, end: usize) -> Self {
        Self::new(entry, start, end, 0, true)
    }

    const fn nested_entry(entry: usize, start: usize, end: usize, body_start: usize) -> Self {
        Self::new(entry, start, end, body_start, false)
    }

    fn body_end(self) -> usize {
        self.body_start
            .saturating_add(self.end.saturating_sub(self.start))
    }
}
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
    Vec<SelectionRowRange>,
);
type CachedTranscriptRender = (
    usize,
    usize,
    Option<i64>,
    Option<i64>,
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
    Dictate,
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
    dictate: Vec<KeyChord>,
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
            dictate: parse_key_chords(&config.dictate)?,
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
            KeyAction::Dictate => &self.dictate,
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

/// A selection endpoint anchored to a transcript entry and a row within that
/// entry's rendered body. Unlike an absolute transcript row, this survives
/// reflows that move content between fixed row indices (the actions accordion
/// window following new output, nested scrolling, streaming growth above or
/// below, and so on).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SelectionPoint {
    entry: usize,
    row_in_entry: usize,
    column: usize,
    /// A rendered-body offset used to keep a released selection on the same
    /// text when a streaming message rewraps.  The row/column coordinates are
    /// still retained for entries whose body cannot be mapped stably.
    logical_offset: Option<usize>,
}

#[derive(Clone, Copy, Debug)]
struct TextSelection {
    anchor: SelectionPoint,
    focus: SelectionPoint,
    dragging: bool,
    autoscroll: isize,
    pointer: Position,
}

impl TextSelection {
    fn is_empty(self) -> bool {
        self.anchor == self.focus
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ComposerSelection {
    anchor: usize,
    focus: usize,
    dragging: bool,
    pointer: Position,
}

impl ComposerSelection {
    fn is_empty(self) -> bool {
        self.anchor == self.focus
    }
}

#[derive(Clone, Debug)]
enum PendingTranscriptClick {
    Link(String),
    ToolRunHeader(usize),
    Tool {
        index: usize,
        run: Option<(usize, usize)>,
    },
    Message(usize),
    Entry(usize),
    Background,
}

const ACTIVE_MESSAGES_SEND_NOW: &str = "Send now and redirect the current turn";
const ACTIVE_MESSAGES_WAIT: &str = "Wait and send after the current turn finishes";

const SLASH_COMMANDS: &[(&str, &str)] = &[
    ("/help", "show commands"),
    ("/copy", "copy the last assistant message"),
    (
        "/ask",
        "ask another model through its persistent peer thread",
    ),
    ("/director", "send text to the persistent director thread"),
    ("/claude", "ask the active model to consult its Claude peer"),
    ("/gpt", "ask the active model to consult its GPT peer"),
    ("/peer", "message, clear, or replace a persistent peer"),
    ("/settings", "view interactive session settings"),
    ("/model", "choose the model"),
    ("/effort", "choose reasoning effort"),
    ("/language", "choose response and drafting language"),
    ("/lsp", "view language server support"),
    ("/extensions", "view the live Blu extension runtime"),
    ("/fast", "toggle provider priority/fast mode"),
    (
        "/followups",
        "choose message delivery: redirect now or wait for this turn to finish",
    ),
    ("/refresh", "choose terminal refresh rate"),
    ("/sleep", "keep the machine awake during active turns"),
    ("/expand-edits", "auto-expand edit diffs"),
    ("/expand-tools", "auto-expand other tool details"),
    ("/icons", "choose the dictation icon"),
    ("/colors", "view configurable transcript colours"),
    ("/color", "set a transcript colour"),
    ("/usage", "view account limits and session usage"),
    ("/clear", "clear conversation context"),
    ("/compact", "compact the current conversation context"),
    ("/resume", "resume a saved Borg session"),
    ("/goal", "view or update the durable goal"),
    ("/todo", "view or update the durable todo list"),
    ("/todos", "alias for /todo"),
    ("/dictate", "start or stop local dictation"),
    ("/queue", "send after the current turn finishes"),
    ("/steer", "send now and redirect the current turn"),
    ("/interrupt", "interrupt the current turn"),
    ("/stop", "alias for /interrupt"),
    ("/login", "reconnect the current provider"),
    ("/remote", "connect this machine to your Borg account"),
    ("/collab", "share this live session"),
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

pub(crate) fn dictation_icon_style_for_preference(
    preference: Option<DictationIconStyle>,
) -> DictationIconStyle {
    dictation_icon_from_environment()
        .or(preference)
        .unwrap_or(DictationIconStyle::Emoji)
}

fn dictation_icon_from_environment() -> Option<DictationIconStyle> {
    match std::env::var("BORG_TUI_NERD_FONT")
        .ok()?
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "1" | "true" | "yes" | "on" => Some(DictationIconStyle::NerdFont),
        "0" | "false" | "no" | "off" => Some(DictationIconStyle::Emoji),
        _ => None,
    }
}

fn dictation_icon(style: DictationIconStyle) -> &'static str {
    match style {
        DictationIconStyle::NerdFont => DICTATION_NERD_FONT_ICON,
        DictationIconStyle::Emoji => DICTATION_EMOJI_ICON,
    }
}

#[derive(Debug)]
pub enum UiAction {
    None,
    ToggleGoal {
        action: GoalAction,
    },
    Submit {
        target: Option<Uuid>,
        text: String,
        attachments: Vec<PathBuf>,
    },
    Queue {
        target: Option<Uuid>,
        message_id: Uuid,
        text: String,
        attachments: Vec<PathBuf>,
    },
    Approve {
        target: Option<Uuid>,
        decision: ApprovalDecision,
    },
    RecallQueuedPrompts {
        target: Option<Uuid>,
    },
    Rewind {
        sequence: u64,
        text: String,
        attachments: Vec<PathBuf>,
    },
    /// Fork the session immediately after a completed compaction checkpoint.
    RevertTo {
        sequence: u64,
    },
    SetModel(String),
    /// The user picked a model from a provider whose credentials are missing
    /// and chose how to supply them.
    AuthenticateProvider {
        provider: CodingProvider,
        model: String,
        choice: ProviderAuthChoice,
    },
    SetEffort(String),
    SetPermissionMode(PermissionMode),
    SetResponseLanguage(ResponseLanguage),
    SetFast(bool),
    SetRefreshRate(u64),
    SetPreventSleep(bool),
    SetSteerActive(bool),
    SetAutoExpandEdits(bool),
    SetAutoExpandTools(bool),
    SetDictationIcon(DictationIconStyle),
    ToggleDictation,
    LoadPayloads(Vec<SessionPayloadRef>),
    Interrupt {
        target: Option<Uuid>,
    },
    /// A repeated Ctrl-C is an explicit request to return control to the
    /// parent shell now. It must not be converted into a background handoff.
    ForceQuit,
    Quit,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum DictationState {
    #[default]
    Idle,
    Installing,
    Recording,
    Transcribing,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct GitWorktreeStatus {
    branch: String,
    dirty: bool,
    ahead: usize,
    behind: usize,
}

impl GitWorktreeStatus {
    fn compact_label(&self) -> String {
        let mut segments = vec![format!("git:{}", self.branch)];
        if self.ahead > 0 {
            segments.push(format!("↑{}", self.ahead));
        }
        if self.behind > 0 {
            segments.push(format!("↓{}", self.behind));
        }
        if self.dirty {
            segments.push("dirty".to_string());
        }
        segments.join(STATUS_SEPARATOR)
    }
}

struct CachedGitStatus {
    value: Option<GitWorktreeStatus>,
    refreshed_at: Instant,
}

struct GitStatusResult {
    cwd: PathBuf,
    value: Option<GitWorktreeStatus>,
}

struct GitStatusCache {
    values: HashMap<PathBuf, CachedGitStatus>,
    refreshing: HashSet<PathBuf>,
    sender: mpsc::Sender<GitStatusResult>,
    receiver: mpsc::Receiver<GitStatusResult>,
}

impl Default for GitStatusCache {
    fn default() -> Self {
        let (sender, receiver) = mpsc::channel();
        Self {
            values: HashMap::new(),
            refreshing: HashSet::new(),
            sender,
            receiver,
        }
    }
}

impl GitStatusCache {
    fn status_for(&mut self, cwd: &Path) -> Option<&GitWorktreeStatus> {
        while let Ok(result) = self.receiver.try_recv() {
            self.refreshing.remove(&result.cwd);
            self.values.insert(
                result.cwd,
                CachedGitStatus {
                    value: result.value,
                    refreshed_at: Instant::now(),
                },
            );
        }

        let needs_refresh = self
            .values
            .get(cwd)
            .is_none_or(|cached| cached.refreshed_at.elapsed() >= GIT_STATUS_REFRESH_INTERVAL);
        if needs_refresh && self.refreshing.insert(cwd.to_path_buf()) {
            let cwd = cwd.to_path_buf();
            let sender = self.sender.clone();
            thread::spawn(move || {
                let value = read_git_worktree_status(&cwd);
                let _ = sender.send(GitStatusResult { cwd, value });
            });
        }
        self.values
            .get(cwd)
            .and_then(|cached| cached.value.as_ref())
    }
}

pub struct BorgTerminal {
    terminal: Terminal<CrosstermBackend<Stdout>>,
    input: TerminalInput,
    mode: ScreenMode,
    transcript: Transcript,
    director_transcript: Option<Box<Transcript>>,
    child_transcripts: HashMap<Uuid, Transcript>,
    child_unhydrated_events: HashMap<Uuid, Vec<SessionEvent>>,
    hydrated_children: HashSet<Uuid>,
    child_history_hydration_complete: bool,
    child_queued_prompts: HashMap<Uuid, Vec<PendingPromptProjection>>,
    child_statuses: HashMap<Uuid, SessionStatus>,
    child_active_since: HashMap<Uuid, DateTime<Utc>>,
    child_pending_approvals: HashSet<Uuid>,
    /// Startup recovery can publish child-state corrections after the root
    /// history has already been seeded. Keep those corrections in the roster
    /// and child projections, but do not turn them into root transcript cards.
    suppress_bootstrap_subagent_activity: bool,
    focused_child: Option<Uuid>,
    sidecar_focus_request: Option<String>,
    team_switcher_open: bool,
    team_roster_hit_areas: Vec<(Rect, Option<Uuid>)>,
    hovered_team_roster: Option<usize>,
    back_to_director_area: Option<Rect>,
    back_to_director_hovered: bool,
    composer: Composer,
    attachment_store: AttachmentStore,
    keymap: KeyMap,
    cwd: PathBuf,
    configured_model_entries: Vec<borg_provider::DynamicModelEntry>,
    extension_commands: Vec<borg_remote::ExtensionApiCommand>,
    git_status_cache: GitStatusCache,
    status: SessionStatus,
    steer_active_turn: bool,
    /// Highest durable root sequence incorporated into this projection.
    /// Asynchronous history/state hydration may finish after live events, so
    /// an older snapshot must never overwrite newer status or metadata.
    session_state_sequence: u64,
    pending_approval: bool,
    pending_provider_interaction: bool,
    pending_provider_interaction_secret: bool,
    scroll_from_bottom: usize,
    scroll_motion: ScrollMotion,
    scrollbar_area: Option<Rect>,
    scrollbar_thumb_area: Option<Rect>,
    scrollbar_drag_offset: u16,
    transcript_viewport_area: Option<Rect>,
    composer_area: Option<Rect>,
    composer_text_area: Option<Rect>,
    composer_text_width: usize,
    composer_scroll: u16,
    transcript_scroll_max: usize,
    dragging_scrollbar: bool,
    scrollbar_hovered: bool,
    jump_to_bottom_area: Option<Rect>,
    jump_to_bottom_hovered: bool,
    keybindings_hint_area: Option<Rect>,
    keybindings_hovered: bool,
    dictation_button_area: Option<Rect>,
    dictation_button_hovered: bool,
    dictation_state: DictationState,
    dictation_icon: DictationIconStyle,
    tool_hit_areas: Vec<(Rect, usize)>,
    tool_run_hit_areas: Vec<(Rect, usize, usize)>,
    tool_run_header_hit_areas: Vec<(Rect, usize)>,
    entry_hit_areas: Vec<(Rect, usize)>,
    message_hit_areas: Vec<(Rect, usize)>,
    link_hit_areas: Vec<(Rect, String)>,
    picker_hit_areas: Vec<(Rect, usize)>,
    hovered_tool: Option<usize>,
    hovered_tool_run: Option<(usize, usize)>,
    hovered_tool_run_header: Option<usize>,
    hovered_entry: Option<usize>,
    hovered_message: Option<usize>,
    hovered_link: Option<String>,
    hovered_picker_option: Option<usize>,
    last_mouse_position: Option<Position>,
    status_area: Option<Rect>,
    status_hovered: bool,
    goal_status_area: Option<Rect>,
    goal_status_hovered: bool,
    todo_status_area: Option<Rect>,
    todo_status_hovered: bool,
    todo_status_expanded: bool,
    agents_status_area: Option<Rect>,
    agents_status_hovered: bool,
    model_status_area: Option<Rect>,
    model_status_hovered: bool,
    effort_status_area: Option<Rect>,
    effort_status_hovered: bool,
    context_status_area: Option<Rect>,
    context_status_hovered: bool,
    fast_status_area: Option<Rect>,
    fast_status_hovered: bool,
    permission_status_area: Option<Rect>,
    permission_status_hovered: bool,
    nested_scroll_capture: Option<NestedScrollCapture>,
    nested_scroll_motion: Option<NestedScrollMotion>,
    text_selection: Option<TextSelection>,
    composer_selection: Option<ComposerSelection>,
    pending_transcript_click: Option<PendingTranscriptClick>,
    pending_tool_copy: Option<usize>,
    active_since: Option<DateTime<Utc>>,
    notice: Option<String>,
    copy_notice_expires_at: Option<Instant>,
    clipboard_lease: Option<clipboard::ClipboardLease>,
    last_ctrl_c: Option<Instant>,
    queued_prompts: Vec<PendingPromptProjection>,
    replaying_history: bool,
    history_page_requested: bool,
    history_page_loading: bool,
    picker: Option<Picker>,
    /// Model the user picked from a provider that still needs credentials;
    /// applied once the auth picker resolves.
    pending_auth_model: Option<String>,
    keybindings_open: bool,
    slash_selection: usize,
    rewind_targets: Vec<RewindTarget>,
    rewind_primed: bool,
    borging_this_run: bool,
    last_terminal_title: Option<String>,
    transcript_render_cache: Option<CachedTranscriptRender>,
    transcript_full_render_cache: Option<CachedTranscriptRender>,
    rendered_transcript_height: usize,
    pending_scroll_anchor_height: Option<usize>,
    pending_transcript_anchor: Option<TranscriptViewportAnchor>,
    event_redraw_needed: bool,
    cursor_blink_started_at: Instant,
    splash_started_at: Instant,
    splash_glitch_seed: u64,
    terminal_restored: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct HoverState {
    hovered_tool: Option<usize>,
    hovered_tool_run_header: Option<usize>,
    hovered_entry: Option<usize>,
    hovered_message: Option<usize>,
    hovered_picker_option: Option<usize>,
    hovered_team_roster: Option<usize>,
    hovered_link: Option<String>,
    status_hovered: bool,
    goal_status_hovered: bool,
    todo_status_hovered: bool,
    agents_status_hovered: bool,
    model_status_hovered: bool,
    effort_status_hovered: bool,
    context_status_hovered: bool,
    fast_status_hovered: bool,
    permission_status_hovered: bool,
    back_to_director_hovered: bool,
    scrollbar_hovered: bool,
    jump_to_bottom_hovered: bool,
    keybindings_hovered: bool,
    dictation_button_hovered: bool,
}

impl BorgTerminal {
    /// Mouse motion is high-volume input. Keep the redraw gate tied to visual
    /// hover state so moving inside one control does not rebuild the whole
    /// transcript for every terminal cell crossed.
    fn hover_state(&self) -> HoverState {
        HoverState {
            hovered_tool: self.hovered_tool,
            hovered_tool_run_header: self.hovered_tool_run_header,
            hovered_entry: self.hovered_entry,
            hovered_message: self.hovered_message,
            hovered_picker_option: self.hovered_picker_option,
            hovered_team_roster: self.hovered_team_roster,
            hovered_link: self.hovered_link.clone(),
            status_hovered: self.status_hovered,
            goal_status_hovered: self.goal_status_hovered,
            todo_status_hovered: self.todo_status_hovered,
            agents_status_hovered: self.agents_status_hovered,
            model_status_hovered: self.model_status_hovered,
            effort_status_hovered: self.effort_status_hovered,
            context_status_hovered: self.context_status_hovered,
            fast_status_hovered: self.fast_status_hovered,
            permission_status_hovered: self.permission_status_hovered,
            back_to_director_hovered: self.back_to_director_hovered,
            scrollbar_hovered: self.scrollbar_hovered,
            jump_to_bottom_hovered: self.jump_to_bottom_hovered,
            keybindings_hovered: self.keybindings_hovered,
            dictation_button_hovered: self.dictation_button_hovered,
        }
    }
}

fn hover_state_changed(previous: HoverState, current: HoverState) -> bool {
    previous != current
}

fn update_mouse_position(
    last: &mut Option<Position>,
    kind: &MouseEventKind,
    pointer: Position,
) -> bool {
    let moved = matches!(kind, MouseEventKind::Moved) && *last != Some(pointer);
    *last = Some(pointer);
    moved
}

fn session_state_snapshot_is_stale(projected_sequence: u64, state: &SessionState) -> bool {
    state.latest_sequence < projected_sequence
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
    /// Live filter text for pickers the user types into. `None` leaves the
    /// picker on its number-key shortcuts, which typing would otherwise eat.
    query: Option<String>,
    /// First rendered content line. Keeping this independent from `selected`
    /// prevents mouse hover from snapping a scrolled list back around the row
    /// under the pointer.
    viewport_offset: Cell<usize>,
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum PickerKind {
    Settings,
    Resume,
    Model,
    Effort,
    Permission,
    Language,
    Fast,
    RefreshRate,
    PreventSleep,
    ActiveMessages,
    AutoExpandEdits,
    AutoExpandTools,
    DictationIcon,
    Rewind,
    MessageActions,
    Commands,
    ProviderAuth,
    Goal,
}

/// How the user chose to authenticate a provider they selected a model from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderAuthChoice {
    Subscription,
    ApiKey,
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
            query: None,
            viewport_offset: Cell::new(0),
        }
    }

    /// Indices of the options the current filter admits, in display order.
    /// Every navigation and render path goes through this so a filtered-out
    /// row can never be selected or counted.
    fn matches(&self) -> Vec<usize> {
        let Some(query) = self
            .query
            .as_deref()
            .map(str::trim)
            .filter(|q| !q.is_empty())
        else {
            return (0..self.options.len()).collect();
        };
        self.options
            .iter()
            .enumerate()
            .filter(|(_, option)| {
                fuzzy_matches(&option.label, query)
                    || fuzzy_matches(&option.value, query)
                    || option
                        .section
                        .as_deref()
                        .is_some_and(|section| fuzzy_matches(section, query))
                    || option
                        .preview
                        .as_deref()
                        .is_some_and(|preview| fuzzy_matches(preview, query))
            })
            .map(|(index, _)| index)
            .collect()
    }

    fn selected_position(&self) -> Option<usize> {
        self.matches()
            .iter()
            .position(|index| *index == self.selected)
    }

    fn previous(&mut self) {
        let matches = self.matches();
        if matches.is_empty() {
            return;
        }
        let position = self
            .selected_position()
            .and_then(|position| position.checked_sub(1))
            .unwrap_or(matches.len() - 1);
        self.selected = matches[position];
    }

    fn next(&mut self) {
        let matches = self.matches();
        if matches.is_empty() {
            return;
        }
        let position = self
            .selected_position()
            .map_or(0, |position| (position + 1) % matches.len());
        self.selected = matches[position];
    }

    fn page(&mut self, delta: isize) {
        let matches = self.matches();
        let Some(position) = self.selected_position() else {
            return;
        };
        let target = position
            .saturating_add_signed(delta)
            .min(matches.len().saturating_sub(1));
        self.selected = matches[target];
    }

    /// Retarget the filter, keeping the selection on a row that still matches.
    fn set_query(&mut self, query: String) {
        self.query = Some(query);
        self.viewport_offset.set(0);
        let matches = self.matches();
        if !matches.contains(&self.selected)
            && let Some(first) = matches.first()
        {
            self.selected = *first;
        }
    }

    fn select_index(&mut self, index: usize) -> bool {
        let Some(&option) = self.matches().get(index) else {
            return false;
        };
        self.selected = option;
        true
    }

    fn select_option(&mut self, option: usize) -> bool {
        if !self.matches().contains(&option) {
            return false;
        }
        self.selected = option;
        true
    }

    fn select_hovered(&mut self, pointer_moved: bool, hovered: Option<usize>) -> bool {
        if !pointer_moved {
            return false;
        }
        hovered.is_some_and(|index| self.select_option(index))
    }

    fn displayed_option_rows(&self) -> Vec<(Option<usize>, String, Style)> {
        let mut owner = None;
        let sections = self
            .options
            .iter()
            .map(|option| {
                if option.section.is_some() {
                    owner.clone_from(&option.section);
                }
                owner.clone()
            })
            .collect::<Vec<_>>();
        let mut heading = None;
        let mut rows = Vec::new();
        for index in self.matches() {
            if sections[index].is_some() && sections[index] != heading {
                heading.clone_from(&sections[index]);
                rows.push((
                    None,
                    format!(
                        "  {}",
                        heading.as_deref().unwrap_or_default().to_ascii_uppercase()
                    ),
                    Style::default()
                        .fg(Color::DarkGray)
                        .add_modifier(Modifier::BOLD),
                ));
            }
            let option = &self.options[index];
            let selected = index == self.selected;
            rows.push((
                Some(index),
                format!(
                    "  {} {}",
                    if selected { "\u{203a}" } else { " " },
                    if self.query.is_some() {
                        option.label.clone()
                    } else {
                        numbered_picker_option(
                            rows.iter().filter(|(index, _, _)| index.is_some()).count(),
                            &option.label,
                        )
                    },
                ),
                if selected {
                    Style::default()
                        .fg(BORG_ORANGE)
                        .bg(MESSAGE_HOVER_BG)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(Color::Gray)
                },
            ));
        }
        rows
    }

    /// Returns the rendered content-line offset for each visible option.
    /// Section headings occupy a line of their own, while the picker header
    /// is always line zero. Derive these offsets from the same rows rendered
    /// by `styled_option_rows` so hitboxes cannot drift from the highlight.
    fn option_row_offsets(&self) -> Vec<(usize, usize)> {
        self.displayed_option_rows()
            .into_iter()
            .enumerate()
            .filter_map(|(line, (index, _, _))| index.map(|index| (index, line + 1)))
            .collect()
    }

    fn scroll(&mut self, delta: isize) -> bool {
        let matches = self.matches();
        let Some(position) = self.selected_position() else {
            return false;
        };
        let next_position = position
            .saturating_add_signed(delta)
            .min(matches.len().saturating_sub(1));
        let next = matches[next_position];
        if next == self.selected {
            return false;
        }
        let row_offsets = self.option_row_offsets();
        let selected_line = row_offsets
            .iter()
            .find_map(|(index, line)| (*index == self.selected).then_some(*line));
        let next_line = row_offsets
            .iter()
            .find_map(|(index, line)| (*index == next).then_some(*line));
        if let (Some(selected_line), Some(next_line)) = (selected_line, next_line) {
            let line_delta = isize::try_from(next_line).unwrap_or(isize::MAX)
                - isize::try_from(selected_line).unwrap_or(isize::MAX);
            self.viewport_offset
                .set(self.viewport_offset.get().saturating_add_signed(line_delta));
        }
        self.selected = next;
        true
    }

    /// Keep the selected row inside a picker viewport. This is shared by the
    /// normal one-column picker and Resume's two-column surface so keyboard,
    /// wheel, and mouse hit-testing all use the same slice.
    fn scroll_offset(&self, content_height: usize, line_count: usize) -> usize {
        if content_height == 0 {
            self.viewport_offset.set(0);
            return 0;
        }
        let max_scroll = line_count.saturating_sub(content_height);
        let selected_line = self
            .option_row_offsets()
            .iter()
            .find_map(|(index, line)| (*index == self.selected).then_some(*line))
            .unwrap_or(1);
        let current = self.viewport_offset.get().min(max_scroll);
        let last_safe_line = current.saturating_add(content_height.saturating_sub(2));
        let next = if selected_line < current {
            selected_line.saturating_sub(1)
        } else if selected_line > last_safe_line {
            selected_line.saturating_sub(content_height.saturating_sub(2))
        } else {
            current
        }
        .min(max_scroll);
        self.viewport_offset.set(next);
        next
    }

    fn selected_value(self) -> String {
        self.options[self.selected].value.clone()
    }

    fn select_number(&mut self, number: char) -> bool {
        // A filterable picker spends its digits on the filter.
        if self.query.is_some() {
            return false;
        }
        let Some(index) = number
            .to_digit(10)
            .and_then(|number| usize::try_from(number).ok())
            .and_then(|number| number.checked_sub(1))
        else {
            return false;
        };
        self.select_index(index)
    }

    /// Header line, echoing the live filter so the user can see what is
    /// narrowing the list without a separate input row.
    fn header(&self) -> String {
        match self
            .query
            .as_deref()
            .map(str::trim)
            .filter(|q| !q.is_empty())
        {
            Some(query) => format!("> {} · {query}", self.title),
            None => format!("> {}", self.title),
        }
    }

    fn resume_header(&self) -> String {
        let header = self.header();
        if self
            .query
            .as_deref()
            .is_none_or(|query| query.trim().is_empty())
        {
            format!("{header} · type to filter")
        } else {
            header
        }
    }

    fn styled_option_rows(&self) -> Vec<(String, Style)> {
        self.displayed_option_rows()
            .into_iter()
            .map(|(_, row, style)| (row, style))
            .collect()
    }

    fn styled_lines(
        &self,
        width: usize,
        preview_label_color: Color,
        preview_message_color: Color,
    ) -> Vec<Line<'static>> {
        if !matches!(self.kind, PickerKind::Resume) || width < 60 {
            let rows = self.styled_option_rows();
            let empty = rows.is_empty().then(|| {
                (
                    "  no match".to_string(),
                    Style::default().fg(Color::DarkGray),
                )
            });
            let header = if matches!(self.kind, PickerKind::Resume) {
                truncate_table_cell(&self.resume_header(), width)
            } else {
                self.header()
            };
            return std::iter::once(Line::from(Span::styled(
                header,
                Style::default()
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            )))
            .chain(
                rows.into_iter()
                    .chain(empty)
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
        let left_width = resume_left_width(width);
        let left_content_width = left_width.saturating_sub(2);
        let right_width = width.saturating_sub(left_width + 3).max(1);
        let preview = self
            .selected_position()
            .and_then(|_| self.options.get(self.selected))
            .and_then(|option| option.preview.as_deref())
            .unwrap_or("No matching session");
        let preview_lines = markdown_lines(preview, right_width, Some(preview_message_color));
        let mut option_rows = self.styled_option_rows();
        if option_rows.is_empty() {
            option_rows.push((
                "  no match".to_string(),
                Style::default().fg(Color::DarkGray),
            ));
        }
        let row_count = option_rows.len().max(preview_lines.len());
        let preview_header = if self
            .query
            .as_deref()
            .is_none_or(|query| query.trim().is_empty())
        {
            "Latest response · type to filter · PgUp/PgDn older"
        } else {
            "Latest response"
        };
        let mut lines = vec![Line::from(vec![
            Span::styled(
                pad_display(
                    &truncate_table_cell(&self.resume_header(), left_width),
                    left_width,
                ),
                Style::default()
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" │ ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                truncate_table_cell(preview_header, right_width),
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

fn resume_left_width(width: usize) -> usize {
    (width * 2 / 5).clamp(28, 44)
}

/// Keep plan presentation consistent across the transcript and the statusline
/// tooltip: actionable items first, completed history last.
fn ordered_plan_items(items: &[PlanItem]) -> Vec<&PlanItem> {
    [
        PlanItemStatus::InProgress,
        PlanItemStatus::Pending,
        PlanItemStatus::Completed,
    ]
    .into_iter()
    .flat_map(|status| items.iter().filter(move |item| item.status == status))
    .collect()
}

fn pad_display(value: &str, width: usize) -> String {
    let used = UnicodeWidthStr::width(value);
    format!("{value}{}", " ".repeat(width.saturating_sub(used)))
}

/// Rows for the model picker. Every catalog-backed provider is listed, not
/// just the session's current one — picking a model from another provider
/// repoints the live session at that provider. Fixed catalogs use the
/// canonical provider order; an open-ended current provider remains above them.
#[cfg(test)]
fn model_picker_options(
    provider: Option<CodingProvider>,
    current: Option<&str>,
) -> Vec<PickerOption> {
    let discovered =
        if provider.is_none_or(|provider| matches!(provider, CodingProvider::OpenAiCompatible)) {
            let config = borg_provider::LocalModelDiscoveryConfig::from_standard_environment();
            borg_provider::discover_dynamic_model_entries(&config).unwrap_or_default()
        } else {
            Vec::new()
        };
    model_picker_options_with_discovered(provider, current, &discovered)
}

#[cfg(test)]
fn model_picker_options_with_discovered(
    provider: Option<CodingProvider>,
    current: Option<&str>,
    discovered: &[borg_provider::DynamicModelEntry],
) -> Vec<PickerOption> {
    model_picker_options_with_configured(provider, current, discovered, &[])
}

fn model_picker_options_with_configured(
    provider: Option<CodingProvider>,
    current: Option<&str>,
    discovered: &[borg_provider::DynamicModelEntry],
    configured: &[borg_provider::DynamicModelEntry],
) -> Vec<PickerOption> {
    let mut options = Vec::new();
    for (index, model) in configured.iter().cloned().enumerate() {
        let mut option = PickerOption::new(model.label, model.id);
        option.preview = model.detail;
        if index == 0 {
            option.section = Some("Configured providers".to_string());
        }
        options.push(option);
    }
    let push_catalog = |options: &mut Vec<PickerOption>, target: CodingProvider| {
        let Some(catalog) = target.model_catalog() else {
            return;
        };
        for (index, (id, label)) in catalog.selectable_models.iter().enumerate() {
            let mut option = PickerOption::new(*id, *id);
            option.preview = Some((*label).to_string());
            if index == 0 {
                option.section = Some(target.label().to_string());
            }
            options.push(option);
        }
    };

    match provider {
        Some(provider) if provider.model_catalog().is_some() => {}
        Some(CodingProvider::OpenRouter) => {
            let runtime_entries = if discovered.is_empty() {
                borg_provider::openrouter_model_entries()
            } else {
                discovered.to_vec()
            };
            let mut models =
                borg_provider::dynamic_models_for_backend("openrouter", current, &runtime_entries);
            if models.is_empty() {
                models.push(borg_provider::DynamicModelEntry {
                    id: borg_provider::openrouter_product_model().to_string(),
                    label: borg_provider::openrouter_product_model().to_string(),
                    detail: None,
                });
            }
            for (index, model) in models.into_iter().enumerate() {
                let mut option = PickerOption::new(model.label.clone(), model.id);
                if let Some(detail) = model.detail {
                    option.preview = Some(detail);
                }
                if index == 0 {
                    option.section = Some(CodingProvider::OpenRouter.label().to_string());
                }
                options.push(option);
            }
        }
        Some(CodingProvider::Kimi) => {
            options.push(PickerOption::new(
                borg_provider::kimi_product_model(),
                borg_provider::kimi_product_model(),
            ));
        }
        provider @ (Some(CodingProvider::OpenAiCompatible)
        | Some(CodingProvider::OpenCode)
        | None) => {
            let backend = provider
                .map(CodingProvider::catalog_backend)
                .unwrap_or("openai-compatible");
            let entries = borg_provider::dynamic_models_for_backend(backend, current, discovered);
            for (index, entry) in entries.into_iter().enumerate() {
                let label = entry.label;
                let mut option = PickerOption::new(label.clone(), entry.id);
                option.preview = Some(label);
                if let Some(detail) = entry.detail {
                    option.preview = Some(format!(
                        "{}\n{}",
                        option.preview.as_deref().unwrap_or_default(),
                        detail
                    ));
                }
                if index == 0 {
                    option.section = Some(
                        provider
                            .map(CodingProvider::label)
                            .unwrap_or("Current")
                            .to_string(),
                    );
                }
                options.push(option);
            }
        }
        Some(CodingProvider::Codex | CodingProvider::Claude) => {
            unreachable!("catalog-backed providers are handled above")
        }
    }

    for target in CodingProvider::CATALOG_PROVIDERS {
        if provider.is_none_or(|provider| provider.model_catalog().is_some() || target != provider)
        {
            push_catalog(&mut options, target);
        }
    }

    // OpenRouter is open-ended rather than a compile-time catalog, but it is
    // still a first-class destination from every provider. Keep the cached
    // catalog in the same picker so `/model` is a real fuzzy switcher instead
    // of requiring the user to know a provider-specific slash command.
    if provider != Some(CodingProvider::OpenRouter) {
        let discovered = borg_provider::openrouter_model_entries();
        let mut models = borg_provider::dynamic_models_for_backend("openrouter", None, &discovered);
        if models.is_empty() {
            models.push(borg_provider::DynamicModelEntry {
                id: borg_provider::openrouter_product_model().to_string(),
                label: borg_provider::openrouter_product_model().to_string(),
                detail: None,
            });
        }
        for (index, model) in models.into_iter().enumerate() {
            let mut option = PickerOption::new(model.label, model.id);
            option.preview = model.detail;
            if index == 0 {
                option.section = Some(CodingProvider::OpenRouter.label().to_string());
            }
            options.push(option);
        }
    }
    options
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
        if mode == ScreenMode::Alternate
            && let Err(error) = execute!(stdout, EnterAlternateScreen)
        {
            discard_pending_terminal_input();
            let _ = disable_raw_mode();
            return Err(error.into());
        }
        if let Err(error) = execute!(stdout, EnableBracketedPaste) {
            if mode == ScreenMode::Alternate {
                let _ = execute!(stdout, LeaveAlternateScreen);
            }
            discard_pending_terminal_input();
            let _ = disable_raw_mode();
            return Err(error.into());
        }
        if let Err(error) = execute!(stdout, EnableMouseCapture, SetCursorStyle::BlinkingBar) {
            let _ = execute!(stdout, DisableBracketedPaste);
            if mode == ScreenMode::Alternate {
                let _ = execute!(stdout, LeaveAlternateScreen);
            }
            discard_pending_terminal_input();
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
                discard_pending_terminal_input();
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
            child_unhydrated_events: HashMap::new(),
            hydrated_children: HashSet::new(),
            child_history_hydration_complete: false,
            child_queued_prompts: HashMap::new(),
            child_statuses: HashMap::new(),
            child_active_since: HashMap::new(),
            child_pending_approvals: HashSet::new(),
            suppress_bootstrap_subagent_activity: true,
            focused_child: None,
            sidecar_focus_request: None,
            team_switcher_open: false,
            team_roster_hit_areas: Vec::new(),
            hovered_team_roster: None,
            back_to_director_area: None,
            back_to_director_hovered: false,
            composer: Composer::default(),
            attachment_store,
            keymap,
            cwd,
            configured_model_entries: Vec::new(),
            extension_commands: Vec::new(),
            git_status_cache: GitStatusCache::default(),
            status: SessionStatus::Starting,
            steer_active_turn: false,
            session_state_sequence: 0,
            pending_approval: false,
            pending_provider_interaction: false,
            pending_provider_interaction_secret: false,
            scroll_from_bottom: 0,
            scroll_motion: ScrollMotion::default(),
            scrollbar_area: None,
            scrollbar_thumb_area: None,
            scrollbar_drag_offset: 0,
            transcript_viewport_area: None,
            composer_area: None,
            composer_text_area: None,
            composer_text_width: 0,
            composer_scroll: 0,
            transcript_scroll_max: 0,
            dragging_scrollbar: false,
            scrollbar_hovered: false,
            jump_to_bottom_area: None,
            jump_to_bottom_hovered: false,
            keybindings_hint_area: None,
            keybindings_hovered: false,
            dictation_button_area: None,
            dictation_button_hovered: false,
            dictation_state: DictationState::Idle,
            dictation_icon: dictation_icon_style_for_preference(None),
            tool_hit_areas: Vec::new(),
            tool_run_hit_areas: Vec::new(),
            tool_run_header_hit_areas: Vec::new(),
            entry_hit_areas: Vec::new(),
            message_hit_areas: Vec::new(),
            link_hit_areas: Vec::new(),
            picker_hit_areas: Vec::new(),
            hovered_tool: None,
            hovered_tool_run: None,
            hovered_tool_run_header: None,
            hovered_entry: None,
            hovered_message: None,
            hovered_link: None,
            hovered_picker_option: None,
            last_mouse_position: None,
            status_area: None,
            status_hovered: false,
            goal_status_area: None,
            goal_status_hovered: false,
            todo_status_area: None,
            todo_status_hovered: false,
            todo_status_expanded: false,
            agents_status_area: None,
            agents_status_hovered: false,
            model_status_area: None,
            model_status_hovered: false,
            effort_status_area: None,
            effort_status_hovered: false,
            context_status_area: None,
            context_status_hovered: false,
            fast_status_area: None,
            fast_status_hovered: false,
            permission_status_area: None,
            permission_status_hovered: false,
            nested_scroll_capture: None,
            nested_scroll_motion: None,
            text_selection: None,
            composer_selection: None,
            pending_transcript_click: None,
            pending_tool_copy: None,
            active_since: None,
            notice: None,
            copy_notice_expires_at: None,
            clipboard_lease: None,
            last_ctrl_c: None,
            queued_prompts: Vec::new(),
            replaying_history: false,
            history_page_requested: false,
            history_page_loading: false,
            picker: None,
            pending_auth_model: None,
            keybindings_open: false,
            slash_selection: 0,
            rewind_targets: Vec::new(),
            rewind_primed: false,
            borging_this_run: false,
            last_terminal_title: None,
            transcript_render_cache: None,
            transcript_full_render_cache: None,
            rendered_transcript_height: 0,
            pending_scroll_anchor_height: None,
            pending_transcript_anchor: None,
            event_redraw_needed: false,
            cursor_blink_started_at: Instant::now(),
            splash_started_at: Instant::now(),
            splash_glitch_seed: Uuid::new_v4().as_u128() as u64,
            terminal_restored: false,
        })
    }

    /// Reuse the active screen and input reader while the owner switches to a
    /// different durable session. Dropping a terminal here would briefly leave
    /// the alternate screen, which makes `/resume` look like Borg exited.
    pub fn retarget(
        &mut self,
        sessions_dir: &Path,
        session_id: Uuid,
        cwd: PathBuf,
        keybindings: &KeybindingConfig,
    ) -> Result<()> {
        self.attachment_store = AttachmentStore::for_session(sessions_dir, session_id)?;
        self.keymap = KeyMap::from_config(keybindings)?;
        self.transcript = Transcript::default();
        self.director_transcript = None;
        self.child_transcripts.clear();
        self.child_unhydrated_events.clear();
        self.hydrated_children.clear();
        self.child_history_hydration_complete = false;
        self.child_queued_prompts.clear();
        self.child_statuses.clear();
        self.child_active_since.clear();
        self.child_pending_approvals.clear();
        self.suppress_bootstrap_subagent_activity = true;
        self.focused_child = None;
        self.sidecar_focus_request = None;
        self.team_switcher_open = false;
        self.team_roster_hit_areas.clear();
        self.hovered_team_roster = None;
        self.back_to_director_area = None;
        self.back_to_director_hovered = false;
        self.composer = Composer::default();
        self.cwd = cwd;
        self.extension_commands.clear();
        self.git_status_cache = GitStatusCache::default();
        self.status = SessionStatus::Starting;
        self.session_state_sequence = 0;
        self.pending_approval = false;
        self.pending_provider_interaction = false;
        self.pending_provider_interaction_secret = false;
        self.scroll_from_bottom = 0;
        self.scroll_motion = ScrollMotion::default();
        self.scrollbar_area = None;
        self.scrollbar_thumb_area = None;
        self.scrollbar_drag_offset = 0;
        self.transcript_viewport_area = None;
        self.transcript_scroll_max = 0;
        self.dragging_scrollbar = false;
        self.scrollbar_hovered = false;
        self.jump_to_bottom_area = None;
        self.jump_to_bottom_hovered = false;
        self.keybindings_hint_area = None;
        self.keybindings_hovered = false;
        self.dictation_button_area = None;
        self.dictation_button_hovered = false;
        self.dictation_state = DictationState::Idle;
        self.tool_hit_areas.clear();
        self.tool_run_hit_areas.clear();
        self.tool_run_header_hit_areas.clear();
        self.entry_hit_areas.clear();
        self.message_hit_areas.clear();
        self.link_hit_areas.clear();
        self.picker_hit_areas.clear();
        self.hovered_tool = None;
        self.hovered_tool_run = None;
        self.hovered_tool_run_header = None;
        self.hovered_entry = None;
        self.hovered_message = None;
        self.hovered_link = None;
        self.hovered_picker_option = None;
        self.last_mouse_position = None;
        self.status_area = None;
        self.status_hovered = false;
        self.goal_status_area = None;
        self.goal_status_hovered = false;
        self.todo_status_area = None;
        self.todo_status_hovered = false;
        self.todo_status_expanded = false;
        self.agents_status_area = None;
        self.agents_status_hovered = false;
        self.model_status_area = None;
        self.model_status_hovered = false;
        self.effort_status_area = None;
        self.effort_status_hovered = false;
        self.context_status_area = None;
        self.context_status_hovered = false;
        self.fast_status_area = None;
        self.fast_status_hovered = false;
        self.permission_status_area = None;
        self.permission_status_hovered = false;
        self.nested_scroll_capture = None;
        self.nested_scroll_motion = None;
        self.text_selection = None;
        self.pending_transcript_click = None;
        self.pending_tool_copy = None;
        self.active_since = None;
        self.notice = None;
        self.copy_notice_expires_at = None;
        self.clipboard_lease = None;
        self.last_ctrl_c = None;
        self.queued_prompts.clear();
        self.replaying_history = false;
        self.history_page_requested = false;
        self.history_page_loading = false;
        self.picker = None;
        self.pending_auth_model = None;
        self.keybindings_open = false;
        self.slash_selection = 0;
        self.rewind_targets.clear();
        self.rewind_primed = false;
        self.borging_this_run = false;
        self.last_terminal_title = None;
        self.transcript_render_cache = None;
        self.rendered_transcript_height = 0;
        self.pending_scroll_anchor_height = None;
        self.pending_transcript_anchor = None;
        self.event_redraw_needed = true;
        self.cursor_blink_started_at = Instant::now();
        Ok(())
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

    /// Replace the live keymap after the agent config is edited by a Borg
    /// self-service tool. Parsing happens before the map is swapped, so an
    /// invalid update leaves the current terminal controls intact.
    pub fn reload_keybindings(&mut self, keybindings: &KeybindingConfig) -> Result<()> {
        self.keymap = KeyMap::from_config(keybindings)?;
        self.event_redraw_needed = true;
        Ok(())
    }

    pub fn handle_external_interrupt(&mut self) {
        self.composer.clear();
        self.composer_selection = None;
        self.notice = Some("Prompt cleared · press Ctrl-C again to exit".to_string());
        self.event_redraw_needed = true;
    }

    pub async fn shutdown(mut self) {
        // Stop the reader before changing terminal modes, but restore the
        // terminal synchronously before awaiting task cancellation. A second
        // interrupt must never strand the shell behind an async teardown.
        self.input.abort();
        self.restore_terminal();
        self.input.shutdown().await;
        discard_pending_terminal_input();
    }

    pub fn seed_history(&mut self, events: &[SessionEvent]) {
        // A newly resumed terminal always opens at the live tail. Historical
        // hydration is presentation state, never a reason to inherit or
        // animate an older viewport position.
        self.scroll_from_bottom = 0;
        self.scroll_motion.cancel();
        self.history_page_requested = false;
        self.history_page_loading = false;
        self.pending_scroll_anchor_height = None;
        self.pending_transcript_anchor = None;
        self.transcript.reserve_history(events.len());
        self.rewind_targets.reserve(events.len() / 4);
        let display_events = transcript_history_in_display_order(events);
        self.composer.seed_session_events(&display_events);
        self.replaying_history = true;
        for event in &display_events {
            let _ = self.apply_session_event(event);
        }
        self.transcript.follow_tail = true;
        self.replaying_history = false;
    }

    /// Restore pending input from the durable queue projection separately from
    /// transcript history. Resume deliberately omits queued messages from the
    /// transcript bootstrap so they cannot be mistaken for new conversation.
    pub fn seed_pending_prompt_events(&mut self, events: &[SessionEvent]) {
        for pending in pending_prompt_projection_from_events(events) {
            push_queued_prompt(
                &mut self.queued_prompts,
                pending.message_id,
                pending.text,
                pending.delivery,
            );
        }
    }

    /// Seed durable composer recall independently from the bounded transcript
    /// window. This keeps Up-arrow history stable across resumes even when the
    /// visible tail is aggressively lazy-loaded.
    pub fn seed_composer_history(&mut self, events: &[SessionEvent]) {
        self.composer.seed_session_events(events);
    }

    pub fn has_composer_history(&self) -> bool {
        !self.composer.history.is_empty()
    }

    pub fn has_active_queued_prompts(&self) -> bool {
        !self.active_queued_prompts().is_empty()
    }

    pub fn has_empty_composer_text(&self) -> bool {
        self.composer.text.trim().is_empty()
    }

    pub fn up_may_recall_history(&self) -> bool {
        let width = self
            .terminal
            .size()
            .map_or(1, |size| terminal_content_width(size.width).max(1) as usize);
        self.composer.history_index.is_some()
            || self.composer.text.is_empty()
            || composer_cursor_position(&self.composer.text, self.composer.cursor, width).0 == 0
    }

    pub fn replace_history(&mut self, events: &[SessionEvent]) {
        let previous_height = self.rendered_transcript_height;
        let replaced_displayed = replace_root_transcript_history(
            &mut self.transcript,
            &mut self.director_transcript,
            self.focused_child.is_some(),
            events,
        );
        if let Some(sequence) = events
            .iter()
            .map(|event| event.sequence)
            .filter(|sequence| *sequence > 0)
            .max()
        {
            self.session_state_sequence = self.session_state_sequence.max(sequence);
        }
        if !replaced_displayed {
            return;
        }
        self.rewind_targets = rewind_targets_from_history(events);
        self.text_selection = None;
        self.pending_transcript_click = None;
        self.pending_scroll_anchor_height = Some(previous_height);
        self.transcript_render_cache = None;
    }

    /// Consume one explicit upward-navigation request once the loaded
    /// transcript is near its oldest edge. Ordinary redraw/activity ticks
    /// must never hydrate historical pages behind a user who is following the
    /// live tail.
    pub fn take_history_page_request(&mut self) -> bool {
        if self.focused_child.is_some() {
            self.history_page_requested = false;
            return false;
        }
        let viewport_height = self.transcript_viewport_area.map_or(12, |area| area.height);
        let should_load = should_load_history_page(
            self.history_page_requested,
            self.scroll_from_bottom,
            self.transcript_scroll_max,
            usize::from(viewport_height),
        );
        if should_load {
            self.history_page_requested = false;
        }
        should_load
    }

    pub fn set_history_page_loading(&mut self, loading: bool) {
        if self.history_page_loading != loading {
            self.history_page_loading = loading;
            self.event_redraw_needed = true;
        }
    }

    pub fn is_history_page_loading(&self) -> bool {
        self.history_page_loading
    }

    pub fn seed_session_state(&mut self, state: &SessionState) {
        if session_state_snapshot_is_stale(self.session_state_sequence, state) {
            return;
        }
        let root_transcript = self
            .director_transcript
            .as_deref_mut()
            .unwrap_or(&mut self.transcript);
        root_transcript.seed_session_state(state);
        root_transcript.reconcile_session_status(state);
        if let Some(status) = state.status {
            self.status = status;
        }
        self.session_state_sequence = state.latest_sequence;
        self.pending_approval = state.pending_approval_id.is_some();
        self.pending_provider_interaction = state.pending_provider_interaction_id.is_some();
        self.pending_provider_interaction_secret = state
            .pending_provider_interaction_payload
            .as_ref()
            .is_some_and(provider_interaction_contains_secret);
    }

    pub fn restore_composer(&mut self, text: String, attachments: Vec<PathBuf>) {
        self.composer.restore(text, attachments);
        self.composer_selection = None;
    }

    pub fn insert_dictation(&mut self, text: &str) {
        self.composer_selection = None;
        if !self.composer.text.is_empty()
            && !self
                .composer
                .text
                .chars()
                .last()
                .is_some_and(char::is_whitespace)
            && !text.chars().next().is_some_and(char::is_whitespace)
        {
            self.composer.insert(" ");
        }
        self.composer.insert(text);
        self.slash_selection = 0;
        self.notice = Some("Dictation added to composer".to_string());
    }

    pub fn set_dictation_state(&mut self, state: DictationState) {
        self.dictation_state = state;
        self.event_redraw_needed = true;
    }

    pub fn composer_draft(&self) -> Option<(String, Vec<PathBuf>)> {
        self.composer.draft()
    }

    pub fn project_pending_prompt(
        &mut self,
        target: Option<Uuid>,
        message_id: Uuid,
        text: String,
        delivery: PromptDelivery,
    ) {
        if let Some(child) = target {
            push_queued_prompt(
                self.child_queued_prompts.entry(child).or_default(),
                message_id,
                text,
                delivery,
            );
        } else {
            push_queued_prompt(&mut self.queued_prompts, message_id, text, delivery);
        }
    }

    pub fn discard_pending_prompt(&mut self, target: Option<Uuid>, message_id: Uuid) {
        if let Some(child) = target {
            self.child_queued_prompts
                .entry(child)
                .or_default()
                .retain(|queued| queued.message_id != message_id);
        } else {
            self.queued_prompts
                .retain(|queued| queued.message_id != message_id);
        }
    }

    /// Put an idle user submission in the transcript before the session actor
    /// persists it. The durable Message event will replace this transient row
    /// once the command reaches the actor.
    pub fn project_submitted_prompt(
        &mut self,
        message_id: Uuid,
        text: String,
        attachments: Vec<PathBuf>,
        delivery: PromptDelivery,
    ) {
        let event = SessionEvent::new(
            Uuid::nil(),
            0,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text,
                attachments,
                status: MessageStatus::Complete,
                delivery: Some(delivery),
            },
        );
        self.transcript.project_optimistic_message(&event);
        self.status = SessionStatus::Starting;
        self.active_since = Some(event.created_at);
        self.transcript.follow_tail = true;
        self.transcript_render_cache = None;
    }

    pub fn reject_optimistic_prompt(
        &mut self,
        target: Option<Uuid>,
        message_id: Uuid,
        text: String,
        attachments: Vec<PathBuf>,
    ) {
        if let Some(child) = target {
            self.child_queued_prompts
                .entry(child)
                .or_default()
                .retain(|queued| queued.message_id != message_id);
        } else {
            self.queued_prompts
                .retain(|queued| queued.message_id != message_id);
            if let Some(removed) = self.transcript.remove_message(message_id) {
                self.remap_selection_after_entry_removal(removed);
                self.transcript_render_cache = None;
            }
            if self
                .transcript
                .active_turn
                .as_ref()
                .is_some_and(|turn| turn.message_id == message_id)
            {
                self.transcript.active_turn = None;
                self.status = SessionStatus::Ready;
                self.active_since = None;
            }
        }
        self.composer.restore(text, attachments);
        self.composer_selection = None;
        self.notice = Some("Could not send the prompt; it was returned to the composer".into());
    }

    pub fn is_launch_screen(&self) -> bool {
        self.transcript.order.is_empty()
            && self.active_queued_prompts().is_empty()
            && self.composer.text.is_empty()
            && self.composer.attachments.is_empty()
    }

    pub fn has_active_splash_animation(&self) -> bool {
        self.is_launch_screen() && self.splash_started_at.elapsed() < SPLASH_ANIMATION_DURATION
    }

    pub fn has_running_tool(&self) -> bool {
        self.transcript.has_running_tool()
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
        if !self.replaying_history
            && matches!(
                event.kind,
                SessionEventKind::StatusChanged {
                    status: SessionStatus::Ready
                        | SessionStatus::Running
                        | SessionStatus::WaitingForApproval,
                    ..
                }
            )
        {
            // The first live lifecycle boundary ends startup recovery. Any
            // child activity after it is a real update from this run and may
            // appear as a root action card.
            self.suppress_bootstrap_subagent_activity = false;
        }
        let suppress_root_subagent_activity = should_suppress_root_subagent_activity(
            self.suppress_bootstrap_subagent_activity,
            &event.kind,
        );
        if let SessionEventKind::SubagentControl {
            outcome: borg_remote::SubagentControlOutcome::Accepted { agent },
            ..
        } = &event.kind
            && self
                .sidecar_focus_request
                .as_deref()
                .is_some_and(|task_name| task_name == agent.task_name)
        {
            self.focus_child_transcript(agent.session_id);
            self.sidecar_focus_request = None;
        }
        let focused_child_transcript_changed = self.record_child_event(event);
        let projection_changed = match &event.kind {
            SessionEventKind::SessionStarted
            | SessionEventKind::ProviderSessionLinked { .. }
            | SessionEventKind::SubagentControl { .. } => false,
            SessionEventKind::ProviderEvent { kind, .. } => {
                is_context_compaction(kind) || is_live_tool_call_event(kind)
            }
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
        if event.sequence > 0 {
            self.session_state_sequence = self.session_state_sequence.max(event.sequence);
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
                // CLI launch prompts and prompts admitted through another
                // attached surface may never pass through `Composer::take`.
                // Every completed user message must still become recallable
                // with Up in this running instance and future resumes.
                self.composer
                    .seed_session_events(std::slice::from_ref(event));
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
            SessionEventKind::SubagentControl {
                outcome: borg_remote::SubagentControlOutcome::Failed { message },
                ..
            } => self.notice = Some(format!("Agent action failed · {message}")),
            SessionEventKind::ContextCleared if !self.replaying_history => {
                self.notice = Some("Conversation context cleared".to_string());
                self.scroll_from_bottom = 0;
                self.transcript.follow_tail = true;
                self.hovered_tool = None;
                self.hovered_tool_run = None;
                self.hovered_tool_run_header = None;
                self.hovered_entry = None;
                self.hovered_message = None;
                self.pending_tool_copy = None;
            }
            SessionEventKind::PromptRecalled {
                text, attachments, ..
            } if !self.replaying_history => {
                self.composer
                    .append_recalled(text.clone(), attachments.clone());
                self.notice = Some("Queued prompts returned to composer".to_string());
            }
            _ => {}
        }
        let (removed_entry, transcript_changed) = {
            let replaying_history = self.replaying_history;
            let transcript = self
                .director_transcript
                .as_deref_mut()
                .unwrap_or(&mut self.transcript);
            let entries_before = transcript.order.len();
            let changed = focused_child_transcript_changed
                || (!suppress_root_subagent_activity
                    && session_event_changes_transcript(&event.kind));
            let removed_entry = if suppress_root_subagent_activity {
                if let SessionEventKind::SubagentActivity {
                    activity,
                    agent,
                    event: child_event,
                } = &event.kind
                {
                    transcript.upsert_subagent_snapshot_with_status(
                        agent,
                        effective_subagent_status(*activity, agent.status, child_event.as_deref()),
                    );
                }
                None
            } else if replaying_history {
                transcript.apply_history(event)
            } else {
                transcript.apply(event)
            };
            (
                removed_entry,
                changed || transcript.order.len() != entries_before,
            )
        };
        if let Some(removed) = removed_entry {
            self.remap_selection_after_entry_removal(removed);
        }
        if transcript_changed {
            if should_preserve_transcript_viewport(self.transcript.follow_tail)
                && self.pending_scroll_anchor_height.is_none()
            {
                self.pending_scroll_anchor_height = Some(self.rendered_transcript_height);
            }
            self.transcript_render_cache = None;
        }
        if self.transcript.follow_tail {
            self.scroll_from_bottom = 0;
            self.pending_scroll_anchor_height = None;
        }
        projection_changed
    }

    fn record_child_event(&mut self, event: &SessionEvent) -> bool {
        let SessionEventKind::SubagentActivity {
            activity,
            agent,
            event: child_event,
        } = &event.kind
        else {
            return false;
        };
        let child_id = agent.session_id;
        let status = subagent_session_status(effective_subagent_status(
            *activity,
            agent.status,
            child_event.as_deref(),
        ));
        self.child_statuses.insert(child_id, status);
        track_child_activity(
            &mut self.child_active_since,
            child_id,
            status,
            agent.created_at,
        );
        if !self.hydrated_children.contains(&child_id) && self.child_history_hydration_complete {
            self.hydrated_children.insert(child_id);
        }
        let Some(child_event) = child_event else {
            return false;
        };
        if !self.hydrated_children.contains(&child_id) {
            self.child_unhydrated_events
                .entry(child_id)
                .or_default()
                .push(child_event.as_ref().clone());
        }
        if let SessionEventKind::StatusChanged { status, .. } = child_event.kind {
            self.child_statuses.insert(child_id, status);
            track_child_activity(
                &mut self.child_active_since,
                child_id,
                status,
                child_event.created_at,
            );
        }
        update_queued_prompts(
            self.child_queued_prompts.entry(child_id).or_default(),
            &child_event.kind,
        );
        match &child_event.kind {
            SessionEventKind::ApprovalRequested { .. } => {
                self.child_pending_approvals.insert(child_id);
            }
            SessionEventKind::ApprovalResolved { .. } => {
                self.child_pending_approvals.remove(&child_id);
            }
            SessionEventKind::PromptRecalled {
                text, attachments, ..
            } if self.focused_child == Some(child_id) && !self.replaying_history => {
                self.composer
                    .append_recalled(text.clone(), attachments.clone());
                self.notice = Some("Queued prompts returned to composer".to_string());
            }
            _ => {}
        }
        if self.focused_child == Some(agent.session_id) {
            let entries_before = self.transcript.order.len();
            let changed = session_event_changes_transcript(&child_event.kind);
            if let Some(removed) = self.transcript.apply(child_event) {
                self.remap_selection_after_entry_removal(removed);
            }
            changed || self.transcript.order.len() != entries_before
        } else {
            self.child_transcript_mut(child_id).apply(child_event);
            false
        }
    }

    fn child_transcript_mut(&mut self, child_id: Uuid) -> &mut Transcript {
        self.child_transcripts.entry(child_id).or_insert_with(|| {
            let mut transcript = Transcript::default();
            transcript.show_director_context_boundary();
            transcript
        })
    }

    fn focus_child_transcript(&mut self, child_id: Uuid) {
        if self.focused_child == Some(child_id) {
            self.team_switcher_open = false;
            return;
        }
        if let Some(previous_child) = self.focused_child {
            switch_between_child_transcripts(
                &mut self.transcript,
                &mut self.child_transcripts,
                previous_child,
                child_id,
            );
        } else {
            switch_to_child_transcript(
                &mut self.transcript,
                &mut self.director_transcript,
                &mut self.child_transcripts,
                child_id,
            );
        }
        self.focused_child = Some(child_id);
        self.team_switcher_open = false;
        self.reset_transcript_focus();
        let name = self
            .director_transcript
            .as_deref()
            .and_then(|transcript| transcript.subagent_snapshots.get(&child_id))
            .map(|agent| display_agent_name(&agent.task_name))
            .unwrap_or_else(|| child_id.to_string());
        self.notice = Some(format!(
            "Viewing {name} · messages and interrupts target this agent"
        ));
    }

    fn focus_director_transcript(&mut self) {
        let Some(child_id) = self.focused_child.take() else {
            self.notice = None;
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
        // The director transcript is the default view. A persistent banner
        // saying that we are viewing it is both redundant and obscures the
        // statusline's useful state.
        self.notice = None;
    }

    #[must_use]
    pub const fn focused_child(&self) -> Option<Uuid> {
        self.focused_child
    }

    pub fn request_sidecar_focus(&mut self, task_name: impl Into<String>) {
        self.sidecar_focus_request = Some(task_name.into());
    }

    pub fn focus_director(&mut self) {
        self.focus_director_transcript();
    }

    pub fn seed_child_history(&mut self, child_id: Uuid, events: &[SessionEvent]) {
        let was_replaying = self.replaying_history;
        self.replaying_history = true;
        // The root journal may contain a recent nested copy of these events.
        // Merge anything that arrived while the child query was in flight,
        // then atomically replace whichever projection is currently holding
        // this child (including the focused transcript).
        let buffered = self
            .child_unhydrated_events
            .remove(&child_id)
            .unwrap_or_default();
        let events = merge_child_history(events, buffered);
        let previous = if self.focused_child == Some(child_id) {
            &self.transcript
        } else {
            self.child_transcripts
                .get(&child_id)
                .unwrap_or(&self.transcript)
        };
        let mut transcript = fresh_transcript_like(previous);
        transcript.show_director_context_boundary();
        transcript.reserve_history(events.len());
        let optimistic_pending = self
            .child_queued_prompts
            .remove(&child_id)
            .unwrap_or_default();
        self.child_pending_approvals.remove(&child_id);
        for event in &events {
            if let SessionEventKind::StatusChanged { status, .. } = event.kind {
                self.child_statuses.insert(child_id, status);
                track_child_activity(
                    &mut self.child_active_since,
                    child_id,
                    status,
                    event.created_at,
                );
            }
            update_queued_prompts(
                self.child_queued_prompts.entry(child_id).or_default(),
                &event.kind,
            );
            match event.kind {
                SessionEventKind::ApprovalRequested { .. } => {
                    self.child_pending_approvals.insert(child_id);
                }
                SessionEventKind::ApprovalResolved { .. } => {
                    self.child_pending_approvals.remove(&child_id);
                }
                _ => {}
            }
            transcript.apply(event);
        }
        restore_optimistic_pending_prompts(
            self.child_queued_prompts.entry(child_id).or_default(),
            &events,
            optimistic_pending,
        );
        if self.focused_child == Some(child_id) {
            self.transcript = transcript;
            self.text_selection = None;
            self.pending_transcript_click = None;
            self.transcript_render_cache = None;
        } else {
            self.child_transcripts.insert(child_id, transcript);
        }
        self.hydrated_children.insert(child_id);
        self.replaying_history = was_replaying;
    }

    /// Finish the one-time resumed-team hydration pass. Children discovered
    /// after this point are live-only and do not need an authoritative history
    /// query before their nested events can be projected directly.
    pub fn finish_child_history_hydration(&mut self) {
        self.child_history_hydration_complete = true;
        self.hydrated_children
            .extend(self.child_unhydrated_events.keys().copied());
        self.child_unhydrated_events.clear();
    }

    pub fn seed_team_roster(&mut self, agents: &[SubagentSnapshot]) {
        let transcript = self
            .director_transcript
            .as_deref_mut()
            .unwrap_or(&mut self.transcript);
        for agent in agents {
            transcript.upsert_subagent_snapshot(agent);
            let status = subagent_session_status(agent.status);
            self.child_statuses.insert(agent.session_id, status);
            track_child_activity(
                &mut self.child_active_since,
                agent.session_id,
                status,
                agent.created_at,
            );
        }
    }

    fn active_status(&self) -> SessionStatus {
        self.focused_child()
            .and_then(|child| self.child_statuses.get(&child).copied())
            .unwrap_or(self.status)
    }

    fn active_status_started_at(&self) -> Option<DateTime<Utc>> {
        match self.focused_child() {
            Some(child) => self.child_active_since.get(&child).copied(),
            None => self.active_since,
        }
    }

    fn active_pending_approval(&self) -> bool {
        self.focused_child.map_or(self.pending_approval, |child| {
            self.child_pending_approvals.contains(&child)
        })
    }

    fn active_queued_prompts(&self) -> &[PendingPromptProjection] {
        if let Some(child) = self.focused_child {
            self.child_queued_prompts
                .get(&child)
                .map_or(&[], Vec::as_slice)
        } else {
            self.queued_prompts.as_slice()
        }
    }

    fn active_queued_prompts_mut(&mut self) -> &mut Vec<PendingPromptProjection> {
        if let Some(child) = self.focused_child {
            self.child_queued_prompts.entry(child).or_default()
        } else {
            &mut self.queued_prompts
        }
    }

    fn reset_transcript_focus(&mut self) {
        self.scroll_from_bottom = 0;
        self.transcript.follow_tail = true;
        self.transcript_render_cache = None;
        self.text_selection = None;
        self.composer_selection = None;
        self.pending_transcript_click = None;
        self.pending_tool_copy = None;
        self.hovered_entry = None;
        self.hovered_message = None;
        self.hovered_tool = None;
        self.hovered_tool_run = None;
        self.hovered_tool_run_header = None;
    }

    fn clear_background_hover(&mut self) {
        self.hovered_tool = None;
        self.hovered_tool_run = None;
        self.hovered_tool_run_header = None;
        self.hovered_entry = None;
        self.hovered_message = None;
        self.hovered_link = None;
        self.status_hovered = false;
        self.goal_status_hovered = false;
        self.todo_status_hovered = false;
        self.agents_status_hovered = false;
        self.model_status_hovered = false;
        self.effort_status_hovered = false;
        self.context_status_hovered = false;
        self.fast_status_hovered = false;
        self.permission_status_hovered = false;
        self.back_to_director_hovered = false;
        self.scrollbar_hovered = false;
        self.jump_to_bottom_hovered = false;
        self.keybindings_hovered = false;
        self.dictation_button_hovered = false;
    }

    pub fn set_notice(&mut self, notice: impl Into<String>) {
        self.notice = Some(notice.into());
    }

    pub fn set_active_message_behavior(&mut self, steer_active: bool) {
        self.steer_active_turn = steer_active;
    }

    fn record_user_interrupt(&mut self) {
        if self.active_status() != SessionStatus::Running {
            return;
        }
        self.transcript.order.push(TranscriptEntry::Activity {
            text: USER_INTERRUPT_ACTIVITY.to_string(),
            time: canonical_local_time(Local::now()),
        });
        self.transcript.follow_tail = true;
        self.scroll_from_bottom = 0;
        self.transcript_render_cache = None;
        self.transcript_full_render_cache = None;
        self.event_redraw_needed = true;
    }

    pub fn hydrate_payload(&mut self, payload: &SessionPayloadRef, bytes: Vec<u8>) -> Result<()> {
        self.transcript.hydrate_payload(payload, bytes)?;
        self.transcript.tool_body_cache.get_mut().lines.clear();
        self.transcript_render_cache = None;
        if let Some(index) = self.pending_tool_copy
            && self.transcript.tool_payloads(index).is_empty()
        {
            self.pending_tool_copy = None;
            self.copy_transcript_entry(index);
        }
        Ok(())
    }

    pub fn show_goal(&mut self, goal: Option<&SessionGoal>) {
        self.notice = None;
        if let Some(removed) = self.transcript.show_goal(goal) {
            self.remap_selection_after_entry_removal(removed);
        }
        self.transcript_render_cache = None;
    }

    pub fn optimistically_apply_goal_action(&mut self, action: &GoalAction) -> bool {
        let changed = self.transcript.optimistically_apply_goal_action(action);
        if changed {
            self.transcript_render_cache = None;
            self.event_redraw_needed = true;
        }
        changed
    }

    pub fn show_plan(&mut self, items: &[PlanItem]) {
        self.notice = None;
        if let Some(removed) = self.transcript.show_plan(items) {
            self.remap_selection_after_entry_removal(removed);
        }
        self.transcript_render_cache = None;
    }

    fn remap_selection_after_entry_removal(&mut self, removed: usize) {
        let Some(mut selection) = self.text_selection else {
            return;
        };
        if selection.anchor.entry == removed || selection.focus.entry == removed {
            self.text_selection = None;
            self.pending_transcript_click = None;
            return;
        }
        for point in [&mut selection.anchor, &mut selection.focus] {
            point.entry -= usize::from(point.entry > removed);
        }
        self.text_selection = Some(selection);
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

    pub fn set_configured_model_entries(&mut self, entries: Vec<borg_provider::DynamicModelEntry>) {
        self.configured_model_entries = entries;
    }

    pub fn set_extension_commands(&mut self, commands: Vec<borg_remote::ExtensionApiCommand>) {
        self.extension_commands = commands;
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
        let options = model_picker_options_with_configured(
            provider,
            current.as_deref(),
            &[],
            &self.configured_model_entries,
        );
        let selected = current
            .as_deref()
            .and_then(|current| options.iter().position(|option| option.value == current))
            .unwrap_or(0);
        self.picker = Some(Picker {
            kind: PickerKind::Model,
            title: "Choose model",
            options,
            selected,
            query: None,
            viewport_offset: Cell::new(0),
        });
    }

    /// The provider the session is currently configured to use, once the
    /// first `SessionConfigured` event has landed.
    pub fn session_provider(&self) -> Option<CodingProvider> {
        self.transcript
            .config
            .as_ref()
            .map(|config| config.provider)
    }

    /// Asks how to authenticate `provider` before switching to `model`.
    /// Dismissing the picker leaves the session on its current model.
    pub fn open_provider_auth_picker(&mut self, provider: CodingProvider, model: String) {
        let options = vec![
            PickerOption::new(
                format!("Connect your {} subscription", provider.label()),
                "subscription",
            ),
            PickerOption::new(
                format!("Add an API key for {}", provider.label()),
                "api-key",
            ),
            PickerOption::new("Cancel", "cancel"),
        ];
        self.pending_auth_model = Some(model);
        self.picker = Some(Picker {
            kind: PickerKind::ProviderAuth,
            title: "Provider not connected",
            options,
            selected: 0,
            query: None,
            viewport_offset: Cell::new(0),
        });
    }

    pub fn open_settings_picker(&mut self, user_label: &str, assistant_label: &str) {
        let options = vec![
            "Model".to_string(),
            "Reasoning effort".to_string(),
            "Response language".to_string(),
            "Language servers".to_string(),
            "Provider fast mode".to_string(),
            "Active messages".to_string(),
            "Refresh rate".to_string(),
            "Keep machine awake".to_string(),
            "Auto-expand edits".to_string(),
            "Auto-expand tools".to_string(),
            "Microphone icon".to_string(),
            "Transcript colours".to_string(),
            format!("User label · {user_label}"),
            format!("Assistant label · {assistant_label}"),
        ];
        let values = [
            "/model",
            "/effort",
            "/language",
            "/lsp",
            "/fast",
            "/followups",
            "/refresh",
            "/sleep",
            "/expand-edits",
            "/expand-tools",
            "/icons",
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
            query: None,
            viewport_offset: Cell::new(0),
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
            // Resume owns typed characters so session labels, responses, and
            // metadata such as model names can be searched immediately.
            query: Some(String::new()),
            viewport_offset: Cell::new(0),
        });
    }

    pub fn open_effort_picker(&mut self) {
        let provider = self
            .transcript
            .config
            .as_ref()
            .map(|config| config.provider);
        self.open_effort_picker_for(provider);
    }

    /// Opens effort choices for a provider selected in the model picker,
    /// before the asynchronous session-config event has updated the transcript.
    pub fn open_effort_picker_for(&mut self, provider: Option<CodingProvider>) {
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

    pub fn open_permission_picker(&mut self) {
        let current = self
            .transcript
            .config
            .as_ref()
            .map(|config| permission_mode_label(config.permission_mode));
        self.picker = Some(Picker::new(
            PickerKind::Permission,
            "Choose access",
            ["full access", "auto approvals", "manual approvals"],
            current,
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
            query: None,
            viewport_offset: Cell::new(0),
        });
    }

    /// One palette over both slash commands and keybindings, filtered as the
    /// user types. Enter runs a command outright unless it needs an argument,
    /// in which case it lands in the composer ready to finish.
    pub fn open_command_palette(&mut self) {
        self.keybindings_open = false;
        self.notice = None;
        self.picker = Some(Picker {
            kind: PickerKind::Commands,
            title: "Commands and keybindings",
            options: command_palette_options(&self.keymap, &self.extension_commands),
            selected: 0,
            query: Some(String::new()),
            viewport_offset: Cell::new(0),
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
            "Keep machine awake during active turns",
            ["On", "Off"],
            Some(if enabled { "On" } else { "Off" }),
        ));
    }

    pub fn open_active_messages_picker(&mut self, steer_active: bool) {
        self.picker = Some(Picker::new(
            PickerKind::ActiveMessages,
            "Messages sent while Borg is working",
            [ACTIVE_MESSAGES_SEND_NOW, ACTIVE_MESSAGES_WAIT],
            Some(if steer_active {
                ACTIVE_MESSAGES_SEND_NOW
            } else {
                ACTIVE_MESSAGES_WAIT
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

    pub fn open_dictation_icon_picker(&mut self) {
        let options = vec![
            PickerOption::new(
                format!("Nerd Font {}", DICTATION_NERD_FONT_ICON),
                "nerd_font",
            ),
            PickerOption::new(format!("Emoji {}", DICTATION_EMOJI_ICON), "emoji"),
        ];
        let selected = match self.dictation_icon {
            DictationIconStyle::NerdFont => 0,
            DictationIconStyle::Emoji => 1,
        };
        self.picker = Some(Picker {
            kind: PickerKind::DictationIcon,
            title: "Choose microphone icon",
            options,
            selected,
            query: None,
            viewport_offset: Cell::new(0),
        });
        self.notice = Some("Choose the preview that renders correctly in this terminal".into());
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

    pub fn set_dictation_icon(&mut self, style: DictationIconStyle) {
        self.dictation_icon = style;
        self.event_redraw_needed = true;
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
            query: None,
            viewport_offset: Cell::new(0),
        });
    }

    fn open_entry_actions(&mut self, index: usize) -> UiAction {
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
            Some(TranscriptEntry::Action { .. }) => ("Action details", vec!["Copy action"]),
            Some(TranscriptEntry::Compaction {
                complete: true,
                summary,
                sequence,
                ..
            }) if *sequence > 0 && compaction_has_expandable_detail(summary) => (
                "Compaction actions",
                vec!["Revert to after compaction", "Copy compaction summary"],
            ),
            Some(TranscriptEntry::Compaction {
                complete: true,
                summary,
                ..
            }) if compaction_has_expandable_detail(summary) => {
                ("Compaction actions", vec!["Copy compaction summary"])
            }
            _ => return UiAction::None,
        };
        self.transcript.selected = Some(index);
        // A completed compaction with a real summary advertises an action
        // menu because its checkpoint may be revertable, and the first
        // checkpoint still needs a visible way to choose its copy action. Do
        // not silently execute the one-option case as we do for ordinary
        // message cards.
        let run_directly = self
            .transcript
            .order
            .get(index)
            .is_some_and(|entry| entry_action_runs_directly(entry, options.len()));
        self.picker = Some(Picker::new(
            PickerKind::MessageActions,
            title,
            options,
            None,
        ));
        if run_directly {
            self.run_selected_message_action()
        } else {
            UiAction::None
        }
    }

    fn open_goal_picker(&mut self) {
        let Some(goal) = self.active_goal().cloned() else {
            return;
        };
        self.picker = Some(Picker {
            kind: PickerKind::Goal,
            title: "Goal",
            options: goal_picker_options(&goal),
            selected: 0,
            query: None,
            viewport_offset: Cell::new(0),
        });
    }

    fn active_goal(&self) -> Option<&SessionGoal> {
        active_goal_for_view(
            self.focused_child,
            self.director_transcript.as_deref(),
            &self.transcript,
        )
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
        self.event_redraw_needed = !matches!(
            &event,
            Event::Mouse(mouse) if matches!(mouse.kind, MouseEventKind::Moved)
        );
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
                self.composer_selection = None;
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
                let previous_hover = self.hover_state();
                self.last_ctrl_c = None;
                let pointer = Position::new(mouse.column, mouse.row);
                let pointer_moved =
                    update_mouse_position(&mut self.last_mouse_position, &mouse.kind, pointer);
                let background_hover_suppressed = overlay_suppresses_background_hover(
                    self.picker.is_some(),
                    self.team_switcher_open,
                    self.keybindings_open,
                );
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
                self.hovered_tool_run_header = self
                    .tool_run_header_hit_areas
                    .iter()
                    .find_map(|(area, start)| area.contains(pointer).then_some(*start));
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
                self.status_hovered = self.status_area.is_some_and(|area| area.contains(pointer));
                self.goal_status_hovered = self
                    .goal_status_area
                    .is_some_and(|area| area.contains(pointer));
                self.todo_status_hovered = self
                    .todo_status_area
                    .is_some_and(|area| area.contains(pointer));
                self.agents_status_hovered = self
                    .agents_status_area
                    .is_some_and(|area| area.contains(pointer));
                self.model_status_hovered = self
                    .model_status_area
                    .is_some_and(|area| area.contains(pointer));
                self.effort_status_hovered = self
                    .effort_status_area
                    .is_some_and(|area| area.contains(pointer));
                self.context_status_hovered = self
                    .context_status_area
                    .is_some_and(|area| area.contains(pointer));
                self.fast_status_hovered = self
                    .fast_status_area
                    .is_some_and(|area| area.contains(pointer));
                self.permission_status_hovered = self
                    .permission_status_area
                    .is_some_and(|area| area.contains(pointer));
                self.back_to_director_hovered = self
                    .back_to_director_area
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
                self.dictation_button_hovered = self
                    .dictation_button_area
                    .is_some_and(|area| area.contains(pointer));
                if background_hover_suppressed {
                    self.clear_background_hover();
                }
                self.hovered_picker_option = self
                    .picker_hit_areas
                    .iter()
                    .find_map(|(area, index)| area.contains(pointer).then_some(*index));
                self.hovered_team_roster = if self.picker.is_none() && !self.keybindings_open {
                    team_roster_target_at(&self.team_roster_hit_areas, pointer)
                        .map(|(index, _)| index)
                } else {
                    None
                };
                self.event_redraw_needed |= hover_state_changed(previous_hover, self.hover_state());
                if !background_hover_suppressed
                    && matches!(mouse.kind, MouseEventKind::Down(MouseButton::Right))
                    && self
                        .goal_status_area
                        .is_some_and(|area| area.contains(pointer))
                {
                    return Ok(UiAction::Submit {
                        target: None,
                        text: GOAL_CLEAR_COMMAND.to_string(),
                        attachments: Vec::new(),
                    });
                }
                if !background_hover_suppressed
                    && matches!(mouse.kind, MouseEventKind::Down(MouseButton::Right))
                    && let Some(index) = self.hovered_entry
                {
                    return Ok(self.open_entry_actions(index));
                }
                if !background_hover_suppressed
                    && matches!(mouse.kind, MouseEventKind::Down(MouseButton::Right))
                    && let Some(index) = self.hovered_tool
                {
                    let payloads = self.transcript.tool_payloads(index);
                    if payloads.is_empty() {
                        self.copy_transcript_entry(index);
                    } else {
                        self.pending_tool_copy = Some(index);
                        return Ok(UiAction::LoadPayloads(payloads));
                    }
                    return Ok(UiAction::None);
                }
                if matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left)) {
                    if self.dictation_button_hovered {
                        return Ok(UiAction::ToggleDictation);
                    }
                    if self.picker.is_none()
                        && !self.keybindings_open
                        && let Some((_, child_id)) =
                            team_roster_target_at(&self.team_roster_hit_areas, pointer)
                    {
                        if let Some(child_id) = child_id {
                            self.focus_child_transcript(child_id);
                        } else {
                            self.focus_director_transcript();
                        }
                        return Ok(UiAction::None);
                    }
                    if !background_hover_suppressed {
                        if self
                            .back_to_director_area
                            .is_some_and(|area| area.contains(pointer))
                        {
                            self.focus_director_transcript();
                            return Ok(UiAction::None);
                        }
                        if self.status_area.is_some_and(|area| area.contains(pointer))
                            && status_control_is_actionable(self.active_status())
                        {
                            return Ok(UiAction::Interrupt {
                                target: self.focused_child,
                            });
                        }
                        if self
                            .agents_status_area
                            .is_some_and(|area| area.contains(pointer))
                        {
                            self.team_switcher_open = !self.team_switcher_open;
                            return Ok(UiAction::None);
                        }
                        if self
                            .goal_status_area
                            .is_some_and(|area| area.contains(pointer))
                        {
                            if let Some(action) = self.active_goal().and_then(goal_toggle_action) {
                                return Ok(UiAction::ToggleGoal { action });
                            }
                            self.open_goal_picker();
                            return Ok(UiAction::None);
                        }
                        if self
                            .todo_status_area
                            .is_some_and(|area| area.contains(pointer))
                        {
                            if !self.transcript.todos.is_empty() {
                                self.todo_status_expanded = !self.todo_status_expanded;
                            }
                            return Ok(UiAction::None);
                        }
                        if self
                            .model_status_area
                            .is_some_and(|area| area.contains(pointer))
                        {
                            self.open_model_picker();
                            return Ok(UiAction::None);
                        }
                        if self
                            .effort_status_area
                            .is_some_and(|area| area.contains(pointer))
                        {
                            self.open_effort_picker();
                            return Ok(UiAction::None);
                        }
                        if self
                            .context_status_area
                            .is_some_and(|area| area.contains(pointer))
                        {
                            self.notice = Some(self.transcript.context_tooltip());
                            return Ok(UiAction::None);
                        }
                        if self
                            .fast_status_area
                            .is_some_and(|area| area.contains(pointer))
                        {
                            self.open_fast_picker(true);
                            return Ok(UiAction::None);
                        }
                        if self
                            .permission_status_area
                            .is_some_and(|area| area.contains(pointer))
                        {
                            self.open_permission_picker();
                            return Ok(UiAction::None);
                        }
                    }
                    if self.team_switcher_open {
                        self.team_switcher_open = false;
                        self.hovered_team_roster = None;
                    }
                }
                if let Some(picker) = self.picker.as_mut() {
                    let selected_before = picker.selected;
                    picker.select_hovered(pointer_moved, self.hovered_picker_option);
                    self.event_redraw_needed |= picker.selected != selected_before;
                    if !matches!(picker.kind, PickerKind::MessageActions | PickerKind::Goal)
                        && matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left))
                        && let Some(option) = self.hovered_picker_option
                    {
                        picker.select_option(option);
                        return self.run_selected_picker();
                    }
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
                    Some(PickerKind::MessageActions | PickerKind::Goal)
                ) {
                    if matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left)) {
                        if let Some(option) = self.hovered_picker_option {
                            self.picker
                                .as_mut()
                                .expect("checked above")
                                .select_option(option);
                            return if matches!(
                                self.picker.as_ref().map(|picker| picker.kind),
                                Some(PickerKind::Goal)
                            ) {
                                self.run_selected_picker()
                            } else {
                                Ok(self.run_selected_message_action())
                            };
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
                if background_hover_suppressed {
                    return Ok(UiAction::None);
                }
                let hovered_tool_run = self
                    .tool_run_hit_areas
                    .iter()
                    .find_map(|(area, start, max_offset)| {
                        area.contains(pointer).then_some((*start, *max_offset))
                    })
                    .or_else(|| {
                        self.hovered_tool
                            .and_then(|index| self.transcript.tool_run_start_containing(index))
                            .and_then(|start| {
                                self.tool_run_hit_areas.iter().find_map(
                                    |(_, candidate, max_offset)| {
                                        (*candidate == start).then_some((start, *max_offset))
                                    },
                                )
                            })
                    });
                if matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left))
                    && !mouse.modifiers.contains(KeyModifiers::SHIFT)
                    && !self
                        .transcript_viewport_area
                        .is_some_and(|area| area.contains(pointer))
                    && !self
                        .composer_area
                        .is_some_and(|area| area.contains(pointer))
                {
                    self.text_selection = None;
                    self.composer_selection = None;
                    self.pending_transcript_click = None;
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
                        self.composer_selection = None;
                        self.pending_transcript_click = None;
                        self.transcript.follow_tail = true;
                    }
                    MouseEventKind::Down(MouseButton::Left)
                        if self.picker.is_none()
                            && !self.pending_provider_interaction_secret
                            && self
                                .composer_area
                                .is_some_and(|area| area.contains(pointer)) =>
                    {
                        self.text_selection = None;
                        self.pending_transcript_click = None;
                        if let Some(point) = self.composer_selection_point_at(pointer) {
                            self.composer.cursor = point;
                            self.composer.preferred_column = None;
                            self.composer_selection = Some(ComposerSelection {
                                anchor: point,
                                focus: point,
                                dragging: true,
                                pointer,
                            });
                        }
                        self.event_redraw_needed = true;
                    }
                    MouseEventKind::Down(MouseButton::Left)
                        if mouse_starts_text_selection(&mouse, self.transcript_viewport_area) =>
                    {
                        if let Some(point) = self.selection_point_at(pointer) {
                            self.text_selection = Some(TextSelection {
                                anchor: point,
                                focus: point,
                                dragging: true,
                                autoscroll: 0,
                                pointer,
                            });
                            self.composer_selection = None;
                            self.pending_transcript_click =
                                (!mouse.modifiers.contains(KeyModifiers::SHIFT))
                                    .then(|| self.pending_transcript_click(hovered_tool_run));
                        }
                        self.pending_scroll_anchor_height = None;
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
                            .composer_selection
                            .is_some_and(|selection| selection.dragging) =>
                    {
                        self.update_composer_selection_drag(pointer);
                        self.event_redraw_needed = true;
                    }
                    MouseEventKind::Drag(MouseButton::Left)
                        if self
                            .text_selection
                            .is_some_and(|selection| selection.dragging) =>
                    {
                        self.pending_scroll_anchor_height = None;
                        self.pending_transcript_click = None;
                        self.update_text_selection_drag(pointer);
                    }
                    MouseEventKind::Up(MouseButton::Left) => {
                        self.dragging_scrollbar = false;
                        if self
                            .composer_selection
                            .is_some_and(|selection| selection.dragging)
                        {
                            self.update_composer_selection_drag(pointer);
                            if let Some(selection) = self.composer_selection.as_mut() {
                                selection.dragging = false;
                            }
                            if self
                                .composer_selection
                                .is_some_and(ComposerSelection::is_empty)
                            {
                                self.composer_selection = None;
                            }
                            return Ok(UiAction::None);
                        }
                        if self
                            .text_selection
                            .is_some_and(|selection| selection.dragging)
                        {
                            self.update_text_selection_drag(pointer);
                        }
                        let click = finish_text_selection(
                            &mut self.text_selection,
                            &mut self.pending_transcript_click,
                        );
                        if let Some(click) = click {
                            return Ok(self.run_pending_transcript_click(click));
                        }
                    }
                    MouseEventKind::ScrollUp => {
                        if self
                            .composer_selection
                            .is_some_and(|selection| selection.dragging)
                        {
                            return Ok(UiAction::None);
                        }
                        if self
                            .text_selection
                            .is_some_and(|selection| selection.dragging)
                        {
                            self.update_text_selection_drag(pointer);
                            let viewport_height =
                                self.transcript_viewport_area.map_or(1, |area| area.height);
                            let terminal_height =
                                self.terminal.size().map(|size| size.height).unwrap_or(1);
                            let nested = hovered_tool_run.map(|(start, max_offset)| {
                                (
                                    start,
                                    max_offset,
                                    -nested_wheel_scroll_distance(
                                        terminal_height,
                                        scroll_repetitions,
                                    ),
                                )
                            });
                            let nested_scrolled = self.scroll_drag_selection(
                                wheel_scroll_distance(viewport_height, scroll_repetitions),
                                nested,
                            );
                            self.history_page_requested = !nested_scrolled;
                            return Ok(UiAction::None);
                        }
                        let consumed = if let Some((start, max_offset)) = hovered_tool_run {
                            let can_move = self.transcript.tool_run_offset(start, max_offset) > 0;
                            if can_move {
                                let terminal_height =
                                    self.terminal.size().map(|size| size.height).unwrap_or(1);
                                self.queue_nested_wheel_scroll(
                                    start,
                                    max_offset,
                                    -nested_wheel_scroll_distance(
                                        terminal_height,
                                        scroll_repetitions,
                                    ),
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
                            self.history_page_requested = true;
                            let viewport_height =
                                self.transcript_viewport_area.map_or(1, |area| area.height);
                            self.queue_wheel_scroll(wheel_scroll_distance(
                                viewport_height,
                                scroll_repetitions,
                            ));
                        }
                    }
                    MouseEventKind::ScrollDown => {
                        if self
                            .composer_selection
                            .is_some_and(|selection| selection.dragging)
                        {
                            return Ok(UiAction::None);
                        }
                        if self
                            .text_selection
                            .is_some_and(|selection| selection.dragging)
                        {
                            self.update_text_selection_drag(pointer);
                            let viewport_height =
                                self.transcript_viewport_area.map_or(1, |area| area.height);
                            let terminal_height =
                                self.terminal.size().map(|size| size.height).unwrap_or(1);
                            let nested = hovered_tool_run.map(|(start, max_offset)| {
                                (
                                    start,
                                    max_offset,
                                    nested_wheel_scroll_distance(
                                        terminal_height,
                                        scroll_repetitions,
                                    ),
                                )
                            });
                            self.scroll_drag_selection(
                                -wheel_scroll_distance(viewport_height, scroll_repetitions),
                                nested,
                            );
                            self.history_page_requested = false;
                            return Ok(UiAction::None);
                        }
                        let consumed = if let Some((start, max_offset)) = hovered_tool_run {
                            let can_move =
                                self.transcript.tool_run_offset(start, max_offset) < max_offset;
                            if can_move {
                                let terminal_height =
                                    self.terminal.size().map(|size| size.height).unwrap_or(1);
                                self.queue_nested_wheel_scroll(
                                    start,
                                    max_offset,
                                    nested_wheel_scroll_distance(
                                        terminal_height,
                                        scroll_repetitions,
                                    ),
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
                            self.history_page_requested = false;
                            let viewport_height =
                                self.transcript_viewport_area.map_or(1, |area| area.height);
                            self.queue_wheel_scroll(-wheel_scroll_distance(
                                viewport_height,
                                scroll_repetitions,
                            ));
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
            // Changing an actions viewport invalidates the transcript render
            // cache. Apply the complete coalesced wheel gesture in one frame
            // so a long thread is rebuilt once, rather than once per eased
            // animation step. Main transcript scrolling does not invalidate
            // that cache and can keep its smoother multi-frame motion.
            let next = nested
                .motion
                .advance_immediately(current, nested.max_offset);
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
        let scroll_was_active = self.scroll_motion.is_active();
        self.scroll_from_bottom = self
            .scroll_motion
            .advance(self.scroll_from_bottom, self.transcript_scroll_max);
        if scroll_was_active {
            self.transcript.follow_tail = self.scroll_from_bottom == 0;
        }
        let selection_autoscroll = self
            .text_selection
            .filter(|selection| selection.dragging)
            .map_or(0, |selection| selection.autoscroll);
        if selection_autoscroll != 0 {
            self.scroll_from_bottom = advance_selection_autoscroll(
                self.scroll_from_bottom,
                self.transcript_scroll_max,
                selection_autoscroll,
            );
        }
        if selection_autoscroll > 0 {
            self.transcript.follow_tail = false;
        } else if selection_autoscroll < 0 && self.scroll_from_bottom == 0 {
            self.transcript.follow_tail = true;
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
        if lines > 0 {
            self.transcript.follow_tail = false;
        } else if lines < 0 && self.scroll_from_bottom == 0 {
            self.transcript.follow_tail = true;
        }
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

    fn pending_transcript_click(
        &self,
        hovered_tool_run: Option<(usize, usize)>,
    ) -> PendingTranscriptClick {
        if let Some(url) = self.hovered_link.clone() {
            PendingTranscriptClick::Link(url)
        } else if let Some(start) = self.hovered_tool_run_header {
            PendingTranscriptClick::ToolRunHeader(start)
        } else if let Some(index) = self.hovered_tool {
            PendingTranscriptClick::Tool {
                index,
                run: hovered_tool_run,
            }
        } else if let Some(index) = self.hovered_message {
            PendingTranscriptClick::Message(index)
        } else if let Some(index) = self.hovered_entry {
            PendingTranscriptClick::Entry(index)
        } else {
            PendingTranscriptClick::Background
        }
    }

    fn run_pending_transcript_click(&mut self, click: PendingTranscriptClick) -> UiAction {
        match click {
            PendingTranscriptClick::Link(url) => {
                if let Err(error) = open_http_link(&url) {
                    self.notice = Some(format!("Could not open link: {error}"));
                }
            }
            PendingTranscriptClick::ToolRunHeader(start) => {
                if self.transcript.tool_run_expanded(start) {
                    self.capture_transcript_anchor_for_collapse();
                }
                self.nested_scroll_motion = None;
                self.transcript.toggle_tool_run_expansion(start);
                self.transcript_render_cache = None;
            }
            PendingTranscriptClick::Tool { index, run } => {
                self.nested_scroll_motion = None;
                if self.transcript.tool_is_expanded(index) {
                    self.capture_transcript_anchor_for_collapse();
                }
                if let Some((start, max_offset)) = run {
                    self.transcript.anchor_tool_run(start, max_offset);
                }
                let payloads = self.transcript.toggle_tool(index);
                self.transcript_render_cache = None;
                if !payloads.is_empty() {
                    return UiAction::LoadPayloads(payloads);
                }
            }
            PendingTranscriptClick::Message(index) => {
                return self.open_entry_actions(index);
            }
            PendingTranscriptClick::Entry(index) => {
                if self.transcript.compaction_is_expandable(index) {
                    self.capture_transcript_anchor_for_collapse();
                    self.transcript.toggle_compaction_expansion(index);
                    self.transcript_render_cache = None;
                } else if self.transcript.action_is_expandable(index) {
                    self.capture_transcript_anchor_for_collapse();
                    self.transcript.toggle_action_expansion(index);
                    self.transcript_render_cache = None;
                } else if self.transcript.plan_is_clippable(index) {
                    self.transcript.toggle_plan_expansion(index);
                    self.transcript_render_cache = None;
                } else if matches!(
                    self.transcript.order.get(index),
                    Some(TranscriptEntry::Compaction { .. })
                ) {
                    // Compaction actions are deliberately a right-click menu;
                    // a left click only expands a compaction that has detail.
                } else {
                    return self.open_entry_actions(index);
                }
            }
            PendingTranscriptClick::Background => {
                self.transcript.selected = None;
            }
        }
        UiAction::None
    }

    fn scroll_drag_selection(
        &mut self,
        lines: isize,
        nested: Option<(usize, usize, isize)>,
    ) -> bool {
        self.cancel_scroll_motion();
        self.pending_transcript_click = None;
        if let Some((start, max_offset, delta)) = nested
            && self.transcript.scroll_tool_run(start, max_offset, delta)
        {
            self.transcript_render_cache = None;
            if let Some(selection) = self.text_selection.as_mut() {
                selection.autoscroll = 0;
            }
            // The next draw rebuilds the clipped action rows and retargets the
            // held pointer against them. Updating against the invalidated
            // cache here would briefly select the row that used to be under
            // the pointer before the nested viewport moved.
            return true;
        }
        self.scroll_from_bottom =
            scroll_from_bottom_by_lines(self.scroll_from_bottom, self.transcript_scroll_max, lines);
        self.transcript.follow_tail = self.scroll_from_bottom == 0;
        if let Some(pointer) = self.text_selection.map(|selection| selection.pointer) {
            self.update_text_selection_focus(pointer);
        }
        false
    }

    fn transcript_point_at(&self, pointer: Position) -> Option<TranscriptPoint> {
        let area = self.transcript_viewport_area?;
        area.contains(pointer)
            .then(|| self.transcript_point_for_pointer(area, pointer))
    }

    fn selection_point_at(&self, pointer: Position) -> Option<SelectionPoint> {
        let point = self.transcript_point_at(pointer)?;
        let (.., render) = self.transcript_render_cache.as_ref()?;
        Some(selection_point_for_row_in_lines(
            &render.6,
            &render.0,
            point.row,
            point.column,
        ))
    }

    fn composer_selection_point_at(&self, pointer: Position) -> Option<usize> {
        let area = self.composer_text_area?;
        if self.composer.text.is_empty() || area.height == 0 || area.width == 0 {
            return None;
        }
        let ranges = display_ranges(&self.composer.text, self.composer_text_width, true);
        let row = usize::from(
            pointer
                .y
                .clamp(area.y, area.bottom().saturating_sub(1))
                .saturating_sub(area.y),
        )
        .saturating_add(usize::from(self.composer_scroll))
        .min(ranges.len().saturating_sub(1));
        let (start, end) = ranges.get(row).copied()?;
        let column = usize::from(pointer.x.saturating_sub(area.x));
        Some(cursor_at_column(&self.composer.text, start, end, column))
    }

    fn update_composer_selection_drag(&mut self, pointer: Position) {
        let Some(focus) = self.composer_selection_point_at(pointer) else {
            return;
        };
        if let Some(selection) = self.composer_selection.as_mut() {
            selection.focus = focus;
            selection.pointer = pointer;
        }
        self.composer.cursor = focus;
        self.composer.preferred_column = None;
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
        let autoscroll = selection_autoscroll_direction(area, pointer);
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
        let scroll_start = self
            .transcript_scroll_max
            .saturating_sub(self.scroll_from_bottom.min(self.transcript_scroll_max));
        let Some((.., render)) = self.transcript_render_cache.as_ref() else {
            return;
        };
        let ranges = render.6.as_slice();
        let lines = render.0.as_slice();
        let point = selection_point_for_viewport_pointer_in_lines(
            area,
            scroll_start,
            pointer,
            ranges,
            lines,
        );
        if let Some(selection) = self.text_selection.as_mut() {
            selection.focus = point;
        }
    }

    fn copy_text_selection(&mut self) -> bool {
        if self.copy_composer_selection() {
            return true;
        }
        let Some((.., render)) = self.transcript_render_cache.as_ref() else {
            return false;
        };
        let Some((start, end)) = self
            .text_selection
            .filter(|selection| !selection.is_empty())
            .and_then(|selection| resolved_selection_in_lines(selection, &render.6, &render.0))
        else {
            return false;
        };
        let Some(text) = selected_transcript_text(&render.0, start, end) else {
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

    fn copy_composer_selection(&mut self) -> bool {
        let Some(selection) = self.composer_selection.filter(|selection| {
            !selection.is_empty()
                && selection.anchor <= self.composer.text.len()
                && selection.focus <= self.composer.text.len()
        }) else {
            return false;
        };
        let (start, end) = if selection.anchor <= selection.focus {
            (selection.anchor, selection.focus)
        } else {
            (selection.focus, selection.anchor)
        };
        let Some(text) = self
            .composer
            .text
            .get(start..end)
            .filter(|text| !text.is_empty())
        else {
            return false;
        };
        match clipboard::copy(text) {
            Ok(lease) => {
                self.clipboard_lease = lease;
                self.show_copy_notice("✓ Copied composer selection to clipboard");
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
        self.transcript.follow_tail = self.scroll_from_bottom == 0;
    }

    fn capture_transcript_anchor_for_collapse(&mut self) {
        if self.scroll_from_bottom == 0 || self.pending_transcript_anchor.is_some() {
            return;
        }
        let Some(area) = self.transcript_viewport_area else {
            return;
        };
        let Some((.., render)) = self.transcript_render_cache.as_ref() else {
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

    fn copy_last_assistant_message(&mut self) {
        let Some(text) = self.transcript.last_assistant_message_text() else {
            self.notice = Some("No assistant message is available to copy".to_string());
            return;
        };
        match clipboard::copy(&text) {
            Ok(lease) => {
                self.clipboard_lease = lease;
                self.show_copy_notice("✓ Copied last assistant message to clipboard");
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
        let Some(target) = rewind_target_for_output(user_message_count, &self.rewind_targets)
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

    fn revert_compaction_action(&mut self, index: usize) -> UiAction {
        let Some(sequence) = self.transcript.compaction_revert_sequence(index) else {
            self.notice = Some("This compaction checkpoint is not revertable".to_string());
            return UiAction::None;
        };
        UiAction::RevertTo { sequence }
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
            Some("Revert to after compaction") => self.revert_compaction_action(index),
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
        if matches!(picker.kind, PickerKind::Commands) {
            let command = picker.selected_value();
            // Keybinding rows carry no command; selecting one just dismisses.
            if command.is_empty() {
                return Ok(UiAction::None);
            }
            if command.starts_with("/ext:") {
                self.composer.restore(format!("{command} "), Vec::new());
                self.notice = Some(format!("{command} · add JSON or text arguments, then send"));
                return Ok(UiAction::None);
            }
            if slash_command_needs_argument(&command) {
                self.composer.restore(format!("{command} "), Vec::new());
                self.notice = Some(format!("{command} · add the message, then send"));
                return Ok(UiAction::None);
            }
            return Ok(UiAction::Submit {
                target: self.focused_child,
                text: command,
                attachments: Vec::new(),
            });
        }
        Ok(match picker.kind {
            PickerKind::Commands => unreachable!("handled above"),
            PickerKind::Settings => UiAction::Submit {
                target: None,
                text: picker.selected_value(),
                attachments: Vec::new(),
            },
            PickerKind::Resume => UiAction::Submit {
                target: None,
                text: format!("/resume {}", picker.selected_value()),
                attachments: Vec::new(),
            },
            PickerKind::Model => UiAction::SetModel(picker.selected_value()),
            PickerKind::ProviderAuth => {
                let choice = match picker.selected_value().as_str() {
                    "subscription" => Some(ProviderAuthChoice::Subscription),
                    "api-key" => Some(ProviderAuthChoice::ApiKey),
                    _ => None,
                };
                let model = self.pending_auth_model.take();
                match (choice, model) {
                    (Some(choice), Some(model)) => {
                        let provider =
                            CodingProvider::for_model(&model).unwrap_or(CodingProvider::Claude);
                        UiAction::AuthenticateProvider {
                            provider,
                            model,
                            choice,
                        }
                    }
                    _ => UiAction::None,
                }
            }
            PickerKind::Effort => UiAction::SetEffort(picker.selected_value()),
            PickerKind::Permission => {
                UiAction::SetPermissionMode(match picker.selected_value().as_str() {
                    "full access" => PermissionMode::FullAccess,
                    "auto approvals" => PermissionMode::Auto,
                    "manual approvals" => PermissionMode::Manual,
                    _ => unreachable!("permission picker values are canonical"),
                })
            }
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
                UiAction::SetSteerActive(picker.selected_value() == ACTIVE_MESSAGES_SEND_NOW)
            }
            PickerKind::AutoExpandEdits => {
                UiAction::SetAutoExpandEdits(picker.selected_value() == "On")
            }
            PickerKind::AutoExpandTools => {
                UiAction::SetAutoExpandTools(picker.selected_value() == "On")
            }
            PickerKind::DictationIcon => match picker.selected_value().as_str() {
                "nerd_font" => UiAction::SetDictationIcon(DictationIconStyle::NerdFont),
                "emoji" => UiAction::SetDictationIcon(DictationIconStyle::Emoji),
                _ => UiAction::None,
            },
            PickerKind::Goal => {
                let value = picker.selected_value();
                match value.as_str() {
                    "/goal pause" | "/goal resume" | "/goal clear" => UiAction::Submit {
                        // Goals are owned by the director session. A focused
                        // child remains a viewing context, not a different goal
                        // command endpoint.
                        target: None,
                        text: value,
                        attachments: Vec::new(),
                    },
                    _ => UiAction::None,
                }
            }
            PickerKind::Rewind => unreachable!("handled above"),
            PickerKind::MessageActions => unreachable!("handled separately"),
        })
    }

    pub fn draw(&mut self) -> Result<()> {
        self.draw_internal(false)
    }

    /// Draw input feedback without rebuilding a transcript that is currently
    /// being updated by the session stream. The next ordinary frame refreshes
    /// the transcript from the latest projection.
    pub fn draw_for_input(&mut self) -> Result<()> {
        self.draw_internal(true)
    }

    fn draw_internal(&mut self, input_fast_path: bool) -> Result<()> {
        if self
            .copy_notice_expires_at
            .is_some_and(|expires_at| Instant::now() >= expires_at)
        {
            if self.notice.as_deref().is_some_and(is_copy_notice) {
                self.notice = None;
            }
            self.copy_notice_expires_at = None;
        }
        let picker_open = self.picker.is_some();
        let background_hover_suppressed = overlay_suppresses_background_hover(
            picker_open,
            self.team_switcher_open,
            self.keybindings_open,
        );
        if background_hover_suppressed {
            self.clear_background_hover();
        }
        if picker_open || self.keybindings_open {
            self.hovered_team_roster = None;
        }
        let title = terminal_title(self.active_status(), self.transcript.first_prompt());
        if self.last_terminal_title.as_deref() != Some(&title) {
            execute!(self.terminal.backend_mut(), SetTitle(&title))?;
            self.last_terminal_title = Some(title);
        }
        let terminal_size = self.terminal.size()?;
        let content_width = terminal_content_width(terminal_size.width);
        let tool_run_viewport_height = tool_run_viewport_height(terminal_size.height as usize);
        let full_transcript_width = content_width.max(1) as usize;
        let goal_tick = self.transcript.active_goal_cache_tick();
        let tool_elapsed_tick = self.transcript.tool_elapsed_cache_tick();
        let local_date = Local::now().date_naive();
        // Keep a separate full-width measurement so an overflowing transcript
        // can switch to the scrollbar-safe width without rendering both widths
        // again on every frame.
        // Input redraws must not rebuild the transcript while a live event is
        // invalidating its normal viewport cache. Reuse the last complete
        // snapshot for the composer frame; the next ordinary frame catches up
        // the transcript and its elapsed-time projection.
        let stale_full_transcript_render = if input_fast_path {
            self.transcript_full_render_cache
                .as_ref()
                .filter(
                    |(cached_width, cached_tool_run_viewport_height, _, _, cached_date, _)| {
                        *cached_width == full_transcript_width
                            && *cached_tool_run_viewport_height == tool_run_viewport_height
                            && *cached_date == local_date
                    },
                )
                .map(|(_, _, _, _, _, render)| Arc::clone(render))
        } else {
            None
        };
        let full_transcript_render = stale_full_transcript_render.unwrap_or_else(|| {
            if self.transcript_render_cache.is_some() {
                cached_transcript_render(
                    &self.transcript,
                    &mut self.transcript_full_render_cache,
                    full_transcript_width,
                    tool_run_viewport_height,
                    goal_tick,
                    tool_elapsed_tick,
                    local_date,
                )
            } else {
                self.transcript_full_render_cache = None;
                cached_transcript_render(
                    &self.transcript,
                    &mut self.transcript_full_render_cache,
                    full_transcript_width,
                    tool_run_viewport_height,
                    goal_tick,
                    tool_elapsed_tick,
                    local_date,
                )
            }
        });
        let queued_prompts = self.active_queued_prompts().to_vec();
        // Keep the first draft anchored in the splash composition area. Moving
        // it to the chat footer on the first keystroke makes the whole screen
        // jump before the user has actually submitted anything.
        let is_launch_screen = full_transcript_render.0.is_empty() && queued_prompts.is_empty();
        let modal_picker_open = matches!(
            self.picker.as_ref().map(|picker| picker.kind),
            Some(PickerKind::MessageActions | PickerKind::Goal)
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
        let pending_approval = self.active_pending_approval();
        let pending_provider_interaction =
            self.focused_child.is_none() && self.pending_provider_interaction;
        let pending_provider_interaction_secret =
            self.focused_child.is_none() && self.pending_provider_interaction_secret;
        let status = self.active_status();
        let status_label = if self.borging_this_run
            && matches!(status, SessionStatus::Starting | SessionStatus::Running)
        {
            "borging"
        } else {
            status_label(status)
        };
        let status_glyph = activity_glyph(status);
        let status_is_interruptible = status_control_is_actionable(status);
        let (model_status, effort_status, fast_status, permission_status, mut cwd_status) =
            self.transcript.config_statuses();
        let active_cwd = self
            .transcript
            .config
            .as_ref()
            .map(|config| config.cwd.clone())
            .unwrap_or_else(|| self.cwd.clone());
        if cwd_status.is_empty() {
            cwd_status = fish_style_path(&active_cwd);
        }
        if let Some(git_status) = self.git_status_cache.status_for(&active_cwd) {
            cwd_status.push_str(" · ");
            cwd_status.push_str(&git_status.compact_label());
        }
        let cache_status = self.transcript.cache_status(Utc::now());
        let (_, context_imminent) = self.transcript.context_status();
        let context_status = self.transcript.context_limit_label();
        let context_tooltip = self.transcript.context_tooltip();
        let team_transcript = self
            .director_transcript
            .as_deref()
            .unwrap_or(&self.transcript);
        let active_subagents = team_transcript.active_subagent_count();
        let agent_roster_entries = team_transcript.agent_roster_entries();
        let focused_agent_name = self.focused_child.and_then(|child| {
            team_transcript
                .subagent_snapshots
                .get(&child)
                .map(|agent| display_agent_name(&agent.task_name))
        });
        let agent_roster_rows = agent_roster_entries
            .iter()
            .map(|(row, _)| row.clone())
            .collect::<Vec<_>>();
        let total_subagents = agent_roster_entries.len().saturating_sub(1);
        let session_is_active = matches!(status, SessionStatus::Starting | SessionStatus::Running);
        let active_status_started_at = self.active_status_started_at();
        let active_goal = self.active_goal().cloned();
        let goal_status = self.transcript.goal_status();
        let todo_status = self.transcript.todo_status();
        let slash_suggestions = (self.picker.is_none())
            .then(|| slash_suggestion_lines(&self.composer.text, self.slash_selection))
            .filter(|lines| !lines.is_empty());
        let showing_slash_suggestions = slash_suggestions.is_some();
        let notice = self.notice.clone();
        let cold_cache_guidance = cache_status
            .as_ref()
            .filter(|status| status.warning)
            .filter(|_| {
                status == SessionStatus::Ready
                    && self.picker.is_none()
                    && (!self.composer.text.trim().is_empty()
                        || !self.composer.attachments.is_empty())
                    && !showing_slash_suggestions
                    && notice.is_none()
            })
            .map(CacheStatus::cold_cache_guidance);
        let showing_primary_controls =
            !showing_slash_suggestions && notice.is_none() && cold_cache_guidance.is_none();
        let transcript_interaction_hint = self
            .hovered_tool
            .and_then(|index| self.transcript.tool_copy_hint(index))
            .or_else(|| message_interaction_hint(&self.transcript.order, self.hovered_message));
        let showing_transcript_interaction_hint =
            showing_primary_controls && transcript_interaction_hint.is_some();
        let primary_controls = if resume_picker_open {
            format!(
                "filter type · select ↑↓ · older PgUp/PgDn · resume {} · close {}",
                self.keymap.label(KeyAction::Send),
                self.keymap.label(KeyAction::Interrupt)
            )
        } else {
            primary_controls_line(&self.keymap)
        };
        let interaction_hint = bottom_interaction_hint(BottomInteractionHintState {
            status_hovered: self.status_hovered,
            status_is_interruptible,
            goal_status_hovered: self.goal_status_hovered,
            goal_available: active_goal.is_some(),
            agents_status_hovered: self.agents_status_hovered,
            model_status_hovered: self.model_status_hovered,
            effort_status_hovered: self.effort_status_hovered,
            permission_status_hovered: self.permission_status_hovered,
        });
        let hover_notice_hint = transcript_interaction_hint
            .or(interaction_hint)
            .filter(|_| !showing_slash_suggestions && notice.is_some());
        let primary_controls_display = if showing_transcript_interaction_hint {
            transcript_interaction_hint
                .expect("transcript interaction hint is present")
                .to_string()
        } else {
            interaction_hint.map_or_else(
                || primary_controls.clone(),
                |hint| format!("{hint} · {primary_controls}"),
            )
        };
        let keybindings_hint = format!("keybindings {}", self.keymap.label(KeyAction::Keybindings));
        let copy_notice_active = notice.as_deref().is_some_and(is_copy_notice);
        let copy_notice_text = notice.clone().filter(|_| copy_notice_active);
        let notice_style = if copy_notice_active {
            Style::default()
                .fg(Color::Black)
                .bg(Color::LightGreen)
                .add_modifier(Modifier::BOLD)
        } else if self.picker.is_none() && self.composer.text.trim_start().starts_with('/') {
            Style::default().fg(Color::White)
        } else {
            Style::default().fg(Color::DarkGray)
        };
        let controls = slash_suggestions.unwrap_or_else(|| {
            if let Some(notice) = copy_notice_text.as_ref() {
                vec![copy_notice_line(notice.clone())]
            } else if let Some(hint) = hover_notice_hint {
                vec![Line::from(Span::styled(
                    hint,
                    Style::default().fg(Color::Yellow),
                ))]
            } else if let Some(notice) = notice {
                if copy_notice_active {
                    vec![copy_notice_line(notice)]
                } else {
                    vec![Line::from(Span::styled(notice, notice_style))]
                }
            } else if let Some(guidance) = cold_cache_guidance {
                wrap_display(&guidance, content_width.saturating_sub(2).max(1) as usize)
                    .into_iter()
                    .map(|line| Line::from(Span::styled(line, Style::default().fg(Color::Yellow))))
                    .collect()
            } else if showing_transcript_interaction_hint {
                vec![Line::from(Span::styled(
                    transcript_interaction_hint.expect("transcript interaction hint is present"),
                    Style::default().fg(Color::Yellow),
                ))]
            } else {
                vec![if let Some(hint) = interaction_hint {
                    Line::from(vec![
                        Span::styled(hint, Style::default().fg(Color::Yellow)),
                        Span::raw(" · "),
                        Span::raw(primary_controls.clone()),
                    ])
                } else {
                    Line::from(primary_controls.clone())
                }]
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
            .saturating_sub(DICTATION_BUTTON_WIDTH)
            .max(1) as usize;
        let (composer_display_text, composer_display_cursor) =
            if pending_provider_interaction_secret {
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
            .filter(|picker| {
                !matches!(
                    picker.kind,
                    PickerKind::MessageActions | PickerKind::Commands | PickerKind::Goal
                )
            })
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
        let prompt_marker = if pending_approval || pending_provider_interaction {
            " ! "
        } else {
            " > "
        };
        let mut composer_render_lines = if self.picker.as_ref().is_some_and(|_| !modal_picker_open)
        {
            Vec::new()
        } else if self.composer.text.is_empty() {
            let placeholder = if pending_provider_interaction {
                "Answer the provider request…"
            } else {
                match status {
                    SessionStatus::Running | SessionStatus::Starting => {
                        active_message_placeholder(self.steer_active_turn)
                    }
                    SessionStatus::WaitingForApproval => "Allow · Y   Deny · N",
                    _ => "Describe a task…",
                }
            };
            vec![Line::from(vec![
                Span::styled(prompt_marker, Style::default().fg(Color::DarkGray)),
                Span::styled(placeholder, Style::default().fg(Color::DarkGray)),
            ])]
        } else if pending_provider_interaction_secret {
            styled_plain_composer_lines(&composer_display_text, &composer_ranges, prompt_marker)
        } else {
            self.composer
                .styled_lines_for_ranges(&composer_ranges, prompt_marker)
        };
        if self.picker.is_none()
            && !pending_provider_interaction_secret
            && let Some(selection) = self.composer_selection
        {
            apply_composer_selection(
                &mut composer_render_lines,
                &composer_display_text,
                &composer_ranges,
                UnicodeWidthStr::width(prompt_marker),
                selection.anchor,
                selection.focus,
            );
        }
        let composer_max_height = if resume_picker_open { 18 } else { 8 };
        let composer_height = composer_panel_height(
            composer_line_count,
            composer_cursor.0,
            composer_max_height,
            is_launch_screen && resume_picker_open,
        );
        let composer_height = if is_launch_screen {
            bounded_launch_composer_height(composer_height, terminal_size.height, controls_height)
        } else {
            composer_height
        };
        let composer_scroll = if let Some(picker) = self.picker.as_ref().filter(|picker| {
            !matches!(
                picker.kind,
                PickerKind::Commands | PickerKind::MessageActions | PickerKind::Goal
            )
        }) {
            let content_height = usize::from(composer_height.saturating_sub(2));
            picker.scroll_offset(content_height, composer_line_count) as u16
        } else {
            (composer_cursor.0 as u16).saturating_sub(composer_height.saturating_sub(3))
        };
        let transcript_viewport_height = if is_launch_screen {
            0
        } else {
            let area =
                centered_content_area(Rect::new(0, 0, terminal_size.width, terminal_size.height));
            let chunks = terminal_vertical_chunks(
                area,
                queued_prompt_panel_height(&queued_prompts, area.width),
                composer_height,
                footer_height,
                is_launch_screen,
            );
            usize::from(chunks[0].height.saturating_sub(1))
        };
        let transcript_width = transcript_width_for_viewport(
            content_width,
            full_transcript_render.0.len(),
            transcript_viewport_height,
        );
        let transcript_render = if input_fast_path {
            Arc::clone(&full_transcript_render)
        } else if transcript_width == full_transcript_width {
            self.transcript_render_cache = self.transcript_full_render_cache.clone();
            Arc::clone(&full_transcript_render)
        } else {
            cached_transcript_render(
                &self.transcript,
                &mut self.transcript_render_cache,
                transcript_width,
                tool_run_viewport_height,
                goal_tick,
                tool_elapsed_tick,
                local_date,
            )
        };
        let (
            transcript,
            tool_rows,
            tool_run_rows,
            message_rows,
            entry_rows,
            link_rows,
            selection_rows,
        ) = transcript_render.as_ref();
        let transcript_height = transcript.len();
        self.scroll_from_bottom = resolve_pending_scroll_anchor(
            self.transcript.follow_tail,
            self.scroll_from_bottom,
            self.pending_scroll_anchor_height.take(),
            transcript_height,
        );
        if self.transcript.follow_tail {
            // A viewport that returned to the live tail no longer has a
            // detached-content anchor to preserve. This can happen between
            // the event and render arms while wheel motion is animating.
            self.pending_transcript_anchor = None;
        }
        self.rendered_transcript_height = transcript_height;
        let mut next_scrollbar_area = None;
        let mut next_scrollbar_thumb_area = None;
        let mut next_transcript_viewport_area = None;
        let mut next_composer_area = None;
        let mut next_composer_text_area = None;
        let mut next_scroll_max = 0;
        let mut next_tool_hit_areas = Vec::new();
        let mut next_tool_run_hit_areas = Vec::new();
        let mut next_tool_run_header_hit_areas = Vec::new();
        let mut next_message_hit_areas = Vec::new();
        let mut next_link_hit_areas = Vec::new();
        let mut next_entry_hit_areas = Vec::new();
        let mut next_picker_hit_areas = Vec::new();
        let mut next_jump_to_bottom_area = None;
        let mut next_status_area = None;
        let mut next_goal_status_area = None;
        let mut next_todo_status_area = None;
        let mut next_agents_status_area = None;
        let mut next_model_status_area = None;
        let mut next_effort_status_area = None;
        let mut next_context_status_area = None;
        let mut next_fast_status_area = None;
        let mut next_permission_status_area = None;
        let mut next_team_roster_hit_areas = Vec::new();
        let mut next_back_to_director_area = None;
        let mut next_keybindings_hint_area = None;
        let mut next_dictation_button_area = None;
        let dictation_state = self.dictation_state;
        let dictation_button_hovered = self.dictation_button_hovered;
        let pending_transcript_anchor = self.pending_transcript_anchor.take();
        let mut restored_scroll_from_bottom = None;
        let cursor_visible = cursor_blink_visible(self.cursor_blink_started_at.elapsed());
        self.terminal.draw(|frame| {
            let area = centered_content_area(frame.area());
            let chunks = terminal_vertical_chunks(
                area,
                queued_prompt_panel_height(&queued_prompts, area.width),
                composer_height,
                footer_height,
                is_launch_screen,
            );
            let status_color = focused_subagent_status_color(status, self.focused_child.is_some());
            let (status_area, transcript_area, composer_area, footer_area) = if is_launch_screen {
                let launch_width = composer_area_width.min(chunks[0].width);
                let launch_height = composer_height
                    .saturating_add(7)
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
                        Constraint::Length(6),
                        Constraint::Length(composer_height),
                        Constraint::Length(controls_height),
                    ])
                    .split(launch);
                frame.render_widget(
                    Paragraph::new(vec![
                        splash_logo_line(self.splash_started_at.elapsed(), self.splash_glitch_seed),
                        splash_alpha_line(),
                        Line::from(Span::styled(
                            splash_version(),
                            Style::default().fg(Color::DarkGray),
                        )),
                        Line::from(""),
                        Line::from(Span::styled(
                            "What are we working on?",
                            Style::default().fg(Color::Gray),
                        )),
                        Line::from(""),
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
            next_composer_text_area = Some(Rect {
                x: composer_area
                    .x
                    .saturating_add(composer_cursor_x_offset(is_launch_screen)),
                y: composer_area.y.saturating_add(u16::from(!is_launch_screen)),
                width: composer_text_width.min(u16::MAX as usize) as u16,
                height: composer_area
                    .height
                    .saturating_sub(u16::from(!is_launch_screen)),
            });
            next_composer_area = Some(composer_area);
            if !is_launch_screen && let Some(todo_status) = todo_status.as_ref() {
                let metadata_width =
                    footer_metadata_text(todo_status, &cwd_status, usize::MAX).width() as u16;
                let visible_metadata_width = metadata_width.min(footer_area.width);
                next_todo_status_area = Some(Rect {
                    x: footer_area.right().saturating_sub(visible_metadata_width),
                    y: footer_area.y,
                    width: (todo_status.width() as u16).min(visible_metadata_width),
                    height: 1,
                });
            }
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
                let content_area = Rect {
                    width: (transcript_width.min(transcript_area.width as usize)) as u16,
                    ..transcript_area
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
                let show_history_loader = self.history_page_loading;
                let visible_height = content_area.height as usize;
                let sticky_tool_run_header = sticky_tool_run_header_row(
                    tool_run_rows,
                    scroll_start,
                )
                .map(|(index, row, expandable)| {
                    let mut header = transcript[row].clone();
                    if self.transcript.tool_activity_is_running(index) {
                        replace_tool_activity_glyph(
                            &mut header,
                            activity_glyph(SessionStatus::Running),
                        );
                        apply_running_activity_pulse(&mut header, running_activity_pulse_phase());
                    }
                    (index, header, expandable)
                });
                let sticky_index = tool_rows.partition_point(|(_, start, _)| *start < scroll_start);
                let sticky_tool_header = if sticky_tool_run_header.is_some() {
                    None
                } else {
                    sticky_index
                        .checked_sub(1)
                        .and_then(|index| tool_rows.get(index))
                        .filter(|(_, _, end)| *end > scroll_start)
                        .map(|(index, start, _)| {
                            let mut header = transcript[*start].clone();
                            if self.transcript.tool_activity_is_running(*index) {
                                replace_tool_activity_glyph(
                                    &mut header,
                                    activity_glyph(SessionStatus::Running),
                                );
                                apply_running_activity_pulse(
                                    &mut header,
                                    running_activity_pulse_phase(),
                                );
                            }
                            (*index, header)
                        })
                };
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
                    if *start >= scroll_start
                        && self.transcript.tool_activity_is_running(*index)
                        && let Some(line) = visible_transcript.get_mut(*start - scroll_start)
                    {
                        replace_tool_activity_glyph(line, activity_glyph(SessionStatus::Running));
                        apply_running_activity_pulse(line, running_activity_pulse_phase());
                    }
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
                for (start_index, start, end, max_offset, expandable) in tool_run_rows
                    .iter()
                    .filter(|(_, start, end, _, _)| *end > scroll_start && *start < visible_end)
                {
                    if *expandable && self.hovered_tool_run_header == Some(*start_index) {
                        apply_viewport_background(
                            &mut visible_transcript,
                            *start,
                            start.saturating_add(1),
                            scroll_start,
                            content_area.width as usize,
                            MESSAGE_HOVER_BG,
                        );
                    }
                    next_tool_run_hit_areas.push((
                        viewport_hit_area(content_area, scroll_start, *start, *end),
                        *start_index,
                        *max_offset,
                    ));
                    if *expandable {
                        next_tool_run_header_hit_areas.push((
                            viewport_hit_area(
                                content_area,
                                scroll_start,
                                *start,
                                start.saturating_add(1),
                            ),
                            *start_index,
                        ));
                    }
                }
                for (index, start, end) in
                    visible_row_ranges(message_rows, scroll_start, visible_height)
                {
                    apply_viewport_background(
                        &mut visible_transcript,
                        *start,
                        *end,
                        scroll_start,
                        content_area.width as usize,
                        if self.hovered_message == Some(*index) {
                            MESSAGE_HOVER_BG
                        } else {
                            MESSAGE_BG
                        },
                    );
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
                for link in link_rows.iter().filter(|link| {
                    self.hovered_link.as_deref() == Some(link.url.as_str())
                        && link.row >= scroll_start
                        && link.row < visible_end
                }) {
                    if let Some(line) = visible_transcript.get_mut(link.row - scroll_start) {
                        apply_link_hover(line, link.start, link.end);
                    }
                }
                if let Some(selection) = self
                    .text_selection
                    .as_mut()
                    .filter(|selection| selection.dragging)
                {
                    // The pointer is screen-relative while a drag is held.
                    // Resolve it again on every draw so streaming content,
                    // wheel motion, and viewport reflow all extend the focus
                    // to the text that is actually under the mouse now.
                    selection.focus = selection_point_for_viewport_pointer_in_lines(
                        content_area,
                        scroll_start,
                        selection.pointer,
                        selection_rows,
                        transcript,
                    );
                }
                if let Some((selection_start, selection_end)) =
                    self.text_selection.and_then(|selection| {
                        resolved_selection_in_lines(selection, selection_rows, transcript)
                    })
                {
                    apply_text_selection(
                        &mut visible_transcript,
                        scroll_start,
                        selection_start,
                        selection_end,
                    );
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
                if !show_history_loader
                    && let Some((index, mut header, expandable)) = sticky_tool_run_header
                {
                    apply_line_background(
                        &mut header,
                        content_area.width as usize,
                        sticky_tool_header_background(
                            expandable && self.hovered_tool_run_header == Some(index),
                        ),
                    );
                    let sticky_area = Rect {
                        height: 1,
                        ..content_area
                    };
                    frame.render_widget(Paragraph::new(header), sticky_area);
                    if expandable {
                        next_tool_run_header_hit_areas.push((sticky_area, index));
                    }
                } else if !show_history_loader && let Some((index, mut header)) = sticky_tool_header
                {
                    apply_line_background(
                        &mut header,
                        content_area.width as usize,
                        sticky_tool_header_background(self.hovered_tool == Some(index)),
                    );
                    let sticky_area = Rect {
                        height: 1,
                        ..content_area
                    };
                    frame.render_widget(Paragraph::new(header), sticky_area);
                    next_tool_hit_areas.push((sticky_area, index));
                }
                if show_history_loader {
                    let loader_area = Rect {
                        height: 1,
                        ..content_area
                    };
                    frame.render_widget(Clear, loader_area);
                    frame.render_widget(Paragraph::new(history_loading_line()), loader_area);
                    let is_behind_loader = |area: &Rect| {
                        area.y < loader_area.bottom() && area.bottom() > loader_area.y
                    };
                    next_tool_hit_areas.retain(|(area, _)| !is_behind_loader(area));
                    next_tool_run_hit_areas.retain(|(area, _, _)| !is_behind_loader(area));
                    next_tool_run_header_hit_areas.retain(|(area, _)| !is_behind_loader(area));
                    next_message_hit_areas.retain(|(area, _)| !is_behind_loader(area));
                    next_entry_hit_areas.retain(|(area, _)| !is_behind_loader(area));
                    next_link_hit_areas.retain(|(area, _)| !is_behind_loader(area));
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
                                    if self.focused_child.is_some() {
                                        if self.scrollbar_hovered || self.dragging_scrollbar {
                                            SUBAGENT_PINK_HOVER
                                        } else {
                                            SUBAGENT_PINK
                                        }
                                    } else if self.scrollbar_hovered || self.dragging_scrollbar {
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
                        self.focused_child.is_some().then_some(SUBAGENT_PINK),
                    ))
                    .block(
                        Block::default()
                            .borders(Borders::TOP | Borders::LEFT)
                            .border_style(Style::default().fg(Color::DarkGray))
                            .title(Span::styled(
                                format!(" Pending Input · {} ", queued_prompts.len()),
                                Style::default()
                                    .fg(if self.focused_child.is_some() {
                                        SUBAGENT_PINK
                                    } else {
                                        BORG_ORANGE
                                    })
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
            let mut composer_block = Block::default()
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
            if let Some(name) = focused_agent_name.as_deref() {
                composer_block = composer_block.title(Span::styled(
                    format!(" TO {name} "),
                    Style::default()
                        .fg(SUBAGENT_PINK)
                        .add_modifier(Modifier::BOLD),
                ));
            }
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
            if self.picker.is_none() {
                let button = Rect {
                    x: composer_area.right().saturating_sub(DICTATION_BUTTON_WIDTH),
                    y: composer_area.y.saturating_add(u16::from(!is_launch_screen)),
                    width: DICTATION_BUTTON_WIDTH.min(composer_area.width),
                    height: 1,
                };
                let (label, color) = match dictation_state {
                    DictationState::Idle => (dictation_icon(self.dictation_icon), BORG_ORANGE),
                    DictationState::Installing => (" ...  ", Color::Yellow),
                    DictationState::Recording => ("  ■   ", Color::LightRed),
                    DictationState::Transcribing => (" ...  ", Color::Yellow),
                };
                frame.render_widget(
                    Paragraph::new(label).alignment(Alignment::Center).style(
                        if dictation_button_hovered {
                            Style::default()
                                .fg(Color::Black)
                                .bg(color)
                                .add_modifier(Modifier::BOLD)
                        } else {
                            Style::default().fg(color).add_modifier(Modifier::BOLD)
                        },
                    ),
                    button,
                );
                next_dictation_button_area = Some(button);
            }
            if let Some(picker) = self.picker.as_ref().filter(|picker| {
                !matches!(
                    picker.kind,
                    PickerKind::MessageActions | PickerKind::Commands | PickerKind::Goal
                )
            }) {
                let picker_hit_width = if matches!(picker.kind, PickerKind::Resume) {
                    resume_left_width(composer_area.width.saturating_sub(4) as usize) as u16
                } else {
                    composer_area.width.saturating_sub(2)
                };
                for (index, line) in picker.option_row_offsets() {
                    let Some(line) = line.checked_sub(composer_scroll as usize) else {
                        continue;
                    };
                    let row = Rect {
                        x: composer_area.x.saturating_add(1),
                        y: composer_area
                            .y
                            .saturating_add(u16::from(!is_launch_screen))
                            .saturating_add(line as u16),
                        width: picker_hit_width,
                        height: 1,
                    };
                    if row.y < composer_area.bottom() {
                        next_picker_hit_areas.push((row, index));
                    }
                }
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
            let status_highlight = self.status_hovered && status_is_interruptible;
            let status_duration = if session_is_active {
                format_elapsed_duration(active_status_started_at.map_or(0, |started| {
                    Utc::now()
                        .signed_duration_since(started)
                        .num_seconds()
                        .max(0) as u64
                }))
            } else {
                None
            };
            let mut status_spans = status_control_spans(
                status_glyph,
                status_label,
                status_color,
                status_highlight,
                status_duration.as_deref(),
            );
            if session_is_active {
                let mut status_line = Line::from(status_spans);
                apply_running_activity_pulse(&mut status_line, running_activity_pulse_phase());
                status_spans = status_line.spans;
            }
            let status_width = status_spans.iter().map(|span| span.width()).sum::<usize>();
            let agents_status = agents_status_label(active_subagents);
            let agents_status_width = agents_status
                .as_ref()
                .map(|status| activity_glyph(SessionStatus::Running).width() + 1 + status.width());
            if agents_status.is_some() {
                status_spans.push(Span::styled(
                    STATUS_SEPARATOR,
                    Style::default().fg(Color::Gray),
                ));
            }
            let agents_status_start = status_spans.iter().map(|span| span.width()).sum::<usize>();
            if let Some(agents_status) = agents_status {
                let hovered = self.agents_status_hovered || self.team_switcher_open;
                status_spans.push(Span::styled(
                    format!("{} ", activity_glyph(SessionStatus::Running)),
                    agents_status_spinner_style(hovered),
                ));
                status_spans.push(Span::styled(
                    agents_status,
                    agents_status_text_style(hovered),
                ));
            }
            let goal_status_start = status_spans.iter().map(|span| span.width()).sum::<usize>();
            // The separator is rendered unstyled, so the hover target is the
            // value alone.
            let goal_status_start = goal_status_start
                .saturating_add(usize::from(goal_status.is_some()) * STATUS_SEPARATOR.width());
            let goal_status_width = goal_status.as_ref().map(|value| value.width());
            if let Some(goal_status) = goal_status.clone() {
                // Every durable goal can be managed from the modal, including
                // completed or budget-limited goals where only "clear" is
                // available.
                let highlight = self.goal_status_hovered;
                status_spans.push(Span::styled(
                    STATUS_SEPARATOR,
                    Style::default().fg(Color::Gray),
                ));
                status_spans.push(Span::styled(
                    goal_status,
                    Style::default()
                        .fg(if highlight {
                            Color::White
                        } else {
                            Color::Yellow
                        })
                        .add_modifier(if highlight {
                            Modifier::BOLD | Modifier::UNDERLINED
                        } else {
                            Modifier::empty()
                        }),
                ));
            }
            let model_status_start = status_spans.iter().map(|span| span.width()).sum::<usize>();
            // The separator is rendered unstyled, so the hover target is the
            // value alone.
            let model_status_start = model_status_start
                .saturating_add(usize::from(model_status.is_some()) * STATUS_SEPARATOR.width());
            let model_status_width = model_status.as_ref().map(|value| value.width());
            push_interactive_status_segment(
                &mut status_spans,
                model_status,
                self.model_status_hovered,
                Color::Gray,
            );
            let effort_status_start = status_spans.iter().map(|span| span.width()).sum::<usize>();
            // The separator is rendered unstyled, so the hover target is the
            // value alone.
            let effort_status_start = effort_status_start
                .saturating_add(usize::from(effort_status.is_some()) * STATUS_SEPARATOR.width());
            let effort_status_width = effort_status.as_ref().map(|value| value.width());
            let effort_status_color = effort_status
                .as_deref()
                .map(effort_status_color)
                .unwrap_or(Color::Gray);
            push_interactive_status_segment(
                &mut status_spans,
                effort_status,
                self.effort_status_hovered,
                effort_status_color,
            );
            let fast_status_start = status_spans.iter().map(|span| span.width()).sum::<usize>();
            let fast_status_start = fast_status_start
                .saturating_add(usize::from(fast_status.is_some()) * STATUS_SEPARATOR.width());
            let fast_status_width = fast_status.as_ref().map(|value| value.width());
            push_interactive_status_segment(
                &mut status_spans,
                fast_status,
                self.fast_status_hovered,
                Color::LightYellow,
            );
            let permission_status_start =
                status_spans.iter().map(|span| span.width()).sum::<usize>();
            // The separator is rendered unstyled, so the hover target is the
            // value alone.
            let permission_status_start = permission_status_start.saturating_add(
                usize::from(permission_status.is_some()) * STATUS_SEPARATOR.width(),
            );
            let permission_status_width = permission_status.as_ref().map(|value| value.width());
            let permission_status_color = permission_status
                .as_deref()
                .map(permission_status_color)
                .unwrap_or(Color::Gray);
            push_interactive_status_segment(
                &mut status_spans,
                permission_status,
                self.permission_status_hovered,
                permission_status_color,
            );
            let context_status_start = status_spans.iter().map(|span| span.width()).sum::<usize>();
            let context_status_start = if context_status.is_empty() {
                context_status_start
            } else {
                context_status_start.saturating_add(STATUS_SEPARATOR.width())
            };
            let context_status_width = (!context_status.is_empty()).then(|| context_status.width());
            if let Some(context_status) =
                (!context_status.is_empty()).then_some(context_status.clone())
            {
                push_interactive_status_segment(
                    &mut status_spans,
                    Some(context_status),
                    self.context_status_hovered,
                    if context_imminent {
                        Color::Yellow
                    } else {
                        Color::Gray
                    },
                );
            }
            let status_line = Line::from(status_spans);
            let alignment_offset = if is_launch_screen {
                status_area.width.saturating_sub(status_line.width() as u16) / 2
            } else {
                0
            };
            next_status_area =
                status_control_hit_area(status, status_area, alignment_offset, status_width);
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
            let status_hit_area = |start: usize, width: usize| Rect {
                x: status_area
                    .x
                    .saturating_add(alignment_offset)
                    .saturating_add(start as u16),
                y: status_area.y,
                width: (width as u16).min(status_area.width),
                height: 1,
            };
            next_model_status_area =
                model_status_width.map(|width| status_hit_area(model_status_start, width));
            next_effort_status_area =
                effort_status_width.map(|width| status_hit_area(effort_status_start, width));
            next_context_status_area =
                context_status_width.map(|width| status_hit_area(context_status_start, width));
            next_fast_status_area =
                fast_status_width.map(|width| status_hit_area(fast_status_start, width));
            next_permission_status_area = permission_status_width
                .map(|width| status_hit_area(permission_status_start, width));
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
            if (self.agents_status_hovered
                || self.team_switcher_open
                || self.hovered_team_roster.is_some())
                && total_subagents > 0
            {
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
                let roster_lines = agent_roster_entries
                    .iter()
                    .enumerate()
                    .map(|(index, (row, child_id))| {
                        let focused = *child_id == self.focused_child;
                        let hovered = self.hovered_team_roster == Some(index);
                        Line::from(format!("{} {row}", if focused { "›" } else { " " }))
                            .style(team_roster_row_style(focused, hovered, child_id.is_some()))
                    })
                    .collect::<Vec<_>>();
                frame.render_widget(
                    Paragraph::new(roster_lines)
                        .style(Style::default().fg(Color::White).bg(COMMAND_PANEL_BG))
                        .block(
                            Block::default()
                                .borders(Borders::ALL)
                                .border_style(Style::default().fg(SUBAGENT_PINK))
                                .title(Span::styled(
                                    format!(" Team · {active_subagents} working "),
                                    Style::default()
                                        .fg(SUBAGENT_PINK)
                                        .add_modifier(Modifier::BOLD),
                                )),
                        ),
                    tooltip,
                );
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
            if self.focused_child.is_some() {
                let label = " ↩ Return ";
                let button = Rect {
                    x: status_area.right().saturating_sub(label.width() as u16),
                    y: status_area.y,
                    width: label.width() as u16,
                    height: 1,
                };
                frame.render_widget(
                    Paragraph::new(label).style(
                        Style::default()
                            .fg(if self.back_to_director_hovered {
                                Color::White
                            } else {
                                SUBAGENT_PINK
                            })
                            .bg(if self.back_to_director_hovered {
                                MESSAGE_HOVER_BG
                            } else {
                                COMMAND_PANEL_BG
                            })
                            .add_modifier(Modifier::BOLD),
                    ),
                    button,
                );
                next_back_to_director_area = Some(button);
            }
            if self.goal_status_hovered
                && let Some(goal) = active_goal.as_ref()
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
                                .title(goal_tooltip_title(goal)),
                        ),
                    tooltip,
                );
            }
            if self.todo_status_hovered && !self.transcript.todos.is_empty() {
                let rows = self
                    .transcript
                    .todo_tooltip_rows_with_status(self.todo_status_expanded);
                let tooltip_width = rows
                    .iter()
                    .map(|(row, _)| row.width() as u16)
                    .max()
                    .unwrap_or(24)
                    .saturating_add(4)
                    .clamp(30, status_area.width.min(96));
                let content_width = tooltip_width.saturating_sub(4).max(1) as usize;
                let tooltip_lines = rows
                    .iter()
                    .flat_map(|(row, completed)| {
                        let style = todo_tooltip_row_style(*completed);
                        wrap_display(row, content_width)
                            .into_iter()
                            .map(move |line| (line, style))
                    })
                    .collect::<Vec<_>>();
                let todo_anchor = next_todo_status_area.unwrap_or(status_area);
                let tooltip_height = (tooltip_lines.len() as u16)
                    .saturating_add(2)
                    .min(todo_anchor.y.saturating_sub(area.y).max(1));
                let tooltip = Rect {
                    x: todo_anchor
                        .x
                        .min(area.right().saturating_sub(tooltip_width)),
                    y: todo_anchor.y.saturating_sub(tooltip_height),
                    width: tooltip_width,
                    height: tooltip_height,
                };
                frame.render_widget(Clear, tooltip);
                frame.render_widget(
                    Paragraph::new(
                        tooltip_lines
                            .into_iter()
                            .map(|(line, style)| Line::from(line).style(style))
                            .collect::<Vec<_>>(),
                    )
                    .style(Style::default().bg(COMMAND_PANEL_BG))
                    .block(
                        Block::default()
                            .borders(Borders::ALL)
                            .border_style(Style::default().fg(Color::LightGreen))
                            .title(" Plan "),
                    ),
                    tooltip,
                );
            }
            if self.context_status_hovered
                && !context_tooltip.is_empty()
                && next_context_status_area.is_some()
            {
                let tooltip_width = (context_tooltip.width() as u16)
                    .saturating_add(4)
                    .clamp(24, 72);
                let tooltip_lines = wrap_display(
                    &context_tooltip,
                    tooltip_width.saturating_sub(4).max(1) as usize,
                );
                let tooltip_height = (tooltip_lines.len() as u16)
                    .saturating_add(2)
                    .min(status_area.y.saturating_sub(area.y).max(1));
                let tooltip = Rect {
                    x: next_context_status_area
                        .map(|context_area| context_area.x)
                        .unwrap_or(status_area.x)
                        .min(area.right().saturating_sub(tooltip_width)),
                    y: status_area.y.saturating_sub(tooltip_height),
                    width: tooltip_width,
                    height: tooltip_height,
                };
                frame.render_widget(Clear, tooltip);
                frame.render_widget(
                    Paragraph::new(
                        tooltip_lines
                            .into_iter()
                            .map(Line::from)
                            .collect::<Vec<_>>(),
                    )
                    .style(Style::default().fg(Color::White).bg(COMMAND_PANEL_BG))
                    .block(
                        Block::default()
                            .borders(Borders::ALL)
                            .border_style(Style::default().fg(if context_imminent {
                                Color::Yellow
                            } else {
                                Color::Gray
                            }))
                            .title(" Context window "),
                    ),
                    tooltip,
                );
            }
            if !is_launch_screen {
                let footer_metadata = Some(footer_metadata_text(
                    todo_status.as_deref().unwrap_or(""),
                    &cwd_status,
                    usize::MAX,
                ))
                .filter(|value| !value.is_empty());
                let desired_metadata_width = footer_metadata
                    .as_ref()
                    .map(|value| value.width() as u16)
                    .unwrap_or(0);
                let metadata_width = footer_metadata
                    .as_ref()
                    .map(|_| desired_metadata_width.min(footer_area.width))
                    .unwrap_or(0);
                let controls_area = Rect {
                    width: footer_area.width.saturating_sub(metadata_width),
                    ..footer_area
                };
                frame.render_widget(
                    Paragraph::new(controls)
                        .style(Style::default().fg(Color::DarkGray).bg(COMMAND_PANEL_BG)),
                    controls_area,
                );
                if footer_metadata.is_some() && metadata_width > 0 {
                    let metadata_line = if let Some(todo_status) = todo_status.as_deref() {
                        footer_todo_metadata_line(
                            todo_status,
                            &cwd_status,
                            self.todo_status_hovered,
                            metadata_width as usize,
                        )
                    } else {
                        footer_metadata_line("", &cwd_status, false, metadata_width as usize)
                    };
                    frame.render_widget(
                        Paragraph::new(metadata_line)
                            .alignment(Alignment::Right)
                            .style(Style::default().bg(COMMAND_PANEL_BG)),
                        Rect {
                            x: footer_area.right().saturating_sub(metadata_width),
                            width: metadata_width,
                            ..footer_area
                        },
                    );
                }
            }
            if showing_primary_controls && !showing_transcript_interaction_hint {
                let (controls_x, controls_y, controls_width) = if is_launch_screen {
                    let width = primary_controls_display.width() as u16;
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
                        primary_controls_display
                            .width()
                            .min(footer_area.width as usize) as u16,
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
                let tooltip_width = area.width.min(90);
                let tooltip_lines = keybinding_lines(
                    &self.keymap,
                    tooltip_width.saturating_sub(4).max(1) as usize,
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
                        .style(Style::default().fg(Color::White).bg(COMMAND_PANEL_BG))
                        .block(
                            Block::default()
                                .borders(Borders::ALL)
                                .padding(Padding::horizontal(1))
                                .border_style(Style::default().fg(BORG_ORANGE))
                                .title(Span::styled(
                                    format!(
                                        " Keybindings  ·  close {} or {} ",
                                        self.keymap.label(KeyAction::Keybindings),
                                        self.keymap.label(KeyAction::Interrupt)
                                    ),
                                    Style::default()
                                        .fg(BORG_ORANGE_HOVER)
                                        .add_modifier(Modifier::BOLD),
                                )),
                        ),
                    tooltip,
                );
            }
            // Render the command palette with the other overlays, after the
            // status line and footer, so background chrome cannot cover it.
            if let Some(picker) = self
                .picker
                .as_ref()
                .filter(|picker| matches!(picker.kind, PickerKind::Commands))
            {
                let popup = centered_popup(
                    frame.area(),
                    frame.area().width.saturating_sub(4),
                    frame.area().height.saturating_sub(4),
                );
                let lines = picker.styled_lines(
                    popup.width.saturating_sub(2).max(1) as usize,
                    self.transcript.assistant_label_color,
                    self.transcript.assistant_message_color,
                );
                let content_height = popup.height.saturating_sub(2) as usize;
                let scroll = picker.scroll_offset(content_height, lines.len());
                frame.render_widget(Clear, popup);
                frame.render_widget(
                    Paragraph::new(lines)
                        .style(Style::default().bg(COMMAND_PANEL_BG))
                        .scroll((scroll as u16, 0))
                        .block(
                            Block::default()
                                .borders(Borders::ALL)
                                .border_style(Style::default().fg(BORG_ORANGE))
                                .title(Span::styled(
                                    " Command palette ",
                                    Style::default()
                                        .fg(Color::White)
                                        .add_modifier(Modifier::BOLD),
                                )),
                        ),
                    popup,
                );
                for (index, line) in picker.option_row_offsets() {
                    let Some(line) = line.checked_sub(scroll) else {
                        continue;
                    };
                    let row = Rect {
                        x: popup.x.saturating_add(1),
                        y: popup.y.saturating_add(1 + line as u16),
                        width: popup.width.saturating_sub(2),
                        height: 1,
                    };
                    if row.y < popup.bottom().saturating_sub(1) {
                        next_picker_hit_areas.push((row, index));
                    }
                }
            }
            if let Some(picker) = self.picker.as_ref().filter(|picker| {
                matches!(picker.kind, PickerKind::MessageActions | PickerKind::Goal)
            }) {
                let popup = centered_popup(
                    frame.area(),
                    52,
                    (picker.options.len() as u16).saturating_add(3).max(6),
                );
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
                            if matches!(picker.kind, PickerKind::Goal) {
                                "›"
                            } else if option.label.starts_with("Revert") {
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
            // Copy feedback is the topmost transient overlay. Tooltips can
            // extend into the footer, so rendering the badge only with the
            // normal footer content lets a later tooltip overwrite it.
            if let Some(notice) = copy_notice_text.as_deref()
                && !footer_area.is_empty()
            {
                let metadata_width = Some(footer_metadata_text(
                    todo_status.as_deref().unwrap_or(""),
                    &cwd_status,
                    usize::MAX,
                ))
                .filter(|value| !value.is_empty())
                .map(|value| (value.width() as u16).min(footer_area.width))
                .unwrap_or(0);
                let copy_area = Rect {
                    width: footer_area.width.saturating_sub(metadata_width),
                    ..footer_area
                };
                frame.render_widget(Clear, copy_area);
                frame.render_widget(
                    Paragraph::new(copy_notice_line(notice.to_string()))
                        .style(Style::default().bg(COMMAND_PANEL_BG)),
                    copy_area,
                );
            }
        })?;
        if background_hover_suppressed {
            next_scrollbar_area = None;
            next_scrollbar_thumb_area = None;
            next_tool_hit_areas.clear();
            next_tool_run_hit_areas.clear();
            next_tool_run_header_hit_areas.clear();
            next_message_hit_areas.clear();
            next_link_hit_areas.clear();
            next_entry_hit_areas.clear();
            next_jump_to_bottom_area = None;
            next_status_area = None;
            next_goal_status_area = None;
            next_todo_status_area = None;
            next_agents_status_area = None;
            next_model_status_area = None;
            next_effort_status_area = None;
            next_context_status_area = None;
            next_fast_status_area = None;
            next_permission_status_area = None;
            next_back_to_director_area = None;
            next_keybindings_hint_area = None;
            next_dictation_button_area = None;
        }
        if picker_open || self.keybindings_open {
            next_team_roster_hit_areas.clear();
        }
        self.scrollbar_area = next_scrollbar_area;
        self.scrollbar_thumb_area = next_scrollbar_thumb_area;
        self.transcript_viewport_area = next_transcript_viewport_area;
        self.composer_area = next_composer_area;
        self.composer_text_area = next_composer_text_area;
        self.composer_text_width = composer_text_width;
        self.composer_scroll = composer_scroll;
        self.transcript_scroll_max = next_scroll_max;
        self.tool_hit_areas = next_tool_hit_areas;
        self.tool_run_hit_areas = next_tool_run_hit_areas;
        self.tool_run_header_hit_areas = next_tool_run_header_hit_areas;
        self.message_hit_areas = next_message_hit_areas;
        self.link_hit_areas = next_link_hit_areas;
        self.entry_hit_areas = next_entry_hit_areas;
        self.picker_hit_areas = next_picker_hit_areas;
        self.jump_to_bottom_area = next_jump_to_bottom_area;
        self.status_area = next_status_area;
        self.goal_status_area = next_goal_status_area;
        self.todo_status_area = next_todo_status_area;
        self.agents_status_area = next_agents_status_area;
        self.model_status_area = next_model_status_area;
        self.effort_status_area = next_effort_status_area;
        self.context_status_area = next_context_status_area;
        self.fast_status_area = next_fast_status_area;
        self.permission_status_area = next_permission_status_area;
        self.team_roster_hit_areas = next_team_roster_hit_areas;
        self.back_to_director_area = next_back_to_director_area;
        self.keybindings_hint_area = next_keybindings_hint_area;
        self.dictation_button_area = next_dictation_button_area;
        self.scroll_from_bottom = restored_scroll_from_bottom
            .unwrap_or(self.scroll_from_bottom)
            .min(next_scroll_max);
        if self.scroll_from_bottom == 0 {
            self.transcript.follow_tail = true;
        }
        Ok(())
    }

    fn handle_key(&mut self, key: KeyEvent) -> Result<UiAction> {
        if matches!(
            self.picker.as_ref().map(|picker| picker.kind),
            Some(PickerKind::Commands)
        ) {
            // Typing filters, so this runs ahead of every other key path: the
            // palette owns the keyboard while it is open.
            let picker = self.picker.as_mut().expect("checked above");
            let edit_query = |picker: &mut Picker, edit: &dyn Fn(&mut String)| {
                let mut query = picker.query.clone().unwrap_or_default();
                edit(&mut query);
                picker.set_query(query);
            };
            return match key.code {
                KeyCode::Up => {
                    picker.previous();
                    Ok(UiAction::None)
                }
                KeyCode::Down | KeyCode::Tab => {
                    picker.next();
                    Ok(UiAction::None)
                }
                KeyCode::Enter => self.run_selected_picker(),
                KeyCode::Esc => {
                    self.picker = None;
                    self.pending_auth_model = None;
                    Ok(UiAction::None)
                }
                KeyCode::Backspace => {
                    edit_query(picker, &|query| {
                        query.pop();
                    });
                    Ok(UiAction::None)
                }
                KeyCode::Char(character)
                    if !key
                        .modifiers
                        .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
                {
                    edit_query(picker, &|query| query.push(character));
                    Ok(UiAction::None)
                }
                _ => Ok(UiAction::None),
            };
        }
        if matches!(
            self.picker.as_ref().map(|picker| picker.kind),
            Some(PickerKind::MessageActions | PickerKind::Goal)
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
                KeyCode::Enter => {
                    if matches!(
                        self.picker.as_ref().map(|picker| picker.kind),
                        Some(PickerKind::Goal)
                    ) {
                        self.run_selected_picker()?
                    } else {
                        self.run_selected_message_action()
                    }
                }
                _ => UiAction::None,
            });
        }
        if matches!(
            self.picker.as_ref().map(|picker| picker.kind),
            Some(PickerKind::Resume)
        ) {
            if is_composer_newline(&self.keymap, &key) {
                self.composer_selection = None;
                self.composer.insert("\n");
                return Ok(UiAction::None);
            }
            let picker = self.picker.as_mut().expect("checked above");
            let edit_query = |picker: &mut Picker, edit: &dyn Fn(&mut String)| {
                let mut query = picker.query.clone().unwrap_or_default();
                edit(&mut query);
                picker.set_query(query);
            };
            return match key.code {
                KeyCode::Up | KeyCode::Left => {
                    picker.previous();
                    Ok(UiAction::None)
                }
                KeyCode::Down | KeyCode::Right | KeyCode::Tab => {
                    picker.next();
                    Ok(UiAction::None)
                }
                KeyCode::PageUp => {
                    picker.page(-12);
                    Ok(UiAction::None)
                }
                KeyCode::PageDown => {
                    picker.page(12);
                    Ok(UiAction::None)
                }
                KeyCode::Home => {
                    if let Some(index) = picker.matches().first().copied() {
                        picker.selected = index;
                    }
                    Ok(UiAction::None)
                }
                KeyCode::End => {
                    if let Some(index) = picker.matches().last().copied() {
                        picker.selected = index;
                    }
                    Ok(UiAction::None)
                }
                KeyCode::Backspace => {
                    edit_query(picker, &|query| {
                        query.pop();
                    });
                    Ok(UiAction::None)
                }
                KeyCode::Char(character)
                    if !key
                        .modifiers
                        .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
                {
                    edit_query(picker, &|query| query.push(character));
                    Ok(UiAction::None)
                }
                KeyCode::Enter if picker.selected_position().is_some() => {
                    self.run_selected_picker()
                }
                KeyCode::Enter => Ok(UiAction::None),
                KeyCode::Esc => {
                    self.picker = None;
                    Ok(UiAction::None)
                }
                _ => Ok(UiAction::None),
            };
        }
        // Composer editing shortcuts take precedence over Enter-driven picker
        // confirmation. Otherwise opening any picker turns Shift+Enter into a
        // selection action instead of the configured newline action.
        if self.picker.is_none()
            && self.composer.text.is_empty()
            && matches!(key.code, KeyCode::Tab | KeyCode::Char('\t'))
        {
            self.open_command_palette();
            return Ok(UiAction::None);
        }
        if is_composer_newline(&self.keymap, &key) {
            self.composer_selection = None;
            self.composer.insert("\n");
            return Ok(UiAction::None);
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
            self.open_command_palette();
            return Ok(UiAction::None);
        }
        if self.keybindings_open && self.keymap.matches(KeyAction::Interrupt, &key) {
            self.keybindings_open = false;
            return Ok(UiAction::None);
        }
        if let Some(target) = focused_child_interrupt_target(&self.keymap, &key, self.focused_child)
        {
            self.record_user_interrupt();
            return Ok(UiAction::Interrupt {
                target: Some(target),
            });
        }
        if self.keymap.matches(KeyAction::Interrupt, &key)
            && self.composer.text.is_empty()
            && !self.pending_approval
            && !matches!(
                self.status,
                SessionStatus::Starting
                    | SessionStatus::Running
                    | SessionStatus::WaitingForApproval
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
                return Ok(UiAction::ForceQuit);
            }
            self.composer.clear();
            self.composer_selection = None;
            self.notice = Some(format!(
                "Prompt cleared · press {} again to exit",
                self.keymap.label(KeyAction::ClearOrExit)
            ));
            return Ok(UiAction::None);
        }
        self.last_ctrl_c = None;
        self.composer_selection = None;
        if self.pending_approval {
            return Ok(if self.keymap.matches(KeyAction::Approve, &key) {
                UiAction::Approve {
                    target: self.focused_child,
                    decision: ApprovalDecision::AllowOnce,
                }
            } else if self.keymap.matches(KeyAction::Deny, &key) {
                UiAction::Approve {
                    target: self.focused_child,
                    decision: ApprovalDecision::Deny,
                }
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
        if self.keymap.matches(KeyAction::Dictate, &key) {
            return Ok(UiAction::ToggleDictation);
        }
        if self.keymap.matches(KeyAction::Copy, &key) {
            if self.copy_text_selection() {
                return Ok(UiAction::None);
            }
            if let Some(text) = self.transcript.copy_text() {
                match clipboard::copy(&text) {
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
                    self.active_status(),
                    SessionStatus::Starting
                        | SessionStatus::Running
                        | SessionStatus::WaitingForApproval
                ) {
                    let message_id = Uuid::new_v4();
                    push_queued_prompt(
                        self.active_queued_prompts_mut(),
                        message_id,
                        text.clone(),
                        PromptDelivery::Queue,
                    );
                    UiAction::Queue {
                        target: self.focused_child,
                        message_id,
                        text,
                        attachments,
                    }
                } else {
                    UiAction::Submit {
                        target: self.focused_child,
                        text,
                        attachments,
                    }
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
            if self.composer.attachments.is_empty() && self.composer.text.trim() == "/copy" {
                self.composer.clear();
                self.notice = None;
                self.copy_last_assistant_message();
                return Ok(UiAction::None);
            }
            if self.composer.attachments.is_empty() && self.composer.text.trim() == "/dictate" {
                self.composer.clear();
                self.notice = None;
                return Ok(UiAction::ToggleDictation);
            }
            let (text, attachments) = self.composer.take();
            if text.trim().is_empty() && attachments.is_empty() {
                return Ok(UiAction::None);
            }
            self.notice = None;
            return Ok(UiAction::Submit {
                target: self.focused_child,
                text,
                attachments,
            });
        }
        if self.keymap.matches(KeyAction::ScrollUp, &key) {
            self.history_page_requested = true;
            self.transcript.follow_tail = false;
            self.scroll_from_bottom = self.scroll_from_bottom.saturating_add(8);
            return Ok(UiAction::None);
        }
        if self.keymap.matches(KeyAction::ScrollDown, &key) {
            self.history_page_requested = false;
            self.scroll_from_bottom = self.scroll_from_bottom.saturating_sub(8);
            self.transcript.follow_tail = self.scroll_from_bottom == 0;
            return Ok(UiAction::None);
        }
        if self.keymap.matches(KeyAction::Interrupt, &key) {
            self.record_user_interrupt();
            return Ok(UiAction::Interrupt {
                target: self.focused_child,
            });
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
                if has_recallable_queued_prompts(&self.composer.text, self.active_queued_prompts())
                {
                    return Ok(UiAction::RecallQueuedPrompts {
                        target: self.focused_child,
                    });
                }
                if has_pending_steer_prompts(&self.composer.text, self.active_queued_prompts()) {
                    // Only the session knows whether the provider has committed
                    // the steer. Transport acceptance is still recallable, so
                    // ask the session to reconcile it at the provider boundary.
                    return Ok(UiAction::RecallQueuedPrompts {
                        target: self.focused_child,
                    });
                }
                let width = terminal_content_width(self.terminal.size()?.width).max(1) as usize;
                if self.composer.should_recall_history_on_up(width) {
                    self.composer.history_previous();
                } else {
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
                if matches.is_empty() && self.composer.text.is_empty() {
                    self.open_command_palette();
                    return Ok(UiAction::None);
                }
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

fn active_goal_for_view<'a>(
    focused_child: Option<Uuid>,
    director_transcript: Option<&'a Transcript>,
    displayed_transcript: &'a Transcript,
) -> Option<&'a SessionGoal> {
    if focused_child.is_some() {
        displayed_transcript.goal.as_ref()
    } else {
        director_transcript
            .and_then(|transcript| transcript.goal.as_ref())
            .or(displayed_transcript.goal.as_ref())
    }
}

fn entry_action_runs_directly(entry: &TranscriptEntry, option_count: usize) -> bool {
    option_count == 1 && !matches!(entry, TranscriptEntry::Compaction { complete: true, .. })
}

fn is_composer_newline(keymap: &KeyMap, key: &KeyEvent) -> bool {
    // Some terminals add protocol metadata bits to modified Enter events;
    // preserve the built-in multiline shortcut when those bits are present.
    keymap.matches(KeyAction::Newline, key)
        || (key.code == KeyCode::Enter
            && !key.modifiers.contains(KeyModifiers::CONTROL)
            && key
                .modifiers
                .intersects(KeyModifiers::SHIFT | KeyModifiers::ALT))
        // A terminal key binding can emit a literal LF for Shift+Enter. In
        // raw mode crossterm decodes that byte as Ctrl+J, so accept both
        // representations as the composer's newline action.
        || matches!(key.code, KeyCode::Char('\n'))
        || (matches!(key.code, KeyCode::Char('j' | 'J'))
            && key.modifiers == KeyModifiers::CONTROL)
}

/// Discard bytes that belong to an input sequence which was in flight while
/// the event reader stopped. Without this, the shell can receive the tail of
/// a Kitty CSI-u sequence after Borg gives the terminal back.
pub(crate) fn discard_pending_terminal_input() {
    #[cfg(unix)]
    {
        let stdin = io::stdin();
        // The TUI owns the terminal while this is called, so losing a key that
        // arrived during teardown is preferable to handing protocol bytes to
        // the next line editor.
        let _ = unsafe { libc::tcflush(stdin.as_raw_fd(), libc::TCIFLUSH) };
    }
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
        discard_pending_terminal_input();
        // Inline viewports share the shell's scrollback. Clear the viewport
        // before restoring the terminal so the last rendered agent summary
        // and composer do not remain above the copyable resume handoff.
        if self.mode == ScreenMode::Inline {
            let _ = self.terminal.clear();
        }
        let _ = execute!(self.terminal.backend_mut(), SetTitle("Borg Agent"));
        let _ = execute!(self.terminal.backend_mut(), DisableMouseCapture);
        let _ = execute!(self.terminal.backend_mut(), DisableBracketedPaste);
        if self.mode == ScreenMode::Alternate {
            let _ = execute!(self.terminal.backend_mut(), LeaveAlternateScreen);
        }
        let _ = execute!(
            self.terminal.backend_mut(),
            SetCursorStyle::DefaultUserShape
        );
        let _ = disable_raw_mode();
        let _ = self.terminal.show_cursor();
        // A drop-path teardown cannot await the event pump. Flush once more
        // after the terminal modes are restored to catch bytes read during the
        // synchronous cleanup above.
        discard_pending_terminal_input();
    }
}

fn fresh_transcript_like(previous: &Transcript) -> Transcript {
    Transcript {
        auto_expand_edits: previous.auto_expand_edits,
        auto_expand_tools: previous.auto_expand_tools,
        user_label: previous.user_label.clone(),
        assistant_label: previous.assistant_label.clone(),
        user_label_color: previous.user_label_color,
        user_message_color: previous.user_message_color,
        assistant_label_color: previous.assistant_label_color,
        assistant_message_color: previous.assistant_message_color,
        ..Transcript::default()
    }
}

fn transcript_history_in_display_order(events: &[SessionEvent]) -> Vec<SessionEvent> {
    let mut ordered = Vec::with_capacity(events.len());
    let mut turn = Vec::new();

    for event in events {
        // Agent lifecycle has its own roster and child-transcript recovery.
        // Replaying it into the root transcript makes old cards appear only
        // after reconnect even though they were not part of the live view.
        if matches!(event.kind, SessionEventKind::SubagentActivity { .. }) {
            continue;
        }
        let is_terminal_user_message = matches!(
            event.kind,
            SessionEventKind::Message {
                actor: EventActor::User,
                status: MessageStatus::Complete | MessageStatus::Failed,
                ..
            }
        );
        if !is_terminal_user_message {
            turn.push(event.clone());
            continue;
        }

        let message_id = match &event.kind {
            SessionEventKind::Message { message_id, .. } => *message_id,
            _ => unreachable!("terminal user message match must be a message event"),
        };
        let has_lifecycle_start = turn.iter().any(|candidate| {
            matches!(
                candidate.kind,
                SessionEventKind::Message {
                    message_id: candidate_id,
                    actor: EventActor::User,
                    status: MessageStatus::InProgress,
                    ..
                } if candidate_id == message_id
            )
        });

        if has_lifecycle_start {
            ordered.append(&mut turn);
            ordered.push(event.clone());
            continue;
        }

        // Fork projections deliberately omit in-progress user messages so a
        // discarded prompt cannot be recovered and run again. The surviving
        // terminal event is still durable, but it was appended after the
        // assistant/tool output. Put that orphaned user boundary before the
        // visible turn output so a partial projection reads like the original
        // conversation instead of looking reversed.
        if turn.iter().any(transcript_turn_has_terminal_boundary) {
            // A terminal boundary means this prompt was submitted after the
            // previous turn ended. Keep it at the tail even when its own
            // lifecycle start was not included in the projection.
            ordered.append(&mut turn);
            ordered.push(event.clone());
        } else if let Some(output_start) = turn.iter().position(transcript_turn_output) {
            ordered.extend(turn.drain(..output_start));
            ordered.push(event.clone());
            ordered.append(&mut turn);
        } else {
            ordered.append(&mut turn);
            ordered.push(event.clone());
        }
    }

    ordered.extend(turn);
    ordered
}

fn transcript_turn_output(event: &SessionEvent) -> bool {
    matches!(
        event.kind,
        SessionEventKind::Message {
            actor: EventActor::Assistant,
            ..
        } | SessionEventKind::ReasoningDelta { .. }
            | SessionEventKind::ToolStarted { .. }
            | SessionEventKind::ToolCompleted { .. }
    )
}

fn transcript_turn_has_terminal_boundary(event: &SessionEvent) -> bool {
    matches!(
        event.kind,
        SessionEventKind::TurnCompleted { .. }
            | SessionEventKind::StatusChanged {
                status: SessionStatus::Ready
                    | SessionStatus::Completed
                    | SessionStatus::Failed
                    | SessionStatus::Stopped,
                ..
            }
    )
}

fn rewind_targets_from_history(events: &[SessionEvent]) -> Vec<RewindTarget> {
    transcript_history_in_display_order(events)
        .into_iter()
        .filter_map(|event| match event.kind {
            SessionEventKind::Message {
                actor: EventActor::User,
                text,
                attachments,
                status: MessageStatus::Complete,
                ..
            } => Some(RewindTarget {
                sequence: event.sequence,
                text,
                attachments,
            }),
            _ => None,
        })
        .collect()
}

fn rewind_target_for_output(
    preceding_user_messages: usize,
    targets: &[RewindTarget],
) -> Option<RewindTarget> {
    targets
        .get(preceding_user_messages.saturating_sub(1))
        .cloned()
}

fn replace_root_transcript_history(
    transcript: &mut Transcript,
    director_transcript: &mut Option<Box<Transcript>>,
    child_is_focused: bool,
    events: &[SessionEvent],
) -> bool {
    let previous = if child_is_focused {
        director_transcript.as_deref().unwrap_or(&*transcript)
    } else {
        &*transcript
    };
    let reconciled_subagents = previous
        .subagent_snapshots
        .values()
        .cloned()
        .collect::<Vec<_>>();
    let display_events = transcript_history_in_display_order(events);
    let mut replacement = fresh_transcript_like(previous);
    replacement.reserve_history(display_events.len());
    for event in &display_events {
        replacement.apply_history(event);
    }
    // Older-page hydration rebuilds the root transcript. It may contain the
    // parent's last pre-crash Running mirror even though child hydration has
    // already reconciled that child to Ready/Stopped. Historical rows may
    // expand the transcript, but may never regress a newer roster snapshot.
    for agent in reconciled_subagents {
        let should_preserve = replacement
            .subagent_snapshots
            .get(&agent.session_id)
            .is_none_or(|historical| agent.updated_at >= historical.updated_at);
        if should_preserve {
            replacement.upsert_subagent_snapshot(&agent);
        }
    }
    if child_is_focused {
        *director_transcript = Some(Box::new(replacement));
        false
    } else {
        *transcript = replacement;
        true
    }
}

fn merge_child_history(
    authoritative: &[SessionEvent],
    buffered: Vec<SessionEvent>,
) -> Vec<SessionEvent> {
    let mut seen = HashSet::new();
    let mut events = authoritative
        .iter()
        .cloned()
        .chain(buffered)
        .filter(|event| seen.insert(event.id))
        .collect::<Vec<_>>();

    // A completed message supersedes all transport snapshots of the same
    // message. This remains true even if a delayed coalesced snapshot carries
    // a later timestamp than the durable terminal event.
    let completed_messages = events
        .iter()
        .filter_map(|event| match &event.kind {
            SessionEventKind::Message {
                message_id,
                status: MessageStatus::Complete | MessageStatus::Failed,
                ..
            } => Some(*message_id),
            _ => None,
        })
        .collect::<HashSet<_>>();
    events.retain(|event| {
        !matches!(
            &event.kind,
            SessionEventKind::Message {
                message_id,
                status: MessageStatus::InProgress
                    | MessageStatus::Queued,
                ..
            } if completed_messages.contains(message_id)
        )
    });
    events.sort_by(|left, right| {
        left.created_at
            .cmp(&right.created_at)
            .then_with(|| left.sequence.cmp(&right.sequence))
            .then_with(|| left.id.cmp(&right.id))
    });
    events
}

fn switch_to_child_transcript(
    transcript: &mut Transcript,
    director_transcript: &mut Option<Box<Transcript>>,
    child_transcripts: &mut HashMap<Uuid, Transcript>,
    child_id: Uuid,
) {
    let child = child_transcripts.remove(&child_id).unwrap_or_else(|| {
        let mut transcript = Transcript::default();
        transcript.show_director_context_boundary();
        transcript
    });
    *director_transcript = Some(Box::new(std::mem::replace(transcript, child)));
}

fn switch_between_child_transcripts(
    transcript: &mut Transcript,
    child_transcripts: &mut HashMap<Uuid, Transcript>,
    previous_child: Uuid,
    next_child: Uuid,
) {
    let next = child_transcripts.remove(&next_child).unwrap_or_else(|| {
        let mut transcript = Transcript::default();
        transcript.show_director_context_boundary();
        transcript
    });
    child_transcripts.insert(previous_child, std::mem::replace(transcript, next));
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

fn subagent_session_status(status: SubagentStatus) -> SessionStatus {
    match status {
        SubagentStatus::Starting => SessionStatus::Starting,
        SubagentStatus::Running => SessionStatus::Running,
        SubagentStatus::Ready => SessionStatus::Ready,
        SubagentStatus::WaitingForApproval => SessionStatus::WaitingForApproval,
        SubagentStatus::Stopped => SessionStatus::Stopped,
        SubagentStatus::Failed => SessionStatus::Failed,
    }
}

fn effective_subagent_status(
    activity: SubagentActivityKind,
    snapshot_status: SubagentStatus,
    child_event: Option<&SessionEvent>,
) -> SubagentStatus {
    match activity {
        SubagentActivityKind::Completed => SubagentStatus::Ready,
        SubagentActivityKind::Stopped => SubagentStatus::Stopped,
        SubagentActivityKind::Failed => SubagentStatus::Failed,
        SubagentActivityKind::Updated => child_event
            .and_then(|event| subagent_status_from_child_event(&event.kind))
            .unwrap_or(snapshot_status),
        SubagentActivityKind::Started => snapshot_status,
    }
}

fn subagent_status_from_child_event(kind: &SessionEventKind) -> Option<SubagentStatus> {
    let SessionEventKind::StatusChanged { status, .. } = kind else {
        return None;
    };
    Some(match status {
        SessionStatus::Starting => SubagentStatus::Starting,
        SessionStatus::Running => SubagentStatus::Running,
        SessionStatus::Ready | SessionStatus::Completed => SubagentStatus::Ready,
        SessionStatus::WaitingForApproval => SubagentStatus::WaitingForApproval,
        SessionStatus::Stopped => SubagentStatus::Stopped,
        SessionStatus::Failed => SubagentStatus::Failed,
    })
}

fn focused_child_interrupt_target(
    keymap: &KeyMap,
    key: &KeyEvent,
    focused_child: Option<Uuid>,
) -> Option<Uuid> {
    keymap
        .matches(KeyAction::Interrupt, key)
        .then_some(focused_child)
        .flatten()
}

fn track_child_activity(
    active_since: &mut HashMap<Uuid, DateTime<Utc>>,
    child_id: Uuid,
    status: SessionStatus,
    observed_at: DateTime<Utc>,
) {
    if matches!(status, SessionStatus::Starting | SessionStatus::Running) {
        active_since.entry(child_id).or_insert(observed_at);
    } else {
        active_since.remove(&child_id);
    }
}

fn display_agent_name(task_name: &str) -> String {
    match task_name.strip_prefix("/root/").unwrap_or(task_name) {
        "claude" => "Claude".to_string(),
        "gpt" => "GPT".to_string(),
        name => name.to_string(),
    }
}

fn team_roster_row_style(focused: bool, hovered: bool, is_subagent: bool) -> Style {
    if hovered {
        Style::default()
            .fg(Color::Black)
            .bg(SUBAGENT_PINK)
            .add_modifier(Modifier::BOLD)
    } else if focused {
        Style::default()
            .fg(SUBAGENT_PINK)
            .bg(COMMAND_PANEL_BG)
            .add_modifier(Modifier::BOLD)
    } else if is_subagent {
        Style::default().fg(SUBAGENT_PINK).bg(COMMAND_PANEL_BG)
    } else {
        Style::default().fg(Color::White).bg(COMMAND_PANEL_BG)
    }
}

fn team_roster_target_at(
    hit_areas: &[(Rect, Option<Uuid>)],
    pointer: Position,
) -> Option<(usize, Option<Uuid>)> {
    hit_areas
        .iter()
        .enumerate()
        .find_map(|(index, (area, child_id))| area.contains(pointer).then_some((index, *child_id)))
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
    history_message_ids: HashSet<Uuid>,
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
            if seen.insert(*message_id) && self.history_message_ids.insert(*message_id) {
                if !attachments.is_empty() {
                    if let Some(highest) = image_numbers_in_text(text).into_iter().max() {
                        self.next_image_number = self.next_image_number.max(highest);
                    } else {
                        self.next_image_number =
                            self.next_image_number.saturating_add(attachments.len());
                    }
                }
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

    fn draft(&self) -> Option<(String, Vec<PathBuf>)> {
        let text = self.expanded_text();
        let attachments = self
            .attachments
            .iter()
            .map(|attachment| attachment.path.clone())
            .collect::<Vec<_>>();
        (!text.is_empty() || !attachments.is_empty()).then_some((text, attachments))
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
                if let Some(number) = label
                    .strip_prefix("Image ")
                    .and_then(|number| number.parse::<usize>().ok())
                {
                    self.next_image_number = self.next_image_number.max(number);
                }
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

    fn should_recall_history_on_up(&self, width: usize) -> bool {
        !self.history.is_empty()
            && (self.history_index.is_some()
                || self.text.is_empty()
                || composer_cursor_position(&self.text, self.cursor, width).0 == 0)
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

include!("terminal_ui/transcript.rs");

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
    let displayed_tokens = usage
        .context_tokens
        .filter(|tokens| *tokens > 0)
        .map(|tokens| (tokens, "ctx"))
        .or_else(|| (usage.total_tokens > 0).then_some((usage.total_tokens, "tok")));
    if displayed_tokens.is_none() && usage.cost_microusd.is_none() {
        return "  usage —".to_string();
    }
    let mut parts = Vec::new();
    if let Some((tokens, unit)) = displayed_tokens {
        parts.push(format_compact_count(tokens, unit));
    }
    if let Some(cost_microusd) = usage.cost_microusd {
        let cost = cost_microusd as f64 / 1_000_000.0;
        let label = match usage.cost_basis.as_str() {
            "subscription_equivalent" => format!("${cost:.4} (sub)"),
            "estimated_from_pricing" => format!("~${cost:.4} (est)"),
            _ => format!("${cost:.4}"),
        };
        parts.push(label);
    }
    format!("  {}", parts.join(" · "))
}

fn format_compact_count(value: u64, unit: &str) -> String {
    if value >= 1_000_000 {
        format!("{:.1}m {unit}", value as f64 / 1_000_000.0)
    } else if value >= 1_000 {
        format!("{:.1}k {unit}", value as f64 / 1_000.0)
    } else {
        format!("{value} {unit}")
    }
}

fn borg_lsp_diagnostics_view(
    name: &str,
    input: Option<&serde_json::Value>,
    output: &str,
) -> Option<String> {
    let normalized_name = name.to_ascii_lowercase();
    let workspace = normalized_name.ends_with("lsp_workspace_diagnostics");
    if workspace {
        return borg_lsp_workspace_diagnostics_view(output);
    }
    if !normalized_name.ends_with("lsp_diagnostics") {
        return None;
    }
    let value = serde_json::from_str::<serde_json::Value>(output).ok()?;
    let items = value
        .get("items")
        .and_then(serde_json::Value::as_array)
        .or_else(|| value.as_array())?;
    let path = input
        .and_then(|input| json_text(input, &["path"]))
        .unwrap_or("workspace");
    let mut rows = vec![format!(
        "DIAGNOSTICS · {path} · {} issue{}",
        items.len(),
        if items.len() == 1 { "" } else { "s" }
    )];
    for item in items.iter().take(8) {
        let severity = item
            .get("severity")
            .and_then(serde_json::Value::as_u64)
            .map(|value| match value {
                1 => "error",
                2 => "warning",
                3 => "info",
                4 => "hint",
                _ => "issue",
            })
            .unwrap_or("issue");
        let message = json_text(item, &["message"]).unwrap_or("diagnostic");
        let line = item
            .pointer("/range/start/line")
            .and_then(serde_json::Value::as_u64)
            .map(|line| format!(":{}", line + 1))
            .unwrap_or_default();
        rows.push(format!(
            "  {severity:>7}{line}  {}",
            compact_text(message, 120)
        ));
    }
    Some(rows.join("\n"))
}

fn borg_lsp_workspace_diagnostics_view(output: &str) -> Option<String> {
    let workspaces = serde_json::from_str::<serde_json::Value>(output)
        .ok()?
        .as_object()?
        .clone();
    let mut documents = Vec::new();
    for (server, report) in workspaces {
        let items = report.get("items")?.as_array()?;
        for document in items {
            let diagnostics = document
                .get("items")
                .and_then(serde_json::Value::as_array)
                .cloned()
                .unwrap_or_default();
            documents.push((server.clone(), document.clone(), diagnostics));
        }
    }
    let issue_count = documents
        .iter()
        .map(|(_, _, diagnostics)| diagnostics.len())
        .sum::<usize>();
    let mut rows = vec![format!(
        "WORKSPACE DIAGNOSTICS · {} issue{} · {} document{}",
        issue_count,
        if issue_count == 1 { "" } else { "s" },
        documents.len(),
        if documents.len() == 1 { "" } else { "s" }
    )];
    for (server, document, diagnostics) in documents.iter().take(8) {
        let uri = document
            .get("uri")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown document");
        rows.push(format!(
            "  {server} · {} issue{} · {uri}",
            diagnostics.len(),
            if diagnostics.len() == 1 { "" } else { "s" }
        ));
        for diagnostic in diagnostics.iter().take(2) {
            let severity = diagnostic
                .get("severity")
                .and_then(serde_json::Value::as_u64)
                .map(|value| match value {
                    1 => "error",
                    2 => "warning",
                    3 => "info",
                    4 => "hint",
                    _ => "issue",
                })
                .unwrap_or("issue");
            let message = json_text(diagnostic, &["message"]).unwrap_or("diagnostic");
            rows.push(format!("    {severity:>7}  {}", compact_text(message, 120)));
        }
    }
    if documents.len() > 8 {
        rows.push(format!("  … {} more documents", documents.len() - 8));
    }
    Some(rows.join("\n"))
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

    fn advance_immediately(&mut self, scroll_from_bottom: usize, scroll_max: usize) -> usize {
        let requested = std::mem::take(&mut self.remaining_lines);
        if requested > 0 {
            scroll_from_bottom
                .saturating_add(requested as usize)
                .min(scroll_max)
        } else {
            scroll_from_bottom.saturating_sub(requested.unsigned_abs())
        }
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

fn focused_subagent_status_color(status: SessionStatus, focused_subagent: bool) -> Color {
    if focused_subagent
        && !matches!(
            status,
            SessionStatus::WaitingForApproval | SessionStatus::Failed | SessionStatus::Stopped
        )
    {
        SUBAGENT_PINK
    } else {
        session_status_color(status)
    }
}

fn transcript_action_glyph(state: TranscriptActionState) -> &'static str {
    match state {
        TranscriptActionState::Running => "◇",
        TranscriptActionState::Waiting => "?",
        TranscriptActionState::Complete => "✓",
        TranscriptActionState::Stopped => "■",
        TranscriptActionState::Failed => "!",
    }
}

fn transcript_action_color(kind: TranscriptActionKind, state: TranscriptActionState) -> Color {
    if matches!(state, TranscriptActionState::Failed) {
        return Color::LightRed;
    }
    if matches!(state, TranscriptActionState::Waiting) {
        return Color::Yellow;
    }
    match kind {
        TranscriptActionKind::Agent => SUBAGENT_PINK,
        TranscriptActionKind::Approval | TranscriptActionKind::ProviderInteraction => Color::Yellow,
        TranscriptActionKind::Error => Color::LightRed,
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
        | SessionEventKind::ProviderCapabilitiesUpdated { .. }
        | SessionEventKind::EffectiveCapabilitiesUpdated { .. }
        | SessionEventKind::ApprovalResolved { .. }
        | SessionEventKind::ProviderInteractionResolved { .. }
        | SessionEventKind::UsageUpdated { .. }
        | SessionEventKind::ContextWindowUpdated { .. }
        | SessionEventKind::SubagentControl { .. }
        | SessionEventKind::ProviderSessionLinked { .. }
        | SessionEventKind::RuntimeProcessStarted { .. }
        | SessionEventKind::RuntimeProcessOutput { .. }
        | SessionEventKind::RuntimeProcessCompleted { .. }
        | SessionEventKind::BluWorkflowStarted { .. }
        | SessionEventKind::BluWorkflowCallRequested { .. }
        | SessionEventKind::BluWorkflowCallCompleted { .. }
        | SessionEventKind::BluWorkflowCompleted { .. }
        | SessionEventKind::RuntimeWorkflowStarted { .. }
        | SessionEventKind::RuntimeWorkflowCallRequested { .. }
        | SessionEventKind::RuntimeWorkflowCallCompleted { .. }
        | SessionEventKind::RuntimeWorkflowCompleted { .. }
        | SessionEventKind::TurnStarted { .. } => false,
        SessionEventKind::StatusChanged {
            status:
                SessionStatus::Ready
                | SessionStatus::Completed
                | SessionStatus::Failed
                | SessionStatus::Stopped,
            ..
        } => true,
        SessionEventKind::StatusChanged { .. } => false,
        SessionEventKind::ProviderEvent { kind, .. } => {
            is_context_compaction(kind) || is_live_tool_call_event(kind)
        }
        SessionEventKind::SubagentActivity {
            activity,
            agent,
            event,
        } => subagent_activity_summary(*activity, agent, event.as_deref()).is_some(),
        SessionEventKind::Message { .. }
        | SessionEventKind::ReasoningDelta { .. }
        | SessionEventKind::ReasoningCompleted
        | SessionEventKind::ToolStarted { .. }
        | SessionEventKind::ToolCompleted { .. }
        | SessionEventKind::ApprovalRequested { .. }
        | SessionEventKind::ProviderInteractionRequested { .. }
        | SessionEventKind::PlanUpdated { .. }
        | SessionEventKind::GoalUpdated { .. }
        | SessionEventKind::GoalCleared { .. }
        | SessionEventKind::ContextCleared
        | SessionEventKind::PromptRecalled { .. }
        | SessionEventKind::TurnCompleted { .. }
        | SessionEventKind::Error { .. } => true,
    }
}

fn is_live_tool_call_event(kind: &str) -> bool {
    kind == "tool_call_started"
}

fn should_suppress_root_subagent_activity(
    bootstrap_recovery_pending: bool,
    kind: &SessionEventKind,
) -> bool {
    bootstrap_recovery_pending && matches!(kind, SessionEventKind::SubagentActivity { .. })
}

fn context_compaction_started(kind: &str, payload: &serde_json::Value) -> bool {
    payload
        .get("status")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|status| status.eq_ignore_ascii_case("started"))
        || (kind
            .rsplit(['.', ':', '/'])
            .next()
            .unwrap_or(kind)
            .eq_ignore_ascii_case("contextCompaction")
            && kind.to_ascii_lowercase().contains("started"))
}

fn context_compaction_full_summary(payload: &serde_json::Value) -> String {
    [
        "summary",
        "message",
        "detail",
        "/item/summary",
        "/item/message",
        "/item/detail",
        "/params/item/summary",
        "/params/item/message",
        "/params/item/detail",
    ]
    .into_iter()
    .find_map(|field| {
        let value = if field.starts_with('/') {
            payload.pointer(field)
        } else {
            payload.get(field)
        };
        value
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
    })
    .map(str::to_string)
    .unwrap_or_else(|| "Context was compacted.".to_string())
}

fn context_compaction_card_summary(payload: &serde_json::Value) -> String {
    let detail = context_compaction_full_summary(payload);
    if matches!(
        detail.trim().to_ascii_lowercase().as_str(),
        "context was compacted."
            | "context was compacted"
            | "context compacted."
            | "context compacted"
    ) {
        "Context compacted".to_string()
    } else {
        format!("Compacted context: {detail}")
    }
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
            Some(SessionEventKind::Message {
                actor: EventActor::Assistant,
                text,
                status: MessageStatus::Complete,
                ..
            }) if !text.trim().is_empty() => Some(format!("agent · {task} · report ready")),
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
            Some(SessionEventKind::StatusChanged {
                status: SessionStatus::Ready | SessionStatus::Completed,
                ..
            }) => Some(format!("agent · {task} · done · waiting for input")),
            Some(SessionEventKind::StatusChanged {
                status: SessionStatus::WaitingForApproval,
                ..
            }) => Some(format!("agent · {task} · waiting for input")),
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

type SubagentActionProjection = (String, String, Option<String>, TranscriptActionState);

/// Project every subagent lifecycle update into the same typed action shape.
/// The transcript keeps the report as an optional body instead of embedding
/// it in a status string; this makes updates idempotent and gives the renderer
/// one place to decide whether the body is collapsed or expanded.
fn subagent_action_projection(
    activity: SubagentActivityKind,
    agent: &SubagentSnapshot,
    child_event: Option<&SessionEvent>,
) -> Option<SubagentActionProjection> {
    let task = agent.task_name.clone();
    let project = |detail: String, body: Option<String>, state| {
        Some((
            "Agent".to_string(),
            format!("{task} · {detail}"),
            body,
            state,
        ))
    };
    match activity {
        SubagentActivityKind::Started => {
            project("started".to_string(), None, TranscriptActionState::Running)
        }
        SubagentActivityKind::Completed => project(
            "completed".to_string(),
            agent.final_text.clone(),
            TranscriptActionState::Complete,
        ),
        SubagentActivityKind::Stopped => project(
            "stopped".to_string(),
            agent.final_text.clone(),
            TranscriptActionState::Stopped,
        ),
        SubagentActivityKind::Failed => project(
            format!(
                "failed{}",
                agent
                    .detail
                    .as_deref()
                    .filter(|detail| !detail.trim().is_empty())
                    .map(|detail| format!(" · {}", compact_text(detail, 120)))
                    .unwrap_or_default()
            ),
            agent.detail.clone(),
            TranscriptActionState::Failed,
        ),
        SubagentActivityKind::Updated => match child_event.map(|event| &event.kind) {
            Some(SessionEventKind::Message {
                actor: EventActor::Assistant,
                text,
                status: MessageStatus::Complete,
                ..
            }) if !text.trim().is_empty() => project(
                "report ready".to_string(),
                Some(text.clone()),
                TranscriptActionState::Complete,
            ),
            Some(SessionEventKind::ApprovalRequested { title, detail, .. }) => project(
                format!(
                    "needs approval · {}",
                    compact_text(
                        if title.trim().is_empty() {
                            detail
                        } else {
                            title
                        },
                        120
                    )
                ),
                (!detail.trim().is_empty()).then(|| detail.clone()),
                TranscriptActionState::Waiting,
            ),
            Some(SessionEventKind::StatusChanged {
                status: SessionStatus::Ready | SessionStatus::Completed,
                ..
            }) => project(
                "done · waiting for input".to_string(),
                agent.final_text.clone(),
                TranscriptActionState::Complete,
            ),
            Some(SessionEventKind::StatusChanged {
                status: SessionStatus::WaitingForApproval,
                detail,
            }) => project(
                "waiting for input".to_string(),
                detail.clone(),
                TranscriptActionState::Waiting,
            ),
            Some(SessionEventKind::Error { message }) => project(
                format!("error · {}", compact_text(message, 120)),
                Some(message.clone()),
                TranscriptActionState::Failed,
            ),
            _ => match agent.status {
                SubagentStatus::Starting => {
                    project("starting".to_string(), None, TranscriptActionState::Running)
                }
                SubagentStatus::Running => {
                    project("working".to_string(), None, TranscriptActionState::Running)
                }
                SubagentStatus::WaitingForApproval => project(
                    "waiting for approval".to_string(),
                    None,
                    TranscriptActionState::Waiting,
                ),
                SubagentStatus::Ready => Some((
                    "Agent".to_string(),
                    format!("{task} · done · waiting for input"),
                    agent.final_text.clone(),
                    TranscriptActionState::Complete,
                )),
                SubagentStatus::Stopped => project(
                    "stopped".to_string(),
                    agent.final_text.clone(),
                    TranscriptActionState::Stopped,
                ),
                SubagentStatus::Failed => project(
                    "failed".to_string(),
                    agent.detail.clone(),
                    TranscriptActionState::Failed,
                ),
            },
        },
    }
}

fn format_action_detail(label: &str, detail: &str) -> String {
    if label.trim().is_empty() {
        compact_text(detail, 180)
    } else if detail.trim().is_empty() {
        label.to_string()
    } else {
        format!("{label} · {}", compact_text(detail, 180))
    }
}

fn format_action_text(label: &str, detail: &str, body: Option<&str>) -> String {
    let mut text = if detail.is_empty() {
        label.to_string()
    } else {
        format!("{label} · {detail}")
    };
    if let Some(body) = body.filter(|body| !body.trim().is_empty())
        && let Some(first) = body.lines().find(|line| !line.trim().is_empty())
    {
        text.push_str(" · ");
        text.push_str(&compact_text(first, 120));
    }
    text
}

fn longest_suffix_prefix_overlap(left: &str, right: &str) -> usize {
    let maximum = left.len().min(right.len());
    (1..=maximum)
        .rev()
        .find(|overlap| {
            left.is_char_boundary(left.len() - overlap)
                && right.is_char_boundary(*overlap)
                && left.as_bytes()[left.len() - overlap..] == right.as_bytes()[..*overlap]
        })
        .unwrap_or(0)
}

fn terminal_agent_summary(task: &str, outcome: &str, final_text: Option<&str>) -> String {
    let result = final_text
        .and_then(|text| text.lines().find(|line| !line.trim().is_empty()))
        .map(|text| format!(" · {}", compact_text(text, 120)))
        .unwrap_or_default();
    format!("agent · {task} · {outcome}{result}")
}

fn number_message_attachments(
    text: &str,
    attachments: &[PathBuf],
    next_image_number: &mut usize,
) -> Vec<(usize, PathBuf)> {
    let explicit = image_numbers_in_text(text);
    let explicit = &explicit[explicit.len().saturating_sub(attachments.len())..];
    let mut used = explicit.iter().copied().collect::<HashSet<_>>();
    let mut fallback = *next_image_number;
    let numbered = attachments
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, path)| {
            let number = explicit.get(index).copied().unwrap_or_else(|| {
                while used.contains(&fallback) {
                    fallback = fallback.saturating_add(1);
                }
                let number = fallback;
                used.insert(number);
                fallback = fallback.saturating_add(1);
                number
            });
            (number, path)
        })
        .collect::<Vec<_>>();
    if let Some(highest) = numbered.iter().map(|(number, _)| *number).max() {
        *next_image_number = (*next_image_number).max(highest.saturating_add(1));
    }
    numbered
}

fn image_numbers_in_text(text: &str) -> Vec<usize> {
    let mut numbers = Vec::new();
    let mut remaining = text;
    while let Some(start) = remaining.find("[Image ") {
        let value = &remaining[start + "[Image ".len()..];
        let Some(end) = value.find(']') else {
            break;
        };
        let candidate = &value[..end];
        if !candidate.is_empty()
            && candidate.bytes().all(|byte| byte.is_ascii_digit())
            && let Ok(number) = candidate.parse::<usize>()
            && number > 0
        {
            numbers.push(number);
        }
        remaining = &value[end + 1..];
    }
    numbers
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
        SessionEventKind::TurnStarted { message_id, .. } => {
            queued_prompts.retain(|queued| queued.message_id != *message_id);
        }
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text,
            status: MessageStatus::InProgress,
            delivery: Some(delivery @ PromptDelivery::Steer),
            ..
        } => push_queued_prompt(queued_prompts, *message_id, text.clone(), *delivery),
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

fn pending_prompt_projection_from_events(events: &[SessionEvent]) -> Vec<PendingPromptProjection> {
    let mut queued_prompts = Vec::new();
    for event in events {
        update_queued_prompts(&mut queued_prompts, &event.kind);
    }
    queued_prompts
}

fn restore_optimistic_pending_prompts(
    queued_prompts: &mut Vec<PendingPromptProjection>,
    events: &[SessionEvent],
    optimistic_pending: Vec<PendingPromptProjection>,
) {
    for pending in optimistic_pending {
        if !events
            .iter()
            .any(|event| pending_prompt_projection_settled_by(event, pending.message_id))
        {
            push_queued_prompt(
                queued_prompts,
                pending.message_id,
                pending.text,
                pending.delivery,
            );
        }
    }
}

fn pending_prompt_projection_settled_by(event: &SessionEvent, message_id: Uuid) -> bool {
    match &event.kind {
        SessionEventKind::TurnStarted {
            message_id: event_message_id,
            ..
        }
        | SessionEventKind::PromptRecalled {
            message_id: event_message_id,
            ..
        } => *event_message_id == message_id,
        SessionEventKind::Message {
            message_id: event_message_id,
            actor: EventActor::User,
            status: MessageStatus::Complete | MessageStatus::Failed,
            ..
        } => *event_message_id == message_id,
        _ => false,
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

fn has_recallable_queued_prompts(
    composer_text: &str,
    queued_prompts: &[PendingPromptProjection],
) -> bool {
    composer_text.trim().is_empty()
        && queued_prompts
            .iter()
            .any(|prompt| prompt.delivery == PromptDelivery::Queue)
}

fn has_pending_steer_prompts(
    composer_text: &str,
    queued_prompts: &[PendingPromptProjection],
) -> bool {
    composer_text.trim().is_empty()
        && queued_prompts
            .iter()
            .any(|prompt| prompt.delivery == PromptDelivery::Steer)
}

fn queued_prompt_panel_height(queued_prompts: &[PendingPromptProjection], panel_width: u16) -> u16 {
    if queued_prompts.is_empty() {
        return 0;
    }
    let queue_width = panel_width.saturating_sub(26).max(1) as usize;
    let visible = queued_prompts.len().min(6);
    let text_lines = queued_prompts
        .iter()
        .take(visible)
        .map(|prompt| wrapped_pending_prompt_lines(&prompt.text, queue_width).len())
        .sum::<usize>();
    text_lines
        .saturating_add(usize::from(queued_prompts.len() > visible))
        // One top-border/title row plus one contextual shortcut row.
        .saturating_add(2)
        .min(u16::MAX as usize) as u16
}

fn wrapped_pending_prompt_lines(text: &str, width: usize) -> Vec<String> {
    let lines = wrap_display(text, width.max(1));
    if lines.is_empty() {
        vec![String::new()]
    } else {
        lines
    }
}

fn queued_prompt_lines(
    queued_prompts: &[PendingPromptProjection],
    panel_width: u16,
    subagent_accent: Option<Color>,
) -> Vec<Line<'static>> {
    let visible = queued_prompts.len().min(6);
    let queue_width = panel_width.saturating_sub(26).max(1) as usize;
    let mut lines = queued_prompts
        .iter()
        .take(visible)
        .flat_map(|prompt| {
            let label_color = subagent_accent.unwrap_or(match prompt.delivery {
                PromptDelivery::Steer => BORG_ORANGE,
                PromptDelivery::Queue => Color::Gray,
            });
            wrapped_pending_prompt_lines(&prompt.text, queue_width)
                .into_iter()
                .enumerate()
                .map(move |(index, text)| {
                    Line::from(vec![
                        Span::styled(
                            if index == 0 { " ↳ " } else { "   " },
                            Style::default().fg(Color::DarkGray),
                        ),
                        Span::styled(
                            if index == 0 { "Next  " } else { "      " },
                            Style::default()
                                .fg(label_color)
                                .add_modifier(Modifier::BOLD),
                        ),
                        Span::styled(text, Style::default().fg(Color::Gray)),
                    ])
                })
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
    if has_queue || has_steers {
        hints.push("↑ edit / recall pending");
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

fn display_local_time<'a>(time: &'a str, today_prefix: &str) -> &'a str {
    time.strip_prefix(today_prefix).unwrap_or(time)
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

fn format_context_tokens(tokens: u64) -> String {
    const THOUSAND: u64 = 1_000;
    const MILLION: u64 = 1_000_000;
    if tokens >= MILLION {
        let value = format!("{:.1}", tokens as f64 / MILLION as f64);
        format!("{}m", value.trim_end_matches(".0"))
    } else if tokens >= THOUSAND {
        let value = format!("{:.1}", tokens as f64 / THOUSAND as f64);
        format!("{}k", value.trim_end_matches(".0"))
    } else {
        tokens.to_string()
    }
}

impl TranscriptEntry {
    fn copy_text_owned(&self) -> Option<String> {
        match self {
            Self::Message { text, .. } | Self::Activity { text, .. } | Self::Info { text, .. } => {
                Some(markdown_plain_text(text))
            }
            Self::Action {
                label,
                detail,
                body,
                ..
            } => Some(
                [
                    (!label.trim().is_empty()).then_some(label.as_str()),
                    (!detail.trim().is_empty()).then_some(detail.as_str()),
                    body.as_deref().filter(|body| !body.trim().is_empty()),
                ]
                .into_iter()
                .flatten()
                .map(str::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
            ),
            Self::Plan { items, .. } => Some(
                items
                    .iter()
                    .map(|item| {
                        let marker = match item.status {
                            PlanItemStatus::Completed => "✓",
                            PlanItemStatus::InProgress => "●",
                            PlanItemStatus::Pending => "○",
                        };
                        format!("{marker} {}", item.content)
                    })
                    .collect::<Vec<_>>()
                    .join("\n"),
            ),
            Self::Goal { goal, .. } => Some(goal.objective.clone()),
            Self::Tool {
                detail,
                code_view,
                output_view,
                ..
            } => {
                // Diffs are the useful copy target for edit calls. For every
                // other tool, prefer the completed response over the command
                // that produced it, then fall back to the call summary while
                // a deferred payload is still being fetched.
                code_view
                    .as_ref()
                    .filter(|(language, body)| {
                        is_diff_language(language) && !body.trim().is_empty()
                    })
                    .map(|(_, body)| body.clone())
                    .or_else(|| {
                        output_view
                            .as_ref()
                            .filter(|(_, body)| !body.trim().is_empty())
                            .map(|(_, body)| body.clone())
                    })
                    .or_else(|| {
                        code_view
                            .as_ref()
                            .filter(|(_, body)| !body.trim().is_empty())
                            .map(|(_, body)| body.clone())
                    })
                    .or_else(|| (!detail.trim().is_empty()).then(|| detail.clone()))
            }
            Self::Compaction { summary, .. } if compaction_has_expandable_detail(summary) => {
                Some(summary.clone())
            }
            Self::Compaction { .. } => None,
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

fn apply_link_hover(line: &mut Line<'static>, start: usize, end: usize) {
    let mut column = 0usize;
    let mut spans = Vec::new();
    for span in &line.spans {
        for grapheme in span.content.graphemes(true) {
            let grapheme_width = grapheme.width();
            let hovered = column < end && column.saturating_add(grapheme_width) > start;
            let style = if hovered {
                span.style.fg(Color::LightBlue).add_modifier(Modifier::BOLD)
            } else {
                span.style
            };
            spans.push(Span::styled(grapheme.to_string(), style));
            column = column.saturating_add(grapheme_width);
        }
    }
    line.spans = spans;
}

fn apply_text_selection(
    visible_lines: &mut [Line<'static>],
    scroll_start: usize,
    selection_start: TranscriptPoint,
    selection_end: TranscriptPoint,
) {
    for (viewport_row, line) in visible_lines.iter_mut().enumerate() {
        let row = scroll_start.saturating_add(viewport_row);
        let Some((start, end)) = selection_columns_for_row(selection_start, selection_end, row)
        else {
            continue;
        };
        let selectable = selection_line_ranges(line);
        apply_column_selection(line, start, end, &selectable);
    }
}

fn apply_column_selection(
    line: &mut Line<'static>,
    start: usize,
    end: usize,
    selectable: &[(usize, usize)],
) {
    let mut column = 0usize;
    let mut spans = Vec::new();
    for span in &line.spans {
        for grapheme in span.content.graphemes(true) {
            let grapheme_width = grapheme.width();
            let selected = selectable.iter().any(|(selectable_start, selectable_end)| {
                column < end
                    && column.saturating_add(grapheme_width) > start
                    && column < *selectable_end
                    && column.saturating_add(grapheme_width) > *selectable_start
            });
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

fn apply_composer_selection(
    lines: &mut [Line<'static>],
    value: &str,
    ranges: &[(usize, usize)],
    prompt_width: usize,
    anchor: usize,
    focus: usize,
) {
    let (start, end) = if anchor <= focus {
        (anchor, focus)
    } else {
        (focus, anchor)
    };
    if start == end {
        return;
    }
    for (row, (line_start, line_end)) in ranges.iter().copied().enumerate() {
        let from = if start <= line_start {
            0
        } else {
            UnicodeWidthStr::width(&value[line_start..start.min(line_end)])
        };
        let to = if end >= line_end {
            UnicodeWidthStr::width(&value[line_start..line_end])
        } else if end > line_start {
            UnicodeWidthStr::width(&value[line_start..end])
        } else {
            0
        };
        if from >= to {
            continue;
        }
        if let Some(line) = lines.get_mut(row) {
            apply_column_selection(
                line,
                prompt_width.saturating_add(from),
                prompt_width.saturating_add(to),
                &[(prompt_width, usize::MAX)],
            );
        }
    }
}

fn selection_line_ranges(line: &Line<'static>) -> Vec<(usize, usize)> {
    let width = line.width();
    if width == 0 || line.spans.iter().all(|span| span.content.trim().is_empty()) {
        return Vec::new();
    }
    let rendered = line.to_string();
    let trimmed = rendered.trim();
    let content_trimmed = trimmed.strip_prefix("│ ").unwrap_or(trimmed);
    if trimmed.is_empty()
        || trimmed == "│"
        || content_trimmed.starts_with('┌')
        || content_trimmed.starts_with('└')
        || content_trimmed.starts_with("---")
        || content_trimmed.starts_with("+++")
        || content_trimmed.starts_with("@@")
    {
        return Vec::new();
    }
    let first = line
        .spans
        .first()
        .map(|span| span.content.as_ref())
        .unwrap_or_default();
    if rendered.contains('▌') {
        return Vec::new();
    }
    if let Some(ranges) = diff_selection_ranges(line) {
        return ranges;
    }
    if first.contains('│')
        && first[..first.find('│').unwrap_or(0)]
            .chars()
            .any(|character| character.is_ascii_digit())
    {
        return vec![(first.width(), width)];
    }
    let prefix = if first == "  " {
        2
    } else if first.starts_with("│   │ ")
        || first.starts_with("  │ ")
        || matches!(first, "+ " | "− ")
    {
        first.width()
    } else if first.starts_with("│ ") || first.starts_with("  ") {
        2
    } else {
        0
    };
    let suffix = string_after_cells(&rendered, prefix);
    let start = prefix.saturating_add(selection_content_start(suffix));
    let end = prefix.saturating_add(selection_content_end(suffix));
    (start < end).then_some((start, end)).into_iter().collect()
}

fn diff_selection_ranges(line: &Line<'static>) -> Option<Vec<(usize, usize)>> {
    let markers = ["│ + ", "│ − ", "│   ", "+ ", "− "];
    let mut span_start = 0usize;
    let mut matches = Vec::new();
    for span in &line.spans {
        let content = span.content.as_ref();
        if content.starts_with("│   │ ") {
            span_start = span_start.saturating_add(span.width());
            continue;
        }
        for marker in markers {
            let Some(marker_start) = content.find(marker) else {
                continue;
            };
            if marker.contains('│') || content == marker {
                let start = span_start.saturating_add(UnicodeWidthStr::width(
                    &content[..marker_start + marker.len()],
                ));
                let end = span_start.saturating_add(UnicodeWidthStr::width(content.trim_end()));
                matches.push((start, end));
                break;
            }
        }
        span_start = span_start.saturating_add(span.width());
    }
    if matches.is_empty() {
        return None;
    }
    let split_layout = line.spans.iter().any(|span| span.content.as_ref() == " │ ");
    if split_layout {
        return Some(
            matches
                .into_iter()
                .filter(|(start, end)| start < end)
                .collect(),
        );
    }
    if matches.len() == 1 {
        return Some(vec![(
            matches[0].0,
            UnicodeWidthStr::width(line.to_string().trim_end()),
        )]);
    }
    Some(
        matches
            .into_iter()
            .filter(|(start, end)| start < end)
            .collect(),
    )
}

fn string_after_cells(value: &str, cells: usize) -> &str {
    let mut consumed = 0usize;
    for (byte, grapheme) in value.grapheme_indices(true) {
        if consumed >= cells {
            return &value[byte..];
        }
        consumed = consumed.saturating_add(grapheme.width());
        if consumed >= cells {
            return &value[byte + grapheme.len()..];
        }
    }
    ""
}

fn selection_content_start(value: &str) -> usize {
    let leading = value.trim_start();
    let leading_cells = UnicodeWidthStr::width(&value[..value.len() - leading.len()]);
    if let Some(gutter_end) = selection_code_gutter_end(leading) {
        return leading_cells.saturating_add(gutter_end);
    }
    let lifecycle_glyphs = "✓◇⠋⠙⠹⠸⠼⠴⠦⠧!■↗?";
    if let Some((byte, glyph)) = leading
        .char_indices()
        .find(|(_, character)| lifecycle_glyphs.contains(*character))
        && display_time_prefix(&leading[..byte])
    {
        let after = &leading[byte + glyph.len_utf8()..];
        let glyph_width = UnicodeWidthStr::width(&leading[byte..byte + glyph.len_utf8()]);
        return leading_cells
            .saturating_add(UnicodeWidthStr::width(&leading[..byte]))
            .saturating_add(glyph_width)
            .saturating_add(
                UnicodeWidthStr::width(after) - UnicodeWidthStr::width(after.trim_start()),
            );
    }
    for marker in ["• ", "✓  ", "●  ", "○  ", "▣ "] {
        if let Some(rest) = leading.strip_prefix(marker) {
            return leading_cells
                .saturating_add(UnicodeWidthStr::width(marker))
                .saturating_add(
                    UnicodeWidthStr::width(rest) - UnicodeWidthStr::width(rest.trim_start()),
                );
        }
    }
    leading_cells
}

fn selection_code_gutter_end(value: &str) -> Option<usize> {
    let mut byte = 0usize;
    while value[byte..]
        .chars()
        .next()
        .is_some_and(|character| character.is_whitespace())
    {
        byte = byte.saturating_add(value[byte..].chars().next()?.len_utf8());
    }
    let digits_start = byte;
    while value[byte..]
        .chars()
        .next()
        .is_some_and(|character| character.is_ascii_digit())
    {
        byte = byte.saturating_add(value[byte..].chars().next()?.len_utf8());
    }
    if byte == digits_start {
        return None;
    }
    while value[byte..]
        .chars()
        .next()
        .is_some_and(|character| character.is_whitespace())
    {
        byte = byte.saturating_add(value[byte..].chars().next()?.len_utf8());
    }
    value[byte..]
        .strip_prefix("│ ")
        .map(|_| UnicodeWidthStr::width(&value[..byte + "│ ".len()]))
}

fn selection_content_end(value: &str) -> usize {
    let trimmed = value.trim_end();
    if let Some((content, elapsed)) = trimmed.rsplit_once("  ")
        && elapsed.split_whitespace().all(selection_elapsed_token)
        && !elapsed.is_empty()
    {
        return UnicodeWidthStr::width(content);
    }
    UnicodeWidthStr::width(trimmed)
}

fn display_time_prefix(value: &str) -> bool {
    value
        .split_whitespace()
        .last()
        .is_some_and(display_clock_token)
}

fn display_clock_token(value: &str) -> bool {
    let parts = value.split(':').collect::<Vec<_>>();
    matches!(parts.len(), 2 | 3)
        && parts.iter().all(|part| {
            !part.is_empty() && part.chars().all(|character| character.is_ascii_digit())
        })
}

fn selection_elapsed_token(value: &str) -> bool {
    let Some(unit) = value.chars().last() else {
        return false;
    };
    matches!(unit, 's' | 'm' | 'h' | 'd')
        && value[..value.len() - unit.len_utf8()]
            .chars()
            .all(|character| character.is_ascii_digit() || character == '.')
}

fn action_run_bridge(entry: &TranscriptEntry) -> bool {
    matches!(
        entry,
        TranscriptEntry::Action {
            kind: TranscriptActionKind::Agent,
            ..
        }
    ) || matches!(entry, TranscriptEntry::Activity { text, .. } if is_subagent_activity_text(text))
}

fn is_subagent_activity_text(text: &str) -> bool {
    text.starts_with("agent · ") || text.starts_with("Message agent · ")
}

fn selected_transcript_text(
    lines: &[Line<'static>],
    start: TranscriptPoint,
    end: TranscriptPoint,
) -> Option<String> {
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
        let selectable = selection_line_ranges(line);
        let mut chunks = Vec::new();
        for (selectable_start, selectable_end) in selectable {
            let chunk_start = from.max(selectable_start);
            let chunk_end = to.min(selectable_end);
            if chunk_start >= chunk_end {
                continue;
            }
            let mut column = 0usize;
            let mut chunk = String::new();
            for span in &line.spans {
                for grapheme in span.content.graphemes(true) {
                    let grapheme_width = grapheme.width();
                    if column < chunk_end && column.saturating_add(grapheme_width) > chunk_start {
                        chunk.push_str(grapheme);
                    }
                    column = column.saturating_add(grapheme_width);
                }
            }
            let chunk = chunk.trim_end().to_string();
            if !chunk.trim().is_empty() {
                chunks.push(chunk);
            }
        }
        if !chunks.is_empty() {
            if chunks.len() == 2 && chunks[0] == chunks[1] {
                chunks.truncate(1);
            }
            selected.push(chunks.join("\n"));
        }
    }
    let text = selected.join("\n").trim().to_string();
    (!text.is_empty()).then_some(text)
}

fn selection_columns_for_row(
    start: TranscriptPoint,
    end: TranscriptPoint,
    row: usize,
) -> Option<(usize, usize)> {
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

fn resolve_selection_point(
    point: SelectionPoint,
    ranges: &[SelectionRowRange],
) -> Option<TranscriptPoint> {
    let Some(range) = selection_range_for_point(point, ranges) else {
        if let Some(next) = ranges.iter().find(|range| range.entry > point.entry) {
            return Some(TranscriptPoint {
                row: next.start,
                column: 0,
            });
        }
        let previous = ranges
            .iter()
            .rev()
            .find(|range| range.entry < point.entry)?;
        return Some(TranscriptPoint {
            row: previous.end.saturating_sub(1).max(previous.start),
            column: usize::MAX,
        });
    };
    let last = range.end.saturating_sub(1).max(range.start);
    let row = (range.start as isize + point.row_in_entry as isize - range.body_start as isize)
        .clamp(range.start as isize, last as isize) as usize;
    Some(TranscriptPoint {
        row,
        column: point.column,
    })
}

fn selection_range_for_point(
    point: SelectionPoint,
    ranges: &[SelectionRowRange],
) -> Option<&SelectionRowRange> {
    let entry_ranges = ranges.iter().filter(|range| range.entry == point.entry);
    entry_ranges
        .clone()
        .find(|range| {
            point.row_in_entry >= range.body_start && point.row_in_entry < range.body_end()
        })
        .or_else(|| {
            entry_ranges.min_by_key(|range| {
                if point.row_in_entry < range.body_start {
                    range.body_start - point.row_in_entry
                } else {
                    point.row_in_entry.saturating_sub(range.body_end())
                }
            })
        })
}

fn resolve_selection_point_in_lines(
    point: SelectionPoint,
    ranges: &[SelectionRowRange],
    lines: &[Line<'static>],
) -> Option<TranscriptPoint> {
    let range = ranges
        .iter()
        .find(|range| range.entry == point.entry && range.uses_logical_offsets)?;
    let logical_offset = point.logical_offset?;
    let last = range.end.saturating_sub(1).max(range.start);
    let mut remaining = logical_offset;
    for row in range.start..range.end {
        let line = lines.get(row)?;
        let width = selection_line_selectable_width(line);
        if remaining < width {
            return Some(TranscriptPoint {
                row,
                column: selection_column_for_offset(line, remaining),
            });
        }
        remaining = remaining.saturating_sub(width);
    }
    let line = lines.get(last)?;
    let width = selection_line_selectable_width(line);
    Some(TranscriptPoint {
        row: last,
        column: selection_column_for_offset(line, width),
    })
}

#[cfg(test)]
fn resolved_selection(
    selection: TextSelection,
    ranges: &[SelectionRowRange],
) -> Option<(TranscriptPoint, TranscriptPoint)> {
    let anchor = resolve_selection_point(selection.anchor, ranges)?;
    let focus = resolve_selection_point(selection.focus, ranges)?;
    if anchor == focus {
        return None;
    }
    Some(if anchor <= focus {
        (anchor, focus)
    } else {
        (focus, anchor)
    })
}

fn resolved_selection_in_lines(
    selection: TextSelection,
    ranges: &[SelectionRowRange],
    lines: &[Line<'static>],
) -> Option<(TranscriptPoint, TranscriptPoint)> {
    let anchor = selection
        .anchor
        .logical_offset
        .and_then(|_| resolve_selection_point_in_lines(selection.anchor, ranges, lines))
        .or_else(|| resolve_selection_point(selection.anchor, ranges))?;
    let focus = selection
        .focus
        .logical_offset
        .and_then(|_| resolve_selection_point_in_lines(selection.focus, ranges, lines))
        .or_else(|| resolve_selection_point(selection.focus, ranges))?;
    if anchor == focus {
        return None;
    }
    Some(if anchor <= focus {
        (anchor, focus)
    } else {
        (focus, anchor)
    })
}

fn mouse_starts_text_selection(mouse: &MouseEvent, area: Option<Rect>) -> bool {
    matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left))
        && area.is_some_and(|area| area.contains(Position::new(mouse.column, mouse.row)))
}

fn finish_text_selection(
    selection: &mut Option<TextSelection>,
    pending_click: &mut Option<PendingTranscriptClick>,
) -> Option<PendingTranscriptClick> {
    let empty = selection.is_some_and(TextSelection::is_empty);
    if let Some(selection) = selection.as_mut() {
        selection.dragging = false;
        selection.autoscroll = 0;
    }
    let click = empty.then(|| pending_click.take()).flatten();
    if empty {
        *selection = None;
    }
    *pending_click = None;
    click
}

fn selection_autoscroll_direction(area: Rect, pointer: Position) -> isize {
    if pointer.y <= area.y {
        1
    } else if pointer.y >= area.bottom().saturating_sub(1) {
        -1
    } else {
        0
    }
}

fn advance_selection_autoscroll(
    scroll_from_bottom: usize,
    scroll_max: usize,
    direction: isize,
) -> usize {
    scroll_from_bottom_by_lines(
        scroll_from_bottom,
        scroll_max,
        direction.saturating_mul(SELECTION_AUTOSCROLL_LINES_PER_FRAME as isize),
    )
}

fn scroll_from_bottom_by_lines(
    scroll_from_bottom: usize,
    scroll_max: usize,
    lines: isize,
) -> usize {
    if lines >= 0 {
        scroll_from_bottom
            .saturating_add(lines.unsigned_abs())
            .min(scroll_max)
    } else {
        scroll_from_bottom.saturating_sub(lines.unsigned_abs())
    }
}

#[cfg(test)]
fn selection_point_for_viewport_pointer(
    area: Rect,
    scroll_start: usize,
    pointer: Position,
    ranges: &[SelectionRowRange],
) -> SelectionPoint {
    let pointer = Position::new(
        pointer.x.clamp(area.x, area.right().saturating_sub(1)),
        pointer.y.clamp(area.y, area.bottom().saturating_sub(1)),
    );
    let row = scroll_start.saturating_add(usize::from(pointer.y.saturating_sub(area.y)));
    let column = usize::from(pointer.x.saturating_sub(area.x));
    selection_point_for_row(ranges, row, column)
}

fn selection_point_for_viewport_pointer_in_lines(
    area: Rect,
    scroll_start: usize,
    pointer: Position,
    ranges: &[SelectionRowRange],
    lines: &[Line<'static>],
) -> SelectionPoint {
    let pointer = Position::new(
        pointer.x.clamp(area.x, area.right().saturating_sub(1)),
        pointer.y.clamp(area.y, area.bottom().saturating_sub(1)),
    );
    let row = scroll_start.saturating_add(usize::from(pointer.y.saturating_sub(area.y)));
    let column = usize::from(pointer.x.saturating_sub(area.x));
    selection_point_for_row_in_lines(ranges, lines, row, column)
}

fn selection_point_for_row(
    ranges: &[SelectionRowRange],
    row: usize,
    column: usize,
) -> SelectionPoint {
    if let Some(range) = ranges
        .iter()
        .find(|range| row >= range.start && row < range.end)
    {
        return SelectionPoint {
            entry: range.entry,
            row_in_entry: range.body_start + (row - range.start),
            column,
            logical_offset: None,
        };
    }
    if let Some(range) = ranges.iter().find(|range| range.start > row) {
        return SelectionPoint {
            entry: range.entry,
            row_in_entry: range.body_start,
            column,
            logical_offset: None,
        };
    }
    if let Some(range) = ranges.last() {
        return SelectionPoint {
            entry: range.entry,
            row_in_entry: range
                .body_start
                .saturating_add(range.end.saturating_sub(1).saturating_sub(range.start)),
            column,
            logical_offset: None,
        };
    }
    SelectionPoint {
        entry: 0,
        row_in_entry: row,
        column,
        logical_offset: None,
    }
}

fn selection_point_for_row_in_lines(
    ranges: &[SelectionRowRange],
    lines: &[Line<'static>],
    row: usize,
    column: usize,
) -> SelectionPoint {
    let point = selection_point_for_row(ranges, row, column);
    let Some(range) = ranges
        .iter()
        .find(|range| row >= range.start && row < range.end)
        .filter(|range| range.uses_logical_offsets)
    else {
        return point;
    };
    let logical_offset = (range.start..row)
        .filter_map(|line| lines.get(line))
        .map(selection_line_selectable_width)
        .sum::<usize>()
        .saturating_add(
            lines
                .get(row)
                .map_or(0, |line| selection_offset_for_column(line, column)),
        );
    SelectionPoint {
        logical_offset: Some(logical_offset),
        ..point
    }
}

fn selection_line_selectable_width(line: &Line<'static>) -> usize {
    selection_line_ranges(line)
        .into_iter()
        .map(|(start, end)| end.saturating_sub(start))
        .sum()
}

fn selection_offset_for_column(line: &Line<'static>, column: usize) -> usize {
    let mut offset = 0usize;
    for (start, end) in selection_line_ranges(line) {
        if column <= start {
            return offset;
        }
        if column < end {
            return offset.saturating_add(column.saturating_sub(start));
        }
        offset = offset.saturating_add(end.saturating_sub(start));
    }
    offset
}

fn selection_column_for_offset(line: &Line<'static>, mut offset: usize) -> usize {
    let ranges = selection_line_ranges(line);
    for (start, end) in &ranges {
        let width = end.saturating_sub(*start);
        if offset < width {
            return start.saturating_add(offset);
        }
        offset = offset.saturating_sub(width);
    }
    ranges.last().map_or(0, |(_, end)| *end)
}

fn sticky_tool_header_background(hovered: bool) -> Color {
    if hovered {
        MESSAGE_HOVER_BG
    } else {
        Color::Reset
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

fn resolve_pending_scroll_anchor(
    follow_tail: bool,
    scroll_from_bottom: usize,
    previous_height: Option<usize>,
    next_height: usize,
) -> usize {
    if follow_tail {
        return 0;
    }
    previous_height.map_or(scroll_from_bottom, |previous_height| {
        preserve_scroll_anchor(scroll_from_bottom, previous_height, next_height)
    })
}

fn should_preserve_transcript_viewport(follow_tail: bool) -> bool {
    !follow_tail
}

fn should_load_history_page(
    explicitly_requested: bool,
    scroll_from_bottom: usize,
    scroll_max: usize,
    viewport_height: usize,
) -> bool {
    explicitly_requested
        && scroll_max.saturating_sub(scroll_from_bottom.min(scroll_max))
            <= viewport_height.saturating_mul(2)
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
        entry_index,
        entry_row_offset: row.saturating_sub(entry_start),
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

fn read_git_worktree_status(cwd: &Path) -> Option<GitWorktreeStatus> {
    let output = Command::new("git")
        .args(["status", "--porcelain=v1", "--branch"])
        .current_dir(cwd)
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| parse_git_worktree_status(&String::from_utf8_lossy(&output.stdout)))
        .flatten()
}

fn parse_git_worktree_status(output: &str) -> Option<GitWorktreeStatus> {
    let mut lines = output.lines();
    let header = lines.next()?.strip_prefix("## ")?;
    let branch = header
        .strip_prefix("No commits yet on ")
        .unwrap_or(header)
        .split("...")
        .next()
        .unwrap_or(header)
        .split(" [")
        .next()
        .unwrap_or(header);
    let branch = if branch == "HEAD (no branch)" {
        "detached"
    } else {
        branch
    };
    let count = |name: &str| {
        header
            .split(['[', ']', ','])
            .map(str::trim)
            .find_map(|part| part.strip_prefix(name))
            .and_then(|value| value.parse().ok())
            .unwrap_or(0)
    };
    Some(GitWorktreeStatus {
        branch: branch.to_string(),
        dirty: lines.next().is_some(),
        ahead: count("ahead "),
        behind: count("behind "),
    })
}

fn fish_style_path(path: &Path) -> String {
    let home = std::env::var_os("HOME").map(PathBuf::from);
    fish_style_path_with_home(path, home.as_deref())
}

fn fish_style_path_with_home(path: &Path, home: Option<&Path>) -> String {
    if let Some(relative) = home
        .filter(|home| !home.as_os_str().is_empty())
        .and_then(|home| path.strip_prefix(home).ok())
    {
        if relative.as_os_str().is_empty() {
            return "~".to_string();
        }
        return format!(
            "~{}{}",
            std::path::MAIN_SEPARATOR,
            abbreviated_path(relative)
        );
    }
    abbreviated_path(path)
}

fn abbreviated_path(path: &Path) -> String {
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

/// Subsequence match, case-insensitive: "gl" finds "/goal", "expt" finds
/// "/expand-tools". Deliberately not scored — the palette keeps source order so
/// a row never moves out from under the key the user is about to press.
fn fuzzy_matches(haystack: &str, needle: &str) -> bool {
    let mut haystack = haystack.chars().flat_map(char::to_lowercase);
    needle
        .chars()
        .flat_map(char::to_lowercase)
        .filter(|character| !character.is_whitespace())
        .all(|wanted| haystack.any(|character| character == wanted))
}

/// Commands whose bare form is not a command at all: submitting `/steer` with
/// no message would send the literal word to the model, so the palette puts
/// them in the composer for the user to finish.
fn slash_command_needs_argument(command: &str) -> bool {
    matches!(
        command,
        "/ask" | "/director" | "/claude" | "/gpt" | "/peer" | "/queue" | "/steer"
    )
}

/// Every row of the unified palette: the slash commands, then the keybindings
/// as reference rows that insert nothing.
fn command_palette_options(
    keymap: &KeyMap,
    extension_commands: &[borg_remote::ExtensionApiCommand],
) -> Vec<PickerOption> {
    let mut options = Vec::with_capacity(SLASH_COMMANDS.len() + extension_commands.len() + 12);
    for (index, (command, help)) in SLASH_COMMANDS.iter().enumerate() {
        let mut option = PickerOption::new(format!("{command:<16}{help}"), *command);
        if index == 0 {
            option.section = Some("Commands".to_string());
        }
        options.push(option);
    }
    for (index, command) in extension_commands.iter().enumerate() {
        let user_name = borg_remote::ExtensionApiSnapshot::command_user_name(command);
        let mut option = PickerOption::new(
            format!("{user_name:<24}{}", command.description.trim()),
            user_name,
        );
        if index == 0 {
            option.section = Some("Extension commands".to_string());
        }
        options.push(option);
    }
    for (index, (action, chord)) in keybinding_reference(keymap).into_iter().enumerate() {
        // An empty value marks a row with nothing to run; Enter just closes.
        let mut option = PickerOption::new(format!("{action:<26} {chord}"), String::new());
        if index == 0 {
            option.section = Some("Keybindings".to_string());
        }
        options.push(option);
    }
    options
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
        "send {} · commands / · palette tab or {}",
        keymap.label(KeyAction::Send),
        keymap.label(KeyAction::Keybindings)
    )
}

fn active_message_placeholder(steer_active: bool) -> &'static str {
    if steer_active {
        "Type a follow-up to redirect the current turn now…"
    } else {
        "Type a follow-up to send after the current turn finishes…"
    }
}

fn is_copy_notice(message: &str) -> bool {
    message.to_ascii_lowercase().contains("copied")
}

fn copy_notice_line(notice: String) -> Line<'static> {
    let style = Style::default()
        .fg(Color::Black)
        .bg(Color::LightGreen)
        .add_modifier(Modifier::BOLD);
    Line::from(Span::styled(format!("  {notice}  "), style))
}

fn inset_control_lines(mut lines: Vec<Line<'static>>) -> Vec<Line<'static>> {
    for line in &mut lines {
        line.spans.insert(0, Span::raw(" "));
    }
    lines
}

/// The one list of bindings, shared by the tooltip and the command palette so
/// the two can never drift.
fn keybinding_reference(keymap: &KeyMap) -> Vec<(&'static str, String)> {
    vec![
        ("send", keymap.label(KeyAction::Send)),
        ("send after current turn", keymap.label(KeyAction::Queue)),
        ("newline", keymap.label(KeyAction::Newline)),
        ("commands", "/".to_string()),
        ("interrupt or close", keymap.label(KeyAction::Interrupt)),
        ("clear · twice exit", keymap.label(KeyAction::ClearOrExit)),
        ("exit", keymap.label(KeyAction::Exit)),
        ("attach image", keymap.label(KeyAction::AttachImage)),
        ("start/stop dictation", keymap.label(KeyAction::Dictate)),
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
    ]
}

fn keybinding_lines(keymap: &KeyMap, width: usize) -> Vec<Line<'static>> {
    let bindings = keybinding_reference(keymap);
    let action_style = Style::default().fg(Color::White);
    let key_style = Style::default()
        .fg(BORG_ORANGE_HOVER)
        .add_modifier(Modifier::BOLD);
    let separator_style = Style::default().fg(Color::DarkGray);

    if width >= 76 {
        let action_width = |column: usize| {
            bindings
                .iter()
                .skip(column)
                .step_by(2)
                .map(|(action, _)| action.width())
                .max()
                .unwrap_or(0)
        };
        let left_action_width = action_width(0);
        let right_action_width = action_width(1);
        let key_width = |column: usize| {
            bindings
                .iter()
                .skip(column)
                .step_by(2)
                .map(|(_, key)| key.width())
                .max()
                .unwrap_or(0)
        };
        let left_width = left_action_width + 1 + key_width(0);
        let right_width = right_action_width + 1 + key_width(1);

        if left_width + 3 + right_width <= width {
            let mut lines = Vec::new();
            for pair in bindings.chunks(2) {
                let left = &pair[0];
                let right = pair.get(1);
                let mut spans =
                    vec![
                        Span::styled(left.0.to_string(), action_style),
                        Span::raw(" ".repeat(left_action_width.saturating_sub(left.0.width()))),
                        Span::raw(" "),
                        Span::styled(left.1.clone(), key_style),
                        Span::raw(" ".repeat(
                            left_width.saturating_sub(left_action_width + 1 + left.1.width()),
                        )),
                    ];
                if let Some(right) = right {
                    spans.extend([
                        Span::styled(" │ ", separator_style),
                        Span::styled(right.0.to_string(), action_style),
                        Span::raw(" ".repeat(right_action_width.saturating_sub(right.0.width()))),
                        Span::raw(" "),
                        Span::styled(right.1.clone(), key_style),
                    ]);
                }
                lines.push(Line::from(spans));
            }
            return lines;
        }
    }

    bindings
        .iter()
        .flat_map(|binding| {
            let label = format!("{} {}", binding.0, binding.1);
            wrap_display(&label, width.max(1))
                .into_iter()
                .map(|line| Line::from(vec![Span::styled(line, action_style)]))
        })
        .collect()
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

fn cached_transcript_render(
    transcript: &Transcript,
    cache: &mut Option<CachedTranscriptRender>,
    width: usize,
    tool_run_viewport_height: usize,
    goal_tick: Option<i64>,
    tool_elapsed_tick: Option<i64>,
    local_date: NaiveDate,
) -> Arc<TranscriptRender> {
    cache
        .as_ref()
        .filter(
            |(
                cached_width,
                cached_tool_run_viewport_height,
                cached_goal_tick,
                cached_tool_elapsed_tick,
                cached_date,
                _,
            )| {
                *cached_width == width
                    && *cached_tool_run_viewport_height == tool_run_viewport_height
                    && *cached_goal_tick == goal_tick
                    && *cached_tool_elapsed_tick == tool_elapsed_tick
                    && *cached_date == local_date
            },
        )
        .map(|(_, _, _, _, _, render)| Arc::clone(render))
        .unwrap_or_else(|| {
            let render = Arc::new(transcript.render_for_cache(width, tool_run_viewport_height));
            *cache = Some((
                width,
                tool_run_viewport_height,
                goal_tick,
                tool_elapsed_tick,
                local_date,
                Arc::clone(&render),
            ));
            render
        })
}

fn transcript_width_for_viewport(
    content_width: u16,
    transcript_height: usize,
    viewport_height: usize,
) -> usize {
    let full_width = content_width.max(1) as usize;
    if content_width > 4 && transcript_height > viewport_height {
        content_width
            .saturating_sub(TRANSCRIPT_SCROLLBAR_GUTTER_WIDTH)
            .max(1) as usize
    } else {
        full_width
    }
}

fn responsive_launch_width(available: u16) -> u16 {
    if available < 70 {
        available
    } else {
        available.saturating_mul(3) / 5
    }
    .max(1)
}

fn composer_panel_height(
    line_count: usize,
    cursor_row: usize,
    max_content_height: usize,
    fixed_height: bool,
) -> u16 {
    let content_height = if fixed_height {
        max_content_height
    } else {
        line_count
            .max(cursor_row.saturating_add(1))
            .clamp(1, max_content_height)
    };
    (content_height.min(u16::MAX as usize) as u16).saturating_add(2)
}

/// The launch composition lives inside the first root chunk, so its composer
/// must leave room for the splash, controls, and one-line status footer.
fn bounded_launch_composer_height(desired: u16, terminal_height: u16, controls_height: u16) -> u16 {
    desired.min(
        terminal_height
            .saturating_sub(7)
            .saturating_sub(controls_height)
            .max(1),
    )
}

fn terminal_vertical_chunks(
    area: Rect,
    queued_height: u16,
    composer_height: u16,
    footer_height: u16,
    is_launch_screen: bool,
) -> [Rect; 5] {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(3),
            Constraint::Length(queued_height),
            Constraint::Length(2 * u16::from(!is_launch_screen)),
            // On the launch screen the composer is nested in chunk zero. Do
            // not reserve it a second time at the bottom of the root layout.
            Constraint::Length(composer_height * u16::from(!is_launch_screen)),
            Constraint::Length(footer_height),
        ])
        .split(area);
    [chunks[0], chunks[1], chunks[2], chunks[3], chunks[4]]
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

/// Keep the working directory readable in the footer. Context telemetry is
/// the expendable part of this compact line; the path is always appended in
/// full so it never receives an ellipsis or loses its final directory name.
fn footer_metadata_text(context_status: &str, cwd_status: &str, max_width: usize) -> String {
    let context_status = context_status.trim();
    let cwd_status = cwd_status.trim();
    if cwd_status.is_empty() {
        return truncate_table_cell(context_status, max_width);
    }
    // Keep one cell of breathing room between the path and the terminal edge.
    let cwd_display = format!("{cwd_status} ");
    if context_status.is_empty() {
        return cwd_display;
    }
    let separator = STATUS_SEPARATOR;
    let cwd_width = cwd_display.width();
    let separator_width = separator.width();
    let context_width = context_status.width();
    let required_width = context_width
        .saturating_add(separator_width)
        .saturating_add(cwd_width);
    if required_width <= max_width {
        return format!("{context_status}{separator}{cwd_display}");
    }
    if cwd_width.saturating_add(separator_width) > max_width {
        return cwd_display;
    }
    let context_width = max_width.saturating_sub(cwd_width + separator_width);
    format!(
        "{}{separator}{cwd_display}",
        truncate_table_cell(context_status, context_width)
    )
}

fn footer_metadata_line(
    context_status: &str,
    cwd_status: &str,
    context_imminent: bool,
    max_width: usize,
) -> Line<'static> {
    let metadata = footer_metadata_text(context_status, cwd_status, max_width);
    let context_color = if context_imminent {
        Color::Yellow
    } else {
        Color::Gray
    };
    let Some((context, cwd)) = metadata.split_once(STATUS_SEPARATOR) else {
        let cwd_only =
            !cwd_status.trim().is_empty() && metadata == format!("{} ", cwd_status.trim());
        return Line::from(Span::styled(
            metadata,
            Style::default().fg(if cwd_only { Color::Gray } else { context_color }),
        ));
    };
    Line::from(vec![
        Span::styled(context.to_string(), Style::default().fg(context_color)),
        Span::styled(STATUS_SEPARATOR, Style::default().fg(Color::Gray)),
        Span::styled(cwd.to_string(), Style::default().fg(Color::Gray)),
    ])
}

fn footer_todo_metadata_line(
    todo_status: &str,
    cwd_status: &str,
    hovered: bool,
    max_width: usize,
) -> Line<'static> {
    let metadata = footer_metadata_text(todo_status, cwd_status, max_width);
    let todo_style = Style::default()
        .fg(if hovered {
            Color::White
        } else {
            Color::LightGreen
        })
        .add_modifier(if hovered {
            Modifier::BOLD | Modifier::UNDERLINED
        } else {
            Modifier::empty()
        });
    let Some((todo, cwd)) = metadata.split_once(STATUS_SEPARATOR) else {
        return Line::from(Span::styled(metadata, todo_style));
    };
    Line::from(vec![
        Span::styled(todo.to_string(), todo_style),
        Span::styled(STATUS_SEPARATOR, Style::default().fg(Color::Gray)),
        Span::styled(cwd.to_string(), Style::default().fg(Color::Gray)),
    ])
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

fn wheel_scroll_distance(viewport_height: u16, repetitions: usize) -> isize {
    wheel_scroll_lines(viewport_height)
        .saturating_mul(isize::try_from(repetitions).unwrap_or(isize::MAX))
}

fn nested_wheel_scroll_lines(terminal_height: u16) -> isize {
    let full = NESTED_WHEEL_SCROLL_FULL_HEIGHT_ROWS;
    let half = full / 2;
    let span = full - half;
    let t = usize::from(terminal_height).clamp(half, full) - half;
    let range = MAX_WHEEL_SCROLL_LINES_PER_EVENT - MIN_WHEEL_SCROLL_LINES_PER_EVENT;
    (MIN_WHEEL_SCROLL_LINES_PER_EVENT + (range * t * t + span * span / 2) / (span * span)) as isize
}

fn nested_wheel_scroll_distance(terminal_height: u16, repetitions: usize) -> isize {
    nested_wheel_scroll_lines(terminal_height)
        .saturating_mul(isize::try_from(repetitions).unwrap_or(isize::MAX))
}

fn sticky_tool_run_header_row(
    tool_run_rows: &[ToolRunRowRange],
    scroll_start: usize,
) -> Option<(usize, usize, bool)> {
    tool_run_rows
        .iter()
        .rev()
        .find(|(_, start, end, _, _)| *start < scroll_start && *end > scroll_start)
        .map(|(index, start, _, _, expandable)| (*index, *start, *expandable))
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

/// Compact operator-facing results for Borg's own control surface. Unknown
/// tools deliberately return None and retain the generic JSON renderer.
fn borg_control_tool_output_view(
    name: &str,
    input: Option<&serde_json::Value>,
    output: &str,
) -> Option<String> {
    let leaf = name
        .rsplit(['.', '_'])
        .next()
        .unwrap_or(name)
        .to_ascii_lowercase();
    let control = matches!(
        leaf.as_str(),
        "agents" | "agent" | "message" | "task" | "goal" | "plan"
    ) || [
        "list_agents",
        "spawn_agent",
        "followup_task",
        "send_message",
        "wait_agent",
        "get_goal",
        "update_goal",
        "get_plan",
        "update_plan",
        "list_unread_team_messages",
    ]
    .iter()
    .any(|candidate| name.to_ascii_lowercase().ends_with(candidate));
    if !control {
        return None;
    }
    let value = decoded_tool_output(output)?;
    let tool = name.to_ascii_lowercase();
    let mut rows = Vec::new();
    if tool.ends_with("list_unread_team_messages") {
        let messages = value.as_array()?;
        rows.push(format!(
            "UNREAD · {} message{}",
            messages.len(),
            if messages.len() == 1 { "" } else { "s" }
        ));
        for message in messages.iter().take(12) {
            let delivery = json_text(message, &["delivery"]).unwrap_or("queued");
            let sender = json_text(message, &["sender", "from", "actor"]);
            let text = json_text(message, &["text", "message"]).unwrap_or("empty message");
            let prefix = sender.map_or_else(
                || format!("  {delivery:>10}  "),
                |sender| format!("  {delivery:>10}  {sender} · "),
            );
            rows.push(format!("{prefix}{}", compact_text(text, 140)));
        }
    } else if tool.ends_with("list_agents") || value.get("agents").is_some() || value.is_array() {
        let agents = value
            .get("agents")
            .and_then(serde_json::Value::as_array)
            .or_else(|| value.as_array())?;
        rows.push(format!(
            "TEAM · {} subagent{}",
            agents.len(),
            if agents.len() == 1 { "" } else { "s" }
        ));
        for agent in agents.iter().take(12) {
            let id = json_text(agent, &["task_name", "name", "id", "agent_id"]).unwrap_or("agent");
            let status = json_text(agent, &["status", "state"]).unwrap_or("unknown");
            let model = json_text(agent, &["model", "provider"]);
            let effort = json_text(agent, &["effort", "reasoning_effort"]);
            let task = json_text(agent, &["task", "objective", "message"]);
            let mut line = format!("  {status:>10}  {id}");
            if let Some(model) = model {
                line.push_str(&format!(" · {model}"));
            }
            if let Some(effort) = effort {
                line.push_str(&format!("/{effort}"));
            }
            rows.push(line);
            if let Some(task) = task {
                rows.push(format!("              {}", compact_text(task, 100)));
            }
        }
    } else if tool.ends_with("get_plan")
        || tool.ends_with("update_plan")
        || value.get("plan").is_some()
        || value.get("items").is_some()
    {
        let steps = value
            .get("plan")
            .or_else(|| value.get("items"))
            .and_then(serde_json::Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        rows.push(format!(
            "PLAN · {} step{}",
            steps.len(),
            if steps.len() == 1 { "" } else { "s" }
        ));
        for step in steps.iter().take(12) {
            let status = json_text(step, &["status"]).unwrap_or("pending");
            let text = json_text(step, &["step", "content", "title", "description"])
                .unwrap_or("unnamed step");
            rows.push(format!("  {status:>10}  {}", compact_text(text, 120)));
        }
    } else if tool.ends_with("get_goal")
        || tool.ends_with("update_goal")
        || value.get("goal").is_some()
    {
        let goal = value.get("goal").unwrap_or(&value);
        let status = json_text(goal, &["status"]).unwrap_or("current");
        let objective = json_text(goal, &["objective", "title"]).unwrap_or("goal");
        rows.push(format!(
            "GOAL · {status} · {}",
            compact_text(objective, 140)
        ));
    } else if tool.ends_with("wait_agent") || tool.ends_with("spawn_agent") {
        let agent = value.get("agent").unwrap_or(&value);
        let id = json_text(agent, &["task_name", "name", "id", "agent_id"])
            .or_else(|| input.and_then(|input| json_text(input, &["task_name", "target"])))
            .unwrap_or("agent");
        let status = json_text(agent, &["status", "state"]).unwrap_or("updated");
        let action = if tool.ends_with("wait_agent") {
            "WAIT"
        } else {
            "SPAWN"
        };
        let mut row = format!("{action} · {status} · {id}");
        if let Some(model) = json_text(agent, &["model", "provider"]) {
            row.push_str(&format!(" · {model}"));
        }
        if let Some(effort) = json_text(agent, &["effort", "reasoning_effort"]) {
            row.push_str(&format!("/{effort}"));
        }
        rows.push(row);
        if let Some(text) = json_text(
            agent,
            &["message", "update", "final_text", "task", "objective"],
        ) {
            rows.push(format!("  {}", compact_text(text, 140)));
        }
    } else {
        let target = input
            .and_then(|input| json_text(input, &["target", "task_name"]))
            .unwrap_or("team");
        let message = input.and_then(|input| json_text(input, &["message", "prompt"]));
        let action = if tool.ends_with("wait_agent") {
            "WAIT"
        } else if tool.ends_with("spawn_agent") {
            "SPAWN"
        } else if tool.ends_with("followup_task") {
            "FOLLOW UP"
        } else {
            "MESSAGE"
        };
        rows.push(format!("{action} · {target}"));
        if let Some(message) = message {
            rows.push(format!("  {}", compact_text(message, 140)));
        }
    }
    Some(rows.join("\n"))
}

fn decoded_tool_output(output: &str) -> Option<serde_json::Value> {
    let value = serde_json::from_str::<serde_json::Value>(output).ok()?;
    if let Some(structured) = value
        .get("structuredContent")
        .filter(|structured| !structured.is_null())
    {
        return Some(structured.clone());
    }
    value
        .get("content")
        .and_then(serde_json::Value::as_array)
        .and_then(|content| {
            content.iter().find_map(|item| {
                item.get("text")
                    .and_then(serde_json::Value::as_str)
                    .and_then(|text| serde_json::from_str(text).ok())
            })
        })
        .or(Some(value))
}

fn json_text<'a>(value: &'a serde_json::Value, names: &[&str]) -> Option<&'a str> {
    names
        .iter()
        .find_map(|name| value.get(*name).and_then(serde_json::Value::as_str))
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

fn splash_logo_line(elapsed: Duration, seed: u64) -> Line<'static> {
    let bold = Modifier::BOLD;
    const ORIGINAL: [char; 4] = ['B', 'O', 'R', 'G'];
    const GLYPHS: [[char; 8]; 4] = [
        ['界', 'Ж', 'ש', 'ب', 'ß', 'β', '฿', 'Б'],
        ['カ', 'あ', 'ท', 'Ø', 'Ω', 'Ө', '〇', 'ओ'],
        ['한', 'Я', '東', 'Я', '₹', '尺', 'र', 'Ř'],
        ['ก', 'न', 'Ω', 'Ğ', 'Ԍ', 'Ǥ', 'Ǧ', 'ဂ'],
    ];
    let phase = (elapsed.as_millis() / 110) as u64;
    let mut random = splitmix64(seed ^ phase.wrapping_mul(0x9e37_79b9_7f4a_7c15));
    let roll = random % 100;
    let changed_count = if roll < 76 {
        1
    } else if roll < 95 {
        2
    } else if roll < 99 {
        3
    } else {
        4
    };
    let mut cells = ORIGINAL;
    let mut changed = [false; 4];
    if phase < 12 {
        for _ in 0..changed_count {
            random = splitmix64(random);
            let mut index = (random % 4) as usize;
            while changed[index] {
                index = (index + 1) % 4;
            }
            changed[index] = true;
            random = splitmix64(random);
            cells[index] = GLYPHS[index][(random % GLYPHS[index].len() as u64) as usize];
        }
    }
    let colors = [Color::Cyan, BORG_ORANGE, Color::Red, Color::White];
    let mut spans = Vec::with_capacity(4);
    for (index, glyph) in cells.into_iter().enumerate() {
        let mut cell = glyph.to_string();
        if index < 3 {
            cell.push_str(
                &" ".repeat(2usize.saturating_sub(UnicodeWidthStr::width(cell.as_str()))),
            );
        }
        spans.push(Span::styled(
            cell,
            Style::default()
                .fg(if changed[index] {
                    random = splitmix64(random);
                    colors[(random % colors.len() as u64) as usize]
                } else {
                    Color::White
                })
                .add_modifier(bold),
        ));
    }
    Line::from(spans)
}

fn splash_alpha_line() -> Line<'static> {
    Line::from(Span::styled("αlphα", Style::default().fg(BORG_ORANGE)))
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn splash_version() -> String {
    format!("v{}", env!("CARGO_PKG_VERSION"))
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

fn status_control_is_actionable(status: SessionStatus) -> bool {
    matches!(
        status,
        SessionStatus::Starting | SessionStatus::Running | SessionStatus::WaitingForApproval
    )
}

fn status_control_spans(
    glyph: &str,
    label: &str,
    color: Color,
    highlighted: bool,
    duration: Option<&str>,
) -> Vec<Span<'static>> {
    let foreground = if highlighted { Color::White } else { color };
    let supporting_style = Style::default()
        .fg(foreground)
        .add_modifier(if highlighted {
            Modifier::BOLD
        } else {
            Modifier::empty()
        });
    let label_style = Style::default()
        .fg(foreground)
        .add_modifier(if highlighted {
            Modifier::BOLD | Modifier::UNDERLINED
        } else {
            Modifier::empty()
        });
    let mut spans = vec![
        Span::styled(format!(" {glyph} "), supporting_style),
        Span::styled(label.to_string(), label_style),
    ];
    if let Some(duration) = duration {
        spans.push(Span::styled(format!(" {duration}"), supporting_style));
    }
    spans
}

fn status_control_hit_area(
    status: SessionStatus,
    status_area: Rect,
    alignment_offset: u16,
    status_width: usize,
) -> Option<Rect> {
    (status_control_is_actionable(status) && status_width > 0).then(|| Rect {
        x: status_area.x.saturating_add(alignment_offset),
        y: status_area.y,
        width: (status_width as u16).min(status_area.width),
        height: 1,
    })
}

fn overlay_suppresses_background_hover(
    picker_open: bool,
    team_switcher_open: bool,
    keybindings_open: bool,
) -> bool {
    picker_open || team_switcher_open || keybindings_open
}

fn todo_tooltip_row_style(completed: bool) -> Style {
    Style::default()
        .fg(if completed {
            Color::DarkGray
        } else {
            Color::White
        })
        .add_modifier(if completed {
            Modifier::CROSSED_OUT
        } else {
            Modifier::empty()
        })
}

fn message_interaction_hint(
    entries: &[TranscriptEntry],
    hovered_message: Option<usize>,
) -> Option<&'static str> {
    hovered_message.and_then(|index| {
        matches!(
            entries.get(index),
            Some(TranscriptEntry::Message {
                actor: EventActor::User | EventActor::Assistant,
                ..
            })
        )
        .then_some("left click copy message")
    })
}

#[derive(Clone, Copy, Default)]
struct BottomInteractionHintState {
    status_hovered: bool,
    status_is_interruptible: bool,
    goal_status_hovered: bool,
    goal_available: bool,
    agents_status_hovered: bool,
    model_status_hovered: bool,
    effort_status_hovered: bool,
    permission_status_hovered: bool,
}

fn bottom_interaction_hint(state: BottomInteractionHintState) -> Option<&'static str> {
    if state.status_hovered && state.status_is_interruptible {
        Some("left click interrupt")
    } else if state.goal_status_hovered && state.goal_available {
        Some("left click toggle/manage · right click clear goal")
    } else if state.agents_status_hovered {
        Some("left click to open subagents menu")
    } else if state.model_status_hovered {
        Some("left click change model")
    } else if state.effort_status_hovered {
        Some("left click change effort")
    } else if state.permission_status_hovered {
        Some("left click change permissions")
    } else {
        None
    }
}

fn permission_mode_label(mode: PermissionMode) -> &'static str {
    match mode {
        PermissionMode::FullAccess => "full access",
        PermissionMode::Auto => "auto approvals",
        PermissionMode::Manual => "manual approvals",
    }
}

/// Name both goal actions while the pointer is already on the segment, so
/// clearing does not require slash-command knowledge.
fn goal_tooltip_title(goal: &SessionGoal) -> String {
    let left_action = if goal_toggle_command(goal).is_some() {
        "toggle"
    } else {
        "manage"
    };
    format!(" Goal · left {left_action} · right clear ")
}

/// The slash command that flips a goal's run state, or `None` where there is
/// nothing to flip: a completed goal is finished, and a budget-limited one
/// needs a new budget rather than a resume.
fn goal_toggle_command(goal: &SessionGoal) -> Option<&'static str> {
    match goal_toggle_action(goal)? {
        GoalAction::Pause => Some("/goal pause"),
        GoalAction::Resume => Some("/goal resume"),
        GoalAction::Set { .. } | GoalAction::Clear => None,
    }
}

fn goal_toggle_action(goal: &SessionGoal) -> Option<GoalAction> {
    match goal.status {
        GoalStatus::Active => Some(GoalAction::Pause),
        GoalStatus::Paused | GoalStatus::Blocked | GoalStatus::UsageLimited => {
            Some(GoalAction::Resume)
        }
        GoalStatus::BudgetLimited | GoalStatus::Complete => None,
    }
}

fn goal_picker_options(goal: &SessionGoal) -> Vec<PickerOption> {
    let mut options = Vec::with_capacity(3);
    if let Some(toggle) = goal_toggle_command(goal) {
        let toggle_label = if toggle == "/goal pause" {
            "Pause automatic continuation"
        } else {
            "Resume automatic continuation"
        };
        options.push(PickerOption::new(toggle_label, toggle));
    }
    options.push(PickerOption::new("Clear goal", GOAL_CLEAR_COMMAND));
    options.push(PickerOption::new("Cancel", "cancel"));
    options
}

fn push_interactive_status_segment(
    spans: &mut Vec<Span<'static>>,
    value: Option<String>,
    hovered: bool,
    resting_color: Color,
) {
    if let Some(value) = value {
        // The dot separator belongs to the status line, not to the segment, so
        // it keeps its resting style while the value underlines on hover.
        spans.push(Span::styled(
            STATUS_SEPARATOR,
            Style::default().fg(Color::Gray),
        ));
        spans.push(Span::styled(
            value,
            Style::default()
                .fg(if hovered { Color::White } else { resting_color })
                .add_modifier(if hovered {
                    Modifier::BOLD | Modifier::UNDERLINED
                } else {
                    Modifier::empty()
                }),
        ));
    }
}

fn effort_status_color(effort: &str) -> Color {
    match effort.to_ascii_lowercase().as_str() {
        "low" => Color::LightGreen,
        "medium" => Color::Cyan,
        "high" => Color::Yellow,
        "xhigh" => Color::LightMagenta,
        "max" | "ultra" => Color::LightRed,
        _ => Color::Gray,
    }
}

fn permission_status_color(permission: &str) -> Color {
    match permission {
        "manual approvals" => Color::LightGreen,
        "auto approvals" => Color::Yellow,
        "full access" => Color::LightRed,
        _ => Color::Gray,
    }
}

fn terminal_title(status: SessionStatus, first_prompt: Option<&str>) -> String {
    let prompt = first_prompt
        .map(|prompt| prompt.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|prompt| !prompt.is_empty())
        .map(|prompt| prompt.chars().take(48).collect::<String>());
    let prefix = if matches!(status, SessionStatus::Starting | SessionStatus::Running) {
        format!("{} Borg Agent", activity_glyph(status))
    } else {
        "Borg Agent".to_string()
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

fn replace_tool_activity_glyph(line: &mut Line<'static>, glyph: &str) {
    let Some(span) = line
        .spans
        .iter_mut()
        .find(|span| span.content.contains('◇'))
    else {
        return;
    };
    let start = span
        .content
        .find('◇')
        .expect("glyph-containing span was selected");
    let mut content = span.content.to_string();
    content.replace_range(start..start + '◇'.len_utf8(), glyph);
    span.content = Cow::Owned(content);
}

const RUNNING_PULSE_RADIUS: usize = 2;
const RUNNING_PULSE_STEP_MILLIS: u128 = 80;
const RUNNING_PULSE_PAUSE_STEPS: usize = 32;

fn running_activity_pulse_phase() -> usize {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            (elapsed.as_millis() / RUNNING_PULSE_STEP_MILLIS) as usize
        })
}

fn apply_running_activity_pulse(line: &mut Line<'static>, phase: usize) {
    if line.spans.len() < 2 {
        return;
    }
    let content_width = line
        .spans
        .iter()
        .skip(1)
        .map(|span| UnicodeWidthStr::width(span.content.as_ref()))
        .sum::<usize>();
    if content_width == 0 {
        return;
    }

    let sweep_width = content_width.saturating_add(RUNNING_PULSE_RADIUS * 2);
    let cycle = sweep_width.saturating_add(RUNNING_PULSE_PAUSE_STEPS);
    let phase = phase % cycle;
    if phase >= sweep_width {
        return;
    }
    let pulse_center = phase as isize - RUNNING_PULSE_RADIUS as isize;
    let mut offset = 0usize;
    let mut spans = Vec::with_capacity(line.spans.len() + 2);
    spans.push(line.spans[0].clone());
    for span in line.spans.iter().skip(1) {
        for grapheme in span.content.graphemes(true) {
            let distance = (offset as isize - pulse_center).unsigned_abs();
            let style = match distance {
                0 => brighten_style(span.style, 2),
                1 => brighten_style(span.style, 1),
                _ => span.style,
            };
            append_styled_grapheme(&mut spans, grapheme, style);
            offset = offset.saturating_add(UnicodeWidthStr::width(grapheme));
        }
    }
    line.spans = spans;
}

fn append_styled_grapheme(spans: &mut Vec<Span<'static>>, grapheme: &str, style: Style) {
    if spans.len() > 1
        && let Some(previous) = spans.last_mut()
        && previous.style == style
    {
        let mut content = previous.content.to_string();
        content.push_str(grapheme);
        previous.content = Cow::Owned(content);
    } else {
        spans.push(Span::styled(grapheme.to_string(), style));
    }
}

fn brighten_style(style: Style, steps: u8) -> Style {
    style.fg(brighten_color(style.fg.unwrap_or(Color::White), steps))
}

fn brighten_color(color: Color, steps: u8) -> Color {
    match color {
        Color::Rgb(red, green, blue) => {
            let lift = u16::from(steps) * 48;
            let lift_channel =
                |channel: u8| channel.saturating_add(lift.min(u16::from(u8::MAX)) as u8);
            Color::Rgb(lift_channel(red), lift_channel(green), lift_channel(blue))
        }
        Color::DarkGray => Color::Gray,
        Color::Gray => Color::White,
        Color::White => Color::Gray,
        Color::Red => Color::LightRed,
        Color::Green => Color::LightGreen,
        Color::Yellow => Color::LightYellow,
        Color::Blue => Color::LightBlue,
        Color::Magenta => Color::LightMagenta,
        Color::Cyan => Color::LightCyan,
        Color::Black | Color::Reset | Color::Indexed(_) => Color::White,
        light => light,
    }
}

fn history_loading_line() -> Line<'static> {
    Line::from(vec![
        Span::styled(
            format!("{} ", activity_glyph(SessionStatus::Running)),
            Style::default().fg(BORG_ORANGE),
        ),
        Span::styled("Loading thread history…", Style::default().fg(Color::Gray)),
    ])
}

fn agents_status_label(active_agents: usize) -> Option<String> {
    (active_agents > 0).then(|| {
        format!(
            "{active_agents} subagent{}",
            if active_agents == 1 { "" } else { "s" }
        )
    })
}

fn subagent_is_working(status: SubagentStatus) -> bool {
    matches!(
        status,
        SubagentStatus::Starting | SubagentStatus::Running | SubagentStatus::WaitingForApproval
    )
}

fn agents_status_spinner_style(hovered: bool) -> Style {
    Style::default()
        .fg(if hovered { Color::White } else { SUBAGENT_PINK })
        .add_modifier(Modifier::BOLD)
}

fn agents_status_text_style(hovered: bool) -> Style {
    Style::default()
        .fg(if hovered { Color::White } else { SUBAGENT_PINK })
        .add_modifier(if hovered {
            Modifier::BOLD | Modifier::UNDERLINED
        } else {
            Modifier::empty()
        })
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
