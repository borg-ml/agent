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
use std::io::{self, Stdout, Write as _};
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

use ratatui_image::{
    Resize,
    picker::{Picker as ImagePicker, ProtocolType, cap_parser::QueryStdioOptions},
    sliced::{SignedPosition, SlicedImage, SlicedProtocol},
};
use std::process::Command;
use std::sync::{Arc, OnceLock, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use attachments::{AttachmentStore, PasteOutcome};
use borg_remote::{
    ApprovalDecision, CodingProvider, EventActor, GoalAction, GoalStatus, MessageStatus,
    PermissionMode, PlanItem, PlanItemStatus, PromptDelivery, ResponseLanguage, SessionEvent,
    SessionEventKind, SessionGoal, SessionPayloadKind, SessionPayloadRef, SessionState,
    SessionStatus, SubagentActivityKind, SubagentSnapshot, SubagentStatus,
    ToolPresentationCategory, WatchSummary, command_edit_presentation, compact_text,
    edit_is_awaiting_diff, is_diff_language, is_edit_tool, is_mcp_resource_probe, is_subagent_tool,
    project_tool_presentation, tool_action_is_instant, tool_can_start_background_process,
    tool_has_rich_ui, tool_output_background_handle, tool_output_code_view,
    tool_process_followup_handle, tool_process_output_text, web_search_query,
};
#[cfg(test)]
use borg_remote::{tool_call_summary, tool_code_view};
use borg_ui::localization::{UiLanguage, text as ui_text};
use borg_ui::preferences::{
    CompletionAlertPolicy, DictationIconStyle, DiffExpansionPolicy, ToolClickBehavior,
    TranscriptPreferences, parse_hex_color,
};
use borg_ui::timeline::tool_lifecycle_label;
use chrono::{DateTime, Local, NaiveDate, Utc};
use crossterm::cursor::SetCursorStyle;
use crossterm::event::{
    DisableBracketedPaste, DisableFocusChange, DisableMouseCapture, EnableBracketedPaste,
    EnableFocusChange, EnableMouseCapture, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers,
    KeyboardEnhancementFlags, MouseButton, MouseEvent, MouseEventKind, PopKeyboardEnhancementFlags,
    PushKeyboardEnhancementFlags,
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
use regex::Regex;
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;
use uuid::Uuid;

use self::cache_diagnostics::{CacheDiagnostics, CacheSignature, CacheStatus, CacheUsage};
use self::markdown::{
    markdown_lines, markdown_link_ranges, markdown_plain_text, open_link, truncate_table_cell,
};
use self::terminal_input::TerminalInput;
pub use self::terminal_input::TerminalInputEvent;
use borg_ui::KeybindingConfig;

const INLINE_VIEWPORT_HEIGHT: u16 = 24;
const HORIZONTAL_MARGIN: u16 = 0;
const BORG_ORANGE: Color = Color::Rgb(255, 142, 36);
const BORG_ORANGE_HOVER: Color = Color::Rgb(255, 184, 92);
const RUNNING_STATUS_PEACH: Color = Color::Rgb(255, 132, 112);
const SUBAGENT_PINK: Color = Color::Rgb(255, 105, 180);
const USER_LABEL_BLUE: Color = Color::Rgb(74, 163, 255);
const USER_TEXT: Color = Color::Rgb(198, 228, 255);
const BACKGROUND_RUNNING_TEXT: Color = Color::Rgb(142, 199, 255);
const MESSAGE_BG: Color = Color::Rgb(33, 25, 29);
const MESSAGE_HOVER_BG: Color = Color::Rgb(48, 36, 41);
const MESSAGE_HORIZONTAL_PADDING: usize = 2;
const PARALLEL_MARKDOWN_RENDER_MIN_MESSAGES: usize = 512;
const MAX_PARALLEL_MARKDOWN_RENDER_WORKERS: usize = 16;
const COMMAND_PANEL_BG: Color = Color::Rgb(31, 24, 27);
const COMPOSER_BG: Color = Color::Rgb(42, 32, 37);
const COMPOSER_INPUT_BG: Color = Color::Rgb(31, 24, 27);
/// Divider between status-line segments. It is its own span so a hovered
/// segment underlines its own text only.
const STATUS_SEPARATOR: &str = " · ";
const GIT_STATUS_REFRESH_INTERVAL: Duration = Duration::from_secs(2);
const GOAL_CLEAR_COMMAND: &str = "/goal clear";
const DIRECTOR_CONTEXT_BOUNDARY: &str = "— context provided by director agent —";
const CTRL_C_SEQUENCE_WINDOW: Duration = Duration::from_secs(1);
const COPY_NOTICE_DURATION: Duration = Duration::from_secs(5);
const SPLASH_ANIMATION_DURATION: Duration = Duration::from_millis(1_500);
const WHEEL_SCROLL_VIEWPORT_DIVISOR: usize = 6;
const MIN_WHEEL_SCROLL_LINES_PER_EVENT: usize = 1;
const MAX_WHEEL_SCROLL_LINES_PER_EVENT: usize = 12;
const MAX_WHEEL_SCROLL_LINES_PER_FRAME: isize = 8;
const WHEEL_SCROLL_EASING_DIVISOR: usize = 8;
const NESTED_WHEEL_SCROLL_FULL_HEIGHT_ROWS: usize = 72;
const MAX_PENDING_WHEEL_SCROLL_LINES: isize = 160;
const TOOL_RUN_BOX_THRESHOLD: usize = 8;
const MAX_COLLAPSED_PLAN_ITEMS: usize = 5;
/// How many still-open steps a collapsed plan card shows under its change log.
/// The change says what just happened; on its own it does not say what is
/// left, so a plan with seven open steps read as one crossed-off line.
const MAX_COLLAPSED_PLAN_OPEN_ITEMS: usize = 3;
#[cfg(test)]
const LARGE_PASTE_CHAR_THRESHOLD: usize = 1000;

#[cfg(test)]
const DEFAULT_TOOL_RUN_VIEWPORT_HEIGHT: usize = 8;
const MIN_TOOL_RUN_VIEWPORT_HEIGHT: usize = 6;
const MAX_TOOL_RUN_VIEWPORT_HEIGHT: usize = 30;
const TOOL_RUN_CHROME_HEIGHT: usize = 2;
const MIN_SCROLLBAR_THUMB_ROWS: u16 = 6;
const TRANSCRIPT_SCROLLBAR_GUTTER_WIDTH: u16 = 2;
const DICTATION_BUTTON_WIDTH: u16 = 6;
const DICTATION_EMOJI_ICON: &str = "🎤";
const DICTATION_NERD_FONT_ICON: &str = "󰍬";

/// Selectable managed dictation models (value id, human label). Values mirror
/// `borg_dictation::DictationModelId::id`; the CLI maps them back.
const DICTATION_MODEL_OPTIONS: &[(&str, &str)] = &[
    (
        "parakeet-v2",
        "Parakeet V2 · balanced, recommended (~609 MB)",
    ),
    (
        "parakeet-v3",
        "Parakeet V3 · newest, most accurate (~644 MB)",
    ),
    ("lightweight", "Lightweight · faster CTC decoding (~582 MB)"),
];

/// Selectable dictation accelerators (value id, human label). Values mirror
/// `borg_dictation::DictationAccelerator::id`; only shown where a GPU runtime
/// exists for the platform.
const DICTATION_ACCELERATOR_OPTIONS: &[(&str, &str)] = &[
    ("auto", "Automatic · CPU on this platform"),
    ("nvidia", "NVIDIA GPU · CUDA 12 (~891 MB)"),
    ("vulkan", "GPU · Vulkan, cross-vendor (~35 MB)"),
];
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
    Vec<(usize, Option<String>)>,
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
    Find,
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
    find: Vec<KeyChord>,
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
            find: parse_key_chords(&config.find)?,
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
            KeyAction::Find => &self.find,
            KeyAction::ScrollUp => &self.scroll_up,
            KeyAction::ScrollDown => &self.scroll_down,
            KeyAction::SelectPrevious => &self.select_previous,
            KeyAction::SelectNext => &self.select_next,
            KeyAction::Approve => &self.approve,
            KeyAction::Deny => &self.deny,
        }
    }

    fn matches(&self, action: KeyAction, key: &KeyEvent) -> bool {
        let modifiers = key.modifiers
            & (KeyModifiers::CONTROL
                | KeyModifiers::ALT
                | KeyModifiers::SHIFT
                | KeyModifiers::SUPER);
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
            "cmd" | "command" | "super" => modifiers.insert(KeyModifiers::SUPER),
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
    actor: EventActor,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ComposerNavigation {
    WordLeft,
    WordRight,
    LineStart,
    LineEnd,
    LineUp,
    LineDown,
    DocumentStart,
    DocumentEnd,
}

fn composer_navigation(key: &KeyEvent) -> Option<(ComposerNavigation, bool)> {
    let modifiers = key.modifiers;
    let command = modifiers.intersects(KeyModifiers::SUPER | KeyModifiers::META);
    let word = modifiers.intersects(KeyModifiers::ALT | KeyModifiers::CONTROL);
    let navigation = match key.code {
        KeyCode::Left if command => ComposerNavigation::LineStart,
        KeyCode::Right if command => ComposerNavigation::LineEnd,
        KeyCode::Up if command => ComposerNavigation::DocumentStart,
        KeyCode::Down if command => ComposerNavigation::DocumentEnd,
        KeyCode::Left if word => ComposerNavigation::WordLeft,
        KeyCode::Right if word => ComposerNavigation::WordRight,
        KeyCode::Up if word => ComposerNavigation::LineUp,
        KeyCode::Down if word => ComposerNavigation::LineDown,
        KeyCode::Home if modifiers.contains(KeyModifiers::CONTROL) => {
            ComposerNavigation::DocumentStart
        }
        KeyCode::End if modifiers.contains(KeyModifiers::CONTROL) => {
            ComposerNavigation::DocumentEnd
        }
        KeyCode::Home => ComposerNavigation::LineStart,
        KeyCode::End => ComposerNavigation::LineEnd,
        KeyCode::Char('b') if modifiers.contains(KeyModifiers::ALT) => ComposerNavigation::WordLeft,
        KeyCode::Char('f') if modifiers.contains(KeyModifiers::ALT) => {
            ComposerNavigation::WordRight
        }
        KeyCode::Char('a') if modifiers.contains(KeyModifiers::CONTROL) => {
            ComposerNavigation::LineStart
        }
        KeyCode::Char('e') if modifiers.contains(KeyModifiers::CONTROL) => {
            ComposerNavigation::LineEnd
        }
        _ => return None,
    };
    Some((navigation, key.modifiers.contains(KeyModifiers::SHIFT)))
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

#[derive(Clone, Debug)]
struct ThreadFindState {
    pattern: String,
    row: usize,
}

pub const PREVENT_SLEEP_LID: &str = "On, even with the lid closed";
pub const PREVENT_SLEEP_IDLE: &str = "On, idle sleep only";
pub const PREVENT_SLEEP_OFF: &str = "Off";
const LID_AUTH_AUTHORIZE: &str = "Authorize once (Touch ID or password)";
const LID_AUTH_NOT_NOW: &str = "Not now";
const LID_AUTH_NEVER: &str = "Never ask again";

const ACTIVE_MESSAGES_SEND_NOW: &str = "Send now and redirect the current turn";
const ACTIVE_MESSAGES_WAIT: &str = "Wait and send after the current turn finishes";

const SLASH_COMMANDS: &[(&str, &str)] = &[
    ("/help", "open commands and keybindings"),
    ("/copy", "copy the last assistant message"),
    ("/find", "find regex in this thread"),
    (
        "/ask",
        "ask another model through its persistent peer thread",
    ),
    ("/director", "send text to the persistent director thread"),
    ("/claude", "ask the active model to consult its Claude peer"),
    ("/gpt", "ask the active model to consult its GPT peer"),
    ("/peer", "message, clear, or replace a persistent peer"),
    ("/settings", "view interactive session settings"),
    (
        "/customize",
        "inspect effective settings and extension authority",
    ),
    ("/model", "choose the model"),
    ("/effort", "choose reasoning effort"),
    ("/lsp", "view language server support"),
    ("/extensions", "view the live Blu extension runtime"),
    ("/usage", "view account limits and session usage"),
    ("/status", "alias for /usage"),
    ("/clear", "clear conversation context"),
    ("/compact", "compact the current conversation context"),
    ("/resume", "resume a saved Borg session"),
    ("/import", "copy threads and memory from another assistant"),
    ("/goal", "view or update the durable goal"),
    ("/todo", "view or update the durable todo list"),
    ("/todos", "alias for /todo"),
    ("/dictate", "start or stop local dictation"),
    ("/queue", "send after the current turn finishes"),
    ("/steer", "send now and redirect the current turn"),
    ("/team", "message every agent in the team"),
    (
        "/broadcast",
        "message every Borg instance running on this machine",
    ),
    ("/interrupt", "interrupt the current turn"),
    ("/stop", "alias for /interrupt"),
    ("/login", "switch ChatGPT / API key billing"),
    ("/connect", "manage provider connections"),
    ("/remote", "connect this machine to your Borg account"),
    ("/collab", "share this live session"),
    ("/quit", "close this view; active work continues"),
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

fn subagent_messages_visible_from_environment() -> bool {
    std::env::var("BORG_TUI_SHOW_SUBAGENT_MESSAGES")
        .ok()
        .is_some_and(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
}

fn root_transcript_from_environment() -> Transcript {
    Transcript {
        show_subagent_messages: subagent_messages_visible_from_environment(),
        ..Transcript::default()
    }
}

fn new_child_transcript() -> Transcript {
    let mut transcript = Transcript {
        show_subagent_messages: true,
        ..Transcript::default()
    };
    transcript.show_director_context_boundary();
    transcript
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

pub fn dictation_icon_style_for_preference(
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

pub struct TerminalIoRequest {
    kind: TerminalIoRequestKind,
}

enum TerminalIoRequestKind {
    Copy {
        text: String,
        notice: String,
    },
    Paste {
        store: AttachmentStore,
        value: String,
        cwd: PathBuf,
    },
    CaptureClipboard {
        store: AttachmentStore,
        cwd: PathBuf,
    },
    OpenLink {
        url: String,
    },
}

impl std::fmt::Debug for TerminalIoRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("TerminalIoRequest")
    }
}

pub struct TerminalIoCompletion {
    kind: TerminalIoCompletionKind,
}

enum TerminalIoCompletionKind {
    Copied(std::result::Result<String, String>),
    Pasted(std::result::Result<PasteOutcome, String>),
    LinkOpened(std::result::Result<(), String>),
}

#[derive(Clone)]
pub struct TerminalIoSender(std::sync::mpsc::Sender<TerminalIoRequest>);

impl TerminalIoSender {
    pub fn send(&self, request: TerminalIoRequest) -> bool {
        self.0.send(request).is_ok()
    }
}

pub fn spawn_terminal_io_worker() -> (
    TerminalIoSender,
    tokio::sync::mpsc::UnboundedReceiver<TerminalIoCompletion>,
) {
    let (requests_tx, requests_rx) = std::sync::mpsc::channel::<TerminalIoRequest>();
    let (completions_tx, completions_rx) = tokio::sync::mpsc::unbounded_channel();
    thread::spawn(move || {
        let mut clipboard_lease = None;
        while let Ok(request) = requests_rx.recv() {
            let kind = match request.kind {
                TerminalIoRequestKind::Copy { text, notice } => {
                    let result = clipboard::copy(&text).map(|lease| {
                        clipboard_lease = lease;
                        notice
                    });
                    TerminalIoCompletionKind::Copied(result)
                }
                TerminalIoRequestKind::Paste { store, value, cwd } => {
                    TerminalIoCompletionKind::Pasted(
                        store
                            .stage_paste(&value, &cwd)
                            .map_err(|error| format!("{error:#}")),
                    )
                }
                TerminalIoRequestKind::CaptureClipboard { store, cwd } => {
                    TerminalIoCompletionKind::Pasted(
                        store
                            .capture_clipboard_paste(&cwd)
                            .map_err(|error| format!("{error:#}")),
                    )
                }
                TerminalIoRequestKind::OpenLink { url } => TerminalIoCompletionKind::LinkOpened(
                    open_link(&url).map_err(|error| format!("{error:#}")),
                ),
            };
            if completions_tx.send(TerminalIoCompletion { kind }).is_err() {
                break;
            }
        }
    });
    (TerminalIoSender(requests_tx), completions_rx)
}

impl TerminalIoRequest {
    fn copy(text: String, notice: impl Into<String>) -> Self {
        Self {
            kind: TerminalIoRequestKind::Copy {
                text,
                notice: notice.into(),
            },
        }
    }

    fn paste(store: AttachmentStore, value: String, cwd: PathBuf) -> Self {
        Self {
            kind: TerminalIoRequestKind::Paste { store, value, cwd },
        }
    }

    fn capture_clipboard(store: AttachmentStore, cwd: PathBuf) -> Self {
        Self {
            kind: TerminalIoRequestKind::CaptureClipboard { store, cwd },
        }
    }

    fn open_link(url: String) -> Self {
        Self {
            kind: TerminalIoRequestKind::OpenLink { url },
        }
    }
}

/// Outcome of the one-time "keep awake with the lid closed" admin prompt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LidSleepAuthorizationChoice {
    Authorize,
    NotNow,
    Never,
}

#[derive(Debug)]
pub enum UiAction {
    None,
    /// Stop a watch from the watches panel.
    StopWatch(Uuid),
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
    /// `/team <message>`: queue one message to every non-terminal agent in the
    /// team (subagents plus root). Always addressed at the director session.
    Broadcast {
        text: String,
    },
    /// `/broadcast <message>`: deliver one message to every Borg instance
    /// running on this machine, beyond this session's own team.
    BroadcastInstances {
        text: String,
    },
    Approve {
        target: Option<Uuid>,
        decision: ApprovalDecision,
    },
    RecallQueuedPrompts {
        target: Option<Uuid>,
    },
    FlushPendingInput {
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
    SetUiLanguage(UiLanguage),
    SetFast(bool),
    SetRefreshRate(u64),
    SetPreventSleep {
        enabled: bool,
        lid: bool,
    },
    LidSleepAuthorization(LidSleepAuthorizationChoice),
    SetSteerActive(bool),
    SetDiffExpansion(DiffExpansionPolicy),
    SetAutoExpandTools(bool),
    SetAutoExpandThinking(bool),
    SetToolClickBehavior(ToolClickBehavior),
    SetActionDescriptors(bool),
    SetRunningSweeps(bool),
    SetCompletionNotifications(CompletionAlertPolicy),
    SetCompletionSound(CompletionAlertPolicy),
    SetAutoCopySelection(bool),
    SetLunaTitlesForAllProviders(bool),
    SetDictationIcon(DictationIconStyle),
    /// Completes the enable-dictation flow: persist model/accelerator/icon,
    /// mark dictation enabled, and begin recording (which prompts the OS for
    /// microphone access).
    EnableDictation {
        model: String,
        accelerator: String,
        icon: DictationIconStyle,
    },
    ToggleDictation,
    TerminalIo(TerminalIoRequest),
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
pub enum DictationState {
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
        let mut segments = vec![format!(
            "{}{}",
            self.branch,
            if self.dirty { "*" } else { "" }
        )];
        if self.ahead > 0 {
            segments.push(format!("↑{}", self.ahead));
        }
        if self.behind > 0 {
            segments.push(format!("↓{}", self.behind));
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

/// A remote-sync command the footer's ahead/behind indicators run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum GitRemoteAction {
    Push,
    Pull,
}

impl GitRemoteAction {
    fn run(self, cwd: &Path) -> std::result::Result<String, String> {
        match self {
            Self::Push => run_git_push(cwd),
            Self::Pull => run_git_pull(cwd),
        }
    }

    /// Lowercase command name, used in failure notices.
    fn verb(self) -> &'static str {
        match self {
            Self::Push => "push",
            Self::Pull => "pull",
        }
    }

    /// Past-tense label for a successful sync.
    fn done(self) -> &'static str {
        match self {
            Self::Push => "Pushed",
            Self::Pull => "Pulled",
        }
    }

    /// Progress notice shown while the command runs.
    fn in_progress(self) -> &'static str {
        match self {
            Self::Push => "Pushing to upstream…",
            Self::Pull => "Pulling from upstream…",
        }
    }
}

/// Background `git push`/`git pull` runner for the footer's ahead/behind
/// indicators. Mirrors [`GitStatusCache`]: the remote command runs off the UI
/// thread and its result is drained on a later tick so a slow network sync
/// never blocks rendering. Push and pull are tracked separately per directory
/// so each indicator shows its own progress.
struct GitRemoteState {
    in_flight: HashSet<(PathBuf, GitRemoteAction)>,
    sender: mpsc::Sender<(
        PathBuf,
        GitRemoteAction,
        std::result::Result<String, String>,
    )>,
    receiver: mpsc::Receiver<(
        PathBuf,
        GitRemoteAction,
        std::result::Result<String, String>,
    )>,
}

impl Default for GitRemoteState {
    fn default() -> Self {
        let (sender, receiver) = mpsc::channel();
        Self {
            in_flight: HashSet::new(),
            sender,
            receiver,
        }
    }
}

impl GitRemoteState {
    fn is_running(&self, cwd: &Path, action: GitRemoteAction) -> bool {
        self.in_flight.contains(&(cwd.to_path_buf(), action))
    }

    /// Spawn `action` for `cwd` unless the same action is already running
    /// there. Returns true when a new command started.
    fn start(&mut self, cwd: &Path, action: GitRemoteAction) -> bool {
        if !self.in_flight.insert((cwd.to_path_buf(), action)) {
            return false;
        }
        let cwd = cwd.to_path_buf();
        let sender = self.sender.clone();
        thread::spawn(move || {
            let outcome = action.run(&cwd);
            let _ = sender.send((cwd, action, outcome));
        });
        true
    }

    fn drain(
        &mut self,
    ) -> Vec<(
        PathBuf,
        GitRemoteAction,
        std::result::Result<String, String>,
    )> {
        let mut finished = Vec::new();
        while let Ok(result) = self.receiver.try_recv() {
            self.in_flight.remove(&(result.0.clone(), result.1));
            finished.push(result);
        }
        finished
    }
}

fn run_git_push(cwd: &Path) -> std::result::Result<String, String> {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(cwd)
        .arg("push")
        .output()
        .map_err(|error| format!("could not run git: {error}"))?;
    if output.status.success() {
        // git writes push progress to stderr; the last non-empty line is the
        // useful summary ("main -> main" or "Everything up-to-date").
        let summary = String::from_utf8_lossy(&output.stderr)
            .lines()
            .rev()
            .map(str::trim)
            .find(|line| !line.is_empty())
            .unwrap_or("Pushed")
            .to_string();
        Ok(summary)
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(stderr
            .lines()
            .rev()
            .map(str::trim)
            .find(|line| !line.is_empty())
            .unwrap_or("git push failed")
            .to_string())
    }
}

fn run_git_pull(cwd: &Path) -> std::result::Result<String, String> {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(cwd)
        .arg("pull")
        .output()
        .map_err(|error| format!("could not run git: {error}"))?;
    if output.status.success() {
        // git writes the merge/rebase summary to stdout; the last non-empty
        // line is the useful one ("Already up to date." or a fast-forward).
        let summary = String::from_utf8_lossy(&output.stdout)
            .lines()
            .rev()
            .map(str::trim)
            .find(|line| !line.is_empty())
            .unwrap_or("Pulled")
            .to_string();
        Ok(summary)
    } else {
        Err(git_last_stderr_line(&output))
    }
}

/// Screen rectangle of the `↓M` token inside a right-aligned footer metadata
/// line, or `None` when there is nothing to pull. Behind is the right-most
/// metadata token, so its box ends at the metadata's right edge. Right
/// alignment clips overflow on the left, so the suffix stays intact even when
/// the path is truncated.
fn git_behind_hit_area(status: &GitWorktreeStatus, metadata: Rect) -> Option<Rect> {
    if status.behind == 0 {
        return None;
    }
    let behind_token = format!("↓{}", status.behind);
    let behind_width = behind_token.width() as u16;
    let x = metadata.right().saturating_sub(behind_width);
    if x < metadata.x {
        return None;
    }
    Some(Rect {
        x,
        y: metadata.y,
        width: behind_width,
        height: 1,
    })
}

/// Screen rectangle of the `↑N` token inside a right-aligned footer metadata
/// line, or `None` when there is nothing to push. The token sits before an
/// optional `↓M` behind-count, so its right edge is the metadata's right edge
/// minus the width of whatever trails it. Right alignment clips overflow on the
/// left, so the git suffix is always intact even when the path is truncated.
fn git_ahead_hit_area(status: &GitWorktreeStatus, metadata: Rect) -> Option<Rect> {
    if status.ahead == 0 {
        return None;
    }
    let ahead_token = format!("↑{}", status.ahead);
    let ahead_width = ahead_token.width() as u16;
    let trailing_width = if status.behind > 0 {
        format!("{STATUS_SEPARATOR}↓{}", status.behind).width() as u16
    } else {
        0
    };
    let right = metadata.right().saturating_sub(trailing_width);
    let x = right.saturating_sub(ahead_width);
    if x < metadata.x {
        return None;
    }
    Some(Rect {
        x,
        y: metadata.y,
        width: ahead_width,
        height: 1,
    })
}

/// Background "commit everything and push" runner for the footer's dirty
/// worktree indicator. The commit message is drafted by a cheap model (see
/// [`commit_model_from_environment`]); when that model is unavailable the
/// commit still happens with a deterministic summary so a click never fails
/// just because a provider is out of quota.
struct GitCommitState {
    in_flight: HashSet<PathBuf>,
    sender: mpsc::Sender<(PathBuf, std::result::Result<String, String>)>,
    receiver: mpsc::Receiver<(PathBuf, std::result::Result<String, String>)>,
}

impl Default for GitCommitState {
    fn default() -> Self {
        let (sender, receiver) = mpsc::channel();
        Self {
            in_flight: HashSet::new(),
            sender,
            receiver,
        }
    }
}

impl GitCommitState {
    fn is_committing(&self, cwd: &Path) -> bool {
        self.in_flight.contains(cwd)
    }

    /// Spawn stage → draft message → commit → push for `cwd` unless one is
    /// already running there. Returns true when a new run started.
    fn start(&mut self, cwd: &Path, model: CommitMessageModel) -> bool {
        if !self.in_flight.insert(cwd.to_path_buf()) {
            return false;
        }
        let cwd = cwd.to_path_buf();
        let sender = self.sender.clone();
        thread::spawn(move || {
            let outcome = run_git_commit_and_push(&cwd, &model);
            let _ = sender.send((cwd, outcome));
        });
        true
    }

    fn drain(&mut self) -> Vec<(PathBuf, std::result::Result<String, String>)> {
        let mut finished = Vec::new();
        while let Ok(result) = self.receiver.try_recv() {
            self.in_flight.remove(&result.0);
            finished.push(result);
        }
        finished
    }
}

/// Model used to draft click-to-commit messages, as `MODEL[@EFFORT]`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct CommitMessageModel {
    model: String,
    effort: String,
}

const DEFAULT_COMMIT_MESSAGE_MODEL: &str = "gpt-6-luna@low";
const COMMIT_DIFF_CHAR_BUDGET: usize = 60_000;

impl CommitMessageModel {
    fn parse(spec: &str) -> Self {
        let spec = spec.trim();
        let spec = if spec.is_empty() {
            DEFAULT_COMMIT_MESSAGE_MODEL
        } else {
            spec
        };
        let (model, effort) = spec
            .rsplit_once('@')
            .map_or((spec, "low"), |(model, effort)| (model, effort));
        Self {
            model: model.trim().to_string(),
            effort: effort.trim().to_string(),
        }
    }

    fn label(&self) -> String {
        format!("{}@{}", self.model, self.effort)
    }
}

/// `BORG_COMMIT_MODEL=MODEL[@EFFORT]` overrides the default cheap model.
fn commit_model_from_environment() -> CommitMessageModel {
    CommitMessageModel::parse(&std::env::var("BORG_COMMIT_MODEL").unwrap_or_default())
}

fn git_in(cwd: &Path, args: &[&str]) -> std::result::Result<std::process::Output, String> {
    Command::new("git")
        .arg("-C")
        .arg(cwd)
        .args(args)
        .output()
        .map_err(|error| format!("could not run git: {error}"))
}

fn git_last_stderr_line(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stderr)
        .lines()
        .rev()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("git failed")
        .to_string()
}

/// Stage every change, draft a message, commit, then push. Returns a short
/// human summary for the footer notice.
fn run_git_commit_and_push(
    cwd: &Path,
    model: &CommitMessageModel,
) -> std::result::Result<String, String> {
    let add = git_in(cwd, &["add", "-A"])?;
    if !add.status.success() {
        return Err(git_last_stderr_line(&add));
    }
    let stat = git_in(cwd, &["diff", "--cached", "--stat"])?;
    let stat = String::from_utf8_lossy(&stat.stdout).trim().to_string();
    if stat.is_empty() {
        return Err("nothing to commit".to_string());
    }
    let diff = git_in(cwd, &["diff", "--cached", "--no-color"])?;
    let diff = String::from_utf8_lossy(&diff.stdout);
    let diff = truncate_chars(&diff, COMMIT_DIFF_CHAR_BUDGET);
    let message = draft_commit_message(cwd, model, &stat, &diff)
        .unwrap_or_else(|| fallback_commit_message(&stat));
    let commit = git_in(cwd, &["commit", "-m", &message])?;
    if !commit.status.success() {
        return Err(git_last_stderr_line(&commit));
    }
    let short = git_in(cwd, &["rev-parse", "--short", "HEAD"])?;
    let short = String::from_utf8_lossy(&short.stdout).trim().to_string();
    let subject = message
        .lines()
        .next()
        .unwrap_or_default()
        .trim()
        .to_string();
    match run_git_push(cwd) {
        Ok(push) => Ok(format!("{short} {subject} · {push}")),
        Err(error) => Err(format!("committed {short} but push failed · {error}")),
    }
}

fn truncate_chars(text: &str, budget: usize) -> String {
    if text.chars().count() <= budget {
        return text.to_string();
    }
    let mut out: String = text.chars().take(budget).collect();
    out.push_str(
        "
… (diff truncated)
",
    );
    out
}

/// Deterministic message used when no drafting model is reachable.
fn fallback_commit_message(stat: &str) -> String {
    let files: Vec<&str> = stat
        .lines()
        .filter(|line| line.contains('|'))
        .filter_map(|line| line.split('|').next())
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .collect();
    let summary = stat.lines().last().unwrap_or("").trim();
    match files.as_slice() {
        [] => "Update working tree".to_string(),
        [one] => format!(
            "Update {one}

{summary}"
        ),
        [a, b] => format!(
            "Update {a} and {b}

{summary}"
        ),
        [a, b, rest @ ..] => format!(
            "Update {a}, {b} and {} more

{summary}",
            rest.len()
        ),
    }
}

fn commit_message_prompt(stat: &str, diff: &str) -> String {
    format!(
        "Write a git commit message for the staged changes below. Output ONLY the message:          an imperative subject line of at most 72 characters, then a blank line, then at most          four short bullet points explaining what changed and why. No code fences, no preamble.

         --- git diff --cached --stat ---
{stat}

--- git diff --cached ---
{diff}"
    )
}

/// Draft with native GPT model access or the unchanged Claude CLI route.
/// Returns None on failure so the caller can use its deterministic fallback.
fn draft_commit_message(
    cwd: &Path,
    model: &CommitMessageModel,
    stat: &str,
    diff: &str,
) -> Option<String> {
    use std::io::Write;
    let prompt = commit_message_prompt(stat, diff);
    let output = if model.model.starts_with("claude") {
        let mut child = Command::new("claude")
            .args([
                "-p",
                "--model",
                &model.model,
                "--effort",
                &model.effort,
                "--no-session-persistence",
                // The diff is in the prompt: no tools, and no MCP servers to start.
                "--tools",
                "",
                "--strict-mcp-config",
            ])
            .current_dir(cwd)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .ok()?;
        child.stdin.take()?.write_all(prompt.as_bytes()).ok()?;
        child.wait_with_output().ok()?
    } else {
        #[cfg(not(feature = "subscription-adapters"))]
        return None;
        #[cfg(feature = "subscription-adapters")]
        {
            use borg_provider::provider::{CodexModelProvider, ModelMessage, ModelTurnRequest};
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .ok()?;
            return runtime.block_on(async {
                tokio::time::timeout(std::time::Duration::from_secs(90), async {
                    let account = CodexModelProvider::account_identity().await.ok()?;
                    let provider = CodexModelProvider {
                        model: model.model.clone(),
                        effort: model.effort.clone(),
                    };
                    let result = provider
                        .model_turn_for_account(
                            ModelTurnRequest {
                                fast: false,
                                request_id: Some(uuid::Uuid::new_v4().to_string()),
                                session_id: None,
                                prompt_cache_key: None,
                                turn_routing: Default::default(),
                                messages: vec![ModelMessage::user(prompt)],
                                tools: Vec::new(),
                                output_schema: None,
                            },
                            None,
                            &account,
                        )
                        .await
                        .ok()?;
                    let (content, _, calls) = result.assistant_parts()?;
                    if !calls.is_empty() {
                        return None;
                    }
                    clean_commit_message(content.as_deref()?)
                })
                .await
                .ok()
                .flatten()
            });
        }
    };
    if !output.status.success() {
        return None;
    }
    clean_commit_message(&String::from_utf8_lossy(&output.stdout))
}

/// Strip code fences and surrounding whitespace; reject empty drafts.
fn clean_commit_message(raw: &str) -> Option<String> {
    let mut lines: Vec<&str> = raw.lines().map(str::trim_end).collect();
    while lines
        .first()
        .is_some_and(|line| line.trim().is_empty() || line.trim_start().starts_with("```"))
    {
        lines.remove(0);
    }
    while lines
        .last()
        .is_some_and(|line| line.trim().is_empty() || line.trim_start().starts_with("```"))
    {
        lines.pop();
    }
    let message = lines.join("\n").trim().to_string();
    (!message.is_empty()).then_some(message)
}

/// Clickable area over the `branch*` token when the worktree is dirty. The
/// git label is the right-most metadata item, so the branch token starts at
/// `right - width(label)`.
fn git_commit_hit_area(status: &GitWorktreeStatus, metadata: Rect) -> Option<Rect> {
    if !status.dirty {
        return None;
    }
    let label_width = status.compact_label().width() as u16;
    let branch_width = format!("{}*", status.branch).width() as u16;
    let x = metadata.right().saturating_sub(label_width);
    if x < metadata.x || branch_width == 0 {
        return None;
    }
    Some(Rect {
        x,
        y: metadata.y,
        width: branch_width,
        height: 1,
    })
}

impl GitStatusCache {
    /// Drop the cached status for `cwd` so the next `status_for` re-reads it.
    /// Used after a push changes the ahead/behind counts.
    fn invalidate(&mut self, cwd: &Path) {
        self.values.remove(cwd);
    }

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
    keyboard_enhanced: bool,
    transcript: Transcript,
    director_transcript: Option<Box<Transcript>>,
    child_transcripts: HashMap<Uuid, Transcript>,
    child_unhydrated_events: HashMap<Uuid, Vec<SessionEvent>>,
    hydrated_children: HashSet<Uuid>,
    child_history_hydration_complete: bool,
    child_queued_prompts: HashMap<Uuid, Vec<PendingPromptProjection>>,
    child_requeue_cursors: HashMap<Uuid, Option<usize>>,
    child_statuses: HashMap<Uuid, SessionStatus>,
    child_activity_clocks: HashMap<Uuid, ActivityClock>,
    child_pending_approvals: HashSet<Uuid>,
    /// Startup recovery can publish child-state corrections after the root
    /// history has already been seeded. Keep those corrections in the roster
    /// and child projections, but do not turn them into root transcript cards.
    suppress_bootstrap_subagent_activity: bool,
    focused_child: Option<Uuid>,
    focused_tool: Option<usize>,
    tool_return_scroll_from_bottom: usize,
    tool_return_follow_tail: bool,
    sidecar_focus_request: Option<String>,
    team_switcher_open: bool,
    team_roster_hit_areas: Vec<(Rect, Option<Uuid>)>,
    hovered_team_roster: Option<usize>,
    back_to_director_area: Option<Rect>,
    back_to_director_hovered: bool,
    composer: Composer,
    attachment_store: AttachmentStore,
    keymap: KeyMap,
    ui_language: UiLanguage,
    cwd: PathBuf,
    configured_model_entries: Vec<borg_provider::DynamicModelEntry>,
    extension_commands: Vec<borg_remote::ExtensionApiCommand>,
    git_status_cache: GitStatusCache,
    git_push: GitRemoteState,
    git_commit: GitCommitState,
    git_status_area: Option<Rect>,
    git_status_hovered: bool,
    git_pull_area: Option<Rect>,
    git_commit_area: Option<Rect>,
    git_commit_hovered: bool,
    git_pull_hovered: bool,
    status: SessionStatus,
    interrupt_requested: bool,
    interrupt_requested_at: Option<Instant>,
    connection_retry_at: Option<DateTime<Utc>>,
    /// Which attempt of the bounded resend chain the countdown belongs to, as
    /// the runtime reported it. Shown in the status line so a session waiting on
    /// a retry never looks idle: it says how far along the chain it is.
    connection_retry_attempt: Option<(u64, u64)>,
    usage_retry_at: Option<DateTime<Utc>>,
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
    pending_input_header_area: Option<Rect>,
    pending_input_expanded: bool,
    keybindings_hint_area: Option<Rect>,
    keybindings_hovered: bool,
    dictation_button_area: Option<Rect>,
    dictation_button_hovered: bool,
    dictation_state: DictationState,
    dictation_icon: DictationIconStyle,
    running_sweeps: bool,
    action_descriptors: bool,
    tool_click_behavior: ToolClickBehavior,
    thread_find: Option<ThreadFindState>,
    completion_notifications: CompletionAlertPolicy,
    completion_sound: CompletionAlertPolicy,
    completion_alert_pending: bool,
    auto_copy_selection: bool,
    luna_titles_for_all_providers: bool,
    horizontal_margin: u16,
    composer_max_height: u16,
    show_footer: bool,
    window_focused: bool,
    tool_hit_areas: Vec<(Rect, usize)>,
    tool_run_hit_areas: Vec<(Rect, usize, usize)>,
    tool_run_header_hit_areas: Vec<(Rect, usize)>,
    entry_hit_areas: Vec<(Rect, usize)>,
    message_hit_areas: Vec<(Rect, usize)>,
    link_hit_areas: Vec<(Rect, String)>,
    /// Terminal graphics protocol detected at startup (Kitty/Sixel/iTerm2);
    /// `None` keeps the half-block fallback drawn by the transcript.
    image_picker: Option<ImagePicker>,
    image_scroll_settles_at: Option<Instant>,
    /// Encoded previews keyed by source path and tile size in cells.
    image_protocols: HashMap<(PathBuf, u16, u16), SlicedProtocol>,
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
    shell_status_area: Option<Rect>,
    shell_status_hovered: bool,
    shell_menu_open: bool,
    shell_row_hit_areas: Vec<(Rect, Option<usize>)>,
    hovered_shell_row: Option<usize>,
    watch_status_area: Option<Rect>,
    watch_status_hovered: bool,
    watch_menu_open: bool,
    watch_row_hit_areas: Vec<(Rect, Uuid)>,
    hovered_watch_row: Option<usize>,
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
    nested_scroll_motion: Option<NestedScrollMotion>,
    text_selection: Option<TextSelection>,
    composer_selection: Option<ComposerSelection>,
    pending_transcript_click: Option<PendingTranscriptClick>,
    pending_tool_copy: Option<usize>,
    activity_clock: ActivityClock,
    notice: Option<String>,
    copy_notice_expires_at: Option<Instant>,
    last_ctrl_c: Option<Instant>,
    ctrl_c_count: u8,
    queued_prompts: Vec<PendingPromptProjection>,
    /// An idle submission this surface already projected as a starting turn.
    ///
    /// The session journals such a prompt as `Message{status: Queued}` a beat
    /// before `TurnStarted`, so projecting that transient straight into the
    /// pending list made every post-turn submission visibly bounce
    /// Starting -> pending -> Running. The projection is withheld while this
    /// is set and only materialises if something other than the matching
    /// `TurnStarted` resolves it.
    optimistic_idle_prompt: Option<Uuid>,
    withheld_queued_prompt: Option<PendingPromptProjection>,
    /// Insertion point for prompts the session re-queues right after a failed
    /// turn; they precede everything queued while that turn ran.
    requeue_cursor: Option<usize>,
    active_turn_followup: bool,
    child_active_turn_followups: HashSet<Uuid>,
    replaying_history: bool,
    history_page_requested: bool,
    history_page_loading: bool,
    picker: Option<Picker>,
    /// Model the user picked from a provider that still needs credentials;
    /// applied once the auth picker resolves.
    pending_auth_model: Option<(CodingProvider, String)>,
    /// True once the enable-dictation flow has completed (mirrors the durable
    /// preference); gates whether the dictation key records or opens the flow.
    dictation_enabled: bool,
    /// Set while the model/accelerator/icon pickers are chained together as the
    /// enable-dictation flow, so the final icon choice emits one combined
    /// EnableDictation action instead of a bare icon change.
    dictation_enable_flow: bool,
    /// The durable model preference, used to preselect the enable-flow picker.
    dictation_model: Option<String>,
    pending_dictation_model: Option<String>,
    pending_dictation_accelerator: Option<String>,
    keybindings_open: bool,
    slash_selection: usize,
    rewind_targets: Vec<RewindTarget>,
    rewind_primed: bool,
    borging_this_run: bool,
    last_terminal_title: Option<String>,
    transcript_render_cache: Option<CachedTranscriptRender>,
    transcript_full_render_cache: Option<CachedTranscriptRender>,
    active_transcript_render: Option<Arc<TranscriptRender>>,
    last_committed_viewport_render: Option<CachedTranscriptRender>,
    last_reasoning_summary_phases: Vec<(usize, i64)>,
    rendered_transcript_height: usize,
    pending_scroll_anchor_height: Option<usize>,
    pending_transcript_anchor: Option<TranscriptViewportAnchor>,
    event_redraw_needed: bool,
    last_tool_timer_refresh_tick: Option<i64>,
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
    shell_status_hovered: bool,
    hovered_shell_row: Option<usize>,
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
            shell_status_hovered: self.shell_status_hovered,
            hovered_shell_row: self.hovered_shell_row,
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
    message_id: Uuid,
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
    key_hint: Option<String>,
    disabled: bool,
}

impl PickerOption {
    fn new(label: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            value: value.into(),
            preview: None,
            section: None,
            key_hint: None,
            disabled: false,
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
    ImportSource,
    ImportPreview { threads: bool, memory: bool },
    Settings,
    Resume,
    Model,
    Effort,
    Permission,
    Language,
    UiLanguage,
    Fast,
    RefreshRate,
    PreventSleep,
    LidSleepAuthorization,
    ActiveMessages,
    AutoExpandEdits,
    AutoExpandTools,
    AutoExpandThinking,
    ToolClickBehavior,
    ActionDescriptors,
    RunningSweeps,
    CompletionNotifications,
    CompletionSound,
    AutoCopySelection,
    LunaTitlesForAllProviders,
    DictationModel,
    DictationAccelerator,
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
    ReconnectSubscription,
    ReplaceApiKey,
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
                    || (self.kind != PickerKind::Model
                        && option
                            .preview
                            .as_deref()
                            .is_some_and(|preview| fuzzy_matches(preview, query)))
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
                if option.disabled {
                    Style::default().fg(Color::DarkGray)
                } else if selected {
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
            None if self.query.is_some() => format!("> {} · type to filter", self.title),
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
            let rows = self.displayed_option_rows();
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
                    .map(|(index, row, style)| self.styled_option_line(index, row, style))
                    .chain(empty.map(|(row, style)| Line::from(Span::styled(row, style)))),
            )
            .collect();
        }

        self.styled_resume_lines(width, preview_label_color, preview_message_color)
    }

    fn styled_option_line(&self, index: Option<usize>, row: String, style: Style) -> Line<'static> {
        let Some(key_hint) = index
            .filter(|_| matches!(self.kind, PickerKind::Commands))
            .and_then(|index| self.options[index].key_hint.as_deref())
        else {
            return Line::from(Span::styled(row, style));
        };
        let Some(action) = row.strip_suffix(key_hint) else {
            return Line::from(Span::styled(row, style));
        };
        Line::from(vec![
            Span::styled(action.to_string(), style.fg(Color::White)),
            Span::styled(
                key_hint.to_string(),
                style.fg(BORG_ORANGE_HOVER).add_modifier(Modifier::BOLD),
            ),
        ])
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

/// The items an update actually changed: added, reworded, or moved to a new
/// status, matched by durable item id. An update with nothing to compare
/// against — a session's first plan or a replayed one — and an update that
/// only removed items both return nothing, and the card falls back to the
/// ordinary ordered list.
fn changed_plan_items<'a>(items: &'a [PlanItem], previous: &[PlanItem]) -> Vec<&'a PlanItem> {
    if previous.is_empty() {
        return Vec::new();
    }
    ordered_plan_items(items)
        .into_iter()
        .filter(|item| {
            previous
                .iter()
                .find(|superseded| superseded.id == item.id)
                .is_none_or(|superseded| {
                    superseded.status != item.status || superseded.content != item.content
                })
        })
        .collect()
}

/// The rows a plan card shows and the number of plan items it leaves hidden.
///
/// A collapsed card is a change log for the update that produced it, so the
/// newest or newly finished step is always visible instead of buried under
/// unchanged leading steps. Expanding shows the whole plan.
fn plan_card_rows<'a>(
    items: &'a [PlanItem],
    previous: &[PlanItem],
    collapsed: bool,
) -> (Vec<&'a PlanItem>, usize) {
    let changed = changed_plan_items(items, previous);
    let mut rows = if collapsed && !changed.is_empty() {
        changed
    } else {
        ordered_plan_items(items)
    };
    if collapsed {
        rows.truncate(MAX_COLLAPSED_PLAN_ITEMS);
        // The changed row stays on top: it is why the card updated, and that
        // holds when the change is a step being completed. What it cannot say
        // is what remains, so follow it with the work that is still open.
        // Rows already shown keep their place rather than repeating.
        let shown = rows.iter().map(|item| item.id).collect::<Vec<_>>();
        let room = MAX_COLLAPSED_PLAN_ITEMS
            .saturating_sub(rows.len())
            .min(MAX_COLLAPSED_PLAN_OPEN_ITEMS);
        rows.extend(
            ordered_plan_items(items)
                .into_iter()
                .filter(|item| item.status != PlanItemStatus::Completed)
                .filter(|item| !shown.contains(&item.id))
                .take(room),
        );
    }
    let hidden = items.len().saturating_sub(rows.len());
    (rows, hidden)
}

fn removed_plan_items(items: &[PlanItem], previous: &[PlanItem]) -> usize {
    previous
        .iter()
        .filter(|superseded| !items.iter().any(|item| item.id == superseded.id))
        .count()
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
            let mut option = PickerOption::new(format!("{label} · {id}"), *id);
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
        Some(CodingProvider::Grok) => {
            options.push(PickerOption::new(
                borg_provider::grok_product_model(),
                borg_provider::grok_product_model(),
            ));
        }
        Some(CodingProvider::Muse) => {
            options.push(PickerOption::new(
                borg_provider::muse_product_model(),
                borg_provider::muse_product_model(),
            ));
        }
        Some(CodingProvider::Glm) => {
            // The Coding Plan serves these two; older ids are silently routed
            // to them by the vendor, so offering them would mislead.
            for model in ["glm-5.3", "glm-5.3-flash"] {
                options.push(PickerOption::new(model, model));
            }
        }
        Some(CodingProvider::Qwen) => {
            // The Alibaba Coding Plan's recommended models; the plan also
            // serves GLM, Kimi and MiniMax under its own quota, which the
            // vendor's list covers and older ids route to.
            for model in [
                "qwen3.7-plus",
                "qwen3.6-plus",
                "qwen3-coder-plus",
                "qwen3-coder-next",
            ] {
                options.push(PickerOption::new(model, model));
            }
        }
        provider @ (Some(CodingProvider::Anthropic)
        | Some(CodingProvider::OpenAiCompatible)
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

    // The canonical fixed order is Codex, Claude, then OpenCode Go, then
    // OpenRouter. OpenCode Go is a first-class subscription destination, so it
    // stays adjacent to the other fixed catalogs rather than trailing the
    // open-ended OpenRouter list.
    let go_models = borg_provider::opencode_go_model_entries();
    if go_models.is_empty() {
        let mut option = PickerOption::new("Connect OpenCode Go…", "/connect-go");
        option.section = Some("OpenCode Go".to_string());
        option.preview =
            Some("Add your Go subscription key and load the available models.".to_string());
        options.push(option);
    }
    let mut first_go = true;
    for model in go_models {
        if options.iter().any(|option| option.value == model.id) {
            continue;
        }
        let mut option = PickerOption::new(model.label, model.id);
        option.preview = model.detail;
        if first_go {
            option.section = Some("OpenCode Go".to_string());
            first_go = false;
        }
        options.push(option);
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
        if let Err(error) = execute!(
            stdout,
            EnableMouseCapture,
            EnableFocusChange,
            SetCursorStyle::BlinkingBar
        ) {
            let _ = execute!(stdout, DisableBracketedPaste);
            if mode == ScreenMode::Alternate {
                let _ = execute!(stdout, LeaveAlternateScreen);
            }
            discard_pending_terminal_input();
            let _ = disable_raw_mode();
            return Err(error.into());
        }
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = match Terminal::with_options(
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
        let keyboard_enhanced = execute!(
            terminal.backend_mut(),
            PushKeyboardEnhancementFlags(KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES)
        )
        .is_ok();
        // Query before the input thread takes stdin: the probe reads the
        // terminal's reply itself.
        let image_picker = detect_image_picker();
        let mut transcript = root_transcript_from_environment();
        // The preview resolution is the tile geometry, and the tile is sized in
        // cells: without the cell pixel size a graphics preview cannot know how
        // many rows the image needs. A picker that reports none keeps glyph
        // previews, which say so rather than pretending to be legible.
        transcript.set_image_cell(image_preview_cell(image_picker.as_ref()));
        Ok(Self {
            terminal,
            input: TerminalInput::spawn(),
            mode,
            keyboard_enhanced,
            transcript,
            director_transcript: None,
            child_transcripts: HashMap::new(),
            child_unhydrated_events: HashMap::new(),
            hydrated_children: HashSet::new(),
            child_history_hydration_complete: false,
            child_queued_prompts: HashMap::new(),
            child_requeue_cursors: HashMap::new(),
            child_statuses: HashMap::new(),
            child_activity_clocks: HashMap::new(),
            child_pending_approvals: HashSet::new(),
            suppress_bootstrap_subagent_activity: true,
            focused_child: None,
            focused_tool: None,
            tool_return_scroll_from_bottom: 0,
            tool_return_follow_tail: true,
            sidecar_focus_request: None,
            team_switcher_open: false,
            team_roster_hit_areas: Vec::new(),
            hovered_team_roster: None,
            back_to_director_area: None,
            back_to_director_hovered: false,
            composer: Composer::default(),
            attachment_store,
            keymap,
            ui_language: UiLanguage::Auto,
            cwd,
            configured_model_entries: Vec::new(),
            extension_commands: Vec::new(),
            git_status_cache: GitStatusCache::default(),
            git_push: GitRemoteState::default(),
            git_commit: GitCommitState::default(),
            git_status_area: None,
            git_status_hovered: false,
            git_pull_area: None,
            git_commit_area: None,
            git_commit_hovered: false,
            git_pull_hovered: false,
            status: SessionStatus::Starting,
            interrupt_requested: false,
            interrupt_requested_at: None,
            connection_retry_at: None,
            connection_retry_attempt: None,
            usage_retry_at: None,
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
            pending_input_header_area: None,
            pending_input_expanded: true,
            keybindings_hint_area: None,
            keybindings_hovered: false,
            dictation_button_area: None,
            dictation_button_hovered: false,
            dictation_state: DictationState::Idle,
            dictation_icon: dictation_icon_style_for_preference(None),
            running_sweeps: true,
            action_descriptors: true,
            tool_click_behavior: ToolClickBehavior::Fullscreen,
            thread_find: None,
            completion_notifications: CompletionAlertPolicy::Unfocused,
            completion_sound: CompletionAlertPolicy::Unfocused,
            completion_alert_pending: false,
            auto_copy_selection: true,
            luna_titles_for_all_providers: false,
            horizontal_margin: HORIZONTAL_MARGIN,
            composer_max_height: 8,
            show_footer: true,
            window_focused: true,
            tool_hit_areas: Vec::new(),
            tool_run_hit_areas: Vec::new(),
            tool_run_header_hit_areas: Vec::new(),
            entry_hit_areas: Vec::new(),
            message_hit_areas: Vec::new(),
            link_hit_areas: Vec::new(),
            image_picker,
            image_scroll_settles_at: None,
            image_protocols: HashMap::new(),
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
            shell_status_area: None,
            shell_status_hovered: false,
            shell_menu_open: false,
            shell_row_hit_areas: Vec::new(),
            hovered_shell_row: None,
            watch_status_area: None,
            watch_status_hovered: false,
            watch_menu_open: false,
            watch_row_hit_areas: Vec::new(),
            hovered_watch_row: None,
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
            nested_scroll_motion: None,
            text_selection: None,
            composer_selection: None,
            pending_transcript_click: None,
            pending_tool_copy: None,
            activity_clock: ActivityClock::default(),
            notice: None,
            copy_notice_expires_at: None,
            last_ctrl_c: None,
            ctrl_c_count: 0,
            queued_prompts: Vec::new(),
            optimistic_idle_prompt: None,
            withheld_queued_prompt: None,
            requeue_cursor: None,
            active_turn_followup: false,
            child_active_turn_followups: HashSet::new(),
            replaying_history: false,
            history_page_requested: false,
            history_page_loading: false,
            picker: None,
            pending_auth_model: None,
            dictation_enabled: false,
            dictation_enable_flow: false,
            dictation_model: None,
            pending_dictation_model: None,
            pending_dictation_accelerator: None,
            keybindings_open: false,
            slash_selection: 0,
            rewind_targets: Vec::new(),
            rewind_primed: false,
            borging_this_run: false,
            last_terminal_title: None,
            transcript_render_cache: None,
            transcript_full_render_cache: None,
            active_transcript_render: None,
            last_committed_viewport_render: None,
            last_reasoning_summary_phases: Vec::new(),
            rendered_transcript_height: 0,
            pending_scroll_anchor_height: None,
            pending_transcript_anchor: None,
            event_redraw_needed: false,
            last_tool_timer_refresh_tick: None,
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
        self.transcript = root_transcript_from_environment();
        self.transcript
            .set_image_cell(image_preview_cell(self.image_picker.as_ref()));
        self.director_transcript = None;
        self.child_transcripts.clear();
        self.child_unhydrated_events.clear();
        self.hydrated_children.clear();
        self.child_history_hydration_complete = false;
        self.child_queued_prompts.clear();
        self.child_statuses.clear();
        self.child_activity_clocks.clear();
        self.child_pending_approvals.clear();
        self.suppress_bootstrap_subagent_activity = true;
        self.focused_child = None;
        self.focused_tool = None;
        self.tool_return_scroll_from_bottom = 0;
        self.tool_return_follow_tail = true;
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
        self.git_push = GitRemoteState::default();
        self.git_commit = GitCommitState::default();
        self.git_status_area = None;
        self.git_status_hovered = false;
        self.git_pull_area = None;
        self.git_commit_area = None;
        self.git_commit_hovered = false;
        self.git_pull_hovered = false;
        self.status = SessionStatus::Starting;
        self.interrupt_requested = false;
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
        self.pending_input_header_area = None;
        self.pending_input_expanded = true;
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
        self.shell_status_area = None;
        self.shell_status_hovered = false;
        self.shell_menu_open = false;
        self.shell_row_hit_areas.clear();
        self.hovered_shell_row = None;
        self.watch_status_area = None;
        self.watch_status_hovered = false;
        self.watch_menu_open = false;
        self.watch_row_hit_areas.clear();
        self.hovered_watch_row = None;
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
        self.nested_scroll_motion = None;
        self.text_selection = None;
        self.pending_transcript_click = None;
        self.pending_tool_copy = None;
        self.activity_clock = ActivityClock::default();
        self.notice = None;
        self.copy_notice_expires_at = None;
        self.last_ctrl_c = None;
        self.ctrl_c_count = 0;
        self.queued_prompts.clear();
        self.active_turn_followup = false;
        self.child_active_turn_followups.clear();
        self.replaying_history = false;
        self.history_page_requested = false;
        self.history_page_loading = false;
        self.picker = None;
        self.pending_auth_model = None;
        self.dictation_enable_flow = false;
        self.keybindings_open = false;
        self.slash_selection = 0;
        self.rewind_targets.clear();
        self.rewind_primed = false;
        self.borging_this_run = false;
        self.last_terminal_title = None;
        self.invalidate_transcript_render_cache();
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
        self.last_ctrl_c = None;
        self.ctrl_c_count = 0;
        self.notice = Some("Prompt cleared".to_string());
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
                pending.actor,
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

    pub fn is_inspecting_action(&self) -> bool {
        self.focused_tool.is_some()
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
        self.invalidate_transcript_render_cache();
    }

    /// Consume one explicit upward-navigation request once the loaded
    /// transcript is near its oldest edge. Ordinary redraw/activity ticks
    /// must never hydrate historical pages behind a user who is following the
    /// live tail.
    pub fn take_history_page_request(&mut self) -> bool {
        // An open action inspector holds a transcript index, and hydrating an
        // older page rebuilds `order` from a longer event list, renumbering
        // every entry. Defer paging until the inspector closes rather than let
        // the focused action silently become a different one.
        if self.focused_child.is_some() || self.focused_tool.is_some() {
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
            self.activity_clock.observe(status, Utc::now());
            if !status_control_is_actionable(status) {
                self.interrupt_requested = false;
            }
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
        // Never destroy a draft. A returned prompt used to overwrite the
        // composer outright, so anything typed between submitting and the
        // rejection arriving was silently lost. `append_recalled` degrades to
        // a plain restore when the composer is empty, which is the usual case
        // because submission takes it.
        self.composer.append_recalled(text, attachments);
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
            self.child_active_turn_followups.insert(child);
            push_queued_prompt(
                self.child_queued_prompts.entry(child).or_default(),
                message_id,
                text,
                delivery,
                EventActor::User,
            );
        } else {
            self.active_turn_followup = true;
            push_queued_prompt(
                &mut self.queued_prompts,
                message_id,
                text,
                delivery,
                EventActor::User,
            );
        }
    }

    /// Decide whether `event` may update the pending-prompt projection.
    ///
    /// Returns false only for the one transient this surface already rendered
    /// optimistically: the `Message{status: Queued}` the session writes for an
    /// idle submission immediately before `TurnStarted`. Holding it back keeps
    /// the prompt from appearing in the pending list for a frame and then
    /// vanishing. The hold is resolved deterministically rather than on a
    /// timer: the matching `TurnStarted` drops it, and anything else that
    /// settles a prompt releases it into the pending list where it belongs, so
    /// a genuinely queued prompt is never lost.
    fn absorb_optimistic_idle_prompt(&mut self, event: &SessionEventKind) -> bool {
        let Some(optimistic) = self.optimistic_idle_prompt else {
            return true;
        };
        match optimistic_idle_prompt_decision(event, optimistic) {
            OptimisticPromptDecision::Withhold(withheld) => {
                self.withheld_queued_prompt = Some(withheld);
                false
            }
            OptimisticPromptDecision::Settled => {
                // The transient resolved exactly as projected.
                self.optimistic_idle_prompt = None;
                self.withheld_queued_prompt = None;
                true
            }
            OptimisticPromptDecision::Release => {
                // Something else settled first, so the prompt really is
                // waiting. Release the withheld projection before the event is
                // applied so ordering matches an unsuppressed run.
                self.release_withheld_queued_prompt();
                true
            }
            OptimisticPromptDecision::Ignore => true,
        }
    }

    fn release_withheld_queued_prompt(&mut self) {
        self.optimistic_idle_prompt = None;
        if let Some(withheld) = self.withheld_queued_prompt.take() {
            push_queued_prompt(
                &mut self.queued_prompts,
                withheld.message_id,
                withheld.text,
                withheld.delivery,
                withheld.actor,
            );
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
        self.optimistic_idle_prompt = Some(message_id);
        self.withheld_queued_prompt = None;
        self.status = SessionStatus::Starting;
        self.interrupt_requested = false;
        self.activity_clock
            .observe(SessionStatus::Starting, event.created_at);
        self.transcript.follow_tail = true;
        self.invalidate_transcript_render_cache();
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
                self.invalidate_transcript_render_cache();
            }
            if self
                .transcript
                .active_turn
                .as_ref()
                .is_some_and(|turn| turn.message_id == message_id)
            {
                self.transcript.active_turn = None;
                self.status = SessionStatus::Ready;
                self.activity_clock
                    .observe(SessionStatus::Ready, Utc::now());
            }
        }
        self.optimistic_idle_prompt = None;
        self.withheld_queued_prompt = None;
        self.composer.append_recalled(text, attachments);
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

    pub fn running_tool_timer_refresh_due(&mut self) -> bool {
        let tick = self.transcript.running_tool_timer_tick_at(Utc::now());
        let due = tick.is_some() && tick != self.last_tool_timer_refresh_tick;
        self.last_tool_timer_refresh_tick = tick;
        due
    }

    fn visible_reasoning_summary_phases_at(&self, now: DateTime<Utc>) -> Vec<(usize, i64)> {
        if self.focused_tool.is_some() {
            return Vec::new();
        }
        self.tool_hit_areas
            .iter()
            .filter_map(|(_, index)| {
                self.transcript
                    .reasoning_summary_rotation_phase_at(*index, now)
                    .map(|phase| (*index, phase))
            })
            .collect()
    }

    pub fn has_active_subagents(&self) -> bool {
        self.transcript.active_subagent_count() > 0
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
        if !self.replaying_history {
            if self.connection_retry_at.is_some()
                && matches!(
                    &event.kind,
                    SessionEventKind::Message {
                        actor: EventActor::Assistant,
                        ..
                    } | SessionEventKind::ReasoningDelta { .. }
                        | SessionEventKind::ToolStarted { .. }
                )
            {
                self.connection_retry_at = None;
                self.connection_retry_attempt = None;
                self.notice = None;
            }
            match &event.kind {
                SessionEventKind::ProviderEvent { kind, payload, .. }
                    if kind == "usage_limit_retry" =>
                {
                    self.usage_retry_at = payload
                        .get("retry_at")
                        .cloned()
                        .and_then(|value| serde_json::from_value(value).ok());
                    if let Some(deadline) = self.usage_retry_at {
                        self.set_notice(format!(
                            "Usage limit · resumes {} · /login or /model to switch · Esc to cancel",
                            deadline.with_timezone(&Local).format("%a %H:%M")
                        ));
                    }
                }
                SessionEventKind::ProviderEvent { kind, .. }
                    if kind == "usage_limit_retry_cancelled" =>
                {
                    self.usage_retry_at = None;
                    self.set_notice("Automatic retry cancelled. Your work is saved.");
                }
                SessionEventKind::TurnStarted { .. }
                | SessionEventKind::SessionConfigured { .. } => {
                    self.usage_retry_at = None;
                }
                SessionEventKind::ProviderEvent { kind, payload, .. }
                    if kind == "network_retry" =>
                {
                    let delay = payload
                        .get("delay_ms")
                        .and_then(serde_json::Value::as_i64)
                        .unwrap_or(0);
                    self.connection_retry_at =
                        Some(event.created_at + chrono::Duration::milliseconds(delay));
                    self.connection_retry_attempt = payload
                        .get("attempt")
                        .and_then(serde_json::Value::as_u64)
                        .zip(
                            payload
                                .get("max_attempts")
                                .and_then(serde_json::Value::as_u64),
                        );
                    let bound = self
                        .connection_retry_attempt
                        .map(|(_, max_attempts)| max_attempts.to_string())
                        .unwrap_or_else(|| "?".to_string());
                    if let Some(attempt) = payload
                        .get("auth_lookup_retry")
                        .and_then(serde_json::Value::as_u64)
                    {
                        self.set_notice(format!("Codex authentication lookup unavailable · attempt {attempt}/{bound} · work saved · Esc to cancel"));
                    } else if let Some((attempt, max_attempts)) = self.connection_retry_attempt {
                        self.set_notice(format!(
                            "Retrying the request · attempt {attempt}/{max_attempts} · work saved · Esc to cancel · the reason: {}",
                            payload
                                .get("error")
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or("the provider did not say")
                        ));
                    } else {
                        self.set_notice("Retrying the request · work saved · Esc to cancel");
                    }
                }
                SessionEventKind::ProviderEvent { kind, payload, .. }
                    if kind == "network_retry_exhausted" =>
                {
                    self.connection_retry_at = None;
                    self.connection_retry_attempt = None;
                    let attempts = payload
                        .get("attempts")
                        .and_then(serde_json::Value::as_u64)
                        .map(|attempts| attempts.to_string())
                        .unwrap_or_else(|| "the allowed number of".to_string());
                    self.set_notice(format!(
                        "Stopped retrying after {attempts} attempts · the turn was not resent again · check the provider status or /model, then resend · your work is saved · last error: {}",
                        payload
                            .get("error")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("the provider did not say")
                    ));
                }
                SessionEventKind::ProviderEvent { kind, .. } if kind == "network_recovered" => {
                    self.connection_retry_at = None;
                    self.connection_retry_attempt = None;
                    self.set_notice("Connection restored · work resumed");
                }
                SessionEventKind::StatusChanged {
                    status:
                        status @ (SessionStatus::Ready | SessionStatus::Stopped | SessionStatus::Failed),
                    ..
                } => {
                    if self.connection_retry_at.take().is_some() {
                        self.connection_retry_attempt = None;
                        self.notice = None;
                    }
                    if *status != SessionStatus::Ready {
                        self.usage_retry_at = None;
                    }
                }
                _ => {}
            }
        }
        if !self.replaying_history
            && completion_alert_due(&mut self.completion_alert_pending, &event.kind)
        {
            let notification =
                completion_alert_enabled(self.completion_notifications, self.window_focused);
            let sound = completion_alert_enabled(self.completion_sound, self.window_focused);
            if notification || sound {
                self.send_completion_alert(notification, sound);
            }
        }
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
            SessionEventKind::ProviderEvent { kind, payload, .. } => {
                is_context_compaction(kind)
                    || is_live_tool_call_event(kind)
                    || kind == "action/preparing"
                    || kind == "action/generation_status"
                    || kind == "action/preparing_cancelled"
                    || kind == "network_retry"
                    || kind == "network_recovered"
                    || kind == "mcp_server_unavailable"
                    || Transcript::provider_reasoning_lifecycle(kind, payload).is_some()
            }
            _ => true,
        };
        if matches!(
            &event.kind,
            SessionEventKind::Message {
                actor: EventActor::User,
                status: MessageStatus::Queued,
                ..
            }
        ) && matches!(
            self.status,
            SessionStatus::Running | SessionStatus::WaitingForApproval
        ) {
            self.active_turn_followup = true;
        }
        if turn_completion_clears_followup_marker(&event.kind, self.queued_prompts.is_empty()) {
            self.active_turn_followup = false;
        }
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
            if !status_control_is_actionable(status) {
                self.interrupt_requested = false;
            }
            self.activity_clock.observe(status, event.created_at);
        }
        if matches!(event.kind, SessionEventKind::TurnStarted { .. }) {
            self.interrupt_requested = false;
        }
        if event.sequence > 0 {
            self.session_state_sequence = self.session_state_sequence.max(event.sequence);
        }
        if self.absorb_optimistic_idle_prompt(&event.kind) {
            update_queued_prompts(
                &mut self.queued_prompts,
                &event.kind,
                &mut self.requeue_cursor,
            );
        }
        if self.notice.as_deref() == Some("Sending pending input")
            && self.active_queued_prompts().is_empty()
        {
            self.notice = None;
        }
        if let SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text,
            attachments,
            ..
        } = &event.kind
            && event.sequence > 0
            && !self
                .rewind_targets
                .iter()
                .any(|target| target.message_id == *message_id)
        {
            self.rewind_targets.push(RewindTarget {
                message_id: *message_id,
                sequence: event.sequence,
                text: text.clone(),
                attachments: attachments.clone(),
            });
        }
        match &event.kind {
            SessionEventKind::Message {
                actor: EventActor::User,
                status: MessageStatus::Complete,
                ..
            } => {
                // CLI launch prompts and prompts admitted through another
                // attached surface may never pass through `Composer::take`.
                // Every completed user message must still become recallable
                // with Up in this running instance and future resumes.
                self.composer
                    .seed_session_events(std::slice::from_ref(event));
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
        let (removed_entry, inserted_entries, transcript_changed) = {
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
            let changed = focused_child_transcript_changed
                || (changed
                    && (transcript.show_subagent_messages
                        || match &event.kind {
                            SessionEventKind::SubagentActivity { agent, .. } => {
                                transcript.subagent_entries.contains_key(&agent.session_id)
                            }
                            SessionEventKind::AgentMessageReceived { .. } => false,
                            _ => true,
                        }));
            let inserted_entries = transcript.take_entry_insertions();
            (
                removed_entry,
                inserted_entries,
                changed || transcript.order.len() != entries_before,
            )
        };
        if self.focused_child.is_none() {
            if let Some(removed) = removed_entry {
                self.remap_selection_after_entry_removal(removed);
            }
            // Removals are reported before insertions because the transcript
            // applies them in that order within a single event.
            for inserted in inserted_entries {
                self.remap_selection_after_entry_insertion(inserted);
            }
        }
        if transcript_changed {
            if should_preserve_transcript_viewport(self.transcript.follow_tail)
                && self.pending_scroll_anchor_height.is_none()
            {
                self.pending_scroll_anchor_height = Some(self.rendered_transcript_height);
            }
            if removed_entry.is_some() || matches!(event.kind, SessionEventKind::ContextCleared) {
                self.invalidate_transcript_render_cache();
            } else {
                // Streaming dirties layout, not the last painted viewport.
                // Input can reuse it until an ordinary frame commits the update.
                self.transcript_render_cache = None;
                self.transcript_full_render_cache = None;
            }
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
        let observed_at = if self.child_activity_clocks.contains_key(&child_id) {
            event.created_at
        } else {
            agent.created_at
        };
        track_child_activity(
            &mut self.child_activity_clocks,
            child_id,
            status,
            observed_at,
        );
        if !self.hydrated_children.contains(&child_id) && self.child_history_hydration_complete {
            self.hydrated_children.insert(child_id);
        }
        let Some(child_event) = child_event else {
            return false;
        };
        if !self.hydrated_children.contains(&child_id)
            && !matches!(
                child_event.kind,
                SessionEventKind::MessageDelta { .. } | SessionEventKind::ReasoningTextDelta { .. }
            )
        {
            self.child_unhydrated_events
                .entry(child_id)
                .or_default()
                .push(child_event.as_ref().clone());
        }
        if matches!(
            &child_event.kind,
            SessionEventKind::Message {
                actor: EventActor::User,
                status: MessageStatus::Queued,
                ..
            }
        ) && self
            .child_statuses
            .get(&child_id)
            .copied()
            .is_some_and(|status| {
                matches!(
                    status,
                    SessionStatus::Running | SessionStatus::WaitingForApproval
                )
            })
        {
            self.child_active_turn_followups.insert(child_id);
        }
        if turn_completion_clears_followup_marker(
            &child_event.kind,
            self.child_queued_prompts
                .get(&child_id)
                .is_none_or(Vec::is_empty),
        ) {
            self.child_active_turn_followups.remove(&child_id);
        }
        if let SessionEventKind::StatusChanged { status, .. } = child_event.kind {
            self.child_statuses.insert(child_id, status);
            track_child_activity(
                &mut self.child_activity_clocks,
                child_id,
                status,
                child_event.created_at,
            );
        }
        if self.focused_child == Some(child_id)
            && (matches!(child_event.kind, SessionEventKind::TurnStarted { .. })
                || matches!(
                    child_event.kind,
                    SessionEventKind::StatusChanged { status, .. }
                        if !status_control_is_actionable(status)
                ))
        {
            self.interrupt_requested = false;
        }
        update_queued_prompts(
            self.child_queued_prompts.entry(child_id).or_default(),
            &child_event.kind,
            self.child_requeue_cursors.entry(child_id).or_default(),
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
            let removed_entry = self.transcript.apply(child_event);
            let inserted_entries = self.transcript.take_entry_insertions();
            if let Some(removed) = removed_entry {
                self.remap_selection_after_entry_removal(removed);
            }
            for inserted in inserted_entries {
                self.remap_selection_after_entry_insertion(inserted);
            }
            changed || self.transcript.order.len() != entries_before
        } else {
            self.child_transcript_mut(child_id).apply(child_event);
            false
        }
    }

    fn child_transcript_mut(&mut self, child_id: Uuid) -> &mut Transcript {
        self.child_transcripts
            .entry(child_id)
            .or_insert_with(new_child_transcript)
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
        self.interrupt_requested = false;
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
        self.interrupt_requested = false;
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
        let roster_updated_at = self
            .director_transcript
            .as_deref()
            .unwrap_or(&self.transcript)
            .subagent_snapshots
            .get(&child_id)
            .map(|agent| agent.updated_at);
        let mut transcript = fresh_transcript_like(previous);
        transcript.show_director_context_boundary();
        transcript.reserve_history(events.len());
        let optimistic_pending = self
            .child_queued_prompts
            .remove(&child_id)
            .unwrap_or_default();
        self.child_pending_approvals.remove(&child_id);
        for event in &events {
            // A live parent completion can arrive while the child query is in
            // flight. Its newer roster status must survive this older tail.
            if let Some(status) = subagent_status_from_child_event(&event.kind)
                && roster_updated_at.is_none_or(|updated_at| event.created_at >= updated_at)
            {
                let status = subagent_session_status(status);
                self.child_statuses.insert(child_id, status);
                track_child_activity(
                    &mut self.child_activity_clocks,
                    child_id,
                    status,
                    event.created_at,
                );
            }
            update_queued_prompts(
                self.child_queued_prompts.entry(child_id).or_default(),
                &event.kind,
                self.child_requeue_cursors.entry(child_id).or_default(),
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
            self.invalidate_transcript_render_cache();
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
            if transcript
                .subagent_snapshots
                .get(&agent.session_id)
                .is_some_and(|current| current.updated_at > agent.updated_at)
            {
                continue;
            }
            transcript.upsert_subagent_snapshot(agent);
            let status = subagent_session_status(agent.status);
            self.child_statuses.insert(agent.session_id, status);
            let observed_at = if self.child_activity_clocks.contains_key(&agent.session_id) {
                agent.updated_at
            } else {
                agent.created_at
            };
            track_child_activity(
                &mut self.child_activity_clocks,
                agent.session_id,
                status,
                observed_at,
            );
        }
    }

    fn active_status(&self) -> SessionStatus {
        self.focused_child()
            .and_then(|child| self.child_statuses.get(&child).copied())
            .unwrap_or(self.status)
    }

    fn active_activity_clock(&self) -> ActivityClock {
        match self.focused_child() {
            Some(child) => self
                .child_activity_clocks
                .get(&child)
                .copied()
                .unwrap_or_default(),
            None => self.activity_clock,
        }
    }

    fn active_pending_approval(&self) -> bool {
        self.focused_child.map_or(self.pending_approval, |child| {
            self.child_pending_approvals.contains(&child)
        })
    }

    /// The working directory whose git status the footer shows: the active
    /// session's cwd, or the terminal's own when no session is configured.
    fn active_git_cwd(&self) -> PathBuf {
        self.transcript
            .config
            .as_ref()
            .map(|config| config.cwd.clone())
            .unwrap_or_else(|| self.cwd.clone())
    }

    /// Push the branch shown in the footer indicator. Runs `git push` off the
    /// UI thread; the result is drained on a later frame and shown as a notice.
    fn push_unpushed_commits(&mut self) {
        let cwd = self.active_git_cwd();
        let action = GitRemoteAction::Push;
        if self.git_push.is_running(&cwd, action) {
            self.notice = Some("Already pushing…".to_string());
            return;
        }
        if self.git_push.start(&cwd, action) {
            self.notice = Some(action.in_progress().to_string());
        }
    }

    /// Pull the upstream commits the footer behind-count is showing. Same
    /// off-thread shape as the push path, with its own in-flight entry so a
    /// push and a pull of the same directory do not block each other.
    fn pull_upstream_commits(&mut self) {
        let cwd = self.active_git_cwd();
        let action = GitRemoteAction::Pull;
        if self.git_push.is_running(&cwd, action) {
            self.notice = Some("Already pulling…".to_string());
            return;
        }
        if self.git_push.start(&cwd, action) {
            self.notice = Some(action.in_progress().to_string());
        }
    }

    /// Drain finished background pushes, report the outcome, and force a git
    /// status refresh so the footer's ↑N reflects the new upstream.
    fn drain_git_push_results(&mut self) {
        for (cwd, action, outcome) in self.git_push.drain() {
            match outcome {
                Ok(summary) => self.notice = Some(format!("{} · {summary}", action.done())),
                Err(error) => self.notice = Some(format!("git {} failed · {error}", action.verb())),
            }
            self.git_status_cache.invalidate(&cwd);
        }
        for (cwd, outcome) in self.git_commit.drain() {
            match outcome {
                Ok(summary) => self.notice = Some(format!("Committed · {summary}")),
                Err(error) => self.notice = Some(format!("git commit failed · {error}")),
            }
            self.git_status_cache.invalidate(&cwd);
        }
    }

    /// Stage everything in the footer's worktree, draft a commit message with
    /// the cheap commit model, commit, and push — all off the UI thread.
    fn commit_and_push_working_tree(&mut self) {
        let cwd = self.active_git_cwd();
        if self.git_commit.is_committing(&cwd) {
            self.notice = Some("Already committing…".to_string());
            return;
        }
        let model = commit_model_from_environment();
        if self.git_commit.start(&cwd, model.clone()) {
            self.notice = Some(format!(
                "Committing · drafting message with {}…",
                model.label()
            ));
        }
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

    fn invalidate_transcript_render_cache(&mut self) {
        self.transcript_render_cache = None;
        self.transcript_full_render_cache = None;
        self.last_committed_viewport_render = None;
    }

    fn reset_transcript_focus(&mut self) {
        self.focused_tool = None;
        self.scroll_from_bottom = 0;
        self.transcript.follow_tail = true;
        self.invalidate_transcript_render_cache();
        self.active_transcript_render = None;
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

    fn open_tool_inspector(&mut self, index: usize) -> Vec<SessionPayloadRef> {
        let Some((_, complete)) = self.transcript.inspector_heading(index) else {
            return Vec::new();
        };
        // A tool row with nothing to show (an empty Thinking window, a probe
        // with no body) has no inspector; treat the click as a no-op.
        if matches!(
            self.transcript.order.get(index),
            Some(TranscriptEntry::Tool { .. })
        ) && !self.transcript.tool_is_expandable(index)
        {
            return Vec::new();
        }
        if self.focused_tool == Some(index) {
            return Vec::new();
        }
        self.tool_return_scroll_from_bottom = self.scroll_from_bottom;
        self.tool_return_follow_tail = self.transcript.follow_tail;
        self.focused_tool = Some(index);
        self.transcript.follow_tail = !complete;
        self.scroll_from_bottom = if complete { usize::MAX } else { 0 };
        self.cancel_scroll_motion();
        self.text_selection = None;
        self.pending_transcript_click = None;
        self.hovered_tool = None;
        self.hovered_tool_run = None;
        self.hovered_tool_run_header = None;
        self.invalidate_transcript_render_cache();
        self.transcript.tool_payloads(index)
    }

    fn close_tool_inspector(&mut self) {
        if self.focused_tool.take().is_none() {
            return;
        }
        self.scroll_from_bottom = self.tool_return_scroll_from_bottom;
        self.transcript.follow_tail = self.tool_return_follow_tail;
        self.cancel_scroll_motion();
        self.text_selection = None;
        self.pending_transcript_click = None;
        self.hovered_tool = None;
        self.invalidate_transcript_render_cache();
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
        self.shell_status_hovered = false;
        self.hovered_shell_row = None;
        self.watch_status_hovered = false;
        self.hovered_watch_row = None;
        self.agents_status_hovered = false;
        self.model_status_hovered = false;
        self.effort_status_hovered = false;
        self.context_status_hovered = false;
        self.fast_status_hovered = false;
        self.permission_status_hovered = false;
        self.git_status_hovered = false;
        self.git_commit_hovered = false;
        self.git_pull_hovered = false;
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

    fn begin_user_interrupt(&mut self) -> bool {
        let status = self.active_status();
        let interrupt_status = if self.usage_retry_at.is_some() {
            SessionStatus::Starting
        } else {
            status
        };
        // A repeated Esc resends the interrupt: an earlier request may have
        // raced a turn boundary, and Esc must never look dead.
        if self.interrupt_requested
            && status_control_is_actionable(interrupt_status)
            && self
                .interrupt_requested_at
                .is_some_and(|at| at.elapsed() >= INTERRUPT_RESEND_AFTER)
        {
            self.interrupt_requested_at = Some(Instant::now());
            return true;
        }
        if !claim_interrupt(&mut self.interrupt_requested, interrupt_status) {
            return false;
        }
        self.interrupt_requested_at = Some(Instant::now());
        if status == SessionStatus::Running {
            self.transcript.order.push(TranscriptEntry::Activity {
                text: USER_INTERRUPT_ACTIVITY.to_string(),
                time: canonical_local_time(Local::now()),
            });
            self.transcript.follow_tail = true;
            self.scroll_from_bottom = 0;
            self.invalidate_transcript_render_cache();
            self.event_redraw_needed = true;
        }
        true
    }

    /// Esc stops an active turn even with input queued: the runtime sends
    /// that input as the next turn, so stopping never waits behind it.
    fn escape_interrupts_turn(&self) -> bool {
        status_control_is_actionable(self.active_status())
    }

    fn has_pending_input_for_escape(&self) -> bool {
        self.active_queued_prompts()
            .iter()
            .any(|prompt| prompt.actor == EventActor::User)
    }

    fn flush_pending_input(&mut self) -> UiAction {
        let target = self.focused_child;
        let root_retrying = target.is_none()
            && (self.connection_retry_at.is_some() || self.usage_retry_at.is_some());
        if self.active_status() != SessionStatus::Running || root_retrying {
            self.notice = Some("Pending input stays queued until the session resumes".to_string());
            return UiAction::None;
        }
        if let Some(child) = target {
            self.child_active_turn_followups.remove(&child);
        } else {
            self.active_turn_followup = false;
        }
        self.notice = Some("Sending pending input".to_string());
        UiAction::FlushPendingInput { target }
    }

    pub fn hydrate_payload(
        &mut self,
        payload: &SessionPayloadRef,
        bytes: Vec<u8>,
    ) -> Result<Option<TerminalIoRequest>> {
        self.transcript.hydrate_payload(payload, bytes)?;
        self.transcript.tool_body_cache.get_mut().lines.clear();
        self.invalidate_transcript_render_cache();
        if let Some(index) = self.pending_tool_copy
            && self.transcript.tool_payloads(index).is_empty()
        {
            self.pending_tool_copy = None;
            return Ok(self.copy_transcript_entry_request(index));
        }
        Ok(None)
    }

    pub fn apply_terminal_io_completion(&mut self, completion: TerminalIoCompletion) {
        match completion.kind {
            TerminalIoCompletionKind::Copied(Ok(notice)) => self.show_copy_notice(notice),
            TerminalIoCompletionKind::Copied(Err(error)) => {
                self.notice = Some(format!("Copy failed: {error}"));
            }
            TerminalIoCompletionKind::Pasted(Ok(PasteOutcome { text, attachments })) => {
                self.composer.insert(&text);
                for path in attachments {
                    self.composer.insert_attachment(path);
                }
                self.notice = self
                    .composer
                    .attachments
                    .last()
                    .map(|attachment| format!("Attached {}", attachment.label));
            }
            TerminalIoCompletionKind::Pasted(Err(error)) => {
                self.notice = Some(format!("Paste failed: {error}"));
            }
            TerminalIoCompletionKind::LinkOpened(Ok(())) => {}
            TerminalIoCompletionKind::LinkOpened(Err(error)) => {
                self.notice = Some(format!("Could not open link: {error}"));
            }
        }
        self.event_redraw_needed = true;
    }

    pub fn show_goal(&mut self, goal: Option<&SessionGoal>) {
        self.notice = None;
        if let Some(removed) = self.transcript.show_goal(goal) {
            self.remap_selection_after_entry_removal(removed);
        }
        self.invalidate_transcript_render_cache();
    }

    pub fn optimistically_apply_goal_action(&mut self, action: &GoalAction) -> bool {
        let changed = self.transcript.optimistically_apply_goal_action(action);
        if changed {
            if matches!(action, GoalAction::Resume)
                && self.focused_child.is_none()
                && !matches!(
                    self.status,
                    SessionStatus::Starting
                        | SessionStatus::Running
                        | SessionStatus::WaitingForApproval
                )
            {
                self.status = SessionStatus::Starting;
                self.interrupt_requested = false;
                self.activity_clock
                    .observe(SessionStatus::Starting, Utc::now());
                self.borging_this_run = borging_for_run(Uuid::new_v4());
            }
            self.invalidate_transcript_render_cache();
            self.event_redraw_needed = true;
        }
        changed
    }

    pub fn show_plan(&mut self, items: &[PlanItem]) {
        self.notice = None;
        if let Some(removed) = self.transcript.show_plan(items) {
            self.remap_selection_after_entry_removal(removed);
        }
        self.invalidate_transcript_render_cache();
    }

    /// Shift viewport state that lives outside the transcript when an entry is
    /// inserted ahead of it. A late user message (or any reordered insertion)
    /// renumbers every following entry, so an inspector or selection anchored
    /// by index would silently re-target whatever row slid into its place.
    fn remap_selection_after_entry_insertion(&mut self, inserted: usize) {
        if let Some(index) = self.focused_tool.as_mut()
            && *index >= inserted
        {
            *index += 1;
        }
        if let Some(selection) = self.text_selection.as_mut() {
            for point in [&mut selection.anchor, &mut selection.focus] {
                point.entry += usize::from(point.entry >= inserted);
            }
        }
    }

    fn remap_selection_after_entry_removal(&mut self, removed: usize) {
        if self.focused_tool == Some(removed) {
            self.close_tool_inspector();
        } else if let Some(index) = self.focused_tool.as_mut()
            && *index > removed
        {
            *index -= 1;
        }
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
        self.invalidate_transcript_render_cache();
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
        let mut options = model_picker_options_with_configured(
            provider,
            current.as_deref(),
            &[],
            &self.configured_model_entries,
        );
        for option in &mut options {
            let target = CodingProvider::for_model(&option.value);
            if let Some(capability) = self
                .transcript
                .provider_capabilities
                .iter()
                .find(|capability| Some(capability.provider) == target)
            {
                let connection = if capability.authenticated {
                    capability
                        .billing_label()
                        .unwrap_or_else(|| "connected".to_string())
                } else {
                    "connect to use".to_string()
                };
                if let Some(section) = &mut option.section {
                    *section = format!("{section} · {connection}");
                }
                option.preview = Some(format!(
                    "{}\n{} · {}{}",
                    option.preview.as_deref().unwrap_or(&option.value),
                    capability.provider.label(),
                    connection,
                    if capability
                        .usage
                        .as_ref()
                        .is_some_and(|usage| usage.availability
                            == borg_remote::ProviderUsageAvailability::Exhausted)
                    {
                        " · allowance exhausted"
                    } else {
                        ""
                    }
                ));
            }
            if current.as_deref() == Some(&option.value) {
                option.label = format!("{} · current", option.label);
            }
        }
        let mut connection = PickerOption::new("Manage connection / billing…", "/login");
        connection.section = Some("Connections".to_string());
        connection.preview = Some(
            "Switch saved credentials, sign in, or add an API key. API billing is usage-based."
                .to_string(),
        );
        options.push(connection);
        options.push(PickerOption::new("Enter a model ID…", "/model-custom"));
        let selected = current
            .as_deref()
            .and_then(|current| options.iter().position(|option| option.value == current))
            .unwrap_or(0);
        self.picker = Some(Picker {
            kind: PickerKind::Model,
            title: "Choose model",
            options,
            selected,
            query: Some(String::new()),
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
        let mut options = match provider {
            CodingProvider::Codex => vec![
                PickerOption::new("ChatGPT subscription · included allowance", "subscription"),
                PickerOption::new("OpenAI API key · pay per use", "api-key"),
                PickerOption::new("Sign in to ChatGPT…", "reconnect-subscription"),
                PickerOption::new("Add or replace OpenAI API key…", "replace-api-key"),
            ],
            CodingProvider::OpenCode => vec![
                PickerOption::new("OpenCode Go · subscription key", "api-key"),
                PickerOption::new("Add or replace Go key…", "replace-api-key"),
                PickerOption::new("Other OpenCode connections…", "reconnect-subscription"),
            ],
            CodingProvider::Claude => vec![
                PickerOption::new("Connect Claude subscription…", "subscription"),
                PickerOption::new("Add Anthropic API key · pay per use", "api-key"),
            ],
            CodingProvider::Grok => vec![PickerOption::new(
                "Connect Grok Build subscription…",
                "reconnect-subscription",
            )],
            CodingProvider::Muse => vec![PickerOption::new(
                "Connect Muse Code subscription…",
                "reconnect-subscription",
            )],
            _ => vec![PickerOption::new(
                format!("Add {} API key…", provider.label()),
                "api-key",
            )],
        };
        for option in &mut options {
            option.preview = Some(match option.value.as_str() {
                "subscription" if provider == CodingProvider::Codex => "Use your saved ChatGPT login. If needed, Borg opens the subscription sign-in flow.",
                "api-key" if provider == CodingProvider::Codex => "Use your saved OpenAI key, or enter one privately. Requests are billed to your OpenAI API account. Your ChatGPT login is kept.",
                "api-key" | "replace-api-key" if provider == CodingProvider::OpenCode => "Get your subscription key at opencode.ai/auth. Go models use your Go allowance; select one in /model.",
                "reconnect-subscription" if provider == CodingProvider::Grok => "Opens the Grok Build sign-in. SuperGrok and X Premium Plus plans are supported; set XAI_API_KEY to use the API instead.",
                "reconnect-subscription" if provider == CodingProvider::Muse => "Opens the Muse Code sign-in with your Meta Model API account; set META_API_KEY for CI.",
                _ => "Credentials are entered privately and are never added to the conversation.",
            }.to_string());
        }
        options.push(PickerOption::new("Cancel", "cancel"));
        self.pending_auth_model = Some((provider, model));
        self.picker = Some(Picker {
            kind: PickerKind::ProviderAuth,
            title: "Connection & billing",
            options,
            selected: 0,
            query: None,
            viewport_offset: Cell::new(0),
        });
    }

    pub fn open_settings_picker(&mut self, user_label: &str, assistant_label: &str) {
        let options = vec![
            "Import threads and memory".to_string(),
            self.tr("Model").to_string(),
            "Reasoning effort".to_string(),
            self.tr("Response language").to_string(),
            self.tr("UI language").to_string(),
            "Language servers".to_string(),
            "Provider fast mode".to_string(),
            "Active messages".to_string(),
            "Refresh rate".to_string(),
            "Keep machine awake".to_string(),
            "Auto-expand edits".to_string(),
            "Auto-expand tools".to_string(),
            "Tool click behavior".to_string(),
            "Action descriptors".to_string(),
            "Running sweep animations".to_string(),
            "Completion notifications".to_string(),
            "Completion sound".to_string(),
            "Auto-copy selections".to_string(),
            "Luna titles across providers".to_string(),
            "Microphone icon".to_string(),
            "Transcript colours".to_string(),
            format!("User label · {user_label}"),
            format!("Assistant label · {assistant_label}"),
        ];
        let values = [
            "/import",
            "/model",
            "/effort",
            "/language",
            "/ui-language",
            "/lsp",
            "/fast",
            "/followups",
            "/refresh",
            "/sleep",
            "/expand-edits",
            "/expand-tools",
            "/expand-thinking",
            "/tool-click",
            "/action-descriptors",
            "/animations",
            "/notifications",
            "/sound",
            "auto-copy",
            "luna-titles",
            "/icons",
            "/colors",
            "/user-label",
            "/assistant-label",
        ];
        self.picker = Some(Picker {
            kind: PickerKind::Settings,
            title: self.tr("Settings"),
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

    pub fn open_import_source_picker(&mut self) {
        self.picker = Some(Picker {
            kind: PickerKind::ImportSource,
            title: "Import into Borg",
            options: [
                ("Codex CLI / Desktop", "codex"),
                ("Claude Code", "claude-code"),
                ("Claude Desktop export", "claude-desktop"),
                ("Other apps · portable JSON", "portable"),
            ]
            .into_iter()
            .map(|(label, value)| PickerOption::new(label, value))
            .collect(),
            selected: 0,
            query: None,
            viewport_offset: Cell::new(0),
        });
    }

    pub fn open_import_preview(
        &mut self,
        threads: usize,
        memory: usize,
        include_threads: bool,
        include_memory: bool,
        warnings: &[String],
    ) {
        self.picker = Some(Picker {
            kind: PickerKind::ImportPreview {
                threads: include_threads,
                memory: include_memory,
            },
            title: "Import · originals stay unchanged",
            options: vec![
                PickerOption::new(
                    format!(
                        "[{}] Threads · {threads}",
                        if include_threads { "x" } else { " " }
                    ),
                    "threads",
                ),
                PickerOption::new(
                    format!(
                        "[{}] Memory · {memory}",
                        if include_memory { "x" } else { " " }
                    ),
                    "memory",
                ),
                PickerOption::new("Import", "import"),
            ],
            selected: 2,
            query: None,
            viewport_offset: Cell::new(0),
        });
        if !warnings.is_empty() {
            self.show_info("Import notes", warnings.join("\n"));
        }
        self.set_notice("Enter toggles a category or starts Import · Escape cancels · repeat imports skip duplicates");
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
                        key_hint: None,
                        disabled: false,
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
        let model = self
            .transcript
            .config
            .as_ref()
            .and_then(|config| config.model.clone());
        self.open_effort_picker_for(provider, model.as_deref());
    }

    /// Opens effort choices for a provider selected in the model picker,
    /// before the asynchronous session-config event has updated the transcript.
    pub fn open_effort_picker_for(
        &mut self,
        provider: Option<CodingProvider>,
        model: Option<&str>,
    ) {
        let current = self
            .transcript
            .config
            .as_ref()
            .and_then(|config| config.effort.clone());
        let options = effort_picker_options(provider);
        let mut picker = Picker::new(
            PickerKind::Effort,
            "Choose effort",
            options.iter().copied(),
            current.as_deref(),
        );
        if provider == Some(CodingProvider::Codex) && model == Some("gpt-6-astra") {
            for option in &mut picker.options {
                option.disabled = option.value == "none";
            }
            if picker.options[picker.selected].disabled {
                picker.next();
            }
        }
        self.picker = Some(picker);
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

    pub fn open_ui_language_picker(&mut self) {
        self.picker = Some(Picker {
            kind: PickerKind::UiLanguage,
            title: self.tr("Interface language"),
            options: UiLanguage::ALL
                .into_iter()
                .map(|language| {
                    PickerOption::new(ui_text(self.ui_language, language.name()), language.code())
                })
                .collect(),
            selected: UiLanguage::ALL
                .iter()
                .position(|language| *language == self.ui_language)
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

    pub fn open_prevent_sleep_picker(&mut self, enabled: bool, lid: bool) {
        let current = match (enabled, lid) {
            (true, true) => PREVENT_SLEEP_LID,
            (true, false) => PREVENT_SLEEP_IDLE,
            (false, _) => PREVENT_SLEEP_OFF,
        };
        self.picker = Some(Picker::new(
            PickerKind::PreventSleep,
            "Keep machine awake while Borg works",
            [PREVENT_SLEEP_LID, PREVENT_SLEEP_IDLE, PREVENT_SLEEP_OFF],
            Some(current),
        ));
    }

    /// One-time macOS prompt: lid-close sleep can only be vetoed by root, so
    /// the user is asked once whether Borg may install a narrow sudo rule.
    pub fn open_lid_sleep_authorization_picker(&mut self) {
        self.picker = Some(Picker::new(
            PickerKind::LidSleepAuthorization,
            "Keep this Mac awake with the lid closed while Borg works? Needs admin approval once.",
            [LID_AUTH_AUTHORIZE, LID_AUTH_NOT_NOW, LID_AUTH_NEVER],
            Some(LID_AUTH_AUTHORIZE),
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
        let current = match self.transcript.diff_expansion {
            DiffExpansionPolicy::Expanded => "Expanded",
            DiffExpansionPolicy::Collapsed => "Collapsed",
            DiffExpansionPolicy::UntilNextAction => "Until next action",
        };
        self.picker = Some(Picker::new(
            PickerKind::AutoExpandEdits,
            "Edit diff display",
            ["Expanded", "Collapsed", "Until next action"],
            Some(current),
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

    pub fn open_auto_expand_thinking_picker(&mut self) {
        self.picker = Some(Picker::new(
            PickerKind::AutoExpandThinking,
            "Auto-expand thinking while it streams",
            ["On", "Off"],
            Some(if self.transcript.auto_expand_thinking {
                "On"
            } else {
                "Off"
            }),
        ));
    }

    pub fn open_tool_click_behavior_picker(&mut self) {
        self.picker = Some(Picker::new(
            PickerKind::ToolClickBehavior,
            "Tool click behavior",
            ["Fullscreen", "Inline"],
            Some(match self.tool_click_behavior {
                ToolClickBehavior::Fullscreen => "Fullscreen",
                ToolClickBehavior::Inline => "Inline",
            }),
        ));
    }

    pub fn open_action_descriptors_picker(&mut self) {
        self.picker = Some(Picker::new(
            PickerKind::ActionDescriptors,
            "Preparation descriptors before tools",
            ["On", "Off"],
            Some(if self.action_descriptors { "On" } else { "Off" }),
        ));
    }

    pub fn open_running_sweeps_picker(&mut self) {
        self.picker = Some(Picker::new(
            PickerKind::RunningSweeps,
            "Running sweep animations",
            ["On", "Off"],
            Some(if self.running_sweeps { "On" } else { "Off" }),
        ));
    }

    pub fn open_completion_notifications_picker(&mut self) {
        self.open_completion_alert_picker(
            PickerKind::CompletionNotifications,
            "Completion notifications",
            self.completion_notifications,
        );
    }

    pub fn open_luna_titles_for_all_providers_picker(&mut self) {
        self.picker = Some(Picker::new(
            PickerKind::LunaTitlesForAllProviders,
            "Send first prompt to Codex subscription for Luna titles",
            ["On", "Off"],
            Some(if self.luna_titles_for_all_providers {
                "On"
            } else {
                "Off"
            }),
        ));
    }

    pub fn open_auto_copy_selection_picker(&mut self) {
        self.picker = Some(Picker::new(
            PickerKind::AutoCopySelection,
            "Auto-copy mouse selections",
            ["On", "Off"],
            Some(if self.auto_copy_selection {
                "On"
            } else {
                "Off"
            }),
        ));
    }

    pub fn open_completion_sound_picker(&mut self) {
        self.open_completion_alert_picker(
            PickerKind::CompletionSound,
            "Completion sound",
            self.completion_sound,
        );
    }

    fn open_completion_alert_picker(
        &mut self,
        kind: PickerKind,
        title: &'static str,
        current: CompletionAlertPolicy,
    ) {
        self.picker = Some(Picker::new(
            kind,
            title,
            ["When unfocused", "Always", "Off"],
            Some(completion_alert_policy_label(current)),
        ));
    }

    pub fn set_running_sweeps(&mut self, enabled: bool) {
        self.running_sweeps = enabled;
    }

    pub fn set_action_descriptors(&mut self, enabled: bool) {
        self.action_descriptors = enabled;
        self.transcript.set_action_descriptors(enabled);
        self.invalidate_transcript_render_cache();
    }

    pub fn set_layout_preferences(
        &mut self,
        preferences: &borg_ui::preferences::LayoutPreferences,
    ) {
        self.horizontal_margin = preferences.horizontal_margin;
        self.composer_max_height = preferences.composer_max_height;
        self.show_footer = preferences.show_footer;
        self.invalidate_transcript_render_cache();
    }

    pub fn set_ui_language(&mut self, language: UiLanguage) {
        self.ui_language = language;
        self.invalidate_transcript_render_cache();
        self.event_redraw_needed = true;
    }

    fn tr<'a>(&self, english: &'a str) -> &'a str {
        ui_text(self.ui_language, english)
    }

    pub fn set_dictation_settings(&mut self, enabled: bool, model: Option<String>) {
        self.dictation_enabled = enabled;
        self.dictation_model = model;
    }

    /// Whether the platform publishes a GPU dictation runtime beyond the
    /// automatic default. Mirrors `DictationAccelerator::available_on_platform`
    /// without a dependency on the dictation crate: parakeet.cpp ships NVIDIA
    /// and Vulkan builds only for Linux and Windows on x86_64.
    fn dictation_gpu_runtime_available() -> bool {
        cfg!(all(
            any(target_os = "linux", target_os = "windows"),
            target_arch = "x86_64"
        ))
    }

    /// Open the enable-dictation flow: choose a model, then (where a GPU
    /// runtime exists) an accelerator, then the mic icon, then begin recording.
    pub fn open_enable_dictation_picker(&mut self) {
        self.dictation_enable_flow = true;
        self.pending_dictation_model = None;
        self.pending_dictation_accelerator = None;
        let selected = DICTATION_MODEL_OPTIONS
            .iter()
            .position(|(value, _)| *value == self.dictation_model_default())
            .unwrap_or(0);
        self.picker = Some(Picker {
            kind: PickerKind::DictationModel,
            title: "Enable dictation · choose a model",
            options: DICTATION_MODEL_OPTIONS
                .iter()
                .map(|(value, label)| PickerOption::new(*label, *value))
                .collect(),
            selected,
            query: None,
            viewport_offset: Cell::new(0),
        });
        self.notice = Some(
            "Local speech-to-text · the model downloads once, then transcription stays on-device"
                .into(),
        );
    }

    fn dictation_model_default(&self) -> &'static str {
        DICTATION_MODEL_OPTIONS
            .iter()
            .find(|(value, _)| Some(*value) == self.dictation_model.as_deref())
            .map_or(DICTATION_MODEL_OPTIONS[0].0, |(value, _)| *value)
    }

    fn open_dictation_accelerator_picker(&mut self) {
        self.picker = Some(Picker {
            kind: PickerKind::DictationAccelerator,
            title: "Enable dictation · choose an accelerator",
            options: DICTATION_ACCELERATOR_OPTIONS
                .iter()
                .map(|(value, label)| PickerOption::new(*label, *value))
                .collect(),
            selected: 0,
            query: None,
            viewport_offset: Cell::new(0),
        });
        self.notice = Some("NVIDIA/Vulkan builds run on your GPU; Automatic uses CPU here".into());
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

    pub fn set_diff_expansion(&mut self, policy: DiffExpansionPolicy) {
        if policy == DiffExpansionPolicy::Collapsed {
            self.capture_transcript_anchor_for_collapse();
        }
        self.transcript.set_diff_expansion(policy);
        self.invalidate_transcript_render_cache();
    }

    pub fn set_auto_expand_tools(&mut self, enabled: bool) {
        if !enabled {
            self.capture_transcript_anchor_for_collapse();
        }
        self.transcript.set_auto_expand_tools(enabled);
        self.invalidate_transcript_render_cache();
    }

    pub fn set_auto_expand_thinking(&mut self, enabled: bool) {
        if !enabled {
            self.capture_transcript_anchor_for_collapse();
        }
        self.transcript.set_auto_expand_thinking(enabled);
        self.invalidate_transcript_render_cache();
    }

    pub fn set_tool_click_behavior(&mut self, behavior: ToolClickBehavior) {
        self.tool_click_behavior = behavior;
        self.transcript.tool_click_behavior = behavior;
        self.invalidate_transcript_render_cache();
    }

    pub fn set_dictation_icon(&mut self, style: DictationIconStyle) {
        self.dictation_icon = style;
        self.event_redraw_needed = true;
    }

    pub fn set_transcript_labels(&mut self, user: String, assistant: String) {
        self.transcript.user_label = user;
        self.transcript.assistant_label = assistant;
        self.invalidate_transcript_render_cache();
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
        self.invalidate_transcript_render_cache();
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
                complete: true,
                ..
            }) => ("Message actions", vec!["Revert to here", "Copy message"]),
            // Not yet admitted by the provider, so there is nothing to revert to.
            Some(TranscriptEntry::Message {
                actor: EventActor::User,
                ..
            }) => ("Message actions", vec!["Copy message"]),
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
            Event::FocusGained => {
                self.window_focused = true;
                Ok(UiAction::None)
            }
            Event::FocusLost => {
                self.window_focused = false;
                Ok(UiAction::None)
            }
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
                self.ctrl_c_count = 0;
                self.composer_selection = None;
                let value = normalize_terminal_capture_paste(&value);
                self.notice = Some("Processing paste…".to_string());
                Ok(UiAction::TerminalIo(TerminalIoRequest::paste(
                    self.attachment_store.clone(),
                    value.into_owned(),
                    self.cwd.clone(),
                )))
            }
            Event::Mouse(mouse) => {
                let previous_hover = self.hover_state();
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
                self.shell_status_hovered = self
                    .shell_status_area
                    .is_some_and(|area| area.contains(pointer));
                self.hovered_shell_row = self
                    .shell_row_hit_areas
                    .iter()
                    .position(|(area, _)| area.contains(pointer));
                self.watch_status_hovered = self
                    .watch_status_area
                    .is_some_and(|area| area.contains(pointer));
                self.hovered_watch_row = self
                    .watch_row_hit_areas
                    .iter()
                    .position(|(area, _)| area.contains(pointer));
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
                self.git_status_hovered = self
                    .git_status_area
                    .is_some_and(|area| area.contains(pointer));
                self.git_commit_hovered = self
                    .git_commit_area
                    .is_some_and(|area| area.contains(pointer));
                self.git_pull_hovered = self
                    .git_pull_area
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
                    return Ok(UiAction::ToggleGoal {
                        action: GoalAction::Clear,
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
                        return Ok(self
                            .copy_transcript_entry_request(index)
                            .map_or(UiAction::None, UiAction::TerminalIo));
                    } else {
                        self.pending_tool_copy = Some(index);
                        return Ok(UiAction::LoadPayloads(payloads));
                    }
                }
                if matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left)) {
                    if self.dictation_button_hovered {
                        if self.dictation_enabled {
                            return Ok(UiAction::ToggleDictation);
                        }
                        self.open_enable_dictation_picker();
                        return Ok(UiAction::None);
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
                        if !self.active_queued_prompts().is_empty()
                            && self
                                .pending_input_header_area
                                .is_some_and(|area| area.contains(pointer))
                        {
                            self.pending_input_expanded = !self.pending_input_expanded;
                            return Ok(UiAction::None);
                        }
                        if self
                            .back_to_director_area
                            .is_some_and(|area| area.contains(pointer))
                        {
                            if self.focused_tool.is_some() {
                                self.close_tool_inspector();
                            } else {
                                self.focus_director_transcript();
                            }
                            return Ok(UiAction::None);
                        }
                        if self.status_area.is_some_and(|area| area.contains(pointer))
                            && status_control_is_actionable(self.active_status())
                        {
                            return Ok(if self.begin_user_interrupt() {
                                UiAction::Interrupt {
                                    target: self.focused_child,
                                }
                            } else {
                                UiAction::None
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
                            .shell_status_area
                            .is_some_and(|area| area.contains(pointer))
                        {
                            self.shell_menu_open = !self.shell_menu_open;
                            return Ok(UiAction::None);
                        }
                        if let Some(row) = self.hovered_shell_row
                            && let Some(tool_index) = self
                                .shell_row_hit_areas
                                .get(row)
                                .and_then(|(_, tool_index)| *tool_index)
                        {
                            return Ok(self.run_pending_transcript_click(
                                PendingTranscriptClick::Tool {
                                    index: tool_index,
                                    run: None,
                                },
                            ));
                        }
                        if self
                            .watch_status_area
                            .is_some_and(|area| area.contains(pointer))
                        {
                            self.watch_menu_open = !self.watch_menu_open;
                            return Ok(UiAction::None);
                        }
                        if let Some(row) = self.hovered_watch_row
                            && let Some((_, watch_id)) = self.watch_row_hit_areas.get(row)
                        {
                            return Ok(UiAction::StopWatch(*watch_id));
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
                        if self
                            .git_commit_area
                            .is_some_and(|area| area.contains(pointer))
                        {
                            self.commit_and_push_working_tree();
                            return Ok(UiAction::None);
                        }
                        if self
                            .git_status_area
                            .is_some_and(|area| area.contains(pointer))
                        {
                            self.push_unpushed_commits();
                            return Ok(UiAction::None);
                        }
                        if self
                            .git_pull_area
                            .is_some_and(|area| area.contains(pointer))
                        {
                            self.pull_upstream_commits();
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
                            if self.auto_copy_selection
                                && let Some(request) = self.copy_composer_selection_request()
                            {
                                return Ok(UiAction::TerminalIo(request));
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
                        if self.auto_copy_selection
                            && let Some(request) = self.copy_text_selection_request()
                        {
                            return Ok(UiAction::TerminalIo(request));
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
                        if let Some((start, max_offset)) = hovered_tool_run {
                            let terminal_height =
                                self.terminal.size().map(|size| size.height).unwrap_or(1);
                            self.queue_nested_wheel_scroll(
                                start,
                                max_offset,
                                -nested_wheel_scroll_distance(terminal_height, scroll_repetitions),
                            );
                        } else {
                            self.history_page_requested = self.focused_tool.is_none();
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
                        self.history_page_requested = false;
                        if let Some((start, max_offset)) = hovered_tool_run {
                            let terminal_height =
                                self.terminal.size().map(|size| size.height).unwrap_or(1);
                            self.queue_nested_wheel_scroll(
                                start,
                                max_offset,
                                nested_wheel_scroll_distance(terminal_height, scroll_repetitions),
                            );
                        } else {
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

    pub fn set_luna_titles_for_all_providers(&mut self, enabled: bool) {
        self.luna_titles_for_all_providers = enabled;
    }

    pub fn set_auto_copy_selection(&mut self, enabled: bool) {
        self.auto_copy_selection = enabled;
    }

    pub fn set_completion_alerts(
        &mut self,
        notifications: CompletionAlertPolicy,
        sound: CompletionAlertPolicy,
    ) {
        self.completion_notifications = notifications;
        self.completion_sound = sound;
    }

    fn send_completion_alert(&mut self, notification: bool, sound: bool) {
        if notification {
            let sequence = desktop_notification_sequence("Borg Agent", "Finished working");
            let _ = write!(self.terminal.backend_mut(), "{sequence}");
        }
        // Play the chime without holding up the TUI. A terminal bell has no
        // per-play volume control, so a missing sound player stays silent.
        if sound {
            let _ = play_system_completion_sound();
        }
        let _ = io::Write::flush(self.terminal.backend_mut());
    }

    /// Emit one desktop notification so the host terminal asks the OS for
    /// notification permission the first time Borg runs. On macOS this is what
    /// surfaces the "<terminal> wants to send notifications" prompt; a CLI
    /// process cannot request it directly. Returns whether the sequence was
    /// written.
    pub fn prime_desktop_notification(&mut self) -> bool {
        let sequence = desktop_notification_sequence(
            "Borg Agent",
            "Notifications are on \u{2014} you'll be alerted when a turn finishes.",
        );
        let written = write!(self.terminal.backend_mut(), "{sequence}").is_ok();
        let _ = io::Write::flush(self.terminal.backend_mut());
        written
    }

    pub fn advance_scroll_frame(&mut self) {
        self.advance_nested_scroll_frame();
        let scroll_was_active = self.scroll_motion.is_active();
        self.scroll_from_bottom = self.scroll_motion.advance_at(
            self.scroll_from_bottom,
            self.transcript_scroll_max,
            Instant::now(),
        );
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

    fn advance_nested_scroll_frame(&mut self) {
        let Some(mut nested) = self.nested_scroll_motion.take() else {
            return;
        };
        let current = self
            .transcript
            .tool_run_offset(nested.tool_run_start, nested.max_offset);
        // Inner scrolling invalidates the render cache, so coalesce the whole
        // pending input into one frame.
        let (next, handoff) =
            nested_scroll_handoff(current, nested.max_offset, nested.motion.take_pending());
        if next != current {
            let delta = if next >= current {
                isize::try_from(next - current).unwrap_or(isize::MAX)
            } else {
                -isize::try_from(current - next).unwrap_or(isize::MAX)
            };
            self.transcript
                .scroll_tool_run(nested.tool_run_start, nested.max_offset, delta);
            self.invalidate_transcript_render_cache();
        }
        if handoff != 0 {
            // Inner offsets grow downward; transcript offsets grow upward.
            // Convert the handed-off input to the outer viewport's normal
            // wheel speed so the transcript keeps its usual momentum.
            let terminal_height = self.terminal.size().map(|size| size.height).unwrap_or(1);
            let viewport_height = self.transcript_viewport_area.map_or(1, |area| area.height);
            let lines = handoff
                .unsigned_abs()
                .saturating_mul(wheel_scroll_lines(viewport_height) as usize)
                .div_ceil(nested_wheel_scroll_lines(terminal_height) as usize);
            let lines = isize::try_from(lines).unwrap_or(isize::MAX);
            self.history_page_requested = handoff < 0 && self.focused_tool.is_none();
            if handoff < 0 {
                self.transcript.follow_tail = false;
            }
            self.scroll_motion
                .push(if handoff < 0 { lines } else { -lines });
        }
    }

    pub fn has_pending_scroll_frame(&self) -> bool {
        self.image_scroll_settles_at
            .is_some_and(|deadline| Instant::now() < deadline)
            || self.scroll_motion.is_active()
            || self
                .nested_scroll_motion
                .as_ref()
                .is_some_and(|nested| nested.motion.is_active())
            || self
                .text_selection
                .is_some_and(|selection| selection.dragging && selection.autoscroll != 0)
    }

    fn queue_wheel_scroll(&mut self, lines: isize) {
        self.advance_nested_scroll_frame();
        if self.image_picker.is_some() {
            self.image_scroll_settles_at = Some(Instant::now() + Duration::from_millis(75));
        }
        self.history_page_requested = lines > 0 && self.focused_tool.is_none();
        if lines > 0 {
            self.transcript.follow_tail = false;
        } else if lines < 0 && self.scroll_from_bottom == 0 {
            self.transcript.follow_tail = true;
        }
        self.scroll_motion.push(lines);
    }

    fn queue_nested_wheel_scroll(&mut self, start: usize, max_offset: usize, lines: isize) {
        if self
            .nested_scroll_motion
            .as_ref()
            .is_some_and(|nested| nested.tool_run_start != start)
        {
            self.advance_nested_scroll_frame();
        }
        if self.scroll_motion.remaining_lines.signum() == lines.signum() {
            self.scroll_motion.cancel();
        }
        let nested = self
            .nested_scroll_motion
            .get_or_insert_with(|| NestedScrollMotion {
                tool_run_start: start,
                max_offset,
                motion: ScrollMotion::default(),
            });
        nested.max_offset = max_offset;
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
                return UiAction::TerminalIo(TerminalIoRequest::open_link(url));
            }
            PendingTranscriptClick::ToolRunHeader(start) => {
                if self.transcript.tool_run_expanded(start) {
                    self.capture_transcript_anchor_for_collapse();
                }
                self.nested_scroll_motion = None;
                self.transcript.toggle_tool_run_expansion(start);
                self.invalidate_transcript_render_cache();
            }
            PendingTranscriptClick::Tool { index, run } => {
                self.nested_scroll_motion = None;
                if let Some((start, max_offset)) = run {
                    self.transcript.anchor_tool_run(start, max_offset);
                }
                let payloads = match self.tool_click_behavior {
                    ToolClickBehavior::Fullscreen => self.open_tool_inspector(index),
                    ToolClickBehavior::Inline => {
                        if self.transcript.tool_is_expanded(index) {
                            self.capture_transcript_anchor_for_collapse();
                        }
                        let payloads = self.transcript.toggle_tool(index);
                        self.invalidate_transcript_render_cache();
                        payloads
                    }
                };
                if !payloads.is_empty() {
                    return UiAction::LoadPayloads(payloads);
                }
            }
            PendingTranscriptClick::Message(index) => {
                return self.open_entry_actions(index);
            }
            PendingTranscriptClick::Entry(index) => {
                if self.tool_click_behavior == ToolClickBehavior::Fullscreen
                    && (self.transcript.compaction_is_expandable(index)
                        || self.transcript.action_is_expandable(index)
                        || self.transcript.plan_is_clippable(index))
                {
                    self.open_tool_inspector(index);
                } else if self.transcript.compaction_is_expandable(index) {
                    self.capture_transcript_anchor_for_collapse();
                    self.transcript.toggle_compaction_expansion(index);
                    self.invalidate_transcript_render_cache();
                } else if self.transcript.action_is_expandable(index) {
                    self.capture_transcript_anchor_for_collapse();
                    self.transcript.toggle_action_expansion(index);
                    self.invalidate_transcript_render_cache();
                } else if self.transcript.plan_is_clippable(index) {
                    self.transcript.toggle_plan_expansion(index);
                    self.invalidate_transcript_render_cache();
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
            self.invalidate_transcript_render_cache();
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
        let render = self.active_transcript_render.as_ref()?;
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
        let Some(render) = self.active_transcript_render.as_ref() else {
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

    fn copy_text_selection_request(&self) -> Option<TerminalIoRequest> {
        if let Some(request) = self.copy_composer_selection_request() {
            return Some(request);
        }
        let render = self.active_transcript_render.as_ref()?;
        let (start, end) = self
            .text_selection
            .filter(|selection| !selection.is_empty())
            .and_then(|selection| resolved_selection_in_lines(selection, &render.6, &render.0))?;
        let text = selected_transcript_text(&render.0, start, end)?;
        Some(TerminalIoRequest::copy(
            text,
            "✓ Copied selection to clipboard",
        ))
    }

    fn copy_composer_selection_request(&self) -> Option<TerminalIoRequest> {
        let selection = self.composer_selection.filter(|selection| {
            !selection.is_empty()
                && selection.anchor <= self.composer.text.len()
                && selection.focus <= self.composer.text.len()
        })?;
        let (start, end) = if selection.anchor <= selection.focus {
            (selection.anchor, selection.focus)
        } else {
            (selection.focus, selection.anchor)
        };
        let text = self
            .composer
            .text
            .get(start..end)
            .filter(|text| !text.is_empty())?;
        Some(TerminalIoRequest::copy(
            text.to_string(),
            "✓ Copied composer selection to clipboard",
        ))
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

    fn open_thread_find(&mut self) {
        let pattern = self
            .thread_find
            .as_ref()
            .map(|find| find.pattern.as_str())
            .unwrap_or_default();
        self.composer.replace_text(format!(
            "/find{}",
            if pattern.is_empty() {
                " ".to_string()
            } else {
                format!(" {pattern}")
            }
        ));
        self.composer_selection = None;
        self.notice = Some("Enter a regex and press Enter · repeat to find next".to_string());
    }

    fn find_in_thread(&mut self, requested_pattern: &str) {
        let requested_pattern = requested_pattern.trim();
        let pattern = if requested_pattern.is_empty() {
            self.thread_find
                .as_ref()
                .map(|find| find.pattern.clone())
                .unwrap_or_default()
        } else {
            requested_pattern.to_string()
        };
        if pattern.is_empty() {
            self.notice = Some("Usage: /find <regex>".to_string());
            return;
        }
        let regex = match Regex::new(&pattern) {
            Ok(regex) => regex,
            Err(error) => {
                self.notice = Some(format!("Invalid regex: {error}"));
                return;
            }
        };
        let Some(render) = self.active_transcript_render.as_ref() else {
            self.notice = Some("Nothing in this thread to search yet".to_string());
            return;
        };
        let matches = thread_find_matches(&regex, &render.0);
        if matches.is_empty() {
            self.notice = Some(format!("No matches for /{pattern}/"));
            return;
        }
        let previous_row = self
            .thread_find
            .as_ref()
            .filter(|find| find.pattern == pattern)
            .map(|find| find.row);
        let (row, position) = next_thread_match(&matches, previous_row);
        let viewport_height = self
            .transcript_viewport_area
            .map_or(1, |area| usize::from(area.height).max(1));
        let scroll_start = row
            .saturating_sub(viewport_height / 2)
            .min(self.transcript_scroll_max);
        self.scroll_from_bottom = self.transcript_scroll_max.saturating_sub(scroll_start);
        self.transcript.follow_tail = false;
        self.history_page_requested = false;
        self.thread_find = Some(ThreadFindState {
            pattern: pattern.clone(),
            row,
        });
        self.notice = Some(format!(
            "Match {position}/{} for /{pattern}/ · repeat /find to continue",
            matches.len()
        ));
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

    fn copy_transcript_entry_request(&self, index: usize) -> Option<TerminalIoRequest> {
        let text = self
            .transcript
            .order
            .get(index)
            .and_then(TranscriptEntry::copy_text_owned)?;
        Some(TerminalIoRequest::copy(text, "✓ Copied to clipboard"))
    }

    fn copy_last_assistant_message_request(&mut self) -> Option<TerminalIoRequest> {
        let Some(text) = self.transcript.last_assistant_message_text() else {
            self.notice = Some("No assistant message is available to copy".to_string());
            return None;
        };
        Some(TerminalIoRequest::copy(
            text,
            "✓ Copied last assistant message to clipboard",
        ))
    }

    fn show_copy_notice(&mut self, notice: impl Into<String>) {
        self.notice = Some(notice.into());
        self.copy_notice_expires_at = Some(Instant::now() + COPY_NOTICE_DURATION);
    }

    fn rewind_action_for_output(&mut self, index: usize) -> UiAction {
        let Some(target) = self
            .transcript
            .message_id_at(index)
            .and_then(|message_id| {
                self.rewind_targets
                    .iter()
                    .find(|target| target.message_id == message_id)
            })
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
            Some(selected) if selected.starts_with("Copy ") => self
                .copy_transcript_entry_request(index)
                .map_or(UiAction::None, UiAction::TerminalIo),
            _ => UiAction::None,
        }
    }

    fn run_selected_picker(&mut self) -> Result<UiAction> {
        if self
            .picker
            .as_ref()
            .is_some_and(|picker| picker.selected_position().is_none())
        {
            return Ok(UiAction::None);
        }
        if self
            .picker
            .as_ref()
            .and_then(|picker| picker.options.get(picker.selected))
            .is_some_and(|option| option.disabled)
        {
            return Ok(UiAction::None);
        }
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
            PickerKind::ImportSource => {
                let source = picker.selected_value();
                if matches!(source.as_str(), "claude-desktop" | "portable") {
                    self.composer
                        .restore(format!("/import {source} --path "), Vec::new());
                    self.set_notice("Add the export ZIP or JSON path, then send. Claude Desktop: Settings > Privacy > Export data.");
                    UiAction::None
                } else {
                    UiAction::Submit {
                        target: None,
                        text: format!("/import {source}"),
                        attachments: Vec::new(),
                    }
                }
            }
            PickerKind::ImportPreview {
                mut threads,
                mut memory,
            } => {
                if picker.selected < 2 {
                    let mut picker = picker;
                    if picker.selected == 0 {
                        threads = !threads;
                    } else {
                        memory = !memory;
                    }
                    let enabled = if picker.selected == 0 {
                        threads
                    } else {
                        memory
                    };
                    picker.options[picker.selected]
                        .label
                        .replace_range(1..2, if enabled { "x" } else { " " });
                    picker.kind = PickerKind::ImportPreview { threads, memory };
                    self.picker = Some(picker);
                    UiAction::None
                } else if threads || memory {
                    UiAction::Submit {
                        target: None,
                        text: format!("/import-confirm {} {}", threads, memory),
                        attachments: Vec::new(),
                    }
                } else {
                    self.picker = Some(picker);
                    self.set_notice("Select Threads, Memory, or both.");
                    UiAction::None
                }
            }
            PickerKind::Commands => unreachable!("handled above"),
            PickerKind::Settings if picker.options[picker.selected].value == "luna-titles" => {
                self.open_luna_titles_for_all_providers_picker();
                UiAction::None
            }
            PickerKind::Settings if picker.options[picker.selected].value == "auto-copy" => {
                self.open_auto_copy_selection_picker();
                UiAction::None
            }
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
            PickerKind::Model => match picker.selected_value().as_str() {
                "/connect-go" => {
                    self.open_provider_auth_picker(CodingProvider::OpenCode, String::new());
                    UiAction::None
                }
                "/login" => UiAction::Submit {
                    target: None,
                    text: "/login".to_string(),
                    attachments: Vec::new(),
                },
                "/model-custom" => {
                    self.composer.replace_text("/model ".to_string());
                    UiAction::None
                }
                model => UiAction::SetModel(model.to_string()),
            },
            PickerKind::ProviderAuth => {
                let choice = match picker.selected_value().as_str() {
                    "subscription" => Some(ProviderAuthChoice::Subscription),
                    "api-key" => Some(ProviderAuthChoice::ApiKey),
                    "reconnect-subscription" => Some(ProviderAuthChoice::ReconnectSubscription),
                    "replace-api-key" => Some(ProviderAuthChoice::ReplaceApiKey),
                    _ => None,
                };
                let model = self.pending_auth_model.take();
                match (choice, model) {
                    (Some(choice), Some((provider, model))) => UiAction::AuthenticateProvider {
                        provider,
                        model,
                        choice,
                    },
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
            PickerKind::UiLanguage => UiAction::SetUiLanguage(
                UiLanguage::parse(&picker.selected_value())
                    .expect("UI language picker values are canonical"),
            ),
            PickerKind::Fast => UiAction::SetFast(picker.selected_value() == "On"),
            PickerKind::RefreshRate => UiAction::SetRefreshRate(
                picker
                    .selected_value()
                    .parse()
                    .expect("FPS options are numeric"),
            ),
            PickerKind::PreventSleep => {
                let value = picker.selected_value();
                UiAction::SetPreventSleep {
                    enabled: value != PREVENT_SLEEP_OFF,
                    lid: value == PREVENT_SLEEP_LID,
                }
            }
            PickerKind::LidSleepAuthorization => {
                UiAction::LidSleepAuthorization(match picker.selected_value().as_str() {
                    LID_AUTH_AUTHORIZE => LidSleepAuthorizationChoice::Authorize,
                    LID_AUTH_NEVER => LidSleepAuthorizationChoice::Never,
                    _ => LidSleepAuthorizationChoice::NotNow,
                })
            }
            PickerKind::ActiveMessages => {
                UiAction::SetSteerActive(picker.selected_value() == ACTIVE_MESSAGES_SEND_NOW)
            }
            PickerKind::AutoExpandEdits => {
                UiAction::SetDiffExpansion(match picker.selected_value().as_str() {
                    "Expanded" => DiffExpansionPolicy::Expanded,
                    "Collapsed" => DiffExpansionPolicy::Collapsed,
                    "Until next action" => DiffExpansionPolicy::UntilNextAction,
                    _ => unreachable!("diff expansion picker values are canonical"),
                })
            }
            PickerKind::AutoExpandTools => {
                UiAction::SetAutoExpandTools(picker.selected_value() == "On")
            }
            PickerKind::AutoExpandThinking => {
                UiAction::SetAutoExpandThinking(picker.selected_value() == "On")
            }
            PickerKind::ToolClickBehavior => {
                UiAction::SetToolClickBehavior(if picker.selected_value() == "Inline" {
                    ToolClickBehavior::Inline
                } else {
                    ToolClickBehavior::Fullscreen
                })
            }
            PickerKind::ActionDescriptors => {
                UiAction::SetActionDescriptors(picker.selected_value() == "On")
            }
            PickerKind::RunningSweeps => {
                UiAction::SetRunningSweeps(picker.selected_value() == "On")
            }
            PickerKind::CompletionNotifications => UiAction::SetCompletionNotifications(
                completion_alert_policy_from_picker(&picker.selected_value()),
            ),
            PickerKind::CompletionSound => UiAction::SetCompletionSound(
                completion_alert_policy_from_picker(&picker.selected_value()),
            ),
            PickerKind::LunaTitlesForAllProviders => {
                UiAction::SetLunaTitlesForAllProviders(picker.selected_value() == "On")
            }
            PickerKind::AutoCopySelection => {
                UiAction::SetAutoCopySelection(picker.selected_value() == "On")
            }
            PickerKind::DictationModel => {
                self.pending_dictation_model = Some(picker.selected_value());
                // Only offer the accelerator step where a GPU runtime exists;
                // otherwise Automatic is the only choice.
                if Self::dictation_gpu_runtime_available() {
                    self.open_dictation_accelerator_picker();
                } else {
                    self.pending_dictation_accelerator = Some("auto".to_string());
                    self.open_dictation_icon_picker();
                }
                UiAction::None
            }
            PickerKind::DictationAccelerator => {
                self.pending_dictation_accelerator = Some(picker.selected_value());
                self.open_dictation_icon_picker();
                UiAction::None
            }
            PickerKind::DictationIcon => {
                let icon = match picker.selected_value().as_str() {
                    "nerd_font" => DictationIconStyle::NerdFont,
                    "emoji" => DictationIconStyle::Emoji,
                    _ => self.dictation_icon,
                };
                if self.dictation_enable_flow {
                    self.dictation_enable_flow = false;
                    let model = self
                        .pending_dictation_model
                        .take()
                        .unwrap_or_else(|| DICTATION_MODEL_OPTIONS[0].0.to_string());
                    let accelerator = self
                        .pending_dictation_accelerator
                        .take()
                        .unwrap_or_else(|| "auto".to_string());
                    self.dictation_enabled = true;
                    self.dictation_model = Some(model.clone());
                    UiAction::EnableDictation {
                        model,
                        accelerator,
                        icon,
                    }
                } else {
                    UiAction::SetDictationIcon(icon)
                }
            }
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

    /// Paint interaction feedback without laying out transcript history.
    /// After the first committed frame this path is bounded by the visible
    /// terminal surface, even while newer transcript content awaits reflow.
    pub fn draw_for_interaction(&mut self) -> Result<()> {
        self.draw_internal(true)
    }

    /// Draw activity animation from the last committed transcript viewport.
    /// Session events still use [`Self::draw`] so new transcript content is
    /// committed before subsequent animation frames reuse it.
    pub fn draw_for_activity(&mut self) -> Result<()> {
        self.draw_internal(true)
    }

    fn draw_internal(&mut self, input_fast_path: bool) -> Result<()> {
        if self.transcript.tool_click_behavior != self.tool_click_behavior {
            self.transcript.tool_click_behavior = self.tool_click_behavior;
            self.invalidate_transcript_render_cache();
        }
        // Child and resumed transcripts are built fresh; whichever one is shown
        // must size previews for the graphics protocol that will draw them, or
        // it falls back to glyph tiles under a stretched graphics image.
        let image_cell = image_preview_cell(self.image_picker.as_ref());
        if self.transcript.image_cell() != image_cell {
            self.transcript.set_image_cell(image_cell);
            self.invalidate_transcript_render_cache();
        }
        self.drain_git_push_results();
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
        let cwd = self
            .transcript
            .config
            .as_ref()
            .map(|config| config.cwd.as_path())
            .unwrap_or(&self.cwd);
        let home = std::env::var_os("HOME").map(PathBuf::from);
        let title = terminal_title(cwd, home.as_deref());
        let title = if matches!(
            self.status,
            SessionStatus::Starting | SessionStatus::Running
        ) {
            format!("{} {title}", activity_glyph(self.status))
        } else {
            title
        };
        if self.last_terminal_title.as_deref() != Some(&title) {
            execute!(self.terminal.backend_mut(), SetTitle(&title))?;
            self.last_terminal_title = Some(title);
        }
        let terminal_size = self.terminal.size()?;
        let content_width = terminal_content_width(terminal_size.width);
        let tool_run_viewport_height = tool_run_viewport_height(terminal_size.height as usize);
        // Frozen to the last committed frame during an input-only redraw so the
        // committed-snapshot lookup below is keyed to the width that snapshot
        // was actually rendered at.
        let full_transcript_width = transcript_frame_width(
            content_width,
            input_fast_path,
            self.last_committed_viewport_render
                .as_ref()
                .map(|(width, ..)| *width),
        );
        if !input_fast_path {
            // This draw replaces both snapshots. Releasing them first leaves the
            // transcript sole owner of its last render, so it can redraw only
            // the changed tail in place instead of copying every row.
            self.active_transcript_render = None;
            self.last_committed_viewport_render = None;
        }
        let goal_tick = self.transcript.active_goal_cache_tick();
        let tool_elapsed_tick = self.transcript.tool_elapsed_cache_tick();
        let render_time = Utc::now();
        let refresh_reasoning_summary = if input_fast_path {
            let phases = self.visible_reasoning_summary_phases_at(render_time);
            let changed = !phases.is_empty() && phases != self.last_reasoning_summary_phases;
            self.last_reasoning_summary_phases = phases;
            changed
        } else {
            false
        };
        if refresh_reasoning_summary {
            self.invalidate_transcript_render_cache();
        }
        let current_tool_elapsed = self.transcript.running_tool_elapsed_labels_at(render_time);
        let local_date = Local::now().date_naive();
        let committed_viewport_render = if input_fast_path {
            self.last_committed_viewport_render
                .as_ref()
                .filter(|cached| {
                    committed_viewport_is_reusable(
                        cached,
                        full_transcript_width,
                        tool_run_viewport_height,
                        &current_tool_elapsed,
                    )
                })
                .map(|(_, _, _, _, _, render)| Arc::clone(render))
        } else {
            None
        };
        let using_committed_snapshot = committed_viewport_render.is_some();
        let transcript_snapshot_current =
            self.transcript_render_cache.is_some() || using_committed_snapshot;
        let stale_full_transcript_render = if input_fast_path && transcript_snapshot_current {
            self.transcript_full_render_cache
                .as_ref()
                .filter(
                    |(
                        cached_width,
                        cached_tool_run_viewport_height,
                        cached_goal_tick,
                        _,
                        cached_date,
                        render,
                    )| {
                        *cached_width == full_transcript_width
                            && *cached_tool_run_viewport_height == tool_run_viewport_height
                            && *cached_date == local_date
                            && *cached_goal_tick == goal_tick
                            && tool_elapsed_widths_match(&render.7, &current_tool_elapsed)
                    },
                )
                .map(|(_, _, _, _, _, render)| Arc::clone(render))
        } else {
            None
        };
        let full_transcript_render = select_transcript_snapshot(
            input_fast_path,
            transcript_snapshot_current,
            committed_viewport_render,
            || {
                if let Some(index) = self.focused_tool {
                    return Arc::new(self.transcript.render_tool_for_cache_at(
                        index,
                        full_transcript_width,
                        tool_run_viewport_height,
                        render_time,
                    ));
                }
                stale_full_transcript_render.unwrap_or_else(|| {
                    if self.transcript_render_cache.is_some() {
                        cached_transcript_render(
                            &self.transcript,
                            &mut self.transcript_full_render_cache,
                            full_transcript_width,
                            tool_run_viewport_height,
                            goal_tick,
                            &current_tool_elapsed,
                            local_date,
                            render_time,
                        )
                    } else {
                        self.transcript_full_render_cache = None;
                        cached_transcript_render(
                            &self.transcript,
                            &mut self.transcript_full_render_cache,
                            full_transcript_width,
                            tool_run_viewport_height,
                            goal_tick,
                            &current_tool_elapsed,
                            local_date,
                            render_time,
                        )
                    }
                })
            },
        );
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
        let reconnect_label = self
            .connection_retry_at
            .or(self.usage_retry_at)
            .filter(|_| self.focused_child.is_none())
            .map(|deadline| {
                let seconds = (deadline - Utc::now()).num_seconds().max(0);
                if self.usage_retry_at.is_some() {
                    format!(
                        "resumes {}",
                        deadline.with_timezone(&Local).format("%a %H:%M")
                    )
                } else if let Some((attempt, max_attempts)) = self.connection_retry_attempt {
                    // The bound is what makes the countdown honest: a session is
                    // never waiting on an attempt that cannot be the last one
                    // without saying so.
                    if seconds > 0 {
                        format!("retry {attempt}/{max_attempts} in {seconds}s")
                    } else {
                        format!("retry {attempt}/{max_attempts}")
                    }
                } else if seconds > 0 {
                    format!("retry in {seconds}s")
                } else {
                    "reconnecting".to_string()
                }
            });
        let status_label = if self.interrupt_requested && status_control_is_actionable(status) {
            "stopping"
        } else if let Some(label) = reconnect_label.as_deref() {
            label
        } else if self.borging_this_run
            && matches!(status, SessionStatus::Starting | SessionStatus::Running)
        {
            "borging"
        } else if status == SessionStatus::Ready && self.transcript.watch_status().is_some() {
            "waiting"
        } else {
            self.transcript.status_label(status)
        };
        let status_glyph = activity_glyph(status);
        let status_is_interruptible = status_control_is_actionable(status);
        let ConfigStatuses {
            model: model_status,
            effort: effort_status,
            fast: fast_status,
            permission: permission_status,
            billing: billing_status,
            cwd: mut cwd_status,
        } = self.transcript.config_statuses();
        let active_cwd = self
            .transcript
            .config
            .as_ref()
            .map(|config| config.cwd.clone())
            .unwrap_or_else(|| self.cwd.clone());
        if cwd_status.is_empty() {
            cwd_status = fish_style_path(&active_cwd);
        }
        let mut footer_git_status = None;
        if let Some(git_status) = self.git_status_cache.status_for(&active_cwd) {
            cwd_status.push_str(" · ");
            cwd_status.push_str(&git_status.compact_label());
            footer_git_status = Some(git_status.clone());
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
        let total_subagents = agent_roster_entries.len().saturating_sub(1);
        let session_is_active = matches!(status, SessionStatus::Starting | SessionStatus::Running);
        let activity_clock = self.active_activity_clock();
        let active_goal = self.active_goal().cloned();
        let goal_status = self.transcript.goal_status();
        let shell_status = self.transcript.shell_status();
        let shell_rows = self.transcript.active_shell_rows();
        if shell_rows.is_empty() {
            self.shell_menu_open = false;
        }
        let watch_status = self.transcript.watch_status();
        let watch_rows = self.transcript.watch_rows();
        if watch_rows.is_empty() {
            self.watch_menu_open = false;
        }
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
        } else if self.picker.is_some() {
            "↑↓ select · enter confirm · esc cancel".to_string()
        } else {
            primary_controls_line(&self.keymap, self.ui_language)
        };
        let interaction_hint = bottom_interaction_hint(BottomInteractionHintState {
            status_hovered: self.status_hovered,
            status_is_interruptible,
            goal_status_hovered: self.goal_status_hovered,
            goal_available: active_goal.is_some(),
            shell_status_hovered: self.shell_status_hovered,
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
            if self.picker.is_some() {
                vec![Line::from(primary_controls.clone())]
            } else if let Some(notice) = copy_notice_text.as_ref() {
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
                    let mut spans = vec![
                        Span::styled(hint, Style::default().fg(Color::Yellow)),
                        Span::raw(" · "),
                    ];
                    if resume_picker_open {
                        spans.push(Span::raw(primary_controls.clone()));
                    } else {
                        spans.extend(primary_controls_spans(&self.keymap, self.ui_language));
                    }
                    Line::from(spans)
                } else if resume_picker_open {
                    Line::from(primary_controls.clone())
                } else {
                    Line::from(primary_controls_spans(&self.keymap, self.ui_language))
                }]
            }
        });
        let controls = if is_launch_screen {
            controls
        } else {
            inset_control_lines(controls)
        };
        let controls_height = controls.len().min(u16::MAX as usize) as u16;
        let footer_height = if !self.show_footer {
            0
        } else if is_launch_screen {
            1
        } else {
            controls_height
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
        let ui_language = self.ui_language;
        let mut composer_render_lines = if self.picker.as_ref().is_some_and(|_| !modal_picker_open)
        {
            Vec::new()
        } else if self.composer.text.is_empty() {
            let placeholder = if pending_provider_interaction {
                "Answer the provider request…"
            } else {
                match status {
                    SessionStatus::Running | SessionStatus::Starting => ui_text(
                        ui_language,
                        active_message_placeholder(self.steer_active_turn),
                    ),
                    SessionStatus::WaitingForApproval => "Allow · Y   Deny · N",
                    _ => ui_text(ui_language, "Describe a task…"),
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
        let composer_max_height = if resume_picker_open
            || self
                .picker
                .as_ref()
                .is_some_and(|picker| picker.kind == PickerKind::Model)
        {
            18.max(self.composer_max_height)
        } else {
            self.composer_max_height
        };
        let composer_height = composer_panel_height(
            composer_line_count,
            composer_cursor.0,
            usize::from(composer_max_height),
            is_launch_screen && resume_picker_open,
        )
        .saturating_add(u16::from(!is_launch_screen));
        let composer_height = if is_launch_screen {
            bounded_launch_composer_height(composer_height, terminal_size.height, controls_height)
        } else {
            composer_height
        };
        let composer_border_rows = if is_launch_screen { 1 } else { 2 };
        let composer_scroll = if let Some(picker) = self.picker.as_ref().filter(|picker| {
            !matches!(
                picker.kind,
                PickerKind::Commands | PickerKind::MessageActions | PickerKind::Goal
            )
        }) {
            let content_height = usize::from(composer_height.saturating_sub(composer_border_rows));
            picker.scroll_offset(content_height, composer_line_count) as u16
        } else {
            (composer_cursor.0 as u16)
                .saturating_sub(composer_height.saturating_sub(composer_border_rows + 1))
        };
        let transcript_viewport_height = if is_launch_screen {
            0
        } else {
            let area = centered_content_area_with_margin(
                Rect::new(0, 0, terminal_size.width, terminal_size.height),
                self.horizontal_margin,
            );
            let chunks = terminal_vertical_chunks(
                area,
                queued_prompt_panel_height(
                    &queued_prompts,
                    area.width,
                    self.pending_input_expanded,
                ),
                composer_height,
                footer_height,
                is_launch_screen,
            );
            usize::from(chunks[0].height)
        };
        let transcript_width = transcript_width_for_viewport(
            content_width,
            full_transcript_render.0.len(),
            transcript_viewport_height,
        );
        let transcript_render =
            if reuse_current_transcript_width(input_fast_path, transcript_snapshot_current)
                && transcript_width == full_transcript_width
            {
                Arc::clone(&full_transcript_render)
            } else if transcript_width == full_transcript_width {
                if self.focused_tool.is_none() {
                    self.transcript_render_cache = self.transcript_full_render_cache.clone();
                }
                Arc::clone(&full_transcript_render)
            } else if let Some(index) = self.focused_tool {
                Arc::new(self.transcript.render_tool_for_cache_at(
                    index,
                    transcript_width,
                    tool_run_viewport_height,
                    render_time,
                ))
            } else {
                cached_transcript_render(
                    &self.transcript,
                    &mut self.transcript_render_cache,
                    transcript_width,
                    tool_run_viewport_height,
                    goal_tick,
                    &current_tool_elapsed,
                    local_date,
                    render_time,
                )
            };
        self.active_transcript_render = Some(Arc::clone(&transcript_render));
        let (
            transcript,
            tool_rows,
            tool_run_rows,
            message_rows,
            entry_rows,
            link_rows,
            selection_rows,
            cached_tool_elapsed,
        ) = transcript_render.as_ref();
        let transcript_height = transcript.len();
        self.scroll_from_bottom = resolve_pending_scroll_anchor(
            self.transcript.follow_tail,
            self.scroll_from_bottom,
            if using_committed_snapshot {
                None
            } else {
                self.pending_scroll_anchor_height.take()
            },
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
        let mut next_pending_input_header_area = None;
        let mut next_status_area = None;
        let mut next_goal_status_area = None;
        let mut next_todo_status_area = None;
        let mut next_shell_status_area = None;
        let mut next_agents_status_area = None;
        let mut next_model_status_area = None;
        let mut next_effort_status_area = None;
        let mut next_context_status_area = None;
        let mut next_fast_status_area = None;
        let mut next_permission_status_area = None;
        let mut next_team_roster_hit_areas = Vec::new();
        let mut next_shell_row_hit_areas = Vec::new();
        let mut next_watch_status_area = None;
        let mut next_watch_row_hit_areas: Vec<(Rect, Uuid)> = Vec::new();
        let mut next_back_to_director_area = None;
        let mut next_keybindings_hint_area = None;
        let mut next_dictation_button_area = None;
        let dictation_state = self.dictation_state;
        let dictation_button_hovered = self.dictation_button_hovered;
        let pending_transcript_anchor = if using_committed_snapshot {
            None
        } else {
            self.pending_transcript_anchor.take()
        };
        let mut restored_scroll_from_bottom = None;
        let cursor_visible = cursor_blink_visible(self.cursor_blink_started_at.elapsed());
        // Ratatui flushes changed cells, then shows the cursor where the last
        // cell was written before moving it. Keep the cursor out of the frame
        // and place it while hidden, so animated transcript diffs cannot flash
        // a caret through action rows.
        self.terminal.hide_cursor()?;
        let mut frame_cursor = None;
        self.terminal.draw(|frame| {
            let area = centered_content_area_with_margin(frame.area(), self.horizontal_margin);
            let chunks = terminal_vertical_chunks(
                area,
                queued_prompt_panel_height(
                    &queued_prompts,
                    area.width,
                    self.pending_input_expanded,
                ),
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
                            ui_text(ui_language, "What are we working on?"),
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
                (chunks[2], chunks[0], chunks[3], chunks[4])
            };
            next_composer_text_area = Some(Rect {
                x: composer_area
                    .x
                    .saturating_add(composer_cursor_x_offset(is_launch_screen)),
                y: composer_area.y.saturating_add(u16::from(!is_launch_screen)),
                width: composer_text_width.min(u16::MAX as usize) as u16,
                height: composer_area.height.saturating_sub(composer_border_rows),
            });
            next_composer_area = Some(composer_area);
            if !is_launch_screen
                && (billing_status.is_some()
                    || shell_status.is_some()
                    || watch_status.is_some()
                    || todo_status.is_some())
            {
                let combined_status = footer_status_text(
                    billing_status.as_deref(),
                    shell_status.as_deref(),
                    watch_status.as_deref(),
                    todo_status.as_deref(),
                );
                let metadata_width =
                    footer_metadata_text(&combined_status, &cwd_status, usize::MAX).width() as u16;
                let visible_metadata_width = metadata_width.min(footer_area.width);
                let metadata_x = footer_area.right().saturating_sub(visible_metadata_width);
                // Billing leads the metadata, so the interactive shell/todo
                // hit areas start after it.
                let interactive_follow =
                    shell_status.is_some() || watch_status.is_some() || todo_status.is_some();
                let billing_prefix_width = billing_status
                    .as_deref()
                    .filter(|_| interactive_follow)
                    .map(|status| status.width() + STATUS_SEPARATOR.width())
                    .unwrap_or(0) as u16;
                // Tokens are laid out left to right; each interactive token's
                // hit area starts after everything before it.
                let mut cursor_x = metadata_x.saturating_add(billing_prefix_width);
                let mut place = |status: Option<&str>| -> Option<Rect> {
                    let status = status?;
                    let area = Rect {
                        x: cursor_x,
                        y: footer_area.y,
                        width: (status.width() as u16).min(visible_metadata_width),
                        height: 1,
                    };
                    cursor_x = cursor_x
                        .saturating_add(status.width() as u16)
                        .saturating_add(STATUS_SEPARATOR.width() as u16);
                    Some(area)
                };
                next_shell_status_area = place(shell_status.as_deref());
                next_watch_status_area = place(watch_status.as_deref());
                next_todo_status_area = place(todo_status.as_deref());
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
                        x: transcript_area.right() - TRANSCRIPT_SCROLLBAR_GUTTER_WIDTH,
                        width: TRANSCRIPT_SCROLLBAR_GUTTER_WIDTH,
                        ..transcript_area
                    })
                } else {
                    None
                };
                let scroll_start = scroll;
                let show_history_loader = self.history_page_loading && self.focused_tool.is_none();
                let visible_height = content_area.height as usize;
                let sticky_tool_run_header =
                    sticky_tool_run_header_row(tool_run_rows, scroll_start).map(
                        |(index, row, expandable)| {
                            let mut header = transcript[row].clone();
                            if let Some(animation) = tool_activity_animation(
                                self.running_sweeps,
                                self.transcript.tool_activity_is_running(index),
                            ) {
                                animation.apply_header(&mut header);
                            }
                            (index, header, expandable)
                        },
                    );
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
                            refresh_tool_elapsed_line(
                                &mut header,
                                *index,
                                cached_tool_elapsed,
                                &current_tool_elapsed,
                            );
                            if let Some(animation) = tool_activity_animation(
                                self.running_sweeps,
                                self.transcript.tool_activity_is_running(*index),
                            ) {
                                animation.apply_header(&mut header);
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
                let visible_end = scroll_start.saturating_add(visible_height);
                for (index, start, end) in
                    visible_row_ranges(tool_rows, scroll_start, visible_height)
                {
                    if *start >= scroll_start
                        && let Some(line) = visible_transcript.get_mut(*start - scroll_start)
                    {
                        refresh_tool_elapsed_line(
                            line,
                            *index,
                            cached_tool_elapsed,
                            &current_tool_elapsed,
                        );
                    }
                    if let Some(animation) = tool_activity_animation(
                        self.running_sweeps,
                        self.transcript.tool_activity_is_running(*index),
                    ) {
                        let content_width = transcript[*start..*end]
                            .iter()
                            .map(running_activity_content_width)
                            .max()
                            .unwrap_or(0);
                        for row in (*start).max(scroll_start)..(*end).min(visible_end) {
                            if let Some(line) = visible_transcript.get_mut(row - scroll_start) {
                                animation.apply(line, row == *start, content_width);
                            }
                        }
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
                // Message backgrounds and diff bars reach the screen edge: carry
                // each row's trailing background through the scrollbar gutter,
                // which the thin scrollbar is then drawn over.
                if content_area.right() < transcript_area.right() {
                    let buffer = frame.buffer_mut();
                    for y in content_area.y..content_area.bottom() {
                        let bg = buffer[(content_area.right() - 1, y)].bg;
                        if bg == Color::Reset {
                            continue;
                        }
                        for x in content_area.right()..transcript_area.right() {
                            buffer[(x, y)].set_bg(bg);
                        }
                    }
                }
                // Diff bars also reach the left edge, under the detail rule.
                {
                    let buffer = frame.buffer_mut();
                    for y in content_area.y..content_area.bottom() {
                        let bg = buffer[(content_area.right() - 1, y)].bg;
                        if !matches!(bg, rendering::DIFF_ADDED_BG | rendering::DIFF_REMOVED_BG) {
                            continue;
                        }
                        for x in content_area.x..content_area.right() {
                            if buffer[(x, y)].bg != Color::Reset {
                                break;
                            }
                            buffer[(x, y)].set_bg(bg);
                        }
                    }
                }
                if let Some(picker) = self.image_picker.as_ref() {
                    for slot in image_preview_slots(link_rows) {
                        let first = slot.first_row.max(scroll_start);
                        let end = (slot.first_row + slot.rows)
                            .min(scroll_start.saturating_add(visible_height));
                        if first >= end {
                            continue;
                        }
                        let skipped_rows = first - slot.first_row;
                        let x = content_area.x.saturating_add(slot.start as u16);
                        let width = (slot.width as u16).min(content_area.right().saturating_sub(x));
                        if width == 0 {
                            continue;
                        }
                        let area = Rect {
                            x,
                            y: content_area.y + (first - scroll_start) as u16,
                            width,
                            height: (end - first) as u16,
                        };
                        // One encoded tile per image: scrolling places it at an
                        // offset instead of encoding and transmitting a new crop
                        // for every clipped position.
                        let key = (slot.path.clone(), area.width, slot.rows as u16);
                        if !self.image_protocols.contains_key(&key) {
                            // Encode once the motion settles; draw the text meanwhile.
                            if self.scroll_motion.is_active()
                                || self
                                    .image_scroll_settles_at
                                    .is_some_and(|deadline| Instant::now() < deadline)
                            {
                                continue;
                            }
                            if self.image_protocols.len() >= 16 {
                                self.image_protocols.clear();
                            }
                            let Some(image) = attachments::load_preview_image(&slot.path) else {
                                continue;
                            };
                            // Downscale here with a real filter. The library's
                            // default is nearest-neighbour, which at a scale like
                            // 0.95 drops whole pixel rows and columns and tears
                            // the strokes out of text in a screenshot.
                            let cell = picker.font_size();
                            let (max_width, max_height) = (
                                u32::from(area.width) * u32::from(cell.width),
                                slot.rows as u32 * u32::from(cell.height),
                            );
                            let image = if image.width() > max_width || image.height() > max_height
                            {
                                image.resize(
                                    max_width,
                                    max_height,
                                    image::imageops::FilterType::CatmullRom,
                                )
                            } else {
                                image
                            };
                            let Ok(protocol) = SlicedProtocol::new_with_resize(
                                picker,
                                image,
                                ratatui::layout::Size::new(area.width, slot.rows as u16),
                                Resize::Fit(Some(image::imageops::FilterType::CatmullRom)),
                            ) else {
                                continue;
                            };
                            self.image_protocols.insert(key.clone(), protocol);
                        }
                        if let Some(protocol) = self.image_protocols.get(&key) {
                            frame.render_widget(
                                SlicedImage::new(
                                    protocol,
                                    SignedPosition::from((0, -(skipped_rows as i16))),
                                ),
                                area,
                            );
                        }
                    }
                }
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
                    let (thumb_top, thumb_height) = scrollbar_thumb_geometry(
                        area.height,
                        transcript_height,
                        scroll,
                        scroll_max,
                    );
                    let rows = (0..area.height)
                        .map(|row| {
                            let in_thumb = row >= thumb_top && row < thumb_top + thumb_height;
                            let (glyph, color) = if in_thumb {
                                (
                                    " ▐",
                                    if self.scrollbar_hovered || self.dragging_scrollbar {
                                        Color::Rgb(205, 214, 226)
                                    } else {
                                        Color::Rgb(155, 165, 180)
                                    },
                                )
                            } else {
                                (" ▕", Color::Rgb(67, 72, 81))
                            };
                            Line::from(Span::styled(glyph, Style::default().fg(color)))
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
            let jump_to_bottom_label = (!is_launch_screen && self.scroll_from_bottom > 0)
                .then(|| format!(" ↓ {} ", ui_text(ui_language, "Jump to bottom")));
            if !queued_prompts.is_empty() {
                next_pending_input_header_area = Some(Rect {
                    height: chunks[1].height.min(1),
                    ..chunks[1]
                });
                frame.render_widget(
                    Paragraph::new(if self.pending_input_expanded {
                        queued_prompt_lines(
                            queued_prompts.as_slice(),
                            chunks[1].width,
                            self.focused_child.is_some().then_some(SUBAGENT_PINK),
                        )
                    } else {
                        Vec::new()
                    })
                    .block(
                        Block::default()
                            .borders(Borders::TOP | Borders::LEFT)
                            .border_style(Style::default().fg(Color::DarkGray))
                            .title(Span::styled(
                                pending_input_title(
                                    ui_language,
                                    queued_prompts.len(),
                                    self.pending_input_expanded,
                                    chunks[1].width,
                                ),
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
                    Block::default().style(Style::default().bg(COMPOSER_BG)),
                    Rect {
                        x: area.x,
                        y: composer_area.y,
                        width: area.width,
                        height: footer_area.bottom().saturating_sub(composer_area.y),
                    },
                );
                frame.render_widget(
                    Block::default().style(Style::default().bg(Color::Black)),
                    footer_area,
                );
            }
            let composer_block = Block::default()
                .style(Style::default().bg(if is_launch_screen {
                    Color::Reset
                } else {
                    COMPOSER_INPUT_BG
                }))
                .borders(if is_launch_screen {
                    Borders::LEFT
                } else {
                    Borders::NONE
                })
                .border_style(Style::default().fg(if is_launch_screen {
                    BORG_ORANGE
                } else {
                    Color::DarkGray
                }));
            let composer_content_area = if is_launch_screen {
                composer_block.inner(composer_area)
            } else {
                Rect {
                    y: composer_area.y.saturating_add(1),
                    height: composer_area.height.saturating_sub(2),
                    ..composer_area
                }
            };
            frame.render_widget(composer_block, composer_area);
            let composer_content_style = Style::default().bg(if is_launch_screen {
                Color::Reset
            } else {
                COMPOSER_INPUT_BG
            });
            if let Some(lines) = picker_lines.clone() {
                frame.render_widget(
                    Paragraph::new(lines)
                        .style(composer_content_style)
                        .scroll((composer_scroll, 0)),
                    composer_content_area,
                );
            } else {
                frame.render_widget(
                    Paragraph::new(composer_render_lines.clone())
                        .style(composer_content_style)
                        .scroll((composer_scroll, 0)),
                    composer_content_area,
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
                        y: composer_content_area.y.saturating_add(line as u16),
                        width: picker_hit_width,
                        height: 1,
                    };
                    if row.y < composer_content_area.bottom() {
                        next_picker_hit_areas.push((row, index));
                    }
                }
            }
            if self.picker.is_none()
                && cursor_visible
                && let Some(cursor) = composer_frame_cursor(
                    composer_area,
                    composer_cursor,
                    composer_scroll,
                    is_launch_screen,
                )
            {
                frame_cursor = Some(cursor);
            }
            let status_highlight = self.status_hovered && status_is_interruptible;
            let status_duration = if session_is_active && reconnect_label.is_none() {
                activity_clock.status_duration(Utc::now())
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
            if session_is_active && self.running_sweeps && !status_highlight {
                apply_running_status_shimmer(&mut status_spans, running_status_shimmer_phase());
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
            if let Some(name) = focused_agent_name.as_deref() {
                status_spans.push(Span::styled(
                    format!("{STATUS_SEPARATOR}to {name}"),
                    Style::default()
                        .fg(SUBAGENT_PINK)
                        .add_modifier(Modifier::BOLD),
                ));
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
                                Color::Black
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
                let tooltip_width = team_roster_table_width(&agent_roster_entries)
                    .saturating_add(2)
                    .clamp(30, status_area.width.min(96));
                let tooltip_height = (agent_roster_entries.len() as u16)
                    .saturating_add(3)
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
                let roster_lines = team_roster_table_lines(
                    &agent_roster_entries,
                    tooltip.width.saturating_sub(2) as usize,
                    self.focused_child,
                    self.hovered_team_roster,
                    ui_language,
                );
                frame.render_widget(
                    Paragraph::new(roster_lines)
                        .style(Style::default().fg(Color::White).bg(COMMAND_PANEL_BG))
                        .block(
                            Block::default()
                                .borders(Borders::ALL)
                                .border_style(Style::default().fg(Color::DarkGray))
                                .title(Span::styled(
                                    format!(" Team · {active_subagents} working "),
                                    Style::default()
                                        .fg(SUBAGENT_PINK)
                                        .add_modifier(Modifier::BOLD),
                                )),
                        ),
                    tooltip,
                );
                for (index, entry) in agent_roster_entries.iter().enumerate() {
                    next_team_roster_hit_areas.push((
                        Rect {
                            x: tooltip.x.saturating_add(1),
                            y: tooltip.y.saturating_add(2 + index as u16),
                            width: tooltip.width.saturating_sub(2),
                            height: 1,
                        },
                        entry.child_id,
                    ));
                }
            }
            if self.focused_child.is_some() || self.focused_tool.is_some() {
                let (label, idle_color) = if self.focused_tool.is_some() {
                    (" ← Back to actions ", BACKGROUND_RUNNING_TEXT)
                } else {
                    (" ↩ Return ", SUBAGENT_PINK)
                };
                let button = Rect {
                    x: chunks[2].right().saturating_sub(label.width() as u16 + 1),
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
                                idle_color
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
            if let Some(label) = jump_to_bottom_label {
                // Share the right edge and style of the return button; when
                // both are shown they sit side by side on the status row.
                let width = label.width() as u16;
                let button = match next_back_to_director_area {
                    Some(back) => Rect {
                        x: back.x.saturating_sub(width + 1),
                        y: back.y,
                        width,
                        height: 1,
                    },
                    None => Rect {
                        x: chunks[2].right().saturating_sub(width + 1),
                        y: chunks[0].bottom().saturating_sub(1),
                        width,
                        height: 1,
                    },
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
            if (self.git_status_hovered || self.git_commit_hovered || self.git_pull_hovered)
                && let Some(git_area) = if self.git_commit_hovered {
                    self.git_commit_area
                } else if self.git_pull_hovered {
                    self.git_pull_area
                } else {
                    self.git_status_area
                }
                && let Some(git_status) = footer_git_status.as_ref()
            {
                let pushing = self.git_push.is_running(&active_cwd, GitRemoteAction::Push);
                let committing = self.git_commit.is_committing(&active_cwd);
                let pulling = self.git_push.is_running(&active_cwd, GitRemoteAction::Pull);
                let tooltip_title = if self.git_commit_hovered {
                    " git commit "
                } else if self.git_pull_hovered {
                    " git pull "
                } else {
                    " git push "
                };
                let text = if self.git_commit_hovered {
                    if committing {
                        format!("Committing on {}…", git_status.branch)
                    } else {
                        format!(
                            "Commit all changes on {} — message by {} — then push — click",
                            git_status.branch,
                            commit_model_from_environment().label()
                        )
                    }
                } else if self.git_pull_hovered {
                    if pulling {
                        format!("Pulling {} from upstream…", git_status.branch)
                    } else {
                        format!(
                            "Pull ↓{} into {} — click",
                            git_status.behind, git_status.branch
                        )
                    }
                } else if pushing {
                    format!("Pushing {} to {}…", git_status.branch, "upstream")
                } else {
                    format!(
                        "Push ↑{} to {} — click",
                        git_status.ahead, git_status.branch
                    )
                };
                let tooltip_width = (text.width() as u16)
                    .saturating_add(4)
                    .clamp(24, area.width.min(80));
                let tooltip_height = 3u16.min(git_area.y.saturating_sub(area.y).max(1));
                let tooltip = Rect {
                    x: git_area
                        .right()
                        .saturating_sub(tooltip_width)
                        .max(area.x)
                        .min(area.right().saturating_sub(tooltip_width)),
                    y: git_area.y.saturating_sub(tooltip_height),
                    width: tooltip_width,
                    height: tooltip_height,
                };
                frame.render_widget(Clear, tooltip);
                frame.render_widget(
                    Paragraph::new(text)
                        .wrap(ratatui::widgets::Wrap { trim: true })
                        .style(Style::default().fg(Color::White).bg(COMMAND_PANEL_BG))
                        .block(
                            Block::default()
                                .borders(Borders::ALL)
                                .border_style(Style::default().fg(Color::Cyan))
                                .title(tooltip_title),
                        ),
                    tooltip,
                );
            }
            if (self.shell_menu_open || self.hovered_shell_row.is_some()) && !shell_rows.is_empty()
            {
                let tooltip_width = shell_rows
                    .iter()
                    .map(|(row, _)| row.width() as u16)
                    .max()
                    .unwrap_or(24)
                    .saturating_add(4)
                    .clamp(30, status_area.width.min(96));
                let tooltip_height = (shell_rows.len() as u16)
                    .saturating_add(2)
                    .min(status_area.y.saturating_sub(area.y).max(1));
                let shell_anchor = next_shell_status_area.unwrap_or(status_area);
                let tooltip = Rect {
                    x: shell_anchor
                        .x
                        .min(area.right().saturating_sub(tooltip_width)),
                    y: shell_anchor.y.saturating_sub(tooltip_height),
                    width: tooltip_width,
                    height: tooltip_height,
                };
                frame.render_widget(Clear, tooltip);
                let lines = shell_rows
                    .iter()
                    .enumerate()
                    .map(|(index, (row, _))| {
                        Line::from(format!("  {row}"))
                            .style(shell_row_style(self.hovered_shell_row == Some(index)))
                    })
                    .collect::<Vec<_>>();
                frame.render_widget(
                    Paragraph::new(lines)
                        .style(Style::default().fg(Color::White).bg(COMMAND_PANEL_BG))
                        .block(
                            Block::default()
                                .borders(Borders::ALL)
                                .border_style(Style::default().fg(USER_LABEL_BLUE))
                                .title(" Shells · click to inspect "),
                        ),
                    tooltip,
                );
                for (index, (_, tool_index)) in shell_rows.iter().enumerate() {
                    next_shell_row_hit_areas.push((
                        Rect {
                            x: tooltip.x.saturating_add(1),
                            y: tooltip.y.saturating_add(1 + index as u16),
                            width: tooltip.width.saturating_sub(2),
                            height: 1,
                        },
                        *tool_index,
                    ));
                }
            }
            if (self.watch_menu_open || self.hovered_watch_row.is_some()) && !watch_rows.is_empty()
            {
                let tooltip_width = watch_rows
                    .iter()
                    .map(|(row, _)| row.width() as u16)
                    .max()
                    .unwrap_or(24)
                    .saturating_add(4)
                    .clamp(30, status_area.width.min(110));
                let tooltip_height = (watch_rows.len() as u16)
                    .saturating_add(2)
                    .min(status_area.y.saturating_sub(area.y).max(1));
                let watch_anchor = next_watch_status_area.unwrap_or(status_area);
                let tooltip = Rect {
                    x: watch_anchor
                        .x
                        .min(area.right().saturating_sub(tooltip_width)),
                    y: watch_anchor.y.saturating_sub(tooltip_height),
                    width: tooltip_width,
                    height: tooltip_height,
                };
                frame.render_widget(Clear, tooltip);
                let lines = watch_rows
                    .iter()
                    .enumerate()
                    .map(|(index, (row, _))| {
                        Line::from(format!("  {row}"))
                            .style(shell_row_style(self.hovered_watch_row == Some(index)))
                    })
                    .collect::<Vec<_>>();
                frame.render_widget(
                    Paragraph::new(lines)
                        .style(Style::default().fg(Color::White).bg(COMMAND_PANEL_BG))
                        .block(
                            Block::default()
                                .borders(Borders::ALL)
                                .border_style(Style::default().fg(Color::Yellow))
                                .title(" Watchers · click to stop "),
                        ),
                    tooltip,
                );
                for (index, (_, watch_id)) in watch_rows.iter().enumerate() {
                    next_watch_row_hit_areas.push((
                        Rect {
                            x: tooltip.x.saturating_add(1),
                            y: tooltip.y.saturating_add(1 + index as u16),
                            width: tooltip.width.saturating_sub(2),
                            height: 1,
                        },
                        *watch_id,
                    ));
                }
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
            self.git_status_area = None;
            self.git_pull_area = None;
            self.git_commit_area = None;
            if !is_launch_screen {
                let footer_metadata = Some(footer_metadata_text(
                    &footer_status_text(
                        billing_status.as_deref(),
                        shell_status.as_deref(),
                        watch_status.as_deref(),
                        todo_status.as_deref(),
                    ),
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
                    width: footer_left_region_width(footer_area.width, metadata_width),
                    ..footer_area
                };
                frame.render_widget(
                    Paragraph::new(controls)
                        .style(Style::default().fg(Color::DarkGray).bg(Color::Black)),
                    controls_area,
                );
                if footer_metadata.is_some() && metadata_width > 0 {
                    let metadata_line = if billing_status.is_some()
                        || shell_status.is_some()
                        || watch_status.is_some()
                        || todo_status.is_some()
                    {
                        footer_shell_todo_metadata_line(
                            billing_status.as_deref(),
                            shell_status.as_deref(),
                            watch_status.as_deref(),
                            todo_status.as_deref(),
                            &cwd_status,
                            self.shell_status_hovered,
                            self.watch_status_hovered,
                            self.todo_status_hovered,
                            metadata_width as usize,
                        )
                    } else {
                        footer_metadata_line("", &cwd_status, false, metadata_width as usize)
                    };
                    let metadata_rect = Rect {
                        x: footer_area.right().saturating_sub(metadata_width),
                        width: metadata_width,
                        ..footer_area
                    };
                    frame.render_widget(
                        Paragraph::new(metadata_line)
                            .alignment(Alignment::Right)
                            .style(Style::default().bg(Color::Black)),
                        metadata_rect,
                    );
                    self.git_status_area = footer_git_status
                        .as_ref()
                        .and_then(|status| git_ahead_hit_area(status, metadata_rect));
                    self.git_pull_area = footer_git_status
                        .as_ref()
                        .and_then(|status| git_behind_hit_area(status, metadata_rect));
                    self.git_commit_area = footer_git_status
                        .as_ref()
                        .and_then(|status| git_commit_hit_area(status, metadata_rect));
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
                    &footer_status_text(
                        billing_status.as_deref(),
                        shell_status.as_deref(),
                        watch_status.as_deref(),
                        todo_status.as_deref(),
                    ),
                    &cwd_status,
                    usize::MAX,
                ))
                .filter(|value| !value.is_empty())
                .map(|value| (value.width() as u16).min(footer_area.width))
                .unwrap_or(0);
                let copy_area = Rect {
                    width: footer_left_region_width(footer_area.width, metadata_width),
                    ..footer_area
                };
                frame.render_widget(Clear, copy_area);
                frame.render_widget(
                    Paragraph::new(copy_notice_line(notice.to_string()))
                        .style(Style::default().bg(Color::Black)),
                    copy_area,
                );
            }
        })?;
        if let Some(cursor) = frame_cursor {
            self.terminal.set_cursor_position(cursor)?;
            self.terminal.show_cursor()?;
        }
        if !input_fast_path || refresh_reasoning_summary {
            self.last_committed_viewport_render = Some((
                transcript_width,
                tool_run_viewport_height,
                goal_tick,
                tool_elapsed_tick,
                local_date,
                Arc::clone(&transcript_render),
            ));
        }
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
            next_pending_input_header_area = None;
            next_status_area = None;
            next_goal_status_area = None;
            next_todo_status_area = None;
            next_shell_status_area = None;
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
            next_shell_row_hit_areas.clear();
            next_watch_row_hit_areas.clear();
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
        self.pending_input_header_area = next_pending_input_header_area;
        self.status_area = next_status_area;
        self.goal_status_area = next_goal_status_area;
        self.todo_status_area = next_todo_status_area;
        self.shell_status_area = next_shell_status_area;
        self.shell_row_hit_areas = next_shell_row_hit_areas;
        self.watch_status_area = next_watch_status_area;
        self.watch_row_hit_areas = next_watch_row_hit_areas;
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

    fn navigate_composer(&mut self, navigation: ComposerNavigation, selecting: bool) {
        let anchor = self
            .composer_selection
            .map_or(self.composer.cursor, |selection| selection.anchor);
        match navigation {
            ComposerNavigation::WordLeft => self.composer.move_word_left(),
            ComposerNavigation::WordRight => self.composer.move_word_right(),
            ComposerNavigation::LineStart => self.composer.move_line_start(),
            ComposerNavigation::LineEnd => self.composer.move_line_end(),
            ComposerNavigation::DocumentStart => {
                self.composer.cursor = 0;
                self.composer.preferred_column = None;
            }
            ComposerNavigation::DocumentEnd => {
                self.composer.cursor = self.composer.text.len();
                self.composer.preferred_column = None;
            }
            ComposerNavigation::LineUp | ComposerNavigation::LineDown => {
                let width = self
                    .composer_area
                    .map_or(80, |area| area.width.saturating_sub(2).max(1) as usize);
                self.composer.move_vertical(
                    if navigation == ComposerNavigation::LineUp {
                        -1
                    } else {
                        1
                    },
                    width,
                );
            }
        }
        self.composer_selection = selecting.then_some(ComposerSelection {
            anchor,
            focus: self.composer.cursor,
            dragging: false,
            pointer: Position::new(0, 0),
        });
        if self
            .composer_selection
            .is_some_and(ComposerSelection::is_empty)
        {
            self.composer_selection = None;
        }
    }

    fn handle_key(&mut self, mut key: KeyEvent) -> Result<UiAction> {
        if is_selection_copy_shortcut(&key)
            && let Some(request) = self.copy_text_selection_request()
        {
            return Ok(UiAction::TerminalIo(request));
        }
        if self.picker.is_none() && self.keymap.matches(KeyAction::Find, &key) {
            self.open_thread_find();
            return Ok(UiAction::None);
        }
        let ctrl_c = is_ctrl_c(&key);
        if ctrl_c {
            if key.kind == KeyEventKind::Press
                && repeated_ctrl_c(
                    &mut self.last_ctrl_c,
                    &mut self.ctrl_c_count,
                    Instant::now(),
                )
            {
                return Ok(UiAction::ForceQuit);
            }
            key.code = KeyCode::Esc;
            key.modifiers = KeyModifiers::NONE;
        } else {
            self.last_ctrl_c = None;
            self.ctrl_c_count = 0;
        }
        if matches!(
            self.picker.as_ref().map(|picker| picker.kind),
            Some(PickerKind::Commands | PickerKind::Model)
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
                    self.dictation_enable_flow = false;
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
        if key.code == KeyCode::Esc
            && self
                .picker
                .as_ref()
                .is_some_and(|p| matches!(p.kind, PickerKind::ImportPreview { .. }))
        {
            self.picker = None;
            return Ok(UiAction::Submit {
                target: None,
                text: "/import-cancel".into(),
                attachments: Vec::new(),
            });
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
        if self.focused_tool.is_some()
            && key.code == KeyCode::Esc
            && key.modifiers == KeyModifiers::NONE
        {
            self.close_tool_inspector();
            return Ok(UiAction::None);
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
            if !ctrl_c && self.has_pending_input_for_escape() && !self.escape_interrupts_turn() {
                return Ok(self.flush_pending_input());
            }
            return Ok(if self.begin_user_interrupt() {
                UiAction::Interrupt {
                    target: Some(target),
                }
            } else {
                UiAction::None
            });
        }
        if ctrl_c && (!self.composer.text.is_empty() || !self.composer.attachments.is_empty()) {
            self.composer.clear();
            self.composer_selection = None;
            self.notice = Some("Prompt cleared".to_string());
            return Ok(UiAction::None);
        }
        if ctrl_c
            && !self.pending_approval
            && !matches!(
                self.status,
                SessionStatus::Starting
                    | SessionStatus::Running
                    | SessionStatus::WaitingForApproval
            )
        {
            self.rewind_primed = false;
            self.notice = Some("Press Ctrl-C again to exit".to_string());
            return Ok(UiAction::None);
        }
        if !ctrl_c
            && self.keymap.matches(KeyAction::Interrupt, &key)
            && self.has_pending_input_for_escape()
            && !self.escape_interrupts_turn()
        {
            return Ok(self.flush_pending_input());
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
        if deletes_line_prefix(&key) {
            self.composer_selection = None;
            self.composer.backspace_line();
            self.slash_selection = 0;
            self.update_slash_notice();
            return Ok(UiAction::None);
        }
        if deletes_previous_word(&key) {
            self.composer_selection = None;
            self.composer.backspace_word();
            self.update_slash_notice();
            return Ok(UiAction::None);
        }
        if deletes_next_word(&key) {
            self.composer_selection = None;
            self.composer.delete_word();
            self.update_slash_notice();
            return Ok(UiAction::None);
        }
        if self.keymap.matches(KeyAction::Exit, &key) {
            return Ok(UiAction::Quit);
        }
        if self.keymap.matches(KeyAction::AttachImage, &key) {
            self.notice = Some("Reading clipboard…".to_string());
            return Ok(UiAction::TerminalIo(TerminalIoRequest::capture_clipboard(
                self.attachment_store.clone(),
                self.cwd.clone(),
            )));
        }
        if self.keymap.matches(KeyAction::Dictate, &key) {
            if self.dictation_enabled {
                return Ok(UiAction::ToggleDictation);
            }
            self.open_enable_dictation_picker();
            return Ok(UiAction::None);
        }
        if self.keymap.matches(KeyAction::Copy, &key) {
            if let Some(request) = self.copy_text_selection_request() {
                return Ok(UiAction::TerminalIo(request));
            }
            if let Some(text) = self.transcript.copy_text() {
                return Ok(UiAction::TerminalIo(TerminalIoRequest::copy(
                    text,
                    self.transcript.copy_notice(),
                )));
            }
            return Ok(UiAction::None);
        }
        if let Some((navigation, selecting)) = composer_navigation(&key) {
            self.navigate_composer(navigation, selecting);
            return Ok(UiAction::None);
        }
        self.composer_selection = None;
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
                        EventActor::User,
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
                return Ok(self
                    .copy_last_assistant_message_request()
                    .map_or(UiAction::None, UiAction::TerminalIo));
            }
            if self.composer.attachments.is_empty() && self.composer.text.trim() == "/dictate" {
                self.composer.clear();
                self.notice = None;
                return Ok(UiAction::ToggleDictation);
            }
            if self.composer.attachments.is_empty()
                && let Some(pattern) = self.composer.text.trim().strip_prefix("/find")
                && pattern.chars().next().is_none_or(char::is_whitespace)
            {
                let pattern = pattern.trim().to_string();
                self.composer.clear();
                self.find_in_thread(&pattern);
                return Ok(UiAction::None);
            }
            if self.composer.attachments.is_empty()
                && let Some(message) = self.composer.text.trim().strip_prefix("/team")
                && message.chars().next().is_none_or(char::is_whitespace)
            {
                let message = message.trim().to_string();
                if message.is_empty() {
                    self.notice = Some("Usage: /team <message>".to_string());
                    return Ok(UiAction::None);
                }
                self.composer.clear();
                self.notice = None;
                return Ok(UiAction::Broadcast { text: message });
            }
            if self.composer.attachments.is_empty()
                && let Some(message) = self.composer.text.trim().strip_prefix("/broadcast")
                && message.chars().next().is_none_or(char::is_whitespace)
            {
                let message = message.trim().to_string();
                if message.is_empty() {
                    self.notice = Some("Usage: /broadcast <message>".to_string());
                    return Ok(UiAction::None);
                }
                self.composer.clear();
                self.notice = Some("Broadcasting to every instance on this machine".to_string());
                return Ok(UiAction::BroadcastInstances { text: message });
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
            self.history_page_requested = self.focused_tool.is_none();
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
            return Ok(if self.begin_user_interrupt() {
                UiAction::Interrupt {
                    target: self.focused_child,
                }
            } else {
                UiAction::None
            });
        }
        match key.code {
            KeyCode::Char(character) if composer_inserts_character(&key) => {
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
                if self.composer.history_index.is_none() && slash_matches > 0 {
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
                if self.composer.history_index.is_none() && slash_matches > 0 {
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
pub fn discard_pending_terminal_input() {
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

fn play_system_completion_sound() -> bool {
    #[cfg(target_os = "macos")]
    {
        let mut command = Command::new("/usr/bin/afplay");
        command.args(["-v", "0.25", "/System/Library/Sounds/Glass.aiff"]);
        return start_completion_sound(command);
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        let mut command = Command::new("powershell.exe");
        command.args([
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            "[System.Media.SystemSounds]::Asterisk.Play(); Start-Sleep -Milliseconds 750",
        ]);
        command.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
        return start_completion_sound(command);
    }
    #[cfg(target_os = "linux")]
    {
        let mut command = Command::new("canberra-gtk-play");
        command.args(["--id", "complete", "--volume=-12.0"]);
        if start_completion_sound(command) {
            return true;
        }
        let sound = Path::new("/usr/share/sounds/freedesktop/stereo/complete.oga");
        if sound.exists() {
            let mut command = Command::new("paplay");
            command.arg("--volume=16384").arg(sound);
            return start_completion_sound(command);
        }
        false
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
    {
        false
    }
}

#[cfg(any(target_os = "macos", target_os = "linux", windows))]
fn start_completion_sound(mut command: Command) -> bool {
    command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    let Ok(mut child) = command.spawn() else {
        return false;
    };
    thread::spawn(move || {
        let _ = child.wait();
    });
    true
}

/// Whether this live event is the moment work stopped: a turn completed and the
/// session then went idle. A goal or queued prompt starts the next turn at once,
/// and a watcher yield resumes on its own, so neither is a stop.
fn completion_alert_due(pending: &mut bool, kind: &SessionEventKind) -> bool {
    match kind {
        SessionEventKind::TurnCompleted { .. } => *pending = true,
        SessionEventKind::TurnStarted { .. } => *pending = false,
        SessionEventKind::StatusChanged {
            status: SessionStatus::Ready | SessionStatus::Stopped | SessionStatus::Failed,
            detail,
        } => {
            return std::mem::take(pending)
                && !detail
                    .as_deref()
                    .is_some_and(ready_detail_is_waiting_on_watchers);
        }
        _ => {}
    }
    false
}

fn completion_alert_enabled(policy: CompletionAlertPolicy, window_focused: bool) -> bool {
    match policy {
        CompletionAlertPolicy::Off => false,
        CompletionAlertPolicy::Unfocused => !window_focused,
        CompletionAlertPolicy::Always => true,
    }
}

/// Build the desktop-notification escape sequence for the host terminal.
///
/// OSC 777 (`notify;title;body`) carries a title and is used where the
/// terminal, or tmux, understands it; OSC 9 (body only) is the broadly
/// supported fallback. Exactly one is emitted so a terminal that honours both
/// never raises two banners for one event.
fn desktop_notification_sequence(title: &str, body: &str) -> String {
    desktop_notification_sequence_for(title, body, terminal_supports_osc_777())
}

fn desktop_notification_sequence_for(title: &str, body: &str, prefers_osc_777: bool) -> String {
    // Neither `;` nor a control byte can appear inside an OSC payload without
    // truncating it; collapse them to spaces defensively.
    let sanitize = |value: &str| {
        value
            .chars()
            .map(|character| {
                if character == ';' || character.is_control() {
                    ' '
                } else {
                    character
                }
            })
            .collect::<String>()
    };
    let body = sanitize(body);
    if prefers_osc_777 {
        format!("\x1b]777;notify;{};{body}\x1b\\", sanitize(title))
    } else {
        format!("\x1b]9;{body}\x1b\\")
    }
}

/// Whether the host terminal renders OSC 777 notifications. tmux forwards it to
/// the outer terminal (where OSC 9 often does not survive), and the listed
/// emulators support the titled form; everything else gets OSC 9.
fn terminal_supports_osc_777() -> bool {
    if std::env::var_os("TMUX").is_some() {
        return true;
    }
    matches!(
        std::env::var("TERM_PROGRAM").ok().as_deref(),
        Some("ghostty" | "WezTerm" | "rio")
    )
}

fn completion_alert_policy_label(policy: CompletionAlertPolicy) -> &'static str {
    match policy {
        CompletionAlertPolicy::Off => "Off",
        CompletionAlertPolicy::Unfocused => "When unfocused",
        CompletionAlertPolicy::Always => "Always",
    }
}

fn completion_alert_policy_from_picker(value: &str) -> CompletionAlertPolicy {
    match value {
        "Off" => CompletionAlertPolicy::Off,
        "Always" => CompletionAlertPolicy::Always,
        _ => CompletionAlertPolicy::Unfocused,
    }
}

impl BorgTerminal {
    pub fn restore_terminal(&mut self) {
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
        let _ = execute!(self.terminal.backend_mut(), DisableFocusChange);
        let _ = execute!(self.terminal.backend_mut(), DisableBracketedPaste);
        if self.keyboard_enhanced {
            let _ = execute!(self.terminal.backend_mut(), PopKeyboardEnhancementFlags);
            self.keyboard_enhanced = false;
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
        // A drop-path teardown cannot await the event pump. Flush once more
        // after the terminal modes are restored to catch bytes read during the
        // synchronous cleanup above.
        discard_pending_terminal_input();
    }
}

fn fresh_transcript_like(previous: &Transcript) -> Transcript {
    Transcript {
        image_cell: previous.image_cell,
        diff_expansion: previous.diff_expansion,
        auto_expand_tools: previous.auto_expand_tools,
        auto_expand_thinking: previous.auto_expand_thinking,
        tool_click_behavior: previous.tool_click_behavior,
        show_subagent_messages: previous.show_subagent_messages,
        follow_tail: previous.follow_tail,
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
    let events = reorder_queued_user_completions(events);
    let mut ordered = Vec::with_capacity(events.len());
    let mut turn = Vec::new();

    for event in &events {
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
        let turn_start = turn
            .iter()
            .rposition(transcript_turn_has_terminal_boundary)
            .map_or(0, |index| index + 1);
        if let Some(output_start) = turn
            .iter()
            .enumerate()
            .skip(turn_start)
            .find_map(|(index, event)| transcript_turn_output(event).then_some(index))
        {
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

fn reorder_queued_user_completions(events: &[SessionEvent]) -> Vec<SessionEvent> {
    let mut admission_order = HashMap::<Uuid, u64>::new();
    for (index, event) in events.iter().enumerate() {
        if let SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            status: MessageStatus::Queued,
            ..
        } = &event.kind
        {
            admission_order.insert(
                *message_id,
                if event.sequence > 0 {
                    event.sequence
                } else {
                    index as u64
                },
            );
        }
    }
    if admission_order.len() < 2 {
        return events.to_vec();
    }

    let mut completions = events
        .iter()
        .filter_map(|event| {
            let SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                status: MessageStatus::Complete | MessageStatus::Failed,
                ..
            } = &event.kind
            else {
                return None;
            };
            admission_order
                .get(message_id)
                .copied()
                .map(|sequence| (sequence, event.clone()))
        })
        .collect::<Vec<_>>();
    if completions.len() < 2 {
        return events.to_vec();
    }
    completions.sort_by_key(|(sequence, _)| *sequence);

    let mut next_completion = completions.into_iter();
    events
        .iter()
        .map(|event| {
            let is_queued_completion = matches!(
                &event.kind,
                SessionEventKind::Message {
                    message_id,
                    actor: EventActor::User,
                    status: MessageStatus::Complete | MessageStatus::Failed,
                    ..
                } if admission_order.contains_key(message_id)
            );
            if is_queued_completion {
                next_completion
                    .next()
                    .map(|(_, event)| event)
                    .unwrap_or_else(|| event.clone())
            } else {
                event.clone()
            }
        })
        .collect()
}

fn transcript_turn_output(event: &SessionEvent) -> bool {
    matches!(
        event.kind,
        SessionEventKind::Message {
            actor: EventActor::Assistant,
            ..
        } | SessionEventKind::ReasoningDelta { .. }
            | SessionEventKind::ToolStarted { .. }
            | SessionEventKind::ToolUpdated { .. }
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
    let mut seen = HashSet::new();
    transcript_history_in_display_order(events)
        .into_iter()
        .filter_map(|event| match event.kind {
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text,
                attachments,
                ..
            } if event.sequence > 0 && seen.insert(message_id) => Some(RewindTarget {
                message_id,
                sequence: event.sequence,
                text,
                attachments,
            }),
            _ => None,
        })
        .collect()
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
    let reconciled_usage = previous.session_usage.clone();
    let display_events = transcript_history_in_display_order(events);
    let mut replacement = fresh_transcript_like(previous);
    replacement.reserve_history(display_events.len());
    for agent in &reconciled_subagents {
        replacement.upsert_subagent_snapshot(agent);
    }
    for event in &display_events {
        replacement.apply_history(event);
    }
    replacement.session_usage = reconciled_usage;
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
    // Keep sequence-zero live snapshots near their observed time, then put
    // durable events in journal order within the slots they occupied. Sorting
    // all snapshots last can resurrect an unfinished Thinking row after its
    // durable completion; sorting all events by time can put a process start
    // before the tool that owns it.
    events.sort_by(|left, right| {
        left.created_at
            .cmp(&right.created_at)
            .then_with(|| left.sequence.cmp(&right.sequence))
            .then_with(|| left.id.cmp(&right.id))
    });
    let mut durable = events
        .iter()
        .filter(|event| event.sequence > 0)
        .cloned()
        .collect::<Vec<_>>();
    durable.sort_by_key(|event| (event.sequence, event.id));
    let mut durable = durable.into_iter();
    for event in &mut events {
        if event.sequence > 0 {
            *event = durable.next().expect("one durable event per durable slot");
        }
    }
    events
}

fn switch_to_child_transcript(
    transcript: &mut Transcript,
    director_transcript: &mut Option<Box<Transcript>>,
    child_transcripts: &mut HashMap<Uuid, Transcript>,
    child_id: Uuid,
) {
    let child = child_transcripts
        .remove(&child_id)
        .unwrap_or_else(new_child_transcript);
    *director_transcript = Some(Box::new(std::mem::replace(transcript, child)));
}

fn switch_between_child_transcripts(
    transcript: &mut Transcript,
    child_transcripts: &mut HashMap<Uuid, Transcript>,
    previous_child: Uuid,
    next_child: Uuid,
) {
    let next = child_transcripts
        .remove(&next_child)
        .unwrap_or_else(new_child_transcript);
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
    match kind {
        SessionEventKind::StatusChanged { status, .. } => Some(match status {
            SessionStatus::Starting => SubagentStatus::Starting,
            SessionStatus::Running => SubagentStatus::Running,
            SessionStatus::Ready | SessionStatus::Completed => SubagentStatus::Ready,
            SessionStatus::WaitingForApproval => SubagentStatus::WaitingForApproval,
            SessionStatus::Stopped => SubagentStatus::Stopped,
            SessionStatus::Failed => SubagentStatus::Failed,
        }),
        SessionEventKind::TurnCompleted { .. } => Some(SubagentStatus::Ready),
        _ => None,
    }
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

#[derive(Clone, Copy, Default)]
struct ActivityClock {
    started_at: Option<DateTime<Utc>>,
    elapsed: chrono::Duration,
}

impl ActivityClock {
    fn observe(&mut self, status: SessionStatus, at: DateTime<Utc>) {
        if matches!(status, SessionStatus::Starting | SessionStatus::Running) {
            self.started_at.get_or_insert(at);
        } else if let Some(started_at) = self.started_at.take() {
            self.elapsed += at
                .signed_duration_since(started_at)
                .max(chrono::Duration::zero());
        }
    }

    fn status_duration(&self, now: DateTime<Utc>) -> Option<String> {
        let current = self
            .started_at
            .map_or(chrono::Duration::zero(), |started_at| {
                now.signed_duration_since(started_at)
                    .max(chrono::Duration::zero())
            });
        format_elapsed_duration((self.elapsed + current).num_seconds().max(0) as u64)
    }
}

fn track_child_activity(
    clocks: &mut HashMap<Uuid, ActivityClock>,
    child_id: Uuid,
    status: SessionStatus,
    observed_at: DateTime<Utc>,
) {
    clocks
        .entry(child_id)
        .or_default()
        .observe(status, observed_at);
}

fn display_agent_name(task_name: &str) -> String {
    match task_name.strip_prefix("/root/").unwrap_or(task_name) {
        "claude" => "Claude".to_string(),
        "gpt" => "GPT".to_string(),
        name => name.to_string(),
    }
}

fn display_subagent_model(agent: &SubagentSnapshot) -> String {
    let explicit = agent.model.as_deref();
    let model = explicit.or(match (agent.provider, agent.task_name.as_str()) {
        (CodingProvider::Claude, "/root/claude") => Some(borg_provider::claude_product_model()),
        _ => None,
    });
    model
        .map(str::to_string)
        .or_else(|| {
            borg_provider::model_catalog_for_backend(agent.provider.catalog_backend())
                .map(|catalog| catalog.default_model.to_string())
        })
        .unwrap_or_else(|| agent.provider.catalog_backend().to_string())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct AgentRosterColumns {
    name: usize,
    model: usize,
    effort: Option<usize>,
    state: Option<usize>,
    usage: Option<usize>,
}

fn team_roster_table_width(entries: &[AgentRosterEntry]) -> u16 {
    let columns = team_roster_table_columns(entries, usize::MAX);
    roster_columns_width(columns).min(u16::MAX as usize) as u16
}

fn team_roster_table_lines(
    entries: &[AgentRosterEntry],
    width: usize,
    focused_child: Option<Uuid>,
    hovered_row: Option<usize>,
    language: UiLanguage,
) -> Vec<Line<'static>> {
    let columns = team_roster_table_columns(entries, width);
    let header = roster_table_row(
        "  ",
        ui_text(language, "AGENT"),
        ui_text(language, "MODEL NOW"),
        ui_text(language, "EFFORT"),
        ui_text(language, "STATE"),
        ui_text(language, "LIFETIME TOKENS · COST"),
        columns,
    );
    std::iter::once(Line::from(Span::styled(
        header,
        Style::default()
            .fg(Color::DarkGray)
            .bg(COMMAND_PANEL_BG)
            .add_modifier(Modifier::BOLD),
    )))
    .chain(entries.iter().enumerate().map(|(index, entry)| {
        let focused = entry.child_id == focused_child;
        let row = roster_table_row(
            if focused { "› " } else { "  " },
            &entry.name,
            &entry.model,
            ui_text(language, &entry.effort),
            ui_text(language, &entry.state),
            &entry.usage,
            columns,
        );
        Line::from(row).style(team_roster_row_style(focused, hovered_row == Some(index)))
    }))
    .collect()
}

fn team_roster_table_columns(entries: &[AgentRosterEntry], width: usize) -> AgentRosterColumns {
    let column_width = |header: &str, value: fn(&AgentRosterEntry) -> &str, cap: usize| {
        entries
            .iter()
            .map(value)
            .map(UnicodeWidthStr::width)
            .chain(std::iter::once(header.width()))
            .max()
            .unwrap_or(1)
            .min(cap)
    };
    let mut columns = AgentRosterColumns {
        name: column_width("AGENT", |entry| &entry.name, 34),
        model: column_width("MODEL NOW", |entry| &entry.model, 20),
        effort: Some(column_width("EFFORT", |entry| &entry.effort, 8)),
        state: Some(column_width("STATE", |entry| &entry.state, 17)),
        usage: Some(column_width(
            "LIFETIME TOKENS · COST",
            |entry| &entry.usage,
            36,
        )),
    };
    while roster_columns_width(columns) > width && columns.name > 12 {
        columns.name -= 1;
    }
    if roster_columns_width(columns) > width {
        columns.effort = None;
    }
    if roster_columns_width(columns) > width {
        columns.usage = None;
    }
    if roster_columns_width(columns) > width {
        columns.state = None;
    }
    while roster_columns_width(columns) > width && columns.model > 5 {
        columns.model -= 1;
    }
    columns
}

fn roster_columns_width(columns: AgentRosterColumns) -> usize {
    let visible = 2
        + usize::from(columns.effort.is_some())
        + usize::from(columns.state.is_some())
        + usize::from(columns.usage.is_some());
    2 + columns.name
        + columns.model
        + columns.effort.unwrap_or(0)
        + columns.state.unwrap_or(0)
        + columns.usage.unwrap_or(0)
        + visible.saturating_sub(1) * 2
}

#[allow(clippy::too_many_arguments)]
fn roster_table_row(
    marker: &str,
    name: &str,
    model: &str,
    effort: &str,
    state: &str,
    usage: &str,
    columns: AgentRosterColumns,
) -> String {
    let mut cells = vec![
        roster_table_cell(name, columns.name),
        roster_table_cell(model, columns.model),
    ];
    if let Some(width) = columns.effort {
        cells.push(roster_table_cell(effort, width));
    }
    if let Some(width) = columns.state {
        cells.push(roster_table_cell(state, width));
    }
    if let Some(width) = columns.usage {
        cells.push(roster_table_cell(usage, width));
    }
    format!("{marker}{}", cells.join("  "))
}

fn roster_table_cell(value: &str, width: usize) -> String {
    let value = truncate_table_cell(value, width);
    let padding = width.saturating_sub(value.width());
    format!("{value}{}", " ".repeat(padding))
}

fn team_roster_row_style(focused: bool, hovered: bool) -> Style {
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
    #[cfg(test)]
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
            if !event.kind.is_recallable_user_message() {
                continue;
            }
            let SessionEventKind::Message {
                message_id,
                text,
                attachments,
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

    #[cfg(test)]
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
        let start = self.cursor;
        self.cursor = end;
        self.backspace_to(start);
    }

    fn backspace_line(&mut self) {
        let end = self.cursor;
        self.move_line_start();
        let start = self.cursor;
        self.cursor = end;
        self.backspace_to(start);
    }

    fn backspace_to(&mut self, mut start: usize) {
        let end = self.cursor;
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

    /// Moves to the end of the current or next word, mirroring `move_word_left`
    /// (which moves to a word start) so Option/Ctrl+Right, Alt+f, and forward
    /// word deletion all agree on the same boundary.
    fn move_word_right(&mut self) {
        self.cursor = self
            .text
            .unicode_word_indices()
            .map(|(start, word)| start + word.len())
            .find(|end| *end > self.cursor)
            .unwrap_or(self.text.len());
        self.preferred_column = None;
    }

    fn delete_word(&mut self) {
        let start = self.cursor;
        self.move_word_right();
        // Inline tokens (attachments, pasted blocks) are deleted whole.
        let token_end = self
            .attachments
            .iter()
            .map(|attachment| (attachment.start, attachment.end))
            .chain(
                self.pasted_texts
                    .iter()
                    .map(|pasted| (pasted.start, pasted.end)),
            )
            .filter(|(token_start, token_end)| {
                *token_start < self.cursor && *token_end > self.cursor
            })
            .map(|(_, token_end)| token_end)
            .max();
        if let Some(token_end) = token_end {
            self.cursor = token_end;
        }
        self.backspace_to(start);
    }

    fn move_line_start(&mut self) {
        self.cursor = self.text[..self.cursor]
            .rfind('\n')
            .map_or(0, |newline| newline + 1);
        self.preferred_column = None;
    }

    fn move_line_end(&mut self) {
        self.cursor = self.text[self.cursor..]
            .find('\n')
            .map_or(self.text.len(), |newline| self.cursor + newline);
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

fn is_ctrl_c(key: &KeyEvent) -> bool {
    key.modifiers.contains(KeyModifiers::CONTROL)
        && matches!(key.code, KeyCode::Char('c' | 'C' | '\u{3}'))
}

fn is_selection_copy_shortcut(key: &KeyEvent) -> bool {
    matches!(key.code, KeyCode::Char('c' | 'C'))
        && (key.modifiers.contains(KeyModifiers::SUPER)
            || key
                .modifiers
                .contains(KeyModifiers::CONTROL | KeyModifiers::SHIFT))
}

fn repeated_ctrl_c(last: &mut Option<Instant>, count: &mut u8, now: Instant) -> bool {
    if last
        .is_some_and(|previous| now.saturating_duration_since(previous) <= CTRL_C_SEQUENCE_WINDOW)
    {
        *count = count.saturating_add(1);
    } else {
        *count = 1;
    }
    *last = Some(now);
    if *count < 2 {
        return false;
    }
    *last = None;
    *count = 0;
    true
}

fn deletes_line_prefix(key: &KeyEvent) -> bool {
    (key.code == KeyCode::Backspace
        && key
            .modifiers
            .intersects(KeyModifiers::SUPER | KeyModifiers::META))
        || (key.code == KeyCode::Char('u') && key.modifiers.contains(KeyModifiers::CONTROL))
        || key.code == KeyCode::Char('\u{15}')
}

fn composer_inserts_character(key: &KeyEvent) -> bool {
    matches!(key.code, KeyCode::Char(character) if !character.is_control())
        && !key
            .modifiers
            .intersects(KeyModifiers::CONTROL | KeyModifiers::SUPER | KeyModifiers::META)
}

fn deletes_previous_word(key: &KeyEvent) -> bool {
    (key.modifiers.contains(KeyModifiers::CONTROL)
        && matches!(
            key.code,
            KeyCode::Backspace | KeyCode::Char('h' | 'w') | KeyCode::Char('\u{8}' | '\u{17}')
        ))
        || (key.modifiers.contains(KeyModifiers::ALT) && key.code == KeyCode::Backspace)
}

fn deletes_next_word(key: &KeyEvent) -> bool {
    (key.code == KeyCode::Delete
        && key
            .modifiers
            .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT))
        || (key.modifiers.contains(KeyModifiers::ALT) && key.code == KeyCode::Char('d'))
}

include!("transcript.rs");

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
    let displayed_tokens = (usage.total_tokens > 0).then_some(usage.total_tokens);
    if displayed_tokens.is_none() && usage.cost_microusd.is_none() {
        return "  —".to_string();
    }
    let mut parts = Vec::new();
    if let Some(tokens) = displayed_tokens {
        parts.push(format_compact_token_total(tokens));
    }
    if let Some(cost_microusd) = usage.cost_microusd {
        let cost = cost_microusd as f64 / 1_000_000.0;
        let amount = if cost >= 1.0 {
            format!("{cost:.2}")
        } else {
            format!("{cost:.4}")
        };
        let (prefix, basis) = match usage.cost_basis.as_str() {
            "subscription_equivalent" => ("~", "sub eq."),
            "estimated_from_pricing" => ("~", "est"),
            "mixed" => ("~", "mix"),
            "provider_reported" | "provider" => ("", "provider"),
            _ => ("", "basis unknown"),
        };
        let coverage = match usage.cost_complete {
            Some(true) => "",
            Some(false) => ", partial",
            None => ", unverified",
        };
        let label = format!("{prefix}${amount} ({basis}{coverage})");
        parts.push(label);
    } else if displayed_tokens.is_some() {
        parts.push("cost unavailable".to_string());
    }
    format!("  {}", parts.join(" · "))
}

fn format_compact_token_total(value: u64) -> String {
    if value >= 1_000_000 {
        format!("{:.1}m", value as f64 / 1_000_000.0)
    } else if value >= 1_000 {
        format!("{:.1}k", value as f64 / 1_000.0)
    } else {
        value.to_string()
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
    last_advance: Option<Instant>,
}

/// The frame the wheel easing is tuned for. A late frame applies every
/// nominal frame it missed, so slow draws make scrolling coarser rather than
/// leaving a backlog that keeps moving after the wheel stops.
const SCROLL_MOTION_FRAME: Duration = Duration::from_millis(16);

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
        self.last_advance = None;
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

    /// Advance by the nominal frames elapsed since the previous advance.
    fn advance_at(&mut self, scroll_from_bottom: usize, scroll_max: usize, now: Instant) -> usize {
        let frames = self.last_advance.map_or(1, |last| {
            (now.saturating_duration_since(last).as_millis() / SCROLL_MOTION_FRAME.as_millis())
                .clamp(1, 64) as usize
        });
        let mut scroll = scroll_from_bottom;
        for _ in 0..frames {
            if !self.is_active() {
                break;
            }
            scroll = self.advance(scroll, scroll_max);
        }
        self.last_advance = self.is_active().then_some(now);
        scroll
    }

    fn take_pending(&mut self) -> isize {
        std::mem::take(&mut self.remaining_lines)
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
        | SessionEventKind::SessionTitled { .. }
        | SessionEventKind::SessionConfigured { .. }
        | SessionEventKind::ProviderCapabilitiesUpdated { .. }
        | SessionEventKind::EffectiveCapabilitiesUpdated { .. }
        | SessionEventKind::ApprovalResolved { .. }
        | SessionEventKind::ProviderInteractionResolved { .. }
        | SessionEventKind::UsageUpdated { .. }
        | SessionEventKind::ContextWindowUpdated { .. }
        | SessionEventKind::UserStopChanged { .. }
        | SessionEventKind::SubagentControl { .. }
        | SessionEventKind::WatchesChanged { .. }
        | SessionEventKind::ProviderSessionLinked { .. }
        | SessionEventKind::RuntimeProcessStarted { .. }
        | SessionEventKind::RuntimeProcessOutput { .. }
        | SessionEventKind::BluWorkflowStarted { .. }
        | SessionEventKind::BluWorkflowCallRequested { .. }
        | SessionEventKind::BluWorkflowCallCompleted { .. }
        | SessionEventKind::BluWorkflowCompleted { .. }
        | SessionEventKind::RuntimeWorkflowStarted { .. }
        | SessionEventKind::RuntimeWorkflowCallRequested { .. }
        | SessionEventKind::RuntimeWorkflowCallCompleted { .. }
        | SessionEventKind::RuntimeWorkflowCompleted { .. }
        | SessionEventKind::TurnStarted { .. } => false,
        SessionEventKind::RuntimeProcessCompleted { .. } => true,
        SessionEventKind::StatusChanged {
            status:
                SessionStatus::Ready
                | SessionStatus::Completed
                | SessionStatus::Failed
                | SessionStatus::Stopped,
            ..
        } => true,
        SessionEventKind::StatusChanged { .. } => false,
        SessionEventKind::ProviderEvent { kind, payload, .. } => {
            is_context_compaction(kind)
                || is_live_tool_call_event(kind)
                || kind == "action/preparing"
                || kind == "action/generation_status"
                || kind == "action/preparing_cancelled"
                || kind == "mcp_server_unavailable"
                // `reasoning/started` opens the Thinking row. Without this the
                // row only materialises on `reasoning_completed`, i.e. in the
                // same frame as the tool call it precedes.
                || Transcript::provider_reasoning_lifecycle(kind, payload).is_some()
        }
        SessionEventKind::SubagentActivity {
            activity,
            agent,
            event,
        } => subagent_activity_summary(*activity, agent, event.as_deref()).is_some(),
        SessionEventKind::AgentMessageReceived { .. }
        | SessionEventKind::Message { .. }
        | SessionEventKind::MessageDelta { .. }
        | SessionEventKind::ReasoningDelta { .. }
        | SessionEventKind::ReasoningTextDelta { .. }
        | SessionEventKind::ReasoningCompleted
        | SessionEventKind::ToolStarted { .. }
        | SessionEventKind::ToolUpdated { .. }
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

pub fn subagent_activity_summary(
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
                detail: Some(detail),
            }) if ready_detail_is_failure(detail) => Some(format!(
                "agent · {task} · failed turn · {}",
                compact_text(detail, 120)
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

/// Keep legacy report bodies until a standalone AgentMessageReceived owns them.
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
                detail: Some(detail),
            }) if ready_detail_is_failure(detail) => project(
                format!("failed turn · {}", compact_text(detail, 120)),
                Some(detail.clone()),
                TranscriptActionState::Failed,
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
                SubagentStatus::Ready => {
                    if let Some(detail) = agent
                        .detail
                        .as_deref()
                        .filter(|detail| ready_detail_is_failure(detail))
                    {
                        project(
                            format!("failed turn · {}", compact_text(detail, 120)),
                            Some(detail.to_string()),
                            TranscriptActionState::Failed,
                        )
                    } else {
                        Some((
                            "Agent".to_string(),
                            format!("{task} · done · waiting for input"),
                            agent.final_text.clone(),
                            TranscriptActionState::Complete,
                        ))
                    }
                }
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

fn ready_detail_is_failure(detail: &str) -> bool {
    let detail = detail.trim().to_ascii_lowercase();
    detail.starts_with("borg blocked a provider-native delegation attempt")
        || detail.starts_with("turn failed")
        || detail.starts_with("could not wake")
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
    if left.is_empty() || right.is_empty() {
        return 0;
    }
    let pattern = right.as_bytes();
    let mut prefix = vec![0; pattern.len()];
    for index in 1..pattern.len() {
        let mut matched = prefix[index - 1];
        while matched > 0 && pattern[index] != pattern[matched] {
            matched = prefix[matched - 1];
        }
        if pattern[index] == pattern[matched] {
            matched += 1;
        }
        prefix[index] = matched;
    }

    let mut tail_start = left.len().saturating_sub(pattern.len());
    while !left.is_char_boundary(tail_start) {
        tail_start += 1;
    }
    let tail = &left.as_bytes()[tail_start..];
    let mut matched = 0;
    for (index, byte) in tail.iter().enumerate() {
        while matched > 0 && *byte != pattern[matched] {
            matched = prefix[matched - 1];
        }
        if *byte == pattern[matched] {
            matched += 1;
        }
        if matched == pattern.len() && index + 1 < tail.len() {
            matched = prefix[matched - 1];
        }
    }
    debug_assert!(right.is_char_boundary(matched));
    debug_assert!(left.is_char_boundary(left.len() - matched));
    matched
}

#[test]
#[ignore = "manual pathological TUI reasoning-overlap profile"]
fn large_reasoning_snapshot_overlap_stays_linear() {
    let bytes = 256 * 1024;
    let left = format!("{}b", "a".repeat(bytes - 1));
    let right = "a".repeat(bytes);

    let started = Instant::now();
    assert_eq!(longest_suffix_prefix_overlap(&left, &right), 0);
    let elapsed = started.elapsed();
    eprintln!("256 KiB pathological TUI reasoning overlap: {elapsed:?}");

    assert!(
        elapsed < Duration::from_millis(50),
        "TUI reasoning overlap exceeded 50 ms: {elapsed:?}"
    );
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

/// What an incoming event means for a withheld optimistic idle submission.
#[derive(Debug, Clone, PartialEq, Eq)]
enum OptimisticPromptDecision {
    /// The pre-`TurnStarted` queued transient for our own prompt: hold it.
    Withhold(PendingPromptProjection),
    /// Our prompt started its turn; the hold served its purpose.
    Settled,
    /// Another prompt settled first, so ours is genuinely pending.
    Release,
    /// Unrelated event.
    Ignore,
}

fn optimistic_idle_prompt_decision(
    event: &SessionEventKind,
    optimistic: Uuid,
) -> OptimisticPromptDecision {
    match event {
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text,
            status: MessageStatus::Queued,
            delivery: Some(delivery),
            ..
        } if *message_id == optimistic => {
            OptimisticPromptDecision::Withhold(PendingPromptProjection {
                message_id: *message_id,
                text: text.clone(),
                delivery: *delivery,
                actor: EventActor::User,
            })
        }
        SessionEventKind::TurnStarted { message_id, .. } if *message_id == optimistic => {
            OptimisticPromptDecision::Settled
        }
        SessionEventKind::TurnStarted { .. }
        | SessionEventKind::TurnCompleted { .. }
        | SessionEventKind::PromptRecalled { .. }
        | SessionEventKind::Message {
            actor: EventActor::User,
            status: MessageStatus::Complete | MessageStatus::InProgress | MessageStatus::Failed,
            ..
        } => OptimisticPromptDecision::Release,
        _ => OptimisticPromptDecision::Ignore,
    }
}

fn update_queued_prompts(
    queued_prompts: &mut Vec<PendingPromptProjection>,
    event: &SessionEventKind,
    requeue_cursor: &mut Option<usize>,
) {
    if let SessionEventKind::Message {
        message_id,
        actor: EventActor::System,
        status,
        ..
    } = event
    {
        // Team updates belong to the session's durable inbox, not the human
        // Pending Input panel. A correction also removes any legacy row that
        // was first journaled with the same ID as User.
        if let Some(index) = queued_prompts
            .iter()
            .position(|queued| queued.message_id == *message_id)
        {
            queued_prompts.remove(index);
            if let Some(cursor) = requeue_cursor
                && index < *cursor
            {
                *cursor -= 1;
            }
        }
        if *status != MessageStatus::Queued {
            *requeue_cursor = None;
        }
        return;
    }
    match event {
        SessionEventKind::TurnCompleted { error: Some(_), .. } => {
            // The session re-queues a failed turn's prompts at the head of its
            // FIFO, ahead of anything queued while that turn ran.
            *requeue_cursor = Some(0);
            return;
        }
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text,
            status: MessageStatus::Queued,
            delivery: Some(delivery),
            ..
        } => {
            if let Some(cursor) = requeue_cursor {
                queued_prompts.retain(|queued| queued.message_id != *message_id);
                let at = (*cursor).min(queued_prompts.len());
                queued_prompts.insert(
                    at,
                    PendingPromptProjection {
                        message_id: *message_id,
                        text: text.clone(),
                        delivery: *delivery,
                        actor: EventActor::User,
                    },
                );
                *cursor = at + 1;
            } else {
                push_queued_prompt(
                    queued_prompts,
                    *message_id,
                    text.clone(),
                    *delivery,
                    EventActor::User,
                );
            }
            return;
        }
        _ => {}
    }
    *requeue_cursor = None;
    match event {
        SessionEventKind::TurnStarted { message_id, .. }
        | SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            status: MessageStatus::InProgress,
            ..
        } => {
            // An in-progress user message is admitted (as a turn prompt, a
            // batch member, or a steer) and is no longer pending input.
            queued_prompts.retain(|queued| queued.message_id != *message_id);
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
                        let retain = index > admitted
                            || queued.delivery != PromptDelivery::Queue
                            || queued.actor != EventActor::User;
                        index += 1;
                        retain
                    });
                } else {
                    queued_prompts.remove(admitted);
                }
            } else if *delivery == Some(PromptDelivery::Queue) {
                // A later human prompt was admitted while older projected
                // queue entries remained.
                queued_prompts.retain(|queued| {
                    queued.delivery != PromptDelivery::Queue || queued.actor != EventActor::User
                });
            }
        }
        SessionEventKind::Message { message_id, .. }
        | SessionEventKind::PromptRecalled { message_id, .. } => {
            queued_prompts.retain(|queued| queued.message_id != *message_id);
        }
        _ => {}
    }
}

fn turn_completion_clears_followup_marker(
    event: &SessionEventKind,
    pending_queue_is_empty: bool,
) -> bool {
    matches!(event, SessionEventKind::TurnCompleted { .. }) && pending_queue_is_empty
}

fn pending_prompt_projection_from_events(events: &[SessionEvent]) -> Vec<PendingPromptProjection> {
    let mut queued_prompts = Vec::new();
    let mut requeue_cursor = None;
    for event in events {
        update_queued_prompts(&mut queued_prompts, &event.kind, &mut requeue_cursor);
    }
    queued_prompts
}

fn restore_optimistic_pending_prompts(
    queued_prompts: &mut Vec<PendingPromptProjection>,
    events: &[SessionEvent],
    optimistic_pending: Vec<PendingPromptProjection>,
) {
    for pending in optimistic_pending {
        if pending.actor == EventActor::User
            && !queued_prompts
                .iter()
                .any(|queued| queued.message_id == pending.message_id)
            && !events
                .iter()
                .any(|event| pending_prompt_projection_settled_by(event, pending.message_id))
        {
            push_queued_prompt(
                queued_prompts,
                pending.message_id,
                pending.text,
                pending.delivery,
                pending.actor,
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
            status: MessageStatus::InProgress | MessageStatus::Complete | MessageStatus::Failed,
            ..
        }
        | SessionEventKind::Message {
            message_id: event_message_id,
            actor: EventActor::System,
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
    actor: EventActor,
) {
    if let Some(queued) = queued_prompts
        .iter_mut()
        .find(|queued| queued.message_id == message_id)
    {
        queued.text = text;
        queued.delivery = delivery;
        queued.actor = actor;
    } else {
        queued_prompts.push(PendingPromptProjection {
            message_id,
            text,
            delivery,
            actor,
        });
    }
}

fn has_recallable_queued_prompts(
    composer_text: &str,
    queued_prompts: &[PendingPromptProjection],
) -> bool {
    composer_text.trim().is_empty()
        && queued_prompts.iter().any(|prompt| {
            prompt.actor == EventActor::User && prompt.delivery == PromptDelivery::Queue
        })
}

fn has_pending_steer_prompts(
    composer_text: &str,
    queued_prompts: &[PendingPromptProjection],
) -> bool {
    composer_text.trim().is_empty()
        && queued_prompts.iter().any(|prompt| {
            prompt.actor == EventActor::User && prompt.delivery == PromptDelivery::Steer
        })
}

fn queued_prompt_panel_height(
    queued_prompts: &[PendingPromptProjection],
    panel_width: u16,
    expanded: bool,
) -> u16 {
    if queued_prompts.is_empty() {
        return 0;
    }
    if !expanded {
        return 1;
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

fn pending_input_title(
    language: UiLanguage,
    count: usize,
    expanded: bool,
    panel_width: u16,
) -> String {
    let (arrow, action) = if expanded {
        ("▾", "collapse")
    } else {
        ("▸", "expand")
    };
    let full = format!(
        " {arrow} {} · {count} · click to {action} ",
        ui_text(language, "Pending Input")
    );
    if full.width() < usize::from(panel_width) {
        return full;
    }
    let compact = format!(" {arrow} {} · {count} ", ui_text(language, "Pending Input"));
    if compact.width() < usize::from(panel_width) {
        return compact;
    }
    let short = format!(" {arrow} {count} pending ");
    if short.width() < usize::from(panel_width) {
        short
    } else {
        format!(" {arrow} {count} ")
    }
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
    lines.push(Line::from(Span::styled(
        "   esc send input · keep running  ·  ↑ edit / recall input",
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
            Self::Message {
                text, attachments, ..
            } if !attachments.is_empty() => {
                let images = attachments
                    .iter()
                    .map(|(_, path)| {
                        url::Url::from_file_path(path)
                            .ok()
                            .map(|url| format!("![Borg image]({url})"))
                    })
                    .collect::<Option<Vec<_>>>()?;
                Some(format!("{}\n\n{}", text, images.join("\n")))
            }
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
        if span.style.bg.is_none() {
            span.style = span.style.bg(background);
        }
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

/// A code row carrying the dashed gutter continues the previous row's source
/// line. `syntax_lines` wraps a long line across rows to keep every byte on
/// screen, and that wrap is a display artefact: copying has to rejoin the rows
/// or the clipboard gets a line break in the middle of a path.
///
/// Only the gutter `syntax_lines` actually draws counts: blanks, then the
/// dashed bar, then one space, in the gutter's own colour. Matching the
/// character in unrelated message text would let it be read as a
/// continuation, dropping content and splicing the line onto the one above.
/// A message margin may precede the actual gutter span.
fn code_gutter_span<'a>(line: &'a Line<'static>) -> Option<(usize, &'a Span<'static>)> {
    let first = line.spans.first()?;
    let (margin, gutter) = if matches!(first.content.as_ref(), "  " | "  │ ") {
        (first.width(), line.spans.get(1)?)
    } else {
        (0, first)
    };
    (gutter.style.fg == Some(Color::DarkGray)).then_some((margin, gutter))
}

fn is_wrapped_code_continuation(line: &Line<'static>) -> bool {
    let Some((_, gutter)) = code_gutter_span(line) else {
        return false;
    };
    let Some(indent) = gutter.content.strip_suffix("┊ ") else {
        return false;
    };
    !indent.is_empty() && indent.chars().all(|character| character == ' ')
}

/// The first row of a code line, carrying its source line number.
fn is_numbered_code_row(line: &Line<'static>) -> bool {
    let Some((_, gutter)) = code_gutter_span(line) else {
        return false;
    };
    let content = gutter.content.as_ref();
    let Some(bar) = content.find('│') else {
        return false;
    };
    content[..bar]
        .chars()
        .any(|character| character.is_ascii_digit())
}

fn is_code_gutter_row(line: &Line<'static>) -> bool {
    is_numbered_code_row(line) || is_wrapped_code_continuation(line)
}

/// The column after the last span that carries content. A hovered row is
/// padded out to the viewport with a background-only span, and that padding
/// must never reach the clipboard -- but a code line's own trailing
/// whitespace must, and trimming the row cannot tell the two apart. Padding
/// has no foreground colour; rendered source always does.
fn selection_content_columns(line: &Line<'static>) -> usize {
    let mut content_end = 0usize;
    let mut column = 0usize;
    for span in &line.spans {
        column = column.saturating_add(span.content.width());
        let padding = span.style.fg.is_none() && span.content.chars().all(|c| c == ' ');
        if !padding {
            content_end = column;
        }
    }
    content_end
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
    if is_code_gutter_row(line) {
        let (margin, gutter) = code_gutter_span(line).expect("code row has a gutter");
        let gutter = margin.saturating_add(gutter.width());
        let content_end = selection_content_columns(line);
        return (gutter < content_end)
            .then_some((gutter, content_end))
            .into_iter()
            .collect();
    }
    let prefix = if first == "  " {
        2 + line.spans.get(1).map_or(0, |span| {
            let gutter = span.content.as_ref();
            if (span.style.fg == Some(BORG_ORANGE_HOVER) || span.style.fg == Some(Color::Gray))
                && gutter.ends_with(' ')
                && gutter.trim_end().split(' ').all(|part| part == "│")
            {
                span.width()
            } else {
                0
            }
        })
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
    let mut starts = Vec::new();
    let mut split_separator = None;
    for span in &line.spans {
        let content = span.content.as_ref();
        if content == " │ " {
            split_separator = Some(span_start);
        }
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
                starts.push(start);
                break;
            }
        }
        span_start = span_start.saturating_add(span.width());
    }
    if starts.is_empty() {
        return None;
    }
    let rendered = line.to_string();
    let width = line.width();
    Some(
        starts
            .into_iter()
            .map(|start| {
                let pane_end = split_separator
                    .filter(|separator| start < *separator)
                    .unwrap_or(width);
                (start, trimmed_cell_end(&rendered, start, pane_end))
            })
            .filter(|(start, end)| start < end)
            .collect(),
    )
}

fn trimmed_cell_end(value: &str, start: usize, end: usize) -> usize {
    let mut column = 0usize;
    let mut trimmed_end = start;
    for grapheme in value.graphemes(true) {
        let next = column.saturating_add(grapheme.width());
        if column >= end {
            break;
        }
        if next > start && !grapheme.chars().all(char::is_whitespace) {
            trimmed_end = next.min(end);
        }
        column = next;
    }
    trimmed_end
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
        // A following row that resumes this source line means this row is not
        // the end of a line, so it must not be trimmed or newline-separated.
        let continues =
            row < last_row && lines.get(row + 1).is_some_and(is_wrapped_code_continuation);
        // Code keeps its own trailing whitespace: it can be significant, and
        // the selectable range already stops before any hover padding.
        let code_row = is_code_gutter_row(line);
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
            // The wrap point keeps its space on this row. Trimming it here
            // is what would splice two arguments of a command together, so
            // the trailing run survives whenever the next row resumes it.
            let chunk = if continues || code_row {
                chunk
            } else {
                chunk.trim_end().to_string()
            };
            if continues || code_row || !chunk.trim().is_empty() {
                chunks.push(chunk);
            }
        }
        if chunks.is_empty() && code_row {
            // A blank line inside a code block is source. Skipping the row
            // would close the gap and merge the lines on either side of it.
            selected.push(String::new());
            continue;
        }
        if !chunks.is_empty() {
            if chunks.len() == 2 && chunks[0] == chunks[1] {
                chunks.truncate(1);
            }
            let joined = chunks.join("\n");
            match selected.last_mut() {
                // Rejoin a wrapped row onto the line it came from. Pushing it
                // separately is what put a newline inside a copied path.
                Some(previous) if is_wrapped_code_continuation(line) => {
                    previous.push_str(&joined);
                }
                _ => selected.push(joined),
            }
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

fn scrollbar_thumb_geometry(
    track_height: u16,
    transcript_height: usize,
    scroll: usize,
    scroll_max: usize,
) -> (u16, u16) {
    let minimum_thumb_height = MIN_SCROLLBAR_THUMB_ROWS.min(track_height);
    let thumb_height =
        ((u64::from(track_height) * u64::from(track_height)) / transcript_height.max(1) as u64)
            .clamp(u64::from(minimum_thumb_height), u64::from(track_height)) as u16;
    let thumb_travel = track_height.saturating_sub(thumb_height);
    let thumb_top = if scroll_max == 0 {
        0
    } else {
        (scroll.min(scroll_max) as u64 * u64::from(thumb_travel) / scroll_max as u64) as u16
    };
    (thumb_top, thumb_height)
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

fn thread_find_matches(regex: &Regex, lines: &[Line<'static>]) -> Vec<usize> {
    lines
        .iter()
        .enumerate()
        .filter_map(|(row, line)| regex.is_match(&line.to_string()).then_some(row))
        .collect()
}

fn next_thread_match(matches: &[usize], previous_row: Option<usize>) -> (usize, usize) {
    let position = previous_row
        .and_then(|previous| matches.iter().position(|row| *row > previous))
        .unwrap_or(0);
    (matches[position], position + 1)
}

/// Commands whose bare form is not a command at all: submitting `/steer` with
/// no message would send the literal word to the model, so the palette puts
/// them in the composer for the user to finish.
fn slash_command_needs_argument(command: &str) -> bool {
    matches!(
        command,
        "/ask"
            | "/director"
            | "/claude"
            | "/gpt"
            | "/peer"
            | "/queue"
            | "/steer"
            | "/team"
            | "/broadcast"
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
        option.key_hint = Some(chord);
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

fn primary_controls_line(keymap: &KeyMap, language: UiLanguage) -> String {
    format!(
        "{} {} · {} / · {} tab or {}",
        ui_text(language, "send"),
        keymap.label(KeyAction::Send),
        ui_text(language, "commands"),
        ui_text(language, "palette menu"),
        keymap.label(KeyAction::Keybindings)
    )
}

fn primary_controls_spans(keymap: &KeyMap, language: UiLanguage) -> Vec<Span<'static>> {
    let binding_style = Style::default().fg(Color::DarkGray);
    let key_style = Style::default().fg(Color::Gray);
    vec![
        Span::styled(format!("{} ", ui_text(language, "send")), binding_style),
        Span::styled(keymap.label(KeyAction::Send), key_style),
        Span::styled(
            format!(" · {} ", ui_text(language, "commands")),
            binding_style,
        ),
        Span::styled("/", key_style),
        Span::styled(
            format!(" · {} ", ui_text(language, "palette menu")),
            binding_style,
        ),
        Span::styled("tab", key_style),
        Span::styled(" or ", binding_style),
        Span::styled(keymap.label(KeyAction::Keybindings), key_style),
    ]
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
        ("clear · twice exits", keymap.label(KeyAction::ClearOrExit)),
        ("exit", keymap.label(KeyAction::Exit)),
        ("paste clipboard", keymap.label(KeyAction::AttachImage)),
        ("start/stop dictation", keymap.label(KeyAction::Dictate)),
        ("copy selection/response", keymap.label(KeyAction::Copy)),
        ("find in thread", keymap.label(KeyAction::Find)),
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

    let key_width = bindings
        .iter()
        .map(|(_, key)| key.width())
        .max()
        .unwrap_or(0)
        .min(width.saturating_sub(4) / 2)
        .max(1);
    let action_width = width.saturating_sub(key_width + 3).max(1);
    bindings
        .iter()
        .flat_map(|(action, key)| {
            let keys = wrap_display(key, key_width);
            let actions = wrap_display(action, action_width);
            (0..keys.len().max(actions.len()))
                .map(|row| {
                    let key = keys.get(row).cloned().unwrap_or_default();
                    let action = actions.get(row).cloned().unwrap_or_default();
                    Line::from(vec![
                        Span::styled(key.clone(), key_style),
                        Span::raw(" ".repeat(key_width.saturating_sub(key.width()))),
                        Span::styled(" │ ", separator_style),
                        Span::styled(action, action_style),
                    ])
                })
                .collect::<Vec<_>>()
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

#[allow(clippy::too_many_arguments)]
fn cached_transcript_render(
    transcript: &Transcript,
    cache: &mut Option<CachedTranscriptRender>,
    width: usize,
    tool_run_viewport_height: usize,
    goal_tick: Option<i64>,
    current_tool_elapsed: &[(usize, Option<String>)],
    local_date: NaiveDate,
    render_time: DateTime<Utc>,
) -> Arc<TranscriptRender> {
    cache
        .as_ref()
        .filter(
            |(
                cached_width,
                cached_tool_run_viewport_height,
                cached_goal_tick,
                _,
                cached_date,
                render,
            )| {
                *cached_width == width
                    && *cached_tool_run_viewport_height == tool_run_viewport_height
                    && *cached_goal_tick == goal_tick
                    && tool_elapsed_widths_match(&render.7, current_tool_elapsed)
                    && *cached_date == local_date
            },
        )
        .map(|(_, _, _, _, _, render)| Arc::clone(render))
        .unwrap_or_else(|| {
            // Release the stale render first so the transcript can redraw
            // only its changed tail in place.
            cache.take();
            let render =
                transcript.render_for_cache_at(width, tool_run_viewport_height, render_time);
            let tool_elapsed_tick = transcript.tool_elapsed_cache_tick();
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

/// Fast-path draws reuse this snapshot verbatim and only refresh timers through
/// `refresh_tool_elapsed_line`, which resolves labels by transcript order index
/// and rewrites equal-length text. So it stays reusable only while its label set
/// still matches the live one: a renumbered index (goal/plan upsert, message
/// insert or removal) or a changed width both strand the timer.
fn committed_viewport_is_reusable(
    cached: &CachedTranscriptRender,
    width: usize,
    tool_run_viewport_height: usize,
    current_tool_elapsed: &[(usize, Option<String>)],
) -> bool {
    let (cached_width, cached_tool_run_viewport_height, _, _, _, render) = cached;
    *cached_width == width
        && *cached_tool_run_viewport_height == tool_run_viewport_height
        && tool_elapsed_widths_match(&render.7, current_tool_elapsed)
}

fn tool_elapsed_widths_match(
    cached: &[(usize, Option<String>)],
    current: &[(usize, Option<String>)],
) -> bool {
    cached.len() == current.len()
        && cached
            .iter()
            .zip(current)
            .all(|((cached_index, cached), (current_index, current))| {
                cached_index == current_index
                    && cached.as_deref().map(UnicodeWidthStr::width)
                        == current.as_deref().map(UnicodeWidthStr::width)
            })
}

fn refresh_tool_elapsed_line(
    line: &mut Line<'static>,
    tool_index: usize,
    cached: &[(usize, Option<String>)],
    current: &[(usize, Option<String>)],
) {
    let cached = cached
        .iter()
        .find_map(|(index, elapsed)| (*index == tool_index).then_some(elapsed.as_deref()))
        .flatten();
    let current = current
        .iter()
        .find_map(|(index, elapsed)| (*index == tool_index).then_some(elapsed.as_deref()))
        .flatten();
    let (Some(cached), Some(current)) = (cached, current) else {
        return;
    };
    if cached == current || cached.len() != current.len() {
        return;
    }
    let Some(span) = line
        .spans
        .iter_mut()
        .rev()
        .find(|span| span.content.ends_with(cached))
    else {
        return;
    };
    let mut content = span.content.to_string();
    content.replace_range(content.len() - cached.len().., current);
    span.content = Cow::Owned(content);
}

fn select_transcript_snapshot<T, F>(
    input_fast_path: bool,
    transcript_snapshot_current: bool,
    committed_viewport_render: Option<T>,
    fallback_render: F,
) -> T
where
    F: FnOnce() -> T,
{
    if input_fast_path && transcript_snapshot_current {
        committed_viewport_render.unwrap_or_else(fallback_render)
    } else {
        fallback_render()
    }
}

fn reuse_current_transcript_width(input_fast_path: bool, snapshot_current: bool) -> bool {
    input_fast_path && snapshot_current
}

fn transcript_width_for_viewport(
    content_width: u16,
    transcript_height: usize,
    viewport_height: usize,
) -> usize {
    if transcript_height > viewport_height {
        transcript_width_with_gutter(content_width)
    } else {
        transcript_width_without_gutter(content_width)
    }
}

/// The transcript width when history fits on screen and no scrollbar is drawn.
fn transcript_width_without_gutter(content_width: u16) -> usize {
    content_width.max(1) as usize
}

/// The transcript width when a scrollbar is drawn and its gutter is reserved.
/// A terminal too narrow to spare the lane keeps every column it has.
fn transcript_width_with_gutter(content_width: u16) -> usize {
    if content_width > 4 {
        content_width
            .saturating_sub(TRANSCRIPT_SCROLLBAR_GUTTER_WIDTH)
            .max(1) as usize
    } else {
        content_width.max(1) as usize
    }
}

/// The width a frame lays history out at before its own height is known.
///
/// An ordinary frame measures at the ungutted width and lets
/// `transcript_width_for_viewport` reserve the gutter once the measured height
/// proves history overflows. An input-only redraw must not re-decide that: the
/// transcript is unchanged, so the width the last committed frame was rendered
/// at is still the correct one, and it is the width that frame is cached under.
/// Measuring at any other width keys the committed-snapshot lookup to a width
/// the snapshot was never rendered at, misses on every keystroke, and rebuilds
/// the history the fast path exists to reuse. A committed width that no longer
/// belongs to this terminal is discarded, so a resize still measures afresh.
fn transcript_frame_width(
    content_width: u16,
    input_fast_path: bool,
    committed_width: Option<usize>,
) -> usize {
    committed_width
        .filter(|_| input_fast_path)
        .filter(|width| {
            *width == transcript_width_without_gutter(content_width)
                || *width == transcript_width_with_gutter(content_width)
        })
        .unwrap_or_else(|| transcript_width_without_gutter(content_width))
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
    (content_height.min(u16::MAX as usize) as u16).saturating_add(1)
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
            Constraint::Length(u16::from(!is_launch_screen)),
            // On the launch screen the composer is nested in chunk zero. Do
            // not reserve it a second time at the bottom of the root layout.
            Constraint::Length(composer_height * u16::from(!is_launch_screen)),
            Constraint::Length(footer_height),
        ])
        .split(area);
    [chunks[0], chunks[1], chunks[2], chunks[3], chunks[4]]
}

fn centered_content_area_with_margin(area: Rect, margin: u16) -> Rect {
    let width = area.width.saturating_sub(margin.saturating_mul(2)).max(1);
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

fn footer_status_text(
    billing_status: Option<&str>,
    shell_status: Option<&str>,
    watch_status: Option<&str>,
    todo_status: Option<&str>,
) -> String {
    [billing_status, shell_status, watch_status, todo_status]
        .into_iter()
        .flatten()
        .filter(|status| !status.is_empty())
        .collect::<Vec<_>>()
        .join(STATUS_SEPARATOR)
}

fn footer_left_region_width(total_width: u16, right_width: u16) -> u16 {
    total_width
        .saturating_sub(right_width)
        .saturating_sub(u16::from(right_width > 0 && right_width < total_width))
}

#[cfg(test)]
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

#[allow(clippy::too_many_arguments)]
fn footer_shell_todo_metadata_line(
    billing_status: Option<&str>,
    shell_status: Option<&str>,
    watch_status: Option<&str>,
    todo_status: Option<&str>,
    cwd_status: &str,
    shell_hovered: bool,
    watch_hovered: bool,
    todo_hovered: bool,
    max_width: usize,
) -> Line<'static> {
    let status = footer_status_text(billing_status, shell_status, watch_status, todo_status);
    let metadata = footer_metadata_text(&status, cwd_status, max_width);
    if !metadata.starts_with(&status) {
        return Line::from(Span::styled(metadata, Style::default().fg(Color::Gray)));
    }
    let interactive_style = |hovered: bool, color| {
        Style::default()
            .fg(if hovered { Color::White } else { color })
            .add_modifier(if hovered {
                Modifier::BOLD | Modifier::UNDERLINED
            } else {
                Modifier::empty()
            })
    };
    // Billing is always visible so a switch between a subscription and
    // pay-as-you-go credentials is never silent. It sits on the footer row,
    // away from the effort level, so a "max sub" plan never reads as "max"
    // effort. Interactive tokens (shells, watches, to-dos) follow it.
    let parts = [
        billing_status.map(|billing| (billing, Style::default().fg(billing_status_color(billing)))),
        shell_status.map(|shell| (shell, interactive_style(shell_hovered, USER_LABEL_BLUE))),
        watch_status.map(|watch| (watch, interactive_style(watch_hovered, Color::Yellow))),
        todo_status.map(|todo| (todo, interactive_style(todo_hovered, Color::LightGreen))),
    ];
    let mut spans = Vec::new();
    for (text, style) in parts.into_iter().flatten() {
        if !spans.is_empty() {
            spans.push(Span::styled(
                STATUS_SEPARATOR,
                Style::default().fg(Color::Gray),
            ));
        }
        spans.push(Span::styled(text.to_string(), style));
    }
    spans.push(Span::styled(
        metadata[status.len()..].to_string(),
        Style::default().fg(Color::Gray),
    ));
    Line::from(spans)
}

fn shell_row_style(hovered: bool) -> Style {
    Style::default()
        .fg(if hovered {
            Color::White
        } else {
            USER_LABEL_BLUE
        })
        .bg(if hovered {
            MESSAGE_HOVER_BG
        } else {
            COMMAND_PANEL_BG
        })
        .add_modifier(if hovered {
            Modifier::BOLD
        } else {
            Modifier::empty()
        })
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

/// Split one coalesced nested wheel input between an action accordion and the
/// transcript behind it, returning the accordion's new offset and the lines
/// handed to the transcript.
///
/// An input the accordion can act on is consumed there in full, even when it
/// lands on an edge, so reaching the top of an action list never scrolls the
/// background in the same input. Only an input that starts at that edge —
/// including a wheel over an accordion short enough that it cannot scroll —
/// reaches the transcript.
fn nested_scroll_handoff(offset: usize, max_offset: usize, lines: isize) -> (usize, isize) {
    let next = if lines > 0 {
        offset.saturating_add(lines.unsigned_abs()).min(max_offset)
    } else {
        offset.saturating_sub(lines.unsigned_abs())
    };
    (next, if next == offset { lines } else { 0 })
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
        for message in messages.iter() {
            let delivery = json_text(message, &["delivery"]).unwrap_or("queued");
            let sender = json_text(message, &["sender", "from", "actor"]);
            let text = json_text(message, &["text", "message"]).unwrap_or("empty message");
            let prefix = sender.map_or_else(
                || format!("  {delivery:>10}  "),
                |sender| format!("  {delivery:>10}  {sender} · "),
            );
            rows.push(format!("{prefix}{}", text));
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
        for agent in agents.iter() {
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
                rows.push(format!("              {}", task));
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
        for step in steps.iter() {
            let status = json_text(step, &["status"]).unwrap_or("pending");
            let text = json_text(step, &["step", "content", "title", "description"])
                .unwrap_or("unnamed step");
            rows.push(format!("  {status:>10}  {}", text));
        }
    } else if tool.ends_with("get_goal")
        || tool.ends_with("update_goal")
        || value.get("goal").is_some()
    {
        let goal = value.get("goal").unwrap_or(&value);
        let status = json_text(goal, &["status"]).unwrap_or("current");
        let objective = json_text(goal, &["objective", "title"]).unwrap_or("goal");
        rows.push(format!("GOAL · {status} · {}", objective));
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
            rows.push(format!("  {}", text));
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
            rows.push(format!("  {}", message));
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
    // Keep action text at one stable width while the timer changes from
    // tenths to seconds, minutes, hours, or days.
    const ELAPSED_COLUMN_WIDTH: usize = 8;
    let elapsed_width = UnicodeWidthStr::width(elapsed);
    let reserved_width = ELAPSED_COLUMN_WIDTH.saturating_add(2);
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
            .saturating_sub(ELAPSED_COLUMN_WIDTH)
            .saturating_add(ELAPSED_COLUMN_WIDTH.saturating_sub(elapsed_width));
        first.push_str(&" ".repeat(padding));
        first.push_str(elapsed);
    }
    lines
}

#[cfg(test)]
fn format_tool_elapsed(
    started_at: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,
) -> Option<String> {
    format_tool_elapsed_at(started_at, completed_at, Utc::now())
}

fn format_tool_elapsed_at(
    started_at: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Option<String> {
    let elapsed_ms = completed_at
        .unwrap_or(now)
        .signed_duration_since(started_at)
        .num_milliseconds()
        .max(0) as u64;
    if elapsed_ms < 100 {
        return completed_at.is_none().then(|| "0.0s".to_string());
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

fn composer_frame_cursor(
    area: Rect,
    (row, column): (usize, usize),
    scroll: u16,
    is_launch_screen: bool,
) -> Option<Position> {
    let x = area
        .x
        .saturating_add(composer_cursor_x_offset(is_launch_screen));
    let y = area.y.saturating_add(u16::from(!is_launch_screen));
    let content_bottom = area.bottom().saturating_sub(u16::from(!is_launch_screen));
    // A collapsed composer has no text cell between its blank rows.
    if x >= area.right() || y >= content_bottom {
        return None;
    }
    Some(Position {
        x: x.saturating_add(column as u16).min(area.right() - 1),
        y: y.saturating_add((row as u16).saturating_sub(scroll))
            .min(content_bottom - 1),
    })
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

const INTERRUPT_RESEND_AFTER: Duration = Duration::from_millis(500);

fn claim_interrupt(requested: &mut bool, status: SessionStatus) -> bool {
    if *requested || !status_control_is_actionable(status) {
        return false;
    }
    *requested = true;
    true
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
    shell_status_hovered: bool,
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
    } else if state.shell_status_hovered {
        Some("left click to open shells menu")
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

fn billing_status_color(billing: &str) -> Color {
    match billing {
        "api" => Color::LightBlue,
        "endpoint" => Color::Gray,
        _ => Color::LightMagenta,
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

fn terminal_title(cwd: &Path, home: Option<&Path>) -> String {
    let path = match home
        .filter(|home| !home.as_os_str().is_empty())
        .and_then(|home| cwd.strip_prefix(home).ok())
    {
        Some(relative) if relative.as_os_str().is_empty() => "~".to_string(),
        Some(relative) => format!("~/{}", relative.display()),
        None => cwd.display().to_string(),
    };
    let path: String = path.chars().filter(|ch| !ch.is_control()).collect();
    format!("Borg Agent • {path}")
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
    if status == SessionStatus::Ready {
        return "○";
    }
    if !matches!(status, SessionStatus::Starting | SessionStatus::Running) {
        return "●";
    }
    const FRAMES: [&str; 8] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧"];
    FRAMES[spinner_frame_index()]
}

fn replace_tool_activity_glyph(line: &mut Line<'static>, glyph: &str) {
    let Some((span, marker)) = line.spans.iter_mut().find_map(|span| {
        ['◇', '↗']
            .into_iter()
            .find(|marker| span.content.contains(*marker))
            .map(|marker| (span, marker))
    }) else {
        return;
    };
    let start = span
        .content
        .find(marker)
        .expect("glyph-containing span was selected");
    let mut content = span.content.to_string();
    content.replace_range(start..start + marker.len_utf8(), glyph);
    span.content = Cow::Owned(content);
}

const RUNNING_SHIMMER_PADDING: usize = 10;
const RUNNING_SHIMMER_HALF_WIDTH: f32 = 5.0;
const RUNNING_SHIMMER_CYCLE_MILLIS: u128 = 2_000;
/// Tool rows sweep at the same speed but rest one pass between sweeps, so a
/// sweep starts half as often.
const RUNNING_TOOL_SHIMMER_INTERVAL_MILLIS: u128 = RUNNING_SHIMMER_CYCLE_MILLIS * 2;
/// Slow the status sweep by 25% without changing tool-row sweeps.
const RUNNING_STATUS_SHIMMER_PASS_MILLIS: u128 = RUNNING_SHIMMER_CYCLE_MILLIS * 16 / 9;
const RUNNING_STATUS_SHIMMER_HALF_WIDTH: f32 = RUNNING_SHIMMER_HALF_WIDTH * 0.75;
const RUNNING_STATUS_SHIMMER_INTERVAL_MILLIS: u128 = RUNNING_STATUS_SHIMMER_PASS_MILLIS * 4 / 3;
static RUNNING_SHIMMER_START: OnceLock<Instant> = OnceLock::new();

#[derive(Clone, Copy)]
struct ToolActivityAnimation {
    glyph: &'static str,
    shimmer_phase: u128,
}

impl ToolActivityAnimation {
    fn current() -> Self {
        Self {
            glyph: activity_glyph(SessionStatus::Running),
            shimmer_phase: running_shimmer_phase(),
        }
    }

    fn apply_header(self, line: &mut Line<'static>) {
        let content_width = running_activity_content_width(line);
        self.apply(line, true, content_width);
    }

    fn apply(self, line: &mut Line<'static>, replace_glyph: bool, content_width: usize) {
        if replace_glyph {
            replace_tool_activity_glyph(line, self.glyph);
        }
        apply_running_activity_pulse_with_width(line, self.shimmer_phase, content_width);
    }
}

fn tool_activity_animation(enabled: bool, running: bool) -> Option<ToolActivityAnimation> {
    (enabled && running).then(ToolActivityAnimation::current)
}

fn running_shimmer_phase() -> u128 {
    RUNNING_SHIMMER_START
        .get_or_init(Instant::now)
        .elapsed()
        .as_millis()
        % RUNNING_TOOL_SHIMMER_INTERVAL_MILLIS
}

fn running_status_shimmer_phase() -> u128 {
    RUNNING_SHIMMER_START
        .get_or_init(Instant::now)
        .elapsed()
        .as_millis()
        % RUNNING_STATUS_SHIMMER_INTERVAL_MILLIS
}

#[cfg(test)]
fn apply_running_activity_pulse(line: &mut Line<'static>, phase: u128) {
    let content_width = running_activity_content_width(line);
    apply_running_activity_pulse_with_width(line, phase, content_width);
}

fn apply_running_status_shimmer(spans: &mut Vec<Span<'static>>, phase: u128) {
    let cells = spans
        .iter()
        .flat_map(|span| {
            span.content
                .graphemes(true)
                .map(|grapheme| (grapheme.to_string(), span.style))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let width = cells
        .iter()
        .map(|(grapheme, _)| UnicodeWidthStr::width(grapheme.as_str()))
        .sum::<usize>();
    if width == 0 {
        return;
    }

    let period = width.saturating_add(RUNNING_SHIMMER_PADDING * 2);
    // Past one cycle the crest is beyond the text: the rest between passes.
    let center = (phase * period as u128 / RUNNING_STATUS_SHIMMER_PASS_MILLIS) as usize;
    let mut offset = 0usize;
    let mut animated = Vec::with_capacity(cells.len());
    for (grapheme, style) in cells {
        let distance = offset
            .saturating_add(RUNNING_SHIMMER_PADDING)
            .abs_diff(center) as f32;
        let intensity = if distance <= RUNNING_STATUS_SHIMMER_HALF_WIDTH {
            0.5 * (1.0
                + (std::f32::consts::PI * distance / RUNNING_STATUS_SHIMMER_HALF_WIDTH).cos())
        } else {
            0.0
        };
        offset = offset.saturating_add(UnicodeWidthStr::width(grapheme.as_str()));
        animated.push(Span::styled(
            grapheme,
            style
                .fg(sunburst_color(intensity))
                .add_modifier(Modifier::BOLD),
        ));
    }
    spans.clear();
    spans.extend(animated);
}

/// Salmon at rest, sweeping through orange to gold at the crest.
fn sunburst_color(intensity: f32) -> Color {
    const STOPS: [(f32, f32, f32); 3] = [
        (255.0, 132.0, 112.0),
        (255.0, 168.0, 64.0),
        (255.0, 222.0, 120.0),
    ];
    let position = intensity.clamp(0.0, 1.0) * (STOPS.len() - 1) as f32;
    let index = (position as usize).min(STOPS.len() - 2);
    let t = position - index as f32;
    let (from, to) = (STOPS[index], STOPS[index + 1]);
    let mix = |a: f32, b: f32| (a + (b - a) * t).round() as u8;
    Color::Rgb(mix(from.0, to.0), mix(from.1, to.1), mix(from.2, to.2))
}

fn running_activity_content_width(line: &Line<'static>) -> usize {
    line.spans
        .iter()
        .skip(1)
        .map(|span| UnicodeWidthStr::width(span.content.as_ref()))
        .sum()
}

fn apply_running_activity_pulse_with_width(
    line: &mut Line<'static>,
    phase: u128,
    content_width: usize,
) {
    if line.spans.len() < 2 {
        return;
    }
    if content_width == 0 {
        return;
    }

    let period = content_width.saturating_add(RUNNING_SHIMMER_PADDING * 2);
    // Past one pass the crest is beyond the text: the rest between sweeps.
    let shimmer_center = ((phase % RUNNING_TOOL_SHIMMER_INTERVAL_MILLIS) * period as u128
        / RUNNING_SHIMMER_CYCLE_MILLIS) as usize;
    let mut offset = 0usize;
    let mut spans = Vec::with_capacity(line.spans.len() + 2);
    spans.push(line.spans[0].clone());
    for span in line.spans.iter().skip(1) {
        for grapheme in span.content.graphemes(true) {
            let distance = offset
                .saturating_add(RUNNING_SHIMMER_PADDING)
                .abs_diff(shimmer_center) as f32;
            let intensity = if distance <= RUNNING_SHIMMER_HALF_WIDTH {
                0.5 * (1.0 + (std::f32::consts::PI * distance / RUNNING_SHIMMER_HALF_WIDTH).cos())
            } else {
                0.0
            };
            let style = shimmer_style(span.style, intensity);
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

fn shimmer_style(style: Style, intensity: f32) -> Style {
    if intensity < 0.05 {
        return style;
    }
    let color = style.fg.unwrap_or(Color::White);
    let lift = |red: u8, green: u8, blue: u8| {
        let channel =
            |value: u8| (f32::from(value) + (255.0 - f32::from(value)) * intensity * 0.65) as u8;
        Color::Rgb(channel(red), channel(green), channel(blue))
    };
    let color = match color {
        Color::Rgb(red, green, blue) => lift(red, green, blue),
        Color::DarkGray => lift(100, 100, 100),
        Color::Gray => lift(170, 170, 170),
        Color::White => {
            let shade = (255.0 - 95.0 * intensity).round() as u8;
            Color::Rgb(shade, shade, shade)
        }
        other => brighten_color(other, if intensity >= 0.6 { 2 } else { 1 }),
    };
    let style = style.fg(color);
    if intensity >= 0.6 {
        style.add_modifier(Modifier::BOLD)
    } else {
        style
    }
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
        Color::White => Color::White,
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

/// One attachment tile in the rendered transcript: consecutive link rows that
/// share the same image file and column span, minus the trailing label row.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ImagePreviewSlot {
    path: PathBuf,
    first_row: usize,
    rows: usize,
    start: usize,
    width: usize,
}

fn image_preview_slots(links: &[LinkRowRange]) -> Vec<ImagePreviewSlot> {
    let mut slots = Vec::new();
    let mut index = 0;
    while index < links.len() {
        let first = &links[index];
        let mut end = index + 1;
        while end < links.len()
            && links[end].url == first.url
            && links[end].start == first.start
            && links[end].end == first.end
            && links[end].row == links[end - 1].row + 1
        {
            end += 1;
        }
        let rows = end - index;
        if rows >= 2
            && let Ok(url) = url::Url::parse(&first.url)
            && let Ok(path) = url.to_file_path()
            && attachments::is_supported_image(&path)
        {
            slots.push(ImagePreviewSlot {
                path,
                first_row: first.row,
                rows: rows - 1,
                start: first.start,
                width: first.end.saturating_sub(first.start),
            });
        }
        index = end;
    }
    slots
}

/// Cell pixel size for transcript previews, when the terminal reported it.
fn image_preview_cell(picker: Option<&ImagePicker>) -> Option<(u16, u16)> {
    let cell = picker?.font_size();
    (cell.width > 0 && cell.height > 0).then_some((cell.width, cell.height))
}

/// Probe the terminal for a graphics protocol. `BORG_IMAGE_PROTOCOL=halfblocks`
/// (or `off`) skips the probe and keeps glyph previews; `kitty`, `sixel`, or
/// `iterm2` force a protocol for a terminal whose reply the probe missed.
fn detect_image_picker() -> Option<ImagePicker> {
    let requested = std::env::var("BORG_IMAGE_PROTOCOL")
        .ok()
        .map(|value| value.trim().to_ascii_lowercase());
    if matches!(
        requested.as_deref(),
        Some("halfblocks" | "off" | "0" | "none")
    ) {
        return None;
    }
    // The window only elapses when the terminal does not answer, so a longer
    // one costs nothing on a healthy terminal and keeps a busy machine from
    // silently falling back to glyph previews.
    let options = QueryStdioOptions {
        timeout: Duration::from_secs(4),
        ..QueryStdioOptions::default()
    };
    let mut picker = ImagePicker::from_query_stdio_with_options(options).ok()?;
    if let Some(protocol) = requested.as_deref().and_then(|value| match value {
        "kitty" => Some(ProtocolType::Kitty),
        "sixel" => Some(ProtocolType::Sixel),
        "iterm2" | "iterm" => Some(ProtocolType::Iterm2),
        _ => None,
    }) {
        picker.set_protocol_type(protocol);
    }
    // Opaque padding hides stale half-block glyphs beneath terminal graphics.
    if let Color::Rgb(red, green, blue) = MESSAGE_BG {
        picker.set_background_color(Some(image::Rgba([red, green, blue, 255])));
    }
    (picker.protocol_type() != ProtocolType::Halfblocks).then_some(picker)
}
