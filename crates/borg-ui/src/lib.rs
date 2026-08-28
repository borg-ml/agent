//! Frontend-neutral presentation contracts shared by Borg's TUI and GUI.
//!
//! This crate deliberately contains no terminal, windowing, or GPUI types.

use std::path::PathBuf;

use borg_remote::{
    ApprovalDecision, CodingProvider, GoalAction, PermissionMode, PromptDelivery, ResponseLanguage,
    SessionEvent, SessionGoal, SessionState, SubagentSnapshot,
};
use uuid::Uuid;

pub mod local;
pub mod preferences;
pub mod timeline;

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct KeybindingConfig {
    pub send: Vec<String>,
    pub queue: Vec<String>,
    pub newline: Vec<String>,
    pub keybindings: Vec<String>,
    pub interrupt: Vec<String>,
    pub clear_or_exit: Vec<String>,
    pub exit: Vec<String>,
    pub attach_image: Vec<String>,
    pub dictate: Vec<String>,
    pub copy: Vec<String>,
    pub scroll_up: Vec<String>,
    pub scroll_down: Vec<String>,
    pub select_previous: Vec<String>,
    pub select_next: Vec<String>,
    pub approve: Vec<String>,
    pub deny: Vec<String>,
}

impl Default for KeybindingConfig {
    fn default() -> Self {
        Self {
            send: vec!["enter".into()],
            queue: vec!["tab".into()],
            newline: vec!["shift+enter".into(), "alt+enter".into()],
            keybindings: vec!["?".into()],
            interrupt: vec!["esc".into()],
            clear_or_exit: vec!["ctrl+c".into()],
            exit: vec!["ctrl+d".into()],
            attach_image: vec!["ctrl+v".into()],
            dictate: vec!["alt+v".into()],
            copy: vec!["ctrl+y".into()],
            scroll_up: vec!["pageup".into()],
            scroll_down: vec!["pagedown".into()],
            select_previous: vec!["alt+up".into()],
            select_next: vec!["alt+down".into()],
            approve: vec!["y".into()],
            deny: vec!["n".into(), "esc".into()],
        }
    }
}

impl KeybindingConfig {
    pub fn replace(&mut self, action: &str, bindings: Vec<String>) -> anyhow::Result<()> {
        let target = match action {
            "send" => &mut self.send,
            "queue" => &mut self.queue,
            "newline" => &mut self.newline,
            "keybindings" => &mut self.keybindings,
            "interrupt" => &mut self.interrupt,
            "clear_or_exit" => &mut self.clear_or_exit,
            "exit" => &mut self.exit,
            "attach_image" => &mut self.attach_image,
            "dictate" => &mut self.dictate,
            "copy" => &mut self.copy,
            "scroll_up" => &mut self.scroll_up,
            "scroll_down" => &mut self.scroll_down,
            "select_previous" => &mut self.select_previous,
            "select_next" => &mut self.select_next,
            "approve" => &mut self.approve,
            "deny" => &mut self.deny,
            _ => anyhow::bail!("unknown keybinding action `{action}`"),
        };
        anyhow::ensure!(
            !bindings.is_empty(),
            "keybinding action `{action}` cannot be empty"
        );
        for binding in &bindings {
            validate_key_chord(binding)?;
        }
        *target = bindings;
        Ok(())
    }

    pub fn entries(&self) -> [(&'static str, &[String]); 16] {
        [
            ("send", &self.send),
            ("queue", &self.queue),
            ("newline", &self.newline),
            ("keybindings", &self.keybindings),
            ("interrupt", &self.interrupt),
            ("clear_or_exit", &self.clear_or_exit),
            ("exit", &self.exit),
            ("attach_image", &self.attach_image),
            ("dictate", &self.dictate),
            ("copy", &self.copy),
            ("scroll_up", &self.scroll_up),
            ("scroll_down", &self.scroll_down),
            ("select_previous", &self.select_previous),
            ("select_next", &self.select_next),
            ("approve", &self.approve),
            ("deny", &self.deny),
        ]
    }
}

pub fn validate_key_chord(value: &str) -> anyhow::Result<()> {
    let mut parts = value.split('+').peekable();
    let mut key = None;
    while let Some(part) = parts.next() {
        let part = part.trim().to_ascii_lowercase();
        if parts.peek().is_some() && matches!(part.as_str(), "ctrl" | "alt" | "shift") {
            continue;
        }
        anyhow::ensure!(key.is_none(), "only one non-modifier key is allowed");
        anyhow::ensure!(
            matches!(
                part.as_str(),
                "enter"
                    | "esc"
                    | "tab"
                    | "backspace"
                    | "delete"
                    | "up"
                    | "down"
                    | "left"
                    | "right"
                    | "pageup"
                    | "pagedown"
                    | "home"
                    | "end"
                    | "space"
            ) || part.chars().count() == 1,
            "unsupported key `{part}`"
        );
        key = Some(part);
    }
    anyhow::ensure!(key.is_some(), "key chord must include a key");
    Ok(())
}

#[derive(Clone, Debug)]
pub enum FrontendCommand {
    SubmitPrompt {
        text: String,
        attachments: Vec<PathBuf>,
        delivery: PromptDelivery,
    },
    Interrupt,
    Approve(ApprovalDecision),
    ApplyGoal(GoalAction),
    SetModel {
        provider: CodingProvider,
        model: String,
    },
    SetPermission(PermissionMode),
    SetLanguage(ResponseLanguage),
    FocusAgent(Option<Uuid>),
    LoadOlderHistory,
    Quit,
}

#[derive(Clone, Debug)]
pub struct SessionView {
    pub session_id: Uuid,
    pub state: SessionState,
    pub history: Vec<SessionEvent>,
    pub goal: Option<SessionGoal>,
    pub agents: Vec<SubagentSnapshot>,
    pub cwd: PathBuf,
}

impl SessionView {
    pub fn empty(session_id: Uuid, cwd: PathBuf) -> Self {
        Self {
            session_id,
            state: SessionState::default(),
            history: Vec::new(),
            goal: None,
            agents: Vec::new(),
            cwd,
        }
    }
}

pub mod palette {
    pub const CANVAS: u32 = 0x120f10;
    pub const SURFACE: u32 = 0x181315;
    pub const SURFACE_RAISED: u32 = 0x21191d;
    pub const BORDER: u32 = 0x49363d;
    pub const TEXT: u32 = 0xe8e0df;
    pub const TEXT_MUTED: u32 = 0x95878b;
    pub const ORANGE: u32 = 0xff8e24;
    pub const PEACH: u32 = 0xff8470;
    pub const BLUE: u32 = 0x4aa3ff;
    pub const PINK: u32 = 0xff69b4;
    pub const GREEN: u32 = 0x9fcb67;
    pub const RED: u32 = 0xec6a76;
}

pub fn parse_goal_action(line: &str) -> anyhow::Result<GoalAction> {
    use anyhow::Context as _;

    let value = line
        .strip_prefix("/goal ")
        .context("usage: /goal [OBJECTIVE|pause|resume|clear]")?
        .trim();
    match value {
        "pause" => return Ok(GoalAction::Pause),
        "resume" => return Ok(GoalAction::Resume),
        "clear" => return Ok(GoalAction::Clear),
        "" | "view" => anyhow::bail!("usage: /goal [OBJECTIVE|pause|resume|clear]"),
        _ => {}
    }
    let value = value.strip_prefix("set ").unwrap_or(value).trim();
    let (token_budget, objective) = if let Some(rest) = value.strip_prefix("--tokens ") {
        let (budget, objective) = rest
            .split_once(char::is_whitespace)
            .context("usage: /goal set --tokens NUMBER OBJECTIVE")?;
        let budget = budget
            .parse::<u64>()
            .context("goal token budget must be a positive integer")?;
        anyhow::ensure!(budget > 0, "goal token budget must be positive");
        (Some(budget), objective.trim())
    } else {
        (None, value)
    };
    anyhow::ensure!(!objective.is_empty(), "goal objective must not be empty");
    Ok(GoalAction::Set {
        objective: objective.to_string(),
        token_budget,
    })
}
