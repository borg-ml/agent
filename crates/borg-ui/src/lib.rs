//! Frontend-neutral presentation contracts shared by Borg's TUI and GUI.
//!
//! This crate deliberately contains no terminal, windowing, or GPUI types.

use std::path::PathBuf;

pub use borg_remote::{
    ApprovalDecision, CodingProvider, GoalAction, PermissionMode, PlanItem, PlanItemStatus,
    PromptDelivery, ResponseLanguage, SessionEvent, SessionGoal, SessionState, SessionStatus,
    SubagentSnapshot, TodoAction,
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
    RecallQueuedPrompt(Option<Uuid>),
    FlushPendingInput,
    Interrupt,
    Approve(ApprovalDecision),
    RespondToProviderInteraction(serde_json::Value),
    ApplyGoal(GoalAction),
    ApplyTodo(TodoAction),
    RunExtension {
        command: String,
        arguments: serde_json::Value,
    },
    SetModel {
        provider: CodingProvider,
        model: String,
    },
    SetPermission(PermissionMode),
    SetLanguage(ResponseLanguage),
    SetEffort(String),
    SetFast(bool),
    ClearContext,
    Compact,
    FocusAgent(Option<Uuid>),
    NewSession,
    OpenSession(Uuid),
    LoadOlderHistory,
    Quit,
}

pub fn parse_submission(
    text: &str,
    provider: CodingProvider,
    delivery: PromptDelivery,
    todos: &[PlanItem],
) -> anyhow::Result<FrontendCommand> {
    let normalized = normalize_consultation_command(text);
    let text = normalized.trim();
    if let Some(text) = text.strip_prefix("/queue ").map(str::trim) {
        anyhow::ensure!(!text.is_empty(), "usage: /queue TEXT");
        return Ok(FrontendCommand::SubmitPrompt {
            text: text.to_string(),
            attachments: Vec::new(),
            delivery: PromptDelivery::Queue,
        });
    }
    if let Some(text) = text.strip_prefix("/steer ").map(str::trim) {
        anyhow::ensure!(!text.is_empty(), "usage: /steer TEXT");
        return Ok(FrontendCommand::SubmitPrompt {
            text: text.to_string(),
            attachments: Vec::new(),
            delivery: PromptDelivery::Steer,
        });
    }
    if let Some(text) = text.strip_prefix("/director ").map(str::trim) {
        anyhow::ensure!(!text.is_empty(), "usage: /director TEXT");
        return Ok(FrontendCommand::SubmitPrompt {
            text: text.to_string(),
            attachments: Vec::new(),
            delivery,
        });
    }
    if text == "/interrupt" || text == "/stop" {
        return Ok(FrontendCommand::Interrupt);
    }
    if text == "/compact" {
        return Ok(FrontendCommand::Compact);
    }
    if text == "/clear" {
        return Ok(FrontendCommand::ClearContext);
    }
    if text == "/flush" {
        return Ok(FrontendCommand::FlushPendingInput);
    }
    if text == "/recall" {
        return Ok(FrontendCommand::RecallQueuedPrompt(None));
    }
    if text == "/quit" || text == "/exit" {
        return Ok(FrontendCommand::Quit);
    }
    if text.starts_with("/goal ") {
        return Ok(FrontendCommand::ApplyGoal(parse_goal_action(text)?));
    }
    if text.starts_with("/todo ") || text.starts_with("/todos ") {
        return Ok(FrontendCommand::ApplyTodo(parse_todo_action(text, todos)?));
    }
    if let Some(rest) = text.strip_prefix("/ext:") {
        let (qualified, argument_text) = rest
            .split_once(char::is_whitespace)
            .map_or((rest, ""), |(qualified, arguments)| {
                (qualified, arguments.trim())
            });
        let (extension_id, name) = qualified
            .split_once(':')
            .ok_or_else(|| anyhow::anyhow!("usage: /ext:EXTENSION:COMMAND [JSON|TEXT]"))?;
        anyhow::ensure!(
            !extension_id.is_empty() && !name.is_empty(),
            "usage: /ext:EXTENSION:COMMAND [JSON|TEXT]"
        );
        let arguments = if argument_text.is_empty() {
            serde_json::json!({})
        } else if argument_text.starts_with('{') {
            let value: serde_json::Value = serde_json::from_str(argument_text)
                .map_err(|error| anyhow::anyhow!("invalid extension command JSON: {error}"))?;
            anyhow::ensure!(
                value.is_object(),
                "extension arguments must be a JSON object"
            );
            value
        } else {
            serde_json::json!({ "arguments": argument_text })
        };
        return Ok(FrontendCommand::RunExtension {
            command: format!("extcmd__{extension_id}__{name}"),
            arguments,
        });
    }
    if let Some(model) = text.strip_prefix("/model ").map(str::trim) {
        anyhow::ensure!(!model.is_empty(), "usage: /model MODEL");
        return Ok(FrontendCommand::SetModel {
            provider: CodingProvider::for_model(model).unwrap_or(provider),
            model: model.to_string(),
        });
    }
    if let Some(mode) = text.strip_prefix("/permission ").map(str::trim) {
        let mode = match mode {
            "full" | "full-access" => PermissionMode::FullAccess,
            "auto" => PermissionMode::Auto,
            "manual" => PermissionMode::Manual,
            _ => anyhow::bail!("usage: /permission [full|auto|manual]"),
        };
        return Ok(FrontendCommand::SetPermission(mode));
    }
    if let Some(effort) = text.strip_prefix("/effort ").map(str::trim) {
        anyhow::ensure!(!effort.is_empty(), "usage: /effort LEVEL");
        return Ok(FrontendCommand::SetEffort(effort.to_string()));
    }
    if let Some(value) = text.strip_prefix("/fast ").map(str::trim) {
        return Ok(FrontendCommand::SetFast(match value {
            "on" | "true" => true,
            "off" | "false" => false,
            _ => anyhow::bail!("usage: /fast [on|off]"),
        }));
    }
    if let Some(language) = text.strip_prefix("/language ").map(str::trim) {
        let language = ResponseLanguage::parse(language)
            .ok_or_else(|| anyhow::anyhow!("unknown response language `{language}`"))?;
        return Ok(FrontendCommand::SetLanguage(language));
    }
    anyhow::ensure!(
        !text.starts_with('/') || text.starts_with("/ask "),
        "unknown command `{text}`"
    );
    Ok(FrontendCommand::SubmitPrompt {
        text: text.to_string(),
        attachments: Vec::new(),
        delivery,
    })
}

pub fn normalize_consultation_command(line: &str) -> String {
    let trimmed = line.trim();
    for (alias, profile) in [
        ("/claude", "claude"),
        ("/gpt", "gpt"),
        ("/codex", "gpt-5.6-sol@xhigh"),
    ] {
        if trimmed == alias {
            return format!("/ask {profile}");
        }
        if let Some(request) = trimmed.strip_prefix(alias).filter(|rest| {
            rest.chars()
                .next()
                .is_some_and(|character| character.is_whitespace())
        }) {
            return format!("/ask {profile}{request}");
        }
    }
    line.to_string()
}

pub fn parse_todo_action(line: &str, items: &[PlanItem]) -> anyhow::Result<TodoAction> {
    use anyhow::Context as _;

    let value = line
        .strip_prefix("/todo ")
        .or_else(|| line.strip_prefix("/todos "))
        .context("usage: /todo [add|start|done|pending|remove|clear]")?
        .trim();
    if value == "clear" {
        return Ok(TodoAction::Clear);
    }
    let (command, argument) = value
        .split_once(char::is_whitespace)
        .context("usage: /todo [add TEXT|start ID|done ID|pending ID|remove ID|clear]")?;
    let argument = argument.trim();
    anyhow::ensure!(!argument.is_empty(), "todo command requires a value");
    let resolve_id = || {
        let normalized = argument.to_ascii_lowercase();
        let matches = items
            .iter()
            .filter(|item| item.id.to_string().starts_with(&normalized))
            .map(|item| item.id)
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [id] => Ok(*id),
            [] => anyhow::bail!("no todo item matches ID {argument}"),
            _ => anyhow::bail!("todo ID prefix {argument} is ambiguous"),
        }
    };
    match command {
        "add" => Ok(TodoAction::Add {
            content: argument.to_string(),
        }),
        "start" => Ok(TodoAction::SetStatus {
            id: resolve_id()?,
            status: PlanItemStatus::InProgress,
        }),
        "done" | "complete" => Ok(TodoAction::SetStatus {
            id: resolve_id()?,
            status: PlanItemStatus::Completed,
        }),
        "pending" | "reset" => Ok(TodoAction::SetStatus {
            id: resolve_id()?,
            status: PlanItemStatus::Pending,
        }),
        "remove" | "rm" => Ok(TodoAction::Remove { id: resolve_id()? }),
        _ => anyhow::bail!("usage: /todo [add TEXT|start ID|done ID|pending ID|remove ID|clear]"),
    }
}

pub fn cancelled_provider_interaction_response(kind: &str) -> serde_json::Value {
    if kind == "mcp_elicitation" {
        serde_json::json!({ "action": "cancel" })
    } else {
        serde_json::json!({ "answers": {} })
    }
}

pub fn provider_interaction_contains_secret(payload: &serde_json::Value) -> bool {
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

pub fn provider_interaction_response(
    kind: &str,
    payload: &serde_json::Value,
    input: &str,
) -> anyhow::Result<serde_json::Value> {
    use anyhow::Context as _;

    if input.eq_ignore_ascii_case("/cancel") {
        return Ok(cancelled_provider_interaction_response(kind));
    }
    if kind == "mcp_elicitation" {
        if input.eq_ignore_ascii_case("/decline") {
            return Ok(serde_json::json!({ "action": "decline" }));
        }
        let content = match serde_json::from_str::<serde_json::Value>(input) {
            Ok(value) => value,
            Err(_) => {
                let properties = payload
                    .get("requestedSchema")
                    .and_then(|schema| schema.get("properties"))
                    .and_then(serde_json::Value::as_object)
                    .context(
                        "Enter JSON matching the requested form, or use /decline or /cancel",
                    )?;
                anyhow::ensure!(
                    properties.len() == 1,
                    "Enter a JSON object matching the requested form, or use /decline or /cancel"
                );
                let key = properties.keys().next().expect("one property");
                serde_json::json!({ key: input })
            }
        };
        return Ok(serde_json::json!({ "action": "accept", "content": content }));
    }
    let questions = payload
        .get("questions")
        .and_then(serde_json::Value::as_array)
        .context("Provider user-input request did not include questions")?;
    if questions.len() == 1 {
        let id = questions[0]
            .get("id")
            .and_then(serde_json::Value::as_str)
            .context("Provider user-input question did not include an id")?;
        return Ok(serde_json::json!({ "answers": { (id): { "answers": [input] } } }));
    }
    let value: serde_json::Value = serde_json::from_str(input).context(
        "Answer multiple questions with a JSON object keyed by question id, or use /cancel",
    )?;
    if value.get("answers").is_some() {
        return Ok(value);
    }
    let values = value
        .as_object()
        .context("Multiple answers must be a JSON object keyed by question id")?;
    let mut answers = serde_json::Map::new();
    for question in questions {
        let id = question
            .get("id")
            .and_then(serde_json::Value::as_str)
            .context("Provider user-input question did not include an id")?;
        let answer = values
            .get(id)
            .with_context(|| format!("Missing answer for question {id}"))?;
        let answer_values = match answer {
            serde_json::Value::Array(values) => values.clone(),
            value => vec![value.clone()],
        };
        anyhow::ensure!(
            answer_values.iter().all(serde_json::Value::is_string),
            "Answer for {id} must be a string or an array of strings"
        );
        answers.insert(
            id.to_string(),
            serde_json::json!({ "answers": answer_values }),
        );
    }
    Ok(serde_json::json!({ "answers": answers }))
}

#[derive(Clone, Debug)]
pub struct SessionView {
    pub session_id: Uuid,
    pub state: SessionState,
    pub history: std::sync::Arc<Vec<SessionEvent>>,
    pub goal: Option<SessionGoal>,
    pub agents: Vec<SubagentSnapshot>,
    pub cwd: PathBuf,
}

#[derive(Clone, Debug)]
pub struct SessionPresentation {
    pub view: SessionView,
    pub root_session_id: Uuid,
    pub timeline: std::sync::Arc<Vec<std::sync::Arc<timeline::TimelineEntry>>>,
}

impl SessionPresentation {
    pub fn new(view: SessionView) -> Self {
        let timeline = std::sync::Arc::new(
            timeline::TimelineProjector::from_events(&view.history).into_shared_entries(),
        );
        let root_session_id = view.session_id;
        Self {
            view,
            root_session_id,
            timeline,
        }
    }
}

impl SessionView {
    pub fn empty(session_id: Uuid, cwd: PathBuf) -> Self {
        Self {
            session_id,
            state: SessionState::default(),
            history: std::sync::Arc::new(Vec::new()),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submission_parser_keeps_commands_out_of_prompts() {
        assert!(matches!(
            parse_submission(
                "/permission manual",
                CodingProvider::Codex,
                PromptDelivery::Steer,
                &[]
            ),
            Ok(FrontendCommand::SetPermission(PermissionMode::Manual))
        ));
        assert!(
            parse_submission(
                "/unknown",
                CodingProvider::Codex,
                PromptDelivery::Steer,
                &[]
            )
            .is_err()
        );
        assert!(matches!(
            parse_submission("ship it", CodingProvider::Codex, PromptDelivery::Queue, &[]),
            Ok(FrontendCommand::SubmitPrompt {
                delivery: PromptDelivery::Queue,
                ..
            })
        ));
        assert!(matches!(
            parse_submission(
                "/claude review this",
                CodingProvider::Codex,
                PromptDelivery::Steer,
                &[]
            ),
            Ok(FrontendCommand::SubmitPrompt { text, .. }) if text == "/ask claude review this"
        ));
    }
}
