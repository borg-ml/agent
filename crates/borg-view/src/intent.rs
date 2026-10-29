//! Pure slash intents. Quit is client-local, never a host shutdown.
use borg_agent_runtime::{
    ApprovalDecision, CodingProvider, GoalAction, PermissionMode, PlanItem, PlanItemStatus,
    PromptDelivery, ResponseLanguage, TodoAction,
};
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub enum FrontendCommand {
    SubmitPrompt {
        text: String,
        attachments: Vec<PathBuf>,
        delivery: PromptDelivery,
    },
    SubmitDirectorPrompt {
        text: String,
        attachments: Vec<PathBuf>,
        delivery: PromptDelivery,
    },
    RecallQueuedPrompt(Option<Uuid>),
    FlushPendingInput,
    Interrupt,
    Approve(ApprovalDecision),
    RespondToProviderInteraction {
        source_session_id: Uuid,
        interaction_id: String,
        expected_request: Option<Uuid>,
        response: borg_agent_runtime::SensitiveInteractionResponse,
    },
    ApplyGoal(GoalAction),
    ApplyTodo(TodoAction),
    ApplyAgentTodo {
        target_session_id: Uuid,
        action: TodoAction,
    },
    RunExtension {
        command: String,
        arguments: serde_json::Value,
    },
    ControlPeer {
        target: PeerTarget,
        intent: PeerIntent,
        attachments: Vec<PathBuf>,
        delivery: PromptDelivery,
    },
    Inspect(FrontendInspection),
    LoginProvider(CodingProvider),
    SetModel {
        provider: CodingProvider,
        model: String,
    },
    SetPermission(PermissionMode),
    SetLanguage(ResponseLanguage),
    SetEffort(String),
    SetFast(bool),
    SetUltrafast(bool),
    ClearContext,
    Compact,
    /// Stop a watch the agent armed (from the watches panel).
    StopWatch(Uuid),
    FocusAgent(Option<Uuid>),
    PromoteAgent(Option<String>),
    NewSession,
    OpenSession(Uuid),
    LoadOlderHistory,
    Quit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrontendInspection {
    Extensions,
    Customization,
    LanguageServers,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerTarget {
    Claude,
    Gpt,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PeerIntent {
    Ensure,
    Clear,
    Rotate {
        model: Option<String>,
        effort: Option<String>,
    },
    Prompt(String),
}

pub fn parse_peer_command(line: &str) -> Option<anyhow::Result<(PeerTarget, PeerIntent)>> {
    let trimmed = line.trim();
    let rest = trimmed.strip_prefix("/peer")?;
    if rest
        .chars()
        .next()
        .is_some_and(|character| !character.is_whitespace())
    {
        return None;
    }
    Some((|| {
        let mut parts = rest.trim().splitn(2, char::is_whitespace);
        let target = match parts
            .next()
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str()
        {
            "claude" => PeerTarget::Claude,
            "gpt" => PeerTarget::Gpt,
            _ => anyhow::bail!("usage: /peer claude|gpt [TEXT|clear|new [MODEL@EFFORT]]"),
        };
        let request = parts.next().unwrap_or_default().trim();
        let mut words = request.split_whitespace();
        let first = words.next().unwrap_or_default();
        let intent = if first.is_empty() {
            PeerIntent::Ensure
        } else if first.eq_ignore_ascii_case("clear") && words.next().is_none() {
            PeerIntent::Clear
        } else if first.eq_ignore_ascii_case("new") || first.eq_ignore_ascii_case("rotate") {
            let profile = words.next();
            anyhow::ensure!(
                words.next().is_none(),
                "usage: /peer claude|gpt new [MODEL@EFFORT]"
            );
            let (model, effort) = match profile {
                Some(profile) => match profile.rsplit_once('@') {
                    Some((model, effort)) => {
                        anyhow::ensure!(
                            !effort.is_empty() && !model.contains('@'),
                            "peer profile must be MODEL@EFFORT or @EFFORT"
                        );
                        (
                            (!model.is_empty()).then(|| model.to_string()),
                            Some(effort.to_string()),
                        )
                    }
                    None => (Some(profile.to_string()), None),
                },
                None => (None, None),
            };
            PeerIntent::Rotate { model, effort }
        } else {
            PeerIntent::Prompt(request.to_string())
        };
        Ok((target, intent))
    })())
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
        return Ok(FrontendCommand::SubmitDirectorPrompt {
            text: text.to_string(),
            attachments: Vec::new(),
            delivery,
        });
    }
    if let Some(target) = text.strip_prefix("/promote")
        && target.chars().next().is_none_or(char::is_whitespace)
    {
        let mut words = target.split_whitespace();
        let target = words.next().map(str::to_string);
        anyhow::ensure!(words.next().is_none(), "usage: /promote [TARGET]");
        return Ok(FrontendCommand::PromoteAgent(target));
    }
    if text == "/main" || text == "/focus" {
        return Ok(FrontendCommand::FocusAgent(None));
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
    if let Some(command) = parse_peer_command(text) {
        let (target, intent) = command?;
        return Ok(FrontendCommand::ControlPeer {
            target,
            intent,
            attachments: Vec::new(),
            delivery,
        });
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
    if text == "/ultrafast" {
        return Ok(FrontendCommand::SetUltrafast(true));
    }
    if let Some(value) = text.strip_prefix("/ultrafast ").map(str::trim) {
        return Ok(FrontendCommand::SetUltrafast(match value {
            "on" | "true" => true,
            "off" | "false" => false,
            _ => anyhow::bail!(
                "usage: /ultrafast [on|off] (Codex/OpenAI premium, access-dependent tier)"
            ),
        }));
    }
    if let Some(value) = text.strip_prefix("/fast ").map(str::trim) {
        if value == "ultrafast" {
            return Ok(FrontendCommand::SetUltrafast(true));
        }
        return Ok(FrontendCommand::SetFast(match value {
            "on" | "true" => true,
            "off" | "false" => false,
            _ => anyhow::bail!("usage: /fast [on|off|ultrafast]"),
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
        ("/codex", "gpt-6.1-sol@xhigh"),
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
        .context("usage: /todo [add|start|done|pending|blocked|review|remove|clear]")?
        .trim();
    if value == "clear" {
        return Ok(TodoAction::Clear);
    }
    let (command, argument) = value.split_once(char::is_whitespace).context(
        "usage: /todo [add TEXT|start ID|done ID|pending ID|blocked ID|review ID|remove ID|clear]",
    )?;
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
        "blocked" => Ok(TodoAction::SetStatus {
            id: resolve_id()?,
            status: PlanItemStatus::Blocked,
        }),
        "review" | "awaiting_review" => Ok(TodoAction::SetStatus {
            id: resolve_id()?,
            status: PlanItemStatus::AwaitingReview,
        }),
        "pending" | "reset" => Ok(TodoAction::SetStatus {
            id: resolve_id()?,
            status: PlanItemStatus::Pending,
        }),
        "remove" | "rm" => Ok(TodoAction::Remove { id: resolve_id()? }),
        _ => anyhow::bail!(
            "usage: /todo [add TEXT|start ID|done ID|pending ID|blocked ID|review ID|remove ID|clear]"
        ),
    }
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

impl PeerTarget {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Claude => "Claude",
            Self::Gpt => "GPT",
        }
    }

    pub const fn task_name(self) -> &'static str {
        match self {
            Self::Claude => "/root/claude",
            Self::Gpt => "/root/gpt",
        }
    }

    pub const fn task_name_argument(self) -> &'static str {
        match self {
            Self::Claude => "claude",
            Self::Gpt => "gpt",
        }
    }

    pub const fn provider(self) -> CodingProvider {
        match self {
            Self::Claude => CodingProvider::Claude,
            Self::Gpt => CodingProvider::Codex,
        }
    }

    pub const fn default_model(self) -> &'static str {
        match self {
            Self::Claude => borg_provider::CLAUDE_MODEL_CATALOG.default_model,
            Self::Gpt => "gpt-6.1-sol",
        }
    }

    pub const fn default_effort(self) -> &'static str {
        match self {
            Self::Claude => "high",
            Self::Gpt => "xhigh",
        }
    }
}

/// An explicitly local effect never enters an authenticated host command dispatcher.
#[derive(Clone, Debug)]
pub enum SubmissionIntent {
    Session(FrontendCommand),
    Client(crate::wire::ClientEffect),
}

pub fn parse_submission_intent(
    text: &str,
    provider: CodingProvider,
    delivery: PromptDelivery,
    todos: &[PlanItem],
) -> anyhow::Result<SubmissionIntent> {
    match parse_submission(text, provider, delivery, todos)? {
        FrontendCommand::Quit => Ok(SubmissionIntent::Client(
            crate::wire::ClientEffect::CloseView,
        )),
        command => Ok(SubmissionIntent::Session(command)),
    }
}

#[cfg(test)]
mod private_reply_tests {
    use super::*;
    #[test]
    fn request_bound_private_reply_debug_does_not_disclose_the_answer() {
        let command = FrontendCommand::RespondToProviderInteraction {
            source_session_id: Uuid::new_v4(),
            interaction_id: "request-1".into(),
            expected_request: Some(Uuid::new_v4()),
            response: borg_agent_runtime::SensitiveInteractionResponse::new(
                serde_json::json!({"key":"private-test-value"}),
            ),
        };
        assert!(!format!("{command:?}").contains("private-test-value"));
    }
}
