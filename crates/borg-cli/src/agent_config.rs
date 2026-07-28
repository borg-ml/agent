use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct AgentConfig {
    pub(crate) commands: CommandConfig,
    pub(crate) keybindings: KeybindingConfig,
    pub(crate) approvals: ApprovalConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct CommandConfig {
    /// User-defined slash-command aliases. The key omits the leading slash;
    /// the value is a built-in slash command and may include fixed arguments.
    pub(crate) aliases: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct KeybindingConfig {
    pub(crate) send: Vec<String>,
    pub(crate) newline: Vec<String>,
    pub(crate) keybindings: Vec<String>,
    pub(crate) interrupt: Vec<String>,
    pub(crate) clear_or_exit: Vec<String>,
    pub(crate) exit: Vec<String>,
    pub(crate) attach_image: Vec<String>,
    pub(crate) copy: Vec<String>,
    pub(crate) scroll_up: Vec<String>,
    pub(crate) scroll_down: Vec<String>,
    pub(crate) select_previous: Vec<String>,
    pub(crate) select_next: Vec<String>,
    pub(crate) approve: Vec<String>,
    pub(crate) deny: Vec<String>,
}

impl Default for KeybindingConfig {
    fn default() -> Self {
        Self {
            send: vec!["enter".into()],
            newline: vec!["shift+enter".into(), "alt+enter".into()],
            keybindings: vec!["?".into()],
            interrupt: vec!["esc".into()],
            clear_or_exit: vec!["ctrl+c".into()],
            exit: vec!["ctrl+d".into()],
            attach_image: vec!["ctrl+v".into()],
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

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct ApprovalConfig {
    /// Optional faster/cheaper model for native Auto command reviews.
    pub(crate) reviewer_model: Option<String>,
    /// Reviewer reasoning effort. Defaults to `low` when a dedicated model is set.
    pub(crate) reviewer_effort: Option<String>,
}

impl AgentConfig {
    pub(crate) fn load(explicit: Option<&Path>) -> Result<Self> {
        let path = explicit.map(Path::to_path_buf).or_else(default_path);
        let Some(path) = path else {
            return Ok(Self::default());
        };
        if !path.exists() && explicit.is_none() {
            return Ok(Self::default());
        }
        let source = fs::read_to_string(&path)
            .with_context(|| format!("failed to read agent config {}", path.display()))?;
        let config: Self = toml::from_str(&source)
            .with_context(|| format!("invalid agent config {}", path.display()))?;
        config
            .validate()
            .with_context(|| format!("invalid agent config {}", path.display()))?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        for (alias, target) in &self.commands.aliases {
            anyhow::ensure!(
                valid_alias(alias),
                "command alias `{alias}` must contain only ASCII letters, digits, `-`, or `_`"
            );
            anyhow::ensure!(
                target.starts_with('/') && target.len() > 1,
                "command alias `{alias}` target must be a slash command"
            );
            anyhow::ensure!(
                !target.starts_with(&format!("/{alias}")),
                "command alias `{alias}` cannot expand to itself"
            );
        }
        for (action, bindings) in self.keybindings.entries() {
            anyhow::ensure!(
                !bindings.is_empty(),
                "keybinding action `{action}` must have at least one key"
            );
            for binding in bindings {
                validate_key_chord(binding)
                    .with_context(|| format!("invalid `{action}` keybinding `{binding}`"))?;
            }
        }
        if let Some(model) = &self.approvals.reviewer_model {
            anyhow::ensure!(!model.trim().is_empty(), "reviewer_model must not be empty");
        }
        if let Some(effort) = &self.approvals.reviewer_effort {
            anyhow::ensure!(
                !effort.trim().is_empty(),
                "reviewer_effort must not be empty"
            );
        }
        Ok(())
    }

    pub(crate) fn expand_command(&self, line: &str) -> String {
        let Some(command) = line.strip_prefix('/') else {
            return line.to_string();
        };
        let split = command.find(char::is_whitespace).unwrap_or(command.len());
        let (name, suffix) = command.split_at(split);
        self.commands
            .aliases
            .get(name)
            .map(|target| format!("{target}{suffix}"))
            .unwrap_or_else(|| line.to_string())
    }
}

impl KeybindingConfig {
    pub(crate) fn entries(&self) -> [(&'static str, &[String]); 14] {
        [
            ("send", &self.send),
            ("newline", &self.newline),
            ("keybindings", &self.keybindings),
            ("interrupt", &self.interrupt),
            ("clear_or_exit", &self.clear_or_exit),
            ("exit", &self.exit),
            ("attach_image", &self.attach_image),
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

fn validate_key_chord(value: &str) -> Result<()> {
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

fn default_path() -> Option<PathBuf> {
    std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
        .map(|root| root.join("borg").join("agent.toml"))
}

fn valid_alias(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aliases_preserve_user_arguments() {
        let config = AgentConfig {
            commands: CommandConfig {
                aliases: BTreeMap::from([("quick".into(), "/fast on".into())]),
            },
            ..AgentConfig::default()
        };
        assert_eq!(config.expand_command("/quick"), "/fast on");
        assert_eq!(config.expand_command("/quick extra"), "/fast on extra");
        assert_eq!(config.expand_command("quick"), "quick");
    }

    #[test]
    fn checked_in_example_matches_the_typed_config() {
        let config: AgentConfig =
            toml::from_str(include_str!("../../../configs/agent.example.toml")).unwrap();
        config.validate().unwrap();
        assert_eq!(config.expand_command("/quick"), "/fast on");
    }

    #[test]
    fn partial_keybinding_tables_keep_unspecified_defaults() {
        let config: AgentConfig = toml::from_str(
            r#"
            [keybindings]
            send = ["ctrl+s"]
            "#,
        )
        .expect("config");
        config.validate().expect("valid config");
        assert_eq!(config.keybindings.send, ["ctrl+s"]);
        assert_eq!(config.keybindings.interrupt, ["esc"]);
    }
}
