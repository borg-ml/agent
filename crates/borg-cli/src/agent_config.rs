use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct AgentConfig {
    pub(crate) commands: CommandConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct CommandConfig {
    /// User-defined slash-command aliases. The key omits the leading slash;
    /// the value is a built-in slash command and may include fixed arguments.
    pub(crate) aliases: BTreeMap<String, String>,
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
}
