//! Versioned, replay-safe extension registrations.
//!
//! Extensions describe capabilities; Borg owns execution. A registration can
//! point at a workflow already admitted by the immutable catalog snapshot, but
//! it cannot install an opaque callback into the session actor.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const EXTENSION_API_VERSION: u32 = 1;
const MAX_REGISTRATIONS: usize = 256;
const MAX_NAME_BYTES: usize = 128;
const MAX_DESCRIPTION_BYTES: usize = 4 * 1024;
const MAX_TRANSFORM_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExtensionApiScope {
    Session,
    Project,
    User,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExtensionEffectClass {
    Pure,
    Idempotent,
    AtMostOnce,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExtensionApiTransform {
    pub extension_id: String,
    pub name: String,
    pub scope: ExtensionApiScope,
    pub append_system_prompt: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExtensionApiHook {
    pub extension_id: String,
    pub name: String,
    pub scope: ExtensionApiScope,
    pub event: String,
    pub workflow: String,
    pub effect: ExtensionEffectClass,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExtensionApiTool {
    pub extension_id: String,
    pub name: String,
    pub wire_name: String,
    pub scope: ExtensionApiScope,
    pub workflow: String,
    pub description: String,
    pub input_schema: Value,
    pub effect: ExtensionEffectClass,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExtensionApiCommand {
    pub extension_id: String,
    pub name: String,
    pub scope: ExtensionApiScope,
    pub workflow: String,
    pub description: String,
    pub effect: ExtensionEffectClass,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ExtensionApiSnapshot {
    pub api_version: u32,
    pub catalog_revision: String,
    pub transforms: Vec<ExtensionApiTransform>,
    pub hooks: Vec<ExtensionApiHook>,
    pub tools: Vec<ExtensionApiTool>,
    pub commands: Vec<ExtensionApiCommand>,
}

impl ExtensionApiSnapshot {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.api_version == 0 || self.api_version == EXTENSION_API_VERSION,
            "unsupported extension API version {}; expected {}",
            self.api_version,
            EXTENSION_API_VERSION
        );
        let total =
            self.transforms.len() + self.hooks.len() + self.tools.len() + self.commands.len();
        ensure!(
            total <= MAX_REGISTRATIONS,
            "extension API snapshot exceeds {MAX_REGISTRATIONS} registrations"
        );
        let mut wires = BTreeSet::new();
        for transform in &self.transforms {
            validate_name(&transform.extension_id, "extension id")?;
            validate_name(&transform.name, "transform name")?;
            ensure!(
                transform.append_system_prompt.len() <= MAX_TRANSFORM_BYTES,
                "extension transform {}:{} is too large",
                transform.extension_id,
                transform.name
            );
        }
        for hook in &self.hooks {
            validate_name(&hook.extension_id, "extension id")?;
            validate_name(&hook.name, "hook name")?;
            validate_name(&hook.event, "hook event")?;
            ensure!(
                matches!(hook.event.as_str(), "turn_started" | "turn_completed"),
                "unsupported extension hook event {}",
                hook.event
            );
            validate_name(&hook.workflow, "hook workflow")?;
        }
        for tool in &self.tools {
            validate_name(&tool.extension_id, "extension id")?;
            validate_name(&tool.name, "tool name")?;
            validate_name(&tool.wire_name, "tool wire name")?;
            validate_name(&tool.workflow, "tool workflow")?;
            ensure!(
                tool.description.len() <= MAX_DESCRIPTION_BYTES,
                "extension tool {} is too large",
                tool.wire_name
            );
            ensure!(
                tool.input_schema.is_object(),
                "extension tool {} schema must be an object",
                tool.wire_name
            );
            ensure!(
                wires.insert(tool.wire_name.clone()),
                "duplicate extension tool {}",
                tool.wire_name
            );
        }
        for command in &self.commands {
            validate_name(&command.extension_id, "extension id")?;
            validate_name(&command.name, "command name")?;
            validate_name(&command.workflow, "command workflow")?;
            ensure!(
                command.description.len() <= MAX_DESCRIPTION_BYTES,
                "extension command {}:{} is too large",
                command.extension_id,
                command.name
            );
        }
        Ok(())
    }

    pub fn prompt_appendix(&self) -> String {
        self.transforms
            .iter()
            .filter(|transform| !transform.append_system_prompt.trim().is_empty())
            .map(|transform| {
                format!(
                    "\n\n[Extension transform {}:{}]\n{}",
                    transform.extension_id,
                    transform.name,
                    transform.append_system_prompt.trim()
                )
            })
            .collect()
    }

    pub fn tool_specs(&self) -> Vec<Value> {
        self.tools
            .iter()
            .map(|tool| {
                json!({
                    "name": tool.wire_name,
                    "description": format!(
                        "{} (durable extension workflow {}:{})",
                        tool.description, tool.extension_id, tool.workflow
                    ),
                    "inputSchema": tool.input_schema,
                })
            })
            .collect()
    }

    pub fn tool(&self, wire_name: &str) -> Option<&ExtensionApiTool> {
        self.tools.iter().find(|tool| tool.wire_name == wire_name)
    }

    pub fn command_wire_name(command: &ExtensionApiCommand) -> String {
        format!("extcmd__{}__{}", command.extension_id, command.name)
    }

    pub fn command(&self, wire_name: &str) -> Option<&ExtensionApiCommand> {
        self.commands
            .iter()
            .find(|command| Self::command_wire_name(command) == wire_name)
    }

    pub fn tool_wires(&self) -> Vec<String> {
        self.tools
            .iter()
            .map(|tool| tool.wire_name.clone())
            .collect()
    }

    pub fn command_wires(&self) -> Vec<String> {
        self.commands.iter().map(Self::command_wire_name).collect()
    }

    pub fn command_specs(&self) -> Vec<Value> {
        self.commands
            .iter()
            .map(|command| {
                json!({
                    "name": Self::command_wire_name(command),
                    "description": format!(
                        "{} (durable extension command {}:{})",
                        command.description, command.extension_id, command.name
                    ),
                    "inputSchema": {"type": "object"},
                })
            })
            .collect()
    }
}

fn validate_name(value: &str, label: &str) -> Result<()> {
    ensure!(!value.trim().is_empty(), "{label} is empty");
    ensure!(value.len() <= MAX_NAME_BYTES, "{label} is too long");
    ensure!(
        value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':')
        }),
        "{label} contains unsupported characters"
    );
    Ok(())
}

pub fn effect_class_from_name(value: &str) -> Result<ExtensionEffectClass> {
    match value.trim().to_ascii_lowercase().as_str() {
        "pure" => Ok(ExtensionEffectClass::Pure),
        "idempotent" => Ok(ExtensionEffectClass::Idempotent),
        "at_most_once" | "at-most-once" => Ok(ExtensionEffectClass::AtMostOnce),
        other => bail!("unsupported extension effect class {other}"),
    }
}

pub fn empty_snapshot(catalog_revision: impl Into<String>) -> ExtensionApiSnapshot {
    ExtensionApiSnapshot {
        api_version: EXTENSION_API_VERSION,
        catalog_revision: catalog_revision.into(),
        ..ExtensionApiSnapshot::default()
    }
}

#[derive(Debug, Clone, Default)]
pub struct ExtensionApiRegistry {
    snapshot: ExtensionApiSnapshot,
    tool_names: BTreeMap<String, String>,
}

impl ExtensionApiRegistry {
    pub fn new(catalog_revision: impl Into<String>) -> Self {
        Self {
            snapshot: empty_snapshot(catalog_revision),
            tool_names: BTreeMap::new(),
        }
    }

    pub fn register_tool(&mut self, tool: ExtensionApiTool) -> Result<()> {
        ensure!(
            !self.tool_names.contains_key(&tool.wire_name),
            "extension tool {} is already registered",
            tool.wire_name
        );
        self.tool_names
            .insert(tool.wire_name.clone(), tool.extension_id.clone());
        self.snapshot.tools.push(tool);
        self.snapshot.validate()
    }

    pub fn snapshot(&self) -> ExtensionApiSnapshot {
        self.snapshot.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_rejects_duplicate_tool_wires_and_protects_prompt_transform_size() {
        let mut snapshot = empty_snapshot("catalog-1");
        snapshot.tools = vec![
            ExtensionApiTool {
                extension_id: "one".to_string(),
                name: "search".to_string(),
                wire_name: "ext__one__search".to_string(),
                scope: ExtensionApiScope::Project,
                workflow: "search".to_string(),
                description: "Search".to_string(),
                input_schema: json!({"type": "object"}),
                effect: ExtensionEffectClass::Pure,
            },
            ExtensionApiTool {
                extension_id: "two".to_string(),
                name: "search".to_string(),
                wire_name: "ext__one__search".to_string(),
                scope: ExtensionApiScope::Project,
                workflow: "search".to_string(),
                description: "Search".to_string(),
                input_schema: json!({"type": "object"}),
                effect: ExtensionEffectClass::Pure,
            },
        ];
        assert!(snapshot.validate().is_err());
        snapshot.tools.pop();
        snapshot.transforms.push(ExtensionApiTransform {
            extension_id: "one".to_string(),
            name: "prompt".to_string(),
            scope: ExtensionApiScope::Project,
            append_system_prompt: "x".repeat(MAX_TRANSFORM_BYTES + 1),
        });
        assert!(snapshot.validate().is_err());
    }

    #[test]
    fn prompt_transform_is_replayed_from_the_snapshot() {
        let mut snapshot = empty_snapshot("catalog-1");
        snapshot.transforms.push(ExtensionApiTransform {
            extension_id: "one".to_string(),
            name: "prompt".to_string(),
            scope: ExtensionApiScope::Project,
            append_system_prompt: "Use the project glossary.".to_string(),
        });
        snapshot.validate().unwrap();
        assert_eq!(
            snapshot.prompt_appendix(),
            "\n\n[Extension transform one:prompt]\nUse the project glossary."
        );
    }
}
