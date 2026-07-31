use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;
use serde_json::{Map, Value, json};

const MAX_PLUGIN_TEXT: usize = 512 * 1024;
const SETTINGS_SECTIONS: &[&str] = &[
    "capabilities",
    "extensions",
    "team",
    "commands",
    "keybindings",
    "mcp",
    "approvals",
    "updates",
];

#[derive(Debug, Clone)]
pub(crate) struct SelfServiceContext {
    cwd: PathBuf,
}

impl SelfServiceContext {
    pub(crate) fn new(cwd: PathBuf) -> Self {
        Self { cwd }
    }

    pub(crate) fn call(&self, name: &str, arguments: Value) -> Result<Value> {
        match name {
            "list_plugins" => {
                let _: NoArgs = serde_json::from_value(arguments)?;
                self.list_plugins()
            }
            "read_plugin" => {
                let args: PluginIdArgs = serde_json::from_value(arguments)?;
                self.read_plugin(&args.id)
            }
            "get_agent_settings" => {
                let args: SettingsScopeArgs = serde_json::from_value(arguments)?;
                self.get_settings(args.scope.as_deref().unwrap_or("user"))
            }
            "update_agent_settings" => {
                let args: UpdateSettingsArgs = serde_json::from_value(arguments)?;
                self.update_settings(args.scope.as_deref().unwrap_or("user"), args.updates)
            }
            "create_plugin" => {
                let args: CreatePluginArgs = serde_json::from_value(arguments)?;
                self.create_plugin(args)
            }
            other => bail!("unknown self-service tool: {other}"),
        }
    }

    fn get_settings(&self, scope: &str) -> Result<Value> {
        let path = self.settings_path(scope)?;
        let exists = path.is_file();
        let settings = if exists {
            let source = fs::read_to_string(&path)
                .with_context(|| format!("failed to read agent settings {}", path.display()))?;
            let parsed: toml::Value = toml::from_str(&source)
                .with_context(|| format!("invalid agent settings {}", path.display()))?;
            redact_toml(parsed)
        } else {
            toml::Value::Table(toml::map::Map::new())
        };
        Ok(json!({
            "scope": scope,
            "path": path,
            "exists": exists,
            "settings": serde_json::to_value(settings)?,
            "hot_reload": ["commands.aliases", "keybindings"],
            "restart_required": [
                "capabilities", "extensions", "mcp", "team", "approvals", "updates"
            ]
        }))
    }

    fn list_plugins(&self) -> Result<Value> {
        let root = self.cwd.join(".borg").join("skills");
        let mut plugins = Vec::new();
        if root.is_dir() {
            let mut entries = fs::read_dir(&root)?.collect::<std::io::Result<Vec<_>>>()?;
            entries.sort_by_key(|entry| entry.file_name());
            for entry in entries.into_iter().take(128) {
                let id = entry.file_name().to_string_lossy().to_string();
                if validate_plugin_id(&id).is_err() {
                    continue;
                }
                let path = entry.path().join("SKILL.md");
                if !path.is_file() {
                    continue;
                }
                let size_bytes = fs::metadata(&path)?.len();
                if size_bytes > MAX_PLUGIN_TEXT as u64 {
                    plugins.push(json!({
                        "id": id,
                        "path": path,
                        "size_bytes": size_bytes,
                        "description": "Plugin is too large to inspect through the Borg tool"
                    }));
                    continue;
                }
                let content = fs::read_to_string(&path)?;
                plugins.push(json!({
                    "id": id,
                    "path": path,
                    "size_bytes": size_bytes,
                    "description": plugin_description(&content),
                }));
            }
        }
        Ok(json!({ "root": root, "plugins": plugins }))
    }

    fn read_plugin(&self, id: &str) -> Result<Value> {
        validate_plugin_id(id)?;
        let root = self.cwd.join(".borg").join("skills");
        let path = root.join(id).join("SKILL.md");
        let canonical_root = root.canonicalize().unwrap_or(root);
        let canonical = path
            .canonicalize()
            .with_context(|| format!("plugin `{id}` does not exist"))?;
        ensure!(
            canonical.starts_with(&canonical_root),
            "plugin path escapes the project skill root"
        );
        let metadata = fs::metadata(&canonical)?;
        ensure!(
            metadata.len() <= MAX_PLUGIN_TEXT as u64,
            "plugin `{id}` is too large"
        );
        Ok(json!({
            "id": id,
            "path": canonical,
            "content": fs::read_to_string(&canonical)?
        }))
    }

    fn update_settings(&self, scope: &str, updates: Map<String, Value>) -> Result<Value> {
        let path = self.settings_path(scope)?;
        ensure!(
            !updates.is_empty(),
            "updates must contain at least one setting section"
        );
        for key in updates.keys() {
            ensure!(
                SETTINGS_SECTIONS.contains(&key.as_str()),
                "unsupported settings section `{key}`; allowed sections: {}",
                SETTINGS_SECTIONS.join(", ")
            );
        }
        let updated_sections = updates.keys().cloned().collect::<Vec<_>>();
        let mut root = if path.is_file() {
            let source = fs::read_to_string(&path)
                .with_context(|| format!("failed to read agent settings {}", path.display()))?;
            toml::from_str::<toml::Value>(&source)
                .with_context(|| format!("invalid agent settings {}", path.display()))?
        } else {
            toml::Value::Table(toml::map::Map::new())
        };
        let table = root
            .as_table_mut()
            .context("agent settings root must be a TOML table")?;
        for (key, patch) in updates {
            if patch.is_null() {
                table.remove(&key);
                continue;
            }
            let patch = json_to_toml(patch)
                .with_context(|| format!("settings section `{key}` is not TOML-compatible"))?;
            merge_toml(
                table
                    .entry(key)
                    .or_insert_with(|| toml::Value::Table(toml::map::Map::new())),
                patch,
            );
        }
        validate_settings_shape(&root)?;
        let rendered = toml::to_string_pretty(&root).context("serialize agent settings")?;
        write_atomic(&path, rendered.as_bytes())?;
        Ok(json!({
            "scope": scope,
            "path": path,
            "updated_sections": updated_sections,
            "hot_reloaded": ["commands.aliases", "keybindings"],
            "restart_required": [
                "capabilities", "extensions", "mcp", "team", "approvals", "updates"
            ],
            "note": "Aliases and keybindings are reloaded by the running TUI; provider/MCP and capability changes take effect on the next session."
        }))
    }

    fn create_plugin(&self, args: CreatePluginArgs) -> Result<Value> {
        validate_plugin_id(&args.id)?;
        ensure!(
            args.description.len() <= MAX_PLUGIN_TEXT,
            "plugin description is too large"
        );
        ensure!(
            args.instructions.len() <= MAX_PLUGIN_TEXT,
            "plugin instructions are too large"
        );
        ensure!(
            !args.description.contains('\0'),
            "plugin description contains NUL"
        );
        ensure!(
            !args.instructions.contains('\0'),
            "plugin instructions contain NUL"
        );
        let skill_dir = self.cwd.join(".borg").join("skills").join(&args.id);
        let skill_path = skill_dir.join("SKILL.md");
        if skill_path.exists() && !args.overwrite {
            bail!(
                "plugin `{}` already exists at {}; pass overwrite=true to replace it",
                args.id,
                skill_path.display()
            );
        }
        fs::create_dir_all(&skill_dir)
            .with_context(|| format!("create plugin directory {}", skill_dir.display()))?;
        let content = format!(
            "---\nname: {}\ndescription: {}\n---\n\n# {}\n\n{}\n",
            args.id,
            yaml_scalar(&args.description),
            args.id,
            args.instructions.trim()
        );
        write_atomic(&skill_path, content.as_bytes())?;
        Ok(json!({
            "id": args.id,
            "path": skill_path,
            "hot_reload": "next native turn",
            "restart_required": false,
            "note": "Project skills are rescanned at the start of every native turn. MCP servers belong in an extension manifest and require a new session."
        }))
    }

    fn settings_path(&self, scope: &str) -> Result<PathBuf> {
        match scope {
            "user" => default_settings_path()
                .context("cannot locate the user agent config; set HOME or XDG_CONFIG_HOME"),
            other => bail!("unknown settings scope `{other}`; use `user`"),
        }
    }
}

#[derive(Debug, Deserialize)]
struct SettingsScopeArgs {
    #[serde(default)]
    scope: Option<String>,
}

#[derive(Debug, Deserialize)]
struct NoArgs {}

#[derive(Debug, Deserialize)]
struct PluginIdArgs {
    id: String,
}

#[derive(Debug, Deserialize)]
struct UpdateSettingsArgs {
    #[serde(default)]
    scope: Option<String>,
    updates: Map<String, Value>,
}

#[derive(Debug, Deserialize)]
struct CreatePluginArgs {
    id: String,
    description: String,
    instructions: String,
    #[serde(default)]
    overwrite: bool,
}

pub(crate) fn is_tool(name: &str) -> bool {
    matches!(
        name,
        "list_plugins"
            | "read_plugin"
            | "get_agent_settings"
            | "update_agent_settings"
            | "create_plugin"
    )
}

pub(crate) fn tool_specs() -> Vec<Value> {
    vec![
        json!({
            "name": "list_plugins",
            "description": "List project-local Borg plugins in .borg/skills. This is live filesystem state and includes plugins created during the current session.",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }
        }),
        json!({
            "name": "read_plugin",
            "description": "Read one project-local Borg plugin skill from .borg/skills/<id>/SKILL.md. Use this after list_plugins when a plugin's workflow applies.",
            "inputSchema": {
                "type": "object",
                "properties": { "id": { "type": "string", "pattern": "^[A-Za-z0-9_-]+$", "maxLength": 64 } },
                "required": ["id"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "get_agent_settings",
            "description": "Read the effective Borg agent settings with sensitive environment values redacted. Borg's typed agent config is user-scoped and shared across sessions.",
            "inputSchema": {
                "type": "object",
                "properties": { "scope": { "type": "string", "enum": ["user"] } },
                "additionalProperties": false
            }
        }),
        json!({
            "name": "update_agent_settings",
            "description": "Safely merge typed Borg settings into the user agent.toml. Only known top-level sections are accepted; aliases and keybindings hot-reload in the running TUI, while provider/MCP/capability changes require a new session.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "scope": { "type": "string", "enum": ["user"] },
                    "updates": { "type": "object", "description": "Top-level Borg config sections to merge, such as {commands:{aliases:{review:\"/effort high\"}}}." }
                },
                "required": ["updates"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "create_plugin",
            "description": "Create a project-local Borg plugin skill at .borg/skills/<id>/SKILL.md. It is picked up automatically on the next native turn, so no restart is needed for skill-only plugins.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id": { "type": "string", "pattern": "^[A-Za-z0-9_-]+$", "maxLength": 64 },
                    "description": { "type": "string", "minLength": 1, "maxLength": 512 },
                    "instructions": { "type": "string", "minLength": 1, "maxLength": 524288 },
                    "overwrite": { "type": "boolean" }
                },
                "required": ["id", "description", "instructions"],
                "additionalProperties": false
            }
        }),
    ]
}

fn default_settings_path() -> Option<PathBuf> {
    std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
        .map(|root| root.join("borg").join("agent.toml"))
}

fn validate_plugin_id(id: &str) -> Result<()> {
    ensure!(
        !id.is_empty() && id.len() <= 64,
        "plugin id must be 1-64 characters"
    );
    ensure!(
        id.bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')),
        "plugin id may contain only ASCII letters, digits, `_`, or `-`"
    );
    Ok(())
}

fn yaml_scalar(value: &str) -> String {
    let escaped = value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\r', " ")
        .replace('\n', " ");
    format!("\"{escaped}\"")
}

fn plugin_description(content: &str) -> String {
    if content.starts_with("---")
        && let Some(description) = content
            .lines()
            .skip(1)
            .take_while(|line| *line != "---")
            .find_map(|line| line.strip_prefix("description:").map(str::trim))
    {
        return description.trim_matches('"').to_string();
    }
    "Reusable Borg workflow".to_string()
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().context("settings path has no parent")?;
    fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)
        .with_context(|| format!("create temporary file beside {}", path.display()))?;
    temp.as_file_mut().write_all(bytes)?;
    temp.as_file_mut().sync_all()?;
    temp.persist(path)
        .map_err(|error| error.error)
        .with_context(|| format!("atomically replace {}", path.display()))?;
    #[cfg(unix)]
    {
        let _ = OpenOptions::new().read(true).open(parent)?.sync_all();
    }
    Ok(())
}

fn json_to_toml(value: Value) -> Result<toml::Value> {
    match value {
        Value::Null => bail!("null is only supported for removing a top-level section"),
        Value::Bool(value) => Ok(toml::Value::Boolean(value)),
        Value::Number(value) => value
            .as_i64()
            .map(toml::Value::Integer)
            .or_else(|| value.as_f64().map(toml::Value::Float))
            .context("number is not representable in TOML"),
        Value::String(value) => Ok(toml::Value::String(value)),
        Value::Array(values) => values
            .into_iter()
            .map(json_to_toml)
            .collect::<Result<Vec<_>>>()
            .map(toml::Value::Array),
        Value::Object(values) => values
            .into_iter()
            .map(|(key, value)| Ok((key, json_to_toml(value)?)))
            .collect::<Result<toml::map::Map<_, _>>>()
            .map(toml::Value::Table),
    }
}

fn merge_toml(existing: &mut toml::Value, patch: toml::Value) {
    if let (Some(existing), Some(patch)) = (existing.as_table_mut(), patch.as_table()) {
        for (key, value) in patch {
            if let Some(current) = existing.get_mut(key) {
                merge_toml(current, value.clone());
            } else {
                existing.insert(key.clone(), value.clone());
            }
        }
    } else {
        *existing = patch;
    }
}

fn validate_settings_shape(root: &toml::Value) -> Result<()> {
    let Some(root) = root.as_table() else {
        bail!("agent settings root must be a TOML table");
    };
    for (section, value) in root {
        ensure!(
            SETTINGS_SECTIONS.contains(&section.as_str()),
            "unsupported settings section `{section}`"
        );
        ensure!(
            value.is_table(),
            "settings section `{section}` must be a table"
        );
    }
    if let Some(capabilities) = root.get("capabilities") {
        check_keys(
            capabilities,
            &[
                "multiplayer",
                "subagents",
                "autonomous_team",
                "shared_work",
                "presence",
                "cloud_sync",
                "web_relay",
                "telemetry",
            ],
            "capabilities",
        )?;
        check_bool_values(capabilities, "capabilities")?;
    }
    if let Some(extensions) = root.get("extensions") {
        check_keys(extensions, &["allow_project_mcp"], "extensions")?;
        check_bool_values(extensions, "extensions")?;
    }
    if let Some(team) = root.get("team") {
        check_keys(
            team,
            &[
                "preset",
                "worker_concurrency",
                "max_total_assignments",
                "max_total_reports",
                "max_total_escalations",
                "max_specialists",
                "max_tokens",
                "max_cost_microusd",
                "max_wall_time_ms",
            ],
            "team",
        )?;
        if let Some(preset) = team.get("preset") {
            ensure!(
                preset.as_str() == Some("xhigh_director_low_workers"),
                "team.preset must be `xhigh_director_low_workers`"
            );
        }
        if let Some(worker_concurrency) = team.get("worker_concurrency") {
            check_positive_integer(worker_concurrency, "team.worker_concurrency")?;
        }
        for name in [
            "max_total_assignments",
            "max_total_reports",
            "max_total_escalations",
            "max_specialists",
            "max_tokens",
            "max_cost_microusd",
            "max_wall_time_ms",
        ] {
            if let Some(value) = team.get(name) {
                check_positive_integer(value, &format!("team.{name}"))?;
            }
        }
    }
    if let Some(commands) = root.get("commands") {
        check_keys(commands, &["aliases"], "commands")?;
        if let Some(aliases) = commands.get("aliases") {
            let Some(aliases) = aliases.as_table() else {
                bail!("commands.aliases must be a table");
            };
            for (name, target) in aliases {
                ensure!(
                    !name.is_empty()
                        && name
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')),
                    "invalid command alias `{name}`"
                );
                ensure!(
                    target
                        .as_str()
                        .is_some_and(|value| value.starts_with('/') && value.len() > 1),
                    "command alias `{name}` must target a slash command"
                );
            }
        }
    }
    if let Some(keybindings) = root.get("keybindings") {
        check_keys(
            keybindings,
            &[
                "send",
                "queue",
                "newline",
                "keybindings",
                "interrupt",
                "clear_or_exit",
                "exit",
                "attach_image",
                "copy",
                "scroll_up",
                "scroll_down",
                "select_previous",
                "select_next",
                "approve",
                "deny",
            ],
            "keybindings",
        )?;
        for (name, values) in keybindings.as_table().expect("checked table") {
            let Some(values) = values.as_array() else {
                bail!("keybindings.{name} must be an array");
            };
            ensure!(!values.is_empty(), "keybindings.{name} must not be empty");
            ensure!(
                values
                    .iter()
                    .all(|value| value.as_str().is_some_and(|value| !value.trim().is_empty())),
                "keybindings.{name} must contain strings"
            );
        }
    }
    if let Some(mcp) = root.get("mcp") {
        check_keys(mcp, &["servers"], "mcp")?;
        if let Some(servers) = mcp.get("servers") {
            let Some(servers) = servers.as_table() else {
                bail!("mcp.servers must be a table");
            };
            for (name, server) in servers {
                validate_plugin_id(name).with_context(|| {
                    format!(
                        "MCP server name `{name}` must contain only ASCII letters, digits, `_`, or `-`"
                    )
                })?;
                check_keys(
                    server,
                    &["enabled", "command", "args", "env", "allowed_tools"],
                    &format!("mcp.servers.{name}"),
                )?;
                if let Some(enabled) = server.get("enabled") {
                    ensure!(
                        enabled.as_bool().is_some(),
                        "mcp.servers.{name}.enabled must be a boolean"
                    );
                }
                let enabled = server
                    .get("enabled")
                    .and_then(toml::Value::as_bool)
                    .unwrap_or(true);
                let command = server.get("command");
                if let Some(command) = command {
                    ensure!(
                        command.as_str().is_some(),
                        "mcp.servers.{name}.command must be a string"
                    );
                }
                ensure!(
                    !enabled
                        || command
                            .and_then(toml::Value::as_str)
                            .is_some_and(|command| !command.trim().is_empty()),
                    "enabled MCP server `{name}` must define a command"
                );
                if let Some(args) = server.get("args") {
                    check_string_array(args, &format!("mcp.servers.{name}.args"), false)?;
                }
                if let Some(env) = server.get("env") {
                    let Some(env) = env.as_table() else {
                        bail!("mcp.servers.{name}.env must be a table");
                    };
                    for (key, value) in env {
                        ensure!(
                            !key.is_empty() && !key.contains(['=', '\0']),
                            "MCP server `{name}` has an invalid environment key"
                        );
                        ensure!(
                            value.as_str().is_some_and(|value| !value.contains('\0')),
                            "MCP server `{name}` environment values must be strings without NUL bytes"
                        );
                    }
                }
                if let Some(allowed_tools) = server.get("allowed_tools") {
                    check_string_array(
                        allowed_tools,
                        &format!("mcp.servers.{name}.allowed_tools"),
                        true,
                    )?;
                }
            }
        }
    }
    if let Some(approvals) = root.get("approvals") {
        check_keys(
            approvals,
            &["reviewer_model", "reviewer_effort"],
            "approvals",
        )?;
        for name in ["reviewer_model", "reviewer_effort"] {
            if let Some(value) = approvals.get(name) {
                ensure!(
                    value.as_str().is_some_and(|value| !value.trim().is_empty()),
                    "approvals.{name} must be a non-empty string"
                );
            }
        }
    }
    if let Some(updates) = root.get("updates") {
        check_keys(
            updates,
            &["auto_install", "check_interval_hours"],
            "updates",
        )?;
        if let Some(auto_install) = updates.get("auto_install") {
            ensure!(
                auto_install.as_bool().is_some(),
                "updates.auto_install must be a boolean"
            );
        }
        if let Some(interval) = updates.get("check_interval_hours") {
            ensure!(
                interval
                    .as_integer()
                    .is_some_and(|value| (1..=720).contains(&value)),
                "updates.check_interval_hours must be between 1 and 720"
            );
        }
    }
    Ok(())
}

fn check_keys(value: &toml::Value, allowed: &[&str], section: &str) -> Result<()> {
    let Some(table) = value.as_table() else {
        bail!("settings section `{section}` must be a table");
    };
    for key in table.keys() {
        ensure!(
            allowed.contains(&key.as_str()),
            "unsupported setting `{section}.{key}`"
        );
    }
    Ok(())
}

fn check_bool_values(value: &toml::Value, section: &str) -> Result<()> {
    for (key, value) in value.as_table().expect("checked table") {
        ensure!(
            value.as_bool().is_some(),
            "{section}.{key} must be a boolean"
        );
    }
    Ok(())
}

fn check_positive_integer(value: &toml::Value, field: &str) -> Result<()> {
    ensure!(
        value.as_integer().is_some_and(|value| value > 0),
        "{field} must be a positive integer"
    );
    Ok(())
}

fn check_string_array(value: &toml::Value, field: &str, require_non_empty: bool) -> Result<()> {
    let Some(values) = value.as_array() else {
        bail!("{field} must be an array");
    };
    for value in values {
        ensure!(
            value.as_str().is_some_and(|value| {
                !value.contains('\0') && (!require_non_empty || !value.trim().is_empty())
            }),
            "{field} must contain valid strings"
        );
    }
    Ok(())
}

fn redact_toml(value: toml::Value) -> toml::Value {
    match value {
        toml::Value::Table(table) => toml::Value::Table(
            table
                .into_iter()
                .map(|(key, value)| {
                    let sensitive = key.eq_ignore_ascii_case("env")
                        || key.to_ascii_lowercase().contains("token")
                        || key.to_ascii_lowercase().contains("secret")
                        || key.to_ascii_lowercase().contains("password")
                        || key.eq_ignore_ascii_case("api_key");
                    (
                        key,
                        if sensitive {
                            toml::Value::String("[redacted]".to_string())
                        } else {
                            redact_toml(value)
                        },
                    )
                })
                .collect(),
        ),
        toml::Value::Array(values) => {
            toml::Value::Array(values.into_iter().map(redact_toml).collect())
        }
        value => value,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_merge_preserves_unmentioned_sections() {
        let mut root = toml::Value::Table(toml::map::Map::new());
        let patch = json_to_toml(json!({"aliases": {"review": "/effort high"}})).unwrap();
        let table = root.as_table_mut().unwrap();
        table.insert("commands".into(), toml::Value::Table(toml::map::Map::new()));
        merge_toml(table.get_mut("commands").unwrap(), patch);
        assert_eq!(
            root["commands"]["aliases"]["review"].as_str(),
            Some("/effort high")
        );
    }

    #[test]
    fn settings_validation_rejects_unknown_nested_fields_before_writing() {
        let valid = toml::from_str::<toml::Value>(
            r#"
            [team]
            preset = "xhigh_director_low_workers"
            worker_concurrency = 2
            [mcp.servers.docs]
            command = "docs-mcp"
            args = ["--stdio"]
            [approvals]
            reviewer_effort = "low"
            "#,
        )
        .unwrap();
        validate_settings_shape(&valid).unwrap();

        let invalid = toml::from_str::<toml::Value>(
            r#"
            [mcp.servers.docs]
            command = "docs-mcp"
            unexpected = true
            "#,
        )
        .unwrap();
        assert!(validate_settings_shape(&invalid).is_err());
    }

    #[test]
    fn plugin_ids_are_path_safe() {
        assert!(validate_plugin_id("review_tools").is_ok());
        assert!(validate_plugin_id("../escape").is_err());
    }

    #[test]
    fn create_plugin_writes_a_native_skill_that_can_be_rescanned() {
        let workspace = tempfile::tempdir().unwrap();
        let context = SelfServiceContext::new(workspace.path().to_path_buf());
        let result = context
            .call(
                "create_plugin",
                json!({
                    "id": "review-tools",
                    "description": "Review project changes",
                    "instructions": "Inspect the diff and report risks."
                }),
            )
            .unwrap();
        assert_eq!(result["hot_reload"], "next native turn");
        let skill = fs::read_to_string(workspace.path().join(".borg/skills/review-tools/SKILL.md"))
            .unwrap();
        assert!(skill.contains("name: review-tools"));
        assert!(skill.contains("Inspect the diff"));
        let listed = context.call("list_plugins", json!({})).unwrap();
        assert_eq!(listed["plugins"][0]["id"], "review-tools");
        let read = context
            .call("read_plugin", json!({"id": "review-tools"}))
            .unwrap();
        assert!(
            read["content"]
                .as_str()
                .unwrap()
                .contains("Inspect the diff")
        );
    }
}
