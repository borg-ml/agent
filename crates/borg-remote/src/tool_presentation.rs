use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use ts_rs::TS;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ToolPresentationCategory {
    Read,
    Edit,
    Execute,
    Search,
    Web,
    Agent,
    Plan,
    Goal,
    Image,
    Approval,
    Generic,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ToolPresentationBody {
    pub language: String,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ToolPresentation {
    pub label: String,
    pub detail: String,
    pub category: ToolPresentationCategory,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<ToolPresentationBody>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<ToolPresentationBody>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub body_rows: Vec<String>,
    #[serde(default)]
    pub backgrounded: bool,
    #[serde(default)]
    pub hidden: bool,
}

pub fn project_tool_presentation(
    name: &str,
    input: &Value,
    output: Option<&str>,
    is_error: bool,
) -> ToolPresentation {
    let (mut label, mut detail) = tool_call_summary(name, input);
    if is_error
        && is_mcp_resource_probe(name)
        && let Some(output) = output
    {
        detail = mcp_resource_error_detail(output);
    }
    if matches!(tool_leaf_name(name).as_str(), "read" | "read_file")
        && let Some(output) = output
        && let Some(output_detail) = read_output_detail(input, output)
    {
        detail = output_detail;
    }
    let input_body =
        tool_code_view(name, input).map(|(language, text)| ToolPresentationBody { language, text });
    let input_has_diff = input_body
        .as_ref()
        .is_some_and(|body| is_diff_language(&body.language));
    let edit_result = (!input_has_diff)
        .then(|| output.and_then(|output| edit_result_presentation(name, output)))
        .flatten();
    if let Some(edit) = &edit_result {
        label.clone_from(&edit.label);
        detail.clone_from(&edit.detail);
    }
    ToolPresentation {
        category: tool_category(name, &label, input),
        input: input_body,
        output: edit_result.map(|edit| edit.body).or_else(|| {
            output.and_then(|output| {
                tool_output_code_view(name, output)
                    .map(|(language, text)| ToolPresentationBody { language, text })
            })
        }),
        result: output.and_then(|output| summarize_tool_result(name, input, output, is_error)),
        body_rows: tool_detail_rows(name, input),
        backgrounded: output.is_some_and(|output| !is_error && tool_output_is_backgrounded(output)),
        hidden: is_internal_tool(name),
        label,
        detail,
    }
}

/// Codex exposes MCP resource discovery as built-in model tools. Borg's MCP
/// bridge is intentionally a tool server, so these probes are informational
/// and have no useful expandable request/response body in the transcript.
pub fn is_mcp_resource_probe(name: &str) -> bool {
    matches!(
        tool_leaf_name(name).as_str(),
        "list_mcp_resources" | "list_mcp_resource_templates"
    )
}

struct EditResultPresentation {
    label: String,
    detail: String,
    body: ToolPresentationBody,
}

fn edit_result_presentation(name: &str, output: &str) -> Option<EditResultPresentation> {
    let value = serde_json::from_str::<Value>(output).ok()?;
    edit_value_presentation(name, &value)
}

fn edit_value_presentation(name: &str, value: &Value) -> Option<EditResultPresentation> {
    let is_edit_tool = matches!(
        tool_leaf_name(name).as_str(),
        "edit" | "apply_patch" | "write" | "write_file"
    );
    let entries = if is_edit_tool {
        value
            .as_array()
            .cloned()
            .or_else(|| value.get("changes").and_then(Value::as_array).cloned())?
    } else {
        value.get("changes").and_then(Value::as_array).cloned()?
    };
    let rendered = entries
        .iter()
        .filter_map(render_edit_result_entry)
        .collect::<Vec<_>>();
    if rendered.is_empty() {
        return None;
    }
    let paths = rendered.iter().fold(Vec::new(), |mut paths, entry| {
        let path = entry.path.as_str();
        if !paths.contains(&path) {
            paths.push(path);
        }
        paths
    });
    let all_add = rendered.iter().all(|entry| entry.kind == "add");
    let all_delete = rendered.iter().all(|entry| entry.kind == "delete");
    let label = match (rendered.len(), all_add, all_delete) {
        (1, true, _) => "Create file",
        (1, _, true) => "Delete file",
        (_, true, _) => "Create files",
        (_, _, true) => "Delete files",
        _ => "Edit",
    };
    let detail = summarize_edit_paths(&paths).unwrap_or_else(|| "files".to_string());
    let multi_file = paths.len() > 1;
    let language = if paths.len() == 1 {
        Path::new(paths[0])
            .extension()
            .and_then(|extension| extension.to_str())
            .map(|extension| format!("diff:{extension}"))
            .unwrap_or_else(|| "diff".to_string())
    } else {
        "diff".to_string()
    };
    Some(EditResultPresentation {
        label: label.to_string(),
        detail,
        body: ToolPresentationBody {
            language,
            text: rendered
                .into_iter()
                .map(|entry| {
                    if multi_file {
                        // Preserve each file boundary in a multi-file edit so
                        // the terminal renderer can select the right syntax
                        // grammar for every hunk. These control lines are not
                        // shown as diff content.
                        format!("*** Update File: {}\n{}", entry.path, entry.diff)
                    } else {
                        entry.diff
                    }
                })
                .collect::<Vec<_>>()
                .join("\n"),
        },
    })
}

struct RenderedEditResult {
    path: String,
    kind: String,
    diff: String,
}

fn render_edit_result_entry(entry: &Value) -> Option<RenderedEditResult> {
    let path = entry.get("path")?.as_str()?.to_string();
    let kind = entry
        .pointer("/kind/type")
        .and_then(Value::as_str)
        .unwrap_or("update")
        .to_ascii_lowercase();
    let source = entry
        .get("diff")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let diff = match kind.as_str() {
        "add" => file_body_as_diff(&path, source, true),
        "delete" => file_body_as_diff(&path, source, false),
        _ if looks_like_patch(source) => patch_source(source).unwrap_or(source).to_string(),
        _ => source.to_string(),
    };
    (!diff.trim().is_empty()).then_some(RenderedEditResult { path, kind, diff })
}

fn file_body_as_diff(path: &str, source: &str, added: bool) -> String {
    let line_count = source.lines().count();
    let (old_path, new_path, range, prefix) = if added {
        (
            "/dev/null".to_string(),
            path.to_string(),
            format!("-0,0 +1,{line_count}"),
            '+',
        )
    } else {
        (
            path.to_string(),
            "/dev/null".to_string(),
            format!("-1,{line_count} +0,0"),
            '-',
        )
    };
    let body = source
        .split_inclusive('\n')
        .map(|line| format!("{prefix}{}", line.trim_end_matches('\n')))
        .collect::<Vec<_>>()
        .join("\n");
    format!("--- {old_path}\n+++ {new_path}\n@@ {range} @@\n{body}")
}

pub fn tool_code_view(name: &str, input: &Value) -> Option<(String, String)> {
    if matches!(
        tool_leaf_name(name).as_str(),
        "update_plan"
            | "update_todo"
            | "update_todos"
            | "get_plan"
            | "get_todo"
            | "get_todos"
            | "get_goal"
            | "create_goal"
            | "update_goal"
    ) {
        return None;
    }
    if is_mcp_resource_probe(name) {
        return None;
    }
    if is_edit_tool(name, "Edit")
        && let Some(edit) = edit_value_presentation(name, input)
    {
        return Some((edit.body.language, edit.body.text));
    }
    if name.to_ascii_lowercase().contains("edit")
        && let Some((path, source)) = claude_edit_diff(input)
    {
        let language = Path::new(&path)
            .extension()
            .and_then(|extension| extension.to_str())
            .map(|extension| format!("diff:{extension}"))
            .unwrap_or_else(|| "diff".to_string());
        return Some((language, source));
    }
    if matches!(tool_leaf_name(name).as_str(), "write" | "write_file")
        && input.get("overwrite").and_then(Value::as_bool) != Some(true)
        && let (Some(path), Some(content)) = (
            input.get("path").and_then(Value::as_str),
            input.get("content").and_then(Value::as_str),
        )
        && !content.is_empty()
    {
        let language = Path::new(path)
            .extension()
            .and_then(|extension| extension.to_str())
            .map(|extension| format!("diff:{extension}"))
            .unwrap_or_else(|| "diff".to_string());
        return Some((language, file_body_as_diff(path, content, true)));
    }
    if let Some(source) = edit_source(input) {
        let is_edit = name.to_ascii_lowercase().contains("edit")
            || name.to_ascii_lowercase().contains("patch")
            || looks_like_patch(source);
        if is_edit && let Some(source) = patch_source(source) {
            let language = edit_path(input)
                .or_else(|| patch_path(source))
                .and_then(|path| {
                    Path::new(&path)
                        .extension()
                        .and_then(|extension| extension.to_str())
                        .map(|extension| format!("diff:{extension}"))
                })
                .unwrap_or_else(|| "diff".to_string());
            return Some((language, source.to_string()));
        }
    }
    if let Some(command) = command_from_input(input)
        && !command.trim().is_empty()
    {
        return Some(("command".to_string(), unwrapped_shell_command(command)));
    }
    (!input.is_null()).then(|| {
        (
            "json".to_string(),
            serde_json::to_string_pretty(input).unwrap_or_else(|_| input.to_string()),
        )
    })
}

pub fn tool_has_rich_ui(display_name: &str, language: Option<&str>) -> bool {
    language.is_some_and(|language| {
        is_diff_language(language) || matches!(language, "lsp" | "reasoning")
    }) || matches!(
        display_name.to_ascii_lowercase().as_str(),
        "update plan" | "read plan"
    )
}

/// Whether an action is expected to complete quickly enough that animating its
/// pending state would add noise. Unknown actions stay conservative and use
/// the long-running presentation until they finish.
pub fn tool_action_is_instant(display_name: &str, language: Option<&str>) -> bool {
    if language == Some("reasoning") {
        return false;
    }
    let label = display_name.trim().to_ascii_lowercase();
    label == "read"
        || label.starts_with("read ")
        || label.starts_with("inspect ")
        || label.starts_with("list ")
        || label.starts_with("check ")
        || label.starts_with("get ")
        || matches!(
            label.as_str(),
            "create goal"
                | "update goal"
                | "language servers"
                | "workspace diagnostics"
                | "view image"
        )
}

pub fn is_subagent_tool(display_name: &str) -> bool {
    matches!(
        display_name.to_ascii_lowercase().as_str(),
        "spawn agent"
            | "list agents"
            | "follow up"
            | "message agent"
            | "interrupt agent"
            | "stop agent"
            | "wait for agents"
    )
}

/// Whether a tool edits files, decided from the raw tool name and its display
/// label alone. Unlike a diff body this is known the moment the call starts,
/// which is what lets the transcript describe an edit before its patch has
/// arrived.
pub fn is_edit_tool(name: &str, display_name: &str) -> bool {
    tool_category(name, display_name, &Value::Null) == ToolPresentationCategory::Edit
}

pub fn is_diff_language(language: &str) -> bool {
    language == "diff" || language.starts_with("diff:")
}

pub fn tool_call_summary(name: &str, input: &Value) -> (String, String) {
    let tool = tool_leaf_name(name);

    if tool == "action_preparing" {
        let label = string_field(input, "label").unwrap_or("action");
        return ("Prepare next action".to_string(), compact_text(label, 64));
    }

    if is_mcp_resource_probe(name) {
        let label = if name.starts_with("mcp__") {
            format_mcp_tool_name(name)
        } else if tool == "list_mcp_resources" {
            "List MCP resources".to_string()
        } else {
            "List MCP resource templates".to_string()
        };
        return (label, String::new());
    }

    if matches!(tool.as_str(), "toolsearch" | "tool_search") {
        return (
            "Search Tools".to_string(),
            high_signal_detail(input).unwrap_or_default(),
        );
    }

    if let Some(git) = git_call(name, input) {
        return (git.label, git.detail);
    }

    if matches!(
        tool.as_str(),
        "update_plan" | "update_todo" | "update_todos"
    ) {
        let count = input
            .get("plan")
            .or_else(|| input.get("items"))
            .and_then(Value::as_array)
            .map(Vec::len);
        let detail = count
            .map(|count| format!("{count} {}", if count == 1 { "step" } else { "steps" }))
            .unwrap_or_else(|| "session plan".to_string());
        return ("Update plan".to_string(), detail);
    }

    if matches!(tool.as_str(), "get_plan" | "get_todo" | "get_todos") {
        return ("Read plan".to_string(), "current steps".to_string());
    }

    if tool == "get_goal" {
        return ("Read goal".to_string(), String::new());
    }

    if tool == "consult_model" {
        let profile = string_field(input, "profile").unwrap_or("model");
        return (
            "Consult model".to_string(),
            format!("{} · second opinion", compact_text(profile, 80)),
        );
    }

    if tool == "consult_peer" {
        let profile = string_field(input, "profile").unwrap_or("opposite provider");
        return (
            "Consult peer".to_string(),
            format!("{} · persistent peer", compact_text(profile, 80)),
        );
    }

    if tool == "create_goal" {
        let objective = string_field(input, "objective").unwrap_or("new goal");
        let budget = input
            .get("token_budget")
            .and_then(Value::as_u64)
            .map(|budget| format!(" · {budget} tokens"))
            .unwrap_or_default();
        return (
            "Create goal".to_string(),
            format!("{}{budget}", compact_text(objective, 120)),
        );
    }

    if tool == "update_goal" {
        return (
            "Update goal".to_string(),
            string_field(input, "status")
                .unwrap_or("status")
                .to_string(),
        );
    }

    if tool == "spawn_agent" {
        let task = string_field(input, "task_name").unwrap_or("child");
        let provider = string_field(input, "provider")
            .map(|provider| format!(" · {provider}"))
            .unwrap_or_default();
        return ("Spawn agent".to_string(), format!("{task}{provider}"));
    }

    if tool == "list_agents" {
        return (
            "List agents".to_string(),
            string_field(input, "path_prefix")
                .unwrap_or("all children")
                .to_string(),
        );
    }

    if matches!(tool.as_str(), "send_message" | "followup_task") {
        let target = string_field(input, "target").unwrap_or("agent");
        let message = string_field(input, "message")
            .map(|message| format!(" · {}", compact_text(message, 100)))
            .unwrap_or_default();
        let action = if tool == "send_message" {
            "Message agent"
        } else {
            "Follow up"
        };
        return (action.to_string(), format!("{target}{message}"));
    }

    if matches!(tool.as_str(), "interrupt_agent" | "stop_agent") {
        return (
            if tool == "stop_agent" {
                "Stop agent"
            } else {
                "Interrupt agent"
            }
            .to_string(),
            string_field(input, "target").unwrap_or("child").to_string(),
        );
    }

    if tool == "wait_agent" {
        let timeout = input
            .get("timeout_ms")
            .and_then(Value::as_u64)
            .map(format_duration)
            .unwrap_or_else(|| "updates".to_string());
        return ("Wait for agents".to_string(), timeout);
    }

    if matches!(tool.as_str(), "subagentactivity" | "subagent_activity") {
        let kind = string_field(input, "kind").unwrap_or("updated");
        let label = match kind.to_ascii_lowercase().as_str() {
            "started" | "running" => "Agent started",
            "completed" => "Agent completed",
            "interrupted" => "Agent interrupted",
            "failed" | "errored" => "Agent failed",
            "stopped" | "shutdown" => "Agent stopped",
            _ => "Agent activity",
        };
        let path = string_field(input, "agentPath")
            .or_else(|| string_field(input, "agent_path"))
            .or_else(|| string_field(input, "target"))
            .unwrap_or("agent");
        let task = path
            .rsplit('/')
            .find(|segment| !segment.is_empty())
            .unwrap_or(path);
        return (label.to_string(), compact_text(task, 120));
    }

    if tool == "lsp_status" {
        return ("Language servers".to_string(), "status".to_string());
    }

    if tool == "lsp_workspace_diagnostics" {
        return ("Workspace diagnostics".to_string(), source_location(input));
    }

    if tool == "lsp_workspace_symbols" {
        let query = string_field(input, "query").unwrap_or("");
        return (
            "Search symbols".to_string(),
            format!("“{}”", compact_text(query, 120)),
        );
    }

    if let Some(action) = match tool.as_str() {
        "lsp_diagnostics" => Some("Check diagnostics"),
        "lsp_hover" => Some("Inspect symbol"),
        "lsp_definition" => Some("Go to definition"),
        "lsp_references" => Some("Find references"),
        "lsp_document_symbols" => Some("List symbols"),
        _ => None,
    } {
        return (action.to_string(), source_location(input));
    }

    if let Some((label, detail)) = product_tool_summary(name, input) {
        return (label, detail);
    }

    if name.to_ascii_lowercase().contains("web") {
        let detail = web_search_query(input)
            .map(|query| format!("“{}”", compact_text(&query, 120)))
            .unwrap_or_else(|| "searching…".to_string());
        return ("Search web".to_string(), detail);
    }

    if let Some(command) = command_from_input(input) {
        if let Some(query) = search_query(command) {
            return (
                "Search".to_string(),
                format!("“{}”", compact_text(&query, 120)),
            );
        }
        let label = if command_is_read_only(command) {
            "Read"
        } else {
            "Run"
        };
        return (
            label.to_string(),
            compact_text(&unwrapped_shell_command(command), 160),
        );
    }

    if tool.contains("read")
        && let Some(path) = input_path(input)
    {
        return ("Read".to_string(), path.to_string());
    }

    if tool.contains("edit") || tool.contains("patch") || tool.contains("write_file") {
        let detail = edit_detail(input).unwrap_or_else(|| "files".to_string());
        return ("Edit".to_string(), detail);
    }

    if name.starts_with("mcp__") {
        let server = name.split("__").nth(1).unwrap_or_default();
        if matches!(
            server,
            "airtable" | "borg" | "borg_agent" | "codex" | "lawborg" | "notion" | "slack"
        ) {
            return (
                format_mcp_tool_name(name),
                high_signal_detail(input).unwrap_or_default(),
            );
        }
    }

    (humanize_tool_name(name), concise_tool_input(input))
}

pub fn web_search_query(input: &Value) -> Option<String> {
    if let Some(query) = input
        .get("query")
        .and_then(Value::as_str)
        .filter(|query| !query.trim().is_empty())
    {
        return Some(query.to_string());
    }
    let queries = input
        .get("search_query")?
        .as_array()?
        .iter()
        .filter_map(|query| query.get("q").and_then(Value::as_str))
        .filter(|query| !query.trim().is_empty())
        .collect::<Vec<_>>();
    (!queries.is_empty()).then(|| queries.join(" · "))
}

pub fn compact_text(value: &str, limit: usize) -> String {
    let value = value.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut characters = value.chars();
    let compact: String = characters.by_ref().take(limit).collect();
    if characters.next().is_some() {
        format!("{compact}…")
    } else {
        compact
    }
}

pub fn tool_output_code_view(name: &str, output: &str) -> Option<(String, String)> {
    let trimmed = output.trim_end();
    if trimmed.is_empty() || trimmed == "null" {
        return None;
    }
    if is_mcp_resource_probe(name) {
        return None;
    }
    if matches!(
        tool_leaf_name(name).as_str(),
        "edit" | "apply_patch" | "write" | "write_file"
    ) {
        return None;
    }
    let normalized = name.to_ascii_lowercase();
    if normalized.contains("lsp") || normalized.contains("diagnostic") {
        return Some(("lsp".to_string(), trimmed.to_string()));
    }
    let readable = readable_result_text(trimmed);
    if let Ok(value) = serde_json::from_str::<Value>(&readable) {
        return Some((
            "json".to_string(),
            serde_json::to_string_pretty(&value).unwrap_or(readable),
        ));
    }
    Some(("text".to_string(), readable))
}

pub fn tool_output_is_backgrounded(output: &str) -> bool {
    tool_output_background_handle(output).is_some()
}

pub fn tool_can_start_background_process(name: &str) -> bool {
    matches!(
        tool_leaf_name(name).as_str(),
        "bash" | "command_execution" | "exec" | "exec_command"
    )
}

fn process_handle_value(value: &Value) -> Option<String> {
    ["session_id", "cell_id"]
        .into_iter()
        .find_map(|key| value.get(key))
        .and_then(|value| match value {
            Value::String(value) => Some(value.clone()),
            Value::Number(value) => Some(value.to_string()),
            _ => None,
        })
}

pub fn tool_output_background_handle(output: &str) -> Option<String> {
    if let Ok(value) = serde_json::from_str::<Value>(output)
        && let Some(handle) = process_handle_value(&value).or_else(|| {
            value
                .get("structuredContent")
                .and_then(process_handle_value)
        })
    {
        return Some(handle);
    }
    let output = output.trim();
    [
        "Process running with session ID ",
        "Script running with cell ID ",
    ]
    .into_iter()
    .find_map(|marker| {
        output.strip_prefix(marker).map(|suffix| {
            suffix
                .split_whitespace()
                .next()
                .unwrap_or_default()
                .trim_matches(|character: char| !character.is_alphanumeric() && character != '-')
                .to_string()
        })
    })
    .filter(|handle| !handle.is_empty())
}

pub fn tool_process_followup_handle(name: &str, input: Option<&Value>) -> Option<String> {
    matches!(tool_leaf_name(name).as_str(), "wait" | "write_stdin")
        .then(|| input.and_then(process_handle_value))
        .flatten()
}

pub fn tool_process_output_text(output: &str) -> String {
    if let Ok(value) = serde_json::from_str::<Value>(output) {
        let process = value.get("structuredContent").unwrap_or(&value);
        if let Some(stdout) = process.get("output").and_then(Value::as_str) {
            return stdout.to_string();
        }
        if let Some(stdout) = process.get("stdout").and_then(Value::as_str) {
            let stderr = process
                .get("stderr")
                .and_then(Value::as_str)
                .unwrap_or_default();
            return [stdout, stderr]
                .into_iter()
                .filter(|text| !text.is_empty())
                .collect::<Vec<_>>()
                .join("\n");
        }
        if let Some(content) = value.get("content").and_then(Value::as_array) {
            let text = content
                .iter()
                .filter_map(|item| item.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("\n");
            if !text.is_empty() && text != output {
                return tool_process_output_text(&text);
            }
        }
    }
    output.to_string()
}

fn product_tool_summary(name: &str, input: &Value) -> Option<(String, String)> {
    let leaf = tool_leaf_name(name);
    let label = match leaf.as_str() {
        "read" | "read_file" => "Read",
        "write" | "write_file" => "Edit",
        "edit" | "apply_patch" => "Edit",
        "grep" | "glob" => "Search",
        "todo_list" | "todowrite" => "Update plan",
        "image_generation" | "imagegeneration" => "Generate image",
        "view_image" | "imageview" => "View image",
        "remote_approval" => "Approval required",
        "search_documents" => "Search source documents",
        "list_documents" => "List source documents",
        "read_document" => "Read source document",
        "check_coverage" => "Check source coverage",
        "get_document_categories" => "Read source categories",
        "gmail_list_accounts" => "Check Gmail accounts",
        "gmail_list_labels" => "Check Gmail labels",
        "gmail_search_messages" => "Search Gmail",
        "gmail_read_message" => "Read Gmail message",
        "gmail_read_attachment" => "Read Gmail attachment",
        "google_drive_list_accounts" => "Check Google Drive accounts",
        "google_drive_search_files" => "Search Google Drive",
        "google_drive_read_file" => "Read Google Drive file",
        "outlook_list_accounts" => "Check Outlook accounts",
        "outlook_list_folders" => "Check Outlook folders",
        "outlook_search_messages" => "Search Outlook",
        "outlook_read_message" => "Read Outlook message",
        "outlook_read_attachment" => "Read Outlook attachment",
        _ => return None,
    };
    let detail = if matches!(leaf.as_str(), "read" | "read_file") {
        read_tool_detail(input).unwrap_or_default()
    } else if matches!(
        leaf.as_str(),
        "edit" | "apply_patch" | "write" | "write_file"
    ) {
        edit_detail(input).unwrap_or_else(|| "files".to_string())
    } else {
        high_signal_detail(input).unwrap_or_default()
    };
    Some((label.to_string(), compact_text(&detail, 200)))
}

fn tool_category(name: &str, label: &str, input: &Value) -> ToolPresentationCategory {
    let leaf = tool_leaf_name(name);
    if is_subagent_tool(label)
        || matches!(
            leaf.as_str(),
            "task"
                | "agent"
                | "subagentactivity"
                | "subagent_activity"
                | "collab_tool_call"
                | "collabagenttoolcall"
                | "consult_model"
                | "consult_peer"
        )
    {
        ToolPresentationCategory::Agent
    } else if matches!(
        leaf.as_str(),
        "update_plan"
            | "update_todo"
            | "update_todos"
            | "get_plan"
            | "get_todo"
            | "get_todos"
            | "todo_list"
            | "todowrite"
    ) {
        ToolPresentationCategory::Plan
    } else if matches!(leaf.as_str(), "get_goal" | "create_goal" | "update_goal") {
        ToolPresentationCategory::Goal
    } else if is_mcp_resource_probe(name) {
        ToolPresentationCategory::Read
    } else if leaf == "remote_approval" {
        ToolPresentationCategory::Approval
    } else if matches!(
        leaf.as_str(),
        "image_generation" | "imagegeneration" | "view_image" | "imageview"
    ) {
        ToolPresentationCategory::Image
    } else if name.to_ascii_lowercase().contains("web") {
        ToolPresentationCategory::Web
    } else if git_action_from_tool_name(name).is_some()
        || matches!(tool_leaf_name(name).as_str(), "git") && is_git_label(label)
        || matches!(
            leaf.as_str(),
            "bash" | "command_execution" | "exec_command" | "exec"
        )
    {
        if let Some(command) = command_from_input(input)
            && command_is_read_only(command)
        {
            if search_query(command).is_some() {
                ToolPresentationCategory::Search
            } else {
                ToolPresentationCategory::Read
            }
        } else {
            ToolPresentationCategory::Execute
        }
    } else if leaf.contains("edit") || leaf.contains("patch") || leaf.contains("write") {
        ToolPresentationCategory::Edit
    } else if leaf.contains("read") || leaf.contains("fetch") || leaf.contains("get_document") {
        ToolPresentationCategory::Read
    } else if leaf.contains("search") || matches!(leaf.as_str(), "grep" | "glob") {
        ToolPresentationCategory::Search
    } else {
        ToolPresentationCategory::Generic
    }
}

fn is_git_label(label: &str) -> bool {
    matches!(
        label,
        "Add worktree"
            | "List worktrees"
            | "Remove worktree"
            | "Prune worktrees"
            | "Lock worktree"
            | "Unlock worktree"
            | "Git status"
            | "Git diff"
            | "Git log"
            | "Git branch"
            | "Switch branch"
            | "Check out revision"
            | "Commit changes"
            | "Fetch changes"
            | "Pull changes"
            | "Push changes"
            | "Merge branch"
            | "Rebase branch"
            | "Show revision"
            | "Git tags"
            | "Git remotes"
            | "Repository info"
    )
}

fn tool_detail_rows(name: &str, input: &Value) -> Vec<String> {
    let leaf = tool_leaf_name(name);
    if matches!(
        leaf.as_str(),
        "bash" | "command_execution" | "exec_command" | "exec"
    ) {
        return string_field(input, "description")
            .filter(|description| !description.trim().is_empty())
            .map(|description| vec![format!("Purpose: {}", compact_text(description, 180))])
            .unwrap_or_default();
    }
    if name.to_ascii_lowercase().contains("web")
        && let Some(query) = web_search_query(input)
    {
        return vec![format!("Query: {}", compact_text(&query, 180))];
    }
    if name.starts_with("mcp__")
        && let Value::Object(fields) = input
    {
        return fields
            .iter()
            .filter(|(key, value)| !is_noise_param(key, value))
            .filter_map(|(key, value)| {
                let rendered = format_value(value);
                (!rendered.is_empty())
                    .then(|| format!("{}: {}", humanize_key(key), compact_text(&rendered, 180)))
            })
            .take(6)
            .collect();
    }
    Vec::new()
}

fn summarize_tool_result(
    name: &str,
    input: &Value,
    output: &str,
    is_error: bool,
) -> Option<String> {
    let readable = readable_result_text(output);
    let trimmed = readable.trim();
    if trimmed.is_empty() || trimmed == "null" {
        return None;
    }
    if is_mcp_resource_probe(name) {
        if is_error {
            return Some(mcp_resource_error_detail(trimmed));
        }
        let key = if tool_leaf_name(name) == "list_mcp_resources" {
            "resources"
        } else {
            "resourceTemplates"
        };
        if let Ok(Value::Object(fields)) = serde_json::from_str::<Value>(trimmed)
            && let Some(items) = fields.get(key).and_then(Value::as_array)
        {
            let noun = if key == "resources" {
                "resource"
            } else {
                "resource template"
            };
            return Some(format!(
                "{} {noun}{}",
                items.len(),
                if items.len() == 1 { "" } else { "s" }
            ));
        }
    }
    if is_error {
        return trimmed
            .lines()
            .find(|line| !line.trim().is_empty())
            .map(|line| compact_text(line, 100));
    }
    if git_call(name, input).is_some() {
        return concise_git_result(trimmed);
    }
    if let Ok(Value::Object(fields)) = serde_json::from_str::<Value>(trimmed) {
        if let Some(status) = fields.get("status").and_then(Value::as_str) {
            return Some(status.replace('_', " "));
        }
        for key in [
            "documents",
            "messages",
            "records",
            "results",
            "items",
            "sources",
            "files",
            "matches",
        ] {
            if let Some(items) = fields.get(key).and_then(Value::as_array) {
                let label = if items.len() == 1 {
                    key.trim_end_matches('s')
                } else {
                    key
                };
                return Some(format!("{} {label}", items.len()));
            }
        }
    }
    if let Some(captures) = coverage_summary(trimmed) {
        return Some(captures);
    }
    let leaf = tool_leaf_name(name);
    let lines = trimmed
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    if matches!(leaf.as_str(), "grep" | "glob") {
        if trimmed.contains("No matches found") {
            return Some("no matches".to_string());
        }
        return Some(format!(
            "{} {}",
            lines.len(),
            if lines.len() == 1 { "match" } else { "matches" }
        ));
    }
    if name.to_ascii_lowercase().contains("web") {
        return Some(format!(
            "{} {}",
            lines.len(),
            if lines.len() == 1 {
                "result"
            } else {
                "results"
            }
        ));
    }
    if matches!(
        leaf.as_str(),
        "bash" | "command_execution" | "exec_command" | "exec"
    ) {
        return Some(if lines.len() == 1 && lines[0].chars().count() < 80 {
            lines[0].to_string()
        } else {
            "completed".to_string()
        });
    }
    Some(if lines.len() == 1 && lines[0].chars().count() < 80 {
        lines[0].to_string()
    } else {
        "completed".to_string()
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GitAction {
    WorktreeAdd,
    WorktreeList,
    WorktreeRemove,
    WorktreePrune,
    WorktreeLock,
    WorktreeUnlock,
    Add,
    Status,
    Diff,
    Log,
    Branch,
    Switch,
    Checkout,
    Commit,
    Fetch,
    Pull,
    Push,
    Merge,
    Rebase,
    Show,
    Tag,
    Remote,
    RepositoryInfo,
}

impl GitAction {
    fn label(self) -> &'static str {
        match self {
            Self::WorktreeAdd => "Add worktree",
            Self::WorktreeList => "List worktrees",
            Self::WorktreeRemove => "Remove worktree",
            Self::WorktreePrune => "Prune worktrees",
            Self::WorktreeLock => "Lock worktree",
            Self::WorktreeUnlock => "Unlock worktree",
            Self::Add => "Git add",
            Self::Status => "Git status",
            Self::Diff => "Git diff",
            Self::Log => "Git log",
            Self::Branch => "Git branch",
            Self::Switch => "Switch branch",
            Self::Checkout => "Check out revision",
            Self::Commit => "Commit changes",
            Self::Fetch => "Fetch changes",
            Self::Pull => "Pull changes",
            Self::Push => "Push changes",
            Self::Merge => "Merge branch",
            Self::Rebase => "Rebase branch",
            Self::Show => "Show revision",
            Self::Tag => "Git tags",
            Self::Remote => "Git remotes",
            Self::RepositoryInfo => "Repository info",
        }
    }
}

struct GitCall {
    label: String,
    detail: String,
}

fn git_call(name: &str, input: &Value) -> Option<GitCall> {
    if let Some(command) = command_from_input(input)
        && let Some(call) = if tool_leaf_name(name) == "git" {
            git_summary(&format!("git {command}"))
        } else {
            git_summary(command)
        }
    {
        return Some(call);
    }
    let action = git_action_from_tool_name(name)?;
    Some(GitCall {
        label: action.label().to_string(),
        detail: git_tool_detail(action, input),
    })
}

fn git_action_from_tool_name(name: &str) -> Option<GitAction> {
    let lower = name.to_ascii_lowercase();
    let leaf = tool_leaf_name(name);
    let direct_git_tool = leaf.starts_with("git_")
        || leaf.starts_with("git-")
        || lower
            .strip_prefix("mcp__")
            .and_then(|rest| rest.split_once("__"))
            .is_some_and(|(server, _)| server == "git");
    if !direct_git_tool {
        return None;
    }
    let normalized = leaf
        .strip_prefix("git_")
        .or_else(|| leaf.strip_prefix("git-"))
        .unwrap_or(&leaf)
        .replace('-', "_");
    git_action_from_parts(&normalized.split('_').collect::<Vec<_>>())
}

struct GitInvocation {
    action: GitAction,
    arguments: Vec<String>,
}

fn git_summary(command: &str) -> Option<GitCall> {
    let normalized_command = unwrapped_shell_command(command);
    let words = shell_words(&normalized_command);
    let invocations = git_invocations(command);
    let first = invocations.first()?;
    if words
        .split(|word| is_shell_operator(word))
        .filter(|segment| {
            segment
                .iter()
                .any(|word| word.rsplit('/').next() == Some("git"))
        })
        .any(|segment| git_invocation(segment).is_none())
    {
        return None;
    }
    let same_action = invocations
        .iter()
        .all(|invocation| invocation.action == first.action);
    let mut auxiliary_detail = git_auxiliary_detail(&words)?;
    let label = if !auxiliary_detail.is_empty() {
        "Inspect repository".to_string()
    } else if invocations.len() == 1 {
        first.action.label().to_string()
    } else if same_action {
        repeated_git_label(first.action)
    } else if invocations
        .iter()
        .all(|invocation| git_action_is_read_only(invocation.action))
    {
        "Inspect repository".to_string()
    } else {
        "Run Git operations".to_string()
    };
    let detail = if same_action {
        let mut detail = invocations
            .iter()
            .map(|invocation| git_invocation_detail(invocation.action, &invocation.arguments))
            .filter(|detail| !detail.is_empty())
            .collect::<Vec<_>>();
        detail.append(&mut auxiliary_detail);
        detail.join(", ")
    } else {
        let mut detail = invocations
            .iter()
            .map(|invocation| {
                let detail = git_invocation_detail(invocation.action, &invocation.arguments);
                if detail.is_empty() {
                    git_action_name(invocation.action).to_string()
                } else {
                    format!("{}: {detail}", git_action_name(invocation.action))
                }
            })
            .collect::<Vec<_>>();
        detail.append(&mut auxiliary_detail);
        detail.join(", ")
    };
    Some(GitCall { label, detail })
}

/// Return read-only commands composed after a Git invocation. A shell
/// pipeline is treated as presentation plumbing (`git show … | sed …`), but
/// a command joined with `&&` or `;` is part of the user's actual operation
/// and should be visible in the summary. Any unknown or mutating command
/// makes the Git-specific projection unsafe, so the caller falls back to the
/// generic execution summary instead of hiding it.
fn git_auxiliary_detail(words: &[String]) -> Option<Vec<String>> {
    let mut details = Vec::new();
    for (operator, segment) in shell_command_segments(words) {
        if segment.is_empty() || git_invocation(&segment).is_some() {
            continue;
        }
        if operator == Some("|") || is_shell_setup_segment(&segment) {
            continue;
        }
        let command = segment.join(" ");
        if !command_is_read_only(&command) {
            return None;
        }
        let detail = if let Some(query) = search_query(&command) {
            format!("search: {}", compact_text(&query, 120))
        } else {
            format!("read: {}", compact_text(&command, 120))
        };
        details.push(detail);
    }
    Some(details)
}

fn shell_command_segments(words: &[String]) -> Vec<(Option<&str>, Vec<String>)> {
    let mut segments = Vec::new();
    let mut segment = Vec::new();
    let mut operator = None;
    for word in words {
        if is_shell_operator(word) {
            if !segment.is_empty() {
                segments.push((operator, std::mem::take(&mut segment)));
            }
            operator = Some(word.as_str());
        } else {
            segment.push(word.clone());
        }
    }
    if !segment.is_empty() {
        segments.push((operator, segment));
    }
    segments
}

fn is_shell_setup_segment(segment: &[String]) -> bool {
    let Some(command) = segment.first().and_then(|word| word.rsplit('/').next()) else {
        return true;
    };
    matches!(command, "cd" | "true" | "false" | ":")
}

fn git_invocations(command: &str) -> Vec<GitInvocation> {
    let words = shell_words(command);
    if let Some(script) = shell_script(&words) {
        return git_invocations(script);
    }
    words
        .split(|word| is_shell_operator(word))
        .filter_map(git_invocation)
        .collect()
}

fn git_invocation(segment: &[String]) -> Option<GitInvocation> {
    let git_index = segment.iter().enumerate().find_map(|(index, word)| {
        (word.rsplit('/').next() == Some("git")
            && segment[..index]
                .iter()
                .all(|prefix| prefix == "env" || prefix.contains('=')))
        .then_some(index)
    })?;
    let mut parts = segment[git_index + 1..]
        .iter()
        .map(String::as_str)
        .peekable();
    while let Some(part) = parts.peek().copied() {
        if matches!(
            part,
            "-C" | "-c" | "--git-dir" | "--work-tree" | "--namespace"
        ) {
            parts.next();
            parts.next();
        } else if part.starts_with('-') {
            parts.next();
        } else {
            break;
        }
    }
    let arguments = parts.map(str::to_string).collect::<Vec<_>>();
    let refs = arguments.iter().map(String::as_str).collect::<Vec<_>>();
    Some(GitInvocation {
        action: git_action_from_parts(&refs)?,
        arguments,
    })
}

fn git_action_from_parts(parts: &[&str]) -> Option<GitAction> {
    match parts {
        ["worktree", "add", ..] => Some(GitAction::WorktreeAdd),
        ["worktree", "list", ..] => Some(GitAction::WorktreeList),
        ["worktree", "remove", ..] => Some(GitAction::WorktreeRemove),
        ["worktree", "prune", ..] => Some(GitAction::WorktreePrune),
        ["worktree", "lock", ..] => Some(GitAction::WorktreeLock),
        ["worktree", "unlock", ..] => Some(GitAction::WorktreeUnlock),
        ["add", ..] => Some(GitAction::Add),
        ["status", ..] => Some(GitAction::Status),
        ["diff", ..] => Some(GitAction::Diff),
        ["log", ..] => Some(GitAction::Log),
        ["branch", ..] => Some(GitAction::Branch),
        ["switch", ..] => Some(GitAction::Switch),
        ["checkout", ..] => Some(GitAction::Checkout),
        ["commit", ..] => Some(GitAction::Commit),
        ["fetch", ..] => Some(GitAction::Fetch),
        ["pull", ..] => Some(GitAction::Pull),
        ["push", ..] => Some(GitAction::Push),
        ["merge", ..] => Some(GitAction::Merge),
        ["rebase", ..] => Some(GitAction::Rebase),
        ["show", ..] => Some(GitAction::Show),
        ["tag", ..] => Some(GitAction::Tag),
        ["remote", ..] => Some(GitAction::Remote),
        ["rev", "parse", ..]
        | ["rev-parse", ..]
        | ["rev_parse", ..]
        | ["config", ..]
        | ["describe", ..]
        | ["symbolic", "ref", ..]
        | ["symbolic-ref", ..]
        | ["symbolic_ref", ..]
        | ["ls", "files", ..]
        | ["ls-files", ..]
        | ["ls_files", ..]
        | ["ls", "tree", ..]
        | ["ls-tree", ..]
        | ["ls_tree", ..] => Some(GitAction::RepositoryInfo),
        _ => None,
    }
}

fn git_invocation_detail(action: GitAction, arguments: &[String]) -> String {
    let mut details = Vec::new();
    let skip = if matches!(
        action,
        GitAction::WorktreeAdd
            | GitAction::WorktreeList
            | GitAction::WorktreeRemove
            | GitAction::WorktreePrune
            | GitAction::WorktreeLock
            | GitAction::WorktreeUnlock
    ) {
        2
    } else {
        1
    };
    let mut arguments = arguments.iter().skip(skip);
    let mut after_separator = false;
    while let Some(argument) = arguments.next() {
        if argument == "--" {
            after_separator = true;
            continue;
        }
        if !after_separator && argument.starts_with('-') {
            if git_option_takes_value(action, argument)
                && let Some(value) = arguments.next()
                && git_option_value_is_detail(action, argument)
            {
                details.push(value.clone());
            }
            continue;
        }
        details.push(argument.clone());
    }
    compact_text(&details.join(" · "), 160)
}

fn git_option_takes_value(action: GitAction, option: &str) -> bool {
    match option {
        "-b" => matches!(
            action,
            GitAction::WorktreeAdd | GitAction::Branch | GitAction::Switch | GitAction::Checkout
        ),
        "-m" | "--message" => matches!(action, GitAction::Commit),
        "-n" | "--max-count" => matches!(action, GitAction::Log | GitAction::Show),
        "-S" | "-L" | "--grep" | "--since" | "--until" => {
            matches!(action, GitAction::Log | GitAction::Diff | GitAction::Show)
        }
        "--author" | "--committer" | "--date" | "--format" | "--pretty" => {
            matches!(action, GitAction::Log | GitAction::Show)
        }
        "--output" | "--upload-pack" | "--whence" => true,
        _ => false,
    }
}

fn git_option_value_is_detail(action: GitAction, option: &str) -> bool {
    matches!(
        (action, option),
        (
            GitAction::WorktreeAdd | GitAction::Switch | GitAction::Checkout,
            "-b"
        ) | (GitAction::Commit, "-m" | "--message")
    )
}

fn repeated_git_label(action: GitAction) -> String {
    match action {
        GitAction::Status => "Inspect working tree",
        GitAction::Diff => "Compare revisions",
        GitAction::Log => "Review history",
        GitAction::Branch => "Inspect branches",
        GitAction::Show => "Show revisions",
        GitAction::Tag => "Inspect tags",
        GitAction::Remote => "Inspect remotes",
        GitAction::RepositoryInfo => "Inspect repository",
        GitAction::WorktreeList => "Inspect worktrees",
        _ => "Run Git operations",
    }
    .to_string()
}

fn git_action_is_read_only(action: GitAction) -> bool {
    matches!(
        action,
        GitAction::WorktreeList
            | GitAction::Status
            | GitAction::Diff
            | GitAction::Log
            | GitAction::Branch
            | GitAction::Show
            | GitAction::Tag
            | GitAction::Remote
            | GitAction::RepositoryInfo
    )
}

fn git_action_name(action: GitAction) -> &'static str {
    match action {
        GitAction::WorktreeAdd => "add worktree",
        GitAction::WorktreeList => "list worktrees",
        GitAction::WorktreeRemove => "remove worktree",
        GitAction::WorktreePrune => "prune worktrees",
        GitAction::WorktreeLock => "lock worktree",
        GitAction::WorktreeUnlock => "unlock worktree",
        GitAction::Add => "add",
        GitAction::Status => "status",
        GitAction::Diff => "diff",
        GitAction::Log => "log",
        GitAction::Branch => "branch",
        GitAction::Switch => "switch",
        GitAction::Checkout => "checkout",
        GitAction::Commit => "commit",
        GitAction::Fetch => "fetch",
        GitAction::Pull => "pull",
        GitAction::Push => "push",
        GitAction::Merge => "merge",
        GitAction::Rebase => "rebase",
        GitAction::Show => "show",
        GitAction::Tag => "tag",
        GitAction::Remote => "remote",
        GitAction::RepositoryInfo => "repository info",
    }
}

fn git_tool_detail(_action: GitAction, input: &Value) -> String {
    [
        "path", "worktree", "branch", "ref", "revision", "remote", "name", "message",
    ]
    .into_iter()
    .filter_map(|key| string_field(input, key))
    .filter(|value| !value.trim().is_empty())
    .take(2)
    .map(str::to_string)
    .collect::<Vec<_>>()
    .join(" · ")
}

fn concise_git_result(output: &str) -> Option<String> {
    let lines = output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    match lines.as_slice() {
        [] => None,
        [line] if line.chars().count() < 80 => Some((*line).to_string()),
        _ => Some(format!("{} lines", lines.len())),
    }
}

fn readable_result_text(value: &str) -> String {
    let Ok(Value::Object(fields)) = serde_json::from_str::<Value>(value) else {
        return value.to_string();
    };
    if let Some(structured) = fields
        .get("structuredContent")
        .filter(|structured| !structured.is_null())
    {
        return serde_json::to_string_pretty(structured).unwrap_or_else(|_| value.to_string());
    }
    if let Some(Value::Array(content)) = fields.get("content") {
        let text = content
            .iter()
            .filter_map(|item| item.get("text").and_then(Value::as_str))
            .filter(|text| !text.trim().is_empty())
            .collect::<Vec<_>>()
            .join("\n");
        if !text.is_empty() {
            return text;
        }
    }
    value.to_string()
}

fn coverage_summary(value: &str) -> Option<String> {
    let marker = value.to_ascii_lowercase();
    let matched = marker.find("matched:")?;
    let rest = marker[matched + "matched:".len()..].trim_start();
    let (found, rest) = rest.split_once('/')?;
    let found = found.trim().parse::<usize>().ok()?;
    let total = rest.split_whitespace().next()?.parse::<usize>().ok()?;
    Some(format!("{found}/{total} files covered"))
}

fn is_internal_tool(name: &str) -> bool {
    matches!(
        tool_leaf_name(name).replace(['-', '_'], "").as_str(),
        "contextcompaction"
            | "enteredreviewmode"
            | "exitedreviewmode"
            | "hookprompt"
            | "usermessage"
            | "supportrunsuggestion"
            | "suggestrun"
    )
}

fn format_mcp_tool_name(name: &str) -> String {
    let mut parts = name.splitn(3, "__");
    let _ = parts.next();
    let server = parts.next().unwrap_or("MCP");
    let action = parts.next().unwrap_or(name);
    let (service, action) = [
        "gmail", "outlook", "drive", "calendar", "sheets", "docs", "slack", "notion", "teams",
    ]
    .into_iter()
    .find_map(|service| {
        action
            .strip_prefix(&format!("{service}_"))
            .map(|action| (service, action))
    })
    .unwrap_or((server, action));
    let service = service.replace(['_', '-'], " ");
    let action = match action {
        "list_mcp_resources" => "List MCP resources".to_string(),
        "list_mcp_resource_templates" => "List MCP resource templates".to_string(),
        _ => action.replace(['_', '-'], " "),
    };
    format!("{} · {}", title_case(&service), action)
}

fn mcp_resource_error_detail(output: &str) -> String {
    let first_line = output
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or("MCP resource lookup failed")
        .trim();
    let normalized = first_line.to_ascii_lowercase();
    if normalized.contains("not ready") {
        return "MCP server not ready".to_string();
    }
    if normalized.contains("unsupported method") {
        return "MCP resources unavailable".to_string();
    }
    compact_text(first_line, 100)
}

fn high_signal_detail(input: &Value) -> Option<String> {
    let Value::Object(fields) = input else {
        return input.as_str().map(|value| compact_text(value, 200));
    };
    [
        "description",
        "recipient_name",
        "title",
        "query",
        "q",
        "url",
        "pattern",
        "path",
        "file_path",
        "filename",
        "file_name",
        "attachment_filename",
        "document_id",
        "name",
        "subject",
        "prompt",
    ]
    .into_iter()
    .find_map(|key| {
        fields
            .get(key)
            .filter(|value| !is_noise_param(key, value))
            .map(format_value)
            .filter(|value| !value.is_empty())
    })
}

fn is_noise_param(key: &str, value: &Value) -> bool {
    let normalized = key.to_ascii_lowercase();
    if matches!(
        normalized.as_str(),
        "id" | "ids"
            | "uuid"
            | "token"
            | "cursor"
            | "etag"
            | "hash"
            | "checksum"
            | "connection"
            | "limit"
            | "offset"
            | "page"
            | "page_size"
            | "max_results"
            | "raw"
            | "payload"
            | "metadata"
            | "message_id"
            | "attachment_id"
    ) || normalized.ends_with("_token")
        || normalized.ends_with("_cursor")
    {
        return true;
    }
    value.as_str().is_some_and(|value| {
        uuid::Uuid::parse_str(value.trim()).is_ok()
            || (value.len() >= 12 && value.chars().all(|character| character.is_ascii_hexdigit()))
    })
}

fn format_value(value: &Value) -> String {
    match value {
        Value::String(value) => compact_text(value, 200),
        Value::Number(_) | Value::Bool(_) => value.to_string(),
        Value::Array(items) => items
            .iter()
            .map(format_value)
            .filter(|value| !value.is_empty())
            .take(4)
            .collect::<Vec<_>>()
            .join(", "),
        Value::Object(fields) => fields
            .iter()
            .filter_map(|(key, value)| {
                let value = format_value(value);
                (!value.is_empty()).then(|| format!("{key}: {value}"))
            })
            .take(2)
            .collect::<Vec<_>>()
            .join(" · "),
        Value::Null => String::new(),
    }
}

fn humanize_key(value: &str) -> String {
    title_case(&value.replace(['_', '-'], " "))
}

fn title_case(value: &str) -> String {
    let mut value = value.to_string();
    if let Some(first) = value.get_mut(..1) {
        first.make_ascii_uppercase();
    }
    value
}

fn tool_leaf_name(name: &str) -> String {
    name.rsplit("__")
        .next()
        .unwrap_or(name)
        .rsplit('.')
        .next()
        .unwrap_or(name)
        .to_ascii_lowercase()
}

fn string_field<'a>(input: &'a Value, key: &str) -> Option<&'a str> {
    input.get(key).and_then(Value::as_str)
}

fn source_location(input: &Value) -> String {
    let path = input_path(input).unwrap_or("source");
    match (
        input.get("line").and_then(Value::as_u64),
        input.get("character").and_then(Value::as_u64),
    ) {
        (Some(line), Some(character)) => format!("{path}:{line}:{character}"),
        _ => path.to_string(),
    }
}

fn read_tool_detail(input: &Value) -> Option<String> {
    let path = input_path(input)?;
    let range = read_line_range(input).map(|(start, end)| format!(":{start}-{end}"));
    Some(format!("{path}{}", range.unwrap_or_default()))
}

fn read_output_detail(input: &Value, output: &str) -> Option<String> {
    let value = serde_json::from_str::<Value>(output).ok()?;
    let path = input_path(input)
        .or_else(|| value.get("path").and_then(Value::as_str))
        .or_else(|| value.get("file_path").and_then(Value::as_str))?;
    let range = read_line_range(&value)?;
    Some(format!("{path}:{}-{}", range.0, range.1))
}

fn read_line_range(input: &Value) -> Option<(u64, u64)> {
    let start = first_number(
        input,
        &[
            "offset_line",
            "start_line",
            "line_start",
            "offsetLine",
            "startLine",
            "line",
        ],
    );
    let end = first_number(input, &["end_line", "line_end", "endLine"]);
    let limit = first_number(
        input,
        &["limit_lines", "line_count", "num_lines", "limitLines"],
    );
    match (start, end, limit) {
        (Some(start), Some(end), _) => Some((start, end)),
        (Some(start), None, Some(limit)) if limit > 0 => {
            Some((start, start.saturating_add(limit - 1)))
        }
        (None, Some(end), _) => Some((1, end)),
        (None, None, Some(limit)) if limit > 0 => Some((1, limit)),
        _ => None,
    }
}

fn first_number(input: &Value, keys: &[&str]) -> Option<u64> {
    keys.iter()
        .find_map(|key| input.get(key).and_then(Value::as_u64))
}

fn format_duration(milliseconds: u64) -> String {
    if milliseconds >= 1_000 && milliseconds.is_multiple_of(1_000) {
        format!("{}s", milliseconds / 1_000)
    } else {
        format!("{milliseconds}ms")
    }
}

fn command_from_input(input: &Value) -> Option<&str> {
    ["cmd", "command", "script"]
        .iter()
        .find_map(|key| input.get(key).and_then(Value::as_str))
}

fn input_path(input: &Value) -> Option<&str> {
    ["path", "file_path", "filepath", "filename"]
        .iter()
        .find_map(|key| input.get(key).and_then(Value::as_str))
}

fn edit_detail(input: &Value) -> Option<String> {
    input_path(input)
        .map(str::to_string)
        .or_else(|| edit_source(input).and_then(patch_path))
        .or_else(|| {
            let mut paths = Vec::new();
            collect_edit_paths(input, &mut paths);
            summarize_edit_paths(&paths)
        })
}

fn collect_edit_paths<'a>(input: &'a Value, paths: &mut Vec<&'a str>) {
    match input {
        Value::Array(items) => {
            for item in items {
                collect_edit_paths(item, paths);
            }
        }
        Value::Object(fields) => {
            if let Some(path) = ["path", "file_path", "filepath", "filename"]
                .iter()
                .find_map(|key| fields.get(*key).and_then(Value::as_str))
                && !paths.contains(&path)
            {
                paths.push(path);
            }
            for key in ["input", "changes"] {
                if let Some(value) = fields.get(key) {
                    collect_edit_paths(value, paths);
                }
            }
        }
        _ => {}
    }
}

fn summarize_edit_paths(paths: &[&str]) -> Option<String> {
    let display = |path: &str| {
        let path = Path::new(path);
        if path.is_absolute() {
            path.file_name()
                .unwrap_or(path.as_os_str())
                .to_string_lossy()
                .into_owned()
        } else {
            path.to_string_lossy().into_owned()
        }
    };
    match paths {
        [] => None,
        [path] => Some(display(path)),
        [first, second] => Some(format!("{} + {}", display(first), display(second))),
        [first, rest @ ..] => Some(format!("{} + {} more", display(first), rest.len())),
    }
}

fn search_query(command: &str) -> Option<String> {
    let words = shell_words(command);
    if let Some(script) = shell_script(&words) {
        return search_query(script);
    }
    let search_index = words
        .iter()
        .position(|word| matches!(word.rsplit('/').next(), Some("rg" | "grep")))?;
    let mut words = words.into_iter().skip(search_index + 1);
    while let Some(word) = words.next() {
        if word == "-e" || word == "--regexp" {
            return words.next();
        }
        if let Some(query) = word
            .strip_prefix("-e")
            .filter(|query| !query.is_empty())
            .or_else(|| word.strip_prefix("--regexp="))
        {
            return Some(query.to_string());
        }
        if search_option_takes_value(&word) {
            let _ = words.next();
            continue;
        }
        if word == "--" {
            return words.next();
        }
        if !word.starts_with('-') {
            return Some(word);
        }
    }
    None
}

fn shell_script(words: &[String]) -> Option<&str> {
    let shell = words.first()?.rsplit('/').next()?;
    matches!(shell, "bash" | "sh" | "zsh" | "dash")
        .then_some(())
        .and_then(|()| {
            words
                .windows(2)
                .find(|pair| {
                    pair[0] == "-c"
                        || (pair[0].starts_with('-')
                            && !pair[0].starts_with("--")
                            && pair[0][1..].contains('c'))
                })
                .map(|pair| pair[1].as_str())
        })
}

/// Classify only shell commands whose primary effect is reading data and
/// whose syntax does not compose additional commands or redirections. This is
/// intentionally conservative: a command that is not obviously a read stays
/// an execution so the UI never hides a potentially mutating operation.
fn command_is_read_only(command: &str) -> bool {
    let command = unwrapped_shell_command(command);
    if command_has_shell_control_operator(&command) {
        return false;
    }
    let words = shell_words(&command);
    let Some(executable) = words.first().and_then(|word| word.rsplit('/').next()) else {
        return false;
    };
    let arguments = &words[1..];
    match executable {
        "cat" | "head" | "tail" | "cut" | "tr" | "sort" | "uniq" | "wc" | "od" | "xxd" | "file"
        | "ls" | "pwd" | "stat" | "du" => true,
        "sed" => !arguments.iter().any(|argument| {
            argument == "--in-place"
                || argument == "-i"
                || argument.starts_with("--in-place=")
                || argument.starts_with("-i")
        }),
        "awk" | "gawk" => !command.contains("system(") && !command.contains("system ("),
        "find" => !arguments.iter().any(|argument| {
            matches!(
                argument.as_str(),
                "-delete" | "-exec" | "-execdir" | "-ok" | "-okdir"
            )
        }),
        "rg" | "grep" => true,
        _ => false,
    }
}

fn command_has_shell_control_operator(command: &str) -> bool {
    let mut quote = None;
    let mut escaped = false;
    for character in command.chars() {
        if escaped {
            escaped = false;
            continue;
        }
        if character == '\\' && quote != Some('\'') {
            escaped = true;
            continue;
        }
        if quote == Some(character) {
            quote = None;
            continue;
        }
        if quote.is_none() && matches!(character, '\'' | '"') {
            quote = Some(character);
            continue;
        }
        if quote.is_none() && matches!(character, ';' | '|' | '>' | '<' | '&') {
            return true;
        }
    }
    escaped || quote.is_some()
}

fn unwrapped_shell_command(command: &str) -> String {
    let words = shell_words(command);
    shell_script(&words).unwrap_or(command).to_string()
}

fn search_option_takes_value(option: &str) -> bool {
    matches!(
        option,
        "-A" | "-B"
            | "-C"
            | "-f"
            | "-g"
            | "-j"
            | "-m"
            | "-t"
            | "--after-context"
            | "--before-context"
            | "--context"
            | "--exclude"
            | "--file"
            | "--glob"
            | "--iglob"
            | "--include"
            | "--max-count"
            | "--type"
    )
}

fn shell_words(command: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut escaped = false;
    let mut characters = command.chars().peekable();
    while let Some(character) = characters.next() {
        if escaped {
            current.push(character);
            escaped = false;
        } else if character == '\\' && quote != Some('\'') {
            escaped = true;
        } else if quote == Some(character) {
            quote = None;
        } else if quote.is_none() && matches!(character, '\'' | '"') {
            quote = Some(character);
        } else if quote.is_none() && matches!(character, ';' | '|' | '&' | '<' | '>') {
            if !current.is_empty() {
                words.push(std::mem::take(&mut current));
            }
            let mut operator = character.to_string();
            if characters.peek() == Some(&character) && matches!(character, '|' | '&' | '<' | '>') {
                operator.push(characters.next().expect("peeked shell operator"));
            }
            words.push(operator);
        } else if quote.is_none() && character.is_whitespace() {
            if !current.is_empty() {
                words.push(std::mem::take(&mut current));
            }
        } else {
            current.push(character);
        }
    }
    if escaped {
        current.push('\\');
    }
    if !current.is_empty() {
        words.push(current);
    }
    words
}

fn is_shell_operator(word: &str) -> bool {
    matches!(
        word,
        ";" | "&&" | "||" | "|" | "&" | "<" | ">" | "<<" | ">>"
    )
}

fn humanize_tool_name(name: &str) -> String {
    let leaf = tool_leaf_name(name);
    let words = leaf
        .split(['_', '-'])
        .filter(|word| !word.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    if words.is_empty() {
        "Tool".to_string()
    } else {
        title_case(&words)
    }
}

fn concise_tool_input(input: &Value) -> String {
    match input {
        Value::Object(fields) if fields.is_empty() => "no arguments".to_string(),
        Value::Object(fields) => {
            let values = fields
                .iter()
                .take(3)
                .map(|(key, value)| match value {
                    Value::String(value) => {
                        format!("{key}: {}", compact_text(value, 60))
                    }
                    Value::Array(items) => format!("{key}: {} items", items.len()),
                    Value::Object(items) => format!("{key}: {} fields", items.len()),
                    value => format!("{key}: {value}"),
                })
                .collect::<Vec<_>>()
                .join(" · ");
            compact_text(&values, 160)
        }
        Value::String(value) => compact_text(value, 160),
        Value::Array(items) => format!("{} items", items.len()),
        Value::Null => "no arguments".to_string(),
        value => compact_text(&value.to_string(), 160),
    }
}

fn patch_path(source: &str) -> Option<String> {
    source.lines().find_map(|line| {
        ["*** Update File: ", "*** Add File: ", "*** Delete File: "]
            .iter()
            .find_map(|prefix| line.strip_prefix(prefix).map(str::to_string))
    })
}

fn edit_source(value: &Value) -> Option<&str> {
    match value {
        Value::String(source) => looks_like_patch(source).then_some(source),
        Value::Array(items) => items.iter().find_map(edit_source),
        Value::Object(fields) => fields
            .get("diff")
            .or_else(|| fields.get("patch"))
            .and_then(Value::as_str)
            .or_else(|| fields.get("input").and_then(edit_source))
            .or_else(|| fields.get("changes").and_then(edit_source)),
        _ => None,
    }
}

/// Claude's native Edit tool carries a replacement pair instead of a patch.
/// Keep the input useful while the tool is running by projecting that pair
/// into the same diff body used by patch-based edit tools.
fn claude_edit_diff(value: &Value) -> Option<(String, String)> {
    match value {
        Value::Array(items) => items.iter().find_map(claude_edit_diff),
        Value::Object(fields) => {
            let path = ["file_path", "path", "filename"]
                .into_iter()
                .find_map(|key| fields.get(key).and_then(Value::as_str));
            let old = fields
                .get("old_string")
                .or_else(|| fields.get("old_text"))
                .and_then(Value::as_str);
            let new = fields
                .get("new_string")
                .or_else(|| fields.get("new_text"))
                .and_then(Value::as_str);
            if let (Some(path), Some(old), Some(new)) = (path, old, new)
                && old != new
            {
                return Some((path.to_string(), replacement_diff(old, new, path)));
            }
            fields
                .get("input")
                .and_then(claude_edit_diff)
                .or_else(|| fields.get("changes").and_then(claude_edit_diff))
        }
        _ => None,
    }
}

fn replacement_diff(old: &str, new: &str, path: &str) -> String {
    let mut lines = vec![format!("--- {path}"), format!("+++ {path}")];
    lines.extend(prefixed_diff_lines('-', old));
    lines.extend(prefixed_diff_lines('+', new));
    lines.join("\n")
}

fn prefixed_diff_lines(prefix: char, source: &str) -> Vec<String> {
    source
        .split_inclusive('\n')
        .map(|line| format!("{prefix}{}", line.trim_end_matches('\n')))
        .collect()
}

fn edit_path(value: &Value) -> Option<String> {
    match value {
        Value::Array(items) => items.iter().find_map(edit_path),
        Value::Object(fields) => ["path", "file_path", "filename"]
            .into_iter()
            .find_map(|key| fields.get(key).and_then(Value::as_str))
            .map(str::to_string)
            .or_else(|| fields.get("input").and_then(edit_path))
            .or_else(|| fields.get("changes").and_then(edit_path)),
        _ => None,
    }
}

fn looks_like_patch(source: &str) -> bool {
    patch_source(source).is_some()
}

fn patch_source(source: &str) -> Option<&str> {
    let mut offset = 0;
    for line in source.split_inclusive('\n') {
        let line = line.trim_end_matches('\n');
        if line == "*** Begin Patch"
            || line.starts_with("@@")
            || line.starts_with("diff --git ")
            || line.starts_with("--- ")
            || line.starts_with("+++ ")
        {
            return Some(&source[offset..]);
        }
        offset +=
            line.len() + usize::from(source.as_bytes().get(offset + line.len()) == Some(&b'\n'));
    }
    None
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn projects_cli_command_search_and_diff_contracts() {
        let search = project_tool_presentation(
            "functions.exec_command",
            &json!({"cmd": "/usr/bin/bash -c \"rg -n 'tool.?call' crates/borg-cli\""}),
            None,
            false,
        );
        assert_eq!(search.label, "Search");
        assert_eq!(search.detail, "“tool.?call”");

        let edit = project_tool_presentation(
            "functions.apply_patch",
            &json!("*** Begin Patch\n*** Update File: src/main.rs\n@@\n-old\n+new"),
            Some(r#"[{"diff":"@@ -1 +1 @@\n-old\n+new","path":"src/main.rs"}]"#),
            false,
        );
        assert_eq!(edit.label, "Edit");
        assert_eq!(edit.detail, "src/main.rs");
        assert_eq!(
            edit.input.as_ref().map(|body| body.language.as_str()),
            Some("diff:rs")
        );
        assert_eq!(edit.output, None);
    }

    #[test]
    fn presents_image_tools_with_action_labels_across_provider_name_variants() {
        for name in ["view_image", "imageview"] {
            let presentation = project_tool_presentation(name, &json!({}), None, false);
            assert_eq!(presentation.label, "View image", "{name}");
            assert_eq!(presentation.category, ToolPresentationCategory::Image);
        }

        for name in ["image_generation", "imagegeneration"] {
            let presentation = project_tool_presentation(name, &json!({}), None, false);
            assert_eq!(presentation.label, "Generate image", "{name}");
            assert_eq!(presentation.category, ToolPresentationCategory::Image);
        }
    }

    #[test]
    fn classifies_instant_and_long_running_action_labels() {
        for label in [
            "Read",
            "Read plan",
            "Inspect repository",
            "Workspace diagnostics",
            "Create goal",
            "View image",
        ] {
            assert!(tool_action_is_instant(label, None), "{label}");
        }
        for label in [
            "Run",
            "Thinking",
            "Search",
            "Search web",
            "Edit",
            "Update plan",
            "Generate image",
        ] {
            assert!(!tool_action_is_instant(label, None), "{label}");
        }
        assert!(!tool_action_is_instant("Thinking", Some("reasoning")));
    }

    #[test]
    fn presents_tool_search_and_read_line_ranges_compactly() {
        let tools = tool_call_summary(
            "toolsearch",
            &json!({"query": "select:mcp__borg_agent__get_goal"}),
        );
        assert_eq!(
            tools,
            (
                "Search Tools".to_string(),
                "select:mcp__borg_agent__get_goal".to_string()
            )
        );

        let read = tool_call_summary(
            "mcp__borg__read_file",
            &json!({
                "path": "docs/guide.md",
                "offset_line": 12,
                "limit_lines": 8
            }),
        );
        assert_eq!(
            read,
            ("Read".to_string(), "docs/guide.md:12-19".to_string())
        );

        let completed = project_tool_presentation(
            "mcp__borg__read_file",
            &json!({"path": "docs/guide.md"}),
            Some(r#"{"path":"docs/guide.md","start_line":12,"end_line":19}"#),
            false,
        );
        assert_eq!(completed.detail, "docs/guide.md:12-19");
    }

    #[test]
    fn does_not_present_unmarked_file_contents_as_a_diff() {
        let edit = project_tool_presentation(
            "Edit",
            &json!({
                "file_path": "src/main.rs",
                "diff": "fn main() {\n    println!(\"not a diff\");\n}\n"
            }),
            None,
            false,
        );

        assert_eq!(
            edit.input.as_ref().map(|body| body.language.as_str()),
            Some("json")
        );
    }

    #[test]
    fn presents_claude_edit_replacements_as_a_diff() {
        let edit = project_tool_presentation(
            "Edit",
            &json!({
                "replace_all": false,
                "file_path": "docs/review.md",
                "old_string": "old line\n",
                "new_string": "new line\n"
            }),
            None,
            false,
        );

        assert_eq!(
            edit.input.as_ref().map(|body| body.language.as_str()),
            Some("diff:md")
        );
        assert_eq!(
            edit.input.as_ref().map(|body| body.text.as_str()),
            Some("--- docs/review.md\n+++ docs/review.md\n-old line\n+new line")
        );
    }

    #[test]
    fn names_files_in_native_multi_edit_envelopes() {
        let edit = project_tool_presentation(
            "Edit",
            &json!({
                "changes": [
                    {
                        "path": "/home/user/project/src/packed_oblivious_moe_frontier.rs",
                        "kind": {"type": "update"},
                        "diff": "@@ -1 +1 @@\n-old\n+new"
                    },
                    {
                        "path": "/home/user/project/src/tests.rs",
                        "kind": {"type": "update"},
                        "diff": "@@ -1 +1 @@\n-old test\n+new test"
                    }
                ]
            }),
            Some(""),
            false,
        );

        assert_eq!(edit.detail, "packed_oblivious_moe_frontier.rs + tests.rs");
        let input = edit.input.expect("native edit diff");
        assert_eq!(input.language, "diff");
        assert!(
            input.text.contains(
                "*** Update File: /home/user/project/src/packed_oblivious_moe_frontier.rs"
            )
        );
        assert!(
            input
                .text
                .contains("*** Update File: /home/user/project/src/tests.rs")
        );
        assert!(input.text.contains("-old\n+new"));
        assert!(input.text.contains("-old test\n+new test"));
    }

    #[test]
    fn presents_native_edit_file_replacements_as_a_diff() {
        let edit = project_tool_presentation(
            "edit_file",
            &json!({
                "path": "src/main.rs",
                "old_text": "old line\n",
                "new_text": "new line\n"
            }),
            None,
            false,
        );

        assert_eq!(edit.category, ToolPresentationCategory::Edit);
        assert_eq!(
            edit.input.as_ref().map(|body| body.language.as_str()),
            Some("diff:rs")
        );
        assert_eq!(
            edit.input.as_ref().map(|body| body.text.as_str()),
            Some("--- src/main.rs\n+++ src/main.rs\n-old line\n+new line")
        );
    }

    #[test]
    fn presents_native_new_file_writes_as_addition_diffs() {
        let write = project_tool_presentation(
            "write_file",
            &json!({
                "path": "src/new.rs",
                "content": "fn main() {}\n"
            }),
            None,
            false,
        );

        assert_eq!(write.category, ToolPresentationCategory::Edit);
        assert_eq!(
            write.input.as_ref().map(|body| body.language.as_str()),
            Some("diff:rs")
        );
        assert!(
            write
                .input
                .as_ref()
                .is_some_and(|body| body.text.contains("+fn main() {}"))
        );
    }

    #[test]
    fn projects_completed_file_creation_from_edit_result() {
        let edit = project_tool_presentation(
            "Edit",
            &json!({
                "diff": null,
                "file_path": "src/new.rs",
                "paths": ["src/new.rs"]
            }),
            Some(r#"[{"diff":"fn main() {}\n","kind":{"type":"add"},"path":"src/new.rs"}]"#),
            false,
        );

        assert_eq!(edit.label, "Create file");
        assert_eq!(edit.detail, "src/new.rs");
        let output = edit.output.expect("created file diff");
        assert_eq!(output.language, "diff:rs");
        assert_eq!(
            output.text,
            "--- /dev/null\n+++ src/new.rs\n@@ -0,0 +1,1 @@\n+fn main() {}"
        );
    }

    #[test]
    fn projects_wrapped_file_change_envelope_as_diff() {
        let edit = project_tool_presentation(
            "mcp__borg_agent__write_file",
            &json!({}),
            Some(
                r##"{
                    "changes": [{
                        "path": "tmp/runtime-modularity-design.md",
                        "kind": {"type": "add"},
                        "diff": "# Borg Modular Runtime Design\n\nStatus: design discussion"
                    }]
                }"##,
            ),
            false,
        );

        assert_eq!(edit.label, "Create file");
        assert_eq!(edit.detail, "tmp/runtime-modularity-design.md");
        let output = edit.output.expect("wrapped file-change diff");
        assert_eq!(output.language, "diff:md");
        assert!(output.text.contains("+# Borg Modular Runtime Design"));
    }

    #[test]
    fn strips_unmarked_file_contents_before_the_first_real_hunk() {
        let edit = project_tool_presentation(
            "Edit",
            &json!({
                "file_path": "src/main.rs",
                "diff": "fn unrelated() {}\nmore unchanged code\n@@ -8 +8 @@\n-old\n+new"
            }),
            None,
            false,
        );

        assert_eq!(
            edit.input.as_ref().map(|body| body.text.as_str()),
            Some("@@ -8 +8 @@\n-old\n+new")
        );
    }

    #[test]
    fn folds_product_specific_context_into_the_shared_projection() {
        let gmail = project_tool_presentation(
            "mcp__borg__gmail_read_attachment",
            &json!({
                "filename": "Employment Contract.pdf",
                "message_id": "opaque",
                "attachment_id": "opaque"
            }),
            Some("contract text"),
            false,
        );
        assert_eq!(gmail.label, "Read Gmail attachment");
        assert_eq!(gmail.detail, "Employment Contract.pdf");
        assert_eq!(gmail.category, ToolPresentationCategory::Read);
    }

    #[test]
    fn projects_provider_subagent_activity_without_raw_identifiers() {
        let activity = project_tool_presentation(
            "SubagentActivity",
            &json!({
                "agentThreadId": "019fbb79-33be-7592-9982-f79f9f20f1fc",
                "agentPath": "/root/capability_architecture_rank",
                "kind": "interrupted"
            }),
            None,
            false,
        );

        assert_eq!(activity.label, "Agent interrupted");
        assert_eq!(activity.detail, "capability_architecture_rank");
        assert_eq!(activity.category, ToolPresentationCategory::Agent);
    }

    #[test]
    fn humanizes_underscored_mcp_server_names() {
        let decision = project_tool_presentation(
            "mcp__borg_agent__record_workspace_decision",
            &json!({"decision":"opaque IO close state on host failure"}),
            None,
            false,
        );

        assert_eq!(decision.label, "Borg agent · record workspace decision");
        assert!(!decision.label.contains('_'));
    }

    #[test]
    fn resource_probes_are_compact_and_hide_transport_payloads() {
        let probe = project_tool_presentation(
            "mcp__borg_agent__list_mcp_resources",
            &json!({"server": "borg_agent"}),
            Some("resources/list failed: MCP server 'borg_agent' was not ready for this step"),
            true,
        );

        assert_eq!(probe.label, "Borg agent · List MCP resources");
        assert_eq!(probe.detail, "MCP server not ready");
        assert_eq!(probe.category, ToolPresentationCategory::Read);
        assert_eq!(probe.input, None);
        assert_eq!(probe.output, None);
        assert_eq!(probe.result.as_deref(), Some("MCP server not ready"));
        assert_eq!(
            tool_output_code_view("mcp__borg_agent__list_mcp_resources", r#"{"resources":[]}"#),
            None
        );
    }

    #[test]
    fn empty_resource_probe_results_have_a_small_summary() {
        let probe = project_tool_presentation(
            "mcp__borg_agent__list_mcp_resource_templates",
            &json!({"server": "borg_agent"}),
            Some(r#"{"resourceTemplates":[]}"#),
            false,
        );

        assert_eq!(probe.label, "Borg agent · List MCP resource templates");
        assert_eq!(probe.detail, "");
        assert_eq!(probe.result.as_deref(), Some("0 resource templates"));
    }

    #[test]
    fn preserves_real_tool_errors_without_rendering_null_as_a_result() {
        let real_error = project_tool_presentation(
            "mcp__borg_agent__update_goal",
            &json!({"status": "blocked"}),
            Some("goal update was rejected"),
            true,
        );
        assert_eq!(
            real_error.result.as_deref(),
            Some("goal update was rejected")
        );

        let missing_error = project_tool_presentation(
            "mcp__borg_agent__update_goal",
            &json!({"status": "blocked"}),
            Some("null"),
            true,
        );
        assert_eq!(missing_error.result, None);
    }

    #[test]
    fn goal_controls_hide_empty_json_input_and_use_the_compact_result() {
        let presentation =
            project_tool_presentation("mcp__borg_agent__get_goal", &json!({}), None, false);
        assert_eq!(presentation.label, "Read goal");
        assert_eq!(presentation.detail, "");
        assert_eq!(presentation.category, ToolPresentationCategory::Goal);
        assert_eq!(presentation.input, None);
    }

    #[test]
    fn presents_model_consultation_as_an_agent_action() {
        let presentation = project_tool_presentation(
            "mcp__borg_agent__consult_model",
            &json!({"profile": "claude", "prompt": "Review the tradeoffs."}),
            Some(r#"{"provider":"claude","response":"Use the narrower interface."}"#),
            false,
        );
        assert_eq!(presentation.label, "Consult model");
        assert_eq!(presentation.detail, "claude · second opinion");
        assert_eq!(presentation.category, ToolPresentationCategory::Agent);
    }

    #[test]
    fn presents_persistent_peer_consultation_without_exposing_raw_thread_ids() {
        let presentation = project_tool_presentation(
            "mcp__borg_agent__consult_peer",
            &json!({"prompt": "Review the tradeoffs."}),
            Some(
                r#"{"persistent":true,"provider":"claude","thread":"/root/claude","response":"Use the narrower interface."}"#,
            ),
            false,
        );
        assert_eq!(presentation.label, "Consult peer");
        assert_eq!(presentation.detail, "opposite provider · persistent peer");
        assert_eq!(presentation.category, ToolPresentationCategory::Agent);
        assert!(!presentation.detail.contains("/root/claude"));
    }

    #[test]
    fn generic_code_view_unwraps_structured_mcp_results() {
        let output = json!({
            "_meta": null,
            "content": [{
                "type": "text",
                "text": "[{\"message_id\":\"message-1\",\"text\":\"hello\"}]"
            }],
            "structuredContent": [{
                "message_id": "message-1",
                "text": "hello"
            }]
        })
        .to_string();

        let (language, rendered) =
            tool_output_code_view("mcp__example__messages", &output).expect("tool output");

        assert_eq!(language, "json");
        assert_eq!(
            serde_json::from_str::<Value>(&rendered).unwrap(),
            json!([{"message_id": "message-1", "text": "hello"}])
        );
        assert!(!rendered.contains("structuredContent"));
        assert!(!rendered.contains("_meta"));
    }

    #[test]
    fn presents_common_git_shell_commands_with_raw_commands_intact() {
        let cases = [
            ("git status --short", "Git status", ""),
            (
                "git add src/lib.rs src/main.rs",
                "Git add",
                "src/lib.rs · src/main.rs",
            ),
            (
                "git worktree add -b topic ../topic main",
                "Add worktree",
                "topic · ../topic · main",
            ),
            (
                "bash -lc 'git worktree list --porcelain'",
                "List worktrees",
                "",
            ),
            ("git -C /repo diff --stat HEAD~1", "Git diff", "HEAD~1"),
            ("cd /repo && git switch feature", "Switch branch", "feature"),
            ("git rev-parse --show-toplevel", "Repository info", ""),
            (
                "git show HEAD; git show HEAD:src/main.rs | sed -n '1,180p'",
                "Show revisions",
                "HEAD, HEAD:src/main.rs",
            ),
            (
                "git status && rg '!*target*' . | sed -n '1,160p'",
                "Inspect repository",
                "search: !*target*",
            ),
            (
                "git status && git diff --stat HEAD",
                "Inspect repository",
                "status, diff: HEAD",
            ),
        ];

        for (command, label, detail) in cases {
            let presentation = project_tool_presentation(
                "functions.exec_command",
                &json!({"cmd": command}),
                None,
                false,
            );
            assert_eq!(presentation.label, label, "{command}");
            assert_eq!(presentation.detail, detail, "{command}");
            assert_eq!(presentation.category, ToolPresentationCategory::Execute);
            assert_eq!(
                presentation.input.as_ref().map(|body| body.text.as_str()),
                (!command.starts_with("bash "))
                    .then_some(command)
                    .or(Some("git worktree list --porcelain")),
                "{command}"
            );
        }
    }

    #[test]
    fn presents_safe_read_only_shell_commands_as_reads() {
        let read = project_tool_presentation(
            "functions.exec_command",
            &json!({"cmd": "sed -n '1,400p' docs/model-adaptation/CLOSED-ROUTES.md"}),
            None,
            false,
        );
        assert_eq!(read.label, "Read");
        assert_eq!(read.category, ToolPresentationCategory::Read);
        assert_eq!(
            read.detail,
            "sed -n '1,400p' docs/model-adaptation/CLOSED-ROUTES.md"
        );

        let wrapped = project_tool_presentation(
            "functions.exec_command",
            &json!({"cmd": "bash -lc 'sed -n \"1,4p\" README.md'"}),
            None,
            false,
        );
        assert_eq!(wrapped.label, "Read");
        assert_eq!(wrapped.category, ToolPresentationCategory::Read);
    }

    #[test]
    fn keeps_mutating_or_composed_shell_commands_as_execution() {
        for command in [
            "sed -i 's/old/new/' README.md",
            "sed -n '1,4p' README.md && rm -f /tmp/output",
        ] {
            let presentation = project_tool_presentation(
                "functions.exec_command",
                &json!({"cmd": command}),
                None,
                false,
            );
            assert_eq!(presentation.label, "Run", "{command}");
            assert_eq!(
                presentation.category,
                ToolPresentationCategory::Execute,
                "{command}"
            );
        }
    }

    #[test]
    fn composed_git_commands_keep_each_operation_visible() {
        let presentation = project_tool_presentation(
            "functions.exec_command",
            &json!({
                "cmd": "git add docs/model-adaptation/review.md && git commit -m 'Prefer donor-preserving functionality'",
                "description": "docs/model-adaptation/review.md · && · git · commit · Prefer donor-preserving functionality"
            }),
            None,
            false,
        );
        assert_eq!(presentation.label, "Run Git operations");
        assert_eq!(
            presentation.detail,
            "add: docs/model-adaptation/review.md, commit: Prefer donor-preserving functionality"
        );
        assert_eq!(presentation.category, ToolPresentationCategory::Execute);
    }

    #[test]
    fn presents_direct_git_and_mcp_tool_names() {
        let cases = [
            (
                "git_worktree_remove",
                json!({"path": "../old"}),
                "Remove worktree",
                "../old",
            ),
            (
                "mcp__git__worktree_lock",
                json!({"worktree": "../topic"}),
                "Lock worktree",
                "../topic",
            ),
            (
                "mcp__git__push",
                json!({"remote": "origin", "branch": "main"}),
                "Push changes",
                "main · origin",
            ),
            (
                "git_add",
                json!({"path": "src/main.rs"}),
                "Git add",
                "src/main.rs",
            ),
            ("mcp__git__remote", json!({}), "Git remotes", ""),
        ];

        for (name, input, label, detail) in cases {
            let presentation = project_tool_presentation(name, &input, None, false);
            assert_eq!(presentation.label, label, "{name}");
            assert_eq!(presentation.detail, detail, "{name}");
            assert_eq!(presentation.category, ToolPresentationCategory::Execute);
        }

        let git = project_tool_presentation(
            "git",
            &json!({"command": "branch --show-current"}),
            None,
            false,
        );
        assert_eq!(git.label, "Git branch");
        assert_eq!(git.detail, "");
    }

    #[test]
    fn git_results_report_observed_output_and_preserve_errors() {
        let removed = project_tool_presentation(
            "functions.exec_command",
            &json!({"cmd": "git worktree remove ../old"}),
            Some(""),
            false,
        );
        assert_eq!(removed.result, None);

        let failed = project_tool_presentation(
            "functions.exec_command",
            &json!({"cmd": "git worktree remove ../old"}),
            Some("fatal: '../old' is not a working tree"),
            true,
        );
        assert_eq!(
            failed.result.as_deref(),
            Some("fatal: '../old' is not a working tree")
        );
        assert_eq!(
            failed.output.as_ref().map(|body| body.text.as_str()),
            Some("fatal: '../old' is not a working tree")
        );
    }

    #[test]
    fn background_process_helpers_share_provider_handle_and_output_contracts() {
        assert!(tool_can_start_background_process("functions.exec"));
        assert!(!tool_can_start_background_process("spawn_agent"));
        assert_eq!(
            tool_output_background_handle("Script running with cell ID build-1"),
            Some("build-1".to_string())
        );
        assert_eq!(
            tool_process_followup_handle("functions.wait", Some(&json!({"cell_id": "build-1"}))),
            Some("build-1".to_string())
        );
        let wrapped = json!({
            "content": [{"type": "text", "text": json!({"output": "one\ntwo"}).to_string()}]
        })
        .to_string();
        assert_eq!(tool_process_output_text(&wrapped), "one\ntwo");
        assert_eq!(
            tool_output_background_handle(
                "search.rs:42: Script running with cell ID already-finished"
            ),
            None,
            "quoted lifecycle text in ordinary command output is not a live process"
        );
    }
}
