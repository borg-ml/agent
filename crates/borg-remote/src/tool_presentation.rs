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
        category: tool_category(name, &label),
        input: input_body,
        output: edit_result.map(|edit| edit.body).or_else(|| {
            output.and_then(|output| {
                tool_output_code_view(name, output)
                    .map(|(language, text)| ToolPresentationBody { language, text })
            })
        }),
        result: output.and_then(|output| summarize_tool_result(name, output, is_error)),
        body_rows: tool_detail_rows(name, input),
        backgrounded: output.is_some_and(|output| !is_error && tool_output_is_backgrounded(output)),
        hidden: is_internal_tool(name),
        label,
        detail,
    }
}

struct EditResultPresentation {
    label: String,
    detail: String,
    body: ToolPresentationBody,
}

fn edit_result_presentation(name: &str, output: &str) -> Option<EditResultPresentation> {
    if !matches!(
        tool_leaf_name(name).as_str(),
        "edit" | "apply_patch" | "write" | "write_file"
    ) {
        return None;
    }
    let entries = serde_json::from_str::<Value>(output)
        .ok()?
        .as_array()?
        .clone();
    let rendered = entries
        .iter()
        .filter_map(render_edit_result_entry)
        .collect::<Vec<_>>();
    if rendered.is_empty() {
        return None;
    }
    let paths = rendered
        .iter()
        .map(|entry| entry.path.as_str())
        .collect::<Vec<_>>();
    let all_add = rendered.iter().all(|entry| entry.kind == "add");
    let all_delete = rendered.iter().all(|entry| entry.kind == "delete");
    let label = match (rendered.len(), all_add, all_delete) {
        (1, true, _) => "Create file",
        (1, _, true) => "Delete file",
        (_, true, _) => "Create files",
        (_, _, true) => "Delete files",
        _ => "Edit",
    };
    let detail = if paths.len() == 1 {
        paths[0].to_string()
    } else {
        format!("{} files", paths.len())
    };
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
                .map(|entry| entry.diff)
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
        "update_plan" | "update_todo" | "update_todos" | "get_plan" | "get_todo" | "get_todos"
    ) {
        return None;
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

pub fn is_diff_language(language: &str) -> bool {
    language == "diff" || language.starts_with("diff:")
}

pub fn tool_call_summary(name: &str, input: &Value) -> (String, String) {
    let tool = tool_leaf_name(name);

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
        return ("Read goal".to_string(), "current status".to_string());
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

    if tool == "lsp_status" {
        return ("Language servers".to_string(), "status".to_string());
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
        return (
            "Run".to_string(),
            string_field(input, "description")
                .filter(|description| !description.trim().is_empty())
                .map(|description| compact_text(description, 160))
                .unwrap_or_else(|| compact_text(&unwrapped_shell_command(command), 160)),
        );
    }

    if tool.contains("read")
        && let Some(path) = input_path(input)
    {
        return ("Read".to_string(), path.to_string());
    }

    if tool.contains("edit") || tool.contains("patch") || tool.contains("write_file") {
        let detail = input_path(input)
            .map(str::to_string)
            .or_else(|| edit_source(input).and_then(patch_path))
            .unwrap_or_else(|| "files".to_string());
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
    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        return Some((
            "json".to_string(),
            serde_json::to_string_pretty(&value).unwrap_or_else(|_| trimmed.to_string()),
        ));
    }
    Some(("text".to_string(), readable_result_text(trimmed)))
}

pub fn tool_output_is_backgrounded(output: &str) -> bool {
    serde_json::from_str::<Value>(output)
        .ok()
        .is_some_and(|value| value.get("session_id").is_some() || value.get("cell_id").is_some())
        || output.contains("Process running with session ID")
        || output.contains("Script running with cell ID")
}

fn product_tool_summary(name: &str, input: &Value) -> Option<(String, String)> {
    let leaf = tool_leaf_name(name);
    let label = match leaf.as_str() {
        "read" | "read_file" => "Read",
        "write" | "write_file" => "Edit",
        "edit" | "apply_patch" => "Edit",
        "grep" | "glob" => "Search",
        "todo_list" | "todowrite" => "Update plan",
        "image_generation" => "Generate image",
        "view_image" => "View image",
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
    let detail = if matches!(
        leaf.as_str(),
        "edit" | "apply_patch" | "write" | "write_file"
    ) {
        input_path(input)
            .map(str::to_string)
            .or_else(|| edit_source(input).and_then(patch_path))
            .unwrap_or_else(|| "files".to_string())
    } else {
        high_signal_detail(input).unwrap_or_default()
    };
    Some((label.to_string(), compact_text(&detail, 200)))
}

fn tool_category(name: &str, label: &str) -> ToolPresentationCategory {
    let leaf = tool_leaf_name(name);
    if is_subagent_tool(label)
        || matches!(
            leaf.as_str(),
            "task" | "agent" | "collab_tool_call" | "collabagenttoolcall"
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
    } else if leaf == "remote_approval" {
        ToolPresentationCategory::Approval
    } else if matches!(leaf.as_str(), "image_generation" | "view_image") {
        ToolPresentationCategory::Image
    } else if name.to_ascii_lowercase().contains("web") {
        ToolPresentationCategory::Web
    } else if matches!(
        leaf.as_str(),
        "bash" | "command_execution" | "exec_command" | "exec"
    ) {
        ToolPresentationCategory::Execute
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

fn summarize_tool_result(name: &str, output: &str, is_error: bool) -> Option<String> {
    let readable = readable_result_text(output);
    let trimmed = readable.trim();
    if trimmed.is_empty() || trimmed == "null" {
        return None;
    }
    if is_error {
        return trimmed
            .lines()
            .find(|line| !line.trim().is_empty())
            .map(|line| compact_text(line, 100));
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

fn readable_result_text(value: &str) -> String {
    let Ok(Value::Object(fields)) = serde_json::from_str::<Value>(value) else {
        return value.to_string();
    };
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
    if let Some(structured) = fields.get("structuredContent") {
        return serde_json::to_string_pretty(structured).unwrap_or_else(|_| value.to_string());
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
    format!(
        "{} · {}",
        title_case(service),
        action.replace(['_', '-'], " ")
    )
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
    for character in command.chars() {
        if escaped {
            current.push(character);
            escaped = false;
        } else if character == '\\' && quote != Some('\'') {
            escaped = true;
        } else if quote == Some(character) {
            quote = None;
        } else if quote.is_none() && matches!(character, '\'' | '"') {
            quote = Some(character);
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
}
