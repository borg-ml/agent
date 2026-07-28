use serde_json::Value;

pub(crate) fn codex_item_type(item: &Value) -> &str {
    item.get("type").and_then(Value::as_str).unwrap_or("")
}

pub(crate) fn codex_item_id(item: &Value) -> String {
    item.get("id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

pub(super) fn matches_codex_type(item_type: &str, expected: &[&str]) -> bool {
    expected.contains(&item_type)
}

pub(crate) fn should_skip_codex_item(item_type: &str) -> bool {
    matches_codex_type(
        item_type,
        &[
            "agentMessage",
            "agent_message",
            "enteredReviewMode",
            "entered_review_mode",
            "exitedReviewMode",
            "exited_review_mode",
            "hookPrompt",
            "hook_prompt",
            "reasoning",
            "reasoningItem",
            "userMessage",
            "user_message",
        ],
    )
}

pub(crate) fn is_codex_context_compaction(item_type: &str) -> bool {
    matches_codex_type(item_type, &["contextCompaction", "context_compaction"])
}

pub(crate) fn codex_context_compaction_input(item: &Value) -> Value {
    sanitize_codex_item(item)
}

pub(crate) fn codex_tool_signature(item_type: &str, item: &Value) -> (String, Value) {
    match item_type {
        "commandExecution" | "command_execution" | "shellCommand" => {
            let command = item
                .get("command")
                .or_else(|| item.get("commandLine"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            (
                "command_execution".to_string(),
                serde_json::json!({ "command": command }),
            )
        }
        "mcpToolCall" | "mcp_tool_call" => {
            let server = item
                .get("serverName")
                .or_else(|| item.get("server"))
                .and_then(Value::as_str)
                .unwrap_or("");
            let tool = item
                .get("toolName")
                .or_else(|| item.get("tool"))
                .and_then(Value::as_str)
                .unwrap_or("");
            let name = if !server.is_empty() && !tool.is_empty() {
                format!("mcp__{server}__{tool}")
            } else {
                "mcp_tool_call".to_string()
            };
            let input = item
                .get("input")
                .or_else(|| item.get("arguments"))
                .cloned()
                .unwrap_or(Value::Null);
            (name, input)
        }
        "dynamicToolCall" | "dynamic_tool_call" => {
            let name = item
                .get("tool")
                .or_else(|| item.get("name"))
                .and_then(Value::as_str)
                .unwrap_or("dynamic_tool_call")
                .to_string();
            let input = item
                .get("arguments")
                .or_else(|| item.get("input"))
                .cloned()
                .unwrap_or(Value::Null);
            (name, input)
        }
        "collabAgentToolCall" | "collab_agent_tool_call" => (
            "collab_tool_call".to_string(),
            codex_collab_tool_input(item),
        ),
        "plan" => ("todo_list".to_string(), codex_plan_input(item)),
        "webSearch" | "web_search" | "webSearchCall" | "web_search_call" => {
            let input = codex_search_query(item)
                .map(|query| serde_json::json!({ "query": query }))
                .unwrap_or(Value::Null);
            ("web_search".to_string(), input)
        }
        "fileChange" | "file_change" | "patchApply" | "patch_apply" | "fileEdit" | "fileWrite" => {
            ("Edit".to_string(), codex_file_change_input(item))
        }
        "fileRead" => {
            let path = item
                .get("filePath")
                .or_else(|| item.get("path"))
                .and_then(Value::as_str)
                .unwrap_or("");
            ("Read".to_string(), serde_json::json!({ "file_path": path }))
        }
        "imageView" | "image_view" => {
            let path = item.get("path").and_then(Value::as_str).unwrap_or("");
            (
                "view_image".to_string(),
                serde_json::json!({ "path": path }),
            )
        }
        "imageGeneration" | "image_generation" => {
            let prompt = item
                .get("revisedPrompt")
                .or_else(|| item.get("revised_prompt"))
                .and_then(Value::as_str)
                .unwrap_or("");
            (
                "image_generation".to_string(),
                serde_json::json!({ "prompt": prompt }),
            )
        }
        other => (other.to_string(), sanitize_codex_item(item)),
    }
}

pub(crate) fn codex_tool_completion_input(item_type: &str, item: &Value) -> Option<Value> {
    if matches_codex_type(
        item_type,
        &[
            "webSearch",
            "web_search",
            "webSearchCall",
            "web_search_call",
        ],
    ) && let Some(query) = codex_search_query(item)
    {
        return Some(serde_json::json!({ "query": query }));
    }
    if matches_codex_type(item_type, &["mcpToolCall", "mcp_tool_call"]) {
        return item
            .get("input")
            .or_else(|| item.get("arguments"))
            .filter(|input| !input.is_null())
            .cloned();
    }
    if matches_codex_type(item_type, &["dynamicToolCall", "dynamic_tool_call"]) {
        return item
            .get("arguments")
            .or_else(|| item.get("input"))
            .filter(|input| !input.is_null())
            .cloned();
    }
    if matches_codex_type(
        item_type,
        &["collabAgentToolCall", "collab_agent_tool_call"],
    ) {
        return Some(codex_collab_tool_input(item));
    }
    if matches_codex_type(item_type, &["fileChange", "file_change"]) {
        return Some(codex_file_change_input(item));
    }
    if matches_codex_type(item_type, &["plan"]) {
        return Some(codex_plan_input(item));
    }
    None
}

pub(super) fn codex_collab_tool_input(item: &Value) -> Value {
    serde_json::json!({
        "tool": item.get("tool").and_then(Value::as_str).unwrap_or(""),
        "prompt": item.get("prompt").cloned().unwrap_or(Value::Null),
        "sender_thread_id": item.get("senderThreadId").or_else(|| item.get("sender_thread_id")).cloned().unwrap_or(Value::Null),
        "receiver_thread_ids": item.get("receiverThreadIds").or_else(|| item.get("receiver_thread_ids")).cloned().unwrap_or(Value::Null),
        "agents_states": item.get("agentsStates").or_else(|| item.get("agents_states")).cloned().unwrap_or(Value::Null),
        "model": item.get("model").cloned().unwrap_or(Value::Null),
        "reasoning_effort": item.get("reasoningEffort").or_else(|| item.get("reasoning_effort")).cloned().unwrap_or(Value::Null),
    })
}

pub(super) fn codex_plan_input(item: &Value) -> Value {
    let text = item.get("text").and_then(Value::as_str).unwrap_or("");
    let mut items = text
        .lines()
        .filter_map(|line| {
            let trimmed = line
                .trim()
                .trim_start_matches(|ch: char| {
                    ch == '-' || ch == '*' || ch == '•' || ch.is_ascii_digit() || ch == '.'
                })
                .trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(serde_json::json!({ "text": trimmed, "completed": false }))
            }
        })
        .collect::<Vec<_>>();
    if items.is_empty() && !text.trim().is_empty() {
        items.push(serde_json::json!({ "text": text.trim(), "completed": false }));
    }
    serde_json::json!({ "items": items, "text": text })
}

pub(super) fn codex_file_change_input(item: &Value) -> Value {
    let paths = item
        .get("changes")
        .and_then(Value::as_array)
        .map(|changes| {
            changes
                .iter()
                .filter_map(|change| change.get("path").and_then(Value::as_str))
                .filter(|path| !path.trim().is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let fallback = item
        .get("filePath")
        .or_else(|| item.get("file_path"))
        .or_else(|| item.get("path"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let file_path = paths.first().map(String::as_str).unwrap_or(fallback);
    let diff = item
        .get("changes")
        .and_then(Value::as_array)
        .map(|changes| {
            changes
                .iter()
                .filter_map(codex_change_diff)
                .collect::<Vec<_>>()
                .join("\n")
        })
        .filter(|diff| !diff.is_empty())
        .or_else(|| {
            item.get("diff")
                .or_else(|| item.get("patch"))
                .and_then(Value::as_str)
                .filter(|diff| !diff.trim().is_empty())
                .map(str::to_string)
        });
    serde_json::json!({ "file_path": file_path, "paths": paths, "diff": diff })
}

fn codex_change_diff(change: &Value) -> Option<String> {
    let path = change.get("path").and_then(Value::as_str).unwrap_or("");
    let kind = change
        .get("kind")
        .or_else(|| change.get("type"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_ascii_lowercase();
    let source = change
        .get("diff")
        .or_else(|| change.get("unified_diff"))
        .or_else(|| change.get("content"))
        .and_then(Value::as_str)
        .filter(|source| !source.trim().is_empty())?;
    let (header, source) = match kind.as_str() {
        "add" | "added" | "create" | "created" if !looks_like_diff(source) => {
            ("*** Add File: ", mark_file_lines(source, '+'))
        }
        "delete" | "deleted" | "remove" | "removed" if !looks_like_diff(source) => {
            ("*** Delete File: ", mark_file_lines(source, '-'))
        }
        _ if looks_like_diff(source) => ("*** Update File: ", source.to_string()),
        _ => return None,
    };
    Some(format!("{header}{path}\n{source}"))
}

fn mark_file_lines(source: &str, marker: char) -> String {
    let mut marked = source
        .lines()
        .map(|line| format!("{marker}{line}"))
        .collect::<Vec<_>>()
        .join("\n");
    if source.ends_with('\n') {
        marked.push('\n');
    }
    marked
}

fn looks_like_diff(source: &str) -> bool {
    source.contains("*** Begin Patch")
        || source.lines().any(|line| {
            line.starts_with("@@")
                || line.starts_with("diff --git ")
                || line.starts_with("--- ")
                || line.starts_with("+++ ")
        })
}

pub(super) fn codex_search_query(item: &Value) -> Option<String> {
    item.pointer("/action/query")
        .and_then(Value::as_str)
        .filter(|query| !query.trim().is_empty())
        .map(ToString::to_string)
        .or_else(|| {
            item.pointer("/action/queries")
                .and_then(Value::as_array)
                .and_then(|queries| {
                    queries
                        .iter()
                        .filter_map(Value::as_str)
                        .find(|q| !q.trim().is_empty())
                })
                .map(ToString::to_string)
        })
        .or_else(|| {
            item.pointer("/action/url")
                .and_then(Value::as_str)
                .filter(|url| !url.trim().is_empty())
                .map(|url| {
                    let pattern = item
                        .pointer("/action/pattern")
                        .and_then(Value::as_str)
                        .filter(|pattern| !pattern.trim().is_empty());
                    match pattern {
                        Some(pattern) => format!("{pattern} in {url}"),
                        None => url.to_string(),
                    }
                })
        })
        .or_else(|| {
            item.get("query")
                .and_then(Value::as_str)
                .filter(|query| !query.trim().is_empty())
                .map(ToString::to_string)
        })
        .or_else(|| {
            item.get("queries")
                .and_then(Value::as_array)
                .and_then(|queries| {
                    queries
                        .iter()
                        .filter_map(Value::as_str)
                        .find(|q| !q.trim().is_empty())
                })
                .map(ToString::to_string)
        })
}

pub(super) fn sanitize_codex_item(item: &Value) -> Value {
    let mut copy = item.clone();
    if let Some(object) = copy.as_object_mut() {
        for key in [
            "id",
            "type",
            "status",
            "aggregatedOutput",
            "aggregated_output",
            "output",
            "exitCode",
            "exit_code",
            "text",
            "content",
        ] {
            object.remove(key);
        }
    }
    copy
}

pub(crate) fn codex_tool_output(item_type: &str, item: &Value) -> String {
    if codex_tool_is_error(item_type, item)
        && let Some(error) = item.get("error").filter(|error| !error.is_null())
    {
        return error
            .get("message")
            .and_then(Value::as_str)
            .map_or_else(|| stringify_json_field(Some(error)), str::to_string);
    }
    match item_type {
        "commandExecution" | "command_execution" | "shellCommand" => item
            .get("aggregatedOutput")
            .or_else(|| item.get("aggregated_output"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        "mcpToolCall" | "mcp_tool_call" => stringify_json_field(
            item.get("output")
                .or_else(|| item.get("result"))
                .or_else(|| item.get("content")),
        ),
        "dynamicToolCall" | "dynamic_tool_call" => stringify_json_field(
            item.get("contentItems")
                .or_else(|| item.get("content_items"))
                .or_else(|| item.get("output"))
                .or_else(|| item.get("result")),
        ),
        "fileChange" | "file_change" => stringify_json_field(item.get("changes")),
        "collabAgentToolCall" | "collab_agent_tool_call" => stringify_json_field(
            item.get("error")
                .or_else(|| item.get("result"))
                .or_else(|| item.get("status")),
        ),
        _ => stringify_json_field(
            item.get("output")
                .or_else(|| item.get("result"))
                .or_else(|| item.get("content"))
                .or_else(|| item.get("text")),
        ),
    }
}

pub(super) fn stringify_json_field(value: Option<&Value>) -> String {
    let Some(value) = value else {
        return String::new();
    };
    match value {
        Value::String(text) => text.clone(),
        Value::Array(items) => {
            let parts = items
                .iter()
                .filter_map(|item| {
                    item.get("text")
                        .and_then(Value::as_str)
                        .map(ToString::to_string)
                        .or_else(|| item.as_str().map(ToString::to_string))
                })
                .collect::<Vec<_>>();
            if parts.is_empty() {
                value.to_string()
            } else {
                parts.join("\n")
            }
        }
        other => other.to_string(),
    }
}

pub(crate) fn codex_tool_is_error(item_type: &str, item: &Value) -> bool {
    if item
        .get("isError")
        .or_else(|| item.get("is_error"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return true;
    }
    if item
        .get("status")
        .and_then(Value::as_str)
        .is_some_and(|status| status == "failed")
    {
        return true;
    }
    if item.get("error").is_some_and(|error| !error.is_null()) {
        return true;
    }
    if matches_codex_type(
        item_type,
        &["commandExecution", "command_execution", "shellCommand"],
    ) && let Some(code) = item
        .get("exitCode")
        .or_else(|| item.get("exit_code"))
        .and_then(Value::as_i64)
    {
        return code != 0;
    }
    false
}

pub(super) fn extract_codex_agent_message_text(item: &Value) -> Option<String> {
    if let Some(text) = item.get("text").and_then(Value::as_str)
        && !text.is_empty()
    {
        return Some(text.to_string());
    }
    let blocks = item.get("content").and_then(Value::as_array)?;
    let text = blocks
        .iter()
        .filter_map(|block| {
            block
                .get("text")
                .and_then(Value::as_str)
                .map(ToString::to_string)
                .or_else(|| block.as_str().map(ToString::to_string))
        })
        .collect::<String>();
    if text.is_empty() { None } else { Some(text) }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn failed_mcp_call_uses_its_error_instead_of_null_output() {
        let item = json!({
            "type": "mcpToolCall",
            "status": "failed",
            "output": null,
            "error": "goal update was rejected"
        });

        assert!(codex_tool_is_error("mcpToolCall", &item));
        assert_eq!(
            codex_tool_output("mcpToolCall", &item),
            "goal update was rejected"
        );

        let structured_error = json!({
            "type": "mcpToolCall",
            "status": "failed",
            "output": null,
            "error": {
                "message": "session goal actor is unavailable",
                "details": {"code": "actor_unavailable"}
            }
        });
        assert_eq!(
            codex_tool_output("mcpToolCall", &structured_error),
            "session goal actor is unavailable"
        );
    }
}
