//! Renderer-independent transcript event semantics.
use crate::entry::TranscriptActionState;
use borg_agent_runtime::*;
use std::collections::HashSet;
use std::path::PathBuf;

pub fn ready_detail_is_failure(detail: &str) -> bool {
    let detail = detail.trim().to_ascii_lowercase();
    detail.starts_with("borg blocked a provider-native delegation attempt")
        || detail.starts_with("turn failed")
        || detail.starts_with("could not wake")
}

pub fn effective_subagent_status(
    activity: SubagentActivityKind,
    snapshot_status: SubagentStatus,
    child_event: Option<&SessionEvent>,
) -> SubagentStatus {
    match activity {
        SubagentActivityKind::Completed => SubagentStatus::Ready,
        SubagentActivityKind::Stopped => SubagentStatus::Stopped,
        SubagentActivityKind::Failed => SubagentStatus::Failed,
        SubagentActivityKind::Updated => child_event
            .and_then(|event| subagent_status_from_child_event(&event.kind))
            .unwrap_or(snapshot_status),
        SubagentActivityKind::Started => snapshot_status,
    }
}

pub fn subagent_status_from_child_event(kind: &SessionEventKind) -> Option<SubagentStatus> {
    match kind {
        SessionEventKind::StatusChanged { status, .. } => Some(match status {
            SessionStatus::Starting => SubagentStatus::Starting,
            SessionStatus::Running => SubagentStatus::Running,
            SessionStatus::Ready | SessionStatus::Completed => SubagentStatus::Ready,
            SessionStatus::WaitingForApproval => SubagentStatus::WaitingForApproval,
            SessionStatus::Stopped => SubagentStatus::Stopped,
            SessionStatus::Failed => SubagentStatus::Failed,
        }),
        SessionEventKind::TurnCompleted { .. } => Some(SubagentStatus::Ready),
        _ => None,
    }
}

pub fn display_agent_name(task_name: &str) -> String {
    match task_name.strip_prefix("/root/").unwrap_or(task_name) {
        "claude" => "Claude".to_string(),
        "gpt" => "GPT".to_string(),
        name => name.to_string(),
    }
}

pub fn is_live_tool_call_event(kind: &str) -> bool {
    kind == "tool_call_started"
}

pub fn is_context_compaction(kind: &str) -> bool {
    matches!(
        kind.rsplit(['.', ':', '/'])
            .next()
            .unwrap_or(kind)
            .to_ascii_lowercase()
            .replace(['-', '_'], "")
            .as_str(),
        "contextcompaction"
    )
}

pub fn context_compaction_started(kind: &str, payload: &serde_json::Value) -> bool {
    payload
        .get("status")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|status| status.eq_ignore_ascii_case("started"))
        || (kind
            .rsplit(['.', ':', '/'])
            .next()
            .unwrap_or(kind)
            .eq_ignore_ascii_case("contextCompaction")
            && kind.to_ascii_lowercase().contains("started"))
}

pub fn context_compaction_full_summary(payload: &serde_json::Value) -> String {
    [
        "summary",
        "message",
        "detail",
        "/item/summary",
        "/item/message",
        "/item/detail",
        "/params/item/summary",
        "/params/item/message",
        "/params/item/detail",
    ]
    .into_iter()
    .find_map(|field| {
        let value = if field.starts_with('/') {
            payload.pointer(field)
        } else {
            payload.get(field)
        };
        value
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
    })
    .map(str::to_string)
    .unwrap_or_else(|| "Context was compacted.".to_string())
}

pub fn context_compaction_card_summary(payload: &serde_json::Value) -> String {
    let detail = context_compaction_full_summary(payload);
    if matches!(
        detail.trim().to_ascii_lowercase().as_str(),
        "context was compacted."
            | "context was compacted"
            | "context compacted."
            | "context compacted"
    ) {
        "Context compacted".to_string()
    } else {
        format!("Compacted context: {detail}")
    }
}

pub fn context_remaining_percent(tokens: u64, window: u64) -> u8 {
    const BASELINE_TOKENS: u64 = 12_000;
    if window <= BASELINE_TOKENS {
        return 0;
    }
    let effective_window = window - BASELINE_TOKENS;
    let used = tokens.saturating_sub(BASELINE_TOKENS);
    let remaining = effective_window.saturating_sub(used);
    ((remaining as f64 / effective_window as f64) * 100.0)
        .clamp(0.0, 100.0)
        .round() as u8
}

pub fn format_context_tokens(tokens: u64) -> String {
    const THOUSAND: u64 = 1_000;
    const MILLION: u64 = 1_000_000;
    if tokens >= MILLION {
        let value = format!("{:.1}", tokens as f64 / MILLION as f64);
        format!("{}m", value.trim_end_matches(".0"))
    } else if tokens >= THOUSAND {
        let value = format!("{:.1}", tokens as f64 / THOUSAND as f64);
        format!("{}k", value.trim_end_matches(".0"))
    } else {
        tokens.to_string()
    }
}

pub fn format_action_detail(label: &str, detail: &str) -> String {
    if label.trim().is_empty() {
        compact_text(detail, 180)
    } else if detail.trim().is_empty() {
        label.to_string()
    } else {
        format!("{label} · {}", compact_text(detail, 180))
    }
}

pub fn format_action_text(label: &str, detail: &str, body: Option<&str>) -> String {
    let mut text = if detail.is_empty() {
        label.to_string()
    } else {
        format!("{label} · {detail}")
    };
    if let Some(body) = body.filter(|body| !body.trim().is_empty())
        && let Some(first) = body.lines().find(|line| !line.trim().is_empty())
    {
        text.push_str(" · ");
        text.push_str(&compact_text(first, 120));
    }
    text
}

pub fn subagent_action_projection(
    activity: SubagentActivityKind,
    agent: &SubagentSnapshot,
    child_event: Option<&SessionEvent>,
) -> Option<SubagentActionProjection> {
    let task = agent.task_name.clone();
    let project = |detail: String, _body: Option<String>, state| {
        Some((
            "Agent".to_string(),
            format!("{task} · {detail}"),
            None,
            state,
        ))
    };
    match activity {
        SubagentActivityKind::Started => {
            project("started".to_string(), None, TranscriptActionState::Running)
        }
        SubagentActivityKind::Completed => project(
            "completed".to_string(),
            agent.final_text.clone(),
            TranscriptActionState::Complete,
        ),
        SubagentActivityKind::Stopped => project(
            "stopped".to_string(),
            agent.final_text.clone(),
            TranscriptActionState::Stopped,
        ),
        SubagentActivityKind::Failed => project(
            format!(
                "failed{}",
                agent
                    .detail
                    .as_deref()
                    .filter(|detail| !detail.trim().is_empty())
                    .map(|detail| format!(" · {}", compact_text(detail, 120)))
                    .unwrap_or_default()
            ),
            agent.detail.clone(),
            TranscriptActionState::Failed,
        ),
        SubagentActivityKind::Updated => match child_event.map(|event| &event.kind) {
            Some(SessionEventKind::Message {
                actor: EventActor::Assistant,
                text,
                status: MessageStatus::Complete,
                ..
            }) if !text.trim().is_empty() => project(
                "report ready".to_string(),
                Some(text.clone()),
                TranscriptActionState::Complete,
            ),
            Some(SessionEventKind::ApprovalRequested { title, detail, .. }) => project(
                format!(
                    "needs approval · {}",
                    compact_text(
                        if title.trim().is_empty() {
                            detail
                        } else {
                            title
                        },
                        120
                    )
                ),
                (!detail.trim().is_empty()).then(|| detail.clone()),
                TranscriptActionState::Waiting,
            ),
            Some(SessionEventKind::StatusChanged {
                status: SessionStatus::Ready | SessionStatus::Completed,
                detail: Some(detail),
            }) if ready_detail_is_failure(detail) => project(
                format!("failed turn · {}", compact_text(detail, 120)),
                Some(detail.clone()),
                TranscriptActionState::Failed,
            ),
            Some(SessionEventKind::StatusChanged {
                status: SessionStatus::Ready | SessionStatus::Completed,
                ..
            }) => project(
                "done · waiting for input".to_string(),
                agent.final_text.clone(),
                TranscriptActionState::Complete,
            ),
            Some(SessionEventKind::StatusChanged {
                status: SessionStatus::WaitingForApproval,
                detail,
            }) => project(
                "waiting for input".to_string(),
                detail.clone(),
                TranscriptActionState::Waiting,
            ),
            Some(SessionEventKind::Error { message }) => project(
                format!("error · {}", compact_text(message, 120)),
                Some(message.clone()),
                TranscriptActionState::Failed,
            ),
            _ => match agent.status {
                SubagentStatus::Starting => {
                    project("starting".to_string(), None, TranscriptActionState::Running)
                }
                SubagentStatus::Running => {
                    project("working".to_string(), None, TranscriptActionState::Running)
                }
                SubagentStatus::WaitingForApproval => project(
                    "waiting for approval".to_string(),
                    None,
                    TranscriptActionState::Waiting,
                ),
                SubagentStatus::Ready => {
                    if let Some(detail) = agent
                        .detail
                        .as_deref()
                        .filter(|detail| ready_detail_is_failure(detail))
                    {
                        project(
                            format!("failed turn · {}", compact_text(detail, 120)),
                            Some(detail.to_string()),
                            TranscriptActionState::Failed,
                        )
                    } else {
                        Some((
                            "Agent".to_string(),
                            format!("{task} · done · waiting for input"),
                            agent.final_text.clone(),
                            TranscriptActionState::Complete,
                        ))
                    }
                }
                SubagentStatus::Stopped => project(
                    "stopped".to_string(),
                    agent.final_text.clone(),
                    TranscriptActionState::Stopped,
                ),
                SubagentStatus::Failed => project(
                    "failed".to_string(),
                    agent.detail.clone(),
                    TranscriptActionState::Failed,
                ),
            },
        },
    }
}

pub fn number_message_attachments(
    text: &str,
    attachments: &[PathBuf],
    next_image_number: &mut usize,
) -> Vec<(usize, PathBuf)> {
    let explicit = image_numbers_in_text(text);
    let explicit = &explicit[explicit.len().saturating_sub(attachments.len())..];
    let mut used = explicit.iter().copied().collect::<HashSet<_>>();
    let mut fallback = *next_image_number;
    let numbered = attachments
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, path)| {
            let number = explicit.get(index).copied().unwrap_or_else(|| {
                while used.contains(&fallback) {
                    fallback = fallback.saturating_add(1);
                }
                let number = fallback;
                used.insert(number);
                fallback = fallback.saturating_add(1);
                number
            });
            (number, path)
        })
        .collect::<Vec<_>>();
    if let Some(highest) = numbered.iter().map(|(number, _)| *number).max() {
        *next_image_number = (*next_image_number).max(highest.saturating_add(1));
    }
    numbered
}

pub fn image_numbers_in_text(text: &str) -> Vec<usize> {
    let mut numbers = Vec::new();
    let mut remaining = text;
    while let Some(start) = remaining.find("[Image ") {
        let value = &remaining[start + "[Image ".len()..];
        let Some(end) = value.find(']') else {
            break;
        };
        let candidate = &value[..end];
        if !candidate.is_empty()
            && candidate.bytes().all(|byte| byte.is_ascii_digit())
            && let Ok(number) = candidate.parse::<usize>()
            && number > 0
        {
            numbers.push(number);
        }
        remaining = &value[end + 1..];
    }
    numbers
}

pub fn longest_suffix_prefix_overlap(left: &str, right: &str) -> usize {
    if left.is_empty() || right.is_empty() {
        return 0;
    }
    let pattern = right.as_bytes();
    let mut prefix = vec![0; pattern.len()];
    for index in 1..pattern.len() {
        let mut matched = prefix[index - 1];
        while matched > 0 && pattern[index] != pattern[matched] {
            matched = prefix[matched - 1];
        }
        if pattern[index] == pattern[matched] {
            matched += 1;
        }
        prefix[index] = matched;
    }

    let mut tail_start = left.len().saturating_sub(pattern.len());
    while !left.is_char_boundary(tail_start) {
        tail_start += 1;
    }
    let tail = &left.as_bytes()[tail_start..];
    let mut matched = 0;
    for (index, byte) in tail.iter().enumerate() {
        while matched > 0 && *byte != pattern[matched] {
            matched = prefix[matched - 1];
        }
        if *byte == pattern[matched] {
            matched += 1;
        }
        if matched == pattern.len() && index + 1 < tail.len() {
            matched = prefix[matched - 1];
        }
    }
    debug_assert!(right.is_char_boundary(matched));
    debug_assert!(left.is_char_boundary(left.len() - matched));
    matched
}

pub fn provider_interaction_options(payload: &serde_json::Value) -> String {
    payload
        .get("questions")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .flat_map(|question| {
            question
                .get("options")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|option| option.get("label").and_then(serde_json::Value::as_str))
        })
        .collect::<Vec<_>>()
        .join(" · ")
}

pub fn borg_control_tool_output_view(
    name: &str,
    input: Option<&serde_json::Value>,
    output: &str,
) -> Option<String> {
    let leaf = name
        .rsplit(['.', '_'])
        .next()
        .unwrap_or(name)
        .to_ascii_lowercase();
    let control = matches!(
        leaf.as_str(),
        "agents" | "agent" | "message" | "task" | "goal" | "plan"
    ) || [
        "list_agents",
        "spawn_agent",
        "followup_task",
        "send_message",
        "wait_agent",
        "get_goal",
        "update_goal",
        "get_plan",
        "update_plan",
        "list_unread_team_messages",
    ]
    .iter()
    .any(|candidate| name.to_ascii_lowercase().ends_with(candidate));
    if !control {
        return None;
    }
    let value = decoded_tool_output(output)?;
    let tool = name.to_ascii_lowercase();
    let mut rows = Vec::new();
    if tool.ends_with("list_unread_team_messages") {
        let messages = value.as_array()?;
        rows.push(format!(
            "UNREAD · {} message{}",
            messages.len(),
            if messages.len() == 1 { "" } else { "s" }
        ));
        for message in messages.iter() {
            let delivery = json_text(message, &["delivery"]).unwrap_or("queued");
            let sender = json_text(message, &["sender", "from", "actor"]);
            let text = json_text(message, &["text", "message"]).unwrap_or("empty message");
            let prefix = sender.map_or_else(
                || format!("  {delivery:>10}  "),
                |sender| format!("  {delivery:>10}  {sender} · "),
            );
            rows.push(format!("{prefix}{}", text));
        }
    } else if tool.ends_with("list_agents") || value.get("agents").is_some() || value.is_array() {
        let agents = value
            .get("agents")
            .and_then(serde_json::Value::as_array)
            .or_else(|| value.as_array())?;
        rows.push(format!(
            "TEAM · {} subagent{}",
            agents.len(),
            if agents.len() == 1 { "" } else { "s" }
        ));
        for agent in agents.iter() {
            let id = json_text(agent, &["task_name", "name", "id", "agent_id"]).unwrap_or("agent");
            let status = json_text(agent, &["status", "state"]).unwrap_or("unknown");
            let model = json_text(agent, &["model", "provider"]);
            let effort = json_text(agent, &["effort", "reasoning_effort"]);
            let task = json_text(agent, &["task", "objective", "message"]);
            let mut line = format!("  {status:>10}  {id}");
            if let Some(model) = model {
                line.push_str(&format!(" · {model}"));
            }
            if let Some(effort) = effort {
                line.push_str(&format!("/{effort}"));
            }
            rows.push(line);
            if let Some(task) = task {
                rows.push(format!("              {}", task));
            }
        }
    } else if tool.ends_with("get_plan")
        || tool.ends_with("update_plan")
        || value.get("plan").is_some()
        || value.get("items").is_some()
    {
        let steps = value
            .get("plan")
            .or_else(|| value.get("items"))
            .and_then(serde_json::Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        rows.push(format!(
            "PLAN · {} to-do{}",
            steps.len(),
            if steps.len() == 1 { "" } else { "s" }
        ));
        for step in steps.iter() {
            let status = json_text(step, &["status"]).unwrap_or("pending");
            let text = json_text(step, &["step", "content", "title", "description"])
                .unwrap_or("unnamed step");
            rows.push(format!("  {status:>10}  {}", text));
        }
    } else if tool.ends_with("get_goal")
        || tool.ends_with("update_goal")
        || value.get("goal").is_some()
    {
        let goal = value.get("goal").unwrap_or(&value);
        let status = json_text(goal, &["status"]).unwrap_or("current");
        let objective = json_text(goal, &["objective", "title"]).unwrap_or("goal");
        rows.push(format!("GOAL · {status} · {}", objective));
    } else if tool.ends_with("wait_agent") || tool.ends_with("spawn_agent") {
        let agent = value.get("agent").unwrap_or(&value);
        let id = json_text(agent, &["task_name", "name", "id", "agent_id"])
            .or_else(|| input.and_then(|input| json_text(input, &["task_name", "target"])))
            .unwrap_or("agent");
        let status = json_text(agent, &["status", "state"]).unwrap_or("updated");
        let action = if tool.ends_with("wait_agent") {
            "WAIT"
        } else {
            "SPAWN"
        };
        let mut row = format!("{action} · {status} · {id}");
        if let Some(model) = json_text(agent, &["model", "provider"]) {
            row.push_str(&format!(" · {model}"));
        }
        if let Some(effort) = json_text(agent, &["effort", "reasoning_effort"]) {
            row.push_str(&format!("/{effort}"));
        }
        rows.push(row);
        if let Some(text) = json_text(
            agent,
            &["message", "update", "final_text", "task", "objective"],
        ) {
            rows.push(format!("  {}", text));
        }
    } else {
        let target = input
            .and_then(|input| json_text(input, &["target", "task_name"]))
            .unwrap_or("team");
        let message = input.and_then(|input| json_text(input, &["message", "prompt"]));
        let action = if tool.ends_with("wait_agent") {
            "WAIT"
        } else if tool.ends_with("spawn_agent") {
            "SPAWN"
        } else if tool.ends_with("followup_task") {
            "FOLLOW UP"
        } else {
            "MESSAGE"
        };
        rows.push(format!("{action} · {target}"));
        if let Some(message) = message {
            rows.push(format!("  {}", message));
        }
    }
    Some(rows.join("\n"))
}

pub fn decoded_tool_output(output: &str) -> Option<serde_json::Value> {
    let value = serde_json::from_str::<serde_json::Value>(output).ok()?;
    if let Some(structured) = value
        .get("structuredContent")
        .filter(|structured| !structured.is_null())
    {
        return Some(structured.clone());
    }
    value
        .get("content")
        .and_then(serde_json::Value::as_array)
        .and_then(|content| {
            content.iter().find_map(|item| {
                item.get("text")
                    .and_then(serde_json::Value::as_str)
                    .and_then(|text| serde_json::from_str(text).ok())
            })
        })
        .or(Some(value))
}

pub fn json_text<'a>(value: &'a serde_json::Value, names: &[&str]) -> Option<&'a str> {
    names
        .iter()
        .find_map(|name| value.get(*name).and_then(serde_json::Value::as_str))
}

pub fn borg_lsp_diagnostics_view(
    name: &str,
    input: Option<&serde_json::Value>,
    output: &str,
) -> Option<String> {
    let normalized_name = name.to_ascii_lowercase();
    let workspace = normalized_name.ends_with("lsp_workspace_diagnostics");
    if workspace {
        return borg_lsp_workspace_diagnostics_view(output);
    }
    if !normalized_name.ends_with("lsp_diagnostics") {
        return None;
    }
    let value = serde_json::from_str::<serde_json::Value>(output).ok()?;
    let items = value
        .get("items")
        .and_then(serde_json::Value::as_array)
        .or_else(|| value.as_array())?;
    let path = input
        .and_then(|input| json_text(input, &["path"]))
        .unwrap_or("workspace");
    let mut rows = vec![format!(
        "DIAGNOSTICS · {path} · {} issue{}",
        items.len(),
        if items.len() == 1 { "" } else { "s" }
    )];
    for item in items.iter().take(8) {
        let severity = item
            .get("severity")
            .and_then(serde_json::Value::as_u64)
            .map(|value| match value {
                1 => "error",
                2 => "warning",
                3 => "info",
                4 => "hint",
                _ => "issue",
            })
            .unwrap_or("issue");
        let message = json_text(item, &["message"]).unwrap_or("diagnostic");
        let line = item
            .pointer("/range/start/line")
            .and_then(serde_json::Value::as_u64)
            .map(|line| format!(":{}", line + 1))
            .unwrap_or_default();
        rows.push(format!(
            "  {severity:>7}{line}  {}",
            compact_text(message, 120)
        ));
    }
    Some(rows.join("\n"))
}

pub fn borg_lsp_workspace_diagnostics_view(output: &str) -> Option<String> {
    let workspaces = serde_json::from_str::<serde_json::Value>(output)
        .ok()?
        .as_object()?
        .clone();
    let mut documents = Vec::new();
    for (server, report) in workspaces {
        let items = report.get("items")?.as_array()?;
        for document in items {
            let diagnostics = document
                .get("items")
                .and_then(serde_json::Value::as_array)
                .cloned()
                .unwrap_or_default();
            documents.push((server.clone(), document.clone(), diagnostics));
        }
    }
    let issue_count = documents
        .iter()
        .map(|(_, _, diagnostics)| diagnostics.len())
        .sum::<usize>();
    let mut rows = vec![format!(
        "WORKSPACE DIAGNOSTICS · {} issue{} · {} document{}",
        issue_count,
        if issue_count == 1 { "" } else { "s" },
        documents.len(),
        if documents.len() == 1 { "" } else { "s" }
    )];
    for (server, document, diagnostics) in documents.iter().take(8) {
        let uri = document
            .get("uri")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown document");
        rows.push(format!(
            "  {server} · {} issue{} · {uri}",
            diagnostics.len(),
            if diagnostics.len() == 1 { "" } else { "s" }
        ));
        for diagnostic in diagnostics.iter().take(2) {
            let severity = diagnostic
                .get("severity")
                .and_then(serde_json::Value::as_u64)
                .map(|value| match value {
                    1 => "error",
                    2 => "warning",
                    3 => "info",
                    4 => "hint",
                    _ => "issue",
                })
                .unwrap_or("issue");
            let message = json_text(diagnostic, &["message"]).unwrap_or("diagnostic");
            rows.push(format!("    {severity:>7}  {}", compact_text(message, 120)));
        }
    }
    if documents.len() > 8 {
        rows.push(format!("  … {} more documents", documents.len() - 8));
    }
    Some(rows.join("\n"))
}

pub type SubagentActionProjection = (String, String, Option<String>, TranscriptActionState);
