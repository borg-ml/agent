use std::collections::{HashMap, HashSet};

use anyhow::Result;
use serde_json::{Value, json};
use tokio::sync::mpsc;

use super::super::codex_app_server::JsonRpcMessage;
use super::codex_items::*;
use super::{ChatStreamEvent, ProviderEventTelemetry};
use crate::runtime::{CostBasis, ProviderCallUsage};

pub(super) fn codex_turn_usage(
    usage: Option<&super::super::codex_app_server::TokenUsage>,
    _total_usage: Option<&super::super::codex_app_server::TokenUsage>,
    model_context_window: Option<u64>,
    model: Option<&str>,
    duration_ms: u64,
) -> Option<ProviderCallUsage> {
    let usage = usage?;
    // Codex's input count includes cache reads and writes. Borg's usage
    // contract keeps those three billing buckets exclusive.
    let input_tokens = usage
        .input_tokens
        .saturating_sub(usage.cached_input_tokens)
        .saturating_sub(usage.cache_write_input_tokens);
    // Codex output already includes reasoning output.
    let total_tokens = usage
        .total_tokens
        .max(usage.input_tokens.saturating_add(usage.output_tokens));
    let mut provider_usage = ProviderCallUsage {
        duration_ms,
        input_tokens,
        cached_input_tokens: usage.cached_input_tokens,
        cache_creation_input_tokens: usage.cache_write_input_tokens,
        output_tokens: usage.output_tokens,
        total_tokens,
        cost_microusd: None,
        cost_basis: CostBasis::Unavailable,
        // Codex reports `last.total_tokens` as the current active context and
        // `total.total_tokens` as cumulative session usage. The latter does not
        // reset after compaction and therefore cannot drive a context meter.
        context_tokens: Some(total_tokens),
        context_window_tokens: model_context_window,
    };
    if let Some(model) = model
        && let Some(cost_microusd) =
            super::super::estimate_openai_cost_microusd(model, &provider_usage)
    {
        provider_usage.cost_microusd = Some(cost_microusd);
        provider_usage.cost_basis = CostBasis::EstimatedFromPricing;
    }
    Some(provider_usage)
}

#[derive(Default)]
pub(super) struct CodexStreamMapper {
    agent_message_text: HashMap<String, String>,
    emitted_agent_message_ids: HashSet<String>,
    emitted_any_agent_message: bool,
    emitted_phase_item_ids: HashSet<String>,
}

impl CodexStreamMapper {
    pub(super) fn handle(
        &mut self,
        message: &JsonRpcMessage,
        tx: &mpsc::Sender<ChatStreamEvent>,
    ) -> Result<()> {
        let telemetry = classify_codex_provider_event(message);
        if !send_stream_event(
            tx,
            ChatStreamEvent::ProviderEvent {
                kind: summarize_codex_event_kind(message),
                payload: summarize_codex_provider_event(message),
                raw_payload: codex_raw_payload(message),
                stream_channel: telemetry.stream_channel,
                content_text: telemetry.content_text,
                provider_item_id: telemetry.provider_item_id,
                tool_use_id: telemetry.tool_use_id,
                tool_name: telemetry.tool_name,
            },
        ) {
            return Ok(());
        }
        match message.method.as_deref().unwrap_or("") {
            "item/commandExecution/requestApproval" | "execCommandApproval" => {
                let params = message.params.as_ref().unwrap_or(&Value::Null);
                let approval_id = message.id.map(|id| id.to_string()).unwrap_or_default();
                let command = params.get("command").and_then(|command| {
                    command.as_str().map(str::to_string).or_else(|| {
                        command.as_array().map(|parts| {
                            parts
                                .iter()
                                .filter_map(Value::as_str)
                                .collect::<Vec<_>>()
                                .join(" ")
                        })
                    })
                });
                let detail = params
                    .get("reason")
                    .and_then(Value::as_str)
                    .unwrap_or(
                        "Codex wants to run a command outside the current permission boundary.",
                    )
                    .to_string();
                let _ = send_stream_event(
                    tx,
                    ChatStreamEvent::ApprovalRequested {
                        approval_id,
                        title: "Run command?".to_string(),
                        detail,
                        command,
                    },
                );
            }
            "item/fileChange/requestApproval" | "applyPatchApproval" => {
                let params = message.params.as_ref().unwrap_or(&Value::Null);
                let approval_id = message.id.map(|id| id.to_string()).unwrap_or_default();
                let detail = params
                    .get("reason")
                    .and_then(Value::as_str)
                    .unwrap_or("Codex wants to apply file changes outside the current permission boundary.")
                    .to_string();
                let _ = send_stream_event(
                    tx,
                    ChatStreamEvent::ApprovalRequested {
                        approval_id,
                        title: "Apply file changes?".to_string(),
                        detail,
                        command: params
                            .get("grantRoot")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                    },
                );
            }
            "item/permissions/requestApproval" => {
                let params = message.params.as_ref().unwrap_or(&Value::Null);
                let approval_id = message.id.map(|id| id.to_string()).unwrap_or_default();
                let detail = params
                    .get("reason")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_else(|| {
                        format!(
                            "Codex requests additional permissions in {}: {}",
                            params
                                .get("cwd")
                                .and_then(Value::as_str)
                                .unwrap_or("the current workspace"),
                            params.get("permissions").cloned().unwrap_or(Value::Null)
                        )
                    });
                let _ = send_stream_event(
                    tx,
                    ChatStreamEvent::ApprovalRequested {
                        approval_id,
                        title: "Grant additional permissions?".to_string(),
                        detail,
                        command: None,
                    },
                );
            }
            "item/tool/requestUserInput" => {
                let params = message.params.as_ref().cloned().unwrap_or(Value::Null);
                let interaction_id = message.id.map(|id| id.to_string()).unwrap_or_default();
                let questions = params
                    .get("questions")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                let title = questions
                    .first()
                    .and_then(|question| question.get("header"))
                    .and_then(Value::as_str)
                    .unwrap_or("Codex needs input")
                    .to_string();
                let detail = questions
                    .iter()
                    .filter_map(|question| question.get("question").and_then(Value::as_str))
                    .collect::<Vec<_>>()
                    .join("\n");
                let _ = send_stream_event(
                    tx,
                    ChatStreamEvent::ProviderInteractionRequested {
                        interaction_id,
                        kind: "user_input".to_string(),
                        title,
                        detail,
                        payload: params,
                    },
                );
            }
            "mcpServer/elicitation/request" => {
                let params = message.params.as_ref().cloned().unwrap_or(Value::Null);
                let interaction_id = message.id.map(|id| id.to_string()).unwrap_or_default();
                let server_name = params
                    .get("serverName")
                    .and_then(Value::as_str)
                    .unwrap_or("MCP server");
                let detail = params
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("An MCP server needs input.")
                    .to_string();
                let _ = send_stream_event(
                    tx,
                    ChatStreamEvent::ProviderInteractionRequested {
                        interaction_id,
                        kind: "mcp_elicitation".to_string(),
                        title: format!("{server_name} requests input"),
                        detail,
                        payload: params,
                    },
                );
            }
            "item/agentMessage/delta" => {
                let Some(params) = &message.params else {
                    return Ok(());
                };
                let item_id = params
                    .get("itemId")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let Some(delta) = params.get("delta").and_then(Value::as_str) else {
                    return Ok(());
                };
                if !item_id.is_empty() {
                    if !self.emit_agent_message_boundary_if_needed(item_id, tx) {
                        return Ok(());
                    }
                    self.agent_message_text
                        .entry(item_id.to_string())
                        .or_default()
                        .push_str(delta);
                } else if !self.emit_agent_message_boundary_if_needed("", tx) {
                    return Ok(());
                }
                if !send_stream_event(tx, ChatStreamEvent::Delta(delta.to_string())) {
                    return Ok(());
                }
            }
            "item/reasoning/summaryTextDelta" | "item/reasoning/textDelta" => {
                let Some(delta) = message
                    .params
                    .as_ref()
                    .and_then(|params| params.get("delta"))
                    .and_then(Value::as_str)
                else {
                    return Ok(());
                };
                if !send_stream_event(tx, ChatStreamEvent::ReasoningDelta(delta.to_string())) {
                    return Ok(());
                }
            }
            "item/started" => {
                let Some(item) = message
                    .params
                    .as_ref()
                    .and_then(|params| params.get("item"))
                else {
                    return Ok(());
                };
                let item_type = codex_item_type(item);
                if is_codex_context_compaction(item_type) {
                    self.emit_context_compaction(item, tx);
                    return Ok(());
                }
                if should_skip_codex_item(item_type) {
                    return Ok(());
                }
                let id = codex_item_id(item);
                if id.is_empty() {
                    tracing::warn!(
                        item_type,
                        "codex item started without id; skipping tool call"
                    );
                    return Ok(());
                }
                let (name, input) = codex_tool_signature(item_type, item);
                if !send_stream_event(tx, ChatStreamEvent::ToolCall { id, name, input }) {
                    return Ok(());
                }
            }
            "item/completed" => {
                let Some(item) = message
                    .params
                    .as_ref()
                    .and_then(|params| params.get("item"))
                else {
                    return Ok(());
                };
                let item_type = codex_item_type(item);
                if is_codex_context_compaction(item_type) {
                    self.emit_context_compaction(item, tx);
                    return Ok(());
                }
                if matches_codex_type(item_type, &["agentMessage", "agent_message"]) {
                    self.emit_completed_agent_message(item, tx)?;
                    return Ok(());
                }
                if should_skip_codex_item(item_type) {
                    return Ok(());
                }
                let tool_use_id = codex_item_id(item);
                if tool_use_id.is_empty() {
                    tracing::warn!(
                        item_type,
                        "codex item completed without id; skipping tool result"
                    );
                    return Ok(());
                }
                let output = codex_tool_output(item_type, item);
                let is_error = codex_tool_is_error(item_type, item);
                let input = codex_tool_completion_input(item_type, item);
                if !send_stream_event(
                    tx,
                    ChatStreamEvent::ToolResult {
                        tool_use_id,
                        output,
                        is_error,
                        input,
                    },
                ) {
                    return Ok(());
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn emit_context_compaction(&mut self, item: &Value, tx: &mpsc::Sender<ChatStreamEvent>) {
        let id = codex_item_id(item);
        if !id.is_empty() && !self.emitted_phase_item_ids.insert(id) {
            return;
        }
        let _ = send_stream_event(
            tx,
            ChatStreamEvent::Phase {
                name: "context_compaction".to_string(),
                input: codex_context_compaction_input(item),
            },
        );
    }

    fn emit_completed_agent_message(
        &mut self,
        item: &Value,
        tx: &mpsc::Sender<ChatStreamEvent>,
    ) -> Result<()> {
        let Some(text) = extract_codex_agent_message_text(item) else {
            return Ok(());
        };
        let segment = text.clone();
        let id = codex_item_id(item);
        if id.is_empty() {
            if !self.emit_agent_message_boundary_if_needed("", tx) {
                return Ok(());
            }
            if !send_stream_event(tx, ChatStreamEvent::Delta(text)) {
                return Ok(());
            }
            if !send_stream_event(tx, ChatStreamEvent::Narration { text: segment }) {
                return Ok(());
            }
            return Ok(());
        }
        let existing = self
            .agent_message_text
            .get(&id)
            .cloned()
            .unwrap_or_default();
        if let Some(suffix) = text.strip_prefix(existing.as_str()) {
            if !suffix.is_empty() {
                self.agent_message_text
                    .entry(id)
                    .or_default()
                    .push_str(suffix);
                if !send_stream_event(tx, ChatStreamEvent::Delta(suffix.to_string())) {
                    return Ok(());
                }
            }
            if !send_stream_event(tx, ChatStreamEvent::Narration { text: segment }) {
                return Ok(());
            }
            return Ok(());
        }
        if existing.is_empty() {
            if !self.emit_agent_message_boundary_if_needed(&id, tx) {
                return Ok(());
            }
            self.agent_message_text.insert(id, text.clone());
            if !send_stream_event(tx, ChatStreamEvent::Delta(text)) {
                return Ok(());
            }
            if !send_stream_event(tx, ChatStreamEvent::Narration { text: segment }) {
                return Ok(());
            }
        }
        Ok(())
    }

    fn emit_agent_message_boundary_if_needed(
        &mut self,
        item_id: &str,
        tx: &mpsc::Sender<ChatStreamEvent>,
    ) -> bool {
        let is_new = if item_id.is_empty() {
            true
        } else {
            self.emitted_agent_message_ids.insert(item_id.to_string())
        };
        if !is_new {
            return true;
        }
        if self.emitted_any_agent_message
            && !send_stream_event(tx, ChatStreamEvent::Delta("\n\n".to_string()))
        {
            return false;
        }
        self.emitted_any_agent_message = true;
        true
    }
}

fn send_stream_event(tx: &mpsc::Sender<ChatStreamEvent>, event: ChatStreamEvent) -> bool {
    tx.blocking_send(event).is_ok()
}

fn codex_raw_payload(message: &JsonRpcMessage) -> Option<Value> {
    match serde_json::to_value(message) {
        Ok(value) => Some(value),
        Err(error) => {
            tracing::warn!(%error, "failed to serialize Codex raw provider event");
            None
        }
    }
}

fn classify_codex_provider_event(message: &JsonRpcMessage) -> ProviderEventTelemetry {
    let method = message.method.as_deref().unwrap_or("");
    let params = message.params.as_ref();
    let item = params.and_then(|params| params.get("item"));
    let item_type = item.map(codex_item_type).unwrap_or("");
    let item_id = item
        .map(codex_item_id)
        .filter(|id| !id.trim().is_empty())
        .or_else(|| {
            params
                .and_then(|params| params.get("itemId"))
                .and_then(Value::as_str)
                .filter(|id| !id.trim().is_empty())
                .map(str::to_string)
        });

    match method {
        "item/agentMessage/delta" => ProviderEventTelemetry {
            stream_channel: Some("assistant_text".to_string()),
            content_text: params
                .and_then(|params| params.get("delta"))
                .and_then(Value::as_str)
                .map(str::to_string),
            provider_item_id: item_id,
            ..ProviderEventTelemetry::default()
        },
        "item/reasoning/summaryTextDelta" | "item/reasoning/textDelta" => ProviderEventTelemetry {
            stream_channel: Some("reasoning".to_string()),
            content_text: params
                .and_then(|params| params.get("delta"))
                .and_then(Value::as_str)
                .map(str::to_string),
            provider_item_id: item_id,
            ..ProviderEventTelemetry::default()
        },
        method
            if method.contains("function_call_arguments.delta")
                || method.contains("tool_call_arguments.delta") =>
        {
            ProviderEventTelemetry {
                stream_channel: Some("tool_arguments".to_string()),
                content_text: params
                    .and_then(|params| params.get("delta"))
                    .and_then(Value::as_str)
                    .map(str::to_string),
                provider_item_id: item_id.clone(),
                tool_use_id: item_id,
                ..ProviderEventTelemetry::default()
            }
        }
        "item/started" => {
            let Some(item) = item else {
                return ProviderEventTelemetry::default();
            };
            if should_skip_codex_item(item_type) {
                return ProviderEventTelemetry {
                    stream_channel: Some("provider_event".to_string()),
                    provider_item_id: item_id,
                    ..ProviderEventTelemetry::default()
                };
            }
            let (name, input) = codex_tool_signature(item_type, item);
            ProviderEventTelemetry {
                stream_channel: Some("tool_call".to_string()),
                content_text: Some(input.to_string()),
                provider_item_id: item_id.clone(),
                tool_use_id: item_id,
                tool_name: Some(name),
            }
        }
        "item/completed" => {
            let Some(item) = item else {
                return ProviderEventTelemetry::default();
            };
            if matches_codex_type(item_type, &["agentMessage", "agent_message"]) {
                return ProviderEventTelemetry {
                    stream_channel: Some("assistant_message".to_string()),
                    content_text: extract_codex_agent_message_text(item),
                    provider_item_id: item_id,
                    ..ProviderEventTelemetry::default()
                };
            }
            if should_skip_codex_item(item_type) {
                return ProviderEventTelemetry {
                    stream_channel: Some("provider_event".to_string()),
                    provider_item_id: item_id,
                    ..ProviderEventTelemetry::default()
                };
            }
            let (name, _) = codex_tool_signature(item_type, item);
            ProviderEventTelemetry {
                stream_channel: Some("tool_result".to_string()),
                content_text: Some(codex_tool_output(item_type, item)),
                provider_item_id: item_id.clone(),
                tool_use_id: item_id,
                tool_name: Some(name),
            }
        }
        "turn/completed" => ProviderEventTelemetry {
            stream_channel: Some("terminal".to_string()),
            content_text: params
                .and_then(|params| params.get("status"))
                .and_then(Value::as_str)
                .map(str::to_string),
            ..ProviderEventTelemetry::default()
        },
        "error" => ProviderEventTelemetry {
            stream_channel: Some("error".to_string()),
            content_text: message
                .message
                .clone()
                .or_else(|| message.error.as_ref().map(Value::to_string)),
            ..ProviderEventTelemetry::default()
        },
        _ => ProviderEventTelemetry {
            stream_channel: Some("provider_event".to_string()),
            provider_item_id: item_id,
            ..ProviderEventTelemetry::default()
        },
    }
}

fn summarize_codex_event_kind(message: &JsonRpcMessage) -> String {
    let method = message.method.as_deref().unwrap_or("response");
    if let Some(item_type) = message
        .params
        .as_ref()
        .and_then(|params| params.get("item"))
        .and_then(|item| item.get("type"))
        .and_then(Value::as_str)
    {
        return format!("{method}:{item_type}");
    }
    method.to_string()
}

fn summarize_codex_provider_event(message: &JsonRpcMessage) -> Value {
    let mut out = serde_json::Map::new();
    if let Some(method) = message.method.as_deref() {
        out.insert("method".to_string(), Value::String(method.to_string()));
    }
    if let Some(id) = message.id {
        out.insert("id".to_string(), json!(id));
    }
    if message.error.is_some() {
        out.insert("has_error".to_string(), Value::Bool(true));
    }
    if let Some(params) = message.params.as_ref() {
        if method_is_token_usage(message.method.as_deref())
            && let Some(token_usage) = params.get("tokenUsage")
        {
            if let Some(last) = token_usage.get("last") {
                out.insert("last".to_string(), last.clone());
            }
            if let Some(window) = token_usage.get("modelContextWindow") {
                out.insert("model_context_window".to_string(), window.clone());
            }
        }
        if let Some(delta) = params.get("delta").and_then(Value::as_str) {
            out.insert("delta_chars".to_string(), json!(delta.chars().count()));
        }
        if let Some(status) = params.get("status").and_then(Value::as_str) {
            out.insert("status".to_string(), Value::String(status.to_string()));
        }
        for key in ["name", "error"] {
            if let Some(value) = params.get(key).and_then(Value::as_str) {
                out.insert(key.to_string(), Value::String(value.to_string()));
            }
        }
        if let Some(failure_reason) = params.get("failureReason") {
            out.insert("failure_reason".to_string(), failure_reason.clone());
        }
        if let Some(item) = params.get("item") {
            if let Some(item_type) = item.get("type").and_then(Value::as_str) {
                out.insert(
                    "item_type".to_string(),
                    Value::String(item_type.to_string()),
                );
            }
            if let Some(item_id) = item.get("id").and_then(Value::as_str) {
                out.insert("item_id".to_string(), Value::String(item_id.to_string()));
            }
            for key in ["tool", "name", "toolName", "server", "serverName", "status"] {
                if let Some(value) = item.get(key).and_then(Value::as_str) {
                    out.insert(key.to_string(), Value::String(value.to_string()));
                }
            }
        }
    }
    Value::Object(out)
}

fn method_is_token_usage(method: Option<&str>) -> bool {
    method == Some("thread/tokenUsage/updated")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codex_turn_usage_normalizes_cache_buckets_without_double_counting() {
        let usage = super::super::super::codex_app_server::TokenUsage {
            input_tokens: 10,
            cached_input_tokens: 3,
            cache_write_input_tokens: 2,
            reasoning_output_tokens: 1,
            output_tokens: 2,
            total_tokens: 12,
        };

        let projected = codex_turn_usage(Some(&usage), None, None, None, 17).expect("usage");

        assert_eq!(projected.duration_ms, 17);
        assert_eq!(projected.input_tokens, 5);
        assert_eq!(projected.cached_input_tokens, 3);
        assert_eq!(projected.cache_creation_input_tokens, 2);
        assert_eq!(projected.output_tokens, 2);
        assert_eq!(projected.total_tokens, 12);
    }

    #[test]
    fn codex_turn_usage_uses_latest_context_instead_of_cumulative_session_usage() {
        let latest = super::super::super::codex_app_server::TokenUsage {
            total_tokens: 42_000,
            ..Default::default()
        };
        let cumulative = super::super::super::codex_app_server::TokenUsage {
            total_tokens: 900_000,
            ..Default::default()
        };

        let projected = codex_turn_usage(Some(&latest), Some(&cumulative), Some(258_400), None, 17)
            .expect("usage");

        assert_eq!(projected.context_tokens, Some(42_000));
        assert_eq!(projected.context_window_tokens, Some(258_400));
    }

    #[test]
    fn token_usage_notifications_keep_the_live_context_snapshot() {
        let message: JsonRpcMessage = serde_json::from_value(json!({
            "method": "thread/tokenUsage/updated",
            "params": {
                "tokenUsage": {
                    "last": {
                        "inputTokens": 40_000,
                        "cachedInputTokens": 1_000,
                        "outputTokens": 2_000,
                        "totalTokens": 43_000
                    },
                    "modelContextWindow": 258_400
                }
            }
        }))
        .expect("valid notification");

        let summary = summarize_codex_provider_event(&message);

        assert_eq!(summary["last"]["totalTokens"], 43_000);
        assert_eq!(summary["model_context_window"], 258_400);
    }

    #[test]
    fn mcp_startup_failures_keep_the_server_and_reason() {
        let message: JsonRpcMessage = serde_json::from_value(json!({
            "method": "mcpServer/startupStatus/updated",
            "params": {
                "name": "document-tools",
                "status": "failed",
                "error": "server did not become ready",
                "failureReason": {
                    "kind": "startup_timeout"
                }
            }
        }))
        .expect("valid notification");

        let summary = summarize_codex_provider_event(&message);

        assert_eq!(summary["name"], "document-tools");
        assert_eq!(summary["status"], "failed");
        assert_eq!(summary["error"], "server did not become ready");
        assert_eq!(summary["failure_reason"]["kind"], "startup_timeout");
    }

    #[test]
    fn request_user_input_is_projected_as_a_provider_interaction() {
        let message: JsonRpcMessage = serde_json::from_value(json!({
            "id": 42,
            "method": "item/tool/requestUserInput",
            "params": {
                "threadId": "thread-1",
                "turnId": "turn-1",
                "itemId": "item-1",
                "questions": [{
                    "id": "scope",
                    "header": "Scope",
                    "question": "Which scope?",
                    "options": [{
                        "label": "Workspace",
                        "description": "Use the current workspace"
                    }]
                }]
            }
        }))
        .expect("valid request");
        let (tx, mut rx) = mpsc::channel(4);

        CodexStreamMapper::default()
            .handle(&message, &tx)
            .expect("request mapping");

        assert!(matches!(
            rx.try_recv().expect("provider event"),
            ChatStreamEvent::ProviderEvent { .. }
        ));
        match rx.try_recv().expect("interaction event") {
            ChatStreamEvent::ProviderInteractionRequested {
                interaction_id,
                kind,
                title,
                detail,
                ..
            } => {
                assert_eq!(interaction_id, "42");
                assert_eq!(kind, "user_input");
                assert_eq!(title, "Scope");
                assert_eq!(detail, "Which scope?");
            }
            event => panic!("unexpected event: {event:?}"),
        }
    }
}
