//! Append-only Messages transport; never replaces Borg's local history.
use borg_core::{ModelMessage, ModelProviderState, model::AnthropicMessageThread};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub(super) const BETA: &str = "message-threads-2026-08-12";

pub(super) fn enabled() -> bool {
    crate::env::nonempty_var("BORG_CLAUDE_MESSAGE_THREADS").as_deref() == Some("1")
}

fn hash(value: &Value) -> String {
    hex::encode(Sha256::digest(
        serde_json::to_vec(value).expect("JSON value"),
    ))
}

// Cache breakpoints move to the newest turn. Tool inputs are opaque user data.
fn strip_message_cache(messages: &mut Value) {
    fn blocks(content: &mut Value) {
        if let Some(content) = content.as_array_mut() {
            for block in content {
                if block["type"] == "tool_result" {
                    blocks(&mut block["content"]);
                }
                if let Some(block) = block.as_object_mut() {
                    block.remove("cache_control");
                }
            }
        }
    }
    if let Some(messages) = messages.as_array_mut() {
        for message in messages {
            blocks(&mut message["content"]);
        }
    }
}

pub(super) struct ThreadRequest {
    pub wire: Value,
    full: Value,
    cursor: AnthropicMessageThread,
}

impl ThreadRequest {
    pub fn new(mut full: Value, messages: &[ModelMessage], scope: Value) -> Self {
        let betas = full
            .as_object_mut()
            .unwrap()
            .entry("betas")
            .or_insert(json!([]));
        if !betas.as_array().unwrap().iter().any(|beta| beta == BETA) {
            betas.as_array_mut().unwrap().push(json!(BETA));
        }
        // Replays must not accumulate the breakpoints of earlier responses.
        let mut history = full["messages"].clone();
        let newest = history.as_array_mut().unwrap().pop();
        strip_message_cache(&mut history);
        if let Some(newest) = newest {
            history.as_array_mut().unwrap().push(newest);
        }
        full["messages"] = history;
        let mut settings = full.clone();
        for field in [
            "messages",
            "max_tokens",
            "stream",
            "metadata",
            "stop_sequences",
            "service_tier",
        ] {
            settings.as_object_mut().unwrap().remove(field);
        }
        let settings_hash = hash(&settings);
        let prompt_hash = hash(&json!([full.get("system"), full.get("tools")]));
        let scope_hash = hash(&scope);
        let mut normalized = full["messages"].clone();
        strip_message_cache(&mut normalized);
        let prior = messages
            .iter()
            .rev()
            .find_map(|message| match message {
                ModelMessage::Assistant { provider_state, .. } => Some(provider_state),
                _ => None,
            })
            .and_then(|state| match state {
                Some(ModelProviderState::AnthropicMessages { thread, .. }) => thread.as_ref(),
                _ => None,
            })
            .filter(|prior| prior.scope_hash == scope_hash);
        let mut cursor = AnthropicMessageThread {
            previous_message_id: None,
            prefix_hash: String::new(),
            message_count: 0,
            strip_replay_thinking: prior.is_some_and(|prior| {
                prior.strip_replay_thinking || prior.prompt_hash != prompt_hash
            }),
            prompt_hash,
            settings_hash: settings_hash.clone(),
            scope_hash,
            replayed: false,
            disabled: prior
                .is_some_and(|prior| prior.disabled && prior.settings_hash == settings_hash),
        };
        let mut wire = full.clone();
        if !cursor.disabled {
            wire["thread"] = json!({"type":"create"});
            if let Some(prior) = prior.filter(|prior| {
                prior.previous_message_id.is_some() && prior.settings_hash == cursor.settings_hash
            }) {
                let all = normalized.as_array().unwrap();
                if prior.message_count <= all.len()
                    && hash(&json!(&all[..prior.message_count])) == prior.prefix_hash
                {
                    wire["thread"] =
                        json!({"type":"continue", "previous_message_id":prior.previous_message_id});
                    wire["messages"] =
                        json!(&full["messages"].as_array().unwrap()[prior.message_count..]);
                    wire.as_object_mut().unwrap().remove("system");
                    wire.as_object_mut().unwrap().remove("tools");
                    cursor.replayed = prior.replayed;
                }
            }
        }
        let mut plan = Self { wire, full, cursor };
        if plan.wire["thread"]["type"] != "continue" {
            plan.strip_invalid_thinking();
        }
        plan
    }

    /// Only structured thread errors qualify; an unknown model's 404 is not expiry.
    pub fn recover(&mut self, status: u16, raw: &str) -> bool {
        let code = serde_json::from_str::<Value>(raw).ok().and_then(|v| {
            v["error"]["details"]["error_code"]
                .as_str()
                .map(str::to_owned)
        });
        let continuing = self.wire["thread"]["type"] == "continue";
        let unavailable = matches!(
            (status, code.as_deref()),
            (404, Some("thread_not_found"))
                | (
                    400,
                    Some(
                        "thread_already_continued"
                            | "thread_continue_in_flight"
                            | "thread_fingerprint_mismatch"
                    )
                )
        );
        if self.wire.get("thread").is_none()
            || !(status == 400 && code.as_deref() == Some("thread_unsupported_request")
                || continuing && unavailable)
        {
            return false;
        }
        self.cursor.disabled = code.as_deref() == Some("thread_unsupported_request")
            || code.as_deref() == Some("thread_not_found") && self.cursor.replayed;
        self.cursor.replayed = true;
        self.wire = self.full.clone();
        if !self.cursor.disabled {
            self.wire["thread"] = json!({"type":"create"});
        }
        self.strip_invalid_thinking();
        true
    }

    // After a system/tool edit old signed reasoning is invalid. Keep it in
    // local history for prefix validation, but leave it out of create replays.
    fn strip_invalid_thinking(&mut self) {
        if !self.cursor.strip_replay_thinking {
            return;
        }
        let mut turns: Vec<Value> = Vec::new();
        for mut message in self.wire["messages"].as_array().unwrap().iter().cloned() {
            if message["role"] == "assistant" {
                let blocks = message["content"].as_array_mut().unwrap();
                blocks.retain(|b| {
                    !matches!(b["type"].as_str(), Some("thinking" | "redacted_thinking"))
                });
                if blocks.is_empty() {
                    continue;
                }
            }
            if let Some(last) = turns
                .last_mut()
                .filter(|last| last["role"] == message["role"])
            {
                last["content"]
                    .as_array_mut()
                    .unwrap()
                    .extend(message["content"].as_array().unwrap().iter().cloned());
            } else {
                turns.push(message);
            }
        }
        self.wire["messages"] = json!(turns);
    }

    /// Called only after message_stop and complete-stream validation.
    pub fn finish(mut self, response: &Value, message: &mut ModelMessage) {
        let Some(ModelProviderState::AnthropicMessages { thread, .. }) = (match message {
            ModelMessage::Assistant { provider_state, .. } => provider_state.as_mut(),
            _ => None,
        }) else {
            return;
        };
        let mut prefix = self.full["messages"].clone();
        prefix
            .as_array_mut()
            .unwrap()
            .push(json!({"role":"assistant", "content":response["content"]}));
        strip_message_cache(&mut prefix);
        self.cursor.message_count = prefix.as_array().unwrap().len();
        self.cursor.prefix_hash = hash(&prefix);
        let fallback = response["usage"]["iterations"]
            .as_array()
            .is_some_and(|iterations| iterations.iter().any(|i| i["type"] == "fallback_message"))
            || response["content"]
                .as_array()
                .is_some_and(|blocks| blocks.iter().any(|b| b["type"] == "fallback"));
        if !self.cursor.disabled && !fallback && response["stop_reason"] != "refusal" {
            self.cursor.previous_message_id = response["id"].as_str().map(str::to_owned);
        }
        self.cursor.replayed &= self.wire["thread"]["type"] == "create";
        *thread = Some(Box::new(self.cursor));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::anthropic_messages::messages_request_body;
    use borg_core::{ModelToolCall, ModelTurnRequest};

    fn body(messages: &[ModelMessage]) -> Value {
        messages_request_body(
            "claude-opus-5-5",
            None,
            &ModelTurnRequest {
                messages: messages.to_vec(),
                tools: vec![],
                fast: false,
                ultrafast: false,
                request_id: None,
                session_id: Some("session".into()),
                prompt_cache_key: None,
                turn_routing: Default::default(),
                output_schema: None,
            },
        )
    }

    fn response(id: &str) -> Value {
        json!({"id":id, "content":[
            {"type":"thinking", "thinking":"signed", "signature":"opaque"},
            {"type":"tool_use", "id":"call", "name":"exec", "input":{"cache_control":"user data"}}
        ], "stop_reason":"tool_use"})
    }

    fn append(messages: &mut Vec<ModelMessage>, plan: ThreadRequest, response: Value) {
        let mut message = ModelMessage::Assistant {
            content: None,
            reasoning_content: Some("signed".into()),
            reasoning_details: None,
            provider_state: Some(ModelProviderState::AnthropicMessages {
                thread: None,
                content: response["content"].as_array().unwrap().clone(),
                narration_block_indexes: None,
                account_identity: None,
            }),
            tool_calls: vec![ModelToolCall::function(
                "call".into(),
                "exec".into(),
                json!({"cache_control":"user data"}).to_string(),
            )],
        };
        plan.finish(&response, &mut message);
        // The cursor must survive durable serialization without storing credentials.
        let persisted = serde_json::to_string(&message).unwrap();
        assert!(!persisted.contains("secret"));
        messages.push(serde_json::from_str(&persisted).unwrap());
    }

    fn conversation() -> Vec<ModelMessage> {
        let mut messages = vec![
            ModelMessage::System {
                content: "instructions".into(),
            },
            ModelMessage::user("start"),
        ];
        let plan = ThreadRequest::new(body(&messages), &messages, json!("secret"));
        assert_eq!(plan.wire["thread"]["type"], "create");
        append(&mut messages, plan, response("msg_1"));
        messages.push(ModelMessage::tool("call", "done"));
        messages
    }

    #[test]
    fn sends_only_new_tool_results_and_replays_after_expiry() {
        let messages = conversation();
        let full = body(&messages);
        let mut plan = ThreadRequest::new(full.clone(), &messages, json!("secret"));
        assert_eq!(
            plan.wire["thread"],
            json!({"type":"continue", "previous_message_id":"msg_1"})
        );
        assert!(plan.wire.get("system").is_none());
        assert!(plan.wire.get("tools").is_none());
        assert_eq!(plan.wire["messages"], json!([full["messages"][2]]));
        assert!(plan.recover(404, &error("thread_not_found")));
        assert_eq!(plan.wire["thread"]["type"], "create");
        assert_eq!(plan.wire["messages"], full["messages"]);
        assert_eq!(plan.wire["system"], full["system"]);
        assert_eq!(
            plan.wire["messages"][1]["content"][0]["signature"],
            "opaque"
        );
        assert_eq!(
            plan.wire["messages"][1]["content"][1]["input"]["cache_control"],
            "user data"
        );
        assert!(!plan.recover(404, &error("thread_not_found")));
    }

    fn error(code: &str) -> String {
        json!({"error":{"details":{"error_code":code}}}).to_string()
    }

    #[test]
    fn edits_compaction_settings_and_ownership_start_new_threads() {
        let messages = conversation();
        for change in [
            "history",
            "system",
            "tools",
            "thinking",
            "model",
            "scope",
            "compaction",
        ] {
            let mut changed = messages.clone();
            if change == "history" {
                changed[1] = ModelMessage::user("edited");
            }
            if change == "compaction" {
                changed.remove(1);
            }
            let mut full = body(&changed);
            if matches!(change, "system" | "tools" | "thinking" | "model") {
                full[change] = json!("changed");
            }
            let scope = if change == "scope" { "other" } else { "secret" };
            let plan = ThreadRequest::new(full, &changed, json!(scope));
            assert_eq!(plan.wire["thread"]["type"], "create", "{change}");
        }
    }

    #[test]
    fn recovery_is_specific_bounded_and_disables_unstorable_threads() {
        let mut messages = conversation();
        for (status, code) in [
            (404, "model_not_found"),
            (401, "thread_not_found"),
            (429, "thread_not_found"),
        ] {
            let mut plan = ThreadRequest::new(body(&messages), &messages, json!("secret"));
            assert!(!plan.recover(status, &error(code)));
        }
        for code in [
            "thread_already_continued",
            "thread_continue_in_flight",
            "thread_fingerprint_mismatch",
        ] {
            let mut plan = ThreadRequest::new(body(&messages), &messages, json!("secret"));
            assert!(plan.recover(400, &error(code)));
            assert_eq!(plan.wire["thread"]["type"], "create");
            assert!(!plan.recover(400, &error(code)));
        }
        let mut plan = ThreadRequest::new(body(&messages), &messages, json!("secret"));
        assert!(plan.recover(404, &error("thread_not_found")));
        append(&mut messages, plan, response("msg_2"));
        messages.push(ModelMessage::tool("call", "next"));
        let mut plan = ThreadRequest::new(body(&messages), &messages, json!("secret"));
        assert_eq!(plan.wire["thread"]["type"], "continue");
        assert!(plan.recover(404, &error("thread_not_found")));
        assert!(plan.wire.get("thread").is_none());
        assert!(!plan.recover(400, &error("thread_unsupported_request")));
        append(&mut messages, plan, response("msg_3"));
        messages.push(ModelMessage::tool("call", "next"));
        let plan = ThreadRequest::new(body(&messages), &messages, json!("secret"));
        assert!(plan.wire.get("thread").is_none());
    }

    #[test]
    fn prompt_edits_strip_invalid_reasoning_on_replay_but_still_continue() {
        let mut messages = conversation();
        messages[0] = ModelMessage::System {
            content: "new instructions".into(),
        };
        let plan = ThreadRequest::new(body(&messages), &messages, json!("secret"));
        assert_eq!(plan.wire["thread"]["type"], "create");
        assert_eq!(
            plan.wire["messages"][1]["content"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(plan.wire["messages"][1]["content"][0]["type"], "tool_use");
        append(&mut messages, plan, response("msg_2"));
        messages.push(ModelMessage::tool("call", "next"));
        let mut plan = ThreadRequest::new(body(&messages), &messages, json!("secret"));
        assert_eq!(plan.wire["thread"]["type"], "continue");
        assert_eq!(plan.wire["messages"].as_array().unwrap().len(), 1);
        assert!(plan.recover(404, &error("thread_not_found")));
        for message in plan.wire["messages"].as_array().unwrap() {
            assert!(
                message["content"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|b| b["type"] != "thinking")
            );
        }
        // The signed local state is never rewritten by transport preparation.
        assert!(
            body(&messages)["messages"][1]["content"]
                .as_array()
                .unwrap()
                .iter()
                .any(|b| b["type"] == "thinking")
        );
    }

    #[test]
    fn unsupported_threads_fall_back_once_and_pause_turn_can_send_no_messages() {
        let mut messages = conversation();
        messages.pop();
        let mut plan = ThreadRequest::new(body(&messages), &messages, json!("secret"));
        assert_eq!(plan.wire["thread"]["type"], "continue");
        assert_eq!(plan.wire["messages"], json!([]));
        assert!(plan.recover(400, &error("thread_unsupported_request")));
        assert!(plan.wire.get("thread").is_none());
        assert_eq!(plan.wire["messages"], body(&messages)["messages"]);
        assert!(!plan.recover(400, &error("thread_unsupported_request")));
    }

    #[test]
    fn refusals_fallbacks_and_missing_ids_do_not_publish_a_continuation() {
        for kind in ["refusal", "fallback", "missing_id"] {
            let mut messages = conversation();
            let plan = ThreadRequest::new(body(&messages), &messages, json!("secret"));
            let mut reply = response("msg_2");
            match kind {
                "refusal" => reply["stop_reason"] = json!("refusal"),
                "fallback" => reply["usage"] = json!({"iterations":[{"type":"fallback_message"}]}),
                _ => {
                    reply.as_object_mut().unwrap().remove("id");
                }
            }
            append(&mut messages, plan, reply);
            messages.push(ModelMessage::tool("call", "next"));
            let plan = ThreadRequest::new(body(&messages), &messages, json!("secret"));
            assert_eq!(plan.wire["thread"]["type"], "create", "{kind}");
        }
    }
}
