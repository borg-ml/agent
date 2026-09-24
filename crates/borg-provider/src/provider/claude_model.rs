//! Borg-owned Claude turns using the shared subscription model connector.

use std::{path::Path, time::Instant};

use anyhow::{Context, Result, ensure};
use borg_core::ModelProviderState;
use futures::StreamExt;
use serde_json::{Value, json};
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;

use super::{
    ModelMessage, ModelTurnRequest, ModelTurnResult, ProviderAttemptTrace, ProviderCallError,
    ProviderErrorKind, ProviderInvocation, ProviderProgress,
    anthropic_messages::{
        AnthropicStreamState, anthropic_error_kind, apply_stream_event, messages_request_body,
    },
    claude_connector::{Capabilities, Connector, MAX_FRAME_BYTES, MAX_RESPONSE_BYTES},
};
use crate::runtime::{CostBasis, elapsed_millis_u64};

#[derive(Debug, Clone)]
pub struct ClaudeModelProvider {
    pub model: String,
    pub effort: Option<String>,
}

impl ClaudeModelProvider {
    pub async fn context_window(&self, auth_directory: Option<&Path>) -> Result<u64> {
        let connector = Connector::connect(auth_directory).await?;
        Ok(connector
            .info(Some(&self.model))
            .await?
            .capabilities
            .context("missing Claude capabilities")?
            .context_window)
    }

    /// Complete model input and output belong to Borg. No upstream session,
    /// tool handler, approval callback, or native child is created here.
    pub async fn model_turn(
        &self,
        request: ModelTurnRequest,
        progress: Option<UnboundedSender<ProviderProgress>>,
        auth_directory: Option<&Path>,
        parent_agent_id: Option<&str>,
    ) -> std::result::Result<ModelTurnResult, ProviderCallError> {
        let trace = ProviderAttemptTrace {
            invocation: ProviderInvocation {
                provider_label: "claude-subscription".into(),
                executable: "shared-claude-model-connector".into(),
                args: Vec::new(),
                cwd: None,
                model: Some(self.model.clone()),
                effort: self.effort.clone(),
            },
            exit_status: None,
            stdout: String::new(),
            stderr: String::new(),
        };
        self.run(
            request,
            progress,
            auth_directory,
            parent_agent_id,
            trace.clone(),
        )
        .await
        .map_err(|error| {
            let kind = error
                .downcast_ref::<ModelFailure>()
                .map(|failure| failure.kind)
                .or_else(|| {
                    error.downcast_ref::<reqwest::Error>().map(|error| {
                        if error.status().is_some_and(|status| {
                            status.as_u16() == 429 || status.is_server_error()
                        }) {
                            ProviderErrorKind::ConnectionLost
                        } else {
                            ProviderErrorKind::from_transport(error)
                        }
                    })
                })
                .unwrap_or(ProviderErrorKind::Fatal);
            ProviderCallError {
                message: format!("{error:#}"),
                trace: Box::new(trace),
                session_id: None,
                kind,
            }
        })
    }

    async fn run(
        &self,
        request: ModelTurnRequest,
        progress: Option<UnboundedSender<ProviderProgress>>,
        auth_directory: Option<&Path>,
        parent_agent_id: Option<&str>,
        mut trace: ProviderAttemptTrace,
    ) -> Result<ModelTurnResult> {
        let started = Instant::now();
        let connector = Connector::connect(auth_directory).await?;
        let info = connector.info(Some(&self.model)).await?;
        let capabilities = info.capabilities.context("missing Claude capabilities")?;
        validate_replay_account(&request.messages, &info.account_identity)?;
        let body = subscription_request_body(
            &self.model,
            self.effort.as_deref(),
            &request,
            &capabilities,
        )?;
        let id = request
            .request_id
            .clone()
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let session = request
            .session_id
            .clone()
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        publish(
            &progress,
            "native_model_request",
            json!({
                "request_id": id, "session_id": session, "parent_agent_id": parent_agent_id,
                "connector_pid": info.pid, "account_identity": info.account_identity,
                "model": body["model"], "thinking": body.get("thinking"),
                "output_config": body.get("output_config"), "max_tokens": body["max_tokens"],
                "betas": body["betas"], "cache_ttl": "1h", "fast": request.fast,
                "auth": "subscription_oauth", "agent_loop": "borg",
            }),
        );
        let mut cancellation = connector.cancellation_guard(id.clone());
        let response = connector
            .infer(&json!({
                "id": id, "session_id": session, "parent_agent_id": parent_agent_id,
                "account_identity": info.account_identity, "body": body,
            }))
            .await?;
        let mut stream = response.bytes_stream();
        let mut buffer = Vec::new();
        let mut received = 0usize;
        let mut state = AnthropicStreamState::default();
        let mut complete = false;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            received = received.saturating_add(chunk.len());
            ensure!(
                received <= MAX_RESPONSE_BYTES,
                "Claude response exceeded the connector limit"
            );
            buffer.extend_from_slice(&chunk);
            let mut consumed = 0;
            while let Some(end) = buffer[consumed..].iter().position(|byte| *byte == b'\n') {
                let end = consumed + end;
                ensure!(
                    end - consumed <= MAX_FRAME_BYTES,
                    "Claude event exceeded the connector limit"
                );
                let frame: Value = serde_json::from_slice(&buffer[consumed..end])
                    .map_err(|_| ModelFailure::lost("Invalid Claude connector event"))?;
                ensure!(
                    frame["id"] == id,
                    "Claude connector returned another request's stream"
                );
                ensure!(!complete, "Claude connector sent data after completion");
                match frame["type"].as_str() {
                    Some("started") => {
                        ensure!(
                            frame["account_identity"] == info.account_identity,
                            "Claude connector changed account identity"
                        );
                        trace.invocation.args = vec![format!("pid={}", info.pid)];
                    }
                    Some("event") => {
                        let event = &frame["event"];
                        let kind = event["type"]
                            .as_str()
                            .context("Claude event omitted its type")?;
                        apply_stream_event(&mut state, kind, event, progress.as_ref());
                        if matches!(kind, "message_start" | "message_delta") {
                            publish_usage(&progress, &id, &state, false);
                        }
                        if let Some((message, kind)) = state.take_error() {
                            return Err(ModelFailure { message, kind }.into());
                        }
                    }
                    Some("done") => {
                        state
                            .validate_complete()
                            .map_err(|message| ModelFailure::lost(&message))?;
                        complete = true;
                    }
                    Some("cancelled") => {
                        return Err(ModelFailure::lost("Claude model request cancelled").into());
                    }
                    Some("error") => {
                        let message = frame["message"]
                            .as_str()
                            .unwrap_or("Claude model request failed")
                            .to_string();
                        let status = frame["status"].as_u64().unwrap_or(500) as u16;
                        return Err(ModelFailure {
                            kind: anthropic_error_kind(status, &message),
                            message,
                        }
                        .into());
                    }
                    _ => return Err(ModelFailure::lost("Unknown Claude connector event").into()),
                }
                consumed = end + 1;
            }
            buffer.drain(..consumed);
            ensure!(
                buffer.len() <= MAX_FRAME_BYTES,
                "Claude event exceeded the connector limit"
            );
        }
        if !complete || !buffer.is_empty() {
            return Err(ModelFailure::lost(
                "Claude connector disconnected before completing the response",
            )
            .into());
        }
        cancellation.complete();
        publish_usage(&progress, &id, &state, true);
        let mut message = state.assistant_message();
        if let ModelMessage::Assistant {
            provider_state:
                Some(ModelProviderState::AnthropicMessages {
                    account_identity, ..
                }),
            ..
        } = &mut message
        {
            *account_identity = Some(info.account_identity);
        }
        let mut usage = state.usage(elapsed_millis_u64(started));
        usage.context_window_tokens = Some(capabilities.context_window);
        usage.cost_basis = CostBasis::SubscriptionEquivalent;
        trace.exit_status = Some(0);
        Ok(ModelTurnResult {
            message,
            finish_reason: state.finish_reason(),
            usage,
            raw_response: state.raw_response(),
            trace,
        })
    }
}

fn validate_replay_account(messages: &[ModelMessage], account: &str) -> Result<()> {
    for message in messages {
        if let ModelMessage::Assistant {
            provider_state:
                Some(ModelProviderState::AnthropicMessages {
                    account_identity, ..
                }),
            ..
        } = message
        {
            ensure!(
                account_identity.as_deref() == Some(account),
                "Claude continuation belongs to a different or unknown account; use its original subscription or start a new conversation"
            );
        }
    }
    Ok(())
}

fn subscription_request_body(
    model: &str,
    effort: Option<&str>,
    request: &ModelTurnRequest,
    capabilities: &Capabilities,
) -> Result<Value> {
    let mut body = messages_request_body(model, None, request);
    body["model"] = json!(capabilities.model);
    body["max_tokens"] = json!(
        capabilities
            .default_output_tokens
            .min(capabilities.max_output_tokens)
    );
    let effort = effort.unwrap_or(crate::claude_default_effort());
    let disabled = matches!(effort, "none" | "off" | "minimal");
    ensure!(
        !disabled || !capabilities.thinking_required,
        "the selected Claude model requires thinking"
    );
    if disabled || !capabilities.thinking {
        body["thinking"] = json!({"type": "disabled"});
    } else if capabilities.adaptive_thinking {
        body["thinking"] = json!({"type": "adaptive"});
    } else {
        body["thinking"] = json!({"type": "enabled", "budget_tokens": 4096.min(capabilities.default_output_tokens.saturating_sub(1))});
    }
    if !disabled && !capabilities.efforts.is_empty() {
        ensure!(
            capabilities.efforts.iter().any(|level| level == effort),
            "effort {effort} is unavailable for the selected Claude model"
        );
        body["output_config"] = json!({"effort": effort});
    }
    let mut betas = capabilities.betas.clone();
    if body["output_config"].get("effort").is_some() {
        betas.push("effort-2025-11-24".into());
    }
    if let Some(schema) = &request.output_schema {
        if !body["output_config"].is_object() {
            body["output_config"] = json!({});
        }
        body["output_config"]["format"] = json!({"type": "json_schema", "schema": schema});
        betas.push("structured-outputs-2025-12-15".into());
    }
    if request.fast {
        ensure!(
            capabilities.fast,
            "fast mode is unavailable for the selected Claude model"
        );
        body["speed"] = json!("fast");
        betas.push("fast-mode-2026-02-01".into());
    }
    betas.push("extended-cache-ttl-2025-04-11".into());
    body["betas"] = json!(betas);
    // Borg chooses breakpoints and TTL; the connector never changes history.
    for field in ["tools", "system"] {
        if let Some(blocks) = body.get_mut(field).and_then(Value::as_array_mut) {
            for block in blocks {
                if block["cache_control"].is_object() {
                    block["cache_control"]["ttl"] = json!("1h");
                }
            }
        }
    }
    if let Some(messages) = body["messages"].as_array_mut() {
        for message in messages {
            if let Some(blocks) = message["content"].as_array_mut() {
                for block in blocks {
                    if block["cache_control"].is_object() {
                        block["cache_control"]["ttl"] = json!("1h");
                    }
                }
            }
        }
    }
    Ok(body)
}

fn publish(progress: &Option<UnboundedSender<ProviderProgress>>, kind: &str, payload: Value) {
    if let Some(progress) = progress {
        let _ = progress.send(ProviderProgress::ProviderEvent {
            kind: kind.into(),
            payload,
            raw_payload: Box::new(None),
            stream_channel: None,
            content_text: None,
            provider_item_id: None,
            tool_use_id: None,
            tool_name: None,
            model: None,
            effort: None,
        });
    }
}

fn publish_usage(
    progress: &Option<UnboundedSender<ProviderProgress>>,
    id: &str,
    state: &AnthropicStreamState,
    complete: bool,
) {
    // Snapshots, keyed by request_id. A cancelled stream's latest snapshot is
    // partial; it is never presented as a complete subscription debit.
    publish(
        progress,
        "native_model_usage",
        json!({
            "request_id": id, "protocol": "anthropic_messages", "complete": complete,
            "usage": state.raw_response().get("usage"),
        }),
    );
}

#[derive(Debug)]
struct ModelFailure {
    message: String,
    kind: ProviderErrorKind,
}
impl ModelFailure {
    fn lost(message: &str) -> Self {
        Self {
            message: message.into(),
            kind: ProviderErrorKind::ConnectionLost,
        }
    }
}
impl std::fmt::Display for ModelFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.message.fmt(f)
    }
}
impl std::error::Error for ModelFailure {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_continuation_requires_its_original_subscription_identity() {
        for origin in [None, Some("account-a"), Some("account-b")] {
            let message = ModelMessage::Assistant {
                content: None,
                reasoning_content: Some("private reasoning".into()),
                reasoning_details: None,
                tool_calls: Vec::new(),
                provider_state: Some(ModelProviderState::AnthropicMessages {
                    content: vec![
                        json!({"type":"thinking","thinking":"private reasoning","signature":"opaque-state"}),
                    ],
                    account_identity: origin.map(str::to_string),
                }),
            };
            let restored = serde_json::from_slice(&serde_json::to_vec(&message).unwrap()).unwrap();
            assert_eq!(
                validate_replay_account(&[restored], "account-a").is_ok(),
                origin == Some("account-a")
            );
        }
    }
}
