//! Borg-owned Claude turns using direct entitled subscription HTTP/SSE.

use std::{path::Path, time::Instant};

use anyhow::{Context, Result, ensure};
use borg_core::ModelProviderState;
use futures::StreamExt;
use serde_json::{Value, json};
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;

use super::{
    ModelMessage, ModelTurnRequest, ModelTurnResult, PromptCacheRefresh, ProviderAttemptTrace,
    ProviderCallError, ProviderErrorKind, ProviderInvocation, ProviderProgress,
    anthropic_messages::{
        AnthropicStreamState, anthropic_error_kind, apply_stream_event, messages_request_body,
    },
    claude_connector::Capabilities,
    claude_native_client::NativeConnector as Connector,
    claude_subscription_protocol,
    claude_subscription_transport::{self, MAX_FRAME_BYTES, MAX_RESPONSE_BYTES, SseFrame},
};
use crate::runtime::{CostBasis, elapsed_millis_u64};

#[derive(Debug, Clone)]
pub struct ClaudeModelProvider {
    pub model: String,
    pub effort: Option<String>,
    /// Let Claude answer a refused request with its refusal-fallback model.
    pub refusal_fallback: bool,
}

const SERVER_SIDE_FALLBACK_BETA: &str = "server-side-fallback-2026-06-01";

/// The model Claude answers a refused request with, for the models that
/// offer a server-side refusal fallback.
pub fn claude_refusal_fallback_model(model: &str) -> Option<&'static str> {
    let model = model.trim().to_ascii_lowercase();
    matches!(
        model.as_str(),
        "claude-sonnet-5-5"
            | "claude-opus-5"
            | "claude-opus-5-5"
            | "claude-fable-5"
            | "claude-fable-5-1"
    )
    .then_some("claude-opus-4-8")
}

/// Ask for the refusal fallback when enabled. A replayed fallback block needs
/// the beta whether or not the option is still on.
fn apply_refusal_fallback(body: &mut Value, enabled: bool) {
    let target = enabled
        .then(|| claude_refusal_fallback_model(body["model"].as_str().unwrap_or_default()))
        .flatten();
    if let Some(target) = target {
        body["fallbacks"] = json!([{ "model": target }]);
    }
    let replayed = body["messages"].as_array().is_some_and(|messages| {
        messages.iter().any(|message| {
            message["content"]
                .as_array()
                .is_some_and(|blocks| blocks.iter().any(|block| block["type"] == "fallback"))
        })
    });
    if (target.is_some() || replayed)
        && let Some(betas) = body["betas"].as_array_mut()
    {
        betas.push(json!(SERVER_SIDE_FALLBACK_BETA));
    }
}

impl ClaudeModelProvider {
    pub async fn account_identity(auth_directory: Option<&Path>) -> Result<String> {
        Ok(Connector::connect(auth_directory)
            .await?
            .info(None)
            .await?
            .account_identity)
    }

    pub async fn context_window(&self, auth_directory: Option<&Path>) -> Result<u64> {
        let connector = Connector::connect(auth_directory).await?;
        Ok(connector
            .info(Some(&self.model))
            .await?
            .capabilities
            .context("missing Claude capabilities")?
            .context_window)
    }

    /// Whether the selected model offers fast mode on these credentials.
    pub async fn supports_fast(&self, auth_directory: Option<&Path>) -> Result<bool> {
        Ok(Connector::connect(auth_directory)
            .await?
            .info(Some(&self.model))
            .await?
            .capabilities
            .context("missing Claude capabilities")?
            .fast)
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
        self.model_turn_for_account(request, progress, auth_directory, parent_agent_id, None)
            .await
    }

    pub async fn model_turn_for_account(
        &self,
        request: ModelTurnRequest,
        progress: Option<UnboundedSender<ProviderProgress>>,
        auth_directory: Option<&Path>,
        parent_agent_id: Option<&str>,
        expected_account: Option<&str>,
    ) -> std::result::Result<ModelTurnResult, ProviderCallError> {
        self.model_turn_capped(
            request,
            progress,
            auth_directory,
            parent_agent_id,
            expected_account,
            None,
        )
        .await
    }

    /// Refresh through the same entitled subscription transport; never execute the reply.
    pub async fn refresh_prompt_cache_for_account(
        &self,
        request: ModelTurnRequest,
        auth_directory: Option<&Path>,
        parent_agent_id: Option<&str>,
        expected_account: &str,
        refresh: PromptCacheRefresh,
    ) -> std::result::Result<crate::ProviderCallUsage, ProviderCallError> {
        self.model_turn_capped(
            request,
            None,
            auth_directory,
            parent_agent_id,
            Some(expected_account),
            Some(refresh),
        )
        .await
        .map(|result| result.usage)
    }

    async fn model_turn_capped(
        &self,
        request: ModelTurnRequest,
        progress: Option<UnboundedSender<ProviderProgress>>,
        auth_directory: Option<&Path>,
        parent_agent_id: Option<&str>,
        expected_account: Option<&str>,
        refresh: Option<PromptCacheRefresh>,
    ) -> std::result::Result<ModelTurnResult, ProviderCallError> {
        let trace = ProviderAttemptTrace {
            invocation: ProviderInvocation {
                provider_label: "claude-subscription".into(),
                executable: claude_subscription_transport::ENDPOINT.into(),
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
            expected_account,
            trace.clone(),
            refresh,
        )
        .await
        .map_err(|error| {
            let throttled = error
                .chain()
                .any(|cause| cause.is::<crate::claude_subscription::Throttled>());
            let kind = error
                .downcast_ref::<ModelFailure>()
                .map(|failure| failure.kind)
                .or(throttled.then_some(ProviderErrorKind::ConnectionLost))
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

    #[allow(clippy::too_many_arguments)]
    async fn run(
        &self,
        request: ModelTurnRequest,
        progress: Option<UnboundedSender<ProviderProgress>>,
        auth_directory: Option<&Path>,
        parent_agent_id: Option<&str>,
        _expected_account: Option<&str>,
        mut trace: ProviderAttemptTrace,
        refresh: Option<PromptCacheRefresh>,
    ) -> Result<ModelTurnResult> {
        let started = Instant::now();
        let connector = Connector::connect(auth_directory).await?;
        let info = connector.info(Some(&self.model)).await?;
        let capabilities = info.capabilities.context("missing Claude capabilities")?;
        let mut body = subscription_request_body(
            &self.model,
            self.effort.as_deref(),
            &request,
            &capabilities,
            refresh,
        )?;
        apply_refusal_fallback(&mut body, self.refusal_fallback);
        let id = if refresh.is_some() {
            Uuid::new_v4().to_string()
        } else {
            request
                .request_id
                .clone()
                .unwrap_or_else(|| Uuid::new_v4().to_string())
        };
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
        let response = connector
            .post(&body, &id, &session, parent_agent_id)
            .await?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let mut response = response;
            let mut details = Vec::new();
            while details.len() < 16 * 1024 {
                let Some(chunk) = response.chunk().await? else {
                    break;
                };
                let remaining = 16 * 1024 - details.len();
                details.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
            }
            let details = String::from_utf8_lossy(&details);
            let message = format!(
                "Claude subscription request failed (HTTP {status}){}: {details}; no API-key fallback",
                if status == 429 { " rate_limit" } else { "" },
            );
            return Err(ModelFailure {
                // Subscription quota refusals must reach the session's route
                // fallback, not the transport reconnection retry loop.
                kind: subscription_error_kind(status, &message),
                message,
            }
            .into());
        }
        super::chat_stream::record_claude_usage_headers(&info.account_identity, response.headers());
        if let Some((five_hour, weekly)) = claude_subscription_transport::grace(response.headers())
        {
            publish(
                &progress,
                "claude_usage_grace",
                json!({"five_hour":five_hour,"weekly":weekly}),
            );
        }
        let mut stream = response.bytes_stream();
        let mut buffer = Vec::new();
        let mut received = 0usize;
        let mut state = AnthropicStreamState::claude_summaries();
        let mut complete = false;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            received = received.saturating_add(chunk.len());
            ensure!(
                received <= MAX_RESPONSE_BYTES,
                "Claude response exceeded the stream limit"
            );
            buffer.extend_from_slice(&chunk);
            while let Some(frame) = claude_subscription_transport::take_event(&mut buffer)
                .map_err(|_| ModelFailure::lost("Invalid Claude subscription event"))?
            {
                let SseFrame::Event(mut event) = frame else {
                    continue;
                };
                let kind = event["type"]
                    .as_str()
                    .context("Claude event omitted its type")?
                    .to_owned();
                let kind = kind.as_str();
                if kind == "content_block_start" && event["content_block"]["type"] == "fallback" {
                    let block = &event["content_block"];
                    publish(
                        &progress,
                        "model_refusal_fallback",
                        json!({
                            "request_id": id,
                            "from": block["from"]["model"],
                            "to": block["to"]["model"],
                            "category": block["trigger"]["category"],
                        }),
                    );
                    // Replay carries the switch, not the server's trigger detail.
                    event["content_block"] = json!({
                        "type": "fallback", "from": block["from"], "to": block["to"],
                    });
                }
                if kind == "ping" {
                    continue;
                }
                ensure!(!complete, "Claude sent data after message completion");
                if kind == "content_block_stop" {
                    let raw = state.raw_response();
                    let indexes = claude_subscription_protocol::narration_block_indexes(
                        raw["content"]
                            .as_array()
                            .map(Vec::as_slice)
                            .unwrap_or_default(),
                    );
                    state.set_narration_block_indexes(&json!(indexes));
                }
                apply_stream_event(&mut state, kind, &event, progress.as_ref());
                if matches!(kind, "message_start" | "message_delta") {
                    publish_usage(&progress, &id, &state, false);
                }
                if let Some((message, kind)) = state.take_error() {
                    return Err(ModelFailure { message, kind }.into());
                }
                if kind == "message_stop" {
                    state
                        .validate_complete()
                        .map_err(|message| ModelFailure::lost(&message))?;
                    complete = true;
                }
            }
            ensure!(
                buffer.len() <= MAX_FRAME_BYTES,
                "Claude event exceeded the stream limit"
            );
        }
        if !complete || !buffer.is_empty() {
            return Err(
                ModelFailure::lost("Claude disconnected before completing the response").into(),
            );
        }
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

fn subscription_request_body(
    model: &str,
    effort: Option<&str>,
    request: &ModelTurnRequest,
    capabilities: &Capabilities,
    refresh: Option<PromptCacheRefresh>,
) -> Result<Value> {
    anyhow::ensure!(!request.ultrafast, "Claude does not support ultrafast mode");
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
    // Claude Code's default display: narration, the between-tool answers the
    // user must see, arrives verbatim while reasoning stays omitted. Unset
    // display omits narration too, and "summarized" rewrites it.
    let updates = capabilities
        .betas
        .iter()
        .any(|beta| beta == claude_subscription_protocol::DISPLAY_UPDATES_BETA);
    if updates && body["thinking"]["type"] != "disabled" {
        body["thinking"]["display"] = json!("updates");
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
    if let Some(refresh) = refresh {
        ensure!(
            body["thinking"]["type"] != "enabled",
            "budget-based thinking cannot be replayed under a cache refresh output cap"
        );
        ensure!(
            refresh.max_output_tokens > 0,
            "cache refresh output cap must be positive"
        );
        body["max_tokens"] = json!(
            refresh
                .max_output_tokens
                .min(capabilities.max_output_tokens)
        );
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
            "response_id": state.raw_response().get("id"),
            "usage": state.raw_response().get("usage"),
        }),
    );
}

fn subscription_error_kind(status: u16, message: &str) -> ProviderErrorKind {
    if status == 429 {
        ProviderErrorKind::Fatal
    } else {
        anthropic_error_kind(status, message)
    }
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

    // Subscription quota exhaustion is a route refusal, not a broken
    // connection: reconnect retries must not prevent the configured fallback.
    #[test]
    fn subscription_quota_refusal_is_not_reconnectable() {
        assert_eq!(
            subscription_error_kind(429, "rate_limit_error: five-hour usage limit reached"),
            ProviderErrorKind::Fatal
        );
        assert_eq!(
            subscription_error_kind(529, "overloaded_error"),
            ProviderErrorKind::ConnectionLost
        );
    }

    // Off sends no fallback; a replayed fallback block keeps its beta, or
    // turning the option off would break every later request in the session.
    #[test]
    fn refusal_fallback_is_opt_in_and_replay_keeps_its_beta() {
        let body = || json!({"model": "claude-opus-5-5", "betas": [], "messages": []});
        let mut off = body();
        apply_refusal_fallback(&mut off, false);
        assert_eq!(off, body());
        let mut on = body();
        apply_refusal_fallback(&mut on, true);
        assert_eq!(on["fallbacks"], json!([{"model": "claude-opus-4-8"}]));
        assert_eq!(on["betas"], json!([SERVER_SIDE_FALLBACK_BETA]));
        let mut replay = body();
        replay["messages"] = json!([{"role": "assistant", "content": [
            {"type": "fallback", "from": {"model": "claude-opus-5-5"}, "to": {"model": "claude-opus-4-8"}}
        ]}]);
        apply_refusal_fallback(&mut replay, false);
        assert!(replay.get("fallbacks").is_none());
        assert_eq!(replay["betas"], json!([SERVER_SIDE_FALLBACK_BETA]));
    }

    #[test]
    fn subscription_refresh_preserves_prefix_and_rejects_budget_thinking() {
        let mut capabilities = Capabilities {
            model: "claude-test".into(),
            context_window: 200_000,
            default_output_tokens: 8192,
            max_output_tokens: 16384,
            thinking: true,
            adaptive_thinking: true,
            thinking_required: false,
            fast: false,
            efforts: vec!["high".into()],
            betas: vec![claude_subscription_protocol::DISPLAY_UPDATES_BETA.into()],
        };
        let request = ModelTurnRequest {
            fast: false,
            ultrafast: false,
            request_id: Some("real-request".into()),
            session_id: Some("session".into()),
            prompt_cache_key: Some("cache-key".into()),
            turn_routing: Default::default(),
            messages: vec![ModelMessage::user("Retain this conversation unchanged")],
            tools: vec![
                crate::provider::ModelToolDefinition::new(
                    "read",
                    "Read",
                    json!({"type":"object","properties":{}}),
                )
                .unwrap(),
            ],
            output_schema: None,
        };
        let original =
            subscription_request_body("claude-test", Some("high"), &request, &capabilities, None)
                .unwrap();
        let mut warmed = subscription_request_body(
            "claude-test",
            Some("high"),
            &request,
            &capabilities,
            Some(PromptCacheRefresh::ONE_TOKEN),
        )
        .unwrap();
        assert_eq!(original["thinking"]["display"], "updates");
        assert_eq!(warmed["max_tokens"], 1);
        assert_eq!(warmed["thinking"]["type"], "adaptive");
        warmed["max_tokens"] = original["max_tokens"].clone();
        assert_eq!(
            warmed, original,
            "warming must preserve every cache-keyed request field"
        );
        capabilities.adaptive_thinking = false;
        assert!(
            subscription_request_body(
                "claude-test",
                Some("high"),
                &request,
                &capabilities,
                Some(PromptCacheRefresh::ONE_TOKEN)
            )
            .is_err()
        );
        assert!(
            subscription_request_body(
                "claude-test",
                Some("off"),
                &request,
                &capabilities,
                Some(PromptCacheRefresh::ONE_TOKEN)
            )
            .is_ok()
        );
    }
}
