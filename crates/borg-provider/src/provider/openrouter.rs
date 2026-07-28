use std::time::Instant;

use async_trait::async_trait;
use futures::StreamExt;
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::sync::mpsc::UnboundedSender;

use crate::env::bool_var;
use crate::runtime::elapsed_millis_u64;

use super::{
    ChatCompletionResponseFormat, Provider, ProviderAttemptTrace, ProviderCallError,
    ProviderCallResult, ProviderInvocation, ProviderProgress, StructuredOutputDialect,
    apply_provider_request_timeout, chat_completion_response_format,
    extract_chat_completions_usage, nonempty_env, parse_chat_completion_json_text,
    provider_cost_usd_to_microusd, read_provider_error_response_text,
    read_provider_success_response_text, trace_from_buffers, truncate_provider_text,
};

const OPENROUTER_CHAT_COMPLETIONS_URL: &str = "https://openrouter.ai/api/v1/chat/completions";

#[derive(Debug, Clone)]
pub struct OpenRouterProvider {
    pub model: String,
    pub effort: Option<String>,
    pub system_prompt: &'static str,
}

#[async_trait]
impl Provider for OpenRouterProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn structured_call_with_progress(
        &self,
        prompt: &str,
        schema: &Value,
        _session_id: Option<&str>,
        progress: Option<UnboundedSender<ProviderProgress>>,
    ) -> std::result::Result<ProviderCallResult, ProviderCallError> {
        self.call(prompt, Some(schema), progress).await
    }

    async fn freeform_call_with_progress(
        &self,
        prompt: &str,
        _session_id: Option<&str>,
        progress: Option<UnboundedSender<ProviderProgress>>,
    ) -> std::result::Result<ProviderCallResult, ProviderCallError> {
        self.call(prompt, None, progress).await
    }

    fn label(&self) -> &'static str {
        "openrouter"
    }

    fn structured_output_dialect(&self) -> StructuredOutputDialect {
        StructuredOutputDialect::FlexibleJson
    }
}

impl OpenRouterProvider {
    async fn call(
        &self,
        prompt: &str,
        schema: Option<&Value>,
        progress: Option<UnboundedSender<ProviderProgress>>,
    ) -> std::result::Result<ProviderCallResult, ProviderCallError> {
        let started_at = Instant::now();
        let mut trace = self.trace(None, "", "");

        let Some(api_key) = nonempty_env("OPENROUTER_API_KEY") else {
            return Err(ProviderCallError {
                message: "OPENROUTER_API_KEY is not set".to_string(),
                trace,
                session_id: None,
            });
        };

        let mut messages = Vec::new();
        if !self.system_prompt.trim().is_empty() {
            messages.push(json!({ "role": "system", "content": self.system_prompt }));
        }
        messages.push(json!({ "role": "user", "content": prompt }));

        let mut body = json!({
            "model": self.model,
            "messages": messages,
            "stream": true,
            "stream_options": { "include_usage": true },
        });
        if let Some(provider) = openrouter_provider_preferences() {
            body["provider"] = provider;
        }
        if let Some(reasoning) = openrouter_reasoning(self.effort.as_deref()) {
            body["reasoning"] = reasoning;
        }
        if let Some(schema) = schema {
            let response_format_env =
                nonempty_env("BORG_OPENROUTER_RESPONSE_FORMAT").map(|value| value.to_lowercase());
            if let Some(format) = response_format_env {
                body["response_format"] = if format == "json_object" {
                    chat_completion_response_format(
                        schema,
                        ChatCompletionResponseFormat::JsonObject,
                    )
                } else {
                    chat_completion_response_format(
                        schema,
                        ChatCompletionResponseFormat::JsonSchema,
                    )
                };
            }
            // When BORG_OPENROUTER_RESPONSE_FORMAT is unset, skip response_format
            // entirely. Some providers (e.g. DeepSeek) reject it.
        }

        let client = reqwest::Client::new();
        let mut attempt = 0_u32;
        let response = loop {
            attempt += 1;
            let request = client
                .post(OPENROUTER_CHAT_COMPLETIONS_URL)
                .bearer_auth(&api_key)
                .header("HTTP-Referer", "https://borg.ml")
                .header("X-Title", "Borg")
                .json(&body);
            match apply_provider_request_timeout(request).send().await {
                Ok(response) if attempt < 3 && openrouter_retryable_status(response.status()) => {
                    let delay = openrouter_retry_delay(&response, attempt);
                    tracing::warn!(
                        status = response.status().as_u16(),
                        attempt,
                        retry_delay_ms = delay.as_millis(),
                        "retrying OpenRouter provider response"
                    );
                    tokio::time::sleep(delay).await;
                }
                Ok(response) => break response,
                Err(error) if attempt < 3 && error.is_connect() => {
                    let delay = openrouter_retry_delay_without_response(attempt);
                    tracing::warn!(
                        %error,
                        attempt,
                        retry_delay_ms = delay.as_millis(),
                        "retrying OpenRouter connection failure"
                    );
                    tokio::time::sleep(delay).await;
                }
                Err(error) => {
                    trace.exit_status = Some(1);
                    trace.stderr = error.to_string();
                    return Err(ProviderCallError {
                        message: format!("OpenRouter request failed: {error}"),
                        trace,
                        session_id: None,
                    });
                }
            }
        };
        trace.invocation.args.push(format!("attempts={attempt}"));

        let status = response.status();
        if status.is_success() {
            let (text, raw) = match read_openrouter_stream(response, progress.as_ref()).await {
                Ok(result) => result,
                Err(error) => {
                    trace.exit_status = Some(1);
                    trace.stderr = error.clone();
                    return Err(ProviderCallError {
                        message: format!("OpenRouter streaming response failed: {error}"),
                        trace,
                        session_id: None,
                    });
                }
            };
            trace.stdout = raw.to_string();
            trace.exit_status = Some(0);
            let value = if schema.is_some() {
                parse_chat_completion_json_text(&text).ok_or_else(|| ProviderCallError {
                    message: format!(
                        "OpenRouter returned non-JSON content for structured call: {}",
                        truncate_provider_text(&text, 500)
                    ),
                    trace: trace.clone(),
                    session_id: None,
                })?
            } else {
                Value::String(text)
            };
            return Ok(ProviderCallResult {
                value,
                raw_response: raw.clone(),
                usage: extract_chat_completions_usage(
                    &raw,
                    elapsed_millis_u64(started_at),
                    openrouter_provider_cost_microusd(&raw),
                ),
                trace,
                session_id: None,
            });
        }
        let raw_text = if status.is_success() {
            match read_provider_success_response_text(response).await {
                Ok(text) => text,
                Err(error) => {
                    trace.exit_status = Some(1);
                    trace.stderr = error.to_string();
                    return Err(ProviderCallError {
                        message: format!("OpenRouter response read failed: {error}"),
                        trace,
                        session_id: None,
                    });
                }
            }
        } else {
            match read_provider_error_response_text(response).await {
                Ok(text) => text,
                Err(error) => {
                    trace.exit_status = Some(1);
                    trace.stderr = error.to_string();
                    return Err(ProviderCallError {
                        message: format!("OpenRouter error response read failed: {error}"),
                        trace,
                        session_id: None,
                    });
                }
            }
        };
        trace.stdout = raw_text.clone();
        trace.exit_status = Some(if status.is_success() { 0 } else { 1 });
        if !status.is_success() {
            trace.stderr = raw_text.clone();
            return Err(ProviderCallError {
                message: format_openrouter_status(status, &raw_text),
                trace,
                session_id: None,
            });
        }

        let raw: Value = match serde_json::from_str(&raw_text) {
            Ok(value) => value,
            Err(error) => {
                trace.stderr = error.to_string();
                return Err(ProviderCallError {
                    message: format!("OpenRouter returned invalid JSON: {error}"),
                    trace,
                    session_id: None,
                });
            }
        };
        let text = raw
            .pointer("/choices/0/message/content")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_string();
        if !text.is_empty()
            && let Some(sender) = progress.as_ref()
        {
            let _ = sender.send(ProviderProgress::stdout(text.as_bytes().to_vec()));
        }

        let value = if schema.is_some() {
            parse_chat_completion_json_text(&text).ok_or_else(|| ProviderCallError {
                message: format!(
                    "OpenRouter returned non-JSON content for structured call: {}",
                    truncate_provider_text(&text, 500)
                ),
                trace: trace.clone(),
                session_id: None,
            })?
        } else {
            Value::String(text)
        };

        Ok(ProviderCallResult {
            value,
            raw_response: raw.clone(),
            usage: extract_chat_completions_usage(
                &raw,
                elapsed_millis_u64(started_at),
                openrouter_provider_cost_microusd(&raw),
            ),
            trace,
            session_id: None,
        })
    }
    /// Tool-based call using OpenAI function calling. Returns the raw API response Value;
    /// the caller extracts `choices[0].message.tool_calls[]`.
    /// DeepSeek reasoner does not support `tool_choice` — tools are offered but not forced.
    pub async fn call_with_tool(
        &self,
        prompt: &str,
        tool_name: &str,
        tool_description: &str,
        parameters: &Value,
    ) -> std::result::Result<Value, ProviderCallError> {
        self.call_with_tool_result(prompt, tool_name, tool_description, parameters)
            .await
            .map(|result| result.raw_response)
    }

    pub async fn call_with_tool_result(
        &self,
        prompt: &str,
        tool_name: &str,
        tool_description: &str,
        parameters: &Value,
    ) -> std::result::Result<ProviderCallResult, ProviderCallError> {
        let started_at = Instant::now();
        let api_key = nonempty_env("OPENROUTER_API_KEY").ok_or_else(|| ProviderCallError {
            message: "OPENROUTER_API_KEY not set".to_string(),
            trace: self.trace(None, "", ""),
            session_id: None,
        })?;

        let mut messages = Vec::new();
        if !self.system_prompt.trim().is_empty() {
            messages.push(json!({ "role": "system", "content": self.system_prompt }));
        }
        messages.push(json!({ "role": "user", "content": prompt }));

        let mut body = json!({
            "model": self.model,
            "messages": messages,
            "tools": [{
                "type": "function",
                "function": { "name": tool_name, "description": tool_description, "parameters": parameters }
            }],
        });
        if let Some(provider) = openrouter_provider_preferences() {
            body["provider"] = provider;
        }
        if let Some(reasoning) = openrouter_reasoning(self.effort.as_deref()) {
            body["reasoning"] = reasoning;
        }

        let request = reqwest::Client::new()
            .post(OPENROUTER_CHAT_COMPLETIONS_URL)
            .bearer_auth(&api_key)
            .header("HTTP-Referer", "https://borg.ml")
            .header("X-Title", "Borg")
            .json(&body);
        let response = apply_provider_request_timeout(request).send().await;

        let (raw_text, status) = match response {
            Ok(r) => {
                let s = r.status();
                let raw_text = if s.is_success() {
                    read_provider_success_response_text(r).await
                } else {
                    read_provider_error_response_text(r).await
                };
                match raw_text {
                    Ok(t) => (t, s),
                    Err(e) => {
                        return Err(ProviderCallError {
                            message: format!("read failed: {e}"),
                            trace: self.trace(Some(1), "", &e.to_string()),
                            session_id: None,
                        });
                    }
                }
            }
            Err(e) => {
                return Err(ProviderCallError {
                    message: format!("request failed: {e}"),
                    trace: self.trace(Some(1), "", &e.to_string()),
                    session_id: None,
                });
            }
        };

        if !status.is_success() {
            return Err(ProviderCallError {
                message: format_openrouter_status(status, &raw_text),
                trace: self.trace(Some(1), &raw_text, &raw_text),
                session_id: None,
            });
        }

        let raw: Value = serde_json::from_str(&raw_text).map_err(|e| ProviderCallError {
            message: format!("invalid JSON: {e}"),
            trace: self.trace(Some(1), &raw_text, &e.to_string()),
            session_id: None,
        })?;
        let value = raw
            .pointer("/choices/0/message/tool_calls")
            .cloned()
            .unwrap_or(Value::Array(Vec::new()));
        Ok(ProviderCallResult {
            value,
            raw_response: raw.clone(),
            usage: extract_chat_completions_usage(
                &raw,
                elapsed_millis_u64(started_at),
                openrouter_provider_cost_microusd(&raw),
            ),
            trace: self.trace(Some(0), &raw_text, ""),
            session_id: None,
        })
    }

    fn trace(&self, exit_status: Option<i32>, stdout: &str, stderr: &str) -> ProviderAttemptTrace {
        trace_from_buffers(
            ProviderInvocation {
                provider_label: self.label().to_string(),
                executable: OPENROUTER_CHAT_COMPLETIONS_URL.to_string(),
                args: vec![self.model.clone()],
                cwd: None,
                model: Some(self.model.clone()),
                effort: self.effort.clone(),
            },
            exit_status,
            stdout.as_bytes(),
            stderr.as_bytes(),
        )
    }
}

async fn read_openrouter_stream(
    response: reqwest::Response,
    progress: Option<&UnboundedSender<ProviderProgress>>,
) -> Result<(String, Value), String> {
    let mut stream = response.bytes_stream();
    let mut pending = Vec::new();
    let mut content = String::new();
    let mut usage = None;
    let mut saw_done = false;
    while let Some(chunk) = stream.next().await {
        pending.extend_from_slice(&chunk.map_err(|error| error.to_string())?);
        while let Some(newline) = pending.iter().position(|byte| *byte == b'\n') {
            let mut line = pending.drain(..=newline).collect::<Vec<_>>();
            while matches!(line.last(), Some(b'\n' | b'\r')) {
                line.pop();
            }
            let line = std::str::from_utf8(&line)
                .map_err(|error| format!("stream contained invalid UTF-8: {error}"))?;
            let Some(data) = line.strip_prefix("data:") else {
                continue;
            };
            let data = data.trim();
            if data == "[DONE]" {
                saw_done = true;
                break;
            }
            if data.is_empty() {
                continue;
            }
            let chunk: Value = serde_json::from_str(data)
                .map_err(|error| format!("invalid SSE JSON chunk: {error}"))?;
            if let Some(delta) = chunk
                .pointer("/choices/0/delta/content")
                .and_then(Value::as_str)
            {
                content.push_str(delta);
                if let Some(sender) = progress {
                    let _ = sender.send(ProviderProgress::stdout(delta.as_bytes().to_vec()));
                }
            }
            if let Some(chunk_usage) = chunk
                .get("usage")
                .or_else(|| chunk.pointer("/choices/0/usage"))
            {
                usage = Some(chunk_usage.clone());
            }
        }
        if saw_done {
            break;
        }
    }
    if !saw_done {
        return Err("stream ended before the required data: [DONE] marker".to_string());
    }
    let raw = json!({
        "choices": [{ "message": { "role": "assistant", "content": content } }],
        "usage": usage.unwrap_or_else(|| json!({})),
    });
    Ok((content, raw))
}

fn openrouter_reasoning(effort: Option<&str>) -> Option<Value> {
    match effort {
        Some("low") => Some(json!({ "effort": "low" })),
        Some("medium") => Some(json!({ "effort": "medium" })),
        Some("high") => Some(json!({ "effort": "high" })),
        Some("xhigh") | Some("max") => Some(json!({ "effort": "max" })),
        _ => None,
    }
}

fn openrouter_provider_preferences() -> Option<Value> {
    let order = nonempty_env("BORG_OPENROUTER_PROVIDER_ORDER")?
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if order.is_empty() {
        return None;
    }
    let allow_fallbacks = bool_var("BORG_OPENROUTER_ALLOW_FALLBACKS").unwrap_or(true);
    Some(json!({
        "order": order,
        "allow_fallbacks": allow_fallbacks,
    }))
}

fn openrouter_provider_cost_microusd(raw: &Value) -> Option<u64> {
    let usage = raw.get("usage").unwrap_or(&Value::Null);
    usage
        .get("cost")
        .and_then(Value::as_f64)
        .and_then(provider_cost_usd_to_microusd)
}

fn openrouter_retryable_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

fn openrouter_retry_delay(response: &reqwest::Response, attempt: u32) -> std::time::Duration {
    response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(|seconds| std::time::Duration::from_secs(seconds.min(30)))
        .unwrap_or_else(|| openrouter_retry_delay_without_response(attempt))
}

fn openrouter_retry_delay_without_response(attempt: u32) -> std::time::Duration {
    std::time::Duration::from_millis(500_u64.saturating_mul(1_u64 << attempt.saturating_sub(1)))
}

fn format_openrouter_status(status: StatusCode, body: &str) -> String {
    format!(
        "OpenRouter request failed with HTTP {}: {}",
        status.as_u16(),
        truncate_provider_text(body, 500)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openrouter_cost_uses_authoritative_response_cost() {
        let raw = json!({ "usage": { "cost": 0.123456 } });
        assert_eq!(openrouter_provider_cost_microusd(&raw), Some(123_456));
    }

    #[test]
    fn openrouter_retries_rate_limits_and_server_failures_only() {
        assert!(openrouter_retryable_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(openrouter_retryable_status(StatusCode::SERVICE_UNAVAILABLE));
        assert!(!openrouter_retryable_status(StatusCode::PAYMENT_REQUIRED));
        assert!(!openrouter_retryable_status(StatusCode::BAD_REQUEST));
        assert_eq!(
            openrouter_retry_delay_without_response(1),
            std::time::Duration::from_millis(500)
        );
        assert_eq!(
            openrouter_retry_delay_without_response(2),
            std::time::Duration::from_secs(1)
        );
    }
}
