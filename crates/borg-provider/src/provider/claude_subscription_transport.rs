//! Direct subscription HTTP/SSE primitives; no child process or API-key fallback.
//! Staged until authority/catalog policy and the provider caller are integrated.

use super::{
    claude_connector::Capabilities,
    claude_subscription_protocol::{self as protocol, RequestIdentity},
    claude_subscription_wire,
};
use anyhow::{Context, Result, ensure};
use reqwest::header::HeaderMap;
use serde_json::{Value, json};
use std::{sync::OnceLock, time::Duration};

pub(super) const ENDPOINT: &str = "https://api.anthropic.com/v1/messages?beta=true";
pub(super) const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
pub(super) const MAX_RESPONSE_BYTES: usize = 128 * 1024 * 1024;
const MAX_REQUEST_BYTES: usize = 64 * 1024 * 1024;

// Contains credentials and user context. Intentionally not Debug.
pub(super) struct PreparedRequest {
    headers: HeaderMap,
    bytes: Vec<u8>,
}

pub(super) fn prepare(
    body: &Value,
    capabilities: &Capabilities,
    bearer: &str,
    identity: &RequestIdentity<'_>,
    request_id: &str,
) -> Result<PreparedRequest> {
    let mut wire = body.clone();
    let object = wire
        .as_object_mut()
        .context("invalid Claude subscription body")?;
    ensure!(
        object.get("model").and_then(Value::as_str) == Some(capabilities.model.as_str()),
        "Claude model differs from verified selection"
    );
    let messages = object
        .get("messages")
        .and_then(Value::as_array)
        .context("Claude messages missing")?;
    let attribution =
        protocol::billing_system_text(messages, "borg", identity.parent_agent_id.is_some())?;
    let mut system = vec![json!({"type":"text","text":attribution})];
    if let Some(blocks) = object.get("system") {
        system.extend(
            blocks
                .as_array()
                .context("invalid Claude system blocks")?
                .iter()
                .cloned(),
        );
    }
    let mut betas = capabilities.betas.clone();
    if let Some(requested) = object.remove("betas") {
        for beta in requested.as_array().context("invalid Claude betas")? {
            let beta = beta.as_str().context("invalid Claude beta")?;
            if !betas.iter().any(|known| known == beta) {
                betas.push(beta.to_owned());
            }
        }
    }
    object.insert("system".into(), Value::Array(system));
    object.insert("stream".into(), Value::Bool(true));
    object.insert("metadata".into(), identity.metadata()?);
    let headers = protocol::inference_headers(bearer, identity, request_id, "borg", &betas)?;
    let mut bytes = serde_json::to_vec(&wire)?;
    ensure!(
        bytes.len() <= MAX_REQUEST_BYTES,
        "Claude subscription request too large"
    );
    claude_subscription_wire::stamp(&mut bytes).map_err(anyhow::Error::msg)?;
    Ok(PreparedRequest { headers, bytes })
}

fn http_client() -> Result<&'static reqwest::Client> {
    static HTTP: OnceLock<reqwest::Client> = OnceLock::new();
    if let Some(client) = HTTP.get() {
        return Ok(client);
    }
    let client = reqwest::Client::builder()
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(30))
        .build()?;
    let _ = HTTP.set(client);
    HTTP.get()
        .context("Claude subscription HTTP client unavailable")
}

/// The enclosing provider owns account/replay guards and one pre-event 401 retry.
/// Dropping this future or its response stream cancels HTTP, not a helper RPC.
pub(super) async fn send(request: PreparedRequest) -> Result<reqwest::Response> {
    ensure!(
        !request.headers.contains_key("x-api-key"),
        "API credentials refused on subscription route"
    );
    Ok(http_client()?
        .post(ENDPOINT)
        .headers(request.headers)
        .body(request.bytes)
        .send()
        .await?)
}

pub(super) fn grace(headers: &HeaderMap) -> Option<(f64, f64)> {
    let text = |name| headers.get(name).and_then(|v| v.to_str().ok());
    if !headers.contains_key("anthropic-ratelimit-unified-grace-status")
        || text("anthropic-ratelimit-unified-overage-in-use") == Some("true")
        || matches!(
            text("anthropic-ratelimit-unified-overage-status"),
            Some("allowed" | "allowed_warning")
        )
    {
        return None;
    }
    let utilization = |name| {
        text(name)
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|n| n.is_finite() && *n > 0.0)
            .unwrap_or(0.0)
    };
    let five = utilization("anthropic-ratelimit-unified-grace-5h-utilization");
    let week = utilization("anthropic-ratelimit-unified-grace-7d-utilization");
    (five > 0.0 || week > 0.0).then_some((five, week))
}

/// One complete SSE frame. Event JSON remains unchanged for the shared parser.
pub(super) enum SseFrame {
    Ignored,
    Event(Value),
}

pub(super) fn take_event(buffer: &mut Vec<u8>) -> Result<Option<SseFrame>> {
    let lf = buffer.windows(2).position(|w| w == b"\n\n").map(|n| (n, 2));
    let crlf = buffer
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|n| (n, 4));
    let boundary = [lf, crlf].into_iter().flatten().min_by_key(|(n, _)| *n);
    let Some((end, width)) = boundary else {
        ensure!(
            buffer.len() <= MAX_FRAME_BYTES,
            "Claude SSE frame too large"
        );
        return Ok(None);
    };
    ensure!(end <= MAX_FRAME_BYTES, "Claude SSE frame too large");
    let frame = std::str::from_utf8(&buffer[..end]).context("invalid Claude SSE UTF-8")?;
    let mut event = None;
    let mut data = String::new();
    for line in frame.lines() {
        if let Some(name) = line.strip_prefix("event:") {
            event = Some(name.trim());
        }
        if let Some(value) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(value.strip_prefix(' ').unwrap_or(value));
        }
    }
    let result = if data.trim().is_empty() || data.trim() == "[DONE]" {
        SseFrame::Ignored
    } else {
        let payload: Value = serde_json::from_str(&data).context("invalid Claude SSE JSON")?;
        let kind = payload
            .get("type")
            .and_then(Value::as_str)
            .context("Claude SSE type missing")?;
        ensure!(
            event.is_none_or(|name| name == kind),
            "Claude SSE event/type mismatch"
        );
        SseFrame::Event(payload)
    };
    buffer.drain(..end + width);
    Ok(Some(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderValue;
    fn decoded(frame: Option<SseFrame>) -> Value {
        match frame.unwrap() {
            SseFrame::Event(value) => value,
            SseFrame::Ignored => panic!("expected event"),
        }
    }

    #[test]
    fn preserves_signed_cache_body_and_stamps_exact_bytes() {
        let caps = protocol::source_model_facts("claude-opus-5-5", &[])
            .unwrap()
            .capabilities;
        let identity = RequestIdentity {
            device_id: &"a".repeat(64),
            account_uuid: "account-a",
            session_id: "session-a",
            parent_agent_id: None,
        };
        let block = json!({"type":"thinking","thinking":"owned fixture","signature":"opaque","cache_control":{"type":"ephemeral","ttl":"1h"}});
        let body = json!({"model":caps.model,"max_tokens":1,"messages":[{"role":"assistant","content":[block]}],"system":[],"betas":["extended-cache-ttl-2025-04-11"]});
        let prepared = prepare(&body, &caps, "owned-fake-bearer", &identity, "request-a").unwrap();
        assert!(prepared.headers["authorization"].is_sensitive());
        assert!(!prepared.headers.contains_key("x-api-key"));
        let wire: Value = serde_json::from_slice(&prepared.bytes).unwrap();
        assert!(wire.get("betas").is_none());
        assert_eq!(wire["messages"], body["messages"]);
        assert!(
            wire["system"][0]["text"]
                .as_str()
                .unwrap()
                .contains("cc_entrypoint=borg;")
        );
        assert!(
            !wire["system"][0]["text"]
                .as_str()
                .unwrap()
                .contains("cch=00000")
        );
        let mut reset = prepared.bytes.clone();
        let at = reset.windows(4).position(|w| w == b"cch=").unwrap() + 4;
        reset[at..at + 5].copy_from_slice(b"00000");
        claude_subscription_wire::stamp(&mut reset).unwrap();
        assert_eq!(reset, prepared.bytes);
    }

    #[test]
    fn sse_uses_earliest_mixed_delimiter_and_rejects_type_mismatch() {
        let mut bytes = b"event: ping\r\ndata: {\"type\":\"ping\"}\r\n\r\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n".to_vec();
        assert_eq!(decoded(take_event(&mut bytes).unwrap())["type"], "ping");
        assert_eq!(
            decoded(take_event(&mut bytes).unwrap())["type"],
            "message_stop"
        );
        assert!(bytes.is_empty());
        let mut bytes = b"event: ping\ndata: {\"type\":\"message_stop\"}\n\n".to_vec();
        assert!(take_event(&mut bytes).is_err());
    }

    #[test]
    fn sse_waits_for_complete_utf8_and_handles_multiline_data() {
        let event =
            "event: text_delta\ndata: {\"type\":\"text_delta\",\ndata: \"text\":\"π 🧪\"}\n\n"
                .as_bytes();
        for split in 0..event.len() - 1 {
            let mut bytes = event[..split].to_vec();
            assert!(take_event(&mut bytes).unwrap().is_none());
            bytes.extend_from_slice(&event[split..]);
            assert_eq!(decoded(take_event(&mut bytes).unwrap())["text"], "π 🧪");
            assert!(bytes.is_empty());
        }
    }

    #[test]
    fn grace_is_not_extra_usage_or_usage_percent() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "anthropic-ratelimit-unified-grace-status",
            HeaderValue::from_static("allowed"),
        );
        headers.insert(
            "anthropic-ratelimit-unified-grace-5h-utilization",
            HeaderValue::from_static("0.25"),
        );
        assert_eq!(grace(&headers), Some((0.25, 0.0)));
        headers.insert(
            "anthropic-ratelimit-unified-overage-in-use",
            HeaderValue::from_static("true"),
        );
        assert!(grace(&headers).is_none());
    }
    #[test]
    fn ignored_frame_does_not_hide_buffered_events() {
        let mut bytes =
            b": heartbeat\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n".to_vec();
        assert!(matches!(
            take_event(&mut bytes).unwrap(),
            Some(SseFrame::Ignored)
        ));
        assert_eq!(
            decoded(take_event(&mut bytes).unwrap())["type"],
            "message_stop"
        );
        assert!(bytes.is_empty());
    }
}
