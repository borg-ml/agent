//! Model-only access adapter for the OpenCode Go subscription route.
//!
//! # Scope
//!
//! This module resolves *access* and nothing else: the endpoint, the
//! subscription credential, the wire model id, and the one routing header the
//! service requires. It never spawns the `opencode` binary, never reads its
//! session state, and owns no tool loop. Once [`gateway`] returns, the turn
//! belongs to Borg: `NativeHarness` drives it through
//! `OpenAiCompatibleProvider` exactly like Kimi, GLM, or OpenRouter, so tool
//! calls are executed by Borg's own capabilities and permissions.
//!
//! This is the [`crate::subscription`] pattern applied to a route that happens
//! to be reached through a vendor that also ships an agent CLI. The plan is a
//! gateway configuration; the CLI is not in the path.
//!
//! # What this module is deliberately *not*
//!
//! It is not a general "OpenCode" adapter. OpenCode multiplexes many upstream
//! providers, and a user's `auth.json` may authenticate several of them
//! independently. Only the `opencode-go` route is known to expose an
//! authenticated OpenAI-compatible HTTP API that Borg can call directly; the
//! other routes have their own credentials and billing and generally no
//! Borg-reachable endpoint. [`gateway`] therefore *refuses* any model that is
//! not `opencode-go/…` instead of assuming that one authenticated route
//! implies the rest. See [`UNSUPPORTED_ROUTE_HINT`].
//!
//! # Credentials and billing
//!
//! The Go route bills against the user's Go allowance and is keyed by the
//! `opencode-go` subscription key alone, read through
//! [`crate::credentials::opencode_go_api_key`] from the key the user has
//! already configured. This module performs no login, no refresh, and no
//! credential writes. It never falls back to another vendor's key: doing so
//! would silently move spend onto a different account.

use std::collections::{BTreeMap, HashMap};
use std::sync::{OnceLock, RwLock};

use anyhow::{Result, bail};
use uuid::Uuid;

use super::ModelGateway;

/// Borg's catalog alias prefix for the Go route. `runtime.rs` applies this
/// prefix when it publishes the catalog, so a selected model reaches the
/// provider already namespaced and must be stripped before it goes on the wire.
pub const MODEL_PREFIX: &str = "opencode-go/";

/// OpenAI-compatible base for the Go route. This is the subscription host, not
/// a pay-as-you-go one; it is the same origin the catalog refresh already uses.
pub const BASE_URL: &str = "https://opencode.ai/zen/go/v1";

/// Required routing header. The service rejects a request without it with a
/// `MissingSessionID` error, so this is part of the wire contract rather than
/// an optimization.
pub const SESSION_HEADER: &str = "x-opencode-session";

/// Provider identity used in traces and diagnostics.
pub const LABEL: &str = "opencode-go";

/// Shown when a non-Go OpenCode model reaches this adapter.
pub const UNSUPPORTED_ROUTE_HINT: &str = "only the `opencode-go` route exposes an API Borg can call directly; other OpenCode routes \
     keep their own credentials and billing and still run through the OpenCode agent";

/// The chat-completions endpoint for the Go route.
pub fn chat_completions_endpoint() -> String {
    format!("{BASE_URL}/chat/completions")
}

/// Whether `model` names the Go route.
pub fn is_go_model(model: &str) -> bool {
    wire_model(model).is_some()
}

/// The upstream model id for a Borg `opencode-go/<model>` alias.
///
/// Returns `None` for every other model, including bare ids and other OpenCode
/// routes, so a caller cannot accidentally spend Go allowance on a model the
/// user selected from a different provider.
pub fn wire_model(model: &str) -> Option<&str> {
    let upstream = model.trim().strip_prefix(MODEL_PREFIX)?.trim();
    (!upstream.is_empty()).then_some(upstream)
}

/// The value sent as [`SESSION_HEADER`].
///
/// The service uses this only to route a conversation's turns consistently, so
/// it is derived from the Borg session id and stays stable for the life of that
/// session. The plain hyphenated UUID form is what the route accepts; it
/// carries no credential material.
pub fn session_header_value(session_id: Uuid) -> String {
    session_id.to_string()
}

/// Build the access gateway for a Go-route model, resolving the configured
/// subscription key.
///
/// Fails, rather than falling back, when the route is wrong or the
/// subscription is not connected.
pub fn gateway(model: &str, session_id: Uuid) -> Result<ModelGateway> {
    let Some(api_key) = crate::credentials::opencode_go_api_key() else {
        bail!(
            "OpenCode Go is not connected; run `borg login opencode --api-key` or set \
             OPENCODE_GO_API_KEY to use {model}"
        );
    };
    gateway_with_key(model, session_id, &api_key)
}

/// [`gateway`] with the credential supplied by the caller.
///
/// Kept separate so the wire contract can be exercised without reading, or
/// depending on the presence of, a real subscription key.
pub fn gateway_with_key(model: &str, session_id: Uuid, api_key: &str) -> Result<ModelGateway> {
    let Some(upstream_model) = wire_model(model) else {
        bail!("`{model}` is not an OpenCode Go model: {UNSUPPORTED_ROUTE_HINT}");
    };
    let api_key = api_key.trim();
    if api_key.is_empty() {
        bail!("the OpenCode Go subscription key is empty; reconnect the subscription");
    }

    let mut headers = BTreeMap::new();
    headers.insert(SESSION_HEADER.to_string(), session_header_value(session_id));

    let mut gateway = ModelGateway::new(chat_completions_endpoint(), api_key);
    gateway.model = Some(upstream_model.to_string());
    gateway.label = Some(LABEL.to_string());
    gateway.headers = headers;
    Ok(gateway)
}

/// Per-process cache of the Go route's advertised context windows, keyed by the
/// bare upstream model id. `None` means the model list has not been fetched.
static CONTEXT_WINDOWS: OnceLock<RwLock<Option<HashMap<String, u64>>>> = OnceLock::new();

fn context_windows() -> &'static RwLock<Option<HashMap<String, u64>>> {
    CONTEXT_WINDOWS.get_or_init(|| RwLock::new(None))
}

/// The context window the Go service advertises for `model`.
///
/// The chat-completions response carries no context metadata, so without this
/// `UsageUpdated` reports a null window: the context meter stays blank and
/// auto-compaction never engages. The service's own model list is the
/// authority, so it is fetched once per process and cached. An unknown model
/// stays `None` rather than guessing a window.
pub async fn context_window_tokens(model: &str) -> Option<u64> {
    let id = wire_model(model).unwrap_or_else(|| model.trim());
    if id.is_empty() {
        return None;
    }
    if let Ok(cache) = context_windows().read()
        && let Some(windows) = cache.as_ref()
    {
        return windows.get(id).copied();
    }
    if let Ok(windows) = fetch_context_windows().await
        && let Ok(mut cache) = context_windows().write()
    {
        *cache = Some(windows);
    }
    context_windows()
        .read()
        .ok()
        .and_then(|cache| cache.as_ref()?.get(id).copied())
}

async fn fetch_context_windows() -> Result<HashMap<String, u64>> {
    let response = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()?
        .get(format!("{BASE_URL}/models"))
        .send()
        .await?
        .error_for_status()?;
    let payload: serde_json::Value = response.json().await?;
    Ok(parse_context_windows(&payload))
}

fn parse_context_windows(payload: &serde_json::Value) -> HashMap<String, u64> {
    payload
        .get("data")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|model| {
            let id = model.get("id")?.as_str()?.trim();
            let context = model
                .get("context_length")
                .or_else(|| model.get("contextLength"))
                .and_then(serde_json::Value::as_u64)?;
            (!id.is_empty() && context > 0).then(|| (id.to_string(), context))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_KEY: &str = "test-opencode-go-key";

    /// The catalog publishes `opencode-go/<id>` but the service expects the
    /// bare id. Sending the alias through unchanged is a silent 4xx, so the
    /// mapping is part of the wire contract.
    #[test]
    fn go_aliases_map_to_their_upstream_model_id() {
        assert_eq!(
            wire_model("opencode-go/kimi-k2.7-code"),
            Some("kimi-k2.7-code")
        );
        assert_eq!(wire_model("  opencode-go/glm-5.3  "), Some("glm-5.3"));
        assert!(is_go_model("opencode-go/gpt-5.6-luna"));
    }

    /// The adapter must not treat "authenticated for one OpenCode route" as
    /// "authenticated for all of them". Each of these would otherwise be billed
    /// against the Go allowance, or fail upstream with the wrong credential.
    #[test]
    fn non_go_routes_are_refused_rather_than_assumed() {
        for model in [
            "opencode/kimi-k2.7-code",
            "kimi-k2.7-code",
            "opencode-go/",
            "opencode-go/   ",
            "",
            "openrouter/auto",
        ] {
            assert_eq!(wire_model(model), None, "{model}");
            assert!(!is_go_model(model), "{model}");
            assert!(
                gateway_with_key(model, Uuid::new_v4(), TEST_KEY).is_err(),
                "{model} must not produce a Go gateway"
            );
        }
    }

    /// The route rejects a request without `x-opencode-session`
    /// (`MissingSessionID`), so losing the header is a hard outage rather than
    /// a degradation. This pins the header name, the endpoint, and the stripped
    /// wire model together — the exact shape a model-only turn puts on the wire.
    #[test]
    fn gateway_carries_the_required_session_routing_header() {
        let session_id = Uuid::new_v4();
        let gateway = gateway_with_key("opencode-go/kimi-k2.7-code", session_id, TEST_KEY).unwrap();

        assert_eq!(
            gateway.endpoint,
            "https://opencode.ai/zen/go/v1/chat/completions"
        );
        assert_eq!(gateway.model.as_deref(), Some("kimi-k2.7-code"));
        assert_eq!(gateway.label.as_deref(), Some(LABEL));
        assert_eq!(
            gateway.headers.get(SESSION_HEADER).map(String::as_str),
            Some(session_id.to_string().as_str())
        );
        // Access only: the adapter contributes no request-body fields, so it
        // cannot alter the conversation, tools, or streaming contract that the
        // shared provider owns.
        assert!(gateway.body.is_empty());
        assert!(gateway.variant_bodies.is_empty());
    }

    /// A session's turns must route consistently, and two sessions must not
    /// collide.
    #[test]
    fn session_header_is_stable_per_session_and_distinct_across_sessions() {
        let session_id = Uuid::new_v4();
        let first = gateway_with_key("opencode-go/glm-5.3", session_id, TEST_KEY).unwrap();
        let second = gateway_with_key("opencode-go/glm-5.3", session_id, TEST_KEY).unwrap();
        assert_eq!(first.headers, second.headers);

        let other = gateway_with_key("opencode-go/glm-5.3", Uuid::new_v4(), TEST_KEY).unwrap();
        assert_ne!(first.headers, other.headers);
    }

    /// The subscription key reaches the transport but must never reach a log,
    /// a trace, or an error message.
    #[test]
    fn the_subscription_key_is_never_rendered() {
        let gateway = gateway_with_key("opencode-go/kimi-k3", Uuid::new_v4(), TEST_KEY).unwrap();
        assert_eq!(gateway.bearer_token, TEST_KEY);

        let rendered = format!("{gateway:?}");
        assert!(!rendered.contains(TEST_KEY), "{rendered}");
        assert!(rendered.contains("[redacted]"), "{rendered}");
        // Header *names* are diagnostic; values are not.
        assert!(rendered.contains(SESSION_HEADER), "{rendered}");
    }

    #[test]
    fn a_blank_subscription_key_is_refused() {
        for key in ["", "   "] {
            let error = gateway_with_key("opencode-go/kimi-k3", Uuid::new_v4(), key)
                .expect_err("blank key must not build a gateway");
            assert!(error.to_string().contains("empty"), "{error}");
        }
    }

    /// The chat-completions payload carries no window, so the model list is the
    /// authority. Losing `context_length` would silently blank the context meter
    /// and disable auto-compaction rather than fail loudly.
    #[test]
    fn context_windows_are_read_from_the_model_list() {
        let payload = serde_json::json!({
            "data": [
                {"id": "kimi-k2.7-code", "context_length": 262_144},
                {"id": "glm-5.3", "contextLength": 200_000},
                {"id": "no-window"},
                {"id": "zero", "context_length": 0},
                {"id": "  spaced  ", "context_length": 131_072},
            ]
        });
        let windows = parse_context_windows(&payload);
        assert_eq!(windows.get("kimi-k2.7-code"), Some(&262_144));
        assert_eq!(windows.get("glm-5.3"), Some(&200_000));
        assert_eq!(windows.get("spaced"), Some(&131_072));
        assert!(!windows.contains_key("no-window"));
        assert!(!windows.contains_key("zero"));
    }
}
