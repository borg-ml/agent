//! In-process subscription client. No upstream executable, local RPC, or API-key fallback.
//! Policy and an optional active served snapshot are verified inputs from the controller:
//! HIPAA is a policy-verdict taint, NOT a guessed profile/bootstrap property (2.1.296,
//! offset 207595016; vF/EF/Th in selected 0066.js). Absent optional policy verdict has empty taints; explicit evidence is retained.

use std::{
    collections::BTreeMap,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
#[cfg(not(test))]
use futures::StreamExt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use super::{
    claude_connector::Capabilities,
    claude_subscription_protocol::{self as protocol, Bootstrap},
};
#[cfg(not(test))]
use super::{
    claude_subscription_protocol::RequestIdentity, claude_subscription_transport as transport,
};
use crate::claude_subscription::{
    self as auth, AccountIdentity, AuthAuthority, SubscriptionAccess,
};

#[cfg(not(test))]
const BOOTSTRAP_ENDPOINT: &str = "https://api.anthropic.com/api/claude_cli/bootstrap";
#[cfg(not(test))]
const POLICY_ENDPOINT: &str = "https://api.anthropic.com/api/claude_code/policy_limits";
#[cfg(not(test))]
const BOOTSTRAP_LIMIT: usize = 1024 * 1024;
const CACHE_AGE: Duration = Duration::from_secs(60);
const EFFORTS: &[&str] = &["low", "medium", "high", "xhigh", "max"];

/// Concrete source defaults apply without an optional verdict. Explicit controller
/// managed settings override them; unsupported required settings must fail there. Exact managed
/// model allow/deny predicates are handed off as permitted wire IDs, not CLI flags.
#[derive(Clone)]
pub(super) struct VerifiedPolicy {
    pub hipaa: bool,
    pub experimental_betas: bool,
    pub allow_model_catalog: bool,
    pub allowed_models: Option<Vec<String>>,
    pub denied_models: Vec<String>,
}

impl Default for VerifiedPolicy {
    fn default() -> Self {
        // Source policy host starts taints=[], verdict=undefined (207593972).
        Self {
            hipaa: false,
            experimental_betas: true,
            allow_model_catalog: true,
            allowed_models: None,
            denied_models: vec![],
        }
    }
}

/// An already activated, validated first-party `cc` snapshot. No feature payload means
/// off; this client does not fetch/activate the served picker opportunistically.
pub(super) struct VerifiedServedCatalog {
    pub rows: Vec<ServedModel>,
    pub enforced_default_model: Option<String>,
}

pub(super) struct ServedModel {
    pub id: String,
    pub family: Option<String>,
    pub disabled: bool,
    pub deprecated: bool,
    pub context_window: Option<u64>,
    pub max_input_tokens: Option<u64>,
    pub max_output_tokens: Option<u64>,
    pub default_output_tokens: Option<u64>,
    pub effort_levels: Option<Vec<String>>,
    /// Source runtime capabilities are positive grants, not a negative allowlist.
    pub capabilities: Vec<String>,
}

#[derive(Debug)]
pub(super) struct NativeInfo {
    pub pid: u32,
    pub account_identity: String,
    pub capabilities: Option<Capabilities>,
}

#[derive(Clone)]
pub(super) struct NativeConnector {
    authority: AuthAuthority,
    bound: AccountIdentity,
    #[cfg_attr(test, expect(dead_code))]
    device_id: Arc<String>,
    #[cfg_attr(test, expect(dead_code))]
    http: reqwest::Client,
    state: Arc<Mutex<State>>,
}

struct State {
    policy: VerifiedPolicy,
    served: Option<VerifiedServedCatalog>,
    bootstrap: Value,
    fetched: BTreeMap<String, (Instant, [u8; 32])>,
    // client_data is a per-model slot, unlike the account-level deny/default overlay.
    client_data: BTreeMap<String, Value>,
    fast: Option<(Instant, [u8; 32], bool)>,
}

// Turns reuse the account-bound connector so its metadata caches survive
// between requests instead of being refetched before every first token.
static CONNECTORS: std::sync::Mutex<Vec<NativeConnector>> = std::sync::Mutex::new(Vec::new());

impl NativeConnector {
    pub async fn connect(selected: Option<&Path>) -> Result<Self> {
        let authority = AuthAuthority::resolve(selected)?;
        let result = auth::discover(authority.clone()).await;
        super::claude_connector::record_account_verification(selected, result.is_ok());
        let access = result?;
        let cached = CONNECTORS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .find(|c| c.authority == authority && c.bound == access.profile.identity)
            .cloned();
        if let Some(connector) = cached {
            return Ok(connector);
        }
        let device_id = tokio::task::spawn_blocking(installation_device_id)
            .await
            .context("Claude device metadata task failed")??;
        let connector = Self {
            authority,
            bound: access.profile.identity,
            device_id: Arc::new(device_id),
            http: reqwest::Client::builder()
                .https_only(true)
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(5))
                .build()?,
            state: Arc::new(Mutex::new(State {
                policy: VerifiedPolicy::default(),
                served: None,
                bootstrap: json!({}),
                fetched: BTreeMap::new(),
                client_data: BTreeMap::new(),
                fast: None,
            })),
        };
        let mut connectors = CONNECTORS.lock().unwrap_or_else(|e| e.into_inner());
        connectors.retain(|c| c.authority != connector.authority);
        connectors.push(connector.clone());
        Ok(connector)
    }

    /// Account-owned profile/token access for usage/admission without another agent loop.
    /// Tokens must never be logged or persisted outside their selected auth authority.
    pub async fn access(&self) -> Result<SubscriptionAccess> {
        auth::access(self.authority.clone(), self.bound.clone(), None).await
    }

    pub async fn info(&self, model: Option<&str>) -> Result<NativeInfo> {
        let access = self.access().await?;
        let capabilities = if let Some(model) = model {
            let (_, caps) = self.admit(access, model, None, false, None).await?;
            Some(caps)
        } else {
            None
        };
        Ok(NativeInfo {
            pid: std::process::id(),
            account_identity: self.bound.subscription_id(),
            capabilities,
        })
    }

    /// Returns the raw HTTP response before any SSE is read. Only HTTP 401 gets one
    /// same-account refresh retry; no retry of events, speed, effort or model selection.
    pub async fn send_model(
        &self,
        body: &Value,
        id: &str,
        session: &str,
        parent: Option<&str>,
    ) -> Result<reqwest::Response> {
        let model = body["model"].as_str().context("Claude model missing")?;
        let effort = body["output_config"]
            .get("effort")
            .map(|v| v.as_str().context("invalid Claude effort"))
            .transpose()?;
        let fast = match body.get("speed") {
            None | Some(Value::Null) => false,
            Some(Value::String(s)) if s == "fast" => true,
            _ => anyhow::bail!("unknown Claude speed; refusing to downgrade"),
        };
        let mut access = self.access().await?;
        for attempt in 0..2 {
            let (current, caps) = self.admit(access, model, effort, fast, Some(body)).await?;
            let response = self
                .send(body, &caps, &current, id, session, parent)
                .await?;
            if response.status() != reqwest::StatusCode::UNAUTHORIZED || attempt == 1 {
                return Ok(response);
            }
            // Drop the unread 401 response before rotation; no event can escape.
            drop(response);
            access = auth::refresh(
                self.authority.clone(),
                self.bound.clone(),
                current.token.bearer().to_owned(),
            )
            .await?;
        }
        unreachable!("two bounded HTTP attempts")
    }

    pub async fn post(
        &self,
        body: &Value,
        id: &str,
        session: &str,
        parent: Option<&str>,
    ) -> Result<reqwest::Response> {
        self.send_model(body, id, session, parent).await
    }

    #[cfg(test)]
    async fn send(
        &self,
        _: &Value,
        _: &Capabilities,
        _: &SubscriptionAccess,
        _: &str,
        _: &str,
        _: Option<&str>,
    ) -> Result<reqwest::Response> {
        tests::record_send();
        anyhow::bail!("offline model send")
    }

    #[cfg(not(test))]
    async fn send(
        &self,
        body: &Value,
        caps: &Capabilities,
        access: &SubscriptionAccess,
        id: &str,
        session: &str,
        parent: Option<&str>,
    ) -> Result<reqwest::Response> {
        ensure!(
            access.profile.identity == self.bound,
            "Claude account changed before request"
        );
        let identity = RequestIdentity {
            device_id: &self.device_id,
            account_uuid: &self.bound.account_uuid,
            session_id: session,
            parent_agent_id: parent,
        };
        transport::send(transport::prepare(
            body,
            caps,
            access.token.bearer(),
            &identity,
            id,
        )?)
        .await
    }

    async fn capabilities(
        &self,
        access: &SubscriptionAccess,
        model: &str,
        effort: Option<&str>,
        fast: bool,
    ) -> Result<Capabilities> {
        let state = self.state.lock().await;
        let caps = model_capabilities(&state, model, effort)?;
        drop(state);
        // Do not collapse unknown/account-fetch errors into `fast=false`.
        let enabled = if caps.fast {
            let cached =
                self.state.lock().await.fast.filter(|(at, stamp, _)| {
                    at.elapsed() < CACHE_AGE && *stamp == generation(access)
                });
            match cached {
                Some((_, _, enabled)) => enabled,
                None => {
                    let enabled = auth::fast_eligibility(access, &self.bound).await?;
                    self.state.lock().await.fast =
                        Some((Instant::now(), generation(access), enabled));
                    enabled
                }
            }
        } else {
            false
        };
        let state = self.state.lock().await;
        let current = model_capabilities(&state, model, effort)?;
        ensure!(
            current.model == caps.model
                && current.betas == caps.betas
                && current.efforts == caps.efforts
                && current.max_output_tokens == caps.max_output_tokens,
            "Claude policy changed during request admission; retry before sending context"
        );
        let caps = Capabilities {
            fast: enabled && current.fast,
            ..current
        };
        protocol::validate_selection(&caps, effort, fast)?;
        Ok(caps)
    }

    // Generation races are retried only before context preparation, independently
    // of the single unread HTTP-401 retry. Metadata and final checks share this bound.
    async fn admit(
        &self,
        mut access: SubscriptionAccess,
        model: &str,
        effort: Option<&str>,
        fast: bool,
        body: Option<&Value>,
    ) -> Result<(SubscriptionAccess, Capabilities)> {
        for _ in 0..3 {
            let (next, metadata_admitted) = self.bootstrap(access, model).await?;
            access = next;
            if !metadata_admitted {
                continue;
            }
            let caps = self.capabilities(&access, model, effort, fast).await;
            let validation = match (&caps, body) {
                (Ok(caps), Some(body)) => {
                    let state = self.state.lock().await;
                    validate_body(body, caps, &state)
                }
                _ => Ok(()),
            };
            let current = self.access().await?;
            if generation(&current) == generation(&access) {
                validation?;
                return Ok((current, caps?));
            }
            access = current;
        }
        anyhow::bail!(
            "Claude credentials kept changing during request admission; no model request sent"
        )
    }

    async fn bootstrap(
        &self,
        access: SubscriptionAccess,
        model: &str,
    ) -> Result<(SubscriptionAccess, bool)> {
        let key = normalized(model);
        let mut state = self.state.lock().await;
        if let Some((at, stamp)) = state.fetched.get_mut(&key)
            && *stamp == generation(&access)
        {
            // Like native, serve this generation's last verified metadata and
            // revalidate off the request path instead of delaying the first token.
            if at.elapsed() >= CACHE_AGE {
                *at = Instant::now();
                let connector = self.clone();
                tokio::spawn(async move {
                    if let Ok(access) = connector.access().await {
                        let _ = connector.refresh_metadata(access, key).await;
                    }
                });
            }
            return Ok((access, true));
        }
        drop(state);
        self.refresh_metadata(access, key).await
    }

    async fn refresh_metadata(
        &self,
        access: SubscriptionAccess,
        key: String,
    ) -> Result<(SubscriptionAccess, bool)> {
        // Optional endpoint failure leaves the last account-bound deny/default state
        // intact. Explicit policy evidence is fetched separately and never guessed.
        let (policy, fetched) = tokio::join!(
            self.fetch_policy(&access),
            self.fetch_bootstrap(&access, &key)
        );
        let current = self.access().await?;
        if generation(&access) != generation(&current) {
            // Credential generation changed during fetch: never commit its response.
            return Ok((current, false));
        }
        let mut state = self.state.lock().await;
        if let Some(policy) = policy? {
            if policy["compliance_taints"]
                .as_array()
                .is_some_and(|taints| taints.iter().any(|t| t.as_str() == Some("hipaa")))
            {
                // HIPAA is latched for this immutable account-bound connector lifetime.
                state.policy.hipaa = true;
                state.served = None;
            }
            if policy["restrictions"]["allow_model_catalog"]["allowed"].as_bool() == Some(false) {
                state.policy.allow_model_catalog = false;
                state.served = None;
            }
        }
        if let Ok(response) = fetched {
            check_bootstrap_identity(&response, &self.bound)?;
            merge_bootstrap(&mut state.bootstrap, &response)?;
            state.client_data.insert(
                key.clone(),
                response.get("client_data").cloned().unwrap_or(Value::Null),
            );
            state
                .fetched
                .insert(key, (Instant::now(), generation(&current)));
        }
        Ok((current, true))
    }

    // Exact subscription bearer read: Afo/Ne source contract, policy_limits schema
    // xe in chunk-0c6rtmqc.js. Missing optional verdict starts with empty taints;
    // an HTTP auth/org refusal is not concealed as a successful default verdict.
    #[cfg(test)]
    async fn fetch_policy(&self, access: &SubscriptionAccess) -> Result<Option<Value>> {
        tests::fetch_policy(access)
    }

    #[cfg(not(test))]
    async fn fetch_policy(&self, access: &SubscriptionAccess) -> Result<Option<Value>> {
        let fetched = tokio::time::timeout(Duration::from_secs(5), async {
            let mut headers = protocol::bootstrap_headers(access.token.bearer())?;
            headers.insert(
                "cache-control",
                reqwest::header::HeaderValue::from_static("no-cache"),
            );
            headers.insert(
                "pragma",
                reqwest::header::HeaderValue::from_static("no-cache"),
            );
            let response = self
                .http
                .get(POLICY_ENDPOINT)
                .headers(headers)
                .send()
                .await?;
            let status = response.status();
            if status == reqwest::StatusCode::UNAUTHORIZED
                || status == reqwest::StatusCode::FORBIDDEN
            {
                return Ok::<_, anyhow::Error>((status, Value::Null));
            }
            let bytes = bounded_response(response).await?;
            let value = serde_json::from_slice::<Value>(&bytes)?;
            Ok::<_, anyhow::Error>((status, value))
        })
        .await;
        let Ok(Ok((status, value))) = fetched else {
            return Ok(None);
        };
        ensure!(
            status != reqwest::StatusCode::UNAUTHORIZED && status != reqwest::StatusCode::FORBIDDEN,
            "Claude subscription policy endpoint refused account/organization access (HTTP {status})"
        );
        if !status.is_success() {
            return Ok(None);
        }
        let restrictions = value["restrictions"]
            .as_object()
            .context("invalid explicit Claude policy restrictions")?;
        ensure!(
            restrictions.values().all(|r| r["allowed"].is_boolean()),
            "invalid explicit Claude policy restriction"
        );
        ensure!(
            value
                .get("compliance_taints")
                .is_none_or(|t| t.as_array().is_some_and(|a| a.iter().all(Value::is_string))),
            "invalid explicit Claude compliance taints"
        );
        Ok(Some(value))
    }

    #[cfg(test)]
    async fn fetch_bootstrap(&self, access: &SubscriptionAccess, _: &str) -> Result<Value> {
        tests::fetch_bootstrap(access).await
    }

    #[cfg(not(test))]
    async fn fetch_bootstrap(&self, access: &SubscriptionAccess, model: &str) -> Result<Value> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let response = self
                .http
                .get(BOOTSTRAP_ENDPOINT)
                .headers(protocol::bootstrap_headers(access.token.bearer())?)
                .query(&protocol::bootstrap_query(model, "borg"))
                .send()
                .await?
                .error_for_status()?;
            let bytes = bounded_response(response).await?;
            let response: Value = serde_json::from_slice(&bytes)?;
            // A syntactically invalid optional response is a fetch failure, not deny-all.
            let _: Bootstrap = serde_json::from_value(response.clone())?;
            ensure!(response.is_object(), "invalid Claude bootstrap response");
            Ok(response)
        })
        .await
        .context("Claude bootstrap timed out")?
    }
}

#[cfg(not(test))]
async fn bounded_response(response: reqwest::Response) -> Result<Vec<u8>> {
    ensure!(
        response
            .content_length()
            .is_none_or(|n| n <= BOOTSTRAP_LIMIT as u64),
        "Claude metadata response too large"
    );
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        ensure!(
            bytes.len().saturating_add(chunk.len()) <= BOOTSTRAP_LIMIT,
            "Claude metadata response too large"
        );
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn generation(access: &SubscriptionAccess) -> [u8; 32] {
    Sha256::digest(access.token.bearer().as_bytes()).into()
}

fn normalized(id: &str) -> String {
    id.trim().to_ascii_lowercase()
}

fn aliases(served: Option<&VerifiedServedCatalog>) -> BTreeMap<String, String> {
    let mut aliases = BTreeMap::from([
        ("opus".into(), "claude-opus-5-5".into()),
        ("sonnet".into(), "claude-sonnet-5-5".into()),
        ("haiku".into(), "claude-haiku-5-5".into()),
        ("fable".into(), "claude-fable-5-1".into()),
    ]);
    if let Some(served) = served {
        for family in ["opus", "sonnet", "haiku"] {
            if let Some(row) = served
                .rows
                .iter()
                .find(|r| !r.disabled && !r.deprecated && r.family.as_deref() == Some(family))
            {
                aliases.insert(family.into(), row.id.clone());
            }
        }
    }
    aliases
}

fn resolve(model: &str, aliases: &BTreeMap<String, String>) -> String {
    let key = normalized(model);
    aliases.get(&key).cloned().unwrap_or(key)
}

fn check_bootstrap_identity(response: &Value, bound: &AccountIdentity) -> Result<()> {
    for (field, expected) in [
        ("account_uuid", &bound.account_uuid),
        ("organization_uuid", &bound.organization_uuid),
    ] {
        if let Some(value) = response["oauth_account"]
            .get(field)
            .filter(|v| !v.is_null())
        {
            ensure!(
                value.as_str() == Some(expected.as_str()),
                "Claude bootstrap account/organization mismatch"
            );
        }
    }
    Ok(())
}

fn merge_bootstrap(previous: &mut Value, response: &Value) -> Result<()> {
    let bootstrap: Bootstrap = serde_json::from_value(response.clone())?;
    let narrowed = bootstrap.narrowed.unwrap_or(false);
    // Only these fields preserve prior values in a narrowed response. client_data
    // always becomes null on omission and is stored separately in its model slot.
    for field in [
        "model_access",
        "additional_model_options",
        "additional_model_costs",
        "org_model_default",
        "auto_compact_windows",
    ] {
        if let Some(value) = response.get(field).filter(|v| !v.is_null()) {
            previous[field] = value.clone();
        } else if !narrowed {
            previous[field] = Value::Null;
        }
    }
    previous["client_data"] = response.get("client_data").cloned().unwrap_or(Value::Null);
    Ok(())
}

fn model_capabilities(
    state: &State,
    requested: &str,
    effort: Option<&str>,
) -> Result<Capabilities> {
    let policy = &state.policy;
    ensure!(
        !requested.is_empty() && requested.len() <= 200,
        "invalid Claude model"
    );
    let aliases = aliases(state.served.as_ref());
    let resolved = resolve(requested, &aliases);
    // No unknown-model fallback, including otherwise tempting behaves_as mappings.
    let mut facts = protocol::source_model_facts(&resolved, &[])
        .context("unknown Claude model; verified baseline facts unavailable")?;
    let canonical = facts.id.clone();
    let matches = |name: &str| {
        let name = resolve(name.trim_end_matches("[1m]"), &aliases);
        protocol::source_model_facts(&name, &[]).is_some_and(|f| f.id == canonical)
    };
    ensure!(
        !policy.denied_models.iter().any(|m| matches(m)),
        "Claude model denied by managed policy"
    );
    ensure!(
        policy
            .allowed_models
            .as_ref()
            .is_none_or(|ids| ids.iter().any(|m| matches(m))),
        "Claude model not allowed by managed policy"
    );
    if let Some(served) = &state.served {
        if let Some(row) = served.rows.iter().find(|r| matches(&r.id)) {
            ensure!(
                !row.disabled && !row.deprecated,
                "Claude model disabled by active served catalog"
            );
            apply_runtime(&mut facts.capabilities, row)?;
        }
        if let Some(enforced) = &served.enforced_default_model {
            ensure!(
                matches(enforced),
                "Claude explicit model conflicts with enforced organization selection"
            );
        }
    }
    let defaults = &state.bootstrap["org_model_default"];
    if defaults["override_user_selection"].as_bool() == Some(true) {
        let name = defaults["name"]
            .as_str()
            .context("invalid enforced Claude organization model")?;
        ensure!(
            matches(name),
            "Claude explicit model conflicts with enforced organization selection"
        );
    }
    if defaults["override_user_effort"].as_bool() == Some(true)
        && defaults["name"].as_str().is_some_and(matches)
        && let Some(required) = defaults["default_effort_level"].as_str()
    {
        ensure!(
            EFFORTS.contains(&required),
            "unknown enforced Claude organization effort"
        );
        if let Some(effort) = effort {
            ensure!(
                effort == required,
                "Claude explicit effort conflicts with enforced organization effort"
            );
        }
    }
    facts.capabilities.betas = baseline_betas(&canonical, policy);
    let bootstrap: Bootstrap = serde_json::from_value(state.bootstrap.clone())?;
    // Normalize aliases/dates/suffixes in deny rows and ignore an unknown effort cap,
    // as dJt does; a known cap still restricts selection without clamping the caller.
    let mut bootstrap = bootstrap;
    if let Some(rows) = &mut bootstrap.model_access {
        for row in rows {
            if matches(&row.api_name) {
                row.api_name = canonical.clone();
            }
            if row
                .max_effort_level
                .as_deref()
                .is_some_and(|e| !EFFORTS.contains(&e))
            {
                row.max_effort_level = None;
            }
        }
    }
    if let Some(rows) = &mut bootstrap.additional_model_options {
        for row in rows {
            if matches(&row.model) {
                row.model = canonical.clone();
            }
        }
    }
    let caps = bootstrap.capabilities(&resolved, &BTreeMap::new(), &[facts], true)?;
    protocol::validate_selection(&caps, effort, false)?;
    Ok(caps)
}

fn apply_runtime(caps: &mut Capabilities, row: &ServedModel) -> Result<()> {
    if let Some(n) = row
        .max_input_tokens
        .or(row.context_window)
        .filter(|n| (8192..=1_000_000).contains(n))
    {
        caps.context_window = n;
    }
    if let Some(n) = row
        .max_output_tokens
        .filter(|n| (1024..=128000).contains(n))
    {
        caps.max_output_tokens = n;
    }
    if let Some(n) = row
        .default_output_tokens
        .filter(|n| (1024..=128000).contains(n))
    {
        caps.default_output_tokens = n;
    }
    caps.default_output_tokens = caps.default_output_tokens.min(caps.max_output_tokens);
    if let Some(efforts) = &row.effort_levels {
        let efforts: Vec<_> = efforts.iter().map(|e| normalized(e)).collect();
        ensure!(
            efforts.iter().all(|e| EFFORTS.contains(&e.as_str())),
            "unknown Claude runtime effort"
        );
        caps.efforts = efforts;
    }
    let grants = |s| row.capabilities.iter().any(|c| normalized(c) == s);
    if row.effort_levels.is_none() {
        if grants("effort") {
            caps.efforts
                .extend(["low", "medium", "high"].map(str::to_owned));
        }
        if grants("xhigh_effort") {
            caps.efforts.push("xhigh".into());
        }
        if grants("max_effort") {
            caps.efforts.push("max".into());
        }
        caps.efforts
            .sort_by_key(|e| EFFORTS.iter().position(|v| v == &e.as_str()));
        caps.efforts.dedup();
    }
    caps.fast |= grants("fast_mode");
    caps.thinking |= grants("adaptive_thinking") || grants("rejects_disabled_thinking");
    caps.adaptive_thinking |= grants("adaptive_thinking");
    caps.thinking_required |= grants("rejects_disabled_thinking");
    Ok(())
}

fn baseline_betas(id: &str, policy: &VerifiedPolicy) -> Vec<String> {
    // Independent predicates from YF/XF/w7/hBn, pBn/WF/zF in selected 0139.js.
    // Agentic Claude-Code and OAuth are classification, not experimental entitlements.
    let mut betas = vec![
        protocol::CLAUDE_CODE_BETA.into(),
        protocol::OAUTH_BETA.into(),
    ];
    let interleaved = !id.starts_with("claude-3-");
    let experimental = policy.experimental_betas && !policy.hipaa;
    if interleaved {
        betas.push("interleaved-thinking-2025-05-14".into());
    }
    if experimental {
        if interleaved {
            betas.push("thinking-token-count-2026-05-13".into());
            betas.push("context-management-2025-06-27".into());
        }
        // k7e/tXn: display updates need interleaved thinking, which Haiku 4.5 lacks.
        if interleaved && id != "claude-haiku-4-5" {
            betas.push(protocol::DISPLAY_UPDATES_BETA.into());
        }
        betas.push("prompt-caching-scope-2026-01-05".into());
    }
    // zF's concrete pre-4.8 ordering, not a substring/version guess or family blacklist.
    let pre48 = matches!(
        id,
        "claude-3-5-haiku"
            | "claude-3-5-sonnet"
            | "claude-3-7-sonnet"
            | "claude-opus-4-0"
            | "claude-sonnet-4-0"
            | "claude-opus-4-1"
            | "claude-sonnet-4-5"
            | "claude-haiku-4-5"
            | "claude-opus-4-5"
            | "claude-opus-4-6"
            | "claude-sonnet-4-6"
            | "claude-opus-4-7"
    );
    if !policy.hipaa && !pre48 {
        betas.push("mid-conversation-system-2026-04-07".into());
    }
    if experimental
        && matches!(
            id,
            "claude-opus-5-5" | "claude-sonnet-5-5" | "claude-haiku-5-5"
        )
    {
        betas.push("per-turn-control-2026-07-01".into());
        betas.push("mid-conversation-tool-changes-2026-07-01".into());
    }
    // Other per-turn/tool-change table rows require separately verified capabilities.
    // Explicit timing/inline-tool/redaction/1m UI flags are not invented here.
    betas
}

fn validate_body(body: &Value, caps: &Capabilities, state: &State) -> Result<()> {
    ensure!(
        body["model"].as_str() == Some(caps.model.as_str()),
        "Claude explicit wire model changed; refusing remap"
    );
    let max = body["max_tokens"]
        .as_u64()
        .context("invalid Claude output limit")?;
    ensure!(
        max > 0 && max <= caps.max_output_tokens,
        "Claude output limit conflicts with current capabilities"
    );
    ensure!(
        !caps.thinking_required
            || matches!(
                body["thinking"]["type"].as_str(),
                Some("adaptive" | "enabled")
            ),
        "Claude model requires thinking"
    );
    let policy = &state.policy;
    let defaults = &state.bootstrap["org_model_default"];
    if defaults["override_user_effort"].as_bool() == Some(true) {
        let maps = aliases(state.served.as_ref());
        let same_model = defaults["name"]
            .as_str()
            .and_then(|m| protocol::source_model_facts(&resolve(m, &maps), &[]))
            .is_some_and(|f| f.capabilities.model == caps.model);
        if same_model {
            ensure!(
                body["output_config"]["effort"] == defaults["default_effort_level"],
                "Claude body omits/conflicts with enforced organization effort"
            );
        }
    }
    let mut allowed = caps.betas.clone();
    if !caps.efforts.is_empty() {
        allowed.push("effort-2025-11-24".into());
    }
    if caps.fast {
        allowed.push("fast-mode-2026-02-01".into());
    }
    if policy.experimental_betas && !policy.hipaa {
        allowed.push("extended-cache-ttl-2025-04-11".into());
        let id = protocol::source_model_facts(&caps.model, &[])
            .context("unknown Claude model")?
            .id;
        if !matches!(
            id.as_str(),
            "claude-3-5-haiku"
                | "claude-3-5-sonnet"
                | "claude-3-7-sonnet"
                | "claude-opus-4-0"
                | "claude-sonnet-4-0"
        ) {
            allowed.push("structured-outputs-2025-12-15".into());
        }
    }
    if let Some(betas) = body.get("betas") {
        for beta in betas.as_array().context("invalid Claude betas")? {
            ensure!(
                beta.as_str()
                    .is_some_and(|b| allowed.iter().any(|a| a == b)),
                "Claude beta requires a verified model/policy predicate"
            );
        }
    }
    Ok(())
}

fn installation_device_id() -> Result<String> {
    let home = crate::provider_bin::home_directory()
        .context("HOME missing for Borg private device metadata")?;
    private_device_id(&home)
}

#[cfg(unix)]
fn private_device_id(home: &Path) -> Result<String> {
    use std::{
        ffi::CString,
        fs::File,
        io::{Read, Write},
        os::{
            fd::{AsRawFd, FromRawFd},
            unix::fs::MetadataExt,
        },
    };
    ensure!(home.is_absolute(), "Borg metadata home must be absolute");
    // Resolve every component through directory descriptors. Reject symlinks instead
    // of chmodding/following foreign paths; no check-then-open path race.
    let mut dir = File::open("/")?;
    for component in home.components() {
        match component {
            std::path::Component::RootDir => continue,
            std::path::Component::Normal(name) => dir = open_directory(&dir, name, false)?,
            _ => anyhow::bail!("unsafe Borg metadata path"),
        }
    }
    let home_meta = dir.metadata()?;
    ensure!(
        home_meta.uid() == unsafe { libc::geteuid() } && home_meta.mode() & 0o022 == 0,
        "unsafe Borg metadata home"
    );
    for (name, private) in [
        (".config", false),
        ("borg", true),
        ("claude-subscription", true),
    ] {
        dir = open_directory(&dir, std::ffi::OsStr::new(name), true)?;
        let meta = dir.metadata()?;
        ensure!(
            meta.uid() == unsafe { libc::geteuid() }
                && meta.mode() & (if private { 0o077 } else { 0o022 }) == 0,
            "unsafe Borg private metadata directory"
        );
    }
    let open = |name: &str, flags| -> Result<File> {
        let name = CString::new(name)?;
        let fd = unsafe {
            libc::openat(
                dir.as_raw_fd(),
                name.as_ptr(),
                flags | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
                0o600,
            )
        };
        ensure!(
            fd >= 0,
            "cannot open Borg device metadata: {}",
            std::io::Error::last_os_error()
        );
        let file = unsafe { File::from_raw_fd(fd) };
        let meta = file.metadata()?;
        ensure!(
            meta.is_file()
                && meta.uid() == unsafe { libc::geteuid() }
                && meta.mode() & 0o077 == 0
                && meta.nlink() == 1,
            "unsafe Borg device metadata file"
        );
        Ok(file)
    };
    let lock = open("device-id.lock", libc::O_CREAT | libc::O_RDWR)?;
    lock.lock()?;
    let name = CString::new("device-id")?;
    let fd = unsafe {
        libc::openat(
            dir.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
        )
    };
    if fd >= 0 {
        let mut file = unsafe { File::from_raw_fd(fd) };
        let meta = file.metadata()?;
        ensure!(
            meta.is_file()
                && meta.uid() == unsafe { libc::geteuid() }
                && meta.mode() & 0o077 == 0
                && meta.nlink() == 1,
            "unsafe Borg device metadata file"
        );
        let mut bytes = Vec::new();
        Read::by_ref(&mut file).take(65).read_to_end(&mut bytes)?;
        ensure!(
            bytes.len() == 64
                && bytes
                    .iter()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b)),
            "invalid Borg device ID; refusing replacement"
        );
        return String::from_utf8(bytes).context("invalid Borg device ID");
    }
    ensure!(
        std::io::Error::last_os_error().kind() == std::io::ErrorKind::NotFound,
        "unsafe/unreadable Borg device metadata path"
    );
    let mut random = [0; 32];
    getrandom::fill(&mut random).map_err(|_| anyhow::anyhow!("Borg device entropy unavailable"))?;
    let id = hex::encode(random);
    let mut file = open("device-id", libc::O_CREAT | libc::O_EXCL | libc::O_WRONLY)?;
    file.write_all(id.as_bytes())?;
    file.sync_all()?;
    dir.sync_all()?;
    Ok(id)
}

#[cfg(unix)]
fn open_directory(
    parent: &std::fs::File,
    name: &std::ffi::OsStr,
    create: bool,
) -> Result<std::fs::File> {
    use std::os::{
        fd::{AsRawFd, FromRawFd},
        unix::ffi::OsStrExt,
    };
    let name = std::ffi::CString::new(name.as_bytes())?;
    if create {
        let result = unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o700) };
        ensure!(
            result == 0
                || std::io::Error::last_os_error().kind() == std::io::ErrorKind::AlreadyExists,
            "cannot create Borg private metadata directory"
        );
    }
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    ensure!(fd >= 0, "unsafe/unreadable Borg metadata directory");
    Ok(unsafe { std::fs::File::from_raw_fd(fd) })
}

#[cfg(not(unix))]
fn private_device_id(_: &Path) -> Result<String> {
    anyhow::bail!(
        "Borg native device persistence requires verified private filesystem ACL support on this platform"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::claude_subscription::tests as auth_tests;
    use std::{
        collections::VecDeque,
        sync::{
            Mutex as StdMutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    struct Metadata {
        bearer: &'static str,
        policy: std::result::Result<Option<Value>, &'static str>,
        bootstrap: Value,
        write: Option<(AuthAuthority, Value)>,
    }
    tokio::task_local! {
        static METADATA: Arc<StdMutex<VecDeque<Metadata>>>;
        static SENDS: Arc<AtomicUsize>;
    }
    pub(super) fn fetch_policy(access: &SubscriptionAccess) -> Result<Option<Value>> {
        METADATA
            .try_with(|queue| {
                let queue = queue.lock().unwrap();
                let next = queue.front().context("unexpected offline policy fetch")?;
                ensure!(
                    next.bearer == access.token.bearer(),
                    "offline policy bearer mismatch"
                );
                next.policy.clone().map_err(anyhow::Error::msg)
            })
            .context("offline metadata context missing")?
    }
    pub(super) async fn fetch_bootstrap(access: &SubscriptionAccess) -> Result<Value> {
        let next = METADATA
            .try_with(|queue| queue.lock().unwrap().pop_front())
            .context("offline metadata context missing")?
            .context("unexpected offline bootstrap fetch")?;
        ensure!(
            next.bearer == access.token.bearer(),
            "offline bootstrap bearer mismatch"
        );
        if let Some((authority, document)) = next.write {
            auth_tests::save_fixture(&authority, &document).await?;
        }
        Ok(next.bootstrap)
    }
    pub(super) fn record_send() {
        SENDS.with(|sends| {
            sends.fetch_add(1, Ordering::SeqCst);
        });
    }
    fn connector(authority: AuthAuthority) -> NativeConnector {
        NativeConnector {
            authority,
            bound: auth_tests::identity(),
            device_id: Arc::new("offline-device".into()),
            http: reqwest::Client::new(),
            state: Arc::new(Mutex::new(state())),
        }
    }
    fn sibling() -> Value {
        let mut doc = auth_tests::document(chrono::Utc::now().timestamp_millis() + 3_600_000);
        doc["claudeAiOauth"]["accessToken"] = json!("sibling-access");
        doc["claudeAiOauth"]["refreshToken"] = json!("sibling-refresh");
        doc
    }

    // Stale bootstrap used to return new access without checking new metadata.
    // An injected durable rotation then fresh refusal must fail before any context send.
    #[tokio::test]
    async fn bootstrap_generation_change_refetches_and_fresh_denial_prevents_send() {
        let directory = tempfile::tempdir().unwrap();
        let authority =
            AuthAuthority::selected(directory.path().to_owned(), auth::CredentialStore::File)
                .unwrap();
        auth_tests::save_fixture(
            &authority,
            &auth_tests::document(chrono::Utc::now().timestamp_millis() + 3_600_000),
        )
        .await
        .unwrap();
        let client = connector(authority.clone());
        let metadata = Arc::new(StdMutex::new(VecDeque::from([
            Metadata {
                bearer: "fixture-access",
                policy: Err("stale policy denial"),
                bootstrap: json!({}),
                write: Some((authority, sibling())),
            },
            Metadata {
                bearer: "sibling-access",
                policy: Err("fresh policy denial"),
                bootstrap: json!({}),
                write: None,
            },
        ])));
        let sends = Arc::new(AtomicUsize::new(0));
        // One profile verification per distinct bearer.
        let result = auth_tests::offline(
            vec![auth_tests::profile_fixture(); 2],
            vec![],
            METADATA.scope(
                metadata.clone(),
                SENDS.scope(
                    sends.clone(),
                    client.send_model(
                        &json!({"model":"haiku"}),
                        "offline-id",
                        "offline-session",
                        None,
                    ),
                ),
            ),
        )
        .await;
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("fresh policy denial")
        );
        assert!(
            metadata.lock().unwrap().is_empty(),
            "fresh generation metadata must be fetched"
        );
        assert_eq!(sends.load(Ordering::SeqCst), 0);
        let state = client.state.lock().await;
        assert!(state.fetched.is_empty());
        assert_eq!(state.bootstrap, json!({}));
    }

    // Credentials can rotate after a bootstrap cache hit. Re-admit the new
    // generation, rather than returning Fatal or reusing cached capabilities.
    #[tokio::test]
    async fn final_generation_change_readmits_same_account() {
        let directory = tempfile::tempdir().unwrap();
        let authority =
            AuthAuthority::selected(directory.path().to_owned(), auth::CredentialStore::File)
                .unwrap();
        auth_tests::save_fixture(
            &authority,
            &auth_tests::document(chrono::Utc::now().timestamp_millis() + 3_600_000),
        )
        .await
        .unwrap();
        let client = connector(authority.clone());
        let access =
            auth_tests::offline(vec![auth_tests::profile_fixture()], vec![], client.access())
                .await
                .unwrap();
        client
            .state
            .lock()
            .await
            .fetched
            .insert("haiku".into(), (Instant::now(), generation(&access)));
        let metadata = Arc::new(StdMutex::new(VecDeque::from([Metadata {
            bearer: "sibling-access",
            policy: Ok(Some(json!({"compliance_taints":["hipaa"]}))),
            bootstrap: json!({}),
            write: None,
        }])));
        auth_tests::save_fixture(&authority, &sibling())
            .await
            .unwrap();
        let (current, caps) = auth_tests::offline(
            vec![auth_tests::profile_fixture()],
            vec![],
            METADATA.scope(
                metadata.clone(),
                client.admit(access, "haiku", None, false, None),
            ),
        )
        .await
        .unwrap();
        assert_eq!(current.token.bearer(), "sibling-access");
        assert!(metadata.lock().unwrap().is_empty());
        assert!(client.state.lock().await.policy.hipaa);
        assert!(
            !caps
                .betas
                .iter()
                .any(|b| b == "thinking-token-count-2026-05-13")
        );
    }

    #[tokio::test]
    async fn bootstrap_generation_churn_is_bounded_before_send() {
        let directory = tempfile::tempdir().unwrap();
        let authority =
            AuthAuthority::selected(directory.path().to_owned(), auth::CredentialStore::File)
                .unwrap();
        auth_tests::save_fixture(
            &authority,
            &auth_tests::document(chrono::Utc::now().timestamp_millis() + 3_600_000),
        )
        .await
        .unwrap();
        let client = connector(authority.clone());
        let mut steps = VecDeque::new();
        for (old, new) in [
            ("fixture-access", "generation-1"),
            ("generation-1", "generation-2"),
            ("generation-2", "generation-3"),
        ] {
            let mut doc = sibling();
            doc["claudeAiOauth"]["accessToken"] = json!(new);
            steps.push_back(Metadata {
                bearer: old,
                policy: Ok(None),
                bootstrap: json!({}),
                write: Some((authority.clone(), doc)),
            });
        }
        let metadata = Arc::new(StdMutex::new(steps));
        let sends = Arc::new(AtomicUsize::new(0));
        let error = auth_tests::offline(
            vec![auth_tests::profile_fixture(); 4],
            vec![],
            METADATA.scope(
                metadata.clone(),
                SENDS.scope(
                    sends.clone(),
                    client.send_model(
                        &json!({"model":"haiku"}),
                        "offline-id",
                        "offline-session",
                        None,
                    ),
                ),
            ),
        )
        .await
        .err()
        .unwrap();
        assert!(error.to_string().contains("kept changing"));
        assert!(metadata.lock().unwrap().is_empty());
        assert_eq!(sends.load(Ordering::SeqCst), 0);
    }

    // These protect account policy, silent fallback, and local filesystem boundaries;
    // compiler/format checks cannot verify the semantic or permission failure modes.
    fn state() -> State {
        State {
            policy: VerifiedPolicy::default(),
            served: None,
            bootstrap: json!({}),
            fetched: BTreeMap::new(),
            client_data: BTreeMap::new(),
            fast: None,
        }
    }
    #[test]
    fn narrowed_preserves_denials_full_omission_clears() {
        let mut state = state();
        merge_bootstrap(
            &mut state.bootstrap,
            &json!({"model_access":[{"api_name":"claude-opus-5-5","entitled":false}]}),
        )
        .unwrap();
        merge_bootstrap(&mut state.bootstrap, &json!({"narrowed":true})).unwrap();
        assert!(model_capabilities(&state, "opus", None).is_err());
        merge_bootstrap(
            &mut state.bootstrap,
            &json!({"narrowed":true,"model_access":null}),
        )
        .unwrap();
        assert!(model_capabilities(&state, "opus", None).is_err());
        merge_bootstrap(&mut state.bootstrap, &json!({})).unwrap();
        assert!(model_capabilities(&state, "opus", None).is_ok());
    }
    #[test]
    fn explicit_ids_never_follow_alias_or_org_default() {
        let mut state = state();
        state.bootstrap =
            json!({"org_model_default":{"name":"sonnet","override_user_selection":true}});
        assert!(model_capabilities(&state, "claude-opus-5-5", None).is_err());
        state.bootstrap = json!({});
        assert_eq!(
            model_capabilities(&state, "claude-opus-4-5-20251101", None)
                .unwrap()
                .model,
            "claude-opus-4-5-20251101"
        );
        assert!(model_capabilities(&state, "claude-unknown", None).is_err());
    }
    #[test]
    fn model_and_policy_beta_gates_are_independent() {
        let mut state = state();
        let caps = model_capabilities(&state, "opus", Some("xhigh")).unwrap();
        assert_eq!(caps.context_window, 1_000_000);
        assert!(caps.fast);
        assert!(!model_capabilities(&state, "sonnet", None).unwrap().fast);
        assert!(
            caps.betas
                .iter()
                .any(|b| b == "thinking-token-count-2026-05-13")
        );
        assert!(!caps.betas.iter().any(|b| b.starts_with("context-1m")));
        state.policy.hipaa = true;
        let caps = model_capabilities(&state, "haiku", None).unwrap();
        assert_eq!(
            caps.betas,
            [
                protocol::CLAUDE_CODE_BETA,
                protocol::OAUTH_BETA,
                "interleaved-thinking-2025-05-14"
            ]
        );
    }
    #[test]
    fn bootstrap_cannot_cross_account_or_org() {
        let bound = AccountIdentity {
            account_uuid: "a".into(),
            organization_uuid: "o".into(),
        };
        assert!(
            check_bootstrap_identity(
                &json!({"oauth_account":{"account_uuid":"a","organization_uuid":"other"}}),
                &bound
            )
            .is_err()
        );
        assert!(check_bootstrap_identity(&json!({}), &bound).is_ok());
    }
    #[cfg(unix)]
    #[test]
    fn device_is_stable_private_and_rejects_symlink_paths() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let home = tempfile::tempdir().unwrap();
        let id = private_device_id(home.path()).unwrap();
        assert_eq!(id.len(), 64);
        assert_eq!(id, private_device_id(home.path()).unwrap());
        assert!(
            id.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        );
        let path = home
            .path()
            .join(".config/borg/claude-subscription/device-id");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(private_device_id(home.path()).is_err());
        let other = tempfile::tempdir().unwrap();
        symlink(home.path().join(".config"), other.path().join(".config")).unwrap();
        assert!(private_device_id(other.path()).is_err());
    }
}
