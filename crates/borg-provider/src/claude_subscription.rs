//! Borg-owned Claude subscription auth, with no API-key or upstream-runtime fallback.
//! Wire contracts: Claude Code 2.1.296 extracted chunks 0032, 0139 and 0195.
//! The embedding controller selects ONE persistent authority; never copy refresh tokens.

mod browser;
mod login;
mod storage;
#[cfg(test)]
pub(crate) mod tests;

pub use browser::{BrowserLogin, begin_browser_login};
pub use login::PendingLogin;
pub use storage::{AuthAuthority, CredentialStore, KeychainIdentity};

#[cfg(not(test))]
use anyhow::bail;
use anyhow::{Context, Result, ensure};
#[cfg(not(test))]
use futures::StreamExt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::time::Duration;

const CLIENT_ID: &str = "9d1c250a-e61b-44d9-88ed-5944d1962f5e";
#[cfg(not(test))]
const TOKEN_ENDPOINT: &str = "https://platform.claude.com/v1/oauth/token";
#[cfg(not(test))]
const PROFILE_ENDPOINT: &str = "https://api.anthropic.com/api/oauth/profile";
#[cfg(not(test))]
const FAST_ENDPOINT: &str = "https://api.anthropic.com/api/claude_code_penguin_mode";
#[cfg(not(test))]
const USAGE_ENDPOINT: &str = "https://api.anthropic.com/api/oauth/usage";
const MAX_BYTES: usize = 1024 * 1024;
const REFRESH_SKEW_MS: i64 = 300_000;
const SCOPES: &[&str] = &[
    "user:profile",
    "user:inference",
    "user:sessions:claude_code",
    "user:mcp_servers",
    "user:file_upload",
    "user:plugins",
];

/// Secrets intentionally have no Debug or Serialize implementation.
#[derive(Clone)]
pub struct OAuthToken {
    access_token: String,
    refresh_token: Option<String>,
    pub expires_at_ms: Option<i64>,
    pub scopes: Vec<String>,
    pub subscription_type: Option<String>,
    pub rate_limit_tier: Option<String>,
}

impl OAuthToken {
    pub fn bearer(&self) -> &str {
        &self.access_token
    }
    pub fn needs_refresh_at(&self, now_ms: i64) -> bool {
        self.expires_at_ms
            .is_some_and(|expiry| now_ms.saturating_add(REFRESH_SKEW_MS) >= expiry)
    }
    fn from_document(document: &Value) -> Result<Self> {
        let value = &document["claudeAiOauth"];
        let scopes = value["scopes"]
            .as_array()
            .context("Claude subscription scopes missing")?
            .iter()
            .map(|v| {
                v.as_str()
                    .map(str::to_owned)
                    .context("invalid Claude scope")
            })
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            scopes.iter().any(|s| s == "user:inference"),
            "Claude login lacks user:inference; reconnect the subscription (no API-key fallback)"
        );
        let expiry = match &value["expiresAt"] {
            Value::Null => None,
            v => Some(v.as_i64().context("invalid Claude token expiry")?),
        };
        Ok(Self {
            access_token: secret(
                &value["accessToken"],
                "Claude subscription access token missing",
            )?,
            refresh_token: optional_string(&value["refreshToken"]),
            expires_at_ms: expiry,
            scopes,
            subscription_type: optional_string(&value["subscriptionType"]),
            rate_limit_tier: optional_string(&value["rateLimitTier"]),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountIdentity {
    pub account_uuid: String,
    pub organization_uuid: String,
}
impl AccountIdentity {
    pub fn subscription_id(&self) -> String {
        let mut hash = Sha256::new();
        hash.update(b"claude-subscription\0");
        hash.update(self.account_uuid.as_bytes());
        hash.update(b"\0");
        hash.update(self.organization_uuid.as_bytes());
        hex::encode(hash.finalize())
    }
    fn check(&self, other: &Self) -> Result<()> {
        ensure!(
            self == other,
            "Claude subscription account/organization changed; reconnect explicitly, no credentials were replaced"
        );
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct SubscriptionProfile {
    pub identity: AccountIdentity,
    pub email: Option<String>,
    pub subscription_type: Option<String>,
    pub rate_limit_tier: Option<String>,
    /// Includes billing type, seat/trial/extra-usage entitlements without guessing a plan.
    pub raw: Value,
}
impl SubscriptionProfile {
    fn parse(raw: Value) -> Result<Self> {
        let account = &raw["account"];
        let org = &raw["organization"];
        let kind = match org["organization_type"].as_str() {
            Some("claude_max") => Some("max"),
            Some("claude_pro") => Some("pro"),
            Some("claude_enterprise") => Some("enterprise"),
            Some("claude_team") => Some("team"),
            _ => None,
        }
        .map(str::to_owned);
        let result = Self {
            identity: AccountIdentity {
                account_uuid: secret(&account["uuid"], "Claude profile account UUID missing")?,
                organization_uuid: secret(
                    &org["uuid"],
                    "Claude profile organization UUID missing",
                )?,
            },
            email: optional_string(&account["email"]),
            subscription_type: kind,
            rate_limit_tier: optional_string(&org["rate_limit_tier"]),
            raw,
        };
        ensure!(
            !result.identity.account_uuid.contains('\0')
                && !result.identity.organization_uuid.contains('\0'),
            "invalid Claude account identity"
        );
        Ok(result)
    }
}

/// Local availability probe: no model call, refresh, profile request or token output.
/// An expired token may still be recoverable through its refresh token.
pub async fn credentials_present(authority: &AuthAuthority) -> Result<bool> {
    match authority.load().await {
        Ok(document) => Ok(OAuthToken::from_document(&document).is_ok()),
        Err(error) if error.is::<storage::MissingAuthority>() => Ok(false),
        Err(error) => Err(error),
    }
}

/// Detailed subscription status, preserving the frontend's existing JSON contract.
pub async fn status(authority: AuthAuthority) -> Result<Value> {
    if !credentials_present(&authority).await? {
        return Ok(json!({"loggedIn":false}));
    }
    let access = discover(authority).await?;
    Ok(
        json!({"loggedIn":true,"authMethod":"oauth_token", "email":access.profile.email,
        "orgName":access.profile.raw["organization"]["name"], "subscriptionType":access.profile.subscription_type}),
    )
}

pub struct SubscriptionAccess {
    pub token: OAuthToken,
    pub profile: SubscriptionProfile,
}

/// Read/refresh the selected authority. `rejected_bearer` enables one 401 recovery;
/// a sibling's newer bearer is adopted rather than rotated a second time.
/// Detaching this task on caller cancellation protects rotation -> durable save.
pub async fn access(
    authority: AuthAuthority,
    expected: AccountIdentity,
    rejected_bearer: Option<String>,
) -> Result<SubscriptionAccess> {
    spawn_auth(async move { access_inner(authority, Some(expected), rejected_bearer).await })
        .await
        .map_err(|_| anyhow::anyhow!("Claude auth task failed"))?
}

/// First connection/import, including an expired native store. Any stored binding
/// remains mandatory; the first successful profile binds an unbound authority.
/// The controller must compare the returned subscription_id with its session's
/// expected hash BEFORE sending model requests or replaying provider continuation.
pub async fn discover(authority: AuthAuthority) -> Result<SubscriptionAccess> {
    spawn_auth(async move { access_inner(authority, None, None).await })
        .await
        .map_err(|_| anyhow::anyhow!("Claude auth task failed"))?
}

fn spawn_auth<F>(future: F) -> tokio::task::JoinHandle<Result<SubscriptionAccess>>
where
    F: std::future::Future<Output = Result<SubscriptionAccess>> + Send + 'static,
{
    #[cfg(test)]
    let future = tests::inherit_fetch_context(future);
    tokio::spawn(future)
}

async fn access_inner(
    authority: AuthAuthority,
    mut expected: Option<AccountIdentity>,
    rejected: Option<String>,
) -> Result<SubscriptionAccess> {
    let initial = authority.load().await?;
    adopt_binding(&initial, &mut expected)?;
    let token = OAuthToken::from_document(&initial)?;
    if !token.needs_refresh_at(now_ms()) && rejected.as_deref() != Some(token.bearer()) {
        return verified_access(&authority, token, expected.as_ref()).await;
    }
    let lock = authority.lock().await?;
    let document = authority.load().await?;
    adopt_binding(&document, &mut expected)?;
    let old = OAuthToken::from_document(&document)?;
    if (old.bearer() != token.bearer() || rejected.as_deref() != Some(old.bearer()))
        && !old.needs_refresh_at(now_ms())
    {
        return verified_access(&authority, old, expected.as_ref()).await;
    }
    let posted = old
        .refresh_token
        .as_deref()
        .context("Claude subscription cannot refresh; reconnect (no API-key fallback)")?;
    ensure!(
        document["claudeAiOauth"]["refreshTokenExpiresAt"]
            .as_i64()
            .is_none_or(|expiry| expiry > now_ms()),
        "Claude refresh token expired; reconnect the subscription"
    );
    ensure!(
        document["claudeAiOauth"]["clientId"]
            .as_str()
            .unwrap_or(CLIENT_ID)
            == CLIENT_ID,
        "Claude credential belongs to another OAuth client; reconnect with Borg"
    );
    let request = json!({"grant_type":"refresh_token", "refresh_token":posted, "client_id":CLIENT_ID, "scope":old.scopes.join(" ")});
    let response = match fetch(Operation::Refresh, Some(&request), None).await {
        Ok(value) => value,
        Err(error) => {
            let latest = authority.load().await?;
            adopt_binding(&latest, &mut expected)?;
            let sibling = OAuthToken::from_document(&latest)?;
            if sibling.bearer() != old.bearer() && !sibling.needs_refresh_at(now_ms()) {
                return verified_access(&authority, sibling, expected.as_ref()).await;
            }
            return Err(error);
        }
    };
    if let Some(expected) = &expected {
        check_token_account(&response, expected)?;
    }
    let candidate = rotated_document(document, &response, now_ms())?;
    // Rotation must reach durable storage even if a subsequent profile request
    // fails. No bearer becomes model-eligible until verified_access succeeds.
    lock.check()?;
    let write_lock = authority.write_lock().await?;
    let mut latest = authority.load().await?;
    adopt_binding(&latest, &mut expected)?;
    if latest["claudeAiOauth"]["refreshToken"].as_str() == Some(posted) {
        latest = compare_and_swap(latest, posted, &candidate)?;
        write_lock.check()?;
        authority.save(&latest).await?;
    }
    drop(write_lock);
    let result = OAuthToken::from_document(&latest)?;
    verified_access(&authority, result, expected.as_ref()).await
}

async fn verified_access(
    authority: &AuthAuthority,
    token: OAuthToken,
    expected: Option<&AccountIdentity>,
) -> Result<SubscriptionAccess> {
    let mut token = token;
    let mut expected = expected.cloned();
    for _ in 0..3 {
        let profile = fetch_profile(&token, expected.as_ref()).await?;
        // Discovery also pins its first verified account across concurrent rotations.
        expected.get_or_insert_with(|| profile.identity.clone());
        let lock = authority.write_lock().await?;
        let mut latest = authority.load().await?;
        check_saved_identity(&latest, &profile.identity)?;
        let current = OAuthToken::from_document(&latest)?;
        if current.bearer() != token.bearer() {
            drop(lock);
            ensure!(
                !current.needs_refresh_at(now_ms()),
                "Claude replacement credential needs refresh; reconnect the subscription"
            );
            token = current;
            continue;
        }
        apply_profile(&mut latest, &profile)?;
        lock.check()?;
        authority.save(&latest).await?;
        return Ok(SubscriptionAccess { token, profile });
    }
    anyhow::bail!(
        "Claude credentials kept changing during profile verification; no model request sent"
    )
}

fn adopt_binding(document: &Value, expected: &mut Option<AccountIdentity>) -> Result<()> {
    if let Some(saved) = document.get("borgClaudeSubscription") {
        let identity = AccountIdentity {
            account_uuid: secret(&saved["accountUuid"], "invalid saved Claude identity")?,
            organization_uuid: secret(&saved["organizationUuid"], "invalid saved Claude identity")?,
        };
        if let Some(expected) = expected {
            expected.check(&identity)?;
        }
        *expected = Some(identity);
    }
    Ok(())
}

/// Explicit refresh/401 recovery, using the same locked CAS path as access.
pub async fn refresh(
    authority: AuthAuthority,
    expected: AccountIdentity,
    rejected_bearer: String,
) -> Result<SubscriptionAccess> {
    access(authority, expected, Some(rejected_bearer)).await
}

pub async fn profile(
    token: &OAuthToken,
    expected: &AccountIdentity,
) -> Result<SubscriptionProfile> {
    fetch_profile(token, Some(expected)).await
}
async fn fetch_profile(
    token: &OAuthToken,
    expected: Option<&AccountIdentity>,
) -> Result<SubscriptionProfile> {
    ensure!(
        token.scopes.iter().any(|s| s == "user:profile"),
        "Claude login lacks user:profile; reconnect to expose account and usage"
    );
    let profile =
        SubscriptionProfile::parse(fetch(Operation::Profile, None, Some(token.bearer())).await?)?;
    if let Some(expected) = expected {
        expected.check(&profile.identity)?;
    }
    Ok(profile)
}

#[derive(Clone, Debug, PartialEq)]
pub struct UsageWindow {
    pub name: String,
    /// Native usage percentages converted to a fraction; values above 1 remain visible.
    pub utilization: Option<f64>,
    pub resets_at: Option<String>,
}
#[derive(Clone, Debug)]
pub struct SubscriptionUsage {
    pub subscription_id: String,
    pub windows: Vec<UsageWindow>,
    /// Preserve native credit units/currency and newer entitlements unchanged.
    pub raw: Value,
}
pub async fn usage(
    access: &SubscriptionAccess,
    expected: &AccountIdentity,
) -> Result<SubscriptionUsage> {
    // `discover` already verified this token's profile.
    expected.check(&access.profile.identity)?;
    usage_from(
        fetch(Operation::Usage, None, Some(access.token.bearer())).await?,
        expected,
    )
}
fn usage_from(raw: Value, identity: &AccountIdentity) -> Result<SubscriptionUsage> {
    ensure!(raw.is_object(), "invalid Claude usage response");
    let mut windows = Vec::new();
    for name in [
        "five_hour",
        "seven_day",
        "seven_day_oauth_apps",
        "seven_day_opus",
        "seven_day_sonnet",
        "cinder_cove",
        "wattle_ember",
    ] {
        let value = &raw[name];
        if value.is_null() {
            continue;
        }
        ensure!(value.is_object(), "invalid Claude usage window");
        let utilization = if value["utilization"].is_null() {
            None
        } else {
            let percent = value["utilization"]
                .as_f64()
                .context("invalid Claude usage percentage")?;
            ensure!(
                percent.is_finite() && percent >= 0.0,
                "invalid Claude usage percentage"
            );
            Some(percent / 100.0)
        };
        windows.push(UsageWindow {
            name: name.to_owned(),
            utilization,
            resets_at: optional_string(&value["resets_at"]),
        });
    }
    Ok(SubscriptionUsage {
        subscription_id: identity.subscription_id(),
        windows,
        raw,
    })
}

fn rotated_document(mut document: Value, response: &Value, now: i64) -> Result<Value> {
    let access = secret(
        &response["access_token"],
        "Claude refresh omitted access token",
    )?;
    let seconds = response["expires_in"]
        .as_i64()
        .filter(|v| *v > 0)
        .context("invalid Claude token lifetime")?;
    let expiry = seconds
        .checked_mul(1000)
        .and_then(|v| now.checked_add(v))
        .context("invalid Claude token lifetime")?;
    let scope = response["scope"]
        .as_str()
        .context("Claude refresh omitted scopes")?;
    let scopes: Vec<_> = scope.split_whitespace().collect();
    ensure!(
        scopes.contains(&"user:inference"),
        "Claude refreshed token lacks inference scope; saved credentials kept"
    );
    let value = document
        .get_mut("claudeAiOauth")
        .and_then(Value::as_object_mut)
        .context("Claude credentials malformed")?;
    value.insert("accessToken".into(), json!(access));
    value.insert("expiresAt".into(), json!(expiry));
    value.insert("scopes".into(), json!(scopes));
    if !response["refresh_token"].is_null() {
        value.insert(
            "refreshToken".into(),
            json!(secret(
                &response["refresh_token"],
                "invalid Claude refresh token"
            )?),
        );
    }
    if !response["refresh_token_expires_in"].is_null() {
        let seconds = response["refresh_token_expires_in"]
            .as_i64()
            .filter(|v| *v > 0)
            .context("invalid Claude refresh lifetime")?;
        let expiry = seconds
            .checked_mul(1000)
            .and_then(|v| now.checked_add(v))
            .context("invalid Claude refresh lifetime")?;
        value.insert("refreshTokenExpiresAt".into(), json!(expiry));
    }
    Ok(document)
}

fn compare_and_swap(mut latest: Value, posted: &str, candidate: &Value) -> Result<Value> {
    if latest["claudeAiOauth"]["refreshToken"].as_str() != Some(posted) {
        return Ok(latest);
    }
    copy_token_fields(&mut latest, candidate)?;
    Ok(latest)
}
/// Account/org fast eligibility is separate from a model's fast capability.
/// A missing/network result is an error, never an implicit standard-speed retry.
pub async fn fast_eligibility(
    access: &SubscriptionAccess,
    expected: &AccountIdentity,
) -> Result<bool> {
    expected.check(&access.profile.identity)?;
    let value = fetch(Operation::Fast, None, Some(access.token.bearer())).await?;
    value["enabled"]
        .as_bool()
        .context("Claude fast eligibility missing")
}

fn copy_token_fields(latest: &mut Value, candidate: &Value) -> Result<()> {
    let object = latest["claudeAiOauth"]
        .as_object_mut()
        .context("Claude credential disappeared")?;
    for key in [
        "accessToken",
        "refreshToken",
        "expiresAt",
        "refreshTokenExpiresAt",
        "scopes",
    ] {
        if let Some(value) = candidate["claudeAiOauth"].get(key) {
            object.insert(key.into(), value.clone());
        }
    }
    Ok(())
}
fn apply_profile(latest: &mut Value, profile: &SubscriptionProfile) -> Result<()> {
    let object = latest["claudeAiOauth"]
        .as_object_mut()
        .context("Claude credential disappeared")?;
    if let Some(value) = &profile.subscription_type {
        object.insert("subscriptionType".into(), json!(value));
    }
    if let Some(value) = &profile.rate_limit_tier {
        object.insert("rateLimitTier".into(), json!(value));
    }
    latest["borgClaudeSubscription"] = json!({"accountUuid":profile.identity.account_uuid,"organizationUuid":profile.identity.organization_uuid});
    Ok(())
}
fn check_saved_identity(document: &Value, expected: &AccountIdentity) -> Result<()> {
    if let Some(saved) = document.get("borgClaudeSubscription") {
        expected.check(&AccountIdentity {
            account_uuid: secret(&saved["accountUuid"], "invalid saved Claude identity")?,
            organization_uuid: secret(&saved["organizationUuid"], "invalid saved Claude identity")?,
        })?;
    }
    Ok(())
}
fn check_token_account(response: &Value, expected: &AccountIdentity) -> Result<()> {
    if let Some(account) = response.get("account") {
        ensure!(
            account["uuid"].as_str() == Some(expected.account_uuid.as_str()),
            "Claude token account changed; credentials kept"
        );
    }
    if let Some(org) = response.get("organization") {
        ensure!(
            org["uuid"].as_str() == Some(expected.organization_uuid.as_str()),
            "Claude token organization changed; credentials kept"
        );
    }
    Ok(())
}
fn secret(value: &Value, error: &'static str) -> Result<String> {
    value
        .as_str()
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .context(error)
}
fn optional_string(value: &Value) -> Option<String> {
    value.as_str().filter(|s| !s.is_empty()).map(str::to_owned)
}
fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[derive(Clone, Copy)]
enum Operation {
    Refresh,
    Exchange,
    Profile,
    Usage,
    Fast,
}
impl Operation {
    #[cfg(not(test))]
    fn endpoint(self) -> &'static str {
        match self {
            Self::Refresh | Self::Exchange => TOKEN_ENDPOINT,
            Self::Profile => PROFILE_ENDPOINT,
            Self::Usage => USAGE_ENDPOINT,
            Self::Fast => FAST_ENDPOINT,
        }
    }
}
#[cfg(test)]
async fn fetch(operation: Operation, body: Option<&Value>, bearer: Option<&str>) -> Result<Value> {
    tests::injected_fetch(operation, body, bearer)
        .await
        .unwrap_or_else(|| Err(anyhow::anyhow!("offline auth test context missing")))
}

#[cfg(not(test))]
async fn fetch(operation: Operation, body: Option<&Value>, bearer: Option<&str>) -> Result<Value> {
    let client = reqwest::Client::builder()
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(10))
        // Claude throttles the usage endpoint per client; Claude Code's own
        // identity is the bucket that answers reliably.
        .user_agent("claude-code/2.1.296")
        .build()
        .map_err(|_| anyhow::anyhow!("cannot initialize Claude TLS client"))?;
    let mut request = match body {
        Some(value) => client.post(operation.endpoint()).json(value),
        None => client.get(operation.endpoint()),
    };
    request = request
        .header("Content-Type", "application/json")
        .header("Cache-Control", "no-cache");
    if let Some(token) = bearer {
        request = request.bearer_auth(token);
    }
    if matches!(operation, Operation::Usage | Operation::Fast) {
        request = request.header("anthropic-beta", "oauth-2025-04-20");
    }
    let response = request.send().await.map_err(|_| {
        anyhow::anyhow!("Claude subscription connection failed; saved credentials kept")
    })?;
    let status = response.status();
    if !status.is_success() {
        if matches!(status.as_u16(), 400 | 401 | 403) {
            bail!(
                "Claude subscription rejected (HTTP {}); reconnect or resolve account restrictions. No API-key fallback",
                status.as_u16()
            );
        }
        bail!(
            "Claude subscription unavailable (HTTP {}); saved credentials kept",
            status.as_u16()
        );
    }
    ensure!(
        response
            .content_length()
            .is_none_or(|size| size <= MAX_BYTES as u64),
        "Claude response too large"
    );
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| anyhow::anyhow!("Claude response read failed"))?;
        ensure!(
            bytes.len().saturating_add(chunk.len()) <= MAX_BYTES,
            "Claude response too large"
        );
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes)
        .map_err(|_| anyhow::anyhow!("malformed Claude subscription response"))
}
