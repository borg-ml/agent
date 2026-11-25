//! Native Gemini subscription credentials. Since 2026-06-18 Google serves
//! personal Gemini subscriptions (free, Google AI Pro, Ultra) only to its
//! Antigravity clients, so Borg signs in and calls Cloud Code as Antigravity.
//! Google's Antigravity terms forbid third-party clients and Google suspends
//! accounts it detects; the login warns before anything is sent. No provider
//! executable is involved. Wire compatibility follows opencode-antigravity-auth
//! (MIT), https://github.com/NoeFabris/opencode-antigravity-auth.

use anyhow::{Context, Result, bail, ensure};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::Utc;
use futures::StreamExt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::Write,
    net::Ipv4Addr,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{Instant, timeout, timeout_at},
};

// Antigravity's installed-application OAuth client. Installed-app secrets are
// not confidential; the PKCE verifier protects the exchange.
const CLIENT_ID: &str = "1071006060591-tmhssin2h21lcre235vtolojh4g403ep.apps.googleusercontent.com";
const CLIENT_SECRET: &str = "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";
const AUTHORIZE: &str = "https://accounts.google.com/o/oauth2/v2/auth";
const TOKEN_ENDPOINT: &str = "https://oauth2.googleapis.com/token";
/// The only redirect registered for the Antigravity client.
const REDIRECT_PORT: u16 = 51121;
const REDIRECT_PATH: &str = "/oauth-callback";
const SCOPES: [&str; 5] = [
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/cclog",
    "https://www.googleapis.com/auth/experimentsandconfigs",
];
/// Antigravity talks only to the daily host; mixing hosts can grant
/// incompatible entitlements.
pub(crate) const CODE_ASSIST_ENDPOINT: &str =
    "https://daily-cloudcode-pa.googleapis.com/v1internal";
/// Project Antigravity falls back to when Cloud Code assigns none.
const DEFAULT_PROJECT: &str = "rising-fact-p41fc";
/// Cloud Code serves current models only to Hub releases at or above this.
const MINIMUM_HUB_VERSION: (u32, u32, u32) = (2, 23, 0);
const FALLBACK_HUB_VERSION: &str = "2.24.0";
const HUB_MANIFEST: &str = "https://antigravity-hub-auto-updater-974169037036.us-central1.run.app/manifest/latest-arm64-mac.yml";
/// Shown before sign-in: the account risk is the user's to accept.
pub const TERMS_WARNING: &str = "Gemini subscriptions reach Borg through Google's Antigravity sign-in. Google's Antigravity terms forbid third-party clients, and Google has suspended accounts it caught using them.";
const MAX_CREDENTIAL_BYTES: u64 = 256 * 1024;
/// The model catalog alone runs to hundreds of kilobytes.
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const LOGIN_TIMEOUT: Duration = Duration::from_secs(600);
const MAX_HEADER_BYTES: usize = 8 * 1024;

/// Deliberately not Debug or Serialize: access credentials never enter journals.
pub struct SubscriptionAccess {
    pub(crate) token: String,
    pub(crate) project: String,
    pub(crate) account: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionAccount {
    /// The Google account email, which is also the billing identity Borg pins
    /// a session's replay state to.
    pub email: String,
    /// The Code Assist tier name, for example the Google AI Pro entitlement.
    pub tier: Option<String>,
}

pub fn auth_path() -> Result<PathBuf> {
    if let Some(path) = crate::credentials::gemini_subscription_auth_file()? {
        return Ok(path);
    }
    Ok(crate::credentials::borg_dir()
        .context("cannot locate Gemini subscription credentials")?
        .join("gemini-subscription.json"))
}

/// Select an existing authority, never copy its rotating refresh token.
pub fn select_auth_file(path: &Path) -> Result<()> {
    let path = path
        .canonicalize()
        .context("cannot locate saved Gemini login")?;
    if let Some(overridden) = crate::credentials::gemini_auth_file_env_override() {
        ensure!(
            overridden.canonicalize()? == path,
            "BORG_GEMINI_AUTH_FILE overrides the requested authority; unset it before selecting another file"
        );
    }
    account_from_document(&load(&path)?)?;
    crate::credentials::set_gemini_subscription_auth_file(&path)?;
    Ok(())
}

/// The selected login, read without network access.
pub fn account() -> Result<Option<SubscriptionAccount>> {
    account_at(&auth_path()?)
}

pub fn account_at(path: &Path) -> Result<Option<SubscriptionAccount>> {
    if !path.exists() {
        return Ok(None);
    }
    Ok(Some(account_from_document(&load(path)?)?))
}

/// `rejected_token` distinguishes a forced 401 recovery from an ordinary read.
/// Another Borg process may already have rotated the rejected access token.
pub async fn access_from(
    auth_file: Option<PathBuf>,
    rejected_token: Option<String>,
) -> Result<SubscriptionAccess> {
    let path = match auth_file {
        Some(path) => path,
        None => auth_path()?,
    };
    // A caller interrupt must not cancel the refresh between token rotation and persistence.
    tokio::spawn(async move { access_at(path, rejected_token, TOKEN_ENDPOINT).await })
        .await
        .context("Gemini credential task failed")?
}

async fn access_at(
    path: PathBuf,
    rejected_token: Option<String>,
    endpoint: &str,
) -> Result<SubscriptionAccess> {
    let path = path
        .canonicalize()
        .context("cannot locate saved Gemini subscription; reconnect with borg login gemini")?;
    let _lock = credential_lock(&path).await?;
    let mut document = load(&path)?;
    let account = account_from_document(&document)?;
    let token = document["tokens"]["access_token"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    let expired = document["tokens"]["expires_at"]
        .as_i64()
        .is_none_or(|expiry| expiry <= Utc::now().timestamp() + 60);
    let rejected = rejected_token.as_deref() == Some(token.as_str());
    if token.is_empty() || expired || rejected {
        let refresh = document["tokens"]["refresh_token"]
            .as_str()
            .filter(|token| !token.is_empty())
            .context("Gemini session cannot refresh; reconnect with borg login gemini")?;
        let response = client()?
            .post(endpoint)
            .form(&[
                ("client_id", CLIENT_ID),
                ("client_secret", CLIENT_SECRET),
                ("grant_type", "refresh_token"),
                ("refresh_token", refresh),
            ])
            .send()
            .await
            .context("Gemini token refresh connection failed")?;
        let status = response.status();
        if !status.is_success() {
            if status == reqwest::StatusCode::UNAUTHORIZED
                || status == reqwest::StatusCode::BAD_REQUEST
            {
                bail!(
                    "Gemini subscription refresh rejected; reconnect with borg login gemini. No API billing fallback was attempted."
                );
            }
            bail!(
                "Gemini subscription refresh unavailable (HTTP {status}); saved credentials were kept"
            );
        }
        let refreshed = response_json(response).await?;
        merge_tokens(&mut document, &refreshed)?;
        let next = account_from_document(&document)?;
        ensure!(
            next.email == account.email,
            "Gemini account changed during refresh; credentials were not replaced"
        );
        save(&path, &document)?;
    }
    let token = document["tokens"]["access_token"]
        .as_str()
        .context("Gemini access token missing")?
        .to_owned();
    let project = match document["project"].as_str().filter(|id| !id.is_empty()) {
        Some(project) => project.to_owned(),
        None => {
            // A login saved before onboarding finished resumes it here.
            let setup = setup_project(&client()?, &token).await?;
            document["project"] = json!(setup.project);
            document["tier"] = json!(setup.tier_name);
            save(&path, &document)?;
            setup.project
        }
    };
    Ok(SubscriptionAccess {
        token,
        project,
        account: account.email,
    })
}

fn merge_tokens(document: &mut Value, tokens: &Value) -> Result<()> {
    let access = tokens["access_token"]
        .as_str()
        .filter(|token| !token.is_empty())
        .context("Gemini token response omitted the access token")?;
    document["tokens"]["access_token"] = json!(access);
    // Google rotates neither the refresh token nor always the id token.
    for field in ["refresh_token", "id_token"] {
        if let Some(value) = tokens[field].as_str().filter(|value| !value.is_empty()) {
            document["tokens"][field] = json!(value);
        }
    }
    let lifetime = tokens["expires_in"].as_i64().unwrap_or(3600);
    document["tokens"]["expires_at"] = json!(Utc::now().timestamp() + lifetime);
    if let Some(id_token) = document["tokens"]["id_token"].as_str()
        && let Some(email) = jwt_claims(id_token)?["email"].as_str()
    {
        document["email"] = json!(email);
    }
    document["last_refresh"] = json!(Utc::now().to_rfc3339());
    Ok(())
}

pub(crate) fn account_from_document(document: &Value) -> Result<SubscriptionAccount> {
    let email = document["email"]
        .as_str()
        .filter(|email| !email.is_empty())
        .context("saved Gemini login omitted its Google account")?;
    ensure!(
        document["tokens"]["refresh_token"]
            .as_str()
            .is_some_and(|token| !token.is_empty()),
        "saved Gemini login has no refresh token; reconnect with borg login gemini"
    );
    Ok(SubscriptionAccount {
        email: email.to_owned(),
        tier: document["tier"].as_str().map(str::to_owned),
    })
}

fn jwt_claims(token: &str) -> Result<Value> {
    ensure!(
        token.len() <= MAX_CREDENTIAL_BYTES as usize,
        "Gemini token exceeds size limit"
    );
    let payload = token.split('.').nth(1).context("invalid Gemini id token")?;
    let bytes = URL_SAFE_NO_PAD
        .decode(payload.trim_end_matches('='))
        .context("invalid Gemini id token encoding")?;
    serde_json::from_slice(&bytes).context("invalid Gemini id token claims")
}

struct ProjectSetup {
    project: String,
    tier_name: Option<String>,
}

/// Resolve the Cloud Code project the subscription bills, onboarding the
/// account on first use the way the Antigravity Hub does.
async fn setup_project(client: &reqwest::Client, token: &str) -> Result<ProjectSetup> {
    let load = code_assist_json(
        client,
        token,
        "loadCodeAssist",
        &json!({"metadata": {"ideType": "ANTIGRAVITY"}}),
    )
    .await?;
    let tier_name = load["paidTier"]["name"]
        .as_str()
        .or(load["currentTier"]["name"].as_str())
        .map(str::to_owned);
    if let Some(project) = project_id(&load["cloudaicompanionProject"]) {
        return Ok(ProjectSetup { project, tier_name });
    }
    if let Some(validation) = load["ineligibleTiers"].as_array().and_then(|tiers| {
        tiers
            .iter()
            .find_map(|tier| tier["validationUrl"].as_str().map(str::to_owned))
    }) {
        bail!(
            "Google requires account verification before Gemini can be used: open {validation}, then retry"
        );
    }
    let tiers = load["allowedTiers"].as_array();
    let tier = tiers
        .and_then(|tiers| tiers.iter().find(|tier| tier["isDefault"] == true))
        .or_else(|| tiers.and_then(|tiers| tiers.first()))
        .cloned()
        .unwrap_or_else(|| json!({"id": "free-tier"}));
    let request = json!({
        "tier_id": tier["id"].as_str().unwrap_or("free-tier"),
        "metadata": {
            "ide_type": "ANTIGRAVITY",
            "ide_version": hub_version().await,
            "ide_name": "antigravity",
        },
    });
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let response = client
            .post(format!("{CODE_ASSIST_ENDPOINT}:onboardUser"))
            .bearer_auth(token)
            .header(
                reqwest::header::USER_AGENT,
                format!("{} google-api-nodejs-client/10.3.0", user_agent().await),
            )
            .header("X-Goog-Api-Client", "gl-node/22.21.1")
            .json(&request)
            .send()
            .await
            .context("Gemini onboarding connection failed")?;
        let operation = checked_json(response, "Gemini onboarding").await?;
        if operation["done"] == true {
            return Ok(ProjectSetup {
                project: project_id(&operation["response"]["cloudaicompanionProject"])
                    .unwrap_or_else(|| DEFAULT_PROJECT.to_owned()),
                tier_name: tier_name.or_else(|| tier["name"].as_str().map(str::to_owned)),
            });
        }
        ensure!(
            Instant::now() < deadline,
            "Gemini onboarding did not finish; retry borg login gemini"
        );
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

fn project_id(value: &Value) -> Option<String> {
    value
        .as_str()
        .or(value["id"].as_str())
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
}

/// The Antigravity Hub release Borg reports, read once per process from the
/// Hub's update manifest so newly released models are served.
async fn hub_version() -> &'static str {
    static VERSION: tokio::sync::OnceCell<String> = tokio::sync::OnceCell::const_new();
    VERSION
        .get_or_init(|| async {
            let manifest = async {
                reqwest::Client::builder()
                    .timeout(Duration::from_secs(5))
                    .build()
                    .ok()?
                    .get(HUB_MANIFEST)
                    .header(reqwest::header::USER_AGENT, "electron-builder")
                    .send()
                    .await
                    .ok()?
                    .text()
                    .await
                    .ok()
            };
            manifest
                .await
                .and_then(|text| {
                    text.lines()
                        .find_map(|line| line.strip_prefix("version:"))
                        .map(|version| version.trim().trim_matches(['"', '\'']).to_owned())
                })
                .filter(|version| {
                    let parts: Vec<u32> = version
                        .split('.')
                        .filter_map(|part| part.parse().ok())
                        .collect();
                    parts.len() == 3 && (parts[0], parts[1], parts[2]) >= MINIMUM_HUB_VERSION
                })
                .unwrap_or_else(|| FALLBACK_HUB_VERSION.to_owned())
        })
        .await
}

/// The Hub `User-Agent`; Cloud Code decides from it which models to serve.
pub(crate) async fn user_agent() -> String {
    let version = hub_version().await;
    let changelist = match version {
        "2.23.0" => "; cl=998057262",
        "2.23.1" => "; cl=998881661",
        "2.24.0" => "; cl=1000335682",
        _ => "",
    };
    format!("antigravity/hub/{version} (aidev_client; os_type=darwin; arch=arm64{changelist})")
}

async fn code_assist_json(
    client: &reqwest::Client,
    token: &str,
    method: &str,
    body: &Value,
) -> Result<Value> {
    let response = client
        .post(format!("{CODE_ASSIST_ENDPOINT}:{method}"))
        .bearer_auth(token)
        .header(reqwest::header::USER_AGENT, user_agent().await)
        .json(body)
        .send()
        .await
        .with_context(|| format!("Gemini {method} connection failed"))?;
    checked_json(response, &format!("Gemini {method}")).await
}

async fn checked_json(response: reqwest::Response, label: &str) -> Result<Value> {
    let status = response.status();
    if !status.is_success() {
        let body = crate::provider::read_provider_error_response_text(response)
            .await
            .unwrap_or_default();
        let message = serde_json::from_str::<Value>(&body)
            .ok()
            .and_then(|value| value["error"]["message"].as_str().map(str::to_owned))
            .unwrap_or_default();
        bail!("{label} failed (HTTP {status}) {message}");
    }
    response_json(response).await
}

/// One allowance window reported by a Gemini subscription.
#[derive(Clone, Debug, PartialEq)]
pub struct QuotaWindow {
    pub label: String,
    pub remaining_fraction: Option<f64>,
    pub resets_at: Option<chrono::DateTime<Utc>>,
    /// Whether exhausting this window stops the shared Gemini model pool
    /// (5-hour and weekly pool limits) rather than one separate model.
    pub global: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Quota {
    pub tier: Option<String>,
    pub windows: Vec<QuotaWindow>,
}

enum CodeAssistError {
    Auth(anyhow::Error),
    Other(anyhow::Error),
}

impl CodeAssistError {
    fn into_anyhow(self) -> anyhow::Error {
        match self {
            Self::Auth(error) | Self::Other(error) => error,
        }
    }
}

/// The wire model ids this login can call, in the order Antigravity lists them.
pub(crate) async fn available_model_ids(auth_file: Option<PathBuf>) -> Result<Vec<String>> {
    let mut access = access_from(auth_file.clone(), None).await?;
    let client = client()?;
    let response = post_project_json(
        &client,
        &mut access,
        &auth_file,
        "fetchAvailableModels",
        "Gemini model list",
    )
    .await
    .map_err(CodeAssistError::into_anyhow)?;
    Ok(listed_gemini_model_ids(&response))
}

/// The subscription's remaining allowance windows: prefers the pooled summary
/// (`retrieveUserQuotaSummary`, which exposes both 5-hour rolling and weekly
/// windows plus separate model pools such as Gemini 4 Argon) and falls back to
/// per-model `fetchAvailableModels` when the summary is unavailable or empty.
/// Never starts a model turn.
pub async fn quota(auth_file: Option<PathBuf>) -> Result<Quota> {
    let path = match &auth_file {
        Some(path) => path.clone(),
        None => auth_path()?,
    };
    let tier = account_at(&path)?.and_then(|account| account.tier);
    let mut access = access_from(auth_file.clone(), None).await?;
    let client = client()?;
    match post_project_json(
        &client,
        &mut access,
        &auth_file,
        "retrieveUserQuotaSummary",
        "Gemini quota summary",
    )
    .await
    {
        Ok(summary) => {
            let windows = quota_windows_from_summary(&summary);
            if !windows.is_empty() {
                return Ok(Quota { tier, windows });
            }
        }
        Err(CodeAssistError::Auth(error)) => return Err(error),
        Err(CodeAssistError::Other(_)) => {}
    }
    let models = post_project_json(
        &client,
        &mut access,
        &auth_file,
        "fetchAvailableModels",
        "Gemini model list",
    )
    .await
    .map_err(CodeAssistError::into_anyhow)?;
    Ok(Quota {
        tier,
        windows: quota_windows_from_models(&models),
    })
}

fn listed_gemini_model_ids(response: &Value) -> Vec<String> {
    let models = response["models"].as_object();
    // The backend also lists retired aliases and routing ids; Antigravity
    // shows only the ids in its model groups.
    let mut listed: Vec<String> = response["agentModelSorts"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|sort| sort["groups"].as_array().into_iter().flatten())
        .flat_map(|group| group["modelIds"].as_array().into_iter().flatten())
        .filter_map(|id| id.as_str().map(str::to_owned))
        .collect();
    if listed.is_empty()
        && let Some(models) = models
    {
        listed = models.keys().cloned().collect();
    }
    let mut ids = Vec::new();
    for id in listed {
        if ids.contains(&id) || !id.starts_with("gemini-") || id.contains("image") {
            continue;
        }
        ids.push(id);
    }
    ids
}

fn parse_reset_time(value: &Value) -> Option<chrono::DateTime<Utc>> {
    value
        .as_str()
        .and_then(|time| chrono::DateTime::parse_from_rfc3339(time).ok())
        .map(|time| time.with_timezone(&Utc))
}

fn quota_windows_from_summary(response: &Value) -> Vec<QuotaWindow> {
    let mut raw_buckets: Vec<(Option<&str>, &Value)> = Vec::new();
    if let Some(groups) = response["groups"].as_array() {
        for group in groups {
            let group_name = group["displayName"]
                .as_str()
                .map(str::trim)
                .filter(|name| !name.is_empty());
            if let Some(buckets) = group["buckets"].as_array() {
                for bucket in buckets {
                    raw_buckets.push((group_name, bucket));
                }
            }
        }
    }
    if raw_buckets.is_empty()
        && let Some(buckets) = response["buckets"].as_array()
    {
        for bucket in buckets {
            raw_buckets.push((None, bucket));
        }
    }

    let mut seen_ids: Vec<String> = Vec::new();
    let mut ranked: Vec<(u8, QuotaWindow)> = Vec::new();
    for (group_name, bucket) in raw_buckets {
        if bucket["disabled"].as_bool() == Some(true) {
            continue;
        }
        let bucket_id = bucket["bucketId"].as_str().unwrap_or_default().trim();
        let id = bucket_id.to_ascii_lowercase();
        if !id.starts_with("gemini-") || seen_ids.contains(&id) {
            continue;
        }
        let resets_at = parse_reset_time(&bucket["resetTime"]);
        let remaining_fraction = bucket["remainingFraction"]
            .as_f64()
            .filter(|fraction| fraction.is_finite())
            .map(|fraction| fraction.clamp(0.0, 1.0))
            .or_else(|| {
                // Proto3 JSON omits zero float fields; an enabled bucket with a
                // resetTime and no remainingAmount is exhausted (0% left).
                (bucket["remainingAmount"].is_null() && resets_at.is_some()).then_some(0.0)
            });
        let Some(remaining_fraction) = remaining_fraction else {
            continue;
        };
        seen_ids.push(id.clone());

        let window_kind = bucket["window"]
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_ascii_lowercase);
        let window_label = match window_kind.as_deref() {
            Some("5h") => Some("5-hour"),
            Some("weekly") => Some("Weekly"),
            Some("daily") => Some("Daily"),
            Some("monthly") => Some("Monthly"),
            _ if id.ends_with("-5h") => Some("5-hour"),
            _ if id.ends_with("-weekly") => Some("Weekly"),
            _ => None,
        };
        let display_name = bucket["displayName"]
            .as_str()
            .map(str::trim)
            .filter(|name| !name.is_empty());
        let shared_pool = matches!(id.as_str(), "gemini-5h" | "gemini-weekly")
            || group_name.is_some_and(|name| name.eq_ignore_ascii_case("Gemini Models"));
        let (rank, global, label) = if shared_pool {
            let rank = match window_label {
                Some("5-hour") => 0,
                Some("Weekly") => 1,
                _ => 2,
            };
            let label = window_label
                .or(display_name)
                .unwrap_or(bucket_id)
                .to_owned();
            (rank, true, label)
        } else {
            let label = match (window_label, group_name) {
                (Some(window), Some(group)) => format!("{window} · {group}"),
                _ => display_name.or(group_name).unwrap_or(bucket_id).to_owned(),
            };
            (3, false, label)
        };
        ranked.push((
            rank,
            QuotaWindow {
                label,
                remaining_fraction: Some(remaining_fraction),
                resets_at,
                global,
            },
        ));
    }
    ranked.sort_by_key(|(rank, _)| *rank);
    ranked.into_iter().map(|(_, window)| window).collect()
}

fn quota_windows_from_models(response: &Value) -> Vec<QuotaWindow> {
    let models = response["models"].as_object().cloned().unwrap_or_default();
    let mut windows: Vec<QuotaWindow> = Vec::new();
    for id in listed_gemini_model_ids(response) {
        let model = &models.get(&id).cloned().unwrap_or_default();
        let resets_at = parse_reset_time(&model["quotaInfo"]["resetTime"]);
        let remaining_fraction = model["quotaInfo"]["remainingFraction"]
            .as_f64()
            .filter(|fraction| fraction.is_finite())
            .map(|fraction| fraction.clamp(0.0, 1.0))
            .or_else(|| resets_at.is_some().then_some(0.0));
        let Some(remaining_fraction) = remaining_fraction else {
            continue;
        };
        // Every thinking tier of a model shares one allowance, so one row per
        // model ("Gemini 4 Neon (High)" and "(Low)" become "Gemini 4 Neon").
        let raw_label = model["displayName"].as_str().unwrap_or(&id);
        let label = raw_label.split(" (").next().unwrap_or(raw_label).to_owned();
        match windows.iter_mut().find(|known| known.label == label) {
            Some(known) => {
                if let Some(existing) = known.remaining_fraction {
                    known.remaining_fraction = Some(existing.min(remaining_fraction));
                } else {
                    known.remaining_fraction = Some(remaining_fraction);
                }
            }
            None => windows.push(QuotaWindow {
                label,
                remaining_fraction: Some(remaining_fraction),
                resets_at,
                global: false,
            }),
        }
    }
    windows
}

/// `auth_file` reads a named account's authority; `None` reads the selected one.
async fn post_project_json(
    client: &reqwest::Client,
    access: &mut SubscriptionAccess,
    auth_file: &Option<PathBuf>,
    method: &str,
    label: &str,
) -> std::result::Result<Value, CodeAssistError> {
    for attempt in 0..2 {
        let response = client
            .post(format!("{CODE_ASSIST_ENDPOINT}:{method}"))
            .bearer_auth(&access.token)
            .header(reqwest::header::USER_AGENT, user_agent().await)
            .json(&json!({"project": access.project}))
            .send()
            .await
            .with_context(|| format!("{label} connection failed"))
            .map_err(CodeAssistError::Other)?;
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            if attempt == 0 {
                let account = access.account.clone();
                *access = access_from(auth_file.clone(), Some(access.token.clone()))
                    .await
                    .map_err(CodeAssistError::Auth)?;
                if access.account != account {
                    return Err(CodeAssistError::Auth(anyhow::anyhow!(
                        "Gemini account changed during quota recovery; retry for the selected account"
                    )));
                }
                continue;
            }
            return checked_json(response, label)
                .await
                .map_err(CodeAssistError::Auth);
        }
        return checked_json(response, label)
            .await
            .map_err(CodeAssistError::Other);
    }
    unreachable!("the second attempt always returns")
}

/// Owns the loopback listener and the pending PKCE login. Dropping it closes
/// callback collection. No Debug: it holds the verifier and state.
pub struct BrowserLogin {
    listener: TcpListener,
    verifier: String,
    state: String,
    redirect_uri: String,
    authorize_url: String,
    path: PathBuf,
    deadline: Instant,
}

/// `path` is the authority file this login owns: the selected one for a
/// re-login, or a named account's own file.
pub async fn begin_browser_login(path: PathBuf) -> Result<BrowserLogin> {
    // The client registers exactly this redirect, so the port is fixed.
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, REDIRECT_PORT))
        .await
        .with_context(|| {
            format!("port {REDIRECT_PORT} is busy; close the other Gemini or Antigravity sign-in and retry")
        })?;
    let redirect_uri = format!("http://localhost:{REDIRECT_PORT}{REDIRECT_PATH}");
    let mut verifier = [0u8; 32];
    let mut state = [0u8; 32];
    getrandom::fill(&mut verifier).map_err(|_| anyhow::anyhow!("PKCE entropy unavailable"))?;
    getrandom::fill(&mut state).map_err(|_| anyhow::anyhow!("state entropy unavailable"))?;
    let verifier = URL_SAFE_NO_PAD.encode(verifier);
    let state = URL_SAFE_NO_PAD.encode(state);
    let mut url = reqwest::Url::parse(AUTHORIZE)?;
    url.query_pairs_mut()
        .append_pair("client_id", CLIENT_ID)
        .append_pair("redirect_uri", &redirect_uri)
        .append_pair("response_type", "code")
        .append_pair("scope", &SCOPES.join(" "))
        .append_pair("access_type", "offline")
        // Google issues a refresh token only on consent; an account that
        // already approved Gemini CLI would otherwise return none.
        .append_pair("prompt", "consent")
        .append_pair(
            "code_challenge",
            &URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes())),
        )
        .append_pair("code_challenge_method", "S256")
        .append_pair("state", &state);
    Ok(BrowserLogin {
        listener,
        verifier,
        state,
        redirect_uri,
        authorize_url: url.into(),
        path,
        deadline: Instant::now() + LOGIN_TIMEOUT,
    })
}

impl BrowserLogin {
    /// The frontend may display or open this URL; it holds no secret.
    pub fn authorize_url(&self) -> &str {
        &self.authorize_url
    }

    /// Completes from the loopback callback or from `pasted` text: the
    /// address a browser on another device landed on, which it cannot load.
    /// Text that does not belong to this login calls `rejected` and the login
    /// keeps waiting.
    pub async fn complete(
        self,
        mut pasted: tokio::sync::mpsc::UnboundedReceiver<String>,
        mut rejected: impl FnMut(),
    ) -> Result<SubscriptionAccount> {
        let code = {
            let callback = wait_for_callback(&self.listener, &self.state, self.deadline);
            tokio::pin!(callback);
            loop {
                tokio::select! {
                    result = &mut callback => break result?,
                    Some(text) = pasted.recv() => match parse_pasted(&text, &self.state) {
                        Some(Ok(code)) => break code,
                        Some(Err(())) => bail!("Google sign-in was not authorized; restart borg login gemini"),
                        None => rejected(),
                    },
                }
            }
        };
        drop(self.listener);
        let Self {
            verifier,
            redirect_uri,
            path,
            ..
        } = self;
        // Once the single-use code is exchanged, finish persistence even if the UI closes.
        tokio::spawn(async move {
            let client = client()?;
            let response = client
                .post(TOKEN_ENDPOINT)
                .form(&[
                    ("grant_type", "authorization_code"),
                    ("client_id", CLIENT_ID),
                    ("client_secret", CLIENT_SECRET),
                    ("code", code.as_str()),
                    ("code_verifier", verifier.as_str()),
                    ("redirect_uri", redirect_uri.as_str()),
                ])
                .send()
                .await
                .context("Gemini authorization exchange failed")?;
            let tokens = checked_json(response, "Gemini authorization exchange").await?;
            ensure!(
                tokens["refresh_token"].as_str().is_some_and(|t| !t.is_empty()),
                "Google returned no refresh token; remove Borg's access at myaccount.google.com/permissions and sign in again"
            );
            let mut document = json!({"tokens": {}});
            merge_tokens(&mut document, &tokens)?;
            let token = document["tokens"]["access_token"].as_str().unwrap_or_default().to_owned();
            let setup = setup_project(&client, &token).await?;
            document["project"] = json!(setup.project);
            document["tier"] = json!(setup.tier_name);
            let account = account_from_document(&document)?;
            private_directory(path.parent().context("Gemini credential path has no parent")?)?;
            let _lock = credential_lock(&path).await?;
            save(&path, &document)?;
            Ok(account)
        })
        .await
        .context("Gemini login persistence task failed")?
    }
}

async fn wait_for_callback(
    listener: &TcpListener,
    state: &str,
    deadline: Instant,
) -> Result<String> {
    timeout_at(deadline, async {
        loop {
            let (mut stream, peer) = listener.accept().await.context("accept Gemini callback")?;
            if !peer.ip().is_loopback() {
                continue;
            }
            let header = match timeout(Duration::from_secs(5), read_header(&mut stream)).await {
                Ok(Ok(header)) => header,
                _ => continue,
            };
            let outcome = parse_callback(&header, state);
            let accepted = matches!(outcome, Some(Ok(_)));
            let body = if accepted {
                "Gemini is connected. Return to Borg.\n"
            } else {
                "This sign-in link is out of date or was altered. Borg is still waiting: open the newest link it printed.\n"
            };
            let response = format!(
                "HTTP/1.1 {}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n{body}",
                if accepted { "200 OK" } else { "400 Bad Request" },
                body.len()
            );
            let _ = timeout(Duration::from_secs(1), stream.write_all(response.as_bytes())).await;
            match outcome {
                Some(Ok(code)) => return Ok(code),
                Some(Err(())) => bail!("Google sign-in was not authorized; restart borg login gemini"),
                None => continue,
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("Gemini browser login expired; restart borg login gemini"))?
}

async fn read_header(stream: &mut tokio::net::TcpStream) -> Result<String> {
    let mut header = Vec::new();
    let mut chunk = [0u8; 512];
    while !header.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
        ensure!(
            header.len() < MAX_HEADER_BYTES,
            "Gemini callback header too large"
        );
        let count = stream.read(&mut chunk).await?;
        ensure!(count != 0, "incomplete Gemini callback header");
        header.extend_from_slice(&chunk[..count]);
    }
    Ok(String::from_utf8(header)?)
}

/// `None` ignores an unrelated request, `Some(Err)` is a denial carrying our
/// state, `Some(Ok(code))` is the authorization code.
fn parse_callback(header: &str, state: &str) -> Option<std::result::Result<String, ()>> {
    let target = header
        .lines()
        .next()?
        .strip_prefix("GET ")?
        .split(' ')
        .next()?;
    parse_target(target, state)
}

/// A pasted redirect address, with or without its scheme.
fn parse_pasted(text: &str, state: &str) -> Option<std::result::Result<String, ()>> {
    let text = text.trim();
    let url = if text.contains("://") {
        reqwest::Url::parse(text)
    } else {
        reqwest::Url::parse(&format!("http://{text}"))
    }
    .ok()?;
    parse_target(&format!("{}?{}", url.path(), url.query()?), state)
}

fn parse_target(target: &str, state: &str) -> Option<std::result::Result<String, ()>> {
    if target.split('?').next() != Some(REDIRECT_PATH) {
        return None;
    }
    let url = reqwest::Url::parse(&format!("http://127.0.0.1{target}")).ok()?;
    let pairs: std::collections::BTreeMap<_, _> = url.query_pairs().collect();
    let returned = pairs.get("state")?;
    if returned.len() != state.len()
        || returned
            .bytes()
            .zip(state.bytes())
            .fold(0u8, |diff, (a, b)| diff | (a ^ b))
            != 0
    {
        return None;
    }
    if pairs.contains_key("error") {
        return Some(Err(()));
    }
    pairs
        .get("code")
        .filter(|code| !code.is_empty())
        .map(|code| Ok(code.to_string()))
}

async fn credential_lock(path: &Path) -> Result<fs::File> {
    let lock_path = path.with_extension("borg-lock");
    tokio::task::spawn_blocking(move || -> Result<fs::File> {
        let file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)
            .context("open Gemini credential lock")?;
        file.lock().context("lock Gemini credentials")?;
        Ok(file)
    })
    .await
    .context("Gemini credential lock task failed")?
}

fn load(path: &Path) -> Result<Value> {
    let file = fs::File::open(path).context("read saved Gemini subscription")?;
    let bytes = crate::bounded_io::read_open_file_bytes_with_limit(
        path,
        "Gemini credentials",
        file,
        MAX_CREDENTIAL_BYTES,
    )?;
    serde_json::from_slice(&bytes).context("invalid saved Gemini credentials")
}

fn save(path: &Path, document: &Value) -> Result<()> {
    let parent = path
        .parent()
        .context("Gemini credential path has no parent")?;
    private_directory(parent)?;
    let mut temp =
        tempfile::NamedTempFile::new_in(parent).context("create Gemini credential file")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        temp.as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    temp.write_all(&serde_json::to_vec_pretty(document)?)
        .context("write Gemini credentials")?;
    temp.as_file()
        .sync_all()
        .context("sync Gemini credentials")?;
    temp.persist(path)
        .map_err(|error| error.error)
        .context("replace Gemini credentials")?;
    Ok(())
}

fn private_directory(path: &Path) -> Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder
        .create(path)
        .context("create private Gemini credential directory")
}

pub(crate) fn client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("create Gemini authentication client")
}

async fn response_json(response: reqwest::Response) -> Result<Value> {
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("Gemini response disconnected")?;
        ensure!(
            bytes.len().saturating_add(chunk.len()) <= MAX_RESPONSE_BYTES,
            "Gemini response exceeds size limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).context("invalid Gemini response")
}

#[cfg(test)]
mod tests {
    use super::*;

    // Over SSH the browser cannot load the loopback redirect; the address it
    // shows is pasted instead, and only with this login's state.
    #[test]
    fn pasted_redirect_address_needs_this_logins_state() {
        assert_eq!(
            parse_pasted("localhost:51121/oauth-callback?state=s1&code=4%2F0Ab", "s1"),
            Some(Ok("4/0Ab".to_string()))
        );
        assert_eq!(
            parse_pasted(
                "http://localhost:51121/oauth-callback?state=s2&code=x",
                "s1"
            ),
            None
        );
        assert_eq!(parse_pasted("just-a-code", "s1"), None);
    }

    /// Summary buckets drive `/usage` and multi-account balancing: Gemini's
    /// shared 5-hour and weekly pools are global (ordered 5-hour before
    /// Weekly), separate model groups such as Gemini 4 Argon stay model-scoped,
    /// third-party pools and disabled buckets are excluded, and an exhausted
    /// bucket whose proto3 zero `remainingFraction` was omitted still parses as
    /// 0% left instead of disappearing.
    #[test]
    fn quota_summary_parses_shared_weekly_and_model_pools_and_exhausted_zeros() {
        let summary = json!({
            "groups": [
                {
                    "displayName": "Gemini Models",
                    "buckets": [
                        {
                            "bucketId": "gemini-weekly",
                            "displayName": "Weekly Limit Remaining",
                            "window": "weekly",
                            "resetTime": "2026-11-28T16:02:18Z",
                            "remainingFraction": 0.82
                        },
                        {
                            "bucketId": "gemini-5h",
                            "displayName": "Five Hour Limit Remaining",
                            "window": "5h",
                            "resetTime": "2026-11-21T21:02:18Z"
                        }
                    ]
                },
                {
                    "displayName": "Claude and GPT models",
                    "buckets": [
                        {
                            "bucketId": "3p-weekly",
                            "window": "weekly",
                            "resetTime": "2026-11-28T16:03:41Z",
                            "remainingFraction": 1.0
                        }
                    ]
                },
                {
                    "displayName": "Gemini 4 Argon",
                    "buckets": [
                        {
                            "bucketId": "gemini-argon-wtu-weekly",
                            "displayName": "Gemini 4 Argon Weekly Limit",
                            "window": "weekly",
                            "resetTime": "2026-11-28T16:03:41Z",
                            "remainingFraction": 0.5
                        },
                        {
                            "bucketId": "gemini-disabled-weekly",
                            "window": "weekly",
                            "disabled": true,
                            "remainingFraction": 1.0
                        }
                    ]
                }
            ]
        });

        let windows = quota_windows_from_summary(&summary);
        assert_eq!(windows.len(), 3);
        assert_eq!(windows[0].label, "5-hour");
        assert_eq!(windows[0].remaining_fraction, Some(0.0));
        assert!(windows[0].global);
        assert!(windows[0].resets_at.is_some());

        assert_eq!(windows[1].label, "Weekly");
        assert_eq!(windows[1].remaining_fraction, Some(0.82));
        assert!(windows[1].global);

        assert_eq!(windows[2].label, "Weekly · Gemini 4 Argon");
        assert_eq!(windows[2].remaining_fraction, Some(0.5));
        assert!(!windows[2].global);
    }

    /// Fallback per-model responses collapse thinking tiers onto one row and
    /// treat an omitted zero `remainingFraction` with a `resetTime` as 0% left.
    #[test]
    fn quota_models_fallback_collapses_tiers_and_omitted_zero_fraction() {
        let response = json!({
            "agentModelSorts": [{
                "groups": [{
                    "modelIds": [
                        "gemini-4-neon-high",
                        "gemini-4-neon-low",
                        "gemini-4-argon-high",
                        "claude-opus-5-5-high"
                    ]
                }]
            }],
            "models": {
                "gemini-4-neon-high": {
                    "displayName": "Gemini 4 Neon (High)",
                    "quotaInfo": { "remainingFraction": 0.9, "resetTime": "2026-11-21T21:02:18Z" }
                },
                "gemini-4-neon-low": {
                    "displayName": "Gemini 4 Neon (Low)",
                    "quotaInfo": { "remainingFraction": 0.75, "resetTime": "2026-11-21T21:02:18Z" }
                },
                "gemini-4-argon-high": {
                    "displayName": "Gemini 4 Argon (High)",
                    "quotaInfo": { "resetTime": "2026-11-28T16:03:05Z" }
                }
            }
        });

        let windows = quota_windows_from_models(&response);
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].label, "Gemini 4 Neon");
        assert_eq!(windows[0].remaining_fraction, Some(0.75));
        assert!(!windows[0].global);
        assert_eq!(windows[1].label, "Gemini 4 Argon");
        assert_eq!(windows[1].remaining_fraction, Some(0.0));
        assert!(!windows[1].global);
    }
}
