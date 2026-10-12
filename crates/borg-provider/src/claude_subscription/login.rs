use super::*;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};

const AUTHORIZE: &str = "https://claude.com/cai/oauth/authorize";
const MANUAL_REDIRECT: &str = "https://platform.claude.com/oauth/code/callback";

/// Manual browser PKCE substrate; UI and callback collection belong to the parent.
/// Consume once, within ten minutes. No Debug implementation for state/verifier.
pub struct PendingLogin {
    verifier: String,
    state: String,
    created: std::time::Instant,
    redirect_uri: String,
}
impl PendingLogin {
    pub fn new() -> Result<Self> {
        Self::with_redirect(MANUAL_REDIRECT.to_owned())
    }
    /// Browser callback is restricted to a bound loopback port; no arbitrary redirect.
    pub fn loopback(port: u16) -> Result<Self> {
        ensure!(port != 0, "Claude callback port missing");
        Self::with_redirect(format!("http://localhost:{port}/callback"))
    }
    pub fn matches_state(&self, returned: &str) -> bool {
        constant_time_equal(self.state.as_bytes(), returned.as_bytes())
    }
    fn with_redirect(redirect_uri: String) -> Result<Self> {
        let mut verifier = [0u8; 32];
        let mut state = [0u8; 32];
        getrandom::fill(&mut verifier)
            .map_err(|_| anyhow::anyhow!("Claude PKCE entropy unavailable"))?;
        getrandom::fill(&mut state)
            .map_err(|_| anyhow::anyhow!("Claude state entropy unavailable"))?;
        Ok(Self {
            verifier: URL_SAFE_NO_PAD.encode(verifier),
            state: URL_SAFE_NO_PAD.encode(state),
            created: std::time::Instant::now(),
            redirect_uri,
        })
    }
    pub fn authorize_url(&self) -> Result<String> {
        let mut url = reqwest::Url::parse(AUTHORIZE)?;
        url.query_pairs_mut()
            .append_pair("code", "true")
            .append_pair("client_id", CLIENT_ID)
            .append_pair("response_type", "code")
            .append_pair("redirect_uri", &self.redirect_uri)
            .append_pair("scope", &SCOPES.join(" "))
            .append_pair(
                "code_challenge",
                &URL_SAFE_NO_PAD.encode(Sha256::digest(self.verifier.as_bytes())),
            )
            .append_pair("code_challenge_method", "S256")
            .append_pair("state", &self.state);
        Ok(url.into())
    }
    /// Caller supplies the returned code and state separately. Re-login may select
    /// a new identity explicitly; `expected` prevents unintended account switching.
    pub async fn complete(
        self,
        code: String,
        returned_state: String,
        authority: AuthAuthority,
        expected: Option<AccountIdentity>,
    ) -> Result<SubscriptionAccess> {
        ensure!(
            self.created.elapsed() <= Duration::from_secs(600),
            "Claude login expired; restart login"
        );
        ensure!(
            constant_time_equal(self.state.as_bytes(), returned_state.as_bytes()),
            "Claude login state mismatch"
        );
        ensure!(
            !code.is_empty() && code.len() <= 8192,
            "invalid Claude authorization code"
        );
        spawn_auth(async move {
            let lock = authority.lock().await?;
            let previous = match authority.load().await {
                Ok(document) => document,
                Err(error) if error.is::<storage::MissingAuthority>() => {
                    json!({})
                }
                Err(error) => return Err(error),
            };
            let response = fetch(
                Operation::Exchange,
                Some(&json!({
                    "grant_type":"authorization_code", "code":code,
                    "redirect_uri":self.redirect_uri, "client_id":CLIENT_ID,
                    "code_verifier":self.verifier, "state":self.state,
                })),
                None,
            )
            .await?;
            secret(
                &response["refresh_token"],
                "Claude login omitted refresh token; not persisted",
            )?;
            let mut candidate = previous.clone();
            if !candidate["claudeAiOauth"].is_object() {
                candidate["claudeAiOauth"] = json!({});
            }
            candidate["claudeAiOauth"]
                .as_object_mut()
                .unwrap()
                .remove("refreshTokenExpiresAt");
            candidate = rotated_document(candidate, &response, now_ms())?;
            let token = OAuthToken::from_document(&candidate)?;
            ensure!(
                token.refresh_token.is_some(),
                "Claude login omitted refresh token; not persisted"
            );
            let profile = SubscriptionProfile::parse(
                fetch(Operation::Profile, None, Some(token.bearer())).await?,
            )?;
            if let Some(expected) = expected {
                expected.check(&profile.identity)?;
            }
            check_token_account(&response, &profile.identity)?;
            lock.check()?;
            // Protect against non-cooperating writers during a browser login.
            let write_lock = authority.write_lock().await?;
            let latest = match authority.load().await {
                Ok(document) => document,
                Err(error) if error.is::<storage::MissingAuthority>() => {
                    json!({})
                }
                Err(error) => return Err(error),
            };
            ensure!(
                latest.get("claudeAiOauth") == previous.get("claudeAiOauth"),
                "Claude authority changed during login; restart login"
            );
            candidate = merge_login(latest, candidate, &profile)?;
            write_lock.check()?;
            authority.save(&candidate).await?;
            Ok(SubscriptionAccess {
                token: OAuthToken::from_document(&candidate)?,
                profile,
            })
        })
        .await
        .map_err(|_| anyhow::anyhow!("Claude login task failed"))?
    }
}
fn merge_login(
    mut latest: Value,
    candidate: Value,
    profile: &SubscriptionProfile,
) -> Result<Value> {
    latest["claudeAiOauth"] = candidate["claudeAiOauth"].clone();
    latest["claudeAiOauth"]["clientId"] = json!(CLIENT_ID);
    latest["claudeAiOauth"]["subscriptionType"] = json!(profile.subscription_type);
    latest["claudeAiOauth"]["rateLimitTier"] = json!(profile.rate_limit_tier);
    latest["borgClaudeSubscription"] = json!({"accountUuid":profile.identity.account_uuid,"organizationUuid":profile.identity.organization_uuid});
    Ok(latest)
}
fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0u8, |diff, (a, b)| diff | (a ^ b))
        == 0
}
