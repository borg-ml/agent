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
    /// The authorization code is single-use, so the exchanged grant is persisted
    /// before any profile request: a throttled or failed account check must never
    /// discard a completed login. Discovery verifies the profile on first use.
    pub async fn complete(
        self,
        code: String,
        returned_state: String,
        authority: AuthAuthority,
        expected: Option<AccountIdentity>,
    ) -> Result<()> {
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
        // Detached so a dropped caller cannot lose a grant between exchange and save.
        spawn_auth(async move {
            // Held to keep sibling refreshes off the old grant while it is replaced.
            let _refresh = authority.lock().await?;
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
            let mut identity = exchanged_identity(&response);
            if let Some(expected) = &expected {
                if identity.is_none() {
                    let token = OAuthToken::from_document(&rotated_document(
                        json!({"claudeAiOauth":{}}),
                        &response,
                        now_ms(),
                    )?)?;
                    identity = Some(fetch_profile(&token, Some(expected)).await?.identity);
                }
                expected.check(identity.as_ref().unwrap())?;
            }
            let write_lock = authority.write_lock().await?;
            let mut document = match authority.load().await {
                Ok(document) => document,
                Err(error) if error.is::<storage::MissingAuthority>() => json!({}),
                Err(error) => return Err(error),
            };
            // A new grant replaces the previous one; its plan and tier are
            // filled in by profile verification on first use.
            document["claudeAiOauth"] = json!({"clientId":CLIENT_ID});
            document = rotated_document(document, &response, now_ms())?;
            let object = document.as_object_mut().unwrap();
            match &identity {
                Some(identity) => {
                    object.insert(
                        "borgClaudeSubscription".into(),
                        json!({"accountUuid":identity.account_uuid,"organizationUuid":identity.organization_uuid}),
                    );
                }
                None => {
                    object.remove("borgClaudeSubscription");
                }
            }
            write_lock.check()?;
            authority.save(&document).await
        })
        .await
        .map_err(|_| anyhow::anyhow!("Claude login task failed"))?
    }
}
/// Token exchange names its account and organization; native Claude Code
/// relies on the same fields. Both are required to bind an identity.
fn exchanged_identity(response: &Value) -> Option<AccountIdentity> {
    Some(AccountIdentity {
        account_uuid: optional_string(&response["account"]["uuid"])?,
        organization_uuid: optional_string(&response["organization"]["uuid"])?,
    })
    .filter(|identity| {
        !identity.account_uuid.contains('\0') && !identity.organization_uuid.contains('\0')
    })
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
