//! Native ChatGPT subscription credentials. No provider executable is involved.
//! OAuth wire compatibility is based on OpenAI Codex (Apache-2.0),
//! https://github.com/openai/codex/tree/main/codex-rs/login.

use anyhow::{Context, Result, bail, ensure};
use base64::Engine;
use chrono::Utc;
use futures::StreamExt;
use serde_json::{Value, json};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    time::Duration,
};

const CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const TOKEN_ENDPOINT: &str = "https://auth.openai.com/oauth/token";
const MAX_CREDENTIAL_BYTES: u64 = 256 * 1024;

/// Deliberately not Debug or Serialize: access credentials never enter journals.
pub struct SubscriptionAccess {
    pub(crate) token: String,
    pub(crate) account_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionAccount {
    pub account_id: String,
    pub plan: Option<String>,
}

pub fn auth_path() -> Result<PathBuf> {
    if let Some(path) = crate::credentials::openai_subscription_auth_file()? {
        return Ok(path);
    }
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .context("cannot locate ChatGPT subscription credentials")?;
    let borg_home = crate::env::nonempty_var("BORG_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".borg"));
    let native = borg_home.join("openai-subscription.json");
    if native.exists() {
        return Ok(native);
    }
    let legacy = crate::env::nonempty_var("CODEX_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".codex"))
        .join("auth.json");
    if legacy.exists()
        && load(&legacy)?
            .get("tokens")
            .is_some_and(|tokens| tokens.is_object())
    {
        return Ok(legacy);
    }
    Ok(native)
}

/// Select an existing authority, never copy its rotating refresh token.
pub fn select_auth_file(path: &Path) -> Result<()> {
    let path = path
        .canonicalize()
        .context("cannot locate saved ChatGPT login")?;
    if let Some(overridden) = crate::env::nonempty_var("BORG_OPENAI_AUTH_FILE") {
        ensure!(
            PathBuf::from(overridden).canonicalize()? == path,
            "BORG_OPENAI_AUTH_FILE overrides the requested authority; unset it before selecting another file"
        );
    }
    account_from_document(&load(&path)?)?;
    crate::credentials::set_openai_subscription_auth_file(&path)?;
    Ok(())
}

pub fn account() -> Result<Option<SubscriptionAccount>> {
    let path = auth_path()?;
    if !path.exists() {
        return Ok(None);
    }
    let document = load(&path)?;
    if document["auth_mode"] == "apikey" || document["tokens"].is_null() {
        return Ok(None);
    }
    Ok(Some(account_from_document(&document)?))
}

/// `rejected_token` distinguishes a forced 401 recovery from an ordinary read.
/// Another Borg process may already have rotated the rejected access token.
pub async fn access(rejected_token: Option<String>) -> Result<SubscriptionAccess> {
    let path = auth_path()?;
    // A caller interrupt must not cancel the refresh between token rotation and persistence.
    tokio::spawn(async move { access_at(path, rejected_token, TOKEN_ENDPOINT).await })
        .await
        .context("ChatGPT credential task failed")?
}

async fn access_at(
    path: PathBuf,
    rejected_token: Option<String>,
    endpoint: &str,
) -> Result<SubscriptionAccess> {
    let path = path
        .canonicalize()
        .context("cannot locate saved ChatGPT subscription; reconnect with borg login codex")?;
    let _lock = credential_lock(&path).await?;
    let mut document = load(&path)?;
    let account = account_from_document(&document)?;
    let token = document["tokens"]["access_token"]
        .as_str()
        .filter(|token| !token.is_empty())
        .context("saved ChatGPT access token is missing")?
        .to_owned();
    let claims = jwt_claims(&token)?;
    let expired = claims["exp"]
        .as_i64()
        .is_none_or(|expiry| expiry <= Utc::now().timestamp() + 60);
    let rejected = rejected_token.as_deref() == Some(token.as_str());
    if expired || rejected {
        let refresh = document["tokens"]["refresh_token"]
            .as_str()
            .filter(|token| !token.is_empty())
            .context("ChatGPT session cannot refresh; reconnect the subscription")?;
        let response = client()?
            .post(endpoint)
            .json(&json!({
                "client_id": CLIENT_ID, "grant_type": "refresh_token", "refresh_token": refresh,
            }))
            .send()
            .await
            .context("ChatGPT token refresh connection failed")?;
        let status = response.status();
        if !status.is_success() {
            if status == reqwest::StatusCode::UNAUTHORIZED
                || status == reqwest::StatusCode::BAD_REQUEST
            {
                bail!(
                    "ChatGPT subscription refresh rejected; reconnect the subscription. No API billing fallback was attempted."
                );
            }
            bail!(
                "ChatGPT subscription refresh unavailable (HTTP {status}); saved credentials were kept"
            );
        }
        let refreshed = response_json(response).await?;
        ensure!(
            refreshed["access_token"]
                .as_str()
                .is_some_and(|token| !token.is_empty()),
            "ChatGPT refresh omitted the access token"
        );
        for field in ["access_token", "refresh_token", "id_token"] {
            if !refreshed[field].is_null() {
                let value = refreshed[field]
                    .as_str()
                    .filter(|value| !value.is_empty())
                    .context("ChatGPT refresh returned an invalid token")?;
                document["tokens"][field] = json!(value);
            }
        }
        let next_account = account_from_document(&document)?;
        ensure!(
            next_account.account_id == account.account_id,
            "ChatGPT account changed during refresh; credentials were not replaced"
        );
        document["last_refresh"] = json!(Utc::now().to_rfc3339());
        save(&path, &document)?;
    }
    Ok(SubscriptionAccess {
        token: document["tokens"]["access_token"]
            .as_str()
            .context("ChatGPT access token missing")?
            .to_owned(),
        account_id: account.account_id,
    })
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
            .context("open ChatGPT credential lock")?;
        file.lock().context("lock ChatGPT credentials")?;
        Ok(file)
    })
    .await
    .context("ChatGPT credential lock task failed")?
}

pub(crate) fn account_from_document(document: &Value) -> Result<SubscriptionAccount> {
    ensure!(
        document["auth_mode"] != "apikey",
        "saved credentials use API billing, not ChatGPT"
    );
    let tokens = &document["tokens"];
    let access_claims = jwt_claims(
        tokens["access_token"]
            .as_str()
            .context("ChatGPT access token missing")?,
    )?;
    let claims = access_claims
        .get("https://api.openai.com/auth")
        .context("ChatGPT access token omitted account claims")?;
    let account_id = claims["chatgpt_account_id"]
        .as_str()
        .filter(|id| !id.is_empty())
        .context("ChatGPT access token omitted account identity")?;
    if let Some(stored) = tokens["account_id"].as_str().filter(|id| !id.is_empty()) {
        ensure!(
            stored == account_id,
            "saved ChatGPT account does not match its access token"
        );
    }
    Ok(SubscriptionAccount {
        account_id: account_id.to_owned(),
        plan: claims["chatgpt_plan_type"].as_str().map(str::to_owned),
    })
}

fn jwt_claims(token: &str) -> Result<Value> {
    ensure!(
        token.len() <= MAX_CREDENTIAL_BYTES as usize,
        "ChatGPT token exceeds size limit"
    );
    let payload = token
        .split(".")
        .nth(1)
        .context("invalid saved ChatGPT token")?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .context("invalid saved ChatGPT token encoding")?;
    serde_json::from_slice(&bytes).context("invalid saved ChatGPT token claims")
}

fn load(path: &Path) -> Result<Value> {
    let file = fs::File::open(path).context("read saved ChatGPT subscription")?;
    let bytes = crate::bounded_io::read_open_file_bytes_with_limit(
        path,
        "ChatGPT credentials",
        file,
        MAX_CREDENTIAL_BYTES,
    )?;
    serde_json::from_slice(&bytes).context("invalid saved ChatGPT credentials")
}

fn save(path: &Path, document: &Value) -> Result<()> {
    let parent = path
        .parent()
        .context("ChatGPT credential path has no parent")?;
    private_directory(parent)?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)
        .context("create private ChatGPT credential file")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        temp.as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    let bytes = serde_json::to_vec_pretty(document).context("encode ChatGPT credentials")?;
    temp.write_all(&bytes)
        .context("write ChatGPT credentials")?;
    temp.as_file()
        .sync_all()
        .context("sync ChatGPT credentials")?;
    temp.persist(path)
        .map_err(|error| error.error)
        .context("replace ChatGPT credentials")?;
    #[cfg(unix)]
    fs::File::open(parent)?.sync_all()?;
    Ok(())
}

fn client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("create ChatGPT authentication client")
}

async fn response_json(response: reqwest::Response) -> Result<Value> {
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("ChatGPT authentication response disconnected")?;
        ensure!(
            bytes.len().saturating_add(chunk.len()) <= MAX_CREDENTIAL_BYTES as usize,
            "ChatGPT authentication response exceeds size limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).context("invalid ChatGPT authentication response")
}

/// Account usage metadata is subscription-only and never starts a model turn.
pub async fn usage() -> Result<Value> {
    let mut credentials = access(None).await?;
    let account_id = credentials.account_id.clone();
    let client = client()?;
    for attempt in 0..2 {
        let response = client
            .get("https://chatgpt.com/backend-api/wham/usage")
            .bearer_auth(&credentials.token)
            .header("ChatGPT-Account-Id", &credentials.account_id)
            .header("originator", "borg")
            .send()
            .await
            .context("ChatGPT usage connection failed")?;
        if attempt == 0 && response.status() == reqwest::StatusCode::UNAUTHORIZED {
            drop(response);
            credentials = access(Some(credentials.token)).await?;
            ensure!(
                credentials.account_id == account_id,
                "ChatGPT account changed during usage recovery; retry for the selected account"
            );
            continue;
        }
        ensure!(
            response.status().is_success(),
            "ChatGPT usage unavailable (HTTP {})",
            response.status()
        );
        return response_json(response).await;
    }
    unreachable!("the second usage attempt always returns")
}

/// Device approval details are displayed only by the login UI, never journaled as credentials.
pub struct DeviceLogin {
    pub user_code: String,
    pub verification_url: String,
    device_auth_id: String,
    interval: Duration,
    deadline: tokio::time::Instant,
    path: PathBuf,
}

pub async fn begin_device_login() -> Result<DeviceLogin> {
    let path = auth_path()?;
    let path = if path.exists() {
        path.canonicalize()?
    } else {
        path
    };
    let response = client()?
        .post("https://auth.openai.com/api/accounts/deviceauth/usercode")
        .json(&json!({"client_id": CLIENT_ID}))
        .send()
        .await
        .context("ChatGPT device login connection failed")?;
    ensure!(
        response.status().is_success(),
        "ChatGPT device login unavailable (HTTP {})",
        response.status()
    );
    let body = response_json(response).await?;
    let code = body["user_code"]
        .as_str()
        .or_else(|| body["usercode"].as_str())
        .context("ChatGPT device login omitted the approval code")?;
    ensure!(
        !code.is_empty()
            && code.len() <= 128
            && code
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b"-"[0]),
        "ChatGPT device login returned an invalid approval code"
    );
    let device_auth_id = body["device_auth_id"]
        .as_str()
        .filter(|id| !id.is_empty())
        .context("ChatGPT device login omitted its identifier")?
        .to_owned();
    let interval = body["interval"]
        .as_u64()
        .or_else(|| {
            body["interval"]
                .as_str()
                .and_then(|value| value.parse().ok())
        })
        .unwrap_or(5)
        .clamp(1, 60);
    Ok(DeviceLogin {
        user_code: code.to_owned(),
        verification_url: "https://auth.openai.com/codex/device".into(),
        device_auth_id,
        interval: Duration::from_secs(interval),
        deadline: tokio::time::Instant::now() + Duration::from_secs(15 * 60),
        path,
    })
}

pub async fn complete_device_login(login: DeviceLogin) -> Result<SubscriptionAccount> {
    let client = client()?;
    let code = tokio::time::timeout_at(login.deadline, async {
        loop {
            let response = client
                .post("https://auth.openai.com/api/accounts/deviceauth/token")
                .json(
                    &json!({"device_auth_id": login.device_auth_id, "user_code": login.user_code}),
                )
                .send()
                .await
                .context("ChatGPT device approval check failed")?;
            let status = response.status();
            if status.is_success() {
                return response_json(response).await;
            }
            ensure!(
                status == reqwest::StatusCode::FORBIDDEN
                    || status == reqwest::StatusCode::NOT_FOUND,
                "ChatGPT device approval failed (HTTP {status})"
            );
            tokio::time::sleep(login.interval).await;
        }
    })
    .await
    .context("ChatGPT device approval expired; start login again")??;
    // Once the authorization code is exchanged, finish persistence even if the UI closes.
    tokio::spawn(async move {
        let authorization_code = code["authorization_code"].as_str().filter(|s| !s.is_empty())
            .context("ChatGPT device approval omitted its authorization code")?;
        let verifier = code["code_verifier"].as_str().filter(|s| !s.is_empty())
            .context("ChatGPT device approval omitted its PKCE verifier")?;
        let response = client.post(TOKEN_ENDPOINT).form(&[
            ("grant_type", "authorization_code"), ("client_id", CLIENT_ID),
            ("code", authorization_code), ("code_verifier", verifier),
            ("redirect_uri", "https://auth.openai.com/deviceauth/callback"),
        ]).send().await.context("ChatGPT authorization exchange failed")?;
        ensure!(response.status().is_success(), "ChatGPT authorization exchange rejected (HTTP {})", response.status());
        let tokens = response_json(response).await?;
        for field in ["access_token", "refresh_token", "id_token"] {
            ensure!(tokens[field].as_str().is_some_and(|token| !token.is_empty()), "ChatGPT login omitted a required token");
        }
        let mut document = json!({"auth_mode": "chatgpt", "tokens": {
            "access_token": tokens["access_token"], "refresh_token": tokens["refresh_token"], "id_token": tokens["id_token"]
        }, "last_refresh": Utc::now().to_rfc3339()});
        let account = account_from_document(&document)?;
        document["tokens"]["account_id"] = json!(account.account_id);
        let current = auth_path()?;
        let current = if current.exists() { current.canonicalize()? } else { current };
        ensure!(current == login.path, "ChatGPT credential selection changed during login; saved credentials were kept");
        private_directory(login.path.parent().context("ChatGPT credential path has no parent")?)?;
        let _lock = credential_lock(&login.path).await?;
        save(&login.path, &document)?;
        Ok(account)
    }).await.context("ChatGPT login persistence task failed")?
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
        .context("create private ChatGPT credential directory")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn token(account: &str, expiry: i64) -> String {
        let claims = json!({"exp": expiry, "https://api.openai.com/auth": {
            "chatgpt_account_id": account, "chatgpt_plan_type": "pro"
        }});
        format!(
            "test.{}.test",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(claims.to_string())
        )
    }

    fn document(access: &str) -> Value {
        json!({"auth_mode": "chatgpt", "tokens": {
            "account_id": "account-a", "access_token": access,
            "refresh_token": "old-refresh", "id_token": "retained-id"
        }, "extension": "preserve-me"})
    }

    async fn response_server(status: u16, body: Value) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/token", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut headers = Vec::new();
            while !headers.ends_with(b"\r\n\r\n") {
                headers.push(socket.read_u8().await.unwrap());
                assert!(headers.len() < 8192);
            }
            let headers = String::from_utf8(headers).unwrap();
            let length: usize = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length: ")
                        .map(|v| v.parse().unwrap())
                })
                .unwrap();
            let mut request = vec![0; length];
            socket.read_exact(&mut request).await.unwrap();
            let request: Value = serde_json::from_slice(&request).unwrap();
            assert_eq!(request["grant_type"], "refresh_token");
            assert_eq!(request["client_id"], CLIENT_ID);
            assert_eq!(request["refresh_token"], "old-refresh");
            let body = body.to_string();
            let response = format!(
                "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        (endpoint, task)
    }

    #[tokio::test]
    async fn concurrent_recovery_reloads_and_persists_one_rotation() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("auth.json");
        let old = token("account-a", Utc::now().timestamp() + 3600);
        let next = token("account-a", Utc::now().timestamp() + 7200);
        save(&path, &document(&old)).unwrap();
        #[cfg(not(unix))]
        let second_path = path.clone();
        #[cfg(unix)]
        let second_path = {
            let alias = directory.path().join("auth-alias.json");
            std::os::unix::fs::symlink(&path, &alias).unwrap();
            alias
        };
        let (endpoint, server) = response_server(
            200,
            json!({
                "access_token": next, "refresh_token": "rotated-refresh"
            }),
        )
        .await;
        let (first, second) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(
                access_at(path.clone(), Some(old.clone()), &endpoint),
                access_at(second_path.clone(), Some(old.clone()), &endpoint)
            )
        })
        .await
        .unwrap();
        for access in [first.unwrap(), second.unwrap()] {
            assert_eq!(access.token, next);
            assert_eq!(access.account_id, "account-a");
        }
        server.await.unwrap();
        let saved = load(&path).unwrap();
        assert_eq!(saved["tokens"]["refresh_token"], "rotated-refresh");
        assert_eq!(saved["tokens"]["id_token"], "retained-id");
        assert_eq!(saved["extension"], "preserve-me");
        assert!(saved["last_refresh"].is_string());
        assert_eq!(
            account_from_document(&saved).unwrap().plan.as_deref(),
            Some("pro")
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[tokio::test]
    async fn rejected_or_foreign_refresh_preserves_saved_credentials() {
        for (status, body) in [
            (401, json!({"error": "private-server-detail"})),
            (
                200,
                json!({"access_token": token("account-b", Utc::now().timestamp() + 3600), "refresh_token": "foreign-refresh"}),
            ),
            (200, json!({"refresh_token": "missing-access"})),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("auth.json");
            save(&path, &document(&token("account-a", 1))).unwrap();
            let before = fs::read(&path).unwrap();
            let (endpoint, server) = response_server(status, body).await;
            let error = access_at(path.clone(), None, &endpoint)
                .await
                .err()
                .unwrap();
            assert!(!format!("{error:#}").contains("private-server-detail"));
            assert_eq!(fs::read(path).unwrap(), before);
            server.await.unwrap();
        }
    }
}
