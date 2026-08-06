use std::{
    collections::HashSet,
    fs,
    path::{Component, Path, PathBuf},
    process::{Output, Stdio},
    time::Duration,
};

use crate::{ProviderAuthBundle, ProviderAuthFile, ProviderAuthProvider};
use anyhow::{Context, Result, anyhow};
use base64::Engine;
use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio::process::Command;

use crate::bounded_io::read_open_file_bytes_with_limit;

pub const PROVIDER_AUTH_REFRESH_WINDOW_MINS: i64 = 15;

const CLAUDE_CREDENTIALS_REL_PATH: &str = ".claude/.credentials.json";
const CODEX_AUTH_REL_PATH: &str = ".codex/auth.json";
const PROVIDER_AUTH_FILE_MAX_BYTES: usize = 1024 * 1024;
const PROVIDER_AUTH_JWT_PAYLOAD_MAX_BYTES: usize = 64 * 1024;
const PROVIDER_AUTH_VALIDATION_TIMEOUT_ENV: &str = "BORG_PROVIDER_AUTH_VALIDATION_TIMEOUT_SECS";
const DEFAULT_PROVIDER_AUTH_VALIDATION_TIMEOUT_SECS: u64 = 30;

#[derive(Debug, Clone, Default)]
pub struct ProviderAuthValidation {
    pub ok: bool,
    pub auth_kind: String,
    pub account_email: String,
    pub account_label: String,
    pub expires_at: Option<DateTime<Utc>>,
    pub last_error: String,
}

fn required_files_for_provider(provider: ProviderAuthProvider) -> &'static [&'static str] {
    match provider {
        ProviderAuthProvider::Claude => &[CLAUDE_CREDENTIALS_REL_PATH],
        ProviderAuthProvider::Openai => &[CODEX_AUTH_REL_PATH],
    }
}

fn auth_kind_for_provider(provider: ProviderAuthProvider, auth_json: Option<&Value>) -> String {
    match provider {
        ProviderAuthProvider::Claude => "claude_code_session".to_string(),
        ProviderAuthProvider::Openai => {
            let has_tokens = auth_json
                .map(auth_json_holds_chatgpt_session)
                .unwrap_or(false);
            if has_tokens {
                "codex_chatgpt_session".to_string()
            } else {
                "openai_api_key".to_string()
            }
        }
    }
}

/// ChatGPT OAuth sessions carry single-use refresh tokens that Codex rotates
/// in place, so an auth.json holding one must only ever live in one persistent
/// CODEX_HOME. API-key auth.json files have no rotating state and may be
/// copied freely.
pub fn auth_json_holds_chatgpt_session(auth_json: &Value) -> bool {
    auth_json
        .get("tokens")
        .and_then(Value::as_object)
        .map(|tokens| !tokens.is_empty())
        .unwrap_or(false)
}

/// Whether the auth.json inside `codex_home` holds a rotating ChatGPT session,
/// failing if an existing auth file cannot be inspected safely.
pub fn codex_home_holds_chatgpt_session_checked(codex_home: &Path) -> Result<bool> {
    let path = codex_home.join("auth.json");
    let file = match fs::File::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(error).with_context(|| format!("read Codex auth JSON {}", path.display()));
        }
    };
    let contents = String::from_utf8(read_provider_auth_file_from_open(&path, file)?)
        .with_context(|| format!("read Codex auth JSON {}", path.display()))?;
    let value = serde_json::from_str::<Value>(&contents)
        .with_context(|| format!("parse Codex auth JSON {}", path.display()))?;
    Ok(auth_json_holds_chatgpt_session(&value))
}

fn encode_file_contents(contents: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(contents)
}

fn decode_file_contents(contents_b64: &str) -> Result<Vec<u8>> {
    if base64_input_exceeds_decoded_limit(contents_b64.len(), PROVIDER_AUTH_FILE_MAX_BYTES) {
        return Err(anyhow!(
            "provider auth bundle file exceeds {} byte limit",
            PROVIDER_AUTH_FILE_MAX_BYTES
        ));
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(contents_b64)
        .context("decode provider auth bundle file")?;
    if bytes.len() > PROVIDER_AUTH_FILE_MAX_BYTES {
        return Err(anyhow!(
            "provider auth bundle file exceeds {} byte limit",
            PROVIDER_AUTH_FILE_MAX_BYTES
        ));
    }
    Ok(bytes)
}

fn base64_input_exceeds_decoded_limit(encoded_len: usize, decoded_limit: usize) -> bool {
    encoded_len > decoded_limit.div_ceil(3).saturating_mul(4)
}

fn read_provider_auth_file(path: &Path) -> Result<Vec<u8>> {
    let file = fs::File::open(path)
        .with_context(|| format!("read provider auth file {}", path.display()))?;
    read_provider_auth_file_from_open(path, file)
}

fn read_provider_auth_file_from_open(path: &Path, file: fs::File) -> Result<Vec<u8>> {
    read_open_file_bytes_with_limit(
        path,
        "provider auth file",
        file,
        PROVIDER_AUTH_FILE_MAX_BYTES as u64,
    )
}

fn sanitize_relative_path(path: &str) -> Result<&str> {
    let path_buf = Path::new(path);
    if path_buf.is_absolute() {
        return Err(anyhow!("provider auth path must be relative: {path}"));
    }
    if !path_buf
        .components()
        .all(|component| matches!(component, Component::Normal(_) | Component::CurDir))
    {
        return Err(anyhow!(
            "provider auth path contains unsafe components: {path}"
        ));
    }
    Ok(path)
}

pub fn capture_bundle(
    provider: ProviderAuthProvider,
    home_dir: &Path,
) -> Result<ProviderAuthBundle> {
    let files = required_files_for_provider(provider)
        .iter()
        .map(|rel_path| {
            let path = home_dir.join(rel_path);
            let contents = read_provider_auth_file(&path)?;
            Ok(ProviderAuthFile {
                path: (*rel_path).to_string(),
                contents_b64: encode_file_contents(&contents),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ProviderAuthBundle { files })
}

pub fn restore_bundle(
    provider: ProviderAuthProvider,
    bundle: &ProviderAuthBundle,
    home_dir: &Path,
) -> Result<()> {
    validate_provider_auth_bundle(provider, bundle)?;
    for rel_path in required_files_for_provider(provider) {
        let rel_path = sanitize_relative_path(rel_path)?;
        let file = bundle
            .files
            .iter()
            .find(|file| file.path == rel_path)
            .ok_or_else(|| anyhow!("missing provider auth file: {rel_path}"))?;
        let path = home_dir.join(rel_path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create provider auth dir {}", parent.display()))?;
        }
        let contents = decode_file_contents(&file.contents_b64)?;
        fs::write(&path, contents)
            .with_context(|| format!("write provider auth file {}", path.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
                .with_context(|| format!("restrict provider auth file {}", path.display()))?;
        }
    }
    Ok(())
}

pub fn validate_provider_auth_bundle(
    provider: ProviderAuthProvider,
    bundle: &ProviderAuthBundle,
) -> Result<()> {
    let required = required_files_for_provider(provider);
    let mut seen = HashSet::new();
    for file in &bundle.files {
        let path = sanitize_relative_path(&file.path)?;
        if !required.contains(&path) {
            return Err(anyhow!(
                "provider auth bundle contains unexpected file for {}: {}",
                provider.as_str(),
                file.path
            ));
        }
        if !seen.insert(path) {
            return Err(anyhow!(
                "provider auth bundle contains duplicate file for {}: {}",
                provider.as_str(),
                file.path
            ));
        }
    }
    for path in required {
        if !seen.contains(path) {
            return Err(anyhow!("missing provider auth file: {path}"));
        }
    }
    Ok(())
}

pub fn bundle_contains_required_files(
    provider: ProviderAuthProvider,
    bundle: &ProviderAuthBundle,
) -> bool {
    validate_provider_auth_bundle(provider, bundle).is_ok()
}

pub fn claude_credentials_path(home_dir: &Path) -> PathBuf {
    home_dir.join(CLAUDE_CREDENTIALS_REL_PATH)
}

pub fn codex_credentials_path(home_dir: &Path) -> PathBuf {
    home_dir.join(CODEX_AUTH_REL_PATH)
}

pub fn ensure_codex_home(home_dir: &Path) -> Result<PathBuf> {
    let codex_home = home_dir.join(".codex");
    fs::create_dir_all(&codex_home)
        .with_context(|| format!("create provider auth dir {}", codex_home.display()))?;
    Ok(codex_home)
}

fn read_openai_auth_json(home_dir: &Path) -> Result<Option<Value>> {
    let path = codex_credentials_path(home_dir);
    let file = match fs::File::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("read OpenAI auth JSON {}", path.display()));
        }
    };
    let contents = String::from_utf8(read_provider_auth_file_from_open(&path, file)?)
        .with_context(|| format!("read OpenAI auth JSON {}", path.display()))?;
    serde_json::from_str(&contents)
        .map(Some)
        .with_context(|| format!("parse OpenAI auth JSON {}", path.display()))
}

fn decode_jwt_payload(token: &str) -> Option<Value> {
    let payload = token.split('.').nth(1)?;
    if base64_input_exceeds_decoded_limit(payload.len(), PROVIDER_AUTH_JWT_PAYLOAD_MAX_BYTES) {
        return None;
    }
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(payload))
        .ok()?;
    if decoded.len() > PROVIDER_AUTH_JWT_PAYLOAD_MAX_BYTES {
        return None;
    }
    serde_json::from_slice(&decoded).ok()
}

fn openai_identity_from_auth_json(
    auth_json: Option<&Value>,
) -> (String, String, Option<DateTime<Utc>>) {
    let Some(auth_json) = auth_json else {
        return (String::new(), String::new(), None);
    };
    let claims = auth_json
        .get("tokens")
        .and_then(|value| value.get("id_token"))
        .and_then(Value::as_str)
        .and_then(decode_jwt_payload);
    let email = claims
        .as_ref()
        .and_then(|value| value.get("email"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    // Codex ChatGPT auth stores refreshable session tokens. The ID token
    // expiry is not the session expiry, so treating it as authoritative makes
    // a freshly linked ChatGPT login look expired in Borg.
    (email.clone(), email, None)
}

pub async fn validate_home(
    provider: ProviderAuthProvider,
    home_dir: &Path,
) -> Result<ProviderAuthValidation> {
    match provider {
        ProviderAuthProvider::Claude => validate_claude_home(home_dir).await,
        ProviderAuthProvider::Openai => validate_openai_home(home_dir).await,
    }
}

async fn validate_claude_home(home_dir: &Path) -> Result<ProviderAuthValidation> {
    let mut cmd = Command::new("claude");
    cmd.args(["auth", "status", "--json"])
        .env("HOME", home_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let output = run_provider_auth_status_command(&mut cmd, "claude auth status").await?;
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let (value, parse_error) = match serde_json::from_str::<Value>(&stdout) {
        Ok(value) => (value, None),
        Err(error) => (Value::Null, Some(error)),
    };
    let ok = output.status.success()
        && parse_error.is_none()
        && value
            .get("loggedIn")
            .and_then(Value::as_bool)
            .unwrap_or(false);
    Ok(ProviderAuthValidation {
        ok,
        auth_kind: auth_kind_for_provider(ProviderAuthProvider::Claude, Some(&value)),
        account_email: value
            .get("email")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        account_label: value
            .get("orgName")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        expires_at: None,
        last_error: if ok {
            String::new()
        } else if let Some(error) = parse_error {
            format!("failed to parse claude auth status JSON: {error}; stdout: {stdout}")
        } else if !stderr.is_empty() {
            stderr
        } else {
            stdout
        },
    })
}

async fn validate_openai_home(home_dir: &Path) -> Result<ProviderAuthValidation> {
    let codex_home = ensure_codex_home(home_dir)?;
    let mut cmd = Command::new("codex");
    cmd.args(["login", "status"])
        .env("HOME", home_dir)
        .env("CODEX_HOME", &codex_home)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let output = run_provider_auth_status_command(&mut cmd, "codex login status").await?;
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let ok = output.status.success() && openai_status_is_logged_in(&stdout, &stderr);
    let auth_json = read_openai_auth_json(home_dir)?;
    let (account_email, account_label, expires_at) =
        openai_identity_from_auth_json(auth_json.as_ref());
    Ok(ProviderAuthValidation {
        ok,
        auth_kind: auth_kind_for_provider(ProviderAuthProvider::Openai, auth_json.as_ref()),
        account_email,
        account_label,
        expires_at,
        last_error: if ok {
            String::new()
        } else if !stderr.is_empty() {
            stderr
        } else {
            stdout
        },
    })
}

async fn run_provider_auth_status_command(command: &mut Command, label: &str) -> Result<Output> {
    run_provider_auth_status_command_with_timeout(
        command,
        label,
        provider_auth_validation_timeout()?,
    )
    .await
}

async fn run_provider_auth_status_command_with_timeout(
    command: &mut Command,
    label: &str,
    timeout: Duration,
) -> Result<Output> {
    command.kill_on_drop(true);
    tokio::time::timeout(timeout, command.output())
        .await
        .map_err(|_| anyhow!("{label} timed out after {}s", timeout.as_secs_f64()))?
        .with_context(|| format!("run {label}"))
}

fn provider_auth_validation_timeout() -> Result<Duration> {
    let raw = match std::env::var(PROVIDER_AUTH_VALIDATION_TIMEOUT_ENV) {
        Ok(raw) => raw,
        Err(std::env::VarError::NotPresent) => {
            return Ok(Duration::from_secs(
                DEFAULT_PROVIDER_AUTH_VALIDATION_TIMEOUT_SECS,
            ));
        }
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(anyhow!(
                "{PROVIDER_AUTH_VALIDATION_TIMEOUT_ENV} contains invalid unicode"
            ));
        }
    };
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(Duration::from_secs(
            DEFAULT_PROVIDER_AUTH_VALIDATION_TIMEOUT_SECS,
        ));
    }
    match raw.parse::<u64>() {
        Ok(secs) if secs > 0 => Ok(Duration::from_secs(secs)),
        Ok(_) => Err(anyhow!(
            "{PROVIDER_AUTH_VALIDATION_TIMEOUT_ENV} must be positive"
        )),
        Err(error) => Err(anyhow!(
            "invalid {PROVIDER_AUTH_VALIDATION_TIMEOUT_ENV}: {error}"
        )),
    }
}

fn openai_status_is_logged_in(stdout: &str, stderr: &str) -> bool {
    let status_text = format!("{stdout}\n{stderr}").to_ascii_lowercase();
    status_text.contains("logged in")
}

pub fn should_revalidate(
    last_validated_at: Option<DateTime<Utc>>,
    expires_at: Option<DateTime<Utc>>,
) -> bool {
    let now = Utc::now();
    match last_validated_at {
        None => return true,
        Some(last_validated_at)
            if now.signed_duration_since(last_validated_at).num_minutes()
                >= PROVIDER_AUTH_REFRESH_WINDOW_MINS =>
        {
            return true;
        }
        Some(_) => {}
    }
    expires_at
        .map(|expiry| expiry <= now + chrono::Duration::minutes(PROVIDER_AUTH_REFRESH_WINDOW_MINS))
        .unwrap_or(false)
}

pub fn normalize_provider(raw: &str) -> Result<ProviderAuthProvider> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "claude" => Ok(ProviderAuthProvider::Claude),
        "openai" => Ok(ProviderAuthProvider::Openai),
        other => Err(anyhow!("unsupported provider auth provider: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct EnvGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = std::env::var(key).ok();
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, previous }
        }

        fn unset(key: &'static str) -> Self {
            let previous = std::env::var(key).ok();
            unsafe {
                std::env::remove_var(key);
            }
            Self { key, previous }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            unsafe {
                if let Some(previous) = self.previous.as_ref() {
                    std::env::set_var(self.key, previous);
                } else {
                    std::env::remove_var(self.key);
                }
            }
        }
    }

    #[test]
    fn bundle_requires_expected_paths() {
        let bundle = ProviderAuthBundle {
            files: vec![ProviderAuthFile {
                path: ".claude/.credentials.json".to_string(),
                contents_b64: encode_file_contents(br#"{"claudeAiOauth":{"accessToken":"x"}}"#),
            }],
        };
        assert!(bundle_contains_required_files(
            ProviderAuthProvider::Claude,
            &bundle
        ));
        assert!(!bundle_contains_required_files(
            ProviderAuthProvider::Openai,
            &bundle
        ));
    }

    #[test]
    fn provider_auth_bundles_must_match_expected_file_contract() {
        let valid = ProviderAuthBundle {
            files: vec![ProviderAuthFile {
                path: ".codex/auth.json".to_string(),
                contents_b64: encode_file_contents(br#"{"OPENAI_API_KEY":"sk-test"}"#),
            }],
        };
        validate_provider_auth_bundle(ProviderAuthProvider::Openai, &valid)
            .expect("single expected file should validate");

        let duplicate = ProviderAuthBundle {
            files: vec![
                ProviderAuthFile {
                    path: ".codex/auth.json".to_string(),
                    contents_b64: encode_file_contents(b"one"),
                },
                ProviderAuthFile {
                    path: ".codex/auth.json".to_string(),
                    contents_b64: encode_file_contents(b"two"),
                },
            ],
        };
        assert!(validate_provider_auth_bundle(ProviderAuthProvider::Openai, &duplicate).is_err());

        let unexpected = ProviderAuthBundle {
            files: vec![
                ProviderAuthFile {
                    path: ".codex/auth.json".to_string(),
                    contents_b64: encode_file_contents(b"{}"),
                },
                ProviderAuthFile {
                    path: ".ssh/id_rsa".to_string(),
                    contents_b64: encode_file_contents(b"secret"),
                },
            ],
        };
        assert!(validate_provider_auth_bundle(ProviderAuthProvider::Openai, &unexpected).is_err());

        let unsafe_path = ProviderAuthBundle {
            files: vec![ProviderAuthFile {
                path: "../auth.json".to_string(),
                contents_b64: encode_file_contents(b"{}"),
            }],
        };
        assert!(validate_provider_auth_bundle(ProviderAuthProvider::Openai, &unsafe_path).is_err());
    }

    #[test]
    fn refresh_window_revalidates_old_or_expiring_credentials() {
        assert!(should_revalidate(
            Some(Utc::now() - chrono::Duration::minutes(16)),
            None
        ));
        assert!(should_revalidate(
            Some(Utc::now()),
            Some(Utc::now() + chrono::Duration::minutes(5))
        ));
        assert!(!should_revalidate(
            Some(Utc::now()),
            Some(Utc::now() + chrono::Duration::minutes(45))
        ));
    }

    #[test]
    fn decodes_openai_identity_from_jwt_payload() {
        let claims = serde_json::json!({
            "email": "user@example.com",
            "exp": 1_900_000_000i64,
        });
        let claims_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&claims).unwrap());
        let token = format!("x.{claims_b64}.y");
        let payload = decode_jwt_payload(&token).unwrap();
        assert_eq!(payload["email"].as_str(), Some("user@example.com"));
    }

    #[test]
    fn jwt_payload_decode_rejects_oversized_payloads() {
        let encoded_limit = PROVIDER_AUTH_JWT_PAYLOAD_MAX_BYTES.div_ceil(3) * 4;
        let token = format!("x.{}.y", "a".repeat(encoded_limit + 1));

        assert!(decode_jwt_payload(&token).is_none());
    }

    #[test]
    fn provider_auth_file_decode_rejects_oversized_payloads() {
        let encoded_limit = PROVIDER_AUTH_FILE_MAX_BYTES.div_ceil(3) * 4;
        let error = decode_file_contents(&"a".repeat(encoded_limit + 1))
            .expect_err("oversized file payload should fail before decode");

        assert!(
            error
                .to_string()
                .contains("provider auth bundle file exceeds")
        );
    }

    #[test]
    fn openai_auth_json_reader_rejects_invalid_existing_file() {
        let home = tempfile::tempdir().expect("tempdir");
        let codex_home = home.path().join(".codex");
        fs::create_dir_all(&codex_home).expect("codex home");
        fs::write(codex_home.join("auth.json"), b"{not-json").expect("write auth");

        let error = read_openai_auth_json(home.path()).expect_err("invalid auth should fail");
        assert!(format!("{error:#}").contains("parse OpenAI auth JSON"));
    }

    #[test]
    fn provider_auth_file_reader_rejects_oversized_file() {
        let home = tempfile::tempdir().expect("tempdir");
        let path = home.path().join("auth.json");
        let file = fs::File::create(&path).expect("create sparse auth");
        file.set_len(PROVIDER_AUTH_FILE_MAX_BYTES as u64 + 1)
            .expect("set sparse auth length");

        let error = read_provider_auth_file(&path)
            .expect_err("oversized provider auth file should fail")
            .to_string();

        assert!(
            error.contains("provider auth file")
                && error.contains("exceeds")
                && error.contains("byte limit"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn openai_auth_json_reader_rejects_oversized_file() {
        let home = tempfile::tempdir().expect("tempdir");
        let codex_home = home.path().join(".codex");
        fs::create_dir_all(&codex_home).expect("codex home");
        let file = fs::File::create(codex_home.join("auth.json")).expect("create sparse auth");
        file.set_len(PROVIDER_AUTH_FILE_MAX_BYTES as u64 + 1)
            .expect("set sparse auth length");

        let error = read_openai_auth_json(home.path())
            .expect_err("oversized auth should fail")
            .to_string();

        assert!(
            error.contains("provider auth file")
                && error.contains("exceeds")
                && error.contains("byte limit"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn openai_status_accepts_logged_in_message_on_stderr() {
        assert!(openai_status_is_logged_in("", "Logged in using ChatGPT"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn provider_auth_status_command_times_out_hanging_process() {
        let mut command = Command::new("sleep");
        command.arg("5");
        let started = std::time::Instant::now();

        let error = run_provider_auth_status_command_with_timeout(
            &mut command,
            "test provider auth status",
            Duration::from_millis(100),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("timed out"));
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "timeout should fire promptly"
        );
    }

    #[test]
    fn provider_auth_validation_timeout_rejects_invalid_config() {
        let _guard = EnvGuard::unset(PROVIDER_AUTH_VALIDATION_TIMEOUT_ENV);
        assert_eq!(
            provider_auth_validation_timeout().unwrap(),
            Duration::from_secs(DEFAULT_PROVIDER_AUTH_VALIDATION_TIMEOUT_SECS)
        );

        let _guard = EnvGuard::set(PROVIDER_AUTH_VALIDATION_TIMEOUT_ENV, " 45 ");
        assert_eq!(
            provider_auth_validation_timeout().unwrap(),
            Duration::from_secs(45)
        );

        let _guard = EnvGuard::set(PROVIDER_AUTH_VALIDATION_TIMEOUT_ENV, " ");
        assert_eq!(
            provider_auth_validation_timeout().unwrap(),
            Duration::from_secs(DEFAULT_PROVIDER_AUTH_VALIDATION_TIMEOUT_SECS)
        );

        let _guard = EnvGuard::set(PROVIDER_AUTH_VALIDATION_TIMEOUT_ENV, "0");
        assert!(provider_auth_validation_timeout().is_err());

        let _guard = EnvGuard::set(PROVIDER_AUTH_VALIDATION_TIMEOUT_ENV, "soon");
        assert!(provider_auth_validation_timeout().is_err());
    }

    #[test]
    fn codex_home_chatgpt_session_detection_reads_tokens() {
        let home = tempfile::tempdir().expect("tempdir");
        let codex_home = home.path().join(".codex");
        fs::create_dir_all(&codex_home).expect("codex home");
        fs::write(
            codex_home.join("auth.json"),
            serde_json::to_vec(&serde_json::json!({
                "tokens": {
                    "id_token": "header.payload.signature"
                }
            }))
            .expect("auth json"),
        )
        .expect("write auth");

        assert!(
            codex_home_holds_chatgpt_session_checked(&codex_home)
                .expect("session detection should read valid auth")
        );
    }

    #[test]
    fn codex_home_chatgpt_session_detection_treats_missing_auth_as_false() {
        let home = tempfile::tempdir().expect("tempdir");
        let codex_home = home.path().join(".codex");
        fs::create_dir_all(&codex_home).expect("codex home");

        assert!(
            !codex_home_holds_chatgpt_session_checked(&codex_home)
                .expect("missing auth is not a ChatGPT session")
        );
    }

    #[test]
    fn codex_home_chatgpt_session_detection_rejects_invalid_auth_json() {
        let home = tempfile::tempdir().expect("tempdir");
        let codex_home = home.path().join(".codex");
        fs::create_dir_all(&codex_home).expect("codex home");
        fs::write(codex_home.join("auth.json"), b"{not-json").expect("write auth");

        let error = codex_home_holds_chatgpt_session_checked(&codex_home)
            .expect_err("invalid existing auth must fail inspection");
        assert!(format!("{error:#}").contains("parse Codex auth JSON"));
    }

    #[test]
    fn rejects_unsafe_restore_paths() {
        assert!(sanitize_relative_path("../secrets").is_err());
        assert!(sanitize_relative_path("/tmp/secrets").is_err());
        assert!(sanitize_relative_path(".codex/auth.json").is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn restore_bundle_restricts_provider_auth_file_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let home = tempfile::tempdir().expect("tempdir");
        let bundle = ProviderAuthBundle {
            files: vec![ProviderAuthFile {
                path: ".codex/auth.json".to_string(),
                contents_b64: encode_file_contents(br#"{"OPENAI_API_KEY":"sk-test"}"#),
            }],
        };

        restore_bundle(ProviderAuthProvider::Openai, &bundle, home.path()).expect("restore bundle");

        let mode = fs::metadata(home.path().join(".codex/auth.json"))
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }
}
