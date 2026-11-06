//! On-disk API key store for providers that authenticate with a key rather
//! than an OAuth session.
//!
//! Keys live in `~/.borg/credentials.json` with the same posture the Claude
//! and Codex CLIs use for their own credential files: a `0700` directory
//! holding a `0600` file, written through a temp file in the same directory so
//! a partial write never leaves a readable half-file behind. Environment
//! variables always win over the store, so an operator-managed key in the
//! environment is never silently shadowed by a stale stored one.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::bounded_io::read_open_file_bytes_with_limit;

const CREDENTIALS_FILE_MAX_BYTES: u64 = 256 * 1024;

/// A provider credential that borg itself stores, keyed by the environment
/// variable the provider harness reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiKeyCredential {
    OpenAi,
    OpenCodeGo,
    Anthropic,
    OpenRouter,
    /// Vercel AI Gateway key, for the Vercel gateway lane.
    Vercel,
    /// Z.ai key, for the GLM Coding Plan.
    Zai,
    /// Moonshot key, for Kimi Code.
    Kimi,
    /// Alibaba Cloud Model Studio key, for the Qwen Coding Plan.
    Qwen,
}

impl ApiKeyCredential {
    pub fn env_var(self) -> &'static str {
        match self {
            Self::OpenAi => "OPENAI_API_KEY",
            Self::OpenCodeGo => "OPENCODE_GO_API_KEY",
            Self::Anthropic => "ANTHROPIC_API_KEY",
            Self::OpenRouter => "OPENROUTER_API_KEY",
            // What `vercel ai-gateway api-keys create` prints.
            Self::Vercel => "VERCEL_AI_GATEWAY_API_KEY",
            // The user-facing variable for their own key. The plan overlay maps
            // it onto whichever token header the hosting CLI expects.
            Self::Zai => "ZAI_API_KEY",
            Self::Kimi => "KIMI_API_KEY",
            // The Coding Plan's own key. The pay-as-you-go key is a different
            // credential (`DASHSCOPE_API_KEY`) and is read as a fallback by the
            // Qwen profile, never stored under this one.
            Self::Qwen => "BAILIAN_CODING_PLAN_API_KEY",
        }
    }

    fn storage_key(self) -> &'static str {
        match self {
            Self::OpenAi => "openai_api_key",
            Self::OpenCodeGo => "opencode_go_api_key",
            Self::Anthropic => "anthropic_api_key",
            Self::OpenRouter => "openrouter_api_key",
            Self::Vercel => "vercel_ai_gateway_api_key",
            Self::Zai => "zai_api_key",
            Self::Kimi => "kimi_api_key",
            Self::Qwen => "qwen_api_key",
        }
    }
}

#[derive(Default, Serialize, Deserialize)]
struct CredentialsFile {
    #[serde(flatten)]
    keys: BTreeMap<String, String>,
}

pub(crate) fn borg_dir() -> Option<PathBuf> {
    if let Some(dir) = crate::env::nonempty_var("BORG_HOME") {
        return Some(PathBuf::from(dir));
    }
    std::env::var_os("HOME")
        .filter(|home| !home.is_empty())
        .map(|home| PathBuf::from(home).join(".borg"))
}

fn credentials_path() -> Option<PathBuf> {
    borg_dir().map(|dir| dir.join("credentials.json"))
}

fn read_credentials(path: &Path) -> Result<CredentialsFile> {
    let file = match fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(CredentialsFile::default());
        }
        Err(error) => {
            return Err(error).with_context(|| format!("read {}", path.display()));
        }
    };
    let bytes = read_open_file_bytes_with_limit(
        path,
        "borg credential store",
        file,
        CREDENTIALS_FILE_MAX_BYTES,
    )?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))
}

/// The stored key for `credential`, ignoring the environment.
pub fn stored_api_key(credential: ApiKeyCredential) -> Option<String> {
    let path = credentials_path()?;
    let file = read_credentials(&path).ok()?;
    file.keys
        .get(credential.storage_key())
        .map(|key| key.trim().to_string())
        .filter(|key| !key.is_empty())
}

/// The effective key for `credential`: the environment wins, the store is the
/// fallback.
pub fn api_key(credential: ApiKeyCredential) -> Option<String> {
    crate::env::nonempty_var(credential.env_var()).or_else(|| stored_api_key(credential))
}

/// Persists `key` for `credential`, replacing any previously stored value.
pub fn store_api_key(credential: ApiKeyCredential, key: &str) -> Result<PathBuf> {
    store_value(credential.storage_key(), key)
}

/// A custom provider slot. No key is returned to an agent-facing API.
pub fn named_provider_api_key(provider: &str) -> Option<String> {
    let path = credentials_path()?;
    read_credentials(&path)
        .ok()?
        .keys
        .get(&format!("provider:{provider}"))
        .cloned()
}

pub fn store_named_provider_api_key(provider: &str, key: &str) -> Result<PathBuf> {
    anyhow::ensure!(
        !provider.is_empty()
            && provider
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')),
        "invalid provider ID"
    );
    store_value(&format!("provider:{provider}"), key)
}

pub fn clear_named_provider_api_key(provider: &str) -> Result<()> {
    clear_value(&format!("provider:{provider}"))
}

/// The coding plan persisted on this machine, if any. Written by
/// `borg login <plan>`; the `BORG_SUBSCRIPTION` environment variable still
/// takes precedence so an operator can override a saved selection.
pub fn stored_active_subscription() -> Option<String> {
    let path = credentials_path()?;
    let file = read_credentials(&path).ok()?;
    file.keys
        .get("active_subscription")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// Records the active coding plan so the choice survives across sessions.
pub fn set_active_subscription(plan: &str) -> Result<PathBuf> {
    store_value("active_subscription", plan)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenAiAuthMode {
    Subscription,
    ApiKey,
}

/// No preference means follow the user's current Codex login. Selecting a
/// lane in Borg keeps both credentials and never falls back to the other lane.
pub fn openai_auth_mode() -> Result<Option<OpenAiAuthMode>> {
    let Some(path) = credentials_path() else {
        return Ok(None);
    };
    let file = read_credentials(&path)?;
    match file.keys.get("openai_auth_mode").map(String::as_str) {
        None => Ok(None),
        Some("subscription") => Ok(Some(OpenAiAuthMode::Subscription)),
        Some("api-key") => Ok(Some(OpenAiAuthMode::ApiKey)),
        Some(_) => anyhow::bail!("Unknown OpenAI billing mode; choose a connection with /login"),
    }
}

pub fn set_openai_auth_mode(mode: OpenAiAuthMode) -> Result<PathBuf> {
    store_value(
        "openai_auth_mode",
        match mode {
            OpenAiAuthMode::Subscription => "subscription",
            OpenAiAuthMode::ApiKey => "api-key",
        },
    )
}

/// Whether Claude turns bill the stored Anthropic API key instead of the
/// Claude subscription login. Only an explicit choice selects the API; the
/// subscription stays the default so a stored key never starts paid usage.
pub fn claude_uses_api_key() -> bool {
    credentials_path()
        .and_then(|path| read_credentials(&path).ok())
        .is_some_and(|file| {
            file.keys.get("claude_auth_mode").map(String::as_str) == Some("api-key")
        })
}

pub fn set_claude_uses_api_key(api_key: bool) -> Result<PathBuf> {
    store_value(
        "claude_auth_mode",
        if api_key { "api-key" } else { "subscription" },
    )
}

pub(crate) const CLAUDE_CONFIG_DIR_KEY: &str = "claude_subscription_config_dir";
pub(crate) const OPENAI_AUTH_FILE_KEY: &str = "openai_subscription_auth_file";

fn env_override(env_var: &str) -> Option<PathBuf> {
    crate::env::nonempty_var(env_var).map(PathBuf::from)
}

/// An operator-set `BORG_CLAUDE_CONFIG_DIR`. It wins over every stored
/// selection, so account switching must refuse rather than pretend.
pub fn claude_config_dir_env_override() -> Option<PathBuf> {
    env_override("BORG_CLAUDE_CONFIG_DIR")
}

/// An operator-set `BORG_OPENAI_AUTH_FILE`; see
/// [`claude_config_dir_env_override`].
pub fn openai_auth_file_env_override() -> Option<PathBuf> {
    env_override("BORG_OPENAI_AUTH_FILE")
}

/// A path setting: the environment variable wins, the store is the fallback.
fn stored_path(env_var: &str, storage_key: &str) -> Result<Option<PathBuf>> {
    if let Some(path) = env_override(env_var) {
        return Ok(Some(path));
    }
    let Some(path) = credentials_path() else {
        return Ok(None);
    };
    Ok(read_credentials(&path)?
        .keys
        .get(storage_key)
        .map(PathBuf::from))
}

pub fn openai_subscription_auth_file() -> Result<Option<PathBuf>> {
    stored_path("BORG_OPENAI_AUTH_FILE", OPENAI_AUTH_FILE_KEY)
}

/// The Claude config directory Borg signs in to, when Borg owns one instead
/// of sharing Claude Code's.
pub fn claude_subscription_config_dir() -> Result<Option<PathBuf>> {
    stored_path("BORG_CLAUDE_CONFIG_DIR", CLAUDE_CONFIG_DIR_KEY)
}

pub fn set_claude_subscription_config_dir(path: &Path) -> Result<PathBuf> {
    store_values(&[(CLAUDE_CONFIG_DIR_KEY, path_string(path, "Claude config")?)])
}

/// Where `borg login claude` keeps a Borg-owned Claude login.
pub fn default_claude_subscription_config_dir() -> Option<PathBuf> {
    borg_dir().map(|dir| dir.join("claude"))
}

pub fn set_openai_subscription_auth_file(path: &Path) -> Result<PathBuf> {
    store_values(&[(OPENAI_AUTH_FILE_KEY, path_string(path, "ChatGPT auth")?)])
}

fn path_string<'a>(path: &'a Path, label: &str) -> Result<&'a str> {
    path.to_str()
        .with_context(|| format!("{label} path must be UTF-8"))
}

/// Stored values, for callers that own a family of keys (the subscription
/// account registry). Environment overrides are not part of the store.
pub(crate) fn stored_values() -> Result<BTreeMap<String, String>> {
    match credentials_path() {
        Some(path) => Ok(read_credentials(&path)?.keys),
        None => Ok(BTreeMap::new()),
    }
}

pub fn codex_auth_json() -> Option<serde_json::Value> {
    let directory = crate::env::nonempty_var("CODEX_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".codex")))?;
    let path = directory.join("auth.json");
    let file = fs::File::open(&path).ok()?;
    let bytes = read_open_file_bytes_with_limit(
        &path,
        "Codex credentials",
        file,
        CREDENTIALS_FILE_MAX_BYTES,
    )
    .ok()?;
    serde_json::from_slice(&bytes).ok()
}

pub fn openai_api_key() -> Option<String> {
    api_key(ApiKeyCredential::OpenAi).or_else(|| {
        codex_auth_json()?
            .get("OPENAI_API_KEY")?
            .as_str()
            .map(str::trim)
            .filter(|key| !key.is_empty())
            .map(str::to_string)
    })
}

pub fn opencode_auth_json() -> Result<serde_json::Value> {
    if let Some(contents) = crate::env::nonempty_var("OPENCODE_AUTH_CONTENT") {
        anyhow::ensure!(
            contents.len() as u64 <= CREDENTIALS_FILE_MAX_BYTES,
            "OpenCode credentials exceed size limit"
        );
        return serde_json::from_str(&contents).context("Invalid OpenCode credential data");
    }
    let directory = crate::env::nonempty_var("XDG_DATA_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/share")))
        .context("Cannot locate OpenCode credentials")?;
    let path = directory.join("opencode/auth.json");
    let file = match fs::File::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(serde_json::json!({}));
        }
        Err(error) => return Err(error).context("Cannot read OpenCode credentials"),
    };
    let bytes = read_open_file_bytes_with_limit(
        &path,
        "OpenCode credentials",
        file,
        CREDENTIALS_FILE_MAX_BYTES,
    )?;
    serde_json::from_slice(&bytes).context("Invalid OpenCode credential data")
}

pub fn opencode_go_api_key() -> Option<String> {
    api_key(ApiKeyCredential::OpenCodeGo).or_else(|| {
        opencode_auth_json().ok()?["opencode-go"]["key"]
            .as_str()
            .map(str::trim)
            .filter(|key| !key.is_empty())
            .map(str::to_string)
    })
}

/// Billing selection for admission and requests. A selected subscription authority
/// never silently falls through to an unrelated legacy API key.
pub fn openai_uses_api_key() -> bool {
    match openai_auth_mode() {
        Ok(Some(OpenAiAuthMode::ApiKey)) => true,
        Ok(Some(OpenAiAuthMode::Subscription)) | Err(_) => false,
        Ok(None)
            if crate::openai_subscription::auth_path()
                .map(|path| path.exists())
                .unwrap_or(true) =>
        {
            false
        }
        Ok(None) => match codex_auth_json() {
            Some(auth) => match auth["auth_mode"].as_str() {
                Some("apikey") => true,
                Some("chatgpt" | "chatgptAuthTokens") => false,
                _ => {
                    auth["OPENAI_API_KEY"]
                        .as_str()
                        .is_some_and(|key| !key.is_empty())
                        && auth["tokens"].is_null()
                }
            },
            None => openai_api_key().is_some(),
        },
    }
}

fn lock_credentials(dir: &Path) -> Result<fs::File> {
    let mut options = fs::OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options
        .open(dir.join("credentials.lock"))
        .context("open private credential lock")?;
    file.lock().context("lock credential store")?;
    Ok(file)
}

fn store_value(storage_key: &str, key: &str) -> Result<PathBuf> {
    store_values(&[(storage_key, key)])
}

/// Replace several values in one locked write, so a selection and the account
/// it belongs to can never be observed half-updated.
pub(crate) fn store_values(entries: &[(&str, &str)]) -> Result<PathBuf> {
    for (storage_key, key) in entries {
        anyhow::ensure!(!key.trim().is_empty(), "{storage_key} cannot be empty");
    }
    let dir = borg_dir().context("cannot locate a home directory for the key store")?;
    fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
    restrict_permissions(&dir, 0o700)?;
    let _lock = lock_credentials(&dir)?;
    let path = dir.join("credentials.json");
    let mut file = read_credentials(&path)?;
    for (storage_key, key) in entries {
        file.keys
            .insert((*storage_key).to_string(), key.trim().to_string());
    }
    let contents = serde_json::to_vec_pretty(&file).context("serialize credential store")?;

    // Same-directory temp file keeps the rename atomic and never widens the
    // window in which the key is readable by others.
    use std::io::Write;
    let mut temp =
        tempfile::NamedTempFile::new_in(&dir).context("create private credential file")?;
    temp.write_all(&contents)
        .context("write private credential file")?;
    temp.as_file().sync_all().context("sync credential store")?;
    temp.persist(&path)
        .with_context(|| format!("replace {}", path.display()))?;
    restrict_permissions(&path, 0o600)?;
    Ok(path)
}

/// Removes the stored key for `credential`, if any.
pub fn clear_api_key(credential: ApiKeyCredential) -> Result<()> {
    clear_value(credential.storage_key())
}

fn clear_value(storage_key: &str) -> Result<()> {
    let Some(path) = credentials_path() else {
        return Ok(());
    };
    let dir = path.parent().context("credential store has no parent")?;
    if !dir.exists() {
        return Ok(());
    }
    let _lock = lock_credentials(dir)?;
    let mut file = read_credentials(&path)?;
    if file.keys.remove(storage_key).is_none() {
        return Ok(());
    }
    let contents = serde_json::to_vec_pretty(&file).context("serialize credential store")?;
    use std::io::Write;
    let mut temp =
        tempfile::NamedTempFile::new_in(path.parent().context("credential store has no parent")?)?;
    temp.write_all(&contents)
        .context("write private credential file")?;
    temp.as_file().sync_all().context("sync credential store")?;
    temp.persist(&path)
        .with_context(|| format!("replace {}", path.display()))?;
    restrict_permissions(&path, 0o600)
}

#[cfg(unix)]
fn restrict_permissions(path: &Path, mode: u32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(mode))
        .with_context(|| format!("chmod {:o} {}", mode, path.display()))
}

#[cfg(not(unix))]
fn restrict_permissions(_path: &Path, _mode: u32) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        crate::env::test_env_lock()
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    #[test]
    fn stored_key_round_trips_and_env_wins() {
        let _guard = env_lock();
        let home = tempfile::tempdir().unwrap();
        // SAFETY: serialised by env_lock() for the duration of the test.
        unsafe {
            std::env::set_var("BORG_HOME", home.path());
            std::env::remove_var("ANTHROPIC_API_KEY");
        }
        assert_eq!(api_key(ApiKeyCredential::Anthropic), None);

        let path = store_api_key(ApiKeyCredential::Anthropic, " sk-test ").unwrap();
        assert_eq!(
            stored_api_key(ApiKeyCredential::Anthropic).as_deref(),
            Some("sk-test")
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }

        // SAFETY: serialised by env_lock() for the duration of the test.
        unsafe { std::env::set_var("ANTHROPIC_API_KEY", "sk-env") };
        assert_eq!(
            api_key(ApiKeyCredential::Anthropic).as_deref(),
            Some("sk-env")
        );

        store_named_provider_api_key("work", "private-custom-test-key").unwrap();
        assert_eq!(
            named_provider_api_key("work").as_deref(),
            Some("private-custom-test-key")
        );
        let writers: Vec<_> = (0..8)
            .map(|index| {
                std::thread::spawn(move || {
                    store_named_provider_api_key(&format!("worker-{index}"), "parallel-test-key")
                        .unwrap()
                })
            })
            .collect();
        for writer in writers {
            writer.join().unwrap();
        }
        for index in 0..8 {
            assert_eq!(
                named_provider_api_key(&format!("worker-{index}")).as_deref(),
                Some("parallel-test-key")
            );
        }
        clear_named_provider_api_key("work").unwrap();
        assert!(named_provider_api_key("work").is_none());
        clear_api_key(ApiKeyCredential::Anthropic).unwrap();
        assert_eq!(stored_api_key(ApiKeyCredential::Anthropic), None);
        store_api_key(ApiKeyCredential::OpenAi, "saved-openai-key").unwrap();
        for mode in [
            OpenAiAuthMode::ApiKey,
            OpenAiAuthMode::Subscription,
            OpenAiAuthMode::ApiKey,
        ] {
            set_openai_auth_mode(mode).unwrap();
            assert_eq!(openai_auth_mode().unwrap(), Some(mode));
            assert_eq!(openai_uses_api_key(), mode == OpenAiAuthMode::ApiKey);
            assert_eq!(
                stored_api_key(ApiKeyCredential::OpenAi).as_deref(),
                Some("saved-openai-key")
            );
        }
        // SAFETY: serialised by env_lock() for the duration of the test.
        unsafe {
            std::env::remove_var("ANTHROPIC_API_KEY");
            std::env::remove_var("BORG_HOME");
        }
    }
}
