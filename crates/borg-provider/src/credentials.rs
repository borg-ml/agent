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
    /// Z.ai key, for the GLM Coding Plan.
    Zai,
    /// Moonshot key, for Kimi Code.
    Kimi,
}

impl ApiKeyCredential {
    pub fn env_var(self) -> &'static str {
        match self {
            Self::OpenAi => "OPENAI_API_KEY",
            Self::OpenCodeGo => "OPENCODE_GO_API_KEY",
            Self::Anthropic => "ANTHROPIC_API_KEY",
            Self::OpenRouter => "OPENROUTER_API_KEY",
            // The user-facing variable for their own key. The plan overlay maps
            // it onto whichever token header the hosting CLI expects.
            Self::Zai => "ZAI_API_KEY",
            Self::Kimi => "KIMI_API_KEY",
        }
    }

    fn storage_key(self) -> &'static str {
        match self {
            Self::OpenAi => "openai_api_key",
            Self::OpenCodeGo => "opencode_go_api_key",
            Self::Anthropic => "anthropic_api_key",
            Self::OpenRouter => "openrouter_api_key",
            Self::Zai => "zai_api_key",
            Self::Kimi => "kimi_api_key",
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct CredentialsFile {
    #[serde(flatten)]
    keys: BTreeMap<String, String>,
}

fn credentials_dir() -> Option<PathBuf> {
    if let Some(dir) = crate::env::nonempty_var("BORG_HOME") {
        return Some(PathBuf::from(dir));
    }
    std::env::var_os("HOME")
        .filter(|home| !home.is_empty())
        .map(|home| PathBuf::from(home).join(".borg"))
}

fn credentials_path() -> Option<PathBuf> {
    credentials_dir().map(|dir| dir.join("credentials.json"))
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

/// File-backed status for admission and the footer. The request adapter also
/// asks Codex's auth manager, which covers operating-system credential stores.
pub fn openai_uses_api_key() -> bool {
    match openai_auth_mode() {
        Ok(Some(OpenAiAuthMode::ApiKey)) => true,
        Ok(Some(OpenAiAuthMode::Subscription)) | Err(_) => false,
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

fn store_value(storage_key: &str, key: &str) -> Result<PathBuf> {
    let key = key.trim();
    anyhow::ensure!(!key.is_empty(), "API key cannot be empty");
    let dir = credentials_dir().context("cannot locate a home directory for the key store")?;
    fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
    restrict_permissions(&dir, 0o700)?;
    let path = dir.join("credentials.json");
    let mut file = read_credentials(&path)?;
    file.keys.insert(storage_key.to_string(), key.to_string());
    let contents = serde_json::to_vec_pretty(&file).context("serialize credential store")?;

    // Same-directory temp file keeps the rename atomic and never widens the
    // window in which the key is readable by others.
    use std::io::Write;
    let mut temp =
        tempfile::NamedTempFile::new_in(&dir).context("create private credential file")?;
    temp.write_all(&contents)
        .context("write private credential file")?;
    temp.persist(&path)
        .with_context(|| format!("replace {}", path.display()))?;
    restrict_permissions(&path, 0o600)?;
    Ok(path)
}

/// Removes the stored key for `credential`, if any.
pub fn clear_api_key(credential: ApiKeyCredential) -> Result<()> {
    let Some(path) = credentials_path() else {
        return Ok(());
    };
    let mut file = read_credentials(&path)?;
    if file.keys.remove(credential.storage_key()).is_none() {
        return Ok(());
    }
    let contents = serde_json::to_vec_pretty(&file).context("serialize credential store")?;
    fs::write(&path, contents).with_context(|| format!("write {}", path.display()))?;
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
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn stored_key_round_trips_and_env_wins() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
        let home = tempfile::tempdir().unwrap();
        // SAFETY: serialised by ENV_LOCK for the duration of the test.
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

        // SAFETY: serialised by ENV_LOCK for the duration of the test.
        unsafe { std::env::set_var("ANTHROPIC_API_KEY", "sk-env") };
        assert_eq!(
            api_key(ApiKeyCredential::Anthropic).as_deref(),
            Some("sk-env")
        );

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
        // SAFETY: serialised by ENV_LOCK for the duration of the test.
        unsafe {
            std::env::remove_var("ANTHROPIC_API_KEY");
            std::env::remove_var("BORG_HOME");
        }
    }
}
