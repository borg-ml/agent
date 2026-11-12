//! Several subscription keys for one key-billed lane.
//!
//! Claude and ChatGPT logins own a directory or an `auth.json`, so
//! [`crate::subscription_accounts`] keeps a name -> path map. OpenCode Go, the
//! GLM Coding Plan, Kimi Code and the Qwen Coding Plan are billed by a plain
//! key instead: several subscriptions are several keys. This module keeps a
//! name -> key map plus one selected name per lane in the same credential
//! store, so switching the name changes which subscription the next turn
//! spends without any adapter-side state.
//!
//! A lane that has never been given a name keeps using the single key the user
//! connected before named accounts existed: naming a second subscription
//! adopts the existing key as `default` instead of making it disappear.

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail, ensure};

use crate::credentials::{self, ApiKeyCredential};

/// A subscription billed by an API key rather than an OAuth login.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyLane {
    OpenCodeGo,
    Glm,
    Kimi,
    Qwen,
    /// Pay-per-use API keys. Several can be kept and one selected, exactly like
    /// the subscription keys above; they are never rotated automatically.
    Anthropic,
    OpenAi,
    OpenRouter,
    Vercel,
}

impl KeyLane {
    pub const ALL: [KeyLane; 8] = [
        KeyLane::OpenCodeGo,
        KeyLane::Glm,
        KeyLane::Kimi,
        KeyLane::Qwen,
        KeyLane::Anthropic,
        KeyLane::OpenAi,
        KeyLane::OpenRouter,
        KeyLane::Vercel,
    ];

    /// Storage id, matching the provider id clients already use. The API-key
    /// lanes avoid `claude` and `openai`, whose `{id}_account_` prefix belongs
    /// to the OAuth login registry.
    pub fn id(self) -> &'static str {
        match self {
            KeyLane::OpenCodeGo => "opencode",
            KeyLane::Glm => "glm",
            KeyLane::Kimi => "kimi",
            KeyLane::Qwen => "qwen",
            KeyLane::Anthropic => "anthropic_key",
            KeyLane::OpenAi => "openai_key",
            KeyLane::OpenRouter => "openrouter",
            KeyLane::Vercel => "vercel",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            KeyLane::OpenCodeGo => "OpenCode Go",
            KeyLane::Glm => "GLM Coding Plan",
            KeyLane::Kimi => "Kimi Code",
            KeyLane::Qwen => "Qwen Coding Plan",
            KeyLane::Anthropic => "Anthropic API",
            KeyLane::OpenAi => "OpenAI API",
            KeyLane::OpenRouter => "OpenRouter",
            KeyLane::Vercel => "Vercel AI Gateway",
        }
    }

    pub fn credential(self) -> ApiKeyCredential {
        match self {
            KeyLane::OpenCodeGo => ApiKeyCredential::OpenCodeGo,
            KeyLane::Glm => ApiKeyCredential::Zai,
            KeyLane::Kimi => ApiKeyCredential::Kimi,
            KeyLane::Qwen => ApiKeyCredential::Qwen,
            KeyLane::Anthropic => ApiKeyCredential::Anthropic,
            KeyLane::OpenAi => ApiKeyCredential::OpenAi,
            KeyLane::OpenRouter => ApiKeyCredential::OpenRouter,
            KeyLane::Vercel => ApiKeyCredential::Vercel,
        }
    }

    pub fn parse(id: &str) -> Option<KeyLane> {
        let id = id.trim().to_ascii_lowercase();
        KeyLane::ALL.into_iter().find(|lane| lane.id() == id)
    }

    /// The lane whose keys back `credential`, if it is a key-billed
    /// subscription.
    pub fn for_credential(credential: ApiKeyCredential) -> Option<KeyLane> {
        KeyLane::ALL
            .into_iter()
            .find(|lane| lane.credential() == credential)
    }
}

/// Where one set of named keys is kept: a built-in key lane, or a custom
/// OpenAI-compatible provider by its configured id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeyStore {
    Lane(KeyLane),
    Custom(String),
}

impl From<KeyLane> for KeyStore {
    fn from(lane: KeyLane) -> Self {
        KeyStore::Lane(lane)
    }
}

impl KeyStore {
    /// Prefix of every credential-store entry of this set.
    fn id(&self) -> String {
        match self {
            KeyStore::Lane(lane) => lane.id().to_string(),
            KeyStore::Custom(id) => format!("provider:{id}"),
        }
    }

    fn label(&self) -> String {
        match self {
            KeyStore::Lane(lane) => lane.label().to_string(),
            KeyStore::Custom(id) => id.clone(),
        }
    }

    /// The single key kept before named keys existed, reported as `default`.
    fn legacy(&self) -> Option<String> {
        match self {
            KeyStore::Lane(lane) => credentials::stored_api_key(lane.credential()),
            KeyStore::Custom(id) => credentials::named_provider_api_key(id),
        }
    }

    fn account_key(&self, name: &str) -> String {
        format!("{}_account_{name}", self.id())
    }

    fn selection_key(&self) -> String {
        format!("{}_account", self.id())
    }

    fn used_key(&self, name: &str) -> String {
        format!("{}_account_used_{name}", self.id())
    }
}

/// One key connected under a name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyAccount {
    pub name: String,
    pub active: bool,
}

/// Every named key in `store`, with the one the next turn spends marked
/// active. A store that only has the single legacy key is reported as one
/// `default` login, so a user who connected before named keys never has to
/// re-enter a key.
pub fn list(store: impl Into<KeyStore>) -> Result<Vec<KeyAccount>> {
    let store = store.into();
    let mut accounts = named(&store)?;
    if accounts.is_empty() && store.legacy().is_some() {
        accounts.push(KeyAccount {
            name: "default".to_owned(),
            active: true,
        });
    }
    // With no selection stored, the single legacy key is what turns spend, so
    // `default` is the active login rather than an unmarked row.
    let selected = selected_name(store)?;
    for account in &mut accounts {
        account.active = match selected.as_deref() {
            Some(name) => account.name == name,
            None => account.name == "default",
        };
    }
    accounts.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(accounts)
}

/// The named logins in the store, without the synthesized legacy one.
fn named(store: &KeyStore) -> Result<Vec<KeyAccount>> {
    let values = credentials::stored_values()?;
    let prefix = format!("{}_account_", store.id());
    Ok(values
        .keys()
        .filter_map(|key| {
            let name = key.strip_prefix(&prefix)?;
            // `{lane}_account_used_{name}` and the lane's
            // `{lane}_account_strategy` share the prefix; they are bookkeeping,
            // not logins.
            (!name.starts_with("used_") && name != crate::subscription_accounts::STRATEGY).then(
                || KeyAccount {
                    name: name.to_owned(),
                    active: false,
                },
            )
        })
        .collect())
}

/// The selected login name, if one was ever selected on this host.
pub fn selected_name(store: impl Into<KeyStore>) -> Result<Option<String>> {
    Ok(credentials::stored_values()?
        .get(&store.into().selection_key())
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty()))
}

/// The selected login's key. Callers resolve the environment first and the
/// legacy single key last, so an operator's variable is never shadowed and an
/// unnamed store keeps working.
pub fn selected_key(store: impl Into<KeyStore>) -> Option<String> {
    let store = store.into();
    let values = credentials::stored_values().ok()?;
    let name = values.get(&store.selection_key())?.trim();
    let key = values.get(&store.account_key(name))?;
    let key = key.trim();
    (!key.is_empty()).then(|| key.to_string())
}

/// The key stored for one named login.
///
/// A usage probe is per subscription: several named logins each report their
/// own allowance, so the selected key is not enough. `default` falls back to the
/// single legacy key, which is the login [`list`] synthesizes for a store that
/// was connected before named keys existed.
pub fn key(store: impl Into<KeyStore>, name: &str) -> Option<String> {
    let store = store.into();
    let values = credentials::stored_values().ok()?;
    if let Some(key) = values
        .get(&store.account_key(name))
        .map(|key| key.trim())
        .filter(|key| !key.is_empty())
    {
        return Some(key.to_string());
    }
    (name == "default").then(|| store.legacy()).flatten()
}

/// The selected key for any credential backed by named keys.
pub fn selected_key_for_credential(credential: ApiKeyCredential) -> Option<String> {
    KeyLane::for_credential(credential).and_then(selected_key)
}

/// The key a custom provider spends: its selected named key, or the single key
/// it was saved with.
pub fn custom_provider_key(id: &str) -> Option<String> {
    selected_key(KeyStore::Custom(id.to_string()))
        .or_else(|| credentials::named_provider_api_key(id))
}

/// Every credential-store entry of a custom provider's named keys.
fn custom_entries(id: &str) -> Result<Vec<(String, String)>> {
    let store = KeyStore::Custom(id.to_string());
    let selection = store.selection_key();
    let prefix = format!("{selection}_");
    Ok(credentials::stored_values()?
        .into_iter()
        .filter(|(key, _)| *key == selection || key.starts_with(&prefix))
        .collect())
}

/// Forget every named key of a custom provider, as when it is removed or its
/// authentication is cleared.
pub fn forget_custom(id: &str) -> Result<()> {
    let entries = custom_entries(id)?;
    let keys: Vec<&str> = entries.iter().map(|(key, _)| key.as_str()).collect();
    if !keys.is_empty() {
        credentials::clear_values(&keys)?;
    }
    Ok(())
}

/// Move a custom provider's named keys to its new id.
pub fn rename_custom(from: &str, to: &str) -> Result<()> {
    let old = KeyStore::Custom(from.to_string()).id();
    let new = KeyStore::Custom(to.to_string()).id();
    let moved: Vec<(String, String)> = custom_entries(from)?
        .into_iter()
        .map(|(key, value)| (format!("{new}{}", &key[old.len()..]), value))
        .collect();
    let pairs: Vec<(&str, &str)> = moved
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect();
    if !pairs.is_empty() {
        credentials::store_values(&pairs)?;
    }
    forget_custom(from)
}

/// Connect `name` with `key` and select it. The selection is written in the
/// same locked update as the key, so no turn can observe a name without its
/// key.
pub fn register(store: impl Into<KeyStore>, name: &str, key: &str) -> Result<KeyAccount> {
    let store = store.into();
    crate::subscription_accounts::validate_name(name)?;
    let key = key.trim();
    ensure!(!key.is_empty(), "the key cannot be empty");
    // A store connected before named keys keeps its key under `default`, so
    // adding a second key never hides the first.
    if named(&store)?.is_empty()
        && let Some(legacy) = store.legacy()
    {
        credentials::store_values(&[(&store.account_key("default"), &legacy)])?;
    }
    credentials::store_values(&[
        (&store.account_key(name), key),
        (&store.selection_key(), name),
    ])?;
    Ok(KeyAccount {
        name: name.to_owned(),
        active: true,
    })
}

/// Point `store` at another connected key.
pub fn activate(store: impl Into<KeyStore>, name: &str) -> Result<KeyAccount> {
    let store = store.into();
    crate::subscription_accounts::validate_name(name)?;
    let account = list(store.clone())?
        .into_iter()
        .find(|account| account.name == name)
        .with_context(|| format!("no connected {} key named {name}", store.label()))?;
    if account.active {
        return Ok(account);
    }
    credentials::store_values(&[(&store.selection_key(), name)])?;
    mark_used(store, name)?;
    Ok(KeyAccount {
        name: account.name,
        active: true,
    })
}

/// Disconnect a named key. Removing the selected login selects another
/// connected one, or clears the selection so the legacy key applies.
pub fn remove(store: impl Into<KeyStore>, name: &str) -> Result<()> {
    let store = store.into();
    crate::subscription_accounts::validate_name(name)?;
    let accounts = list(store.clone())?;
    let removed = accounts
        .iter()
        .find(|account| account.name == name)
        .with_context(|| format!("no connected {} key named {name}", store.label()))?;
    if accounts.len() == 1 && !accounts.iter().any(|account| account.name == "default") {
        bail!("cannot remove the only connected {} key", store.label());
    }
    credentials::clear_values(&[&store.account_key(name), &store.used_key(name)])?;
    if removed.active {
        match accounts.iter().find(|account| account.name != name) {
            Some(next) => {
                credentials::store_values(&[(&store.selection_key(), &next.name)])?;
            }
            None => credentials::clear_values(&[&store.selection_key()])?,
        }
    }
    Ok(())
}

/// When this login last served a turn, for balancing lanes whose provider
/// reports no quota.
pub fn last_used(store: impl Into<KeyStore>, name: &str) -> Result<Option<u64>> {
    Ok(credentials::stored_values()?
        .get(&store.into().used_key(name))
        .and_then(|value| value.trim().parse::<u64>().ok()))
}

/// Record that a turn just spent this login, so the next turn prefers another.
pub fn mark_used(store: impl Into<KeyStore>, name: &str) -> Result<()> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or_default();
    let stamp = now.to_string();
    credentials::store_values(&[(&store.into().used_key(name), stamp.as_str())])?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn scratch_home() -> (tokio::sync::MutexGuard<'static, ()>, tempfile::TempDir) {
        let guard = crate::env::test_env_lock().lock().await;
        let home = tempfile::tempdir().unwrap();
        // SAFETY: serialised by the shared test environment lock.
        unsafe {
            std::env::set_var("BORG_HOME", home.path());
            std::env::remove_var("OPENCODE_GO_API_KEY");
        }
        (guard, home)
    }

    // A custom endpoint's keys follow it: the selected one is what turns
    // spend, a rename keeps them, and removing the endpoint forgets them.
    #[tokio::test]
    async fn custom_provider_keys_follow_the_provider() {
        let (_guard, _home) = scratch_home().await;
        credentials::store_named_provider_api_key("work", "sk-legacy").unwrap();
        let store = || KeyStore::Custom("work".to_string());
        register(store(), "second", "sk-second").unwrap();
        assert_eq!(custom_provider_key("work").as_deref(), Some("sk-second"));
        activate(store(), "default").unwrap();
        assert_eq!(custom_provider_key("work").as_deref(), Some("sk-legacy"));
        rename_custom("work", "office").unwrap();
        let moved: Vec<_> = list(KeyStore::Custom("office".to_string()))
            .unwrap()
            .into_iter()
            .map(|account| (account.name, account.active))
            .collect();
        assert_eq!(
            moved,
            [("default".to_string(), true), ("second".to_string(), false)]
        );
        forget_custom("office").unwrap();
        assert!(
            list(KeyStore::Custom("office".to_string()))
                .unwrap()
                .is_empty()
        );
    }

    // The switching policy is stored under the logins' own prefix; it must
    // never show up as a login that can be selected or removed.
    #[tokio::test]
    async fn the_switching_policy_is_not_a_login() {
        let (_guard, _home) = scratch_home().await;
        register(KeyLane::OpenCodeGo, "work", "sk-work").unwrap();
        crate::subscriptions::set_strategy(
            crate::subscriptions::Lane::OpenCode,
            crate::subscriptions::Strategy::Manual,
        )
        .unwrap();
        let names: Vec<_> = list(KeyLane::OpenCodeGo)
            .unwrap()
            .into_iter()
            .map(|account| account.name)
            .collect();
        assert_eq!(names, ["work"]);
    }

    // A key connected before named accounts must stay selected and reachable
    // once a second subscription is added.
    #[tokio::test]
    async fn naming_a_second_key_adopts_the_first_one() {
        let (_guard, _home) = scratch_home().await;
        credentials::store_api_key(ApiKeyCredential::OpenCodeGo, "sk-legacy").unwrap();

        let accounts = list(KeyLane::OpenCodeGo).unwrap();
        // The single legacy key is what turns spend, so it is the active
        // login rather than an unmarked row.
        assert_eq!(
            accounts
                .iter()
                .map(|account| (account.name.as_str(), account.active))
                .collect::<Vec<_>>(),
            [("default", true)]
        );
        assert_eq!(
            credentials::api_key(ApiKeyCredential::OpenCodeGo).unwrap(),
            "sk-legacy"
        );

        register(KeyLane::OpenCodeGo, "work", "sk-work").unwrap();
        assert_eq!(
            credentials::api_key(ApiKeyCredential::OpenCodeGo).unwrap(),
            "sk-work"
        );
        assert_eq!(
            list(KeyLane::OpenCodeGo)
                .unwrap()
                .iter()
                .map(|account| (account.name.as_str(), account.active))
                .collect::<Vec<_>>(),
            [("default", false), ("work", true)]
        );

        activate(KeyLane::OpenCodeGo, "default").unwrap();
        assert_eq!(
            credentials::api_key(ApiKeyCredential::OpenCodeGo).unwrap(),
            "sk-legacy"
        );

        remove(KeyLane::OpenCodeGo, "default").unwrap();
        assert_eq!(
            credentials::api_key(ApiKeyCredential::OpenCodeGo).unwrap(),
            "sk-work"
        );
        // The last named login cannot be dropped in favour of nothing: a lane
        // always keeps a key a turn can spend.
        remove(KeyLane::OpenCodeGo, "work").unwrap_err();
        assert_eq!(
            credentials::api_key(ApiKeyCredential::OpenCodeGo).unwrap(),
            "sk-work"
        );
    }
}
