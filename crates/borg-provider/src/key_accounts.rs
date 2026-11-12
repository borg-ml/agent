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

/// One subscription key connected under a name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyAccount {
    pub lane: KeyLane,
    pub name: String,
    pub active: bool,
}

fn account_key(lane: KeyLane, name: &str) -> String {
    format!("{}_account_{name}", lane.id())
}

fn selection_key(lane: KeyLane) -> String {
    format!("{}_account", lane.id())
}

fn used_key(lane: KeyLane, name: &str) -> String {
    format!("{}_account_used_{name}", lane.id())
}

/// Every named subscription key for `lane`, with the one whose key the next
/// turn spends marked active. A lane that only has the single legacy key is
/// reported as one `default` login, so a user who connected before named
/// accounts never has to re-enter a key.
pub fn list(lane: KeyLane) -> Result<Vec<KeyAccount>> {
    let mut accounts = named(lane)?;
    if accounts.is_empty() && credentials::stored_api_key(lane.credential()).is_some() {
        accounts.push(KeyAccount {
            lane,
            name: "default".to_owned(),
            active: true,
        });
    }
    // With no selection stored, the single legacy key is what turns spend, so
    // `default` is the active login rather than an unmarked row.
    let selected = selected_name(lane)?;
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
fn named(lane: KeyLane) -> Result<Vec<KeyAccount>> {
    let values = credentials::stored_values()?;
    let prefix = format!("{}_account_", lane.id());
    Ok(values
        .keys()
        .filter_map(|key| {
            let name = key.strip_prefix(&prefix)?;
            // `{lane}_account_used_{name}` and the lane's
            // `{lane}_account_strategy` share the prefix; they are bookkeeping,
            // not logins.
            (!name.starts_with("used_") && name != crate::subscription_accounts::STRATEGY).then(
                || KeyAccount {
                    lane,
                    name: name.to_owned(),
                    active: false,
                },
            )
        })
        .collect())
}

/// The selected login name, if one was ever selected on this host.
pub fn selected_name(lane: KeyLane) -> Result<Option<String>> {
    Ok(credentials::stored_values()?
        .get(&selection_key(lane))
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty()))
}

/// The key the next turn spends: the selected login's key. Resolution order is
/// environment, selected account, legacy single key, so an operator's variable
/// is never shadowed and an unnamed lane keeps working.
pub fn selected_key(lane: KeyLane) -> Option<String> {
    let values = credentials::stored_values().ok()?;
    let name = values.get(&selection_key(lane))?.trim();
    let key = values.get(&account_key(lane, name))?;
    let key = key.trim();
    (!key.is_empty()).then(|| key.to_string())
}

/// The key stored for one named login.
///
/// A usage probe is per subscription: several named logins each report their
/// own allowance, so the selected key is not enough. `default` falls back to the
/// lane's single legacy key, which is the login [`list`] synthesizes for a lane
/// that was connected before named accounts existed.
pub fn key(lane: KeyLane, name: &str) -> Option<String> {
    let values = credentials::stored_values().ok()?;
    if let Some(key) = values
        .get(&account_key(lane, name))
        .map(|key| key.trim())
        .filter(|key| !key.is_empty())
    {
        return Some(key.to_string());
    }
    (name == "default")
        .then(|| credentials::stored_api_key(lane.credential()))
        .flatten()
}

/// The selected key for any credential backed by a key-billed subscription.
pub fn selected_key_for_credential(credential: ApiKeyCredential) -> Option<String> {
    KeyLane::for_credential(credential).and_then(selected_key)
}

/// Connect `name` with `key` and select it. The selection is written in the
/// same locked update as the key, so no turn can observe a name without its
/// key.
pub fn register(lane: KeyLane, name: &str, key: &str) -> Result<KeyAccount> {
    crate::subscription_accounts::validate_name(name)?;
    let key = key.trim();
    ensure!(!key.is_empty(), "the subscription key cannot be empty");
    // A lane connected before named accounts keeps its key under `default`, so
    // adding a second subscription never hides the first.
    if named(lane)?.is_empty()
        && let Some(legacy) = credentials::stored_api_key(lane.credential())
    {
        credentials::store_values(&[(&account_key(lane, "default"), &legacy)])?;
    }
    credentials::store_values(&[
        (&account_key(lane, name), key),
        (&selection_key(lane), name),
    ])?;
    Ok(KeyAccount {
        lane,
        name: name.to_owned(),
        active: true,
    })
}

/// Point `lane` at another connected subscription key.
pub fn activate(lane: KeyLane, name: &str) -> Result<KeyAccount> {
    crate::subscription_accounts::validate_name(name)?;
    let account = list(lane)?
        .into_iter()
        .find(|account| account.name == name)
        .with_context(|| format!("no connected {} subscription named {name}", lane.label()))?;
    if account.active {
        return Ok(account);
    }
    credentials::store_values(&[(&selection_key(lane), name)])?;
    mark_used(lane, name)?;
    Ok(KeyAccount {
        lane,
        name: account.name,
        active: true,
    })
}

/// Disconnect a named subscription key. Removing the selected login selects
/// another connected one, or clears the selection so the legacy key applies.
pub fn remove(lane: KeyLane, name: &str) -> Result<()> {
    crate::subscription_accounts::validate_name(name)?;
    let accounts = list(lane)?;
    let removed = accounts
        .iter()
        .find(|account| account.name == name)
        .with_context(|| format!("no connected {} subscription named {name}", lane.label()))?;
    if accounts.len() == 1 && !accounts.iter().any(|account| account.name == "default") {
        bail!(
            "cannot remove the only connected {} subscription",
            lane.label()
        );
    }
    credentials::clear_values(&[&account_key(lane, name), &used_key(lane, name)])?;
    if removed.active {
        match accounts.iter().find(|account| account.name != name) {
            Some(next) => {
                credentials::store_values(&[(&selection_key(lane), &next.name)])?;
            }
            None => credentials::clear_values(&[&selection_key(lane)])?,
        }
    }
    Ok(())
}

/// When this login last served a turn, for balancing lanes whose provider
/// reports no quota.
pub fn last_used(lane: KeyLane, name: &str) -> Result<Option<u64>> {
    Ok(credentials::stored_values()?
        .get(&used_key(lane, name))
        .and_then(|value| value.trim().parse::<u64>().ok()))
}

/// Record that a turn just spent this login, so the next turn prefers another.
pub fn mark_used(lane: KeyLane, name: &str) -> Result<()> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or_default();
    let stamp = now.to_string();
    credentials::store_values(&[(&used_key(lane, name), stamp.as_str())])?;
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
