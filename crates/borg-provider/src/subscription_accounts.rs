//! Several subscription logins for one provider, connected side by side.
//!
//! Each `borg login` authority is a persistent path (a Claude config directory
//! or a ChatGPT `auth.json`). The registry keeps a name -> path map in the
//! credential store and a selected account per provider that *is* the key the
//! provider adapter already reads, so switching a name changes which
//! subscription the next turn bills without any adapter-side state.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};

use crate::ProviderAuthProvider;
use crate::credentials;

const LEGACY_ACCOUNT: &str = "default";

/// One connected subscription login.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionAccount {
    pub name: String,
    /// Claude: the config directory holding `.credentials.json`.
    /// ChatGPT: the `auth.json` file itself.
    pub path: PathBuf,
    pub active: bool,
}

pub fn validate_name(name: &str) -> Result<&str> {
    let valid = !name.is_empty()
        && name.len() <= 40
        && name
            .chars()
            .next()
            .is_some_and(|first| first.is_ascii_alphanumeric())
        && name.chars().all(|character| {
            character.is_ascii_alphanumeric() || character == '-' || character == '_'
        });
    ensure!(
        valid,
        "subscription account name must be 1-40 ASCII letters, digits, '-' or '_'"
    );
    Ok(name)
}

fn accounts_dir() -> Result<PathBuf> {
    Ok(credentials::borg_dir()
        .context("cannot locate a home directory for subscription accounts")?
        .join("accounts"))
}

/// The authority path a new named login owns. Nothing is created here.
pub fn new_account_path(provider: ProviderAuthProvider, name: &str) -> Result<PathBuf> {
    validate_name(name)?;
    let directory = accounts_dir()?.join(provider.as_str()).join(name);
    Ok(match provider {
        ProviderAuthProvider::Claude => directory,
        ProviderAuthProvider::Openai => directory.join("auth.json"),
    })
}

fn account_key(provider: ProviderAuthProvider, name: &str) -> String {
    format!("{}_account_{name}", provider.as_str())
}

fn selection_key(provider: ProviderAuthProvider) -> &'static str {
    match provider {
        ProviderAuthProvider::Claude => credentials::CLAUDE_CONFIG_DIR_KEY,
        ProviderAuthProvider::Openai => credentials::OPENAI_AUTH_FILE_KEY,
    }
}

fn env_override(provider: ProviderAuthProvider) -> Option<PathBuf> {
    match provider {
        ProviderAuthProvider::Claude => credentials::claude_config_dir_env_override(),
        ProviderAuthProvider::Openai => credentials::openai_auth_file_env_override(),
    }
}

/// The authority the provider adapter reads right now.
fn selection(provider: ProviderAuthProvider) -> Result<Option<PathBuf>> {
    match provider {
        ProviderAuthProvider::Claude => credentials::claude_subscription_config_dir(),
        ProviderAuthProvider::Openai => credentials::openai_subscription_auth_file(),
    }
}

fn same_path(left: &Path, right: &Path) -> bool {
    match (left.canonicalize(), right.canonicalize()) {
        (Ok(left), Ok(right)) => left == right,
        _ => left == right,
    }
}

/// Every connected login for `provider`, with the one whose authority the
/// provider adapter reads right now marked active. A host that never named an
/// account keeps reporting its single existing login as `default`, so a user
/// who connected before this registry existed does not have to sign in again.
pub async fn list(provider: ProviderAuthProvider) -> Result<Vec<SubscriptionAccount>> {
    let values = credentials::stored_values()?;
    let prefix = format!("{}_account_", provider.as_str());
    let mut accounts = values
        .iter()
        .filter_map(|(key, value)| {
            key.strip_prefix(&prefix).map(|name| SubscriptionAccount {
                name: name.to_owned(),
                path: PathBuf::from(value),
                active: false,
            })
        })
        .collect::<Vec<_>>();
    // The effective authority is the single source of truth: it is exactly
    // what the provider adapter reads, so nothing else may claim to be active.
    let effective = match selection(provider)? {
        Some(path) => Some(path),
        None => resolved_default(provider).await,
    };
    if accounts.is_empty()
        && let Some(path) = &effective
    {
        accounts.push(SubscriptionAccount {
            name: LEGACY_ACCOUNT.to_owned(),
            path: path.clone(),
            active: true,
        });
    }
    for account in &mut accounts {
        account.active = effective
            .as_ref()
            .is_some_and(|path| same_path(path, &account.path));
    }
    accounts.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(accounts)
}

/// The authority the provider adapter resolves when no selection is stored,
/// but only when a login is actually present there.
async fn resolved_default(provider: ProviderAuthProvider) -> Option<PathBuf> {
    match provider {
        ProviderAuthProvider::Claude => {
            #[cfg(feature = "claude")]
            {
                let authority = crate::claude_subscription::AuthAuthority::discover().ok()?;
                crate::claude_subscription::credentials_present(&authority)
                    .await
                    .ok()?
                    .then(|| authority.config_dir().to_path_buf())
            }
            #[cfg(not(feature = "claude"))]
            {
                None
            }
        }
        ProviderAuthProvider::Openai => {
            let path = crate::openai_subscription::auth_path().ok()?;
            path.is_file().then_some(path)
        }
    }
}

/// Keep the login that is selected now under the name `default` the first time
/// a named account is added, so connecting a second subscription never leaves
/// the first one invisible and unswitchable.
async fn adopt_selected_login(provider: ProviderAuthProvider) -> Result<()> {
    let values = credentials::stored_values()?;
    let prefix = format!("{}_account_", provider.as_str());
    if values.keys().any(|key| key.starts_with(&prefix)) {
        return Ok(());
    }
    // The selected authority, or the default one the adapter reads when no
    // selection is stored (a host that connected before named accounts).
    let selected = match selection(provider)? {
        Some(path) => Some(path),
        None => resolved_default(provider).await,
    };
    let Some(selected) = selected else {
        return Ok(());
    };
    credentials::store_values(&[(
        &account_key(provider, LEGACY_ACCOUNT),
        selection_key_value(&selected)?,
    )])
    .map(|_| ())
}

fn selection_key_value(path: &Path) -> Result<&str> {
    path.to_str()
        .context("subscription account path must be UTF-8")
}

fn write_selection(provider: ProviderAuthProvider, name: &str, path: &Path) -> Result<()> {
    if let Some(overridden) = env_override(provider) {
        let variable = match provider {
            ProviderAuthProvider::Claude => "BORG_CLAUDE_CONFIG_DIR",
            ProviderAuthProvider::Openai => "BORG_OPENAI_AUTH_FILE",
        };
        ensure!(
            same_path(&overridden, path),
            "{variable} selects the {} authority; unset it to choose between named accounts",
            provider.as_str()
        );
    }
    let path = selection_key_value(path)?;
    credentials::store_values(&[
        (&account_key(provider, name), path),
        (selection_key(provider), path),
    ])
    .map(|_| ())
}

/// Name and select a login that was just connected. The selection is written
/// only after the sign-in succeeded, so a failed login leaves the working
/// account in place.
pub async fn register(
    provider: ProviderAuthProvider,
    name: &str,
    path: &Path,
) -> Result<SubscriptionAccount> {
    validate_name(name)?;
    ensure!(
        path.is_absolute(),
        "subscription account path must be absolute"
    );
    adopt_selected_login(provider).await?;
    write_selection(provider, name, path)?;
    Ok(SubscriptionAccount {
        name: name.to_owned(),
        path: path.to_owned(),
        active: true,
    })
}

/// Point `provider` at another connected login.
pub async fn activate(provider: ProviderAuthProvider, name: &str) -> Result<SubscriptionAccount> {
    validate_name(name)?;
    let account = list(provider)
        .await?
        .into_iter()
        .find(|account| account.name == name)
        .with_context(|| {
            format!(
                "no connected {} subscription named {name}; connect it with `borg login {} --account {name}`",
                provider.as_str(),
                match provider {
                    ProviderAuthProvider::Claude => "claude",
                    ProviderAuthProvider::Openai => "codex",
                }
            )
        })?;
    adopt_selected_login(provider).await?;
    write_selection(provider, name, &account.path)?;
    Ok(SubscriptionAccount {
        active: true,
        ..account
    })
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
            std::env::remove_var("BORG_CLAUDE_CONFIG_DIR");
            std::env::remove_var("BORG_OPENAI_AUTH_FILE");
        }
        (guard, home)
    }

    // Switching a name must move the exact authority the provider adapter
    // reads, and a login from before named accounts must stay reachable.
    #[tokio::test]
    async fn selecting_an_account_moves_the_authority_and_keeps_the_old_login() {
        let (_guard, home) = scratch_home().await;
        let legacy = home.path().join("claude");
        std::fs::create_dir_all(&legacy).unwrap();
        std::fs::write(legacy.join(".credentials.json"), b"{}").unwrap();
        // What an older Borg wrote when the user connected this login.
        credentials::set_claude_subscription_config_dir(&legacy).unwrap();

        let accounts = list(ProviderAuthProvider::Claude).await.unwrap();
        assert_eq!(accounts.len(), 1);
        assert_eq!(accounts[0].name, "default");
        assert!(accounts[0].active);

        let work = new_account_path(ProviderAuthProvider::Claude, "work").unwrap();
        register(ProviderAuthProvider::Claude, "work", &work)
            .await
            .unwrap();
        assert_eq!(
            credentials::claude_subscription_config_dir().unwrap(),
            Some(work.clone())
        );
        let accounts = list(ProviderAuthProvider::Claude).await.unwrap();
        assert_eq!(
            accounts
                .iter()
                .map(|account| (account.name.as_str(), account.active))
                .collect::<Vec<_>>(),
            [("default", false), ("work", true)]
        );

        let restored = activate(ProviderAuthProvider::Claude, "default")
            .await
            .unwrap();
        assert_eq!(restored.path, legacy);
        assert_eq!(
            credentials::claude_subscription_config_dir().unwrap(),
            Some(legacy)
        );
        assert!(
            list(ProviderAuthProvider::Claude)
                .await
                .unwrap()
                .iter()
                .find(|account| account.name == "default")
                .unwrap()
                .active
        );
    }

    #[test]
    fn account_names_cannot_escape_the_accounts_directory() {
        for rejected in [
            "",
            "../escape",
            ".hidden",
            "a/b",
            "a b",
            "trailing/",
            &"x".repeat(41),
        ] {
            assert!(
                validate_name(rejected).is_err(),
                "{rejected:?} was accepted"
            );
        }
        assert!(
            new_account_path(ProviderAuthProvider::Openai, "work-2_x")
                .unwrap()
                .ends_with("accounts/openai/work-2_x/auth.json")
        );
    }

    // An operator override must fail loudly instead of being silently ignored
    // while an agent believes it switched accounts.
    #[tokio::test]
    async fn an_environment_override_refuses_to_be_silently_replaced() {
        let (_guard, home) = scratch_home().await;
        let overridden = home.path().join("operator-claude");
        // SAFETY: serialised by the shared test environment lock.
        unsafe { std::env::set_var("BORG_CLAUDE_CONFIG_DIR", &overridden) };
        let work = new_account_path(ProviderAuthProvider::Claude, "work").unwrap();
        let error = register(ProviderAuthProvider::Claude, "work", &work)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("BORG_CLAUDE_CONFIG_DIR"),
            "{error}"
        );
        unsafe { std::env::remove_var("BORG_CLAUDE_CONFIG_DIR") };
    }
}
