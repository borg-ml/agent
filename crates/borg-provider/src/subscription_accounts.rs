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

/// Characters a login name cannot contain, because the name is used as a
/// directory under `~/.borg/accounts/<provider>/`: the path separators, and
/// the set Windows reserves in file names.
const NAME_PATH_UNSAFE: &[char] = &['/', '\\', ':', '*', '?', '"', '<', '>', '|'];

pub fn validate_name(name: &str) -> Result<&str> {
    // A space is allowed inside a name so a login can be called "Profile 2";
    // it never appears at either end, where it would be invisible and would
    // make two names that read the same differ. Everything else printable is
    // allowed as long as the filesystem can carry it, because a login people
    // actually want to type is often an email such as "sasha@borg.ml". A name
    // still begins with a letter or digit, so "." and ".." stay impossible.
    let valid = !name.is_empty()
        && name.len() <= 40
        && name
            .chars()
            .next()
            .is_some_and(|first| first.is_ascii_alphanumeric())
        && name.chars().all(|character| {
            character == ' '
                || (character.is_ascii_graphic() && !NAME_PATH_UNSAFE.contains(&character))
        })
        && !name.ends_with(' ')
        && !name.ends_with('.')
        && !name.contains("  ");
    ensure!(
        valid,
        "login name must be 1-40 printable ASCII characters starting with a letter or digit; \
         path separators and the reserved \":*?<>|\" set are not allowed"
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

/// The lane's switching policy is stored as `{id}_account_strategy`, under
/// the same prefix as the logins; it is not one.
pub(crate) const STRATEGY: &str = "strategy";

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
            key.strip_prefix(&prefix)
                .filter(|name| *name != STRATEGY)
                .map(|name| SubscriptionAccount {
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

/// Every named login in the store, without consulting the filesystem. A
/// settings screen has to draw this on every frame, so it must not await a
/// login discovery; a host that never named an account reports its single
/// selected login as `default`, exactly as [`list`] does.
pub fn stored(provider: ProviderAuthProvider) -> Result<Vec<SubscriptionAccount>> {
    let values = credentials::stored_values()?;
    let prefix = format!("{}_account_", provider.as_str());
    let mut accounts = values
        .iter()
        .filter_map(|(key, value)| {
            key.strip_prefix(&prefix)
                .filter(|name| *name != STRATEGY)
                .map(|name| SubscriptionAccount {
                    name: name.to_owned(),
                    path: PathBuf::from(value),
                    active: false,
                })
        })
        .collect::<Vec<_>>();
    let selected = values.get(selection_key(provider)).map(PathBuf::from);
    if accounts.is_empty()
        && let Some(path) = &selected
    {
        accounts.push(SubscriptionAccount {
            name: LEGACY_ACCOUNT.to_owned(),
            path: path.clone(),
            active: true,
        });
    }
    for account in &mut accounts {
        account.active = selected
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
    if values
        .keys()
        .filter_map(|key| key.strip_prefix(&prefix))
        .any(|name| name != STRATEGY)
    {
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

/// Forget a connected login, and with it the credentials Borg stored in its own
/// accounts directory. A login that predates named accounts lives wherever the
/// provider keeps it (such as `~/.claude`), so removing it only forgets the
/// entry: those files belong to whatever wrote them.
pub fn remove(provider: ProviderAuthProvider, name: &str) -> Result<()> {
    validate_name(name)?;
    let accounts = stored(provider)?;
    let removed = accounts
        .iter()
        .find(|account| account.name == name)
        .with_context(|| format!("no connected {} login named {name}", provider.as_str()))?;
    // The lane always keeps one login to spend; removing the last one is a
    // sign-out, which the user does in the provider's own client.
    ensure!(
        accounts.len() > 1,
        "cannot remove the only connected {} login",
        provider.as_str()
    );
    ensure!(
        !(removed.active && env_override(provider).is_some()),
        "an environment override selects this {} login; unset it before removing one",
        provider.as_str()
    );
    credentials::clear_values(&[&account_key(provider, name)])?;
    if removed.active
        && let Some(next) = accounts.iter().find(|account| account.name != name)
    {
        write_selection(provider, &next.name, &next.path)?;
    }
    if let Some(directory) = owned_directory(provider, name, &removed.path) {
        match std::fs::remove_dir_all(&directory) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| format!("delete {}", directory.display()));
            }
        }
    }
    Ok(())
}

/// The directory Borg created for `name`, when `path` is the login it wrote
/// there. Anything else -- a legacy login, a login another tool owns -- is not
/// Borg's to delete.
fn owned_directory(provider: ProviderAuthProvider, name: &str, path: &Path) -> Option<PathBuf> {
    let owned = new_account_path(provider, name).ok()?;
    if !same_path(&owned, path) {
        return None;
    }
    match provider {
        ProviderAuthProvider::Claude => Some(owned),
        ProviderAuthProvider::Openai => owned.parent().map(Path::to_path_buf),
    }
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
            "a\\b",
            "a:b",
            "a*b",
            "trailing.",
            " leading",
            "trailing ",
            "double  space",
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
        // A login the user never named is called "Profile 2".
        assert!(
            new_account_path(ProviderAuthProvider::Claude, "Profile 2")
                .unwrap()
                .ends_with("accounts/claude/Profile 2")
        );
        // An email is a login name people actually want to type.
        assert!(
            new_account_path(ProviderAuthProvider::Openai, "sasha@borg.ml")
                .unwrap()
                .ends_with("accounts/openai/sasha@borg.ml/auth.json")
        );
    }

    // Removing the login turns spend has to leave the lane on another one, and
    // delete only the credentials Borg wrote itself.
    #[tokio::test]
    async fn removing_the_selected_login_selects_another_and_deletes_the_owned_files() {
        let (_guard, home) = scratch_home().await;
        let legacy = home.path().join("claude");
        std::fs::create_dir_all(&legacy).unwrap();
        std::fs::write(legacy.join(".credentials.json"), b"{}").unwrap();
        credentials::set_claude_subscription_config_dir(&legacy).unwrap();
        let work = new_account_path(ProviderAuthProvider::Claude, "work").unwrap();
        std::fs::create_dir_all(&work).unwrap();
        std::fs::write(work.join(".credentials.json"), b"{}").unwrap();
        register(ProviderAuthProvider::Claude, "work", &work)
            .await
            .unwrap();

        remove(ProviderAuthProvider::Claude, "work").unwrap();

        assert!(!work.exists(), "the removed login's files stayed behind");
        assert_eq!(
            credentials::claude_subscription_config_dir().unwrap(),
            Some(legacy.clone()),
            "removing the selected login must select the one that is left"
        );
        // The last login is never removed: that is a sign-out, not a switch.
        assert!(remove(ProviderAuthProvider::Claude, "default").is_err());
        assert!(legacy.exists(), "a login Borg did not create was deleted");
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
