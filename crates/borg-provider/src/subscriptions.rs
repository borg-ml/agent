//! One product concept over the two shapes a subscription login can have.
//!
//! Claude and ChatGPT logins own an authority path
//! ([`crate::subscription_accounts`]); OpenCode Go and the coding plans are
//! billed by a key ([`crate::key_accounts`]). Settings, the CLI and automatic
//! balancing all want the same three verbs — list, select, remove — and the
//! same per-lane switching policy, so this module owns those and the two
//! registries stay behind it.

use anyhow::{Context, Result, bail};

use crate::ProviderAuthProvider;
use crate::key_accounts::{self, KeyLane};

/// What happens when a provider has several connected subscriptions.
///
/// A turn only ever moves between logins of the same provider, and never into
/// API-key billing. On a lane that reports no quota at all, both automatic
/// strategies rotate to the login that has waited longest, because there is
/// nothing else to rank on.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Strategy {
    /// The selected login stays selected until the user changes it.
    Manual,
    /// Spend the login that has the most quota left, switching only when the
    /// selected one is exhausted or markedly behind.
    Headroom,
    /// Spend the login whose remaining quota expires first, so an allowance
    /// that would be lost at its reset is used before a later one. This drains
    /// a stack of subscriptions in the order it would otherwise waste them.
    #[default]
    Drain,
}

impl Strategy {
    pub const ALL: [Strategy; 3] = [Strategy::Manual, Strategy::Headroom, Strategy::Drain];

    pub fn id(self) -> &'static str {
        match self {
            Strategy::Manual => "manual",
            Strategy::Headroom => "headroom",
            Strategy::Drain => "drain",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Strategy::Manual => "Manual",
            Strategy::Headroom => "Most headroom",
            Strategy::Drain => "Soonest reset",
        }
    }

    /// Whether a turn may move between this provider's logins on its own.
    pub fn is_automatic(self) -> bool {
        self != Strategy::Manual
    }

    pub fn parse(value: &str) -> Option<Strategy> {
        match value.trim().to_ascii_lowercase().as_str() {
            "manual" | "manual-only" | "off" => Some(Strategy::Manual),
            "headroom" | "most-headroom" | "most" => Some(Strategy::Headroom),
            // `auto` predates the choice and now means the default strategy.
            "drain" | "soonest" | "soonest-reset" | "auto" | "automatic" | "on" | "balance" => {
                Some(Strategy::Drain)
            }
            _ => None,
        }
    }
}

/// A provider lane a user can connect one or more subscriptions to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lane {
    Claude,
    Codex,
    OpenCode,
    Glm,
    Kimi,
    Qwen,
}

impl Lane {
    pub const ALL: [Lane; 6] = [
        Lane::Claude,
        Lane::Codex,
        Lane::OpenCode,
        Lane::Glm,
        Lane::Kimi,
        Lane::Qwen,
    ];

    /// Storage id, shared with the credential store keys.
    pub fn id(self) -> &'static str {
        match self {
            Lane::Claude => "claude",
            Lane::Codex => "openai",
            Lane::OpenCode => "opencode",
            Lane::Glm => "glm",
            Lane::Kimi => "kimi",
            Lane::Qwen => "qwen",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Lane::Claude => "Claude",
            Lane::Codex => "ChatGPT",
            Lane::OpenCode => "OpenCode Go",
            Lane::Glm => "GLM Coding Plan",
            Lane::Kimi => "Kimi Code",
            Lane::Qwen => "Qwen Coding Plan",
        }
    }

    /// What the user types to name this lane in the CLI and in settings.
    pub fn parse(value: &str) -> Option<Lane> {
        match value.trim().to_ascii_lowercase().as_str() {
            "claude" | "anthropic" => Some(Lane::Claude),
            "codex" | "openai" | "chatgpt" => Some(Lane::Codex),
            "opencode" | "opencode-go" | "go" => Some(Lane::OpenCode),
            "glm" | "zai" => Some(Lane::Glm),
            "kimi" | "moonshot" => Some(Lane::Kimi),
            "qwen" | "alibaba" => Some(Lane::Qwen),
            _ => None,
        }
    }

    /// The OAuth login registry behind this lane, if it keeps authorities as
    /// paths rather than keys.
    pub fn auth_provider(self) -> Option<ProviderAuthProvider> {
        match self {
            Lane::Claude => Some(ProviderAuthProvider::Claude),
            Lane::Codex => Some(ProviderAuthProvider::Openai),
            Lane::OpenCode | Lane::Glm | Lane::Kimi | Lane::Qwen => None,
        }
    }

    /// The key registry behind this lane, if it is billed by a key.
    pub fn key_lane(self) -> Option<KeyLane> {
        match self {
            Lane::Claude | Lane::Codex => None,
            Lane::OpenCode => Some(KeyLane::OpenCodeGo),
            Lane::Glm => Some(KeyLane::Glm),
            Lane::Kimi => Some(KeyLane::Kimi),
            Lane::Qwen => Some(KeyLane::Qwen),
        }
    }

    /// How a new login is connected, so every surface can explain it.
    pub fn connect_hint(self) -> String {
        format!("borg login {} --account NAME", self.login_command())
    }

    fn login_command(self) -> &'static str {
        match self {
            Lane::Claude => "claude",
            Lane::Codex => "codex",
            Lane::OpenCode => "opencode",
            Lane::Glm => "glm",
            Lane::Kimi => "kimi",
            Lane::Qwen => "qwen",
        }
    }
}

/// One connected subscription login on a lane.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Account {
    pub lane: Lane,
    pub name: String,
    pub active: bool,
    /// The authority path of an OAuth login, which is also what a per-login
    /// quota probe needs. `None` for a key-billed plan.
    pub path: Option<std::path::PathBuf>,
}

fn strategy_key(lane: Lane) -> String {
    format!("{}_account_strategy", lane.id())
}

/// The switching policy for `lane`. A lane that never chose one follows
/// [`Strategy::Drain`]: with a single connected login nothing changes, and with
/// several the allowance that expires first is spent first.
pub fn strategy(lane: Lane) -> Strategy {
    crate::credentials::stored_values()
        .ok()
        .and_then(|values| values.get(&strategy_key(lane)).cloned())
        .and_then(|value| Strategy::parse(&value))
        .unwrap_or_default()
}

pub fn set_strategy(lane: Lane, strategy: Strategy) -> Result<()> {
    crate::credentials::store_values(&[(&strategy_key(lane), strategy.id())]).map(|_| ())
}

/// Every connected login for `lane`, from whichever registry stores it.
pub async fn accounts(lane: Lane) -> Result<Vec<Account>> {
    let mut accounts = match (lane.auth_provider(), lane.key_lane()) {
        (Some(provider), _) => crate::subscription_accounts::list(provider)
            .await?
            .into_iter()
            .map(|account| Account {
                lane,
                name: account.name,
                active: account.active,
                path: Some(account.path),
            })
            .collect(),
        (None, Some(key_lane)) => key_accounts::list(key_lane)?
            .into_iter()
            .map(|account| Account {
                lane,
                name: account.name,
                active: account.active,
                path: None,
            })
            .collect(),
        (None, None) => Vec::new(),
    };
    accounts.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(accounts)
}

/// [`accounts`], read synchronously from the credential store. This is what a
/// settings screen draws every frame: no login discovery, no provider process.
pub fn stored_accounts(lane: Lane) -> Vec<Account> {
    let accounts = match (lane.auth_provider(), lane.key_lane()) {
        (Some(provider), _) => crate::subscription_accounts::stored(provider)
            .unwrap_or_default()
            .into_iter()
            .map(|account| Account {
                lane,
                name: account.name,
                active: account.active,
                path: Some(account.path),
            })
            .collect(),
        (None, Some(key_lane)) => crate::key_accounts::list(key_lane)
            .unwrap_or_default()
            .into_iter()
            .map(|account| Account {
                lane,
                name: account.name,
                active: account.active,
                path: None,
            })
            .collect(),
        (None, None) => Vec::new(),
    };
    accounts
}

/// Make `name` the login the next turn of this lane spends.
pub async fn activate(lane: Lane, name: &str) -> Result<()> {
    match (lane.auth_provider(), lane.key_lane()) {
        (Some(provider), _) => crate::subscription_accounts::activate(provider, name)
            .await
            .map(|_| ()),
        (None, Some(key_lane)) => key_accounts::activate(key_lane, name).map(|_| ()),
        (None, None) => bail!("{} keeps no subscription logins", lane.label()),
    }
}

/// Connect a named key subscription. OAuth lanes are connected by signing in,
/// so they are refused here rather than silently doing nothing.
pub fn add_key(lane: Lane, name: &str, key: &str) -> Result<()> {
    let key_lane = lane.key_lane().with_context(|| {
        format!(
            "{} is connected by signing in, not with a key",
            lane.label()
        )
    })?;
    key_accounts::register(key_lane, name, key).map(|_| ())
}

/// Disconnect a named key subscription. An OAuth login owns a directory of
/// rotating tokens, so it is removed by signing that login out, not here.
pub fn remove(lane: Lane, name: &str) -> Result<()> {
    let key_lane = lane.key_lane().with_context(|| {
        format!(
            "{} logins own rotating credentials; sign in to another login instead of removing one",
            lane.label()
        )
    })?;
    key_accounts::remove(key_lane, name)
}
