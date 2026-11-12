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

/// A built-in provider whose logins Borg keeps: subscription logins, API
/// keys, or both. Every lane offers the same verbs -- list, add, select, remove
/// -- whichever registry stores its logins.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lane {
    Claude,
    Codex,
    OpenCode,
    Glm,
    Kimi,
    Qwen,
    OpenRouter,
    Vercel,
}

/// Whether a login spends a subscription's included allowance or is billed per
/// use through an API key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccountKind {
    Subscription,
    ApiKey,
}

impl Lane {
    pub const ALL: [Lane; 8] = [
        Lane::Claude,
        Lane::Codex,
        Lane::OpenCode,
        Lane::Glm,
        Lane::Kimi,
        Lane::Qwen,
        Lane::OpenRouter,
        Lane::Vercel,
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
            Lane::OpenRouter => "openrouter",
            Lane::Vercel => "vercel",
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
            Lane::OpenRouter => "OpenRouter",
            Lane::Vercel => "Vercel AI Gateway",
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
            "openrouter" => Some(Lane::OpenRouter),
            "vercel" => Some(Lane::Vercel),
            _ => None,
        }
    }

    /// Whether this lane has subscription logins at all, and so quota and a
    /// switching policy.
    pub fn has_subscriptions(self) -> bool {
        self.auth_provider().is_some() || self.key_lane().is_some()
    }

    /// The OAuth login registry behind this lane, if it keeps authorities as
    /// paths rather than keys.
    pub fn auth_provider(self) -> Option<ProviderAuthProvider> {
        match self {
            Lane::Claude => Some(ProviderAuthProvider::Claude),
            Lane::Codex => Some(ProviderAuthProvider::Openai),
            _ => None,
        }
    }

    /// The key registry behind this lane's subscriptions, if they are billed
    /// by a key.
    pub fn key_lane(self) -> Option<KeyLane> {
        match self {
            Lane::OpenCode => Some(KeyLane::OpenCodeGo),
            Lane::Glm => Some(KeyLane::Glm),
            Lane::Kimi => Some(KeyLane::Kimi),
            Lane::Qwen => Some(KeyLane::Qwen),
            _ => None,
        }
    }

    /// The registry of this lane's pay-per-use API keys.
    pub fn api_key_lane(self) -> Option<KeyLane> {
        match self {
            Lane::Claude => Some(KeyLane::Anthropic),
            Lane::Codex => Some(KeyLane::OpenAi),
            Lane::OpenRouter => Some(KeyLane::OpenRouter),
            Lane::Vercel => Some(KeyLane::Vercel),
            _ => None,
        }
    }

    /// The registry that stores logins of `kind` on this lane as keys.
    pub fn keys(self, kind: AccountKind) -> Option<KeyLane> {
        match kind {
            AccountKind::Subscription => self.key_lane(),
            AccountKind::ApiKey => self.api_key_lane(),
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
            Lane::OpenRouter => "openrouter",
            Lane::Vercel => "vercel",
        }
    }

    /// On a lane with both kinds, which kind turns bill now.
    fn billing(self) -> AccountKind {
        let api = match self {
            Lane::Claude => crate::credentials::claude_uses_api_key(),
            Lane::Codex => crate::credentials::openai_uses_api_key(),
            _ => !self.has_subscriptions(),
        };
        if api {
            AccountKind::ApiKey
        } else {
            AccountKind::Subscription
        }
    }

    fn set_billing(self, kind: AccountKind) -> Result<()> {
        let api = kind == AccountKind::ApiKey;
        match self {
            Lane::Claude => crate::credentials::set_claude_uses_api_key(api).map(|_| ()),
            Lane::Codex => crate::credentials::set_openai_auth_mode(if api {
                crate::credentials::OpenAiAuthMode::ApiKey
            } else {
                crate::credentials::OpenAiAuthMode::Subscription
            })
            .map(|_| ()),
            _ => Ok(()),
        }
    }
}

/// One connected login on a lane.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Account {
    pub lane: Lane,
    pub kind: AccountKind,
    pub name: String,
    /// Whether the next turn on this lane spends this login.
    pub active: bool,
    /// The authority path of an OAuth login, which is also what a per-login
    /// quota probe needs. `None` for a key.
    pub path: Option<std::path::PathBuf>,
}

fn strategy_key(lane: Lane) -> String {
    format!(
        "{}_account_{}",
        lane.id(),
        crate::subscription_accounts::STRATEGY
    )
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

fn key_accounts_of(lane: Lane, kind: AccountKind, key_lane: KeyLane) -> Vec<Account> {
    key_accounts::list(key_lane)
        .unwrap_or_default()
        .into_iter()
        .map(|account| Account {
            lane,
            kind,
            name: account.name,
            active: account.active,
            path: None,
        })
        .collect()
}

fn oauth_accounts(lane: Lane, accounts: Vec<crate::SubscriptionAccount>) -> Vec<Account> {
    accounts
        .into_iter()
        .map(|account| Account {
            lane,
            kind: AccountKind::Subscription,
            name: account.name,
            active: account.active,
            path: Some(account.path),
        })
        .collect()
}

/// Every subscription login for `lane`, from whichever registry stores it.
/// This is what quota probes and automatic switching work over; API keys are
/// never part of it.
pub async fn accounts(lane: Lane) -> Result<Vec<Account>> {
    let mut accounts = match (lane.auth_provider(), lane.key_lane()) {
        (Some(provider), _) => {
            oauth_accounts(lane, crate::subscription_accounts::list(provider).await?)
        }
        (None, Some(key_lane)) => key_accounts_of(lane, AccountKind::Subscription, key_lane),
        (None, None) => Vec::new(),
    };
    accounts.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(accounts)
}

/// Every login on `lane` -- subscriptions first, then API keys -- read
/// synchronously from the credential store, with only the login the next turn
/// spends marked active. This is what a settings screen draws every frame: no
/// login discovery, no provider process.
pub fn stored_accounts(lane: Lane) -> Vec<Account> {
    let mut subscriptions = match (lane.auth_provider(), lane.key_lane()) {
        (Some(provider), _) => oauth_accounts(
            lane,
            crate::subscription_accounts::stored(provider).unwrap_or_default(),
        ),
        (None, Some(key_lane)) => key_accounts_of(lane, AccountKind::Subscription, key_lane),
        (None, None) => Vec::new(),
    };
    let mut keys = lane
        .api_key_lane()
        .map(|key_lane| key_accounts_of(lane, AccountKind::ApiKey, key_lane))
        .unwrap_or_default();
    subscriptions.sort_by(|left, right| left.name.cmp(&right.name));
    keys.sort_by(|left, right| left.name.cmp(&right.name));
    let billing = lane.billing();
    subscriptions
        .into_iter()
        .chain(keys)
        .map(|mut account| {
            account.active &= account.kind == billing;
            account
        })
        .collect()
}

/// Make `name` the login the next turn of this lane spends. On a lane with
/// both kinds this also chooses how turns are billed.
pub async fn activate(lane: Lane, kind: AccountKind, name: &str) -> Result<()> {
    match (kind, lane.auth_provider(), lane.keys(kind)) {
        (_, _, Some(key_lane)) => key_accounts::activate(key_lane, name).map(|_| ())?,
        (AccountKind::Subscription, Some(provider), None) => {
            crate::subscription_accounts::activate(provider, name)
                .await
                .map(|_| ())?
        }
        _ => bail!("{} keeps no such login", lane.label()),
    }
    lane.set_billing(kind)
}

/// Connect a named key. It is selected among the lane's keys of that kind, but
/// billing only moves to it when the user selects it, so adding a pay-per-use
/// key never silently stops spending a subscription.
pub fn add_key(lane: Lane, kind: AccountKind, name: &str, key: &str) -> Result<()> {
    let key_lane = lane.keys(kind).with_context(|| {
        format!(
            "{} is connected by signing in, not with a key",
            lane.label()
        )
    })?;
    key_accounts::register(key_lane, name, key).map(|_| ())
}

/// Disconnect a named key. An OAuth login owns a directory of rotating tokens,
/// so it is removed by signing that login out, not here.
pub fn remove(lane: Lane, kind: AccountKind, name: &str) -> Result<()> {
    let key_lane = lane.keys(kind).with_context(|| {
        format!(
            "{} logins own rotating credentials; sign in to another login instead of removing one",
            lane.label()
        )
    })?;
    key_accounts::remove(key_lane, name)
}
