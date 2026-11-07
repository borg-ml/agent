//! Provider subscription-usage probing, split out of `host` so the agent
//! runtime (session/subagents) can refresh capability usage without depending
//! on the transport/enrollment layer. Keeping it here also breaks the module
//! cycle that blocked extracting the runtime into its own crate.

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

use chrono::{DateTime, Utc};

use crate::{
    CodingProvider, ProviderAccount, ProviderAuthMethod, ProviderCapability, ProviderUsage,
    ProviderUsageAvailability, ProviderUsageWindow,
};

pub const PROVIDER_CAPABILITIES_CACHE_TTL: Duration = Duration::from_secs(5);

type ProviderUsageCache = HashMap<CodingProvider, (Instant, Option<ProviderUsage>)>;

pub static PROVIDER_USAGE_CACHE: OnceLock<Mutex<ProviderUsageCache>> = OnceLock::new();

/// How long one `claude auth status` login read is shared. Every session runs
/// a capability refresh loop every few seconds; without this, each loop
/// spawned its own heavy Claude CLI process, which on a host with dozens of
/// sessions kept several cores busy permanently.
pub const CLAUDE_LOGIN_STATUS_CACHE_TTL: Duration = Duration::from_secs(60);

static CLAUDE_LOGIN_STATUS_CACHE: OnceLock<Mutex<Option<(Instant, bool)>>> = OnceLock::new();

/// Process-wide, single-flight cached Claude login read. Holding the async
/// mutex across the probe makes concurrent refreshes wait for one process
/// instead of each spawning their own. A failed read is not cached, so the
/// caller keeps its last admitted state and the next refresh retries.
async fn cached_claude_subscription_status() -> Option<bool> {
    let cache = CLAUDE_LOGIN_STATUS_CACHE.get_or_init(|| Mutex::new(None));
    let mut slot = cache.lock().await;
    if let Some((read_at, authenticated)) = *slot
        && read_at.elapsed() < CLAUDE_LOGIN_STATUS_CACHE_TTL
    {
        return Some(authenticated);
    }
    let authenticated = borg_provider::provider::read_claude_subscription_status()
        .await
        .ok()?;
    *slot = Some((Instant::now(), authenticated));
    Some(authenticated)
}

/// Forget everything derived from a provider's login state. Call it after any
/// login completes: otherwise the next capability refresh still reads the
/// pre-login "not authenticated" answer, for up to the status TTL, and the
/// connection looks like it failed.
pub async fn clear_login_dependent_usage_caches() {
    if let Some(cache) = PROVIDER_USAGE_CACHE.get() {
        cache.lock().await.clear();
    }
    if let Some(cache) = CLAUDE_LOGIN_STATUS_CACHE.get() {
        *cache.lock().await = None;
    }
    if let Some(cache) = SESSION_CAPABILITY_REFRESH.get() {
        *cache.lock().await = None;
    }
}

/// How long one session's periodic capability refresh serves the others.
/// Every session refreshes every few seconds; done per session, a host with
/// a hundred idle agents re-read the same credential files dozens of times a
/// second.
const SESSION_CAPABILITY_REFRESH_TTL: Duration = Duration::from_secs(2);

type SessionCapabilityRefresh = Option<(Instant, Vec<ProviderCapability>, Vec<ProviderCapability>)>;

static SESSION_CAPABILITY_REFRESH: OnceLock<Mutex<SessionCapabilityRefresh>> = OnceLock::new();

/// The periodic session refresh, single-flight and shared by sessions that
/// pass the same capabilities. Explicit snapshots keep calling
/// [`refresh_provider_capability_usage`] directly.
pub async fn refresh_session_provider_capabilities(
    capabilities: &[ProviderCapability],
) -> Vec<ProviderCapability> {
    let cache = SESSION_CAPABILITY_REFRESH.get_or_init(|| Mutex::new(None));
    let mut slot = cache.lock().await;
    if let Some((refreshed_at, input, output)) = &*slot
        && refreshed_at.elapsed() < SESSION_CAPABILITY_REFRESH_TTL
        && input.as_slice() == capabilities
    {
        return output.clone();
    }
    let output = refresh_provider_capability_usage(capabilities).await;
    *slot = Some((Instant::now(), capabilities.to_vec(), output.clone()));
    output
}

pub async fn refresh_provider_capability_usage(
    capabilities: &[ProviderCapability],
) -> Vec<ProviderCapability> {
    let mut capabilities = capabilities.to_vec();
    for capability in &mut capabilities {
        if capability.provider == CodingProvider::Claude {
            // The billing choice can change after the startup snapshot.
            if borg_provider::credentials::claude_uses_api_key() {
                let key = borg_provider::credentials::api_key(
                    borg_provider::credentials::ApiKeyCredential::Anthropic,
                )
                .is_some();
                capability.billing = key.then_some(crate::BillingLane::ApiKey);
                capability.usage = None;
                capability.authenticated = key;
                capability
                    .auth_methods
                    .retain(|method| *method != ProviderAuthMethod::Subscription);
                continue;
            }
            if capability.billing == Some(crate::BillingLane::ApiKey) {
                capability.billing = None;
            }
        }
        if capability.provider == CodingProvider::Claude
            && capability.installed
            && !matches!(
                capability.billing,
                Some(crate::BillingLane::ApiKey | crate::BillingLane::Endpoint)
            )
        {
            // Login can change after this session's startup snapshot. A failed
            // read is not proof of logout; retain the last admitted state.
            if let Some(authenticated) = cached_claude_subscription_status().await {
                apply_claude_subscription_status(capability, authenticated);
            }
        }
        if capability.provider == CodingProvider::OpenCode
            && borg_provider::credentials::opencode_go_api_key().is_some()
        {
            capability.billing = Some(crate::BillingLane::Subscription);
            capability.authenticated = true;
            capability.can_spawn = capability.installed;
            if !capability
                .auth_methods
                .contains(&ProviderAuthMethod::Subscription)
            {
                capability
                    .auth_methods
                    .push(ProviderAuthMethod::Subscription);
            }
        }
        if capability.provider == CodingProvider::Codex {
            if borg_provider::credentials::openai_uses_api_key() {
                capability.billing = Some(crate::BillingLane::ApiKey);
                capability.usage = None;
                capability
                    .auth_methods
                    .retain(|method| *method != ProviderAuthMethod::Subscription);
                if borg_provider::credentials::openai_api_key().is_some() {
                    if !capability
                        .auth_methods
                        .contains(&ProviderAuthMethod::ApiKey)
                    {
                        capability.auth_methods.push(ProviderAuthMethod::ApiKey);
                    }
                    capability.authenticated = true;
                } else {
                    capability.auth_methods.clear();
                    capability.authenticated = false;
                }
            } else {
                let authenticated = borg_provider::provider::read_codex_subscription_status()
                    .await
                    .unwrap_or(
                        capability.authenticated
                            && capability.billing == Some(crate::BillingLane::Subscription),
                    );
                capability.billing = Some(crate::BillingLane::Subscription);
                capability.authenticated = authenticated;
                if authenticated
                    && !capability
                        .auth_methods
                        .contains(&ProviderAuthMethod::Subscription)
                {
                    capability
                        .auth_methods
                        .push(ProviderAuthMethod::Subscription);
                } else if !authenticated {
                    capability
                        .auth_methods
                        .retain(|method| *method != ProviderAuthMethod::Subscription);
                    capability.usage = None;
                }
            }
        }
    }
    let has_subscription = |provider| {
        capabilities.iter().any(|capability| {
            capability.provider == provider
                && capability
                    .auth_methods
                    .contains(&ProviderAuthMethod::Subscription)
        })
    };
    let codex = async {
        if has_subscription(CodingProvider::Codex) {
            probe_provider_usage(CodingProvider::Codex).await
        } else {
            None
        }
    };
    let claude = async {
        if has_subscription(CodingProvider::Claude) {
            probe_provider_usage(CodingProvider::Claude).await
        } else {
            None
        }
    };
    let (codex, claude) = tokio::join!(codex, claude);
    // Account rows are read after the provider probes above so Claude's usage
    // endpoint is asked once per interval rather than once per row, and so a
    // connected login is still described when the selected one is unusable.
    let codex_accounts = subscription_accounts(CodingProvider::Codex).await;
    let claude_accounts = subscription_accounts(CodingProvider::Claude).await;
    let mut capabilities: Vec<ProviderCapability> = capabilities
        .iter()
        .cloned()
        .map(|mut capability| {
            let usage = match capability.provider {
                CodingProvider::Codex => codex.clone(),
                CodingProvider::Claude => claude.clone(),
                _ => return capability,
            };
            if capability.billing == Some(crate::BillingLane::ApiKey) {
                capability.usage = None;
            } else if usage.is_some() {
                capability.usage = usage;
            }
            let accounts: &[ProviderAccount] = match capability.provider {
                CodingProvider::Codex => &codex_accounts,
                CodingProvider::Claude => &claude_accounts,
                _ => &[],
            };
            attach_account_usage(&mut capability, accounts);
            apply_spawn_admission(&mut capability);
            capability
        })
        .collect();
    // Key-billed plans (OpenCode Go, GLM, Kimi, Qwen) keep their logins as
    // named keys rather than authorities, so they are read here instead of
    // being probed per path. None of them reports quota, so these rows carry
    // the name and selection and no windows.
    for capability in &mut capabilities {
        if matches!(
            capability.provider,
            CodingProvider::OpenCode
                | CodingProvider::Glm
                | CodingProvider::Kimi
                | CodingProvider::Qwen
        ) && capability.billing != Some(crate::BillingLane::Endpoint)
        {
            let accounts = account_rows(capability.provider, false).await;
            attach_account_usage(capability, &accounts);
        }
    }
    capabilities
}

// Codex's reported allowance excludes subscription credit-backed capacity.
// Keep usage visible, but let the selected provider route decide whether an
// authenticated request can be funded; never select another billing lane here.
fn apply_spawn_admission(capability: &mut ProviderCapability) {
    let quota_blocks = capability.provider != CodingProvider::Codex
        && capability
            .usage
            .as_ref()
            .is_some_and(|usage| usage.availability == ProviderUsageAvailability::Exhausted)
        && !matches!(
            capability.billing,
            Some(crate::BillingLane::ApiKey | crate::BillingLane::Endpoint)
        );
    capability.can_spawn = capability.installed && capability.authenticated && !quota_blocks;
    if capability.provider == CodingProvider::Claude
        && capability.billing == Some(crate::BillingLane::Subscription)
        && borg_provider::provider::claude_native_account_verification_failed_recently()
            .unwrap_or(false)
    {
        hold_unverified_claude_transport(capability);
    }
}

fn hold_unverified_claude_transport(capability: &mut ProviderCapability) {
    if capability.provider == CodingProvider::Claude
        && capability.billing == Some(crate::BillingLane::Subscription)
    {
        capability.can_spawn = false;
        capability.auth_detail =
            Some("Claude login present; native model account verification unavailable".to_string());
    }
}

async fn probe_provider_usage(provider: CodingProvider) -> Option<ProviderUsage> {
    let cache = PROVIDER_USAGE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some((probed_at, usage)) = cache.lock().await.get(&provider)
        && probed_at.elapsed() < Duration::from_secs(60)
    {
        return usage.clone();
    }
    let usage = match provider {
        CodingProvider::Codex => borg_provider::provider::read_codex_account_rate_limits(None)
            .await
            .ok()
            .map(codex_provider_usage),
        CodingProvider::Claude => {
            borg_provider::provider::read_claude_account_rate_limits(None, false)
                .await
                .ok()
                .map(claude_provider_usage)
        }
        _ => None,
    };
    cache
        .lock()
        .await
        .insert(provider, (Instant::now(), usage.clone()));
    usage
}

/// Publish connected accounts on a subscription capability. A login that
/// answers keeps the provider admitted even when the selected login's own read
/// failed, because the agent can still see every account and switch to one
/// with capacity. API-key lanes own no subscription accounts.
fn attach_account_usage(capability: &mut ProviderCapability, accounts: &[ProviderAccount]) {
    if accounts.is_empty() || capability.billing == Some(crate::BillingLane::ApiKey) {
        return;
    }
    capability.authenticated |= accounts.iter().any(|account| account.usage.is_some());
    capability.account_strategy = subscription_lane(capability.provider)
        .map(|lane| borg_provider::subscriptions::strategy(lane).into());
    capability.accounts = accounts.to_vec();
}

/// The subscription lane a provider bills subscriptions against. API-key-only
/// providers have none, and a login lane switched to API-key billing reports
/// none either.
pub fn subscription_lane(provider: CodingProvider) -> Option<borg_provider::subscriptions::Lane> {
    use borg_provider::subscriptions::Lane;
    Some(match provider {
        CodingProvider::Claude if !borg_provider::credentials::claude_uses_api_key() => {
            Lane::Claude
        }
        CodingProvider::Codex if !borg_provider::credentials::openai_uses_api_key() => Lane::Codex,
        CodingProvider::OpenCode => Lane::OpenCode,
        CodingProvider::Glm => Lane::Glm,
        CodingProvider::Kimi => Lane::Kimi,
        CodingProvider::Qwen => Lane::Qwen,
        _ => return None,
    })
}

/// How long one account's usage read is shared by every capability refresh.
const ACCOUNT_USAGE_TTL: Duration = Duration::from_secs(60);

#[derive(Clone)]
struct AccountProbe {
    usage: Option<ProviderUsage>,
    detail: Option<String>,
}

type AccountUsageCache = HashMap<(CodingProvider, String), (Instant, AccountProbe)>;

static PROVIDER_ACCOUNT_USAGE_CACHE: OnceLock<Mutex<AccountUsageCache>> = OnceLock::new();

/// Connected subscriptions for `provider`, each with its own usage and reset
/// times. A selected API-key lane reports none: probing one there would
/// suggest a switch that does not exist, and switching lanes is never
/// implicit.
pub async fn subscription_accounts(provider: CodingProvider) -> Vec<ProviderAccount> {
    account_rows(provider, true).await
}

/// The same rows, but only from quota reads that already happened. A turn
/// must not wait on a provider round trip before it can start, so automatic
/// switching uses this warm view and simply keeps the current login when a
/// quota read has not completed yet.
pub async fn cached_subscription_accounts(provider: CodingProvider) -> Vec<ProviderAccount> {
    account_rows(provider, false).await
}

async fn account_rows(provider: CodingProvider, probe: bool) -> Vec<ProviderAccount> {
    let Some(lane) = subscription_lane(provider) else {
        return Vec::new();
    };
    let accounts = borg_provider::subscriptions::accounts(lane)
        .await
        .unwrap_or_default();
    let mut rows = Vec::with_capacity(accounts.len());
    for account in accounts {
        let probe = match &account.path {
            Some(path) => account_probe(provider, path, probe).await,
            None => AccountProbe {
                usage: None,
                detail: Some(
                    "this plan reports no quota; turns rotate between its logins".to_string(),
                ),
            },
        };
        rows.push(ProviderAccount {
            name: account.name,
            active: account.active,
            plan: probe.usage.as_ref().and_then(|usage| usage.plan.clone()),
            usage: probe.usage,
            detail: probe.detail,
        });
    }
    rows
}

async fn account_probe(
    provider: CodingProvider,
    path: &std::path::Path,
    probe: bool,
) -> AccountProbe {
    let cache = PROVIDER_ACCOUNT_USAGE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let key = (provider, path.display().to_string());
    if let Some((probed_at, probe)) = cache.lock().await.get(&key)
        && probed_at.elapsed() < ACCOUNT_USAGE_TTL
    {
        return probe.clone();
    }
    if !probe {
        return AccountProbe {
            usage: None,
            detail: Some("quota has not been read yet".to_string()),
        };
    }
    let probe = match provider {
        CodingProvider::Claude => {
            borg_provider::provider::read_claude_account_rate_limits(Some(path), false)
                .await
                .map(claude_provider_usage)
        }
        CodingProvider::Codex => {
            borg_provider::provider::read_codex_account_rate_limits(Some(path.to_path_buf()))
                .await
                .map(codex_provider_usage)
        }
        _ => {
            return AccountProbe {
                usage: None,
                detail: Some("this provider has no subscription accounts".to_string()),
            };
        }
    };
    let probe = match probe {
        Ok(usage) => AccountProbe {
            usage: Some(usage),
            detail: None,
        },
        Err(error) => AccountProbe {
            usage: None,
            detail: Some(bounded_usage_error(&error)),
        },
    };
    cache
        .lock()
        .await
        .insert(key, (Instant::now(), probe.clone()));
    probe
}

/// Provider errors can quote request context; only one bounded line reaches a
/// journaled capability snapshot.
fn bounded_usage_error(error: &anyhow::Error) -> String {
    let text = format!("{error:#}");
    let first = text.lines().next().unwrap_or_default().trim();
    first.chars().take(160).collect()
}

fn codex_provider_usage(limits: borg_provider::provider::CodexAccountRateLimits) -> ProviderUsage {
    let plan = limits.plan_type.clone();
    let windows = [limits.primary, limits.secondary]
        .into_iter()
        .flatten()
        .map(|window| ProviderUsageWindow {
            label: provider_usage_window_label(window.window_duration_mins),
            used_percent: window.used_percent,
            resets_at: window
                .resets_at
                .and_then(|timestamp| DateTime::<Utc>::from_timestamp(timestamp, 0)),
            global: true,
        })
        .collect::<Vec<_>>();
    let exhausted = windows.iter().any(|window| window.used_percent >= 100);
    ProviderUsage {
        availability: if exhausted {
            ProviderUsageAvailability::Exhausted
        } else {
            ProviderUsageAvailability::Available
        },
        windows,
        detail: exhausted.then(|| "Codex subscription usage is exhausted".to_string()),
        plan,
    }
}

fn claude_provider_usage(
    limits: borg_provider::provider::ClaudeAccountRateLimits,
) -> ProviderUsage {
    let plan = limits.subscription_type.clone();
    let exhausted = limits.rate_limits_available
        && limits
            .windows
            .iter()
            .any(|window| window.global && window.used_percent >= 100)
        && !limits.extra_usage_available;
    ProviderUsage {
        availability: if !limits.rate_limits_available {
            ProviderUsageAvailability::Unknown
        } else if exhausted {
            ProviderUsageAvailability::Exhausted
        } else {
            ProviderUsageAvailability::Available
        },
        windows: limits
            .windows
            .into_iter()
            .map(|window| ProviderUsageWindow {
                label: window.label,
                used_percent: window.used_percent,
                resets_at: window.resets_at,
                global: window.global,
            })
            .collect(),
        detail: if exhausted {
            Some("Claude subscription usage is exhausted".to_string())
        } else if limits.extra_usage_available {
            Some("Claude extra usage is available after plan limits".to_string())
        } else {
            None
        },
        plan,
    }
}

fn provider_usage_window_label(duration_mins: u64) -> String {
    match duration_mins {
        10_080 => "Weekly".to_string(),
        1_440 => "Daily".to_string(),
        300 => "5-hour".to_string(),
        60 => "Hourly".to_string(),
        mins if mins % 1_440 == 0 => format!("{}-day", mins / 1_440),
        mins if mins % 60 == 0 => format!("{}-hour", mins / 60),
        mins => format!("{mins}-minute"),
    }
}

/// Update only the subscription lane; never select a different paid route.
fn apply_claude_subscription_status(capability: &mut ProviderCapability, authenticated: bool) {
    if matches!(
        capability.billing,
        Some(crate::BillingLane::ApiKey | crate::BillingLane::Endpoint)
    ) {
        return;
    }
    capability.authenticated = authenticated;
    capability
        .auth_methods
        .retain(|method| *method != ProviderAuthMethod::Subscription);
    if authenticated {
        capability
            .auth_methods
            .push(ProviderAuthMethod::Subscription);
        capability.billing = Some(crate::BillingLane::Subscription);
        capability.auth_detail = Some("Claude subscription authenticated".to_string());
    } else {
        capability.billing = None;
        capability.auth_detail = None;
        capability.usage = None;
        capability.can_spawn = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A confirmed native startup refusal must not repeatedly admit zero-token
    // children merely because CLI login reports success; billing stays selected.
    #[test]
    fn unverified_claude_transport_holds_spawn_without_logout_or_billing_switch() {
        let mut capability = ProviderCapability {
            provider: CodingProvider::Claude,
            installed: true,
            version: None,
            authenticated: true,
            auth_detail: None,
            auth_methods: vec![ProviderAuthMethod::Subscription],
            can_spawn: true,
            usage: None,
            billing: Some(crate::BillingLane::Subscription),
            accounts: Vec::new(),
            account_strategy: None,
        };
        hold_unverified_claude_transport(&mut capability);
        assert!(!capability.can_spawn);
        assert!(capability.authenticated);
        assert_eq!(capability.billing, Some(crate::BillingLane::Subscription));
        assert_eq!(
            capability.auth_methods,
            vec![ProviderAuthMethod::Subscription]
        );
        capability.billing = Some(crate::BillingLane::ApiKey);
        capability.can_spawn = true;
        hold_unverified_claude_transport(&mut capability);
        assert!(capability.can_spawn);
        assert_eq!(capability.billing, Some(crate::BillingLane::ApiKey));
    }

    // Regression: an exhausted base allowance must not block credit-backed
    // Codex subscription attempts or silently switch them to API-key billing.
    #[test]
    fn exhausted_codex_allowance_does_not_veto_authenticated_credit_requests() {
        let mut capability = ProviderCapability {
            provider: CodingProvider::Codex,
            installed: true,
            version: None,
            authenticated: true,
            auth_detail: None,
            auth_methods: vec![ProviderAuthMethod::Subscription],
            can_spawn: false,
            usage: Some(ProviderUsage {
                availability: ProviderUsageAvailability::Exhausted,
                windows: Vec::new(),
                detail: Some("base allowance exhausted".into()),
                plan: None,
            }),
            billing: Some(crate::BillingLane::Subscription),
            accounts: Vec::new(),
            account_strategy: None,
        };
        apply_spawn_admission(&mut capability);
        assert!(capability.can_spawn);
        assert_eq!(capability.billing, Some(crate::BillingLane::Subscription));
        assert_eq!(
            capability.auth_methods,
            vec![ProviderAuthMethod::Subscription]
        );
        assert_eq!(
            capability.usage.as_ref().unwrap().availability,
            ProviderUsageAvailability::Exhausted
        );
        capability.authenticated = false;
        apply_spawn_admission(&mut capability);
        assert!(!capability.can_spawn);
        capability.authenticated = true;
        capability.installed = false;
        apply_spawn_admission(&mut capability);
        assert!(!capability.can_spawn);
        capability.installed = true;
        capability.provider = CodingProvider::Claude;
        apply_spawn_admission(&mut capability);
        assert!(!capability.can_spawn);
    }

    fn subscription_capability(provider: CodingProvider) -> ProviderCapability {
        ProviderCapability {
            provider,
            installed: true,
            version: None,
            authenticated: false,
            auth_detail: None,
            auth_methods: Vec::new(),
            can_spawn: false,
            usage: None,
            billing: Some(crate::BillingLane::Subscription),
            accounts: Vec::new(),
            account_strategy: None,
        }
    }

    fn account(name: &str, active: bool, used_percent: u8) -> ProviderAccount {
        ProviderAccount {
            name: name.to_string(),
            active,
            plan: None,
            usage: Some(ProviderUsage {
                availability: ProviderUsageAvailability::Available,
                windows: vec![ProviderUsageWindow {
                    label: "5-hour".to_string(),
                    used_percent,
                    resets_at: None,
                    global: true,
                }],
                detail: None,
                plan: None,
            }),
            detail: None,
        }
    }

    // The agent chooses an account from this snapshot: a login that answers
    // must both appear and keep the provider admissible when the selected one
    // is unusable, while an API-key lane must never advertise one.
    #[test]
    fn account_usage_reaches_the_snapshot_and_keeps_a_working_login_admitted() {
        let mut capability = subscription_capability(CodingProvider::Claude);
        attach_account_usage(
            &mut capability,
            &[account("work", true, 100), account("personal", false, 10)],
        );
        assert!(capability.authenticated);
        assert_eq!(
            capability
                .accounts
                .iter()
                .map(|account| (account.name.as_str(), account.active))
                .collect::<Vec<_>>(),
            [("work", true), ("personal", false)]
        );
        apply_spawn_admission(&mut capability);
        assert!(capability.can_spawn);

        let mut api_key = subscription_capability(CodingProvider::Codex);
        api_key.billing = Some(crate::BillingLane::ApiKey);
        attach_account_usage(&mut api_key, &[account("work", true, 10)]);
        assert!(api_key.accounts.is_empty());
        assert!(!api_key.authenticated);
    }

    #[test]
    fn fresh_claude_login_repairs_a_stale_admission_snapshot() {
        let mut capability = ProviderCapability {
            provider: CodingProvider::Claude,
            installed: true,
            version: None,
            authenticated: false,
            auth_detail: None,
            auth_methods: Vec::new(),
            can_spawn: false,
            usage: None,
            billing: None,
            accounts: Vec::new(),
            account_strategy: None,
        };
        apply_claude_subscription_status(&mut capability, true);
        assert!(capability.authenticated);
        assert_eq!(
            capability.auth_methods,
            vec![ProviderAuthMethod::Subscription]
        );
        assert_eq!(capability.billing, Some(crate::BillingLane::Subscription));
        apply_claude_subscription_status(&mut capability, false);
        assert!(!capability.authenticated);
        assert!(!capability.can_spawn);
        assert!(capability.auth_methods.is_empty());
        assert_eq!(capability.billing, None);
        for lane in [crate::BillingLane::ApiKey, crate::BillingLane::Endpoint] {
            capability.billing = Some(lane);
            capability.authenticated = true;
            apply_claude_subscription_status(&mut capability, false);
            assert_eq!(capability.billing, Some(lane));
            assert!(capability.authenticated);
        }
    }
}
