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
    CodingProvider, ProviderAuthMethod, ProviderCapability, ProviderUsage,
    ProviderUsageAvailability, ProviderUsageWindow,
};

pub(crate) const PROVIDER_CAPABILITIES_CACHE_TTL: Duration = Duration::from_secs(5);

type ProviderUsageCache = HashMap<CodingProvider, (Instant, Option<ProviderUsage>)>;

pub(crate) static PROVIDER_USAGE_CACHE: OnceLock<Mutex<ProviderUsageCache>> = OnceLock::new();

pub(crate) async fn refresh_provider_capability_usage(
    capabilities: &[ProviderCapability],
) -> Vec<ProviderCapability> {
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
    capabilities
        .iter()
        .cloned()
        .map(|mut capability| {
            let usage = match capability.provider {
                CodingProvider::Codex => codex.clone(),
                CodingProvider::Claude => claude.clone(),
                _ => return capability,
            };
            if usage.is_some() {
                capability.usage = usage;
            }
            let exhausted = capability
                .usage
                .as_ref()
                .is_some_and(|usage| usage.availability == ProviderUsageAvailability::Exhausted);
            let alternate_route = capability.auth_methods.iter().any(|method| {
                matches!(
                    method,
                    ProviderAuthMethod::ApiKey | ProviderAuthMethod::Endpoint
                )
            });
            capability.can_spawn =
                capability.installed && capability.authenticated && (!exhausted || alternate_route);
            capability
        })
        .collect()
}

async fn probe_provider_usage(provider: CodingProvider) -> Option<ProviderUsage> {
    let cache = PROVIDER_USAGE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some((probed_at, usage)) = cache.lock().await.get(&provider)
        && probed_at.elapsed() < PROVIDER_CAPABILITIES_CACHE_TTL
    {
        return usage.clone();
    }
    let usage = match provider {
        CodingProvider::Codex => borg_provider::provider::read_codex_account_rate_limits()
            .await
            .ok()
            .map(codex_provider_usage),
        CodingProvider::Claude => borg_provider::provider::read_claude_account_rate_limits()
            .await
            .ok()
            .map(claude_provider_usage),
        _ => None,
    };
    cache
        .lock()
        .await
        .insert(provider, (Instant::now(), usage.clone()));
    usage
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
