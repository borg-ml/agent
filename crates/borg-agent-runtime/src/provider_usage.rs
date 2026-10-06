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

pub const PROVIDER_CAPABILITIES_CACHE_TTL: Duration = Duration::from_secs(5);

type ProviderUsageCache = HashMap<CodingProvider, (Instant, Option<ProviderUsage>)>;

pub static PROVIDER_USAGE_CACHE: OnceLock<Mutex<ProviderUsageCache>> = OnceLock::new();

pub async fn refresh_provider_capability_usage(
    capabilities: &[ProviderCapability],
) -> Vec<ProviderCapability> {
    let mut capabilities = capabilities.to_vec();
    for capability in &mut capabilities {
        if capability.provider == CodingProvider::Claude
            && capability.installed
            && !matches!(
                capability.billing,
                Some(crate::BillingLane::ApiKey | crate::BillingLane::Endpoint)
            )
        {
            // Login can change after this session's startup snapshot. A failed
            // read is not proof of logout; retain the last admitted state.
            if let Ok(authenticated) =
                borg_provider::provider::read_claude_subscription_status().await
            {
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
    capabilities
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
            apply_spawn_admission(&mut capability);
            capability
        })
        .collect()
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
