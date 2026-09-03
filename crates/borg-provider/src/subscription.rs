//! Vendor coding-plan subscriptions, driven through Borg's own model client.
//!
//! Why this exists:
//!   - Several vendors sell a *coding subscription* separate from their
//!     pay-as-you-go API: Z.ai's GLM Coding Plan, Moonshot's Kimi Code. The plan
//!     is served from a different host than the PAYG API, and billed against
//!     plan quota rather than credits.
//!   - The vendors document these as "point your agent CLI at our base URL".
//!     Borg does not need that framing: Borg *is* the agent harness, and it
//!     already speaks OpenAI Chat Completions natively. Routing a Moonshot
//!     subscription through a third-party CLI would add a binary dependency and
//!     a process boundary to reach an HTTP endpoint Borg can call directly.
//!   - So a plan here is a *gateway configuration* — a base URL and a key —
//!     applied to the native profile that already exists. No CLI is involved,
//!     and a user on these plans needs no vendor binary installed at all.
//!
//! The distinction that matters: [`crate::provider_bin`] installs binaries for
//! providers that require one (Codex, Claude, Grok, Muse). This module needs
//! none of that, because these plans are reachable over plain HTTP.

use anyhow::{Result, bail};

use crate::credentials::{ApiKeyCredential, api_key};

/// Selects the active plan, e.g. `BORG_SUBSCRIPTION=glm`.
pub const PLAN_ENV: &str = "BORG_SUBSCRIPTION";

/// A vendor coding plan Borg can drive directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Plan {
    /// Z.ai GLM Coding Plan.
    GlmCoding,
    /// Moonshot Kimi Code.
    KimiCode,
}

impl Plan {
    pub const ALL: [Plan; 2] = [Plan::GlmCoding, Plan::KimiCode];

    /// Short identifier accepted by [`PLAN_ENV`].
    pub fn id(self) -> &'static str {
        match self {
            Plan::GlmCoding => "glm",
            Plan::KimiCode => "kimi",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Plan::GlmCoding => "GLM Coding Plan",
            Plan::KimiCode => "Kimi Code",
        }
    }

    /// OpenAI Chat Completions base URL for the plan's quota.
    ///
    /// These are deliberately not the vendors' pay-as-you-go hosts: Moonshot
    /// serves the plan from `api.kimi.com/coding`, not `api.moonshot.ai`, and
    /// Z.ai bills plan quota on the `coding/` path rather than the general
    /// `paas/v4` one. Pointing at the wrong host silently spends credits
    /// instead of plan quota.
    pub fn base_url(self) -> &'static str {
        match self {
            Plan::GlmCoding => "https://api.z.ai/api/coding/paas/v4",
            Plan::KimiCode => "https://api.kimi.com/coding/v1",
        }
    }

    /// Where the user's key is read from, by environment or credential store.
    pub fn credential(self) -> ApiKeyCredential {
        match self {
            Plan::GlmCoding => ApiKeyCredential::Zai,
            Plan::KimiCode => ApiKeyCredential::Kimi,
        }
    }

    /// Where the user obtains a key. Shown in errors and by `borg doctor`.
    pub fn key_url(self) -> &'static str {
        match self {
            Plan::GlmCoding => "https://z.ai/manage-apikey/apikey-list",
            Plan::KimiCode => "the Kimi Code console at https://www.kimi.com/code",
        }
    }

    pub fn parse(value: &str) -> Option<Plan> {
        let value = value.trim().to_ascii_lowercase();
        Plan::ALL.into_iter().find(|plan| plan.id() == value)
    }

    /// The key for this plan, if the user has provided one.
    pub fn api_key(self) -> Option<String> {
        api_key(self.credential())
    }
}

/// The plan the user has selected, if any.
pub fn active() -> Result<Option<Plan>> {
    let Some(value) = crate::env::nonempty_var(PLAN_ENV) else {
        return Ok(None);
    };
    let trimmed = value.trim();
    if matches!(trimmed.to_ascii_lowercase().as_str(), "none" | "off") {
        return Ok(None);
    }
    match Plan::parse(trimmed) {
        Some(plan) => Ok(Some(plan)),
        None => {
            let known: Vec<&str> = Plan::ALL.iter().map(|plan| plan.id()).collect();
            bail!(
                "{PLAN_ENV} is set to `{trimmed}`, which is not a known coding plan. \
                 Known plans: {}. Unset {PLAN_ENV} to use pay-as-you-go API credits.",
                known.join(", ")
            )
        }
    }
}

/// The active plan, but only when it is the one asked for.
///
/// Profiles call this to decide whether to serve themselves from plan quota.
/// A selected plan never redirects a different vendor's profile.
pub fn active_for(plan: Plan) -> Option<Plan> {
    match active() {
        Ok(Some(selected)) if selected == plan => Some(selected),
        // A malformed selection is surfaced by `borg doctor` and at call time;
        // it must not silently redirect a gateway here.
        _ => None,
    }
}

/// One-line description for `borg doctor`.
pub fn describe() -> String {
    match active() {
        Ok(None) => {
            "Coding plan: none (providers use their own subscription or API credits)".to_string()
        }
        Ok(Some(plan)) => {
            let key = match plan.api_key() {
                Some(_) => "key present".to_string(),
                None => format!("NO KEY — set {}", plan.credential().env_var()),
            };
            format!(
                "Coding plan: {} · {} · {} · served by Borg directly, no CLI required",
                plan.label(),
                plan.base_url(),
                key
            )
        }
        Err(error) => format!("Coding plan: misconfigured — {error}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_ids_are_unique_and_parse_round_trip() {
        for plan in Plan::ALL {
            assert_eq!(Plan::parse(plan.id()), Some(plan));
            assert_eq!(Plan::parse(&plan.id().to_uppercase()), Some(plan));
        }
        let mut ids: Vec<&str> = Plan::ALL.iter().map(|plan| plan.id()).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), Plan::ALL.len());
    }

    #[test]
    fn unknown_plans_do_not_parse() {
        assert_eq!(Plan::parse("gemini"), None);
        assert_eq!(Plan::parse(""), None);
    }

    #[test]
    fn plan_hosts_are_the_subscription_hosts_not_the_payg_ones() {
        // Spending plan quota depends on hitting the coding host. The PAYG
        // hosts would succeed with the same key and silently bill credits.
        assert!(
            Plan::KimiCode
                .base_url()
                .starts_with("https://api.kimi.com/coding")
        );
        assert!(!Plan::KimiCode.base_url().contains("moonshot"));
        assert!(Plan::GlmCoding.base_url().contains("/coding/"));
    }

    #[test]
    fn base_urls_are_absolute_https_without_a_trailing_slash() {
        for plan in Plan::ALL {
            assert!(plan.base_url().starts_with("https://"), "{}", plan.label());
            assert!(!plan.base_url().ends_with('/'), "{}", plan.label());
        }
    }

    #[test]
    fn each_plan_uses_a_distinct_credential() {
        let mut vars: Vec<&str> = Plan::ALL
            .iter()
            .map(|plan| plan.credential().env_var())
            .collect();
        vars.sort_unstable();
        vars.dedup();
        assert_eq!(vars.len(), Plan::ALL.len());
    }
}
