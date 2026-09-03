//! Coding plans must route Borg's own model client at the plan's host.
//!
//! The failure this guards against is silent and expensive: hitting the vendor's
//! pay-as-you-go host with a plan key spends credits while the paid plan sits
//! unused. Nothing here spawns a CLI — these plans are served over plain HTTP by
//! Borg itself.
//!
//! Environment is process-global, so these serialize on a mutex rather than
//! relying on `--test-threads=1`, which the harness does not enforce.

use std::sync::{Mutex, MutexGuard};

use borg_provider::Plan;
use borg_provider::subscription::{PLAN_ENV, active, active_for};

static ENV_LOCK: Mutex<()> = Mutex::new(());

fn env_guard() -> MutexGuard<'static, ()> {
    let guard = ENV_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    clear();
    guard
}

fn clear() {
    unsafe {
        std::env::remove_var(PLAN_ENV);
        std::env::remove_var("ZAI_API_KEY");
        std::env::remove_var("KIMI_API_KEY");
    }
}

#[test]
fn no_plan_selected_leaves_every_profile_alone() {
    let _guard = env_guard();
    assert_eq!(active().expect("no plan"), None);
    for plan in Plan::ALL {
        assert_eq!(active_for(plan), None);
    }
}

#[test]
fn a_selected_plan_only_claims_its_own_profile() {
    let _guard = env_guard();
    unsafe { std::env::set_var(PLAN_ENV, "glm") }
    assert_eq!(active_for(Plan::GlmCoding), Some(Plan::GlmCoding));
    // Selecting GLM must never redirect Moonshot's profile.
    assert_eq!(active_for(Plan::KimiCode), None);
}

#[test]
fn a_plan_key_is_read_from_its_own_variable() {
    let _guard = env_guard();
    unsafe {
        std::env::set_var(PLAN_ENV, "kimi");
        std::env::set_var("KIMI_API_KEY", "kimi-plan-key");
    }
    assert_eq!(
        Plan::KimiCode.api_key().as_deref(),
        Some("kimi-plan-key"),
        "the plan should use its own credential"
    );
}

#[test]
fn a_malformed_selection_does_not_silently_redirect_anything() {
    let _guard = env_guard();
    unsafe { std::env::set_var(PLAN_ENV, "not-a-plan") }
    assert!(active().is_err(), "an unknown plan must be reported");
    // But it must not be treated as "some plan is active" by a gateway.
    for plan in Plan::ALL {
        assert_eq!(active_for(plan), None);
    }
}

#[test]
fn plans_point_at_subscription_hosts_not_payg_hosts() {
    // Kimi Code is billed on api.kimi.com/coding; api.moonshot.ai is credits.
    assert_eq!(Plan::KimiCode.base_url(), "https://api.kimi.com/coding/v1");
    // Z.ai bills plan quota on the /coding/ path, not the general paas/v4 one.
    assert_eq!(
        Plan::GlmCoding.base_url(),
        "https://api.z.ai/api/coding/paas/v4"
    );
}

#[test]
fn turning_a_plan_off_is_honoured() {
    let _guard = env_guard();
    for value in ["none", "off"] {
        unsafe { std::env::set_var(PLAN_ENV, value) }
        assert_eq!(active().expect("explicit off"), None, "{value}");
    }
}
