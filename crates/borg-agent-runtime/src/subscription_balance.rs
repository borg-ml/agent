//! Automatic switching between the subscription logins of one provider.
//!
//! Two facts shape the policy:
//!   - Claude and ChatGPT report per-login quota windows, so the login with the
//!     most headroom is knowable. OpenCode Go and the coding plans report none,
//!     so their logins are spread by turn instead.
//!   - A turn resolves its credential once, at the start. Switching is decided
//!     there and never mid-turn, so a running turn keeps the login it began
//!     with even when another session has just moved the selection.
//!
//! Nothing here changes billing: the choice is always among several logins of
//! the same provider, never an API-key lane.

use borg_provider::subscriptions::{Lane, Strategy};

use crate::{CodingProvider, ProviderAccount, ProviderUsage, provider_usage};

/// Headroom, in percentage points, the selected login keeps before a turn
/// moves to another one. Without a margin, two logins a percent apart would
/// swap on every turn and rewrite the credential store for no gain.
pub const KEEP_MARGIN: u8 = 15;

/// Remaining capacity of one login: its tightest *global* window. Model-scoped
/// windows stay advisory because another model may still have capacity, so
/// they must not decide which login a turn spends.
pub fn headroom(usage: &ProviderUsage) -> Option<u8> {
    usage
        .windows
        .iter()
        .filter(|window| window.global)
        .map(|window| window.used_percent)
        .max()
        .map(|used| 100u8.saturating_sub(used))
}

/// The login a turn should use instead of the selected one, or `None` to keep
/// the selection. A login that reports nothing never wins on a guess, and an
/// exhausted selected login yields to one with headroom.
pub fn choose(accounts: &[ProviderAccount], strategy: Strategy) -> Option<String> {
    if strategy == Strategy::Manual || accounts.len() < 2 {
        return None;
    }
    let current = accounts.iter().find(|account| account.active)?;
    let mut ranked = accounts
        .iter()
        .filter_map(|account| {
            let headroom = account
                .usage
                .as_ref()
                .and_then(headroom)
                .filter(|headroom| *headroom > 0)?;
            Some((account.name.as_str(), headroom))
        })
        .collect::<Vec<_>>();
    // Most headroom wins; a tie goes to the first name so the answer does not
    // depend on the order the store happened to return.
    ranked.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(right.0)));
    let (best, best_headroom) = *ranked.first()?;
    if best == current.name {
        return None;
    }
    if let Some(current_headroom) = current.usage.as_ref().and_then(headroom)
        && current_headroom.saturating_add(KEEP_MARGIN) >= best_headroom
    {
        return None;
    }
    Some(best.to_owned())
}

/// The next login for a lane whose provider reports no quota: the login that
/// waited longest, so turns spread over every connected subscription.
pub fn rotate(
    accounts: &[ProviderAccount],
    strategy: Strategy,
    waited: impl Fn(&str) -> u64,
) -> Option<String> {
    if strategy == Strategy::Manual || accounts.len() < 2 {
        return None;
    }
    let next = accounts.iter().min_by(|left, right| {
        waited(&left.name)
            .cmp(&waited(&right.name))
            .then_with(|| left.name.cmp(&right.name))
    })?;
    (accounts
        .iter()
        .find(|account| account.active)
        .is_none_or(|current| current.name != next.name))
    .then(|| next.name.clone())
}

/// Move to another login of `provider` when automatic switching says so, and
/// return the login the turn will now spend. Only quota reads that already
/// happened are used: a turn must never wait on a usage probe to start, so a
/// cold read keeps the current selection.
pub async fn before_turn(provider: CodingProvider) -> Option<String> {
    let lane = provider_usage::subscription_lane(provider)?;
    let strategy = borg_provider::subscriptions::strategy(lane);
    if strategy == Strategy::Manual {
        return None;
    }
    let accounts = provider_usage::cached_subscription_accounts(provider).await;
    let target = match lane.key_lane() {
        Some(key_lane) => rotate(&accounts, strategy, |name| {
            borg_provider::key_accounts::last_used(key_lane, name)
                .ok()
                .flatten()
                .unwrap_or_default()
        }),
        None => choose(&accounts, strategy),
    }?;
    borg_provider::subscriptions::activate(lane, &target)
        .await
        .ok()?;
    if let Some(key_lane) = lane.key_lane() {
        let _ = borg_provider::key_accounts::mark_used(key_lane, &target);
    }
    Some(target)
}

/// The lane a provider spends subscriptions against, for callers that need to
/// read or change the policy.
pub fn lane(provider: CodingProvider) -> Option<Lane> {
    provider_usage::subscription_lane(provider)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ProviderUsageAvailability, ProviderUsageWindow};

    fn account(name: &str, active: bool, used: Option<u8>) -> ProviderAccount {
        ProviderAccount {
            name: name.to_owned(),
            active,
            plan: None,
            usage: used.map(|used| ProviderUsage {
                availability: if used >= 100 {
                    ProviderUsageAvailability::Exhausted
                } else {
                    ProviderUsageAvailability::Available
                },
                windows: vec![ProviderUsageWindow {
                    label: "5h".to_owned(),
                    used_percent: used,
                    resets_at: None,
                    global: true,
                }],
                detail: None,
                plan: None,
            }),
            detail: None,
        }
    }

    // An exhausted login must yield: that is the whole point of automatic
    // switching, and staying would fail the turn.
    #[test]
    fn an_exhausted_login_yields_to_one_with_headroom() {
        let accounts = [
            account("work", true, Some(100)),
            account("home", false, Some(20)),
        ];
        assert_eq!(choose(&accounts, Strategy::Auto).as_deref(), Some("home"));
    }

    // Two logins within the margin must not trade the selection every turn.
    #[test]
    fn a_small_difference_does_not_move_the_selection() {
        let accounts = [
            account("work", true, Some(40)),
            account("home", false, Some(45)),
        ];
        assert_eq!(choose(&accounts, Strategy::Auto), None);
        let accounts = [
            account("work", true, Some(70)),
            account("home", false, Some(10)),
        ];
        assert_eq!(choose(&accounts, Strategy::Auto).as_deref(), Some("home"));
    }

    #[test]
    fn manual_keeps_the_selected_login_and_unknown_quota_never_wins() {
        let accounts = [
            account("work", true, Some(100)),
            account("home", false, Some(0)),
        ];
        assert_eq!(choose(&accounts, Strategy::Manual), None);
        let accounts = [
            account("work", true, Some(100)),
            account("home", false, None),
        ];
        assert_eq!(choose(&accounts, Strategy::Auto), None);
    }

    // A lane without a quota view spreads turns instead of pinning one login.
    #[test]
    fn a_quota_less_lane_alternates_between_logins() {
        let accounts = [account("work", true, None), account("home", false, None)];
        let waited = |name: &str| match name {
            "work" => 30,
            _ => 10,
        };
        assert_eq!(
            rotate(&accounts, Strategy::Auto, waited).as_deref(),
            Some("home")
        );
        assert_eq!(rotate(&accounts, Strategy::Manual, |_| 0), None);
        let single = [account("work", true, None)];
        assert_eq!(rotate(&single, Strategy::Auto, |_| 0), None);
    }
}
