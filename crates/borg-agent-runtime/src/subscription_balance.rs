//! Automatic switching between the subscription logins of one provider.
//!
//! Two facts shape the policy:
//!   - Claude and ChatGPT report per-login quota windows, so both the login with
//!     the most headroom and the login whose allowance expires first are
//!     knowable ([`most_headroom`], [`expires_first`]). OpenCode Go and the
//!     coding plans report no quota, so their logins are spread by turn.
//!   - A turn resolves its credential once, at the start. Switching is decided
//!     there and never mid-turn, so a running turn keeps the login it began
//!     with even when another session has just moved the selection.
//!
//! Nothing here changes billing: the choice is always among several logins of
//! the same provider, never an API-key lane.

use borg_provider::subscriptions::{Lane, Strategy};

use chrono::{DateTime, Utc};

use crate::{
    CodingProvider, ProviderAccount, ProviderUsage, ProviderUsageAvailability, provider_usage,
};

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

/// When this login's largest remaining allowance expires: the reset of the
/// window with the furthest horizon that still has quota. That is the weekly
/// window where the provider reports one, and it is the budget a subscription
/// loses when that window resets -- the 5-hour window resets many times over
/// and its leftover is small next to it.
pub fn expiring_at(usage: &ProviderUsage) -> Option<DateTime<Utc>> {
    usage
        .windows
        .iter()
        .filter(|window| window.used_percent < 100)
        .filter_map(|window| window.resets_at)
        .max()
}

/// The headroom this login can still spend, or `None` when it reports nothing
/// or has nothing left.
fn usable_headroom(account: &ProviderAccount) -> Option<u8> {
    let usage = account.usage.as_ref()?;
    if usage.availability == ProviderUsageAvailability::Exhausted {
        return None;
    }
    headroom(usage).filter(|headroom| *headroom > 0)
}

fn usable(accounts: &[ProviderAccount]) -> impl Iterator<Item = &ProviderAccount> {
    accounts
        .iter()
        .filter(|account| usable_headroom(account).is_some())
}

/// The login a turn should use instead of the selected one, or `None` to keep
/// the selection. A login that reports nothing never wins on a guess.
pub fn choose(accounts: &[ProviderAccount], strategy: Strategy) -> Option<String> {
    if !strategy.is_automatic() || accounts.len() < 2 {
        return None;
    }
    let current = accounts.iter().find(|account| account.active)?;
    let best = match strategy {
        Strategy::Manual => return None,
        Strategy::Headroom => most_headroom(accounts)?,
        // A lane that reports no reset times has nothing to drain in order, so
        // the quota picture is the only thing left to rank on.
        Strategy::Drain => expires_first(accounts).or_else(|| most_headroom(accounts))?,
    };
    (best.name != current.name).then(|| best.name.clone())
}

/// Most quota left wins. The selected login keeps priority while it is within
/// [`KEEP_MARGIN`] of the best, so two similar logins do not trade the
/// selection every turn.
fn most_headroom(accounts: &[ProviderAccount]) -> Option<&ProviderAccount> {
    let best = usable(accounts)
        // A tie goes to the first name so the answer does not depend on the
        // order the store happened to return.
        .min_by(|left, right| {
            usable_headroom(right)
                .cmp(&usable_headroom(left))
                .then_with(|| left.name.cmp(&right.name))
        })?;
    let current = accounts.iter().find(|account| account.active)?;
    let (Some(current_headroom), Some(best_headroom)) =
        (usable_headroom(current), usable_headroom(best))
    else {
        return Some(best);
    };
    if current_headroom.saturating_add(KEEP_MARGIN) >= best_headroom {
        return Some(current);
    }
    Some(best)
}

/// The allowance that expires first is spent first, so a stack of
/// subscriptions drains in the order it would otherwise waste. Logins that
/// expire together drain fullest-last: use up the one with less left before it
/// evaporates. `None` when no login reports a reset at all.
fn expires_first(accounts: &[ProviderAccount]) -> Option<&ProviderAccount> {
    let expiring = usable(accounts)
        .filter_map(|account| Some((account, expiring_at(account.usage.as_ref()?)?)))
        .collect::<Vec<_>>();
    let best = expiring
        .iter()
        .min_by(|(left, left_at), (right, right_at)| {
            left_at
                .cmp(right_at)
                .then_with(|| usable_headroom(left).cmp(&usable_headroom(right)))
                .then_with(|| left.name.cmp(&right.name))
        })?;
    let current = accounts.iter().find(|account| account.active)?;
    let current_at = current.usage.as_ref().and_then(expiring_at);
    // The selected login keeps priority when it expires at the same moment with
    // the same room left: there is nothing to gain by moving.
    if current_at.is_some_and(|current_at| current_at == best.1)
        && usable_headroom(current) == usable_headroom(best.0)
    {
        return Some(current);
    }
    Some(best.0)
}

/// The next login for a lane whose provider reports no quota: the login that
/// waited longest, so turns spread over every connected subscription.
pub fn rotate(
    accounts: &[ProviderAccount],
    strategy: Strategy,
    waited: impl Fn(&str) -> u64,
) -> Option<String> {
    if !strategy.is_automatic() || accounts.len() < 2 {
        return None;
    }
    // Least recently used wins. Two logins used in the same second tie on the
    // stamp, so the one that is not selected now goes first instead of one
    // name being preferred forever.
    let next = accounts.iter().min_by(|left, right| {
        waited(&left.name)
            .cmp(&waited(&right.name))
            .then_with(|| left.active.cmp(&right.active))
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
    if !strategy.is_automatic() {
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
    use crate::ProviderUsageWindow;

    fn at(day: u32) -> DateTime<Utc> {
        chrono::TimeZone::with_ymd_and_hms(&Utc, 2026, 11, day, 12, 0, 0).unwrap()
    }

    fn window(label: &str, used: u8, resets_in: Option<u32>) -> ProviderUsageWindow {
        ProviderUsageWindow {
            label: label.to_owned(),
            used_percent: used,
            resets_at: resets_in.map(at),
            global: true,
        }
    }

    /// A login reporting a 5-hour window that resets today and a weekly window
    /// that resets on `weekly` (a day of November 2026).
    fn account(
        name: &str,
        active: bool,
        five_hour_used: u8,
        weekly_used: u8,
        weekly: Option<u32>,
    ) -> ProviderAccount {
        ProviderAccount {
            name: name.to_owned(),
            active,
            plan: None,
            usage: Some(ProviderUsage {
                availability: if weekly_used >= 100 {
                    ProviderUsageAvailability::Exhausted
                } else {
                    ProviderUsageAvailability::Available
                },
                windows: vec![
                    window("5-hour", five_hour_used, Some(7)),
                    window("Weekly", weekly_used, weekly),
                ],
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
            account("work", true, 100, 100, Some(10)),
            account("home", false, 20, 20, Some(12)),
        ];
        for strategy in [Strategy::Headroom, Strategy::Drain] {
            assert_eq!(choose(&accounts, strategy).as_deref(), Some("home"));
        }
    }

    // Two logins within the margin must not trade the selection every turn.
    #[test]
    fn a_small_difference_does_not_move_the_most_headroom_selection() {
        let accounts = [
            account("work", true, 40, 40, Some(10)),
            account("home", false, 45, 45, Some(12)),
        ];
        assert_eq!(choose(&accounts, Strategy::Headroom), None);
        let accounts = [
            account("work", true, 70, 70, Some(10)),
            account("home", false, 10, 10, Some(12)),
        ];
        assert_eq!(
            choose(&accounts, Strategy::Headroom).as_deref(),
            Some("home")
        );
    }

    // Draining is the other question: spend what expires first, even when the
    // other login has more room left.
    #[test]
    fn draining_spends_the_allowance_that_expires_first() {
        let accounts = [
            account("work", true, 70, 70, Some(14)),
            account("home", false, 5, 5, Some(9)),
        ];
        assert_eq!(choose(&accounts, Strategy::Drain).as_deref(), Some("home"));
        assert_eq!(
            choose(&accounts, Strategy::Headroom).as_deref(),
            Some("home")
        );
        // The reverse case is where the two policies disagree.
        let accounts = [
            account("work", true, 70, 70, Some(9)),
            account("home", false, 5, 5, Some(14)),
        ];
        assert_eq!(choose(&accounts, Strategy::Drain), None);
        assert_eq!(
            choose(&accounts, Strategy::Headroom).as_deref(),
            Some("home")
        );
        let accounts = [
            account("work", true, 5, 5, Some(14)),
            account("home", false, 70, 70, Some(9)),
        ];
        assert_eq!(choose(&accounts, Strategy::Drain).as_deref(), Some("home"));
        // The selected login already has the most room left, so nothing moves.
        assert_eq!(choose(&accounts, Strategy::Headroom), None);
    }

    // Two logins whose allowance expires together: spend the one that is
    // nearly used up before it evaporates, and do not move once there.
    #[test]
    fn equal_expiry_drains_the_fuller_one_last_and_then_settles() {
        let accounts = [
            account("work", true, 80, 80, Some(9)),
            account("home", false, 20, 20, Some(9)),
        ];
        // Already spending the nearly-used one, which is the right order.
        assert_eq!(choose(&accounts, Strategy::Drain), None);
        let accounts = [
            account("work", true, 20, 20, Some(9)),
            account("home", false, 80, 80, Some(9)),
        ];
        assert_eq!(choose(&accounts, Strategy::Drain).as_deref(), Some("home"));
    }

    #[test]
    fn manual_keeps_the_selected_login_and_unknown_quota_never_wins() {
        let accounts = [
            account("work", true, 100, 100, Some(10)),
            account("home", false, 0, 0, Some(9)),
        ];
        assert_eq!(choose(&accounts, Strategy::Manual), None);
        let mut unknown = account("home", false, 0, 0, None);
        unknown.usage = None;
        let accounts = [account("work", true, 100, 100, Some(10)), unknown];
        assert_eq!(choose(&accounts, Strategy::Drain), None);
        assert_eq!(choose(&accounts, Strategy::Headroom), None);
    }

    // A lane without a quota view spreads turns instead of pinning one login.
    #[test]
    fn a_quota_less_lane_alternates_between_logins() {
        let mut accounts = [
            account("work", true, 0, 0, None),
            account("home", false, 0, 0, None),
        ];
        for account in &mut accounts {
            account.usage = None;
        }
        let waited = |name: &str| match name {
            "work" => 30,
            _ => 10,
        };
        assert_eq!(
            rotate(&accounts, Strategy::Drain, waited).as_deref(),
            Some("home")
        );
        assert_eq!(rotate(&accounts, Strategy::Manual, |_| 0), None);
        // Two turns inside the same second tie on the stamp; the login that is
        // not selected goes first so one login is not preferred forever.
        assert_eq!(
            rotate(&accounts, Strategy::Headroom, |_| 7).as_deref(),
            Some("home")
        );
        let single = [accounts[0].clone()];
        assert_eq!(rotate(&single, Strategy::Drain, |_| 0), None);
    }

    // A provider that reports quota but no reset times still gets a sensible
    // order rather than no switching at all.
    #[test]
    fn draining_falls_back_to_headroom_without_reset_times() {
        let mut accounts = [
            account("work", true, 70, 70, None),
            account("home", false, 5, 5, None),
        ];
        for account in &mut accounts {
            for window in &mut account.usage.as_mut().unwrap().windows {
                window.resets_at = None;
            }
        }
        assert_eq!(choose(&accounts, Strategy::Drain).as_deref(), Some("home"));
        accounts[1].usage = None;
        assert_eq!(choose(&accounts, Strategy::Drain), None);
    }
}
