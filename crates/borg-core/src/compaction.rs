//! Per-model compaction budgets.
//!
//! Borg sizes automatic compaction from two numbers: the *reserve* (remaining
//! headroom at which a tool round triggers compaction) and the *tail* (how
//! much recent context survives the summary verbatim). Both have always been
//! percentages of the context window, which is the right default because it
//! scales across a 32k local model and a 1M hosted one.
//!
//! Percentages are the wrong answer for a specific model whose real cost or
//! round size is known: 15% of a 1M window is 150k tokens held back from every
//! turn. This module keeps the percentage defaults and lets an exact
//! `provider/model` pair override either number in absolute tokens, without
//! forcing the operator to restate the one they are happy with.
//!
//! The crate stays provider-neutral: policies key on the provider's string id
//! rather than an enum owned by a higher layer.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Remaining share of the context window at which a tool round triggers
/// automatic compaction. The next round adds the model's reply plus every new
/// tool result before usage is reported again, so a smaller cushion is
/// routinely overrun by a single large read and the turn fails on length.
pub const DEFAULT_RESERVE_PERCENT: u64 = 15;

/// Share of the window kept verbatim after the summary so the model keeps the
/// evidence it was just reasoning about, not only a prose recollection of it.
pub const DEFAULT_KEEP_RECENT_PERCENT: u64 = 10;

/// Largest share of the window the reserve and the retained tail may consume
/// together. Past this point a turn has no room to do work between
/// compactions: it summarizes, restores a tail that already fills the window,
/// and immediately trips the threshold again. Absolute overrides are clamped
/// to this bound because the window is only known at runtime and may be
/// smaller than the operator assumed.
pub const MAX_BUDGET_PERCENT: u64 = 75;

/// Where one resolved budget number came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionBudgetSource {
    /// The percentage default applied to the live context window.
    Default,
    /// An exact `provider/model` override from configuration.
    ModelOverride,
}

impl CompactionBudgetSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::ModelOverride => "model_override",
        }
    }
}

/// One model's overrides. Each field is independent: supplying only
/// `reserve_tokens` leaves the tail on its percentage default.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionBudgetOverride {
    pub reserve_tokens: Option<u64>,
    pub keep_recent_tokens: Option<u64>,
}

impl CompactionBudgetOverride {
    pub const fn is_empty(&self) -> bool {
        self.reserve_tokens.is_none() && self.keep_recent_tokens.is_none()
    }
}

/// Why a configured policy was rejected. Compaction budgets are load-bearing
/// for whether a long turn survives, so a malformed one is a startup error
/// rather than a warning that strands the operator with silent defaults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompactionPolicyError {
    /// The key was not an exact `provider/model` pair.
    MalformedKey { key: String },
    /// An override was present but set nothing.
    EmptyOverride { key: String },
    /// A reserve of zero disables the guard the budget exists to provide.
    ZeroReserve { key: String },
    /// The pair cannot fit a context window that is already known.
    ExceedsWindow {
        key: String,
        requested_tokens: u64,
        context_window_tokens: u64,
    },
}

impl std::fmt::Display for CompactionPolicyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MalformedKey { key } => write!(
                formatter,
                "compaction budget key `{key}` must be an exact `provider/model` pair"
            ),
            Self::EmptyOverride { key } => write!(
                formatter,
                "compaction budget `{key}` sets neither reserve_tokens nor keep_recent_tokens"
            ),
            Self::ZeroReserve { key } => write!(
                formatter,
                "compaction budget `{key}` sets reserve_tokens = 0, which disables automatic \
                 compaction and fails the turn at the context wall"
            ),
            Self::ExceedsWindow {
                key,
                requested_tokens,
                context_window_tokens,
            } => write!(
                formatter,
                "compaction budget `{key}` reserves {requested_tokens} tokens of a \
                 {context_window_tokens} token window, leaving no room to work between \
                 compactions (limit is {MAX_BUDGET_PERCENT}% of the window)"
            ),
        }
    }
}

impl std::error::Error for CompactionPolicyError {}

/// Exact `provider/model` compaction overrides. An empty policy resolves every
/// model to the percentage defaults, which is the behavior Borg shipped before
/// this existed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CompactionBudgetPolicy {
    overrides: BTreeMap<String, CompactionBudgetOverride>,
}

impl CompactionBudgetPolicy {
    /// Build a validated policy. `known_windows` supplies context windows for
    /// models whose size configuration is already known, so an impossible
    /// budget is rejected at load instead of silently clamped on the first
    /// long turn.
    pub fn new(
        overrides: BTreeMap<String, CompactionBudgetOverride>,
        known_windows: &BTreeMap<String, u64>,
    ) -> Result<Self, CompactionPolicyError> {
        for (key, value) in &overrides {
            validate_key(key)?;
            if value.is_empty() {
                return Err(CompactionPolicyError::EmptyOverride { key: key.clone() });
            }
            if value.reserve_tokens == Some(0) {
                return Err(CompactionPolicyError::ZeroReserve { key: key.clone() });
            }
            let Some(&window) = known_windows.get(key) else {
                continue;
            };
            let reserve = value
                .reserve_tokens
                .unwrap_or_else(|| percent_of(window, DEFAULT_RESERVE_PERCENT));
            let keep_recent = value
                .keep_recent_tokens
                .unwrap_or_else(|| percent_of(window, DEFAULT_KEEP_RECENT_PERCENT));
            let requested = reserve.saturating_add(keep_recent);
            if requested > percent_of(window, MAX_BUDGET_PERCENT) {
                return Err(CompactionPolicyError::ExceedsWindow {
                    key: key.clone(),
                    requested_tokens: requested,
                    context_window_tokens: window,
                });
            }
        }
        Ok(Self { overrides })
    }

    pub fn is_empty(&self) -> bool {
        self.overrides.is_empty()
    }

    /// Resolve the budget for one model against the window the provider
    /// actually reported. Each number falls back independently, so an override
    /// that sets only one leaves the other on its percentage default.
    pub fn resolve(
        &self,
        provider: &str,
        model: &str,
        context_window_tokens: u64,
    ) -> EffectiveCompactionBudget {
        let key = budget_key(provider, model);
        let configured = self.overrides.get(&key).copied().unwrap_or_default();

        let (reserve_tokens, reserve_source) = match configured.reserve_tokens {
            Some(tokens) => (tokens, CompactionBudgetSource::ModelOverride),
            None => (
                percent_of(context_window_tokens, DEFAULT_RESERVE_PERCENT),
                CompactionBudgetSource::Default,
            ),
        };
        let (keep_recent_tokens, keep_recent_source) = match configured.keep_recent_tokens {
            Some(tokens) => (tokens, CompactionBudgetSource::ModelOverride),
            None => (
                percent_of(context_window_tokens, DEFAULT_KEEP_RECENT_PERCENT),
                CompactionBudgetSource::Default,
            ),
        };

        let budget = EffectiveCompactionBudget {
            reserve_tokens,
            keep_recent_tokens,
            reserve_source,
            keep_recent_source,
            context_window_tokens,
            clamped_to_window: false,
        };
        budget.fit_to_window()
    }
}

/// The budget one turn actually runs on, carrying enough provenance for the
/// compaction event to explain why it fired when it did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectiveCompactionBudget {
    /// Headroom below which a tool round compacts.
    pub reserve_tokens: u64,
    /// Recent context kept verbatim after the summary.
    pub keep_recent_tokens: u64,
    pub reserve_source: CompactionBudgetSource,
    pub keep_recent_source: CompactionBudgetSource,
    pub context_window_tokens: u64,
    /// An override did not fit the window the provider reported and was scaled
    /// down. Surfaced so a misconfigured budget is visible instead of quietly
    /// behaving unlike what the operator wrote.
    pub clamped_to_window: bool,
}

impl EffectiveCompactionBudget {
    /// The percentage defaults for a window, with no policy involved.
    pub fn defaults_for_window(context_window_tokens: u64) -> Self {
        Self {
            reserve_tokens: percent_of(context_window_tokens, DEFAULT_RESERVE_PERCENT),
            keep_recent_tokens: percent_of(context_window_tokens, DEFAULT_KEEP_RECENT_PERCENT),
            reserve_source: CompactionBudgetSource::Default,
            keep_recent_source: CompactionBudgetSource::Default,
            context_window_tokens,
            clamped_to_window: false,
        }
    }

    /// True once used context has eaten into the reserve.
    pub fn should_compact(&self, context_tokens: u64) -> bool {
        self.context_window_tokens.saturating_sub(context_tokens) <= self.reserve_tokens
    }

    /// Scale an oversized pair back inside [`MAX_BUDGET_PERCENT`], keeping the
    /// operator's ratio between reserve and tail.
    fn fit_to_window(self) -> Self {
        let limit = percent_of(self.context_window_tokens, MAX_BUDGET_PERCENT);
        let requested = self.reserve_tokens.saturating_add(self.keep_recent_tokens);
        if requested <= limit || requested == 0 {
            return self;
        }
        let scale = |value: u64| -> u64 {
            u64::try_from(u128::from(value) * u128::from(limit) / u128::from(requested))
                .unwrap_or(value)
        };
        Self {
            reserve_tokens: scale(self.reserve_tokens).max(1),
            keep_recent_tokens: scale(self.keep_recent_tokens),
            clamped_to_window: true,
            ..self
        }
    }
}

/// The exact lookup key for a model, matching the `provider/model` alias
/// Borg already uses for configured models.
pub fn budget_key(provider: &str, model: &str) -> String {
    format!("{}/{}", provider.trim(), model.trim())
}

fn validate_key(key: &str) -> Result<(), CompactionPolicyError> {
    // Split once: an OpenRouter model id is itself `vendor/name`, so the alias
    // for one is `open_router/anthropic/claude-opus-5`. Only the first
    // separator divides provider from model.
    let Some((provider, model)) = key.split_once('/') else {
        return Err(CompactionPolicyError::MalformedKey {
            key: key.to_string(),
        });
    };
    if provider.trim().is_empty() || model.trim().is_empty() {
        return Err(CompactionPolicyError::MalformedKey {
            key: key.to_string(),
        });
    }
    Ok(())
}

fn percent_of(tokens: u64, percent: u64) -> u64 {
    tokens.saturating_mul(percent) / 100
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The predicate Borg used before budgets existed, kept verbatim as the
    /// oracle. The whole change is only safe if the default path still fires
    /// at exactly this point.
    fn legacy_needs_compaction(context_tokens: u64, window: u64) -> bool {
        u128::from(context_tokens).saturating_mul(100)
            >= u128::from(window).saturating_mul(100 - u128::from(DEFAULT_RESERVE_PERCENT))
    }

    /// An off-by-one here moves the compaction trigger, and the failure is not
    /// a wrong number on a dashboard: compacting late means the next round
    /// overruns the window and the turn dies at the wall with the work half
    /// done. Integer truncation differs between the two formulations
    /// (`window*85/100` vs `window - window*15/100`), so equivalence is a real
    /// claim and not a restatement of the code.
    #[test]
    fn default_budget_fires_exactly_where_the_legacy_threshold_did() {
        let policy = CompactionBudgetPolicy::default();
        for window in [
            1_000_u64, 32_768, 128_000, 128_001, 199_999, 200_000, 1_000_000,
        ] {
            let budget = policy.resolve("claude", "claude-opus-5", window);
            // Walk the boundary rather than the whole window.
            let first = window.saturating_sub(window / 5);
            for context_tokens in first..=window {
                assert_eq!(
                    budget.should_compact(context_tokens),
                    legacy_needs_compaction(context_tokens, window),
                    "window {window} disagreed at {context_tokens} tokens"
                );
            }
        }
    }

    /// The contract the operator is promised: setting one number leaves the
    /// other on its percentage default, rather than silently zeroing it or
    /// forcing them to restate a value they are happy with.
    #[test]
    fn reserve_and_tail_fall_back_independently() {
        let window = 200_000;
        let policy = CompactionBudgetPolicy::new(
            BTreeMap::from([(
                "claude/claude-opus-5".to_string(),
                CompactionBudgetOverride {
                    reserve_tokens: Some(20_000),
                    keep_recent_tokens: None,
                },
            )]),
            &BTreeMap::new(),
        )
        .expect("valid policy");

        let budget = policy.resolve("claude", "claude-opus-5", window);
        assert_eq!(budget.reserve_tokens, 20_000);
        assert_eq!(budget.reserve_source, CompactionBudgetSource::ModelOverride);
        // Untouched by the override: still 10% of the window.
        assert_eq!(budget.keep_recent_tokens, 20_000);
        assert_eq!(budget.keep_recent_source, CompactionBudgetSource::Default);

        // A model with no entry is wholly unaffected.
        let other = policy.resolve("claude", "claude-sonnet-5", window);
        assert_eq!(
            other,
            EffectiveCompactionBudget::defaults_for_window(window)
        );
    }

    /// A budget written for a large window, then run against a small one (a
    /// provider that reports less than expected, or the 128k assumed window),
    /// would otherwise reserve more than the window holds: every round
    /// compacts, restores a tail that already fills the window, and compacts
    /// again. That is a hang, not a misconfiguration warning.
    #[test]
    fn an_override_too_large_for_the_real_window_cannot_livelock() {
        let policy = CompactionBudgetPolicy::new(
            BTreeMap::from([(
                "claude/claude-opus-5".to_string(),
                CompactionBudgetOverride {
                    reserve_tokens: Some(150_000),
                    keep_recent_tokens: Some(100_000),
                },
            )]),
            &BTreeMap::new(),
        )
        .expect("valid without a known window");

        let window = 128_000;
        let budget = policy.resolve("claude", "claude-opus-5", window);
        assert!(budget.clamped_to_window);
        assert!(
            budget.reserve_tokens + budget.keep_recent_tokens <= window * MAX_BUDGET_PERCENT / 100
        );
        // Room is left to actually do work between compactions.
        assert!(!budget.should_compact(budget.keep_recent_tokens));
    }

    /// Budgets decide whether a long turn survives, so a broken one is a
    /// startup error rather than a warning the operator scrolls past.
    #[test]
    fn strict_validation_rejects_budgets_that_cannot_work() {
        let zero_reserve = CompactionBudgetPolicy::new(
            BTreeMap::from([(
                "claude/claude-opus-5".to_string(),
                CompactionBudgetOverride {
                    reserve_tokens: Some(0),
                    keep_recent_tokens: None,
                },
            )]),
            &BTreeMap::new(),
        );
        assert!(matches!(
            zero_reserve,
            Err(CompactionPolicyError::ZeroReserve { .. })
        ));

        let no_model = CompactionBudgetPolicy::new(
            BTreeMap::from([("claude".to_string(), CompactionBudgetOverride::default())]),
            &BTreeMap::new(),
        );
        assert!(matches!(
            no_model,
            Err(CompactionPolicyError::MalformedKey { .. })
        ));

        // Caught at load because this model's window is already configured.
        let too_big = CompactionBudgetPolicy::new(
            BTreeMap::from([(
                "local/qwen".to_string(),
                CompactionBudgetOverride {
                    reserve_tokens: Some(30_000),
                    keep_recent_tokens: Some(30_000),
                },
            )]),
            &BTreeMap::from([("local/qwen".to_string(), 32_768)]),
        );
        assert!(matches!(
            too_big,
            Err(CompactionPolicyError::ExceedsWindow { .. })
        ));
    }

    /// OpenRouter model ids contain a slash of their own, so a naive split
    /// would reject every OpenRouter budget as malformed.
    #[test]
    fn a_model_id_containing_a_slash_is_a_valid_key() {
        let key = "open_router/anthropic/claude-opus-5";
        let policy = CompactionBudgetPolicy::new(
            BTreeMap::from([(
                key.to_string(),
                CompactionBudgetOverride {
                    reserve_tokens: Some(12_000),
                    keep_recent_tokens: None,
                },
            )]),
            &BTreeMap::new(),
        )
        .expect("an OpenRouter alias is a valid key");
        assert_eq!(
            policy
                .resolve("open_router", "anthropic/claude-opus-5", 200_000)
                .reserve_tokens,
            12_000
        );
    }
}
