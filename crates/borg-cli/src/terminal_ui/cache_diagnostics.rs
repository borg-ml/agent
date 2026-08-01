use std::time::Duration;

use borg_provider::provider::estimate_openai_cache_miss_microusd;
use borg_remote::CodingProvider;
use chrono::{DateTime, Utc};

// Provider cache counters are block-aligned and the reusable prefix naturally
// ends before the newly appended assistant/user tail. Treating any four-digit
// uncached remainder as a miss produces false alarms on otherwise excellent
// cache reuse (for example 141,056 cached tokens from a 142,155-token prior
// prompt). A warning should describe a materially lost prefix, not rounding.
const CACHE_MISS_NOISE_FLOOR_TOKENS: u64 = 2_048;
const CACHE_MISS_MINIMUM_PRIOR_PREFIX_PERCENT: u64 = 5;
const CODEX_CACHE_WINDOW: Duration = Duration::from_secs(30 * 60);
const CLAUDE_CACHE_WINDOW: Duration = Duration::from_secs(5 * 60);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct CacheSignature {
    provider: CodingProvider,
    model: Option<String>,
    effort: Option<String>,
}

impl CacheSignature {
    pub(super) fn new(provider: CodingProvider, model: Option<&str>, effort: Option<&str>) -> Self {
        Self {
            provider,
            model: model.map(str::to_string),
            effort: effort.map(str::to_string),
        }
    }
}

pub(super) struct CacheUsage<'a> {
    pub input_tokens: u64,
    pub cached_input_tokens: u64,
    pub cache_creation_input_tokens: u64,
    pub cost_microusd: Option<u64>,
    pub cost_basis: &'a str,
}

#[derive(Default)]
pub(super) struct CacheDiagnostics {
    previous: Option<PromptSnapshot>,
    latest: Option<LatestCacheUse>,
}

struct PromptSnapshot {
    prompt_tokens: u64,
    at: DateTime<Utc>,
    signature: CacheSignature,
    cache_telemetry_available: bool,
}

struct LatestCacheUse {
    hit_percent: u8,
    signature: CacheSignature,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CacheMissCause {
    ProviderChanged,
    ModelChanged,
    EffortChanged,
    ModelAndEffortChanged,
    Idle(Duration),
    Unknown,
}

pub(super) struct CacheMissNotice {
    missed_tokens: u64,
    prompt_tokens: u64,
    reusable_prefix_tokens: u64,
    cached_input_tokens: u64,
    cause: CacheMissCause,
    model: Option<String>,
    turn_cost_microusd: Option<u64>,
    cost_basis: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct CacheStatus {
    pub label: String,
    pub warning: bool,
    resend_tokens: Option<u64>,
}

impl CacheDiagnostics {
    pub(super) fn reset(&mut self) {
        *self = Self::default();
    }

    pub(super) fn needs_idle_timer(&self) -> bool {
        self.previous
            .as_ref()
            .is_some_and(|previous| cache_window(previous.signature.provider).is_some())
    }

    pub(super) fn observe(
        &mut self,
        at: DateTime<Utc>,
        signature: CacheSignature,
        usage: CacheUsage<'_>,
    ) -> Option<CacheMissNotice> {
        // ProviderCallUsage keeps uncached input, cache reads, and cache writes
        // in exclusive buckets, so their sum is the full prompt processed by
        // the provider for this turn.
        let prompt_tokens = usage
            .input_tokens
            .saturating_add(usage.cached_input_tokens)
            .saturating_add(usage.cache_creation_input_tokens);
        if prompt_tokens == 0 {
            return None;
        }

        let cache_telemetry_available = matches!(
            signature.provider,
            CodingProvider::Codex | CodingProvider::Claude
        ) || usage.cached_input_tokens > 0
            || usage.cache_creation_input_tokens > 0;
        let had_prior_prompt = self.previous.is_some();
        let notice = self.previous.as_ref().and_then(|previous| {
            if !(cache_telemetry_available || previous.cache_telemetry_available) {
                return None;
            }
            let reusable_prefix_tokens = previous.prompt_tokens.min(prompt_tokens);
            let missed_tokens = reusable_prefix_tokens.saturating_sub(usage.cached_input_tokens);
            if !material_cache_miss(missed_tokens, reusable_prefix_tokens) {
                return None;
            }
            Some(CacheMissNotice {
                missed_tokens,
                prompt_tokens,
                reusable_prefix_tokens,
                cached_input_tokens: usage.cached_input_tokens,
                cause: cache_miss_cause(previous, &signature, at),
                model: signature.model.clone(),
                turn_cost_microusd: usage.cost_microusd,
                cost_basis: usage.cost_basis.to_string(),
            })
        });

        // A first turn has no prior prompt to hit. Recording its natural zero
        // as a cache result makes the idle composer claim a false cache miss.
        if cache_telemetry_available
            && had_prior_prompt
            && let Some(previous) = self.previous.as_ref()
        {
            let reusable_prefix_tokens = previous.prompt_tokens.min(prompt_tokens);
            self.latest = Some(LatestCacheUse {
                // The newly appended user/assistant suffix was never eligible
                // for reuse. Report how much of the prior reusable prefix hit,
                // rather than diluting the percentage with brand-new tokens.
                hit_percent: cache_hit_percent(
                    usage.cached_input_tokens.min(reusable_prefix_tokens),
                    reusable_prefix_tokens,
                ),
                signature: signature.clone(),
            });
        }
        self.previous = Some(PromptSnapshot {
            prompt_tokens,
            at,
            signature,
            cache_telemetry_available,
        });
        notice
    }

    pub(super) fn status(
        &self,
        now: DateTime<Utc>,
        signature: &CacheSignature,
    ) -> Option<CacheStatus> {
        let previous = self.previous.as_ref()?;
        if previous.signature.provider != signature.provider {
            return Some(warning_status(
                "cache cold · provider changed",
                previous.prompt_tokens,
            ));
        }
        let model_changed = previous.signature.model != signature.model;
        let effort_changed = previous.signature.effort != signature.effort;
        if model_changed || effort_changed {
            let changed = match (model_changed, effort_changed) {
                (true, true) => "model + effort changed",
                (true, false) => "model changed",
                (false, true) => "effort changed",
                (false, false) => unreachable!(),
            };
            return Some(warning_status(
                format!("cache cold · {changed}"),
                previous.prompt_tokens,
            ));
        }

        let idle = elapsed(previous.at, now);
        if let Some(window) = cache_window(signature.provider)
            && idle >= window
        {
            return Some(warning_status(
                format!("cache may be cold · {} idle", format_duration(idle)),
                previous.prompt_tokens,
            ));
        }

        self.latest
            .as_ref()
            .filter(|latest| latest.signature == *signature)
            .map(|latest| CacheStatus {
                label: format!("cache {}% hit", latest.hit_percent),
                // This describes the completed turn. A zero hit does not
                // predict another miss: processing that turn can warm the
                // provider cache for the next request.
                warning: false,
                resend_tokens: None,
            })
    }
}

impl CacheStatus {
    pub(super) fn cold_cache_guidance(&self) -> String {
        debug_assert!(self.warning);
        let reason = self
            .label
            .strip_prefix("cache cold · ")
            .or_else(|| self.label.strip_prefix("cache may be cold · "))
            .unwrap_or(&self.label);
        let resend = self.resend_tokens.map_or_else(
            || "resend earlier context for reprocessing".to_string(),
            |tokens| {
                format!(
                    "resend up to {} from the prior prompt for reprocessing",
                    format_tokens(tokens)
                )
            },
        );
        format!(
            "Cold cache: {reason}; the next turn may {resend}. Run /clear first if that \
             context is no longer useful."
        )
    }
}

fn material_cache_miss(missed_tokens: u64, reusable_prefix_tokens: u64) -> bool {
    missed_tokens > CACHE_MISS_NOISE_FLOOR_TOKENS
        && u128::from(missed_tokens).saturating_mul(100)
            >= u128::from(reusable_prefix_tokens)
                .saturating_mul(u128::from(CACHE_MISS_MINIMUM_PRIOR_PREFIX_PERCENT))
}

impl CacheMissNotice {
    pub(super) fn text(&self) -> String {
        let hit_percent = cache_hit_percent(
            self.cached_input_tokens.min(self.reusable_prefix_tokens),
            self.reusable_prefix_tokens,
        );
        let mut facts = vec![
            format!(
                "{} of the prior prompt was reprocessed",
                format_tokens(self.missed_tokens)
            ),
            format!("{hit_percent}% cache hit"),
        ];
        if let Some(model) = self.model.as_deref()
            && let Some(cost) =
                estimate_openai_cache_miss_microusd(model, self.missed_tokens, self.prompt_tokens)
        {
            facts.push(format!(
                "estimated API cache-miss premium {}",
                format_microusd(cost)
            ));
        }
        if let Some(cost) = self.turn_cost_microusd {
            let label = match self.cost_basis.as_str() {
                "provider_reported" => "provider-reported turn cost",
                "estimated_from_pricing" => "estimated API-equivalent turn cost",
                _ => "turn cost",
            };
            facts.push(format!("{label} {}", format_microusd(cost)));
        }

        format!(
            "{}.\nLikely cause: {}.\nIf the earlier conversation is no longer useful, \
             /clear starts a fresh context. /compact keeps a lossy summary and also starts a new \
             cache prefix.",
            facts.join(" · "),
            self.cause.explanation()
        )
    }
}

impl CacheMissCause {
    fn explanation(&self) -> String {
        match self {
            Self::ProviderChanged => "the provider changed".to_string(),
            Self::ModelChanged => "the model changed".to_string(),
            Self::EffortChanged => "reasoning effort changed".to_string(),
            Self::ModelAndEffortChanged => "the model and reasoning effort changed".to_string(),
            Self::Idle(duration) => format!(
                "{} idle exceeded the provider's usual cache window",
                format_duration(*duration)
            ),
            Self::Unknown => {
                "the exact prompt prefix changed, or the provider evicted or rerouted the cache"
                    .to_string()
            }
        }
    }
}

fn cache_miss_cause(
    previous: &PromptSnapshot,
    current: &CacheSignature,
    at: DateTime<Utc>,
) -> CacheMissCause {
    if previous.signature.provider != current.provider {
        return CacheMissCause::ProviderChanged;
    }
    match (
        previous.signature.model != current.model,
        previous.signature.effort != current.effort,
    ) {
        (true, true) => return CacheMissCause::ModelAndEffortChanged,
        (true, false) => return CacheMissCause::ModelChanged,
        (false, true) => return CacheMissCause::EffortChanged,
        (false, false) => {}
    }
    let idle = elapsed(previous.at, at);
    if cache_window(current.provider).is_some_and(|window| idle >= window) {
        return CacheMissCause::Idle(idle);
    }
    CacheMissCause::Unknown
}

fn cache_window(provider: CodingProvider) -> Option<Duration> {
    match provider {
        CodingProvider::Codex => Some(CODEX_CACHE_WINDOW),
        CodingProvider::Claude => Some(CLAUDE_CACHE_WINDOW),
        CodingProvider::OpenCode
        | CodingProvider::Kimi
        | CodingProvider::OpenRouter
        | CodingProvider::OpenAiCompatible => None,
    }
}

fn elapsed(from: DateTime<Utc>, to: DateTime<Utc>) -> Duration {
    to.signed_duration_since(from).to_std().unwrap_or_default()
}

fn cache_hit_percent(cached_tokens: u64, prompt_tokens: u64) -> u8 {
    if prompt_tokens == 0 {
        return 0;
    }
    let percent = u128::from(cached_tokens)
        .saturating_mul(100)
        .checked_div(u128::from(prompt_tokens))
        .unwrap_or_default()
        .min(100);
    percent as u8
}

fn warning_status(label: impl Into<String>, resend_tokens: u64) -> CacheStatus {
    CacheStatus {
        label: label.into(),
        warning: true,
        resend_tokens: Some(resend_tokens),
    }
}

fn format_tokens(tokens: u64) -> String {
    if tokens >= 1_000_000 {
        format!("{:.1}m tokens", tokens as f64 / 1_000_000.0)
    } else if tokens >= 1_000 {
        format!("{:.1}k tokens", tokens as f64 / 1_000.0)
    } else {
        format!("{tokens} tokens")
    }
}

fn format_microusd(microusd: u64) -> String {
    let dollars = microusd as f64 / 1_000_000.0;
    if dollars < 0.01 {
        format!("${dollars:.4}")
    } else {
        format!("${dollars:.2}")
    }
}

fn format_duration(duration: Duration) -> String {
    let minutes = duration.as_secs() / 60;
    if minutes >= 60 {
        let hours = minutes / 60;
        let remainder = minutes % 60;
        if remainder == 0 {
            format!("{hours}h")
        } else {
            format!("{hours}h {remainder}m")
        }
    } else {
        format!("{minutes}m")
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;

    use super::*;

    fn signature(model: &str, effort: &str) -> CacheSignature {
        CacheSignature::new(CodingProvider::Codex, Some(model), Some(effort))
    }

    fn usage(input: u64, cached: u64) -> CacheUsage<'static> {
        usage_with_cache_creation(input, cached, 0)
    }

    fn usage_with_cache_creation(
        input: u64,
        cached: u64,
        cache_creation: u64,
    ) -> CacheUsage<'static> {
        CacheUsage {
            input_tokens: input,
            cached_input_tokens: cached,
            cache_creation_input_tokens: cache_creation,
            cost_microusd: None,
            cost_basis: "unavailable",
        }
    }

    #[test]
    fn observed_miss_reports_idle_and_ignores_new_prompt_suffix() {
        let mut diagnostics = CacheDiagnostics::default();
        let at = Utc::now();
        assert!(
            diagnostics
                .observe(at, signature("gpt-5.6-sol", "high"), usage(1_000, 99_000))
                .is_none()
        );

        let notice = diagnostics
            .observe(
                at + TimeDelta::minutes(31),
                signature("gpt-5.6-sol", "high"),
                usage(101_000, 0),
            )
            .expect("observed miss");

        assert_eq!(notice.missed_tokens, 100_000);
        assert_eq!(
            notice.cause,
            CacheMissCause::Idle(Duration::from_secs(31 * 60))
        );
    }

    #[test]
    fn high_prefix_reuse_is_not_mislabeled_as_a_cache_miss() {
        let mut diagnostics = CacheDiagnostics::default();
        let at = Utc::now();
        assert!(
            diagnostics
                .observe(at, signature("gpt-5.6-sol", "high"), usage(2_123, 140_032),)
                .is_none()
        );

        let notice = diagnostics.observe(
            at + TimeDelta::seconds(109),
            signature("gpt-5.6-sol", "high"),
            usage(3_137, 141_056),
        );

        assert!(notice.is_none());
    }

    #[test]
    fn cache_hit_percentage_excludes_the_new_prompt_suffix() {
        let mut diagnostics = CacheDiagnostics::default();
        let at = Utc::now();
        diagnostics.observe(at, signature("gpt-5.6-sol", "high"), usage(1_000, 99_000));
        diagnostics.observe(
            at + TimeDelta::minutes(1),
            signature("gpt-5.6-sol", "high"),
            usage(51_000, 100_000),
        );

        let status = diagnostics
            .status(
                at + TimeDelta::minutes(1),
                &signature("gpt-5.6-sol", "high"),
            )
            .expect("measured cache status");
        assert_eq!(status.label, "cache 100% hit");
    }

    #[test]
    fn first_turn_does_not_claim_a_zero_percent_cache_hit() {
        let mut diagnostics = CacheDiagnostics::default();
        let at = Utc::now();
        diagnostics.observe(
            at,
            signature("gpt-5.6-sol", "high"),
            usage_with_cache_creation(1_000, 0, 99_000),
        );

        assert!(
            diagnostics
                .status(at, &signature("gpt-5.6-sol", "high"))
                .is_none()
        );
    }

    #[test]
    fn measured_zero_hit_does_not_predict_another_cold_turn() {
        let mut diagnostics = CacheDiagnostics::default();
        let at = Utc::now();
        diagnostics.observe(
            at,
            signature("gpt-5.6-sol", "high"),
            usage_with_cache_creation(1_000, 49_000, 50_000),
        );
        diagnostics.observe(
            at + TimeDelta::minutes(1),
            signature("gpt-5.6-sol", "high"),
            usage(100_000, 0),
        );

        let status = diagnostics
            .status(
                at + TimeDelta::minutes(1),
                &signature("gpt-5.6-sol", "high"),
            )
            .expect("measured cache status");
        assert_eq!(status.label, "cache 0% hit");
        assert!(!status.warning);
    }

    #[test]
    fn cold_cache_guidance_includes_resend_token_count() {
        let mut diagnostics = CacheDiagnostics::default();
        let at = Utc::now();
        diagnostics.observe(
            at,
            signature("gpt-5.6-sol", "high"),
            usage_with_cache_creation(1_000, 49_000, 50_000),
        );

        let status = diagnostics
            .status(at, &signature("gpt-5.6-sol", "low"))
            .expect("cold cache status");
        assert!(status.warning);
        assert_eq!(
            status.cold_cache_guidance(),
            "Cold cache: effort changed; the next turn may resend up to 100.0k tokens from the \
             prior prompt for reprocessing. Run /clear first if that context is no longer useful."
        );
    }

    #[test]
    fn model_and_effort_changes_take_precedence_over_idle() {
        let mut diagnostics = CacheDiagnostics::default();
        let at = Utc::now();
        diagnostics.observe(at, signature("old", "low"), usage(1_000, 99_000));

        let status = diagnostics
            .status(at + TimeDelta::hours(1), &signature("new", "high"))
            .expect("cache status");
        assert!(status.warning);
        assert!(status.label.contains("model + effort changed"));

        let notice = diagnostics
            .observe(
                at + TimeDelta::hours(1),
                signature("new", "high"),
                usage(100_000, 0),
            )
            .expect("observed miss");
        assert_eq!(notice.cause, CacheMissCause::ModelAndEffortChanged);
    }

    #[test]
    fn noise_floor_and_providers_without_cache_telemetry_do_not_false_alarm() {
        let mut diagnostics = CacheDiagnostics::default();
        let at = Utc::now();
        let unknown = CacheSignature::new(CodingProvider::Kimi, Some("model"), None);
        diagnostics.observe(at, unknown.clone(), usage(50_000, 0));
        assert!(
            diagnostics
                .observe(at + TimeDelta::minutes(1), unknown, usage(50_500, 0))
                .is_none()
        );

        let mut diagnostics = CacheDiagnostics::default();
        diagnostics.observe(at, signature("gpt-5.6-sol", "high"), usage(500, 1_500));
        assert!(
            diagnostics
                .observe(
                    at + TimeDelta::minutes(1),
                    signature("gpt-5.6-sol", "high"),
                    usage(1_500, 1_000),
                )
                .is_none()
        );
    }

    #[test]
    fn reset_for_clear_or_compaction_forgets_the_old_prefix() {
        let mut diagnostics = CacheDiagnostics::default();
        let at = Utc::now();
        diagnostics.observe(at, signature("gpt-5.6-sol", "high"), usage(1_000, 99_000));
        diagnostics.reset();

        assert!(
            diagnostics
                .observe(
                    at + TimeDelta::minutes(31),
                    signature("gpt-5.6-sol", "high"),
                    usage(100_000, 0),
                )
                .is_none()
        );
    }
}
