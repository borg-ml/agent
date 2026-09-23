use std::time::Duration;

use borg_provider::provider::estimate_cache_miss_microusd;
use borg_remote::CodingProvider;
use chrono::{DateTime, Utc};

// Provider cache counters are block-aligned and the reusable prefix naturally
// ends before the newly appended assistant/user tail. Treating any four-digit
// uncached remainder as a miss produces false alarms on otherwise excellent
// cache reuse (for example 141,056 cached tokens from a 142,155-token prior
// prompt). A warning should describe a materially lost prefix, not rounding.
const CACHE_MISS_NOISE_FLOOR_TOKENS: u64 = 2_048;
const CACHE_MISS_MINIMUM_PRIOR_PREFIX_PERCENT: u64 = 5;
// These are warning thresholds, not expiry guarantees. OpenAI documents a
// 30-minute minimum TTL for GPT-5.6 prompt-cache breakpoints. Claude Code
// requests a one-hour TTL automatically for subscription auth; its five-minute
// default applies to API-key and third-party billing routes. The display
// projection currently identifies the native Claude route, not the auth mode,
// so use the subscription-safe one-hour threshold to avoid false warnings.
const CODEX_CACHE_WINDOW: Duration = Duration::from_secs(30 * 60);
const CLAUDE_CACHE_WINDOW: Duration = Duration::from_secs(60 * 60);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct CacheSignature {
    provider: CodingProvider,
    model: Option<String>,
    effort: Option<String>,
    effort_can_reuse_cache: bool,
}

impl CacheSignature {
    pub(super) fn new(provider: CodingProvider, model: Option<&str>, effort: Option<&str>) -> Self {
        Self {
            provider,
            model: model.map(str::to_string),
            effort: effort.map(str::to_string),
            effort_can_reuse_cache: false,
        }
    }

    pub(super) fn for_session(
        provider: CodingProvider,
        model: Option<&str>,
        effort: Option<&str>,
        claude_direct_auth: bool,
    ) -> Self {
        let effort_can_reuse_cache = claude_direct_auth
            && provider == CodingProvider::Claude
            && matches!(model, Some("claude-opus-5-5" | "claude-fable-5-1"));
        Self {
            effort_can_reuse_cache,
            ..Self::new(provider, model, effort)
        }
    }

    fn same_cache_identity(&self, other: &Self) -> bool {
        self.provider == other.provider
            && self.model == other.model
            && (self.effort == other.effort
                || (self.effort_can_reuse_cache && other.effort_can_reuse_cache))
    }
}

pub(super) struct CacheUsage<'a> {
    pub input_tokens: u64,
    pub cached_input_tokens: u64,
    pub cache_creation_input_tokens: u64,
    pub context_tokens: Option<u64>,
    pub cost_microusd: Option<u64>,
    pub cost_basis: &'a str,
    pub provider_context_reused: Option<bool>,
}

#[derive(Default)]
pub(super) struct CacheDiagnostics {
    previous: Option<PromptSnapshot>,
    latest: Option<LatestCacheUse>,
}

struct PromptSnapshot {
    prompt_tokens: u64,
    cached_input_tokens: u64,
    reusable_context_tokens: Option<u64>,
    at: DateTime<Utc>,
    signature: CacheSignature,
    cache_telemetry_available: bool,
}

impl PromptSnapshot {
    /// Did this snapshot describe a single provider request? A turn that makes
    /// several model/tool rounds reports the sum of their counters, and a sum
    /// of cache reads can exceed the turn's real context. Comparing such a
    /// total with the next request's cached prefix is meaningless.
    fn single_request(&self) -> bool {
        // The context gauge is what proves these counters describe one request.
        // A turn that sums several model rounds reports cached reads far larger
        // than its real context, and a provider that omits the gauge leaves no
        // way to tell a summed turn from a single request. Absent telemetry is
        // unknown, not a licence to compare incomparable totals.
        self.reusable_context_tokens
            .is_some_and(|context| self.cached_input_tokens <= context)
    }
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
    BorgReplayedContext,
    Idle(Duration),
    Unknown,
}

pub(super) struct CacheMissNotice {
    missed_tokens: u64,
    prompt_tokens: u64,
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
        self.previous.as_ref().is_some_and(|previous| {
            previous.cache_telemetry_available
                && cache_window(previous.signature.provider).is_some()
        })
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

        // A zero counter is ambiguous: some subscription/runtime versions
        // omit cache fields entirely, while a real cold request also reports
        // zero. Start warning only after this lane has actually exposed a
        // positive cache-read/write counter, then carry that knowledge across
        // later zero-hit turns.
        let cache_telemetry_available = (usage.cached_input_tokens > 0
            || usage.cache_creation_input_tokens > 0)
            || self
                .previous
                .as_ref()
                .is_some_and(|previous| previous.cache_telemetry_available);
        let had_prior_prompt = self.previous.is_some();
        let notice = self.previous.as_ref().and_then(|previous| {
            if !(cache_telemetry_available || previous.cache_telemetry_available) {
                return None;
            }
            // A healthy follow-up appends to a stable prefix: its uncached
            // `input_tokens` are the new tail, not the prior prompt. Comparing
            // that tail with the previous full snapshot turns every ordinary
            // follow-up into another loud miss card, so announce only at a real
            // boundary (signature change, cache expiry, or a measured Borg
            // replay) or when the cached prefix itself shrank.
            let same_signature = previous.signature.same_cache_identity(&signature);
            let within_cache_window = cache_window(signature.provider)
                .is_none_or(|window| elapsed(previous.at, at) < window);
            let boundary = !same_signature
                || !within_cache_window
                || usage.provider_context_reused == Some(false);
            let missed_tokens = if boundary {
                let reusable_prefix_tokens = previous.prompt_tokens.min(prompt_tokens);
                let missed = reusable_prefix_tokens.saturating_sub(usage.cached_input_tokens);
                if !material_cache_miss(missed, reusable_prefix_tokens) {
                    return None;
                }
                missed
            } else {
                // No boundary: the prior prefix should still be cached, so a
                // material drop in cached reads is a real eviction. Only trust
                // it when both snapshots are single requests; a multi-round
                // turn reports summed counters that cannot be compared.
                let single_request = usage
                    .context_tokens
                    .is_none_or(|context| usage.cached_input_tokens <= context);
                if !single_request || !previous.single_request() {
                    return None;
                }
                let lost = previous
                    .cached_input_tokens
                    .saturating_sub(usage.cached_input_tokens);
                if !material_cache_miss(lost, previous.cached_input_tokens) {
                    return None;
                }
                lost
            };
            Some(CacheMissNotice {
                missed_tokens,
                prompt_tokens,
                cached_input_tokens: usage.cached_input_tokens,
                cause: cache_miss_cause(previous, &signature, at, usage.provider_context_reused),
                model: signature.model.clone(),
                turn_cost_microusd: usage.cost_microusd,
                cost_basis: usage.cost_basis.to_string(),
            })
        });

        // A first turn has no prior prompt to hit. Recording its natural zero
        // as a cache result makes the idle composer claim a false cache miss.
        if cache_telemetry_available && had_prior_prompt {
            self.latest = Some(LatestCacheUse {
                // Match provider-reported cache hit rates (and ZCode's
                // latestHitRate): cached prompt tokens divided by the full
                // prompt processed for this request.
                hit_percent: cache_hit_percent(
                    usage.cached_input_tokens.min(prompt_tokens),
                    prompt_tokens,
                ),
                signature: signature.clone(),
            });
        }
        self.previous = Some(PromptSnapshot {
            prompt_tokens,
            cached_input_tokens: usage.cached_input_tokens,
            // A provider turn can contain several model/tool-loop calls. Its
            // processed-token total grows once per call and can be many times
            // larger than the context that a cold request would resend.
            reusable_context_tokens: usage.context_tokens,
            at,
            signature,
            cache_telemetry_available,
        });
        notice
    }

    pub(super) fn update_context_tokens(&mut self, tokens: u64) {
        if let Some(previous) = self.previous.as_mut() {
            previous.reusable_context_tokens = Some(tokens);
        }
    }

    pub(super) fn status(
        &self,
        now: DateTime<Utc>,
        signature: &CacheSignature,
    ) -> Option<CacheStatus> {
        let previous = self.previous.as_ref()?;
        if !previous.cache_telemetry_available {
            return None;
        }
        if previous.signature.provider != signature.provider {
            return Some(warning_status(
                "cache cold · provider changed",
                previous.reusable_context_tokens,
            ));
        }
        let model_changed = previous.signature.model != signature.model;
        let effort_changed = previous.signature.effort != signature.effort;
        if effort_changed
            && !model_changed
            && previous.signature.effort_can_reuse_cache
            && signature.effort_can_reuse_cache
        {
            // The next turn may reuse its prefix; its first usage report will
            // tell us whether it did. The previous hit rate is stale here.
            return None;
        }
        if model_changed || effort_changed {
            let changed = match (model_changed, effort_changed) {
                (true, true) => "model + effort changed",
                (true, false) => "model changed",
                (false, true) => "effort changed",
                (false, false) => unreachable!(),
            };
            return Some(warning_status(
                format!("cache cold · {changed}"),
                previous.reusable_context_tokens,
            ));
        }

        let idle = elapsed(previous.at, now);
        if let Some(window) = cache_window(signature.provider)
            && idle >= window
        {
            return Some(warning_status(
                format!("cache may be cold · {} idle", format_duration(idle)),
                previous.reusable_context_tokens,
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
        let qualification = if self.label.starts_with("cache may be cold · ") {
            " Review before sending; provider retention is not a guarantee."
        } else {
            ""
        };
        format!(
            "Cold cache: {reason}; the next turn may {resend}.{qualification} Run /clear first if that \
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
            self.cached_input_tokens.min(self.prompt_tokens),
            self.prompt_tokens,
        );
        let mut facts = vec![
            format!(
                "{} of the prior prompt was reprocessed",
                format_tokens(self.missed_tokens)
            ),
            format!("{hit_percent}% cache hit"),
        ];
        // Only explicit API billing bases justify a dollar projection. Native
        // subscription adapters report token counters for observability, and
        // legacy/unknown telemetry must remain cost-free rather than being
        // guessed into an API bill.
        let api_billing_basis = matches!(
            self.cost_basis.as_str(),
            "provider_reported" | "estimated_from_pricing"
        );
        if api_billing_basis
            && let Some(model) = self.model.as_deref()
            && let Some(cost) = estimate_cache_miss_microusd(None, model, self.missed_tokens)
        {
            facts.push(format!(
                "estimated API cache-miss premium {}",
                format_microusd(cost)
            ));
        }
        if api_billing_basis && let Some(cost) = self.turn_cost_microusd {
            let label = match self.cost_basis.as_str() {
                "provider_reported" => "provider-reported turn cost",
                "estimated_from_pricing" => "estimated API-equivalent turn cost",
                _ => return self.text_without_cost(facts),
            };
            facts.push(format!("{label} {}", format_microusd(cost)));
        }

        self.text_without_cost(facts)
    }

    fn text_without_cost(&self, facts: Vec<String>) -> String {
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
            Self::BorgReplayedContext => {
                "Borg had to replay the durable journal into a new provider context".to_string()
            }
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
    provider_context_reused: Option<bool>,
) -> CacheMissCause {
    if previous.signature.provider != current.provider {
        return CacheMissCause::ProviderChanged;
    }
    let effort_changed = previous.signature.effort != current.effort
        && !(previous.signature.effort_can_reuse_cache && current.effort_can_reuse_cache);
    match (previous.signature.model != current.model, effort_changed) {
        (true, true) => return CacheMissCause::ModelAndEffortChanged,
        (true, false) => return CacheMissCause::ModelChanged,
        (false, true) => return CacheMissCause::EffortChanged,
        (false, false) => {}
    }
    if provider_context_reused == Some(false) {
        return CacheMissCause::BorgReplayedContext;
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
        // The API lane sends no cache breakpoints yet, so there is no window to
        // explain a miss against, and guessing one would misreport the cause.
        CodingProvider::Anthropic
        | CodingProvider::OpenCode
        | CodingProvider::Grok
        | CodingProvider::Muse
        | CodingProvider::Kimi
        | CodingProvider::Glm
        | CodingProvider::Qwen
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

fn warning_status(label: impl Into<String>, resend_tokens: Option<u64>) -> CacheStatus {
    CacheStatus {
        label: label.into(),
        warning: true,
        resend_tokens,
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

    fn claude_signature(model: &str) -> CacheSignature {
        CacheSignature::new(CodingProvider::Claude, Some(model), None)
    }

    fn usage(input: u64, cached: u64) -> CacheUsage<'static> {
        usage_with_cache_creation(input, cached, 0)
    }

    fn usage_with_cache_creation(
        input: u64,
        cached: u64,
        cache_creation: u64,
    ) -> CacheUsage<'static> {
        usage_with_context_reuse(input, cached, cache_creation, None)
    }

    fn usage_with_context_reuse(
        input: u64,
        cached: u64,
        cache_creation: u64,
        provider_context_reused: Option<bool>,
    ) -> CacheUsage<'static> {
        CacheUsage {
            input_tokens: input,
            cached_input_tokens: cached,
            cache_creation_input_tokens: cache_creation,
            context_tokens: None,
            cost_microusd: None,
            cost_basis: "unavailable",
            provider_context_reused,
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
    fn idle_warning_is_available_before_the_next_turn() {
        let mut diagnostics = CacheDiagnostics::default();
        let at = Utc::now();
        let signature = claude_signature("claude-opus-5");
        diagnostics.observe(at, signature.clone(), usage(1_000, 99_000));
        diagnostics.observe(
            at + TimeDelta::minutes(1),
            signature.clone(),
            usage(0, 100_000),
        );

        let status = diagnostics
            .status(at + TimeDelta::minutes(8), &signature)
            .expect("measured cache status");
        assert!(!status.warning);
        assert_eq!(status.label, "cache 100% hit");

        let status = diagnostics
            .status(at + TimeDelta::minutes(61), &signature)
            .expect("idle cache status");
        assert!(status.warning);
        assert_eq!(status.label, "cache may be cold · 1h idle");
        assert!(status.cold_cache_guidance().contains("before sending"));
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
    fn cache_hit_percentage_matches_the_full_provider_prompt() {
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
        assert_eq!(status.label, "cache 66% hit");
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
        let mut measured = usage_with_cache_creation(1_000, 49_000, 50_000);
        measured.context_tokens = Some(100_000);
        diagnostics.observe(at, signature("gpt-5.6-sol", "high"), measured);
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
        let mut measured = usage_with_cache_creation(1_000, 49_000, 50_000);
        measured.context_tokens = Some(100_000);
        diagnostics.observe(at, signature("gpt-5.6-sol", "high"), measured);

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
    fn cold_cache_guidance_uses_current_context_not_cumulative_provider_work() {
        let mut diagnostics = CacheDiagnostics::default();
        let at = Utc::now();
        let mut processed = usage_with_cache_creation(410_000, 390_000, 0);
        processed.context_tokens = Some(100_000);
        diagnostics.observe(at, signature("gpt-5.6-sol", "high"), processed);

        let status = diagnostics
            .status(at, &signature("gpt-5.6-sol", "low"))
            .expect("cold cache status");
        let guidance = status.cold_cache_guidance();
        assert!(guidance.contains("100.0k tokens"), "{guidance}");
        assert!(!guidance.contains("800.0k tokens"), "{guidance}");
    }

    #[test]
    fn resend_size_is_unknown_without_context_and_tracks_compaction() {
        let mut diagnostics = CacheDiagnostics::default();
        let at = Utc::now();
        diagnostics.observe(
            at,
            signature("gpt-5.6-sol", "high"),
            usage(1_200_000, 1_000_000),
        );
        let changed = signature("gpt-5.6-sol", "low");
        let status = diagnostics.status(at, &changed).unwrap();
        assert_eq!(status.resend_tokens, None);
        assert!(!status.cold_cache_guidance().contains("2.2m"));
        diagnostics.update_context_tokens(80_000);
        assert_eq!(
            diagnostics.status(at, &changed).unwrap().resend_tokens,
            Some(80_000)
        );
        diagnostics.update_context_tokens(12_000);
        assert_eq!(
            diagnostics.status(at, &changed).unwrap().resend_tokens,
            Some(12_000)
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
    fn direct_claude_effort_switch_waits_for_measured_cache_usage() {
        let at = Utc::now();
        for model in ["claude-opus-5-5", "claude-fable-5-1"] {
            let at_medium = CacheSignature::for_session(
                CodingProvider::Claude,
                Some(model),
                Some("medium"),
                true,
            );
            let at_xhigh = CacheSignature::for_session(
                CodingProvider::Claude,
                Some(model),
                Some("xhigh"),
                true,
            );
            let mut diagnostics = CacheDiagnostics::default();
            let mut warm = usage(1_000, 99_000);
            warm.context_tokens = Some(100_000);
            diagnostics.observe(at, at_medium.clone(), warm);
            let mut warm = usage(1_000, 99_000);
            warm.context_tokens = Some(100_000);
            diagnostics.observe(at + TimeDelta::seconds(1), at_medium.clone(), warm);

            assert!(
                diagnostics
                    .status(at + TimeDelta::seconds(2), &at_xhigh)
                    .is_none(),
                "{model}: prior cache measurement is stale after an effort switch"
            );
            let mut warm = usage(1_000, 99_000);
            warm.context_tokens = Some(100_000);
            assert!(
                diagnostics
                    .observe(at + TimeDelta::seconds(3), at_xhigh.clone(), warm)
                    .is_none(),
                "{model}: an effort change alone is not a measured miss"
            );
            assert_eq!(
                diagnostics
                    .status(at + TimeDelta::seconds(3), &at_xhigh)
                    .as_ref()
                    .map(|status| status.label.as_str()),
                Some("cache 99% hit")
            );

            let mut cold = usage(100_000, 0);
            cold.context_tokens = Some(100_000);
            assert!(
                diagnostics
                    .status(at + TimeDelta::seconds(4), &at_medium)
                    .is_none()
            );
            let notice = diagnostics
                .observe(at + TimeDelta::seconds(4), at_medium, cold)
                .expect("measured cache loss must still warn");
            assert_eq!(
                notice.cause,
                CacheMissCause::Unknown,
                "{model}: do not misattribute the measured loss to effort"
            );

            let unknown_route_medium = CacheSignature::for_session(
                CodingProvider::Claude,
                Some(model),
                Some("medium"),
                false,
            );
            let unknown_route_xhigh = CacheSignature::for_session(
                CodingProvider::Claude,
                Some(model),
                Some("xhigh"),
                false,
            );
            assert_ne!(unknown_route_medium, unknown_route_xhigh);
        }
    }

    #[test]
    fn noise_floor_and_providers_without_cache_telemetry_do_not_false_alarm() {
        let mut diagnostics = CacheDiagnostics::default();
        let at = Utc::now();
        let unknown = CacheSignature::new(CodingProvider::OpenRouter, Some("model"), None);
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

        let mut diagnostics = CacheDiagnostics::default();
        let codex = signature("gpt-5.6-sol", "high");
        diagnostics.observe(at, codex.clone(), usage(50_000, 0));
        assert!(
            diagnostics
                .observe(at + TimeDelta::minutes(1), codex, usage(50_500, 0))
                .is_none()
        );
    }

    #[test]
    fn a_growing_single_request_prefix_does_not_report_the_uncached_tail() {
        let mut diagnostics = CacheDiagnostics::default();
        let at = Utc::now();
        let signature = signature("gpt-5.6-sol", "high");
        // Context 100k with a stable 98k cached prefix.
        let mut first = usage_with_context_reuse(2_000, 98_000, 0, None);
        first.context_tokens = Some(100_000);
        assert!(diagnostics.observe(at, signature.clone(), first).is_none());

        // Ordinary follow-up: the same prefix plus a 20k tail. The tail is new
        // work, not a reprocessed prefix, so no miss card may fire.
        let mut second = usage_with_context_reuse(20_000, 100_000, 0, None);
        second.context_tokens = Some(120_000);
        assert!(
            diagnostics
                .observe(at + TimeDelta::minutes(1), signature, second)
                .is_none(),
            "an appended uncached tail is not a lost cache prefix"
        );
    }

    #[test]
    fn a_shrinking_cached_prefix_is_reported_without_a_boundary() {
        let mut diagnostics = CacheDiagnostics::default();
        let at = Utc::now();
        let signature = signature("gpt-5.6-sol", "high");
        let mut first = usage_with_context_reuse(2_000, 98_000, 0, None);
        first.context_tokens = Some(100_000);
        diagnostics.observe(at, signature.clone(), first);

        // Same signature, seconds later, but the cached prefix collapsed to a
        // single block. That is a real eviction and must still be announced.
        let mut second = usage_with_cache_creation(100_000, 1_792, 0);
        second.context_tokens = Some(101_792);
        let notice = diagnostics
            .observe(at + TimeDelta::seconds(5), signature, second)
            .expect("a lost cached prefix is a real miss");
        assert_eq!(notice.missed_tokens, 96_208);
    }

    #[test]
    fn an_aggregated_multi_round_snapshot_is_not_compared_with_one_request() {
        let mut diagnostics = CacheDiagnostics::default();
        let at = Utc::now();
        let signature = signature("gpt-5.6-sol", "high");
        // A turn that summed several model rounds: its cached reads far exceed
        // its real context, so the snapshot is not a single request.
        let mut aggregate = usage_with_context_reuse(5_000, 5_000_000, 0, None);
        aggregate.context_tokens = Some(120_000);
        diagnostics.observe(at, signature.clone(), aggregate);

        let mut next = usage_with_cache_creation(120_000, 1_792, 0);
        next.context_tokens = Some(121_792);
        assert!(
            diagnostics
                .observe(at + TimeDelta::seconds(5), signature, next)
                .is_none(),
            "summed multi-round counters cannot be compared with one request"
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

    #[test]
    fn a_short_interval_is_not_reported_as_idle_expiry() {
        let mut diagnostics = CacheDiagnostics::default();
        let at = Utc::now();
        let signature = claude_signature("claude-opus-5");
        // A context gauge proves each snapshot is a single request, which is
        // what lets the short-interval eviction be compared at all.
        let mut first = usage(1_000, 99_000);
        first.context_tokens = Some(100_000);
        diagnostics.observe(at, signature.clone(), first);
        let mut second = usage(100_000, 0);
        second.context_tokens = Some(100_000);

        let notice = diagnostics
            .observe(at + TimeDelta::seconds(2), signature, second)
            .expect("observed short-interval miss");
        assert_eq!(notice.cause, CacheMissCause::Unknown);
        assert!(!notice.text().contains("idle exceeded"));
    }

    /// The OpenCode CLI route reports per-turn sums across model rounds and no
    /// context gauge. Consecutive turns can differ by millions of cached reads
    /// purely because they ran a different number of tool rounds; that is not an
    /// eviction, and comparing the sums raised a false "Prompt cache miss" card
    /// on every turn.
    #[test]
    fn a_multi_round_snapshot_without_a_context_gauge_is_not_reported_as_a_miss() {
        let mut diagnostics = CacheDiagnostics::default();
        let at = Utc::now();
        let signature = CacheSignature::new(
            CodingProvider::OpenCode,
            Some("opencode-go/deepseek-v4.1-flash"),
            None,
        );

        assert!(
            diagnostics
                .observe(at, signature.clone(), usage(842_493, 9_462_656))
                .is_none()
        );
        assert!(
            diagnostics
                .observe(
                    at + TimeDelta::minutes(1),
                    signature.clone(),
                    usage(221_309, 6_119_680),
                )
                .is_none()
        );
        assert!(
            diagnostics
                .observe(
                    at + TimeDelta::minutes(2),
                    signature,
                    usage(243_802, 2_437_632),
                )
                .is_none()
        );
    }

    #[test]
    fn subscription_usage_never_looks_like_an_api_bill() {
        let mut diagnostics = CacheDiagnostics::default();
        let at = Utc::now();
        let signature = claude_signature("claude-opus-5");
        diagnostics.observe(at, signature.clone(), usage(1_000, 99_000));
        let notice = diagnostics
            .observe(
                at + TimeDelta::seconds(2),
                signature,
                CacheUsage {
                    input_tokens: 100_000,
                    cached_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    context_tokens: None,
                    cost_microusd: Some(1_100_000),
                    cost_basis: "subscription_equivalent",
                    provider_context_reused: Some(false),
                },
            )
            .expect("observed subscription miss");
        let text = notice.text();
        assert!(!text.contains("$"));
        assert!(!text.contains("turn cost"));
        assert!(!text.contains("API cache-miss premium"));
    }

    #[test]
    fn reused_provider_context_does_not_repeat_miss_cards() {
        let mut diagnostics = CacheDiagnostics::default();
        let at = Utc::now();
        let signature = signature("gpt-5.6-sol", "high");

        diagnostics.observe(
            at,
            signature.clone(),
            CacheUsage {
                input_tokens: 1_000,
                cached_input_tokens: 99_000,
                cache_creation_input_tokens: 0,
                context_tokens: None,
                cost_microusd: None,
                cost_basis: "unavailable",
                provider_context_reused: Some(true),
            },
        );

        let replay = diagnostics
            .observe(
                at + TimeDelta::seconds(1),
                signature.clone(),
                CacheUsage {
                    input_tokens: 101_000,
                    cached_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    context_tokens: None,
                    cost_microusd: None,
                    cost_basis: "unavailable",
                    provider_context_reused: Some(false),
                },
            )
            .expect("the first post-resume replay is worth reporting");
        assert_eq!(replay.cause, CacheMissCause::BorgReplayedContext);

        assert!(
            diagnostics
                .observe(
                    at + TimeDelta::seconds(2),
                    signature.clone(),
                    usage_with_context_reuse(102_000, 0, 0, Some(true)),
                )
                .is_none()
        );
        assert!(
            diagnostics
                .observe(
                    at + TimeDelta::seconds(3),
                    signature.clone(),
                    usage_with_context_reuse(103_000, 0, 0, Some(true)),
                )
                .is_none()
        );

        let status = diagnostics
            .status(at + TimeDelta::seconds(3), &signature)
            .expect("measured cache status");
        assert_eq!(status.label, "cache 0% hit");
    }

    #[test]
    fn reused_context_still_reports_signature_and_idle_boundaries() {
        let mut diagnostics = CacheDiagnostics::default();
        let at = Utc::now();
        let original = signature("gpt-5.6-sol", "high");
        diagnostics.observe(
            at,
            original.clone(),
            usage_with_context_reuse(1_000, 99_000, 0, Some(true)),
        );

        let changed = diagnostics
            .observe(
                at + TimeDelta::seconds(1),
                signature("gpt-5.6-sol", "low"),
                usage_with_context_reuse(100_000, 0, 0, Some(true)),
            )
            .expect("effort change is a cache boundary");
        assert_eq!(changed.cause, CacheMissCause::EffortChanged);

        let idle = diagnostics
            .observe(
                at + TimeDelta::minutes(31),
                signature("gpt-5.6-sol", "low"),
                usage_with_context_reuse(101_000, 0, 0, Some(true)),
            )
            .expect("cache expiry is a cache boundary");
        assert_eq!(
            idle.cause,
            CacheMissCause::Idle(Duration::from_secs(31 * 60 - 1))
        );
    }
}
