//! Keeping a prompt cache entry alive between requests.
//!
//! A provider drops a cached prefix after a period of inactivity, so the first
//! request after a long tool run reprocesses the whole conversation at full
//! input price. Warming re-sends the last request with a minimal output budget
//! shortly before the entry expires: a cache read buys another lifetime.
//!
//! Three rules shape everything here:
//!
//! * **Never fake it.** A route with no documented cache lifetime or no known
//!   prices reports [`Ineligible`] and arms no timer.
//! * **Never touch the conversation.** A refresh replays the request verbatim
//!   and discards the reply, so no message is emitted and no returned tool
//!   call runs. Its cost is still recorded.
//! * **Never outlive its warrant.** A context change, a model switch and two
//!   fixed horizons each stop warming, and a refresh never extends them.
//!
//! Economics follow pi's: warm when expected avoided miss cost minus the
//! refresh's own cost clears a threshold, at 100% continuation while the agent
//! runs and a flat 15% while idle.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
pub(crate) use borg_core::warming::CacheWarmingMode;
use borg_provider::ProviderCallUsage;
use borg_provider::provider::{ModelTurnRequest, ProviderCallError};
use serde_json::json;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::SessionEventKind;

/// Cancellation for idle runs handed off to outlive their turn.
///
/// Keyed by session because the supersede check inside a run compares against
/// a counter owned by its own warmer, and every turn builds a new warmer.
/// Without this, a session in idle mode accumulates one paid refresh loop per
/// turn, each keeping alive a prefix that has already been replaced.
static DETACHED_IDLE_RUNS: LazyLock<Mutex<HashMap<Uuid, (u64, CancellationToken)>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Distinguishes one run from its replacement. A run that ends on its own must
/// drop only its own entry: by then a newer turn may already have registered a
/// live run under the same session, and removing that would leak the very loop
/// this map exists to stop.
static NEXT_RUN_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Stop the idle run this session left behind, if it has one.
pub(crate) fn cancel_detached_idle_run(session_id: Uuid) {
    let entry = DETACHED_IDLE_RUNS
        .lock()
        .ok()
        .and_then(|mut runs| runs.remove(&session_id));
    if let Some((_, token)) = entry {
        token.cancel();
    }
}

/// Drop a finished run's registration, unless a newer run already replaced it.
fn release_detached_idle_run(session_id: Uuid, run_id: u64) {
    if let Ok(mut runs) = DETACHED_IDLE_RUNS.lock()
        && runs.get(&session_id).is_some_and(|(id, _)| *id == run_id)
    {
        runs.remove(&session_id);
    }
}

/// Streaming warming never continues past this long after the real request
/// that started it.
const MAX_WARMING_AGE: Duration = Duration::from_secs(60 * 60);
/// Idle warming uses a shorter horizon because a continuation estimate gets
/// less reliable the longer nobody has typed anything.
const MAX_IDLE_WARMING_AGE: Duration = Duration::from_secs(30 * 60);
/// A refresh is sent only when it is expected to save at least this much.
/// $0.05, in the microdollars the rest of Borg's usage accounting uses.
const MINIMUM_EXPECTED_SAVINGS_MICROUSD: i64 = 50_000;
/// Chance that a real request arrives before the entry expires while the agent
/// sits idle. A fixed constant, not a per-session estimate: pi measured that
/// per-session estimates were not better than this.
const IDLE_CONTINUATION_PERCENT: u32 = 15;
/// Refreshes are scheduled at this share of the cache lifetime...
const WARM_AT_PERCENT_OF_TTL: u32 = 90;
/// ...while always leaving at least this much margin before expiry, so a slow
/// refresh still lands inside the window it is trying to extend.
const WARM_SAFETY_MARGIN: Duration = Duration::from_secs(10);

/// Why a session is not being warmed. Each variant is surfaced verbatim
/// through [`CacheWarmingStatus::reason`], because "not warming this" and
/// "warming is running" are different things to be told.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Ineligible {
    /// The operator turned warming off.
    ModeOff,
    /// This provider never reaches Borg's native model client, so there is no
    /// request for Borg to replay. Claude and the CLI-owned OpenCode and Codex
    /// routes keep their conversation inside the provider's own process.
    RouteNotNative,
    /// No documented prompt cache lifetime for this model, so there is no
    /// defensible moment to refresh at.
    CacheLifetimeUnknown,
    /// Borg knows no prices for this model and the last real call reported no
    /// cost, so a refresh cannot be shown to be worth sending.
    EconomicsUnavailable,
    /// The route bills a subscription. A refresh consumes quota, and the
    /// API prices Borg knows are not what the user pays, so the saving a
    /// decision would be justified by is not a real number here.
    #[cfg(feature = "subscription-adapters")]
    SubscriptionQuota,
    /// Extended thinking is enabled on this route with a thinking budget the
    /// provider keys the cached prefix on. A refresh replays the request under
    /// a minimal output cap, which cannot reproduce that budget, so the replay
    /// would not refresh the entry the next real request reads -- and the
    /// model could still spend thousands of tokens thinking.
    ThinkingBudgetNotReplayable,
}

impl Ineligible {
    pub(crate) fn reason(&self) -> String {
        match self {
            Self::ModeOff => "cache warming is off".to_string(),
            Self::RouteNotNative => {
                "this provider route keeps its own conversation, so Borg has no request to replay"
                    .to_string()
            }
            Self::CacheLifetimeUnknown => {
                "no documented prompt cache lifetime for this model".to_string()
            }
            Self::EconomicsUnavailable => {
                "no price or reported cost for this model, so a refresh cannot be justified"
                    .to_string()
            }
            #[cfg(feature = "subscription-adapters")]
            Self::SubscriptionQuota => {
                "this route spends subscription quota, which Borg cannot price against a cache miss"
                    .to_string()
            }
            Self::ThinkingBudgetNotReplayable => {
                "reasoning is enabled on this route, so a replay cannot reproduce the thinking budget its cache prefix is keyed on"
                    .to_string()
            }
        }
    }
}

/// Whether warming is counting down, in flight, or stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WarmingState {
    Inactive,
    Scheduled,
    Refreshing,
}

impl WarmingState {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Inactive => "inactive",
            Self::Scheduled => "scheduled",
            Self::Refreshing => "refreshing",
        }
    }
}

/// Which side of the settle boundary a refresh is on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    /// The agent run that sent the real request is still working.
    Streaming,
    /// The run settled; Borg is waiting on the human.
    Idle,
}

impl Phase {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Streaming => "streaming",
            Self::Idle => "idle",
        }
    }

    const fn horizon(self) -> Duration {
        match self {
            Self::Streaming => MAX_WARMING_AGE,
            Self::Idle => MAX_IDLE_WARMING_AGE,
        }
    }

    const fn continuation_percent(self) -> u32 {
        match self {
            // The run is still going, so the next request is not a guess.
            Self::Streaming => 100,
            Self::Idle => IDLE_CONTINUATION_PERCENT,
        }
    }
}

/// Inputs and outcome of one warm-or-stop decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CacheWarmingDecision {
    pub phase: Phase,
    /// Price of this refresh: a cache read of the prompt plus its output cap.
    pub warm_microusd: u64,
    /// Extra price the next real request pays if the entry is lost.
    pub miss_microusd: u64,
    pub continuation_percent: u32,
    /// `continuation_percent% * miss - warm`, which can be negative.
    pub expected_savings_microusd: i64,
    pub warm: bool,
}

impl CacheWarmingDecision {
    fn evaluate(phase: Phase, economics: Economics) -> Self {
        let continuation_percent = phase.continuation_percent();
        let expected_miss = i64::try_from(
            u128::from(economics.miss_microusd).saturating_mul(u128::from(continuation_percent))
                / 100,
        )
        .unwrap_or(i64::MAX);
        let warm_cost = i64::try_from(economics.warm_microusd).unwrap_or(i64::MAX);
        let expected_savings_microusd = expected_miss.saturating_sub(warm_cost);
        Self {
            phase,
            warm_microusd: economics.warm_microusd,
            miss_microusd: economics.miss_microusd,
            continuation_percent,
            expected_savings_microusd,
            warm: expected_savings_microusd >= MINIMUM_EXPECTED_SAVINGS_MICROUSD,
        }
    }

    fn payload(&self) -> serde_json::Value {
        json!({
            "phase": self.phase.as_str(),
            "warm_microusd": self.warm_microusd,
            "miss_microusd": self.miss_microusd,
            "continuation_percent": self.continuation_percent,
            "expected_savings_microusd": self.expected_savings_microusd,
            "threshold_microusd": MINIMUM_EXPECTED_SAVINGS_MICROUSD,
            "action": if self.warm { "warm" } else { "stop" },
        })
    }
}

/// What a refresh costs and what losing the entry would cost, in microdollars.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Economics {
    pub warm_microusd: u64,
    pub miss_microusd: u64,
}

/// What Borg observed about a route before deciding to warm it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RefreshSupport {
    /// How long the provider keeps the entry this request wrote.
    pub cache_lifetime: Duration,
    /// The output budget the refresh will ask for, so the cost shown to the
    /// user is the cost actually incurred.
    pub max_output_tokens: u64,
}

/// The route that can replay one request as a prompt-cache refresh.
///
/// A trait so the scheduler's billing and cancellation boundaries can be
/// exercised against a fake instead of a paid provider.
#[async_trait]
pub(crate) trait PromptCacheRefreshClient: Send + Sync {
    /// Report whether this route can be warmed at all, before any request is
    /// sent. Returning `Err` is a first-class answer, not a failure.
    fn refresh_support(
        &self,
        provider: crate::CodingProvider,
        model: &str,
        effort: Option<&str>,
    ) -> Result<RefreshSupport, Ineligible>;

    /// Price one refresh against the prompt it would re-read. `None` means the
    /// model's economics are unknown, which stops warming.
    fn refresh_economics(
        &self,
        provider: crate::CodingProvider,
        model: &str,
        prompt_tokens: u64,
        max_output_tokens: u64,
    ) -> Option<Economics>;

    /// Re-send `request` with a minimal output budget and return only what it
    /// billed. Implementations must discard the reply.
    async fn refresh(
        &self,
        provider: crate::CodingProvider,
        model: &str,
        effort: Option<&str>,
        request: ModelTurnRequest,
        support: &RefreshSupport,
    ) -> Result<ProviderCallUsage, ProviderCallError>;
}

/// The request whose cache entry should be kept warm, exactly as it was sent.
#[derive(Debug, Clone)]
pub(crate) struct CacheWarmRequest {
    pub provider: crate::CodingProvider,
    pub model: String,
    pub effort: Option<String>,
    /// Replayed verbatim, `prompt_cache_key` included. Anything else would
    /// refresh a different entry from the one the next real turn will read.
    pub request: ModelTurnRequest,
    /// Prompt size the provider reported for the real request, which is what
    /// a miss would have to reprocess.
    pub prompt_tokens: u64,
}

/// A one-line account of what warming is doing and why.
#[derive(Debug, Clone)]
pub(crate) struct CacheWarmingStatus {
    pub state: WarmingState,
    pub reason: Option<String>,
    pub decision: Option<CacheWarmingDecision>,
}

impl CacheWarmingStatus {
    fn inactive(reason: impl Into<String>) -> Self {
        Self {
            state: WarmingState::Inactive,
            reason: Some(reason.into()),
            decision: None,
        }
    }
}

/// Identity of the conversation a warm run belongs to. A run captures the
/// counter it started with and abandons itself once the shared value moves
/// past it, which is how a context change or a newer turn stops it.
#[derive(Debug, Default)]
pub(crate) struct WarmGeneration(std::sync::atomic::AtomicU64);

impl WarmGeneration {
    fn current(&self) -> u64 {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn bump(&self) -> u64 {
        self.0
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            .wrapping_add(1)
    }
}

/// Keeps one prompt cache entry alive until something says to stop. Each
/// [`CacheWarmer::start`] supersedes the previous run, so a session has one
/// refresh timer and it belongs to the most recent real request.
pub(crate) struct CacheWarmer {
    /// Owns the lifetime of a detached idle run, which outlives the turn.
    session_id: Uuid,
    mode: CacheWarmingMode,
    client: Arc<dyn PromptCacheRefreshClient>,
    events: mpsc::Sender<SessionEventKind>,
    /// Cancels the refresh currently armed or in flight, with the id that
    /// distinguishes it from its replacement. One entry per run, so stopping a
    /// superseded run never disarms the one that replaced it.
    cancel: Mutex<Option<(u64, CancellationToken)>>,
    generation: Arc<WarmGeneration>,
    status: Arc<Mutex<CacheWarmingStatus>>,
    phase: Arc<Mutex<Phase>>,
    /// Set when idle warming has been handed off to outlive the turn that
    /// started it. Until then, dropping the warmer stops it -- which is what
    /// makes an errored or interrupted turn safe by construction.
    detached: std::sync::atomic::AtomicBool,
}

impl Drop for CacheWarmer {
    /// A warmer that has not been detached dies with its turn. Every way out
    /// of a turn runs this, so no path leaves a refresh armed against a
    /// conversation nobody is having any more.
    fn drop(&mut self) {
        if !self.detached.load(std::sync::atomic::Ordering::SeqCst) {
            self.stop("the turn ended");
        }
    }
}

impl std::fmt::Debug for CacheWarmer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CacheWarmer")
            .field("mode", &self.mode)
            .field("state", &self.status().state)
            .finish()
    }
}

impl CacheWarmer {
    pub(crate) fn new(
        session_id: Uuid,
        mode: CacheWarmingMode,
        client: Arc<dyn PromptCacheRefreshClient>,
        events: mpsc::Sender<SessionEventKind>,
    ) -> Self {
        // A previous turn on this session may have handed off an idle run.
        // That run is warming a prefix this turn is about to replace, so it
        // stops here rather than billing alongside its own replacement.
        cancel_detached_idle_run(session_id);
        let reason = if mode == CacheWarmingMode::Off {
            Ineligible::ModeOff.reason()
        } else {
            "waiting for the first request".to_string()
        };
        Self {
            session_id,
            mode,
            client,
            events,
            cancel: Mutex::new(None),
            generation: Arc::new(WarmGeneration::default()),
            status: Arc::new(Mutex::new(CacheWarmingStatus::inactive(reason))),
            phase: Arc::new(Mutex::new(Phase::Streaming)),
            detached: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub(crate) fn status(&self) -> CacheWarmingStatus {
        self.status
            .lock()
            .map(|status| status.clone())
            .unwrap_or_else(|_| CacheWarmingStatus::inactive("warming status unavailable"))
    }

    fn set_status(&self, status: CacheWarmingStatus) {
        if let Ok(mut slot) = self.status.lock() {
            *slot = status;
        }
    }

    fn set_phase(&self, phase: Phase) {
        if let Ok(mut slot) = self.phase.lock() {
            *slot = phase;
        }
    }

    /// Whether this route can be warmed at all, recorded as the current
    /// status. A property of the route and model rather than of any one
    /// request, so an ineligible session says why once.
    pub(crate) fn arm(
        &self,
        provider: crate::CodingProvider,
        model: &str,
        effort: Option<&str>,
    ) -> bool {
        if self.mode == CacheWarmingMode::Off {
            self.set_status(CacheWarmingStatus::inactive(Ineligible::ModeOff.reason()));
            return false;
        }
        match self.client.refresh_support(provider, model, effort) {
            Ok(_) => true,
            Err(ineligible) => {
                self.set_status(CacheWarmingStatus::inactive(ineligible.reason()));
                false
            }
        }
    }

    /// Stop the current run. The token aborts a refresh already in flight; the
    /// generation bump makes a scheduled one abandon itself on its next check.
    fn stop(&self, reason: impl Into<String>) {
        self.generation.bump();
        if let Ok(mut cancel) = self.cancel.lock()
            && let Some((_, token)) = cancel.take()
        {
            token.cancel();
        }
        self.set_status(CacheWarmingStatus::inactive(reason));
    }

    /// The conversation moved: a context clear, a compaction, a fork, or a
    /// model switch. Refreshing now would extend an entry nothing will read.
    pub(crate) fn on_context_changed(&self) {
        self.stop("the conversation context changed");
    }

    /// The agent run finished and Borg is waiting on the human.
    pub(crate) fn on_agent_settled(&self) {
        match self.mode {
            // Streaming warming exists to protect a prefix across a long tool
            // run. Once the run is over, continuing would be idle warming that
            // the operator did not ask for.
            CacheWarmingMode::Streaming => self.stop("the agent run settled"),
            CacheWarmingMode::Off => {}
            CacheWarmingMode::Idle => {
                self.set_phase(Phase::Idle);
                // Idle warming is the one case that is meant to outlive the
                // turn, so it is also the one case that has to say so.
                // Detach only once the session is actually holding the token.
                // Setting it first and failing to register would make Drop a
                // no-op with nothing left that could ever stop the run.
                let entry = self.cancel.lock().ok().and_then(|cancel| cancel.clone());
                let handed_off = match (entry, DETACHED_IDLE_RUNS.lock()) {
                    (Some(entry), Ok(mut runs)) => {
                        runs.insert(self.session_id, entry);
                        true
                    }
                    _ => false,
                };
                if handed_off {
                    self.detached
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                }
            }
        }
    }

    /// Record the current status where a human can find it later. Warming
    /// spends unprompted, so its reasons belong in the session record.
    pub(crate) async fn publish_status(&self, provider: crate::CodingProvider) {
        let status = self.status();
        let _ = self
            .events
            .send(SessionEventKind::ProviderEvent {
                provider,
                kind: "cache_warming_status".to_string(),
                payload: json!({
                    "mode": self.mode.as_str(),
                    "state": status.state.as_str(),
                    "reason": status.reason,
                    "decision": status.decision.map(|decision| decision.payload()),
                }),
            })
            .await;
    }

    /// Begin keeping the entry written by `request` warm.
    ///
    /// Supersedes any previous run. Called after a real turn reports usage, so
    /// `prompt_tokens` is the provider's own count rather than an estimate.
    pub(crate) fn start(&self, request: CacheWarmRequest) {
        // Supersede rather than accumulate: the previous run's entry is the
        // one this request just replaced, so refreshing it would pay to keep
        // a prefix alive that nothing will ask for again.
        self.stop("superseded by a newer request");
        self.set_phase(Phase::Streaming);
        if self.mode == CacheWarmingMode::Off {
            self.set_status(CacheWarmingStatus::inactive(Ineligible::ModeOff.reason()));
            return;
        }
        let support = match self.client.refresh_support(
            request.provider,
            &request.model,
            request.effort.as_deref(),
        ) {
            Ok(support) => support,
            Err(ineligible) => {
                self.set_status(CacheWarmingStatus::inactive(ineligible.reason()));
                return;
            }
        };
        let Some(delay) = refresh_delay(support.cache_lifetime) else {
            self.set_status(CacheWarmingStatus::inactive(
                Ineligible::CacheLifetimeUnknown.reason(),
            ));
            return;
        };
        let generation = self.generation.current();
        let cancel = CancellationToken::new();
        let run_id = NEXT_RUN_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if let Ok(mut slot) = self.cancel.lock() {
            *slot = Some((run_id, cancel.clone()));
        }
        let run = WarmRun {
            session_id: self.session_id,
            run_id,
            request,
            support,
            delay,
            started_at: Instant::now(),
            generation,
            client: Arc::clone(&self.client),
            events: self.events.clone(),
            cancel,
            shared_generation: Arc::clone(&self.generation),
            status: Arc::clone(&self.status),
            phase: Arc::clone(&self.phase),
        };
        self.set_status(CacheWarmingStatus {
            state: WarmingState::Scheduled,
            reason: None,
            decision: None,
        });
        tokio::spawn(run.drive());
    }
}

/// One scheduled refresh loop, owning everything it needs so it can outlive
/// the borrow that created it without holding the harness open.
struct WarmRun {
    session_id: Uuid,
    run_id: u64,
    request: CacheWarmRequest,
    support: RefreshSupport,
    delay: Duration,
    started_at: Instant,
    generation: u64,
    client: Arc<dyn PromptCacheRefreshClient>,
    events: mpsc::Sender<SessionEventKind>,
    cancel: CancellationToken,
    shared_generation: Arc<WarmGeneration>,
    status: Arc<Mutex<CacheWarmingStatus>>,
    phase: Arc<Mutex<Phase>>,
}

impl WarmRun {
    fn is_current(&self) -> bool {
        self.shared_generation.current() == self.generation && !self.cancel.is_cancelled()
    }

    fn phase(&self) -> Phase {
        self.phase
            .lock()
            .map(|phase| *phase)
            .unwrap_or(Phase::Streaming)
    }

    fn publish(&self, status: CacheWarmingStatus) {
        // Only the run that still owns warming may write status; a superseded
        // run reporting "stopped" would overwrite its successor's "scheduled".
        if !self.is_current() {
            return;
        }
        if let Ok(mut slot) = self.status.lock() {
            *slot = status;
        }
    }

    fn stop(&self, reason: impl Into<String>, decision: Option<CacheWarmingDecision>) {
        self.publish(CacheWarmingStatus {
            state: WarmingState::Inactive,
            reason: Some(reason.into()),
            decision,
        });
    }

    /// Runs the refresh loop, then drops this run's registration whatever
    /// ended it -- reaching a horizon, a failed refresh, or cancellation.
    async fn drive(self) {
        let (session_id, run_id) = (self.session_id, self.run_id);
        self.refresh_loop().await;
        release_detached_idle_run(session_id, run_id);
    }

    async fn refresh_loop(self) {
        loop {
            tokio::select! {
                // Cancellation wins over a pending refresh, so an interrupt
                // never leaves a request in flight that still bills.
                () = self.cancel.cancelled() => {
                    self.stop("cancelled", None);
                    return;
                }
                () = tokio::time::sleep(self.delay) => {}
            }
            if !self.is_current() {
                return;
            }
            let phase = self.phase();
            // Horizons are measured from the real request and never extended
            // by a refresh, so a session cannot warm itself indefinitely.
            if self.started_at.elapsed() + self.delay > phase.horizon() {
                self.stop(
                    match phase {
                        Phase::Streaming => "reached the one-hour active warming limit",
                        Phase::Idle => "reached the thirty-minute idle warming limit",
                    },
                    None,
                );
                return;
            }
            let Some(economics) = self.client.refresh_economics(
                self.request.provider,
                &self.request.model,
                self.request.prompt_tokens,
                self.support.max_output_tokens,
            ) else {
                self.stop(Ineligible::EconomicsUnavailable.reason(), None);
                return;
            };
            let decision = CacheWarmingDecision::evaluate(phase, economics);
            if !decision.warm {
                self.record_decision(&decision, "skipped").await;
                self.stop("expected savings below the threshold", Some(decision));
                return;
            }
            self.publish(CacheWarmingStatus {
                state: WarmingState::Refreshing,
                reason: None,
                decision: Some(decision),
            });
            self.record_decision(&decision, "sent").await;
            let refreshed = tokio::select! {
                () = self.cancel.cancelled() => {
                    self.stop("cancelled", Some(decision));
                    return;
                }
                refreshed = self.client.refresh(
                    self.request.provider,
                    &self.request.model,
                    self.request.effort.as_deref(),
                    self.request.request.clone(),
                    &self.support,
                ) => refreshed,
            };
            if !self.is_current() {
                return;
            }
            match refreshed {
                Ok(usage) => self.record_usage(&usage).await,
                Err(error) => {
                    // Warming is best-effort. A failed refresh means the next
                    // real request pays for one miss, which is exactly the
                    // cost this module was willing to risk; it must never
                    // surface as a turn failure.
                    self.stop(format!("refresh failed: {}", error.message), Some(decision));
                    return;
                }
            }
            self.publish(CacheWarmingStatus {
                state: WarmingState::Scheduled,
                reason: None,
                decision: Some(decision),
            });
        }
    }

    /// Record what warming decided, separately from what it spent. A provider
    /// event rather than a model message, so prompt replay never sees it.
    async fn record_decision(&self, decision: &CacheWarmingDecision, outcome: &str) {
        let mut payload = decision.payload();
        payload["outcome"] = json!(outcome);
        payload["model"] = json!(self.request.model);
        payload["prompt_tokens"] = json!(self.request.prompt_tokens);
        payload["max_output_tokens"] = json!(self.support.max_output_tokens);
        let _ = self
            .events
            .send(SessionEventKind::ProviderEvent {
                provider: self.request.provider,
                kind: "cache_warming_decision".to_string(),
                payload,
            })
            .await;
    }

    /// Record what the refresh billed. `turn_id` is `None` because a refresh
    /// belongs to no turn, and `context_tokens` is cleared because a refresh
    /// adds nothing to the context: letting it through would move the
    /// auto-compaction trigger on spend that never entered the conversation.
    async fn record_usage(&self, usage: &ProviderCallUsage) {
        let _ = self
            .events
            .send(SessionEventKind::UsageUpdated {
                provider_duration_ms: usage.duration_ms,
                turn_id: None,
                provider_context_reused: Some(true),
                input_tokens: usage.input_tokens,
                output_tokens: usage.output_tokens,
                cached_input_tokens: usage.cached_input_tokens,
                cache_creation_input_tokens: usage.cache_creation_input_tokens,
                total_tokens: usage.total_tokens,
                cost_microusd: usage.cost_microusd,
                cost_basis: usage.cost_basis.to_string(),
                cost_usd: None,
                context_tokens: None,
                context_window_tokens: usage.context_window_tokens,
            })
            .await;
    }
}

/// When to refresh an entry with the given lifetime: ninety percent of it,
/// but never closer than a ten-second margin to expiry, so a slow refresh
/// still lands inside the window. A lifetime at or under the margin has no
/// usable moment and returns `None` rather than racing expiry.
fn refresh_delay(lifetime: Duration) -> Option<Duration> {
    if lifetime <= WARM_SAFETY_MARGIN {
        return None;
    }
    let ninety_percent = lifetime.mul_f64(f64::from(WARM_AT_PERCENT_OF_TTL) / 100.0);
    let with_margin = lifetime.saturating_sub(WARM_SAFETY_MARGIN);
    Some(ninety_percent.min(with_margin).max(Duration::from_secs(1)))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use borg_provider::provider::ModelMessage;

    use super::*;

    const CACHE_KEY: &str = "borg-cache:generation-1";

    /// A route that bills nothing and records what it was asked to replay.
    #[derive(Default)]
    struct FakeRoute {
        refreshes: AtomicUsize,
        replayed_cache_key: Mutex<Option<String>>,
        replayed_cap: Mutex<Option<u64>>,
    }

    #[async_trait]
    impl PromptCacheRefreshClient for FakeRoute {
        fn refresh_support(
            &self,
            _provider: crate::CodingProvider,
            _model: &str,
            _effort: Option<&str>,
        ) -> Result<RefreshSupport, Ineligible> {
            Ok(RefreshSupport {
                cache_lifetime: Duration::from_secs(100),
                max_output_tokens: 1,
            })
        }

        fn refresh_economics(
            &self,
            _provider: crate::CodingProvider,
            _model: &str,
            _prompt_tokens: u64,
            _max_output_tokens: u64,
        ) -> Option<Economics> {
            // A miss costs far more than the refresh, so the decision clears
            // the threshold in both the streaming and the idle phase.
            Some(Economics {
                warm_microusd: 1_000,
                miss_microusd: 5_000_000,
            })
        }

        async fn refresh(
            &self,
            _provider: crate::CodingProvider,
            _model: &str,
            _effort: Option<&str>,
            request: ModelTurnRequest,
            support: &RefreshSupport,
        ) -> Result<ProviderCallUsage, ProviderCallError> {
            self.refreshes.fetch_add(1, Ordering::SeqCst);
            *self.replayed_cache_key.lock().unwrap() = request.prompt_cache_key.clone();
            *self.replayed_cap.lock().unwrap() = Some(support.max_output_tokens);
            Ok(ProviderCallUsage {
                input_tokens: 3,
                cached_input_tokens: 40_000,
                output_tokens: 1,
                total_tokens: 40_004,
                cost_microusd: Some(1_000),
                // A refresh reads a whole context it must not report.
                context_tokens: Some(40_000),
                ..ProviderCallUsage::default()
            })
        }
    }

    fn warm_request() -> CacheWarmRequest {
        CacheWarmRequest {
            provider: crate::CodingProvider::OpenAiCompatible,
            model: "test-model".to_string(),
            effort: None,
            request: ModelTurnRequest {
                fast: false,
                request_id: Some("turn-1:1".to_string()),
                session_id: Some("borg-session:test".to_string()),
                prompt_cache_key: Some(CACHE_KEY.to_string()),
                turn_routing: Default::default(),
                messages: vec![ModelMessage::user("hello")],
                tools: Vec::new(),
                output_schema: None,
            },
            prompt_tokens: 40_000,
        }
    }

    /// The whole contract in one pass, with no provider and no money: a refresh
    /// goes out on time, replays the same cache entry under a minimal cap, is
    /// billed where the user can see it, and never becomes part of the
    /// conversation.
    #[tokio::test(start_paused = true)]
    async fn a_refresh_is_sent_billed_and_never_becomes_a_message() {
        let (events, mut received) = mpsc::channel(64);
        let route = Arc::new(FakeRoute::default());
        let warmer = CacheWarmer::new(
            Uuid::new_v4(),
            CacheWarmingMode::Streaming,
            route.clone(),
            events,
        );
        assert!(warmer.arm(crate::CodingProvider::OpenAiCompatible, "test-model", None));
        warmer.start(warm_request());

        // 90% of a 100s lifetime, so one refresh is due and the second is not.
        tokio::time::sleep(Duration::from_secs(95)).await;

        assert_eq!(route.refreshes.load(Ordering::SeqCst), 1);
        assert_eq!(
            route.replayed_cache_key.lock().unwrap().as_deref(),
            Some(CACHE_KEY),
            "a refresh that changed the cache key would extend an entry the next real request never reads"
        );
        assert_eq!(*route.replayed_cap.lock().unwrap(), Some(1));

        let mut billed = 0;
        let mut decisions = 0;
        while let Ok(event) = received.try_recv() {
            match event {
                SessionEventKind::Message { .. } => {
                    panic!("a refresh must never emit a model message")
                }
                SessionEventKind::UsageUpdated {
                    turn_id,
                    context_tokens,
                    cost_microusd,
                    ..
                } => {
                    billed += 1;
                    assert_eq!(cost_microusd, Some(1_000));
                    assert!(turn_id.is_none(), "a refresh belongs to no turn");
                    assert!(
                        context_tokens.is_none(),
                        "a refresh adds nothing to context and must not move the compaction trigger"
                    );
                }
                SessionEventKind::ProviderEvent { kind, .. }
                    if kind == "cache_warming_decision" =>
                {
                    decisions += 1;
                }
                _ => {}
            }
        }
        assert_eq!(billed, 1, "the spend has to be visible exactly once");
        assert_eq!(decisions, 1);
    }

    /// The cancellation contract: an armed refresh belongs to its turn, so a
    /// turn that ends for any reason takes the refresh with it.
    #[tokio::test(start_paused = true)]
    async fn dropping_the_turn_cancels_an_armed_refresh() {
        let (events, _received) = mpsc::channel(64);
        let route = Arc::new(FakeRoute::default());
        {
            let warmer = CacheWarmer::new(
                Uuid::new_v4(),
                CacheWarmingMode::Streaming,
                route.clone(),
                events,
            );
            warmer.start(warm_request());
        }
        tokio::time::sleep(Duration::from_secs(300)).await;
        assert_eq!(route.refreshes.load(Ordering::SeqCst), 0);
    }

    /// Idle warming outlives its turn on purpose, which is what makes it the
    /// one mode that can be left behind. The next turn on the session has to
    /// end it, or a session accumulates one paid loop per turn.
    #[tokio::test(start_paused = true)]
    async fn a_new_turn_stops_the_idle_run_the_previous_one_detached() {
        let (events, _received) = mpsc::channel(64);
        let session_id = Uuid::new_v4();
        let route = Arc::new(FakeRoute::default());
        {
            let warmer = CacheWarmer::new(
                session_id,
                CacheWarmingMode::Idle,
                route.clone(),
                events.clone(),
            );
            warmer.start(warm_request());
            warmer.on_agent_settled();
        }
        let _next_turn = CacheWarmer::new(
            session_id,
            CacheWarmingMode::Idle,
            route.clone(),
            events.clone(),
        );
        tokio::time::sleep(Duration::from_secs(300)).await;
        assert_eq!(
            route.refreshes.load(Ordering::SeqCst),
            0,
            "the detached run kept billing after the turn that replaced it"
        );
    }
}
