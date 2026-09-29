//! One reader for the models.dev catalog, and the only place model facts
//! are parsed out of it.
//!
//! Three consumers used to read this document separately: pricing, context
//! windows and reasoning levels. Each kept its own parse and its own cache, so
//! a single startup fetched a multi-megabyte document more than once and the
//! same model could be described two different ways. Everything now resolves
//! through [`facts`].
//!
//! Two rules hold everywhere:
//!
//! * **Unknown stays unknown.** A model the catalog does not describe yields
//!   `None`. Nothing here invents a default ladder, a window or a price,
//!   because a plausible guess about a model's limits is what produces a
//!   failed turn.
//! * **A serving gateway outranks this.** OpenRouter answers what *it* will
//!   accept for a model it routes, which a shared catalog cannot know. That
//!   override lives in the caller, above this module.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, OnceLock, RwLock};

use serde_json::Value;

const MODELS_DEV_URL: &str = "https://models.dev/api.json";

/// What a model offers, as the catalog describes it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ModelFacts {
    /// Ordered effort words, highest first.
    pub effort_values: Vec<String>,
    /// The model can run with reasoning turned off.
    pub reasoning_toggle: bool,
    /// The largest reasoning token budget the model accepts.
    pub budget_tokens_max: Option<u64>,
    /// Price in micro-USD per million tokens. `None` unless the entry states
    /// all three rates: a partial price would make an estimate quietly wrong
    /// in whichever direction it was missing.
    pub pricing: Option<Pricing>,
    pub context_window: Option<u64>,
    pub output_limit: Option<u64>,
}

impl ModelFacts {
    /// Whether the catalog describes this model at all.
    pub fn is_known(&self) -> bool {
        !self.effort_values.is_empty()
            || self.reasoning_toggle
            || self.budget_tokens_max.is_some()
            || self.pricing.is_some()
            || self.context_window.is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pricing {
    pub input: u64,
    pub cached_input: u64,
    pub output: u64,
}

type Facts = HashMap<(String, String), ModelFacts>;

static CATALOG: OnceLock<RwLock<Option<Facts>>> = OnceLock::new();

fn catalog() -> &'static RwLock<Option<Facts>> {
    CATALOG.get_or_init(|| RwLock::new(None))
}

/// Load the catalog once per process, if it is not already loaded.
///
/// Concurrent callers join the same load rather than each issuing a request:
/// the document is large and every consumer wants all of it.
pub async fn ensure_loaded() {
    if catalog()
        .read()
        .ok()
        .and_then(|cache| cache.clone())
        .is_some()
    {
        return;
    }
    if let Ok(parsed) = fetch().await
        && let Ok(mut guard) = catalog().write()
    {
        *guard = Some(parsed);
    }
}

/// Read-only access for callers that cannot await. Returns `None` until the
/// catalog has loaded, which callers treat as "unknown", never as a default.
pub fn loaded() -> bool {
    catalog()
        .read()
        .map(|cache| cache.is_some())
        .unwrap_or(false)
}

/// Facts for `model` under `provider`.
///
/// A miss falls back to the same model id under any other provider: list price
/// and limits are properties of the model, and a deployment that lists a model
/// under a different key should still get a real price instead of none.
pub fn facts(provider: &str, model: &str) -> Option<ModelFacts> {
    let id = model.trim();
    if id.is_empty() {
        return None;
    }
    let guard = catalog().read().ok()?;
    let entries = guard.as_ref()?;
    if !provider.is_empty()
        && let Some(found) = entries.get(&(provider.to_string(), id.to_string()))
    {
        return Some(found.clone());
    }
    entries
        .iter()
        .find(|((_, listed), _)| listed == id)
        .map(|(_, found)| found.clone())
}

pub fn pricing(provider: &str, model: &str) -> Option<Pricing> {
    facts(provider, model).and_then(|found| found.pricing)
}

/// The context window, scoped to `provider` with no cross-provider fallback.
///
/// A window is deliberately stricter than a price here. The Go route reports
/// the window of the model *it* serves, and borrowing another provider's entry
/// for the same id would let an unrelated model's limits drive auto-compaction
/// on this one.
pub fn context_window(provider: &str, model: &str) -> Option<u64> {
    let id = model.trim();
    if id.is_empty() {
        return None;
    }
    let guard = catalog().read().ok()?;
    guard
        .as_ref()?
        .get(&(provider.to_string(), id.to_string()))
        .and_then(|found| found.context_window)
}

/// Effort words for `model`, or an empty list when it publishes none.
///
/// An empty list is a real answer, not a miss: a model that offers only a
/// toggle has no ladder, and offering it one would mean offering levels it
/// refuses.
pub fn effort_values(provider: &str, model: &str) -> Vec<String> {
    facts(provider, model)
        .map(|found| found.effort_values)
        .unwrap_or_default()
}

async fn fetch() -> Result<Facts, String> {
    let response = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(5))
        .timeout(std::time::Duration::from_secs(20))
        .build()
        .map_err(|error| error.to_string())?
        .get(MODELS_DEV_URL)
        .send()
        .await
        .map_err(|error| error.to_string())?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("models.dev catalog returned HTTP {status}"));
    }
    let payload: Value = response
        .json()
        .await
        .map_err(|error| format!("models.dev catalog is not valid JSON: {error}"))?;
    Ok(parse(&payload))
}

/// Parse the catalog into one map, keeping only the fields Borg acts on.
pub fn parse(payload: &Value) -> Facts {
    let mut facts = Facts::new();
    let Some(providers) = payload.as_object() else {
        return facts;
    };
    for (provider, entry) in providers {
        let Some(models) = entry.get("models").and_then(Value::as_object) else {
            continue;
        };
        for (id, model) in models {
            let id = id.trim();
            if id.is_empty() {
                continue;
            }
            let parsed = ModelFacts {
                effort_values: reasoning_effort_values(model),
                reasoning_toggle: has_reasoning_option(model, "toggle"),
                budget_tokens_max: reasoning_budget_max(model),
                pricing: parse_pricing(model),
                context_window: positive_u64(model.pointer("/limit/context")),
                output_limit: positive_u64(model.pointer("/limit/output")),
            };
            if parsed.is_known() {
                facts.insert((provider.clone(), id.to_string()), parsed);
            }
        }
    }
    facts
}

fn positive_u64(value: Option<&Value>) -> Option<u64> {
    value.and_then(Value::as_u64).filter(|number| *number > 0)
}

fn reasoning_options(model: &Value) -> impl Iterator<Item = &Value> {
    model
        .get("reasoning_options")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
}

fn has_reasoning_option(model: &Value, kind: &str) -> bool {
    reasoning_options(model).any(|option| option.get("type").and_then(Value::as_str) == Some(kind))
}

fn reasoning_effort_values(model: &Value) -> Vec<String> {
    for option in reasoning_options(model) {
        if option.get("type").and_then(Value::as_str) != Some("effort") {
            continue;
        }
        let values: Vec<String> = option
            .get("values")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect();
        // Some entries list a ladder twice; the first is the narrower claim
        // about the same control, so it is the one kept.
        if !values.is_empty() {
            return values;
        }
    }
    Vec::new()
}

fn reasoning_budget_max(model: &Value) -> Option<u64> {
    reasoning_options(model)
        .find(|option| option.get("type").and_then(Value::as_str) == Some("budget_tokens"))
        .and_then(|option| positive_u64(option.get("max")))
}

fn parse_pricing(model: &Value) -> Option<Pricing> {
    let cost = model.get("cost")?;
    let rate = |field: &str| {
        cost.get(field)
            .and_then(Value::as_f64)
            .filter(|value| *value >= 0.0)
            .map(usd_per_million_to_microusd)
    };
    // All three or nothing: a cached rate that silently fell back to the input
    // rate would overstate what a cache hit saves.
    Some(Pricing {
        input: rate("input")?,
        cached_input: rate("cache_read")?,
        output: rate("output")?,
    })
}

fn usd_per_million_to_microusd(usd_per_million: f64) -> u64 {
    (usd_per_million * 1_000_000.0).round().max(0.0) as u64
}

/// Replace the catalog for tests; retain the guard while using the fixture.
pub fn set_for_test(facts: Facts) -> MutexGuard<'static, ()> {
    static TEST_CATALOG: Mutex<()> = Mutex::new(());
    let owner = TEST_CATALOG
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    if let Ok(mut guard) = catalog().write() {
        *guard = Some(facts);
    }
    owner
}
