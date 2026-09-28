//! Model capability data sourced from models.dev.
//!
//! Per-provider effort tables rot: they are written from one probe and never
//! learn that a vendor shipped a new model. models.dev publishes what each
//! model can actually do, so this reads that instead of maintaining a copy.
//!
//! A serving gateway still wins over this. OpenRouter answers what *it* will
//! accept for a model it routes, which a shared database cannot know.

use std::collections::BTreeMap;
use std::sync::{OnceLock, RwLock};

/// What a model offers for reasoning, as the catalog describes it.
///
/// The three shapes are mutually exclusive in practice: an ordered ladder of
/// effort words, a bare on/off switch, or a token budget. A model can offer a
/// toggle alongside one of the others, so they are not a single enum.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReasoningCapability {
    /// Ordered effort words, highest first, exactly as published.
    pub effort_values: Vec<String>,
    /// The model can run with reasoning turned off.
    pub toggle: bool,
    /// The largest reasoning token budget the model accepts.
    pub budget_tokens_max: Option<u64>,
}

impl ReasoningCapability {
    /// Whether this model has any reasoning control at all.
    pub fn is_empty(&self) -> bool {
        self.effort_values.is_empty() && !self.toggle && self.budget_tokens_max.is_none()
    }
}

type Catalog = BTreeMap<String, BTreeMap<String, ReasoningCapability>>;

static CATALOG: OnceLock<RwLock<Catalog>> = OnceLock::new();

fn catalog() -> &'static RwLock<Catalog> {
    CATALOG.get_or_init(|| RwLock::new(BTreeMap::new()))
}

/// Fetch the whole catalog and replace whatever was cached.
///
/// Called on the same schedule as the gateway catalogs, so a route that has
/// never been configured is still described by its vendor's real capabilities.
pub async fn refresh() -> anyhow::Result<usize> {
    let response = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(5))
        .timeout(std::time::Duration::from_secs(20))
        .build()?
        .get("https://models.dev/api.json")
        .send()
        .await?;
    let status = response.status();
    anyhow::ensure!(
        status.is_success(),
        "models.dev catalog returned HTTP {status}"
    );
    let parsed = models_dev_capabilities(&response.json::<serde_json::Value>().await?);
    let count: usize = parsed.values().map(BTreeMap::len).sum();
    if let Ok(mut guard) = catalog().write() {
        *guard = parsed;
    }
    Ok(count)
}

/// The capability of `model` under `provider`, if the catalog knows both.
///
/// A model absent from the catalog is unknown, not unrestricted: callers must
/// not widen it to some default ladder on a miss.
pub fn capability(provider: &str, model: &str) -> Option<ReasoningCapability> {
    let guard = catalog().read().ok()?;
    guard.get(provider)?.get(model).cloned()
}

/// Replace the cached catalog without a network call, for tests and for a
/// build that vendors the data.
pub fn set(catalog: Catalog) {
    if let Ok(mut guard) = self::catalog().write() {
        *guard = catalog;
    }
}

/// Parse the models.dev payload into `provider -> model -> capability`.
///
/// Only reasoning is read. The payload also carries pricing, limits and
/// modalities, but a capability this adapter does not use is data that can go
/// stale without anyone noticing, so it is not mirrored here.
pub fn models_dev_capabilities(
    payload: &serde_json::Value,
) -> BTreeMap<String, BTreeMap<String, ReasoningCapability>> {
    let Some(providers) = payload.as_object() else {
        return BTreeMap::new();
    };
    providers
        .iter()
        .filter_map(|(provider, entry)| {
            let models = entry.get("models")?.as_object()?;
            let parsed = models
                .iter()
                .filter_map(|(id, model)| {
                    Some((id.clone(), parse_reasoning(model.get("reasoning_options")?)))
                })
                .collect::<BTreeMap<_, _>>();
            (!parsed.is_empty()).then_some((provider.clone(), parsed))
        })
        .collect()
}

fn parse_reasoning(options: &serde_json::Value) -> ReasoningCapability {
    let mut capability = ReasoningCapability::default();
    for option in options.as_array().into_iter().flatten() {
        match option.get("type").and_then(serde_json::Value::as_str) {
            Some("toggle") => capability.toggle = true,
            Some("budget_tokens") => {
                capability.budget_tokens_max = option.get("max").and_then(serde_json::Value::as_u64);
            }
            // Published highest first. A later ladder does not replace an
            // earlier one: some entries list both, and the ladder is the
            // narrower claim about the same control.
            Some("effort") => {
                if capability.effort_values.is_empty()
                    && let Some(values) = option.get("values").and_then(serde_json::Value::as_array)
                {
                    capability.effort_values = values
                        .iter()
                        .filter_map(serde_json::Value::as_str)
                        .map(str::to_string)
                        .collect();
                }
            }
            _ => {}
        }
    }
    capability
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verbatim from models.dev. The point of sourcing this is that a
    /// hand-written table gets it wrong: Borg clamped GLM to
    /// low/medium/high, while the catalog says low/high/max.
    #[test]
    fn capabilities_are_read_from_the_published_shape() {
        let parsed = models_dev_capabilities(&serde_json::json!({
            "zai": { "models": { "glm-5.3": { "reasoning_options": [
                { "type": "effort", "values": ["low", "high", "max"] }
            ] } } },
            "alibaba": { "models": { "qwen3.7-plus": { "reasoning_options": [
                { "type": "toggle" },
                { "type": "budget_tokens", "max": 81920 }
            ] } } },
        }));

        let glm = &parsed["zai"]["glm-5.3"];
        assert_eq!(glm.effort_values, ["low", "high", "max"]);
        assert!(!glm.toggle);

        // A Qwen model offers a switch and a token budget, and no effort
        // ladder at all: listing levels for it would offer choices it refuses.
        let qwen = &parsed["alibaba"]["qwen3.7-plus"];
        assert!(qwen.effort_values.is_empty());
        assert!(qwen.toggle);
        assert_eq!(qwen.budget_tokens_max, Some(81920));
        assert!(!qwen.is_empty());
    }

    /// A model with no reasoning metadata is absent, not permissive: a caller
    /// that misses must not widen it to a default ladder.
    #[test]
    fn a_model_without_reasoning_metadata_is_unknown() {
        let parsed = models_dev_capabilities(&serde_json::json!({
            "openrouter": { "models": { "vendor/plain": { "id": "vendor/plain" } } },
        }));
        assert!(parsed.get("openrouter").is_none());
    }
}
