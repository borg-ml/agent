use std::fmt;
use std::sync::{OnceLock, RwLock};
use std::time::Instant;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderModelCatalog {
    pub backend: &'static str,
    pub default_model: &'static str,
    pub selectable_models: &'static [(&'static str, &'static str)],
    pub effort_levels: &'static [&'static str],
}

impl ProviderModelCatalog {
    pub fn supports_model(self, model: &str) -> bool {
        self.selectable_models
            .iter()
            .any(|(candidate, _)| *candidate == model)
    }

    pub fn supports_effort(self, effort: &str) -> bool {
        self.effort_levels.contains(&effort)
    }
}

pub const CODEX_SELECTABLE_MODELS: [(&str, &str); 3] = [
    ("gpt-5.6-sol", "Sol"),
    ("gpt-5.6-terra", "Terra"),
    ("gpt-5.6-luna", "Luna"),
];
pub const CODEX_EFFORT_LEVELS: [&str; 6] = ["low", "medium", "high", "xhigh", "max", "ultra"];
pub const CODEX_MODEL_CATALOG: ProviderModelCatalog = ProviderModelCatalog {
    backend: "codex",
    default_model: "gpt-5.6-luna",
    selectable_models: &CODEX_SELECTABLE_MODELS,
    effort_levels: &CODEX_EFFORT_LEVELS,
};

pub const CLAUDE_SELECTABLE_MODELS: [(&str, &str); 3] = [
    ("claude-opus-5", "Opus 5"),
    ("claude-sonnet-5", "Sonnet 5"),
    ("claude-fable-5", "Fable 5"),
];
pub const CLAUDE_EFFORT_LEVELS: [&str; 5] = ["low", "medium", "high", "xhigh", "max"];
pub const CLAUDE_MODEL_CATALOG: ProviderModelCatalog = ProviderModelCatalog {
    backend: "claude",
    default_model: "claude-sonnet-5",
    selectable_models: &CLAUDE_SELECTABLE_MODELS,
    effort_levels: &CLAUDE_EFFORT_LEVELS,
};

pub const MODEL_CATALOGS: [ProviderModelCatalog; 2] = [CODEX_MODEL_CATALOG, CLAUDE_MODEL_CATALOG];

pub fn model_catalog_for_backend(backend: &str) -> Option<ProviderModelCatalog> {
    MODEL_CATALOGS
        .iter()
        .copied()
        .find(|catalog| catalog.backend == backend)
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CostBasis {
    ProviderReported,
    EstimatedFromPricing,
    /// A provider exposed token counters for a subscription-backed session,
    /// but the account is not billed at the public API rate. This value is
    /// useful for observability only and must never be rendered as a charge.
    SubscriptionEquivalent,
    #[default]
    Unavailable,
}

impl CostBasis {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProviderReported => "provider_reported",
            Self::EstimatedFromPricing => "estimated_from_pricing",
            Self::SubscriptionEquivalent => "subscription_equivalent",
            Self::Unavailable => "unavailable",
        }
    }
}

impl fmt::Display for CostBasis {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ProviderCallUsage {
    #[serde(default)]
    pub duration_ms: u64,
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub cached_input_tokens: u64,
    #[serde(default)]
    pub cache_creation_input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
    #[serde(default)]
    pub total_tokens: u64,
    #[serde(default)]
    pub context_tokens: Option<u64>,
    #[serde(default)]
    pub context_window_tokens: Option<u64>,
    #[serde(default)]
    pub cost_microusd: Option<u64>,
    #[serde(default)]
    pub cost_basis: CostBasis,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ProviderChannel {
    #[default]
    Direct,
    Vertex,
    Bedrock,
    AzureOpenAi,
}

impl ProviderChannel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Vertex => "vertex",
            Self::Bedrock => "bedrock",
            Self::AzureOpenAi => "azure_openai",
        }
    }
}

impl fmt::Display for ProviderChannel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

pub fn elapsed_millis_u64(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX)
}

pub fn codex_product_model() -> &'static str {
    CODEX_MODEL_CATALOG.default_model
}

pub fn codex_default_effort() -> &'static str {
    "max"
}

pub fn codex_effort_levels() -> Vec<String> {
    CODEX_MODEL_CATALOG
        .effort_levels
        .iter()
        .map(|effort| (*effort).to_string())
        .collect()
}

pub fn codex_effort_supported(value: &str) -> bool {
    CODEX_MODEL_CATALOG.supports_effort(value)
}

/// OpenRouter's capability-aware router. Individual OpenRouter model slugs
/// remain valid overrides; this default avoids coupling Borg to one vendor.
pub fn openrouter_product_model() -> &'static str {
    "openrouter/auto"
}

/// A runtime-discovered model entry for the /model picker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DynamicModelEntry {
    pub id: String,
    pub label: String,
    pub detail: Option<String>,
}

static OPENROUTER_MODEL_ENTRIES: OnceLock<RwLock<Vec<DynamicModelEntry>>> = OnceLock::new();

fn openrouter_model_entries_store() -> &'static RwLock<Vec<DynamicModelEntry>> {
    OPENROUTER_MODEL_ENTRIES.get_or_init(|| RwLock::new(Vec::new()))
}

/// Return the last successfully fetched OpenRouter catalog for synchronous UI
/// callers. An empty result means the catalog has not been fetched or the
/// request failed; callers must retain the current/manual model fallback.
pub fn openrouter_model_entries() -> Vec<DynamicModelEntry> {
    openrouter_model_entries_store()
        .read()
        .map(|entries| entries.clone())
        .unwrap_or_default()
}

pub fn set_openrouter_model_entries(entries: Vec<DynamicModelEntry>) {
    if let Ok(mut current) = openrouter_model_entries_store().write() {
        *current = entries;
    }
}

/// Fetch OpenRouter's current model catalog when credentials are configured.
/// Failure is returned to the caller so startup can keep the manual/current
/// model path usable without making the network a prerequisite for the TUI.
pub async fn refresh_openrouter_model_catalog() -> anyhow::Result<Vec<DynamicModelEntry>> {
    let Some(api_key) =
        crate::credentials::api_key(crate::credentials::ApiKeyCredential::OpenRouter)
    else {
        set_openrouter_model_entries(Vec::new());
        return Ok(Vec::new());
    };
    let base = std::env::var("BORG_OPENROUTER_BASE_URL")
        .unwrap_or_else(|_| "https://openrouter.ai/api/v1".to_string());
    let endpoint = format!("{}/models", base.trim_end_matches('/'));
    let response = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(5))
        .timeout(std::time::Duration::from_secs(10))
        .build()?
        .get(endpoint)
        .bearer_auth(api_key)
        .send()
        .await?;
    let status = response.status();
    anyhow::ensure!(
        status.is_success(),
        "OpenRouter model catalog returned HTTP {status}"
    );
    let payload: serde_json::Value = response.json().await?;
    let entries = openrouter_model_entries_from_response(&payload);
    set_openrouter_model_entries(entries.clone());
    Ok(entries)
}

fn openrouter_model_entries_from_response(payload: &serde_json::Value) -> Vec<DynamicModelEntry> {
    let mut entries = payload
        .get("data")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|model| {
            let id = model.get("id")?.as_str()?.trim();
            if id.is_empty() {
                return None;
            }
            let label = model
                .get("name")
                .and_then(serde_json::Value::as_str)
                .filter(|name| !name.trim().is_empty())
                .unwrap_or(id)
                .trim()
                .to_string();
            let mut details = Vec::new();
            if let Some(context) = model
                .get("context_length")
                .and_then(serde_json::Value::as_u64)
            {
                details.push(format!("{context} context"));
            }
            if let Some(description) = model
                .get("description")
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|description| !description.is_empty())
            {
                details.push(description.to_string());
            }
            Some(DynamicModelEntry {
                id: id.to_string(),
                label,
                detail: (!details.is_empty()).then(|| details.join(" · ")),
            })
        })
        .collect::<Vec<_>>();
    entries.sort_by_cached_key(|entry| entry.label.to_lowercase());
    entries.dedup_by(|left, right| left.id == right.id);
    entries
}

/// Return dynamic model entries for `backend` given a set of discovered models.
///
/// Returns an empty vec for any backend that has a compile-time const catalog,
/// because those backends must never mix with the dynamic arm.
///
/// For an open-ended backend (including `"openai-compatible"`) the rules are:
/// * `current` is emitted first when `Some`.
/// * `discovered` entries follow, deduped by `id` against `current`.
pub fn dynamic_models_for_backend(
    backend: &str,
    current: Option<&str>,
    discovered: &[DynamicModelEntry],
) -> Vec<DynamicModelEntry> {
    // If the backend has a const catalog, return nothing — those backends are
    // handled by the existing provider_catalog arm.
    if model_catalog_for_backend(backend).is_some() {
        return Vec::new();
    }

    // The catalog backend names are stable wire names. Accept the historical
    // display-label spelling too because picker callers may be older than the
    // runtime API, but keep the fixed catalogs on their existing path above.
    let open_ended = matches!(
        backend,
        "openai-compatible"
            | "open-ai-compatible"
            | "OpenAI-compatible"
            | "Open-ai-compatible"
            | "openrouter"
            | "open-router"
            | "OpenRouter"
    );
    if !open_ended {
        return Vec::new();
    }

    let mut out = Vec::new();
    let mut seen_ids = std::collections::HashSet::new();
    if let Some(cur) = current.filter(|current| !current.is_empty()) {
        if let Some(discovered_entry) = discovered.iter().find(|entry| entry.id == cur) {
            out.push(discovered_entry.clone());
        } else {
            out.push(DynamicModelEntry {
                id: cur.to_string(),
                label: cur.to_string(),
                detail: None,
            });
        }
        seen_ids.insert(cur.to_string());
    }
    for entry in discovered {
        if seen_ids.insert(entry.id.clone()) {
            out.push(entry.clone());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codex_defaults_use_luna_at_max_effort() {
        assert_eq!(codex_product_model(), "gpt-5.6-luna");
        assert_eq!(codex_default_effort(), "max");
        assert!(codex_effort_supported(codex_default_effort()));
    }

    #[test]
    fn dynamic_models_codex_backend_returns_empty() {
        let result = dynamic_models_for_backend("codex", Some("gpt-5.6-luna"), &[]);
        assert!(result.is_empty());
    }

    #[test]
    fn dynamic_models_claude_backend_returns_empty() {
        let result = dynamic_models_for_backend("claude", None, &[]);
        assert!(result.is_empty());
    }

    #[test]
    fn dynamic_models_openai_compatible_current_first() {
        let current = "qwen3.6:35b-a3b";
        let discovered = vec![
            DynamicModelEntry {
                id: "llama4".to_string(),
                label: "Llama 4".to_string(),
                detail: None,
            },
            DynamicModelEntry {
                id: "mixtral".to_string(),
                label: "Mixtral".to_string(),
                detail: None,
            },
        ];
        let result = dynamic_models_for_backend("openai-compatible", Some(current), &discovered);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].id, current);
        assert_eq!(result[1].id, "llama4");
        assert_eq!(result[2].id, "mixtral");
    }

    #[test]
    fn dynamic_models_openai_compatible_no_current() {
        let discovered = vec![DynamicModelEntry {
            id: "qwen3".to_string(),
            label: "Qwen 3".to_string(),
            detail: None,
        }];
        let result = dynamic_models_for_backend("openai-compatible", None, &discovered);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, "qwen3");
    }

    #[test]
    fn dynamic_models_openrouter_current_and_catalog_are_available() {
        let entries = vec![DynamicModelEntry {
            id: "anthropic/claude-sonnet".to_string(),
            label: "Claude Sonnet".to_string(),
            detail: Some("200000 context".to_string()),
        }];
        let result = dynamic_models_for_backend("openrouter", Some("openrouter/auto"), &entries);
        assert_eq!(result[0].id, "openrouter/auto");
        assert_eq!(result[1], entries[0]);
    }

    #[test]
    fn openrouter_catalog_response_preserves_model_details() {
        let entries = openrouter_model_entries_from_response(&serde_json::json!({
            "data": [
                {
                    "id": "z/model",
                    "name": "Z Model",
                    "context_length": 128000,
                    "description": "A coding model"
                },
                {
                    "id": "a/model",
                    "name": "A Model",
                    "context_length": 32000
                },
                {
                    "id": "a/model",
                    "name": "Duplicate"
                }
            ]
        }));
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].id, "a/model");
        assert_eq!(entries[1].label, "Z Model");
        assert!(
            entries[1].detail.as_deref().is_some_and(
                |detail| detail.contains("128000 context") && detail.contains("coding")
            )
        );
    }

    #[test]
    fn dynamic_models_dedup_when_current_in_discovered() {
        let current = "llama4";
        let discovered = vec![
            DynamicModelEntry {
                id: "llama4".to_string(),
                label: "Llama 4".to_string(),
                detail: None,
            },
            DynamicModelEntry {
                id: "qwen3".to_string(),
                label: "Qwen 3".to_string(),
                detail: None,
            },
        ];
        let result = dynamic_models_for_backend("openai-compatible", Some(current), &discovered);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].id, current); // current first
        assert_eq!(result[1].id, "qwen3"); // llama4 deduped out
    }

    #[test]
    fn dynamic_models_dedup_repeated_discovered_ids_and_keeps_current_details() {
        let current = "llama4";
        let discovered = vec![
            DynamicModelEntry {
                id: current.to_string(),
                label: "Llama 4 · Q4_K_M".to_string(),
                detail: Some("qwen35 · fits".to_string()),
            },
            DynamicModelEntry {
                id: "qwen3".to_string(),
                label: "Qwen 3".to_string(),
                detail: None,
            },
            DynamicModelEntry {
                id: "qwen3".to_string(),
                label: "Duplicate Qwen 3".to_string(),
                detail: None,
            },
        ];
        let result = dynamic_models_for_backend("openai-compatible", Some(current), &discovered);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].label, "Llama 4 · Q4_K_M");
        assert_eq!(result[1].id, "qwen3");
    }

    #[test]
    fn dynamic_models_unknown_backend_returns_empty() {
        let result = dynamic_models_for_backend("unknown-backend", Some("foo"), &[]);
        assert!(result.is_empty());
    }
}
