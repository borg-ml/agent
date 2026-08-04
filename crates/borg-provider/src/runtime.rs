use std::fmt;
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

pub const KIMI_SELECTABLE_MODELS: [(&str, &str); 1] = [("kimi-k3", "Kimi K3")];
pub const KIMI_EFFORT_LEVELS: [&str; 3] = ["low", "high", "max"];
pub const KIMI_MODEL_CATALOG: ProviderModelCatalog = ProviderModelCatalog {
    backend: "kimi",
    default_model: "kimi-k3",
    selectable_models: &KIMI_SELECTABLE_MODELS,
    effort_levels: &KIMI_EFFORT_LEVELS,
};

pub const MODEL_CATALOGS: [ProviderModelCatalog; 3] = [
    CODEX_MODEL_CATALOG,
    CLAUDE_MODEL_CATALOG,
    KIMI_MODEL_CATALOG,
];

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
    #[default]
    Unavailable,
}

impl CostBasis {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProviderReported => "provider_reported",
            Self::EstimatedFromPricing => "estimated_from_pricing",
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

pub fn kimi_product_model() -> &'static str {
    KIMI_MODEL_CATALOG.default_model
}

pub fn kimi_default_effort() -> &'static str {
    "max"
}

pub fn kimi_effort_levels() -> Vec<String> {
    KIMI_MODEL_CATALOG
        .effort_levels
        .iter()
        .map(|effort| (*effort).to_string())
        .collect()
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

/// Return dynamic model entries for `backend` given a set of discovered models.
///
/// Returns an empty vec for any backend that has a compile-time const catalog,
/// because those backends must never mix with the dynamic arm.
///
/// For `"openai-compatible"` (and any other backend without a catalog) the rules are:
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

    // Special case: "openai-compatible" is the primary open-ended backend we
    // expose via the picker right now.
    let vec = match backend {
        "openai-compatible" => {
            let mut out = Vec::new();
            if let Some(cur) = current {
                out.push(DynamicModelEntry {
                    id: cur.to_string(),
                    label: cur.to_string(),
                    detail: None,
                });
            }
            let seen_ids: std::collections::HashSet<String> = out
                .iter()
                .map(|e| e.id.clone())
                .collect();
            for entry in discovered {
                if !seen_ids.contains(&entry.id) {
                    out.push(entry.clone());
                }
            }
            out
        }
        _ => Vec::new(),
    };

    vec
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
    fn dynamic_models_kimi_backend_returns_empty() {
        let result = dynamic_models_for_backend("kimi", Some("kimi-k3"), &[]);
        assert!(result.is_empty());
    }

    #[test]
    fn dynamic_models_openai_compatible_current_first() {
        let current = "qwen3.6:35b-a3b";
        let discovered = vec![
            DynamicModelEntry { id: "llama4".to_string(), label: "Llama 4".to_string(), detail: None },
            DynamicModelEntry { id: "mixtral".to_string(), label: "Mixtral".to_string(), detail: None },
        ];
        let result = dynamic_models_for_backend("openai-compatible", Some(current), &discovered);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].id, current);
        assert_eq!(result[1].id, "llama4");
        assert_eq!(result[2].id, "mixtral");
    }

    #[test]
    fn dynamic_models_openai_compatible_no_current() {
        let discovered = vec![
            DynamicModelEntry { id: "qwen3".to_string(), label: "Qwen 3".to_string(), detail: None },
        ];
        let result = dynamic_models_for_backend("openai-compatible", None, &discovered);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, "qwen3");
    }

    #[test]
    fn dynamic_models_dedup_when_current_in_discovered() {
        let current = "llama4";
        let discovered = vec![
            DynamicModelEntry { id: "llama4".to_string(), label: "Llama 4".to_string(), detail: None },
            DynamicModelEntry { id: "qwen3".to_string(), label: "Qwen 3".to_string(), detail: None },
        ];
        let result = dynamic_models_for_backend("openai-compatible", Some(current), &discovered);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].id, current); // current first
        assert_eq!(result[1].id, "qwen3"); // llama4 deduped out
    }

    #[test]
    fn dynamic_models_unknown_backend_returns_empty() {
        let result = dynamic_models_for_backend("unknown-backend", Some("foo"), &[]);
        assert!(result.is_empty());
    }
}
