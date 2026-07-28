use std::fmt;
use std::time::Instant;

use serde::{Deserialize, Serialize};

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
    "gpt-5.6-sol"
}

pub fn codex_default_effort() -> &'static str {
    "low"
}

pub fn codex_effort_levels() -> Vec<String> {
    ["low", "medium", "high", "xhigh", "max", "ultra"]
        .map(str::to_string)
        .to_vec()
}

pub fn codex_effort_supported(value: &str) -> bool {
    matches!(value, "low" | "medium" | "high" | "xhigh" | "max" | "ultra")
}

pub fn kimi_product_model() -> &'static str {
    "kimi-k3"
}

pub fn kimi_default_effort() -> &'static str {
    "low"
}

pub fn kimi_effort_levels() -> Vec<String> {
    ["low", "medium", "high"].map(str::to_string).to_vec()
}
