use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CostBasis {
    ProviderReported,
    EstimatedFromPricing,
    /// A provider exposed token counters for a subscription-backed session,
    /// but the account is not billed at the public API rate.
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
    /// Input that was neither read from nor written to the prompt cache.
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub cached_input_tokens: u64,
    #[serde(default)]
    pub cache_creation_input_tokens: u64,
    /// Includes reasoning tokens when the provider counts them as output.
    #[serde(default)]
    pub output_tokens: u64,
    /// All processed input (including cache reads/writes) plus output. This is
    /// not a subscription allowance debit or a provider's blended UI counter.
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
