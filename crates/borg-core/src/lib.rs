//! The minimal provider-neutral contract used by Borg model adapters.
//!
//! This crate intentionally has no HTTP, subprocess, MCP, credential, or
//! vendor SDK dependency. It is suitable for a small agent loop, a custom
//! model adapter, or a product embedding Borg's canonical tool/message shape.

pub mod compaction;
pub mod model;
pub mod usage;
pub mod warming;

pub use compaction::{
    CompactionBudgetOverride, CompactionBudgetPolicy, CompactionBudgetSource,
    CompactionPolicyError, EffectiveCompactionBudget,
};
pub use model::{
    ModelFunctionCall, ModelInputAttachment, ModelMessage, ModelProviderState, ModelToolCall,
    ModelToolDefinition, ModelTurnRequest, TurnRouting,
};
pub use usage::{CostBasis, ProviderCallUsage, ProviderChannel};
pub use warming::CacheWarmingMode;
