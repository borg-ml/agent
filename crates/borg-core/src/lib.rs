//! The minimal provider-neutral contract used by Borg model adapters.
//!
//! This crate intentionally has no HTTP, subprocess, MCP, credential, or
//! vendor SDK dependency. It is suitable for a small agent loop, a custom
//! model adapter, or a product embedding Borg's canonical tool/message shape.

pub mod model;
pub mod usage;

pub use model::{
    ModelFunctionCall, ModelInputAttachment, ModelMessage, ModelToolCall, ModelToolDefinition,
    ModelTurnRequest,
};
pub use usage::{CostBasis, ProviderCallUsage, ProviderChannel};
