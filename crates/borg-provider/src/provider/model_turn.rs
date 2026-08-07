pub use borg_core::{
    ModelFunctionCall, ModelInputAttachment, ModelMessage, ModelToolCall, ModelToolDefinition,
    ModelTurnRequest,
};

use crate::runtime::ProviderCallUsage;

use super::ProviderAttemptTrace;

#[derive(Debug, Clone)]
pub struct ModelTurnResult {
    pub message: ModelMessage,
    pub finish_reason: String,
    pub usage: ProviderCallUsage,
    pub raw_response: serde_json::Value,
    pub trace: ProviderAttemptTrace,
}

impl ModelTurnResult {
    pub fn assistant_parts(&self) -> Option<(&Option<String>, &Option<String>, &[ModelToolCall])> {
        match &self.message {
            ModelMessage::Assistant {
                content,
                reasoning_content,
                reasoning_details: _,
                tool_calls,
            } => Some((content, reasoning_content, tool_calls)),
            _ => None,
        }
    }
}
