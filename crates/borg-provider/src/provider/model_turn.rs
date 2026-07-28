use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::runtime::ProviderCallUsage;

use super::ProviderAttemptTrace;

/// Provider-neutral conversation state for Borg-owned agent loops.
///
/// These messages are intentionally limited to the roles required by a
/// tool-calling model. Provider adapters translate this contract to their wire
/// format while Borg remains responsible for persistence and tool execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "role", rename_all = "lowercase")]
pub enum ModelMessage {
    System {
        content: String,
    },
    User {
        content: String,
    },
    Assistant {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reasoning_content: Option<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        tool_calls: Vec<ModelToolCall>,
    },
    Tool {
        tool_call_id: String,
        content: String,
    },
}

impl ModelMessage {
    pub fn assistant(
        content: Option<String>,
        reasoning_content: Option<String>,
        tool_calls: Vec<ModelToolCall>,
    ) -> Self {
        Self::Assistant {
            content,
            reasoning_content,
            tool_calls,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelToolCall {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub function: ModelFunctionCall,
}

impl ModelToolCall {
    pub fn function(id: String, name: String, arguments: String) -> Self {
        Self {
            id,
            kind: "function".to_string(),
            function: ModelFunctionCall { name, arguments },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelFunctionCall {
    pub name: String,
    pub arguments: String,
}

/// One callable function exposed by the Borg harness.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

impl ModelToolDefinition {
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        input_schema: Value,
    ) -> Result<Self, String> {
        let name = name.into();
        if name.is_empty()
            || name.len() > 64
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        {
            return Err(format!(
                "tool name `{name}` must be 1-64 ASCII letters, digits, underscores, or hyphens"
            ));
        }
        if !input_schema.is_object() {
            return Err(format!("tool `{name}` input schema must be a JSON object"));
        }
        Ok(Self {
            name,
            description: description.into(),
            input_schema,
        })
    }

    pub fn from_mcp_spec(spec: &Value) -> Result<Self, String> {
        let name = spec
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| !name.trim().is_empty())
            .ok_or_else(|| "tool spec is missing a nonempty name".to_string())?;
        let description = spec
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let input_schema = spec
            .get("inputSchema")
            .cloned()
            .unwrap_or_else(|| json!({ "type": "object", "properties": {} }));
        Self::new(name, description, input_schema)
    }

    pub fn chat_completions_value(&self) -> Value {
        json!({
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.input_schema,
            }
        })
    }
}

#[derive(Debug, Clone)]
pub struct ModelTurnRequest {
    /// Stable idempotency key for one provider request. Harnesses must reuse
    /// this value when retrying the same model turn and choose a new value for
    /// the next tool round.
    pub request_id: Option<String>,
    pub messages: Vec<ModelMessage>,
    pub tools: Vec<ModelToolDefinition>,
    pub output_schema: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct ModelTurnResult {
    pub message: ModelMessage,
    pub finish_reason: String,
    pub usage: ProviderCallUsage,
    pub raw_response: Value,
    pub trace: ProviderAttemptTrace,
}

impl ModelTurnResult {
    pub fn assistant_parts(&self) -> Option<(&Option<String>, &Option<String>, &[ModelToolCall])> {
        match &self.message {
            ModelMessage::Assistant {
                content,
                reasoning_content,
                tool_calls,
            } => Some((content, reasoning_content, tool_calls)),
            _ => None,
        }
    }
}
