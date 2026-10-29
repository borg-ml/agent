//! Named model endpoints shared by configuration and the settings UI.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ConfiguredProvider {
    /// Currently `openai-compatible`; the field makes the config forward
    /// compatible with future provider protocols without hiding semantics.
    pub protocol: String,
    pub name: Option<String>,
    /// Base URL or a complete `/chat/completions` URL.
    pub base_url: String,
    /// Prefer an environment reference for secrets. `api_key` is supported
    /// for local-only setups but is never copied into durable session state.
    pub api_key_env: Option<String>,
    pub api_key: Option<String>,
    pub headers: BTreeMap<String, String>,
    pub models: BTreeMap<String, ConfiguredModel>,
}

impl Default for ConfiguredProvider {
    fn default() -> Self {
        Self {
            protocol: "openai-compatible".to_string(),
            name: None,
            base_url: String::new(),
            api_key_env: None,
            api_key: None,
            headers: BTreeMap::new(),
            models: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ConfiguredModel {
    pub name: Option<String>,
    pub context_window_tokens: Option<u64>,
    pub max_output_tokens: Option<u64>,
    /// How long this route keeps a prompt cache entry. Set it only from the
    /// upstream's documented retention: it is the schedule Borg pays refreshes
    /// on, and there is no default, so leaving it unset means no warming.
    pub prompt_cache_ttl_seconds: Option<u64>,
    /// Variant names normally match Borg effort values (`low`, `high`, ...).
    pub variants: BTreeMap<String, ConfiguredModelVariant>,
    /// Extra request fields for this model, such as `temperature` or a vendor
    /// routing object. Core conversation/tool fields are protected by the
    /// provider adapter.
    pub body: BTreeMap<String, toml::Value>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ConfiguredModelVariant {
    pub body: BTreeMap<String, toml::Value>,
}

impl std::fmt::Debug for ConfiguredProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConfiguredProvider")
            .field("protocol", &self.protocol)
            .field("name", &self.name)
            .field("base_url", &self.base_url)
            .field("api_key_env", &self.api_key_env)
            .field("api_key", &self.api_key.as_ref().map(|_| "[redacted]"))
            .field("header_names", &self.headers.keys().collect::<Vec<_>>())
            .field("model_ids", &self.models.keys().collect::<Vec<_>>())
            .finish()
    }
}
