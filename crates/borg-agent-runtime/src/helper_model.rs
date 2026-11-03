//! One-shot text from a model-only route, for background helpers such as
//! commit messages and thread titles. No tools and no external agent loop.

use std::time::Duration;

use borg_provider::provider::{ModelGateway, ModelMessage, ModelTurnRequest};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::CodingProvider;

/// Default route for every background helper.
pub const DEFAULT_HELPER_MODEL: &str = "gpt-6-luna@low";

/// A helper route written `MODEL[@EFFORT]` or `PROVIDER::MODEL[@EFFORT]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HelperModel {
    pub model: String,
    /// Empty uses the provider's default effort.
    pub effort: String,
}

impl HelperModel {
    /// Parse a spec; blank selects [`DEFAULT_HELPER_MODEL`].
    pub fn parse(spec: &str) -> Self {
        let spec = spec.trim();
        let spec = if spec.is_empty() {
            DEFAULT_HELPER_MODEL
        } else {
            spec
        };
        let (model, effort) = spec.rsplit_once('@').unwrap_or((spec, ""));
        Self {
            model: model.trim().to_string(),
            effort: effort.trim().to_string(),
        }
    }

    pub fn label(&self) -> String {
        if self.effort.is_empty() {
            self.model.clone()
        } else {
            format!("{}@{}", self.model, self.effort)
        }
    }

    pub fn provider(&self) -> Option<CodingProvider> {
        if let Some((provider, _)) = self.model.split_once("::") {
            CodingProvider::ALL
                .into_iter()
                .find(|candidate| candidate.config_alias() == provider)
        } else {
            CodingProvider::for_model(&self.model)
        }
    }

    pub fn model_id(&self) -> &str {
        self.model
            .split_once("::")
            .map_or(self.model.as_str(), |(_, model)| model)
    }
}

/// Which new threads get a model-written title.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "snake_case")]
pub enum ThreadTitleScope {
    /// Keep the title taken from the first prompt.
    Off,
    /// Only threads that already run on the title model's provider, so the
    /// first prompt never goes to a provider the thread does not use.
    #[default]
    SameProvider,
    /// Every thread, whichever provider runs it.
    AllProviders,
}

/// Ask `model` for one reply to `prompt`. Returns the text and total tokens,
/// or None on any failure so callers can keep their deterministic fallback.
/// A `gateway` routes the call through a configured OpenAI-compatible endpoint.
pub async fn helper_model_text(
    model: &HelperModel,
    gateway: Option<&ModelGateway>,
    prompt: String,
    timeout: Duration,
) -> Option<(String, u64)> {
    #[cfg(feature = "subscription-adapters")]
    use borg_provider::provider::{ClaudeModelProvider, CodexModelProvider};
    use borg_provider::provider::{OpenAiCompatibleProfile as Profile, OpenAiCompatibleProvider};

    let request = ModelTurnRequest {
        fast: false,
        ultrafast: false,
        request_id: Some(Uuid::new_v4().to_string()),
        session_id: None,
        prompt_cache_key: None,
        turn_routing: Default::default(),
        messages: vec![ModelMessage::user(prompt)],
        tools: Vec::new(),
        output_schema: None,
    };
    let effort = (!model.effort.is_empty()).then(|| model.effort.clone());
    let provider = if gateway.is_some() {
        CodingProvider::OpenAiCompatible
    } else {
        model.provider()?
    };
    let turn = async {
        match provider {
            #[cfg(feature = "subscription-adapters")]
            CodingProvider::Claude if !borg_provider::credentials::claude_uses_api_key() => {
                let account = ClaudeModelProvider::account_identity(None).await.ok()?;
                ClaudeModelProvider {
                    model: model.model_id().to_string(),
                    effort,
                    refusal_fallback: false,
                }
                .model_turn_for_account(request, None, None, None, Some(&account))
                .await
                .ok()
            }
            #[cfg(feature = "subscription-adapters")]
            CodingProvider::Codex => {
                let account = CodexModelProvider::account_identity().await.ok()?;
                CodexModelProvider {
                    model: model.model_id().to_string(),
                    effort: model.effort.clone(),
                }
                .model_turn_for_account(request, None, &account)
                .await
                .ok()
            }
            CodingProvider::Anthropic | CodingProvider::Claude
                if provider == CodingProvider::Anthropic
                    || borg_provider::credentials::claude_uses_api_key() =>
            {
                let key = borg_provider::credentials::api_key(
                    borg_provider::credentials::ApiKeyCredential::Anthropic,
                )?;
                borg_provider::provider::AnthropicMessagesProvider {
                    model: model.model_id().to_string(),
                    effort,
                }
                .model_turn(request, None, &key)
                .await
                .ok()
            }
            CodingProvider::Kimi
            | CodingProvider::Glm
            | CodingProvider::Qwen
            | CodingProvider::OpenRouter
            | CodingProvider::Vercel
            | CodingProvider::OpenCode
            | CodingProvider::OpenAiCompatible => {
                let go_gateway = if provider == CodingProvider::OpenCode {
                    Some(
                        borg_provider::provider::opencode_model::gateway(
                            model.model_id(),
                            Uuid::new_v4(),
                        )
                        .ok()?,
                    )
                } else {
                    None
                };
                let gateway = gateway.or(go_gateway.as_ref());
                let profile = match provider {
                    CodingProvider::Kimi => Profile::Kimi,
                    CodingProvider::Glm => Profile::Glm,
                    CodingProvider::Qwen => Profile::Qwen,
                    CodingProvider::OpenRouter => Profile::OpenRouter,
                    CodingProvider::Vercel => Profile::Vercel,
                    _ => Profile::Generic,
                };
                OpenAiCompatibleProvider {
                    model: gateway
                        .and_then(|gateway| gateway.model.clone())
                        .unwrap_or_else(|| model.model_id().to_string()),
                    effort,
                    system_prompt: "",
                }
                .model_turn_via_profile(request, None, gateway, profile)
                .await
                .ok()
            }
            _ => None,
        }
    };
    let result = tokio::time::timeout(timeout, turn).await.ok()??;
    let (content, _, calls) = result.assistant_parts()?;
    if !calls.is_empty() {
        return None;
    }
    Some((content.clone()?, result.usage.total_tokens))
}
