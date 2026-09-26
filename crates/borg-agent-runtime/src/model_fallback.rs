//! Ordered model fallback.
//!
//! A session configured with a chain of [`ModelRoute`]s runs on the first
//! route that has quota. When that route reaches a usage limit the session
//! records when it resets and continues the same turn on the next available
//! route, where it stays until the user changes the model or that route is
//! limited too. Only when every route is limited does it wait, and then for the
//! route that resets first.

use std::collections::HashMap;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};

use crate::{BillingLane, CodingProvider, ModelRoute, ProviderCapability};

impl ModelRoute {
    /// Parse `model`, `model@effort`, `provider/model@effort` or
    /// `provider@effort`. A model id is tried whole before the text is split on
    /// `/`, so ids that contain a slash (`opencode-go/deepseek-v4.1`) keep it.
    pub fn parse(spec: &str) -> Result<Self> {
        let spec = spec.trim();
        anyhow::ensure!(!spec.is_empty(), "a model route must not be empty");
        let (route, effort) = match spec.rsplit_once('@') {
            Some((route, effort)) if !effort.trim().is_empty() => {
                (route.trim(), Some(effort.trim().to_ascii_lowercase()))
            }
            _ => (spec, None),
        };
        let (provider, model) = if let Some(provider) = CodingProvider::for_model(route) {
            (provider, Some(route.to_string()))
        } else if let Some(provider) = provider_named(route) {
            (provider, None)
        } else {
            let (hint, model) = route
                .split_once('/')
                .with_context(|| format!("unknown provider or model `{route}`"))?;
            let provider = provider_named(hint.trim())
                .with_context(|| format!("unknown provider `{hint}` in model route `{spec}`"))?;
            let model = model.trim();
            anyhow::ensure!(!model.is_empty(), "model route `{spec}` names no model");
            (provider, Some(model.to_string()))
        };
        let model = model.or_else(|| {
            provider
                .model_catalog()
                .map(|catalog| catalog.default_model.to_string())
        });
        if let Some(effort) = &effort {
            anyhow::ensure!(
                provider
                    .model_catalog()
                    .is_none_or(|catalog| catalog.supports_effort(effort)),
                "{} does not support effort `{effort}` (model route `{spec}`)",
                provider.label()
            );
        }
        Ok(Self {
            provider,
            model,
            effort,
            allow_api_billing: false,
        })
    }

    /// The route as it is written: the model alone when it implies the
    /// provider, otherwise `provider/model`, then `@effort`. Two routes on the
    /// same model but different lanes (Claude subscription, Anthropic API)
    /// therefore never share a label, and never share a usage limit.
    pub fn label(&self) -> String {
        let provider = provider_slug(self.provider).to_string();
        let mut label = match &self.model {
            Some(model) if CodingProvider::for_model(model) == Some(self.provider) => model.clone(),
            Some(model) => format!("{provider}/{model}"),
            None => provider,
        };
        if let Some(effort) = &self.effort {
            label.push('@');
            label.push_str(effort);
        }
        label
    }

    fn runs(&self, provider: CodingProvider, model: Option<&str>) -> bool {
        self.provider == provider
            && self
                .model
                .as_deref()
                .is_none_or(|route_model| Some(route_model) == model)
    }

    /// Whether the host can run this route on a lane it is allowed to spend.
    /// A host that did not report the provider is given the benefit of the
    /// doubt: the turn itself will say if the lane is unusable.
    fn permitted(&self, capabilities: &[ProviderCapability]) -> bool {
        let Some(capability) = capabilities
            .iter()
            .find(|capability| capability.provider == self.provider)
        else {
            return true;
        };
        capability.authenticated
            && match capability.billing {
                Some(BillingLane::ApiKey) => self.allow_api_billing,
                Some(BillingLane::Subscription | BillingLane::Endpoint) | None => true,
            }
    }
}

/// The name a route spec uses for `provider`; `provider_named` reads it back.
fn provider_slug(provider: CodingProvider) -> &'static str {
    match provider {
        CodingProvider::Codex => "codex",
        CodingProvider::Claude => "claude",
        CodingProvider::Anthropic => "anthropic",
        CodingProvider::OpenCode => "opencode",
        CodingProvider::OpenRouter => "openrouter",
        CodingProvider::Vercel => "vercel",
        CodingProvider::OpenAiCompatible => "openai-compatible",
        CodingProvider::Kimi => "kimi",
        CodingProvider::Glm => "glm",
        CodingProvider::Qwen => "qwen",
        CodingProvider::Grok => "grok",
        CodingProvider::Muse => "muse",
    }
}

fn provider_named(name: &str) -> Option<CodingProvider> {
    Some(match name.to_ascii_lowercase().as_str() {
        "codex" | "gpt" | "openai" => CodingProvider::Codex,
        "claude" => CodingProvider::Claude,
        "anthropic" | "anthropic-api" | "anthropic_api" => CodingProvider::Anthropic,
        "opencode" | "open-code" | "open_code" => CodingProvider::OpenCode,
        "openrouter" | "open-router" => CodingProvider::OpenRouter,
        "vercel" | "vercel-ai-gateway" => CodingProvider::Vercel,
        "openai-compatible" | "open-ai-compatible" => CodingProvider::OpenAiCompatible,
        "kimi" => CodingProvider::Kimi,
        "glm" => CodingProvider::Glm,
        "qwen" => CodingProvider::Qwen,
        "grok" => CodingProvider::Grok,
        "muse" => CodingProvider::Muse,
        _ => return None,
    })
}

/// When each route of the chain may be tried again, keyed by its label so a
/// reordered chain still remembers which lane is exhausted.
#[derive(Debug, Default, Clone)]
pub(crate) struct RouteLimits {
    until: HashMap<String, DateTime<Utc>>,
}

/// The journal event recording that a route reached its limit.
pub(crate) const ROUTE_LIMITED_EVENT: &str = "model_route_limited";

impl RouteLimits {
    /// Rebuild the deadlines a previous run of the session recorded, so a
    /// restart does not retry a route that is still exhausted.
    pub(crate) fn from_events(events: &[crate::SessionEvent]) -> Self {
        let mut limits = Self::default();
        for event in events {
            if let crate::SessionEventKind::ProviderEvent { kind, payload, .. } = &event.kind
                && kind == ROUTE_LIMITED_EVENT
                && let (Ok(route), Some(until)) = (
                    serde_json::from_value::<ModelRoute>(payload["route"].clone()),
                    payload["until"]
                        .as_str()
                        .and_then(|until| DateTime::parse_from_rfc3339(until).ok()),
                )
            {
                limits.limit(&route, until.with_timezone(&Utc));
            }
        }
        limits
    }

    pub(crate) fn limit(&mut self, route: &ModelRoute, until: DateTime<Utc>) {
        self.until.insert(route.label(), until);
    }

    fn limited(&self, route: &ModelRoute, now: DateTime<Utc>) -> bool {
        self.until
            .get(&route.label())
            .is_some_and(|until| *until > now)
    }

    /// The earliest moment any route of the chain clears, for when all are
    /// limited.
    pub(crate) fn earliest_reset(&self, chain: &[ModelRoute]) -> Option<(usize, DateTime<Utc>)> {
        chain
            .iter()
            .enumerate()
            .filter_map(|(index, route)| {
                self.until.get(&route.label()).map(|until| (index, *until))
            })
            .min_by_key(|(_, until)| *until)
    }
}

/// The chain entry the session is running on, if it is on the chain at all.
pub(crate) fn current_route(
    chain: &[ModelRoute],
    provider: CodingProvider,
    model: Option<&str>,
) -> Option<usize> {
    // No model means the provider's default, which a route may name.
    let default = provider
        .model_catalog()
        .map(|catalog| catalog.default_model.to_string());
    let model = model.or(default.as_deref());
    chain.iter().position(|route| route.runs(provider, model))
}

/// The route the session should run on: the first one in order that has
/// quota and a permitted billing lane.
pub(crate) fn preferred_route(
    chain: &[ModelRoute],
    limits: &RouteLimits,
    capabilities: &[ProviderCapability],
    now: DateTime<Utc>,
) -> Option<usize> {
    chain
        .iter()
        .position(|route| !limits.limited(route, now) && route.permitted(capabilities))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route(spec: &str) -> ModelRoute {
        ModelRoute::parse(spec).unwrap()
    }

    #[test]
    fn routes_parse_models_providers_and_efforts() {
        let codex = route("gpt-6-sol@xhigh");
        assert_eq!(codex.provider, CodingProvider::Codex);
        assert_eq!(codex.model.as_deref(), Some("gpt-6-sol"));
        assert_eq!(codex.effort.as_deref(), Some("xhigh"));
        let opencode = route("opencode-go/deepseek-v4.1");
        assert_eq!(opencode.provider, CodingProvider::OpenCode);
        assert_eq!(opencode.model.as_deref(), Some("opencode-go/deepseek-v4.1"));
        assert_eq!(route("claude").provider, CodingProvider::Claude);
        assert!(ModelRoute::parse("nonsense/thing").is_err());
        assert!(ModelRoute::parse("gpt-6-sol@loud").is_err());
        // A label reads back as the same route, lane included.
        for spec in [
            "anthropic/claude-opus-5-5@high",
            "gpt-6-sol@xhigh",
            "opencode-go/deepseek-v4.1",
        ] {
            let parsed = route(spec);
            assert_eq!(route(&parsed.label()), parsed, "{spec}");
        }
    }

    #[test]
    fn the_chain_falls_back_in_order_and_returns_once_a_limit_clears() {
        let chain = [
            route("claude"),
            route("gpt-6-sol@xhigh"),
            route("opencode-go/deepseek-v4.1"),
        ];
        let now = Utc::now();
        let mut limits = RouteLimits::default();
        assert_eq!(preferred_route(&chain, &limits, &[], now), Some(0));
        limits.limit(&chain[0], now + chrono::Duration::hours(2));
        assert_eq!(preferred_route(&chain, &limits, &[], now), Some(1));
        limits.limit(&chain[1], now + chrono::Duration::hours(1));
        assert_eq!(preferred_route(&chain, &limits, &[], now), Some(2));
        limits.limit(&chain[2], now + chrono::Duration::hours(3));
        assert_eq!(preferred_route(&chain, &limits, &[], now), None);
        assert_eq!(
            limits.earliest_reset(&chain).map(|(index, _)| index),
            Some(1)
        );
        // Once the first route resets the session goes back to it.
        let later = now + chrono::Duration::hours(2) + chrono::Duration::seconds(1);
        assert_eq!(preferred_route(&chain, &limits, &[], later), Some(0));
    }

    #[test]
    fn a_route_never_spends_api_credit_unless_allowed() {
        let chain = [route("claude"), route("gpt-6-sol")];
        let capability = |provider, billing| ProviderCapability {
            provider,
            installed: true,
            version: None,
            authenticated: true,
            auth_detail: None,
            auth_methods: Vec::new(),
            can_spawn: true,
            usage: None,
            billing: Some(billing),
        };
        let capabilities = [
            capability(CodingProvider::Claude, BillingLane::ApiKey),
            capability(CodingProvider::Codex, BillingLane::Subscription),
        ];
        let limits = RouteLimits::default();
        assert_eq!(
            preferred_route(&chain, &limits, &capabilities, Utc::now()),
            Some(1)
        );
        let mut allowed = chain.clone();
        allowed[0].allow_api_billing = true;
        assert_eq!(
            preferred_route(&allowed, &limits, &capabilities, Utc::now()),
            Some(0)
        );
    }

    #[test]
    fn the_session_knows_which_route_it_runs_on() {
        let chain = [route("claude"), route("gpt-6-sol@xhigh")];
        assert_eq!(
            current_route(&chain, CodingProvider::Codex, Some("gpt-6-sol")),
            Some(1)
        );
        assert_eq!(
            current_route(&chain, CodingProvider::Codex, Some("gpt-5")),
            None
        );
    }
}
