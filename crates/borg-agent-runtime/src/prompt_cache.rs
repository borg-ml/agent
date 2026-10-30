use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::{CodingProvider, SessionEvent, SessionEventKind};

pub(crate) const PROMPT_CACHE_OBSERVED: &str = "prompt_cache_observed";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum PromptCacheRetention {
    #[default]
    Unknown,
    Seconds(u64),
    Indefinite,
}

/// Last completed request or successful keepalive, not the session activity clock.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct PromptCacheObservation {
    pub at: DateTime<Utc>,
    pub provider: CodingProvider,
    pub model: String,
    pub effort: Option<String>,
    pub retention: PromptCacheRetention,
    pub prompt_tokens: u64,
    pub cached_input_tokens: u64,
}

impl PromptCacheObservation {
    pub(crate) fn apply(slot: &mut Option<Self>, event: &SessionEvent) {
        match &event.kind {
            SessionEventKind::ProviderEvent { kind, payload, .. }
                if kind == PROMPT_CACHE_OBSERVED =>
            {
                *slot = serde_json::from_value(payload.clone()).ok();
            }
            SessionEventKind::SessionConfigured {
                provider,
                model,
                effort,
                ..
            } if slot.as_ref().is_some_and(|cache| {
                cache.provider != *provider
                    || model.as_deref() != Some(cache.model.as_str())
                    || (cache.effort != *effort
                        && !(cache.provider == CodingProvider::Claude
                            && matches!(
                                cache.model.as_str(),
                                "claude-opus-5-5" | "claude-fable-5-1"
                            )))
            }) =>
            {
                *slot = None
            }
            SessionEventKind::ContextCleared => *slot = None,
            SessionEventKind::ProviderEvent { kind, .. } if kind == "native_context_edit" => {
                *slot = None;
            }
            kind if kind.is_completed_context_compaction() => *slot = None,
            _ => {}
        }
    }

    pub(crate) fn status(
        &self,
        now: DateTime<Utc>,
        provider: CodingProvider,
        model: Option<&str>,
        effort: Option<&str>,
    ) -> &'static str {
        if self.prompt_tokens == 0 {
            return "unknown (prompt usage unavailable)";
        }
        if self.provider != provider || model.is_some_and(|model| model != self.model) {
            return "cold (provider/model changed)";
        }
        if self.effort.as_deref() != effort
            && !(provider == CodingProvider::Claude
                && matches!(self.model.as_str(), "claude-opus-5-5" | "claude-fable-5-1"))
        {
            return "unknown (effort changed)";
        }
        let age = now.signed_duration_since(self.at);
        if age.num_seconds() < 0 {
            return "unknown (clock skew)";
        }
        match self.retention {
            PromptCacheRetention::Unknown => "unknown (retention unavailable)",
            PromptCacheRetention::Indefinite => "likely warm (no expiry on this route)",
            PromptCacheRetention::Seconds(seconds) if age.num_seconds() as u64 >= seconds => {
                "possibly cold (retention window elapsed)"
            }
            PromptCacheRetention::Seconds(_) => "likely warm (within retention window)",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EventPersistence, SessionState, SessionStatus};
    use chrono::Duration;
    use uuid::Uuid;

    #[test]
    fn prompt_cache_replays_without_refreshing_on_unrelated_activity() {
        let session = Uuid::new_v4();
        let at = Utc::now() - Duration::hours(2);
        let cache = PromptCacheObservation {
            at,
            provider: CodingProvider::Claude,
            model: "claude-opus-5-5".into(),
            effort: Some("high".into()),
            retention: PromptCacheRetention::Seconds(3600),
            prompt_tokens: 100_000,
            cached_input_tokens: 90_000,
        };
        let observation = SessionEvent::new(
            session,
            1,
            SessionEventKind::ProviderEvent {
                provider: cache.provider,
                kind: PROMPT_CACHE_OBSERVED.into(),
                payload: serde_json::json!(cache),
            },
        );
        assert_eq!(observation.kind.persistence(), EventPersistence::Durable);
        assert!(
            !observation.kind.is_fork_inheritable(),
            "a fork starts a new cache epoch"
        );
        let activity = SessionEvent::new(
            session,
            2,
            SessionEventKind::StatusChanged {
                status: SessionStatus::Ready,
                detail: None,
                lifecycle: None,
            },
        );
        let state = SessionState::reduce(&[observation, activity]).unwrap();
        let restored: SessionState =
            serde_json::from_value(serde_json::to_value(state).unwrap()).unwrap();
        let restored_cache = restored.prompt_cache.unwrap();
        assert_eq!(restored_cache, cache);
        assert!(
            cache
                .status(
                    at + Duration::seconds(3599),
                    cache.provider,
                    Some(&cache.model),
                    Some("high")
                )
                .starts_with("likely warm")
        );
        assert!(
            cache
                .status(
                    at + Duration::seconds(3600),
                    cache.provider,
                    Some(&cache.model),
                    Some("high")
                )
                .starts_with("possibly cold")
        );
        assert!(
            cache
                .status(
                    at + Duration::hours(2),
                    cache.provider,
                    Some(&cache.model),
                    Some("low")
                )
                .starts_with("possibly cold")
        );
        assert!(
            cache
                .status(at, cache.provider, Some("other"), Some("high"))
                .starts_with("cold")
        );
        let unknown = PromptCacheObservation {
            retention: PromptCacheRetention::Unknown,
            ..cache.clone()
        };
        assert!(
            unknown
                .status(at, cache.provider, Some(&cache.model), Some("high"))
                .starts_with("unknown")
        );
        let indefinite = PromptCacheObservation {
            retention: PromptCacheRetention::Indefinite,
            ..cache.clone()
        };
        assert!(
            indefinite
                .status(
                    at + Duration::days(7),
                    cache.provider,
                    Some(&cache.model),
                    Some("high")
                )
                .starts_with("likely warm")
        );
        let mut slot = Some(cache);
        PromptCacheObservation::apply(
            &mut slot,
            &SessionEvent::new(session, 3, SessionEventKind::ContextCleared),
        );
        assert!(slot.is_none());
    }
}
