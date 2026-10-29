//! Sensitive interaction metadata is durable; human answers are not.
use serde_json::Value;

#[derive(Clone)]
pub struct SensitiveInteractionResponse(Value);
impl SensitiveInteractionResponse {
    pub fn new(value: Value) -> Self {
        Self(value)
    }
    pub fn into_value(self) -> Value {
        self.0
    }
}
impl std::fmt::Debug for SensitiveInteractionResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[private interaction response]")
    }
}

pub fn provider_interaction_contains_secret(payload: &Value) -> bool {
    fn secret_schema(schema: &Value) -> bool {
        schema
            .get("format")
            .and_then(Value::as_str)
            .is_some_and(|format| matches!(format, "password" | "secret"))
            || schema.get("writeOnly").and_then(Value::as_bool) == Some(true)
            || schema
                .get("properties")
                .and_then(Value::as_object)
                .is_some_and(|properties| properties.values().any(secret_schema))
            || schema.get("items").is_some_and(secret_schema)
    }
    payload
        .get("questions")
        .and_then(Value::as_array)
        .is_some_and(|questions| {
            questions
                .iter()
                .any(|question| question.get("isSecret").and_then(Value::as_bool) == Some(true))
        })
        || payload.get("requestedSchema").is_some_and(secret_schema)
}

pub(crate) fn journal_interaction_response(request: &Value, response: &Value) -> Value {
    if provider_interaction_contains_secret(request) {
        serde_json::json!({"redacted": true})
    } else {
        response.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn private_answers_are_not_serialized_into_resolution_events() {
        let response =
            serde_json::json!({"answers":{"password":{"answers":["private-test-value"]}}});
        for request in [
            serde_json::json!({"questions":[{"id":"password","isSecret":true}]}),
            serde_json::json!({"requestedSchema":{"properties":{"password":{"type":"string","format":"password"}}}}),
        ] {
            assert_eq!(
                journal_interaction_response(&request, &response),
                serde_json::json!({"redacted":true})
            );
        }
        assert_eq!(
            journal_interaction_response(&serde_json::json!({}), &response),
            response
        );
    }
}

use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RequestSecretArgs {
    pub provider: String,
    pub reason: String,
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,
}
fn default_timeout() -> u64 {
    600
}

pub(crate) struct SecretInputRequest {
    pub id: String,
    pub provider: String,
    pub payload: Value,
    pub expires_at: tokio::time::Instant,
    pub response: oneshot::Sender<Value>,
}

#[derive(Clone)]
pub(crate) struct SessionSecretRequests {
    pub requests: mpsc::Sender<SecretInputRequest>,
}

impl SessionSecretRequests {
    pub async fn request(&self, args: RequestSecretArgs) -> Result<Value> {
        ensure!(
            cfg!(unix),
            "Private key requests require a platform with enforced owner-only credential-file permissions"
        );
        ensure!(
            (1..=3600).contains(&args.timeout_seconds),
            "timeout_seconds must be between 1 and 3600"
        );
        ensure!(
            !args.reason.trim().is_empty() && args.reason.len() <= 2048,
            "A short purpose is required"
        );
        ensure!(
            !args.provider.is_empty()
                && args.provider.len() <= 128
                && args
                    .provider
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_')),
            "Use a built-in provider name or configured provider ID"
        );
        if builtin_credential(&args.provider)
            == Some(borg_provider::credentials::ApiKeyCredential::OpenAi)
        {
            ensure!(
                borg_provider::credentials::openai_auth_mode()?.is_some()
                    || borg_provider::credentials::openai_uses_api_key(),
                "Choose an explicit OpenAI billing lane in Connections before saving an API key"
            );
        }
        let (response, receiver) = oneshot::channel();
        let payload = serde_json::json!({
            "questions":[{"id":"api_key","header":format!("API key for {}", args.provider),
                "prompt":format!("API key for {}. {}\nSaved to this host's owner-only credential file, shared by this provider's sessions. The agent receives only a receipt. Not encrypted at rest. Do not enter passwords, card details, bank PINs or CVVs. /cancel declines.", args.provider, args.reason),
                "question":format!("{}\nSaved to this host's owner-only credential file (not encrypted at rest), shared by sessions using this provider slot. The agent receives a receipt, not the key. Enter API keys only—not passwords, card details, bank PINs or CVVs. /cancel declines.", args.reason), "isSecret":true}],
            "secret_destination": {"kind":"provider_api_key", "provider":args.provider},
        });
        self.requests
            .send(SecretInputRequest {
                id: format!("borg-secret:{}", Uuid::new_v4()),
                provider: args.provider,
                payload,
                expires_at: tokio::time::Instant::now()
                    + std::time::Duration::from_secs(args.timeout_seconds),
                response,
            })
            .await
            .context("No active human-input channel")?;
        match tokio::time::timeout(
            std::time::Duration::from_secs(args.timeout_seconds),
            receiver,
        )
        .await
        {
            Ok(Ok(receipt)) => Ok(receipt),
            Ok(Err(_)) => Ok(serde_json::json!({"status":"cancelled"})),
            Err(_) => Ok(serde_json::json!({"status":"timed_out"})),
        }
    }
}

pub(crate) async fn secret_request_closed(pending: &mut Option<SecretInputRequest>) {
    match pending {
        Some(pending) => pending.response.closed().await,
        None => std::future::pending().await,
    }
}

pub(crate) fn builtin_credential(
    provider: &str,
) -> Option<borg_provider::credentials::ApiKeyCredential> {
    use borg_provider::credentials::ApiKeyCredential as Key;
    Some(match provider {
        "openai" | "codex" => Key::OpenAi,
        "anthropic" => Key::Anthropic,
        "openrouter" | "open_router" => Key::OpenRouter,
        "vercel" => Key::Vercel,
        "opencode-go" => Key::OpenCodeGo,
        "kimi" => Key::Kimi,
        "glm" => Key::Zai,
        "qwen" => Key::Qwen,
        _ => return None,
    })
}

/// Consumes the transient answer; only this receipt may reach a model/journal.
pub(crate) fn save_requested_key(provider: &str, response: &mut Value) -> Result<Value> {
    let answer = response
        .get_mut("answers")
        .and_then(|v| v.get_mut("api_key"))
        .and_then(|v| v.get_mut("answers"))
        .and_then(Value::as_array_mut)
        .and_then(|answers| answers.first_mut());
    let Some(Value::String(key)) = answer.map(Value::take) else {
        return Ok(serde_json::json!({"status":"declined"}));
    };
    ensure!(
        !key.trim().is_empty() && key.len() <= 16_384 && !key.contains(['\0', '\r', '\n']),
        "Enter a single-line API key of at most 16 KiB"
    );
    if let Some(credential) = builtin_credential(provider) {
        if credential == borg_provider::credentials::ApiKeyCredential::OpenAi {
            ensure!(
                borg_provider::credentials::openai_auth_mode()?.is_some()
                    || borg_provider::credentials::openai_uses_api_key(),
                "Choose an explicit OpenAI billing lane in Connections before saving an API key"
            );
        }
        borg_provider::credentials::store_api_key(credential, &key)
            .context("Could not save the provider key")?;
    } else {
        borg_provider::credentials::store_named_provider_api_key(provider, &key)
            .context("Could not save the provider key")?;
    }
    Ok(serde_json::json!({"status":"saved","provider":provider,"billing_route_changed":false}))
}

#[cfg(all(test, unix))]
mod request_tests {
    use super::*;
    #[tokio::test]
    async fn private_request_returns_only_receipt_and_expiry_closes_card() {
        let (requests, mut pending) = mpsc::channel(2);
        let service = SessionSecretRequests { requests };
        let request = tokio::spawn({
            let service = service.clone();
            async move {
                service
                    .request(RequestSecretArgs {
                        provider: "anthropic".into(),
                        reason: "Test connection".into(),
                        timeout_seconds: 5,
                    })
                    .await
                    .unwrap()
            }
        });
        let card = pending.recv().await.unwrap();
        assert!(provider_interaction_contains_secret(&card.payload));
        assert_eq!(card.payload["secret_destination"]["provider"], "anthropic");
        card.response
            .send(serde_json::json!({"status":"saved","provider":"anthropic"}))
            .unwrap();
        assert_eq!(
            request.await.unwrap(),
            serde_json::json!({"status":"saved","provider":"anthropic"})
        );
        let request = tokio::spawn(async move {
            service
                .request(RequestSecretArgs {
                    provider: "anthropic".into(),
                    reason: "Test expiry".into(),
                    timeout_seconds: 1,
                })
                .await
                .unwrap()
        });
        let card = pending.recv().await.unwrap();
        assert_eq!(request.await.unwrap()["status"], "timed_out");
        assert!(card.response.is_closed());
        assert_eq!(
            save_requested_key("anthropic", &mut serde_json::json!({"answers":{}})).unwrap()["status"],
            "declined"
        );
    }
}

/// Only the session actor's request channel may issue Borg-owned key cards.
pub(crate) fn mark_external_private_request(event: &mut crate::SessionEventKind) {
    let crate::SessionEventKind::ProviderInteractionRequested {
        kind,
        detail,
        payload,
        ..
    } = event
    else {
        return;
    };
    if kind == "borg_secret_request" {
        *kind = "provider_private_input".into();
    }
    if provider_interaction_contains_secret(payload) {
        const NOTICE: &str = "Provider-owned private input: this answer goes to the external provider, not Borg's credential store. Use Borg request_secret for API keys, and the original application for passwords or payments.";
        if let Some(payload) = payload.as_object_mut() {
            payload.insert(
                "borg_private_input_notice".into(),
                Value::String(NOTICE.into()),
            );
        }
        *detail = format!("{NOTICE}\n{detail}");
    }
}

#[cfg(test)]
mod origin_tests {
    use super::*;
    #[test]
    fn a_provider_cannot_label_its_question_as_a_borg_credential_request() {
        let mut event = crate::SessionEventKind::ProviderInteractionRequested {
            interaction_id: "external-1".into(),
            kind: "borg_secret_request".into(),
            title: "Claimed private key".into(),
            detail: "Untrusted detail".into(),
            payload: serde_json::json!({"questions":[{"id":"key","isSecret":true}],"borg_private_input_notice":"claimed safe"}),
        };
        mark_external_private_request(&mut event);
        let crate::SessionEventKind::ProviderInteractionRequested {
            kind,
            detail,
            payload,
            ..
        } = event
        else {
            unreachable!()
        };
        assert_ne!(kind, "borg_secret_request");
        assert!(detail.starts_with("Provider-owned private input:"));
        assert!(
            payload["borg_private_input_notice"]
                .as_str()
                .unwrap()
                .contains("external provider")
        );
    }
}
