//! Pure subscription protocol helpers. No login, transport, environment reads, or agent runtime.
//!
//! Evidence: `/home/shulgin/borg-delivery/claude-native-protocol-be7e8a1e/latest-selected/`:
//! 0032.js (`pp` OAuth beta); 0139.js (`r0`, `nC`, `oz`, `Noe`, `vRe`, `Kho`, `w7`);
//! 0176.js (`jfo` attribution); 0195.js (`Ml`, `jNe`, SDK buildHeaders);
//! 0290.js (`ug` fingerprint); 0404.js (`Osn`, `ebe`, `SQo` narration).
//! Bootstrap endpoint/schema also verified in `~/claude-extract/readable/0496.js` (`at`, `Q`).
//!
//! Additional missing chunks were read as DATA ONLY from
//! `/home/shulgin/.local/share/claude/versions/2.1.296`: model table at 207705000..207724000
//! (chunk-nj0630nv.js), effort GS/Gre/Lit at 211166000..211169000,
//! bootstrap Q/rt/CDs at 224309000..224318000, codec ie/GKe/mgr/fps at 211084300..211085050.
//! Source table facts below are independent tuples, not copied implementation code.
//!
//! Integration boundary: parent supplies account-bound bootstrap, exact resolved aliases,
//! policy-approved model/SDK betas, and served-model/managed-policy overrides. Source facts
//! are ONLY baseline known models, never proof of account availability. Unknown models,
//! optional entitlement omissions retain known facts; unknown capabilities fail closed. Account authority must
//! merge narrowed responses before construction and invalidate rows on account/org changes.
//! No model fallback, alias default, effort clamp, or speed downgrade occurs here.
//! Remaining gaps: served-catalog activation/refusal and policy overrides (0139.js nC/ii,
//! ri/Gwr/mb/lc, GS te, Qo/d7), dynamic beta predicates (0139.js YF/XF/w7), and 1m opt-in
//! entitlement (0139.js Ra/Jv/jr/cBn). Apply them before using baseline ModelFacts; neither
//! stale source overrides nor a successful login replace those decisions.
//!
//! Attribution is the FIRST system text block, not an HTTP header. `cch=00000` is a
//! placeholder, NOT send-ready: parent stamps the final normalized raw body using seed
//! 0x4d659218e32a3268 and low 20 bits. Cache, signed/redacted blocks and usage stay in the
//! existing AnthropicStreamState; classification below only returns display indexes.
//! Signature classification: HTML atob -> complete protobuf scan, LAST bytes field 2 ->
//! complete scan, LAST bytes field 1 -> complete scan, LAST UTF-8 field 8 == "narration".
//! ie uses JS Number arithmetic (10-byte varints); GKe allows only wire types 0/1/2/5.
//! Replay always retains the original encoded signature, including malformed signatures.

use std::collections::BTreeMap;

use anyhow::{Result, ensure};
use base64::Engine;
use reqwest::header::{HeaderMap, HeaderValue};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::claude_connector::Capabilities;

pub(super) const VERSION: &str = "2.1.296";
pub(super) const OAUTH_BETA: &str = "oauth-2025-04-20";
pub(super) const CLAUDE_CODE_BETA: &str = "claude-code-20250219";
const EFFORTS: &[&str] = &["low", "medium", "high", "xhigh", "max"];

#[derive(Debug, Deserialize)]
pub(super) struct ModelAccess {
    pub api_name: String,
    pub entitled: bool,
    pub max_effort_level: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct ModelOption {
    pub model: String,
    pub disabled_reason: Option<String>,
}

/// Typed capability/denial fields. NativeConnector separately checks identity and
/// merges the raw account-bound defaults and per-model client_data.
#[derive(Debug, Deserialize)]
pub(super) struct Bootstrap {
    pub model_access: Option<Vec<ModelAccess>>,
    pub additional_model_options: Option<Vec<ModelOption>>,
    pub narrowed: Option<bool>,
}

/// Fully verified, normalized facts, not an assumed raw bootstrap wire schema.
/// A missing entry cannot be manufactured from a model-name substring.
pub(super) struct ModelFacts {
    /// Stable catalog name, e.g. claude-haiku-4-5.
    pub id: String,
    pub capabilities: Capabilities,
}

/// Verified first-party baseline, not an account catalog or an override resolver.
/// Betas MUST come from the account/policy resolver. Unknown ids have no static fallback.
pub(super) fn source_model_facts(model: &str, approved_betas: &[String]) -> Option<ModelFacts> {
    // id, first-party wire id, context, default output, upper output, effort count,
    // adaptive thinking, thinking required, model-level fast support.
    #[rustfmt::skip]
    let rows = [
        ("claude-3-5-haiku", "claude-3-5-haiku-20241022", 200000, 8192, 8192, 0, false, false, false),
        ("claude-haiku-4-5", "claude-haiku-4-5-20251001", 200000, 32000, 64000, 0, false, false, false),
        ("claude-haiku-5-5", "claude-haiku-5-5", 1000000, 128000, 128000, 5, true, true, false),
        ("claude-3-5-sonnet", "claude-3-5-sonnet-20241022", 200000, 8192, 8192, 0, false, false, false),
        ("claude-3-7-sonnet", "claude-3-7-sonnet-20250219", 200000, 32000, 64000, 0, false, false, false),
        ("claude-sonnet-4-0", "claude-sonnet-4-20250514", 200000, 32000, 64000, 0, false, false, false),
        ("claude-sonnet-4-5", "claude-sonnet-4-5-20250929", 200000, 32000, 64000, 0, false, false, false),
        ("claude-sonnet-4-6", "claude-sonnet-4-6", 200000, 32000, 128000, 4, true, false, false),
        ("claude-sonnet-5", "claude-sonnet-5", 1000000, 64000, 128000, 5, true, false, false),
        ("claude-sonnet-5-5", "claude-sonnet-5-5", 1000000, 128000, 128000, 5, true, true, false),
        ("claude-opus-4-0", "claude-opus-4-20250514", 200000, 32000, 32000, 0, false, false, false),
        ("claude-opus-4-1", "claude-opus-4-1-20250805", 200000, 32000, 32000, 0, false, false, false),
        ("claude-opus-4-5", "claude-opus-4-5-20251101", 200000, 32000, 64000, 3, false, false, false),
        ("claude-opus-4-6", "claude-opus-4-6", 200000, 64000, 128000, 4, true, false, false),
        ("claude-opus-4-7", "claude-opus-4-7", 1000000, 64000, 128000, 5, true, false, false),
        ("claude-opus-4-8", "claude-opus-4-8", 1000000, 64000, 128000, 5, true, false, true),
        ("claude-opus-5", "claude-opus-5", 1000000, 64000, 128000, 5, true, false, true),
        ("claude-opus-5-5", "claude-opus-5-5", 1000000, 128000, 128000, 5, true, true, true),
        ("claude-fable-5", "claude-fable-5", 1000000, 64000, 128000, 5, true, true, false),
        ("claude-fable-5-1", "claude-fable-5-1", 1000000, 64000, 128000, 5, true, true, false),
        ("claude-mythos-5", "claude-mythos-5", 1000000, 64000, 128000, 5, true, false, false),
        ("claude-mythos-5-1", "claude-mythos-5-1", 1000000, 64000, 128000, 5, true, true, false),
    ];
    let model = key(model);
    let (id, wire, context, default, upper, effort_count, adaptive, required, fast) = rows
        .into_iter()
        .find(|row| row.0 == model || row.1 == model)?;
    let efforts = match effort_count {
        0 => vec![],
        3 => vec!["low", "medium", "high"],
        4 => vec!["low", "medium", "high", "max"],
        _ => EFFORTS.to_vec(),
    };
    Some(ModelFacts {
        id: id.into(),
        capabilities: Capabilities {
            model: wire.into(),
            context_window: context,
            default_output_tokens: default,
            max_output_tokens: upper,
            thinking: !id.starts_with("claude-3-"),
            adaptive_thinking: adaptive,
            thinking_required: required,
            fast,
            efforts: efforts.into_iter().map(str::to_owned).collect(),
            betas: approved_betas.to_vec(),
        },
    })
}

fn key(id: &str) -> String {
    id.trim().to_ascii_lowercase()
}

impl Bootstrap {
    /// Alias map comes from the verified account catalog, never latest-version defaults.
    pub fn capabilities(
        &self,
        requested: &str,
        aliases: &BTreeMap<String, String>,
        facts: &[ModelFacts],
        account_fast_enabled: bool,
    ) -> Result<Capabilities> {
        ensure!(
            !requested.is_empty() && requested.len() <= 200,
            "invalid Claude model"
        );
        let requested = key(requested);
        let resolved = aliases
            .get(&requested)
            .map(String::as_str)
            .unwrap_or(&requested);
        let facts = facts
            .iter()
            .find(|f| key(&f.id) == key(resolved) || key(&f.capabilities.model) == key(resolved))
            .ok_or_else(|| anyhow::anyhow!("Claude model capability facts are unavailable"))?;
        let matches =
            |id: &str| key(id) == key(&facts.id) || key(id) == key(&facts.capabilities.model);
        // Optional catalog rows are a deny overlay, not a positive allowlist.
        // The authenticated inference service remains the entitlement authority.
        let rows: Vec<_> = self
            .model_access
            .as_deref()
            .unwrap_or_default()
            .iter()
            .filter(|r| matches(&r.api_name))
            .collect();
        ensure!(
            rows.iter().all(|r| r.entitled),
            "Claude model is not entitled for this account"
        );
        ensure!(
            !self
                .additional_model_options
                .as_deref()
                .unwrap_or_default()
                .iter()
                .any(|r| matches(&r.model) && r.disabled_reason.is_some()),
            "Claude model is disabled for this account"
        );
        let c = &facts.capabilities;
        ensure!(
            c.context_window > 0
                && c.default_output_tokens > 0
                && c.default_output_tokens <= c.max_output_tokens,
            "invalid Claude model limits"
        );
        ensure!(
            !c.adaptive_thinking || c.thinking,
            "invalid Claude thinking capabilities"
        );
        ensure!(
            !c.thinking_required || c.thinking,
            "invalid Claude required thinking capability"
        );
        ensure!(
            c.efforts.iter().all(|e| EFFORTS.contains(&e.as_str())),
            "unknown Claude effort capability"
        );
        let mut efforts = c.efforts.clone();
        for row in rows {
            if let Some(cap) = &row.max_effort_level {
                let cap = EFFORTS
                    .iter()
                    .position(|e| *e == cap)
                    .ok_or_else(|| anyhow::anyhow!("unknown Claude account effort cap"))?;
                efforts.retain(|e| {
                    EFFORTS
                        .iter()
                        .position(|v| v == &e.as_str())
                        .is_some_and(|rank| rank <= cap)
                });
            }
        }
        Ok(Capabilities {
            model: c.model.clone(),
            context_window: c.context_window,
            default_output_tokens: c.default_output_tokens,
            max_output_tokens: c.max_output_tokens,
            thinking: c.thinking,
            adaptive_thinking: c.adaptive_thinking,
            thinking_required: c.thinking_required,
            fast: c.fast && account_fast_enabled,
            efforts,
            betas: c.betas.clone(),
        })
    }
}

/// Reject an unavailable explicit setting instead of changing model, effort or speed.
pub(super) fn validate_selection(c: &Capabilities, effort: Option<&str>, fast: bool) -> Result<()> {
    ensure!(
        !fast || c.fast,
        "fast mode is unavailable for the selected Claude model/account"
    );
    if let Some(effort) = effort {
        if matches!(effort, "none" | "off" | "minimal") {
            ensure!(
                !c.thinking_required,
                "the selected Claude model requires thinking"
            );
        } else {
            ensure!(
                c.thinking && c.efforts.iter().any(|e| e == effort),
                "requested Claude effort is unavailable; refusing to downgrade or ignore it"
            );
        }
    }
    Ok(())
}

pub(super) struct RequestIdentity<'a> {
    pub device_id: &'a str,
    pub account_uuid: &'a str,
    pub session_id: &'a str,
    pub parent_agent_id: Option<&'a str>,
}

fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_:.-".contains(&b))
}

impl RequestIdentity<'_> {
    pub fn metadata(&self) -> Result<Value> {
        ensure!(
            !self.device_id.is_empty() && !self.account_uuid.is_empty(),
            "Claude identity is incomplete"
        );
        ensure!(
            valid_id(self.session_id) && self.parent_agent_id.is_none_or(valid_id),
            "invalid Claude session identity"
        );
        Ok(json!({"user_id": serde_json::to_string(&json!({
            "device_id": self.device_id, "account_uuid": self.account_uuid, "session_id": self.session_id
        }))?}))
    }
}

/// Caller owns persistent device_id. No creation or access to credential/config files here.
/// SDK/model betas are policy-approved inputs: the OAuth beta cannot be omitted.
pub(super) fn inference_headers(
    bearer: &str,
    identity: &RequestIdentity<'_>,
    request_id: &str,
    entrypoint: &str,
    approved_betas: &[String],
) -> Result<HeaderMap> {
    ensure!(
        !bearer.is_empty(),
        "Claude subscription Bearer token required"
    );
    ensure!(valid_id(request_id), "invalid Claude request id");
    identity.metadata()?;
    let mut headers = HeaderMap::new();
    let mut auth = HeaderValue::from_str(&format!("Bearer {bearer}"))?;
    auth.set_sensitive(true);
    headers.insert("authorization", auth);
    headers.insert("content-type", HeaderValue::from_static("application/json"));
    headers.insert("accept", HeaderValue::from_static("application/json"));
    headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
    headers.insert("x-app", HeaderValue::from_static("cli"));
    headers.insert(
        "user-agent",
        HeaderValue::from_str(&format!("claude-cli/{VERSION} (external, {entrypoint})"))?,
    );
    headers.insert(
        "x-claude-code-session-id",
        HeaderValue::from_str(identity.session_id)?,
    );
    headers.insert(
        "x-claude-code-agent-id",
        HeaderValue::from_str(identity.session_id)?,
    );
    if let Some(parent) = identity.parent_agent_id {
        headers.insert(
            "x-claude-code-parent-agent-id",
            HeaderValue::from_str(parent)?,
        );
    }
    headers.insert("x-client-request-id", HeaderValue::from_str(request_id)?);
    let mut betas = vec![CLAUDE_CODE_BETA, OAUTH_BETA];
    for beta in approved_betas {
        ensure!(
            !beta.is_empty() && beta.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-'),
            "invalid Claude beta"
        );
        if !betas.contains(&beta.as_str()) {
            betas.push(beta);
        }
    }
    headers.insert("anthropic-beta", HeaderValue::from_str(&betas.join(","))?);
    Ok(headers)
}

/// GET query only; no network calls. Bootstrap uses a different UA from inference.
#[cfg_attr(test, expect(dead_code))]
pub(super) fn bootstrap_query(model: &str, entrypoint: &str) -> [(String, String); 2] {
    [
        ("entrypoint".into(), entrypoint.into()),
        ("model".into(), model.into()),
    ]
}

pub(super) fn bootstrap_headers(bearer: &str) -> Result<HeaderMap> {
    ensure!(
        !bearer.is_empty(),
        "Claude subscription Bearer token required"
    );
    let mut headers = HeaderMap::new();
    let mut auth = HeaderValue::from_str(&format!("Bearer {bearer}"))?;
    auth.set_sensitive(true);
    headers.insert("authorization", auth);
    headers.insert("anthropic-beta", HeaderValue::from_static(OAUTH_BETA));
    headers.insert("content-type", HeaderValue::from_static("application/json"));
    headers.insert(
        "user-agent",
        HeaderValue::from_str(&format!("claude-code/{VERSION}"))?,
    );
    Ok(headers)
}

/// JS indexes UTF-16 code units, then Node encodes the sampled string to UTF-8.
/// Unpaired selected surrogates become U+FFFD, not UTF-8 bytes or scalar indexes.
pub(super) fn fingerprint_utf16(first_user_text: &[u16]) -> String {
    let sample: Vec<_> = [4, 7, 20]
        .into_iter()
        .map(|i| first_user_text.get(i).copied().unwrap_or(b'0' as u16))
        .collect();
    let text = String::from_utf16_lossy(&sample);
    hex::encode(Sha256::digest(format!("59cf53e54c78{text}{VERSION}")))[..3].into()
}

pub(super) fn fingerprint(messages: &[Value]) -> String {
    let content = messages
        .iter()
        .find(|m| m["role"] == "user")
        .and_then(|m| m.get("content"));
    let text = content
        .and_then(|c| {
            c.as_str().or_else(|| {
                c.as_array()?
                    .iter()
                    .find(|b| b["type"] == "text")?
                    .get("text")?
                    .as_str()
            })
        })
        .unwrap_or("");
    fingerprint_utf16(&text.encode_utf16().collect::<Vec<_>>())
}

/// Placeholder text only. Caller must prepend then stamp the complete raw request body.
pub(super) fn billing_system_text(
    messages: &[Value],
    entrypoint: &str,
    subagent: bool,
) -> Result<String> {
    ensure!(
        !entrypoint.is_empty()
            && entrypoint
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b)),
        "invalid Claude entrypoint"
    );
    let subagent = if subagent {
        " cc_is_subagent=true;"
    } else {
        ""
    };
    Ok(format!(
        "x-anthropic-billing-header: cc_version={VERSION}.{}; cc_entrypoint={entrypoint}; cch=00000;{subagent}",
        fingerprint(messages)
    ))
}

/// Display-only; input and every signature/redacted/cache field stay untouched.
pub(super) fn narration_block_indexes(blocks: &[Value]) -> Vec<usize> {
    blocks
        .iter()
        .enumerate()
        .filter_map(|(i, b)| {
            (b["type"] == "thinking" && b["signature"].as_str().is_some_and(is_narration_signature))
                .then_some(i)
        })
        .collect()
}

fn is_narration_signature(signature: &str) -> bool {
    // HTML atob allows ASCII whitespace and unpadded standard base64, not URL-safe alphabet.
    let compact: String = signature
        .chars()
        .filter(|c| !matches!(c, '\t' | '\n' | '\u{000c}' | '\r' | ' '))
        .collect();
    let mut padded = compact;
    if padded.len() % 4 == 1 {
        return false;
    }
    while !padded.len().is_multiple_of(4) {
        padded.push('=');
    }
    let decoder = base64::engine::GeneralPurpose::new(
        &base64::alphabet::STANDARD,
        base64::engine::GeneralPurposeConfig::new().with_decode_allow_trailing_bits(true),
    );
    let Ok(bytes) = decoder.decode(padded) else {
        return false;
    };
    let Some(outer) = protobuf_bytes(&bytes, 2) else {
        return false;
    };
    let Some(inner) = protobuf_bytes(outer, 1) else {
        return false;
    };
    protobuf_bytes(inner, 8) == Some(b"narration".as_slice())
}

fn varint(bytes: &[u8], position: &mut usize) -> Option<f64> {
    let mut value = 0.0;
    let mut scale = 1.0;
    for _ in 0..10 {
        let byte = *bytes.get(*position)?;
        *position += 1;
        value += f64::from(byte & 127) * scale;
        if byte < 128 {
            return Some(value);
        }
        scale *= 128.0;
    }
    None
}

fn protobuf_bytes(bytes: &[u8], wanted: u64) -> Option<&[u8]> {
    let mut position = 0;
    let mut found = None;
    while position < bytes.len() {
        let tag = varint(bytes, &mut position)?;
        let wire_type = (tag.rem_euclid(4294967296.0) as u32) & 7;
        let field_number = (tag / 8.0).floor();
        match wire_type {
            0 => {
                varint(bytes, &mut position)?;
            }
            1 => {
                position = position.checked_add(8)?;
            }
            2 => {
                let len = varint(bytes, &mut position)?;
                if len > (bytes.len() - position) as f64 {
                    return None;
                }
                let end = position.checked_add(len as usize)?;
                let field = bytes.get(position..end)?;
                if field_number == wanted as f64 {
                    found = Some(field);
                }
                position = end;
            }
            5 => {
                position = position.checked_add(4)?;
            }
            _ => return None,
        }
        if position > bytes.len() {
            return None;
        }
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::STANDARD;

    // Protect account gates/settings and exact identity/wire contracts, which compilation
    // cannot check. Synthetic signatures exercise display routing without private fixtures.
    fn facts(id: &str, wire: &str) -> ModelFacts {
        ModelFacts {
            id: id.into(),
            capabilities: Capabilities {
                model: wire.into(),
                context_window: 200000,
                default_output_tokens: 32000,
                max_output_tokens: 64000,
                thinking: true,
                adaptive_thinking: false,
                thinking_required: false,
                fast: true,
                efforts: EFFORTS.iter().map(|s| s.to_string()).collect(),
                betas: vec![],
            },
        }
    }

    #[test]
    fn bootstrap_gates_exact_wire_ids_without_a_fallback() {
        let f = facts("claude-haiku-4-5", "claude-haiku-4-5-20251001");
        let aliases = BTreeMap::from([("haiku".into(), f.id.clone())]);
        let mut b: Bootstrap = serde_json::from_value(json!({"model_access":[{
            "api_name":f.id,"entitled":true,"max_effort_level":"high"
        }]}))
        .unwrap();
        let c = b.capabilities("haiku", &aliases, &[f], false).unwrap();
        assert_eq!(c.model, "claude-haiku-4-5-20251001");
        assert_eq!(c.efforts, ["low", "medium", "high"]);
        assert!(validate_selection(&c, Some("max"), false).is_err());
        assert!(validate_selection(&c, Some("high"), true).is_err());
        b.model_access.as_mut().unwrap()[0].entitled = false;
        assert!(
            b.capabilities(
                "haiku",
                &aliases,
                &[facts("claude-haiku-4-5", &c.model)],
                true
            )
            .is_err()
        );
        b.model_access = None;
        assert!(b.capabilities("unknown", &aliases, &[], true).is_err());
    }

    #[test]
    fn known_models_honor_explicit_entitlement_denials() {
        // This is a catalog pass-through invariant, NOT a static latest model list or limits.
        let ids = [
            "claude-3-5-haiku",
            "claude-haiku-4-5",
            "claude-haiku-5-5",
            "claude-3-5-sonnet",
            "claude-3-7-sonnet",
            "claude-sonnet-4-0",
            "claude-sonnet-4-5",
            "claude-sonnet-4-6",
            "claude-sonnet-5",
            "claude-sonnet-5-5",
            "claude-opus-4-0",
            "claude-opus-4-1",
            "claude-opus-4-5",
            "claude-opus-4-6",
            "claude-opus-4-7",
            "claude-opus-4-8",
            "claude-opus-5",
            "claude-opus-5-5",
            "claude-fable-5",
            "claude-fable-5-1",
            "claude-mythos-5",
            "claude-mythos-5-1",
        ];
        for id in ids {
            let b: Bootstrap =
                serde_json::from_value(json!({"model_access":[{"api_name":id,"entitled":true}]}))
                    .unwrap();
            let c = b
                .capabilities(
                    id,
                    &BTreeMap::new(),
                    &[source_model_facts(id, &[]).unwrap()],
                    true,
                )
                .unwrap();
            assert_eq!(
                c.model,
                source_model_facts(id, &[]).unwrap().capabilities.model
            );
            assert!(
                b.capabilities(
                    "unknown",
                    &BTreeMap::new(),
                    &[source_model_facts(id, &[]).unwrap()],
                    true
                )
                .is_err()
            );
        }
    }

    #[test]
    fn latest_model_limits_and_required_thinking_are_concrete() {
        for id in ["claude-opus-5-5", "claude-sonnet-5-5", "claude-haiku-5-5"] {
            let c = source_model_facts(id, &[]).unwrap().capabilities;
            assert_eq!(c.context_window, 1000000);
            assert_eq!(c.default_output_tokens, 128000);
            assert_eq!(c.max_output_tokens, 128000);
            assert!(c.thinking_required && c.adaptive_thinking);
            assert!(validate_selection(&c, Some("none"), false).is_err());
        }
        let old = source_model_facts("claude-opus-4-6", &[])
            .unwrap()
            .capabilities;
        assert_eq!(old.efforts, ["low", "medium", "high", "max"]);
        assert!(!old.fast);
        assert!(source_model_facts("unknown", &[]).is_none());
    }

    #[test]
    fn disabled_unknown_effort_and_required_thinking_fail_closed() {
        let mut b: Bootstrap = serde_json::from_value(json!({"model_access":[{"api_name":"m","entitled":true}],
            "additional_model_options":[{"model":"m","name":"M","description":"", "disabled_reason":"ZDR"}]})).unwrap();
        assert!(
            b.capabilities("m", &BTreeMap::new(), &[facts("m", "m")], true)
                .is_err()
        );
        b.additional_model_options = None;
        b.model_access.as_mut().unwrap()[0].max_effort_level = Some("future".into());
        assert!(
            b.capabilities("m", &BTreeMap::new(), &[facts("m", "m")], true)
                .is_err()
        );
        let mut f = facts("m", "m");
        f.capabilities.thinking_required = true;
        assert!(validate_selection(&f.capabilities, Some("off"), false).is_err());
        f.capabilities.efforts.clear();
        assert!(validate_selection(&f.capabilities, Some("high"), false).is_err());
    }

    #[test]
    fn headers_and_metadata_bind_subscription_account_and_session() {
        let identity = RequestIdentity {
            device_id: "persisted-device",
            account_uuid: "account",
            session_id: "session",
            parent_agent_id: Some("parent"),
        };
        let h = inference_headers(
            "offline-token",
            &identity,
            "request",
            "cli",
            &[OAUTH_BETA.into(), "effort-2025-11-24".into()],
        )
        .unwrap();
        assert_eq!(h["authorization"], "Bearer offline-token");
        assert!(!h.contains_key("x-api-key"));
        assert_eq!(
            h["anthropic-beta"],
            "claude-code-20250219,oauth-2025-04-20,effort-2025-11-24"
        );
        assert_eq!(h["user-agent"], "claude-cli/2.1.296 (external, cli)");
        assert_eq!(h["x-claude-code-parent-agent-id"], "parent");
        let m: Value =
            serde_json::from_str(identity.metadata().unwrap()["user_id"].as_str().unwrap())
                .unwrap();
        assert_eq!(
            m,
            json!({"device_id":"persisted-device","account_uuid":"account","session_id":"session"})
        );
        assert_eq!(
            bootstrap_headers("offline-token").unwrap()["user-agent"],
            "claude-code/2.1.296"
        );
        assert!(inference_headers("bad\r\nheader", &identity, "request", "cli", &[]).is_err());
    }

    #[test]
    fn unicode_fingerprint_uses_selected_utf16_units() {
        assert_eq!(fingerprint(&[]), "406");
        let messages = vec![
            json!({"role":"user", "content":[{"type":"image"},{"type":"text","text":"abcd😀xy😀abcdefghijklmnop"}]}),
            json!({"role":"user","content":"ignored"}),
        ];
        assert_eq!(fingerprint(&messages), "23e");
        assert_eq!(
            fingerprint_utf16(&[0, 0, 0, 0, 0xd83d, 0, 0, 0xde00]),
            "4b9"
        );
        let header = billing_system_text(&messages, "cli", true).unwrap();
        assert_eq!(
            header,
            "x-anthropic-billing-header: cc_version=2.1.296.23e; cc_entrypoint=cli; cch=00000; cc_is_subagent=true;"
        );
    }

    #[test]
    fn narration_is_display_only_and_never_guessed_from_prose() {
        let signature = STANDARD.encode(b"\x12\x0d\x0a\x0b\x42\x09narration");
        let blocks = vec![
            json!({"type":"thinking","thinking":"private-looking text","signature":signature}),
            json!({"type":"thinking","thinking":"I will run tests now","signature":"opaque-unknown"}),
            json!({"type":"redacted_thinking","data":"opaque-redacted"}),
            json!({"type":"text","text":"visible","cache_control":{"type":"ephemeral","ttl":"1h"}}),
        ];
        let before = serde_json::to_vec(&blocks).unwrap();
        assert_eq!(narration_block_indexes(&blocks), [0]);
        assert_eq!(before, serde_json::to_vec(&blocks).unwrap());
        for signature in ["", "%%%", "AAAA", "Eg==", "Eg0KC0IJaW52YWxpZA=="] {
            assert!(!is_narration_signature(signature));
        }
    }
    #[test]
    fn narration_scans_complete_messages_and_uses_last_field() {
        let n = b"\x12\x0d\x0a\x0b\x42\x09narration";
        let mut duplicate = n.to_vec();
        duplicate.extend_from_slice(b"\x12\x0b\x0a\x09\x42\x07summary");
        assert!(!is_narration_signature(&STANDARD.encode(&duplicate)));
        duplicate.extend_from_slice(n);
        assert!(is_narration_signature(&STANDARD.encode(&duplicate)));
        duplicate.push(0xff); // matching field does not excuse invalid trailing protobuf
        assert!(!is_narration_signature(&STANDARD.encode(&duplicate)));
    }
    #[test]
    fn optional_catalog_omission_is_not_deny_all() {
        let facts = source_model_facts("claude-opus-5-5", &[]).unwrap();
        let mut bootstrap: Bootstrap = serde_json::from_value(json!({})).unwrap();
        assert!(
            bootstrap
                .capabilities("claude-opus-5-5", &BTreeMap::new(), &[facts], false)
                .is_ok()
        );
        bootstrap.model_access = Some(vec![ModelAccess {
            api_name: "claude-haiku-5-5".into(),
            entitled: false,
            max_effort_level: None,
        }]);
        assert!(
            bootstrap
                .capabilities(
                    "claude-opus-5-5",
                    &BTreeMap::new(),
                    &[source_model_facts("claude-opus-5-5", &[]).unwrap()],
                    false
                )
                .is_ok()
        );
        assert!(
            bootstrap
                .capabilities(
                    "claude-haiku-5-5",
                    &BTreeMap::new(),
                    &[source_model_facts("claude-haiku-5-5", &[]).unwrap()],
                    false
                )
                .is_err()
        );
    }
}
