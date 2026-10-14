use super::*;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

// The transport hook is PRIVATE and compiled only in tests. No insecure endpoint
// overrides or auth-fetch injection are part of the production interface.
tokio::task_local! {
    static RESPONSES: Arc<Mutex<VecDeque<Value>>>;
    static PROFILE_WRITES: Arc<Mutex<VecDeque<(AuthAuthority, Value)>>>;
}
pub(super) async fn injected_fetch(
    operation: Operation,
    _: Option<&Value>,
    _: Option<&str>,
) -> Option<Result<Value>> {
    let response = RESPONSES
        .try_with(|queue| {
            queue
                .lock()
                .unwrap()
                .pop_front()
                .context("unexpected offline auth fetch")
        })
        .ok()?;
    if matches!(operation, Operation::Profile) {
        let write = PROFILE_WRITES
            .try_with(|queue| queue.lock().unwrap().pop_front())
            .ok()
            .flatten();
        if let Some((authority, document)) = write
            && let Err(error) = authority.save(&document).await
        {
            return Some(Err(error));
        }
    }
    Some(response)
}

pub(crate) async fn save_fixture(authority: &AuthAuthority, document: &Value) -> Result<()> {
    authority.save(document).await
}

pub(crate) async fn offline<F: std::future::Future>(
    responses: Vec<Value>,
    writes: Vec<(AuthAuthority, Value)>,
    future: F,
) -> F::Output {
    let responses = Arc::new(Mutex::new(VecDeque::from(responses)));
    let writes = Arc::new(Mutex::new(VecDeque::from(writes)));
    let result = RESPONSES
        .scope(
            responses.clone(),
            PROFILE_WRITES.scope(writes.clone(), future),
        )
        .await;
    assert!(
        responses.lock().unwrap().is_empty(),
        "required offline auth fetch was skipped"
    );
    assert!(
        writes.lock().unwrap().is_empty(),
        "profile interleaving did not occur"
    );
    result
}

pub(super) fn inherit_fetch_context<F: std::future::Future>(
    future: F,
) -> impl std::future::Future<Output = F::Output> {
    let context = RESPONSES.try_with(Arc::clone).ok();
    let writes = PROFILE_WRITES.try_with(Arc::clone).ok();
    async move {
        let future = async move {
            match writes {
                Some(writes) => PROFILE_WRITES.scope(writes, future).await,
                None => future.await,
            }
        };
        match context {
            Some(context) => RESPONSES.scope(context, future).await,
            None => future.await,
        }
    }
}

#[tokio::test]
async fn missing_mock_cannot_contact_service() {
    let error = fetch(Operation::Profile, None, Some("owned-fake"))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("offline auth test context missing")
    );
}

pub(crate) fn identity() -> AccountIdentity {
    AccountIdentity {
        account_uuid: "account-a".into(),
        organization_uuid: "org-a".into(),
    }
}
pub(crate) fn document(expiry: i64) -> Value {
    json!({"foreign":{"device":"keep"},"claudeAiOauth":{
        "accessToken":"fixture-access", "refreshToken":"fixture-refresh", "expiresAt":expiry,
        "scopes":["user:inference","user:profile"], "subscriptionType":"max", "rateLimitTier":"tier-20x",
        "foreignTokenMetadata":"keep"
    }})
}
pub(crate) fn profile_fixture() -> Value {
    json!({"account":{"uuid":"account-a","email":"fixture@example.invalid"},"organization":{"uuid":"org-a","organization_type":"claude_max","rate_limit_tier":"tier-20x","has_extra_usage_enabled":true}})
}
fn rotation() -> Value {
    json!({"access_token":"rotated-access","refresh_token":"rotated-refresh","expires_in":3600,"scope":"user:inference user:profile"})
}

// Boundary guard: treating an expiring token as fresh or accepting console-only
// scopes would break subscription recovery / silently admit an API authority.
#[test]
fn expiry_and_inference_scope() {
    let now = 1_000_000;
    assert!(
        OAuthToken::from_document(&document(now + 300_000))
            .unwrap()
            .needs_refresh_at(now)
    );
    assert!(
        !OAuthToken::from_document(&document(now + 300_001))
            .unwrap()
            .needs_refresh_at(now)
    );
    let mut doc = document(now);
    doc["claudeAiOauth"]["scopes"] = json!(["org:create_api_key"]);
    assert!(OAuthToken::from_document(&doc).is_err());
}
#[tokio::test]
async fn missing_profile_scope_errors_do_not_disclose_tokens() {
    let mut token = OAuthToken::from_document(&document(1)).unwrap();
    token.scopes.clear();
    let error = profile(&token, &identity()).await.unwrap_err().to_string();
    assert!(!error.contains("fixture-access"));
}

// Lost-update guard: both unrelated root/token keys and a sibling's rotated
// refresh must survive. Native refresh tokens are single-use credentials.
#[test]
fn cas_preserves_foreign_keys_and_adopts_sibling() {
    let candidate = rotated_document(document(1), &rotation(), 10).unwrap();
    let mut latest = document(1);
    latest["newForeignField"] = json!(7);
    let saved = compare_and_swap(latest.clone(), "fixture-refresh", &candidate).unwrap();
    assert_eq!(saved["foreign"], latest["foreign"]);
    assert_eq!(saved["newForeignField"], 7);
    assert_eq!(saved["claudeAiOauth"]["foreignTokenMetadata"], "keep");
    assert_eq!(saved["claudeAiOauth"]["refreshToken"], "rotated-refresh");
    latest["claudeAiOauth"]["refreshToken"] = json!("sibling-refresh");
    assert_eq!(
        compare_and_swap(latest.clone(), "fixture-refresh", &candidate).unwrap(),
        latest
    );
}

// Identity guard: account + org partition cache/billing, not email or plan.
#[test]
fn account_and_organization_are_guarded() {
    let account = identity();
    let mut other = account.clone();
    other.organization_uuid = "other-org".into();
    assert!(account.check(&other).is_err());
    assert_ne!(account.subscription_id(), other.subscription_id());
    assert_eq!(
        account.subscription_id(),
        hex::encode(Sha256::digest(b"claude-subscription\0account-a\0org-a"))
    );
    assert!(check_token_account(&json!({"account":{"uuid":"other"}}), &account).is_err());
}

// Native /usage percentages are not fractions, and null windows are normal.
#[test]
fn usage_percentage_and_entitlements_survive() {
    let raw = json!({"five_hour":{"utilization":37.5,"resets_at":"2026-10-12T00:00:00Z"},"seven_day":null,"extra_usage":{"is_enabled":true,"monthly_limit":10000,"used_credits":1234,"currency":"USD"}});
    let usage = usage_from(raw.clone(), &identity()).unwrap();
    assert_eq!(usage.windows.len(), 1);
    assert_eq!(usage.windows[0].utilization, Some(0.375));
    assert_eq!(usage.raw["extra_usage"], raw["extra_usage"]);
}

// Distinct selected config directories must never hit the default keychain item.
#[test]
fn keychain_selection_is_path_isolated() {
    let a = KeychainIdentity::for_config_dir(std::path::Path::new("/fixture/a"), "alice").unwrap();
    let b = KeychainIdentity::for_config_dir(std::path::Path::new("/fixture/b"), "alice").unwrap();
    assert_ne!(a.service, b.service);
    assert_ne!(
        a.service,
        KeychainIdentity::default_for_user("alice").service
    );
    assert_eq!(
        KeychainIdentity::default_for_user("unsafe user").account,
        "claude-code-user"
    );
    assert!(KeychainIdentity::for_config_dir(std::path::Path::new("/fixture/é"), "alice").is_err());
}

// Real files ONLY inside a disposable test directory; no HOME/ambient credential reads.
#[tokio::test]
async fn durable_file_write_and_refresh_are_offline() {
    let directory = tempfile::tempdir().unwrap();
    let authority =
        AuthAuthority::selected(directory.path().to_owned(), CredentialStore::File).unwrap();
    authority.save(&document(1)).await.unwrap();
    let queue = Arc::new(Mutex::new(VecDeque::from([rotation(), profile_fixture()])));
    // Run private inner auth, so the task-local test transport follows this future.
    let access = RESPONSES
        .scope(
            queue.clone(),
            access_inner(authority.clone(), Some(identity()), None),
        )
        .await
        .unwrap();
    assert_eq!(access.token.bearer(), "rotated-access");
    assert!(queue.lock().unwrap().is_empty());
    let saved = authority.load().await.unwrap();
    assert_eq!(saved["foreign"]["device"], "keep");
    assert_eq!(saved["claudeAiOauth"]["refreshToken"], "rotated-refresh");
    assert!(!directory.path().join(".oauth_refresh.lock").exists());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(authority.credential_file())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}

// Native-compatible refresh lock: a live holder is never stolen, while a holder
// that stopped heartbeating (crashed process) must not wedge auth forever.
#[tokio::test]
async fn refresh_lock_waits_for_live_holder_and_reclaims_dead_one() {
    let directory = tempfile::tempdir().unwrap();
    let authority =
        AuthAuthority::selected(directory.path().to_owned(), CredentialStore::File).unwrap();
    let held = authority.try_lock().unwrap().unwrap();
    assert!(authority.try_lock().unwrap().is_none());
    drop(held);
    let dead = directory.path().join(".oauth_refresh.lock");
    std::fs::create_dir(&dead).unwrap();
    std::fs::File::open(&dead)
        .unwrap()
        .set_modified(std::time::SystemTime::now() - Duration::from_secs(120))
        .unwrap();
    assert!(authority.try_lock().unwrap().is_some());
}

#[tokio::test]
async fn fresh_auth_fetches_profile_without_rotation() {
    let directory = tempfile::tempdir().unwrap();
    let authority =
        AuthAuthority::selected(directory.path().to_owned(), CredentialStore::File).unwrap();
    authority
        .save(&document(now_ms() + 3_600_000))
        .await
        .unwrap();
    let queue = Arc::new(Mutex::new(VecDeque::from([profile_fixture()])));
    let access = RESPONSES
        .scope(
            queue.clone(),
            access_inner(authority, Some(identity()), None),
        )
        .await
        .unwrap();
    assert_eq!(access.token.bearer(), "fixture-access");
    assert!(queue.lock().unwrap().is_empty());
}

// Profile outages AFTER server rotation must not strand a single-use refresh token.
#[tokio::test]
async fn rotation_is_durable_even_when_profile_fails() {
    let directory = tempfile::tempdir().unwrap();
    let authority =
        AuthAuthority::selected(directory.path().to_owned(), CredentialStore::File).unwrap();
    authority.save(&document(1)).await.unwrap();
    let queue = Arc::new(Mutex::new(VecDeque::from([rotation(), json!({})])));
    assert!(
        RESPONSES
            .scope(queue, access_inner(authority.clone(), None, None))
            .await
            .is_err()
    );
    let saved = authority.load().await.unwrap();
    assert_eq!(saved["claudeAiOauth"]["refreshToken"], "rotated-refresh");
    assert_eq!(saved["claudeAiOauth"]["accessToken"], "rotated-access");
    let queue = Arc::new(Mutex::new(VecDeque::from([profile_fixture()])));
    let result = RESPONSES
        .scope(queue.clone(), access_inner(authority.clone(), None, None))
        .await
        .unwrap();
    assert_eq!(result.profile.identity, identity());
    assert!(queue.lock().unwrap().is_empty());
    assert_eq!(
        authority.load().await.unwrap()["borgClaudeSubscription"]["accountUuid"],
        "account-a"
    );
}

#[cfg(target_os = "macos")]
#[test]
fn keychain_directory_hash_is_nfc() {
    let composed =
        KeychainIdentity::for_config_dir(std::path::Path::new("/fixture/é"), "alice").unwrap();
    let decomposed =
        KeychainIdentity::for_config_dir(std::path::Path::new("/fixture/e\u{301}"), "alice")
            .unwrap();
    assert_eq!(composed, decomposed);
}

// Login state rejection must happen before network exchange or persistence.
#[tokio::test]
async fn pkce_login_rejects_wrong_state_offline() {
    use base64::Engine;
    let pending = PendingLogin::new().unwrap();
    let url = reqwest::Url::parse(&pending.authorize_url().unwrap()).unwrap();
    let pairs: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
    assert_eq!(pairs["client_id"], CLIENT_ID);
    assert_eq!(pairs["code_challenge_method"], "S256");
    assert_eq!(
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(&pairs["state"])
            .unwrap()
            .len(),
        32
    );
    let directory = tempfile::tempdir().unwrap();
    let authority =
        AuthAuthority::selected(directory.path().to_owned(), CredentialStore::File).unwrap();
    assert!(
        pending
            .complete(
                "fixture-code".into(),
                "wrong-state".into(),
                authority.clone(),
                None
            )
            .await
            .is_err()
    );
    assert!(!authority.credential_file().exists());
}

// Concurrent durable refresh during the profile await must adopt the new bearer,
// but never a new account/org. Ordinary CAS tests and compilation cannot hit this race.
#[tokio::test]
async fn profile_await_rotation_is_reverified_and_identity_pinned() {
    for changed in [None, Some("account"), Some("organization")] {
        let directory = tempfile::tempdir().unwrap();
        let authority =
            AuthAuthority::selected(directory.path().to_owned(), CredentialStore::File).unwrap();
        authority
            .save(&document(now_ms() + 3_600_000))
            .await
            .unwrap();
        let mut sibling = document(now_ms() + 3_600_000);
        sibling["claudeAiOauth"]["accessToken"] = json!("sibling-access");
        sibling["claudeAiOauth"]["refreshToken"] = json!("sibling-refresh");
        let mut next_profile = profile_fixture();
        if let Some(field) = changed {
            next_profile[field]["uuid"] = json!("other");
        }
        // No prior binding: discovery must pin even its first profile's identity.
        let result = offline(
            vec![profile_fixture(), next_profile],
            vec![(authority.clone(), sibling)],
            discover(authority.clone()),
        )
        .await;
        if changed.is_some() {
            let error = result.err().unwrap().to_string();
            assert!(error.contains("account/organization changed"));
            assert!(!error.contains("sibling-access"));
            assert!(authority.load().await.unwrap()["borgClaudeSubscription"].is_null());
        } else {
            let access = result.unwrap();
            assert_eq!(access.token.bearer(), "sibling-access");
            assert_eq!(access.profile.identity, identity());
            let saved = authority.load().await.unwrap();
            assert_eq!(saved["claudeAiOauth"]["refreshToken"], "sibling-refresh");
            assert_eq!(saved["foreign"]["device"], "keep");
        }
    }
}

#[tokio::test]
async fn profile_generation_churn_is_bounded() {
    let directory = tempfile::tempdir().unwrap();
    let authority =
        AuthAuthority::selected(directory.path().to_owned(), CredentialStore::File).unwrap();
    authority
        .save(&document(now_ms() + 3_600_000))
        .await
        .unwrap();
    let writes = (0..3)
        .map(|n| {
            let mut doc = document(now_ms() + 3_600_000);
            doc["claudeAiOauth"]["accessToken"] = json!(format!("sibling-{n}"));
            (authority.clone(), doc)
        })
        .collect();
    let error = offline(
        vec![profile_fixture(); 3],
        writes,
        access(authority, identity(), None),
    )
    .await
    .err()
    .unwrap()
    .to_string();
    assert!(error.contains("kept changing"));
}
