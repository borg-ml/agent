//! Provider-route and model-identity contracts.
//!
//! WHAT THESE PROTECT: a session is pinned to exactly one harness route, and
//! that pin is part of its execution contract rather than a detail of the row
//! it happens to live in. A conversation that has been answered through a
//! provider CLI keeps a transcript only that CLI can replay; one Borg's own
//! harness owns keeps a transcript only Borg can replay. Moving a live session
//! between the two silently breaks resume, so the route has to survive a
//! restart, a context clear, a fork, and child registration -- and changing
//! the selected model or account must not rewrite it.
//!
//! WHERE POSTGRES DIFFERS: the original suite restarted by closing the SQLite
//! file and reopening it. The equivalent here is a second store opened against
//! the same scratch database: same durable state, a new handle and a new pool.
//! Nothing about the contract changes, only how a restart is spelled.

use std::path::{Path, PathBuf};

use uuid::Uuid;

use super::support::{configured, opencode_configured, store};
use crate::session_store::postgres::PostgresSessionStore;
use crate::{
    CodingProvider, PermissionMode, ResponseLanguage, SessionEvent, SessionEventKind, SessionState,
    SessionStore,
};

#[tokio::test]
async fn codex_harness_route_survives_restart_clear_fork_and_child_registration() {
    let (scratch, store) = store().await;
    let mut expected = Vec::new();
    for native in [true, false] {
        let parent = Uuid::new_v4();
        store.create_session(parent).await.unwrap();
        if !native {
            store
                .append(SessionEvent::new(
                    parent,
                    0,
                    SessionEventKind::SessionStarted,
                ))
                .await
                .unwrap();
        }
        let (first, second) = tokio::join!(
            store.uses_native_codex_harness(parent),
            store.uses_native_codex_harness(parent),
        );
        assert_eq!(first.unwrap(), native);
        assert_eq!(second.unwrap(), native);
        if !native {
            assert!(
                store
                    .record_model_access(parent, CodingProvider::Codex, "account-a")
                    .await
                    .is_err()
            );
        }
        store
            .append(SessionEvent::new(
                parent,
                0,
                SessionEventKind::ContextCleared,
            ))
            .await
            .unwrap();
        let fork = Uuid::new_v4();
        store.fork_before(parent, fork, 1).await.unwrap();
        let child = Uuid::new_v4();
        store.register_child_session(parent, child).await.unwrap();
        store.register_child_session(parent, child).await.unwrap();
        expected.extend([parent, fork, child].map(|id| (id, native)));
    }
    // Attaching an already routed child cannot rewrite its execution contract.
    assert!(
        store
            .register_child_session(expected[0].0, expected[3].0)
            .await
            .is_err()
    );
    store.pool().close().await;
    let reopened = PostgresSessionStore::connect_with_pool_size(&scratch.url, 4)
        .await
        .unwrap();
    for (session_id, native) in expected {
        assert_eq!(
            reopened
                .uses_native_codex_harness(session_id)
                .await
                .unwrap(),
            native
        );
    }
    scratch.discard().await;
}

#[tokio::test]
async fn model_access_changes_preserve_history_across_restart_forks_and_children() {
    let (scratch, store) = store().await;
    let parent = Uuid::new_v4();
    store.create_session(parent).await.unwrap();
    // Existing native history without account tags must remain usable.
    store
        .append(SessionEvent::new(
            parent,
            0,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "native_model_message".to_string(),
                payload: serde_json::to_value(borg_provider::provider::ModelMessage::user(
                    "keep this history",
                ))
                .unwrap(),
            },
        ))
        .await
        .unwrap();
    store
        .record_model_access(parent, CodingProvider::Codex, "account-a")
        .await
        .unwrap();

    let fork = Uuid::new_v4();
    store.fork_before(parent, fork, 2).await.unwrap();
    let child = Uuid::new_v4();
    store.create_session(child).await.unwrap();
    store
        .record_model_access(child, CodingProvider::Codex, "account-b")
        .await
        .unwrap();
    store.register_child_session(parent, child).await.unwrap();

    let mut histories = Vec::new();
    for id in [parent, fork, child] {
        histories.push((
            id,
            serde_json::to_value(store.read(id).await.unwrap()).unwrap(),
        ));
    }
    store.pool().close().await;
    let reopened = PostgresSessionStore::connect_with_pool_size(&scratch.url, 4)
        .await
        .unwrap();
    for (id, history) in histories {
        for identity in [
            "account-b",
            "api-sha256:first-key",
            "api-sha256:replacement-key",
            "account-a",
        ] {
            reopened
                .record_model_access(id, CodingProvider::Codex, identity)
                .await
                .unwrap();
            assert_eq!(
                serde_json::to_value(reopened.read(id).await.unwrap()).unwrap(),
                history
            );
        }
    }
    // Different selected accounts do not prevent reconnecting an existing child.
    reopened
        .record_model_access(child, CodingProvider::Codex, "account-c")
        .await
        .unwrap();
    reopened
        .register_child_session(parent, child)
        .await
        .unwrap();
    scratch.discard().await;
}

#[test]
fn context_generation_changes_only_at_explicit_prefix_boundaries() {
    let session_id = Uuid::new_v4();
    let mut state = SessionState::default();
    state
        .apply(&SessionEvent::new(
            session_id,
            1,
            SessionEventKind::SessionStarted,
        ))
        .unwrap();
    state
        .apply(&SessionEvent::new(
            session_id,
            2,
            configured(Path::new("/tmp")),
        ))
        .unwrap();
    assert_eq!(state.context_generation, 0);
    state
        .apply(&SessionEvent::new(
            session_id,
            3,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "context_compaction".to_string(),
                payload: serde_json::json!({"status": "completed"}),
            },
        ))
        .unwrap();
    assert_eq!(state.context_generation, 1);
    state
        .apply(&SessionEvent::new(
            session_id,
            4,
            SessionEventKind::ContextCleared,
        ))
        .unwrap();
    assert_eq!(state.context_generation, 2);
    state
        .apply(&SessionEvent::new(
            session_id,
            5,
            SessionEventKind::SessionConfigured {
                cwd: PathBuf::from("/tmp"),
                provider: CodingProvider::Claude,
                model: Some("claude-test".to_string()),
                effort: Some("high".to_string()),
                fast: false,
                response_language: ResponseLanguage::Auto,
                permission_mode: PermissionMode::FullAccess,
            },
        ))
        .unwrap();
    assert_eq!(state.context_generation, 3);
}

#[test]
fn same_provider_codex_model_change_preserves_resume_checkpoint() {
    let session_id = Uuid::new_v4();
    let mut state = SessionState::default();
    state
        .apply(&SessionEvent::new(
            session_id,
            1,
            SessionEventKind::SessionStarted,
        ))
        .unwrap();
    state
        .apply(&SessionEvent::new(
            session_id,
            2,
            SessionEventKind::SessionConfigured {
                cwd: PathBuf::from("/tmp"),
                provider: CodingProvider::Codex,
                model: Some("gpt-5.6-sol".to_string()),
                effort: Some("xhigh".to_string()),
                fast: false,
                response_language: ResponseLanguage::Auto,
                permission_mode: PermissionMode::FullAccess,
            },
        ))
        .unwrap();
    state
        .apply(&SessionEvent::new(
            session_id,
            3,
            SessionEventKind::TurnCompleted {
                message_id: Uuid::new_v4(),
                provider_session_id: Some("codex-thread".to_string()),
                final_text: "done".to_string(),
                error: None,
            },
        ))
        .unwrap();
    state
        .apply(&SessionEvent::new(
            session_id,
            4,
            SessionEventKind::SessionConfigured {
                cwd: PathBuf::from("/tmp"),
                provider: CodingProvider::Codex,
                model: Some("gpt-5.6-luna".to_string()),
                effort: Some("ultra".to_string()),
                fast: false,
                response_language: ResponseLanguage::Auto,
                permission_mode: PermissionMode::FullAccess,
            },
        ))
        .unwrap();

    assert_eq!(state.context_generation, 1);
    assert_eq!(state.provider_session_id.as_deref(), Some("codex-thread"));
    assert_eq!(state.usage.context_tokens, Some(0));
}

/// Only the Go aliases have an API Borg can call directly, so the route is
/// decided by the model. Getting this wrong either strands a working route on
/// the CLI or, worse, points a non-Go model at the Go allowance.
#[tokio::test]
async fn opencode_route_is_native_only_for_go_models() {
    let (scratch, store) = store().await;
    for (model, native) in [
        ("opencode-go/kimi-k2.7-code", true),
        ("opencode-go/glm-5.3", true),
        ("opencode/kimi-k2.7-code", false),
        ("opencode-go/", false),
        ("claude-opus-5", false),
    ] {
        let session_id = Uuid::new_v4();
        store.create_session(session_id).await.unwrap();
        assert_eq!(
            store
                .uses_native_opencode_harness(session_id, Some(model))
                .await
                .unwrap(),
            native,
            "{model}"
        );
    }
    scratch.discard().await;
}

/// The route is pinned on first resolution. A session that has been answering
/// through the `opencode` CLI keeps a transcript only that CLI can replay, so
/// switching to a Go model must not move the conversation onto Borg's harness
/// — and a session Borg already owns must not be handed back.
#[tokio::test]
async fn a_pinned_opencode_route_survives_model_switches_and_restart() {
    let (scratch, store) = store().await;

    // Pinned native, then switched to a route with no Borg-reachable API.
    let native_session = Uuid::new_v4();
    store.create_session(native_session).await.unwrap();
    assert!(
        store
            .uses_native_opencode_harness(native_session, Some("opencode-go/kimi-k2.7-code"))
            .await
            .unwrap()
    );
    assert!(
        store
            .uses_native_opencode_harness(native_session, Some("opencode/kimi-k2.7-code"))
            .await
            .unwrap(),
        "a pinned native session keeps its Borg-owned history across a model switch"
    );

    // A CLI-owned conversation that later selects a Go model.
    let legacy_session = Uuid::new_v4();
    store.create_session(legacy_session).await.unwrap();
    store
        .append(SessionEvent::new(
            legacy_session,
            0,
            opencode_configured("opencode/kimi-k2.7-code"),
        ))
        .await
        .unwrap();
    store
        .append(SessionEvent::new(
            legacy_session,
            0,
            SessionEventKind::ProviderSessionLinked {
                provider_session_id: "opencode-cli-thread".to_string(),
                provider_turn_id: None,
                context_contract_version: None,
            },
        ))
        .await
        .unwrap();
    assert!(
        !store
            .uses_native_opencode_harness(legacy_session, Some("opencode-go/kimi-k2.7-code"))
            .await
            .unwrap(),
        "legacy OpenCode history must not be adopted by Borg's harness"
    );

    store.pool().close().await;
    let reopened = PostgresSessionStore::connect_with_pool_size(&scratch.url, 4)
        .await
        .unwrap();
    assert!(
        reopened
            .uses_native_opencode_harness(native_session, None)
            .await
            .unwrap()
    );
    assert!(
        !reopened
            .uses_native_opencode_harness(legacy_session, None)
            .await
            .unwrap()
    );
    scratch.discard().await;
}

/// A fork or child shares its owner's transcript, so it must share whichever
/// harness owns it; otherwise one conversation would have two owners.
#[tokio::test]
async fn opencode_route_is_inherited_by_forks_and_children() {
    let (scratch, store) = store().await;
    for (model, native) in [
        ("opencode-go/kimi-k2.7-code", true),
        ("opencode/glm", false),
    ] {
        let parent = Uuid::new_v4();
        store.create_session(parent).await.unwrap();
        assert_eq!(
            store
                .uses_native_opencode_harness(parent, Some(model))
                .await
                .unwrap(),
            native
        );
        store
            .append(SessionEvent::new(parent, 0, opencode_configured(model)))
            .await
            .unwrap();

        let fork = Uuid::new_v4();
        store.fork_before(parent, fork, 1).await.unwrap();
        let child = Uuid::new_v4();
        store.register_child_session(parent, child).await.unwrap();
        for inheritor in [fork, child] {
            assert_eq!(
                store
                    .uses_native_opencode_harness(inheritor, None)
                    .await
                    .unwrap(),
                native,
                "{model}"
            );
            // Re-resolving under the opposite model cannot rewrite the route.
            assert_eq!(
                store
                    .uses_native_opencode_harness(inheritor, Some("opencode-go/other"))
                    .await
                    .unwrap(),
                native
            );
        }
    }
    scratch.discard().await;
}

/// A fresh session with no model yet must stay undecided. Pinning it here
/// would strand it on the compatibility route before its model was ever known.
#[tokio::test]
async fn an_unknown_opencode_model_does_not_pin_the_route() {
    let (scratch, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    assert!(
        !store
            .uses_native_opencode_harness(session_id, None)
            .await
            .unwrap()
    );
    assert!(
        store
            .uses_native_opencode_harness(session_id, Some("opencode-go/kimi-k2.7-code"))
            .await
            .unwrap(),
        "the route is still open once the model is known"
    );
    scratch.discard().await;
}
