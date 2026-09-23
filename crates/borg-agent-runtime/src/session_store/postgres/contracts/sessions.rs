//! What a session IS to the journal: a row that can be discarded while it is
//! still empty, a projection of everything appended to it, a stable workspace
//! binding, and a searchable history that survives being forked.
//!
//! These are the store-level contracts the rest of the runtime leans on
//! without asking which engine answers them. They were written against
//! `SessionStore`, not against SQL, so they are kept here rather than beside
//! the queries that happen to implement them today.
//!
//! Every test needs a Postgres server; `support::store` fails with
//! instructions when there is none.

use chrono::Utc;
use uuid::Uuid;

use super::support::{configured, message, store};
use crate::session_store::{INLINE_SESSION_PAYLOAD_BYTES, SessionPayloadKind, SessionPayloadRef};
use crate::{
    CodingProvider, EventActor, EventPersistence, MessageStatus, PermissionMode, PromptDelivery,
    ResponseLanguage, SessionEvent, SessionEventKind, SessionHistoryQuery, SessionStore,
    SessionWorkspaceBinding,
};

#[tokio::test]
async fn empty_sessions_are_discarded_but_real_sessions_are_kept() {
    let (scratch, store) = store().await;
    let empty = Uuid::new_v4();
    let real = Uuid::new_v4();
    store.create_session(empty).await.unwrap();
    store.create_session(real).await.unwrap();

    for kind in [
        SessionEventKind::SessionStarted,
        SessionEventKind::SessionConfigured {
            cwd: std::path::PathBuf::from("/tmp/borg-empty"),
            provider: CodingProvider::Codex,
            model: None,
            effort: None,
            fast: false,
            response_language: ResponseLanguage::Auto,
            permission_mode: PermissionMode::FullAccess,
        },
        SessionEventKind::ProviderCapabilitiesUpdated {
            providers: Vec::new(),
        },
        SessionEventKind::EffectiveCapabilitiesUpdated {
            capabilities: crate::EffectiveCapabilities {
                active: Vec::new(),
                inactive: Vec::new(),
            },
        },
    ] {
        store
            .append(SessionEvent::new(empty, 0, kind))
            .await
            .unwrap();
    }
    store
        .append(SessionEvent::new(
            real,
            0,
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::User,
                text: "keep this thread".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: Some(PromptDelivery::Queue),
            },
        ))
        .await
        .unwrap();

    assert!(store.discard_empty_session(empty).await.unwrap());
    assert!(!store.contains_session(empty).await.unwrap());
    assert!(!store.discard_empty_session(real).await.unwrap());
    assert!(store.contains_session(real).await.unwrap());
    scratch.discard().await;
}

#[tokio::test]
async fn provider_capability_snapshot_is_durable_metadata_not_context() {
    let (scratch, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    let providers = vec![crate::ProviderCapability {
        provider: CodingProvider::Codex,
        installed: true,
        version: Some("test".to_string()),
        authenticated: true,
        auth_detail: Some("Codex subscription authenticated".to_string()),
        auth_methods: vec![crate::ProviderAuthMethod::Subscription],
        can_spawn: true,
        usage: None,
        billing: None,
    }];
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ProviderCapabilitiesUpdated {
                providers: providers.clone(),
            },
        ))
        .await
        .unwrap();

    assert_eq!(
        store.state(session_id).await.unwrap().provider_capabilities,
        providers
    );
    assert!(
        store
            .recovery(session_id)
            .await
            .unwrap()
            .context_events
            .is_empty()
    );
    assert_eq!(store.read(session_id).await.unwrap().len(), 1);
    scratch.discard().await;
}

#[tokio::test]
async fn sessions_have_stable_workspace_bindings_and_children_inherit_the_team_workspace() {
    let (scratch, store) = store().await;
    let root = Uuid::new_v4();
    store.create_session(root).await.unwrap();
    let root_binding = store.workspace_binding(root).await.unwrap().unwrap();
    assert_eq!(root_binding.workspace_id, root);
    assert_eq!(root_binding.participant_id, root);

    let host_id = Uuid::new_v4();
    let reattached = store
        .attach_workspace(SessionWorkspaceBinding {
            host_id: Some(host_id),
            attached_at: Utc::now(),
            ..root_binding.clone()
        })
        .await
        .unwrap();
    assert_eq!(reattached.host_id, Some(host_id));
    assert_eq!(
        store
            .workspace_binding(root)
            .await
            .unwrap()
            .unwrap()
            .host_id,
        Some(host_id)
    );
    assert!(
        store
            .attach_workspace(SessionWorkspaceBinding {
                workspace_id: Uuid::new_v4(),
                ..reattached
            })
            .await
            .unwrap_err()
            .to_string()
            .contains("already attached")
    );

    // The original acquired a writer lease on the child's journal file first,
    // because a SQLite session WAS a file. A session is a row here, so the
    // registration below is the whole of it.
    let child = Uuid::new_v4();
    store.register_child_session(root, child).await.unwrap();
    let child_binding = store.workspace_binding(child).await.unwrap().unwrap();
    assert_eq!(child_binding.workspace_id, root);
    assert_eq!(child_binding.participant_id, child);
    scratch.discard().await;
}

#[tokio::test]
async fn new_session_can_start_in_a_selected_workspace_without_becoming_rebindable() {
    let (scratch, store) = store().await;
    let session_id = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    let binding = store
        .create_session_in_workspace(session_id, workspace_id)
        .await
        .unwrap();
    assert_eq!(binding.workspace_id, workspace_id);
    assert_eq!(binding.participant_id, session_id);
    assert_eq!(
        store.workspace_binding(session_id).await.unwrap().unwrap(),
        binding
    );
    assert!(
        store
            .attach_workspace(SessionWorkspaceBinding {
                workspace_id: Uuid::new_v4(),
                ..binding
            })
            .await
            .unwrap_err()
            .to_string()
            .contains("already attached")
    );
    scratch.discard().await;
}

#[tokio::test]
async fn store_appends_projects_and_reads_indexed_suffixes() {
    let (scratch, store) = store().await;
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(std::path::Path::new("/tmp")),
        message(message_id, "hello"),
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }

    let state = store.state(session_id).await.unwrap();
    assert_eq!(state.latest_sequence, 3);
    assert_eq!(
        state.configuration.as_ref().unwrap().model.as_deref(),
        Some("gpt-test")
    );
    assert!(
        store
            .contains_message(session_id, message_id)
            .await
            .unwrap()
    );
    let suffix = store.events_after(session_id, 1, 10).await.unwrap();
    assert_eq!(
        suffix
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        [2, 3]
    );
    let recovery = store.recovery(session_id).await.unwrap();
    assert_eq!(recovery.context_events.len(), 1);
    assert_eq!(recovery.queue_events.len(), 1);
    assert!(recovery.subagent_events.is_empty());
    assert_eq!(store.list_sessions(10).await.unwrap().len(), 1);
    scratch.discard().await;
}

#[tokio::test]
async fn recent_user_messages_are_bounded_ordered_and_ignore_non_recallable_prompts() {
    let (scratch, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        message(Uuid::new_v4(), "first"),
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::Assistant,
            text: "assistant".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::User,
            text: "still queued".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Queued,
            delivery: Some(PromptDelivery::Queue),
        },
        message(Uuid::new_v4(), "second"),
        message(Uuid::new_v4(), "third"),
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::User,
            text: "Team message from /root/worker:\n\ninternal report".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: Some(PromptDelivery::Queue),
        },
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::User,
            text: "failed but recallable".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Failed,
            delivery: Some(PromptDelivery::Queue),
        },
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }

    let prompts = store.recent_user_messages(session_id, 2).await.unwrap();
    let texts = prompts
        .iter()
        .filter_map(|event| match &event.kind {
            SessionEventKind::Message { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(texts, vec!["third", "failed but recallable"]);
    assert!(
        store
            .recent_user_messages(session_id, 0)
            .await
            .unwrap()
            .is_empty()
    );
    scratch.discard().await;
}

#[tokio::test]
async fn state_projects_pending_approval_and_cumulative_usage() {
    let (scratch, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(std::path::Path::new("/tmp")),
        SessionEventKind::ApprovalRequested {
            approval_id: "approval-1".to_string(),
            title: "Run command".to_string(),
            detail: "Needs permission".to_string(),
            command: Some("cargo test".to_string()),
        },
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }
    assert_eq!(
        store.state(session_id).await.unwrap().pending_approval_id,
        Some("approval-1".to_string())
    );
    for kind in [
        SessionEventKind::ApprovalResolved {
            approval_id: "approval-1".to_string(),
            decision: crate::ApprovalDecision::AllowOnce,
        },
        SessionEventKind::UsageUpdated {
            provider_duration_ms: 10,
            turn_id: None,
            provider_context_reused: None,
            input_tokens: 100,
            output_tokens: 20,
            cached_input_tokens: 40,
            cache_creation_input_tokens: 5,
            total_tokens: 120,
            cost_microusd: Some(100),
            cost_basis: "provider".to_string(),
            cost_usd: Some(0.0001),
            context_tokens: Some(100),
            context_window_tokens: Some(1_000),
        },
        SessionEventKind::UsageUpdated {
            provider_duration_ms: 20,
            turn_id: None,
            provider_context_reused: None,
            input_tokens: 200,
            output_tokens: 30,
            cached_input_tokens: 80,
            cache_creation_input_tokens: 7,
            total_tokens: 230,
            cost_microusd: Some(200),
            cost_basis: "provider".to_string(),
            cost_usd: Some(0.0002),
            context_tokens: Some(200),
            context_window_tokens: Some(1_000),
        },
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }

    let state = store.state(session_id).await.unwrap();
    assert_eq!(state.pending_approval_id, None);
    assert_eq!(state.usage.calls, 2);
    assert_eq!(state.usage.provider_duration_ms, 30);
    assert_eq!(state.usage.input_tokens, 300);
    assert_eq!(state.usage.output_tokens, 50);
    assert_eq!(state.usage.cached_input_tokens, 120);
    assert_eq!(state.usage.cache_creation_input_tokens, 12);
    assert_eq!(state.usage.total_tokens, 350);
    assert_eq!(state.usage.cost_microusd, Some(300));
    assert!((state.usage.cost_usd.unwrap() - 0.0003).abs() < f64::EPSILON);
    assert_eq!(state.usage.context_tokens, Some(200));
    scratch.discard().await;
}

#[tokio::test]
async fn large_tool_payloads_are_loaded_only_by_reference() {
    let (scratch, store) = store().await;
    let session_id = Uuid::new_v4();
    let input = serde_json::json!({"text": "x".repeat(INLINE_SESSION_PAYLOAD_BYTES)});
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(std::path::Path::new("/tmp")),
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }
    let appended = store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ToolStarted {
                tool_call_id: "large-tool".to_string(),
                name: "large".to_string(),
                input: input.clone(),
                input_ref: None,
            },
        ))
        .await
        .unwrap();
    assert!(matches!(
        appended.kind,
        SessionEventKind::ToolStarted {
            input_ref: Some(_),
            ..
        }
    ));

    let persisted = store.events_after(session_id, 2, 1).await.unwrap();
    let SessionEventKind::ToolStarted {
        input: preview,
        input_ref: Some(payload),
        ..
    } = &persisted[0].kind
    else {
        panic!("large tool input should be stored by reference");
    };
    assert_ne!(preview, &input);
    assert_eq!(
        store.load_payload(payload).await.unwrap(),
        serde_json::to_vec(&input).unwrap()
    );
    scratch.discard().await;
}

/// The exact text a subscription turn handed its provider is journaled as a
/// deferred payload. Without the embedded-reference extraction, history
/// expansion would silently return the preview instead of the real prompt.
#[tokio::test]
async fn large_provider_prompts_are_stored_by_reference_and_expandable() {
    let (scratch, store) = store().await;
    let session_id = Uuid::new_v4();
    let needle = "uniqueprompt8675309";
    let prompt = format!(
        "{} {needle}",
        "p".repeat(INLINE_SESSION_PAYLOAD_BYTES + 1024)
    );
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(std::path::Path::new("/tmp")),
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }
    let appended = store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Claude,
                kind: crate::PROVIDER_PROMPT_EVENT_KIND.to_string(),
                payload: serde_json::json!({
                    "prompt": prompt,
                    "search_marker": needle,
                    "provider_context_reused": false,
                }),
            },
        ))
        .await
        .unwrap();

    let SessionEventKind::ProviderEvent { payload, .. } = &appended.kind else {
        panic!("provider prompt event should round-trip");
    };
    let reference: SessionPayloadRef = serde_json::from_value(
        payload
            .get(crate::PROVIDER_PROMPT_REF_FIELD)
            .expect("deferred prompt carries a reference")
            .clone(),
    )
    .unwrap();
    assert_eq!(reference.kind, SessionPayloadKind::ProviderPrompt);
    assert_ne!(
        payload
            .get(crate::PROVIDER_PROMPT_FIELD)
            .and_then(serde_json::Value::as_str),
        Some(prompt.as_str())
    );
    assert_eq!(
        store.load_payload(&reference).await.unwrap(),
        prompt.as_bytes()
    );

    let hit = store
        .query_history(
            session_id,
            SessionHistoryQuery {
                text: Some(needle.to_string()),
                expand_payloads: true,
                max_payload_bytes: Some(INLINE_SESSION_PAYLOAD_BYTES * 2),
                ..SessionHistoryQuery::default()
            },
        )
        .await
        .unwrap();
    assert!(
        hit.hits.iter().any(|hit| hit
            .payloads
            .iter()
            .any(|payload| payload.text.contains(needle))),
        "expanded history must surface the deferred provider prompt"
    );
    scratch.discard().await;
}

#[tokio::test]
async fn history_query_preserves_projected_ids_and_sequences_across_forks() {
    let (scratch, store) = store().await;
    let parent_id = Uuid::new_v4();
    let fork_id = Uuid::new_v4();
    store.create_session(parent_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(std::path::Path::new("/tmp")),
        message(Uuid::new_v4(), "inherit this distinctive finding"),
        message(Uuid::new_v4(), "discard this later finding"),
    ] {
        store
            .append(SessionEvent::new(parent_id, 0, kind))
            .await
            .unwrap();
    }
    store.fork_before(parent_id, fork_id, 4).await.unwrap();
    store
        .append(SessionEvent::new(
            fork_id,
            0,
            message(Uuid::new_v4(), "local fork conclusion"),
        ))
        .await
        .unwrap();

    let inherited = store
        .query_history(
            fork_id,
            SessionHistoryQuery {
                text: Some("distinctive finding".to_string()),
                ..SessionHistoryQuery::default()
            },
        )
        .await
        .unwrap();
    // The SQLite original named this backend `lineage_scan`; the composed read
    // it stands for is the same one, reported here under the engine's name.
    assert_eq!(inherited.backend, "postgres_lineage");
    assert_eq!(inherited.hits.len(), 1);
    assert_eq!(inherited.hits[0].event.session_id, fork_id);
    assert_eq!(inherited.hits[0].event.sequence, 3);
    assert_ne!(
        inherited.hits[0].event.id,
        store.read(parent_id).await.unwrap()[2].id
    );

    let all_messages = store
        .query_history(
            fork_id,
            SessionHistoryQuery {
                event_kinds: vec!["message".to_string()],
                ..SessionHistoryQuery::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(all_messages.hits.len(), 2);
    assert_eq!(all_messages.hits[1].event.sequence, 4);

    let index_documents = store
        .history_index_documents_after(fork_id, 0, 10)
        .await
        .unwrap();
    assert_eq!(index_documents.len(), 4);
    assert_eq!(index_documents[2].event_id, inherited.hits[0].event.id);
    assert!(index_documents[2].content.contains("distinctive finding"));
    assert!(
        index_documents
            .iter()
            .all(|document| document.session_id == fork_id)
    );
    scratch.discard().await;
}

#[test]
fn persistence_and_fork_rules_are_typed_rust_contracts() {
    assert_eq!(
        SessionEventKind::ReasoningDelta {
            text: "working".to_string()
        }
        .persistence(),
        EventPersistence::Coalesced
    );
    assert_eq!(
        SessionEventKind::ProviderEvent {
            provider: CodingProvider::Codex,
            kind: "noise".to_string(),
            payload: serde_json::Value::Null,
        }
        .persistence(),
        EventPersistence::Ephemeral
    );
    let user_in_progress = SessionEventKind::Message {
        message_id: Uuid::new_v4(),
        actor: crate::EventActor::User,
        text: "must survive a host crash".to_string(),
        attachments: Vec::new(),
        status: MessageStatus::InProgress,
        delivery: Some(crate::PromptDelivery::Queue),
    };
    assert_eq!(user_in_progress.persistence(), EventPersistence::Durable);
    // A parent journals a mirrored child event only when the child would AND
    // the parent replays something from it. See is_parent_replayable.
    let child_id = Uuid::new_v4();
    let snapshot = || crate::SubagentSnapshot {
        session_id: child_id,
        parent_session_id: Uuid::new_v4(),
        task_name: "worker".to_string(),
        status: crate::SubagentStatus::Running,
        provider: CodingProvider::Codex,
        model: None,
        effort: None,
        cwd: std::path::PathBuf::from("/tmp"),
        created_at: Utc::now(),
        updated_at: Utc::now(),
        detail: None,
        final_text: None,
        usage: crate::SubagentUsage::default(),
        interrupted_by: None,
    };
    let mirrored = |kind: SessionEventKind| SessionEventKind::SubagentActivity {
        activity: crate::SubagentActivityKind::Updated,
        agent: snapshot(),
        event: Some(Box::new(SessionEvent::new(child_id, 0, kind))),
    };
    assert_eq!(
        mirrored(SessionEventKind::ProviderEvent {
            provider: CodingProvider::Codex,
            kind: "heartbeat".to_string(),
            payload: serde_json::Value::Null,
        })
        .persistence(),
        EventPersistence::Ephemeral
    );
    assert_eq!(
        mirrored(SessionEventKind::ReasoningDelta {
            text: "thinking".to_string()
        })
        .persistence(),
        EventPersistence::Ephemeral
    );
    assert_eq!(
        mirrored(SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: crate::EventActor::Assistant,
            text: "streaming".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: None,
        })
        .persistence(),
        EventPersistence::Ephemeral
    );
    assert_eq!(
        mirrored(SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: crate::EventActor::Assistant,
            text: "done".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        })
        .persistence(),
        EventPersistence::Durable
    );
    assert_eq!(
        SessionEventKind::SubagentActivity {
            activity: crate::SubagentActivityKind::Completed,
            agent: snapshot(),
            event: None,
        }
        .persistence(),
        EventPersistence::Durable
    );
    assert!(!user_in_progress.is_fork_inheritable());
    assert_eq!(
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: crate::EventActor::Assistant,
            text: "streaming".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: None,
        }
        .persistence(),
        EventPersistence::Coalesced
    );
    for kind in [
        "item/started:contextCompaction",
        "item/completed:contextCompaction",
        "item/started:context_compaction",
        "item/completed:context_compaction",
    ] {
        let notification = SessionEventKind::ProviderEvent {
            provider: CodingProvider::Codex,
            kind: kind.to_string(),
            payload: serde_json::Value::Null,
        };
        assert_eq!(notification.persistence(), EventPersistence::Durable);
        assert!(!notification.is_completed_context_compaction());
        assert!(!notification.is_completed_provider_recovery_checkpoint());
        assert!(!notification.is_context_relevant());
    }
    let compaction_started = SessionEventKind::ProviderEvent {
        provider: CodingProvider::Codex,
        kind: "context_compaction".to_string(),
        payload: serde_json::json!({"status": "started"}),
    };
    assert_eq!(compaction_started.persistence(), EventPersistence::Durable);
    assert!(!compaction_started.is_context_relevant());
    let compaction_completed = SessionEventKind::ProviderEvent {
        provider: CodingProvider::Codex,
        kind: "context_compaction".to_string(),
        payload: serde_json::json!({"status": "completed", "summary": "done"}),
    };
    assert_eq!(
        compaction_completed.persistence(),
        EventPersistence::Durable
    );
    assert!(compaction_completed.is_context_relevant());
    assert!(
        !SessionEventKind::ProviderSessionLinked {
            provider_session_id: "provider".to_string(),
            provider_turn_id: None,
            context_contract_version: None,
        }
        .is_fork_inheritable()
    );
}

/// A session whose provider no longer exists is hidden from the picker, but is
/// NOT deleted.
///
/// Providers are removed from the catalog over time, and a session configured
/// with one that is gone can no longer be resumed. Listing it would offer the
/// user a session that cannot open. Deleting it would destroy history the user
/// never asked to lose. So `list_sessions` skips it while the row stays exactly
/// where it was, which is what `contains_session` proves here.
#[tokio::test]
async fn list_sessions_skips_state_for_removed_providers() {
    let (scratch, store) = store().await;
    let valid = Uuid::new_v4();
    let incompatible = Uuid::new_v4();
    store.create_session(valid).await.unwrap();
    store.create_session(incompatible).await.unwrap();
    // Written as raw state rather than through `append`, because the point is a
    // provider the current build cannot construct an event for.
    sqlx::query("update sessions set state_json = $1 where id = $2")
        .bind(r#"{"configuration":{"provider":"open_code"}}"#)
        .bind(incompatible)
        .execute(store.pool())
        .await
        .unwrap();

    let sessions = store.list_sessions(10).await.unwrap();
    assert_eq!(
        sessions
            .into_iter()
            .map(|session| session.session_id)
            .collect::<Vec<_>>(),
        vec![valid]
    );
    assert!(
        store.contains_session(incompatible).await.unwrap(),
        "an unlistable session must still exist"
    );

    scratch.discard().await;
}
