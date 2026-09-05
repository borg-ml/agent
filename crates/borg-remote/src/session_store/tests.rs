use std::sync::Arc;
use std::time::{Duration, Instant};

use tempfile::tempdir;

use super::*;
use crate::{EventActor, PromptDelivery};

async fn store() -> (tempfile::TempDir, SqliteSessionStore) {
    let directory = tempdir().unwrap();
    let store = SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
        .await
        .unwrap();
    (directory, store)
}

#[tokio::test]
async fn model_access_binding_is_atomic_durable_and_inherited_without_context_dependence() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    let (first, second) = tokio::join!(
        store.bind_model_access(session_id, CodingProvider::Codex, "account-a"),
        store.bind_model_access(session_id, CodingProvider::Codex, "account-b"),
    );
    assert_ne!(
        first.is_ok(),
        second.is_ok(),
        "only one first-use identity may win"
    );
    let (accepted, rejected) = if first.is_ok() {
        ("account-a", "account-b")
    } else {
        ("account-b", "account-a")
    };
    for kind in [
        SessionEventKind::ContextCleared,
        SessionEventKind::ProviderEvent {
            provider: CodingProvider::Codex,
            kind: "context_compaction".to_string(),
            payload: serde_json::json!({"summary": "compacted"}),
        },
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }
    let fork_id = Uuid::new_v4();
    store.fork_before(session_id, fork_id, 1).await.unwrap();
    let child_id = Uuid::new_v4();
    store
        .register_child_session(session_id, child_id)
        .await
        .unwrap();
    let conflicting_child = Uuid::new_v4();
    store.create_session(conflicting_child).await.unwrap();
    store
        .bind_model_access(conflicting_child, CodingProvider::Codex, rejected)
        .await
        .unwrap();
    assert!(
        store
            .register_child_session(session_id, conflicting_child)
            .await
            .is_err()
    );
    store.pool.close().await;
    let reopened = SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
        .await
        .unwrap();
    for id in [session_id, fork_id, child_id] {
        reopened
            .bind_model_access(id, CodingProvider::Codex, accepted)
            .await
            .unwrap();
        assert!(
            reopened
                .bind_model_access(id, CodingProvider::Codex, rejected)
                .await
                .unwrap_err()
                .to_string()
                .contains("bound account")
        );
    }
    let fresh = Uuid::new_v4();
    reopened.create_session(fresh).await.unwrap();
    reopened
        .bind_model_access(fresh, CodingProvider::Codex, rejected)
        .await
        .unwrap();
}

#[tokio::test]
async fn subscription_access_rejects_unbound_prototype_history_and_its_forks() {
    let (_directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "native_model_message".to_string(),
                payload: serde_json::to_value(borg_provider::provider::ModelMessage::user(
                    "old private context",
                ))
                .unwrap(),
            },
        ))
        .await
        .unwrap();
    let fork_id = Uuid::new_v4();
    store.fork_before(session_id, fork_id, 2).await.unwrap();
    for id in [session_id, fork_id] {
        assert!(
            store
                .bind_model_access(id, CodingProvider::Codex, "current-account")
                .await
                .unwrap_err()
                .to_string()
                .contains("no account binding")
        );
    }
    let legacy = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    store.create_session(legacy).await.unwrap();
    for kind in [
        SessionEventKind::TurnStarted {
            message_id,
            provider: CodingProvider::Codex,
            model: None,
            effort: None,
            fast: false,
        },
        SessionEventKind::TurnCompleted {
            message_id,
            provider_session_id: None,
            final_text: "old result".to_string(),
            error: None,
        },
    ] {
        store
            .append(SessionEvent::new(legacy, 0, kind))
            .await
            .unwrap();
    }
    assert!(
        store
            .bind_model_access(legacy, CodingProvider::Codex, "current-account")
            .await
            .unwrap_err()
            .to_string()
            .contains("no account binding")
    );
}

#[tokio::test]
async fn runtime_manifest_and_checkpoint_survive_store_reopen_and_detect_worker_restart() {
    let (directory, store) = store().await;
    let path = directory.path().join("sessions.sqlite3");
    let session_id = Uuid::new_v4();
    let first_worker = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();

    let first = store
        .activate_runtime_manifest(session_id, "python", "/workspace", "python3", first_worker)
        .await
        .unwrap();
    assert!(!first.recovered_from_previous_worker);
    assert_eq!(first.manifest.status, RuntimeManifestStatus::Running);

    store
        .record_runtime_execution(session_id, first_worker, "sha256:code", false, None)
        .await
        .unwrap();
    let checkpoint = store
        .save_runtime_checkpoint(
            session_id,
            first_worker,
            "calibration-v1",
            &serde_json::json!({"angle": 12.5, "ticks": 240}),
        )
        .await
        .unwrap();
    assert!(checkpoint.content_hash.starts_with("sha256:"));

    store.pool().close().await;
    let reopened = SqliteSessionStore::open(&path).await.unwrap();
    let persisted = reopened
        .runtime_manifest(session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(persisted.execution_count, 1);
    assert_eq!(persisted.last_code_hash.as_deref(), Some("sha256:code"));
    assert_eq!(
        reopened
            .runtime_checkpoint(session_id, Some("calibration-v1"))
            .await
            .unwrap()
            .unwrap()
            .state,
        serde_json::json!({"angle": 12.5, "ticks": 240})
    );

    let second_worker = Uuid::new_v4();
    let recovered = reopened
        .activate_runtime_manifest(session_id, "python", "/workspace", "python3", second_worker)
        .await
        .unwrap();
    assert!(recovered.recovered_from_previous_worker);
    assert_eq!(recovered.manifest.worker_id, second_worker);

    let idempotent = reopened
        .save_runtime_checkpoint(
            session_id,
            second_worker,
            "calibration-v1",
            &serde_json::json!({"angle": 12.5, "ticks": 240}),
        )
        .await
        .unwrap();
    assert_eq!(idempotent.revision, checkpoint.revision);
    assert!(
        reopened
            .save_runtime_checkpoint(
                session_id,
                second_worker,
                "calibration-v1",
                &serde_json::json!({"angle": 13.0}),
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn harness_state_is_durable_and_rolls_back_without_polluting_runtime_checkpoints() {
    let (directory, store) = store().await;
    let path = directory.path().join("sessions.sqlite3");
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();

    let first = serde_json::json!({
        "schema": 1,
        "entries": [{"id": "memory-a", "content": "first"}],
        "refinements": []
    });
    let second = serde_json::json!({
        "schema": 1,
        "entries": [{"id": "memory-a", "content": "second"}],
        "refinements": [{"id": "refine-1"}]
    });
    store.save_harness_state(session_id, &first).await.unwrap();
    store.save_harness_state(session_id, &second).await.unwrap();
    assert_eq!(
        store.load_harness_state(session_id).await.unwrap(),
        Some(second.clone())
    );

    let restored = store.rollback_harness_state(session_id, 1).await.unwrap();
    assert_eq!(restored, first);
    assert_eq!(
        store.load_harness_state(session_id).await.unwrap(),
        Some(first.clone())
    );
    assert!(store.rollback_harness_state(session_id, 1).await.is_err());
    assert!(
        store
            .runtime_checkpoint(session_id, None)
            .await
            .unwrap()
            .is_none()
    );

    store.pool().close().await;
    let reopened = SqliteSessionStore::open(path).await.unwrap();
    assert_eq!(
        reopened.load_harness_state(session_id).await.unwrap(),
        Some(first)
    );
}

#[tokio::test]
async fn durable_append_waits_through_extended_writer_contention() {
    let (_directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();

    let blocker = store.begin_write().await.unwrap();
    let append_store = store.clone();
    let append = tokio::spawn(async move {
        append_store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::StatusChanged {
                    status: SessionStatus::Ready,
                    detail: None,
                },
            ))
            .await
    });

    tokio::time::sleep(SQLITE_BUSY_TIMEOUT + Duration::from_millis(250)).await;
    assert!(
        !append.is_finished(),
        "writer contention must wait instead of failing the session actor"
    );

    blocker.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), append)
        .await
        .expect("append did not resume after the writer lock cleared")
        .expect("append task panicked")
        .expect("append failed after the writer lock cleared");
}

#[tokio::test]
async fn session_creation_waits_through_extended_writer_contention() {
    let (_directory, store) = store().await;
    let blocker = store.begin_write().await.unwrap();
    let session_id = Uuid::new_v4();
    let create_store = store.clone();
    let create = tokio::spawn(async move { create_store.create_session(session_id).await });

    tokio::time::sleep(SQLITE_BUSY_TIMEOUT + Duration::from_millis(250)).await;
    assert!(
        !create.is_finished(),
        "session creation must wait instead of failing on the busy timeout"
    );

    blocker.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), create)
        .await
        .expect("session creation did not resume after the writer lock cleared")
        .expect("session creation task panicked")
        .expect("session creation failed after the writer lock cleared");
    assert!(store.contains_session(session_id).await.unwrap());
}

#[tokio::test]
async fn writer_contention_has_a_bounded_escape_hatch() {
    let (_directory, store) = store().await;
    let blocker = store.begin_write().await.unwrap();

    let result = SqliteSessionStore::begin_sqlite_write_with_timeout(
        store.pool(),
        Duration::from_millis(100),
    )
    .await;

    assert!(matches!(result, Err(sqlx::Error::PoolTimedOut)));
    blocker.rollback().await.unwrap();
}

#[tokio::test]
async fn production_writer_admission_survives_repeated_contention_timeouts() {
    let (_directory, store) = store().await;
    let blocker = store.begin_write().await.unwrap();
    let pool = store.pool().clone();
    let writer = tokio::spawn(async move {
        SqliteSessionStore::begin_sqlite_write_resilient(&pool, Duration::from_millis(100)).await
    });

    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(
        !writer.is_finished(),
        "production writer admission must not fail a session while contention persists"
    );

    blocker.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), writer)
        .await
        .expect("writer did not resume after contention cleared")
        .expect("writer task panicked")
        .expect("writer admission failed after contention cleared")
        .rollback()
        .await
        .unwrap();
}

#[tokio::test]
async fn writer_contention_does_not_exhaust_the_pool() {
    let (_directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    let blocker = store.begin_write().await.unwrap();

    let contenders = (0..8)
        .map(|_| {
            let pool = store.pool().clone();
            tokio::spawn(async move {
                SqliteSessionStore::begin_sqlite_write_with_timeout(&pool, Duration::from_secs(2))
                    .await
            })
        })
        .collect::<Vec<_>>();

    // Before the admission gate, the seven available connections all sit in
    // SQLite busy waits and the next ordinary read times out waiting for the
    // pool. The read must remain available while the journal writer is held.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        tokio::time::timeout(
            Duration::from_millis(500),
            store.contains_session(session_id)
        )
        .await
        .expect("a read must not wait behind blocked writers")
        .expect("the session read must succeed")
    );

    blocker.rollback().await.unwrap();
    for contender in contenders {
        let _transaction = tokio::time::timeout(Duration::from_secs(2), contender)
            .await
            .expect("contending writer did not finish")
            .expect("contending writer task panicked")
            .expect("contending writer failed after the lock cleared");
    }
}

#[tokio::test]
async fn empty_sessions_are_discarded_but_real_sessions_are_kept() {
    let (_directory, store) = store().await;
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
}

#[tokio::test]
async fn opening_schema_v4_archives_and_recreates_the_database() {
    let (directory, store) = store().await;
    let path = directory.path().join("sessions.sqlite3");
    sqlx::query("drop index if exists idx_session_actions_lease_expiry")
        .execute(store.pool())
        .await
        .unwrap();
    for column in [
        "lease_owner",
        "lease_token",
        "lease_heartbeat_at",
        "lease_expires_at",
    ] {
        // These identifiers are fixed by the schema-reset regression fixture;
        // SQLx 0.9 requires the dynamic identifier to be explicitly audited.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "alter table session_actions drop column {column}"
        )))
        .execute(store.pool())
        .await
        .unwrap();
    }
    sqlx::query("update borg_session_schema set version=4 where id=1")
        .execute(store.pool())
        .await
        .unwrap();
    store.pool().close().await;

    let reopened = SqliteSessionStore::open(path).await.unwrap();
    let columns = sqlx::query("pragma table_info(session_actions)")
        .fetch_all(reopened.pool())
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect::<HashSet<_>>();
    assert!(
        [
            "lease_owner",
            "lease_token",
            "lease_heartbeat_at",
            "lease_expires_at",
        ]
        .into_iter()
        .all(|column| columns.contains(column))
    );
    let version: i64 = sqlx::query_scalar("select version from borg_session_schema where id=1")
        .fetch_one(reopened.pool())
        .await
        .unwrap();
    assert_eq!(version, SESSION_SCHEMA_VERSION);
    let sessions: i64 = sqlx::query_scalar("select count(*) from sessions")
        .fetch_one(reopened.pool())
        .await
        .unwrap();
    assert_eq!(sessions, 0, "the incompatible database must not be reused");
    let archive_count = std::fs::read_dir(directory.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("sessions.sqlite3.incompatible-")
        })
        .count();
    assert_eq!(
        archive_count, 1,
        "the old database should remain recoverable"
    );
}

#[tokio::test]
async fn opening_a_legacy_database_without_a_schema_marker_archives_it() {
    let (directory, store) = store().await;
    let path = directory.path().join("sessions.sqlite3");
    sqlx::query("drop table borg_session_schema")
        .execute(store.pool())
        .await
        .unwrap();
    store.pool().close().await;

    SqliteSessionStore::open(&path).await.unwrap();

    assert_eq!(
        std::fs::read_dir(directory.path())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("sessions.sqlite3.incompatible-")
            })
            .count(),
        1
    );
}

#[tokio::test]
async fn list_sessions_skips_state_for_removed_providers() {
    let (_directory, store) = store().await;
    let valid = Uuid::new_v4();
    let incompatible = Uuid::new_v4();
    store.create_session(valid).await.unwrap();
    store.create_session(incompatible).await.unwrap();
    sqlx::query("update sessions set state_json = ? where id = ?")
        .bind(r#"{"configuration":{"provider":"open_code"}}"#)
        .bind(incompatible.to_string())
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
    assert!(store.contains_session(incompatible).await.unwrap());
}

#[tokio::test]
async fn provider_capability_snapshot_is_durable_metadata_not_context() {
    let (_directory, store) = store().await;
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
}

#[tokio::test]
async fn sessions_have_stable_workspace_bindings_and_children_inherit_the_team_workspace() {
    let (directory, store) = store().await;
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

    let child = Uuid::new_v4();
    let child_journal = directory
        .path()
        .join("subagents")
        .join(format!("{child}.lock"));
    let _writer = crate::SessionWriterLease::acquire(&child_journal).unwrap();
    store.register_child_session(root, child).await.unwrap();
    let child_binding = store.workspace_binding(child).await.unwrap().unwrap();
    assert_eq!(child_binding.workspace_id, root);
    assert_eq!(child_binding.participant_id, child);
}

#[tokio::test]
async fn new_session_can_start_in_a_selected_workspace_without_becoming_rebindable() {
    let (_directory, store) = store().await;
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
}

#[tokio::test]
async fn session_writes_wait_for_short_cross_connection_contention() {
    let (_directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    let blocker = store.begin_write().await.unwrap();
    let contender = store.clone();
    let append = tokio::spawn(async move {
        contender
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::SessionStarted,
            ))
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!append.is_finished());
    blocker.commit().await.unwrap();

    let event = tokio::time::timeout(Duration::from_secs(1), append)
        .await
        .expect("contending append should resume")
        .expect("append task should not panic")
        .expect("append should succeed after lock release");
    assert_eq!(event.sequence, 1);
}

fn configured(directory: &Path) -> SessionEventKind {
    SessionEventKind::SessionConfigured {
        cwd: directory.to_path_buf(),
        provider: CodingProvider::Codex,
        model: Some("gpt-test".to_string()),
        effort: Some("high".to_string()),
        fast: false,
        response_language: ResponseLanguage::Auto,
        permission_mode: PermissionMode::FullAccess,
    }
}

fn message(message_id: Uuid, text: &str) -> SessionEventKind {
    SessionEventKind::Message {
        message_id,
        actor: EventActor::User,
        text: text.to_string(),
        attachments: Vec::new(),
        status: MessageStatus::Complete,
        delivery: Some(PromptDelivery::Steer),
    }
}

#[tokio::test]
async fn prompt_event_boundaries_drive_one_atomic_action_lifecycle() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::SessionStarted,
        ))
        .await
        .unwrap();
    store
        .append(SessionEvent::new(
            session_id,
            0,
            configured(directory.path()),
        ))
        .await
        .unwrap();
    let message_id = Uuid::new_v4();
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: "do the work".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Queued,
                delivery: Some(PromptDelivery::Queue),
            },
        ))
        .await
        .unwrap();
    assert_eq!(
        store
            .action(session_id, message_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        SessionActionState::Admitted
    );
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::TurnStarted {
                message_id,
                provider: CodingProvider::Codex,
                model: Some("gpt-test".to_string()),
                effort: Some("high".to_string()),
                fast: false,
            },
        ))
        .await
        .unwrap();
    assert_eq!(
        store
            .action(session_id, message_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        SessionActionState::Running
    );
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::TurnCompleted {
                message_id,
                provider_session_id: Some("provider-session".to_string()),
                final_text: "done".to_string(),
                error: None,
            },
        ))
        .await
        .unwrap();
    let completed = store.action(session_id, message_id).await.unwrap().unwrap();
    assert_eq!(completed.state, SessionActionState::Completed);
    assert!(completed.accepted_at.is_some());
    assert!(completed.delivered_at.is_some());
    assert!(completed.completed_at.is_some());
    let transitions = store
        .action_transitions(session_id, message_id)
        .await
        .unwrap();
    assert_eq!(
        transitions
            .iter()
            .map(|transition| (transition.from, transition.to))
            .collect::<Vec<_>>(),
        [
            (None, SessionActionState::Queued),
            (
                Some(SessionActionState::Queued),
                SessionActionState::Admitted
            ),
            (
                Some(SessionActionState::Admitted),
                SessionActionState::Delivered
            ),
            (
                Some(SessionActionState::Delivered),
                SessionActionState::Preparing
            ),
            (
                Some(SessionActionState::Preparing),
                SessionActionState::Committing
            ),
            (
                Some(SessionActionState::Committing),
                SessionActionState::Running
            ),
            (
                Some(SessionActionState::Running),
                SessionActionState::Completed
            ),
        ]
    );
    assert!(
        store
            .pending_actions(session_id, 10)
            .await
            .unwrap()
            .is_empty()
    );

    // Re-admission with the same id/payload is an idempotent read of the
    // durable terminal action, not a duplicate action.
    let replay = SessionAction::new(
        message_id,
        session_id,
        completed.kind,
        completed.delivery,
        completed.wake,
        completed.payload.clone(),
    );
    assert_eq!(
        store.enqueue_action(replay).await.unwrap().state,
        completed.state
    );
}

#[tokio::test]
async fn prompt_admission_is_durable_visible_and_idempotent_before_routing() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }
    let admission = || {
        SessionEvent::new(
            session_id,
            0,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: "persist me before acknowledgement".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Queued,
                delivery: Some(PromptDelivery::Steer),
            },
        )
    };

    let first = store.admit_prompt(admission()).await.unwrap();
    let duplicate = store.admit_prompt(admission()).await.unwrap();

    assert_eq!(duplicate.id, first.id);
    assert_eq!(duplicate.sequence, first.sequence);
    assert_eq!(
        store
            .state(session_id)
            .await
            .unwrap()
            .latest_prompt
            .as_deref(),
        Some("persist me before acknowledgement")
    );
    assert_eq!(
        store
            .read(session_id)
            .await
            .unwrap()
            .into_iter()
            .filter(|event| {
                matches!(
                    event.kind,
                    SessionEventKind::Message {
                        message_id: stored_id,
                        ..
                    } if stored_id == message_id
                )
            })
            .count(),
        1
    );

    let mut conflicting = admission();
    let SessionEventKind::Message { text, .. } = &mut conflicting.kind else {
        unreachable!()
    };
    *text = "different content".to_string();
    assert!(store.admit_prompt(conflicting).await.is_err());
}

#[tokio::test]
async fn stale_in_progress_message_does_not_resurrect_terminal_action() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text: "original prompt".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Queued,
            delivery: Some(PromptDelivery::Queue),
        },
        SessionEventKind::TurnStarted {
            message_id,
            provider: CodingProvider::Codex,
            model: Some("gpt-test".to_string()),
            effort: Some("high".to_string()),
            fast: false,
        },
        SessionEventKind::TurnCompleted {
            message_id,
            provider_session_id: None,
            final_text: String::new(),
            error: Some("provider stopped".to_string()),
        },
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text: "coalesced stale snapshot".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: Some(PromptDelivery::Queue),
        },
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }

    let action = store.action(session_id, message_id).await.unwrap().unwrap();
    assert_eq!(action.state, SessionActionState::Failed);
    assert_eq!(action.payload["text"], "original prompt");
    assert!(
        !store
            .action_transitions(session_id, message_id)
            .await
            .unwrap()
            .iter()
            .any(|transition| {
                transition.from == Some(SessionActionState::Failed)
                    && transition.to == SessionActionState::Queued
            })
    );
}

#[tokio::test]
async fn accepted_steer_queue_event_reopens_a_terminal_action_projection() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text: "accepted steer".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Queued,
            delivery: Some(PromptDelivery::Steer),
        },
        SessionEventKind::TurnStarted {
            message_id,
            provider: CodingProvider::Codex,
            model: Some("gpt-test".to_string()),
            effort: Some("high".to_string()),
            fast: false,
        },
        SessionEventKind::TurnCompleted {
            message_id,
            provider_session_id: None,
            final_text: String::new(),
            error: None,
        },
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text: "accepted steer".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Queued,
            delivery: Some(PromptDelivery::Queue),
        },
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }

    let action = store.action(session_id, message_id).await.unwrap().unwrap();
    assert_eq!(action.kind, crate::SessionActionKind::Prompt);
    assert_eq!(action.state, SessionActionState::Queued);
    assert!(
        store
            .pending_actions(session_id, 10)
            .await
            .unwrap()
            .iter()
            .any(|pending| {
                pending.action_id == message_id && pending.kind == crate::SessionActionKind::Prompt
            })
    );
}

#[tokio::test]
async fn internal_messages_do_not_reuse_child_action_identity() {
    let (directory, store) = store().await;
    let child_session_id = Uuid::new_v4();
    let parent_session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    for session_id in [child_session_id, parent_session_id] {
        store.create_session(session_id).await.unwrap();
        for kind in [
            SessionEventKind::SessionStarted,
            configured(directory.path()),
        ] {
            store
                .append(SessionEvent::new(session_id, 0, kind))
                .await
                .unwrap();
        }
    }
    store
        .append(SessionEvent::new(
            child_session_id,
            0,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: "team input".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Queued,
                delivery: Some(PromptDelivery::Queue),
            },
        ))
        .await
        .unwrap();

    store
        .append(SessionEvent::new(
            parent_session_id,
            0,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::System,
                text: "internal child report".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Queued,
                delivery: Some(PromptDelivery::Queue),
            },
        ))
        .await
        .unwrap();

    assert!(
        store
            .action(parent_session_id, message_id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn recovered_steer_accepts_a_coalesced_queue_snapshot() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text: "steer while active".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Queued,
            delivery: Some(PromptDelivery::Steer),
        },
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text: "earlier queued input\n\nsteer while active".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: Some(PromptDelivery::Queue),
        },
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }

    let action = store.action(session_id, message_id).await.unwrap().unwrap();
    assert_eq!(action.kind, crate::SessionActionKind::Prompt);
    assert_eq!(action.state, SessionActionState::Admitted);
    assert_eq!(
        action.payload["text"],
        "earlier queued input\n\nsteer while active"
    );
}

#[tokio::test]
async fn concurrent_claims_have_one_winner_and_same_owner_claim_is_idempotent() {
    let (_directory, store) = store().await;
    let session_id = Uuid::new_v4();
    let action_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    store
        .enqueue_action(SessionAction::new(
            action_id,
            session_id,
            crate::SessionActionKind::Prompt,
            crate::ActionDeliveryPolicy::NextTurnBoundary,
            crate::ActionWakePolicy::Immediate,
            serde_json::json!({"text": "claim once"}),
        ))
        .await
        .unwrap();

    let mut tasks = Vec::new();
    for worker_number in 0..8 {
        let contender = store.clone();
        tasks.push(tokio::spawn(async move {
            contender
                .claim_action(
                    session_id,
                    action_id,
                    &format!("worker-{worker_number}"),
                    Duration::from_secs(30),
                )
                .await
                .unwrap()
        }));
    }
    let mut winner = None;
    for task in tasks {
        if let Some(action) = task.await.unwrap() {
            assert!(winner.is_none(), "two workers claimed the action");
            winner = Some(action);
        }
    }
    let winner = winner.expect("one worker should claim the queued action");
    let replay = store
        .claim_action(
            session_id,
            action_id,
            winner.lease_owner.as_deref().unwrap(),
            Duration::from_secs(30),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replay.lease_token, winner.lease_token);
    assert_eq!(replay.lease_heartbeat_at, winner.lease_heartbeat_at);
    assert_eq!(
        store
            .action_transitions(session_id, action_id)
            .await
            .unwrap()
            .len(),
        1,
        "claiming must not create duplicate lifecycle transitions"
    );
}

#[tokio::test]
async fn expired_leases_requeue_once_and_fence_stale_workers() {
    let (_directory, store) = store().await;
    let session_id = Uuid::new_v4();
    let action_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    store
        .enqueue_action(SessionAction::new(
            action_id,
            session_id,
            crate::SessionActionKind::Prompt,
            crate::ActionDeliveryPolicy::NextTurnBoundary,
            crate::ActionWakePolicy::Immediate,
            serde_json::json!({"text": "recover me"}),
        ))
        .await
        .unwrap();
    let claimed = store
        .claim_action(session_id, action_id, "worker-a", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let token = claimed.lease_token.unwrap();
    let expired_at = claimed.lease_expires_at.unwrap() + chrono::Duration::milliseconds(1);
    store
        .transition_claimed_action(ClaimedActionTransition {
            session_id,
            action_id,
            lease_owner: "worker-a".to_string(),
            lease_token: token,
            expected: Some(SessionActionState::Queued),
            next: SessionActionState::Admitted,
            error: None,
        })
        .await
        .unwrap();
    store
        .transition_claimed_action(ClaimedActionTransition {
            session_id,
            action_id,
            lease_owner: "worker-a".to_string(),
            lease_token: token,
            expected: Some(SessionActionState::Admitted),
            next: SessionActionState::Delivered,
            error: None,
        })
        .await
        .unwrap();
    store
        .transition_claimed_action(ClaimedActionTransition {
            session_id,
            action_id,
            lease_owner: "worker-a".to_string(),
            lease_token: token,
            expected: Some(SessionActionState::Delivered),
            next: SessionActionState::Preparing,
            error: None,
        })
        .await
        .unwrap();
    store
        .transition_claimed_action(ClaimedActionTransition {
            session_id,
            action_id,
            lease_owner: "worker-a".to_string(),
            lease_token: token,
            expected: Some(SessionActionState::Preparing),
            next: SessionActionState::Committing,
            error: None,
        })
        .await
        .unwrap();
    let recovered = store
        .recover_expired_actions(session_id, expired_at, 10)
        .await
        .unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].state, SessionActionState::Queued);
    assert!(recovered[0].lease_owner.is_none());
    assert!(
        store
            .heartbeat_action(
                session_id,
                action_id,
                "worker-a",
                token,
                Duration::from_secs(30),
            )
            .await
            .is_err()
    );
    assert!(
        store
            .recover_expired_actions(session_id, Utc::now(), 10)
            .await
            .unwrap()
            .is_empty()
    );
    let reclaimed = store
        .claim_action(session_id, action_id, "worker-b", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_ne!(reclaimed.lease_token, Some(token));
    assert!(
        store
            .transition_claimed_action(ClaimedActionTransition {
                session_id,
                action_id,
                lease_owner: "worker-a".to_string(),
                lease_token: token,
                expected: Some(SessionActionState::Queued),
                next: SessionActionState::Admitted,
                error: None,
            },)
            .await
            .is_err()
    );
    let transitions = store
        .action_transitions(session_id, action_id)
        .await
        .unwrap();
    assert!(transitions.iter().any(|transition| {
        transition.from == Some(SessionActionState::Committing)
            && transition.to == SessionActionState::Queued
    }));
}

#[tokio::test]
async fn expired_action_recovery_never_moves_work_between_sessions() {
    let (_directory, store) = store().await;
    let resumed_session_id = Uuid::new_v4();
    let other_session_id = Uuid::new_v4();
    let other_action_id = Uuid::new_v4();
    store.create_session(resumed_session_id).await.unwrap();
    store.create_session(other_session_id).await.unwrap();
    store
        .enqueue_action(SessionAction::new(
            other_action_id,
            other_session_id,
            crate::SessionActionKind::Prompt,
            crate::ActionDeliveryPolicy::NextTurnBoundary,
            crate::ActionWakePolicy::Immediate,
            serde_json::json!({
                "message_id": other_action_id,
                "text": "belongs to the other session",
            }),
        ))
        .await
        .unwrap();

    let mut expected = SessionActionState::Queued;
    for next in [
        SessionActionState::Admitted,
        SessionActionState::Delivered,
        SessionActionState::Preparing,
        SessionActionState::Committing,
    ] {
        store
            .transition_action(
                other_session_id,
                other_action_id,
                Some(expected),
                next,
                None,
            )
            .await
            .unwrap();
        expected = next;
    }

    assert!(
        store
            .recover_expired_actions(resumed_session_id, Utc::now(), 10)
            .await
            .unwrap()
            .is_empty(),
        "resuming one session must not claim another session's abandoned prompt"
    );
    assert_eq!(
        store
            .action(other_session_id, other_action_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        SessionActionState::Committing
    );
    assert_eq!(
        store
            .recover_expired_actions(other_session_id, Utc::now(), 10)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn compaction_events_drive_one_replayable_action_lifecycle() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }
    let mut started = SessionEvent::new(
        session_id,
        0,
        SessionEventKind::ProviderEvent {
            provider: CodingProvider::OpenRouter,
            kind: "context_compaction".to_string(),
            payload: serde_json::json!({"status": "started"}),
        },
    );
    started = store.append(started).await.unwrap();
    assert_eq!(
        store
            .action(session_id, started.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        SessionActionState::Running
    );
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::OpenRouter,
                kind: "context_compaction".to_string(),
                payload: serde_json::json!({
                    "status": "completed",
                    "summary": "keep the durable decisions"
                }),
            },
        ))
        .await
        .unwrap();
    let action = store.action(session_id, started.id).await.unwrap().unwrap();
    assert_eq!(action.state, SessionActionState::Completed);
    assert_eq!(
        store
            .action_transitions(session_id, started.id)
            .await
            .unwrap()
            .last()
            .unwrap()
            .to,
        SessionActionState::Completed
    );
}

#[tokio::test]
async fn sqlite_store_appends_projects_and_reads_indexed_suffixes() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
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
}

#[tokio::test]
async fn recent_user_messages_are_bounded_ordered_and_ignore_non_recallable_prompts() {
    let (_directory, store) = store().await;
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
}

#[tokio::test]
async fn durable_store_health_requires_full_sync_wal_and_foreign_keys() {
    let (_directory, store) = store().await;
    let readiness = store.readiness().await.unwrap();
    assert_eq!(readiness.integrity, "not_checked");
    assert!(!readiness.integrity_checked);
    assert_eq!(
        readiness.journal_size_limit_bytes,
        i64::try_from(SQLITE_JOURNAL_SIZE_LIMIT_BYTES).unwrap()
    );
    assert!(readiness.is_ready());

    let health = store.health().await.unwrap();
    assert_eq!(health.integrity, "ok");
    assert!(health.integrity_checked);
    assert_eq!(health.journal_mode.to_ascii_lowercase(), "wal");
    assert!(health.synchronous >= 2);
    assert!(health.foreign_keys);
    assert!(health.is_ready());
}

#[tokio::test]
async fn context_clear_resets_provider_projection_and_recovery_prefix() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
        SessionEventKind::ProviderSessionLinked {
            provider_session_id: "old-provider-thread".to_string(),
            provider_turn_id: None,
            context_contract_version: None,
        },
        message(Uuid::new_v4(), "old context"),
        SessionEventKind::ContextWindowUpdated {
            context_tokens: 40_000,
            context_window_tokens: 100_000,
        },
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::User,
            text: "queued across context clear".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Queued,
            delivery: Some(PromptDelivery::Queue),
        },
        SessionEventKind::ContextCleared,
        message(Uuid::new_v4(), "new context"),
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }

    let state = store.state(session_id).await.unwrap();
    assert!(state.provider_session_id.is_none());
    assert_eq!(state.usage.context_tokens, Some(0));
    let recovery = store.recovery(session_id).await.unwrap();
    assert_eq!(recovery.context_events.len(), 2);
    assert!(matches!(
        recovery.context_events[0].kind,
        SessionEventKind::ContextCleared
    ));
    assert!(matches!(
        &recovery.context_events[1].kind,
        SessionEventKind::Message { text, .. } if text == "new context"
    ));
    assert!(recovery.queue_events.iter().any(|event| matches!(
        &event.kind,
        SessionEventKind::Message {
            text,
            status: MessageStatus::Queued,
            ..
        } if text == "queued across context clear"
    )));
}

#[tokio::test]
async fn compacted_recovery_keeps_the_unresolved_prompt_tail() {
    let (_directory, store) = store().await;
    let session_id = Uuid::new_v4();
    let summarized_id = Uuid::new_v4();
    let failed_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::TurnStarted {
            message_id: summarized_id,
            provider: CodingProvider::Codex,
            model: Some("gpt-test".to_string()),
            effort: None,
            fast: false,
        },
        message(summarized_id, "summarized old context"),
        SessionEventKind::TurnCompleted {
            message_id: summarized_id,
            provider_session_id: None,
            final_text: "old result".to_string(),
            error: None,
        },
        SessionEventKind::TurnStarted {
            message_id: failed_id,
            provider: CodingProvider::Codex,
            model: Some("gpt-test".to_string()),
            effort: None,
            fast: false,
        },
        SessionEventKind::TurnCompleted {
            message_id: failed_id,
            provider_session_id: None,
            final_text: String::new(),
            error: Some("provider failed".to_string()),
        },
        SessionEventKind::Message {
            message_id: failed_id,
            actor: EventActor::User,
            text: "preserve exact failed prompt".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Failed,
            delivery: Some(PromptDelivery::Queue),
        },
        SessionEventKind::ProviderEvent {
            provider: CodingProvider::Codex,
            kind: "context_compaction".to_string(),
            payload: serde_json::json!({
                "status": "completed",
                "summary": "summary of the old context",
            }),
        },
        message(Uuid::new_v4(), "new context after compaction"),
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }

    let recovery = store.recovery(session_id).await.unwrap();
    let context_text = recovery
        .context_events
        .iter()
        .filter_map(|event| match &event.kind {
            SessionEventKind::Message { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(!context_text.contains(&"summarized old context"));
    assert!(context_text.contains(&"preserve exact failed prompt"));
    assert!(context_text.contains(&"new context after compaction"));
    assert!(recovery.context_events.iter().any(|event| matches!(
        &event.kind,
        SessionEventKind::ProviderEvent { kind, payload, .. }
            if kind == "context_compaction"
                && payload.get("summary").and_then(serde_json::Value::as_str)
                    == Some("summary of the old context")
    )));
}

#[tokio::test]
async fn only_acknowledged_terminal_turns_remain_provider_resume_checkpoints() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    let completed_id = Uuid::new_v4();
    let failed_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
        SessionEventKind::TurnCompleted {
            message_id: completed_id,
            provider_session_id: Some("acknowledged-thread".to_string()),
            final_text: "done".to_string(),
            error: None,
        },
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }
    assert_eq!(
        store
            .state(session_id)
            .await
            .unwrap()
            .provider_session_id
            .as_deref(),
        Some("acknowledged-thread")
    );

    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::TurnCompleted {
                message_id: failed_id,
                provider_session_id: Some("stale-thread".to_string()),
                final_text: String::new(),
                error: Some("transport closed before a terminal frame".to_string()),
            },
        ))
        .await
        .unwrap();
    assert!(
        store
            .state(session_id)
            .await
            .unwrap()
            .provider_session_id
            .is_none()
    );
}

#[tokio::test]
async fn provider_checkpoint_recovery_keeps_only_the_unacknowledged_tail() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    let completed_id = Uuid::new_v4();
    let pending_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
        message(completed_id, &"old user context ".repeat(20_000)),
        SessionEventKind::TurnStarted {
            message_id: completed_id,
            provider: CodingProvider::Codex,
            model: Some("gpt-test".to_string()),
            effort: Some("high".to_string()),
            fast: false,
        },
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::Assistant,
            text: "old assistant context ".repeat(20_000),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
        SessionEventKind::ProviderSessionLinked {
            provider_session_id: "durable-thread".to_string(),
            provider_turn_id: Some("durable-turn".to_string()),
            context_contract_version: Some(crate::agent::PROVIDER_CONTEXT_CONTRACT_VERSION),
        },
        SessionEventKind::TurnCompleted {
            message_id: completed_id,
            provider_session_id: Some("durable-thread".to_string()),
            final_text: "done".to_string(),
            error: None,
        },
        SessionEventKind::Message {
            message_id: pending_id,
            actor: EventActor::User,
            text: "recover this exact tail".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Queued,
            delivery: Some(PromptDelivery::Queue),
        },
        SessionEventKind::TurnStarted {
            message_id: pending_id,
            provider: CodingProvider::Codex,
            model: Some("gpt-test".to_string()),
            effort: Some("high".to_string()),
            fast: false,
        },
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }

    let recovery = store
        .recovery_from_provider_checkpoint(session_id, "durable-thread")
        .await
        .unwrap()
        .expect("matching durable checkpoint");

    assert_eq!(recovery.queue_events.len(), 1);
    assert!(matches!(
        &recovery.queue_events[0].kind,
        SessionEventKind::Message { message_id, text, .. }
            if *message_id == pending_id && text == "recover this exact tail"
    ));
    assert!(recovery.context_events.iter().any(|event| matches!(
        event.kind,
        SessionEventKind::TurnCompleted { message_id, .. } if message_id == completed_id
    )));
    assert!(serde_json::to_vec(&recovery.context_events).unwrap().len() < 10_000);
}

#[tokio::test]
async fn state_projects_pending_approval_and_cumulative_usage() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
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
}

#[tokio::test]
async fn fork_records_lineage_without_copying_events() {
    let (directory, store) = store().await;
    let parent_id = Uuid::new_v4();
    let fork_id = Uuid::new_v4();
    let retained_message_id = Uuid::new_v4();
    let discarded_message_id = Uuid::new_v4();
    store.create_session(parent_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
        SessionEventKind::ProviderSessionLinked {
            provider_session_id: "provider-thread".to_string(),
            provider_turn_id: None,
            context_contract_version: None,
        },
        message(retained_message_id, "keep"),
        message(discarded_message_id, "discard"),
    ] {
        store
            .append(SessionEvent::new(parent_id, 0, kind))
            .await
            .unwrap();
    }

    let fork = store.fork_before(parent_id, fork_id, 5).await.unwrap();
    assert_eq!(fork.inherited_event_count, 3);
    let copied_rows: i64 =
        sqlx::query_scalar("select count(*) from session_events where session_id = ?")
            .bind(fork_id.to_string())
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(copied_rows, 0);

    let events = store.read(fork_id).await.unwrap();
    assert_eq!(events.len(), 3);
    assert!(events.iter().all(|event| event.session_id == fork_id));
    assert_eq!(
        events
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert!(
        events
            .iter()
            .all(|event| !matches!(event.kind, SessionEventKind::ProviderSessionLinked { .. }))
    );
    assert!(
        store
            .contains_message(fork_id, retained_message_id)
            .await
            .unwrap()
    );
    assert!(
        !store
            .contains_message(fork_id, discarded_message_id)
            .await
            .unwrap()
    );
    let state = store.state(fork_id).await.unwrap();
    assert_eq!(state.latest_sequence, 3);
    assert!(state.provider_session_id.is_none());
    let recovery = store.recovery(fork_id).await.unwrap();
    assert_eq!(recovery.context_events.len(), 1);
    assert_eq!(recovery.queue_events.len(), 1);
}

#[tokio::test]
async fn fork_projection_checkpoints_bound_state_amplification() {
    const LARGE_RESPONSE_BYTES: usize = 128 * 1024;
    const EVENT_COUNT: u64 = FORK_PROJECTION_CHECKPOINT_INTERVAL + 23;

    let (directory, store) = store().await;
    let parent_id = Uuid::new_v4();
    let fork_id = Uuid::new_v4();
    let goal = SessionGoal::new("retained sparse projection goal".to_string(), None);
    let response = "r".repeat(LARGE_RESPONSE_BYTES);
    store.create_session(parent_id).await.unwrap();
    let mut transaction = store.begin_write().await.unwrap();
    for sequence in 1..=EVENT_COUNT {
        let kind = match sequence {
            1 => SessionEventKind::SessionStarted,
            2 => configured(directory.path()),
            3 => message(Uuid::new_v4(), "retained prompt"),
            4 => SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::Assistant,
                text: response.clone(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
            value if value == FORK_PROJECTION_CHECKPOINT_INTERVAL + 7 => {
                SessionEventKind::GoalUpdated { goal: goal.clone() }
            }
            _ => SessionEventKind::Error {
                message: format!("sparse projection fixture {sequence}"),
            },
        };
        store
            .append_durable_in_transaction(&mut transaction, SessionEvent::new(parent_id, 0, kind))
            .await
            .unwrap();
    }
    transaction.commit().await.unwrap();

    let (checkpoint_count, checkpoint_bytes): (i64, i64) = sqlx::query_as(
        "select count(*), coalesce(sum(length(projection_json)), 0) \
         from session_events where session_id = ? and projection_json <> ''",
    )
    .bind(parent_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(checkpoint_count, 2);
    assert!(
        checkpoint_bytes < i64::try_from(LARGE_RESPONSE_BYTES * 3).unwrap(),
        "sparse fork projections retained {checkpoint_bytes} bytes"
    );
    let dense_projection_lower_bound =
        i64::try_from((EVENT_COUNT - 4) * u64::try_from(LARGE_RESPONSE_BYTES).unwrap()).unwrap();
    assert!(dense_projection_lower_bound > checkpoint_bytes * 100);
    eprintln!(
        "fork projection fixture: dense_lower_bound={dense_projection_lower_bound} bytes; sparse={checkpoint_bytes} bytes"
    );

    let fork = store
        .fork_before(parent_id, fork_id, EVENT_COUNT + 1)
        .await
        .unwrap();
    assert_eq!(fork.inherited_event_count, EVENT_COUNT);
    let state = store.state(fork_id).await.unwrap();
    assert_eq!(state.latest_sequence, EVENT_COUNT);
    assert_eq!(state.latest_response.as_deref(), Some(response.as_str()));
    assert_eq!(state.goal, Some(goal));
}

#[tokio::test]
async fn fork_starts_a_fresh_provider_context_without_losing_capacity() {
    let (directory, store) = store().await;
    let parent_id = Uuid::new_v4();
    let fork_id = Uuid::new_v4();
    store.create_session(parent_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
        message(Uuid::new_v4(), "retained context"),
        SessionEventKind::UsageUpdated {
            provider_duration_ms: 1,
            turn_id: None,
            provider_context_reused: None,
            input_tokens: 1,
            output_tokens: 1,
            cached_input_tokens: 0,
            cache_creation_input_tokens: 0,
            total_tokens: 2,
            cost_microusd: None,
            cost_basis: "unknown".to_string(),
            cost_usd: None,
            context_tokens: Some(95_000),
            context_window_tokens: Some(100_000),
        },
    ] {
        store
            .append(SessionEvent::new(parent_id, 0, kind))
            .await
            .unwrap();
    }

    let parent_generation = store.state(parent_id).await.unwrap().context_generation;
    store.fork_before(parent_id, fork_id, 5).await.unwrap();

    let child = store.state(fork_id).await.unwrap();
    assert!(child.provider_session_id.is_none());
    assert_eq!(child.usage.context_tokens, Some(0));
    assert_eq!(child.usage.context_window_tokens, Some(100_000));
    assert_eq!(child.context_generation, parent_generation + 1);
    assert_eq!(
        store
            .read(fork_id)
            .await
            .unwrap()
            .iter()
            .filter_map(|event| match &event.kind {
                SessionEventKind::Message { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec!["retained context"]
    );
}

#[tokio::test]
async fn latest_completed_compaction_is_projected_through_a_fork() {
    let (directory, store) = store().await;
    let parent_id = Uuid::new_v4();
    let fork_id = Uuid::new_v4();
    store.create_session(parent_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
        SessionEventKind::ProviderEvent {
            provider: CodingProvider::Codex,
            kind: "context_compaction".to_string(),
            payload: serde_json::json!({"status": "started"}),
        },
        SessionEventKind::ProviderEvent {
            provider: CodingProvider::Codex,
            kind: "context_compaction".to_string(),
            payload: serde_json::json!({
                "status": "completed",
                "summary": "retained checkpoint"
            }),
        },
    ] {
        store
            .append(SessionEvent::new(parent_id, 0, kind))
            .await
            .unwrap();
    }
    store.fork_before(parent_id, fork_id, 5).await.unwrap();

    let checkpoint = store
        .latest_completed_context_compaction(fork_id)
        .await
        .unwrap()
        .expect("inherited checkpoint");
    assert_eq!(checkpoint.session_id, fork_id);
    assert_eq!(checkpoint.sequence, 4);
    assert!(checkpoint.kind.is_completed_context_compaction());
    assert!(matches!(
        checkpoint.kind,
        SessionEventKind::ProviderEvent { payload, .. }
            if payload.get("summary").and_then(serde_json::Value::as_str)
                == Some("retained checkpoint")
    ));
}

#[test]
fn provider_native_compaction_is_not_a_durable_replay_boundary() {
    let kind = SessionEventKind::ProviderEvent {
        provider: CodingProvider::Codex,
        kind: "context_compaction".to_string(),
        payload: serde_json::json!({
            "status": "completed",
            "provider_context_preserved": true,
        }),
    };

    assert!(!kind.is_completed_context_compaction());
    assert_eq!(kind.persistence(), EventPersistence::Durable);
    assert!(!kind.is_context_relevant());
}

#[test]
fn provider_native_recovery_checkpoint_is_context_without_rotating_generation() {
    let kind = SessionEventKind::ProviderEvent {
        provider: CodingProvider::Codex,
        kind: "context_compaction".to_string(),
        payload: serde_json::json!({
            "status": "completed",
            "summary": "provider recovery summary",
            "provider_context_preserved": true,
            "provider_recovery_checkpoint": true,
        }),
    };

    assert!(!kind.is_completed_context_compaction());
    assert!(kind.is_completed_provider_recovery_checkpoint());
    assert_eq!(kind.persistence(), EventPersistence::Durable);
    assert!(kind.is_context_relevant());
}

#[tokio::test]
async fn inherited_event_pages_match_the_full_projection_across_lineage_boundaries() {
    let (directory, store) = store().await;
    let parent_id = Uuid::new_v4();
    let child_id = Uuid::new_v4();
    let grandchild_id = Uuid::new_v4();
    store.create_session(parent_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
        SessionEventKind::ProviderSessionLinked {
            provider_session_id: "must-not-fork".to_string(),
            provider_turn_id: None,
            context_contract_version: None,
        },
        message(Uuid::new_v4(), "parent-a"),
        message(Uuid::new_v4(), "parent-b"),
    ] {
        store
            .append(SessionEvent::new(parent_id, 0, kind))
            .await
            .unwrap();
    }
    store.fork_before(parent_id, child_id, 6).await.unwrap();
    for text in ["child-a", "child-b", "child-c", "child-d"] {
        store
            .append(SessionEvent::new(
                child_id,
                0,
                message(Uuid::new_v4(), text),
            ))
            .await
            .unwrap();
    }
    store.fork_before(child_id, grandchild_id, 8).await.unwrap();
    for text in ["grandchild-a", "grandchild-b"] {
        store
            .append(SessionEvent::new(
                grandchild_id,
                0,
                message(Uuid::new_v4(), text),
            ))
            .await
            .unwrap();
    }

    let full = store.read(grandchild_id).await.unwrap();
    for sequence in 0..=u64::try_from(full.len() + 1).unwrap() {
        for limit in [1, 2, 4, 100] {
            let expected = full
                .iter()
                .skip(usize::try_from(sequence).unwrap())
                .take(limit)
                .map(|event| serde_json::to_value(event).unwrap())
                .collect::<Vec<_>>();
            let actual = store
                .events_after(grandchild_id, sequence, limit)
                .await
                .unwrap()
                .iter()
                .map(|event| serde_json::to_value(event).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(actual, expected, "sequence={sequence}, limit={limit}");
        }
    }
}

/// A rewind cuts immediately before the admission of the prompt it targets.
/// That prompt's earlier queue entry sits below the cut, so inheriting it
/// would hand the fork a pending prompt and re-run exactly what the user
/// just discarded.
#[tokio::test]
async fn a_rewind_does_not_inherit_the_queue_entry_of_the_discarded_prompt() {
    let (directory, store) = store().await;
    let parent_id = Uuid::new_v4();
    let fork_id = Uuid::new_v4();
    let discarded_message_id = Uuid::new_v4();
    store.create_session(parent_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
        SessionEventKind::Message {
            message_id: discarded_message_id,
            actor: EventActor::User,
            text: "discard".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Queued,
            delivery: Some(PromptDelivery::Queue),
        },
        SessionEventKind::Message {
            message_id: discarded_message_id,
            actor: EventActor::User,
            text: "discard".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: Some(PromptDelivery::Queue),
        },
    ] {
        store
            .append(SessionEvent::new(parent_id, 0, kind))
            .await
            .unwrap();
    }

    // The UI rewinds to the admission at sequence 4; the queue entry it was
    // admitted from is at sequence 3, below the cut.
    store.fork_before(parent_id, fork_id, 4).await.unwrap();
    assert!(
        !store
            .contains_message(fork_id, discarded_message_id)
            .await
            .unwrap()
    );
    let recovery = store.recovery(fork_id).await.unwrap();
    assert!(
        recovery.queue_events.is_empty(),
        "the discarded prompt must not come back as pending work"
    );
    assert!(
        store
            .read(fork_id)
            .await
            .unwrap()
            .iter()
            .all(|event| !matches!(
                event.kind,
                SessionEventKind::Message {
                    status: MessageStatus::Queued,
                    ..
                }
            ))
    );
}

#[tokio::test]
async fn live_state_coalesces_without_consuming_durable_sequences() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
        SessionEventKind::StatusChanged {
            status: SessionStatus::Running,
            detail: None,
        },
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }
    for text in ["a", "a much longer snapshot"] {
        let event = store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::Message {
                    message_id,
                    actor: EventActor::Assistant,
                    text: text.to_string(),
                    attachments: Vec::new(),
                    status: MessageStatus::InProgress,
                    delivery: None,
                },
            ))
            .await
            .unwrap();
        assert_eq!(event.sequence, 0);
    }
    let first_reasoning = store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ReasoningDelta {
                text: "thinking ".to_string(),
            },
        ))
        .await
        .unwrap();
    let reasoning_id = first_reasoning.id;
    let reasoning_started_at = first_reasoning.created_at;
    assert!(matches!(
        first_reasoning.kind,
        SessionEventKind::ReasoningDelta { ref text } if text == "thinking "
    ));
    let second_reasoning = store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ReasoningDelta {
                text: "carefully".to_string(),
            },
        ))
        .await
        .unwrap();
    assert_eq!(second_reasoning.id, reasoning_id);
    assert_eq!(second_reasoning.created_at, reasoning_started_at);
    assert!(matches!(
        second_reasoning.kind,
        SessionEventKind::ReasoningDelta { ref text } if text == "thinking carefully"
    ));
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ReasoningDelta {
                text: "thinking carefully".to_string(),
            },
        ))
        .await
        .unwrap();
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "telemetry".to_string(),
                payload: serde_json::json!({"large": "discarded"}),
            },
        ))
        .await
        .unwrap();

    assert_eq!(store.read(session_id).await.unwrap().len(), 3);
    let live = store.live_events_after(session_id, 0).await.unwrap();
    assert_eq!(live.len(), 2);
    assert!(live.iter().any(|live| matches!(
        &live.event.kind,
        SessionEventKind::Message { text, .. } if text == "a much longer snapshot"
    )));
    assert!(live.iter().any(|live| matches!(
        &live.event.kind,
        SessionEventKind::ReasoningDelta { text } if text == "thinking carefully"
    )));

    let completed = store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::Assistant,
                text: "done".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        ))
        .await
        .unwrap();
    assert_eq!(completed.sequence, 4);
    assert!(
        store
            .live_events_after(session_id, 0)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn reasoning_boundaries_clear_the_snapshot_before_the_next_thought() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
        SessionEventKind::StatusChanged {
            status: SessionStatus::Running,
            detail: None,
        },
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }

    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ReasoningDelta {
                text: "previous thought".to_string(),
            },
        ))
        .await
        .unwrap();
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ReasoningCompleted,
        ))
        .await
        .unwrap();
    let next = store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ReasoningDelta {
                text: "next thought".to_string(),
            },
        ))
        .await
        .unwrap();
    assert!(matches!(
        next.kind,
        SessionEventKind::ReasoningDelta { ref text } if text == "next thought"
    ));

    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ToolStarted {
                tool_call_id: "tool-1".to_string(),
                name: "read_file".to_string(),
                input: serde_json::json!({"path": "src/lib.rs"}),
                input_ref: None,
            },
        ))
        .await
        .unwrap();
    let after_tool = store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ReasoningDelta {
                text: "thought after tool".to_string(),
            },
        ))
        .await
        .unwrap();
    assert!(matches!(
        after_tool.kind,
        SessionEventKind::ReasoningDelta { ref text } if text == "thought after tool"
    ));
}

#[tokio::test]
async fn terminal_boundaries_clear_all_turn_live_state_but_keep_context_window() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }

    let live_message = |message_id| SessionEventKind::Message {
        message_id,
        actor: EventActor::Assistant,
        text: "partial".to_string(),
        attachments: Vec::new(),
        status: MessageStatus::InProgress,
        delivery: None,
    };
    store
        .append(SessionEvent::new(
            session_id,
            0,
            live_message(Uuid::new_v4()),
        ))
        .await
        .unwrap();
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ReasoningDelta {
                text: "thinking".to_string(),
            },
        ))
        .await
        .unwrap();
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ContextWindowUpdated {
                context_tokens: 80,
                context_window_tokens: 100,
            },
        ))
        .await
        .unwrap();

    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::TurnCompleted {
                message_id: Uuid::new_v4(),
                provider_session_id: None,
                final_text: String::new(),
                error: Some("turn interrupted".to_string()),
            },
        ))
        .await
        .unwrap();
    let live = store.live_events_after(session_id, 0).await.unwrap();
    assert_eq!(live.len(), 1);
    assert!(matches!(
        live[0].event.kind,
        SessionEventKind::ContextWindowUpdated {
            context_tokens: 80,
            context_window_tokens: 100,
        }
    ));

    store
        .append(SessionEvent::new(
            session_id,
            0,
            live_message(Uuid::new_v4()),
        ))
        .await
        .unwrap();
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::StatusChanged {
                status: SessionStatus::Ready,
                detail: None,
            },
        ))
        .await
        .unwrap();
    let live = store.live_events_after(session_id, 0).await.unwrap();
    assert_eq!(live.len(), 1);
    assert!(matches!(
        live[0].event.kind,
        SessionEventKind::ContextWindowUpdated { .. }
    ));

    // A delayed coalesced event must not recreate turn state after the
    // session has become idle.
    store
        .append(SessionEvent::new(
            session_id,
            0,
            live_message(Uuid::new_v4()),
        ))
        .await
        .unwrap();
    let live = store.live_events_after(session_id, 0).await.unwrap();
    assert_eq!(live.len(), 1);
    assert!(matches!(
        live[0].event.kind,
        SessionEventKind::ContextWindowUpdated { .. }
    ));
}

#[tokio::test]
async fn reopening_repairs_turn_live_state_left_on_a_terminal_session() {
    let (directory, store) = store().await;
    let path = directory.path().join("sessions.sqlite3");
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::SessionStarted,
        ))
        .await
        .unwrap();
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::StatusChanged {
                status: SessionStatus::Ready,
                detail: None,
            },
        ))
        .await
        .unwrap();

    let message_id = Uuid::new_v4();
    let event = SessionEvent::new(
        session_id,
        0,
        SessionEventKind::Message {
            message_id,
            actor: EventActor::Assistant,
            text: "stale response".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: None,
        },
    );
    sqlx::query(
        "insert into session_live_state \
             (session_id, live_key, revision, event_json, updated_at) values (?, ?, ?, ?, ?)",
    )
    .bind(session_id.to_string())
    .bind(format!("message:{message_id}"))
    .bind(99_i64)
    .bind(serde_json::to_string(&event).unwrap())
    .bind(event.created_at.to_rfc3339())
    .execute(store.pool())
    .await
    .unwrap();
    drop(store);

    let reopened = SqliteSessionStore::open(&path).await.unwrap();
    assert!(
        reopened
            .live_events_after(session_id, 0)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn interactive_open_defers_terminal_live_state_repair_until_requested() {
    let (directory, store) = store().await;
    let path = directory.path().join("sessions.sqlite3");
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::StatusChanged {
                status: SessionStatus::Ready,
                detail: None,
            },
        ))
        .await
        .unwrap();

    let message_id = Uuid::new_v4();
    let event = SessionEvent::new(
        session_id,
        0,
        SessionEventKind::Message {
            message_id,
            actor: EventActor::Assistant,
            text: "stale response".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: None,
        },
    );
    sqlx::query(
        "insert into session_live_state \
         (session_id, live_key, revision, event_json, updated_at) values (?, ?, ?, ?, ?)",
    )
    .bind(session_id.to_string())
    .bind(format!("message:{message_id}"))
    .bind(99_i64)
    .bind(serde_json::to_string(&event).unwrap())
    .bind(event.created_at.to_rfc3339())
    .execute(store.pool())
    .await
    .unwrap();
    drop(store);

    let reopened = SqliteSessionStore::open_interactive(&path).await.unwrap();
    assert_eq!(
        reopened
            .live_events_after(session_id, 0)
            .await
            .unwrap()
            .len(),
        1
    );
    reopened.finish_interactive_open(session_id).await.unwrap();
    assert!(
        reopened
            .live_events_after(session_id, 0)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn interactive_open_adds_account_bindings_without_replacing_existing_sessions() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    // Reproduce the current database schema before the additive binding table.
    sqlx::query("drop table session_model_access")
        .execute(&store.pool)
        .await
        .unwrap();
    store.pool.close().await;
    let reopened = SqliteSessionStore::open_interactive(directory.path().join("sessions.sqlite3"))
        .await
        .unwrap();
    assert!(reopened.contains_session(session_id).await.unwrap());
    reopened
        .bind_model_access(session_id, CodingProvider::Codex, "account-a")
        .await
        .unwrap();
}

#[tokio::test]
async fn current_schema_interactive_open_does_not_wait_for_an_active_writer() {
    let (directory, store) = store().await;
    let path = directory.path().join("sessions.sqlite3");
    let mut writer = store.pool().acquire().await.unwrap();
    sqlx::query("begin immediate")
        .execute(&mut *writer)
        .await
        .unwrap();

    let reopened = tokio::time::timeout(
        Duration::from_millis(250),
        SqliteSessionStore::open_interactive(&path),
    )
    .await
    .expect("current-schema interactive open waited for the active writer")
    .unwrap();
    drop(reopened);

    sqlx::query("rollback").execute(&mut *writer).await.unwrap();
}

#[tokio::test]
async fn large_tool_payloads_are_loaded_only_by_reference() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    let input = serde_json::json!({"text": "x".repeat(INLINE_SESSION_PAYLOAD_BYTES)});
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
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
}

#[tokio::test]
async fn history_query_resolves_fts_regex_exact_and_full_payload_hits_to_canonical_events() {
    let (_directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    let message_event = store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::User,
                text: "compare the contractual remedy matrix".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        ))
        .await
        .unwrap();
    let payload_needle = "uniquepayload8675309";
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ToolCompleted {
                tool_call_id: "large-history-result".to_string(),
                output: format!(
                    "{} {}",
                    "x".repeat(INLINE_SESSION_PAYLOAD_BYTES + 1024),
                    payload_needle
                ),
                output_ref: None,
                is_error: false,
                input: None,
                input_ref: None,
            },
        ))
        .await
        .unwrap();
    let other_session = Uuid::new_v4();
    store.create_session(other_session).await.unwrap();
    store
        .append(SessionEvent::new(
            other_session,
            0,
            message(
                Uuid::new_v4(),
                "contractual remedy from another tenant scope",
            ),
        ))
        .await
        .unwrap();

    let lexical = store
        .query_history(
            session_id,
            SessionHistoryQuery {
                text: Some("contractual remedy".to_string()),
                actors: vec![EventActor::User],
                ..SessionHistoryQuery::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(lexical.backend, "sqlite_fts5");
    assert_eq!(lexical.hits.len(), 1);
    assert_eq!(lexical.hits[0].event.id, message_event.id);

    let payload_hit = store
        .query_history(
            session_id,
            SessionHistoryQuery {
                text: Some(payload_needle.to_string()),
                expand_payloads: true,
                max_payload_bytes: Some(INLINE_SESSION_PAYLOAD_BYTES * 2),
                ..SessionHistoryQuery::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(payload_hit.hits.len(), 1);
    assert!(matches!(
        payload_hit.hits[0].event.kind,
        SessionEventKind::ToolCompleted {
            output_ref: Some(_),
            ..
        }
    ));
    assert!(
        payload_hit.hits[0].payloads[0]
            .text
            .ends_with(payload_needle)
    );
    assert!(!payload_hit.hits[0].payloads[0].truncated);

    let index_documents = store
        .history_index_documents_after(session_id, message_event.sequence, 10)
        .await
        .unwrap();
    assert_eq!(index_documents.len(), 1);
    assert!(index_documents[0].content.contains(payload_needle));
    assert_eq!(index_documents[0].event_id, payload_hit.hits[0].event.id);
    assert!(
        index_documents[0]
            .document_id
            .starts_with("borg-session-event:v1:")
    );

    let regex = store
        .query_history(
            session_id,
            SessionHistoryQuery {
                text: Some("PAYLOAD[0-9]{7}".to_string()),
                mode: SessionHistorySearchMode::Regex,
                case_sensitive: false,
                ..SessionHistoryQuery::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(regex.backend, "sqlite_regex");
    assert_eq!(regex.hits.len(), 1);

    let exact = store
        .query_history(
            session_id,
            SessionHistoryQuery {
                event_id: Some(message_event.id),
                ..SessionHistoryQuery::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(exact.backend, "sqlite_exact");
    assert_eq!(exact.hits.len(), 1);
    assert_eq!(exact.hits[0].event.sequence, message_event.sequence);
}

#[tokio::test]
async fn history_query_preserves_projected_ids_and_sequences_across_forks() {
    let (directory, store) = store().await;
    let parent_id = Uuid::new_v4();
    let fork_id = Uuid::new_v4();
    store.create_session(parent_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
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
    assert_eq!(inherited.backend, "lineage_scan");
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
}

#[tokio::test]
async fn opening_rebuilds_missing_history_projection_from_the_lossless_journal() {
    let (directory, store) = store().await;
    let path = directory.path().join("sessions.sqlite3");
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    store
        .append(SessionEvent::new(
            session_id,
            0,
            message(Uuid::new_v4(), "rebuildable projection evidence"),
        ))
        .await
        .unwrap();
    sqlx::query("delete from session_event_search where session_id=?")
        .bind(session_id.to_string())
        .execute(store.pool())
        .await
        .unwrap();
    drop(store);

    let reopened = SqliteSessionStore::open(path).await.unwrap();
    let result = reopened
        .query_history(
            session_id,
            SessionHistoryQuery {
                text: Some("projection evidence".to_string()),
                ..SessionHistoryQuery::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(result.hits.len(), 1);
    assert_eq!(result.backend, "sqlite_fts5");
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

#[tokio::test]
#[ignore = "explicit large-session p95 performance gate"]
async fn large_session_lineage_and_tail_p95_gates() {
    const EVENT_COUNT: u64 = 38_272;
    const SAMPLES: usize = 100;

    let (directory, store) = store().await;
    let parent_id = Uuid::new_v4();
    store.create_session(parent_id).await.unwrap();
    let mut state = SessionState::default();
    let mut transaction = store.pool().begin().await.unwrap();
    for sequence in 1..=EVENT_COUNT {
        let kind = match sequence {
            1 => SessionEventKind::SessionStarted,
            2 => configured(directory.path()),
            _ => SessionEventKind::Error {
                message: "bounded performance fixture".to_string(),
            },
        };
        let event = SessionEvent::new(parent_id, sequence, kind);
        state.apply(&event).unwrap();
        let projection_json = serde_json::to_string(&state).unwrap();
        sqlx::query(
            "insert into session_events \
                 (session_id, sequence, event_id, event_kind, event_json, projection_json, \
                  fork_inheritable, recovery_relevant, message_id, created_at) \
                 values (?, ?, ?, ?, ?, ?, ?, ?, null, ?)",
        )
        .bind(parent_id.to_string())
        .bind(i64::try_from(sequence).unwrap())
        .bind(event.id.to_string())
        .bind(event_kind(&event.kind).unwrap())
        .bind(serde_json::to_string(&event).unwrap())
        .bind(historical_projection_json(sequence, 0, &projection_json))
        .bind(i64::from(event.kind.is_fork_inheritable()))
        .bind(i64::from(event.kind.is_recovery_relevant()))
        .bind(event.created_at.to_rfc3339())
        .execute(&mut *transaction)
        .await
        .unwrap();
    }
    sqlx::query(
        "update sessions set next_sequence = ?, state_json = ?, updated_at = ? where id = ?",
    )
    .bind(i64::try_from(EVENT_COUNT + 1).unwrap())
    .bind(serde_json::to_string(&state).unwrap())
    .bind(Utc::now().to_rfc3339())
    .bind(parent_id.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    transaction.commit().await.unwrap();

    let mut fork_samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = Instant::now();
        store
            .fork_before(parent_id, Uuid::new_v4(), EVENT_COUNT + 1)
            .await
            .unwrap();
        fork_samples.push(started.elapsed());
    }
    let mut tail_samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = Instant::now();
        let tail = store
            .events_after(parent_id, EVENT_COUNT - 100, 100)
            .await
            .unwrap();
        assert_eq!(tail.len(), 100);
        tail_samples.push(started.elapsed());
    }
    let fork_p95 = duration_p95(&mut fork_samples);
    let tail_p95 = duration_p95(&mut tail_samples);
    eprintln!("lineage fork p95: {fork_p95:?}; indexed tail p95: {tail_p95:?}");
    assert!(
        fork_p95 < Duration::from_millis(200),
        "lineage fork p95 exceeded 200 ms: {fork_p95:?}"
    );
    assert!(
        tail_p95 < Duration::from_millis(50),
        "indexed tail p95 exceeded 50 ms: {tail_p95:?}"
    );
}

#[tokio::test]
#[ignore = "explicit large-session prompt-recall p95 performance gate"]
async fn large_session_recent_prompt_recall_p95_gate() {
    const EVENT_COUNT: u64 = 25_000;
    const LIMIT: usize = 100;
    const SAMPLES: usize = 100;

    let (_directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    let mut state = SessionState::default();
    let mut transaction = store.pool().begin().await.unwrap();
    for sequence in 1..=EVENT_COUNT {
        let message_id = Uuid::new_v4();
        let kind = SessionEventKind::Message {
            message_id,
            actor: if sequence % 2 == 0 {
                EventActor::User
            } else {
                EventActor::Assistant
            },
            text: format!("bounded prompt recall fixture {sequence}"),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        };
        let event = SessionEvent::new(session_id, sequence, kind);
        state.apply(&event).unwrap();
        insert_performance_profile_event(&mut transaction, &event, &state).await;
    }
    sqlx::query("update sessions set next_sequence=?, state_json=?, updated_at=? where id=?")
        .bind(i64::try_from(EVENT_COUNT + 1).unwrap())
        .bind(serde_json::to_string(&state).unwrap())
        .bind(Utc::now().to_rfc3339())
        .bind(session_id.to_string())
        .execute(&mut *transaction)
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = Instant::now();
        let prompts = store.recent_user_messages(session_id, LIMIT).await.unwrap();
        assert_eq!(prompts.len(), LIMIT);
        assert_eq!(prompts.last().unwrap().sequence, EVENT_COUNT);
        samples.push(started.elapsed());
    }
    let p95 = duration_p95(&mut samples);
    eprintln!("25k-message recent prompt recall p95: {p95:?}");
    assert!(
        p95 < Duration::from_millis(10),
        "recent prompt recall p95 exceeded 10 ms: {p95:?}"
    );
}

#[tokio::test]
#[ignore = "explicit sparse-message prompt-recall p95 performance gate"]
async fn sparse_recent_prompt_recall_p95_gate() {
    const EVENT_COUNT: u64 = 25_000;
    const LIMIT: usize = 100;
    const SAMPLES: usize = 100;

    let (_directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    let mut state = SessionState::default();
    let mut transaction = store.pool().begin().await.unwrap();
    for sequence in 1..=EVENT_COUNT {
        let kind = if sequence % 250 == 0 {
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::User,
                text: format!("sparse prompt {sequence}"),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            }
        } else {
            SessionEventKind::Error {
                message: format!("non-message event {sequence}"),
            }
        };
        let event = SessionEvent::new(session_id, sequence, kind);
        state.apply(&event).unwrap();
        insert_performance_profile_event(&mut transaction, &event, &state).await;
    }
    sqlx::query("update sessions set next_sequence=?, state_json=?, updated_at=? where id=?")
        .bind(i64::try_from(EVENT_COUNT + 1).unwrap())
        .bind(serde_json::to_string(&state).unwrap())
        .bind(Utc::now().to_rfc3339())
        .bind(session_id.to_string())
        .execute(&mut *transaction)
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = Instant::now();
        let prompts = store.recent_user_messages(session_id, LIMIT).await.unwrap();
        assert_eq!(prompts.len(), LIMIT);
        samples.push(started.elapsed());
    }
    let p95 = duration_p95(&mut samples);
    eprintln!("25k-event sparse prompt recall p95: {p95:?}");
    assert!(
        p95 < Duration::from_millis(10),
        "sparse recent prompt recall p95 exceeded 10 ms: {p95:?}"
    );
}

const RECOVERY_PROFILE_OBSOLETE_EVENTS: u64 = 25_000;
const RECOVERY_PROFILE_RETAINED_EVENTS: u64 = 100;

async fn recovery_profile_store(
    boundary_kind: SessionEventKind,
) -> (tempfile::TempDir, SqliteSessionStore, Uuid) {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    let mut state = SessionState::default();
    let mut transaction = store.pool().begin().await.unwrap();
    let completed_message_id = Uuid::new_v4();
    for sequence in 1..=RECOVERY_PROFILE_OBSOLETE_EVENTS {
        let kind = match sequence {
            value if value == RECOVERY_PROFILE_OBSOLETE_EVENTS - 1 => {
                SessionEventKind::TurnStarted {
                    message_id: completed_message_id,
                    provider: CodingProvider::Codex,
                    model: Some("gpt-profile".to_string()),
                    effort: None,
                    fast: false,
                }
            }
            value if value == RECOVERY_PROFILE_OBSOLETE_EVENTS => SessionEventKind::TurnCompleted {
                message_id: completed_message_id,
                provider_session_id: None,
                final_text: String::new(),
                error: None,
            },
            _ => SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::Assistant,
                text: format!("obsolete recovery context {sequence}"),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        };
        let event = SessionEvent::new(session_id, sequence, kind);
        state.apply(&event).unwrap();
        insert_performance_profile_event(&mut transaction, &event, &state).await;
    }
    let boundary_sequence = RECOVERY_PROFILE_OBSOLETE_EVENTS + 1;
    let boundary = SessionEvent::new(session_id, boundary_sequence, boundary_kind);
    state.apply(&boundary).unwrap();
    insert_performance_profile_event(&mut transaction, &boundary, &state).await;
    for sequence in (boundary_sequence + 1)..=(boundary_sequence + RECOVERY_PROFILE_RETAINED_EVENTS)
    {
        let event = SessionEvent::new(
            session_id,
            sequence,
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::Assistant,
                text: format!("retained recovery context {sequence}"),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        );
        state.apply(&event).unwrap();
        insert_performance_profile_event(&mut transaction, &event, &state).await;
    }
    let next_sequence = boundary_sequence + RECOVERY_PROFILE_RETAINED_EVENTS + 1;
    sqlx::query("update sessions set next_sequence=?, state_json=?, updated_at=? where id=?")
        .bind(i64::try_from(next_sequence).unwrap())
        .bind(serde_json::to_string(&state).unwrap())
        .bind(Utc::now().to_rfc3339())
        .bind(session_id.to_string())
        .execute(&mut *transaction)
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    (directory, store, session_id)
}

#[tokio::test]
#[ignore = "explicit cleared-context recovery p95 performance gate"]
async fn large_cleared_context_recovery_p95_gate() {
    const SAMPLES: usize = 20;
    let (_directory, store, session_id) =
        recovery_profile_store(SessionEventKind::ContextCleared).await;

    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = Instant::now();
        let recovery = store.recovery(session_id).await.unwrap();
        assert_eq!(
            recovery.context_events.len(),
            usize::try_from(RECOVERY_PROFILE_RETAINED_EVENTS + 1).unwrap()
        );
        assert!(matches!(
            recovery.context_events.first().unwrap().kind,
            SessionEventKind::ContextCleared
        ));
        assert!(recovery.queue_events.is_empty());
        samples.push(started.elapsed());
    }
    let p95 = duration_p95(&mut samples);
    eprintln!("25k-event cleared-context recovery p95: {p95:?}");
    assert!(
        p95 < Duration::from_millis(10),
        "cleared-context recovery p95 exceeded 10 ms: {p95:?}"
    );
}

#[tokio::test]
#[ignore = "explicit compacted-context recovery p95 performance gate"]
async fn large_compacted_context_recovery_p95_gate() {
    const SAMPLES: usize = 20;
    let (_directory, store, session_id) = recovery_profile_store(SessionEventKind::ProviderEvent {
        provider: CodingProvider::Codex,
        kind: "context_compaction".to_string(),
        payload: serde_json::json!({
            "status": "completed",
            "summary": "retained recovery summary",
        }),
    })
    .await;

    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = Instant::now();
        let recovery = store.recovery(session_id).await.unwrap();
        assert_eq!(
            recovery.context_events.len(),
            usize::try_from(RECOVERY_PROFILE_RETAINED_EVENTS + 1).unwrap()
        );
        assert!(matches!(
            recovery.context_events.first().unwrap().kind,
            SessionEventKind::ProviderEvent { ref kind, .. } if kind == "context_compaction"
        ));
        assert!(recovery.queue_events.is_empty());
        samples.push(started.elapsed());
    }
    let p95 = duration_p95(&mut samples);
    eprintln!("25k-event compacted-context recovery p95: {p95:?}");
    assert!(
        p95 < Duration::from_millis(10),
        "compacted-context recovery p95 exceeded 10 ms: {p95:?}"
    );
}

async fn insert_performance_profile_event(
    transaction: &mut Transaction<'_, Sqlite>,
    event: &SessionEvent,
    state: &SessionState,
) {
    let message_id = match &event.kind {
        SessionEventKind::Message { message_id, .. } => Some(message_id.to_string()),
        _ => None,
    };
    let projection_json = serde_json::to_string(state).unwrap();
    sqlx::query(
        "insert into session_events \
         (session_id, sequence, event_id, event_kind, event_json, projection_json, \
          fork_inheritable, recovery_relevant, message_id, created_at) \
         values (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
    )
    .bind(event.session_id.to_string())
    .bind(i64::try_from(event.sequence).unwrap())
    .bind(event.id.to_string())
    .bind(event_kind(&event.kind).unwrap())
    .bind(serde_json::to_string(event).unwrap())
    .bind(historical_projection_json(
        event.sequence,
        0,
        &projection_json,
    ))
    .bind(i64::from(event.kind.is_fork_inheritable()))
    .bind(message_id)
    .bind(event.created_at.to_rfc3339())
    .execute(&mut **transaction)
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "explicit lossless-history retrieval p95 performance gate"]
async fn large_session_history_query_p95_gate() {
    const EVENT_COUNT: u64 = 25_000;
    const SAMPLES: usize = 100;
    const NEEDLE: &str = "rare-history-needle-8675309";

    let (_directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    let mut state = SessionState::default();
    let mut transaction = store.pool().begin().await.unwrap();
    for sequence in 1..=EVENT_COUNT {
        let kind = SessionEventKind::Error {
            message: if sequence == EVENT_COUNT - 17 {
                NEEDLE.to_string()
            } else {
                format!("ordinary bounded history fixture {sequence}")
            },
        };
        let event = SessionEvent::new(session_id, sequence, kind);
        state.apply(&event).unwrap();
        let event_json = serde_json::to_string(&event).unwrap();
        let stored_kind = event_kind(&event.kind).unwrap();
        let projection_json = serde_json::to_string(&state).unwrap();
        sqlx::query(
            "insert into session_events \
             (session_id, sequence, event_id, event_kind, event_json, projection_json, \
              fork_inheritable, recovery_relevant, message_id, created_at) \
             values (?, ?, ?, ?, ?, ?, ?, ?, null, ?)",
        )
        .bind(session_id.to_string())
        .bind(i64::try_from(sequence).unwrap())
        .bind(event.id.to_string())
        .bind(&stored_kind)
        .bind(&event_json)
        .bind(historical_projection_json(sequence, 0, &projection_json))
        .bind(i64::from(event.kind.is_fork_inheritable()))
        .bind(i64::from(event.kind.is_recovery_relevant()))
        .bind(event.created_at.to_rfc3339())
        .execute(&mut *transaction)
        .await
        .unwrap();
        sqlx::query(
            "insert into session_event_search \
             (session_id, sequence, event_id, event_kind, actor, body) \
             values (?, ?, ?, ?, null, ?)",
        )
        .bind(session_id.to_string())
        .bind(i64::try_from(sequence).unwrap())
        .bind(event.id.to_string())
        .bind(stored_kind)
        .bind(event_json)
        .execute(&mut *transaction)
        .await
        .unwrap();
    }
    sqlx::query("update sessions set next_sequence=?, state_json=?, updated_at=? where id=?")
        .bind(i64::try_from(EVENT_COUNT + 1).unwrap())
        .bind(serde_json::to_string(&state).unwrap())
        .bind(Utc::now().to_rfc3339())
        .bind(session_id.to_string())
        .execute(&mut *transaction)
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    let query = SessionHistoryQuery {
        text: Some(NEEDLE.to_string()),
        ..SessionHistoryQuery::default()
    };
    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = Instant::now();
        let page = store
            .query_history(session_id, query.clone())
            .await
            .unwrap();
        assert_eq!(page.hits.len(), 1);
        samples.push(started.elapsed());
    }
    let p95 = duration_p95(&mut samples);
    eprintln!("25k-event canonical FTS query p95: {p95:?}");
    assert!(
        p95 < Duration::from_millis(10),
        "history FTS query p95 exceeded 10 ms: {p95:?}"
    );

    let regex_query = SessionHistoryQuery {
        text: Some("rare-history-needle-[0-9]+".to_string()),
        mode: SessionHistorySearchMode::Regex,
        prefilter: Some("rare history needle".to_string()),
        scan_limit: Some(EVENT_COUNT as usize),
        ..SessionHistoryQuery::default()
    };
    let mut regex_samples = Vec::with_capacity(10);
    for _ in 0..10 {
        let started = Instant::now();
        let page = store
            .query_history(session_id, regex_query.clone())
            .await
            .unwrap();
        assert_eq!(page.hits.len(), 1);
        regex_samples.push(started.elapsed());
    }
    let regex_p95 = duration_p95(&mut regex_samples);
    eprintln!("25k-event bounded regex query p95: {regex_p95:?}");
    assert!(
        regex_p95 < Duration::from_millis(10),
        "history regex query p95 exceeded 10 ms: {regex_p95:?}"
    );
}

#[tokio::test]
#[ignore = "explicit first-search projection catch-up performance gate"]
async fn first_history_search_after_sustained_appends_profile() {
    const EVENT_COUNT: u64 = 2_000;
    const NEEDLE: &str = "first-search-catchup-needle";

    let (_directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    let append_started = Instant::now();
    for sequence in 1..=EVENT_COUNT {
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::Error {
                    message: if sequence == EVENT_COUNT {
                        NEEDLE.to_string()
                    } else {
                        format!("ordinary first-search fixture {sequence}")
                    },
                },
            ))
            .await
            .unwrap();
    }
    let append_elapsed = append_started.elapsed();
    let other_session = Uuid::new_v4();
    store.create_session(other_session).await.unwrap();
    let store = Arc::new(store);
    let search_store = Arc::clone(&store);
    let search = tokio::spawn(async move {
        let search_started = Instant::now();
        let page = search_store
            .query_history(
                session_id,
                SessionHistoryQuery {
                    text: Some(NEEDLE.to_string()),
                    ..SessionHistoryQuery::default()
                },
            )
            .await
            .unwrap();
        (page, search_started.elapsed())
    });
    tokio::time::sleep(Duration::from_millis(5)).await;
    let concurrent_append_started = Instant::now();
    store
        .append(SessionEvent::new(
            other_session,
            0,
            SessionEventKind::Error {
                message: "concurrent append".to_string(),
            },
        ))
        .await
        .unwrap();
    let concurrent_append_elapsed = concurrent_append_started.elapsed();
    let (page, first_search_elapsed) = search.await.unwrap();
    assert_eq!(page.hits.len(), 1);
    assert!(
        concurrent_append_elapsed < Duration::from_millis(25),
        "history projection catch-up blocked another session append for {concurrent_append_elapsed:?}"
    );
    eprintln!(
        "2k durable appends: {append_elapsed:?}; first FTS search: {first_search_elapsed:?}; concurrent append: {concurrent_append_elapsed:?}"
    );
}

fn duration_p95(samples: &mut [Duration]) -> Duration {
    samples.sort_unstable();
    samples[(samples.len() * 95).div_ceil(100).saturating_sub(1)]
}
