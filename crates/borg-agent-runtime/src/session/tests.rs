use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::json;
use tempfile::tempdir;
use tokio::sync::Notify;

use borg_provider::ProviderCallUsage;

use super::*;

#[test]
fn fresh_replay_reattaches_images_the_model_never_finished_answering() {
    let dir = tempdir().unwrap();
    let image = |name: &str| {
        let path = dir.path().join(name);
        std::fs::write(&path, b"png").unwrap();
        path
    };
    let (answered, steered, current) = (image("a.png"), image("b.png"), image("c.png"));
    let session_id = Uuid::new_v4();
    let prompt = |attachments: Vec<PathBuf>| {
        SessionEvent::new(
            session_id,
            0,
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::User,
                text: "look [Image 1]".to_string(),
                attachments,
                status: MessageStatus::Complete,
                delivery: None,
            },
        )
    };
    let completed = |error: Option<&str>| {
        SessionEvent::new(
            session_id,
            0,
            SessionEventKind::TurnCompleted {
                message_id: Uuid::new_v4(),
                provider_session_id: None,
                final_text: String::new(),
                error: error.map(str::to_string),
            },
        )
    };
    let events = [
        prompt(vec![answered]),
        completed(None),
        prompt(vec![steered.clone(), dir.path().join("deleted.png")]),
        completed(Some("turn interrupted")),
        prompt(vec![current.clone()]),
    ];
    assert_eq!(
        unanswered_prompt_attachments(&events, std::slice::from_ref(&current)),
        [steered]
    );
}

#[tokio::test]
async fn claude_to_native_replay_preserves_available_images_and_marks_omissions() {
    use borg_provider::provider::ModelMessage;

    let dir = tempdir().unwrap();
    let image = dir.path().join("available.png");
    std::fs::write(&image, b"png").unwrap();
    let unsupported = dir.path().join("unsupported.txt");
    std::fs::write(&unsupported, b"text").unwrap();
    let oversized = dir.path().join("oversized.png");
    std::fs::File::create(&oversized)
        .unwrap()
        .set_len(26 * 1024 * 1024)
        .unwrap();
    let attachments = vec![
        image,
        dir.path().join("missing.png"),
        unsupported,
        oversized,
    ];
    let session_id = Uuid::new_v4();
    let claude_turn = Uuid::new_v4();
    let events = vec![
        SessionEvent::new(
            session_id,
            1,
            SessionEventKind::Message {
                message_id: claude_turn,
                actor: EventActor::User,
                text: "inspect these images".to_string(),
                attachments: attachments.clone(),
                status: MessageStatus::InProgress,
                delivery: None,
            },
        ),
        SessionEvent::new(
            session_id,
            2,
            SessionEventKind::TurnStarted {
                message_id: claude_turn,
                provider: CodingProvider::Claude,
                model: Some("claude-opus-5-5".to_string()),
                effort: None,
                fast: false,
            },
        ),
        SessionEvent::new(
            session_id,
            3,
            SessionEventKind::Message {
                message_id: claude_turn,
                actor: EventActor::User,
                text: "inspect these images".to_string(),
                attachments,
                status: MessageStatus::Complete,
                delivery: None,
            },
        ),
        SessionEvent::new(
            session_id,
            4,
            SessionEventKind::TurnCompleted {
                message_id: claude_turn,
                provider_session_id: None,
                final_text: String::new(),
                error: None,
            },
        ),
        SessionEvent::new(
            session_id,
            5,
            SessionEventKind::TurnStarted {
                message_id: Uuid::new_v4(),
                provider: CodingProvider::Codex,
                model: Some("gpt-6-sol".to_string()),
                effort: None,
                fast: false,
            },
        ),
    ];

    let replay =
        native_conversation_with_historical_images(&events, CodingProvider::Codex, dir.path())
            .await
            .unwrap();
    assert_eq!(replay.len(), 1);
    match &replay[0] {
        ModelMessage::User {
            content,
            attachments,
        } => {
            assert!(content.contains("[3 historical images not replayed]"));
            assert_eq!(attachments.len(), 1);
            assert_eq!(attachments[0].media_type, "image/png");
            assert_eq!(attachments[0].data_base64, "cG5n");
        }
        other => panic!("expected replayed user image, got {other:?}"),
    }
}

#[tokio::test]
async fn failed_compaction_start_keeps_native_history_and_claude_images() {
    use borg_provider::provider::ModelMessage;

    let dir = tempdir().unwrap();
    let image = dir.path().join("screenshot.png");
    std::fs::write(&image, b"png").unwrap();
    let session_id = Uuid::new_v4();
    let claude_turn = Uuid::new_v4();
    let native_turn = Uuid::new_v4();
    let event = |sequence, kind| SessionEvent::new(session_id, sequence, kind);
    let events = vec![
        event(
            1,
            SessionEventKind::Message {
                message_id: claude_turn,
                actor: EventActor::User,
                text: "inspect screenshot".into(),
                attachments: vec![image],
                status: MessageStatus::InProgress,
                delivery: None,
            },
        ),
        event(
            2,
            SessionEventKind::TurnStarted {
                message_id: claude_turn,
                provider: CodingProvider::Claude,
                model: None,
                effort: None,
                fast: false,
            },
        ),
        event(
            3,
            SessionEventKind::TurnCompleted {
                message_id: claude_turn,
                provider_session_id: None,
                final_text: String::new(),
                error: None,
            },
        ),
        event(
            4,
            SessionEventKind::TurnStarted {
                message_id: native_turn,
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: false,
            },
        ),
        event(
            5,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "native_model_message".into(),
                payload: serde_json::to_value(ModelMessage::user("native follow-up")).unwrap(),
            },
        ),
        event(
            6,
            SessionEventKind::TurnCompleted {
                message_id: native_turn,
                provider_session_id: None,
                final_text: String::new(),
                error: None,
            },
        ),
        event(
            7,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "context_compaction".into(),
                payload: json!({"status": "started", "summary": "Compacting context…"}),
            },
        ),
        event(
            8,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "context_compaction_failed".into(),
                payload: json!({"error": "provider unavailable"}),
            },
        ),
    ];

    let replay =
        native_conversation_with_historical_images(&events, CodingProvider::Codex, dir.path())
            .await
            .unwrap();
    assert_eq!(replay.len(), 2);
    match &replay[0] {
        ModelMessage::User {
            content,
            attachments,
        } => {
            assert_eq!(content, "inspect screenshot");
            assert_eq!(attachments.len(), 1);
            assert_eq!(attachments[0].data_base64, "cG5n");
        }
        other => panic!("expected Claude image prompt, got {other:?}"),
    }
    assert!(
        matches!(&replay[1], ModelMessage::User { content, .. } if content == "native follow-up")
    );
}

#[tokio::test]
async fn failed_claude_turn_keeps_recent_images_in_its_interruption_record() {
    use borg_provider::provider::ModelMessage;

    let dir = tempdir().unwrap();
    let images = (0..5)
        .map(|index| {
            let path = dir.path().join(format!("image-{index}.png"));
            std::fs::write(&path, [b'0' + index]).unwrap();
            path
        })
        .collect::<Vec<_>>();
    let session_id = Uuid::new_v4();
    let claude_turn = Uuid::new_v4();
    let steer_id = Uuid::new_v4();
    let message = |sequence, message_id, text: &str, attachments| {
        SessionEvent::new(
            session_id,
            sequence,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: text.to_string(),
                attachments,
                status: MessageStatus::InProgress,
                delivery: None,
            },
        )
    };
    let events = vec![
        message(1, claude_turn, "inspect originals", images[..4].to_vec()),
        SessionEvent::new(
            session_id,
            2,
            SessionEventKind::TurnStarted {
                message_id: claude_turn,
                provider: CodingProvider::Claude,
                model: None,
                effort: None,
                fast: false,
            },
        ),
        message(3, steer_id, "compare latest", images[4..].to_vec()),
        SessionEvent::new(
            session_id,
            4,
            SessionEventKind::TurnCompleted {
                message_id: claude_turn,
                provider_session_id: None,
                final_text: String::new(),
                error: Some("connection closed before message completed".to_string()),
            },
        ),
        SessionEvent::new(
            session_id,
            5,
            SessionEventKind::TurnStarted {
                message_id: Uuid::new_v4(),
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: false,
            },
        ),
    ];

    let replay =
        native_conversation_with_historical_images(&events, CodingProvider::Codex, dir.path())
            .await
            .unwrap();
    assert_eq!(replay.len(), 1);
    match &replay[0] {
        ModelMessage::User {
            content,
            attachments,
        } => {
            assert!(content.contains("completed actions must not be repeated"));
            assert!(content.contains("inspect originals"));
            assert!(content.contains("compare latest"));
            assert!(content.contains("[1 historical image not replayed]"));
            assert_eq!(attachments.len(), 4);
            assert_eq!(attachments[0].filename.as_deref(), Some("image-1.png"));
            assert_eq!(attachments[3].filename.as_deref(), Some("image-4.png"));
        }
        other => panic!("expected interrupted user record, got {other:?}"),
    }
}

#[test]
fn prompt_title_has_a_durable_fallback_for_attachment_only_messages() {
    assert_eq!(prompt_session_title("  \n"), "New conversation");
    assert_eq!(
        prompt_session_title("  **Plan this work**\nMore detail"),
        "Plan this work"
    );
}

#[cfg(feature = "subscription-adapters")]
#[test]
fn luna_title_requires_explicit_cross_provider_opt_in_and_subscription_billing() {
    let mut launch: LaunchSession = serde_json::from_value(json!({
        "request_id": Uuid::new_v4(),
        "cwd": "/tmp",
        "provider": "claude",
        "model": null,
        "effort": null,
        "permission_mode": "auto",
        "name": null,
        "initial_prompt": null
    }))
    .unwrap();
    assert!(!launch.capabilities.luna_titles_for_all_providers);
    launch.capabilities.provider_capabilities = vec![
        serde_json::from_value(json!({
            "provider": "codex",
            "installed": true,
            "version": null,
            "authenticated": true,
            "auth_detail": null,
            "can_spawn": true,
            "billing": "subscription"
        }))
        .unwrap(),
    ];
    assert!(!eligible_luna_title(&launch, false));
    launch.capabilities.luna_titles_for_all_providers = true;
    assert!(eligible_luna_title(&launch, false));
    assert!(
        !eligible_luna_title(&launch, true),
        "API keys cannot bill title turns"
    );
    launch.capabilities.provider_capabilities[0].billing = Some(crate::BillingLane::ApiKey);
    assert!(!eligible_luna_title(&launch, false));
    launch.capabilities.provider_capabilities[0].billing = Some(crate::BillingLane::Subscription);
    launch.capabilities.provider_capabilities[0].can_spawn = false;
    assert!(!eligible_luna_title(&launch, false));
    launch.capabilities.provider_capabilities[0].can_spawn = true;
    launch.capabilities.runtime_provider_context = Some(crate::RuntimeProviderContext {
        persist_session: Some(false),
        ..Default::default()
    });
    assert!(
        !eligible_luna_title(&launch, false),
        "controller access cannot use host credentials"
    );
    launch.capabilities.runtime_provider_context = None;
    launch.capabilities.luna_titles_for_all_providers = false;
    launch.provider = CodingProvider::Codex;
    assert!(
        eligible_luna_title(&launch, false),
        "Codex subscription is eligible by default"
    );
}
use crate::{
    AgentCompaction, AgentTurnResult, CodingProvider, LocalAgentTurnExecutor, PermissionMode,
    PostgresSessionStore,
};

/// Run the canonical session actor against a caller-owned Postgres store.
///
/// The path-based entry points are gone with the file-backed store: a filesystem
/// path no longer implies a store. Tests therefore own the store, and this
/// mirrors what the removed wrapper did around the writer lease so the call
/// sites keep reading as "run the actor over this journal".
async fn run_session_actor(
    lock_path: &std::path::Path,
    session_id: Uuid,
    launch: LaunchSession,
    commands: mpsc::Receiver<HostCommand>,
    events: mpsc::Sender<SessionEvent>,
    executor: Arc<dyn AgentTurnExecutor>,
    store: Arc<dyn SessionStore>,
) -> anyhow::Result<()> {
    let writer = SessionWriterLease::acquire(lock_path)?;
    let session_root = lock_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    run_agent_session_with_store_and_writer(
        session_root,
        session_id,
        launch,
        commands,
        events,
        executor,
        store,
        writer,
    )
    .await
}

type RecordedTurns = Arc<Mutex<Vec<(PathBuf, Option<serde_json::Value>)>>>;
type RecordedPromptTurns = Arc<Mutex<Vec<(String, Vec<PathBuf>)>>>;
type RecordedContextTurns = Arc<Mutex<Vec<(String, Option<String>)>>>;
type RecordedDurableResumeTurns = Arc<Mutex<Vec<(String, Option<String>, Option<String>, usize)>>>;
type RecordedProviderTurns =
    Arc<Mutex<Vec<(CodingProvider, Option<String>, Option<String>, String)>>>;
type RecordedCompactionTurns = Arc<Mutex<Vec<(CodingProvider, Option<String>, String)>>>;
type SeenConsultProvider = Arc<Mutex<Vec<(CodingProvider, Option<String>, String)>>>;

#[tokio::test]
async fn aborting_session_cancels_its_provider_turn_and_action_heartbeat() {
    struct PendingExecutor {
        started: Arc<Notify>,
        dropped: Arc<Notify>,
    }
    struct NotifyDrop(Arc<Notify>);
    impl Drop for NotifyDrop {
        fn drop(&mut self) {
            self.0.notify_one();
        }
    }
    #[async_trait::async_trait]
    impl AgentTurnExecutor for PendingExecutor {
        async fn execute(
            &self,
            _turn: AgentTurn,
            _events: mpsc::Sender<SessionEventKind>,
            _controls: Option<mpsc::Receiver<AgentTurnControl>>,
        ) -> Result<AgentTurnResult> {
            let _lifetime = NotifyDrop(Arc::clone(&self.dropped));
            self.started.notify_one();
            std::future::pending().await
        }
    }
    let root = tempdir().unwrap();
    let root_path = root.path().to_path_buf();
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    let (scratch, postgres) = crate::session_store::postgres::testing::session_store().await;
    let postgres = Arc::new(postgres);
    let store: Arc<dyn SessionStore> = postgres.clone();
    let writer =
        SessionWriterLease::acquire(root.path().join(format!("{session_id}.lock"))).unwrap();
    let (_commands, command_rx) = mpsc::channel(8);
    let (events, _event_rx) = mpsc::channel(128);
    let started = Arc::new(Notify::new());
    let dropped = Arc::new(Notify::new());
    let executor = Arc::new(PendingExecutor {
        started: Arc::clone(&started),
        dropped: Arc::clone(&dropped),
    });
    let actor = tokio::spawn(async move {
        run_agent_session_with_store_and_writer(
            &root_path,
            session_id,
            LaunchSession {
                request_id: message_id,
                cwd: root_path.clone(),
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: Some(false),
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
                name: None,
                initial_prompt: Some("wait until cancelled".to_string()),
                capabilities: Default::default(),
                subagent_concurrency_limit: None,
                extension_skill_roots: vec![],
                team_policy: None,
            },
            command_rx,
            events,
            executor,
            store,
            writer,
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), started.notified())
        .await
        .unwrap();
    let before = postgres
        .action(session_id, message_id)
        .await
        .unwrap()
        .unwrap();
    assert!(before.lease_token.is_some());
    actor.abort();
    assert!(actor.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(1), dropped.notified())
        .await
        .expect("aborting the actor must also drop its pending provider execution");
    assert!(
        SessionWriterLease::try_acquire(root.path().join(format!("{session_id}.lock")))
            .unwrap()
            .is_some()
    );
    tokio::time::sleep(Duration::from_secs(16)).await;
    let after = postgres
        .action(session_id, message_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after.lease_heartbeat_at, before.lease_heartbeat_at,
        "an abandoned actor must not keep renewing its action lease"
    );
    let recovered = postgres
        .recover_expired_actions(session_id, Utc::now() + chrono::Duration::seconds(120), 8)
        .await
        .unwrap();
    assert!(
        recovered
            .iter()
            .any(|action| action.action_id == message_id)
    );
    scratch.discard().await;
}

#[tokio::test]
async fn session_generation_waits_on_silence_and_resumes_without_exposing_fragment_pulses() {
    struct FragmentExecutor {
        resume: Arc<Notify>,
        finish: Arc<Notify>,
    }
    #[async_trait::async_trait]
    impl AgentTurnExecutor for FragmentExecutor {
        async fn execute(
            &self,
            turn: AgentTurn,
            events: mpsc::Sender<SessionEventKind>,
            _controls: Option<mpsc::Receiver<AgentTurnControl>>,
        ) -> Result<AgentTurnResult> {
            events
                .send(SessionEventKind::ProviderEvent {
                    provider: turn.provider,
                    kind: "action/preparing".into(),
                    payload: json!({"tool_call_id": "call", "label": "read file"}),
                })
                .await?;
            tokio::time::sleep(Duration::from_secs(3)).await;
            events
                .send(SessionEventKind::ReasoningDelta {
                    text: "unrelated output".into(),
                })
                .await?;
            self.resume.notified().await;
            events
                .send(SessionEventKind::ProviderEvent {
                    provider: turn.provider,
                    kind: "action/input_delta".into(),
                    payload: json!({"tool_call_id": "call"}),
                })
                .await?;
            self.finish.notified().await;
            events
                .send(SessionEventKind::ToolStarted {
                    tool_call_id: "call".into(),
                    name: "read_file".into(),
                    input: json!({}),
                    input_ref: None,
                })
                .await?;
            Ok(AgentTurnResult {
                provider_session_id: None,
                final_text: "done".into(),
            })
        }
    }
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(128);
    let resume = Arc::new(Notify::new());
    let finish = Arc::new(Notify::new());
    let executor = Arc::new(FragmentExecutor {
        resume: resume.clone(),
        finish: finish.clone(),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn(async move {
        run_session_actor(
            &root.path().join("session.lock"),
            session_id,
            LaunchSession {
                request_id: Uuid::new_v4(),
                cwd: root.path().to_path_buf(),
                provider: CodingProvider::Claude,
                model: None,
                effort: None,
                fast: Some(false),
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
                name: None,
                initial_prompt: Some("test generation".into()),
                capabilities: Default::default(),
                subagent_concurrency_limit: None,
                extension_skill_roots: Vec::new(),
                team_policy: None,
            },
            command_rx,
            event_tx,
            executor,
            actor_store,
        )
        .await
    });
    let mut statuses = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(event) = event_rx.recv().await {
            match event.kind {
                SessionEventKind::ProviderEvent { kind, payload, .. } => {
                    assert_ne!(
                        kind, "action/input_delta",
                        "fragment pulses are internal, not timeline rows"
                    );
                    if kind == "action/generation_status" {
                        assert_eq!(payload["label"], "read file");
                        let waiting = payload["waiting"].as_bool().unwrap();
                        statuses.push(waiting);
                        if waiting {
                            resume.notify_one();
                        } else {
                            finish.notify_one();
                        }
                    }
                }
                SessionEventKind::TurnCompleted { error, .. } => {
                    assert!(error.is_none());
                    break;
                }
                _ => {}
            }
        }
    })
    .await
    .expect("session must publish waiting, immediate resumption and completion");
    assert_eq!(statuses, [true, false]);
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), actor)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    scratch.discard().await;
}

fn subscription_prompt_ends_with(prompt: &str, text: &str) -> bool {
    let frame = format_subscription_frame(&format_subscription_actor_value(EventActor::User, text));
    prompt.ends_with(&frame)
}

#[tokio::test]
async fn generation_boundary_waits_for_a_busy_live_projection() {
    let session_id = Uuid::new_v4();
    let (events, mut receiver) = mpsc::channel(1);
    events
        .send(SessionEvent::new(
            session_id,
            1,
            SessionEventKind::ReasoningCompleted,
        ))
        .await
        .unwrap();
    let boundary = SessionEvent::new(
        session_id,
        0,
        SessionEventKind::ProviderEvent {
            provider: CodingProvider::Codex,
            kind: "action/preparing".to_string(),
            payload: serde_json::json!({"label": "", "tool_call_id": null}),
        },
    );
    let delivery = tokio::spawn(async move {
        deliver_recorded_event(
            &events,
            session_id,
            boundary,
            crate::EventPersistence::Ephemeral,
        )
        .await;
    });

    tokio::task::yield_now().await;
    assert!(matches!(
        receiver.recv().await.unwrap().kind,
        SessionEventKind::ReasoningCompleted
    ));
    delivery.await.unwrap();
    assert!(matches!(
        receiver.recv().await.unwrap().kind,
        SessionEventKind::ProviderEvent { ref kind, .. } if kind == "action/preparing"
    ));
}

#[test]
fn provider_event_batch_coalescing_preserves_text_and_boundaries() {
    let first_message_id = Uuid::new_v4();
    let second_message_id = Uuid::new_v4();
    let mut batch = Vec::new();
    for kind in [
        SessionEventKind::ReasoningDelta {
            text: "hel".to_string(),
        },
        SessionEventKind::ReasoningDelta {
            text: "lo".to_string(),
        },
        SessionEventKind::ReasoningDelta {
            text: "hello world".to_string(),
        },
        SessionEventKind::Message {
            message_id: first_message_id,
            actor: EventActor::Assistant,
            text: "draft".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: None,
        },
        SessionEventKind::Message {
            message_id: first_message_id,
            actor: EventActor::Assistant,
            text: "finished draft".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: None,
        },
        SessionEventKind::StatusChanged {
            status: SessionStatus::Running,
            detail: Some("durable boundary".to_string()),
        },
        SessionEventKind::Message {
            message_id: second_message_id,
            actor: EventActor::Assistant,
            text: "next message".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: None,
        },
    ] {
        push_coalesced_provider_event(&mut batch, kind);
    }

    assert_eq!(batch.len(), 4);
    assert!(matches!(
        &batch[0],
        SessionEventKind::ReasoningDelta { text } if text == "hello world"
    ));
    assert!(matches!(
        &batch[1],
        SessionEventKind::Message { message_id, text, .. }
            if *message_id == first_message_id && text == "finished draft"
    ));
    assert!(matches!(batch[2], SessionEventKind::StatusChanged { .. }));
    assert!(matches!(
        &batch[3],
        SessionEventKind::Message { message_id, .. } if *message_id == second_message_id
    ));
}

async fn runtime_store(
    session_id: Uuid,
) -> (
    crate::session_store::postgres::testing::ScratchDatabase,
    Arc<dyn SessionStore>,
    RuntimeSessionStore,
) {
    let (scratch, postgres) = crate::session_store::postgres::testing::session_store().await;
    let postgres = Arc::new(postgres);
    postgres.create_session(session_id).await.unwrap();
    let store: Arc<dyn SessionStore> = postgres;
    let runtime = RuntimeSessionStore::new(Arc::clone(&store), Vec::new(), true);
    (scratch, store, runtime)
}

#[tokio::test]
async fn accepted_steers_settle_in_fifo_order_when_acknowledgements_arrive_out_of_order() {
    let session_id = Uuid::new_v4();
    let (scratch, _store, mut journal) = runtime_store(session_id).await;
    let (events, mut event_rx) = mpsc::channel(8);
    let first_id = Uuid::new_v4();
    let second_id = Uuid::new_v4();
    let steer = |message_id| {
        let admission = SteerAdmission::pending();
        assert!(admission.accept());
        PendingSteer {
            prompt: QueuedPrompt {
                message_id,
                text: message_id.to_string(),
                actor: EventActor::User,
                attachments: Vec::new(),
                output_schema: None,
                delivery: PromptDelivery::Steer,
                visible: true,
                interrupt_batch: true,
                batch: Vec::new(),
            },
            admission,
            state: PendingSteerState::AwaitingAcknowledgement,
            acknowledgement_id: Uuid::new_v4(),
            attempt_boundary: 0,
        }
    };
    let mut pending_steers = VecDeque::from([steer(first_id), steer(second_id)]);

    pending_steers[1].state = PendingSteerState::Accepted;
    settle_accepted_steers(
        &mut journal,
        &events,
        session_id,
        &mut pending_steers,
        false,
        &mut HashMap::new(),
    )
    .await
    .unwrap();
    assert!(event_rx.try_recv().is_err());
    assert_eq!(pending_steers.len(), 2);

    pending_steers[0].state = PendingSteerState::Accepted;
    settle_accepted_steers(
        &mut journal,
        &events,
        session_id,
        &mut pending_steers,
        false,
        &mut HashMap::new(),
    )
    .await
    .unwrap();

    let mut settled = Vec::new();
    while let Ok(event) = event_rx.try_recv() {
        if let SessionEventKind::Message {
            message_id, status, ..
        } = event.kind
        {
            settled.push((message_id, status));
        }
    }
    assert_eq!(
        settled,
        [
            (first_id, MessageStatus::InProgress),
            (first_id, MessageStatus::Complete),
            (second_id, MessageStatus::InProgress),
            (second_id, MessageStatus::Complete),
        ]
    );
    assert!(pending_steers.is_empty());
    scratch.discard().await;
}

#[test]
fn structured_claude_result_terminations_classify_without_prose() {
    for status in [500, 502, 503, 504, 529] {
        assert!(provider_error_is_transient_api_failure(&format!(
            "openrouter request failed with HTTP {status}: Provider returned error"
        )));
    }
    for status in [400, 401, 403, 404, 429, 5020] {
        assert!(!provider_error_is_transient_api_failure(&format!(
            "openrouter request failed with HTTP {status}: Provider returned error"
        )));
    }

    for status in [500, 502, 503, 504, 529] {
        assert!(is_safe_automatic_retry_error(&format!(
            "Codex subscription response did not complete. HTTP {status}. No billing fallback was attempted; no tools from this response were executed."
        )));
    }
    for status in [400, 401, 403, 404, 429] {
        assert!(!is_safe_automatic_retry_error(&format!(
            "Codex subscription response did not complete. HTTP {status}. No billing fallback was attempted; no tools from this response were executed."
        )));
    }

    assert!(is_safe_automatic_retry_error(
        r#"claude SDK error_during_execution: upstream failed "terminal_reason":"api_error" "status":529"#
    ));
    assert!(is_safe_automatic_retry_error(
        r#"claude SDK error_during_execution: gateway "terminal_reason":"api_error""#
    ));
    assert!(!is_safe_automatic_retry_error(
        r#"claude SDK error_during_execution: bad request "terminal_reason":"api_error" "status":400"#
    ));
    assert!(provider_error_is_usage_limited(
        r#"claude SDK error_during_execution: slow down "terminal_reason":"api_error" "status":429"#
    ));
    assert!(!is_safe_automatic_retry_error(
        "claude SDK error_max_turns: stopped"
    ));
}

#[tokio::test]
async fn claude_steers_stay_pending_input_until_the_cli_reports_consumption() {
    let session_id = Uuid::new_v4();
    let (scratch, _store, mut journal) = runtime_store(session_id).await;
    let (events, mut event_rx) = mpsc::channel(16);
    let consumed_id = Uuid::new_v4();
    let leftover_id = Uuid::new_v4();
    let steer = |message_id| {
        let admission = SteerAdmission::pending();
        assert!(admission.accept());
        PendingSteer {
            prompt: QueuedPrompt {
                message_id,
                text: message_id.to_string(),
                actor: EventActor::User,
                attachments: Vec::new(),
                output_schema: None,
                delivery: PromptDelivery::Steer,
                visible: true,
                interrupt_batch: true,
                batch: Vec::new(),
            },
            admission,
            state: PendingSteerState::Accepted,
            acknowledgement_id: Uuid::new_v4(),
            attempt_boundary: 0,
        }
    };
    let mut pending_steers = VecDeque::from([steer(consumed_id), steer(leftover_id)]);
    let mut awaiting = HashMap::new();
    settle_accepted_steers(
        &mut journal,
        &events,
        session_id,
        &mut pending_steers,
        true,
        &mut awaiting,
    )
    .await
    .unwrap();
    assert_eq!(awaiting.len(), 2, "deferred steers wait for consumption");

    // Unrelated lifecycle traffic (the original prompt's own command) is ignored.
    let lifecycle = |state: &str, message_id: Option<Uuid>| {
        let mut payload = serde_json::json!({"type": "command_lifecycle", "state": state});
        if let Some(message_id) = message_id {
            payload["client_user_message_id"] = serde_json::json!(message_id.to_string());
        }
        SessionEventKind::ProviderEvent {
            provider: CodingProvider::Claude,
            kind: "claude.command_lifecycle".to_string(),
            payload,
        }
    };
    complete_consumed_steers(
        &lifecycle("started", None),
        &mut awaiting,
        &mut journal,
        &events,
        session_id,
    )
    .await
    .unwrap();
    complete_consumed_steers(
        &lifecycle("queued", Some(consumed_id)),
        &mut awaiting,
        &mut journal,
        &events,
        session_id,
    )
    .await
    .unwrap();
    assert_eq!(awaiting.len(), 2);
    complete_consumed_steers(
        &lifecycle("started", Some(consumed_id)),
        &mut awaiting,
        &mut journal,
        &events,
        session_id,
    )
    .await
    .unwrap();
    assert_eq!(awaiting.len(), 1);
    flush_awaiting_steers(&mut awaiting, false, &mut journal, &events, session_id)
        .await
        .unwrap();
    assert!(awaiting.is_empty());

    let mut settled = Vec::new();
    while let Ok(event) = event_rx.try_recv() {
        if let SessionEventKind::Message {
            message_id, status, ..
        } = event.kind
        {
            settled.push((message_id, status));
        }
    }
    assert_eq!(
        settled,
        [
            (consumed_id, MessageStatus::InProgress),
            (leftover_id, MessageStatus::InProgress),
            (consumed_id, MessageStatus::Complete),
            (leftover_id, MessageStatus::Complete),
        ]
    );
    scratch.discard().await;
}

#[tokio::test]
async fn durable_session_events_project_once_into_the_bound_workspace() {
    let session_id = Uuid::new_v4();
    let (scratch, session_store) = crate::session_store::postgres::testing::session_store().await;
    let session_store = Arc::new(session_store);
    session_store.create_session(session_id).await.unwrap();
    let binding = session_store
        .workspace_binding(session_id)
        .await
        .unwrap()
        .unwrap();
    let workspace_store = session_store.workspace_store().await.unwrap().unwrap();
    let human_id = crate::local_human_participant_id("Human");
    workspace_store
        .ensure_execution_workspace(
            binding.workspace_id,
            "test workspace",
            human_id,
            "Human",
            binding.participant_id,
            "Agent",
        )
        .await
        .unwrap();
    let projection = WorkspaceProjection::new(
        workspace_store.clone(),
        binding.workspace_id,
        binding.participant_id,
        human_id,
        0,
        0,
    );
    let store: Arc<dyn SessionStore> = session_store.clone();
    let mut runtime = RuntimeSessionStore::new(store.clone(), Vec::new(), true)
        .with_workspace_projection(projection.clone());
    let message_id = Uuid::new_v4();
    workspace_store
        .append(WorkspaceEvent {
            id: message_id,
            workspace_id: binding.workspace_id,
            sequence: 0,
            author_id: human_id,
            idempotency_key: format!("test-team-message:{message_id}"),
            created_at: chrono::Utc::now(),
            kind: WorkspaceEventKind::Message {
                message: crate::WorkspaceMessage {
                    id: message_id,
                    workspace_id: binding.workspace_id,
                    thread_id: None,
                    reply_to_message_id: None,
                    author_id: human_id,
                    body: crate::WorkspaceMessageBody {
                        text: "coordinate this".to_string(),
                        mentions: Vec::new(),
                        attachments: Vec::new(),
                    },
                    audience: crate::Audience::Direct {
                        participant: binding.participant_id,
                    },
                    created_at: chrono::Utc::now(),
                },
                mode: crate::DeliveryMode::Boundary,
            },
        })
        .await
        .unwrap();
    let queued = runtime
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: "coordinate this".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Queued,
                delivery: Some(PromptDelivery::Steer),
            },
        ))
        .await
        .unwrap();
    let pending = workspace_store
        .deliveries_after(binding.workspace_id, binding.participant_id, 0, 10)
        .await
        .unwrap();
    assert_eq!(pending[0].state, crate::DeliveryState::Pending);
    assert_eq!(pending[0].sequence, 1);
    drop(runtime);
    // A restarted actor reopens the durable session/workspace stores. The
    // queued session event is not an admission acknowledgement.
    let mut runtime = RuntimeSessionStore::new(store.clone(), Vec::new(), true)
        .with_workspace_projection(projection.clone());
    let _admitted = runtime
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: "coordinate this".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: Some(PromptDelivery::Steer),
            },
        ))
        .await
        .unwrap();
    let admitted_delivery = workspace_store
        .deliveries_after(binding.workspace_id, binding.participant_id, 0, 10)
        .await
        .unwrap();
    assert_eq!(admitted_delivery[0].state, crate::DeliveryState::Admitted);
    runtime
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
    let acknowledged = workspace_store
        .deliveries_after(binding.workspace_id, binding.participant_id, 0, 10)
        .await
        .unwrap();
    assert_eq!(acknowledged[0].state, crate::DeliveryState::Acknowledged);

    // A coordinator-authored team notification can be mirrored into the
    // root session after its workspace delivery was already acknowledged.
    // It is transcript provenance, not a second admission boundary, so it
    // must never try to move that delivery backwards to Admitted.
    runtime
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::System,
                text: "team report".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: Some(PromptDelivery::Queue),
            },
        ))
        .await
        .expect("a completed team notification must not regress delivery state");
    let after_team_report = workspace_store
        .deliveries_after(binding.workspace_id, binding.participant_id, 0, 10)
        .await
        .unwrap();
    assert_eq!(
        after_team_report[0].state,
        crate::DeliveryState::Acknowledged
    );
    assert!(!store.read(session_id).await.unwrap().iter().any(|event| {
        matches!(
            &event.kind,
            SessionEventKind::Error { message }
                if message.contains("invalid non-monotonic delivery transition")
        )
    }));

    for event in store.read(session_id).await.unwrap() {
        projection.project(&event).await.unwrap();
    }
    let acknowledged_after_restart = workspace_store
        .deliveries_after(binding.workspace_id, binding.participant_id, 0, 10)
        .await
        .unwrap();
    assert_eq!(
        acknowledged_after_restart[0].state,
        crate::DeliveryState::Acknowledged
    );

    let replay = workspace_store
        .replay(binding.workspace_id, binding.participant_id, 0, 10)
        .await
        .unwrap();
    assert_eq!(replay.len(), 5, "repair replay must be idempotent");
    assert_eq!(replay[1].author_id, human_id);
    assert!(matches!(
        replay[1].kind,
        WorkspaceEventKind::SessionEvent {
            session_id: projected_session,
            session_event_id,
            session_sequence: 1,
            ..
        } if projected_session == session_id && session_event_id == queued.id
    ));
    scratch.discard().await;
}

#[tokio::test]
async fn pending_prompt_admission_does_not_wait_for_workspace_repair() {
    let session_id = Uuid::new_v4();
    let (scratch, session_store) = crate::session_store::postgres::testing::session_store().await;
    let session_store = Arc::new(session_store);
    session_store.create_session(session_id).await.unwrap();
    let binding = session_store
        .workspace_binding(session_id)
        .await
        .unwrap()
        .unwrap();
    let workspace = session_store.workspace_store().await.unwrap().unwrap();
    let human = crate::local_human_participant_id("Human");
    workspace
        .ensure_execution_workspace(
            binding.workspace_id,
            "test",
            human,
            "Human",
            binding.participant_id,
            "Agent",
        )
        .await
        .unwrap();
    let projection = WorkspaceProjection::new(
        workspace,
        binding.workspace_id,
        binding.participant_id,
        human,
        0,
        0,
    );
    let message_id = Uuid::new_v4();
    let queued = session_store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: "next request".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Queued,
                delivery: Some(PromptDelivery::Queue),
            },
        ))
        .await
        .unwrap();
    let runtime = RuntimeSessionStore::new(session_store, Vec::new(), true)
        .with_workspace_projection(projection.clone());
    let blocked_repair = projection.projected_sequence.lock().await;
    assert_eq!(
        tokio::time::timeout(
            Duration::from_millis(250),
            runtime.prompt_admission_state(session_id, message_id),
        )
        .await
        .expect("admission waited for background repair")
        .unwrap(),
        PromptAdmissionState::Pending,
    );
    assert_eq!(*blocked_repair, 0);
    drop(blocked_repair);
    tokio::time::timeout(Duration::from_secs(2), async {
        while *projection.projected_sequence.lock().await < queued.sequence {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("deferred repair must still project the queued prompt");
    scratch.discard().await;
}

#[tokio::test]
async fn projection_delivery_failure_is_durable_and_does_not_fail_the_session_append() {
    let session_id = Uuid::new_v4();
    let (scratch, session_store) = crate::session_store::postgres::testing::session_store().await;
    let session_store = Arc::new(session_store);
    session_store.create_session(session_id).await.unwrap();
    let projection = WorkspaceProjection::new(
        session_store.workspace_store().await.unwrap().unwrap(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        0,
        0,
    );
    let store: Arc<dyn SessionStore> = session_store.clone();
    let mut runtime =
        RuntimeSessionStore::new(store, Vec::new(), true).with_workspace_projection(projection);

    let (event_tx, mut event_rx) = mpsc::channel(4);
    record(
        &mut runtime,
        &event_tx,
        session_id,
        SessionEventKind::StatusChanged {
            status: SessionStatus::Running,
            detail: Some("turn phase: awaiting provider".to_string()),
        },
    )
    .await
    .expect("repairable projection failure must not fail the source append");

    let projected = event_rx.recv().await.unwrap();
    let diagnostic = event_rx.recv().await.unwrap();
    assert_eq!(projected.sequence, 1);
    assert_eq!(diagnostic.sequence, 2);
    assert!(matches!(
        &diagnostic.kind,
        SessionEventKind::Error { message }
            if message.contains("workspace projection delivery failed")
    ));

    let durable = session_store.read(session_id).await.unwrap();
    assert!(durable.iter().any(|event| matches!(
        &event.kind,
        SessionEventKind::Error { message }
            if message.contains("workspace projection delivery failed")
                && message.contains("sequence 1")
    )));
    scratch.discard().await;
}

/// A rewind forks the session into the parent's workspace under a brand new
/// participant.  Replaying the inherited ancestry there would re-append
/// every parent event and then fail on the first message the new
/// participant was never an audience of.
#[tokio::test]
async fn a_forked_session_never_reprojects_the_inherited_ancestry() {
    let session_id = Uuid::new_v4();
    let (scratch, session_store) = crate::session_store::postgres::testing::session_store().await;
    let session_store = Arc::new(session_store);
    session_store.create_session(session_id).await.unwrap();
    let binding = session_store
        .workspace_binding(session_id)
        .await
        .unwrap()
        .unwrap();
    let workspace_store = session_store.workspace_store().await.unwrap().unwrap();
    let human_id = crate::local_human_participant_id("Human");
    workspace_store
        .ensure_execution_workspace(
            binding.workspace_id,
            "test workspace",
            human_id,
            "Human",
            binding.participant_id,
            "Agent",
        )
        .await
        .unwrap();
    let projection = WorkspaceProjection::new(
        workspace_store.clone(),
        binding.workspace_id,
        binding.participant_id,
        human_id,
        0,
        0,
    );
    let store: Arc<dyn SessionStore> = session_store.clone();
    let mut runtime = RuntimeSessionStore::new(store.clone(), Vec::new(), true)
        .with_workspace_projection(projection.clone());

    // A team message the parent participant is addressed by, mirrored into
    // the session transcript under the same message id.
    let message_id = Uuid::new_v4();
    workspace_store
        .append(WorkspaceEvent {
            id: message_id,
            workspace_id: binding.workspace_id,
            sequence: 0,
            author_id: human_id,
            idempotency_key: format!("test-team-message:{message_id}"),
            created_at: chrono::Utc::now(),
            kind: WorkspaceEventKind::Message {
                message: crate::WorkspaceMessage {
                    id: message_id,
                    workspace_id: binding.workspace_id,
                    thread_id: None,
                    reply_to_message_id: None,
                    author_id: human_id,
                    body: crate::WorkspaceMessageBody {
                        text: "coordinate this".to_string(),
                        mentions: Vec::new(),
                        attachments: Vec::new(),
                    },
                    audience: crate::Audience::Direct {
                        participant: binding.participant_id,
                    },
                    created_at: chrono::Utc::now(),
                },
                mode: crate::DeliveryMode::Boundary,
            },
        })
        .await
        .unwrap();
    for status in [MessageStatus::Queued, MessageStatus::Complete] {
        runtime
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::Message {
                    message_id,
                    actor: EventActor::User,
                    text: "coordinate this".to_string(),
                    attachments: Vec::new(),
                    status,
                    delivery: Some(PromptDelivery::Steer),
                },
            ))
            .await
            .unwrap();
    }
    let parent_events = workspace_store
        .replay(binding.workspace_id, binding.participant_id, 0, 64)
        .await
        .unwrap()
        .len();

    // Restarting the parent itself resumes from the watermark instead of
    // re-walking the transcript to re-prove idempotency.
    assert_eq!(
        workspace_store
            .latest_projected_session_sequence(binding.workspace_id, session_id)
            .await
            .unwrap(),
        2
    );
    assert!(
        store
            .events_after(session_id, 2, usize::MAX)
            .await
            .unwrap()
            .is_empty()
    );

    let fork_id = Uuid::new_v4();
    let fork = store.fork_before(session_id, fork_id, 3).await.unwrap();
    // The queue entry is not inheritable, so only the admission survives.
    assert_eq!(fork.inherited_event_count, 1);
    let fork_binding = store.workspace_binding(fork_id).await.unwrap().unwrap();
    assert_eq!(fork_binding.workspace_id, binding.workspace_id);
    workspace_store
        .ensure_execution_workspace(
            fork_binding.workspace_id,
            "test workspace",
            human_id,
            "Human",
            fork_binding.participant_id,
            "Agent",
        )
        .await
        .unwrap();
    let fork_projection = WorkspaceProjection::new(
        workspace_store.clone(),
        fork_binding.workspace_id,
        fork_binding.participant_id,
        human_id,
        fork.inherited_event_count,
        0,
    );

    // The hazard: a plain read renumbers the ancestry into the fork's own
    // identity, so filtering on session_id cannot separate the two.
    let read_back = store.read(fork_id).await.unwrap();
    assert_eq!(read_back.len(), 1);
    assert!(read_back.iter().all(|event| event.session_id == fork_id));

    // Exactly what the session kernel does when it resumes the fork.
    let inherited = store.inherited_event_count(fork_id).await.unwrap();
    assert_eq!(inherited, 1);
    let replayed = store
        .events_after(fork_id, inherited, usize::MAX)
        .await
        .unwrap();
    assert!(replayed.is_empty(), "a fresh fork has authored nothing");
    for event in replayed {
        fork_projection.project(&event).await.unwrap();
    }
    assert_eq!(
        workspace_store
            .replay(binding.workspace_id, binding.participant_id, 0, 64)
            .await
            .unwrap()
            .len(),
        parent_events,
        "resuming a fork must not re-append the ancestry"
    );

    // Even so, a participant outside a message's audience transitions
    // nothing instead of failing the session.
    assert!(
        workspace_store
            .transition_message_delivery(
                fork_binding.workspace_id,
                message_id,
                fork_binding.participant_id,
                crate::DeliveryState::Recalled,
                None,
            )
            .await
            .unwrap()
            .is_none()
    );
    scratch.discard().await;
}

struct RecordingExecutor {
    seen: RecordedTurns,
    called: Arc<Notify>,
}

struct ContextRecordingExecutor {
    seen: RecordedContextTurns,
}

struct ReusableContextExecutor {
    prompt_lengths: Arc<Mutex<Vec<usize>>>,
    called: Arc<Notify>,
}

struct InterruptibleReusableContextExecutor {
    prompt_lengths: Arc<Mutex<Vec<usize>>>,
    called: Arc<Notify>,
    calls: AtomicUsize,
    compaction_calls: Arc<AtomicUsize>,
}

struct DurableResumeExecutor {
    seen: RecordedDurableResumeTurns,
    called: Arc<Notify>,
    compaction_calls: Arc<AtomicUsize>,
}

struct ProviderRecordingExecutor {
    seen: RecordedProviderTurns,
    called: Arc<Notify>,
}

struct ConsultingExecutor {
    seen_tool: Arc<Mutex<Vec<(String, String)>>>,
    seen_provider: SeenConsultProvider,
    called: Arc<Notify>,
}

struct CrossProviderCompactionExecutor {
    seen: RecordedCompactionTurns,
    compacted: Arc<Notify>,
    released: Arc<Mutex<Vec<CodingProvider>>>,
}

struct OversizedCompactionExecutor {
    calls: Arc<AtomicUsize>,
    fail_on_second: bool,
}

fn test_provider_capabilities() -> Vec<crate::ProviderCapability> {
    [
        CodingProvider::Codex,
        CodingProvider::Claude,
        CodingProvider::OpenRouter,
        CodingProvider::OpenAiCompatible,
    ]
    .into_iter()
    .map(|provider| crate::ProviderCapability {
        provider,
        installed: true,
        version: Some("test".to_string()),
        authenticated: true,
        auth_detail: Some("test credentials".to_string()),
        auth_methods: vec![crate::ProviderAuthMethod::Subscription],
        can_spawn: true,
        usage: None,
        billing: None,
    })
    .collect()
}

#[async_trait::async_trait]
impl AgentTurnExecutor for RecordingExecutor {
    async fn execute(
        &self,
        turn: AgentTurn,
        events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        self.seen
            .lock()
            .unwrap()
            .push((turn.cwd, turn.output_schema));
        events
            .send(SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::Assistant,
                text: "managed executor response".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            })
            .await
            .unwrap();
        self.called.notify_one();
        Ok(AgentTurnResult {
            provider_session_id: Some("provider-session".to_string()),
            final_text: "managed executor response".to_string(),
        })
    }
}

#[async_trait::async_trait]
impl AgentTurnExecutor for ContextRecordingExecutor {
    async fn execute(
        &self,
        turn: AgentTurn,
        _events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        self.seen
            .lock()
            .unwrap()
            .push((turn.prompt, turn.provider_session_id));
        Ok(AgentTurnResult {
            provider_session_id: Some("provider-session".to_string()),
            final_text: "done".to_string(),
        })
    }
}

#[async_trait::async_trait]
impl AgentTurnExecutor for ReusableContextExecutor {
    fn supports_subscription_context_reuse(&self, provider: CodingProvider) -> bool {
        matches!(provider, CodingProvider::Codex | CodingProvider::Claude)
    }

    async fn execute(
        &self,
        turn: AgentTurn,
        events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        self.prompt_lengths.lock().unwrap().push(turn.prompt.len());
        let final_text = "r".repeat(650_000);
        events
            .send(SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::Assistant,
                text: final_text.clone(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            })
            .await
            .unwrap();
        self.called.notify_one();
        Ok(AgentTurnResult {
            provider_session_id: Some("reusable-provider-session".to_string()),
            final_text,
        })
    }
}

#[async_trait::async_trait]
impl AgentTurnExecutor for InterruptibleReusableContextExecutor {
    fn supports_subscription_context_reuse(&self, provider: CodingProvider) -> bool {
        provider == CodingProvider::Codex
    }

    async fn execute(
        &self,
        turn: AgentTurn,
        events: mpsc::Sender<SessionEventKind>,
        controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        self.prompt_lengths.lock().unwrap().push(turn.prompt.len());
        let call = self.calls.fetch_add(1, Ordering::AcqRel);
        self.called.notify_one();
        let final_text = match call {
            0 => {
                let text = "r".repeat(650_000);
                events
                    .send(SessionEventKind::Message {
                        message_id: Uuid::new_v4(),
                        actor: EventActor::Assistant,
                        text: text.clone(),
                        attachments: Vec::new(),
                        status: MessageStatus::Complete,
                        delivery: None,
                    })
                    .await
                    .unwrap();
                text
            }
            1 => {
                let mut controls = controls.expect("active turn has controls");
                while !matches!(
                    controls.recv().await,
                    Some(AgentTurnControl::Interrupt) | None
                ) {}
                String::new()
            }
            _ => "done".to_string(),
        };
        Ok(AgentTurnResult {
            provider_session_id: Some("reusable-provider-session".to_string()),
            final_text,
        })
    }

    async fn compact_retained_context(&self, _turn: AgentTurn) -> Result<AgentCompaction> {
        self.compaction_calls.fetch_add(1, Ordering::AcqRel);
        Ok(AgentCompaction {
            summary: "unexpected compaction".to_string(),
            usage: ProviderCallUsage::default(),
            provider_session_id: None,
        })
    }
}

#[async_trait::async_trait]
impl AgentTurnExecutor for DurableResumeExecutor {
    fn supports_subscription_context_reuse(&self, provider: CodingProvider) -> bool {
        provider == CodingProvider::Codex
    }

    async fn execute(
        &self,
        turn: AgentTurn,
        _events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        self.seen.lock().unwrap().push((
            turn.prompt,
            turn.provider_session_id,
            turn.provider_fork_turn_id,
            turn.conversation.len(),
        ));
        self.called.notify_one();
        Ok(AgentTurnResult {
            provider_session_id: Some("resumed-codex-thread".to_string()),
            final_text: "resumed".to_string(),
        })
    }

    async fn compact_retained_context(&self, _turn: AgentTurn) -> Result<AgentCompaction> {
        self.compaction_calls.fetch_add(1, Ordering::AcqRel);
        Ok(AgentCompaction {
            summary: "unexpected compaction".to_string(),
            usage: ProviderCallUsage::default(),
            provider_session_id: None,
        })
    }
}

#[async_trait::async_trait]
impl AgentTurnExecutor for ProviderRecordingExecutor {
    async fn execute(
        &self,
        turn: AgentTurn,
        _events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        self.seen
            .lock()
            .unwrap()
            .push((turn.provider, turn.model, turn.effort, turn.prompt));
        self.called.notify_waiters();
        Ok(AgentTurnResult {
            provider_session_id: Some(format!("{:?}-session", turn.provider)),
            final_text: "done".to_string(),
        })
    }
}

#[async_trait::async_trait]
impl AgentTurnExecutor for ConsultingExecutor {
    async fn execute(
        &self,
        turn: AgentTurn,
        events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        let consultation = turn
            .agent_tools
            .call(
                "consult_model",
                json!({
                    "profile": "claude-fable-5-1@high",
                    "prompt": "Review the selected interface and call out hidden risks."
                }),
            )
            .await?;
        self.seen_tool.lock().unwrap().push((
            "claude".to_string(),
            consultation["response"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
        ));
        events
            .send(SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::Assistant,
                text: consultation["response"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            })
            .await
            .unwrap();
        self.called.notify_one();
        Ok(AgentTurnResult {
            provider_session_id: Some("main-session".to_string()),
            final_text: "reconciled consultation".to_string(),
        })
    }

    async fn consult(&self, request: ConsultationRequest) -> Result<ConsultationResult> {
        self.seen_provider
            .lock()
            .unwrap()
            .push((request.provider, request.effort, request.prompt));
        Ok(ConsultationResult {
            provider: request.provider,
            model: request.model,
            final_text: "The interface hides a cancellation edge case.".to_string(),
            usage: Default::default(),
        })
    }
}

#[async_trait::async_trait]
impl AgentTurnExecutor for CrossProviderCompactionExecutor {
    fn supports_subscription_context_reuse(&self, provider: CodingProvider) -> bool {
        provider == CodingProvider::Codex
    }

    async fn release_provider_context(
        &self,
        _session_id: Uuid,
        provider: CodingProvider,
    ) -> Result<()> {
        self.released.lock().unwrap().push(provider);
        Ok(())
    }

    async fn execute(
        &self,
        turn: AgentTurn,
        events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        self.seen.lock().unwrap().push((
            turn.provider,
            turn.provider_session_id.clone(),
            turn.prompt.clone(),
        ));
        events
            .send(SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::Assistant,
                text: format!("response to {}", turn.prompt),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            })
            .await
            .unwrap();
        Ok(AgentTurnResult {
            provider_session_id: Some(format!("{:?}-session", turn.provider)),
            final_text: format!("response to {}", turn.prompt),
        })
    }

    async fn compact_retained_context(&self, turn: AgentTurn) -> Result<AgentCompaction> {
        assert_eq!(turn.provider, CodingProvider::Codex);
        assert!(turn.prompt.contains("first"));
        assert!(turn.prompt.contains("response to"));
        self.compacted.notify_one();
        Ok(AgentCompaction {
            summary: "retained summary".to_string(),
            usage: Default::default(),
            provider_session_id: Some("codex-compacted-session".to_string()),
        })
    }
}

#[async_trait::async_trait]
impl AgentTurnExecutor for OversizedCompactionExecutor {
    async fn execute(
        &self,
        _turn: AgentTurn,
        _events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        anyhow::bail!("ordinary execution is not used by the compaction test")
    }

    async fn compact_retained_context(&self, _turn: AgentTurn) -> Result<AgentCompaction> {
        let previous_calls = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_on_second && previous_calls == 1 {
            return Err(borg_provider::provider::ProviderStreamError {
                kind: borg_provider::provider::ProviderErrorKind::ConnectionLost,
                message: "second compaction fold disconnected".to_string(),
            }
            .into());
        }
        Ok(AgentCompaction {
            summary: format!(
                "summary-start{}summary-end",
                "s".repeat(SUBSCRIPTION_INPUT_BUDGET_CHARS * 4)
            ),
            usage: ProviderCallUsage {
                duration_ms: 11,
                input_tokens: 101,
                cached_input_tokens: 20,
                cache_creation_input_tokens: 5,
                output_tokens: 9,
                total_tokens: 135,
                context_tokens: Some(300),
                context_window_tokens: Some(500),
                cost_microusd: Some(700),
                cost_basis: borg_provider::CostBasis::SubscriptionEquivalent,
            },
            provider_session_id: None,
        })
    }
}

struct InterruptibleQueueExecutor {
    abort_error: Option<&'static str>,
    seen: RecordedPromptTurns,
    provider_sessions: Arc<Mutex<Vec<Option<String>>>>,
    called: Arc<Notify>,
}

struct RejectingSteerExecutor {
    turns: RecordedPromptTurns,
    steers: RecordedPromptTurns,
    turn_started: Arc<Notify>,
    steer_seen: Arc<Notify>,
}

struct HoldingSteerExecutor {
    turns: RecordedPromptTurns,
    turn_started: Arc<Notify>,
    steer_seen: Arc<Notify>,
    native: bool,
}

struct CommittingSteerExecutor {
    turn_started: Arc<Notify>,
    steer_accepted: Arc<Notify>,
}

struct FlushingQueueExecutor {
    turn_started: Arc<Notify>,
    steers: Arc<Mutex<Vec<String>>>,
    steer_seen: Arc<Notify>,
    interrupted: Arc<AtomicBool>,
}

struct BoundaryRetrySteerExecutor {
    turn_started: Arc<Notify>,
    first_attempt_rejected: Arc<Notify>,
    release_tool_boundary: Arc<Notify>,
    retry_accepted: Arc<Notify>,
}

struct BoundaryQueueExecutor {
    turns: RecordedPromptTurns,
    first_started: Arc<Notify>,
    release_first: Arc<Notify>,
}

struct PrematureReadyExecutor {
    first_started: Arc<Notify>,
    release_first: Arc<Notify>,
}

struct EmptyThenSuccessExecutor {
    calls: Arc<AtomicUsize>,
    prompts: RecordedPromptTurns,
}

struct UsageLimitThenSuccessExecutor {
    calls: Arc<AtomicUsize>,
    /// Emit a tool call before the first failure so the turn counts as having
    /// side effects, and record every prompt text the executor receives.
    side_effects_before_limit: bool,
    prompts: Arc<Mutex<Vec<String>>>,
}

#[async_trait::async_trait]
impl AgentTurnExecutor for UsageLimitThenSuccessExecutor {
    async fn execute(
        &self,
        turn: AgentTurn,
        events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        self.prompts.lock().unwrap().push(turn.prompt.clone());
        if self.calls.fetch_add(1, Ordering::AcqRel) == 0 {
            if self.side_effects_before_limit {
                let _ = events
                    .send(SessionEventKind::ToolStarted {
                        tool_call_id: "call-1".into(),
                        name: "shell".into(),
                        input: serde_json::json!({"command": "git commit"}),
                        input_ref: None,
                    })
                    .await;
                let _ = events
                    .send(SessionEventKind::ToolCompleted {
                        tool_call_id: "call-1".into(),
                        output: "committed".into(),
                        output_ref: None,
                        is_error: false,
                        input: None,
                        input_ref: None,
                    })
                    .await;
            }
            return Err(anyhow::anyhow!(
                "You've hit your usage limit. Try again later."
            ));
        }
        Ok(AgentTurnResult {
            provider_session_id: Some("provider-session".to_string()),
            final_text: "resumed after reset".to_string(),
        })
    }
}

/// Hits a usage limit whose reset is an hour away, then succeeds.
struct LongUsageLimitExecutor {
    calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl AgentTurnExecutor for LongUsageLimitExecutor {
    async fn execute(
        &self,
        _turn: AgentTurn,
        _events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        if self.calls.fetch_add(1, Ordering::AcqRel) == 0 {
            return Err(anyhow::anyhow!(
                "You've hit your usage limit. Provider-reported retry delay: 3600 seconds."
            ));
        }
        Ok(AgentTurnResult {
            provider_session_id: Some("provider-session".to_string()),
            final_text: "ran after the top-up".to_string(),
        })
    }
}

struct HungProviderExecutor;

struct NarrationThenDelayedCompletionExecutor;

struct ReadyThenHungExecutor;

struct CleanupBarrierExecutor {
    cooperative: bool,
    started: Arc<Notify>,
    cleanup_started: Arc<Notify>,
    release_cleanup: Arc<Notify>,
    cleanup_calls: AtomicUsize,
}

#[async_trait::async_trait]
impl AgentTurnExecutor for HungProviderExecutor {
    async fn execute(
        &self,
        _turn: AgentTurn,
        _events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        std::future::pending().await
    }
}

#[async_trait::async_trait]
impl AgentTurnExecutor for NarrationThenDelayedCompletionExecutor {
    async fn execute(
        &self,
        turn: AgentTurn,
        events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        events
            .send(SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::Assistant,
                text: "I am starting the consultation now.".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            })
            .await
            .unwrap();
        tokio::time::sleep(PROVIDER_DRAIN_LIVENESS_TIMEOUT + Duration::from_millis(50)).await;
        events
            .send(SessionEventKind::Message {
                message_id: turn.message_id,
                actor: EventActor::Assistant,
                text: "consultation complete".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            })
            .await
            .unwrap();
        events
            .send(SessionEventKind::StatusChanged {
                status: SessionStatus::Ready,
                detail: None,
            })
            .await
            .unwrap();
        Ok(AgentTurnResult {
            provider_session_id: Some("provider-session".to_string()),
            final_text: "consultation complete".to_string(),
        })
    }
}

#[async_trait::async_trait]
impl AgentTurnExecutor for ReadyThenHungExecutor {
    async fn execute(
        &self,
        turn: AgentTurn,
        events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        events
            .send(SessionEventKind::Message {
                message_id: turn.message_id,
                actor: EventActor::Assistant,
                text: "final answer".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            })
            .await
            .unwrap();
        events
            .send(SessionEventKind::StatusChanged {
                status: SessionStatus::Ready,
                detail: None,
            })
            .await
            .unwrap();
        std::future::pending().await
    }
}

#[async_trait::async_trait]
impl AgentTurnExecutor for CleanupBarrierExecutor {
    fn uses_native_harness(&self, _provider: CodingProvider) -> bool {
        self.cooperative
    }

    async fn execute(
        &self,
        _turn: AgentTurn,
        events: mpsc::Sender<SessionEventKind>,
        controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        events
            .send(SessionEventKind::ReasoningDelta {
                text: "provider active".to_string(),
            })
            .await
            .ok();
        self.started.notify_one();
        if self.cooperative {
            let mut controls = controls.expect("active native turn has controls");
            while !matches!(
                controls.recv().await,
                Some(AgentTurnControl::Interrupt) | None
            ) {}
            anyhow::bail!("native provider turn interrupted");
        }
        std::future::pending().await
    }

    async fn stop_session(&self, _session_id: Uuid) -> Result<()> {
        if self.cleanup_calls.fetch_add(1, Ordering::AcqRel) == 0 {
            self.cleanup_started.notify_one();
            self.release_cleanup.notified().await;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl AgentTurnExecutor for InterruptibleQueueExecutor {
    fn supports_subscription_context_reuse(&self, provider: CodingProvider) -> bool {
        provider == CodingProvider::Codex
    }

    async fn execute(
        &self,
        turn: AgentTurn,
        events: mpsc::Sender<SessionEventKind>,
        controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        self.seen
            .lock()
            .unwrap()
            .push((turn.prompt.clone(), turn.attachments.clone()));
        self.provider_sessions
            .lock()
            .unwrap()
            .push(turn.provider_session_id.clone());
        self.called.notify_one();
        if subscription_prompt_ends_with(&turn.prompt, "first") {
            let mut controls = controls.expect("active turn has controls");
            while !matches!(
                controls.recv().await,
                Some(AgentTurnControl::Interrupt) | None
            ) {}
            if let Some(error) = self.abort_error {
                events
                    .send(SessionEventKind::Error {
                        message: error.to_string(),
                    })
                    .await
                    .unwrap();
                anyhow::bail!(error);
            }
        }
        Ok(AgentTurnResult {
            provider_session_id: Some("provider-session".to_string()),
            final_text: String::new(),
        })
    }
}

#[async_trait::async_trait]
impl AgentTurnExecutor for RejectingSteerExecutor {
    async fn execute(
        &self,
        turn: AgentTurn,
        _events: mpsc::Sender<SessionEventKind>,
        controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        self.turns
            .lock()
            .unwrap()
            .push((turn.prompt.clone(), turn.attachments.clone()));
        self.turn_started.notify_one();
        if subscription_prompt_ends_with(&turn.prompt, "first") {
            let mut controls = controls.expect("active turn has controls");
            if let Some(AgentTurnControl::Steer {
                text,
                attachments,
                ack,
                ..
            }) = controls.recv().await
            {
                self.steers.lock().unwrap().push((text, attachments));
                let _ = ack.send(Err("turn ended before steer was accepted".to_string()));
                self.steer_seen.notify_one();
            }
        }
        Ok(AgentTurnResult {
            provider_session_id: Some("provider-session".to_string()),
            final_text: String::new(),
        })
    }
}

#[async_trait::async_trait]
impl AgentTurnExecutor for HoldingSteerExecutor {
    fn uses_native_harness(&self, _provider: CodingProvider) -> bool {
        self.native
    }

    async fn execute(
        &self,
        turn: AgentTurn,
        _events: mpsc::Sender<SessionEventKind>,
        controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        self.turns
            .lock()
            .unwrap()
            .push((turn.prompt.clone(), turn.attachments));
        self.turn_started.notify_one();
        // A native turn carries the raw prompt; a subscription turn frames it.
        if subscription_prompt_ends_with(&turn.prompt, "first")
            || (self.native && turn.prompt.trim_end().ends_with("first"))
        {
            let mut controls = controls.expect("active turn has controls");
            let mut held_ack = None;
            while let Some(control) = controls.recv().await {
                match control {
                    AgentTurnControl::Steer { ack, .. } => {
                        held_ack = Some(ack);
                        self.steer_seen.notify_one();
                    }
                    AgentTurnControl::Interrupt => break,
                    AgentTurnControl::Approval { .. }
                    | AgentTurnControl::ProviderInteractionResponse { .. } => {}
                }
            }
            drop(held_ack);
        }
        Ok(AgentTurnResult {
            provider_session_id: Some("provider-session".to_string()),
            final_text: String::new(),
        })
    }
}

#[async_trait::async_trait]
impl AgentTurnExecutor for CommittingSteerExecutor {
    async fn execute(
        &self,
        _turn: AgentTurn,
        _events: mpsc::Sender<SessionEventKind>,
        controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        self.turn_started.notify_one();
        let mut controls = controls.expect("active turn has controls");
        if let Some(AgentTurnControl::Steer { admission, ack, .. }) = controls.recv().await {
            assert!(admission.accept());
            let _ = ack.send(Ok(()));
            self.steer_accepted.notify_one();
        }
        while !matches!(
            controls.recv().await,
            Some(AgentTurnControl::Interrupt) | None
        ) {}
        Ok(AgentTurnResult {
            provider_session_id: Some("provider-session".to_string()),
            final_text: String::new(),
        })
    }
}

struct NativeFoldSteerExecutor {
    turn_started: Arc<Notify>,
    steer_handled: Arc<Notify>,
    /// Journal the fold marker before acknowledging, rather than after.
    marker_first: bool,
    /// Whether the steer is folded into the model input at all.
    fold: bool,
}

#[async_trait::async_trait]
impl AgentTurnExecutor for NativeFoldSteerExecutor {
    fn uses_native_harness(&self, _provider: CodingProvider) -> bool {
        true
    }

    async fn execute(
        &self,
        _turn: AgentTurn,
        events: mpsc::Sender<SessionEventKind>,
        controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        self.turn_started.notify_one();
        let mut controls = controls.expect("active turn has controls");
        if let Some(AgentTurnControl::Steer {
            message_id,
            admission,
            ack,
            ..
        }) = controls.recv().await
        {
            assert!(admission.accept());
            // The array shape the harness actually emits: one fold can
            // capture several steer controls. Emitting the singular form
            // here would have let the actor read a field the harness never
            // sends and still pass.
            let marker = |id: Uuid| SessionEventKind::ProviderEvent {
                provider: CodingProvider::OpenRouter,
                kind: crate::session::NATIVE_STEER_APPLIED.to_string(),
                payload: json!({ "message_ids": [id] }),
            };
            if self.marker_first {
                if self.fold {
                    let _ = events.send(marker(message_id)).await;
                }
                let _ = ack.send(Ok(()));
            } else {
                let _ = ack.send(Ok(()));
                if self.fold {
                    let _ = events.send(marker(message_id)).await;
                }
            }
            self.steer_handled.notify_one();
        }
        while !matches!(
            controls.recv().await,
            Some(AgentTurnControl::Interrupt) | None
        ) {}
        Ok(AgentTurnResult {
            provider_session_id: None,
            final_text: String::new(),
        })
    }
}

#[async_trait::async_trait]
impl AgentTurnExecutor for FlushingQueueExecutor {
    async fn execute(
        &self,
        _turn: AgentTurn,
        events: mpsc::Sender<SessionEventKind>,
        controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        self.turn_started.notify_one();
        events
            .send(SessionEventKind::ReasoningDelta {
                text: "working".to_string(),
            })
            .await?;
        let mut controls = controls.expect("active turn has controls");
        while let Some(control) = controls.recv().await {
            match control {
                AgentTurnControl::Steer {
                    text,
                    admission,
                    ack,
                    ..
                } => {
                    assert!(admission.accept());
                    self.steers.lock().unwrap().push(text);
                    let _ = ack.send(Ok(()));
                    self.steer_seen.notify_one();
                }
                AgentTurnControl::Interrupt => {
                    self.interrupted.store(true, Ordering::Release);
                    break;
                }
                AgentTurnControl::Approval { .. }
                | AgentTurnControl::ProviderInteractionResponse { .. } => {}
            }
        }
        Ok(AgentTurnResult {
            provider_session_id: Some("provider-session".to_string()),
            final_text: String::new(),
        })
    }
}

#[async_trait::async_trait]
impl AgentTurnExecutor for BoundaryRetrySteerExecutor {
    async fn execute(
        &self,
        _turn: AgentTurn,
        events: mpsc::Sender<SessionEventKind>,
        controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        self.turn_started.notify_one();
        let mut controls = controls.expect("active turn has controls");
        events
            .send(SessionEventKind::ToolStarted {
                tool_call_id: "tool-1".to_string(),
                name: "command_execution".to_string(),
                input: json!({"command": "long-running-check"}),
                input_ref: None,
            })
            .await
            .unwrap();

        let Some(AgentTurnControl::Steer { ack, .. }) = controls.recv().await else {
            panic!("first steer attempt");
        };
        let _ = ack.send(Err("temporary active-turn boundary rejection".to_string()));
        self.first_attempt_rejected.notify_one();

        self.release_tool_boundary.notified().await;
        events
            .send(SessionEventKind::ToolCompleted {
                tool_call_id: "tool-1".to_string(),
                output: "done".to_string(),
                output_ref: None,
                is_error: false,
                input: None,
                input_ref: None,
            })
            .await
            .unwrap();

        let Some(AgentTurnControl::Steer { admission, ack, .. }) = controls.recv().await else {
            panic!("boundary retry");
        };
        assert!(admission.accept());
        let _ = ack.send(Ok(()));
        self.retry_accepted.notify_one();

        while !matches!(
            controls.recv().await,
            Some(AgentTurnControl::Interrupt) | None
        ) {}
        Ok(AgentTurnResult {
            provider_session_id: Some("provider-session".to_string()),
            final_text: String::new(),
        })
    }
}

#[async_trait::async_trait]
impl AgentTurnExecutor for BoundaryQueueExecutor {
    async fn execute(
        &self,
        turn: AgentTurn,
        events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        self.turns
            .lock()
            .unwrap()
            .push((turn.prompt.clone(), turn.attachments));
        if subscription_prompt_ends_with(&turn.prompt, "first") {
            self.first_started.notify_one();
            events
                .send(SessionEventKind::ReasoningDelta {
                    text: "working".to_string(),
                })
                .await?;
            self.release_first.notified().await;
        }
        Ok(AgentTurnResult {
            provider_session_id: Some("provider-session".to_string()),
            final_text: String::new(),
        })
    }
}

#[async_trait::async_trait]
impl AgentTurnExecutor for PrematureReadyExecutor {
    async fn execute(
        &self,
        turn: AgentTurn,
        events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        if subscription_prompt_ends_with(&turn.prompt, "first") {
            self.first_started.notify_one();
            self.release_first.notified().await;
        }
        events
            .send(SessionEventKind::StatusChanged {
                status: SessionStatus::Running,
                detail: Some("executor lifecycle".to_string()),
            })
            .await
            .unwrap();
        events
            .send(SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::Assistant,
                text: format!("response to {}", turn.prompt),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            })
            .await
            .unwrap();
        events
            .send(SessionEventKind::StatusChanged {
                status: SessionStatus::Ready,
                detail: Some("executor returned early".to_string()),
            })
            .await
            .unwrap();
        Ok(AgentTurnResult {
            provider_session_id: Some("provider-session".to_string()),
            final_text: format!("response to {}", turn.prompt),
        })
    }
}

#[async_trait::async_trait]
impl AgentTurnExecutor for EmptyThenSuccessExecutor {
    async fn execute(
        &self,
        turn: AgentTurn,
        events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        self.prompts
            .lock()
            .unwrap()
            .push((turn.prompt, turn.attachments));
        if self.calls.fetch_add(1, Ordering::AcqRel) == 0 {
            events
                .send(SessionEventKind::Error {
                    message: "codex returned an empty response".to_string(),
                })
                .await
                .unwrap();
            return Err(anyhow::anyhow!("codex returned an empty response"));
        }
        Ok(AgentTurnResult {
            provider_session_id: Some("provider-session".to_string()),
            final_text: "recovered".to_string(),
        })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn empty_provider_response_retries_without_losing_user_prompt() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let message_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(128);
    let calls = Arc::new(AtomicUsize::new(0));
    let prompts = Arc::new(Mutex::new(Vec::new()));
    let executor = Arc::new(EmptyThenSuccessExecutor {
        calls: Arc::clone(&calls),
        prompts: Arc::clone(&prompts),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn({
        let journal_path = journal_path.clone();
        let cwd = root.path().to_path_buf();
        async move {
            run_session_actor(
                &journal_path,
                session_id,
                LaunchSession {
                    request_id: message_id,
                    cwd,
                    provider: CodingProvider::Codex,
                    model: None,
                    effort: None,
                    fast: Some(false),
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::Manual,
                    name: None,
                    initial_prompt: Some("do not lose this request".to_string()),
                    capabilities: Default::default(),
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                executor,
                actor_store,
            )
            .await
        }
    });

    let mut transitions = Vec::new();
    let mut completed = 0;
    let mut retry_error_was_rendered = false;
    while completed < 2 {
        let event = tokio::time::timeout(Duration::from_secs(2), event_rx.recv())
            .await
            .expect("automatic retry is bounded")
            .expect("session remains attached");
        if let SessionEventKind::Message {
            message_id: event_message_id,
            status,
            delivery: Some(delivery),
            ..
        } = &event.kind
            && *event_message_id == message_id
        {
            transitions.push((*status, *delivery));
        }
        if matches!(
            &event.kind,
            SessionEventKind::Error { message }
                if message == "codex returned an empty response"
        ) {
            retry_error_was_rendered = true;
        }
        if matches!(
            event.kind,
            SessionEventKind::TurnCompleted {
                message_id: completed_message_id,
                ..
            } if completed_message_id == message_id
        ) {
            completed += 1;
        }
    }

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();

    let store = PostgresSessionStore::connect_with_pool_size(&scratch.url, 2)
        .await
        .unwrap();
    let action = store.action(session_id, message_id).await.unwrap().unwrap();
    assert_eq!(action.state, crate::SessionActionState::Completed);
    assert_eq!(action.kind, crate::SessionActionKind::Prompt);
    let action_transitions = store
        .action_transitions(session_id, message_id)
        .await
        .unwrap();
    assert!(action_transitions.iter().any(|transition| {
        transition.from == Some(crate::SessionActionState::Failed)
            && transition.to == crate::SessionActionState::Queued
    }));

    assert_eq!(calls.load(Ordering::Acquire), 2);
    assert!(!retry_error_was_rendered);
    assert_eq!(
        transitions,
        [
            (MessageStatus::InProgress, PromptDelivery::Queue),
            (MessageStatus::Queued, PromptDelivery::Queue),
            (MessageStatus::InProgress, PromptDelivery::Queue),
            (MessageStatus::Complete, PromptDelivery::Queue),
        ]
    );
    assert_eq!(prompts.lock().unwrap().len(), 2);
    scratch.discard().await;
}

#[tokio::test(flavor = "current_thread")]
async fn usage_limited_prompt_resumes_automatically_after_the_retry_delay() {
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let message_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(128);
    let calls = Arc::new(AtomicUsize::new(0));
    let executor = Arc::new(UsageLimitThenSuccessExecutor {
        calls: Arc::clone(&calls),
        side_effects_before_limit: false,
        prompts: Arc::default(),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn({
        let journal_path = root.path().join("session.lock");
        let cwd = root.path().to_path_buf();
        async move {
            run_session_actor(
                &journal_path,
                session_id,
                LaunchSession {
                    request_id: message_id,
                    cwd,
                    provider: CodingProvider::Codex,
                    model: None,
                    effort: None,
                    fast: Some(false),
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::Manual,
                    name: None,
                    initial_prompt: Some("finish this task".to_string()),
                    capabilities: Default::default(),
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                executor,
                actor_store,
            )
            .await
        }
    });

    let mut completions = 0;
    while completions < 2 {
        let event = tokio::time::timeout(Duration::from_secs(2), event_rx.recv())
            .await
            .expect("usage-limit retry completes")
            .expect("session remains attached");
        if matches!(
            event.kind,
            SessionEventKind::TurnCompleted {
                message_id: completed_message_id,
                ..
            } if completed_message_id == message_id
        ) {
            completions += 1;
        }
    }

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();
    assert_eq!(calls.load(Ordering::Acquire), 2);

    let store = PostgresSessionStore::connect_with_pool_size(&scratch.url, 2)
        .await
        .unwrap();
    assert_eq!(
        store
            .action(session_id, message_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        crate::SessionActionState::Completed
    );
    scratch.discard().await;
}

async fn next_turn_completion(event_rx: &mut mpsc::Receiver<SessionEvent>, what: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let event = tokio::time::timeout_at(deadline, event_rx.recv())
            .await
            .unwrap_or_else(|_| panic!("{what}"))
            .expect("session remains attached");
        if matches!(event.kind, SessionEventKind::TurnCompleted { .. }) {
            return;
        }
    }
}

/// Failure mode: after a usage limit, a person's new message sat queued
/// behind the automatic retry timer (up to hours) even though the account
/// had been topped up, leaving the session stuck on "starting".
#[tokio::test]
async fn a_human_message_ends_a_usage_limit_wait_immediately() {
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(128);
    let calls = Arc::new(AtomicUsize::new(0));
    let executor = Arc::new(LongUsageLimitExecutor {
        calls: Arc::clone(&calls),
    });
    let actor = tokio::spawn({
        let journal_path = root.path().join("session.lock");
        let cwd = root.path().to_path_buf();
        let store = Arc::clone(&store);
        async move {
            run_session_actor(
                &journal_path,
                session_id,
                LaunchSession {
                    request_id: Uuid::new_v4(),
                    cwd,
                    provider: CodingProvider::Codex,
                    model: None,
                    effort: None,
                    fast: Some(false),
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::Manual,
                    name: None,
                    initial_prompt: Some("finish this task".to_string()),
                    capabilities: Default::default(),
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                executor,
                store,
            )
            .await
        }
    });
    next_turn_completion(&mut event_rx, "the first turn hits the usage limit").await;
    assert_eq!(calls.load(Ordering::Acquire), 1);

    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: Uuid::new_v4(),
            text: "I topped up, carry on".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Queue,
        })
        .await
        .unwrap();
    next_turn_completion(
        &mut event_rx,
        "the human message runs without waiting an hour",
    )
    .await;
    assert_eq!(calls.load(Ordering::Acquire), 2);

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();
    scratch.discard().await;
}

#[tokio::test(flavor = "current_thread")]
async fn usage_limit_after_side_effects_continues_instead_of_replaying_the_prompt() {
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let message_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(256);
    let calls = Arc::new(AtomicUsize::new(0));
    let prompts = Arc::new(Mutex::new(Vec::new()));
    let executor = Arc::new(UsageLimitThenSuccessExecutor {
        calls: Arc::clone(&calls),
        side_effects_before_limit: true,
        prompts: Arc::clone(&prompts),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn({
        let journal_path = root.path().join("session.lock");
        let cwd = root.path().to_path_buf();
        async move {
            run_session_actor(
                &journal_path,
                session_id,
                LaunchSession {
                    request_id: message_id,
                    cwd,
                    provider: CodingProvider::Codex,
                    model: None,
                    effort: None,
                    fast: Some(false),
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::Manual,
                    name: None,
                    initial_prompt: Some("finish this task".to_string()),
                    capabilities: Default::default(),
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                executor,
                actor_store,
            )
            .await
        }
    });

    let mut completions = 0;
    let mut user_statuses = Vec::new();
    let mut continuation_message_id = None;
    let mut checkpoint = None;
    while completions < 2 {
        let Some(event) = tokio::time::timeout(Duration::from_secs(2), event_rx.recv())
            .await
            .expect("continuation turn completes")
        else {
            let result = actor.await.unwrap();
            panic!(
                "session exited early after {completions} completion(s): {result:?}; statuses {user_statuses:?}"
            );
        };
        match &event.kind {
            SessionEventKind::ProviderEvent { kind, payload, .. }
                if kind == "usage_limit_retry" =>
            {
                assert_eq!(event.kind.persistence(), crate::EventPersistence::Durable);
                checkpoint = Some(
                    serde_json::from_value::<crate::session_store::PendingUsageLimitRetry>(
                        payload.clone(),
                    )
                    .unwrap(),
                );
            }
            SessionEventKind::Message {
                message_id: id,
                actor: EventActor::User,
                status,
                ..
            } if *id == message_id => user_statuses.push(*status),
            SessionEventKind::TurnCompleted { message_id: id, .. } => {
                completions += 1;
                if *id != message_id {
                    continuation_message_id = Some(*id);
                }
            }
            _ => {}
        }
    }
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();

    // The delivered message is settled as complete, never re-queued into
    // pending input, and the resume is a distinct internal continuation turn.
    assert_eq!(calls.load(Ordering::Acquire), 2);
    assert_eq!(user_statuses.last(), Some(&MessageStatus::Complete));
    assert!(
        !user_statuses
            .iter()
            .skip(2)
            .any(|status| *status == MessageStatus::Queued),
        "user prompt must not return to the queue: {user_statuses:?}"
    );
    assert!(continuation_message_id.is_some());
    let checkpoint = checkpoint.unwrap();
    assert!(checkpoint.retry_at.is_some());
    assert_eq!(Some(checkpoint.prompt.message_id), continuation_message_id);
    assert_eq!(checkpoint.replaced_message_ids, vec![message_id]);
    // Drop the guard before the scratch database is discarded: the
    // assertions need it, the teardown await must not hold it.
    {
        let prompts = prompts.lock().unwrap();
        assert!(prompts[1].contains(&serde_json::to_string(&checkpoint.prompt.text).unwrap()));
        assert!(prompts[0].contains("finish this task"));
        assert!(prompts[1].contains("Continue from exactly where it left off"));
        assert!(prompts[1].contains("finish this task"));
    }
    scratch.discard().await;
}

// Seed the crash boundary immediately after the atomic checkpoint, before
// the old message was settled. Same-process retry tests cannot cover this.
#[tokio::test(flavor = "current_thread")]
async fn usage_limit_checkpoint_survives_restart_without_early_or_duplicate_delivery() {
    for (continuation, cancel, already_started, model_changed) in [
        (false, false, false, false),
        (true, false, false, false),
        (true, false, true, false),
        (true, false, false, true),
        (true, true, false, false),
    ] {
        let root = tempdir().unwrap();
        let session_id = Uuid::new_v4();
        let original = QueuedPrompt {
            message_id: Uuid::new_v4(),
            text: "finish the committed work".into(),
            actor: EventActor::User,
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Queue,
            visible: true,
            interrupt_batch: true,
            batch: Vec::new(),
        };
        let prompt = if continuation {
            usage_limit_continuation(&original)
        } else {
            original.clone()
        };
        // Under a concurrent workspace suite the shared PostgreSQL setup can
        // delay Ready; leave enough time to send Interrupt before retry fires.
        let deadline = Utc::now() + chrono::Duration::seconds(10);
        let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
        let store: Arc<dyn SessionStore> = Arc::new(store);
        store.create_session(session_id).await.unwrap();
        store.create_session(session_id).await.unwrap();
        for kind in [
            SessionEventKind::SessionStarted,
            SessionEventKind::SessionConfigured {
                cwd: root.path().to_path_buf(),
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: false,
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
            },
            SessionEventKind::Message {
                message_id: original.message_id,
                actor: original.actor,
                text: original.text.clone(),
                attachments: Vec::new(),
                status: MessageStatus::InProgress,
                delivery: Some(PromptDelivery::Queue),
            },
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "usage_limit_retry".into(),
                payload: serde_json::to_value(crate::session_store::PendingUsageLimitRetry {
                    retry_at: Some(deadline),
                    prompt: prompt.clone(),
                    replaced_message_ids: vec![original.message_id],
                    continuation,
                    in_progress: false,
                })
                .unwrap(),
            },
        ] {
            store
                .append(SessionEvent::new(session_id, 0, kind))
                .await
                .unwrap();
        }
        if already_started {
            store
                .append(SessionEvent::new(
                    session_id,
                    0,
                    SessionEventKind::TurnStarted {
                        message_id: prompt.message_id,
                        provider: CodingProvider::Codex,
                        model: model_changed.then(|| "replacement-model".into()),
                        effort: None,
                        fast: false,
                    },
                ))
                .await
                .unwrap();
        }
        if model_changed {
            store
                .append(SessionEvent::new(
                    session_id,
                    0,
                    SessionEventKind::SessionConfigured {
                        cwd: root.path().to_path_buf(),
                        provider: CodingProvider::Codex,
                        model: Some("replacement-model".into()),
                        effort: None,
                        fast: false,
                        response_language: crate::ResponseLanguage::Auto,
                        permission_mode: PermissionMode::Manual,
                    },
                ))
                .await
                .unwrap();
            let retry = store
                .state(session_id)
                .await
                .unwrap()
                .usage_limit_retry
                .unwrap();
            assert!(retry.retry_at.is_none());
            assert_eq!(retry.prompt, prompt);
        }
        let state = store.state(session_id).await.unwrap();
        let fork_id = Uuid::new_v4();
        store
            .fork_before(session_id, fork_id, state.latest_sequence + 1)
            .await
            .unwrap();
        assert!(
            store
                .state(fork_id)
                .await
                .unwrap()
                .usage_limit_retry
                .is_none()
        );
        let calls = Arc::new(AtomicUsize::new(1));
        let prompts = Arc::new(Mutex::new(Vec::new()));
        // A second actor generation must not resurrect the completed original
        // or a cancelled continuation after the retry checkpoint is gone.
        for generation in 0..2 {
            let (command_tx, command_rx) = mpsc::channel(8);
            let (event_tx, mut event_rx) = mpsc::channel(128);
            let executor: Arc<dyn AgentTurnExecutor> = Arc::new(UsageLimitThenSuccessExecutor {
                calls: Arc::clone(&calls),
                side_effects_before_limit: false,
                prompts: Arc::clone(&prompts),
            });
            let journal_path = root.path().join("session.lock");
            let cwd = root.path().to_path_buf();
            let actor_store = Arc::clone(&store);
            let actor = tokio::spawn(async move {
                run_session_actor(
                    &journal_path,
                    session_id,
                    LaunchSession {
                        request_id: Uuid::new_v4(),
                        cwd,
                        provider: CodingProvider::Codex,
                        model: model_changed.then(|| "replacement-model".into()),
                        effort: None,
                        fast: Some(false),
                        response_language: crate::ResponseLanguage::Auto,
                        permission_mode: PermissionMode::Manual,
                        name: None,
                        initial_prompt: None,
                        capabilities: Default::default(),
                        subagent_concurrency_limit: None,
                        extension_skill_roots: Vec::new(),
                        team_policy: None,
                    },
                    command_rx,
                    event_tx,
                    executor,
                    actor_store,
                )
                .await
            });
            loop {
                let event = tokio::time::timeout(Duration::from_secs(12), event_rx.recv())
                    .await
                    .unwrap()
                    .unwrap();
                match event.kind {
                    SessionEventKind::TurnStarted { message_id, .. } => {
                        assert_eq!(generation, 0);
                        assert!(!cancel);
                        assert_eq!(message_id, prompt.message_id);
                        assert!(already_started || model_changed || Utc::now() >= deadline);
                    }
                    SessionEventKind::TurnCompleted { error, .. } => {
                        assert!(error.is_none());
                        break;
                    }
                    SessionEventKind::StatusChanged {
                        status: SessionStatus::Ready,
                        ..
                    } if generation == 1 || cancel => break,
                    _ => {}
                }
            }
            if generation == 0 && cancel {
                assert!(
                    Utc::now() < deadline,
                    "cancel while the deadline is pending"
                );
                command_tx
                    .send(HostCommand::Interrupt { session_id })
                    .await
                    .unwrap();
                loop {
                    let event = tokio::time::timeout(Duration::from_secs(5), event_rx.recv())
                        .await
                        .unwrap()
                        .unwrap();
                    if matches!(
                        event.kind,
                        SessionEventKind::UserStopChanged { engaged: true }
                    ) {
                        break;
                    }
                }
            }
            if generation == 1 {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            command_tx
                .send(HostCommand::Stop { session_id })
                .await
                .unwrap();
            actor.await.unwrap().unwrap();
            assert!(
                store
                    .state(session_id)
                    .await
                    .unwrap()
                    .usage_limit_retry
                    .is_none()
            );
            assert!(
                recover_prompts_on_resume(&store.recovery(session_id).await.unwrap().queue_events)
                    .is_empty()
            );
        }
        // Drop the guard before the scratch database is discarded: the
        // assertions need it, the teardown await must not hold it.
        {
            let prompts = prompts.lock().unwrap();
            assert_eq!(prompts.len(), usize::from(!cancel));
            if !cancel {
                assert!(prompts[0].contains(&serde_json::to_string(&prompt.text).unwrap()));
            }
        }
        scratch.discard().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn queued_retry_can_be_recalled_while_waiting_without_later_delivery() {
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let message_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(128);
    let calls = Arc::new(AtomicUsize::new(0));
    let executor = Arc::new(UsageLimitThenSuccessExecutor {
        calls: Arc::clone(&calls),
        side_effects_before_limit: false,
        prompts: Arc::default(),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn({
        let journal_path = root.path().join("session.lock");
        let cwd = root.path().to_path_buf();
        async move {
            run_session_actor(
                &journal_path,
                session_id,
                LaunchSession {
                    request_id: message_id,
                    cwd,
                    provider: CodingProvider::Codex,
                    model: None,
                    effort: None,
                    fast: Some(false),
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::Manual,
                    name: None,
                    initial_prompt: Some("finish this task".to_string()),
                    capabilities: Default::default(),
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                executor,
                actor_store,
            )
            .await
        }
    });

    loop {
        let event = tokio::time::timeout(Duration::from_secs(2), event_rx.recv())
            .await
            .unwrap()
            .unwrap();
        if matches!(event.kind, SessionEventKind::StatusChanged {
            status: SessionStatus::Ready, detail: Some(ref detail),
        } if detail.contains("usage limit"))
        {
            break;
        }
    }
    command_tx
        .send(HostCommand::RecallQueuedPrompt {
            session_id,
            message_id: Some(message_id),
        })
        .await
        .unwrap();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(2), event_rx.recv())
            .await
            .expect("idle retry recall is acknowledged")
            .unwrap();
        if let SessionEventKind::PromptRecalled {
            message_id: recalled,
            text,
            ..
        } = event.kind
        {
            assert_eq!(recalled, message_id);
            assert_eq!(text, "finish this task");
            break;
        }
    }
    tokio::time::sleep(USAGE_LIMIT_RETRY_INITIAL_DELAY * 3).await;

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();
    assert_eq!(calls.load(Ordering::Acquire), 1);
    scratch.discard().await;
}

#[tokio::test(flavor = "current_thread")]
async fn ready_is_emitted_only_after_all_queued_turn_events_are_complete() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let cwd = root.path().to_path_buf();
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let initial_message_id = Uuid::new_v4();
    let queued_message_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(128);
    let first_started = Arc::new(Notify::new());
    let release_first = Arc::new(Notify::new());
    let executor = Arc::new(PrematureReadyExecutor {
        first_started: Arc::clone(&first_started),
        release_first: Arc::clone(&release_first),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn(async move {
        run_session_actor(
            &journal_path,
            session_id,
            LaunchSession {
                request_id: initial_message_id,
                cwd,
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: Some(false),
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
                name: None,
                initial_prompt: Some("first".to_string()),
                capabilities: Default::default(),
                subagent_concurrency_limit: None,
                extension_skill_roots: Vec::new(),
                team_policy: None,
            },
            command_rx,
            event_tx,
            executor,
            actor_store,
        )
        .await
    });

    tokio::time::timeout(Duration::from_secs(1), first_started.notified())
        .await
        .expect("first turn starts");
    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: queued_message_id,
            text: "second".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Queue,
        })
        .await
        .unwrap();

    let mut observed = Vec::new();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .expect("queued event arrives")
            .expect("session event stream remains open");
        let queued = matches!(
            event.kind,
            SessionEventKind::Message {
                message_id,
                status: MessageStatus::Queued,
                ..
            } if message_id == queued_message_id
        );
        observed.push(event.kind);
        if queued {
            break;
        }
    }
    release_first.notify_one();

    while observed
        .iter()
        .filter(|kind| matches!(kind, SessionEventKind::TurnCompleted { .. }))
        .count()
        < 2
    {
        observed.push(
            tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
                .await
                .expect("turn event arrives")
                .expect("session event stream remains open")
                .kind,
        );
    }
    observed.push(
        tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .expect("canonical ready event arrives")
            .expect("session event stream remains open")
            .kind,
    );

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();

    let running = observed
        .iter()
        .enumerate()
        .filter_map(|(index, kind)| {
            matches!(
                kind,
                SessionEventKind::StatusChanged {
                    status: SessionStatus::Running,
                    ..
                }
            )
            .then_some(index)
        })
        .collect::<Vec<_>>();
    let ready = observed
        .iter()
        .enumerate()
        .filter_map(|(index, kind)| {
            matches!(
                kind,
                SessionEventKind::StatusChanged {
                    status: SessionStatus::Ready,
                    ..
                }
            )
            .then_some(index)
        })
        .collect::<Vec<_>>();
    let completed = observed
        .iter()
        .enumerate()
        .filter_map(|(index, kind)| {
            matches!(kind, SessionEventKind::TurnCompleted { .. }).then_some(index)
        })
        .collect::<Vec<_>>();

    assert_eq!(
        running.len(),
        4,
        "each turn must expose exactly one awaiting and one active phase"
    );
    assert_eq!(
        running
            .iter()
            .filter_map(|index| match &observed[*index] {
                SessionEventKind::StatusChanged { detail, .. } => detail.as_deref(),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![
            "turn phase: awaiting provider",
            "turn phase: provider active",
            "turn phase: awaiting provider",
            "turn phase: provider active",
        ],
        "executor lifecycle statuses stay filtered while Borg phases remain deterministic"
    );
    assert_eq!(completed.len(), 2);
    assert_eq!(ready.len(), 1, "executor Ready events must be filtered");
    assert!(
        ready[0] > completed[1],
        "Ready must follow the final queued TurnCompleted event"
    );
    scratch.discard().await;
}

#[tokio::test(flavor = "current_thread")]
async fn provider_setup_stall_has_a_durable_terminal_boundary() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let message_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(64);
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn({
        let journal_path = journal_path.clone();
        let cwd = root.path().to_path_buf();
        async move {
            run_session_actor(
                &journal_path,
                session_id,
                LaunchSession {
                    request_id: message_id,
                    cwd,
                    provider: CodingProvider::Codex,
                    model: None,
                    effort: None,
                    fast: Some(false),
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::Manual,
                    name: None,
                    initial_prompt: Some("hang".to_string()),
                    capabilities: Default::default(),
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                Arc::new(HungProviderExecutor),
                actor_store,
            )
            .await
        }
    });

    let mut observed = Vec::new();
    loop {
        // The event this waits for IS the setup-liveness timeout firing, so the
        // wait has to outlast that budget plus the watchdog's poll, not be a
        // flat two seconds. Derived from the constant so the two cannot drift.
        let event = tokio::time::timeout(
            PROVIDER_SETUP_LIVENESS_TIMEOUT + Duration::from_secs(2),
            event_rx.recv(),
        )
        .await
        .expect("liveness timeout is bounded")
        .expect("actor remains attached");
        let ready = matches!(
            event.kind,
            SessionEventKind::StatusChanged {
                status: SessionStatus::Ready,
                ..
            }
        );
        observed.push(event.kind);
        if ready {
            break;
        }
    }

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();

    assert!(observed.iter().any(|kind| matches!(
        kind,
        SessionEventKind::StatusChanged {
            status: SessionStatus::Running,
            detail: Some(detail),
        } if detail == TurnPhase::AwaitingProvider.detail()
    )));
    assert!(observed.iter().any(|kind| matches!(
        kind,
        SessionEventKind::TurnCompleted {
            message_id: completed,
            final_text,
            error: Some(error),
            ..
        } if *completed == message_id && final_text.is_empty()
            && error.contains("liveness timeout while awaiting provider")
    )));

    let durable = PostgresSessionStore::connect_with_pool_size(&scratch.url, 2)
        .await
        .unwrap();
    let durable = durable.read(session_id).await.unwrap();
    assert!(durable.iter().any(|event| matches!(
        &event.kind,
        SessionEventKind::TurnCompleted {
            message_id: completed,
            error: Some(error),
            ..
        } if *completed == message_id
            && error.contains("liveness timeout while awaiting provider")
    )));
    scratch.discard().await;
}

#[tokio::test(flavor = "current_thread")]
async fn intermediate_narration_does_not_start_provider_drain_timeout() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let message_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(64);
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn({
        let journal_path = journal_path.clone();
        let cwd = root.path().to_path_buf();
        async move {
            run_session_actor(
                &journal_path,
                session_id,
                LaunchSession {
                    request_id: message_id,
                    cwd,
                    provider: CodingProvider::Claude,
                    model: None,
                    effort: None,
                    fast: Some(false),
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::Manual,
                    name: None,
                    initial_prompt: Some("consult the peer".to_string()),
                    capabilities: Default::default(),
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                Arc::new(NarrationThenDelayedCompletionExecutor),
                actor_store,
            )
            .await
        }
    });

    let mut observed = Vec::new();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(5), event_rx.recv())
            .await
            .expect("delayed consultation remains live");
        let Some(event) = event else {
            panic!("session ended early: {:?}", actor.await.unwrap());
        };
        let ready = matches!(
            event.kind,
            SessionEventKind::StatusChanged {
                status: SessionStatus::Ready,
                ..
            }
        );
        observed.push(event.kind);
        if ready {
            break;
        }
    }

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();

    assert!(observed.iter().any(|kind| matches!(
        kind,
        SessionEventKind::TurnCompleted {
            message_id: completed_id,
            final_text,
            error: None,
            ..
        } if *completed_id == message_id && final_text == "consultation complete"
    )));
    assert!(!observed.iter().any(|kind| matches!(
        kind,
        SessionEventKind::Error { message }
            if message.contains("provider draining")
    )));
    scratch.discard().await;
}

#[tokio::test(flavor = "current_thread")]
async fn executor_ready_has_a_bounded_provider_drain() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let message_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(64);
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn({
        let journal_path = journal_path.clone();
        let cwd = root.path().to_path_buf();
        async move {
            run_session_actor(
                &journal_path,
                session_id,
                LaunchSession {
                    request_id: message_id,
                    cwd,
                    provider: CodingProvider::Claude,
                    model: None,
                    effort: None,
                    fast: Some(false),
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::Manual,
                    name: None,
                    initial_prompt: Some("finish and hang".to_string()),
                    capabilities: Default::default(),
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                Arc::new(ReadyThenHungExecutor),
                actor_store,
            )
            .await
        }
    });

    let mut observed = Vec::new();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(2), event_rx.recv())
            .await
            .expect("provider drain timeout is bounded")
            .expect("session remains attached");
        let ready = matches!(
            event.kind,
            SessionEventKind::StatusChanged {
                status: SessionStatus::Ready,
                ..
            }
        );
        observed.push(event.kind);
        if ready {
            break;
        }
    }

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();

    let completed = observed.iter().position(|kind| {
        matches!(
            kind,
            SessionEventKind::TurnCompleted {
                message_id: completed_id,
                error: Some(error),
                ..
            } if *completed_id == message_id && error.contains("provider draining")
        )
    });
    let ready = observed.iter().position(|kind| {
        matches!(
            kind,
            SessionEventKind::StatusChanged {
                status: SessionStatus::Ready,
                ..
            }
        )
    });
    assert!(
        completed.is_some(),
        "draining timeout must complete the turn"
    );
    assert!(
        ready.is_some_and(|ready| ready > completed.unwrap()),
        "Ready must follow the draining TurnCompleted boundary"
    );
    assert!(observed.iter().any(|kind| matches!(
        kind,
        SessionEventKind::Message {
            actor: EventActor::Assistant,
            text,
            status: MessageStatus::Complete,
            ..
        } if text == "final answer"
    )));
    scratch.discard().await;
}

#[tokio::test(flavor = "current_thread")]
async fn idle_immediate_input_is_persisted_as_queue_before_turn_admission() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let cwd = root.path().to_path_buf();
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let message_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(64);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let called = Arc::new(Notify::new());
    let executor = Arc::new(RecordingExecutor {
        seen: Arc::clone(&seen),
        called,
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn(async move {
        run_session_actor(
            &journal_path,
            session_id,
            LaunchSession {
                request_id: Uuid::new_v4(),
                cwd,
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: Some(false),
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
                name: None,
                initial_prompt: None,
                capabilities: Default::default(),
                subagent_concurrency_limit: None,
                extension_skill_roots: Vec::new(),
                team_policy: None,
            },
            command_rx,
            event_tx,
            executor,
            actor_store,
        )
        .await
    });

    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id,
            text: "start this idle turn".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();

    let mut in_progress_delivery = None;
    loop {
        let event = tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .expect("idle prompt reaches a terminal turn boundary")
            .expect("session remains attached");
        if let SessionEventKind::Message {
            message_id: event_message_id,
            status: MessageStatus::InProgress,
            delivery,
            ..
        } = &event.kind
            && *event_message_id == message_id
        {
            in_progress_delivery = *delivery;
        }
        if matches!(
            event.kind,
            SessionEventKind::TurnCompleted {
                message_id: completed_id,
                error: None,
                ..
            } if completed_id == message_id
        ) {
            break;
        }
    }

    assert_eq!(in_progress_delivery, Some(PromptDelivery::Queue));
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();

    let store = PostgresSessionStore::connect_with_pool_size(&scratch.url, 2)
        .await
        .unwrap();
    let action = store.action(session_id, message_id).await.unwrap().unwrap();
    assert_eq!(action.kind, crate::SessionActionKind::Prompt);
    assert_eq!(action.state, crate::SessionActionState::Completed);
    assert_eq!(seen.lock().unwrap().len(), 1);
    scratch.discard().await;
}

#[tokio::test(flavor = "current_thread")]
async fn stopping_an_active_turn_marks_its_prompt_failed() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let message_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(64);
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn({
        let journal_path = journal_path.clone();
        let cwd = root.path().to_path_buf();
        async move {
            run_session_actor(
                &journal_path,
                session_id,
                LaunchSession {
                    request_id: message_id,
                    cwd,
                    provider: CodingProvider::Codex,
                    model: None,
                    effort: None,
                    fast: Some(false),
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::Manual,
                    name: None,
                    initial_prompt: None,
                    capabilities: Default::default(),
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                Arc::new(HungProviderExecutor),
                actor_store,
            )
            .await
        }
    });

    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id,
            text: "stop me".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Queue,
        })
        .await
        .unwrap();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .expect("prompt enters the active turn")
            .expect("session remains open");
        if matches!(
            event.kind,
            SessionEventKind::Message {
                message_id: event_message_id,
                status: MessageStatus::InProgress,
                ..
            } if event_message_id == message_id
        ) {
            break;
        }
    }

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();

    let mut terminal_events = Vec::new();
    while let Some(event) = event_rx.recv().await {
        terminal_events.push(event.kind);
    }
    assert!(terminal_events.iter().any(|kind| matches!(
        kind,
        SessionEventKind::Message {
            message_id: event_message_id,
            status: MessageStatus::Failed,
            ..
        } if *event_message_id == message_id
    )));
    assert!(terminal_events.iter().any(|kind| matches!(
        kind,
        SessionEventKind::TurnCompleted {
            message_id: event_message_id,
            error: Some(error),
            ..
        } if *event_message_id == message_id && error == "session stopped during turn"
    )));

    let durable = PostgresSessionStore::connect_with_pool_size(&scratch.url, 2)
        .await
        .unwrap();
    let durable = durable.read(session_id).await.unwrap();
    assert!(durable.iter().any(|event| matches!(
        &event.kind,
        SessionEventKind::Message {
            message_id: event_message_id,
            status: MessageStatus::Failed,
            ..
        } if *event_message_id == message_id
    )));
    scratch.discard().await;
}

#[tokio::test(flavor = "current_thread")]
async fn detached_live_projection_cannot_block_durable_turn_terminalization() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(2);
    let (event_tx, event_rx) = mpsc::channel(1);
    drop(event_rx);
    let (scratch, session_store) = crate::session_store::postgres::testing::session_store().await;
    let session_store: Arc<dyn SessionStore> = Arc::new(session_store);
    session_store.create_session(session_id).await.unwrap();
    let (actor_result_tx, mut actor_result_rx) = tokio::sync::oneshot::channel();
    let actor_store = Arc::clone(&session_store);
    let actor = tokio::spawn({
        let cwd = root.path().to_path_buf();
        async move {
            let result = run_session_actor(
                &journal_path,
                session_id,
                LaunchSession {
                    request_id: message_id,
                    cwd,
                    provider: CodingProvider::Codex,
                    model: None,
                    effort: None,
                    fast: Some(false),
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::Manual,
                    name: None,
                    initial_prompt: Some("hang while detached".to_string()),
                    capabilities: Default::default(),
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                Arc::new(HungProviderExecutor),
                actor_store,
            )
            .await;
            let _ = actor_result_tx.send(result.as_ref().map(|_| ()).map_err(ToString::to_string));
            result
        }
    });

    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let durable = session_store.read(session_id).await.unwrap();
            if durable.iter().any(|event| {
                matches!(
                    &event.kind,
                    SessionEventKind::TurnStarted {
                        message_id: started,
                        ..
                    } if *started == message_id
                )
            }) {
                break;
            }
            tokio::select! {
                result = &mut actor_result_rx => {
                    panic!("detached actor exited before the durable turn boundary: {result:?}");
                }
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
    })
    .await
    .expect("detached actor can reach the durable turn boundary");
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let durable = session_store.read(session_id).await.unwrap();
            if durable.iter().any(|event| {
                matches!(
                    &event.kind,
                    SessionEventKind::TurnCompleted {
                        message_id: completed,
                        error: Some(error),
                        ..
                    } if *completed == message_id && error.contains("liveness timeout")
                )
            }) {
                break;
            }
            tokio::select! {
                result = &mut actor_result_rx => {
                    panic!("detached actor exited before durable terminalization: {result:?}");
                }
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
    })
    .await
    .expect("detached consumer can recover the durable timeout boundary");
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), actor)
        .await
        .expect("detached projection cannot wedge the actor")
        .unwrap()
        .unwrap();

    let durable = session_store.read(session_id).await.unwrap();
    assert!(durable.iter().any(|event| matches!(
        &event.kind,
        SessionEventKind::TurnCompleted {
            message_id: completed,
            error: Some(error),
            ..
        } if *completed == message_id && error.contains("liveness timeout")
    )));
    assert!(matches!(
        durable.last().map(|event| &event.kind),
        Some(SessionEventKind::StatusChanged {
            status: SessionStatus::Stopped,
            ..
        })
    ));
    scratch.discard().await;
}

#[tokio::test(flavor = "current_thread")]
async fn all_queued_prompts_can_be_recalled_at_the_turn_completion_boundary() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let queued_message_ids = [Uuid::new_v4(), Uuid::new_v4()];
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(64);
    let turns = Arc::new(Mutex::new(Vec::new()));
    let first_started = Arc::new(Notify::new());
    let release_first = Arc::new(Notify::new());
    let executor = Arc::new(BoundaryQueueExecutor {
        turns: Arc::clone(&turns),
        first_started: Arc::clone(&first_started),
        release_first: Arc::clone(&release_first),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn(async move {
        run_session_actor(
            &journal_path,
            session_id,
            LaunchSession {
                request_id: Uuid::new_v4(),
                cwd: root.path().to_path_buf(),
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: Some(false),
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
                name: None,
                initial_prompt: None,
                capabilities: Default::default(),
                subagent_concurrency_limit: None,
                extension_skill_roots: Vec::new(),
                team_policy: None,
            },
            command_rx,
            event_tx,
            executor,
            actor_store,
        )
        .await
    });

    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: Uuid::new_v4(),
            text: "first".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), first_started.notified())
        .await
        .expect("first turn starts");
    for (message_id, text) in queued_message_ids
        .into_iter()
        .zip(["recall first", "recall second"])
    {
        command_tx
            .send(HostCommand::Prompt {
                session_id,
                message_id,
                text: text.to_string(),
                attachments: Vec::new(),
                output_schema: None,
                delivery: PromptDelivery::Queue,
            })
            .await
            .unwrap();
    }
    let mut queued = Vec::new();
    while queued.len() < queued_message_ids.len() {
        let event = tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .expect("queued event arrives")
            .expect("session event stream remains open");
        if let SessionEventKind::Message {
            message_id,
            status: MessageStatus::Queued,
            ..
        } = event.kind
            && queued_message_ids.contains(&message_id)
        {
            queued.push(message_id);
        }
    }

    release_first.notify_one();
    tokio::task::yield_now().await;
    command_tx
        .send(HostCommand::RecallQueuedPrompt {
            session_id,
            message_id: None,
        })
        .await
        .unwrap();
    let mut recalled = Vec::new();
    while recalled.len() < queued_message_ids.len() {
        let event = tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .expect("recall event arrives")
            .expect("session event stream remains open");
        if let SessionEventKind::PromptRecalled { message_id, .. } = event.kind
            && queued_message_ids.contains(&message_id)
        {
            recalled.push(message_id);
        }
    }
    assert_eq!(recalled, queued_message_ids);

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();
    assert_eq!(
            turns.lock().unwrap().as_slice(),
            [(
                "Borg canonical provider context v2. The history below is a read-only, provider-neutral projection of durable Borg state; answer the current request normally.\n<borg-message>{\"content\":\"first\",\"role\":\"user\"}</borg-message>".to_string(),
                Vec::new()
            )]
        );
    scratch.discard().await;
}

#[tokio::test(flavor = "current_thread")]
async fn recalled_queue_prompt_is_not_started_after_the_pre_turn_handoff() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let queued_message_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel::<SessionEvent>(64);
    let turns = Arc::new(Mutex::new(Vec::new()));
    let first_started = Arc::new(Notify::new());
    let release_first = Arc::new(Notify::new());
    let queued_ready = Arc::new(Notify::new());
    let handoff_ready = Arc::new(Notify::new());
    let release_handoff = Arc::new(Notify::new());
    let recalled_seen = Arc::new(Notify::new());
    let cwd = root.path().to_path_buf();
    let executor = Arc::new(BoundaryQueueExecutor {
        turns: Arc::clone(&turns),
        first_started: Arc::clone(&first_started),
        release_first: Arc::clone(&release_first),
    });
    let collector = tokio::spawn({
        let queued_ready = Arc::clone(&queued_ready);
        let handoff_ready = Arc::clone(&handoff_ready);
        let release_handoff = Arc::clone(&release_handoff);
        let recalled_seen = Arc::clone(&recalled_seen);
        async move {
            while let Some(event) = event_rx.recv().await {
                match &event.kind {
                    SessionEventKind::Message {
                        message_id,
                        status: MessageStatus::Queued,
                        ..
                    } if *message_id == queued_message_id => queued_ready.notify_one(),
                    SessionEventKind::Message {
                        message_id,
                        status: MessageStatus::InProgress,
                        ..
                    } if *message_id == queued_message_id => {
                        handoff_ready.notify_one();
                        release_handoff.notified().await;
                    }
                    SessionEventKind::PromptRecalled { message_id, .. }
                        if *message_id == queued_message_id =>
                    {
                        recalled_seen.notify_one()
                    }
                    _ => {}
                }
            }
        }
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn(async move {
        run_session_actor(
            &journal_path,
            session_id,
            LaunchSession {
                request_id: Uuid::new_v4(),
                cwd,
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: Some(false),
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
                name: None,
                initial_prompt: None,
                capabilities: Default::default(),
                subagent_concurrency_limit: None,
                extension_skill_roots: Vec::new(),
                team_policy: None,
            },
            command_rx,
            event_tx,
            executor,
            actor_store,
        )
        .await
    });

    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: Uuid::new_v4(),
            text: "first".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), first_started.notified())
        .await
        .expect("first turn starts");
    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: queued_message_id,
            text: "recall during handoff".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Queue,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), queued_ready.notified())
        .await
        .expect("queued prompt is durable before the turn ends");
    release_first.notify_one();

    tokio::time::timeout(Duration::from_secs(1), handoff_ready.notified())
        .await
        .expect("queued prompt reaches the pre-turn handoff");
    command_tx
        .send(HostCommand::RecallQueuedPrompt {
            session_id,
            message_id: Some(queued_message_id),
        })
        .await
        .unwrap();
    release_handoff.notify_one();
    tokio::time::timeout(Duration::from_secs(1), recalled_seen.notified())
        .await
        .expect("recall is durable before provider admission");

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();
    collector.await.unwrap();

    assert_eq!(turns.lock().unwrap().len(), 1);
    let store = PostgresSessionStore::connect_with_pool_size(&scratch.url, 2)
        .await
        .unwrap();
    assert_eq!(
        store
            .action(session_id, queued_message_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        crate::SessionActionState::Cancelled
    );
    assert!(
        !store
            .read(session_id)
            .await
            .unwrap()
            .iter()
            .any(|event| matches!(
                event.kind,
                SessionEventKind::TurnStarted {
                    message_id,
                    ..
                } if message_id == queued_message_id
            ))
    );
    scratch.discard().await;
}

#[tokio::test(flavor = "current_thread")]
async fn multiple_queue_mode_prompts_drain_fifo_after_a_natural_turn_boundary() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let queued_message_ids = [Uuid::new_v4(), Uuid::new_v4()];
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(64);
    let turns = Arc::new(Mutex::new(Vec::new()));
    let first_started = Arc::new(Notify::new());
    let release_first = Arc::new(Notify::new());
    let executor = Arc::new(BoundaryQueueExecutor {
        turns: Arc::clone(&turns),
        first_started: Arc::clone(&first_started),
        release_first: Arc::clone(&release_first),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn(async move {
        run_session_actor(
            &journal_path,
            session_id,
            LaunchSession {
                request_id: Uuid::new_v4(),
                cwd: root.path().to_path_buf(),
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: Some(false),
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
                name: None,
                initial_prompt: None,
                capabilities: Default::default(),
                subagent_concurrency_limit: None,
                extension_skill_roots: Vec::new(),
                team_policy: None,
            },
            command_rx,
            event_tx,
            executor,
            actor_store,
        )
        .await
    });

    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: Uuid::new_v4(),
            text: "first".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), first_started.notified())
        .await
        .expect("first turn starts");

    for (message_id, text) in queued_message_ids.iter().copied().zip(["second", "third"]) {
        command_tx
            .send(HostCommand::Prompt {
                session_id,
                message_id,
                text: text.to_string(),
                attachments: Vec::new(),
                output_schema: None,
                delivery: PromptDelivery::Queue,
            })
            .await
            .unwrap();
    }
    let mut queued = Vec::new();
    while queued.len() < queued_message_ids.len() {
        let event = tokio::time::timeout(Duration::from_secs(5), event_rx.recv())
            .await
            .expect("queued event arrives")
            .expect("session event stream remains open");
        if let SessionEventKind::Message {
            message_id,
            status: MessageStatus::Queued,
            ..
        } = event.kind
            && queued_message_ids.contains(&message_id)
        {
            queued.push(message_id);
        }
    }

    // Releasing the first turn must batch all queue-mode prompts at the
    // natural boundary. They are one provider input, in FIFO text order.
    release_first.notify_one();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if turns.lock().unwrap().len() == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all queued turns drain");
    assert_eq!(
        turns
            .lock()
            .unwrap()
            .iter()
            .map(|(text, _)| text.as_str())
            .collect::<Vec<_>>(),
        [
            "Borg canonical provider context v2. The history below is a read-only, provider-neutral projection of durable Borg state; answer the current request normally.\n<borg-message>{\"content\":\"first\",\"role\":\"user\"}</borg-message>",
            "Borg canonical provider context v2. The history below is a read-only, provider-neutral projection of durable Borg state; answer the current request normally.\n<borg-message>{\"content\":\"first\",\"role\":\"user\"}</borg-message>\n<borg-message>{\"content\":\"second\\n\\nthird\",\"role\":\"user\"}</borg-message>",
        ]
    );

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();
    scratch.discard().await;
}

#[tokio::test]
async fn interrupted_turn_reaches_fifo_drain_boundary() {
    assert_interrupted_fifo(CodingProvider::Codex, None, true).await;
}

#[tokio::test]
async fn claude_interrupt_preserves_queue_and_only_normalizes_expected_aborts() {
    for (error, expected) in [
        (
            "claude SDK error_during_execution: claude SDK returned subtype=error_during_execution",
            true,
        ),
        (
            r#"claude SDK error_during_execution: stopped "terminal_reason":"aborted_tools""#,
            true,
        ),
        (
            "claude SDK error_during_execution: permission denied",
            false,
        ),
    ] {
        assert_interrupted_fifo(CodingProvider::Claude, Some(error), expected).await;
    }
}

#[tokio::test]
async fn escape_flush_opencode_redirects_pending_input_without_user_stop() {
    assert_interrupted_fifo(CodingProvider::OpenCode, None, true).await;
}

async fn assert_interrupted_fifo(
    provider: CodingProvider,
    abort_error: Option<&'static str>,
    expected_interrupt: bool,
) {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(128);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let provider_sessions = Arc::new(Mutex::new(Vec::new()));
    let called = Arc::new(Notify::new());
    let executor = Arc::new(InterruptibleQueueExecutor {
        abort_error,
        seen: Arc::clone(&seen),
        provider_sessions: Arc::clone(&provider_sessions),
        called: Arc::clone(&called),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn(async move {
        run_session_actor(
            &journal_path,
            session_id,
            LaunchSession {
                request_id: Uuid::new_v4(),
                cwd: root.path().to_path_buf(),
                provider,
                model: None,
                effort: None,
                fast: Some(false),
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
                name: None,
                initial_prompt: None,
                capabilities: Default::default(),
                subagent_concurrency_limit: None,
                extension_skill_roots: Vec::new(),
                team_policy: None,
            },
            command_rx,
            event_tx,
            executor,
            actor_store,
        )
        .await
    });

    for (text, attachments, delivery) in [
        ("first", Vec::new(), PromptDelivery::Steer),
        (
            "second [Image 1]",
            vec![PathBuf::from("/tmp/queued-image.png")],
            PromptDelivery::Queue,
        ),
        ("third", Vec::new(), PromptDelivery::Queue),
    ] {
        command_tx
            .send(HostCommand::Prompt {
                session_id,
                message_id: Uuid::new_v4(),
                text: text.to_string(),
                attachments,
                output_schema: None,
                delivery,
            })
            .await
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(5), called.notified())
        .await
        .expect("first turn starts");
    command_tx
        .send(if provider == CodingProvider::OpenCode {
            HostCommand::FlushPendingInput { session_id }
        } else {
            HostCommand::Interrupt { session_id }
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), called.notified())
        .await
        .expect("queued turn starts after interruption");
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();

    let events = std::iter::from_fn(|| event_rx.try_recv().ok()).collect::<Vec<_>>();
    assert_eq!(
        events.iter().any(|event| matches!(
            &event.kind,
            SessionEventKind::TurnCompleted {
                error: Some(error),
                ..
            } if error.starts_with("turn interrupted")
        )),
        expected_interrupt
    );
    assert!(!events.iter().any(|event| matches!(
        &event.kind,
        SessionEventKind::Error { message }
            if message.contains("provider completed without a visible response")
    )));

    if let Some(error) = abort_error {
        assert_eq!(
            events.iter().any(|event| matches!(
                &event.kind, SessionEventKind::Error { message } if message == error
            )),
            !expected_interrupt
        );
    }

    if provider == CodingProvider::OpenCode {
        assert!(!events.iter().any(|event| matches!(
            event.kind,
            SessionEventKind::UserStopChanged { engaged: true, .. }
        )));
    }
    // Drop the guard before the scratch database is discarded: the
    // assertions need it, the teardown await must not hold it.
    {
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(
                seen[0],
                (
                    "Borg canonical provider context v2. The history below is a read-only, provider-neutral projection of durable Borg state; answer the current request normally.\n<borg-message>{\"content\":\"first\",\"role\":\"user\"}</borg-message>".to_string(),
                    Vec::new()
                )
            );
        if provider == CodingProvider::Codex {
            assert_eq!(
                seen[1].0,
                format_subscription_frame(&format_subscription_actor_value(
                    EventActor::User,
                    "second [Image 1]\n\nthird"
                ))
            );
        } else {
            // Escaped rather than a raw multi-line literal: this assertion
            // is nested one block deeper than it used to be, and the
            // re-indent that moved it silently indented the literal's second
            // line too, so the expected text grew four spaces it never had.
            assert!(subscription_prompt_ends_with(
                &seen[1].0,
                "second [Image 1]\n\nthird"
            ));
        }
        assert_eq!(
            seen[1].1,
            [PathBuf::from("/tmp/queued-image.png")],
            "queued image attachments must stay on their FIFO prompt"
        );
        if provider == CodingProvider::Codex {
            assert_eq!(
                provider_sessions.lock().unwrap().as_slice(),
                [None, Some("provider-session".to_string())],
                "interrupting a Codex turn must preserve its provider thread"
            );
        }
    }
    scratch.discard().await;
}

#[tokio::test]
async fn user_stop_gate_holds_background_turns_until_a_human_prompt() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let (command_tx, command_rx) = mpsc::channel(16);
    let (event_tx, mut event_rx) = mpsc::channel(256);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let provider_sessions = Arc::new(Mutex::new(Vec::new()));
    let called = Arc::new(Notify::new());
    let executor = Arc::new(InterruptibleQueueExecutor {
        abort_error: None,
        seen: Arc::clone(&seen),
        provider_sessions: Arc::clone(&provider_sessions),
        called: Arc::clone(&called),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn(async move {
        run_session_actor(
            &journal_path,
            session_id,
            LaunchSession {
                request_id: Uuid::new_v4(),
                cwd: root.path().to_path_buf(),
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: Some(false),
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
                name: None,
                initial_prompt: None,
                capabilities: Default::default(),
                subagent_concurrency_limit: None,
                extension_skill_roots: Vec::new(),
                team_policy: None,
            },
            command_rx,
            event_tx,
            executor,
            actor_store,
        )
        .await
    });

    // A human turn is running, with one more prompt queued behind it.
    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: Uuid::new_v4(),
            text: "first".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), called.notified())
        .await
        .expect("first turn starts");
    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: Uuid::new_v4(),
            text: "queued-before-escape".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Queue,
        })
        .await
        .unwrap();

    // The human presses Escape.
    command_tx
        .send(HostCommand::Interrupt { session_id })
        .await
        .unwrap();

    // Input queued before the Escape still runs, but it must not count as the
    // human re-engaging: the gate stays latched afterwards.
    tokio::time::timeout(Duration::from_secs(5), called.notified())
        .await
        .expect("the prompt queued before Escape still runs");

    // A subagent reply arrives as a Steer team prompt while the session is
    // stopped. It must stay visible but never open a provider turn.
    command_tx
        .send(HostCommand::TeamPrompt {
            session_id,
            message_id: Uuid::new_v4(),
            text: "Team message from /root/worker:\n\nbackground report".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(750), called.notified())
            .await
            .is_err(),
        "a stopped session must not admit a background team turn"
    );
    assert_eq!(
        seen.lock().unwrap().len(),
        2,
        "provider saw only the human turns"
    );

    // A fresh human prompt after the stop clears the gate.
    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: Uuid::new_v4(),
            text: "second".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Queue,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), called.notified())
        .await
        .expect("an explicit human prompt clears the gate and starts a turn");
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();
    let state = store.state(session_id).await.unwrap();
    assert_eq!(state.title.as_deref(), Some("first"));
    assert!(!state.title_generated);

    // Drop the guard before the scratch database is discarded: the
    // assertions need it, the teardown await must not hold it.
    {
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 3);
        assert!(seen[1].0.contains("queued-before-escape"));
        assert!(seen[2].0.contains("second"));
        assert!(
            seen[..2]
                .iter()
                .all(|turn| !turn.0.contains("background report")),
            "the held team report never became a provider turn"
        );
        let resumed_prompt = &seen[2].0;
        assert!(
            resumed_prompt
                .find("background report")
                .is_some_and(|index| index < resumed_prompt.find("second").unwrap()),
            "the held report becomes context only when the human resumes"
        );

        let events = std::iter::from_fn(|| event_rx.try_recv().ok()).collect::<Vec<_>>();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event.kind, SessionEventKind::SessionTitled { .. }))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    &event.kind,
                    SessionEventKind::UserStopChanged { engaged: true }
                ))
                .count(),
            1,
            "Escape engages the durable gate exactly once"
        );
        let stop_index = events
            .iter()
            .position(|event| {
                matches!(
                    &event.kind,
                    SessionEventKind::UserStopChanged { engaged: true }
                )
            })
            .expect("stop engaged");
        let clear_index = events
            .iter()
            .position(|event| {
                matches!(
                    &event.kind,
                    SessionEventKind::UserStopChanged { engaged: false }
                )
            })
            .expect("stop cleared by the fresh human prompt");
        assert!(stop_index < clear_index);
        let between = &events[stop_index..clear_index];
        assert!(
            between
                .iter()
                .any(|event| matches!(&event.kind, SessionEventKind::TurnStarted { .. })),
            "the prompt queued before Escape runs while the gate is still latched"
        );
        assert!(
            events[..clear_index].iter().any(|event| matches!(
                &event.kind,
                SessionEventKind::Message {
                    actor: EventActor::System,
                    status: MessageStatus::Complete,
                    text,
                    ..
                } if text.contains("background report")
            )),
            "the held team report is settled visible before the human resumes"
        );
    }
    scratch.discard().await;
}

#[tokio::test]
async fn user_stop_gate_is_re_engaged_from_durable_state_after_actor_restart() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let first_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    store.create_session(session_id).await.unwrap();
    // A prior actor generation ran a turn and then the human pressed Escape.
    for kind in [
        SessionEventKind::SessionStarted,
        SessionEventKind::SessionConfigured {
            cwd: root.path().to_path_buf(),
            provider: CodingProvider::Codex,
            model: None,
            effort: None,
            fast: false,
            response_language: crate::ResponseLanguage::Auto,
            permission_mode: PermissionMode::Manual,
        },
        SessionEventKind::Message {
            message_id: first_id,
            actor: EventActor::User,
            text: "first".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: Some(PromptDelivery::Queue),
        },
        SessionEventKind::TurnStarted {
            message_id: first_id,
            provider: CodingProvider::Codex,
            model: None,
            effort: None,
            fast: false,
        },
        SessionEventKind::ProviderSessionLinked {
            provider_session_id: "codex-thread".to_string(),
            provider_turn_id: Some("codex-turn".to_string()),
            context_contract_version: Some(crate::agent::PROVIDER_CONTEXT_CONTRACT_VERSION),
        },
        SessionEventKind::TurnCompleted {
            message_id: first_id,
            provider_session_id: Some("codex-thread".to_string()),
            final_text: String::new(),
            error: Some("turn interrupted".to_string()),
        },
        SessionEventKind::StatusChanged {
            status: SessionStatus::Ready,
            detail: Some("Interrupted".to_string()),
        },
        SessionEventKind::UserStopChanged { engaged: true },
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }

    let (command_tx, command_rx) = mpsc::channel(16);
    let (event_tx, mut event_rx) = mpsc::channel(256);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let provider_sessions = Arc::new(Mutex::new(Vec::new()));
    let called = Arc::new(Notify::new());
    let executor = Arc::new(InterruptibleQueueExecutor {
        abort_error: None,
        seen: Arc::clone(&seen),
        provider_sessions: Arc::clone(&provider_sessions),
        called: Arc::clone(&called),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn(async move {
        run_session_actor(
            &journal_path,
            session_id,
            LaunchSession {
                request_id: Uuid::new_v4(),
                cwd: root.path().to_path_buf(),
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: Some(false),
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
                name: None,
                initial_prompt: None,
                capabilities: Default::default(),
                subagent_concurrency_limit: None,
                extension_skill_roots: Vec::new(),
                team_policy: None,
            },
            command_rx,
            event_tx,
            executor,
            actor_store,
        )
        .await
    });

    // The restarted actor never saw the Escape in-process; it must re-engage
    // the gate purely from the durable `UserStopChanged` event.
    command_tx
        .send(HostCommand::TeamPrompt {
            session_id,
            message_id: Uuid::new_v4(),
            text: "Team message from /root/worker:\n\npost-restart report".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(750), called.notified())
            .await
            .is_err(),
        "a reloaded stopped session must not admit a background team turn"
    );
    assert!(seen.lock().unwrap().is_empty());

    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: Uuid::new_v4(),
            text: "second".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Queue,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), called.notified())
        .await
        .expect("an explicit human prompt clears the reloaded gate");
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();

    assert_eq!(seen.lock().unwrap().len(), 1);
    let events = std::iter::from_fn(|| event_rx.try_recv().ok()).collect::<Vec<_>>();
    assert!(events.iter().any(|event| matches!(
        &event.kind,
        SessionEventKind::Message {
            actor: EventActor::System,
            status: MessageStatus::Complete,
            text,
            ..
        } if text.contains("post-restart report")
    )));
    assert!(events.iter().any(|event| matches!(
        &event.kind,
        SessionEventKind::UserStopChanged { engaged: false }
    )));
    scratch.discard().await;
}

#[tokio::test]
async fn interrupt_timeout_cannot_publish_ready_before_provider_cleanup_finishes() {
    assert_interrupt_waits_for_cleanup(false).await;
}

#[tokio::test]
async fn cooperative_native_interrupt_waits_for_process_cleanup_before_completion() {
    assert_interrupt_waits_for_cleanup(true).await;
}

async fn assert_interrupt_waits_for_cleanup(cooperative: bool) {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(64);
    let started = Arc::new(Notify::new());
    let cleanup_started = Arc::new(Notify::new());
    let release_cleanup = Arc::new(Notify::new());
    let executor = Arc::new(CleanupBarrierExecutor {
        cooperative,
        started: Arc::clone(&started),
        cleanup_started: Arc::clone(&cleanup_started),
        release_cleanup: Arc::clone(&release_cleanup),
        cleanup_calls: AtomicUsize::new(0),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn(async move {
        run_session_actor(
            &journal_path,
            session_id,
            LaunchSession {
                request_id: Uuid::new_v4(),
                cwd: root.path().to_path_buf(),
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: Some(false),
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
                name: None,
                initial_prompt: None,
                capabilities: Default::default(),
                subagent_concurrency_limit: None,
                extension_skill_roots: Vec::new(),
                team_policy: None,
            },
            command_rx,
            event_tx,
            executor,
            actor_store,
        )
        .await
    });

    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: Uuid::new_v4(),
            text: "run until interrupted".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), started.notified())
        .await
        .expect("provider starts");
    while event_rx.try_recv().is_ok() {}

    command_tx
        .send(HostCommand::Interrupt { session_id })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(4), cleanup_started.notified())
        .await
        .expect("interrupt enters provider cleanup");

    let mut before_cleanup = Vec::new();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .expect("cancelling is visible while cleanup remains blocked")
            .expect("session remains open");
        let cancelling = matches!(
            &event.kind,
            SessionEventKind::StatusChanged { detail: Some(detail), .. }
                if detail == "turn phase: cancelling"
        );
        before_cleanup.push(event);
        if cancelling {
            break;
        }
    }
    assert!(before_cleanup.iter().any(|event| matches!(
        &event.kind,
        SessionEventKind::StatusChanged {
            status: SessionStatus::Running,
            detail: Some(detail),
        } if detail == "turn phase: cancelling"
    )));
    assert!(!before_cleanup.iter().any(|event| matches!(
        event.kind,
        SessionEventKind::StatusChanged {
            status: SessionStatus::Ready,
            ..
        } | SessionEventKind::TurnCompleted { .. }
    )));

    release_cleanup.notify_one();
    let mut saw_turn_completed = false;
    loop {
        let event = tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .expect("terminal event after cleanup")
            .expect("session remains open");
        match event.kind {
            SessionEventKind::TurnCompleted { .. } => saw_turn_completed = true,
            SessionEventKind::StatusChanged {
                status: SessionStatus::Ready,
                detail: Some(detail),
            } if detail == "Interrupted" => {
                assert!(saw_turn_completed, "Ready must follow TurnCompleted");
                break;
            }
            _ => {}
        }
    }

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();
    scratch.discard().await;
}

/// Failure mode this pins: with provider cleanup unresponsive, a human who
/// presses Escape gets no terminal boundary until the cleanup bound expires.
/// That bound used to be the watchdog's, which is sized for recovering a wedged
/// provider unattended, so the cancelling footer could sit on screen for tens of
/// seconds after the keypress.
///
/// The ordering invariant is deliberately NOT changed here: `Ready` still
/// follows cleanup, which
/// `interrupt_timeout_cannot_publish_ready_before_provider_cleanup_finishes`
/// pins. Only the wait is human-sized. Note what this test does and does not
/// claim: it proves the human is answered promptly, NOT that the processes were
/// reaped -- an expired bound reports that they may still be running.
///
/// Measured on the real clock, deliberately, and not on a paused one.
///
/// A paused clock is the usual way to make a timing assertion deterministic,
/// but it cannot work here and narrowing the paused window does not rescue it.
/// The interval this test measures is not made of timers: between Escape and
/// the boundary the actor latches the stop gate and records `TurnCompleted`,
/// both Postgres round trips. While either is outstanding no task is runnable,
/// so a paused clock advances to the next armed timer -- and the turn loop's
/// watchdog poll re-arms every 20ms under cfg(test), so one slow real write can
/// be converted into an unbounded run of simulated advances. The reading it
/// would corrupt is exactly the one asserted below.
///
/// What keeps real time honest here is that the assertion is a ratio, not an
/// absolute: the boundary must land short of the watchdog budget this path used
/// to inherit. Under cfg(test) that is a 100ms human bound against a 500ms
/// watchdog budget, and under the old code the wait IS the watchdog budget, so
/// this fails loudly rather than marginally.
#[tokio::test]
async fn an_unresponsive_cleanup_answers_escape_on_a_human_bound() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(64);
    let started = Arc::new(Notify::new());
    let cleanup_started = Arc::new(Notify::new());
    // Never released: this cleanup never returns.
    let release_cleanup = Arc::new(Notify::new());
    let executor = Arc::new(CleanupBarrierExecutor {
        cooperative: true,
        started: Arc::clone(&started),
        cleanup_started: Arc::clone(&cleanup_started),
        release_cleanup: Arc::clone(&release_cleanup),
        cleanup_calls: AtomicUsize::new(0),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn(async move {
        run_session_actor(
            &journal_path,
            session_id,
            LaunchSession {
                request_id: Uuid::new_v4(),
                cwd: root.path().to_path_buf(),
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: Some(false),
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
                name: None,
                initial_prompt: None,
                capabilities: Default::default(),
                subagent_concurrency_limit: None,
                extension_skill_roots: Vec::new(),
                team_policy: None,
            },
            command_rx,
            event_tx,
            executor,
            actor_store,
        )
        .await
    });

    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: Uuid::new_v4(),
            text: "run until interrupted".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), started.notified())
        .await
        .expect("provider starts");

    let escape_at = std::time::Instant::now();
    command_tx
        .send(HostCommand::Interrupt { session_id })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), cleanup_started.notified())
        .await
        .expect("interrupt enters provider cleanup");

    let mut boundary_at = None;
    while boundary_at.is_none() {
        let event = tokio::time::timeout(TURN_WATCHDOG_STOP_TIMEOUT * 4, event_rx.recv())
            .await
            .expect("the human is answered without waiting on a cleanup that never returns")
            .expect("session remains open");
        if matches!(event.kind, SessionEventKind::TurnCompleted { .. }) {
            boundary_at = Some(std::time::Instant::now());
        }
    }
    let waited = boundary_at.expect("terminal boundary observed") - escape_at;
    assert!(
        waited < TURN_WATCHDOG_STOP_TIMEOUT,
        "Escape must not inherit the watchdog budget: waited {waited:?}"
    );

    // The successor is unharmed: an explicit human prompt still runs a turn,
    // and the previous generation's cleanup never reached it.
    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: Uuid::new_v4(),
            text: "carry on".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), started.notified())
        .await
        .expect("a successor turn still starts after an unresponsive cleanup");

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(5), actor).await;
    scratch.discard().await;
}

/// Queue batching, end to end over the queue itself.
///
/// Failure mode this pins: with an active goal the actor admits one queued
/// prompt per model turn, so a backlog of team notifications is serviced one
/// note per provider turn. A 419-note backlog measured 191 system turns against
/// 16 human ones, with notes waiting hours. Nothing was lost or duplicated --
/// it is a service rate problem.
///
/// One test rather than a fixture per property, because the properties are only
/// meaningful together: batching is worthless if it drops a note, and dangerous
/// if it borrows human priority or swallows a class it should not.
#[test]
fn queued_team_notifications_batch_without_borrowing_priority_or_losing_members() {
    fn note(text: &str, attachment: &str) -> QueuedPrompt {
        QueuedPrompt {
            message_id: Uuid::new_v4(),
            text: text.into(),
            actor: EventActor::System,
            attachments: vec![std::path::PathBuf::from(attachment)],
            output_schema: None,
            delivery: PromptDelivery::Queue,
            visible: true,
            interrupt_batch: false,
            batch: Vec::new(),
        }
    }

    let human = QueuedPrompt {
        actor: EventActor::User,
        ..note("a person is talking", "human.png")
    };
    let watcher_steer = QueuedPrompt {
        delivery: PromptDelivery::Steer,
        ..note("watcher output", "watch.png")
    };
    // Autonomy prompts and goal continuations are System but not visible.
    let autonomy = QueuedPrompt {
        visible: false,
        ..note("autonomy job", "job.png")
    };
    let continuation = note("usage limit continuation", "limit.png");
    let notes: Vec<QueuedPrompt> = (0..40)
        .map(|i| note(&format!("note {i}"), "note.png"))
        .collect();
    let note_ids: Vec<Uuid> = notes.iter().map(|prompt| prompt.message_id).collect();

    let mut pending = VecDeque::new();
    pending.push_back(human.clone());
    pending.push_back(watcher_steer.clone());
    pending.push_back(autonomy.clone());
    pending.push_back(continuation.clone());
    for prompt in notes {
        pending.push_back(prompt);
    }

    let mut autonomy_ids = HashSet::new();
    autonomy_ids.insert(autonomy.message_id);
    coalesce_pending_team_notifications(&mut pending, &autonomy_ids, Some(continuation.message_id));

    // The member cap bounds one turn: 32 of the 40 notes merge, 8 remain queued
    // in order for the next turn. Nothing is expired.
    let merged = pending
        .iter()
        .find(|prompt| prompt.batch.len() > 1)
        .expect("the backlog batches");
    assert_eq!(merged.batch.len(), TEAM_BATCH_MAX_MEMBERS);
    assert_eq!(
        merged
            .batch
            .iter()
            .map(|entry| entry.message_id)
            .collect::<Vec<_>>(),
        note_ids[..TEAM_BATCH_MAX_MEMBERS].to_vec(),
        "members settle in arrival order, so every note keeps its durable identity"
    );
    assert_eq!(
        merged.attachments.len(),
        TEAM_BATCH_MAX_MEMBERS,
        "every member's attachments survive the merge"
    );
    for index in 0..TEAM_BATCH_MAX_MEMBERS {
        assert!(merged.text.contains(&format!("note {index}")));
    }

    // The batch never borrows human standing.
    assert_eq!(merged.actor, EventActor::System);
    assert_eq!(merged.delivery, PromptDelivery::Queue);
    assert!(!merged.interrupt_batch);

    // Human priority is untouched: the person is still admitted first.
    let admitted = pop_next_pending_prompt(&mut pending, true).expect("a prompt is admitted");
    assert_eq!(admitted.message_id, human.message_id);

    // Excluded classes kept their places rather than being absorbed.
    for excluded in [&watcher_steer, &autonomy, &continuation] {
        assert!(
            pending
                .iter()
                .any(|prompt| prompt.message_id == excluded.message_id && prompt.batch.is_empty()),
            "an excluded prompt must stay queued on its own"
        );
    }
    let remaining = pending
        .iter()
        .filter(|prompt| note_ids[TEAM_BATCH_MAX_MEMBERS..].contains(&prompt.message_id))
        .count();
    assert_eq!(remaining, note_ids.len() - TEAM_BATCH_MAX_MEMBERS);

    // A note too large for the byte cap still progresses instead of starving.
    let mut oversized = VecDeque::new();
    let huge = note(&"x".repeat(TEAM_BATCH_MAX_TEXT_BYTES + 1), "huge.png");
    let huge_id = huge.message_id;
    oversized.push_back(huge);
    oversized.push_back(note("small", "small.png"));
    coalesce_pending_team_notifications(&mut oversized, &HashSet::new(), None);
    let first = oversized
        .front()
        .expect("the oversized note is still queued");
    assert_eq!(first.message_id, huge_id);
    assert!(
        first.batch.is_empty(),
        "an oversized note is admitted alone rather than dragging another in"
    );

    // Re-running cannot grow an existing batch past the cap.
    let before = pending.clone();
    coalesce_pending_team_notifications(&mut pending, &autonomy_ids, Some(continuation.message_id));
    let rebatched = pending
        .iter()
        .find(|prompt| prompt.batch.len() > 1)
        .expect("the batch survives");
    assert!(
        rebatched.batch.len() <= TEAM_BATCH_MAX_MEMBERS,
        "counting members across an existing batch keeps re-entry bounded"
    );
    assert_eq!(
        before.len(),
        pending.len(),
        "re-entry neither drops nor invents prompts"
    );
}

#[tokio::test]
async fn rejected_multimodal_steer_falls_back_to_the_front_of_the_fifo() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, _event_rx) = mpsc::channel(32);
    let turns = Arc::new(Mutex::new(Vec::new()));
    let steers = Arc::new(Mutex::new(Vec::new()));
    let turn_started = Arc::new(Notify::new());
    let steer_seen = Arc::new(Notify::new());
    let executor = Arc::new(RejectingSteerExecutor {
        turns: Arc::clone(&turns),
        steers: Arc::clone(&steers),
        turn_started: Arc::clone(&turn_started),
        steer_seen: Arc::clone(&steer_seen),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn(async move {
        run_session_actor(
            &journal_path,
            session_id,
            LaunchSession {
                request_id: Uuid::new_v4(),
                cwd: root.path().to_path_buf(),
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: Some(false),
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
                name: None,
                initial_prompt: None,
                capabilities: Default::default(),
                subagent_concurrency_limit: None,
                extension_skill_roots: Vec::new(),
                team_policy: None,
            },
            command_rx,
            event_tx,
            executor,
            actor_store,
        )
        .await
    });

    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: Uuid::new_v4(),
            text: "first".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), turn_started.notified())
        .await
        .expect("first turn starts");
    let image = PathBuf::from("/tmp/steered-image.png");
    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: Uuid::new_v4(),
            text: "inspect this [Image 1]".to_string(),
            attachments: vec![image.clone()],
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), steer_seen.notified())
        .await
        .expect("provider receives the multimodal steer");
    tokio::time::timeout(Duration::from_secs(1), turn_started.notified())
        .await
        .expect("rejected steer starts as the next queued turn");
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();

    assert_eq!(
        steers.lock().unwrap().as_slice(),
        [("inspect this [Image 1]".to_string(), vec![image.clone()])]
    );
    // Drop the guard before the scratch database is discarded: the
    // assertions need it, the teardown await must not hold it.
    {
        let turns = turns.lock().unwrap();
        assert_eq!(turns.len(), 2);
        assert_eq!(
                turns[0],
                (
                    "Borg canonical provider context v2. The history below is a read-only, provider-neutral projection of durable Borg state; answer the current request normally.\n<borg-message>{\"content\":\"first\",\"role\":\"user\"}</borg-message>".to_string(),
                    Vec::new()
                )
            );
        assert_eq!(
            turns[1].0,
            "Borg canonical provider context v2. The history below is a read-only, provider-neutral projection of durable Borg state; answer the current request normally.\n<borg-message>{\"content\":\"first\",\"role\":\"user\"}</borg-message>\n<borg-message>{\"content\":\"inspect this [Image 1]\",\"role\":\"user\"}</borg-message>"
        );
        assert_eq!(turns[1].1, [image]);
    }
    scratch.discard().await;
}

async fn assert_native_steer_settlement(marker_first: bool, fold: bool) {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let steer_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, _event_rx) = mpsc::channel(256);
    let turn_started = Arc::new(Notify::new());
    let steer_handled = Arc::new(Notify::new());
    let executor = Arc::new(NativeFoldSteerExecutor {
        turn_started: Arc::clone(&turn_started),
        steer_handled: Arc::clone(&steer_handled),
        marker_first,
        fold,
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn({
        let cwd = root.path().to_path_buf();
        async move {
            run_session_actor(
                &journal_path,
                session_id,
                LaunchSession {
                    request_id: Uuid::new_v4(),
                    cwd,
                    provider: CodingProvider::OpenRouter,
                    model: None,
                    effort: None,
                    fast: Some(false),
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::Manual,
                    name: None,
                    initial_prompt: None,
                    capabilities: Default::default(),
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                executor,
                actor_store,
            )
            .await
        }
    });

    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: Uuid::new_v4(),
            text: "start the turn".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Queue,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), turn_started.notified())
        .await
        .expect("first turn starts");
    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: steer_id,
            text: "fold this in".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), steer_handled.notified())
        .await
        .expect("provider handles the steer");
    command_tx
        .send(HostCommand::Interrupt { session_id })
        .await
        .unwrap();
    if !fold {
        // The requeued steer opens the next turn on its own. Waiting for that
        // turn rather than stopping into it is what makes "delivered, not
        // lost" an observation instead of a race.
        tokio::time::timeout(Duration::from_secs(5), turn_started.notified())
            .await
            .expect("the requeued steer is delivered on the next turn");
    }
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(30), actor)
        .await
        .expect("session stops");

    let journal = store.read(session_id).await.unwrap();
    let completions = journal
        .iter()
        .filter(|event| {
            matches!(
                &event.kind,
                SessionEventKind::Message {
                    message_id,
                    status: MessageStatus::Complete,
                    ..
                } if *message_id == steer_id
            )
        })
        .collect::<Vec<_>>();
    if fold {
        let marker = journal
            .iter()
            .find(|event| {
                matches!(
                    &event.kind,
                    SessionEventKind::ProviderEvent { kind, .. }
                        if kind == NATIVE_STEER_APPLIED
                )
            })
            .expect("the fold marker is durable");
        assert_eq!(
            completions.len(),
            1,
            "a folded steer completes exactly once"
        );
        assert!(
            marker.sequence < completions[0].sequence,
            "the marker is persisted before the completion it justifies, whichever \
             order the provider acknowledged and folded in"
        );
    } else {
        // Never settled as consumed: that is what the marker is for, and
        // there was no marker.
        assert!(
            !journal.iter().any(|event| matches!(
                &event.kind,
                SessionEventKind::Message {
                    message_id,
                    status: MessageStatus::Complete,
                    delivery: Some(PromptDelivery::Steer),
                    ..
                } if *message_id == steer_id
            )),
            "an accepted steer the model was never handed must not be settled as consumed"
        );
        // Returned to the input queue rather than dropped...
        assert!(
            journal.iter().any(|event| matches!(
                &event.kind,
                SessionEventKind::Message {
                    message_id,
                    status: MessageStatus::Queued,
                    delivery: Some(PromptDelivery::Queue),
                    ..
                } if *message_id == steer_id
            )),
            "it is requeued as ordinary input"
        );
        // ...and actually handed to the model on the next turn. Asserting the
        // queue still held it was wrong: by then it has been taken off the
        // queue and delivered, which is the outcome this is here to require.
        assert!(
            journal.iter().any(|event| matches!(
                &event.kind,
                SessionEventKind::TurnStarted { message_id, .. } if *message_id == steer_id
            )),
            "and a turn is opened for it, so it is delivered rather than lost"
        );
    }
    scratch.discard().await;
}

#[tokio::test]
async fn a_native_steer_completes_only_once_its_fold_is_journaled() {
    // An acknowledgement says the provider accepted the text for admission.
    // Only the journaled fold says the model was handed it, so it is the only
    // thing that may complete a native steer -- and the two can arrive in
    // either order without changing the outcome.
    assert_native_steer_settlement(false, true).await;
    assert_native_steer_settlement(true, true).await;
    // Accepted, then the turn ends without a fold. Completing here would
    // record input the model never saw and silently lose the human's steer.
    assert_native_steer_settlement(false, false).await;
}

#[test]
fn a_fold_marker_is_read_from_the_array_the_harness_emits() {
    // The protocol is `message_ids`, because one fold can capture several
    // steer controls. This is pinned separately from the actor tests: a fake
    // executor that emits whatever the actor happens to read agrees with
    // itself and proves nothing about the harness.
    let marker = |payload: Value| SessionEventKind::ProviderEvent {
        provider: CodingProvider::OpenRouter,
        kind: NATIVE_STEER_APPLIED.to_string(),
        payload,
    };
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    assert_eq!(
        native_steer_applied_to(&marker(
            json!({ "message_ids": [first.to_string(), second.to_string()] })
        )),
        vec![first, second]
    );
    assert_eq!(
        native_steer_applied_to(&marker(json!({ "message_id": first.to_string() }))),
        vec![first]
    );
    assert!(
        native_steer_applied_to(&SessionEventKind::ProviderEvent {
            provider: CodingProvider::OpenRouter,
            kind: "something_else".to_string(),
            payload: json!({ "message_ids": [first.to_string()] }),
        })
        .is_empty()
    );
}

#[tokio::test]
async fn a_fold_marker_settles_every_steer_batched_under_one_acknowledgement() {
    // Grouped dispatch folds several steers into one request under a single
    // acknowledgement id, and the marker can only name the representative the
    // combined text was built from. Settling that one alone would strand the
    // rest and re-deliver them at teardown.
    let session_id = Uuid::new_v4();
    let (scratch, store, mut journal) = runtime_store(session_id).await;
    let (events, _events_rx) = mpsc::channel::<SessionEvent>(64);
    let representative = Uuid::new_v4();
    let folded_in = Uuid::new_v4();
    let acknowledgement_id = Uuid::new_v4();
    let steer = |message_id: Uuid| PendingSteer {
        prompt: QueuedPrompt {
            message_id,
            text: "batched".to_string(),
            actor: EventActor::User,
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
            visible: true,
            interrupt_batch: false,
            batch: Vec::new(),
        },
        acknowledgement_id,
        admission: SteerAdmission::pending(),
        state: PendingSteerState::AwaitingAcknowledgement,
        attempt_boundary: 0,
    };
    let mut pending_steers = VecDeque::from(vec![steer(representative), steer(folded_in)]);
    let mut awaiting = HashMap::new();

    settle_marked_native_steers(
        representative,
        &mut pending_steers,
        &mut journal,
        &events,
        session_id,
        &mut awaiting,
    )
    .await
    .unwrap();

    assert!(
        pending_steers.is_empty(),
        "the whole batch settles, not only the prompt the marker names"
    );
    let settled = store.read(session_id).await.unwrap();
    for message_id in [representative, folded_in] {
        assert!(
            settled.iter().any(|event| matches!(
                &event.kind,
                SessionEventKind::Message {
                    message_id: id,
                    status: MessageStatus::Complete,
                    ..
                } if *id == message_id
            )),
            "every batched steer reaches Complete"
        );
    }
    scratch.discard().await;
}

#[tokio::test]
async fn accepted_codex_steer_is_settled_before_turn_is_interrupted() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let followup_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(64);
    let turn_started = Arc::new(Notify::new());
    let steer_accepted = Arc::new(Notify::new());
    let executor = Arc::new(CommittingSteerExecutor {
        turn_started: Arc::clone(&turn_started),
        steer_accepted: Arc::clone(&steer_accepted),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn(async move {
        run_session_actor(
            &journal_path,
            session_id,
            LaunchSession {
                request_id: Uuid::new_v4(),
                cwd: root.path().to_path_buf(),
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: Some(false),
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
                name: None,
                initial_prompt: None,
                capabilities: Default::default(),
                subagent_concurrency_limit: None,
                extension_skill_roots: Vec::new(),
                team_policy: None,
            },
            command_rx,
            event_tx,
            executor,
            actor_store,
        )
        .await
    });

    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: Uuid::new_v4(),
            text: "first".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), turn_started.notified())
        .await
        .expect("first turn starts");
    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: followup_id,
            text: "steer at the next boundary".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), steer_accepted.notified())
        .await
        .expect("provider accepts steer transport");

    let mut transitions = Vec::new();
    command_tx
        .send(HostCommand::Interrupt { session_id })
        .await
        .unwrap();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .expect("interrupted steer status arrives")
            .expect("session remains open");
        if let SessionEventKind::Message {
            message_id,
            status,
            delivery: Some(delivery),
            ..
        } = event.kind
            && message_id == followup_id
        {
            transitions.push((status, delivery));
            if status == MessageStatus::Complete && delivery == PromptDelivery::Steer {
                break;
            }
        }
    }
    assert_eq!(
        transitions,
        [
            (MessageStatus::Queued, PromptDelivery::Steer),
            (MessageStatus::InProgress, PromptDelivery::Steer),
            (MessageStatus::Complete, PromptDelivery::Steer),
        ],
        "provider-accepted input must settle before a later interruption can resurrect it"
    );

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();
    scratch.discard().await;
}

#[tokio::test]
async fn escape_flush_keeps_the_turn_running_after_admission_and_steers_queued_input_fifo() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let admitted_id = Uuid::new_v4();
    let queued_ids = [Uuid::new_v4(), Uuid::new_v4()];
    let (command_tx, command_rx) = mpsc::channel(16);
    let (event_tx, mut event_rx) = mpsc::channel(64);
    let turn_started = Arc::new(Notify::new());
    let steer_seen = Arc::new(Notify::new());
    let steers = Arc::new(Mutex::new(Vec::new()));
    let interrupted = Arc::new(AtomicBool::new(false));
    let executor = Arc::new(FlushingQueueExecutor {
        turn_started: Arc::clone(&turn_started),
        steers: Arc::clone(&steers),
        steer_seen: Arc::clone(&steer_seen),
        interrupted: Arc::clone(&interrupted),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn(async move {
        run_session_actor(
            &journal_path,
            session_id,
            LaunchSession {
                request_id: Uuid::new_v4(),
                cwd: root.path().to_path_buf(),
                provider: CodingProvider::Codex,
                model: Some("gpt-5.6-luna".to_string()),
                effort: None,
                fast: Some(false),
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
                name: None,
                initial_prompt: None,
                capabilities: Default::default(),
                subagent_concurrency_limit: None,
                extension_skill_roots: Vec::new(),
                team_policy: None,
            },
            command_rx,
            event_tx,
            executor,
            actor_store,
        )
        .await
    });

    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: Uuid::new_v4(),
            text: "keep working".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), turn_started.notified())
        .await
        .expect("active turn starts");
    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: admitted_id,
            text: "already admitted before escape".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();

    loop {
        let notified = steer_seen.notified();
        if !steers.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::timeout(Duration::from_secs(1), notified)
            .await
            .expect("provider accepts the first follow-up");
    }
    loop {
        let event = tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .expect("admission status arrives")
            .expect("session remains open");
        if matches!(
            event.kind,
            SessionEventKind::Message {
                message_id,
                status: MessageStatus::Complete,
                delivery: Some(PromptDelivery::Steer),
                ..
            } if message_id == admitted_id
        ) {
            break;
        }
    }

    command_tx
        .send(HostCommand::FlushPendingInput { session_id })
        .await
        .unwrap();
    for (message_id, text) in queued_ids.into_iter().zip(["queued one", "queued two"]) {
        command_tx
            .send(HostCommand::Prompt {
                session_id,
                message_id,
                text: text.to_string(),
                attachments: Vec::new(),
                output_schema: None,
                delivery: PromptDelivery::Queue,
            })
            .await
            .unwrap();
    }
    command_tx
        .send(HostCommand::FlushPendingInput { session_id })
        .await
        .unwrap();

    loop {
        let notified = steer_seen.notified();
        if steers.lock().unwrap().len() >= 2 {
            break;
        }
        if tokio::time::timeout(Duration::from_secs(1), notified)
            .await
            .is_err()
        {
            let events = std::iter::from_fn(|| event_rx.try_recv().ok())
                .map(|event| event.kind)
                .collect::<Vec<_>>();
            panic!(
                "every queued input reaches the active provider turn: {:?}; events: {events:?}",
                *steers.lock().unwrap(),
            );
        }
    }
    assert_eq!(
        *steers.lock().unwrap(),
        ["already admitted before escape", "queued one\n\nqueued two"]
    );
    assert!(
        !interrupted.load(Ordering::Acquire),
        "flushing input must never send provider cancellation"
    );

    let mut completed = HashSet::new();
    while completed.len() < queued_ids.len() {
        let event = tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .expect("flushed input settles durably")
            .expect("session remains open");
        assert!(
            !matches!(
                &event.kind,
                SessionEventKind::StatusChanged {
                    detail: Some(detail),
                    ..
                } if detail.contains("cancelling")
            ),
            "Escape flush changed the active turn into cancellation"
        );
        if let SessionEventKind::Message {
            message_id,
            status: MessageStatus::Complete,
            delivery: Some(PromptDelivery::Steer),
            ..
        } = event.kind
            && queued_ids.contains(&message_id)
        {
            completed.insert(message_id);
        }
    }

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();
    scratch.discard().await;
}

#[tokio::test]
async fn accepted_claude_steer_is_settled_before_turn_is_interrupted() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let followup_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(64);
    let turn_started = Arc::new(Notify::new());
    let steer_accepted = Arc::new(Notify::new());
    let executor = Arc::new(CommittingSteerExecutor {
        turn_started: Arc::clone(&turn_started),
        steer_accepted: Arc::clone(&steer_accepted),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn(async move {
        run_session_actor(
            &journal_path,
            session_id,
            LaunchSession {
                request_id: Uuid::new_v4(),
                cwd: root.path().to_path_buf(),
                provider: CodingProvider::Claude,
                model: None,
                effort: None,
                fast: Some(false),
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
                name: None,
                initial_prompt: None,
                capabilities: Default::default(),
                subagent_concurrency_limit: None,
                extension_skill_roots: Vec::new(),
                team_policy: None,
            },
            command_rx,
            event_tx,
            executor,
            actor_store,
        )
        .await
    });

    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: Uuid::new_v4(),
            text: "first".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), turn_started.notified())
        .await
        .expect("first turn starts");
    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: followup_id,
            text: "steer at the next boundary".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), steer_accepted.notified())
        .await
        .expect("Claude accepts steer transport");

    let mut transitions = Vec::new();
    command_tx
        .send(HostCommand::Interrupt { session_id })
        .await
        .unwrap();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(5), event_rx.recv())
            .await
            .expect("interrupted Claude steer status arrives")
            .expect("session remains open");
        if let SessionEventKind::Message {
            message_id,
            status,
            delivery: Some(delivery),
            ..
        } = event.kind
            && message_id == followup_id
        {
            transitions.push((status, delivery));
            if matches!(status, MessageStatus::Complete | MessageStatus::Failed)
                && delivery == PromptDelivery::Steer
            {
                break;
            }
        }
    }
    // Claude reports consumption per stdin message, so an accepted steer stays
    // InProgress (pending input) until the CLI starts it. An interrupt before
    // that settles it as Failed, like the interrupted prompt itself, rather
    // than claiming the model saw it; either way it is settled and cannot be
    // resurrected by the interruption.
    assert_eq!(
        transitions,
        [
            (MessageStatus::Queued, PromptDelivery::Steer),
            (MessageStatus::InProgress, PromptDelivery::Steer),
            (MessageStatus::Failed, PromptDelivery::Steer),
        ],
        "provider-accepted Claude input must settle before a later interruption can resurrect it"
    );

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();
    scratch.discard().await;
}

#[tokio::test]
async fn rejected_codex_steer_retries_at_the_next_tool_boundary() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let followup_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(64);
    let turn_started = Arc::new(Notify::new());
    let first_attempt_rejected = Arc::new(Notify::new());
    let release_tool_boundary = Arc::new(Notify::new());
    let retry_accepted = Arc::new(Notify::new());
    let executor = Arc::new(BoundaryRetrySteerExecutor {
        turn_started: Arc::clone(&turn_started),
        first_attempt_rejected: Arc::clone(&first_attempt_rejected),
        release_tool_boundary: Arc::clone(&release_tool_boundary),
        retry_accepted: Arc::clone(&retry_accepted),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn(async move {
        run_session_actor(
            &journal_path,
            session_id,
            LaunchSession {
                request_id: Uuid::new_v4(),
                cwd: root.path().to_path_buf(),
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: Some(false),
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
                name: None,
                initial_prompt: None,
                capabilities: Default::default(),
                subagent_concurrency_limit: None,
                extension_skill_roots: Vec::new(),
                team_policy: None,
            },
            command_rx,
            event_tx,
            executor,
            actor_store,
        )
        .await
    });

    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: Uuid::new_v4(),
            text: "first".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), turn_started.notified())
        .await
        .expect("first turn starts");
    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: followup_id,
            text: "apply after the running tool".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), first_attempt_rejected.notified())
        .await
        .expect("first steer attempt is rejected");

    tokio::time::sleep(Duration::from_millis(20)).await;
    let mut transitions = Vec::new();
    while let Ok(event) = event_rx.try_recv() {
        if let SessionEventKind::Message {
            message_id,
            status,
            delivery: Some(delivery),
            ..
        } = event.kind
            && message_id == followup_id
        {
            transitions.push((status, delivery));
        }
    }
    assert_eq!(
        transitions,
        [(MessageStatus::Queued, PromptDelivery::Steer)],
        "a transient rejection must not downgrade a same-turn steer"
    );

    release_tool_boundary.notify_one();
    tokio::time::timeout(Duration::from_secs(1), retry_accepted.notified())
        .await
        .expect("steer retries when the tool completes");
    command_tx
        .send(HostCommand::Interrupt { session_id })
        .await
        .unwrap();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .expect("interrupted retry event arrives")
            .expect("session remains open");
        if matches!(
            event.kind,
            SessionEventKind::Message {
                message_id,
                status: MessageStatus::Complete,
                delivery: Some(PromptDelivery::Steer),
                ..
            } if message_id == followup_id
        ) {
            break;
        }
    }

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();
    scratch.discard().await;
}

#[tokio::test]
async fn compaction_defers_steers_preserves_next_attachments_and_respects_stop() {
    struct ControlledTurn {
        turn: AgentTurn,
        events: mpsc::Sender<SessionEventKind>,
        controls: mpsc::Receiver<AgentTurnControl>,
        finish: oneshot::Sender<()>,
    }
    struct ControlledExecutor(mpsc::Sender<ControlledTurn>);
    #[async_trait::async_trait]
    impl AgentTurnExecutor for ControlledExecutor {
        async fn execute(
            &self,
            turn: AgentTurn,
            events: mpsc::Sender<SessionEventKind>,
            controls: Option<mpsc::Receiver<AgentTurnControl>>,
        ) -> Result<AgentTurnResult> {
            let (finish, finished) = oneshot::channel();
            self.0
                .send(ControlledTurn {
                    turn,
                    events,
                    controls: controls.unwrap(),
                    finish,
                })
                .await
                .unwrap();
            finished.await?;
            Ok(AgentTurnResult {
                provider_session_id: Some("provider-session".to_string()),
                final_text: String::new(),
            })
        }
    }
    async fn next<T>(rx: &mut mpsc::Receiver<T>) -> T {
        tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("session makes progress")
            .expect("channel remains open")
    }
    async fn observe(rx: &mut mpsc::Receiver<SessionEvent>, kind: &SessionEventKind) {
        loop {
            if serde_json::to_value(next(rx).await.kind).unwrap()
                == serde_json::to_value(kind).unwrap()
            {
                break;
            }
        }
    }
    let tool_boundary = || SessionEventKind::ToolCompleted {
        tool_call_id: "tool-1".to_string(),
        output: "done".to_string(),
        output_ref: None,
        is_error: false,
        input: None,
        input_ref: None,
    };
    for completion_kind in ["context_compaction", "item/completed:contextCompaction"] {
        for stop in [false, true] {
            let root = tempdir().unwrap();
            let journal_path = root.path().join("session.lock");
            let session_id = Uuid::new_v4();
            let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
            let store: Arc<dyn SessionStore> = Arc::new(store);
            store.create_session(session_id).await.unwrap();
            let (command_tx, command_rx) = mpsc::channel(8);
            let (event_tx, mut event_rx) = mpsc::channel(128);
            let (turn_tx, mut turns) = mpsc::channel(8);
            let actor_store = Arc::clone(&store);
            let actor = tokio::spawn(async move {
                run_session_actor(
                    &journal_path,
                    session_id,
                    LaunchSession {
                        request_id: Uuid::new_v4(),
                        cwd: root.path().to_path_buf(),
                        provider: CodingProvider::Codex,
                        model: None,
                        effort: None,
                        fast: Some(false),
                        response_language: crate::ResponseLanguage::Auto,
                        permission_mode: PermissionMode::Manual,
                        name: None,
                        initial_prompt: None,
                        capabilities: Default::default(),
                        subagent_concurrency_limit: None,
                        extension_skill_roots: Vec::new(),
                        team_policy: None,
                    },
                    command_rx,
                    event_tx,
                    Arc::new(ControlledExecutor(turn_tx)),
                    actor_store,
                )
                .await
            });
            let prompt = |message_id, delivery| HostCommand::Prompt {
                session_id,
                message_id,
                text: "follow up".to_string(),
                attachments: vec![PathBuf::from("screenshot.png")],
                output_schema: None,
                delivery,
            };
            command_tx
                .send(prompt(Uuid::new_v4(), PromptDelivery::Steer))
                .await
                .unwrap();
            let mut active = next(&mut turns).await;
            let rejected_id = Uuid::new_v4();
            command_tx
                .send(prompt(rejected_id, PromptDelivery::Steer))
                .await
                .unwrap();
            let AgentTurnControl::Steer {
                ack: rejected_ack, ..
            } = next(&mut active.controls).await
            else {
                panic!("initial steer");
            };
            // A boundary passes while the first acknowledgement is still outstanding.
            active.events.send(tool_boundary()).await.unwrap();
            let started = SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "item/started:contextCompaction".to_string(),
                payload: json!({}),
            };
            active.events.send(started.clone()).await.unwrap();
            observe(&mut event_rx, &started).await;
            let steer_id = Uuid::new_v4();
            let queued_id = Uuid::new_v4();
            for (message_id, delivery) in [
                (steer_id, PromptDelivery::Steer),
                (queued_id, PromptDelivery::Queue),
            ] {
                command_tx.send(prompt(message_id, delivery)).await.unwrap();
                observe(
                    &mut event_rx,
                    &SessionEventKind::Message {
                        message_id,
                        actor: EventActor::User,
                        text: "follow up".to_string(),
                        attachments: vec![PathBuf::from("screenshot.png")],
                        status: MessageStatus::Queued,
                        delivery: Some(delivery),
                    },
                )
                .await;
            }
            rejected_ack.send(Err("compacting".to_string())).unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(50), active.controls.recv())
                    .await
                    .is_err(),
                "late rejection must not retry during compaction"
            );
            if stop {
                command_tx
                    .send(HostCommand::Interrupt { session_id })
                    .await
                    .unwrap();
                assert!(matches!(
                    next(&mut active.controls).await,
                    AgentTurnControl::Interrupt
                ));
            }
            let completed = SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: completion_kind.to_string(),
                payload: if completion_kind == "context_compaction" {
                    json!({"status": "completed"})
                } else {
                    json!({})
                },
            };
            active.events.send(completed.clone()).await.unwrap();
            observe(&mut event_rx, &completed).await;
            if !stop {
                let AgentTurnControl::Steer {
                    message_id,
                    attachments,
                    admission,
                    ack,
                    ..
                } = next(&mut active.controls).await
                else {
                    panic!("compaction completion retries the pending steer");
                };
                assert_eq!(message_id, rejected_id);
                assert_eq!(attachments, vec![PathBuf::from("screenshot.png"); 2]);
                assert!(admission.accept());
                ack.send(Ok(())).unwrap();
                for expected_id in [rejected_id, steer_id] {
                    observe(
                        &mut event_rx,
                        &SessionEventKind::Message {
                            message_id: expected_id,
                            actor: EventActor::User,
                            text: "follow up".to_string(),
                            attachments: vec![PathBuf::from("screenshot.png")],
                            status: MessageStatus::Complete,
                            delivery: Some(PromptDelivery::Steer),
                        },
                    )
                    .await;
                }
            }
            if stop {
                // Cancellation can close the turn before these late provider events.
                let _ = active.events.send(completed).await;
                let _ = active.events.send(tool_boundary()).await;
            } else {
                active.events.send(completed).await.unwrap();
                active.events.send(tool_boundary()).await.unwrap();
            }
            assert!(
                !matches!(
                    tokio::time::timeout(Duration::from_millis(50), active.controls.recv()).await,
                    Ok(Some(_))
                ),
                "stop, accepted steers, and Next input must not be retried on later boundaries"
            );
            if stop {
                let _ = active.finish.send(());
            } else {
                active.finish.send(()).unwrap();
            }
            // Escape cancels the active turn, not already-queued human input.
            // Rejected steers join that input without clearing the stop gate.
            let queued = next(&mut turns).await;
            assert_eq!(queued.turn.message_id, queued_id);
            assert_eq!(
                queued.turn.attachments,
                vec![PathBuf::from("screenshot.png"); if stop { 3 } else { 1 }]
            );
            if stop {
                assert!(
                    std::iter::from_fn(|| event_rx.try_recv().ok()).all(|event| !matches!(
                        event.kind,
                        SessionEventKind::UserStopChanged { engaged: false }
                    )),
                    "pre-stop human input must not clear the background-work stop gate"
                );
            }
            queued.finish.send(()).unwrap();
            command_tx
                .send(HostCommand::Stop { session_id })
                .await
                .unwrap();
            actor.await.unwrap().unwrap();
            scratch.discard().await;
        }
    }
}

#[tokio::test]
async fn unacknowledged_steer_does_not_block_interrupt_or_fifo_fallback() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(32);
    let turns = Arc::new(Mutex::new(Vec::new()));
    let turn_started = Arc::new(Notify::new());
    let steer_seen = Arc::new(Notify::new());
    let executor = Arc::new(HoldingSteerExecutor {
        turns: Arc::clone(&turns),
        turn_started: Arc::clone(&turn_started),
        steer_seen: Arc::clone(&steer_seen),
        native: false,
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn(async move {
        run_session_actor(
            &journal_path,
            session_id,
            LaunchSession {
                request_id: Uuid::new_v4(),
                cwd: root.path().to_path_buf(),
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: Some(false),
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
                name: None,
                initial_prompt: None,
                capabilities: Default::default(),
                subagent_concurrency_limit: None,
                extension_skill_roots: Vec::new(),
                team_policy: None,
            },
            command_rx,
            event_tx,
            executor,
            actor_store,
        )
        .await
    });

    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: Uuid::new_v4(),
            text: "first".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), turn_started.notified())
        .await
        .expect("first turn starts");
    let followup_id = Uuid::new_v4();
    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: followup_id,
            text: "followup".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), steer_seen.notified())
        .await
        .expect("provider receives steer");
    let mut transitions = Vec::new();

    command_tx
        .send(HostCommand::Interrupt { session_id })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), turn_started.notified())
        .await
        .expect("unacknowledged steer falls back to the FIFO");
    loop {
        let event = tokio::time::timeout(Duration::from_secs(5), event_rx.recv())
            .await
            .expect("the FIFO turn reaches a terminal boundary")
            .expect("session remains open");
        if let SessionEventKind::Message {
            message_id,
            status,
            delivery: Some(delivery),
            ..
        } = event.kind
            && message_id == followup_id
        {
            transitions.push((status, delivery));
            if status == MessageStatus::Complete {
                break;
            }
        }
    }
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();

    {
        let turns = turns.lock().unwrap();
        assert_eq!(turns.len(), 2);
        assert!(subscription_prompt_ends_with(&turns[0].0, "first"));
        assert!(subscription_prompt_ends_with(&turns[1].0, "followup"));
    }

    while let Some(event) = event_rx.recv().await {
        if let SessionEventKind::Message {
            message_id,
            status,
            delivery: Some(delivery),
            ..
        } = event.kind
            && message_id == followup_id
        {
            transitions.push((status, delivery));
        }
    }
    assert_eq!(
        transitions,
        [
            (MessageStatus::Queued, PromptDelivery::Steer),
            (MessageStatus::Queued, PromptDelivery::Queue),
            (MessageStatus::InProgress, PromptDelivery::Queue),
            (MessageStatus::Complete, PromptDelivery::Queue),
        ]
    );
    scratch.discard().await;
}

#[tokio::test]
async fn recalling_unacknowledged_active_steer_emits_prompt_recalled() {
    // Native harness steers wait for the tool boundary; until then they must
    // stay recallable, whether Up recalls everything or one message is named.
    for (native, targeted) in [(false, true), (false, false), (true, true), (true, false)] {
        let root = tempdir().unwrap();
        let journal_path = root.path().join("session.lock");
        let session_id = Uuid::new_v4();
        let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
        let store: Arc<dyn SessionStore> = Arc::new(store);
        store.create_session(session_id).await.unwrap();
        let (command_tx, command_rx) = mpsc::channel(8);
        let (event_tx, mut event_rx) = mpsc::channel(32);
        let turns = Arc::new(Mutex::new(Vec::new()));
        let turn_started = Arc::new(Notify::new());
        let steer_seen = Arc::new(Notify::new());
        let executor = Arc::new(HoldingSteerExecutor {
            turns,
            turn_started: Arc::clone(&turn_started),
            steer_seen: Arc::clone(&steer_seen),
            native,
        });
        let actor_store = Arc::clone(&store);
        let actor = tokio::spawn(async move {
            run_session_actor(
                &journal_path,
                session_id,
                LaunchSession {
                    request_id: Uuid::new_v4(),
                    cwd: root.path().to_path_buf(),
                    provider: CodingProvider::Codex,
                    model: None,
                    effort: None,
                    fast: Some(false),
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::Manual,
                    name: None,
                    initial_prompt: None,
                    capabilities: Default::default(),
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                executor,
                actor_store,
            )
            .await
        });

        command_tx
            .send(HostCommand::Prompt {
                session_id,
                message_id: Uuid::new_v4(),
                text: "first".to_string(),
                attachments: Vec::new(),
                output_schema: None,
                delivery: PromptDelivery::Steer,
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), turn_started.notified())
            .await
            .expect("first turn starts");

        // Two steers back to back: the second is drained behind the first and
        // must stay the human's, or Up cannot recall it.
        let followup_id = Uuid::new_v4();
        let drained_id = Uuid::new_v4();
        for (message_id, text) in [
            (followup_id, "recall this follow-up"),
            (drained_id, "and this one"),
        ] {
            command_tx
                .send(HostCommand::Prompt {
                    session_id,
                    message_id,
                    text: text.to_string(),
                    attachments: Vec::new(),
                    output_schema: None,
                    delivery: PromptDelivery::Steer,
                })
                .await
                .unwrap();
        }
        tokio::time::timeout(Duration::from_secs(10), steer_seen.notified())
            .await
            .expect("provider has received the unacknowledged steer");

        command_tx
            .send(HostCommand::RecallQueuedPrompt {
                session_id,
                // Up in the composer names no message; recall every pending one.
                message_id: targeted.then_some(drained_id),
            })
            .await
            .unwrap();
        let mut expected = if targeted {
            HashSet::from([drained_id])
        } else {
            HashSet::from([followup_id, drained_id])
        };
        let mut seen = Vec::new();
        while !expected.is_empty() {
            let event = tokio::time::timeout(Duration::from_secs(10), event_rx.recv())
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "recall event arrives (native={native}, targeted={targeted}) after {seen:?}"
                    )
                })
                .expect("session remains open");
            seen.push(
                format!("{:?}", event.kind)
                    .chars()
                    .take(160)
                    .collect::<String>(),
            );
            if let SessionEventKind::PromptRecalled { message_id, .. } = event.kind {
                expected.remove(&message_id);
            }
        }

        command_tx
            .send(HostCommand::Interrupt { session_id })
            .await
            .unwrap();
        command_tx
            .send(HostCommand::Stop { session_id })
            .await
            .unwrap();
        actor.await.unwrap().unwrap();
        scratch.discard().await;
    }
}

#[tokio::test]
async fn session_semantics_are_independent_of_turn_execution_location() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    std::fs::create_dir_all(root.path().join("managed-workspace")).unwrap();
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let (command_tx, command_rx) = mpsc::channel(2);
    let (event_tx, mut event_rx) = mpsc::channel(16);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let called = Arc::new(Notify::new());
    let executor = Arc::new(RecordingExecutor {
        seen: Arc::clone(&seen),
        called: Arc::clone(&called),
    });
    let launch = LaunchSession {
        request_id: Uuid::new_v4(),
        cwd: root.path().join("managed-workspace"),
        provider: CodingProvider::Codex,
        model: Some("managed-model".to_string()),
        effort: Some("medium".to_string()),
        fast: Some(false),
        response_language: crate::ResponseLanguage::Auto,
        permission_mode: PermissionMode::FullAccess,
        name: None,
        initial_prompt: None,
        capabilities: Default::default(),
        subagent_concurrency_limit: None,
        extension_skill_roots: Vec::new(),
        team_policy: None,
    };
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn(async move {
        run_session_actor(
            &journal_path,
            session_id,
            launch,
            command_rx,
            event_tx,
            executor,
            actor_store,
        )
        .await
    });
    let output_schema = json!({
        "type": "object",
        "required": ["answer"],
        "properties": {"answer": {"type": "string"}}
    });
    let message_id = Uuid::new_v4();
    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id,
            text: "work in the remote workspace".to_string(),
            attachments: Vec::new(),
            output_schema: Some(output_schema.clone()),
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();
    called.notified().await;
    let mut observed_managed_response = false;
    let mut observed_turn_completion = false;
    while !observed_turn_completion {
        let event = event_rx.recv().await.expect("session event");
        if matches!(
            &event.kind,
            SessionEventKind::Message {
                actor: EventActor::Assistant,
                text,
                ..
            } if text == "managed executor response"
        ) {
            observed_managed_response = true;
        }
        if matches!(
            &event.kind,
            SessionEventKind::TurnCompleted {
                message_id: completed_message_id,
                provider_session_id,
                final_text,
                error: None,
            } if *completed_message_id == message_id
                && provider_session_id.as_deref() == Some("provider-session")
                && final_text == "managed executor response"
        ) {
            observed_turn_completion = true;
        }
    }
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    drop(command_tx);
    actor.await.unwrap().unwrap();

    assert_eq!(
        seen.lock().unwrap().as_slice(),
        [(root.path().join("managed-workspace"), Some(output_schema))]
    );
    while let Some(event) = event_rx.recv().await {
        if matches!(
            &event.kind,
            SessionEventKind::Message {
                actor: EventActor::Assistant,
                text,
                ..
            } if text == "managed executor response"
        ) {
            observed_managed_response = true;
        }
    }
    assert!(observed_managed_response);
    assert!(observed_turn_completion);
    scratch.discard().await;
}

#[tokio::test]
async fn compaction_after_provider_switch_rehydrates_the_new_provider_session() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(64);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let compacted = Arc::new(Notify::new());
    let released = Arc::new(Mutex::new(Vec::new()));
    let executor = Arc::new(CrossProviderCompactionExecutor {
        seen: Arc::clone(&seen),
        compacted: Arc::clone(&compacted),
        released: Arc::clone(&released),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn({
        let cwd = root.path().to_path_buf();
        async move {
            run_session_actor(
                &journal_path,
                session_id,
                LaunchSession {
                    request_id: Uuid::new_v4(),
                    cwd,
                    provider: CodingProvider::Claude,
                    model: Some("claude-test".to_string()),
                    effort: Some("medium".to_string()),
                    fast: Some(false),
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::FullAccess,
                    name: None,
                    initial_prompt: None,
                    capabilities: Default::default(),
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                executor,
                actor_store,
            )
            .await
        }
    });

    let first_id = Uuid::new_v4();
    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: first_id,
            text: "first".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .expect("first turn completes")
            .expect("session remains open");
        if matches!(
            event.kind,
            SessionEventKind::TurnCompleted { message_id, error: None, .. }
                if message_id == first_id
        ) {
            break;
        }
    }

    command_tx
        .send(HostCommand::Configure {
            session_id,
            action: crate::SessionConfigAction::SetProvider {
                provider: CodingProvider::Codex,
                model: Some("gpt-test".to_string()),
            },
        })
        .await
        .unwrap();
    command_tx
        .send(HostCommand::Compact { session_id })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), compacted.notified())
        .await
        .expect("cross-provider compaction is invoked");

    let mut observed_compaction = false;
    while !observed_compaction {
        let event = tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .expect("compaction completes")
            .expect("session remains open");
        observed_compaction = matches!(
            event.kind,
            SessionEventKind::ProviderEvent { kind, .. } if kind == "context_compaction"
        );
    }

    let followup_id = Uuid::new_v4();
    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: followup_id,
            text: "continue".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .expect("follow-up turn completes")
            .expect("session remains open");
        if matches!(
            event.kind,
            SessionEventKind::TurnCompleted { message_id, error: None, .. }
                if message_id == followup_id
        ) {
            break;
        }
    }

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    drop(command_tx);
    actor.await.unwrap().unwrap();

    assert_eq!(
        released.lock().unwrap().as_slice(),
        [CodingProvider::Claude]
    );

    assert_eq!(
            seen.lock().unwrap().as_slice(),
            [
                (
                    CodingProvider::Claude,
                    None,
                    "Borg canonical provider context v2. The history below is a read-only, provider-neutral projection of durable Borg state; answer the current request normally.\n<borg-message>{\"content\":\"first\",\"role\":\"user\"}</borg-message>".to_string(),
                ),
                (
                    CodingProvider::Codex,
                    Some("codex-compacted-session".to_string()),
                    format_subscription_frame(&format_subscription_actor_value(
                        EventActor::User,
                        "continue"
                    )),
                ),
            ]
        );
    scratch.discard().await;
}

#[tokio::test]
async fn clear_context_starts_the_next_turn_without_provider_or_retained_context() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(32);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let executor = Arc::new(ContextRecordingExecutor {
        seen: Arc::clone(&seen),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn(async move {
        run_session_actor(
            &journal_path,
            session_id,
            LaunchSession {
                request_id: Uuid::new_v4(),
                cwd: root.path().to_path_buf(),
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: Some(false),
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
                name: None,
                initial_prompt: None,
                capabilities: Default::default(),
                subagent_concurrency_limit: None,
                extension_skill_roots: Vec::new(),
                team_policy: None,
            },
            command_rx,
            event_tx,
            executor,
            actor_store,
        )
        .await
    });

    for text in ["first", "second"] {
        command_tx
            .send(if text == "second" {
                HostCommand::ClearContext { session_id }
            } else {
                HostCommand::Prompt {
                    session_id,
                    message_id: Uuid::new_v4(),
                    text: text.to_string(),
                    attachments: Vec::new(),
                    output_schema: None,
                    delivery: PromptDelivery::Steer,
                }
            })
            .await
            .unwrap();
        let awaited_clear = text == "second";
        while let Some(event) = event_rx.recv().await {
            if (awaited_clear && matches!(event.kind, SessionEventKind::ContextCleared))
                || (!awaited_clear && matches!(event.kind, SessionEventKind::TurnCompleted { .. }))
            {
                break;
            }
        }
        if awaited_clear {
            command_tx
                .send(HostCommand::Prompt {
                    session_id,
                    message_id: Uuid::new_v4(),
                    text: text.to_string(),
                    attachments: Vec::new(),
                    output_schema: None,
                    delivery: PromptDelivery::Steer,
                })
                .await
                .unwrap();
            while let Some(event) = event_rx.recv().await {
                if matches!(event.kind, SessionEventKind::TurnCompleted { .. }) {
                    break;
                }
            }
        }
    }
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();

    assert_eq!(
            seen.lock().unwrap().as_slice(),
            [
                (
                    "Borg canonical provider context v2. The history below is a read-only, provider-neutral projection of durable Borg state; answer the current request normally.\n<borg-message>{\"content\":\"first\",\"role\":\"user\"}</borg-message>".to_string(),
                    None
                ),
                (
                    "Borg canonical provider context v2. The history below is a read-only, provider-neutral projection of durable Borg state; answer the current request normally.\n<borg-message>{\"content\":\"second\",\"role\":\"user\"}</borg-message>".to_string(),
                    None
                ),
            ]
        );
    scratch.discard().await;
}

#[tokio::test]
async fn fresh_idle_session_has_one_durable_lifecycle() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(2);
    let (event_tx, mut event_rx) = mpsc::channel(8);
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    drop(command_tx);

    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    run_session_actor(
        &journal_path,
        session_id,
        LaunchSession {
            request_id: Uuid::new_v4(),
            cwd: root.path().to_path_buf(),
            provider: CodingProvider::Codex,
            model: None,
            effort: None,
            fast: Some(false),
            response_language: crate::ResponseLanguage::Auto,
            permission_mode: PermissionMode::Manual,
            name: None,
            initial_prompt: None,
            capabilities: Default::default(),
            subagent_concurrency_limit: None,
            extension_skill_roots: Vec::new(),
            team_policy: None,
        },
        command_rx,
        event_tx,
        Arc::new(LocalAgentTurnExecutor::default()),
        Arc::clone(&store),
    )
    .await
    .unwrap();

    let mut observed = Vec::new();
    while let Some(event) = event_rx.recv().await {
        observed.push(event);
    }
    assert_eq!(observed.len(), 5);
    assert!(matches!(observed[0].kind, SessionEventKind::SessionStarted));
    assert!(matches!(
        observed[1].kind,
        SessionEventKind::SessionConfigured { .. }
    ));
    assert!(matches!(
        observed[2].kind,
        SessionEventKind::EffectiveCapabilitiesUpdated { .. }
    ));
    assert!(matches!(
        observed[3].kind,
        SessionEventKind::StatusChanged {
            status: SessionStatus::Ready,
            ..
        }
    ));
    assert!(matches!(
        observed[4].kind,
        SessionEventKind::StatusChanged {
            status: SessionStatus::Stopped,
            ..
        }
    ));
    let journal_events = PostgresSessionStore::connect_with_pool_size(&scratch.url, 2)
        .await
        .unwrap();
    let journal_events = journal_events.read(session_id).await.unwrap();
    assert_eq!(
        journal_events
            .iter()
            .map(|event| (event.id, event.sequence))
            .collect::<Vec<_>>(),
        observed
            .iter()
            .map(|event| (event.id, event.sequence))
            .collect::<Vec<_>>()
    );
    scratch.discard().await;
}

#[tokio::test]
async fn durably_preadmitted_prompt_executes_once_after_actor_handoff() {
    let root = tempdir().unwrap();
    let root_path = root.path().to_path_buf();
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    let writer =
        SessionWriterLease::acquire(root.path().join(format!("{session_id}.lock"))).unwrap();
    let (scratch, postgres) = crate::session_store::postgres::testing::session_store().await;
    let postgres = Arc::new(postgres);
    let store: Arc<dyn SessionStore> = postgres.clone();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(64);
    let called = Arc::new(Notify::new());
    let executor = Arc::new(RecordingExecutor {
        seen: Arc::new(Mutex::new(Vec::new())),
        called: Arc::clone(&called),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn(async move {
        run_agent_session_with_store_and_writer(
            &root_path,
            session_id,
            LaunchSession {
                request_id: Uuid::new_v4(),
                cwd: root_path.clone(),
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: Some(false),
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
                name: None,
                initial_prompt: None,
                capabilities: Default::default(),
                subagent_concurrency_limit: None,
                extension_skill_roots: Vec::new(),
                team_policy: None,
            },
            command_rx,
            event_tx,
            executor,
            actor_store,
            writer,
        )
        .await
    });

    loop {
        let event = event_rx.recv().await.expect("actor emits ready");
        if matches!(
            event.kind,
            SessionEventKind::StatusChanged {
                status: SessionStatus::Ready,
                ..
            }
        ) {
            break;
        }
    }
    store
        .admit_prompt(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: "saved before in-memory handoff".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Queued,
                delivery: Some(PromptDelivery::Steer),
            },
        ))
        .await
        .unwrap();
    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id,
            text: "saved before in-memory handoff".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), called.notified())
        .await
        .expect("preadmitted prompt reaches the executor");
    loop {
        let event = tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .expect("preadmitted prompt completes")
            .expect("session remains open");
        if matches!(
            event.kind,
            SessionEventKind::Message {
                message_id: event_message_id,
                status: MessageStatus::Complete,
                ..
            } if event_message_id == message_id
        ) {
            break;
        }
    }
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();

    let statuses = store
        .read(session_id)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|event| match event.kind {
            SessionEventKind::Message {
                message_id: event_message_id,
                status,
                ..
            } if event_message_id == message_id => Some(status),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        statuses,
        [
            MessageStatus::Queued,
            MessageStatus::InProgress,
            MessageStatus::Complete,
        ]
    );
    assert_eq!(
        store
            .action(session_id, message_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        crate::SessionActionState::Completed
    );
    scratch.discard().await;
}

#[tokio::test]
async fn the_session_store_runs_the_canonical_session_actor() {
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let writer =
        SessionWriterLease::acquire(root.path().join(format!("{session_id}.lock"))).unwrap();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
    let (command_tx, command_rx) = mpsc::channel(2);
    let (event_tx, mut event_rx) = mpsc::channel(8);
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    drop(command_tx);

    run_agent_session_with_store_and_writer(
        root.path(),
        session_id,
        LaunchSession {
            request_id: Uuid::new_v4(),
            cwd: root.path().to_path_buf(),
            provider: CodingProvider::Codex,
            model: None,
            effort: None,
            fast: Some(false),
            response_language: crate::ResponseLanguage::Auto,
            permission_mode: PermissionMode::Manual,
            name: None,
            initial_prompt: None,
            capabilities: Default::default(),
            subagent_concurrency_limit: None,
            extension_skill_roots: Vec::new(),
            team_policy: None,
        },
        command_rx,
        event_tx,
        Arc::new(LocalAgentTurnExecutor::default()),
        store.clone(),
        writer,
    )
    .await
    .unwrap();

    let mut observed = Vec::new();
    while let Some(event) = event_rx.recv().await {
        observed.push(event);
    }
    let stored = store.read(session_id).await.unwrap();
    assert_eq!(stored.len(), 5);
    assert_eq!(
        stored
            .iter()
            .map(|event| (event.id, event.sequence))
            .collect::<Vec<_>>(),
        observed
            .iter()
            .map(|event| (event.id, event.sequence))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        store.state(session_id).await.unwrap().status,
        Some(SessionStatus::Stopped)
    );
    scratch.discard().await;
}

#[tokio::test]
async fn an_autonomy_job_runs_through_the_session_turn_boundary() {
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let writer =
        SessionWriterLease::acquire(root.path().join(format!("{session_id}.lock"))).unwrap();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
    let autonomy = store
        .autonomy_store()
        .await
        .unwrap()
        .expect("the Postgres store exposes the autonomy projection");
    let job = autonomy
        .enqueue(crate::EnqueueAutonomyJob {
            job_id: None,
            idempotency_key: format!("scheduled-{session_id}"),
            kind: "prompt".to_string(),
            payload: json!({"prompt": "run the scheduled verification"}),
            due_at: Utc::now(),
            max_attempts: 2,
            session_id: Some(session_id),
            goal_id: None,
        })
        .await
        .unwrap();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(32);
    let actor = tokio::spawn({
        let root = root.path().to_path_buf();
        let store = Arc::clone(&store);
        async move {
            run_agent_session_with_store_and_writer(
                &root,
                session_id,
                LaunchSession {
                    request_id: Uuid::new_v4(),
                    cwd: root.clone(),
                    provider: CodingProvider::Codex,
                    model: None,
                    effort: None,
                    fast: Some(false),
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::Manual,
                    name: None,
                    initial_prompt: None,
                    capabilities: Default::default(),
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                Arc::new(RecordingExecutor {
                    seen: Arc::new(Mutex::new(Vec::new())),
                    called: Arc::new(Notify::new()),
                }),
                store,
                writer,
            )
            .await
        }
    });

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if autonomy
                .get(job.job_id)
                .await
                .unwrap()
                .is_some_and(|job| job.state == crate::AutonomyJobState::Completed)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("scheduled job completes through the actor");
    let completed = autonomy.get(job.job_id).await.unwrap().unwrap();
    assert_eq!(
        completed.result,
        Some(json!({"final_text": "managed executor response"}))
    );
    assert!(completed.attempt >= 1);
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();
    while event_rx.try_recv().is_ok() {}
    scratch.discard().await;
}

#[tokio::test]
async fn a_blu_workflow_job_runs_without_blocking_the_session_actor() {
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let writer =
        SessionWriterLease::acquire(root.path().join(format!("{session_id}.lock"))).unwrap();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
    let autonomy = store
        .autonomy_store()
        .await
        .unwrap()
        .expect("the Postgres store exposes the autonomy projection");
    let job = autonomy
        .enqueue(crate::EnqueueAutonomyJob {
            job_id: None,
            idempotency_key: format!("blu-scheduled-{session_id}"),
            kind: "blu_workflow".to_string(),
            payload: json!({"name": "scheduled", "source": "return 7"}),
            due_at: Utc::now(),
            max_attempts: 1,
            session_id: Some(session_id),
            goal_id: None,
        })
        .await
        .unwrap();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(32);
    let actor = tokio::spawn({
        let root = root.path().to_path_buf();
        let store = Arc::clone(&store);
        async move {
            run_agent_session_with_store_and_writer(
                &root,
                session_id,
                LaunchSession {
                    request_id: Uuid::new_v4(),
                    cwd: root.clone(),
                    provider: CodingProvider::OpenRouter,
                    model: Some("openrouter/auto".to_string()),
                    effort: None,
                    fast: Some(false),
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::FullAccess,
                    name: None,
                    initial_prompt: None,
                    capabilities: Default::default(),
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                Arc::new(LocalAgentTurnExecutor::default()),
                store,
                writer,
            )
            .await
        }
    });

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if autonomy
                .get(job.job_id)
                .await
                .unwrap()
                .is_some_and(|job| job.state == crate::AutonomyJobState::Completed)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("Blu workflow job completes");
    let completed = autonomy.get(job.job_id).await.unwrap().unwrap();
    assert_eq!(
        completed
            .result
            .as_ref()
            .and_then(|value| value["success"].as_bool()),
        Some(true)
    );
    let events = store.read(session_id).await.unwrap();
    assert!(
        events
            .iter()
            .any(|event| matches!(event.kind, SessionEventKind::BluWorkflowStarted { .. }))
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event.kind, SessionEventKind::BluWorkflowCompleted { .. }))
    );
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();
    while event_rx.try_recv().is_ok() {}
    scratch.discard().await;
}

#[tokio::test]
async fn crash_reconciled_child_stop_is_durable_before_resumed_ready() {
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let child_id = Uuid::new_v4();
    let cwd = root.path().to_path_buf();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        SessionEventKind::SessionConfigured {
            cwd: cwd.clone(),
            provider: CodingProvider::Codex,
            model: Some("gpt-test".to_string()),
            effort: Some("low".to_string()),
            fast: false,
            response_language: crate::ResponseLanguage::Auto,
            permission_mode: PermissionMode::Manual,
        },
        SessionEventKind::SubagentActivity {
            activity: SubagentActivityKind::Updated,
            agent: crate::SubagentSnapshot {
                session_id: child_id,
                parent_session_id: session_id,
                task_name: "/root/worker".to_string(),
                status: crate::SubagentStatus::Running,
                provider: CodingProvider::Codex,
                model: Some("gpt-test".to_string()),
                effort: Some("low".to_string()),
                cwd: cwd.clone(),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
                detail: Some("turn phase: provider active".to_string()),
                final_text: None,
                usage: Default::default(),
                interrupted_by: None,
            },
            event: None,
        },
        SessionEventKind::StatusChanged {
            status: SessionStatus::Stopped,
            detail: None,
        },
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }
    let child_path = root
        .path()
        .join("subagents")
        .join(format!("{child_id}.lock"));
    store.create_session(child_id).await.unwrap();
    store
        .append(SessionEvent::new(
            child_id,
            0,
            SessionEventKind::SessionStarted,
        ))
        .await
        .unwrap();
    store
        .append(SessionEvent::new(
            child_id,
            0,
            SessionEventKind::SessionConfigured {
                cwd: cwd.clone(),
                provider: CodingProvider::Codex,
                model: Some("gpt-test".to_string()),
                effort: Some("low".to_string()),
                fast: false,
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
            },
        ))
        .await
        .unwrap();
    store
        .append(SessionEvent::new(
            child_id,
            0,
            SessionEventKind::StatusChanged {
                status: SessionStatus::Stopped,
                detail: Some("crash cleanup completed".to_string()),
            },
        ))
        .await
        .unwrap();

    let writer =
        SessionWriterLease::acquire(root.path().join(format!("{session_id}.lock"))).unwrap();
    let (command_tx, command_rx) = mpsc::channel(2);
    let (event_tx, mut event_rx) = mpsc::channel(16);
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    drop(command_tx);

    run_agent_session_with_store_and_writer(
        root.path(),
        session_id,
        LaunchSession {
            request_id: Uuid::new_v4(),
            cwd,
            provider: CodingProvider::Codex,
            model: Some("gpt-test".to_string()),
            effort: Some("low".to_string()),
            fast: Some(false),
            response_language: crate::ResponseLanguage::Auto,
            permission_mode: PermissionMode::Manual,
            name: None,
            initial_prompt: None,
            capabilities: Default::default(),
            subagent_concurrency_limit: None,
            extension_skill_roots: Vec::new(),
            team_policy: None,
        },
        command_rx,
        event_tx,
        Arc::new(LocalAgentTurnExecutor::default()),
        store,
        writer,
    )
    .await
    .unwrap();

    let mut observed = Vec::new();
    while let Some(event) = event_rx.recv().await {
        observed.push(event);
    }
    let correction = observed
        .iter()
        .position(|event| {
            matches!(
                &event.kind,
                SessionEventKind::SubagentActivity {
                    activity: SubagentActivityKind::Stopped,
                    agent,
                    ..
                } if agent.session_id == child_id
            )
        })
        .expect("child terminal correction");
    let ready = observed
        .iter()
        .position(|event| {
            matches!(
                event.kind,
                SessionEventKind::StatusChanged {
                    status: SessionStatus::Ready,
                    ..
                }
            )
        })
        .expect("resumed Ready");
    assert!(correction < ready);
    let idle_writer = SessionWriterLease::try_acquire(&child_path)
        .unwrap()
        .expect("crash reconciliation must not start the child actor");
    drop(idle_writer);
    scratch.discard().await;
}

#[tokio::test]
async fn initial_mixed_provider_peer_starts_with_isolated_provider_configuration() {
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let writer =
        SessionWriterLease::acquire(root.path().join(format!("{session_id}.lock"))).unwrap();
    let store = Arc::new(
        PostgresSessionStore::connect_with_pool_size(&scratch.url, 2)
            .await
            .unwrap(),
    );
    let seen = Arc::new(Mutex::new(Vec::new()));
    let called = Arc::new(Notify::new());
    let executor = Arc::new(ProviderRecordingExecutor {
        seen: Arc::clone(&seen),
        called: Arc::clone(&called),
    });
    let (command_tx, command_rx) = mpsc::channel(4);
    let (event_tx, _event_rx) = mpsc::channel(256);
    let actor_root = root.path().to_path_buf();
    let actor_store = store.clone();
    let actor = tokio::spawn(async move {
        run_agent_session_with_store_writer_and_peers(
            &actor_root,
            session_id,
            LaunchSession {
                request_id: Uuid::new_v4(),
                cwd: actor_root.clone(),
                provider: CodingProvider::Codex,
                model: Some("gpt-test".to_string()),
                effort: Some("low".to_string()),
                fast: Some(false),
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::FullAccess,
                name: None,
                initial_prompt: Some("root topic".to_string()),
                capabilities: crate::SessionCapabilities {
                    provider_capabilities: test_provider_capabilities(),
                    ..crate::SessionCapabilities::default()
                },
                subagent_concurrency_limit: None,
                extension_skill_roots: Vec::new(),
                team_policy: None,
            },
            command_rx,
            event_tx,
            executor,
            actor_store,
            writer,
            vec![crate::SpawnSubagent {
                task_name: "peer_claude".to_string(),
                message: "peer topic".to_string(),
                provider: Some(CodingProvider::Claude),
                model: None,
                effort: None,
            }],
        )
        .await
    });

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if seen.lock().unwrap().len() >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("root and peer turns start");

    let turns = seen.lock().unwrap().clone();
    assert!(turns.iter().any(|(provider, model, effort, prompt)| {
        *provider == CodingProvider::Codex
            && model.as_deref() == Some("gpt-test")
            && effort.as_deref() == Some("low")
            && subscription_prompt_ends_with(prompt, "root topic")
    }));
    assert!(turns.iter().any(|(provider, model, effort, prompt)| {
        // Borg runs Claude itself, so the peer resolves Claude's own
        // defaults rather than inheriting the Codex root's model and effort.
        *provider == CodingProvider::Claude
            && model.as_deref() == Some(borg_provider::claude_product_model())
            && effort.as_deref() == Some(borg_provider::claude_default_effort())
            && subscription_prompt_ends_with(prompt, "peer topic")
    }));

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();
    scratch.discard().await;
}

#[tokio::test]
async fn model_consultation_dispatches_a_freeform_briefing_to_an_isolated_provider() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let (command_tx, command_rx) = mpsc::channel(4);
    let (event_tx, _event_rx) = mpsc::channel(64);
    let seen_tool = Arc::new(Mutex::new(Vec::new()));
    let seen_provider = Arc::new(Mutex::new(Vec::new()));
    let called = Arc::new(Notify::new());
    let executor = Arc::new(ConsultingExecutor {
        seen_tool: Arc::clone(&seen_tool),
        seen_provider: Arc::clone(&seen_provider),
        called: Arc::clone(&called),
    });
    let launch = LaunchSession {
        request_id: Uuid::new_v4(),
        cwd: root.path().to_path_buf(),
        provider: CodingProvider::Codex,
        model: Some("gpt-test".to_string()),
        effort: Some("medium".to_string()),
        fast: Some(false),
        response_language: crate::ResponseLanguage::Auto,
        permission_mode: PermissionMode::FullAccess,
        name: None,
        initial_prompt: None,
        capabilities: crate::SessionCapabilities {
            provider_capabilities: test_provider_capabilities(),
            ..crate::SessionCapabilities::default()
        },
        subagent_concurrency_limit: None,
        extension_skill_roots: Vec::new(),
        team_policy: None,
    };
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn(async move {
        run_session_actor(
            &journal_path,
            session_id,
            launch,
            command_rx,
            event_tx,
            executor,
            actor_store,
        )
        .await
    });
    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: Uuid::new_v4(),
            text: "/ask claude review the design".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), called.notified())
        .await
        .expect("main executor received the consultation result");

    assert_eq!(
        seen_provider.lock().unwrap().as_slice(),
        [(
            CodingProvider::Claude,
            Some("high".to_string()),
            "Review the selected interface and call out hidden risks.".to_string()
        )]
    );
    assert_eq!(
        seen_tool.lock().unwrap().as_slice(),
        [(
            "claude".to_string(),
            "The interface hides a cancellation edge case.".to_string()
        )]
    );

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();
    scratch.discard().await;
}

#[tokio::test]
async fn resuming_an_idle_goal_emits_starting_before_running() {
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let (scratch, store, mut journal) = runtime_store(session_id).await;
    let mut goal = SessionGoal::new("Keep going".to_string(), None);
    goal.status = GoalStatus::Paused;
    journal
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::SessionStarted,
        ))
        .await
        .unwrap();
    journal
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::SessionConfigured {
                cwd: root.path().to_path_buf(),
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: false,
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
            },
        ))
        .await
        .unwrap();
    journal
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::GoalUpdated { goal },
        ))
        .await
        .unwrap();
    drop(journal);

    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(128);
    let executor = Arc::new(RecordingExecutor {
        seen: Arc::new(Mutex::new(Vec::new())),
        called: Arc::new(Notify::new()),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn({
        let journal_path = root.path().join("session.lock");
        let cwd = root.path().to_path_buf();
        async move {
            run_session_actor(
                &journal_path,
                session_id,
                LaunchSession {
                    request_id: Uuid::new_v4(),
                    cwd,
                    provider: CodingProvider::Codex,
                    model: None,
                    effort: None,
                    fast: Some(false),
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::Manual,
                    name: None,
                    initial_prompt: None,
                    capabilities: Default::default(),
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                executor,
                actor_store,
            )
            .await
        }
    });

    loop {
        let event = tokio::time::timeout(Duration::from_secs(2), event_rx.recv())
            .await
            .expect("idle session reaches Ready");
        let Some(event) = event else {
            let outcome = actor.await;
            panic!("session actor ended before Ready: {outcome:?}");
        };
        if matches!(
            event.kind,
            SessionEventKind::StatusChanged {
                status: SessionStatus::Ready,
                ..
            }
        ) {
            break;
        }
    }

    command_tx
        .send(HostCommand::Goal {
            session_id,
            action: GoalAction::Resume,
        })
        .await
        .unwrap();

    let mut statuses = Vec::new();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(2), event_rx.recv())
            .await
            .expect("resumed goal starts a turn")
            .expect("session event stream remains open");
        if let SessionEventKind::StatusChanged { status, .. } = event.kind {
            statuses.push(status);
            if status == SessionStatus::Running {
                break;
            }
        }
    }
    assert_eq!(statuses, [SessionStatus::Starting, SessionStatus::Running]);

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    // Stopping settles goal time and records the stop, both journal writes, so
    // the wait is bounded by Postgres rather than by the actor's own work.
    tokio::time::timeout(Duration::from_secs(30), actor)
        .await
        .expect("session stops after the lifecycle assertion")
        .unwrap()
        .unwrap();
    assert!(store.state(session_id).await.unwrap().goal.is_some());
    scratch.discard().await;
}

/// A human resume that arrives while a turn is already running must release
/// the stop latch. Otherwise the durable goal reads `active` while the latch
/// still parks every automatic continuation at the next boundary, so the
/// session stops with an apparently active goal.
#[tokio::test]
async fn resuming_a_goal_mid_turn_releases_the_stop_latch() {
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let (scratch, store, mut journal) = runtime_store(session_id).await;
    let mut goal = SessionGoal::new("Keep going".to_string(), None);
    goal.status = GoalStatus::Paused;
    for event in [
        SessionEvent::new(session_id, 0, SessionEventKind::SessionStarted),
        SessionEvent::new(
            session_id,
            0,
            SessionEventKind::SessionConfigured {
                cwd: root.path().to_path_buf(),
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: false,
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
            },
        ),
        SessionEvent::new(session_id, 0, SessionEventKind::GoalUpdated { goal }),
        SessionEvent::new(
            session_id,
            0,
            SessionEventKind::UserStopChanged { engaged: true },
        ),
        // A prompt queued before the stop still runs when the actor resumes,
        // but it does not clear the latch on admission.
        SessionEvent::new(
            session_id,
            0,
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::User,
                text: "the pre-stop prompt".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Queued,
                delivery: Some(PromptDelivery::Queue),
            },
        ),
    ] {
        journal.append(event).await.unwrap();
    }
    drop(journal);

    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(128);
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn({
        let journal_path = root.path().join("session.lock");
        let cwd = root.path().to_path_buf();
        async move {
            run_session_actor(
                &journal_path,
                session_id,
                LaunchSession {
                    request_id: Uuid::new_v4(),
                    cwd,
                    provider: CodingProvider::Codex,
                    model: None,
                    effort: None,
                    fast: Some(false),
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::Manual,
                    name: None,
                    initial_prompt: None,
                    capabilities: Default::default(),
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                Arc::new(HungProviderExecutor),
                actor_store,
            )
            .await
        }
    });

    // The pre-stop prompt opens a turn that the hung executor never finishes.
    loop {
        let event = tokio::time::timeout(Duration::from_secs(2), event_rx.recv())
            .await
            .expect("the queued prompt starts a turn")
            .expect("session event stream remains open");
        if matches!(
            event.kind,
            SessionEventKind::StatusChanged {
                status: SessionStatus::Running,
                ..
            }
        ) {
            break;
        }
    }

    command_tx
        .send(HostCommand::Goal {
            session_id,
            action: GoalAction::Resume,
        })
        .await
        .unwrap();

    let resumed = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let Some(event) = event_rx.recv().await else {
                panic!("session event stream closed before the latch cleared");
            };
            if matches!(
                event.kind,
                SessionEventKind::UserStopChanged { engaged: false }
            ) {
                return;
            }
        }
    })
    .await;
    assert!(
        resumed.is_ok(),
        "a mid-turn goal resume must release the stop latch"
    );
    assert!(
        store
            .state(session_id)
            .await
            .unwrap()
            .goal
            .is_some_and(|goal| goal.status == GoalStatus::Active)
    );

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), actor).await;
    scratch.discard().await;
}

/// Escape parks a goal until the next human prompt. A pre-stop prompt must
/// not resume it, and a separate /goal pause must remain paused.
#[tokio::test]
async fn fresh_human_input_resumes_only_an_interrupted_goal() {
    // A blocked goal also reopens: fresh human input is the new direction it
    // was waiting for, with or without a stop latch.
    for (stopped, pre_stop_turn, initial) in [
        (true, false, GoalStatus::Paused),
        (true, true, GoalStatus::Paused),
        (false, false, GoalStatus::Paused),
        (false, false, GoalStatus::Blocked),
    ] {
        let resumes = stopped || initial == GoalStatus::Blocked;
        let root = tempdir().unwrap();
        let session_id = Uuid::new_v4();
        let (scratch, store, mut journal) = runtime_store(session_id).await;
        let mut goal = SessionGoal::new("Finish verification".to_string(), None);
        goal.status = initial;
        let mut events = vec![
            SessionEventKind::SessionStarted,
            SessionEventKind::SessionConfigured {
                cwd: root.path().to_path_buf(),
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: false,
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
            },
            SessionEventKind::GoalUpdated { goal },
        ];
        if stopped {
            events.push(SessionEventKind::UserStopChanged { engaged: true });
        }
        if pre_stop_turn {
            events.push(SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::User,
                text: "queued before Escape".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Queued,
                delivery: Some(PromptDelivery::Queue),
            });
        }
        for kind in events {
            journal
                .append(SessionEvent::new(session_id, 0, kind))
                .await
                .unwrap();
        }
        drop(journal);

        let (command_tx, command_rx) = mpsc::channel(8);
        let (event_tx, mut event_rx) = mpsc::channel(128);
        let actor_store = Arc::clone(&store);
        let actor = tokio::spawn({
            let journal_path = root.path().join("session.lock");
            let cwd = root.path().to_path_buf();
            async move {
                run_session_actor(
                    &journal_path,
                    session_id,
                    LaunchSession {
                        request_id: Uuid::new_v4(),
                        cwd,
                        provider: CodingProvider::Codex,
                        model: None,
                        effort: None,
                        fast: Some(false),
                        response_language: crate::ResponseLanguage::Auto,
                        permission_mode: PermissionMode::Manual,
                        name: None,
                        initial_prompt: None,
                        capabilities: Default::default(),
                        subagent_concurrency_limit: None,
                        extension_skill_roots: Vec::new(),
                        team_policy: None,
                    },
                    command_rx,
                    event_tx,
                    Arc::new(HungProviderExecutor),
                    actor_store,
                )
                .await
            }
        });
        // The pre-stop queued turn is already running; the other cases are idle.
        let expected = if pre_stop_turn {
            SessionStatus::Running
        } else {
            SessionStatus::Ready
        };
        tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(event) = event_rx.recv().await {
                if matches!(event.kind, SessionEventKind::StatusChanged { status, .. } if status == expected) {
                    break;
                }
            }
        }).await.expect("session is ready for the fresh prompt");
        assert_eq!(
            store.state(session_id).await.unwrap().goal.unwrap().status,
            initial
        );

        command_tx
            .send(HostCommand::Prompt {
                session_id,
                message_id: Uuid::new_v4(),
                text: "continue verification".to_string(),
                attachments: Vec::new(),
                output_schema: None,
                delivery: PromptDelivery::Steer,
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(event) = event_rx.recv().await {
                if matches!(event.kind, SessionEventKind::Message { actor: EventActor::User, ref text, .. } if text == "continue verification") {
                    break;
                }
            }
        }).await.expect("fresh prompt is admitted");
        let state = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let state = store.state(session_id).await.unwrap();
                if !resumes
                    || (!state.user_stopped
                        && state
                            .goal
                            .as_ref()
                            .is_some_and(|goal| goal.status == GoalStatus::Active))
                {
                    break state;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("fresh input resumes the interrupted goal");
        assert_eq!(
            state.goal.unwrap().status,
            if resumes { GoalStatus::Active } else { initial }
        );
        if stopped {
            assert!(!state.user_stopped);
        }
        command_tx
            .send(HostCommand::Stop { session_id })
            .await
            .unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), actor).await;
        scratch.discard().await;
    }
}

#[tokio::test]
async fn goal_state_is_recoverable_from_the_session_journal() {
    let session_id = Uuid::new_v4();
    let (scratch, store, mut journal) = runtime_store(session_id).await;
    let (event_tx, mut event_rx) = mpsc::channel(16);
    let mut goal = None;
    let mut active_since = None;

    apply_goal_action(
        &mut journal,
        &event_tx,
        session_id,
        &mut goal,
        &mut active_since,
        GoalAction::Set {
            objective: "Ship it".to_string(),
            token_budget: Some(100),
        },
    )
    .await
    .unwrap();
    apply_goal_action(
        &mut journal,
        &event_tx,
        session_id,
        &mut goal,
        &mut active_since,
        GoalAction::Pause,
    )
    .await
    .unwrap();
    assert_eq!(goal.as_ref().unwrap().status, GoalStatus::Paused);
    assert!(active_since.is_none());
    assert_eq!(
        store.state(session_id).await.unwrap().goal.unwrap().status,
        GoalStatus::Paused
    );
    apply_goal_action(
        &mut journal,
        &event_tx,
        session_id,
        &mut goal,
        &mut active_since,
        GoalAction::Resume,
    )
    .await
    .unwrap();
    assert!(active_since.is_some());
    let usage = SessionEventKind::UsageUpdated {
        provider_duration_ms: 0,
        turn_id: None,
        provider_context_reused: None,
        input_tokens: 3,
        output_tokens: 7,
        cached_input_tokens: 80,
        cache_creation_input_tokens: 10,
        total_tokens: 100,
        cost_microusd: None,
        cost_basis: "unavailable".to_string(),
        cost_usd: None,
        context_tokens: None,
        context_window_tokens: None,
    };
    account_goal_tokens(
        &mut journal,
        &event_tx,
        session_id,
        &mut goal,
        &mut active_since,
        goal_token_usage(&usage).unwrap(),
    )
    .await
    .unwrap();

    let recovered = store.state(session_id).await.unwrap().goal.unwrap();
    assert_eq!(recovered.objective, "Ship it");
    assert_eq!(recovered.tokens_used, 100);
    assert_eq!(recovered.status, GoalStatus::BudgetLimited);

    apply_goal_action(
        &mut journal,
        &event_tx,
        session_id,
        &mut goal,
        &mut active_since,
        GoalAction::Clear,
    )
    .await
    .unwrap();
    assert!(store.state(session_id).await.unwrap().goal.is_none());

    drop(event_tx);
    let mut kinds = Vec::new();
    while let Some(event) = event_rx.recv().await {
        kinds.push(event.kind);
    }
    assert!(matches!(
        kinds.as_slice(),
        [
            SessionEventKind::GoalUpdated { .. },
            SessionEventKind::GoalUpdated { .. },
            SessionEventKind::GoalUpdated { .. },
            SessionEventKind::GoalUpdated { .. },
            SessionEventKind::GoalCleared { .. }
        ]
    ));
    scratch.discard().await;
}

#[test]
fn watcher_yield_instruction_requires_opt_in() {
    let goal = SessionGoal::new("work".to_string(), None);
    assert!(!continuation_prompt(&goal, false).contains("await_watchers"));
    assert!(continuation_prompt(&goal, true).contains("await_watchers"));
}

#[test]
fn automatic_goal_continuation_supports_unbudgeted_goals() {
    let unbudgeted = SessionGoal::new("work continuously".to_string(), None);
    assert!(goal_allows_automatic_continuation(&unbudgeted));

    let mut budgeted = SessionGoal::new("bounded continuation".to_string(), Some(100));
    assert!(goal_allows_automatic_continuation(&budgeted));
    budgeted.tokens_used = 100;
    assert!(!goal_allows_automatic_continuation(&budgeted));
}

#[tokio::test]
async fn model_can_mark_an_active_goal_blocked() {
    let session_id = Uuid::new_v4();
    let (scratch, store, mut journal) = runtime_store(session_id).await;
    let (event_tx, _event_rx) = mpsc::channel(16);
    let mut goal = Some(SessionGoal::new("Need user input".to_string(), None));
    let mut active_since = Some(Instant::now());

    let response = apply_model_goal_request(
        &mut journal,
        &event_tx,
        session_id,
        &mut goal,
        &mut active_since,
        SessionGoalToolRequest::Update {
            status: ModelGoalStatus::Blocked,
        },
    )
    .await
    .unwrap();

    assert_eq!(
        response.goal.as_ref().map(|goal| goal.status),
        Some(GoalStatus::Blocked)
    );
    assert!(active_since.is_none());
    assert_eq!(
        store.state(session_id).await.unwrap().goal.unwrap().status,
        GoalStatus::Blocked
    );
    scratch.discard().await;
}

#[test]
fn goal_turn_failure_audit_reaches_three_only_for_the_same_blocker() {
    let mut failures = ConsecutiveGoalTurnFailures::default();

    assert_eq!(failures.record("provider unavailable"), 1);
    assert_eq!(failures.record("provider unavailable"), 2);
    assert_eq!(failures.record("permission denied"), 1);
    assert_eq!(failures.record("permission denied"), 2);
    assert_eq!(failures.record("permission denied"), 3);

    failures.reset();
    assert_eq!(failures.record("permission denied"), 1);
}

#[test]
fn structured_rate_and_billing_errors_are_usage_limited() {
    assert!(provider_error_is_usage_limited(
        r#"claude SDK API error: limit reached "kind":"rate_limit" "status":429"#
    ));
    assert!(provider_error_is_usage_limited(
        r#"claude SDK API error: payment required "kind": "billing_error""#
    ));
    assert!(provider_error_is_usage_limited(
        "You've hit your usage limit. Try again later."
    ));
    assert!(provider_error_is_usage_limited(
        "OpenCode request failed: rate limit exceeded"
    ));
    assert!(provider_error_is_temporary_usage_limited(
        "You've hit your usage limit. Try again later."
    ));
    assert!(!provider_error_is_temporary_usage_limited(
        r#"claude SDK API error: payment required "kind": "billing_error""#
    ));
    assert!(!provider_error_is_usage_limited(
        r#"claude SDK API error: overloaded "kind":"overloaded" "status":529"#
    ));
}

#[test]
fn usage_limit_reset_honors_provider_time_zone_and_long_waits() {
    let now = chrono::DateTime::parse_from_rfc3339("2026-09-15T13:47:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let delay = |message| provider_error_usage_limit_reset_delay_at(message, now);
    assert_eq!(
        delay("You've hit your session limit · resets 5:50pm (Europe/London)"),
        Some(Duration::from_secs(3 * 3600 + 3 * 60))
    );
    assert_eq!(
        delay("resets 1am (Europe/London)"),
        Some(Duration::from_secs(10 * 3600 + 13 * 60))
    );
    assert_eq!(
        delay("Provider-reported reset: 2026-09-17 13:47:00 UTC."),
        Some(Duration::from_secs(2 * 86400))
    );
    assert_eq!(
        delay("Provider-reported retry delay: 7200 seconds."),
        Some(Duration::from_secs(7200))
    );
    assert_eq!(delay("resets 5pm (Unknown/Zone)"), None);
    assert!(provider_error_is_temporary_usage_limited(
        "You've hit your session limit · resets 5:50pm (Europe/London)"
    ));
}

#[test]
fn usage_limit_auto_resume_is_restricted_to_subscription_cli_providers() {
    assert!(provider_supports_usage_limit_resume(CodingProvider::Claude));
    assert!(provider_supports_usage_limit_resume(CodingProvider::Codex));
    assert!(provider_supports_usage_limit_resume(
        CodingProvider::OpenCode
    ));
    assert!(!provider_supports_usage_limit_resume(
        CodingProvider::OpenRouter
    ));
    assert!(!provider_supports_usage_limit_resume(
        CodingProvider::OpenAiCompatible
    ));
}

#[tokio::test]
async fn usage_limit_failure_stops_an_active_goal() {
    let session_id = Uuid::new_v4();
    let (scratch, store, mut journal) = runtime_store(session_id).await;
    let (event_tx, _event_rx) = mpsc::channel(16);
    let mut goal = Some(SessionGoal::new("Keep working".to_string(), None));
    let mut active_since = Some(Instant::now());

    usage_limit_active_goal(
        &mut journal,
        &event_tx,
        session_id,
        &mut goal,
        &mut active_since,
    )
    .await
    .unwrap();

    assert_eq!(
        goal.as_ref().map(|goal| goal.status),
        Some(GoalStatus::UsageLimited)
    );
    assert!(active_since.is_none());
    assert_eq!(
        store.state(session_id).await.unwrap().goal.unwrap().status,
        GoalStatus::UsageLimited
    );
    scratch.discard().await;
}

#[test]
fn todo_list_rejects_multiple_in_progress_items() {
    let items = vec![
        PlanItem {
            id: Uuid::new_v4(),
            content: "First".into(),
            status: PlanItemStatus::InProgress,
        },
        PlanItem {
            id: Uuid::new_v4(),
            content: "Second".into(),
            status: PlanItemStatus::InProgress,
        },
    ];

    let error = validate_todos(items).unwrap_err();
    assert!(error.to_string().contains("at most one in-progress item"));
}

#[test]
fn recalling_prompts_targets_the_exact_queue_entry_and_skips_steers() {
    let first_visible_id = Uuid::new_v4();
    let internal_id = Uuid::new_v4();
    let second_visible_id = Uuid::new_v4();
    let mut pending = VecDeque::from([
        QueuedPrompt {
            message_id: first_visible_id,
            text: "first".to_string(),
            actor: EventActor::User,
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Queue,
            visible: true,
            interrupt_batch: true,
            batch: Vec::new(),
        },
        QueuedPrompt {
            message_id: internal_id,
            text: "internal continuation".to_string(),
            actor: EventActor::System,
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Queue,
            visible: false,
            interrupt_batch: false,
            batch: Vec::new(),
        },
        QueuedPrompt {
            message_id: second_visible_id,
            text: "second".to_string(),
            actor: EventActor::User,
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Queue,
            visible: true,
            interrupt_batch: true,
            batch: Vec::new(),
        },
        QueuedPrompt {
            message_id: Uuid::new_v4(),
            text: "pending steer".to_string(),
            actor: EventActor::User,
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
            visible: true,
            interrupt_batch: true,
            batch: Vec::new(),
        },
    ]);

    let recalled = recall_visible_queued_prompts(&mut pending, Some(first_visible_id));

    assert_eq!(
        recalled
            .iter()
            .map(|prompt| prompt.message_id)
            .collect::<Vec<_>>(),
        [first_visible_id]
    );
    assert_eq!(pending.len(), 3);
    assert_eq!(pending[0].message_id, internal_id);
    assert_eq!(pending[1].message_id, second_visible_id);
    assert_eq!(pending[2].delivery, PromptDelivery::Steer);
    let steer_id = pending[2].message_id;
    assert!(recall_visible_queued_prompts(&mut pending, Some(steer_id)).is_empty());

    let recalled = recall_visible_queued_prompts(&mut pending, None);
    assert_eq!(
        recalled
            .iter()
            .map(|prompt| prompt.message_id)
            .collect::<Vec<_>>(),
        [second_visible_id]
    );
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].message_id, internal_id);
    assert_eq!(pending[1].message_id, steer_id);
}

/// A visible, queued peer message is System input the human never authored.
/// Neither a targeted recall nor recall-all may retract it: the actor
/// predicate, not visibility, is what protects imported relay deliveries.
#[test]
fn recalling_prompts_preserves_visible_system_queued_input() {
    let human_id = Uuid::new_v4();
    let imported_id = Uuid::new_v4();
    let queued = |message_id, actor, text: &str| QueuedPrompt {
        message_id,
        text: text.to_string(),
        actor,
        attachments: Vec::new(),
        output_schema: None,
        delivery: PromptDelivery::Queue,
        // Imported peer input is rendered in the transcript, so visibility
        // alone cannot distinguish it from a human draft.
        visible: true,
        interrupt_batch: false,
        batch: Vec::new(),
    };
    let mut pending = VecDeque::from([
        queued(imported_id, EventActor::System, "Team message from peer"),
        queued(human_id, EventActor::User, "my own draft"),
    ]);

    // Targeted recall of the peer message is a no-op.
    assert!(recall_visible_queued_prompts(&mut pending, Some(imported_id)).is_empty());
    assert_eq!(pending.len(), 2);

    // Recall-all takes only the human draft.
    let recalled = recall_visible_queued_prompts(&mut pending, None);
    assert_eq!(
        recalled
            .iter()
            .map(|prompt| prompt.message_id)
            .collect::<Vec<_>>(),
        [human_id]
    );
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].message_id, imported_id);
    assert_eq!(pending[0].actor, EventActor::System);
}

/// ↑ on an empty composer must give back exactly the work whose provider
/// admission is still unclaimed. Once provider admission wins, recall cannot
/// claim or remove the steer.
#[test]
fn only_an_uncommitted_steer_is_withdrawable_from_the_active_turn() {
    let rejected_id = Uuid::new_v4();
    let awaiting_id = Uuid::new_v4();
    let accepted_id = Uuid::new_v4();
    let team_id = Uuid::new_v4();
    let steer =
        |message_id: Uuid, state: PendingSteerState, admission: SteerAdmission| PendingSteer {
            prompt: QueuedPrompt {
                message_id,
                text: "steer".to_string(),
                actor: EventActor::User,
                attachments: Vec::new(),
                output_schema: None,
                delivery: PromptDelivery::Steer,
                visible: true,
                interrupt_batch: true,
                batch: Vec::new(),
            },
            admission,
            state,
            acknowledgement_id: Uuid::new_v4(),
            attempt_boundary: 0,
        };
    let accepted = SteerAdmission::pending();
    assert!(accepted.accept());
    let mut pending_steers = VecDeque::from([
        steer(
            awaiting_id,
            PendingSteerState::AwaitingAcknowledgement,
            SteerAdmission::pending(),
        ),
        steer(
            rejected_id,
            PendingSteerState::RetryAtBoundary {
                error: "provider refused the steer".to_string(),
            },
            SteerAdmission::pending(),
        ),
        steer(
            accepted_id,
            PendingSteerState::AwaitingAcknowledgement,
            accepted,
        ),
    ]);
    let mut team = steer(
        team_id,
        PendingSteerState::AwaitingAcknowledgement,
        pending_steers[0].admission.clone(),
    );
    team.prompt.actor = EventActor::System;
    team.prompt.interrupt_batch = false;
    team.acknowledgement_id = pending_steers[0].acknowledgement_id;
    pending_steers.insert(1, team);

    let recalled = recall_withdrawable_steers(&mut pending_steers, Some(accepted_id));
    assert!(recalled.is_empty());
    assert!(recall_withdrawable_steers(&mut pending_steers, Some(team_id)).is_empty());
    assert_eq!(pending_steers.len(), 4);

    let recalled = recall_withdrawable_steers(&mut pending_steers, None);
    assert_eq!(
        recalled
            .iter()
            .map(|prompt| prompt.message_id)
            .collect::<Vec<_>>(),
        [awaiting_id, rejected_id]
    );
    assert_eq!(pending_steers.len(), 2);
    assert_eq!(pending_steers[0].prompt.message_id, team_id);
    assert_eq!(pending_steers[1].prompt.message_id, accepted_id);
}

#[test]
fn coalescing_keeps_the_last_prompts_attachments_on_its_batch_entry() {
    // A retried prompt is pushed back and coalesced on its own before the
    // next admission. Its journaled batch entry must keep the attachments,
    // otherwise the transcript loses the image preview on retry.
    let image = PathBuf::from("/tmp/only.png");
    let id = Uuid::new_v4();
    let mut pending = VecDeque::from([QueuedPrompt {
        message_id: id,
        text: "why [Image 1]".to_string(),
        actor: EventActor::User,
        attachments: vec![image.clone()],
        output_schema: None,
        delivery: PromptDelivery::Queue,
        visible: true,
        interrupt_batch: true,
        batch: Vec::new(),
    }]);

    coalesce_queued_prompts(&mut pending);

    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].attachments, std::slice::from_ref(&image));
    let entries = pending[0].batch_entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].message_id, id);
    assert_eq!(entries[0].attachments, [image]);
}

#[tokio::test]
async fn interrupt_overtakes_a_full_command_queue_and_deferred_input() {
    let session_id = Uuid::new_v4();
    let (normal_tx, normal_rx) = mpsc::channel(1);
    let (urgent_tx, urgent_rx) = mpsc::channel(1);
    normal_tx
        .try_send(HostCommand::Prompt {
            session_id,
            message_id: Uuid::new_v4(),
            text: "queued".into(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Queue,
        })
        .unwrap();
    urgent_tx
        .try_send(HostCommand::Interrupt { session_id })
        .unwrap();
    let mut inbox = HostCommandInbox::new(normal_rx, Some(urgent_rx));
    let mut deferred = VecDeque::from([HostCommand::FlushPendingInput { session_id }]);
    assert!(matches!(
        next_host_command(&mut deferred, &mut inbox).await,
        Some(HostCommand::Interrupt { .. })
    ));
    assert!(matches!(
        next_host_command(&mut deferred, &mut inbox).await,
        Some(HostCommand::FlushPendingInput { .. })
    ));
    assert!(matches!(
        next_host_command(&mut deferred, &mut inbox).await,
        Some(HostCommand::Prompt { .. })
    ));
}

#[test]
fn all_queued_human_input_is_one_turn_even_without_an_interrupt() {
    let first_id = Uuid::new_v4();
    let second_id = Uuid::new_v4();
    let prompt = |message_id, text: &str| QueuedPrompt {
        message_id,
        text: text.to_string(),
        actor: EventActor::User,
        attachments: Vec::new(),
        output_schema: None,
        delivery: PromptDelivery::Queue,
        visible: true,
        interrupt_batch: false,
        batch: Vec::new(),
    };
    let mut pending = VecDeque::from([
        prompt(first_id, "first follow-up"),
        prompt(second_id, "second follow-up"),
    ]);
    coalesce_queued_prompts(&mut pending);
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].text, "first follow-up\n\nsecond follow-up");
    assert_eq!(
        pending[0]
            .batch_entries()
            .iter()
            .map(|entry| entry.message_id)
            .collect::<Vec<_>>(),
        [first_id, second_id],
    );
}

#[test]
fn escape_batch_coalesces_queued_prompts_in_fifo_order() {
    let first_image = PathBuf::from("/tmp/first.png");
    let last_image = PathBuf::from("/tmp/last.png");
    let first_id = Uuid::new_v4();
    let second_id = Uuid::new_v4();
    let last_id = Uuid::new_v4();
    let mut pending = VecDeque::from([
        QueuedPrompt {
            message_id: first_id,
            text: "first [Image 1]".to_string(),
            actor: EventActor::User,
            attachments: vec![first_image.clone()],
            output_schema: None,
            delivery: PromptDelivery::Queue,
            visible: true,
            interrupt_batch: true,
            batch: Vec::new(),
        },
        QueuedPrompt {
            message_id: second_id,
            text: "second".to_string(),
            actor: EventActor::User,
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Queue,
            visible: true,
            interrupt_batch: true,
            batch: Vec::new(),
        },
        QueuedPrompt {
            message_id: last_id,
            text: "last [Image 2]".to_string(),
            actor: EventActor::User,
            attachments: vec![last_image.clone()],
            output_schema: None,
            delivery: PromptDelivery::Queue,
            visible: true,
            interrupt_batch: true,
            batch: Vec::new(),
        },
    ]);

    coalesce_queued_prompts(&mut pending);

    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].message_id, last_id);
    assert_eq!(
        pending[0].text,
        "first [Image 1]\n\nsecond\n\nlast [Image 2]"
    );
    assert_eq!(pending[0].attachments, [first_image, last_image]);
    assert_eq!(pending[0].delivery, PromptDelivery::Queue);
    // Every durable message keeps its own identity and original text; the
    // combined text is provider input only and is never journaled as a
    // user message under the last member's id.
    assert_eq!(
        pending[0]
            .batch
            .iter()
            .map(|entry| (entry.message_id, entry.text.as_str()))
            .collect::<Vec<_>>(),
        [
            (first_id, "first [Image 1]"),
            (second_id, "second"),
            (last_id, "last [Image 2]")
        ]
    );
    assert_eq!(
        pending[0]
            .batch_entries()
            .iter()
            .map(|entry| entry.message_id)
            .collect::<Vec<_>>(),
        [first_id, second_id, last_id]
    );
}

#[tokio::test]
async fn batched_prompt_status_settles_every_durable_message() {
    let session_id = Uuid::new_v4();
    let (scratch, _store, mut runtime) = runtime_store(session_id).await;
    let (events, mut received) = mpsc::channel(16);
    let first_id = Uuid::new_v4();
    let second_id = Uuid::new_v4();
    let last_id = Uuid::new_v4();
    let prompt = |message_id: Uuid, text: &str| QueuedPrompt {
        message_id,
        text: text.to_string(),
        actor: EventActor::User,
        attachments: Vec::new(),
        output_schema: None,
        delivery: PromptDelivery::Queue,
        visible: true,
        interrupt_batch: true,
        batch: Vec::new(),
    };
    let mut pending = VecDeque::from([
        prompt(first_id, "first"),
        prompt(second_id, "second"),
        prompt(last_id, "last"),
    ]);
    coalesce_queued_prompts(&mut pending);
    let combined = pending.pop_front().unwrap();

    record_prompt_status(
        &mut runtime,
        &events,
        session_id,
        &combined,
        MessageStatus::Complete,
        PromptDelivery::Queue,
    )
    .await
    .unwrap();

    let mut settled = Vec::new();
    while let Ok(event) = received.try_recv() {
        if let SessionEventKind::Message {
            message_id,
            status: MessageStatus::Complete,
            ..
        } = event.kind
        {
            settled.push(message_id);
        }
    }
    assert_eq!(settled, [first_id, second_id, last_id]);
    scratch.discard().await;
}

#[test]
fn escape_batch_runs_user_prompts_before_separate_team_messages() {
    let prompt = |text: &str, interrupt_batch| QueuedPrompt {
        message_id: Uuid::new_v4(),
        text: text.to_string(),
        actor: if interrupt_batch {
            EventActor::User
        } else {
            EventActor::System
        },
        attachments: Vec::new(),
        output_schema: None,
        delivery: PromptDelivery::Queue,
        visible: true,
        interrupt_batch,
        batch: Vec::new(),
    };
    let mut pending = VecDeque::from([
        prompt("Team message from /root/worker:\n\ninternal report", false),
        prompt("first user follow-up", true),
        prompt("second user follow-up", true),
    ]);

    coalesce_queued_prompts(&mut pending);

    assert_eq!(pending.len(), 2);
    assert_eq!(
        pending[0].text,
        "first user follow-up\n\nsecond user follow-up"
    );
    assert_eq!(
        pending[1].text,
        "Team message from /root/worker:\n\ninternal report"
    );
    assert!(pending[0].interrupt_batch);
    assert!(!pending[1].interrupt_batch);
}

#[test]
fn pending_user_input_always_owns_the_next_turn_boundary() {
    let prompt = |actor, text: &str| QueuedPrompt {
        message_id: Uuid::new_v4(),
        text: text.to_string(),
        actor,
        attachments: Vec::new(),
        output_schema: None,
        delivery: PromptDelivery::Queue,
        visible: true,
        interrupt_batch: actor == EventActor::User,
        batch: Vec::new(),
    };
    let mut pending = VecDeque::from([
        prompt(EventActor::System, "internal report"),
        prompt(EventActor::User, "human request"),
    ]);

    let next = pop_next_pending_prompt(&mut pending, true).unwrap();

    assert_eq!(next.actor, EventActor::User);
    assert_eq!(next.text, "human request");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].actor, EventActor::System);
}

#[test]
fn resumed_team_backlog_is_deferred_behind_the_triggering_user_prompt() {
    let session_id = Uuid::new_v4();
    let current_user_id = Uuid::new_v4();
    let already_deferred_id = Uuid::new_v4();
    let team_ids = [Uuid::new_v4(), Uuid::new_v4()];
    let prompt = |message_id, text: &str| HostCommand::Prompt {
        session_id,
        message_id,
        text: text.to_string(),
        attachments: Vec::new(),
        output_schema: None,
        delivery: PromptDelivery::Queue,
    };
    let mut deferred = VecDeque::from([prompt(already_deferred_id, "next human prompt")]);
    let inbox = team_ids
        .into_iter()
        .map(|message_id| TeamInboxMessage {
            message_id,
            text: "Team message from /root/worker:\n\nold report".to_string(),
            report_text: "old report".to_string(),
            sender_session_id: Uuid::new_v4(),
            delivery: PromptDelivery::Queue,
            attachments: Vec::new(),
        })
        .collect();
    let mut team_message_ids = HashSet::new();

    defer_root_inbox_behind_current_command(
        &mut deferred,
        session_id,
        prompt(current_user_id, "triggering user prompt"),
        inbox,
        &mut team_message_ids,
    );

    let ordered_ids = deferred
        .iter()
        .filter_map(|command| match command {
            HostCommand::Prompt { message_id, .. } | HostCommand::TeamPrompt { message_id, .. } => {
                Some(*message_id)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        ordered_ids,
        vec![
            current_user_id,
            already_deferred_id,
            team_ids[0],
            team_ids[1]
        ]
    );
    assert!(!team_message_ids.contains(&current_user_id));
    assert!(team_ids.iter().all(|id| team_message_ids.contains(id)));
}

#[tokio::test]
async fn inactive_team_reports_settle_without_starting_a_provider_turn() {
    let session_id = Uuid::new_v4();
    let (scratch, store, mut runtime) = runtime_store(session_id).await;
    let (event_tx, mut event_rx) = mpsc::channel(8);
    let mut pending = VecDeque::from([QueuedPrompt {
        message_id: Uuid::new_v4(),
        text: "Team message from /root/worker:\n\nfinished".to_string(),
        actor: EventActor::System,
        attachments: Vec::new(),
        output_schema: None,
        delivery: PromptDelivery::Queue,
        visible: true,
        interrupt_batch: false,
        batch: Vec::new(),
    }]);

    settle_non_waking_team_notifications(
        &mut runtime,
        &event_tx,
        session_id,
        &mut pending,
        false,
        None,
    )
    .await
    .unwrap();

    assert!(pending.is_empty());
    let event = event_rx.recv().await.unwrap();
    assert!(matches!(
        event.kind,
        SessionEventKind::Message {
            actor: EventActor::System,
            status: MessageStatus::Complete,
            ..
        }
    ));
    assert!(
        !store
            .read(session_id)
            .await
            .unwrap()
            .iter()
            .any(|event| { matches!(event.kind, SessionEventKind::TurnStarted { .. }) })
    );
    scratch.discard().await;
}

#[tokio::test]
async fn inactive_wake_report_is_retained_for_the_root_provider_turn() {
    let session_id = Uuid::new_v4();
    let (scratch, _store, mut runtime) = runtime_store(session_id).await;
    let (event_tx, mut event_rx) = mpsc::channel(8);
    let message_id = Uuid::new_v4();
    let mut pending = VecDeque::from([QueuedPrompt {
        message_id,
        text: "Team message from /root/worker:\n\nfinished".to_string(),
        actor: EventActor::System,
        attachments: Vec::new(),
        output_schema: None,
        delivery: PromptDelivery::Steer,
        visible: true,
        interrupt_batch: false,
        batch: Vec::new(),
    }]);

    settle_non_waking_team_notifications(
        &mut runtime,
        &event_tx,
        session_id,
        &mut pending,
        false,
        None,
    )
    .await
    .unwrap();

    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].message_id, message_id);
    assert!(event_rx.try_recv().is_err());
    scratch.discard().await;
}

#[tokio::test]
async fn recovered_pending_batch_is_admitted_before_flush_at_turn_boundary() {
    let session_id = Uuid::new_v4();
    let (scratch, _store, mut journal) = runtime_store(session_id).await;
    let (event_tx, _event_rx) = mpsc::channel(8);
    let (command_tx, command_rx) = mpsc::channel(8);
    let ids = [Uuid::new_v4(), Uuid::new_v4()];
    command_tx
        .send(HostCommand::RecoverPendingInput {
            session_id,
            prompts: ids
                .iter()
                .enumerate()
                .map(|(index, message_id)| crate::RecoveredPendingPrompt {
                    message_id: *message_id,
                    text: format!("follow-up {index}"),
                    attachments: Vec::new(),
                    output_schema: None,
                })
                .collect(),
        })
        .await
        .unwrap();
    command_tx
        .send(HostCommand::FlushPendingInput { session_id })
        .await
        .unwrap();
    let mut commands = HostCommandInbox::new(command_rx, None);
    let mut pending = VecDeque::new();
    let mut deferred = VecDeque::new();
    let mut team_ids = HashSet::new();
    let mut stale = HashSet::new();
    collect_input_at_turn_boundary(
        &mut journal,
        &event_tx,
        session_id,
        &mut pending,
        &mut commands,
        &mut deferred,
        &mut team_ids,
        &mut stale,
    )
    .await
    .unwrap();
    coalesce_queued_prompts(&mut pending);
    assert!(deferred.is_empty());
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].text, "follow-up 0\n\nfollow-up 1");
    assert_eq!(
        pending[0]
            .batch_entries()
            .iter()
            .map(|entry| entry.message_id)
            .collect::<Vec<_>>(),
        ids
    );
    scratch.discard().await;
}

#[tokio::test]
async fn turn_boundary_waits_for_a_late_sibling_of_pending_input() {
    let session_id = Uuid::new_v4();
    let (scratch, _store, mut journal) = runtime_store(session_id).await;
    let (event_tx, _event_rx) = mpsc::channel(8);
    let (command_tx, command_rx) = mpsc::channel(8);
    let first_id = Uuid::new_v4();
    let second_id = Uuid::new_v4();
    let mut pending = VecDeque::new();
    let mut team_message_ids = HashSet::new();
    queue_pending_prompt(
        &mut journal,
        &event_tx,
        session_id,
        &mut pending,
        &mut team_message_ids,
        first_id,
        "first".into(),
        Vec::new(),
        None,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(5)).await;
        command_tx
            .send(HostCommand::Prompt {
                session_id,
                message_id: second_id,
                text: "second".into(),
                attachments: Vec::new(),
                output_schema: None,
                delivery: PromptDelivery::Queue,
            })
            .await
            .unwrap();
    });
    let mut command_rx = HostCommandInbox::new(command_rx, None);
    let mut deferred = VecDeque::new();
    let mut stale = HashSet::new();
    collect_input_at_turn_boundary(
        &mut journal,
        &event_tx,
        session_id,
        &mut pending,
        &mut command_rx,
        &mut deferred,
        &mut team_message_ids,
        &mut stale,
    )
    .await
    .unwrap();
    coalesce_queued_prompts(&mut pending);
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].text, "first\n\nsecond");
    assert_eq!(
        pending[0]
            .batch_entries()
            .iter()
            .map(|entry| entry.message_id)
            .collect::<Vec<_>>(),
        [first_id, second_id]
    );
    scratch.discard().await;
}

#[tokio::test]
async fn turn_boundary_collects_all_emitted_prompts_before_escape() {
    let session_id = Uuid::new_v4();
    let (scratch, _store, mut journal) = runtime_store(session_id).await;
    let (event_tx, _event_rx) = mpsc::channel(8);
    let (command_tx, command_rx) = mpsc::channel(8);
    let last_id = Uuid::new_v4();
    for (message_id, text) in [
        (Uuid::new_v4(), "first follow-up"),
        (last_id, "second follow-up"),
    ] {
        command_tx
            .send(HostCommand::Prompt {
                session_id,
                message_id,
                text: text.to_string(),
                attachments: Vec::new(),
                output_schema: None,
                delivery: PromptDelivery::Queue,
            })
            .await
            .unwrap();
    }
    command_tx
        .send(HostCommand::Interrupt { session_id })
        .await
        .unwrap();

    let mut pending = VecDeque::new();
    let mut command_rx = HostCommandInbox::new(command_rx, None);
    let mut deferred = VecDeque::new();
    let mut team_message_ids = HashSet::new();
    let mut stale_user_prompts = HashSet::new();
    let interrupted = collect_input_at_turn_boundary(
        &mut journal,
        &event_tx,
        session_id,
        &mut pending,
        &mut command_rx,
        &mut deferred,
        &mut team_message_ids,
        &mut stale_user_prompts,
    )
    .await
    .unwrap();

    assert!(interrupted);
    assert!(deferred.is_empty());
    assert_eq!(pending.len(), 2);
    assert_eq!(
        stale_user_prompts,
        pending.iter().map(|prompt| prompt.message_id).collect(),
        "prompts queued before Escape are snapshotted as pre-stop"
    );
    coalesce_queued_prompts(&mut pending);
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].message_id, last_id);
    assert_eq!(pending[0].text, "first follow-up\n\nsecond follow-up");
    scratch.discard().await;
}

#[test]
fn subagent_concurrency_defaults_to_sixteen_and_accepts_a_lower_launch_limit() {
    let mut launch = LaunchSession {
        request_id: Uuid::new_v4(),
        cwd: PathBuf::from("/workspace"),
        provider: CodingProvider::Codex,
        model: None,
        effort: None,
        fast: Some(false),
        response_language: crate::ResponseLanguage::Auto,
        permission_mode: PermissionMode::Manual,
        name: None,
        initial_prompt: None,
        capabilities: Default::default(),
        subagent_concurrency_limit: None,
        extension_skill_roots: Vec::new(),
        team_policy: None,
    };

    assert_eq!(
        subagent_concurrency_limit(&launch),
        crate::DEFAULT_MAX_SUBAGENTS
    );
    assert_eq!(crate::DEFAULT_MAX_SUBAGENTS, 16);

    launch.subagent_concurrency_limit = Some(4);
    assert_eq!(subagent_concurrency_limit(&launch), 4);

    launch.subagent_concurrency_limit = Some(0);
    assert!(validate_launch_session(&mut launch).is_err());
}

#[test]
fn launch_rejects_serialized_skill_root_outside_host_extension_bases() {
    let root = tempdir().unwrap();
    let cwd = root.path().join("workspace");
    std::fs::create_dir_all(cwd.join(".borg/extensions")).unwrap();
    let serialized = serde_json::json!({
        "request_id": Uuid::new_v4(),
        "cwd": cwd,
        "provider": "codex",
        "permission_mode": "manual",
        "extension_skill_roots": ["/tmp"]
    });
    let mut launch: LaunchSession = serde_json::from_value(serialized).unwrap();

    let error = validate_launch_session(&mut launch).unwrap_err();

    assert!(error.to_string().contains("outside this host"));
}

#[test]
fn extension_skill_root_resolution_accepts_project_and_user_bases() {
    let root = tempdir().unwrap();
    let project_base = root.path().join("workspace/.borg/extensions");
    let user_base = root.path().join("user-config/borg/extensions");
    let project_skill = project_base.join("trusted-project/skills");
    let user_skill = user_base.join("trusted-user/skills");
    std::fs::create_dir_all(&project_skill).unwrap();
    std::fs::create_dir_all(&user_skill).unwrap();
    let bases = vec![
        project_base.canonicalize().unwrap(),
        user_base.canonicalize().unwrap(),
    ];

    let resolved =
        resolve_extension_skill_roots(&[project_skill.clone(), user_skill.clone()], &bases)
            .unwrap();

    let mut expected = vec![
        project_skill.canonicalize().unwrap(),
        user_skill.canonicalize().unwrap(),
    ];
    expected.sort();
    assert_eq!(resolved, expected);
}

#[test]
fn extension_skill_root_resolution_rejects_sibling_and_missing_roots() {
    let root = tempdir().unwrap();
    let base = root.path().join("workspace/.borg/extensions");
    let sibling = root.path().join("workspace/.borg/not-extensions/skills");
    std::fs::create_dir_all(&base).unwrap();
    std::fs::create_dir_all(&sibling).unwrap();
    let bases = vec![base.canonicalize().unwrap()];

    let sibling_error = resolve_extension_skill_roots(&[sibling], &bases).unwrap_err();
    assert!(sibling_error.to_string().contains("outside this host"));

    let missing = base.join("trusted/skills");
    let missing_error = resolve_extension_skill_roots(&[missing], &bases).unwrap_err();
    assert!(missing_error.to_string().contains("missing or unreadable"));

    assert!(resolve_extension_skill_roots(&[], &[]).unwrap().is_empty());
}

#[test]
fn active_provider_steer_uses_turn_control_across_provider_lanes() {
    for provider in [
        CodingProvider::Codex,
        CodingProvider::Claude,
        CodingProvider::OpenRouter,
        CodingProvider::OpenAiCompatible,
    ] {
        let native = provider.uses_native_harness();
        assert!(steers_active_provider_turn(
            provider,
            PromptDelivery::Steer,
            native
        ));
        assert!(!steers_active_provider_turn(
            provider,
            PromptDelivery::Queue,
            native
        ));
    }
    // `opencode-go` steers because its session runs Borg's native harness even
    // though the billing provider itself is not native; the CLI compatibility
    // route (native_harness=false) still cannot take a steer.
    assert!(steers_active_provider_turn(
        CodingProvider::OpenCode,
        PromptDelivery::Steer,
        true
    ));
    assert!(!steers_active_provider_turn(
        CodingProvider::OpenCode,
        PromptDelivery::Steer,
        false
    ));
}

#[test]
fn queued_prompt_recovery_preserves_fifo_and_excludes_settled_messages() {
    let session_id = Uuid::new_v4();
    let settled_id = Uuid::new_v4();
    let pending_id = Uuid::new_v4();
    let events = vec![
        SessionEvent::new(
            session_id,
            1,
            SessionEventKind::Message {
                message_id: settled_id,
                actor: EventActor::User,
                text: "settled".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Queued,
                delivery: Some(PromptDelivery::Queue),
            },
        ),
        SessionEvent::new(
            session_id,
            2,
            SessionEventKind::Message {
                message_id: pending_id,
                actor: EventActor::User,
                text: "still pending".to_string(),
                attachments: vec![PathBuf::from("/tmp/image.png")],
                status: MessageStatus::Queued,
                delivery: Some(PromptDelivery::Steer),
            },
        ),
        SessionEvent::new(
            session_id,
            3,
            SessionEventKind::Message {
                message_id: settled_id,
                actor: EventActor::User,
                text: "settled".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: Some(PromptDelivery::Queue),
            },
        ),
    ];

    let recovered = recover_queued_prompts(&events);

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].message_id, pending_id);
    assert_eq!(recovered[0].text, "still pending");
    assert_eq!(recovered[0].attachments, [PathBuf::from("/tmp/image.png")]);
    assert_eq!(recovered[0].delivery, PromptDelivery::Queue);
}

#[test]
fn resume_recovers_unresolved_queue_entries_for_any_session_status() {
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    let queued = SessionEvent::new(
        session_id,
        1,
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text: "do not replay this old queue entry".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Queued,
            delivery: Some(PromptDelivery::Queue),
        },
    );
    let in_progress = SessionEvent::new(
        session_id,
        2,
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text: "recover this admitted turn".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: Some(PromptDelivery::Queue),
        },
    );

    let recovered = recover_prompts_on_resume(std::slice::from_ref(&queued));
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].message_id, message_id);
    assert_eq!(recovered[0].text, "do not replay this old queue entry");

    let recovered = recover_prompts_on_resume(&[queued, in_progress]);
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].message_id, message_id);
    assert_eq!(recovered[0].text, "recover this admitted turn");

    let recovered = recover_prompts_on_resume(&[SessionEvent::new(
        session_id,
        3,
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::User,
            text: "old queue entry without an admitted turn".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Queued,
            delivery: Some(PromptDelivery::Queue),
        },
    )]);
    assert_eq!(recovered.len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn resumed_session_drains_unresolved_input_and_preserves_last_context_usage() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let stale_id = Uuid::new_v4();
    let next_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        SessionEventKind::SessionConfigured {
            cwd: root.path().to_path_buf(),
            provider: CodingProvider::OpenRouter,
            model: Some("test-model".to_string()),
            effort: Some("medium".to_string()),
            fast: false,
            response_language: crate::ResponseLanguage::Auto,
            permission_mode: PermissionMode::FullAccess,
        },
        SessionEventKind::Message {
            message_id: stale_id,
            actor: EventActor::User,
            text: "stale queued input".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Queued,
            delivery: Some(PromptDelivery::Queue),
        },
        SessionEventKind::UsageUpdated {
            provider_duration_ms: 1,
            turn_id: None,
            provider_context_reused: None,
            input_tokens: 95_000,
            output_tokens: 0,
            cached_input_tokens: 0,
            cache_creation_input_tokens: 0,
            total_tokens: 95_000,
            cost_microusd: None,
            cost_basis: String::new(),
            cost_usd: None,
            context_tokens: Some(95_000),
            context_window_tokens: Some(100_000),
        },
        SessionEventKind::StatusChanged {
            status: SessionStatus::Ready,
            detail: None,
        },
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }

    let seen = Arc::new(Mutex::new(Vec::new()));
    let called = Arc::new(Notify::new());
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(64);
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn({
        let root = root.path().to_path_buf();
        let seen = Arc::clone(&seen);
        let called = Arc::clone(&called);
        async move {
            run_session_actor(
                &journal_path,
                session_id,
                LaunchSession {
                    request_id: Uuid::new_v4(),
                    cwd: root,
                    provider: CodingProvider::OpenRouter,
                    model: Some("test-model".to_string()),
                    effort: Some("medium".to_string()),
                    fast: Some(false),
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::FullAccess,
                    name: None,
                    initial_prompt: None,
                    capabilities: Default::default(),
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                Arc::new(RecordingExecutor { seen, called }),
                actor_store,
            )
            .await
        }
    });

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let event = event_rx.recv().await.expect("session remains live");
            if matches!(
                event.kind,
                SessionEventKind::TurnCompleted { message_id, error: None, .. }
                    if message_id == stale_id
            ) {
                break;
            }
        }
    })
    .await
    .expect("unresolved input starts and completes during resume");
    assert_eq!(seen.lock().unwrap().len(), 1);
    assert!(
        !store
            .live_events_after(session_id, 0)
            .await
            .unwrap()
            .iter()
            .any(|live| matches!(
                live.event.kind,
                SessionEventKind::ContextWindowUpdated {
                    context_tokens: 0,
                    ..
                }
            ))
    );
    assert_eq!(
        store.state(session_id).await.unwrap().usage.context_tokens,
        Some(95_000)
    );

    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: next_id,
            text: "fresh input".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Queue,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let event = event_rx.recv().await.expect("session remains live");
            if matches!(
                event.kind,
                SessionEventKind::TurnCompleted { message_id, error: None, .. }
                    if message_id == next_id
            ) {
                break;
            }
        }
    })
    .await
    .expect("fresh input starts a turn");
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();

    assert_eq!(
        store
            .action(session_id, stale_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        crate::SessionActionState::Completed
    );
    scratch.discard().await;
}

#[test]
fn in_progress_prompt_recovery_preserves_input_after_a_host_crash() {
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    let events = vec![
        SessionEvent::new(
            session_id,
            1,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: "recover this request".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Queued,
                delivery: Some(PromptDelivery::Steer),
            },
        ),
        SessionEvent::new(
            session_id,
            2,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: "recover this request".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::InProgress,
                delivery: Some(PromptDelivery::Steer),
            },
        ),
    ];

    let recovered = recover_queued_prompts(&events);

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].message_id, message_id);
    assert_eq!(recovered[0].text, "recover this request");
    assert_eq!(recovered[0].delivery, PromptDelivery::Queue);
}

fn crashed_turn_events(session_id: Uuid, message_id: Uuid) -> Vec<SessionEventKind> {
    vec![
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text: "how often does it send them".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Queued,
            delivery: Some(PromptDelivery::Steer),
        },
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text: "how often does it send them".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: Some(PromptDelivery::Queue),
        },
        SessionEventKind::TurnStarted {
            message_id,
            provider: CodingProvider::Codex,
            model: None,
            effort: None,
            fast: false,
        },
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::Assistant,
            text: "every 90% of the cache lifetime".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
        SessionEventKind::ToolStarted {
            tool_call_id: format!("call-{session_id}"),
            name: "Bash".to_string(),
            input: json!({}),
            input_ref: None,
        },
    ]
}

#[tokio::test]
async fn a_turn_cut_off_by_a_crash_resumes_its_prompt_instead_of_re_asking_it() {
    // A host OOM killed a605872f mid-turn on 2026-09-20. The prompt that
    // opened that turn had already been answered, but `Complete` is only
    // written when a turn ends, so it stayed at `InProgress` and resume
    // re-dispatched it as a brand new turn: the same question answered
    // twice, 13 minutes apart.
    //
    // Both halves of the contract are asserted here, because either one
    // alone is a different bug. The prompt must STILL be recovered -- that
    // turn went on working for seven minutes after it answered, and dropping
    // it would silently discard unfinished work. And it must be recognisable
    // as interrupted, so dispatch can resume it rather than ask again.
    //
    // Driven through the real recovery projection on purpose: the turn
    // boundaries this depends on live only in `context_events`, and a
    // hand-built slice would let a scan that can never fire in production
    // look correct here.
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in crashed_turn_events(session_id, message_id) {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }

    let recovery = store.recovery(session_id).await.unwrap();
    assert_eq!(
        interrupted_turn_prompt(&recovery.context_events),
        Some(message_id),
        "an unmatched TurnStarted is the only durable evidence that the turn was cut off"
    );
    assert_eq!(
        recover_prompts_on_resume(&recovery.queue_events)
            .iter()
            .map(|prompt| prompt.message_id)
            .collect::<Vec<_>>(),
        vec![message_id],
        "the interrupted prompt is resumed, not dropped: its turn had unfinished work"
    );
    scratch.discard().await;
}

#[tokio::test]
async fn a_turn_killed_before_it_produced_anything_replays_its_prompt_verbatim() {
    // The other side of the line, and the one that keeps this honest. A turn
    // whose boundary is journaled and is then killed before the provider
    // emitted anything has nothing to continue from. Telling the model it may
    // already have answered and made progress would be false, and the request
    // it never acted on is owed a plain replay -- byte for byte, so a
    // subscription prompt still ends with exactly the input.
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text: "recover this exact input".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: Some(PromptDelivery::Queue),
        },
        SessionEventKind::TurnStarted {
            message_id,
            provider: CodingProvider::Codex,
            model: None,
            effort: None,
            fast: false,
        },
        // The host died here, between the boundary and the first output.
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }

    let recovery = store.recovery(session_id).await.unwrap();
    assert_eq!(
        interrupted_turn_prompt(&recovery.context_events),
        None,
        "a boundary alone is not progress: there is nothing to continue from"
    );
    assert_eq!(
        recover_prompts_on_resume(&recovery.queue_events)
            .iter()
            .map(|prompt| prompt.message_id)
            .collect::<Vec<_>>(),
        vec![message_id],
        "and the input it never acted on is still replayed"
    );
    scratch.discard().await;
}

#[tokio::test]
async fn a_resumed_turn_continues_the_original_prompt_and_settles_it_once() {
    // The runtime half of the crash-resume contract. The classifier test
    // above would still pass if the dispatch and admission wiring were
    // deleted, so this drives a real actor over a journal that ends inside a
    // turn and checks what the provider was actually handed.
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        SessionEventKind::SessionConfigured {
            cwd: root.path().to_path_buf(),
            provider: CodingProvider::Codex,
            model: Some("gpt-5.6-luna".to_string()),
            effort: Some("max".to_string()),
            fast: false,
            response_language: crate::ResponseLanguage::Auto,
            permission_mode: PermissionMode::Manual,
        },
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text: "how often does it send them".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: Some(PromptDelivery::Queue),
        },
        SessionEventKind::TurnStarted {
            message_id,
            provider: CodingProvider::Codex,
            model: Some("gpt-5.6-luna".to_string()),
            effort: Some("max".to_string()),
            fast: false,
        },
        // The answer the human already read, and a side effect that already
        // landed. Both must reach the resumed turn as history.
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::Assistant,
            text: "every 90% of the cache lifetime".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
        SessionEventKind::ToolStarted {
            tool_call_id: "call-1".to_string(),
            name: "Bash".to_string(),
            input: json!({}),
            input_ref: None,
        },
        SessionEventKind::ToolCompleted {
            tool_call_id: "call-1".to_string(),
            output: "worker spawned".to_string(),
            output_ref: None,
            is_error: false,
            input: None,
            input_ref: None,
        },
        // No TurnCompleted: this is the host dying mid-turn.
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }
    let admissions_before = in_progress_admissions(store.as_ref(), session_id, message_id).await;
    assert_eq!(admissions_before, 1, "the crashed run admitted it once");

    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, _event_rx) = mpsc::channel(256);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let called = Arc::new(Notify::new());
    let compaction_calls = Arc::new(AtomicUsize::new(0));
    let executor = Arc::new(DurableResumeExecutor {
        seen: Arc::clone(&seen),
        called: Arc::clone(&called),
        compaction_calls: Arc::clone(&compaction_calls),
    });
    let actor_store = Arc::clone(&store);
    let actor_cwd = root.path().to_path_buf();
    let actor = tokio::spawn(async move {
        run_session_actor(
            &journal_path,
            session_id,
            LaunchSession {
                request_id: Uuid::new_v4(),
                cwd: actor_cwd,
                provider: CodingProvider::Codex,
                model: Some("gpt-5.6-luna".to_string()),
                effort: Some("max".to_string()),
                fast: Some(false),
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
                name: None,
                initial_prompt: None,
                capabilities: Default::default(),
                subagent_concurrency_limit: None,
                extension_skill_roots: Vec::new(),
                team_policy: None,
            },
            command_rx,
            event_tx,
            executor,
            actor_store,
        )
        .await
    });

    // Nothing is sent: recovery alone must resume the interrupted turn.
    tokio::time::timeout(Duration::from_secs(10), called.notified())
        .await
        .expect("the interrupted turn is resumed without a new prompt");
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();

    let dispatched = {
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1, "resumed exactly once");
        seen[0].clone()
    };
    let (sent_prompt, _, _, _) = dispatched;
    assert!(
        sent_prompt.contains("how often does it send them"),
        "the original request is re-delivered verbatim, not paraphrased"
    );
    assert!(
        sent_prompt.ends_with(RESUMED_TURN_CONTINUATION),
        "the dispatch is demoted to a continuation rather than repeated as an instruction"
    );
    // On the channel that actually carries it. A subscription provider is
    // handed its history inside the prompt, as the canonical replay
    // `retained_conversation_context` builds; `turn.conversation` is
    // structurally empty for every non-native provider, so counting it here
    // would have tested nothing. Asserting the answer text is also stricter
    // than a count: the continuation tells the model the conversation above
    // records what was done, and this is what makes that true.
    assert!(
        sent_prompt.contains("every 90% of the cache lifetime"),
        "the resumed turn is handed the answer the crashed turn already gave"
    );
    assert!(
        sent_prompt.contains("worker spawned"),
        "and the side effect that already landed, so it does not run it twice"
    );

    let events = store.read(session_id).await.unwrap();
    assert_eq!(
        in_progress_admissions(store.as_ref(), session_id, message_id).await,
        admissions_before,
        "a resumed prompt is not admitted a second time: its original admission, \
         and its original timestamp, are already durable"
    );
    assert!(
        events.iter().any(|event| matches!(
            &event.kind,
            SessionEventKind::Message {
                message_id: id,
                status: MessageStatus::Complete,
                actor: EventActor::User,
                ..
            } if *id == message_id
        )),
        "the resumed turn still settles the message, so it cannot be recovered again"
    );
    assert!(
        events.iter().any(|event| matches!(
            &event.kind,
            SessionEventKind::TurnCompleted { message_id: id, .. } if *id == message_id
        )),
        "the resumed turn reaches a durable boundary"
    );

    let recovery = store.recovery(session_id).await.unwrap();
    assert_eq!(
        interrupted_turn_prompt(&recovery.context_events),
        None,
        "the turn is no longer interrupted once it completed"
    );
    assert!(
        recover_prompts_on_resume(&recovery.queue_events).is_empty(),
        "a second resume has nothing left to replay"
    );
    scratch.discard().await;
}

async fn in_progress_admissions(
    store: &dyn SessionStore,
    session_id: Uuid,
    message_id: Uuid,
) -> usize {
    store
        .read(session_id)
        .await
        .unwrap()
        .iter()
        .filter(|event| {
            matches!(
                &event.kind,
                SessionEventKind::Message {
                    message_id: id,
                    status: MessageStatus::InProgress,
                    ..
                } if *id == message_id
            )
        })
        .count()
}

#[tokio::test]
async fn a_resumed_turn_that_hits_a_usage_limit_checkpoints_as_a_continuation() {
    // The boundary after the boundary. A turn the host killed is resumed as
    // a continuation, and then that resumed attempt hits a usage limit
    // before making any side effects of its own. The checkpoint it writes is
    // what the NEXT restart reads, so if it persists the bare prompt the
    // original question is asked again one boundary later -- the same defect
    // this contract exists to prevent, just deferred.
    //
    // The progress being continued predates this attempt: it is in the
    // conversation, not in this turn's side effects, which is exactly why
    // `turn_had_side_effects` alone is the wrong test here.
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        SessionEventKind::SessionConfigured {
            cwd: root.path().to_path_buf(),
            provider: CodingProvider::Codex,
            model: None,
            effort: None,
            fast: false,
            response_language: crate::ResponseLanguage::Auto,
            permission_mode: PermissionMode::Manual,
        },
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text: "finish the committed work".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: Some(PromptDelivery::Queue),
        },
        SessionEventKind::TurnStarted {
            message_id,
            provider: CodingProvider::Codex,
            model: None,
            effort: None,
            fast: false,
        },
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::Assistant,
            text: "starting on it".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
        // No TurnCompleted: the host died inside this turn.
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }

    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(256);
    let executor = Arc::new(UsageLimitThenSuccessExecutor {
        calls: Arc::new(AtomicUsize::new(0)),
        side_effects_before_limit: false,
        prompts: Arc::new(Mutex::new(Vec::new())),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn({
        let journal_path = root.path().join("session.lock");
        let cwd = root.path().to_path_buf();
        async move {
            run_session_actor(
                &journal_path,
                session_id,
                LaunchSession {
                    request_id: Uuid::new_v4(),
                    cwd,
                    provider: CodingProvider::Codex,
                    model: None,
                    effort: None,
                    fast: Some(false),
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::Manual,
                    name: None,
                    initial_prompt: None,
                    capabilities: Default::default(),
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                executor,
                actor_store,
            )
            .await
        }
    });

    let mut checkpoint = None;
    while checkpoint.is_none() {
        let Some(event) = tokio::time::timeout(Duration::from_secs(10), event_rx.recv())
            .await
            .expect("the resumed turn checkpoints when it hits the usage limit")
        else {
            panic!("session exited before writing a usage limit checkpoint");
        };
        if let SessionEventKind::ProviderEvent { kind, payload, .. } = &event.kind
            && kind == "usage_limit_retry"
        {
            checkpoint = Some(
                serde_json::from_value::<crate::session_store::PendingUsageLimitRetry>(
                    payload.clone(),
                )
                .unwrap(),
            );
        }
    }
    let checkpoint = checkpoint.unwrap();
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    let _ = actor.await.unwrap();

    assert!(
        checkpoint.continuation,
        "a resumed turn is still a continuation when it parks on a usage limit; \
         without this its originals never settle and it is re-announced and re-asked"
    );
    // Structural rather than prose, so rewording the continuation text does
    // not break a test that is here for the wiring.
    assert_ne!(
        checkpoint.prompt.message_id, message_id,
        "the checkpoint carries a continuation prompt, not the bare original"
    );
    assert_eq!(checkpoint.prompt.actor, EventActor::System);
    assert!(!checkpoint.prompt.visible);
    assert!(
        checkpoint.prompt.text.contains("finish the committed work"),
        "the original request travels with the continuation as reference data"
    );
    assert!(
        checkpoint.replaced_message_ids.contains(&message_id),
        "the original is named so the next resume settles it instead of replaying it"
    );
    scratch.discard().await;
}

#[tokio::test]
async fn a_turn_that_reached_its_boundary_is_not_resumed_as_a_continuation() {
    // The other half. Once `TurnCompleted` is durable the turn ended on its
    // own terms, so nothing about it is interrupted and a later prompt with
    // the same id is an ordinary new request.
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in crashed_turn_events(session_id, message_id)
        .into_iter()
        .chain([SessionEventKind::TurnCompleted {
            message_id,
            provider_session_id: None,
            final_text: "every 90% of the cache lifetime".to_string(),
            error: None,
        }])
    {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }

    let recovery = store.recovery(session_id).await.unwrap();
    assert_eq!(
        interrupted_turn_prompt(&recovery.context_events),
        None,
        "a turn that journaled its own boundary was not cut off"
    );
    scratch.discard().await;
}

#[test]
fn prompt_recovery_updates_delivery_and_ignores_stale_terminal_snapshots() {
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    let events = vec![
        SessionEvent::new(
            session_id,
            1,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: "retry me".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Queued,
                delivery: Some(PromptDelivery::Steer),
            },
        ),
        SessionEvent::new(
            session_id,
            2,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: "retry me".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Queued,
                delivery: Some(PromptDelivery::Queue),
            },
        ),
        SessionEvent::new(
            session_id,
            3,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: "retry me".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: Some(PromptDelivery::Queue),
            },
        ),
        // A completed action cannot be retried by reusing its id.
        SessionEvent::new(
            session_id,
            4,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: "invalid completed retry".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Queued,
                delivery: Some(PromptDelivery::Queue),
            },
        ),
        // This is a stale durable snapshot from before the terminal event.
        SessionEvent::new(
            session_id,
            5,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: "coalesced retry me".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::InProgress,
                delivery: Some(PromptDelivery::Queue),
            },
        ),
    ];

    assert!(recover_queued_prompts(&events).is_empty());
}

#[test]
fn prompt_recovery_allows_an_explicit_retry_after_terminal_status() {
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    let events = vec![
        SessionEvent::new(
            session_id,
            1,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: "try again".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Failed,
                delivery: Some(PromptDelivery::Queue),
            },
        ),
        SessionEvent::new(
            session_id,
            2,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: "try again".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Queued,
                delivery: Some(PromptDelivery::Queue),
            },
        ),
        SessionEvent::new(
            session_id,
            3,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: "try again".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::InProgress,
                delivery: Some(PromptDelivery::Queue),
            },
        ),
    ];

    let recovered = recover_queued_prompts(&events);
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].message_id, message_id);
    assert_eq!(recovered[0].delivery, PromptDelivery::Queue);
}

#[test]
fn terminal_active_steer_is_not_recovered() {
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    let events = vec![
        SessionEvent::new(
            session_id,
            1,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: "preserve this pending steer".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Queued,
                delivery: Some(PromptDelivery::Steer),
            },
        ),
        SessionEvent::new(
            session_id,
            2,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: "preserve this pending steer".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::InProgress,
                delivery: Some(PromptDelivery::Steer),
            },
        ),
        SessionEvent::new(
            session_id,
            3,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: "preserve this pending steer".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: Some(PromptDelivery::Steer),
            },
        ),
    ];

    let recovered = recover_queued_prompts(&events);

    assert!(recovered.is_empty());
}

#[test]
fn automatic_transport_recovery_is_single_shot_and_side_effect_free() {
    assert!(is_safe_automatic_retry_error(
        "codex returned an empty response"
    ));
    assert!(is_safe_automatic_retry_error(
        "Codex durable thread recovery unavailable; retry from Borg's durable journal"
    ));
    assert!(!is_safe_automatic_retry_error("turn interrupted"));
    assert!(!is_safe_automatic_retry_error("tool execution failed"));
    assert!(!automatic_retry_allowed(
        "Codex exposed a forbidden provider-native agent tool: subAgentActivity",
        false,
        true,
        EventActor::User,
        true,
        true,
    ));
    assert!(!automatic_retry_allowed(
        "Codex exposed a forbidden provider-native agent tool: subAgentActivity",
        false,
        true,
        EventActor::User,
        false,
        false,
    ));
    assert!(automatic_retry_allowed(
        "Codex exposed a forbidden provider-native agent tool: subAgentActivity",
        false,
        true,
        EventActor::User,
        false,
        true,
    ));
    // A replaced pooled process is detected before the provider runs, so the
    // replay retry is safe even for invisible or system-delivered prompts.
    assert!(automatic_retry_allowed(
        "durable thread recovery unavailable: the pooled Claude process for this session was replaced",
        false,
        false,
        EventActor::System,
        false,
        true,
    ));
    assert!(!automatic_retry_allowed(
        "durable thread recovery unavailable: the pooled Claude process for this session was replaced",
        false,
        false,
        EventActor::System,
        true,
        true,
    ));
}

#[test]
fn recovered_team_messages_stay_out_of_escape_batches() {
    let session_id = Uuid::new_v4();
    let events = vec![SessionEvent::new(
        session_id,
        1,
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::System,
            text: "Team message from /root/worker:\n\ninternal report".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Queued,
            delivery: Some(PromptDelivery::Queue),
        },
    )];

    let recovered = recover_queued_prompts(&events);

    assert_eq!(recovered.len(), 1);
    assert!(!recovered[0].interrupt_batch);
}

#[test]
fn queued_prompt_recovery_discards_entries_bypassed_by_later_admission() {
    let session_id = Uuid::new_v4();
    let stale_id = Uuid::new_v4();
    let admitted_id = Uuid::new_v4();
    let events = vec![
        SessionEvent::new(
            session_id,
            1,
            SessionEventKind::Message {
                message_id: stale_id,
                actor: EventActor::User,
                text: "stale".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Queued,
                delivery: Some(PromptDelivery::Queue),
            },
        ),
        SessionEvent::new(
            session_id,
            2,
            SessionEventKind::Message {
                message_id: admitted_id,
                actor: EventActor::User,
                text: "later".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Queued,
                delivery: Some(PromptDelivery::Queue),
            },
        ),
        SessionEvent::new(
            session_id,
            3,
            SessionEventKind::Message {
                message_id: admitted_id,
                actor: EventActor::User,
                text: "later".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: Some(PromptDelivery::Queue),
            },
        ),
    ];

    assert!(recover_queued_prompts(&events).is_empty());
}

#[test]
fn committed_steer_does_not_consume_a_separate_next_turn_queue_on_resume() {
    let session_id = Uuid::new_v4();
    let queued_id = Uuid::new_v4();
    let steer_id = Uuid::new_v4();
    let events = vec![
        SessionEvent::new(
            session_id,
            1,
            SessionEventKind::Message {
                message_id: queued_id,
                actor: EventActor::User,
                text: "run next".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Queued,
                delivery: Some(PromptDelivery::Queue),
            },
        ),
        SessionEvent::new(
            session_id,
            2,
            SessionEventKind::Message {
                message_id: steer_id,
                actor: EventActor::User,
                text: "steer now".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Queued,
                delivery: Some(PromptDelivery::Steer),
            },
        ),
        SessionEvent::new(
            session_id,
            3,
            SessionEventKind::Message {
                message_id: steer_id,
                actor: EventActor::User,
                text: "steer now".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: Some(PromptDelivery::Steer),
            },
        ),
    ];

    let recovered = recover_queued_prompts(&events);

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].message_id, queued_id);
    assert_eq!(recovered[0].delivery, PromptDelivery::Queue);
}

#[test]
fn native_replay_preserves_an_interrupted_incomplete_tool_round() {
    use borg_provider::provider::{ModelMessage, ModelToolCall};

    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    let native = |sequence, message: ModelMessage| {
        SessionEvent::new(
            session_id,
            sequence,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::OpenRouter,
                kind: "native_model_message".to_string(),
                payload: serde_json::to_value(message).unwrap(),
            },
        )
    };
    let events = vec![
        native(1, ModelMessage::user("inspect")),
        native(
            2,
            ModelMessage::assistant(
                None,
                None,
                None,
                vec![ModelToolCall::function(
                    "one".to_string(),
                    "read_file".to_string(),
                    r#"{"path":"Cargo.toml"}"#.to_string(),
                )],
            ),
        ),
        native(
            3,
            ModelMessage::Tool {
                tool_call_id: "one".to_string(),
                content: "workspace".to_string(),
                attachments: Vec::new(),
            },
        ),
        SessionEvent::new(
            session_id,
            4,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::OpenRouter,
                kind: "native_tool_round_completed".to_string(),
                payload: json!({ "round": 1 }),
            },
        ),
        native(
            5,
            ModelMessage::assistant(
                None,
                None,
                None,
                vec![ModelToolCall::function(
                    "two".to_string(),
                    "read_file".to_string(),
                    r#"{"path":"missing"}"#.to_string(),
                )],
            ),
        ),
        SessionEvent::new(
            session_id,
            6,
            SessionEventKind::TurnCompleted {
                message_id,
                provider_session_id: None,
                final_text: String::new(),
                error: Some("turn interrupted".to_string()),
            },
        ),
    ];

    let replay = native_conversation(&events, CodingProvider::OpenRouter).unwrap();
    assert_eq!(replay.len(), 5);
    assert!(matches!(replay[0], ModelMessage::User { .. }));
    assert!(matches!(replay[2], ModelMessage::Tool { .. }));
    assert!(
        matches!(&replay[4], ModelMessage::Tool { tool_call_id, content, .. }
        if tool_call_id == "two" && content.contains("outcome unknown"))
    );
}

#[test]
fn an_oversized_native_model_message_is_deferred_and_replays_identically() {
    use crate::session_store::{
        INLINE_SESSION_PAYLOAD_BYTES, deferred_provider_payload, deferred_provider_payload_ref,
        oversized_provider_payload_bytes, resolve_provider_payload,
    };
    use crate::{SessionPayloadKind, SessionPayloadRef};
    use borg_provider::provider::ModelMessage;

    let session_id = Uuid::new_v4();
    let message = ModelMessage::user("x".repeat(INLINE_SESSION_PAYLOAD_BYTES + 1024));
    let payload = serde_json::to_value(&message).unwrap();
    let event = |payload| {
        SessionEvent::new(
            session_id,
            1,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "native_model_message".to_string(),
                payload,
            },
        )
    };

    // The store defers a payload above the inline limit and keeps only a
    // reference behind.
    let bytes = oversized_provider_payload_bytes(&payload)
        .unwrap()
        .expect("a payload above the inline limit must be deferred");
    let reference = SessionPayloadRef {
        id: Uuid::new_v4(),
        kind: SessionPayloadKind::ProviderModelMessage,
        byte_len: bytes.len() as u64,
    };
    let deferred = event(deferred_provider_payload(&reference));
    assert_eq!(
        deferred.kind.deferred_provider_payload_ref(),
        Some(reference.clone())
    );
    // The reference is what a relay transfer moves, so it must be visible
    // through the same accessor tool payloads use.
    assert_eq!(deferred.kind.payload_refs(), vec![reference]);
    assert!(
        serde_json::to_vec(&deferred.kind).unwrap().len()
            < serde_json::to_vec(&payload).unwrap().len()
    );

    // A legacy inline event still replays unchanged.
    let inline = event(payload);
    let expected =
        native_conversation(std::slice::from_ref(&inline), CodingProvider::Codex).unwrap();
    assert_eq!(expected, vec![message]);

    // Resolving the reference reconstructs the identical model message.
    let mut resolved = deferred.clone();
    {
        let SessionEventKind::ProviderEvent { payload, .. } = &mut resolved.kind else {
            unreachable!("provider event");
        };
        resolve_provider_payload(payload, &bytes).unwrap();
        assert_eq!(
            deferred_provider_payload_ref(payload),
            None,
            "a resolved payload is no longer deferred"
        );
    }
    assert_eq!(
        native_conversation(std::slice::from_ref(&resolved), CodingProvider::Codex).unwrap(),
        expected
    );
}

#[test]
fn native_replay_keeps_completed_batch_results_after_failure_or_crash() {
    use borg_provider::provider::{ModelMessage, ModelToolCall};

    let session_id = Uuid::new_v4();
    let calls = ["completed", "uncertain"]
        .map(|id| {
            ModelToolCall::function(
                id.to_string(),
                "exec".to_string(),
                r#"{"cmd":"perform action"}"#.to_string(),
            )
        })
        .to_vec();
    let assistant = ModelMessage::Assistant {
        content: None,
        reasoning_content: None,
        reasoning_details: None,
        tool_calls: calls,
        provider_state: Some(serde_json::from_value(json!({
            "protocol": "open_ai_responses",
            "output": [
                {"type": "reasoning", "encrypted_content": "opaque"},
                {"type": "function_call", "call_id": "completed", "name": "exec", "arguments": "{}"},
                {"type": "function_call", "call_id": "uncertain", "name": "exec", "arguments": "{}"},
            ],
        })).unwrap()),
    };
    let completed = ModelMessage::Tool {
        tool_call_id: "completed".to_string(),
        content: "action succeeded exactly once".to_string(),
        attachments: Vec::new(),
    };
    for error in [
        None,
        Some("turn interrupted"),
        Some("connection lost"),
        Some("provider failed"),
    ] {
        let mut events = [
            ModelMessage::user("perform actions"),
            assistant.clone(),
            completed.clone(),
        ]
        .into_iter()
        .enumerate()
        .map(|(sequence, message)| {
            SessionEvent::new(
                session_id,
                sequence as u64 + 1,
                SessionEventKind::ProviderEvent {
                    provider: CodingProvider::Codex,
                    kind: "native_model_message".to_string(),
                    payload: serde_json::to_value(message).unwrap(),
                },
            )
        })
        .collect::<Vec<_>>();
        events.insert(
            0,
            SessionEvent::new(
                session_id,
                0,
                SessionEventKind::TurnStarted {
                    message_id: Uuid::new_v4(),
                    provider: CodingProvider::Codex,
                    model: None,
                    effort: None,
                    fast: false,
                },
            ),
        );
        events.push(SessionEvent::new(
            session_id,
            4,
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::User,
                text: "stop the remaining actions".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: Some(PromptDelivery::Steer),
            },
        ));
        if let Some(error) = error {
            events.push(SessionEvent::new(
                session_id,
                5,
                SessionEventKind::TurnCompleted {
                    message_id: Uuid::new_v4(),
                    provider_session_id: None,
                    final_text: String::new(),
                    error: Some(error.to_string()),
                },
            ));
        }
        let replay = native_conversation(&events, CodingProvider::Codex).unwrap();
        assert_eq!(replay.len(), 5, "{error:?}");
        assert_eq!(replay[1], assistant);
        assert_eq!(replay[2], completed);
        assert!(
            matches!(&replay[3], ModelMessage::Tool { tool_call_id, content, .. }
            if tool_call_id == "uncertain" && content.contains("outcome unknown"))
        );
        assert_eq!(replay[4], ModelMessage::user("stop the remaining actions"));
        let resumed_id = Uuid::new_v4();
        events.push(SessionEvent::new(
            session_id,
            6,
            SessionEventKind::TurnStarted {
                message_id: resumed_id,
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: false,
            },
        ));
        events.push(SessionEvent::new(
            session_id,
            7,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "native_model_message".to_string(),
                payload: serde_json::to_value(ModelMessage::user("check current state")).unwrap(),
            },
        ));
        events.push(SessionEvent::new(
            session_id,
            8,
            SessionEventKind::TurnCompleted {
                message_id: resumed_id,
                provider_session_id: None,
                final_text: String::new(),
                error: None,
            },
        ));
        let resumed = native_conversation(&events, CodingProvider::Codex).unwrap();
        assert_eq!(&resumed[..5], replay.as_slice());
        assert_eq!(resumed[5], ModelMessage::user("check current state"));
    }
}

#[test]
fn native_replay_restarts_from_the_latest_compaction_summary() {
    use borg_provider::provider::ModelMessage;

    let session_id = Uuid::new_v4();
    let native = |sequence, content: &str| {
        SessionEvent::new(
            session_id,
            sequence,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::OpenRouter,
                kind: "native_model_message".to_string(),
                payload: serde_json::to_value(ModelMessage::user(content)).unwrap(),
            },
        )
    };
    let events = vec![
        native(1, "old context"),
        SessionEvent::new(
            session_id,
            2,
            SessionEventKind::TurnCompleted {
                message_id: Uuid::new_v4(),
                provider_session_id: None,
                final_text: String::new(),
                error: None,
            },
        ),
        SessionEvent::new(
            session_id,
            3,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::OpenRouter,
                kind: "context_compaction".to_string(),
                payload: json!({ "summary": "kept decisions" }),
            },
        ),
        native(4, "new context"),
        SessionEvent::new(
            session_id,
            5,
            SessionEventKind::TurnCompleted {
                message_id: Uuid::new_v4(),
                provider_session_id: None,
                final_text: String::new(),
                error: None,
            },
        ),
    ];

    let replay = native_conversation(&events, CodingProvider::OpenRouter).unwrap();
    assert_eq!(replay.len(), 2);
    assert_eq!(
        replay[0],
        ModelMessage::user("Previous conversation summary:\n\nkept decisions")
    );
    assert_eq!(replay[1], ModelMessage::user("new context"));
}

#[test]
fn native_replay_retains_provider_reasoning_without_text_reconstruction() {
    use borg_provider::provider::{ModelMessage, ModelToolCall};

    let session_id = Uuid::new_v4();
    let assistant = ModelMessage::assistant(
        Some("working".to_string()),
        Some("private retained reasoning".to_string()),
        Some(serde_json::json!([{
            "type": "reasoning.text",
            "text": "private retained reasoning"
        }])),
        vec![ModelToolCall::function(
            "tool-1".to_string(),
            "read_file".to_string(),
            r#"{"path":"README.md"}"#.to_string(),
        )],
    );
    let events = vec![
        SessionEvent::new(
            session_id,
            1,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::OpenRouter,
                kind: "native_model_message".to_string(),
                payload: serde_json::to_value(&assistant).unwrap(),
            },
        ),
        SessionEvent::new(
            session_id,
            2,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::OpenRouter,
                kind: "native_tool_round_completed".to_string(),
                payload: serde_json::json!({ "round": 1 }),
            },
        ),
    ];

    assert_eq!(
        native_conversation(&events, CodingProvider::OpenRouter).unwrap(),
        vec![assistant]
    );
}

#[test]
fn mutable_prompt_context_replays_after_the_user_tail_without_breaking_prefix_order() {
    use borg_provider::provider::ModelMessage;

    let native_session = Uuid::new_v4();
    let native_events = vec![
        SessionEvent::new(
            native_session,
            1,
            SessionEventKind::TurnStarted {
                message_id: Uuid::new_v4(),
                provider: CodingProvider::OpenRouter,
                model: None,
                effort: None,
                fast: false,
            },
        ),
        SessionEvent::new(
            native_session,
            2,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::OpenRouter,
                kind: "native_model_message".to_string(),
                payload: serde_json::to_value(ModelMessage::user("request")).unwrap(),
            },
        ),
        SessionEvent::new(
            native_session,
            3,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::OpenRouter,
                kind: crate::prompt_context::PROMPT_CONTEXT_EVENT.to_string(),
                payload: serde_json::to_value(crate::prompt_context::PromptContext {
                    slot: crate::prompt_context::ContextSlot::Harness,
                    content: "appendix".to_string(),
                })
                .unwrap(),
            },
        ),
        SessionEvent::new(
            native_session,
            4,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::OpenRouter,
                kind: "native_model_message".to_string(),
                payload: serde_json::to_value(ModelMessage::assistant(
                    Some("done".to_string()),
                    None,
                    None,
                    Vec::new(),
                ))
                .unwrap(),
            },
        ),
        SessionEvent::new(
            native_session,
            5,
            SessionEventKind::TurnCompleted {
                message_id: Uuid::new_v4(),
                provider_session_id: None,
                final_text: "done".to_string(),
                error: None,
            },
        ),
    ];
    assert_eq!(
        native_conversation(&native_events, CodingProvider::OpenRouter).unwrap(),
        vec![
            ModelMessage::user("request"),
            ModelMessage::user("appendix"),
            ModelMessage::assistant(Some("done".to_string()), None, None, Vec::new()),
        ]
    );

    let subscription_session = Uuid::new_v4();
    let subscription_events = vec![
        SessionEvent::new(
            subscription_session,
            1,
            SessionEventKind::TurnStarted {
                message_id: Uuid::new_v4(),
                provider: CodingProvider::OpenCode,
                model: None,
                effort: None,
                fast: false,
            },
        ),
        SessionEvent::new(
            subscription_session,
            2,
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::User,
                text: "request".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        ),
        SessionEvent::new(
            subscription_session,
            3,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::OpenCode,
                kind: crate::prompt_context::PROMPT_CONTEXT_EVENT.to_string(),
                payload: serde_json::to_value(crate::prompt_context::PromptContext {
                    slot: crate::prompt_context::ContextSlot::Harness,
                    content: "appendix".to_string(),
                })
                .unwrap(),
            },
        ),
        SessionEvent::new(
            subscription_session,
            4,
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::Assistant,
                text: "done".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        ),
        SessionEvent::new(
            subscription_session,
            5,
            SessionEventKind::TurnCompleted {
                message_id: Uuid::new_v4(),
                provider_session_id: None,
                final_text: "done".to_string(),
                error: None,
            },
        ),
    ];
    assert_eq!(
        native_conversation(&subscription_events, CodingProvider::OpenCode).unwrap(),
        vec![
            ModelMessage::user("request"),
            ModelMessage::user("appendix"),
            ModelMessage::assistant(Some("done".to_string()), None, None, Vec::new()),
        ]
    );
}

#[test]
fn retained_context_restarts_from_the_latest_cross_provider_summary() {
    let session_id = Uuid::new_v4();
    let message = |sequence: u64, actor: EventActor, text: &str| {
        SessionEvent::new(
            session_id,
            sequence,
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor,
                text: text.to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        )
    };
    let events = vec![
        message(1, EventActor::User, "old request"),
        message(2, EventActor::Assistant, "old response"),
        SessionEvent::new(
            session_id,
            3,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "context_compaction".to_string(),
                payload: json!({ "summary": "preserved decisions" }),
            },
        ),
        message(4, EventActor::User, "new request"),
    ];

    assert_eq!(
        retained_conversation_context(&events).as_deref(),
        Some(
            "<borg-message>{\"content\":\"Previous conversation summary:\\n\\npreserved decisions\",\"role\":\"user\"}</borg-message>\n<borg-message>{\"content\":\"new request\",\"role\":\"user\"}</borg-message>"
        )
    );
}

#[test]
fn failed_user_prompt_remains_in_provider_replay() {
    use borg_provider::provider::ModelMessage;

    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    let events = vec![
        SessionEvent::new(
            session_id,
            1,
            SessionEventKind::TurnStarted {
                message_id,
                provider: CodingProvider::Codex,
                model: Some("gpt-5.6-luna".to_string()),
                effort: Some("max".to_string()),
                fast: false,
            },
        ),
        SessionEvent::new(
            session_id,
            2,
            SessionEventKind::TurnCompleted {
                message_id,
                provider_session_id: None,
                final_text: String::new(),
                error: Some("codex returned an empty response".to_string()),
            },
        ),
        SessionEvent::new(
            session_id,
            3,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: "the failed request is still important".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Failed,
                delivery: Some(PromptDelivery::Queue),
            },
        ),
    ];

    assert_eq!(
        native_conversation(&events, CodingProvider::OpenRouter).unwrap(),
        vec![ModelMessage::user("the failed request is still important")]
    );
    assert!(
        retained_conversation_context(&events)
            .unwrap()
            .contains("the failed request is still important")
    );
}

#[test]
fn legacy_completed_prompt_before_a_failed_turn_is_not_dropped() {
    use borg_provider::provider::ModelMessage;

    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    let events = vec![
        SessionEvent::new(
            session_id,
            1,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: "recover the legacy request".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: Some(PromptDelivery::Steer),
            },
        ),
        SessionEvent::new(
            session_id,
            2,
            SessionEventKind::TurnStarted {
                message_id,
                provider: CodingProvider::Codex,
                model: Some("gpt-5.6-luna".to_string()),
                effort: Some("max".to_string()),
                fast: false,
            },
        ),
        SessionEvent::new(
            session_id,
            3,
            SessionEventKind::TurnCompleted {
                message_id,
                provider_session_id: None,
                final_text: String::new(),
                error: Some("codex returned an empty response".to_string()),
            },
        ),
    ];

    assert_eq!(
        native_conversation(&events, CodingProvider::OpenRouter).unwrap(),
        vec![ModelMessage::user("recover the legacy request")]
    );
}

#[test]
fn failed_user_prompt_survives_a_later_context_compaction_boundary() {
    use borg_provider::provider::ModelMessage;

    let session_id = Uuid::new_v4();
    let failed_message_id = Uuid::new_v4();
    let events = vec![
        SessionEvent::new(
            session_id,
            1,
            SessionEventKind::TurnStarted {
                message_id: failed_message_id,
                provider: CodingProvider::Codex,
                model: Some("gpt-5.6-luna".to_string()),
                effort: Some("max".to_string()),
                fast: false,
            },
        ),
        SessionEvent::new(
            session_id,
            2,
            SessionEventKind::TurnCompleted {
                message_id: failed_message_id,
                provider_session_id: None,
                final_text: String::new(),
                error: Some("codex returned an empty response".to_string()),
            },
        ),
        SessionEvent::new(
            session_id,
            3,
            SessionEventKind::Message {
                message_id: failed_message_id,
                actor: EventActor::User,
                text: "preserve this before compacting".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Failed,
                delivery: Some(PromptDelivery::Queue),
            },
        ),
        SessionEvent::new(
            session_id,
            4,
            SessionEventKind::TurnStarted {
                message_id: Uuid::new_v4(),
                provider: CodingProvider::Claude,
                model: Some("claude-sonnet-5".to_string()),
                effort: None,
                fast: false,
            },
        ),
        SessionEvent::new(
            session_id,
            5,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Claude,
                kind: "context_compaction".to_string(),
                payload: json!({ "summary": "keep this decision" }),
            },
        ),
    ];

    assert_eq!(
        native_conversation(&events, CodingProvider::Claude).unwrap(),
        vec![
            ModelMessage::user("Previous conversation summary:\n\nkeep this decision"),
            ModelMessage::user("preserve this before compacting"),
        ]
    );
}

/// A completed compaction boundary that follows a successful turn opens a new
/// context generation, so the in-memory projection can drop everything before
/// it while the rebuilt conversation stays identical. A boundary after a failed
/// or unfinished turn is the unsafe case: its unresolved prompt is carried
/// across the boundary from those pre-boundary events, so it must stay whole.
#[test]
fn a_clean_compaction_boundary_bounds_context_without_changing_replay() {
    use borg_provider::provider::ModelMessage;

    let session_id = Uuid::new_v4();
    let turn = |message_id: Uuid, prompt: &str, error: Option<&str>, sequence: u64| {
        [
            SessionEvent::new(
                session_id,
                sequence,
                SessionEventKind::TurnStarted {
                    message_id,
                    provider: CodingProvider::Codex,
                    model: Some("gpt-5.6-luna".to_string()),
                    effort: Some("max".to_string()),
                    fast: false,
                },
            ),
            SessionEvent::new(
                session_id,
                sequence + 1,
                SessionEventKind::Message {
                    message_id,
                    actor: EventActor::User,
                    text: prompt.to_string(),
                    attachments: Vec::new(),
                    status: MessageStatus::Complete,
                    delivery: None,
                },
            ),
            SessionEvent::new(
                session_id,
                sequence + 2,
                SessionEventKind::TurnCompleted {
                    message_id,
                    provider_session_id: None,
                    final_text: "done".to_string(),
                    error: error.map(str::to_string),
                },
            ),
        ]
    };
    let boundary = SessionEvent::new(
        session_id,
        4,
        SessionEventKind::ProviderEvent {
            provider: CodingProvider::Codex,
            kind: "context_compaction".to_string(),
            payload: json!({"status": "completed", "summary": "kept decisions"}),
        },
    );
    let mut superseded: Vec<SessionEvent> =
        turn(Uuid::new_v4(), "work the summary replaces", None, 1).into();
    let mut full = superseded.clone();
    full.push(boundary.clone());
    full.extend(turn(Uuid::new_v4(), "continue", None, 5));

    bound_context_at_compaction(&mut superseded, &boundary);
    assert!(
        superseded.is_empty(),
        "a clean boundary drops the superseded generation"
    );
    let mut bounded = superseded;
    bounded.push(boundary.clone());
    bounded.extend(full[4..].iter().cloned());

    assert_eq!(
        native_conversation(&bounded, CodingProvider::Codex).unwrap(),
        vec![
            ModelMessage::user("Previous conversation summary:\n\nkept decisions"),
            ModelMessage::user("continue"),
        ]
    );
    assert_eq!(
        native_conversation(&bounded, CodingProvider::Codex).unwrap(),
        native_conversation(&full, CodingProvider::Codex).unwrap(),
        "the bounded generation must rebuild the same conversation"
    );

    let mut failed: Vec<SessionEvent> = turn(
        Uuid::new_v4(),
        "owed to the model",
        Some("turn interrupted"),
        1,
    )
    .into();
    bound_context_at_compaction(&mut failed, &boundary);
    assert_eq!(failed.len(), 3, "a failed boundary keeps its carry");
}

#[test]
fn degraded_native_compaction_replays_the_original_history_once() {
    use borg_provider::provider::ModelMessage;

    let session_id = Uuid::new_v4();
    let native = |sequence, message: ModelMessage| {
        SessionEvent::new(
            session_id,
            sequence,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "native_model_message".to_string(),
                payload: serde_json::to_value(message).unwrap(),
            },
        )
    };
    let degraded = SessionEvent::new(
        session_id,
        4,
        SessionEventKind::ProviderEvent {
            provider: CodingProvider::Codex,
            kind: "context_compaction".to_string(),
            payload: json!({
                "status": "completed",
                "summary": "Automatic summarization failed",
                "degraded": true,
                "retained_messages": 2,
            }),
        },
    );
    assert!(!degraded.kind.is_completed_context_compaction());
    assert!(degraded.kind.is_context_relevant());
    assert!(!starts_context_generation(&degraded.kind));
    let events = vec![
        SessionEvent::new(
            session_id,
            1,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "context_compaction".to_string(),
                payload: json!({"status": "completed", "summary": "earlier work"}),
            },
        ),
        native(2, ModelMessage::user("original request")),
        native(
            3,
            ModelMessage::assistant(Some("original answer".into()), None, None, Vec::new()),
        ),
        degraded.clone(),
        native(5, ModelMessage::user("original request")),
        native(
            6,
            ModelMessage::assistant(Some("original answer".into()), None, None, Vec::new()),
        ),
        native(
            7,
            ModelMessage::assistant(Some("next answer".into()), None, None, Vec::new()),
        ),
    ];
    assert_eq!(
        native_conversation(&events, CodingProvider::Codex).unwrap(),
        vec![
            ModelMessage::user("Previous conversation summary:\n\nearlier work"),
            ModelMessage::user("original request"),
            ModelMessage::assistant(Some("original answer".into()), None, None, Vec::new()),
            ModelMessage::assistant(Some("next answer".into()), None, None, Vec::new()),
        ]
    );
}

#[test]
fn provider_neutral_replay_carries_subscription_tools_across_provider_switches() {
    use borg_provider::provider::{ModelMessage, ModelToolCall};

    let session_id = Uuid::new_v4();
    let first_message_id = Uuid::new_v4();
    let second_message_id = Uuid::new_v4();
    let events = vec![
        SessionEvent::new(
            session_id,
            1,
            SessionEventKind::TurnStarted {
                message_id: first_message_id,
                provider: CodingProvider::Codex,
                model: Some("gpt-5.6-luna".to_string()),
                effort: Some("xhigh".to_string()),
                fast: false,
            },
        ),
        SessionEvent::new(
            session_id,
            2,
            SessionEventKind::Message {
                message_id: first_message_id,
                actor: EventActor::User,
                text: "inspect the repository".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        ),
        SessionEvent::new(
            session_id,
            3,
            SessionEventKind::ToolStarted {
                tool_call_id: "call-1".to_string(),
                name: "read_file".to_string(),
                input: json!({"path": "Cargo.toml"}),
                input_ref: None,
            },
        ),
        SessionEvent::new(
            session_id,
            4,
            SessionEventKind::ToolCompleted {
                tool_call_id: "call-1".to_string(),
                output: "workspace contents".to_string(),
                output_ref: None,
                is_error: false,
                input: None,
                input_ref: None,
            },
        ),
        SessionEvent::new(
            session_id,
            5,
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::Assistant,
                text: "I found the workspace.".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        ),
        SessionEvent::new(
            session_id,
            6,
            SessionEventKind::TurnCompleted {
                message_id: first_message_id,
                provider_session_id: Some("provider-owned-id".to_string()),
                final_text: "I found the workspace.".to_string(),
                error: None,
            },
        ),
        SessionEvent::new(
            session_id,
            7,
            SessionEventKind::TurnStarted {
                message_id: second_message_id,
                provider: CodingProvider::OpenRouter,
                model: Some("openai/gpt-5".to_string()),
                effort: None,
                fast: false,
            },
        ),
        SessionEvent::new(
            session_id,
            8,
            SessionEventKind::Message {
                message_id: second_message_id,
                actor: EventActor::User,
                text: "now summarize it".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        ),
        SessionEvent::new(
            session_id,
            9,
            SessionEventKind::TurnCompleted {
                message_id: second_message_id,
                provider_session_id: None,
                final_text: "summary".to_string(),
                error: None,
            },
        ),
    ];

    let replay = native_conversation(&events, CodingProvider::OpenRouter).unwrap();
    assert_eq!(
        native_conversation(&events, CodingProvider::Codex).unwrap(),
        replay
    );
    assert_eq!(replay.len(), 5);
    assert_eq!(replay[0], ModelMessage::user("inspect the repository"));
    assert_eq!(
        replay[1],
        ModelMessage::assistant(
            None,
            None,
            None,
            vec![ModelToolCall::function(
                "call-1".to_string(),
                "read_file".to_string(),
                r#"{"path":"Cargo.toml"}"#.to_string(),
            )],
        )
    );
    assert_eq!(
        replay[2],
        ModelMessage::Tool {
            tool_call_id: "call-1".to_string(),
            content: "workspace contents".to_string(),
            attachments: Vec::new(),
        }
    );
    assert_eq!(
        replay[3],
        ModelMessage::assistant(
            Some("I found the workspace.".to_string()),
            None,
            None,
            Vec::new(),
        )
    );
    assert_eq!(replay[4], ModelMessage::user("now summarize it"));
}

#[test]
fn compaction_drops_provider_reasoning_without_mutating_durable_evidence() {
    use borg_provider::provider::{ModelMessage, ModelToolCall};

    let call = ModelToolCall::function(
        "call-1".into(),
        "exec".into(),
        r#"{"cmd":"cargo check"}"#.into(),
    );
    let conversation = vec![
        ModelMessage::user("Fix the build without changing the public API"),
        ModelMessage::Assistant {
            content: Some("Checking the build".into()),
            reasoning_content: Some("Private working reasoning".into()),
            reasoning_details: Some(json!([{ "type": "reasoning" }])),
            provider_state: Some(
                serde_json::from_value(json!({
                    "protocol": "open_ai_responses",
                    "output": [{"type": "reasoning", "encrypted_content": "opaque replay"}]
                }))
                .unwrap(),
            ),
            tool_calls: vec![call.clone()],
        },
        ModelMessage::Tool {
            tool_call_id: "call-1".into(),
            content: "Build succeeded".into(),
            attachments: Vec::new(),
        },
    ];
    let original = conversation.clone();
    let projected = prune_conversation_for_compaction(&conversation);
    let expected = vec![
        conversation[0].clone(),
        ModelMessage::assistant(Some("Checking the build".into()), None, None, vec![call]),
        conversation[2].clone(),
    ];
    assert_eq!(projected, expected);
    assert_eq!(conversation, original);
}

#[test]
fn subscription_compaction_projection_truncates_large_tool_results() {
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    let second_message_id = Uuid::new_v4();
    let third_message_id = Uuid::new_v4();
    let tool_output = format!("old-tool-output-{}", "x".repeat(5_000));
    let recent_tool_output = format!("recent-tool-output-{}", "y".repeat(5_000));
    let newest_tool_output = format!("newest-tool-output-{}", "z".repeat(5_000));
    let events = vec![
        SessionEvent::new(
            session_id,
            1,
            SessionEventKind::TurnStarted {
                message_id,
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: false,
            },
        ),
        SessionEvent::new(
            session_id,
            2,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: "inspect the repository".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        ),
        SessionEvent::new(
            session_id,
            3,
            SessionEventKind::ToolStarted {
                tool_call_id: "call-1".to_string(),
                name: "read_file".to_string(),
                input: json!({"path": "large.txt"}),
                input_ref: None,
            },
        ),
        SessionEvent::new(
            session_id,
            4,
            SessionEventKind::ToolCompleted {
                tool_call_id: "call-1".to_string(),
                output: tool_output.clone(),
                output_ref: None,
                is_error: false,
                input: None,
                input_ref: None,
            },
        ),
        SessionEvent::new(
            session_id,
            5,
            SessionEventKind::TurnCompleted {
                message_id,
                provider_session_id: None,
                final_text: "done".to_string(),
                error: None,
            },
        ),
        SessionEvent::new(
            session_id,
            6,
            SessionEventKind::TurnStarted {
                message_id: second_message_id,
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: false,
            },
        ),
        SessionEvent::new(
            session_id,
            7,
            SessionEventKind::Message {
                message_id: second_message_id,
                actor: EventActor::User,
                text: "continue from the repository inspection".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        ),
        SessionEvent::new(
            session_id,
            8,
            SessionEventKind::ToolStarted {
                tool_call_id: "call-2".to_string(),
                name: "read_file".to_string(),
                input: json!({"path": "recent.txt"}),
                input_ref: None,
            },
        ),
        SessionEvent::new(
            session_id,
            9,
            SessionEventKind::ToolCompleted {
                tool_call_id: "call-2".to_string(),
                output: recent_tool_output.clone(),
                output_ref: None,
                is_error: false,
                input: None,
                input_ref: None,
            },
        ),
        SessionEvent::new(
            session_id,
            10,
            SessionEventKind::TurnCompleted {
                message_id: second_message_id,
                provider_session_id: None,
                final_text: "continued".to_string(),
                error: None,
            },
        ),
        SessionEvent::new(
            session_id,
            11,
            SessionEventKind::TurnStarted {
                message_id: third_message_id,
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: false,
            },
        ),
        SessionEvent::new(
            session_id,
            12,
            SessionEventKind::Message {
                message_id: third_message_id,
                actor: EventActor::User,
                text: "finish the inspection".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        ),
        SessionEvent::new(
            session_id,
            13,
            SessionEventKind::ToolStarted {
                tool_call_id: "call-3".to_string(),
                name: "read_file".to_string(),
                input: json!({"path": "newest.txt"}),
                input_ref: None,
            },
        ),
        SessionEvent::new(
            session_id,
            14,
            SessionEventKind::ToolCompleted {
                tool_call_id: "call-3".to_string(),
                output: newest_tool_output.clone(),
                output_ref: None,
                is_error: false,
                input: None,
                input_ref: None,
            },
        ),
        SessionEvent::new(
            session_id,
            15,
            SessionEventKind::TurnCompleted {
                message_id: third_message_id,
                provider_session_id: None,
                final_text: "finished".to_string(),
                error: None,
            },
        ),
    ];

    let full_context = retained_conversation_context(&events).unwrap();
    let compaction_context =
        retained_compaction_context_with_budget(&events, SUBSCRIPTION_INPUT_BUDGET_CHARS)
            .unwrap()
            .context;

    assert!(compaction_context.contains("recent-tool-output-"));
    assert!(!compaction_context.contains(&tool_output));
    assert!(compaction_context.contains(COMPACTION_OLD_TOOL_RESULT_MARKER));
    assert!(compaction_context.contains("continue from the repository inspection"));
    assert!(compaction_context.contains(&recent_tool_output));
    assert!(compaction_context.contains(&newest_tool_output));
    assert!(compaction_context.chars().count() < full_context.chars().count());
}

#[test]
fn subscription_compaction_projection_has_a_hard_total_bound() {
    let oversized = format!(
        "head-α{}middle{}tail-🛠️",
        "x".repeat(SUBSCRIPTION_INPUT_BUDGET_CHARS),
        "y".repeat(SUBSCRIPTION_INPUT_BUDGET_CHARS)
    );

    let bounded = truncate_compaction_context(&oversized, SUBSCRIPTION_INPUT_BUDGET_CHARS);

    assert!(bounded.chars().count() <= SUBSCRIPTION_INPUT_BUDGET_CHARS);
    assert!(bounded.starts_with("head-α"));
    assert!(bounded.ends_with("tail-🛠️"));
    assert!(bounded.contains(COMPACTION_CONTEXT_ELISION));
}

#[test]
fn deterministic_recovery_projection_fits_the_complete_provider_request() {
    let session_id = Uuid::new_v4();
    let events = vec![SessionEvent::new(
        session_id,
        1,
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::User,
            text: "x".repeat(SUBSCRIPTION_INPUT_BUDGET_CHARS * 2),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
    )];
    let current_prompt = "continue safely";
    let projection = retained_compaction_context_with_budget(
        &events,
        subscription_replay_context_budget(EventActor::User, current_prompt),
    )
    .unwrap();

    assert!(
        subscription_prompt_chars(Some(&projection.context), EventActor::User, current_prompt)
            <= SUBSCRIPTION_INPUT_BUDGET_CHARS
    );
}

/// Compaction failing because the provider refused -- a revoked token, an
/// expired OAuth session, a dropped connection -- must never license dropping
/// durable history. The turn that follows fails on that same cause whatever the
/// context looks like, so truncating it destroys the user's conversation and
/// buys nothing. Only a structural failure keeps the backstop.
#[test]
fn a_provider_side_compaction_failure_does_not_license_dropping_history() {
    use borg_provider::provider::{ProviderErrorKind, ProviderStreamError};

    let revoked = anyhow::Error::new(ProviderStreamError {
        kind: ProviderErrorKind::Fatal,
        message: "claude SDK API error: 401 OAuth access token has been revoked".to_string(),
    });
    assert!(
        compaction_failure_is_provider_side(&revoked),
        "a revoked token must keep the durable history intact"
    );

    let dropped = anyhow::Error::new(ProviderStreamError {
        kind: ProviderErrorKind::ConnectionLost,
        message: "stream ended before the summary".to_string(),
    });
    assert!(
        compaction_failure_is_provider_side(&dropped),
        "a lost connection says nothing about how large the history is"
    );

    let structural = anyhow::anyhow!("retained context is empty");
    assert!(
        !compaction_failure_is_provider_side(&structural),
        "a structural failure still permits the bounded backstop"
    );
}

/// A pathological replay drops whole messages. The projection must report how
/// many, so the durable `context_replay_projected` event explains the omission
/// marker instead of leaving a reader to re-derive the projection in code.
#[test]
fn replay_projection_reports_the_messages_it_omitted() {
    use borg_provider::provider::ModelMessage;

    let filler = "u".repeat(2_000);
    let mut conversation = Vec::new();
    for index in 0..6 {
        conversation.push(ModelMessage::user(format!("user turn {index} {filler}")));
        conversation.push(ModelMessage::assistant(
            Some(format!("assistant turn {index}")),
            None,
            None,
            Vec::new(),
        ));
    }
    conversation.push(ModelMessage::user("the newest request".to_string()));

    let projection = fit_compaction_context(&conversation, 2_048);

    assert_eq!(projection.messages_before, conversation.len());
    assert!(projection.messages_omitted > 0);
    assert!(projection.messages_omitted < projection.messages_before);
    assert!(projection.context.chars().count() <= 2_048);
}

/// The projection's message-dropping backstop must never elide the assistant
/// reply that the next prompt answers.
///
/// The conversation ends on the assistant reply: the human is about to answer
/// it, so the reply is not followed by any user message yet. A window measured
/// in user turns never covers that trailing assistant tail, so the reply was
/// marker-replaced and then dropped -- the model could no longer see what the
/// human was responding to, re-read stale history, and re-asked questions it had
/// already put to the human. The live exchange is anchored on the reply itself,
/// so it survives however the tail is shaped.
#[test]
fn replay_projection_keeps_the_trailing_reply_the_next_prompt_answers() {
    use borg_provider::provider::ModelMessage;

    let filler = "u".repeat(2_000);
    let mut conversation = Vec::new();
    for index in 0..6 {
        conversation.push(ModelMessage::user(format!("user turn {index} {filler}")));
        conversation.push(ModelMessage::assistant(
            Some(format!("assistant turn {index}")),
            None,
            None,
            Vec::new(),
        ));
    }
    conversation.push(ModelMessage::user(format!(
        "the request being answered {filler}"
    )));
    conversation.push(ModelMessage::assistant(
        Some("the reply the human is answering".to_string()),
        None,
        None,
        Vec::new(),
    ));

    let projection = fit_compaction_context(&conversation, 2_048);

    assert!(projection.messages_omitted > 0);
    assert!(
        projection
            .context
            .contains("the reply the human is answering"),
        "the trailing assistant reply must survive the projection"
    );
}

#[test]
fn subscription_replay_budget_accounts_for_the_context_separator() {
    let current_prompt = "continue safely";
    let context_budget = subscription_replay_context_budget(EventActor::User, current_prompt);
    let context = "x".repeat(context_budget);

    assert_eq!(context_budget % SUBSCRIPTION_REPLAY_BUDGET_QUANTUM_CHARS, 0);
    assert!(
        subscription_prompt_chars(Some(&context), EventActor::User, current_prompt)
            <= SUBSCRIPTION_INPUT_BUDGET_CHARS
    );
    assert!(
        subscription_prompt_chars(
            Some(&format!(
                "{context}{}",
                "x".repeat(SUBSCRIPTION_REPLAY_BUDGET_QUANTUM_CHARS)
            )),
            EventActor::User,
            current_prompt,
        ) > SUBSCRIPTION_INPUT_BUDGET_CHARS
    );
}

#[test]
fn subscription_replay_budget_keeps_projection_boundaries_stable() {
    let short = subscription_replay_context_budget(EventActor::User, "short");
    let longer =
        subscription_replay_context_budget(EventActor::User, &"longer prompt ".repeat(1024));

    assert_eq!(short, longer);
}

#[tokio::test]
async fn subscription_compaction_counts_every_fold_and_reports_partial_failure() {
    use borg_provider::provider::ModelMessage;

    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let cwd = root.path().to_path_buf();
    let dispatcher = crate::AgentToolDispatcher::new(
        SessionGoalTools::disconnected(),
        SessionTodoTools::disconnected(),
        None,
        crate::LspService::new(&cwd),
        CodingProvider::Codex,
        session_id,
        false,
        None,
        None,
        cwd.clone(),
        None,
        None,
        None,
        Vec::new(),
        None,
        crate::native_process::ProcessManager::default(),
        PermissionMode::FullAccess,
    );
    let launch = LaunchSession {
        request_id: Uuid::new_v4(),
        cwd,
        provider: CodingProvider::Codex,
        model: Some("test-model".to_string()),
        effort: Some("medium".to_string()),
        fast: Some(false),
        response_language: crate::ResponseLanguage::Auto,
        permission_mode: PermissionMode::FullAccess,
        name: None,
        initial_prompt: None,
        capabilities: Default::default(),
        subagent_concurrency_limit: None,
        extension_skill_roots: Vec::new(),
        team_policy: None,
    };
    let agent_mcp_server = borg_provider::mcp::ExternalMcpServer {
        name: "test".to_string(),
        command: "test".to_string(),
        args: Vec::new(),
        env: std::collections::BTreeMap::new(),
        allowed_tools: Vec::new(),
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let executor: Arc<dyn AgentTurnExecutor> = Arc::new(OversizedCompactionExecutor {
        calls: calls.clone(),
        fail_on_second: false,
    });
    let prompt_id = Uuid::new_v4();
    let mut events = vec![SessionEvent::new(
        session_id,
        1,
        SessionEventKind::TurnStarted {
            message_id: prompt_id,
            provider: CodingProvider::Codex,
            model: Some("test-model".to_string()),
            effort: Some("medium".to_string()),
            fast: false,
        },
    )];
    for index in 0..6_u64 {
        events.push(SessionEvent::new(
            session_id,
            index + 2,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "native_model_message".to_string(),
                payload: serde_json::to_value(ModelMessage::user(format!(
                    "durable context {index} {}",
                    "x".repeat(180_000)
                )))
                .unwrap(),
            },
        ));
    }
    events.push(SessionEvent::new(
        session_id,
        8,
        SessionEventKind::TurnCompleted {
            message_id: prompt_id,
            provider_session_id: None,
            final_text: String::new(),
            error: None,
        },
    ));

    let compaction = compact_subscription_context_for_budget(SubscriptionCompactionRequest {
        executor: &executor,
        session_id,
        launch: &launch,
        agent_mcp_server: &agent_mcp_server,
        dispatcher: &dispatcher,
        events: &events,
        actor: EventActor::User,
        current_prompt: "continue",
        retained_tail_budget_chars: subscription_retained_tail_budget_chars(
            SUBSCRIPTION_INPUT_BUDGET_CHARS,
        ),
    })
    .await
    .unwrap()
    .compaction;

    assert!(compaction.summary.chars().count() <= SUBSCRIPTION_INPUT_BUDGET_CHARS);
    assert!(compaction.summary.starts_with("summary-start"));
    assert!(compaction.summary.ends_with("summary-end"));
    let fold_count = calls.load(Ordering::SeqCst);
    assert!(fold_count > 1, "the history must require multiple folds");
    let fold_count = fold_count as u64;
    assert_eq!(compaction.usage.duration_ms, 11 * fold_count);
    assert_eq!(compaction.usage.input_tokens, 101 * fold_count);
    assert_eq!(compaction.usage.cached_input_tokens, 20 * fold_count);
    assert_eq!(compaction.usage.cache_creation_input_tokens, 5 * fold_count);
    assert_eq!(compaction.usage.output_tokens, 9 * fold_count);
    assert_eq!(compaction.usage.total_tokens, 135 * fold_count);
    assert_eq!(compaction.usage.cost_microusd, Some(700 * fold_count));
    assert_eq!(
        compaction.usage.cost_basis,
        borg_provider::CostBasis::SubscriptionEquivalent
    );

    let failed_calls = Arc::new(AtomicUsize::new(0));
    let failing_executor: Arc<dyn AgentTurnExecutor> = Arc::new(OversizedCompactionExecutor {
        calls: failed_calls.clone(),
        fail_on_second: true,
    });
    let error = match compact_subscription_context_for_budget(SubscriptionCompactionRequest {
        executor: &failing_executor,
        session_id,
        launch: &launch,
        agent_mcp_server: &agent_mcp_server,
        dispatcher: &dispatcher,
        events: &events,
        actor: EventActor::User,
        current_prompt: "continue",
        retained_tail_budget_chars: subscription_retained_tail_budget_chars(
            SUBSCRIPTION_INPUT_BUDGET_CHARS,
        ),
    })
    .await
    {
        Ok(_) => panic!("the second fold should fail"),
        Err(error) => error,
    };
    assert_eq!(failed_calls.load(Ordering::SeqCst), 2);
    let partial = error
        .downcast_ref::<crate::agent::PartialCompactionUsage>()
        .expect("completed fold usage survives the later failure");
    assert_eq!(partial.usage.total_tokens, 135);
    assert_eq!(partial.usage.cost_microusd, Some(700));
    assert!(compaction_failure_is_provider_side(&error));
}

/// A resumed or forked session loads its context from the compaction boundary
/// forward, so the messages a verbatim tail was taken from are no longer there
/// to take it from again. Rebuilding the tail from the boundary event itself is
/// the whole of its durability: derive it from the pre-boundary events instead
/// and the live turn keeps it while every resume silently drops back to a
/// summary-only context.
#[test]
fn subscription_compaction_tail_replays_from_the_boundary_event_alone() {
    use borg_provider::provider::{ModelMessage, ModelToolCall};

    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    let tail = vec![
        ModelMessage::assistant(
            None,
            None,
            None,
            vec![ModelToolCall::function(
                "call-1".to_string(),
                "read_file".to_string(),
                r#"{"path":"src/main.rs"}"#.to_string(),
            )],
        ),
        ModelMessage::tool("call-1", "fn main() {}"),
        ModelMessage::user("keep the exact identifier BORG-4821"),
    ];
    let events = vec![
        SessionEvent::new(
            session_id,
            1,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Claude,
                kind: "context_compaction".to_string(),
                payload: json!({
                    "status": "completed",
                    "summary": "preserved decisions",
                    "retained_tail": tail,
                }),
            },
        ),
        SessionEvent::new(
            session_id,
            2,
            SessionEventKind::TurnStarted {
                message_id,
                provider: CodingProvider::Claude,
                model: Some("claude-sonnet-5".to_string()),
                effort: None,
                fast: false,
            },
        ),
        SessionEvent::new(
            session_id,
            3,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: "continue".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        ),
        SessionEvent::new(
            session_id,
            4,
            SessionEventKind::TurnCompleted {
                message_id,
                provider_session_id: None,
                final_text: "done".to_string(),
                error: None,
            },
        ),
    ];

    let mut expected = vec![ModelMessage::user(
        "Previous conversation summary:\n\npreserved decisions",
    )];
    expected.extend(tail);
    expected.push(ModelMessage::user("continue"));
    assert_eq!(
        native_conversation(&events, CodingProvider::Claude).unwrap(),
        expected
    );
}

/// Two ways a bounded tail turns into a corrupt one: it can open on a tool
/// result whose call fell outside the window, which asks the model to trust an
/// answer to a question it cannot see, and it can carry the prompt this turn is
/// about to send, which delivers that prompt to the provider twice.
#[test]
fn a_retained_tail_keeps_whole_tool_units_and_excludes_the_current_prompt() {
    use borg_provider::provider::{ModelMessage, ModelToolCall};

    let conversation = vec![
        ModelMessage::user("old request"),
        ModelMessage::assistant(
            None,
            None,
            None,
            vec![ModelToolCall::function(
                "call-1".to_string(),
                "read_file".to_string(),
                "{}".to_string(),
            )],
        ),
        ModelMessage::tool("call-1", "file contents"),
        ModelMessage::assistant(Some("done".to_string()), None, None, Vec::new()),
        ModelMessage::user("current prompt"),
    ];
    let frame_chars = |message: &ModelMessage| {
        format_subscription_frame(&format_subscription_message(message))
            .chars()
            .count()
            + 1
    };

    // A window that reaches the tool result but not the call that produced it.
    let orphaned = retain_recent_subscription_messages(
        &conversation,
        frame_chars(&conversation[2]) + frame_chars(&conversation[3]),
        "current prompt",
    );
    assert_eq!(
        orphaned,
        vec![ModelMessage::assistant(
            Some("done".to_string()),
            None,
            None,
            Vec::new()
        )]
    );

    let whole = retain_recent_subscription_messages(
        &conversation,
        SUBSCRIPTION_INPUT_BUDGET_CHARS,
        "current prompt",
    );
    assert_eq!(whole, &conversation[..4]);
}

/// The provider input budget is a hard limit: a replay that exceeds it fails
/// the turn outright. Compaction runs precisely when the conversation is large,
/// so a large prompt can leave less headroom than the tail budget, and the tail
/// has to give way rather than the summary that replaced the history.
#[test]
fn a_retained_tail_is_trimmed_until_the_compacted_replay_fits_the_budget() {
    use borg_provider::provider::ModelMessage;

    let summary = "s".repeat(COMPACTION_RUNNING_SUMMARY_BUDGET_CHARS);
    let prompt = "p".repeat(
        SUBSCRIPTION_INPUT_BUDGET_CHARS - COMPACTION_RUNNING_SUMMARY_BUDGET_CHARS - 16 * 1024,
    );
    let mut tail = vec![ModelMessage::user("x".repeat(8 * 1024)); 4];

    fit_retained_tail_to_budget(&summary, &mut tail, EventActor::User, &prompt);

    assert!(
        !tail.is_empty(),
        "the tail must give way message by message, not all at once"
    );
    assert!(tail.len() < 4);
    assert!(
        subscription_prompt_chars(
            Some(&compacted_replay_context(&summary, &tail)),
            EventActor::User,
            &prompt,
        ) <= SUBSCRIPTION_INPUT_BUDGET_CHARS
    );
}

#[test]
fn subscription_projection_is_append_only_until_compaction() {
    let session_id = Uuid::new_v4();
    let first_message_id = Uuid::new_v4();
    let events = vec![
        SessionEvent::new(
            session_id,
            1,
            SessionEventKind::TurnStarted {
                message_id: first_message_id,
                provider: CodingProvider::Codex,
                model: Some("gpt-5.6-luna".to_string()),
                effort: Some("xhigh".to_string()),
                fast: false,
            },
        ),
        SessionEvent::new(
            session_id,
            2,
            SessionEventKind::Message {
                message_id: first_message_id,
                actor: EventActor::User,
                text: "inspect the repository".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        ),
        SessionEvent::new(
            session_id,
            3,
            SessionEventKind::ToolStarted {
                tool_call_id: "call-1".to_string(),
                name: "read_file".to_string(),
                input: json!({"path": "Cargo.toml"}),
                input_ref: None,
            },
        ),
        SessionEvent::new(
            session_id,
            4,
            SessionEventKind::ToolCompleted {
                tool_call_id: "call-1".to_string(),
                output: "workspace contents".to_string(),
                output_ref: None,
                is_error: false,
                input: None,
                input_ref: None,
            },
        ),
        SessionEvent::new(
            session_id,
            5,
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::Assistant,
                text: "I found the workspace.".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        ),
        SessionEvent::new(
            session_id,
            6,
            SessionEventKind::TurnCompleted {
                message_id: first_message_id,
                provider_session_id: Some("codex-session".to_string()),
                final_text: "I found the workspace.".to_string(),
                error: None,
            },
        ),
    ];

    let first =
        format_subscription_provider_prompt(None, EventActor::User, "inspect the repository");
    let retained = retained_conversation_context(&events).expect("completed tree context");
    let second = format_subscription_provider_prompt(Some(&retained), EventActor::User, "continue");

    assert!(second.starts_with(&first));
    assert!(second.contains("read_file"));
    assert!(second.contains("workspace contents"));
    assert!(
        second
            .ends_with("<borg-message>{\"content\":\"continue\",\"role\":\"user\"}</borg-message>")
    );

    let mut compacted_events = events;
    compacted_events.push(SessionEvent::new(
        session_id,
        7,
        SessionEventKind::ProviderEvent {
            provider: CodingProvider::Claude,
            kind: "context_compaction".to_string(),
            payload: json!({"summary": "preserved decisions"}),
        },
    ));
    compacted_events.push(SessionEvent::new(
        session_id,
        8,
        SessionEventKind::TurnStarted {
            message_id: Uuid::new_v4(),
            provider: CodingProvider::Claude,
            model: Some("claude-sonnet-5".to_string()),
            effort: None,
            fast: false,
        },
    ));
    let after_compaction = retained_conversation_context(&compacted_events)
        .expect("compaction summary remains in the tree projection");
    let compacted_prompt = format_subscription_provider_prompt(
        Some(&after_compaction),
        EventActor::User,
        "after compaction",
    );
    assert!(compacted_prompt.contains("preserved decisions"));
    assert!(!compacted_prompt.starts_with(&second));
}

#[test]
fn provider_neutral_replay_resets_subscription_history_at_compaction() {
    use borg_provider::provider::ModelMessage;

    let session_id = Uuid::new_v4();
    let old_message_id = Uuid::new_v4();
    let new_message_id = Uuid::new_v4();
    let events = vec![
        SessionEvent::new(
            session_id,
            1,
            SessionEventKind::TurnStarted {
                message_id: old_message_id,
                provider: CodingProvider::Claude,
                model: Some("claude-sonnet-5".to_string()),
                effort: None,
                fast: false,
            },
        ),
        SessionEvent::new(
            session_id,
            2,
            SessionEventKind::Message {
                message_id: old_message_id,
                actor: EventActor::User,
                text: "old request".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        ),
        SessionEvent::new(
            session_id,
            3,
            SessionEventKind::TurnCompleted {
                message_id: old_message_id,
                provider_session_id: None,
                final_text: "old response".to_string(),
                error: None,
            },
        ),
        SessionEvent::new(
            session_id,
            4,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Claude,
                kind: "context_compaction".to_string(),
                payload: json!({"summary": "preserved decisions"}),
            },
        ),
        SessionEvent::new(
            session_id,
            5,
            SessionEventKind::TurnStarted {
                message_id: new_message_id,
                provider: CodingProvider::OpenRouter,
                model: Some("openai/gpt-5".to_string()),
                effort: None,
                fast: false,
            },
        ),
        SessionEvent::new(
            session_id,
            6,
            SessionEventKind::Message {
                message_id: new_message_id,
                actor: EventActor::User,
                text: "continue".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        ),
        SessionEvent::new(
            session_id,
            7,
            SessionEventKind::TurnCompleted {
                message_id: new_message_id,
                provider_session_id: None,
                final_text: "done".to_string(),
                error: None,
            },
        ),
    ];

    assert_eq!(
        native_conversation(&events, CodingProvider::OpenRouter).unwrap(),
        vec![
            ModelMessage::user("Previous conversation summary:\n\npreserved decisions"),
            ModelMessage::user("continue"),
        ]
    );
    assert_eq!(
        retained_conversation_context(&events).as_deref(),
        Some(
            "<borg-message>{\"content\":\"Previous conversation summary:\\n\\npreserved decisions\",\"role\":\"user\"}</borg-message>\n<borg-message>{\"content\":\"continue\",\"role\":\"user\"}</borg-message>"
        )
    );
}

#[test]
fn interrupted_user_prompt_is_preserved_after_context_compaction() {
    use borg_provider::provider::ModelMessage;

    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    let events = vec![
        SessionEvent::new(
            session_id,
            1,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: "do not lose this interrupted request".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: Some(PromptDelivery::Steer),
            },
        ),
        SessionEvent::new(
            session_id,
            2,
            SessionEventKind::TurnStarted {
                message_id,
                provider: CodingProvider::Codex,
                model: Some("gpt-5.6-luna".to_string()),
                effort: Some("max".to_string()),
                fast: false,
            },
        ),
        SessionEvent::new(
            session_id,
            3,
            SessionEventKind::TurnCompleted {
                message_id,
                provider_session_id: None,
                final_text: String::new(),
                error: Some("turn interrupted".to_string()),
            },
        ),
        SessionEvent::new(
            session_id,
            4,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "context_compaction".to_string(),
                payload: json!({"summary": "preserved earlier decisions"}),
            },
        ),
    ];

    assert_eq!(
        native_conversation(&events, CodingProvider::Claude).unwrap(),
        vec![
            ModelMessage::user("Previous conversation summary:\n\npreserved earlier decisions"),
            ModelMessage::user("do not lose this interrupted request"),
        ]
    );
}

#[test]
fn provider_native_compaction_keeps_borg_recovery_history_lossless() {
    use borg_provider::provider::ModelMessage;

    let session_id = Uuid::new_v4();
    let events = vec![
        SessionEvent::new(
            session_id,
            1,
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::User,
                text: "retain this requirement".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        ),
        SessionEvent::new(
            session_id,
            2,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "context_compaction".to_string(),
                payload: json!({
                    "status": "completed",
                    "summary": "provider-only checkpoint",
                    "provider_context_preserved": true,
                }),
            },
        ),
    ];

    assert_eq!(
        provider_neutral_conversation(&events).unwrap(),
        vec![ModelMessage::user("retain this requirement")]
    );
}

#[test]
fn provider_native_recovery_checkpoint_restarts_cold_replay() {
    use borg_provider::provider::ModelMessage;

    let session_id = Uuid::new_v4();
    let message = |sequence: u64, text: &str| {
        SessionEvent::new(
            session_id,
            sequence,
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::User,
                text: text.to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        )
    };
    let events = vec![
        message(1, "old context retained in the durable journal"),
        SessionEvent::new(
            session_id,
            2,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "context_compaction".to_string(),
                payload: json!({
                    "status": "completed",
                    "summary": "provider recovery summary",
                    "provider_context_preserved": true,
                    "provider_recovery_checkpoint": true,
                }),
            },
        ),
        message(3, "new context after compaction"),
    ];

    assert_eq!(
        provider_neutral_conversation(&events).unwrap(),
        vec![
            ModelMessage::user("Previous conversation summary:\n\nprovider recovery summary"),
            ModelMessage::user("new context after compaction"),
        ]
    );
}

#[test]
fn subscription_context_budget_detects_oversized_resumed_transcripts() {
    let context = format!(
        "{{\"role\":\"tool\",\"content\":\"{}\"}}",
        "x".repeat(SUBSCRIPTION_INPUT_BUDGET_CHARS)
    );
    assert!(
        subscription_prompt_chars(Some(&context), EventActor::User, "continue")
            > SUBSCRIPTION_INPUT_BUDGET_CHARS
    );
}

#[test]
fn reusable_subscription_context_does_not_compact_the_full_replay() {
    let context = format!(
        "{{\"role\":\"tool\",\"content\":\"{}\"}}",
        "x".repeat(SUBSCRIPTION_INPUT_BUDGET_CHARS)
    );

    assert!(subscription_context_needs_projection(
        &context,
        EventActor::User,
        "continue",
        false,
    ));
    assert!(!subscription_context_needs_projection(
        &context,
        EventActor::User,
        "continue",
        true,
    ));
    assert!(
        subscription_prompt_chars(Some(&context), EventActor::User, "continue")
            > SUBSCRIPTION_INPUT_BUDGET_CHARS
    );
    assert!(
        subscription_prompt_chars(None, EventActor::User, "continue")
            <= SUBSCRIPTION_INPUT_BUDGET_CHARS
    );
}

#[test]
fn recovery_projection_depends_on_replay_size_not_stale_context_usage() {
    let oversized_context = "x".repeat(SUBSCRIPTION_INPUT_BUDGET_CHARS * 4);

    assert!(subscription_context_needs_projection(
        &oversized_context,
        EventActor::User,
        "continue",
        false,
    ));

    let context_within_input_limit = "x".repeat(SUBSCRIPTION_INPUT_BUDGET_CHARS / 2);
    assert!(!subscription_context_needs_projection(
        &context_within_input_limit,
        EventActor::User,
        "continue",
        false,
    ));
}

#[test]
fn acknowledged_codex_interrupt_keeps_subscription_context_reusable() {
    assert!(subscription_context_reusable_after_turn(
        CodingProvider::Codex,
        true,
        true
    ));
    assert!(!subscription_context_reusable_after_turn(
        CodingProvider::Claude,
        true,
        true
    ));
    assert!(!subscription_context_reusable_after_turn(
        CodingProvider::Codex,
        true,
        false
    ));
}

#[test]
fn codex_resume_requires_a_terminal_checkpoint_after_the_latest_turn_start() {
    let session_id = Uuid::new_v4();
    let completed_id = Uuid::new_v4();
    let crashed_id = Uuid::new_v4();
    let mut events = vec![
        SessionEvent::new(
            session_id,
            1,
            SessionEventKind::TurnStarted {
                message_id: completed_id,
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: false,
            },
        ),
        SessionEvent::new(
            session_id,
            2,
            SessionEventKind::TurnCompleted {
                message_id: completed_id,
                provider_session_id: Some("thread-1".to_string()),
                final_text: "done".to_string(),
                error: None,
            },
        ),
    ];
    assert!(codex_checkpoint_is_acknowledged(&events, "thread-1"));
    assert_eq!(codex_checkpoint_fork_turn_id(&events, Some("turn-1")), None);

    events.push(SessionEvent::new(
        session_id,
        3,
        SessionEventKind::TurnStarted {
            message_id: crashed_id,
            provider: CodingProvider::Codex,
            model: None,
            effort: None,
            fast: false,
        },
    ));
    events.push(SessionEvent::new(
        session_id,
        4,
        SessionEventKind::ProviderSessionLinked {
            provider_session_id: "thread-1".to_string(),
            provider_turn_id: None,
            context_contract_version: None,
        },
    ));
    assert!(!codex_checkpoint_is_acknowledged(&events, "thread-1"));
    assert_eq!(
        codex_checkpoint_fork_turn_id(&events, Some("turn-1")).as_deref(),
        Some("turn-1")
    );
}

#[test]
fn subscription_input_budget_counts_characters_not_utf8_bytes() {
    let text = "🛠️".repeat(SUBSCRIPTION_INPUT_BUDGET_CHARS / 4);
    let prompt = format_subscription_provider_prompt(None, EventActor::User, &text);

    assert!(prompt.len() > SUBSCRIPTION_INPUT_BUDGET_CHARS);
    assert!(prompt.chars().count() <= SUBSCRIPTION_INPUT_BUDGET_CHARS);
}

#[test]
fn legacy_provider_checkpoint_contract_is_never_resumed() {
    let mut state = SessionState {
        provider_session_id: Some("stale-thread".to_string()),
        ..SessionState::default()
    };
    assert!(!provider_checkpoint_contract_is_current(&state));

    state.provider_context_contract_version = Some(crate::agent::PROVIDER_CONTEXT_CONTRACT_VERSION);
    assert!(provider_checkpoint_contract_is_current(&state));
}

#[tokio::test]
async fn reusable_subscription_pool_does_not_compact_large_durable_replay() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let first_id = Uuid::new_v4();
    let second_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(256);
    let called = Arc::new(Notify::new());
    let prompt_lengths = Arc::new(Mutex::new(Vec::new()));
    let executor = Arc::new(ReusableContextExecutor {
        prompt_lengths: Arc::clone(&prompt_lengths),
        called: Arc::clone(&called),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn(async move {
        run_session_actor(
            &journal_path,
            session_id,
            LaunchSession {
                request_id: Uuid::new_v4(),
                cwd: root.path().to_path_buf(),
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: Some(false),
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
                name: None,
                initial_prompt: None,
                capabilities: Default::default(),
                subagent_concurrency_limit: None,
                extension_skill_roots: Vec::new(),
                team_policy: None,
            },
            command_rx,
            event_tx,
            executor,
            actor_store,
        )
        .await
    });

    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: first_id,
            text: "u".repeat(450_000),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Queue,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), called.notified())
        .await
        .expect("first pooled turn completes");

    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: second_id,
            text: "continue".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Queue,
        })
        .await
        .unwrap();

    let mut provider_input_compacted = false;
    loop {
        let event = tokio::time::timeout(Duration::from_secs(5), event_rx.recv())
            .await
            .expect("second pooled turn remains live")
            .expect("session remains open");
        match &event.kind {
            SessionEventKind::ProviderEvent { kind, payload, .. }
                if kind == "context_compaction"
                    && payload.get("trigger").and_then(Value::as_str)
                        == Some("provider_input_size") =>
            {
                provider_input_compacted = true;
            }
            SessionEventKind::TurnCompleted { message_id, .. } if *message_id == second_id => {
                break;
            }
            _ => {}
        }
    }

    let prompt_lengths = prompt_lengths.lock().unwrap().clone();
    assert_eq!(prompt_lengths.len(), 2);
    assert!(prompt_lengths[1] < SUBSCRIPTION_INPUT_BUDGET_CHARS);
    assert!(
        !provider_input_compacted,
        "a healthy pooled subscription must not compact the full durable replay"
    );

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();
    scratch.discard().await;
}

/// Failure mode: a non-waking team message settled while the root was idle is
/// in the durable transcript, but a resumed provider session only receives
/// each turn's own input, so the model never saw the peer's reply.
#[tokio::test]
async fn resumed_provider_turn_receives_team_messages_settled_while_idle() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let (first_id, team_id, second_id) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(256);
    let executor = Arc::new(ReusableContextExecutor {
        prompt_lengths: Arc::new(Mutex::new(Vec::new())),
        called: Arc::new(Notify::new()),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn(async move {
        run_session_actor(
            &journal_path,
            session_id,
            LaunchSession {
                request_id: Uuid::new_v4(),
                cwd: root.path().to_path_buf(),
                provider: CodingProvider::Claude,
                model: None,
                effort: None,
                fast: Some(false),
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
                name: None,
                initial_prompt: None,
                capabilities: Default::default(),
                subagent_concurrency_limit: None,
                extension_skill_roots: Vec::new(),
                team_policy: None,
            },
            command_rx,
            event_tx,
            executor,
            actor_store,
        )
        .await
    });
    let prompt = |message_id, text: &str| HostCommand::Prompt {
        session_id,
        message_id,
        text: text.to_string(),
        attachments: Vec::new(),
        output_schema: None,
        delivery: PromptDelivery::Queue,
    };
    let mut wait_for = async |mut done: Box<dyn FnMut(&SessionEventKind) -> bool>| {
        let mut prompts = Vec::new();
        loop {
            let event = tokio::time::timeout(Duration::from_secs(5), event_rx.recv())
                .await
                .expect("session makes progress")
                .expect("session remains open");
            if let SessionEventKind::ProviderEvent { kind, payload, .. } = &event.kind
                && kind == crate::PROVIDER_PROMPT_EVENT_KIND
            {
                prompts.push(payload.clone());
            }
            if done(&event.kind) {
                return prompts;
            }
        }
    };

    command_tx.send(prompt(first_id, "first")).await.unwrap();
    wait_for(Box::new(move |kind| {
        matches!(kind, SessionEventKind::TurnCompleted { message_id, .. } if *message_id == first_id)
    }))
    .await;

    command_tx
        .send(HostCommand::TeamPrompt {
            session_id,
            message_id: team_id,
            text: "Team message from /root:\n\npeer reply".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Queue,
        })
        .await
        .unwrap();
    wait_for(Box::new(move |kind| {
        matches!(
            kind,
            SessionEventKind::Message { message_id, status: MessageStatus::Complete, .. }
                if *message_id == team_id
        )
    }))
    .await;

    command_tx.send(prompt(second_id, "second")).await.unwrap();
    let prompts = wait_for(Box::new(move |kind| {
        matches!(kind, SessionEventKind::TurnCompleted { message_id, .. } if *message_id == second_id)
    }))
    .await;
    assert_eq!(prompts.len(), 1, "{prompts:?}");
    assert_eq!(prompts[0]["provider_context_reused"], true, "{prompts:?}");
    let sent = prompts[0]["prompt"].as_str().unwrap();
    assert!(
        sent.contains("peer reply") && sent.contains("second"),
        "{sent}"
    );
    assert!(sent.find("peer reply") < sent.find("second"), "{sent}");

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();
    scratch.discard().await;
}

#[tokio::test]
async fn same_provider_model_switch_does_not_compact_reusable_context() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let first_id = Uuid::new_v4();
    let second_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(256);
    let called = Arc::new(Notify::new());
    let prompt_lengths = Arc::new(Mutex::new(Vec::new()));
    let executor = Arc::new(ReusableContextExecutor {
        prompt_lengths: Arc::clone(&prompt_lengths),
        called: Arc::clone(&called),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn(async move {
        run_session_actor(
            &journal_path,
            session_id,
            LaunchSession {
                request_id: Uuid::new_v4(),
                cwd: root.path().to_path_buf(),
                provider: CodingProvider::Codex,
                model: Some("gpt-5.6-sol".to_string()),
                effort: Some("xhigh".to_string()),
                fast: Some(false),
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
                name: None,
                initial_prompt: None,
                capabilities: Default::default(),
                subagent_concurrency_limit: None,
                extension_skill_roots: Vec::new(),
                team_policy: None,
            },
            command_rx,
            event_tx,
            executor,
            actor_store,
        )
        .await
    });

    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: first_id,
            text: "u".repeat(450_000),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Queue,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), called.notified())
        .await
        .expect("first pooled turn completes");
    loop {
        let event = tokio::time::timeout(Duration::from_secs(5), event_rx.recv())
            .await
            .expect("first model-switch turn remains live")
            .expect("session remains open");
        if matches!(
            event.kind,
            SessionEventKind::TurnCompleted {
                message_id,
                error: None,
                ..
            } if message_id == first_id
        ) {
            break;
        }
    }

    command_tx
        .send(HostCommand::Configure {
            session_id,
            action: crate::SessionConfigAction::SetModel {
                model: "gpt-5.6-luna".to_string(),
            },
        })
        .await
        .unwrap();
    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: second_id,
            text: "continue".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Queue,
        })
        .await
        .unwrap();

    let mut observed_model_switch = false;
    let mut provider_input_compacted = false;
    let second_error = loop {
        let event = tokio::time::timeout(Duration::from_secs(5), event_rx.recv())
            .await
            .expect("model-switch turn remains live")
            .expect("session remains open");
        match &event.kind {
            SessionEventKind::SessionConfigured {
                provider: CodingProvider::Codex,
                model: Some(model),
                ..
            } if model == "gpt-5.6-luna" => {
                observed_model_switch = true;
            }
            SessionEventKind::ProviderEvent { kind, payload, .. }
                if kind == "context_compaction"
                    && payload.get("trigger").and_then(Value::as_str)
                        == Some("provider_input_size") =>
            {
                provider_input_compacted = true;
            }
            SessionEventKind::TurnCompleted {
                message_id, error, ..
            } if *message_id == second_id => {
                break error.clone();
            }
            _ => {}
        }
    };

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();

    assert!(observed_model_switch);
    assert!(!provider_input_compacted);
    assert!(
        second_error.is_none(),
        "model-switch turn failed: {second_error:?}"
    );
    let prompt_lengths = prompt_lengths.lock().unwrap().clone();
    assert_eq!(prompt_lengths.len(), 2);
    assert!(prompt_lengths[1] < SUBSCRIPTION_INPUT_BUDGET_CHARS);
    scratch.discard().await;
}

#[tokio::test]
async fn resumed_codex_checkpoint_avoids_large_replay_compaction_after_actor_restart() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let previous_id = Uuid::new_v4();
    let next_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        SessionEventKind::SessionConfigured {
            cwd: root.path().to_path_buf(),
            provider: CodingProvider::Codex,
            model: Some("gpt-5.6-luna".to_string()),
            effort: Some("max".to_string()),
            fast: false,
            response_language: crate::ResponseLanguage::Auto,
            permission_mode: PermissionMode::Manual,
        },
        SessionEventKind::Message {
            message_id: previous_id,
            actor: EventActor::User,
            text: "u".repeat(450_000),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: Some(PromptDelivery::Queue),
        },
        SessionEventKind::TurnStarted {
            message_id: previous_id,
            provider: CodingProvider::Codex,
            model: Some("gpt-5.6-luna".to_string()),
            effort: Some("max".to_string()),
            fast: false,
        },
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::Assistant,
            text: "r".repeat(650_000),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
        SessionEventKind::ProviderSessionLinked {
            provider_session_id: "resumed-codex-thread".to_string(),
            provider_turn_id: Some("completed-codex-turn".to_string()),
            context_contract_version: Some(crate::agent::PROVIDER_CONTEXT_CONTRACT_VERSION),
        },
        SessionEventKind::TurnCompleted {
            message_id: previous_id,
            provider_session_id: Some("resumed-codex-thread".to_string()),
            final_text: "done".to_string(),
            error: None,
        },
        SessionEventKind::StatusChanged {
            status: SessionStatus::Ready,
            detail: None,
        },
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }

    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, _event_rx) = mpsc::channel(64);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let called = Arc::new(Notify::new());
    let compaction_calls = Arc::new(AtomicUsize::new(0));
    let executor = Arc::new(DurableResumeExecutor {
        seen: Arc::clone(&seen),
        called: Arc::clone(&called),
        compaction_calls: Arc::clone(&compaction_calls),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn(async move {
        run_session_actor(
            &journal_path,
            session_id,
            LaunchSession {
                request_id: Uuid::new_v4(),
                cwd: root.path().to_path_buf(),
                provider: CodingProvider::Codex,
                model: Some("gpt-5.6-luna".to_string()),
                effort: Some("max".to_string()),
                fast: Some(false),
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
                name: None,
                initial_prompt: None,
                capabilities: Default::default(),
                subagent_concurrency_limit: None,
                extension_skill_roots: Vec::new(),
                team_policy: None,
            },
            command_rx,
            event_tx,
            executor,
            actor_store,
        )
        .await
    });

    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: next_id,
            text: "continue after restart".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Queue,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), called.notified())
        .await
        .expect("resumed turn starts without compaction");
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();

    assert_eq!(compaction_calls.load(Ordering::Acquire), 0);
    // Drop the guard before the scratch database is discarded: the
    // assertions need it, the teardown await must not hold it.
    {
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(
            seen[0].0,
            format_subscription_frame(&format_subscription_actor_value(
                EventActor::User,
                "continue after restart"
            ))
        );
        assert_eq!(seen[0].1.as_deref(), Some("resumed-codex-thread"));
        assert_eq!(seen[0].2, None);
        assert_eq!(seen[0].3, 0);
    }
    scratch.discard().await;
}

#[tokio::test]
async fn crash_resume_forks_the_last_completed_codex_turn_before_replaying_input() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let completed_id = Uuid::new_v4();
    let interrupted_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        SessionEventKind::SessionConfigured {
            cwd: root.path().to_path_buf(),
            provider: CodingProvider::Codex,
            model: Some("gpt-5.6-sol".to_string()),
            effort: Some("xhigh".to_string()),
            fast: false,
            response_language: crate::ResponseLanguage::Auto,
            permission_mode: PermissionMode::Manual,
        },
        SessionEventKind::Message {
            message_id: completed_id,
            actor: EventActor::User,
            text: "u".repeat(450_000),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: Some(PromptDelivery::Queue),
        },
        SessionEventKind::TurnStarted {
            message_id: completed_id,
            provider: CodingProvider::Codex,
            model: Some("gpt-5.6-sol".to_string()),
            effort: Some("xhigh".to_string()),
            fast: false,
        },
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::Assistant,
            text: "r".repeat(650_000),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
        SessionEventKind::ProviderSessionLinked {
            provider_session_id: "codex-thread-before-crash".to_string(),
            provider_turn_id: Some("codex-turn-before-crash".to_string()),
            context_contract_version: Some(crate::agent::PROVIDER_CONTEXT_CONTRACT_VERSION),
        },
        SessionEventKind::TurnCompleted {
            message_id: completed_id,
            provider_session_id: Some("codex-thread-before-crash".to_string()),
            final_text: "done".to_string(),
            error: None,
        },
        SessionEventKind::Message {
            message_id: interrupted_id,
            actor: EventActor::User,
            text: "recover this exact input".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: Some(PromptDelivery::Queue),
        },
        SessionEventKind::TurnStarted {
            message_id: interrupted_id,
            provider: CodingProvider::Codex,
            model: Some("gpt-5.6-sol".to_string()),
            effort: Some("xhigh".to_string()),
            fast: false,
        },
        SessionEventKind::StatusChanged {
            status: SessionStatus::Running,
            detail: Some("provider active when host crashed".to_string()),
        },
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }
    let checkpoint = store.state(session_id).await.unwrap();
    assert_eq!(
        checkpoint.provider_session_id.as_deref(),
        Some("codex-thread-before-crash")
    );
    assert_eq!(
        checkpoint.provider_turn_id.as_deref(),
        Some("codex-turn-before-crash")
    );

    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, _event_rx) = mpsc::channel(64);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let called = Arc::new(Notify::new());
    let compaction_calls = Arc::new(AtomicUsize::new(0));
    let executor = Arc::new(DurableResumeExecutor {
        seen: Arc::clone(&seen),
        called: Arc::clone(&called),
        compaction_calls: Arc::clone(&compaction_calls),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn(async move {
        run_session_actor(
            &journal_path,
            session_id,
            LaunchSession {
                request_id: Uuid::new_v4(),
                cwd: root.path().to_path_buf(),
                provider: CodingProvider::Codex,
                model: Some("gpt-5.6-sol".to_string()),
                effort: Some("xhigh".to_string()),
                fast: Some(false),
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
                name: None,
                initial_prompt: None,
                capabilities: Default::default(),
                subagent_concurrency_limit: None,
                extension_skill_roots: Vec::new(),
                team_policy: None,
            },
            command_rx,
            event_tx,
            executor,
            actor_store,
        )
        .await
    });

    tokio::time::timeout(Duration::from_secs(5), called.notified())
        .await
        .expect("recovered turn starts from the provider checkpoint");
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();

    assert_eq!(compaction_calls.load(Ordering::Acquire), 0);
    // Drop the guard before the scratch database is discarded: the
    // assertions need it, the teardown await must not hold it.
    {
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(
            seen[0].0,
            format_subscription_frame(&format_subscription_actor_value(
                EventActor::User,
                "recover this exact input"
            ))
        );
        assert_eq!(seen[0].1.as_deref(), Some("codex-thread-before-crash"));
        assert_eq!(seen[0].2.as_deref(), Some("codex-turn-before-crash"));
        assert_eq!(seen[0].3, 0);
    }
    scratch.discard().await;
}

#[tokio::test]
async fn crash_resume_replays_native_compaction_without_subagent_inflation() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let completed_id = Uuid::new_v4();
    let interrupted_id = Uuid::new_v4();
    let child_id = Uuid::new_v4();
    let now = chrono::Utc::now();
    let kinds = vec![
        SessionEventKind::SessionStarted,
        SessionEventKind::SessionConfigured {
            cwd: root.path().to_path_buf(),
            provider: CodingProvider::Codex,
            model: Some("gpt-5.6-sol".to_string()),
            effort: Some("xhigh".to_string()),
            fast: false,
            response_language: crate::ResponseLanguage::Auto,
            permission_mode: PermissionMode::Manual,
        },
        SessionEventKind::Message {
            message_id: completed_id,
            actor: EventActor::User,
            text: "old-user-context".repeat(30_000),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: Some(PromptDelivery::Queue),
        },
        SessionEventKind::TurnStarted {
            message_id: completed_id,
            provider: CodingProvider::Codex,
            model: Some("gpt-5.6-sol".to_string()),
            effort: Some("xhigh".to_string()),
            fast: false,
        },
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::Assistant,
            text: "old-assistant-context".repeat(30_000),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
        SessionEventKind::TurnCompleted {
            message_id: completed_id,
            provider_session_id: Some("pre-compaction-thread".to_string()),
            final_text: "done".to_string(),
            error: None,
        },
        SessionEventKind::ProviderEvent {
            provider: CodingProvider::Codex,
            kind: "context_compaction".to_string(),
            payload: json!({
                "status": "completed",
                "summary": "the exact compacted semantic checkpoint"
            }),
        },
        SessionEventKind::SubagentActivity {
            activity: SubagentActivityKind::Stopped,
            agent: crate::SubagentSnapshot {
                session_id: child_id,
                parent_session_id: session_id,
                task_name: "/root/historical_agent".to_string(),
                status: crate::SubagentStatus::Stopped,
                provider: CodingProvider::Codex,
                model: Some("gpt-5.6-sol".to_string()),
                effort: Some("xhigh".to_string()),
                cwd: root.path().to_path_buf(),
                created_at: now,
                updated_at: now,
                detail: Some("historical agent card".repeat(20_000)),
                final_text: Some("historical agent output".repeat(20_000)),
                usage: Default::default(),
                interrupted_by: None,
            },
            event: None,
        },
        SessionEventKind::Message {
            message_id: interrupted_id,
            actor: EventActor::User,
            text: "recover the active request".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: Some(PromptDelivery::Queue),
        },
        SessionEventKind::TurnStarted {
            message_id: interrupted_id,
            provider: CodingProvider::Codex,
            model: Some("gpt-5.6-sol".to_string()),
            effort: Some("xhigh".to_string()),
            fast: false,
        },
        SessionEventKind::UsageUpdated {
            provider_duration_ms: 1,
            turn_id: Some(interrupted_id),
            provider_context_reused: Some(true),
            input_tokens: 99_000,
            output_tokens: 1_000,
            cached_input_tokens: 90_000,
            cache_creation_input_tokens: 0,
            total_tokens: 100_000,
            cost_microusd: None,
            cost_basis: String::new(),
            cost_usd: None,
            context_tokens: Some(100_000),
            context_window_tokens: Some(258_400),
        },
        SessionEventKind::StatusChanged {
            status: SessionStatus::Running,
            detail: Some("provider active when host crashed".to_string()),
        },
    ];
    let before_crash = kinds
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, kind)| SessionEvent::new(session_id, index as u64 + 1, kind))
        .collect::<Vec<_>>();
    let retained_before_crash = retained_conversation_context(&before_crash).unwrap();
    assert!(!retained_before_crash.contains("old-user-context"));
    assert!(!retained_before_crash.contains("historical agent"));

    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    store.create_session(session_id).await.unwrap();
    for event in before_crash {
        store.append(event).await.unwrap();
    }

    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, _event_rx) = mpsc::channel(64);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let called = Arc::new(Notify::new());
    let compaction_calls = Arc::new(AtomicUsize::new(0));
    let executor = Arc::new(DurableResumeExecutor {
        seen: Arc::clone(&seen),
        called: Arc::clone(&called),
        compaction_calls: Arc::clone(&compaction_calls),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn(async move {
        run_session_actor(
            &journal_path,
            session_id,
            LaunchSession {
                request_id: Uuid::new_v4(),
                cwd: root.path().to_path_buf(),
                provider: CodingProvider::Codex,
                model: Some("gpt-5.6-sol".to_string()),
                effort: Some("xhigh".to_string()),
                fast: Some(false),
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
                name: None,
                initial_prompt: None,
                capabilities: Default::default(),
                subagent_concurrency_limit: None,
                extension_skill_roots: Vec::new(),
                team_policy: None,
            },
            command_rx,
            event_tx,
            executor,
            actor_store,
        )
        .await
    });

    tokio::time::timeout(Duration::from_secs(5), called.notified())
        .await
        .expect("recovered turn starts without another compaction");
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();

    assert_eq!(compaction_calls.load(Ordering::Acquire), 0);
    // Drop the guard before the scratch database is discarded: the
    // assertions need it, the teardown await must not hold it.
    {
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        // The interrupted prompt is part of the durable branch, but the recovered
        // turn re-issues it as the live request, so it must appear exactly once:
        // as the current request at the end, never duplicated in the history.
        let recovered = &seen[0].0;
        assert_eq!(recovered.matches("recover the active request").count(), 1);
        assert!(recovered.starts_with(SUBSCRIPTION_CONTEXT_HEADER));
        assert!(recovered.contains("Previous conversation summary"));
        assert!(!recovered.contains("old-user-context"));
        assert!(!recovered.contains("historical agent"));
        assert!(recovered.ends_with(&format_subscription_frame(
            &format_subscription_actor_value(EventActor::User, "recover the active request")
        )));
        assert_eq!(seen[0].1, None);
        assert_eq!(seen[0].2, None);
        assert_eq!(seen[0].3, 0);
    }
    scratch.discard().await;
}

#[tokio::test]
async fn acknowledged_codex_escape_does_not_compact_a_reusable_large_context() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let first_id = Uuid::new_v4();
    let interrupted_id = Uuid::new_v4();
    let corrected_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(256);
    let called = Arc::new(Notify::new());
    let prompt_lengths = Arc::new(Mutex::new(Vec::new()));
    let compaction_calls = Arc::new(AtomicUsize::new(0));
    let executor = Arc::new(InterruptibleReusableContextExecutor {
        prompt_lengths: Arc::clone(&prompt_lengths),
        called: Arc::clone(&called),
        calls: AtomicUsize::new(0),
        compaction_calls: Arc::clone(&compaction_calls),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn(async move {
        run_session_actor(
            &journal_path,
            session_id,
            LaunchSession {
                request_id: Uuid::new_v4(),
                cwd: root.path().to_path_buf(),
                provider: CodingProvider::Codex,
                model: Some("gpt-5.6-luna".to_string()),
                effort: Some("max".to_string()),
                fast: Some(false),
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
                name: None,
                initial_prompt: None,
                capabilities: Default::default(),
                subagent_concurrency_limit: None,
                extension_skill_roots: Vec::new(),
                team_policy: None,
            },
            command_rx,
            event_tx,
            executor,
            actor_store,
        )
        .await
    });

    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: first_id,
            text: "u".repeat(450_000),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Queue,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), called.notified())
        .await
        .expect("large first turn starts");

    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: interrupted_id,
            text: "cancel this turn".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Queue,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), called.notified())
        .await
        .expect("interruptible turn starts");
    command_tx
        .send(HostCommand::Interrupt { session_id })
        .await
        .unwrap();

    loop {
        let event = tokio::time::timeout(Duration::from_secs(5), event_rx.recv())
            .await
            .expect("interrupted turn reaches its durable boundary")
            .expect("session remains attached");
        if matches!(
            event.kind,
            SessionEventKind::TurnCompleted { message_id, .. }
                if message_id == interrupted_id
        ) {
            break;
        }
    }

    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: corrected_id,
            text: "corrected request".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Queue,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), called.notified())
        .await
        .expect("corrected turn starts without compaction");

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();

    let remaining_events = std::iter::from_fn(|| event_rx.try_recv().ok()).collect::<Vec<_>>();
    assert_eq!(compaction_calls.load(Ordering::Acquire), 0);
    assert!(!remaining_events.iter().any(|event| matches!(
        &event.kind,
        SessionEventKind::ProviderEvent { kind, payload, .. }
            if kind == "context_compaction"
                && payload.get("trigger").and_then(Value::as_str)
                    == Some("provider_input_size")
    )));
    assert_eq!(prompt_lengths.lock().unwrap().len(), 3);
    scratch.discard().await;
}

#[test]
fn native_auto_compaction_starts_at_five_percent_effective_context_remaining() {
    let state = |context_tokens, context_window_tokens| SessionState {
        usage: crate::SessionUsage {
            context_tokens: Some(context_tokens),
            context_window_tokens: Some(context_window_tokens),
            ..crate::SessionUsage::default()
        },
        ..SessionState::default()
    };
    assert!(!native_auto_compaction_needed(&state(94_999, 100_000)));
    assert!(native_auto_compaction_needed(&state(95_000, 100_000)));
    assert!(native_auto_compaction_needed(&state(100_000, 100_000)));
    assert!(!native_auto_compaction_needed(&SessionState::default()));
}

#[test]
fn consultation_profiles_resolve_aliases_and_catalog_models() {
    assert_eq!(
        resolve_consultation_profile("claude").unwrap(),
        (
            CodingProvider::Claude,
            Some(borg_provider::claude_product_model().to_string()),
            None
        )
    );
    assert_eq!(
        resolve_consultation_profile("gpt").unwrap(),
        (CodingProvider::Codex, Some("gpt-6-astra".to_string()), None)
    );
    assert_eq!(
        resolve_consultation_profile("claude/claude-opus-5-5").unwrap(),
        (
            CodingProvider::Claude,
            Some("claude-opus-5-5".to_string()),
            None
        )
    );
    assert_eq!(
        resolve_consultation_profile("claude-fable-5-1@high").unwrap(),
        (
            CodingProvider::Claude,
            Some("claude-fable-5-1".to_string()),
            Some("high".to_string())
        )
    );
    assert_eq!(
        resolve_consultation_profile("gpt-5.6-sol@xhigh").unwrap(),
        (
            CodingProvider::Codex,
            Some("gpt-5.6-sol".to_string()),
            Some("xhigh".to_string())
        )
    );
    assert!(resolve_consultation_profile("not-a-provider").is_err());
}

#[tokio::test]
async fn borg_tool_approvals_route_and_cancel_without_provider_controls() {
    struct RuntimeExecutor {
        cancel: tokio_util::sync::CancellationToken,
        result: Arc<Mutex<Option<Result<Value, String>>>>,
    }

    #[async_trait::async_trait]
    impl AgentTurnExecutor for RuntimeExecutor {
        async fn execute(
            &self,
            turn: AgentTurn,
            events: mpsc::Sender<SessionEventKind>,
            controls: Option<mpsc::Receiver<AgentTurnControl>>,
        ) -> Result<AgentTurnResult> {
            let mut controls = controls.unwrap();
            let call = turn.agent_tools.call_with_workflow_control(
                "runtime_exec",
                json!({"code": "40 + 2"}),
                false,
                Some(self.cancel.clone()),
            );
            tokio::pin!(call);
            let result = tokio::select! {
                biased;
                control = controls.recv() => {
                    assert!(matches!(control, Some(AgentTurnControl::Interrupt)),
                        "Borg approval must not be forwarded to the provider");
                    Err(anyhow::anyhow!("interrupted"))
                }
                result = &mut call => result,
            };
            *self.result.lock().unwrap() = Some(result.map_err(|error| error.to_string()));
            events
                .send(SessionEventKind::Message {
                    message_id: Uuid::new_v4(),
                    actor: EventActor::Assistant,
                    text: "tool finished".into(),
                    attachments: Vec::new(),
                    status: MessageStatus::Complete,
                    delivery: None,
                })
                .await
                .unwrap();
            Ok(AgentTurnResult {
                provider_session_id: None,
                final_text: "tool finished".into(),
            })
        }
    }

    for outcome in ["allow", "deny", "disconnect", "interrupt"] {
        let root = tempdir().unwrap();
        let journal_path = root.path().join("session.lock");
        let session_id = Uuid::new_v4();
        let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
        let store: Arc<dyn SessionStore> = Arc::new(store);
        store.create_session(session_id).await.unwrap();
        let (command_tx, command_rx) = mpsc::channel(8);
        let (event_tx, mut event_rx) = mpsc::channel(256);
        let cancel = tokio_util::sync::CancellationToken::new();
        let result = Arc::new(Mutex::new(None));
        let executor = Arc::new(RuntimeExecutor {
            cancel: cancel.clone(),
            result: result.clone(),
        });
        let actor_store = Arc::clone(&store);
        let actor = tokio::spawn(async move {
            run_session_actor(
                &journal_path,
                session_id,
                LaunchSession {
                    request_id: Uuid::new_v4(),
                    cwd: root.path().to_path_buf(),
                    provider: CodingProvider::Claude,
                    model: None,
                    effort: None,
                    fast: None,
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::Manual,
                    name: None,
                    initial_prompt: Some("use runtime".into()),
                    capabilities: Default::default(),
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                executor,
                actor_store,
            )
            .await
        });
        let approval_id = loop {
            let event = tokio::time::timeout(Duration::from_secs(5), event_rx.recv())
                .await
                .unwrap()
                .unwrap();
            if let SessionEventKind::ApprovalRequested { approval_id, .. } = event.kind {
                break approval_id;
            }
        };
        assert!(
            result.lock().unwrap().is_none(),
            "execution must wait for authorization"
        );
        command_tx
            .send(HostCommand::Approve {
                session_id,
                approval_id: "unrelated".into(),
                decision: crate::ApprovalDecision::AllowOnce,
            })
            .await
            .unwrap();
        match outcome {
            "disconnect" => cancel.cancel(),
            "interrupt" => {
                command_tx
                    .send(HostCommand::Interrupt { session_id })
                    .await
                    .unwrap();
                command_tx
                    .send(HostCommand::Approve {
                        session_id,
                        approval_id: approval_id.clone(),
                        decision: crate::ApprovalDecision::AllowOnce,
                    })
                    .await
                    .unwrap();
            }
            _ => command_tx
                .send(HostCommand::Approve {
                    session_id,
                    approval_id: approval_id.clone(),
                    decision: if outcome == "allow" {
                        crate::ApprovalDecision::AllowOnce
                    } else {
                        crate::ApprovalDecision::Deny
                    },
                })
                .await
                .unwrap(),
        }
        let mut resolved = false;
        loop {
            let event = tokio::time::timeout(Duration::from_secs(10), event_rx.recv())
                .await
                .unwrap()
                .unwrap();
            match event.kind {
                SessionEventKind::ApprovalResolved {
                    approval_id: id,
                    decision,
                } => {
                    assert_eq!(id, approval_id);
                    assert_eq!(
                        decision,
                        if outcome == "allow" {
                            crate::ApprovalDecision::AllowOnce
                        } else {
                            crate::ApprovalDecision::Deny
                        }
                    );
                    assert!(!resolved);
                    resolved = true;
                }
                SessionEventKind::TurnCompleted { .. } => break,
                _ => {}
            }
        }
        assert!(
            resolved,
            "terminal turn must resolve the canonical approval"
        );
        if outcome != "interrupt" {
            let result = result.lock().unwrap();
            let result = result.as_ref().expect("tool caller completed");
            if outcome == "allow" {
                assert_eq!(result.as_ref().unwrap()["value"], 42);
            } else {
                assert!(result.is_err());
            }
        }
        command_tx
            .send(HostCommand::Stop { session_id })
            .await
            .unwrap();
        actor.await.unwrap().unwrap();
        scratch.discard().await;
    }
}

#[tokio::test]
async fn cancelling_a_turn_resolves_its_pending_approval_as_denied() {
    let session_id = Uuid::new_v4();
    let (scratch, _store, mut journal) = runtime_store(session_id).await;
    let (events, mut event_rx) = mpsc::channel(4);
    let (response, receiver) = oneshot::channel();
    let mut pending = Some(PendingApproval {
        id: "approval-1".to_string(),
        response: Some(response),
    });

    deny_pending_approval(&mut journal, &events, session_id, &mut pending)
        .await
        .unwrap();

    assert!(pending.is_none());
    assert!(
        receiver.await.is_err(),
        "cancellation must not authorize the caller"
    );
    let event = event_rx.recv().await.unwrap();
    assert!(matches!(
        event.kind,
        SessionEventKind::ApprovalResolved {
            ref approval_id,
            decision: crate::ApprovalDecision::Deny,
        } if approval_id == "approval-1"
    ));
    scratch.discard().await;
}

#[tokio::test]
async fn cancelling_a_turn_resolves_its_pending_provider_interaction() {
    let session_id = Uuid::new_v4();
    let (scratch, _store, mut journal) = runtime_store(session_id).await;
    let (events, mut event_rx) = mpsc::channel(4);
    let mut pending = Some("interaction-1".to_string());

    cancel_pending_provider_interaction(&mut journal, &events, session_id, &mut pending)
        .await
        .unwrap();

    assert!(pending.is_none());
    let event = event_rx.recv().await.unwrap();
    assert!(matches!(
        event.kind,
        SessionEventKind::ProviderInteractionResolved {
            ref interaction_id,
            response: serde_json::Value::Null,
        } if interaction_id == "interaction-1"
    ));
    scratch.discard().await;
}

#[tokio::test]
async fn parent_journal_preserves_full_child_transcript_events() {
    let root = tempdir().unwrap();
    let parent_id = Uuid::new_v4();
    let child_id = Uuid::new_v4();
    let (scratch, postgres) = crate::session_store::postgres::testing::session_store().await;
    let postgres = Arc::new(postgres);
    postgres.create_session(parent_id).await.unwrap();
    let launch = LaunchSession {
        request_id: Uuid::new_v4(),
        cwd: root.path().to_path_buf(),
        provider: CodingProvider::Codex,
        model: Some("gpt-test".to_string()),
        effort: Some("low".to_string()),
        fast: Some(false),
        response_language: crate::ResponseLanguage::Auto,
        permission_mode: PermissionMode::FullAccess,
        name: None,
        initial_prompt: None,
        capabilities: Default::default(),
        subagent_concurrency_limit: None,
        extension_skill_roots: Vec::new(),
        team_policy: None,
    };
    let coordinator = SubagentCoordinator::new_with_store_and_executor(
        root.path(),
        parent_id,
        launch,
        16,
        Arc::new(LocalAgentTurnExecutor::default()),
        postgres.clone(),
    )
    .unwrap();
    let snapshot = crate::SubagentSnapshot {
        session_id: child_id,
        parent_session_id: parent_id,
        task_name: "/root/worker".to_string(),
        status: crate::SubagentStatus::Stopped,
        provider: CodingProvider::Codex,
        model: Some("gpt-test".to_string()),
        effort: Some("low".to_string()),
        cwd: root.path().to_path_buf(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        detail: None,
        final_text: None,
        usage: Default::default(),
        interrupted_by: None,
    };
    coordinator
        .restore_from_events(&[SessionEvent::new(
            parent_id,
            1,
            SessionEventKind::SubagentActivity {
                activity: SubagentActivityKind::Stopped,
                agent: snapshot,
                event: None,
            },
        )])
        .await
        .unwrap();

    let persisted = postgres.clone();
    let store: Arc<dyn SessionStore> = postgres;
    let mut journal = RuntimeSessionStore::new(store, Vec::new(), true);
    let (events, mut event_rx) = mpsc::channel(4);
    let child_event = SessionEvent::new(
        child_id,
        7,
        SessionEventKind::ToolStarted {
            tool_call_id: "call-1".to_string(),
            name: "exec".to_string(),
            input: json!({"cmd": "cargo test"}),
            input_ref: None,
        },
    );

    let watches = test_watches(parent_id);
    record_subagent_activity(
        &mut journal,
        &events,
        parent_id,
        &coordinator,
        &watches,
        SubagentActivity::SessionEvent {
            parent_session_id: parent_id,
            task_name: "/root/worker".to_string(),
            event: child_event,
        },
    )
    .await
    .unwrap();

    let projected = event_rx.recv().await.unwrap();
    assert!(matches!(
        projected.kind,
        SessionEventKind::SubagentActivity {
            event: Some(child_event),
            ..
        } if matches!(
            child_event.kind,
            SessionEventKind::ToolStarted {
                ref tool_call_id,
                ref name,
                ..
            } if tool_call_id == "call-1" && name == "exec"
        )
    ));
    assert!(
        persisted
            .read(parent_id)
            .await
            .unwrap()
            .iter()
            .any(|event| matches!(
                event.kind,
                SessionEventKind::SubagentActivity {
                    event: Some(ref child_event),
                    ..
                } if matches!(
                    child_event.kind,
                    SessionEventKind::ToolStarted { ref tool_call_id, .. }
                        if tool_call_id == "call-1"
                )
            )),
        "child activity must remain replayable after the live projection disconnects"
    );

    let message_id = Uuid::new_v4();
    let child_message = |sequence, text: &str, status| SubagentActivity::SessionEvent {
        parent_session_id: parent_id,
        task_name: "/root/worker".to_string(),
        event: SessionEvent::new(
            child_id,
            sequence,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::Assistant,
                text: text.to_string(),
                attachments: Vec::new(),
                status,
                delivery: None,
            },
        ),
    };
    record_subagent_activity(
        &mut journal,
        &events,
        parent_id,
        &coordinator,
        &watches,
        child_message(0, "I", MessageStatus::InProgress),
    )
    .await
    .unwrap();
    record_subagent_activity(
        &mut journal,
        &events,
        parent_id,
        &coordinator,
        &watches,
        child_message(8, "I am complete", MessageStatus::Complete),
    )
    .await
    .unwrap();

    let partial = event_rx.recv().await.unwrap();
    let notification = event_rx.recv().await.unwrap();
    assert!(matches!(
        notification.kind,
        SessionEventKind::AgentMessageReceived {
            message_id: received_id,
            sender_id,
            ref text,
            ..
        } if received_id == message_id && sender_id == child_id && text == "I am complete"
    ));
    let complete = event_rx.recv().await.unwrap();
    assert!(matches!(
        partial.kind,
        SessionEventKind::SubagentActivity {
            event: Some(child_event),
            ..
        } if matches!(
            child_event.kind,
            SessionEventKind::Message {
                ref text,
                status: MessageStatus::InProgress,
                ..
            } if text == "I"
        )
    ));
    assert!(matches!(
        complete.kind,
        SessionEventKind::SubagentActivity {
            event: Some(child_event),
            ..
        } if matches!(
            child_event.kind,
            SessionEventKind::Message {
                ref text,
                status: MessageStatus::Complete,
                ..
            } if text == "I am complete"
        )
    ));
    // The streaming partial reaches a live observer but is never journaled:
    // the child itself keeps only coalesced live state for an in-progress
    // message, so mirroring every keystroke into a durable parent row is what
    // grew one orchestration session's store into the gigabytes. Only the
    // durable child events (the tool call and the completed message) persist.
    let durable_child_events: Vec<_> = persisted
        .read(parent_id)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|event| match event.kind {
            SessionEventKind::SubagentActivity {
                event: Some(child_event),
                ..
            } => Some(child_event.kind),
            _ => None,
        })
        .collect();
    assert_eq!(
        durable_child_events.len(),
        2,
        "the tool call and the completed message persist; the streaming partial does not"
    );
    assert!(matches!(
        durable_child_events[0],
        SessionEventKind::ToolStarted { ref tool_call_id, .. } if tool_call_id == "call-1"
    ));
    assert!(matches!(
        durable_child_events[1],
        SessionEventKind::Message {
            status: MessageStatus::Complete,
            ref text,
            ..
        } if text == "I am complete"
    ));
    assert!(
        !durable_child_events.iter().any(|kind| matches!(
            kind,
            SessionEventKind::Message {
                status: MessageStatus::InProgress,
                ..
            }
        )),
        "an in-progress child message must not become a durable parent row"
    );

    record_subagent_activity(
        &mut journal,
        &events,
        parent_id,
        &coordinator,
        &watches,
        child_message(8, "I am complete", MessageStatus::Complete),
    )
    .await
    .unwrap();
    assert!(event_rx.try_recv().is_err());
    scratch.discard().await;
}

/// Streams durable events as fast as the channel accepts them and never
/// finishes on its own, recording when the turn is finally cancelled.
struct FloodingExecutor {
    aborted_at: Arc<Mutex<Option<std::time::Instant>>>,
    sent: Arc<std::sync::atomic::AtomicU64>,
}

struct AbortMarker(Arc<Mutex<Option<std::time::Instant>>>);

impl Drop for AbortMarker {
    fn drop(&mut self) {
        let mut slot = self.0.lock().unwrap();
        if slot.is_none() {
            *slot = Some(std::time::Instant::now());
        }
    }
}

#[async_trait::async_trait]
impl AgentTurnExecutor for FloodingExecutor {
    async fn execute(
        &self,
        _turn: AgentTurn,
        events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        let _marker = AbortMarker(Arc::clone(&self.aborted_at));
        let mut index = 0u64;
        loop {
            index += 1;
            self.sent.fetch_add(2, std::sync::atomic::Ordering::Relaxed);
            events
                .send(SessionEventKind::ToolStarted {
                    tool_call_id: format!("flood-{index}"),
                    name: "exec_command".into(),
                    input: serde_json::json!({"cmd": "echo"}),
                    input_ref: None,
                })
                .await
                .ok();
            events
                .send(SessionEventKind::ToolCompleted {
                    tool_call_id: format!("flood-{index}"),
                    output: "ok".into(),
                    output_ref: None,
                    is_error: false,
                    input: None,
                    input_ref: None,
                })
                .await
                .ok();
        }
    }
}

/// An operator pressing Escape must be heard while the provider is streaming.
/// Commands and provider events share one `select!`, so any unbounded work in
/// the provider arm is time the interrupt spends waiting in the queue.
#[tokio::test(flavor = "current_thread")]
async fn interrupt_is_honoured_while_a_stalled_observer_backs_up_the_event_stream() {
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let message_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    // Deliberately never drained: models a wedged or far-behind observer.
    let (event_tx, _event_rx) = mpsc::channel(128);
    let aborted_at = Arc::new(Mutex::new(None));
    let sent = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let executor = Arc::new(FloodingExecutor {
        aborted_at: Arc::clone(&aborted_at),
        sent: Arc::clone(&sent),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn({
        let journal_path = root.path().join("session.lock");
        let cwd = root.path().to_path_buf();
        async move {
            run_session_actor(
                &journal_path,
                session_id,
                LaunchSession {
                    request_id: message_id,
                    cwd,
                    provider: CodingProvider::Codex,
                    model: None,
                    effort: None,
                    fast: Some(false),
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::FullAccess,
                    name: None,
                    initial_prompt: Some("stream forever".to_string()),
                    capabilities: Default::default(),
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                executor,
                actor_store,
            )
            .await
        }
    });

    // Saturate the observer by EVENT COUNT, not by elapsed time.
    //
    // The interrupt waits behind whatever work has already been queued, so the
    // latency this test measures scales with how much the flood produced before
    // the interrupt was sent. A fixed 500ms warm-up makes that quantity a
    // property of the machine: measured across runs, 1,346 queued events gave
    // 1.15s and 3,586 gave 3.86s, so a faster host failed a test a slower one
    // passed. Waiting for a fixed backlog makes every run measure the same
    // thing.
    // Escape latency must not scale with provider event volume, so the volume
    // is overridable to measure that: BORG_TEST_FLOOD_EVENTS=50/450/2000.
    let saturation_events: u64 = std::env::var("BORG_TEST_FLOOD_EVENTS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(400);
    let saturate = std::time::Instant::now();
    while sent.load(std::sync::atomic::Ordering::Relaxed) < saturation_events {
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(
            // A guard on the loop, not the measurement. Every flooded event is
            // journaled, so on Postgres reaching the saturation count is bound
            // by database round trips rather than by the actor. The count is
            // what keeps the run comparable; this only has to be long enough
            // that a loaded database does not look like a hang.
            saturate.elapsed() < Duration::from_secs(120),
            "the flood never reached {saturation_events} events"
        );
    }
    // Scheduler starvation is reported with the result below. It is not the
    // cause of the latency -- it measures under a millisecond on runs that take
    // seconds -- but it is worth stating so the theory stays disproved.
    let probe = std::time::Instant::now();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let starvation = probe.elapsed().saturating_sub(Duration::from_millis(50));
    let requested_at = std::time::Instant::now();
    command_tx
        .send(HostCommand::Interrupt { session_id })
        .await
        .unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let observed = loop {
        if let Some(at) = *aborted_at.lock().unwrap() {
            break Some(at);
        }
        if std::time::Instant::now() > deadline {
            break None;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let latency = observed
        .expect("interrupt must cancel the turn")
        .duration_since(requested_at);

    command_tx.send(HostCommand::Stop { session_id }).await.ok();
    let _ = tokio::time::timeout(Duration::from_secs(10), actor).await;

    // The bound is the author's original 3s, unchanged. What changed is that
    // the test now has margin against it: a fixed backlog and a bounded
    // re-probe took the observed latency from 1.15-3.86s down to 1.0-2.0s.
    //
    // The counters are in the message because two plausible theories about this
    // test were wrong -- scheduler starvation, then a per-thread latch -- and
    // each was disproved by numbers rather than argument. A failure here should
    // arrive with the evidence rather than send the next reader guessing.
    assert!(
        latency < Duration::from_secs(3),
        "interrupt took {latency:?} to reach the provider turn \
         (runtime starvation {starvation:?}, {} events queued, {} delivery bursts, \
         {} of them blocked)",
        sent.load(std::sync::atomic::Ordering::Relaxed),
        crate::session::LIVE_DELIVERY_BURSTS.load(std::sync::atomic::Ordering::Relaxed),
        crate::session::LIVE_DELIVERY_BLOCKED.load(std::sync::atomic::Ordering::Relaxed),
    );
    scratch.discard().await;
}

/// A usage limit and a lost connection can arrive in one failure: an exhausted
/// lane drops its stream mid-turn, or answers through a gateway whose error body
/// also reads as a transport fault. The usage path is the only one that carries a
/// reset deadline and a visible "resumes" state, so it has to win. Read as a
/// connection instead, the same failure becomes a resend loop and the deadline
/// the user could plan around is lost.
struct UsageLimitAndConnectionExecutor {
    calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl AgentTurnExecutor for UsageLimitAndConnectionExecutor {
    async fn execute(
        &self,
        _turn: AgentTurn,
        _events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        if self.calls.fetch_add(1, Ordering::AcqRel) == 0 {
            return Err(anyhow::anyhow!(
                "usage limit reached · Provider-reported retry delay: 7200 seconds · connection reset by peer"
            ));
        }
        Ok(AgentTurnResult {
            provider_session_id: Some("provider-session".to_string()),
            final_text: "resumed after the limit cleared".to_string(),
        })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_usage_limit_is_never_retried_as_a_connection_loss() {
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let message_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(128);
    let executor = Arc::new(UsageLimitAndConnectionExecutor {
        calls: Arc::new(AtomicUsize::new(0)),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn({
        let journal_path = root.path().join("session.lock");
        let cwd = root.path().to_path_buf();
        async move {
            run_session_actor(
                &journal_path,
                session_id,
                LaunchSession {
                    request_id: message_id,
                    cwd,
                    provider: CodingProvider::Codex,
                    model: None,
                    effort: None,
                    fast: Some(false),
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::Manual,
                    name: None,
                    initial_prompt: Some("finish this task".to_string()),
                    capabilities: Default::default(),
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                executor,
                actor_store,
            )
            .await
        }
    });

    let mut network_retries = 0_usize;
    let retry_at = loop {
        let event = tokio::time::timeout(Duration::from_secs(30), event_rx.recv())
            .await
            .expect("the usage limit schedules a resume")
            .expect("session remains attached");
        match &event.kind {
            SessionEventKind::ProviderEvent { kind, .. } if kind == "network_retry" => {
                network_retries += 1;
            }
            SessionEventKind::ProviderEvent { kind, payload, .. }
                if kind == "usage_limit_retry" =>
            {
                break payload.get("retry_at").cloned();
            }
            _ => {}
        }
    };
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(30), actor).await;
    assert_eq!(
        network_retries, 0,
        "a usage limit must not be scheduled as a connection retry"
    );
    assert!(
        retry_at.is_some(),
        "the resume carries the deadline the provider reported"
    );
    scratch.discard().await;
}

struct NetworkThenSuccessExecutor {
    calls: Arc<AtomicUsize>,
    failures: usize,
    error: &'static str,
}

#[async_trait::async_trait]
impl AgentTurnExecutor for NetworkThenSuccessExecutor {
    async fn execute(
        &self,
        turn: AgentTurn,
        events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        let attempt = self.calls.fetch_add(1, Ordering::AcqRel);
        if attempt == 0 {
            events
                .send(SessionEventKind::ToolStarted {
                    tool_call_id: "completed-work".into(),
                    name: "exec_command".into(),
                    input: serde_json::json!({"cmd": "git status"}),
                    input_ref: None,
                })
                .await
                .unwrap();
            events
                .send(SessionEventKind::ToolCompleted {
                    tool_call_id: "completed-work".into(),
                    output: "clean".into(),
                    output_ref: None,
                    is_error: false,
                    input: None,
                    input_ref: None,
                })
                .await
                .unwrap();
        } else {
            // The first attempt ran a tool, so every later attempt must tell
            // the model to resume rather than carry the request out afresh:
            // redoing `exec_command` unattended is the failure mode here.
            assert!(
                turn.prompt.contains("do not repeat completed actions"),
                "attempt {attempt}, error {}, prompt {}",
                self.error,
                turn.prompt
            );
            assert!(
                turn.prompt
                    .contains("not as a new instruction to carry out from the start"),
                "the resend must be demoted from instruction to history: attempt {attempt}, error {}, prompt {}",
                self.error,
                turn.prompt
            );
            assert!(
                turn.prompt.contains("completed-work") || turn.prompt.contains("git status"),
                "attempt {attempt}, error {}, prompt {}",
                self.error,
                turn.prompt
            );
        }
        if attempt < self.failures {
            anyhow::bail!(self.error);
        }
        events
            .send(SessionEventKind::ToolStarted {
                tool_call_id: "resumed-work".into(),
                name: "exec".into(),
                input: json!({"cmd": "git status"}),
                input_ref: None,
            })
            .await?;
        tokio::time::sleep(Duration::from_millis(20)).await;
        if self.failures > 10 {
            turn.agent_tools
                .call("update_goal", serde_json::json!({"status": "complete"}))
                .await?;
        }
        Ok(AgentTurnResult {
            provider_session_id: Some("reconnected".into()),
            final_text: "done".into(),
        })
    }
}

#[test]
fn connection_retry_does_not_retry_authentication_or_command_failures() {
    for error in [
        "authentication failed: connection error",
        "invalid API key",
        "permission denied",
        "shell command timed out",
        "tool execution failed",
    ] {
        assert!(!provider_error_is_connection_lost(error), "{error}");
    }
    for error in [
        "Codex subscription connection failed",
        "Codex model catalog disconnected",
        "network is unreachable",
        "error sending request for url",
        "stream disconnected before completion",
        "ConnectionError: connection closed",
        // Observed in production: OpenCode's event stream dropping mid-turn.
        // A truncated HTTP body is a lost connection, and must be retried
        // rather than counted as a goal failure.
        "failed reading OpenCode server events: error decoding response body: error reading a body from connection: unexpected EOF during chunk size line",
        "error decoding response body: unexpected end of file",
        "connection closed before message completed",
    ] {
        assert!(provider_error_is_connection_lost(error), "{error}");
    }
}

#[test]
fn a_typed_transport_failure_is_retried_whatever_its_wording() {
    use borg_provider::provider::{ProviderErrorKind, ProviderStreamError};

    // Deliberately worded so no substring in the prose allowlist can match:
    // the only signal is the typed kind the transport recorded.
    let opaque = "glorp 7 terminated";
    assert!(
        !provider_error_is_connection_lost(opaque),
        "prose matching must genuinely not recognise this wording",
    );

    let typed = anyhow::Error::new(ProviderStreamError {
        kind: ProviderErrorKind::ConnectionLost,
        message: opaque.to_string(),
    })
    .context("provider turn failed");
    assert!(
        turn_error_is_connection_lost(&typed, &format!("{typed:#}")),
        "a typed connection loss must be retried even when its text says nothing",
    );

    // The typed kind must also be able to *prevent* a retry that prose would
    // have wrongly allowed: an auth failure that happens to mention a timeout.
    let fatal = anyhow::Error::new(ProviderStreamError {
        kind: ProviderErrorKind::Fatal,
        message: "connection reset".to_string(),
    });
    assert!(
        !turn_error_is_connection_lost(&fatal, &format!("{fatal:#}")),
        "a typed fatal error must not be retried because its text looks transient",
    );

    // The native path (Codex subscription, OpenRouter, Kimi, GLM,
    // OpenAI-compatible) fails with ProviderCallError, which flattens its cause
    // into a string. Its recorded kind is therefore the only signal left, and
    // it has to survive the trip to this decision.
    let native = anyhow::Error::new(borg_provider::provider::ProviderCallError {
        message: opaque.to_string(),
        trace: Default::default(),
        session_id: None,
        kind: ProviderErrorKind::ConnectionLost,
    })
    .context("native provider turn failed");
    assert!(
        turn_error_is_connection_lost(&native, &format!("{native:#}")),
        "a native transport failure must be retried even when its text says nothing",
    );

    // With no typed cause, prose matching still decides.
    let untyped = anyhow::anyhow!("network is unreachable");
    assert!(turn_error_is_connection_lost(
        &untyped,
        &format!("{untyped:#}")
    ));
    let untyped_fatal = anyhow::anyhow!("invalid api key");
    assert!(!turn_error_is_connection_lost(
        &untyped_fatal,
        &format!("{untyped_fatal:#}")
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn connection_outage_retries_repeatedly_and_preserves_the_durable_prompt() {
    // (error, failures, expected attempts, expected terminal stops). Only a
    // failure the connection path retries can spend its attempts; a refusal such
    // as an authentication error is not retried at all and never reaches the
    // bound.
    for (error, failures, expected_attempts, expected_exhausted) in [
        ("Codex subscription connection failed", 3, 4, 0),
        ("Codex model catalog disconnected", 3, 4, 0),
        (
            "openrouter request failed with HTTP 502: Provider returned error",
            3,
            4,
            0,
        ),
        ("authentication failed", 3, 1, 0),
        (
            "Codex subscription credentials rejected; reconnect Codex",
            3,
            1,
            0,
        ),
        (
            "Codex subscription authentication lookup unavailable",
            9,
            10,
            0,
        ),
        (
            // Twelve failures is past the bound, so the chain stops: one more
            // attempt than it is allowed to resend, never a thirteenth. A
            // session must not keep resending a request the provider keeps
            // refusing while the status line only counts down.
            "Codex subscription authentication lookup unavailable",
            12,
            11,
            1,
        ),
    ] {
        let root = tempdir().unwrap();
        let session_id = Uuid::new_v4();
        let message_id = Uuid::new_v4();
        let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
        let store: Arc<dyn SessionStore> = Arc::new(store);
        store.create_session(session_id).await.unwrap();
        if failures > 10 {
            let mut journal = RuntimeSessionStore::new(Arc::clone(&store), Vec::new(), true);
            journal
                .append(SessionEvent::new(
                    session_id,
                    0,
                    SessionEventKind::SessionStarted,
                ))
                .await
                .unwrap();
            journal
                .append(SessionEvent::new(
                    session_id,
                    0,
                    SessionEventKind::SessionConfigured {
                        cwd: root.path().to_path_buf(),
                        provider: CodingProvider::Codex,
                        model: None,
                        effort: None,
                        fast: false,
                        response_language: crate::ResponseLanguage::Auto,
                        permission_mode: PermissionMode::FullAccess,
                    },
                ))
                .await
                .unwrap();
            journal
                .append(SessionEvent::new(
                    session_id,
                    0,
                    SessionEventKind::GoalUpdated {
                        goal: SessionGoal::new("Finish the task".into(), None),
                    },
                ))
                .await
                .unwrap();
        }
        let (command_tx, command_rx) = mpsc::channel(8);
        let (event_tx, mut event_rx) = mpsc::channel(128);
        let calls = Arc::new(AtomicUsize::new(0));
        let executor = Arc::new(NetworkThenSuccessExecutor {
            calls: Arc::clone(&calls),
            error,
            failures,
        });
        let actor_store = Arc::clone(&store);
        let actor = tokio::spawn({
            let journal_path = root.path().join("session.lock");
            let cwd = root.path().to_path_buf();
            async move {
                run_session_actor(
                    &journal_path,
                    session_id,
                    LaunchSession {
                        request_id: message_id,
                        cwd,
                        provider: CodingProvider::Codex,
                        model: None,
                        effort: None,
                        fast: Some(false),
                        response_language: crate::ResponseLanguage::Auto,
                        permission_mode: PermissionMode::FullAccess,
                        name: None,
                        initial_prompt: Some("finish this task".to_string()),
                        capabilities: Default::default(),
                        subagent_concurrency_limit: None,
                        extension_skill_roots: Vec::new(),
                        team_policy: None,
                    },
                    command_rx,
                    event_tx,
                    executor,
                    actor_store,
                )
                .await
            }
        });

        let mut completions = 0;
        let mut visible_errors = 0;
        let mut completed_tools = 0;
        let mut retry_delays = Vec::new();
        let mut recovered = false;
        let mut exhausted = 0_usize;
        let mut exhausted_reason = None;
        while completions < expected_attempts {
            let event = tokio::time::timeout(Duration::from_secs(60), event_rx.recv())
                .await
                .expect("network retry completes")
                .expect("session remains attached");
            if let SessionEventKind::ProviderEvent { kind, payload, .. } = &event.kind
                && kind == "network_retry"
            {
                retry_delays.push(payload["delay_ms"].as_u64().unwrap());
            }
            if matches!(&event.kind, SessionEventKind::ProviderEvent { kind, .. } if kind == "network_recovered")
            {
                recovered = true;
            }
            if let SessionEventKind::ProviderEvent { kind, payload, .. } = &event.kind
                && kind == "network_retry_exhausted"
            {
                exhausted += 1;
                exhausted_reason = payload["error"].as_str().map(str::to_string);
            }
            if matches!(&event.kind, SessionEventKind::ToolStarted { tool_call_id, .. } if tool_call_id == "resumed-work")
            {
                assert!(
                    recovered,
                    "reconnect status must clear before resumed work finishes"
                );
            }
            if matches!(&event.kind, SessionEventKind::Error { message } if message == error) {
                visible_errors += 1;
            }
            if matches!(&event.kind, SessionEventKind::ToolCompleted { tool_call_id, .. } if tool_call_id == "completed-work")
            {
                completed_tools += 1;
            }
            assert!(
                completed_tools == 0
                    || !matches!(
                        &event.kind,
                        SessionEventKind::Message {
                            message_id: id,
                            status: MessageStatus::Queued,
                            ..
                        } if *id == message_id
                    ),
                "a delivered prompt must not reappear in pending input during reconnect: {event:?}"
            );
            if matches!(
                event.kind,
                SessionEventKind::TurnCompleted {
                    message_id: completed_message_id,
                    ..
                } if completed_message_id == message_id
            ) {
                completions += 1;
            }
        }

        command_tx
            .send(HostCommand::Stop { session_id })
            .await
            .unwrap();
        actor.await.unwrap().unwrap();
        // The terminal event is recorded after the attempt turn_completed, so it
        // can still be queued once the loop above has seen its last completion.
        while let Ok(event) = event_rx.try_recv() {
            if let SessionEventKind::ProviderEvent { kind, payload, .. } = &event.kind
                && kind == "network_retry_exhausted"
            {
                exhausted += 1;
                exhausted_reason = payload["error"].as_str().map(str::to_string);
            }
        }
        assert_eq!(calls.load(Ordering::Acquire), expected_attempts);
        assert_eq!(completed_tools, 1);
        assert_eq!(retry_delays.len(), expected_attempts - 1);
        for (index, delay) in retry_delays.iter().enumerate() {
            assert_eq!(
                *delay,
                (NETWORK_RETRY_INITIAL_DELAY.as_millis() as u64 * (1 << index))
                    .min(NETWORK_RETRY_MAX_DELAY.as_millis() as u64)
            );
        }
        assert_eq!(visible_errors, usize::from(failures >= expected_attempts));
        // The turn stops visibly: one terminal event naming why, and no goal
        // left silently active behind it.
        assert_eq!(
            exhausted, expected_exhausted,
            "a chain that spent its attempts must stop and say so exactly once, \
             and a failure that was never retried must not claim it did"
        );
        if expected_exhausted == 1 {
            assert_eq!(
                exhausted_reason.as_deref(),
                Some(error),
                "the terminal reason is the provider error, not a countdown"
            );
        }

        let store = PostgresSessionStore::connect_with_pool_size(&scratch.url, 2)
            .await
            .unwrap();
        if failures > 10 {
            // Handed back rather than left active: the reported bug was a goal
            // listed active for hours while its session only counted down.
            assert_eq!(
                store.state(session_id).await.unwrap().goal.unwrap().status,
                GoalStatus::Blocked
            );
        }
        assert_eq!(
            store
                .action(session_id, message_id)
                .await
                .unwrap()
                .unwrap()
                .state,
            if failures >= expected_attempts {
                crate::SessionActionState::Failed
            } else {
                crate::SessionActionState::Completed
            }
        );
        scratch.discard().await;
    }
}

struct MonitorWakeExecutor {
    calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl AgentTurnExecutor for MonitorWakeExecutor {
    async fn execute(
        &self,
        turn: AgentTurn,
        _events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        if self.calls.fetch_add(1, Ordering::AcqRel) == 0 {
            turn.agent_tools
                .call(
                    "watch",
                    serde_json::json!({
                        "command": "printf 'deployment ready\\n'", "label": "Deployment"
                    }),
                )
                .await?;
        } else {
            assert!(
                turn.prompt.contains("Watcher event: Deployment"),
                "{}",
                turn.prompt
            );
            assert!(turn.prompt.contains("deployment ready"), "{}", turn.prompt);
        }
        Ok(AgentTurnResult {
            provider_session_id: Some("monitored".into()),
            final_text: "watching".into(),
        })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn monitor_event_wakes_an_idle_session_without_an_active_goal() {
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let message_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(128);
    let calls = Arc::new(AtomicUsize::new(0));
    let executor = Arc::new(MonitorWakeExecutor {
        calls: Arc::clone(&calls),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn({
        let journal_path = root.path().join("session.lock");
        let cwd = root.path().to_path_buf();
        async move {
            run_session_actor(
                &journal_path,
                session_id,
                LaunchSession {
                    request_id: message_id,
                    cwd,
                    provider: CodingProvider::Codex,
                    model: None,
                    effort: None,
                    fast: Some(false),
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::FullAccess,
                    name: None,
                    initial_prompt: Some("finish this task".to_string()),
                    capabilities: Default::default(),
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                executor,
                actor_store,
            )
            .await
        }
    });

    let mut completions = 0;
    while completions < 2 {
        let event = tokio::time::timeout(Duration::from_secs(5), event_rx.recv())
            .await
            .expect("watch notification wakes idle agent")
            .expect("session remains attached");
        if matches!(event.kind, SessionEventKind::TurnCompleted { .. }) {
            completions += 1;
        }
    }

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();
    assert_eq!(calls.load(Ordering::Acquire), 2);

    let store = PostgresSessionStore::connect_with_pool_size(&scratch.url, 2)
        .await
        .unwrap();
    assert_eq!(
        store
            .action(session_id, message_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        crate::SessionActionState::Completed
    );
    scratch.discard().await;
}

#[tokio::test(flavor = "current_thread")]
async fn escape_cancels_connection_retry_without_losing_the_prompt() {
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let message_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(128);
    let calls = Arc::new(AtomicUsize::new(0));
    let executor = Arc::new(NetworkThenSuccessExecutor {
        calls: Arc::clone(&calls),
        error: "Codex subscription authentication lookup unavailable",
        failures: 3,
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn({
        let journal_path = root.path().join("session.lock");
        let cwd = root.path().to_path_buf();
        async move {
            run_session_actor(
                &journal_path,
                session_id,
                LaunchSession {
                    request_id: message_id,
                    cwd,
                    provider: CodingProvider::Codex,
                    model: None,
                    effort: None,
                    fast: Some(false),
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::Manual,
                    name: None,
                    initial_prompt: Some("finish this task".to_string()),
                    capabilities: Default::default(),
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                executor,
                actor_store,
            )
            .await
        }
    });

    loop {
        let event = tokio::time::timeout(Duration::from_secs(2), event_rx.recv())
            .await
            .unwrap()
            .unwrap();
        match event.kind {
            SessionEventKind::ProviderEvent { kind, .. } if kind == "network_retry" => {
                command_tx
                    .send(HostCommand::Interrupt { session_id })
                    .await
                    .unwrap();
            }
            SessionEventKind::StatusChanged {
                status: SessionStatus::Ready,
                detail: Some(detail),
            } if detail.contains("Reconnection cancelled") => break,
            _ => {}
        }
    }

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();
    assert_eq!(calls.load(Ordering::Acquire), 1);

    let store = PostgresSessionStore::connect_with_pool_size(&scratch.url, 2)
        .await
        .unwrap();
    assert_eq!(
        store
            .action(session_id, message_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        crate::SessionActionState::Failed
    );
    scratch.discard().await;
}

#[tokio::test]
async fn imported_conversation_is_atomic_and_replays_both_sides_without_live_provider_state() {
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    let mut events = vec![
        SessionEvent::new(id, 0, SessionEventKind::SessionStarted),
        SessionEvent::new(
            id,
            0,
            SessionEventKind::TurnStarted {
                message_id,
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: false,
            },
        ),
    ];
    for (actor, text) in [
        (EventActor::User, "Imported question"),
        (EventActor::Assistant, "Imported answer"),
    ] {
        events.push(SessionEvent::new(
            id,
            0,
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor,
                text: text.into(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        ));
    }
    events.push(SessionEvent::new(
        id,
        0,
        SessionEventKind::TurnCompleted {
            message_id,
            provider_session_id: None,
            final_text: String::new(),
            error: None,
        },
    ));
    let mut invalid = events.clone();
    invalid.push(events[0].clone());
    assert!(store.import_session_events(id, invalid).await.is_err());
    assert!(store.list_sessions(10).await.unwrap().is_empty());
    assert!(
        store
            .import_session_events(id, events.clone())
            .await
            .unwrap()
    );
    assert!(!store.import_session_events(id, events).await.unwrap());
    let history = store.read(id).await.unwrap();
    let replay = native_conversation(&history, CodingProvider::Codex).unwrap();
    let serialized = serde_json::to_string(&replay).unwrap();
    assert!(serialized.contains("Imported question"));
    assert!(serialized.contains("Imported answer"));
    assert_eq!(replay.len(), 2);
    assert!(store.state(id).await.unwrap().provider_session_id.is_none());
    scratch.discard().await;
}

struct RelayPromptExecutor {
    seen: RecordedPromptTurns,
    called: Arc<Notify>,
}

#[async_trait::async_trait]
impl AgentTurnExecutor for RelayPromptExecutor {
    async fn execute(
        &self,
        turn: AgentTurn,
        _events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        self.seen
            .lock()
            .unwrap()
            .push((turn.prompt, turn.attachments));
        self.called.notify_one();
        Ok(AgentTurnResult {
            provider_session_id: Some("provider-session".to_string()),
            final_text: "acknowledged".to_string(),
        })
    }
}

/// An authenticated relay delivery imported by `sync_relay_inbox` must reach
/// the session actor through the same durable workspace inbox as a local team
/// message. Provenance is the property under test: the imported peer message
/// becomes `EventActor::System` provider input, never a human prompt left
/// editable and recallable in the transcript.
#[tokio::test]
async fn imported_relay_message_wakes_the_actor_as_system_provenance() {
    let root = tempdir().unwrap();
    let root_path = root.path().to_path_buf();
    let session_id = Uuid::new_v4();
    let writer =
        SessionWriterLease::acquire(root.path().join(format!("{session_id}.lock"))).unwrap();
    let (scratch, postgres) = crate::session_store::postgres::testing::session_store().await;
    let postgres = Arc::new(postgres);
    postgres.create_session(session_id).await.unwrap();
    let binding = postgres
        .workspace_binding(session_id)
        .await
        .unwrap()
        .expect("a durable session is bound to its workspace participant");
    let workspace = postgres
        .workspace_store()
        .await
        .unwrap()
        .expect("session store exposes the canonical workspace projection");
    let human_id = crate::local_human_participant_id("Human");
    workspace
        .ensure_execution_workspace(
            binding.workspace_id,
            "relay inbox test",
            human_id,
            "Human",
            binding.participant_id,
            "Agent",
        )
        .await
        .unwrap();

    // The peer lives on another host: its workspace and participant identities
    // are cloud-side and unknown locally until the pull+ack inbox imports them.
    let peer_participant_id = Uuid::new_v4();
    let relay_workspace_id = Uuid::new_v4();
    let relay_message_id = Uuid::new_v4();
    let imported = workspace
        .import_relay_message(
            crate::WorkspaceMessage {
                id: relay_message_id,
                workspace_id: relay_workspace_id,
                thread_id: None,
                reply_to_message_id: None,
                author_id: peer_participant_id,
                body: crate::WorkspaceMessageBody {
                    text: "the benchmark rerun finished".to_string(),
                    mentions: Vec::new(),
                    attachments: Vec::new(),
                },
                audience: crate::Audience::Direct {
                    participant: binding.participant_id,
                },
                created_at: chrono::Utc::now(),
            },
            "peer-instance",
            binding.participant_id,
            crate::DeliveryMode::Wake,
        )
        .await
        .unwrap();
    assert_eq!(imported.id, relay_message_id);
    assert_eq!(
        imported.idempotency_key,
        format!("relay-message:{relay_message_id}")
    );

    let store: Arc<dyn SessionStore> = postgres.clone();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(256);
    let seen: RecordedPromptTurns = Arc::new(Mutex::new(Vec::new()));
    let called = Arc::new(Notify::new());
    let executor = Arc::new(RelayPromptExecutor {
        seen: Arc::clone(&seen),
        called: Arc::clone(&called),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn({
        let root_path = root_path.clone();
        async move {
            run_agent_session_with_store_and_writer(
                &root_path,
                session_id,
                LaunchSession {
                    request_id: Uuid::new_v4(),
                    cwd: root_path.clone(),
                    provider: CodingProvider::Codex,
                    model: None,
                    effort: None,
                    fast: Some(false),
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::Manual,
                    name: None,
                    initial_prompt: None,
                    capabilities: crate::SessionCapabilities {
                        multiplayer: true,
                        subagents: true,
                        ..Default::default()
                    },
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                executor,
                actor_store,
                writer,
            )
            .await
        }
    });

    // The actor owns its own coordinator, so the durable root inbox tick is
    // what discovers the imported delivery. No host command is involved.
    tokio::time::timeout(Duration::from_secs(5), called.notified())
        .await
        .expect("the imported relay message reaches the provider");

    let mut observed_statuses = Vec::new();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(5), event_rx.recv())
            .await
            .expect("the imported relay turn completes")
            .expect("session remains attached");
        if let SessionEventKind::Message {
            message_id,
            actor,
            status,
            ..
        } = &event.kind
            && *message_id == relay_message_id
        {
            // Provenance must be System at every observed transition.
            assert_eq!(
                *actor,
                EventActor::System,
                "an imported relay message must never be attributed to the human"
            );
            observed_statuses.push(*status);
            if *status == MessageStatus::Complete {
                break;
            }
        }
    }
    assert_eq!(observed_statuses.last(), Some(&MessageStatus::Complete));

    // A blanket human recall must not retract already-admitted peer input.
    command_tx
        .send(HostCommand::RecallQueuedPrompt {
            session_id,
            message_id: None,
        })
        .await
        .unwrap();
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();

    let history = store.read(session_id).await.unwrap();
    let relay_messages = history
        .iter()
        .filter_map(|event| match &event.kind {
            SessionEventKind::Message {
                message_id,
                actor,
                status,
                text,
                ..
            } if *message_id == relay_message_id => Some((*actor, *status, text.clone())),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(
        !relay_messages.is_empty(),
        "the imported relay message is projected into the durable transcript"
    );
    assert!(
        relay_messages
            .iter()
            .all(|(actor, _, _)| *actor == EventActor::System),
        "durable provenance stays System: {relay_messages:?}"
    );
    assert!(
        !relay_messages
            .iter()
            .any(|(_, status, _)| *status == MessageStatus::Queued),
        "an imported peer message is never left as human pending input: {relay_messages:?}"
    );
    assert!(
        !history.iter().any(|event| matches!(
            &event.kind,
            SessionEventKind::PromptRecalled { message_id, .. } if *message_id == relay_message_id
        )),
        "a human recall must not retract imported peer input"
    );

    // Attribution survives the import: the provider sees who spoke.
    let prompts = seen.lock().unwrap().clone();
    assert!(
        prompts
            .iter()
            .any(|(prompt, _)| prompt.contains("the benchmark rerun finished")),
        "the peer message body reaches the provider: {prompts:?}"
    );
    assert!(
        prompts
            .iter()
            .any(|(prompt, _)| prompt.contains("peer-instance")),
        "imported author attribution survives the relay import: {prompts:?}"
    );
    scratch.discard().await;
}

#[tokio::test]
async fn expired_connection_retry_is_not_starved_by_busy_maintenance() {
    let deadline = Instant::now();
    tokio::time::sleep(Duration::from_millis(5)).await;
    tokio::time::timeout(Duration::from_millis(100), async {
        loop {
            tokio::select! {
                biased;
                _ = wait_for_retry_deadline(Some(deadline)) => break,
                _ = std::future::ready(()) => {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            }
        }
    })
    .await
    .expect("expired retry must beat continuously ready maintenance");
}

#[tokio::test]
async fn shared_steer_batch_retries_and_targeted_recall_preserve_siblings() {
    let (control_tx, mut controls) = mpsc::channel(16);
    let (result_tx, _results) = mpsc::channel(16);
    let mut pending = VecDeque::new();
    for text in ["first", "second"] {
        pending.push_back(PendingSteer {
            prompt: QueuedPrompt {
                message_id: Uuid::new_v4(),
                text: text.into(),
                actor: EventActor::User,
                attachments: vec![PathBuf::from(format!("{text}.png"))],
                output_schema: None,
                delivery: PromptDelivery::Steer,
                visible: true,
                interrupt_batch: true,
                batch: Vec::new(),
            },
            acknowledgement_id: Uuid::new_v4(),
            admission: SteerAdmission::pending(),
            state: PendingSteerState::RetryAtBoundary {
                error: "compacting".into(),
            },
            attempt_boundary: 0,
        });
    }
    retry_pending_steers(&control_tx, &result_tx, &mut pending, 1, false).await;
    let AgentTurnControl::Steer {
        text,
        attachments,
        admission: old_admission,
        ack: old_ack,
        ..
    } = controls.recv().await.unwrap()
    else {
        panic!("expected steer")
    };
    assert_eq!(
        text,
        "first

second"
    );
    assert_eq!(
        attachments,
        [PathBuf::from("first.png"), PathBuf::from("second.png")]
    );
    let old_attempt = pending[0].acknowledgement_id;
    assert_eq!(old_attempt, pending[1].acknowledgement_id);
    retry_pending_steers(&control_tx, &result_tx, &mut pending, 2, false).await;
    assert!(
        controls.try_recv().is_err(),
        "a shared attempt must not revoke itself"
    );
    let mut third = pending[1].prompt.clone();
    third.message_id = Uuid::new_v4();
    third.text = "third".into();
    third.attachments.clear();
    pending.push_back(PendingSteer {
        prompt: third,
        acknowledgement_id: Uuid::new_v4(),
        admission: SteerAdmission::pending(),
        state: PendingSteerState::RetryAtBoundary {
            error: "new input".into(),
        },
        attempt_boundary: 2,
    });
    retry_pending_steers(&control_tx, &result_tx, &mut pending, 2, false).await;
    assert!(!old_admission.accept());
    let _ = old_ack.send(Err("rebatched".into()));
    let AgentTurnControl::Steer {
        text,
        admission: old_admission,
        ack: old_ack,
        ..
    } = controls.recv().await.unwrap()
    else {
        panic!("expected expanded batch")
    };
    assert_eq!(
        text,
        "first

second

third"
    );
    let old_attempt = pending[0].acknowledgement_id;
    assert!(
        pending
            .iter()
            .all(|steer| steer.acknowledgement_id == old_attempt)
    );
    let target = pending[0].prompt.message_id;
    let sibling = pending[1].prompt.message_id;
    let recalled = recall_withdrawable_steers(&mut pending, Some(target));
    assert_eq!(recalled.len(), 1);
    assert_eq!(recalled[0].message_id, target);
    assert!(!old_admission.accept(), "recalled batch cannot be admitted");
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].prompt.message_id, sibling);
    assert_ne!(pending[0].acknowledgement_id, old_attempt);
    let _ = old_ack.send(Err("withdrawn".into()));
    retry_pending_steers(&control_tx, &result_tx, &mut pending, 3, false).await;
    let AgentTurnControl::Steer {
        text,
        attachments,
        admission,
        ..
    } = controls.recv().await.unwrap()
    else {
        panic!("expected sibling retry")
    };
    assert_eq!(
        text,
        "second

third"
    );
    assert_eq!(attachments, [PathBuf::from("second.png")]);
    assert!(admission.accept());
    assert!(recall_withdrawable_steers(&mut pending, None).is_empty());
}

// Turn watchdog regressions.
//
// The clock and budget semantics below are covered as unit tests on
// `TurnWatchdog` on purpose: a suspended host, a backwards wall clock, and a
// two-hour tool budget cannot be produced deterministically (or quickly)
// through the session actor, and driving them through a fake provider would
// only re-test the loop wiring that the two integration tests below already
// cover. The integration tests are reserved for behaviour that lives in the
// loop itself: publishing a visible stall status, and suspending the watchdog
// while a human owns the turn.

#[tokio::test(flavor = "current_thread")]
async fn a_suspended_host_gets_one_bounded_reconnection_grace() {
    for phase in [TurnPhase::AwaitingProvider, TurnPhase::Active] {
        for monotonic_includes_sleep in [false, true] {
            let mut watchdog = TurnWatchdog::new(phase);
            let sleep = Duration::from_secs(6 * 60 * 60);
            watchdog.last_output_wall = std::time::SystemTime::now() - sleep;
            watchdog.last_poll_wall = watchdog.last_output_wall;
            if monotonic_includes_sleep {
                watchdog.last_poll_mono = tokio::time::Instant::now() - sleep;
            }
            let before = tokio::time::Instant::now();
            assert!(
                matches!(watchdog.verdict(), WatchdogVerdict::Stalling(detail)
                if detail.contains("reconnection"))
            );
            let deadline = watchdog.wake_grace_until.unwrap();
            assert!(deadline >= before + TURN_WATCHDOG_WAKE_GRACE);
            assert!(deadline <= tokio::time::Instant::now() + TURN_WATCHDOG_WAKE_GRACE);
            for _ in 0..3 {
                assert_eq!(watchdog.verdict(), WatchdogVerdict::Healthy);
                assert_eq!(watchdog.wake_grace_until, Some(deadline));
            }
            // Expiring the grace does not erase the pre-sleep silence budget.
            watchdog.wake_grace_until = Some(tokio::time::Instant::now());
            assert!(matches!(watchdog.verdict(), WatchdogVerdict::Expired(_)));
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn progress_and_cancellation_end_reconnection_grace() {
    let mut watchdog = TurnWatchdog::new(TurnPhase::Active);
    watchdog.last_poll_wall = std::time::SystemTime::now() - Duration::from_secs(3600);
    assert!(matches!(watchdog.verdict(), WatchdogVerdict::Stalling(_)));
    assert!(watchdog.note_output());
    assert!(watchdog.wake_grace_until.is_none());
    assert_eq!(watchdog.verdict(), WatchdogVerdict::Healthy);

    for phase in [TurnPhase::Cancelling, TurnPhase::Draining] {
        watchdog.set_phase(phase);
        watchdog.last_poll_wall = std::time::SystemTime::now() - Duration::from_secs(3600);
        watchdog.last_output_wall = watchdog.last_poll_wall;
        assert!(matches!(watchdog.verdict(), WatchdogVerdict::Expired(_)));
        assert!(watchdog.wake_grace_until.is_none());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn quiet_model_escalates_visibility_without_failing_at_five_minutes() {
    let mut watchdog = TurnWatchdog::new(TurnPhase::Active);
    watchdog.stall_timeout = Some(Duration::from_secs(20 * 60));
    watchdog.last_output_wall = std::time::SystemTime::now() - TURN_WATCHDOG_WARNING_AFTER;
    assert!(
        matches!(watchdog.verdict(), WatchdogVerdict::Stalling(detail)
        if detail.contains("waiting for provider") && !detail.contains("possibly stalled"))
    );
    assert_eq!(watchdog.verdict(), WatchdogVerdict::Healthy);

    watchdog.last_output_wall = std::time::SystemTime::now() - TURN_WATCHDOG_SUSPECT_AFTER;
    assert!(
        matches!(watchdog.verdict(), WatchdogVerdict::Stalling(detail)
        if detail.contains("possibly stalled"))
    );
    watchdog.last_output_wall = std::time::SystemTime::now() - Duration::from_secs(19 * 60);
    assert_eq!(watchdog.verdict(), WatchdogVerdict::Healthy);
    watchdog.last_output_wall = std::time::SystemTime::now() - Duration::from_secs(20 * 60);
    assert!(matches!(watchdog.verdict(), WatchdogVerdict::Expired(_)));
    assert!(watchdog.note_output());
    assert!(!watchdog.suspected);
    assert_eq!(watchdog.verdict(), WatchdogVerdict::Healthy);
}

#[tokio::test(flavor = "current_thread")]
async fn a_backwards_wall_clock_cannot_hide_a_stall() {
    let mut watchdog = TurnWatchdog::new(TurnPhase::Active);
    watchdog.last_output_wall = std::time::SystemTime::now() + Duration::from_secs(60 * 60);
    watchdog.last_output_mono =
        tokio::time::Instant::now() - PROVIDER_ACTIVE_LIVENESS_TIMEOUT - Duration::from_secs(1);
    assert!(matches!(watchdog.verdict(), WatchdogVerdict::Expired(_)));
}

#[tokio::test(flavor = "current_thread")]
async fn an_in_flight_tool_call_extends_the_active_budget_without_removing_it() {
    let mut watchdog = TurnWatchdog::new(TurnPhase::Active);
    assert_eq!(watchdog.budget(), Some(PROVIDER_ACTIVE_LIVENESS_TIMEOUT));

    watchdog.observe(
        &SessionEventKind::ToolStarted {
            tool_call_id: "tool-1".to_string(),
            name: "bash".to_string(),
            input: json!({ "command": "cargo test" }),
            input_ref: None,
        },
        true,
    );
    assert_eq!(
        watchdog.budget(),
        Some(PROVIDER_ACTIVE_TOOL_LIVENESS_TIMEOUT),
        "a legitimately long tool call must not be killed at the stall budget"
    );

    // The larger budget is still a budget: a tool that died with the host
    // terminalizes rather than pinning the turn forever.
    watchdog.last_output_wall =
        std::time::SystemTime::now() - PROVIDER_ACTIVE_TOOL_LIVENESS_TIMEOUT;
    assert!(matches!(
        watchdog.verdict(),
        WatchdogVerdict::Expired(error) if error.contains("1 tool call(s) still running")
    ));

    watchdog.observe(
        &SessionEventKind::ToolCompleted {
            tool_call_id: "tool-1".to_string(),
            output: "done".to_string(),
            output_ref: None,
            is_error: false,
            input: None,
            input_ref: None,
        },
        true,
    );
    assert_eq!(watchdog.budget(), Some(PROVIDER_ACTIVE_LIVENESS_TIMEOUT));
}

#[tokio::test(flavor = "current_thread")]
async fn bookkeeping_traffic_cannot_keep_a_zombie_turn_alive() {
    let usage = SessionEventKind::UsageUpdated {
        provider_duration_ms: 1,
        turn_id: None,
        provider_context_reused: None,
        input_tokens: 10,
        output_tokens: 0,
        cached_input_tokens: 0,
        cache_creation_input_tokens: 0,
        total_tokens: 10,
        cost_microusd: None,
        cost_basis: String::new(),
        cost_usd: None,
        context_tokens: Some(10),
        context_window_tokens: Some(100),
    };
    assert!(!provider_event_is_progress(&usage));
    assert!(!provider_event_is_progress(
        &SessionEventKind::ContextWindowUpdated {
            context_tokens: 10,
            context_window_tokens: 100,
        }
    ));
    assert!(provider_event_is_progress(
        &SessionEventKind::ReasoningDelta {
            text: "thinking".to_string(),
        }
    ));

    let mut watchdog = TurnWatchdog::new(TurnPhase::Active);
    watchdog.last_output_wall = std::time::SystemTime::now() - PROVIDER_ACTIVE_LIVENESS_TIMEOUT;
    assert!(
        !watchdog.observe(&usage, provider_event_is_progress(&usage)),
        "usage telemetry is not progress"
    );
    assert!(
        matches!(watchdog.verdict(), WatchdogVerdict::Expired(_)),
        "a provider that only reports token counts is still stalled"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn provider_metadata_events_cannot_extend_the_liveness_timer() {
    let metadata = |kind: &str| SessionEventKind::ProviderEvent {
        provider: CodingProvider::Codex,
        kind: kind.to_string(),
        payload: json!({}),
    };
    for kind in [
        "network_retry",
        "network_recovered",
        "provider_retry",
        "background_task_live",
        "context_replay_projected",
        "action/generation_status",
    ] {
        assert!(
            !provider_event_is_progress(&metadata(kind)),
            "{kind} is provider metadata, not progress"
        );
    }
    // Streaming fragments and a real compaction boundary still count.
    assert!(provider_event_is_progress(&metadata("action/input_delta")));
    assert!(provider_event_is_progress(&metadata("action/preparing")));
    assert!(provider_event_is_progress(
        &SessionEventKind::ProviderEvent {
            provider: CodingProvider::Codex,
            kind: "context_compaction".to_string(),
            payload: json!({ "status": "started" }),
        }
    ));

    let mut watchdog = TurnWatchdog::new(TurnPhase::Active);
    watchdog.last_output_wall = std::time::SystemTime::now() - PROVIDER_ACTIVE_LIVENESS_TIMEOUT;
    let heartbeat = metadata("background_task_live");
    assert!(!watchdog.observe(&heartbeat, provider_event_is_progress(&heartbeat)));
    assert!(
        matches!(watchdog.verdict(), WatchdogVerdict::Expired(_)),
        "a heartbeat cannot hold a stalled turn open"
    );
}

struct NeverStoppingExecutor;

#[async_trait::async_trait]
impl AgentTurnExecutor for NeverStoppingExecutor {
    async fn execute(
        &self,
        _turn: AgentTurn,
        _events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        std::future::pending().await
    }

    async fn stop_session(&self, _session_id: Uuid) -> Result<()> {
        std::future::pending().await
    }
}

#[tokio::test(flavor = "current_thread")]
async fn unstoppable_provider_cleanup_is_bounded_and_reported() {
    let executor: Arc<dyn AgentTurnExecutor> = Arc::new(NeverStoppingExecutor);
    let reason = tokio::time::timeout(
        TURN_WATCHDOG_STOP_TIMEOUT * 4,
        stop_session_bounded(&executor, Uuid::new_v4(), TURN_WATCHDOG_STOP_TIMEOUT),
    )
    .await
    .expect("cleanup cannot outlive its bound");
    let reason = reason.expect("a cleanup that never finishes is reportable");
    assert!(
        reason.contains("may still be running"),
        "the user learns processes may have survived: {reason}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_disabled_stall_policy_reports_idleness_without_failing_the_turn() {
    let mut watchdog = TurnWatchdog::new(TurnPhase::Active);
    watchdog.stall_timeout = None;
    watchdog.last_output_wall = std::time::SystemTime::now() - Duration::from_secs(24 * 60 * 60);
    assert!(matches!(watchdog.verdict(), WatchdogVerdict::Stalling(_)));
    assert!(
        matches!(watchdog.verdict(), WatchdogVerdict::Healthy),
        "a stall is reported once per episode, not once per tick"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn provider_output_closes_a_reported_stall_episode_once() {
    let mut watchdog = TurnWatchdog::new(TurnPhase::Active);
    watchdog.last_output_wall = std::time::SystemTime::now() - PROVIDER_ACTIVE_LIVENESS_TIMEOUT / 2;
    assert!(matches!(watchdog.verdict(), WatchdogVerdict::Stalling(_)));
    assert!(
        watchdog.note_output(),
        "recovering from a reported stall must be publishable"
    );
    assert!(
        !watchdog.note_output(),
        "an unreported stall must not publish a recovery"
    );
}

/// A provider that starts normally and then goes silent: the shape of a worker
/// whose host went to sleep mid-turn.
struct ActiveThenSilentExecutor;

#[async_trait::async_trait]
impl AgentTurnExecutor for ActiveThenSilentExecutor {
    async fn execute(
        &self,
        _turn: AgentTurn,
        events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        events
            .send(SessionEventKind::ReasoningDelta {
                text: "thinking".to_string(),
            })
            .await
            .unwrap();
        std::future::pending().await
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_provider_stall_is_visible_before_the_turn_fails() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let message_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(64);
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn({
        let journal_path = journal_path.clone();
        let cwd = root.path().to_path_buf();
        async move {
            run_session_actor(
                &journal_path,
                session_id,
                LaunchSession {
                    request_id: message_id,
                    cwd,
                    provider: CodingProvider::Codex,
                    model: None,
                    effort: None,
                    fast: Some(false),
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::Manual,
                    name: None,
                    initial_prompt: Some("hang".to_string()),
                    capabilities: Default::default(),
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                Arc::new(ActiveThenSilentExecutor),
                actor_store,
            )
            .await
        }
    });

    let mut observed = Vec::new();
    loop {
        let event = tokio::time::timeout(PROVIDER_ACTIVE_LIVENESS_TIMEOUT * 3, event_rx.recv())
            .await
            .expect("the watchdog terminalizes a stalled turn")
            .expect("actor remains attached");
        let ready = matches!(
            event.kind,
            SessionEventKind::StatusChanged {
                status: SessionStatus::Ready,
                ..
            }
        );
        observed.push(event.kind);
        if ready {
            break;
        }
    }
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();

    let stalled = observed
        .iter()
        .position(|kind| {
            matches!(
                kind,
                SessionEventKind::StatusChanged {
                    status: SessionStatus::Running,
                    detail: Some(detail),
                } if detail.contains("no provider output")
                    && detail.contains("the model has not responded")
            )
        })
        .expect("a silent worker is reported before it is failed");
    let failed = observed
        .iter()
        .position(|kind| {
            matches!(
                kind,
                SessionEventKind::TurnCompleted {
                    error: Some(error),
                    ..
                } if error.contains("liveness timeout")
            )
        })
        .expect("the stalled turn reaches a terminal boundary");
    assert!(
        stalled < failed,
        "the stall status must precede the terminal failure"
    );
    scratch.discard().await;
}

struct InteractionThenHungExecutor;

#[async_trait::async_trait]
impl AgentTurnExecutor for InteractionThenHungExecutor {
    async fn execute(
        &self,
        _turn: AgentTurn,
        events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        events
            .send(SessionEventKind::ProviderInteractionRequested {
                interaction_id: "interaction-1".to_string(),
                kind: "question".to_string(),
                title: "Which branch?".to_string(),
                detail: "The provider needs a human answer".to_string(),
                payload: json!({}),
            })
            .await
            .unwrap();
        std::future::pending().await
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_pending_provider_interaction_suspends_the_watchdog() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let message_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(64);
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn({
        let journal_path = journal_path.clone();
        let cwd = root.path().to_path_buf();
        async move {
            run_session_actor(
                &journal_path,
                session_id,
                LaunchSession {
                    request_id: message_id,
                    cwd,
                    provider: CodingProvider::Codex,
                    model: None,
                    effort: None,
                    fast: Some(false),
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::Manual,
                    name: None,
                    initial_prompt: Some("ask me".to_string()),
                    capabilities: Default::default(),
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                Arc::new(InteractionThenHungExecutor),
                actor_store,
            )
            .await
        }
    });

    let mut observed = Vec::new();
    let deadline =
        tokio::time::Instant::now() + PROVIDER_ACTIVE_LIVENESS_TIMEOUT + Duration::from_secs(1);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, event_rx.recv()).await {
            Ok(Some(event)) => observed.push(event.kind),
            Ok(None) => panic!("actor detached while a human owned the turn"),
            Err(_) => break,
        }
    }
    assert!(
        observed
            .iter()
            .any(|kind| matches!(kind, SessionEventKind::ProviderInteractionRequested { .. })),
        "the provider asked the human a question"
    );
    assert!(
        !observed
            .iter()
            .any(|kind| matches!(kind, SessionEventKind::TurnCompleted { error: Some(_), .. })),
        "a human sitting on a provider question is not a stalled worker"
    );

    // Answering hands the turn back to the provider, which re-arms the
    // watchdog so a worker that stays silent still terminalizes.
    command_tx
        .send(HostCommand::RespondToProviderInteraction {
            session_id,
            interaction_id: "interaction-1".to_string(),
            response: json!("main"),
        })
        .await
        .unwrap();
    let failed = tokio::time::timeout(PROVIDER_ACTIVE_LIVENESS_TIMEOUT * 3, async {
        loop {
            let Some(event) = event_rx.recv().await else {
                panic!("actor detached before terminalizing the stalled turn");
            };
            if matches!(
                &event.kind,
                SessionEventKind::TurnCompleted { error: Some(error), .. }
                    if error.contains("liveness timeout")
            ) {
                break;
            }
        }
    })
    .await;
    assert!(
        failed.is_ok(),
        "the watchdog resumes once the human answers"
    );

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();
    scratch.discard().await;
}

#[test]
fn retry_prompts_carry_prior_checkpoints_without_repeating_completed_work() {
    let checkpoint = |key: &str, state: serde_json::Value| crate::AutonomyCheckpoint {
        checkpoint_id: Uuid::new_v4(),
        job_id: Uuid::new_v4(),
        checkpoint_key: key.to_string(),
        session_id: None,
        goal_id: None,
        kind: "tool-result".to_string(),
        state,
        evidence: serde_json::json!({}),
        content_hash: "hash".to_string(),
        created_at: chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
    };
    let none = autonomy_retry_prompt("Deploy the service".to_string(), 2, &[]);
    assert!(none.starts_with("Deploy the service"));
    assert!(none.contains("attempt 2"), "{none}");
    assert!(none.contains("no checkpoints"), "{none}");

    let with = autonomy_retry_prompt(
        "Deploy the service".to_string(),
        3,
        &[
            checkpoint("build", serde_json::json!({"artifact": "svc-1.2.3"})),
            checkpoint("upload", serde_json::json!({"huge": "x".repeat(5_000)})),
        ],
    );
    assert!(with.contains("attempt 3"), "{with}");
    assert!(
        with.contains(
            "- build [tool-result] at 2023-11-14T22:13:20+00:00: {\"artifact\":\"svc-1.2.3\"}"
        ),
        "{with}"
    );
    assert!(with.contains("- upload [tool-result]"), "{with}");
    assert!(
        with.contains("…"),
        "long checkpoint state is truncated: {with}"
    );
    assert!(with.len() < 3_000, "{}", with.len());
}

/// Journals a subscription turn in the exact order the runtime writes it:
/// prompt admission (`InProgress`) before `TurnStarted`, the assistant reply,
/// then the prompt's terminal status, then `TurnCompleted`. An interrupted
/// turn writes `Failed` before its `TurnCompleted`.
fn subscription_turn_events(
    session_id: Uuid,
    sequence: &mut u64,
    prompt: &str,
    reply: Option<&str>,
) -> Vec<SessionEvent> {
    let message_id = Uuid::new_v4();
    let mut next = || {
        *sequence += 1;
        *sequence
    };
    let user = |status: MessageStatus, delivery: PromptDelivery| SessionEventKind::Message {
        message_id,
        actor: EventActor::User,
        text: prompt.to_string(),
        attachments: Vec::new(),
        status,
        delivery: Some(delivery),
    };
    let mut events = vec![
        SessionEvent::new(
            session_id,
            next(),
            user(MessageStatus::Queued, PromptDelivery::Steer),
        ),
        SessionEvent::new(
            session_id,
            next(),
            user(MessageStatus::InProgress, PromptDelivery::Queue),
        ),
        SessionEvent::new(
            session_id,
            next(),
            SessionEventKind::TurnStarted {
                message_id,
                provider: CodingProvider::Claude,
                model: Some("claude-fable-5-1".to_string()),
                effort: Some("medium".to_string()),
                fast: false,
            },
        ),
    ];
    match reply {
        Some(reply) => {
            events.push(SessionEvent::new(
                session_id,
                next(),
                SessionEventKind::Message {
                    message_id: Uuid::new_v4(),
                    actor: EventActor::Assistant,
                    text: reply.to_string(),
                    attachments: Vec::new(),
                    status: MessageStatus::Complete,
                    delivery: None,
                },
            ));
            events.push(SessionEvent::new(
                session_id,
                next(),
                user(MessageStatus::Complete, PromptDelivery::Queue),
            ));
            events.push(SessionEvent::new(
                session_id,
                next(),
                SessionEventKind::TurnCompleted {
                    message_id,
                    provider_session_id: None,
                    final_text: reply.to_string(),
                    error: None,
                },
            ));
        }
        None => {
            events.push(SessionEvent::new(
                session_id,
                next(),
                user(MessageStatus::Failed, PromptDelivery::Queue),
            ));
            events.push(SessionEvent::new(
                session_id,
                next(),
                SessionEventKind::TurnCompleted {
                    message_id,
                    provider_session_id: None,
                    final_text: String::new(),
                    error: Some("turn interrupted".to_string()),
                },
            ));
        }
    }
    events
}

#[test]
fn subscription_prompts_precede_their_replies_and_survive_interrupts() {
    use borg_provider::provider::ModelMessage;

    let session_id = Uuid::new_v4();
    let mut sequence = 0;
    let mut events = subscription_turn_events(
        session_id,
        &mut sequence,
        "first request",
        Some("first reply"),
    );
    events.extend(subscription_turn_events(
        session_id,
        &mut sequence,
        "this still looks wrong [Image 1]",
        None,
    ));
    events.extend(subscription_turn_events(
        session_id,
        &mut sequence,
        "the ui",
        Some("third reply"),
    ));

    assert_eq!(
        native_conversation(&events, CodingProvider::Claude).unwrap(),
        vec![
            ModelMessage::user("first request"),
            ModelMessage::assistant(Some("first reply".to_string()), None, None, Vec::new()),
            ModelMessage::user("this still looks wrong [Image 1]"),
            ModelMessage::user("the ui"),
            ModelMessage::assistant(Some("third reply".to_string()), None, None, Vec::new()),
        ]
    );
}

/// Manual probe: run the canonical projection over a real journal dump.
/// `BORG_PROBE_EVENTS=/path/to/events.jsonl cargo test -p borg-agent-runtime --lib -- probe_real_journal_projection --ignored --nocapture`
#[test]
#[ignore]
fn probe_real_journal_projection() {
    use borg_provider::provider::ModelMessage;
    let Ok(path) = std::env::var("BORG_PROBE_EVENTS") else {
        return;
    };
    let text = std::fs::read_to_string(&path).expect("read journal dump");
    let events = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str::<SessionEvent>(line).expect("journal event parses"))
        .collect::<Vec<_>>();
    eprintln!("PROBE events={}", events.len());
    let conversation = native_conversation(&events, CodingProvider::Claude).unwrap();
    eprintln!("PROBE projected messages={}", conversation.len());
    let tail = conversation.len().saturating_sub(16);
    let all = std::env::var("BORG_PROBE_ALL").is_ok();
    for (index, message) in conversation.iter().enumerate() {
        let is_dialogue = matches!(message, ModelMessage::User { .. })
            || matches!(message, ModelMessage::Assistant { tool_calls, content: Some(text), .. } if tool_calls.is_empty() && !text.trim().is_empty());
        if !(index >= tail || (all && is_dialogue)) {
            continue;
        }
        let (role, body) = match message {
            ModelMessage::User { content, .. } => ("user", content.as_str()),
            ModelMessage::System { content } => ("system", content.as_str()),
            ModelMessage::Assistant {
                content,
                tool_calls,
                ..
            } => (
                if tool_calls.is_empty() {
                    "assistant"
                } else {
                    "assistant+tools"
                },
                content.as_deref().unwrap_or(""),
            ),
            ModelMessage::Tool { content, .. } => ("tool", content.as_str()),
        };
        let preview = body.chars().take(96).collect::<String>().replace('\n', " ");
        eprintln!("PROBE {index:5} {role:16} {preview}");
    }
}

#[test]
fn interrupted_subscription_turn_keeps_its_delivered_output() {
    use borg_provider::provider::ModelMessage;

    let session_id = Uuid::new_v4();
    let interrupted = Uuid::new_v4();
    let next = Uuid::new_v4();
    let turn_started = |message_id| SessionEventKind::TurnStarted {
        message_id,
        provider: CodingProvider::Claude,
        model: Some("claude-fable-5-1".to_string()),
        effort: Some("medium".to_string()),
        fast: false,
    };
    let user = |message_id, text: &str, status, delivery| SessionEventKind::Message {
        message_id,
        actor: EventActor::User,
        text: text.to_string(),
        attachments: Vec::new(),
        status,
        delivery: Some(delivery),
    };
    // The real runtime order for a steer that the human interrupts with
    // Escape after the reply text and a tool call were already on screen.
    let kinds = vec![
        user(
            interrupted,
            "the ui still looks bad",
            MessageStatus::InProgress,
            PromptDelivery::Steer,
        ),
        turn_started(interrupted),
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::Assistant,
            text: "Short answers to both, then the UI.".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
        SessionEventKind::ToolStarted {
            tool_call_id: "toolu_1".to_string(),
            name: "Bash".to_string(),
            input: serde_json::json!({"command": "pgrep -x UnrealEditor"}),
            input_ref: None,
        },
        SessionEventKind::UserStopChanged { engaged: true },
        user(
            interrupted,
            "the ui still looks bad",
            MessageStatus::Failed,
            PromptDelivery::Queue,
        ),
        SessionEventKind::TurnCompleted {
            message_id: interrupted,
            provider_session_id: None,
            final_text: String::new(),
            error: Some("turn interrupted".to_string()),
        },
        user(
            next,
            "why are you replying to my older messages?",
            MessageStatus::InProgress,
            PromptDelivery::Steer,
        ),
        turn_started(next),
    ];
    let events = kinds
        .into_iter()
        .enumerate()
        .map(|(index, kind)| SessionEvent::new(session_id, index as u64 + 1, kind))
        .collect::<Vec<_>>();

    let replay = native_conversation(&events, CodingProvider::Claude).unwrap();
    assert!(
        matches!(&replay[0], ModelMessage::User { content, .. } if content == "the ui still looks bad"),
        "{replay:?}"
    );
    assert!(
        matches!(replay.last(), Some(ModelMessage::User { content, .. }) if content == "why are you replying to my older messages?"),
        "{replay:?}"
    );
    let middle = &replay[1..replay.len() - 1];
    assert!(
        middle.iter().any(|message| matches!(message, ModelMessage::Assistant { content: Some(text), .. } if text == "Short answers to both, then the UI.")),
        "delivered reply dropped: {replay:?}"
    );
    assert!(
        middle.iter().any(|message| matches!(message, ModelMessage::Assistant { tool_calls, .. } if tool_calls.iter().any(|call| call.id == "toolu_1"))),
        "tool call dropped: {replay:?}"
    );
    assert!(
        middle.iter().any(|message| matches!(message, ModelMessage::Tool { tool_call_id, content, .. } if tool_call_id == "toolu_1" && content.contains("outcome unknown"))),
        "dangling tool call left open: {replay:?}"
    );
    assert_eq!(
        replay
            .iter()
            .filter(|message| matches!(message, ModelMessage::User { content, .. } if content == "the ui still looks bad"))
            .count(),
        1,
        "interrupted prompt must appear exactly once: {replay:?}"
    );
}

/// Manual probe: replicate the live turn-start path against a real store.
/// `BORG_PROBE_STORE=postgres://localhost/borg_sessions BORG_PROBE_SESSION=<uuid> \
///  cargo test -p borg-agent-runtime --lib -- probe_live_store_recovery_projection --ignored --nocapture`
#[tokio::test]
#[ignore]
async fn probe_live_store_recovery_projection() {
    let (Ok(path), Ok(session)) = (
        std::env::var("BORG_PROBE_STORE"),
        std::env::var("BORG_PROBE_SESSION"),
    ) else {
        return;
    };
    let session_id: Uuid = session.parse().expect("session uuid");
    let store = PostgresSessionStore::connect_with_pool_size(&path, 2)
        .await
        .expect("open store");
    let recovery = store.recovery(session_id).await.expect("recovery");
    let events = &recovery.context_events;
    eprintln!(
        "PROBE context_events={} first_sequence={:?} last_sequence={:?}",
        events.len(),
        events.first().map(|event| event.sequence),
        events.last().map(|event| event.sequence)
    );
    let mut kinds = std::collections::BTreeMap::<String, usize>::new();
    for event in events {
        let kind = serde_json::to_value(&event.kind)
            .ok()
            .and_then(|value| {
                value
                    .get("type")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .unwrap_or_default();
        *kinds.entry(kind).or_default() += 1;
    }
    eprintln!("PROBE kinds={kinds:?}");
    let context = retained_conversation_context(events).expect("context");
    eprintln!("PROBE context_chars={}", context.chars().count());
    for (index, line) in context.lines().enumerate() {
        for needle in [
            "first i want you to read",
            "Understood — if you didn't",
            "Read the whole window",
            "we absolutely need to fix",
            "Agreed — this is a correctness",
            "Fixed, tested, installed",
            "ok all i was trying",
            "Short answers to both",
            "what the fuck you didnt",
        ] {
            if line.contains(needle) {
                eprintln!("PROBE frame {index:5}: {needle}");
            }
        }
    }
}

#[test]
fn collapsed_recovery_journal_still_places_prompts_before_their_replies() {
    use borg_provider::provider::ModelMessage;

    // The recovery view keeps only a prompt's terminal row, which lands after
    // the reply. The turn boundary must still anchor the prompt correctly.
    let session_id = Uuid::new_v4();
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    let turn_started = |message_id| SessionEventKind::TurnStarted {
        message_id,
        provider: CodingProvider::Claude,
        model: Some("claude-fable-5-1".to_string()),
        effort: Some("medium".to_string()),
        fast: false,
    };
    let message = |message_id, actor, text: &str, status| SessionEventKind::Message {
        message_id,
        actor,
        text: text.to_string(),
        attachments: Vec::new(),
        status,
        delivery: None,
    };
    let kinds = vec![
        turn_started(first),
        message(
            Uuid::new_v4(),
            EventActor::Assistant,
            "first reply",
            MessageStatus::Complete,
        ),
        message(
            first,
            EventActor::User,
            "first request",
            MessageStatus::Complete,
        ),
        SessionEventKind::TurnCompleted {
            message_id: first,
            provider_session_id: None,
            final_text: "first reply".to_string(),
            error: None,
        },
        turn_started(second),
        message(
            Uuid::new_v4(),
            EventActor::Assistant,
            "second reply",
            MessageStatus::Complete,
        ),
        message(
            second,
            EventActor::User,
            "second request",
            MessageStatus::Complete,
        ),
        SessionEventKind::TurnCompleted {
            message_id: second,
            provider_session_id: None,
            final_text: "second reply".to_string(),
            error: None,
        },
    ];
    let events = kinds
        .into_iter()
        .enumerate()
        .map(|(index, kind)| SessionEvent::new(session_id, index as u64 + 1, kind))
        .collect::<Vec<_>>();

    let replay = native_conversation(&events, CodingProvider::Claude).unwrap();
    assert_eq!(
        replay,
        vec![
            ModelMessage::user("first request"),
            ModelMessage::assistant(Some("first reply".into()), None, None, Vec::new()),
            ModelMessage::user("second request"),
            ModelMessage::assistant(Some("second reply".into()), None, None, Vec::new()),
        ],
        "{replay:?}"
    );
}

#[tokio::test]
async fn human_mid_turn_steers_are_framed_with_a_reply_instruction() {
    fn steer(text: &str, actor: EventActor) -> PendingSteer {
        PendingSteer {
            prompt: QueuedPrompt {
                message_id: Uuid::new_v4(),
                text: text.into(),
                actor,
                attachments: Vec::new(),
                output_schema: None,
                delivery: PromptDelivery::Steer,
                visible: true,
                interrupt_batch: actor == EventActor::User,
                batch: Vec::new(),
            },
            acknowledgement_id: Uuid::new_v4(),
            admission: SteerAdmission::pending(),
            state: PendingSteerState::RetryAtBoundary {
                error: "boundary".into(),
            },
            attempt_boundary: 0,
        }
    }
    async fn dispatched(pending: &mut VecDeque<PendingSteer>, reply_prompt: bool) -> String {
        let (control_tx, mut controls) = mpsc::channel(4);
        let (result_tx, _results) = mpsc::channel(4);
        retry_pending_steers(&control_tx, &result_tx, pending, 1, reply_prompt).await;
        let AgentTurnControl::Steer { text, .. } = controls.recv().await.unwrap() else {
            panic!("expected steer")
        };
        text
    }

    let human = "you dont have my saved world from before?";
    let mut pending = VecDeque::from([steer(human, EventActor::User)]);
    let framed = dispatched(&mut pending, true).await;
    assert_eq!(framed, super::frame_mid_turn_human_message(human));
    assert!(framed.ends_with(human), "the human's words stay verbatim");
    assert!(framed.contains("next visible response"));

    let mut pending = VecDeque::from([steer(human, EventActor::User)]);
    assert_eq!(
        dispatched(&mut pending, false).await,
        human,
        "the option delivers the bare text"
    );

    let team = "Team message from worker: build finished";
    let mut pending = VecDeque::from([steer(team, EventActor::System)]);
    assert_eq!(
        dispatched(&mut pending, true).await,
        team,
        "team input is never framed as the human's words"
    );

    let mut pending = VecDeque::from([
        steer(human, EventActor::User),
        steer(team, EventActor::System),
    ]);
    assert_eq!(
        dispatched(&mut pending, true).await,
        format!("{human}\n\n{team}"),
        "a mixed batch is not framed as the human's words"
    );
}

/// A turn that ends while a steer is still in flight must not strand the
/// message. The provider takes the steer, never acknowledges it (the ack
/// channel is dropped as the turn ends), and the session must promote the
/// still-uncommitted prompt into a brand new turn rather than parking it in
/// `pending_steers` forever while reporting `ready`.
struct TurnEndsWithSteerInFlightExecutor {
    turns: RecordedPromptTurns,
    first_started: Arc<Notify>,
    steer_taken: Arc<Notify>,
    second_started: Arc<Notify>,
}

#[async_trait::async_trait]
impl AgentTurnExecutor for TurnEndsWithSteerInFlightExecutor {
    async fn execute(
        &self,
        turn: AgentTurn,
        _events: mpsc::Sender<SessionEventKind>,
        controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        let attempt = {
            let mut turns = self.turns.lock().unwrap();
            turns.push((turn.prompt.clone(), turn.attachments.clone()));
            turns.len()
        };
        if attempt > 1 {
            // The promoted steer became its own turn: answer it.
            self.second_started.notify_one();
            return Ok(AgentTurnResult {
                provider_session_id: Some("provider-session".to_string()),
                final_text: "answered the promoted steer".to_string(),
            });
        }
        self.first_started.notify_one();
        let mut controls = controls.expect("the first turn is steerable");
        // Take the steer and end the turn without acknowledging it. Dropping
        // `ack` is exactly what a provider process does when its turn finishes
        // before the steer is merged.
        match tokio::time::timeout(Duration::from_secs(5), controls.recv()).await {
            Ok(Some(AgentTurnControl::Steer { ack, .. })) => {
                drop(ack);
                self.steer_taken.notify_one();
            }
            other => panic!("expected a steer on the first turn, got {other:?}"),
        }
        Ok(AgentTurnResult {
            provider_session_id: Some("provider-session".to_string()),
            final_text: "first turn ended".to_string(),
        })
    }
}

#[tokio::test]
async fn steer_in_flight_when_the_turn_ends_starts_a_new_turn() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let followup_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(256);
    let turns: RecordedPromptTurns = Arc::new(Mutex::new(Vec::new()));
    let first_started = Arc::new(Notify::new());
    let steer_taken = Arc::new(Notify::new());
    let second_started = Arc::new(Notify::new());
    let executor = Arc::new(TurnEndsWithSteerInFlightExecutor {
        turns: Arc::clone(&turns),
        first_started: Arc::clone(&first_started),
        steer_taken: Arc::clone(&steer_taken),
        second_started: Arc::clone(&second_started),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn(async move {
        run_session_actor(
            &journal_path,
            session_id,
            LaunchSession {
                request_id: Uuid::new_v4(),
                cwd: root.path().to_path_buf(),
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: Some(false),
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
                name: None,
                initial_prompt: None,
                capabilities: Default::default(),
                subagent_concurrency_limit: None,
                extension_skill_roots: Vec::new(),
                team_policy: None,
            },
            command_rx,
            event_tx,
            executor,
            actor_store,
        )
        .await
    });

    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: Uuid::new_v4(),
            text: "first".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), first_started.notified())
        .await
        .expect("the first turn starts");

    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: followup_id,
            text: "typed while the turn was running".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), steer_taken.notified())
        .await
        .expect("the provider takes the steer");

    // The steer was never acknowledged, so it must be promoted into a new turn
    // instead of sitting in `pending_steers` while the session goes ready.
    tokio::time::timeout(Duration::from_secs(5), second_started.notified())
        .await
        .expect("an unacknowledged steer starts a new turn instead of being dropped");

    let recorded = turns.lock().unwrap().clone();
    assert_eq!(recorded.len(), 2, "the promoted steer runs as its own turn");
    assert!(
        recorded[1].0.contains("typed while the turn was running"),
        "the new turn carries the steered text, got {:?}",
        recorded[1].0
    );

    // The promoted prompt keeps its original message id and reaches Complete,
    // so the UI shows one message answered rather than a duplicate or a
    // message stuck in flight.
    let mut completed = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        let Ok(Some(event)) =
            tokio::time::timeout(Duration::from_millis(500), event_rx.recv()).await
        else {
            continue;
        };
        if matches!(
            event.kind,
            SessionEventKind::Message {
                message_id,
                status: MessageStatus::Complete,
                ..
            } if message_id == followup_id
        ) {
            completed = true;
            break;
        }
    }
    assert!(
        completed,
        "the promoted steer must be answered under its original message id"
    );

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();
    scratch.discard().await;
}

/// Build a workspace directed at `recipient` and return its stores plus a
/// projection standing in for one long-running child session.
async fn team_delivery_fixture() -> (
    crate::session_store::postgres::testing::ScratchDatabase,
    Uuid,
    Arc<PostgresSessionStore>,
    Arc<dyn WorkspaceStore>,
    crate::SessionWorkspaceBinding,
    WorkspaceProjection,
) {
    let session_id = Uuid::new_v4();
    let (scratch, session_store) = crate::session_store::postgres::testing::session_store().await;
    let session_store = Arc::new(session_store);
    session_store.create_session(session_id).await.unwrap();
    let binding = session_store
        .workspace_binding(session_id)
        .await
        .unwrap()
        .unwrap();
    let workspace_store = session_store.workspace_store().await.unwrap().unwrap();
    let human_id = crate::local_human_participant_id("Human");
    workspace_store
        .ensure_execution_workspace(
            binding.workspace_id,
            "test workspace",
            human_id,
            "Human",
            binding.participant_id,
            "Worker",
        )
        .await
        .unwrap();
    let projection = WorkspaceProjection::new(
        workspace_store.clone(),
        binding.workspace_id,
        binding.participant_id,
        human_id,
        0,
        0,
    );
    (
        scratch,
        session_id,
        session_store,
        workspace_store,
        binding,
        projection,
    )
}

/// Route one directed team message and return its id.
async fn append_team_message(
    workspace_store: &Arc<dyn WorkspaceStore>,
    binding: &crate::SessionWorkspaceBinding,
    text: &str,
    mode: crate::DeliveryMode,
) -> Uuid {
    let author = crate::local_human_participant_id("Human");
    let message_id = Uuid::new_v4();
    workspace_store
        .append(WorkspaceEvent {
            id: message_id,
            workspace_id: binding.workspace_id,
            sequence: 0,
            author_id: author,
            idempotency_key: format!("test-team-message:{message_id}"),
            created_at: chrono::Utc::now(),
            kind: WorkspaceEventKind::Message {
                message: crate::WorkspaceMessage {
                    id: message_id,
                    workspace_id: binding.workspace_id,
                    thread_id: None,
                    reply_to_message_id: None,
                    author_id: author,
                    body: crate::WorkspaceMessageBody {
                        text: text.to_string(),
                        mentions: Vec::new(),
                        attachments: Vec::new(),
                    },
                    audience: crate::Audience::Direct {
                        participant: binding.participant_id,
                    },
                    created_at: chrono::Utc::now(),
                },
                mode,
            },
        })
        .await
        .unwrap();
    message_id
}

async fn unread_ids(
    workspace_store: &Arc<dyn WorkspaceStore>,
    binding: &crate::SessionWorkspaceBinding,
) -> Vec<Uuid> {
    workspace_store
        .pending_message_events(binding.workspace_id, binding.participant_id, 100)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|(event, _)| match event.kind {
            WorkspaceEventKind::Message { message, .. } => Some(message.id),
            _ => None,
        })
        .collect()
}

async fn delivery_state(
    workspace_store: &Arc<dyn WorkspaceStore>,
    binding: &crate::SessionWorkspaceBinding,
    message_id: Uuid,
) -> crate::DeliveryState {
    workspace_store
        .message_deliveries(message_id)
        .await
        .unwrap()
        .into_iter()
        .find(|delivery| delivery.recipient_id == binding.participant_id)
        .expect("recipient delivery exists")
        .state
}

/// A busy child is steered mid-turn, so no `TurnCompleted` ever names the
/// team message. Team prompts are journaled as `System`, so gating admission
/// on `User` left the delivery pending forever and every later inbox read
/// replayed a build/freeze/lease instruction the worker had already carried
/// out. Only a coordinator restart used to clear it, so a long-running child
/// accumulated its whole history as unread.
#[tokio::test]
async fn an_accepted_steer_settles_a_long_running_child_team_delivery() {
    let (scratch, session_id, session_store, workspace_store, binding, projection) =
        team_delivery_fixture().await;
    let message_id = append_team_message(
        &workspace_store,
        &binding,
        "freeze the build",
        crate::DeliveryMode::Boundary,
    )
    .await;
    let store: Arc<dyn SessionStore> = session_store.clone();
    let mut runtime = RuntimeSessionStore::new(store.clone(), Vec::new(), true)
        .with_workspace_projection(projection);

    assert_eq!(
        unread_ids(&workspace_store, &binding).await,
        vec![message_id]
    );

    // The provider accepted the steer but the model has not consumed it yet.
    // An unconsumed steer is not an admission.
    runtime
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::System,
                text: "freeze the build".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::InProgress,
                delivery: Some(PromptDelivery::Steer),
            },
        ))
        .await
        .unwrap();
    assert_eq!(
        delivery_state(&workspace_store, &binding, message_id).await,
        crate::DeliveryState::Pending,
        "an unconsumed steer must not settle its delivery"
    );

    runtime
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::System,
                text: "freeze the build".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: Some(PromptDelivery::Steer),
            },
        ))
        .await
        .unwrap();

    // Consumed: admitted, and gone from the unread inbox. Not acknowledged --
    // no turn boundary ever answered for it, and claiming otherwise would
    // overstate what happened.
    assert_eq!(
        delivery_state(&workspace_store, &binding, message_id).await,
        crate::DeliveryState::Admitted
    );
    assert!(
        unread_ids(&workspace_store, &binding).await.is_empty(),
        "a consumed steer must not replay as unread on a still-running child"
    );
    scratch.discard().await;
}

/// The queued path reached `TurnCompleted`, but `Pending -> Acknowledged` is
/// rejected by the store's monotonic transition table, and the rejection only
/// surfaced as a warn-level projection diagnostic. The delivery stayed pending.
#[tokio::test]
async fn a_completed_queued_team_turn_acknowledges_its_delivery() {
    let (scratch, session_id, session_store, workspace_store, binding, projection) =
        team_delivery_fixture().await;
    let message_id = append_team_message(
        &workspace_store,
        &binding,
        "take the deploy lease",
        crate::DeliveryMode::NextTurn,
    )
    .await;
    let store: Arc<dyn SessionStore> = session_store.clone();
    let mut runtime = RuntimeSessionStore::new(store.clone(), Vec::new(), true)
        .with_workspace_projection(projection.clone());

    runtime
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::System,
                text: "take the deploy lease".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: Some(PromptDelivery::Queue),
            },
        ))
        .await
        .unwrap();
    assert_eq!(
        delivery_state(&workspace_store, &binding, message_id).await,
        crate::DeliveryState::Admitted
    );

    runtime
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::TurnCompleted {
                message_id,
                provider_session_id: Some("provider-session".to_string()),
                final_text: "leased".to_string(),
                error: None,
            },
        ))
        .await
        .unwrap();
    assert_eq!(
        delivery_state(&workspace_store, &binding, message_id).await,
        crate::DeliveryState::Acknowledged
    );
    assert!(unread_ids(&workspace_store, &binding).await.is_empty());

    // The rejected shortcut used to be swallowed into a diagnostic rather
    // than settling anything.
    assert!(!store.read(session_id).await.unwrap().iter().any(|event| {
        matches!(
            &event.kind,
            SessionEventKind::Error { message }
                if message.contains("invalid non-monotonic delivery transition")
        )
    }));

    // Replaying the whole transcript is how repair catches up; it must not
    // drag an acknowledged delivery back to admitted.
    for event in store.read(session_id).await.unwrap() {
        projection.project(&event).await.unwrap();
    }
    assert_eq!(
        delivery_state(&workspace_store, &binding, message_id).await,
        crate::DeliveryState::Acknowledged
    );
    scratch.discard().await;
}

#[tokio::test]
async fn projection_repair_settles_system_mail_from_a_direct_workspace() {
    let (scratch, session_id, session_store, workspace_store, binding, projection) =
        team_delivery_fixture().await;
    let author = crate::local_human_participant_id("Human");
    let direct_workspace = workspace_store
        .ensure_direct_workspace(author, binding.participant_id)
        .await
        .unwrap();
    let mut direct_binding = binding.clone();
    direct_binding.workspace_id = direct_workspace;
    let message_id = append_team_message(
        &workspace_store,
        &direct_binding,
        "direct handoff",
        crate::DeliveryMode::NextTurn,
    )
    .await;
    for kind in [
        SessionEventKind::Message {
            message_id,
            actor: EventActor::System,
            text: "direct handoff".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: Some(PromptDelivery::Queue),
        },
        SessionEventKind::TurnCompleted {
            message_id,
            provider_session_id: None,
            final_text: "handled".to_string(),
            error: None,
        },
    ] {
        session_store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }
    assert_eq!(
        delivery_state(&workspace_store, &direct_binding, message_id).await,
        crate::DeliveryState::Pending
    );

    let store: Arc<dyn SessionStore> = session_store.clone();
    projection.repair(store, session_id).await.unwrap();

    assert_eq!(
        delivery_state(&workspace_store, &direct_binding, message_id).await,
        crate::DeliveryState::Acknowledged
    );
    scratch.discard().await;
}

#[tokio::test]
async fn recovery_corrects_only_verified_team_prompts_without_acknowledging_them() {
    let (scratch, session_id, session_store, workspace_store, binding, projection) =
        team_delivery_fixture().await;
    let team_id = append_team_message(
        &workspace_store,
        &binding,
        "report after a usage pause",
        crate::DeliveryMode::NextTurn,
    )
    .await;
    let active_team_id = append_team_message(
        &workspace_store,
        &binding,
        "the interrupted turn",
        crate::DeliveryMode::NextTurn,
    )
    .await;
    let retried_team_id = append_team_message(
        &workspace_store,
        &binding,
        "retry the handoff",
        crate::DeliveryMode::NextTurn,
    )
    .await;
    let human_id = Uuid::new_v4();
    let store: Arc<dyn SessionStore> = session_store.clone();
    let mut runtime = RuntimeSessionStore::new(store.clone(), Vec::new(), true)
        .with_workspace_projection(projection.clone());
    for (message_id, text) in [
        (
            team_id,
            "Team message from /root/worker:\n\nreport after a usage pause",
        ),
        (
            human_id,
            "Team message from /root/worker:\n\nhuman pasted this text",
        ),
    ] {
        runtime
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::Message {
                    message_id,
                    actor: EventActor::User,
                    text: text.to_string(),
                    attachments: Vec::new(),
                    status: MessageStatus::Queued,
                    delivery: Some(PromptDelivery::Queue),
                },
            ))
            .await
            .unwrap();
    }
    runtime
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::Message {
                message_id: active_team_id,
                actor: EventActor::User,
                text: "Team message from /root/worker:\n\nthe interrupted turn".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::InProgress,
                delivery: Some(PromptDelivery::Queue),
            },
        ))
        .await
        .unwrap();
    for status in [
        MessageStatus::Queued,
        MessageStatus::InProgress,
        MessageStatus::Queued,
    ] {
        runtime
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::Message {
                    message_id: retried_team_id,
                    actor: EventActor::User,
                    text: "Team message from /root/worker:\n\nretry the handoff".to_string(),
                    attachments: Vec::new(),
                    status,
                    delivery: Some(PromptDelivery::Queue),
                },
            ))
            .await
            .unwrap();
    }
    let history = store.read(session_id).await.unwrap();
    let mut pending = recover_queued_prompts(&history);
    let (events, mut received) = mpsc::channel(8);
    let durable_admissions = recovered_durable_admissions(&history, &pending);
    assert_eq!(durable_admissions, HashSet::from([active_team_id]));

    repair_recovered_team_prompt_provenance(
        &mut pending,
        &durable_admissions,
        &projection,
        &mut runtime,
        &events,
        session_id,
    )
    .await
    .unwrap();
    assert_eq!(pending.len(), 4);
    assert_eq!(pending[0].actor, EventActor::System);
    assert!(!pending[0].interrupt_batch);
    assert_eq!(pending[1].actor, EventActor::User);
    assert_eq!(pending[2].actor, EventActor::User);
    assert_eq!(pending[3].actor, EventActor::System);
    let corrected_ids = [received.try_recv().unwrap(), received.try_recv().unwrap()]
        .into_iter()
        .map(|event| match event.kind {
            SessionEventKind::Message {
                message_id,
                actor: EventActor::System,
                status: MessageStatus::Queued,
                ..
            } => message_id,
            other => panic!("unexpected correction: {other:?}"),
        })
        .collect::<HashSet<_>>();
    assert_eq!(corrected_ids, HashSet::from([team_id, retried_team_id]));
    assert!(received.try_recv().is_err());
    assert_eq!(
        delivery_state(&workspace_store, &binding, team_id).await,
        crate::DeliveryState::Pending
    );
    let recovered = recover_queued_prompts(&store.read(session_id).await.unwrap());
    assert_eq!(recovered[0].actor, EventActor::System);
    assert_eq!(recovered[1].actor, EventActor::User);
    assert_eq!(recovered[2].actor, EventActor::User);
    assert_eq!(recovered[3].actor, EventActor::System);

    repair_recovered_team_prompt_provenance(
        &mut pending,
        &durable_admissions,
        &projection,
        &mut runtime,
        &events,
        session_id,
    )
    .await
    .unwrap();
    assert!(received.try_recv().is_err(), "repair is idempotent");
    scratch.discard().await;
}

/// The first turn starts a silent watcher and yields on it through the real
/// tool dispatcher. After that the session must stop calling the model even
/// though the goal is still active -- that is the whole point of the yield, and
/// a watch-level test cannot show it. A real prompt must then resume it.
struct YieldingExecutor {
    queued_reports: Option<mpsc::Sender<HostCommand>>,
    calls: Arc<AtomicUsize>,
    watch_id: Arc<Mutex<Option<Uuid>>>,
    yielded: Arc<Notify>,
    /// Stop the watcher right after yielding on it. A stopped watcher is
    /// cancelled, so it never flushes a final event: the only thing that can
    /// end the wait is the liveness re-check.
    stop_after_yield: bool,
    /// The watched command, so a test can arrange output to arrive later.
    command: &'static str,
}

#[async_trait::async_trait]
impl AgentTurnExecutor for YieldingExecutor {
    async fn execute(
        &self,
        turn: AgentTurn,
        events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            turn.agent_tools
                .call(
                    "create_goal",
                    json!({"objective": "finish the sweep", "token_budget": null}),
                )
                .await
                .expect("the goal is created");
            // A watcher that prints nothing: the only way out is the explicit
            // resume, never a stray line of output.
            let started = turn
                .agent_tools
                .call(
                    "watch",
                    json!({"command": self.command, "label": "Sweep", "workdir": null}),
                )
                .await
                .expect("the watcher starts");
            let watch_id: Uuid =
                serde_json::from_value(started["watch_id"].clone()).expect("a watch id");
            *self.watch_id.lock().unwrap() = Some(watch_id);
            let waited = turn
                .agent_tools
                .call(
                    "await_watchers",
                    json!({"watch_ids": [watch_id], "reason": "every remaining step needs the sweep"}),
                )
                .await
                .expect("the yield is accepted");
            assert_eq!(waited["status"], "waiting", "{waited}");
            if let Some(commands) = &self.queued_reports {
                for index in 0..3 {
                    commands
                        .send(HostCommand::TeamPrompt {
                            session_id: turn.session_id,
                            message_id: Uuid::new_v4(),
                            text: format!("old queued report {index}"),
                            attachments: Vec::new(),
                            output_schema: None,
                            delivery: PromptDelivery::Queue,
                        })
                        .await
                        .unwrap();
                }
            }

            if self.stop_after_yield {
                let tools = turn.agent_tools.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    let _ = tools
                        .call("stop_watcher", json!({"watch_id": watch_id}))
                        .await;
                });
            }
        }
        events
            .send(SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::Assistant,
                text: "waiting on the sweep".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            })
            .await
            .unwrap();
        self.yielded.notify_one();
        Ok(AgentTurnResult {
            provider_session_id: None,
            final_text: "waiting on the sweep".to_string(),
        })
    }
}

#[tokio::test]
async fn an_explicit_watcher_yield_stops_automatic_goal_turns_until_real_input() {
    assert_watcher_yield_blocks_automatic_turns(false).await;
}

#[tokio::test]
async fn queued_team_reports_do_not_spend_turns_while_yielded() {
    assert_watcher_yield_blocks_automatic_turns(true).await;
}

async fn assert_watcher_yield_blocks_automatic_turns(queue_reports: bool) {
    let root = tempdir().unwrap();
    let root_path = root.path().to_path_buf();
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let message_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(256);
    let calls = Arc::new(AtomicUsize::new(0));
    let yielded = Arc::new(Notify::new());
    let executor = Arc::new(YieldingExecutor {
        queued_reports: queue_reports.then(|| command_tx.clone()),
        calls: Arc::clone(&calls),
        watch_id: Arc::new(Mutex::new(None)),
        yielded: Arc::clone(&yielded),
        stop_after_yield: false,
        command: "sleep 30",
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn({
        let journal_path = root_path.join("session.lock");
        let cwd = root_path.clone();
        let executor = Arc::clone(&executor);
        async move {
            run_session_actor(
                &journal_path,
                session_id,
                LaunchSession {
                    request_id: message_id,
                    cwd,
                    provider: CodingProvider::Codex,
                    model: None,
                    effort: None,
                    fast: Some(false),
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::FullAccess,
                    name: None,
                    initial_prompt: Some("run the sweep".to_string()),
                    capabilities: crate::SessionCapabilities {
                        watcher_yield: true,
                        ..Default::default()
                    },
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                executor,
                actor_store,
            )
            .await
        }
    });

    tokio::time::timeout(Duration::from_secs(20), yielded.notified())
        .await
        .expect("the first turn yields");

    // The goal is active and unbudgeted, so without the yield the session would
    // immediately issue continuation turns. It must stay quiet instead.
    let mut journalled_yield = false;
    let mut settled_reports = 0;
    let quiet = tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(event) = event_rx.recv().await {
            if let SessionEventKind::Message {
                actor: EventActor::System,
                status: MessageStatus::Complete,
                text,
                ..
            } = &event.kind
                && text.starts_with("old queued report")
            {
                settled_reports += 1;
            }

            if let SessionEventKind::ProviderEvent { kind, .. } = &event.kind
                && kind == "goal_yielded"
            {
                journalled_yield = true;
            }
        }
    })
    .await;
    assert!(quiet.is_err(), "the session ended instead of waiting");
    assert!(journalled_yield, "the wait is journalled, never silent");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "a held yield must not spend any further model turns"
    );

    if queue_reports {
        assert_eq!(
            settled_reports, 3,
            "queued reports remain durable without turns"
        );
        let late_report = Uuid::new_v4();
        command_tx
            .send(HostCommand::TeamPrompt {
                session_id,
                message_id: late_report,
                text: "report arriving after the yield parked".to_string(),
                attachments: Vec::new(),
                output_schema: None,
                delivery: PromptDelivery::Queue,
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(event) = event_rx.recv().await {
                if matches!(event.kind, SessionEventKind::Message { message_id, status: MessageStatus::Complete, .. } if message_id == late_report) {
                    break;
                }
            }
        }).await.expect("the late report is recorded without waking");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    // Real input resumes it.
    let wake = if queue_reports {
        HostCommand::TeamPrompt {
            session_id,
            message_id: Uuid::new_v4(),
            text: "explicit wake: new actionable result".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        }
    } else {
        HostCommand::Prompt {
            session_id,
            message_id: Uuid::new_v4(),
            text: "status?".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        }
    };
    command_tx.send(wake).await.unwrap();
    tokio::time::timeout(Duration::from_secs(20), yielded.notified())
        .await
        .expect("real input resumes the session");
    assert!(calls.load(Ordering::SeqCst) >= 2, "the prompt ran a turn");

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(5), actor).await;
    scratch.discard().await;
}

/// Stopping a watcher cancels it, so it never flushes a final event. Nothing
/// will ever arrive to resume the goal, and the session is parked in the idle
/// select where the outer loop's liveness check never runs. Without the
/// re-check inside the WatchesChanged arm this strands the goal forever.
#[tokio::test]
async fn a_stopped_silent_watcher_ends_the_yield_instead_of_stranding_the_goal() {
    let root = tempdir().unwrap();
    let root_path = root.path().to_path_buf();
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let message_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(256);
    let calls = Arc::new(AtomicUsize::new(0));
    let yielded = Arc::new(Notify::new());
    let executor = Arc::new(YieldingExecutor {
        queued_reports: None,
        calls: Arc::clone(&calls),
        watch_id: Arc::new(Mutex::new(None)),
        yielded: Arc::clone(&yielded),
        stop_after_yield: true,
        command: "sleep 30",
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn({
        let journal_path = root_path.join("session.lock");
        let cwd = root_path.clone();
        let executor = Arc::clone(&executor);
        async move {
            run_session_actor(
                &journal_path,
                session_id,
                LaunchSession {
                    request_id: message_id,
                    cwd,
                    provider: CodingProvider::Codex,
                    model: None,
                    effort: None,
                    fast: Some(false),
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::FullAccess,
                    name: None,
                    initial_prompt: Some("run the sweep".to_string()),
                    capabilities: crate::SessionCapabilities {
                        watcher_yield: true,
                        ..Default::default()
                    },
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                executor,
                actor_store,
            )
            .await
        }
    });

    // First turn yields, then the watcher is stopped out from under it.
    tokio::time::timeout(Duration::from_secs(20), yielded.notified())
        .await
        .expect("the first turn yields");
    // No prompt, no watcher output: only the liveness re-check can resume this.
    tokio::time::timeout(Duration::from_secs(20), yielded.notified())
        .await
        .expect("a stopped watcher must release the goal, not strand it");
    assert!(calls.load(Ordering::SeqCst) >= 2);

    let mut resumed_cause = None;
    while let Ok(Some(event)) =
        tokio::time::timeout(Duration::from_millis(200), event_rx.recv()).await
    {
        if let SessionEventKind::ProviderEvent { kind, payload, .. } = &event.kind
            && kind == "goal_resumed"
        {
            resumed_cause = payload["cause"].as_str().map(str::to_string);
            break;
        }
    }
    // Either cause is correct and both prove the goal was released rather than
    // stranded: the stop announcement can reach the session before the watcher
    // task has finished marking itself not-running, in which case the ordinary
    // queued-event path clears the wait first. What must never happen is no
    // resume at all, which is what the timeout above pins.
    assert!(
        matches!(
            resumed_cause.as_deref(),
            Some("watchers_finished") | Some("input")
        ),
        "unexpected resume cause {resumed_cause:?}"
    );

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(5), actor).await;
    scratch.discard().await;
}

/// Interrupting a session that is parked on a yield is a stop, and a stop has
/// to hold. Before this, the idle command match only honoured an interrupt when
/// a retry was pending; anything else fell to `Some(_) => continue`, so the
/// human's interrupt was swallowed and `user_stop` was never set. A yield parks
/// the session indefinitely, which turns that narrow window into a real one:
/// watcher output would then start a model turn the human had just stopped.
#[tokio::test]
async fn an_interrupt_while_yielded_holds_watcher_output_until_a_human_returns() {
    let root = tempdir().unwrap();
    let root_path = root.path().to_path_buf();
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let message_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(256);
    let calls = Arc::new(AtomicUsize::new(0));
    let yielded = Arc::new(Notify::new());
    let executor = Arc::new(YieldingExecutor {
        queued_reports: None,
        calls: Arc::clone(&calls),
        watch_id: Arc::new(Mutex::new(None)),
        yielded: Arc::clone(&yielded),
        stop_after_yield: false,
        // Output lands well after the interrupt, so it is the stop that is
        // under test and not a race with the first flush.
        command: "sleep 2; printf 'progress\\n'; sleep 30",
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn({
        let journal_path = root_path.join("session.lock");
        let cwd = root_path.clone();
        let executor = Arc::clone(&executor);
        async move {
            run_session_actor(
                &journal_path,
                session_id,
                LaunchSession {
                    request_id: message_id,
                    cwd,
                    provider: CodingProvider::Codex,
                    model: None,
                    effort: None,
                    fast: Some(false),
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::FullAccess,
                    name: None,
                    initial_prompt: Some("run the sweep".to_string()),
                    capabilities: crate::SessionCapabilities {
                        watcher_yield: true,
                        ..Default::default()
                    },
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                executor,
                actor_store,
            )
            .await
        }
    });

    tokio::time::timeout(Duration::from_secs(20), yielded.notified())
        .await
        .expect("the first turn yields");
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    // Let the session actually park in the idle select first. An interrupt
    // delivered any earlier is drained by collect_input_at_turn_boundary and
    // would never reach the idle path this test is about.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    command_tx
        .send(HostCommand::Interrupt { session_id })
        .await
        .unwrap();

    // The watcher then produces real output. It must be held, not run.
    let mut saw_stop = false;
    let quiet = tokio::time::timeout(Duration::from_secs(6), async {
        while let Some(event) = event_rx.recv().await {
            if let SessionEventKind::UserStopChanged { engaged, .. } = &event.kind
                && *engaged
            {
                saw_stop = true;
            }
        }
    })
    .await;
    assert!(quiet.is_err(), "the session ended instead of holding");
    assert!(saw_stop, "the interrupt must record a user stop");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "watcher output must not run a model turn after a human interrupt"
    );

    // Only the human restarts the work.
    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: Uuid::new_v4(),
            text: "carry on".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(20), yielded.notified())
        .await
        .expect("the human resumes the session");
    assert!(calls.load(Ordering::SeqCst) >= 2);

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(5), actor).await;
    scratch.discard().await;
}

/// A watcher that finishes on its own while the session is parked on a yield.
/// The liveness re-check in the `WatchesChanged` arm has to hand control back
/// to the outer loop; leaving the idle select with `None` is the session's
/// shutdown signal, so getting this wrong silently ends the session and drops
/// the watcher's final output instead of resuming the goal.
///
/// This is the completion branch rather than the stop branch on purpose.
/// Stopping a watcher kills a live process, and that teardown is slow enough
/// that the session almost always consumes the stop announcement before the
/// watcher is marked not-running. A natural exit has no process left to reap,
/// so `running = false` lands first and the re-check is what actually runs.
/// Multi-threaded for the same reason: a current-thread runtime serialises the
/// two tasks and hides the ordering this test is about.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_watcher_that_finishes_while_yielded_resumes_the_goal_without_ending_the_session() {
    let root = tempdir().unwrap();
    let root_path = root.path().to_path_buf();
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let message_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, _event_rx) = mpsc::channel(256);
    let calls = Arc::new(AtomicUsize::new(0));
    let yielded = Arc::new(Notify::new());
    let executor = Arc::new(YieldingExecutor {
        queued_reports: None,
        calls: Arc::clone(&calls),
        watch_id: Arc::new(Mutex::new(None)),
        yielded: Arc::clone(&yielded),
        stop_after_yield: false,
        // Runs long enough to be a legal thing to wait on, then exits by
        // itself while the session is parked in the idle select.
        command: "sleep 2",
    });
    let actor_store = Arc::clone(&store);
    let mut actor = tokio::spawn({
        let journal_path = root_path.join("session.lock");
        let cwd = root_path.clone();
        let executor = Arc::clone(&executor);
        async move {
            run_session_actor(
                &journal_path,
                session_id,
                LaunchSession {
                    request_id: message_id,
                    cwd,
                    provider: CodingProvider::Codex,
                    model: None,
                    effort: None,
                    fast: Some(false),
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::FullAccess,
                    name: None,
                    initial_prompt: Some("run the sweep".to_string()),
                    capabilities: crate::SessionCapabilities {
                        watcher_yield: true,
                        ..Default::default()
                    },
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                executor,
                actor_store,
            )
            .await
        }
    });

    tokio::time::timeout(Duration::from_secs(20), yielded.notified())
        .await
        .expect("the first turn yields");

    // Nothing else is sent: the watcher exiting is the only thing that can
    // move this session, and it must resume the goal rather than end it.
    tokio::select! {
        resumed = tokio::time::timeout(Duration::from_secs(20), yielded.notified()) => {
            resumed.expect("a finished watcher must resume the goal");
        }
        actor_result = &mut actor => {
            panic!("the session exited instead of resuming the goal: {actor_result:?}");
        }
    }
    assert!(calls.load(Ordering::SeqCst) >= 2, "the goal kept working");
    assert!(
        !actor.is_finished(),
        "the session must still be running after the watcher finished"
    );

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(5), actor).await;
    scratch.discard().await;
}

/// A completed compaction starts a new context generation, so the declarations
/// the previous generation recorded must not leak past the boundary -- and the
/// ones still in force must survive it. Both halves matter: leaking replays a
/// tool the model can no longer call, and losing them makes a resumed session
/// re-announce a catalog the model was already given.
///
/// This is the durability rule commit bb0dc16 established for the verbatim
/// tail, applied to declarations: read from the boundary event itself, because
/// a resumed session rebuilds from the boundary forward.
#[test]
fn declarations_cross_a_compaction_boundary_only_through_the_boundary_event() {
    use crate::prompt_context::{Declarations, InstructionSlot};

    fn tool(name: &str) -> borg_provider::provider::ModelToolDefinition {
        borg_provider::provider::ModelToolDefinition::new(
            name,
            "",
            serde_json::json!({"type": "object"}),
        )
        .unwrap()
    }

    let session = Uuid::new_v4();
    let before = Declarations::capture(
        [(InstructionSlot::Skills, "deploy".to_string())],
        &[tool("exec"), tool("retired_tool")],
    );
    let carried = Declarations::capture(
        [(InstructionSlot::Skills, "deploy".to_string())],
        &[tool("exec")],
    );

    let boundary = |payload: serde_json::Value| {
        vec![
            SessionEvent::new(
                session,
                1,
                SessionEventKind::ProviderEvent {
                    provider: CodingProvider::OpenRouter,
                    kind: "native_declaration_base".to_string(),
                    payload: serde_json::to_value(&before).unwrap(),
                },
            ),
            SessionEvent::new(
                session,
                2,
                SessionEventKind::ProviderEvent {
                    provider: CodingProvider::OpenRouter,
                    kind: "context_compaction".to_string(),
                    payload,
                },
            ),
        ]
    };

    // The boundary carries declarations: those are what survives, not the
    // pre-boundary base, even though its event is still in the slice.
    let events = boundary(serde_json::json!({
        "status": "completed",
        "summary": "earlier work",
        "native": true,
        "retained_declarations": &carried,
    }));
    assert_eq!(native_declarations(&events), Some(carried));

    // A boundary from before the field leaves no base at all, so the next turn
    // records a fresh one rather than inheriting a generation that has ended.
    let legacy = boundary(serde_json::json!({
        "status": "completed",
        "summary": "earlier work",
        "native": true,
    }));
    assert_eq!(native_declarations(&legacy), None);
}

struct DropNotify(Arc<Notify>);

impl Drop for DropNotify {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

struct SilentProviderExecutor {
    started: Arc<Notify>,
    dropped: Arc<Notify>,
    cleanup_calls: Arc<AtomicUsize>,
    saturated: Option<Arc<Notify>>,
}

#[async_trait::async_trait]
impl AgentTurnExecutor for SilentProviderExecutor {
    async fn execute(
        &self,
        _turn: AgentTurn,
        _events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        let _dropped = DropNotify(Arc::clone(&self.dropped));
        self.started.notify_one();
        if let Some(saturated) = &self.saturated {
            let controls = _controls.as_ref().unwrap();
            while controls.len() < controls.max_capacity() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            saturated.notify_one();
        }
        std::future::pending().await
    }

    async fn stop_session(&self, _session_id: Uuid) -> Result<()> {
        self.cleanup_calls.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

#[tokio::test]
async fn immediate_interrupt_cancels_a_silent_provider() {
    assert_immediate_interrupt(0).await;
}

#[tokio::test]
async fn immediate_interrupt_cancels_with_a_full_control_queue() {
    assert_immediate_interrupt(33).await;
}

async fn assert_immediate_interrupt(control_backlog: usize) {
    const ESCAPE_BOUND: Duration = Duration::from_millis(750);
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let message_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(256);
    let started = Arc::new(Notify::new());
    let dropped = Arc::new(Notify::new());
    let cleanup_calls = Arc::new(AtomicUsize::new(0));
    let saturated = Arc::new(Notify::new());
    let executor = Arc::new(SilentProviderExecutor {
        started: Arc::clone(&started),
        dropped: Arc::clone(&dropped),
        cleanup_calls: Arc::clone(&cleanup_calls),
        saturated: (control_backlog > 0).then(|| Arc::clone(&saturated)),
    });
    let actor_store = Arc::clone(&store);
    let actor = tokio::spawn(async move {
        run_session_actor(
            &journal_path,
            session_id,
            LaunchSession {
                request_id: Uuid::new_v4(),
                cwd: root.path().to_path_buf(),
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: Some(false),
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
                name: None,
                initial_prompt: None,
                capabilities: Default::default(),
                subagent_concurrency_limit: None,
                extension_skill_roots: Vec::new(),
                team_policy: None,
            },
            command_rx,
            event_tx,
            executor,
            actor_store,
        )
        .await
    });

    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id,
            text: "run until interrupted".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), started.notified())
        .await
        .expect("the silent turn starts");

    if control_backlog > 0 {
        tokio::time::timeout(Duration::from_secs(5), async {
            let full = saturated.notified();
            tokio::pin!(full);
            for i in 0..128 {
                let command = HostCommand::TeamPrompt {
                    session_id,
                    message_id: Uuid::new_v4(),
                    text: format!("pending steer {i}"),
                    attachments: Vec::new(),
                    output_schema: None,
                    delivery: PromptDelivery::Steer,
                };
                tokio::select! {
                    biased;
                    _ = &mut full => return,
                    result = command_tx.send(command) => result.unwrap(),
                }
            }
            full.await;
        })
        .await
        .expect("the provider control queue is actually full");
    }

    let escape_at = std::time::Instant::now();
    tokio::time::timeout(
        Duration::from_millis(250),
        command_tx.send(HostCommand::Interrupt { session_id }),
    )
    .await
    .expect("Escape is not blocked by the command backlog")
    .unwrap();

    let dropped_promptly = tokio::time::timeout(Duration::from_millis(250), dropped.notified())
        .await
        .is_ok();
    let cancelled_after = escape_at.elapsed();

    let mut boundary_at = None;
    let mut saw_turn_completed = false;
    let mut saw_failed_prompt = false;
    while boundary_at.is_none() {
        let event = tokio::time::timeout(Duration::from_secs(5), event_rx.recv())
            .await
            .expect("a silent provider still reaches a terminal boundary")
            .expect("session remains open");
        match &event.kind {
            SessionEventKind::Message {
                message_id: event_message_id,
                status: MessageStatus::Failed,
                ..
            } if *event_message_id == message_id => saw_failed_prompt = true,
            SessionEventKind::TurnCompleted {
                message_id: event_message_id,
                error: Some(error),
                final_text,
                ..
            } if *event_message_id == message_id => {
                assert_eq!(error, "turn interrupted");
                assert!(
                    final_text.is_empty(),
                    "an interrupted silent turn has no final text to report"
                );
                saw_turn_completed = true;
            }
            SessionEventKind::StatusChanged {
                status: SessionStatus::Ready,
                detail: Some(detail),
            } if detail == "Interrupted" => {
                assert!(
                    saw_turn_completed,
                    "Ready must still follow TurnCompleted: the terminal ordering \
                     is not what this test relaxes"
                );
                boundary_at = Some(std::time::Instant::now());
            }
            _ => {}
        }
    }
    let waited = boundary_at.expect("terminal boundary observed") - escape_at;

    assert!(
        saw_failed_prompt,
        "the interrupted prompt must still be marked failed"
    );
    assert_eq!(
        cleanup_calls.load(Ordering::Acquire),
        1,
        "provider cleanup still runs exactly once before the boundary"
    );
    assert!(
        store.state(session_id).await.unwrap().user_stopped,
        "the human's stop stays durably latched after the turn is cancelled"
    );

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(5), actor).await;
    scratch.discard().await;
    eprintln!(
        "interrupt cancellation observed after {cancelled_after:?}; terminal after {waited:?}"
    );
    assert!(
        dropped_promptly,
        "the running turn must actually be dropped promptly"
    );
    assert!(
        waited < ESCAPE_BOUND,
        "a turn that produced nothing must not pay the cooperative grace before \
         reaching a terminal boundary: waited {waited:?}"
    );
}

/// The forwarder between the coordinator's broadcast and the actor's channel is
/// the one place child activity can be reordered or dropped.
///
/// Failure mode: the hop reorders activity against itself or loses it, so the
/// durable sequence the actor writes -- and every replay taken from that
/// journal -- differs from what the coordinator emitted, or a child's later
/// state is recorded before its earlier one.
#[tokio::test]
async fn forwarded_child_activity_keeps_the_coordinator_order() {
    let (activity_tx, activity_rx) = broadcast::channel(8);
    let (forwarded_tx, mut forwarded_rx) = mpsc::channel(8);
    let forwarder = tokio::spawn(forward_child_activity(activity_rx, forwarded_tx));
    let children: Vec<Uuid> = (0..4).map(|_| Uuid::new_v4()).collect();
    for child in &children {
        activity_tx
            .send(SubagentActivity::Started {
                agent: watched_child(*child),
            })
            .unwrap();
    }
    for (index, child) in children.iter().enumerate() {
        let activity = forwarded_rx.recv().await.expect("every activity arrives");
        let SubagentActivity::Started { agent } = activity else {
            panic!("the activity has to survive the hop unchanged");
        };
        assert_eq!(agent.session_id, *child, "activity {index} kept its order");
    }
    // The coordinator going away ends the forwarder rather than leaving it
    // waiting on a source that can never speak again.
    drop(activity_tx);
    forwarder.await.unwrap();
}

fn watched_child(session_id: Uuid) -> crate::SubagentSnapshot {
    crate::SubagentSnapshot {
        session_id,
        parent_session_id: Uuid::new_v4(),
        task_name: "/root/worker".to_string(),
        status: crate::SubagentStatus::Running,
        provider: CodingProvider::Codex,
        model: None,
        effort: None,
        cwd: PathBuf::from("/tmp"),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        detail: None,
        final_text: None,
        usage: Default::default(),
        interrupted_by: None,
    }
}

/// The watcher set for tests that drive `record_subagent_activity` directly.
fn test_watches(session_id: Uuid) -> crate::watch::Watches {
    let (events, _events_rx) = mpsc::channel(8);
    crate::watch::Watches::new(
        crate::native_process::ProcessManager::default(),
        events,
        session_id,
    )
}

/// Blocks its turn in `wait_agent`, the way an orchestrator waits on children.
struct WaitingParentExecutor {
    turn_started: Arc<Notify>,
    waited: Arc<std::sync::Mutex<Option<(Duration, serde_json::Value)>>>,
    wait_returned: Arc<Notify>,
}

#[async_trait::async_trait]
impl AgentTurnExecutor for WaitingParentExecutor {
    fn uses_native_harness(&self, _provider: CodingProvider) -> bool {
        true
    }

    async fn execute(
        &self,
        turn: AgentTurn,
        _events: mpsc::Sender<SessionEventKind>,
        controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        self.turn_started.notify_one();
        let started = std::time::Instant::now();
        let result = turn
            .agent_tools
            .call("wait_agent", json!({ "timeout_ms": 1_800_000 }))
            .await?;
        *self.waited.lock().unwrap() = Some((started.elapsed(), result));
        self.wait_returned.notify_one();
        let mut controls = controls.expect("active turn has controls");
        while let Some(control) = controls.recv().await {
            match control {
                AgentTurnControl::Steer { admission, ack, .. } => {
                    assert!(admission.accept());
                    let _ = ack.send(Ok(()));
                }
                AgentTurnControl::Interrupt => break,
                _ => {}
            }
        }
        Ok(AgentTurnResult {
            provider_session_id: None,
            final_text: String::new(),
        })
    }
}

#[tokio::test]
async fn a_human_steer_ends_a_blocking_wait_agent() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, _event_rx) = mpsc::channel(256);
    let turn_started = Arc::new(Notify::new());
    let wait_returned = Arc::new(Notify::new());
    let waited = Arc::new(std::sync::Mutex::new(None));
    let executor = Arc::new(WaitingParentExecutor {
        turn_started: Arc::clone(&turn_started),
        waited: Arc::clone(&waited),
        wait_returned: Arc::clone(&wait_returned),
    });
    let actor = tokio::spawn({
        let cwd = root.path().to_path_buf();
        let store = Arc::clone(&store);
        async move {
            run_session_actor(
                &journal_path,
                session_id,
                LaunchSession {
                    request_id: Uuid::new_v4(),
                    cwd,
                    provider: CodingProvider::OpenRouter,
                    model: None,
                    effort: None,
                    fast: Some(false),
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::Manual,
                    name: None,
                    initial_prompt: None,
                    capabilities: Default::default(),
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                executor,
                store,
            )
            .await
        }
    });
    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: Uuid::new_v4(),
            text: "wait for the team".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Queue,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), turn_started.notified())
        .await
        .expect("turn starts");
    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: Uuid::new_v4(),
            text: "status?".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(4), wait_returned.notified())
        .await
        .expect("the steer ends the wait instead of waiting out the timeout");
    let (elapsed, result) = waited.lock().unwrap().take().unwrap();
    assert_eq!(result["reason"], "input_pending", "{result}");
    assert!(elapsed < Duration::from_secs(4));
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(30), actor).await;
    scratch.discard().await;
}

/// Runs each turn until it is interrupted.
struct InterruptibleExecutor {
    turn_started: Arc<Notify>,
}

#[async_trait::async_trait]
impl AgentTurnExecutor for InterruptibleExecutor {
    async fn execute(
        &self,
        _turn: AgentTurn,
        _events: mpsc::Sender<SessionEventKind>,
        controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        self.turn_started.notify_one();
        let mut controls = controls.expect("active turn has controls");
        while !matches!(
            controls.recv().await,
            Some(AgentTurnControl::Interrupt) | None
        ) {}
        Ok(AgentTurnResult {
            provider_session_id: None,
            final_text: String::new(),
        })
    }
}

#[tokio::test]
async fn resume_from_interrupt_lets_the_parents_follow_up_start_a_turn() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    store.create_session(session_id).await.unwrap();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, _event_rx) = mpsc::channel(256);
    let turn_started = Arc::new(Notify::new());
    let executor = Arc::new(InterruptibleExecutor {
        turn_started: Arc::clone(&turn_started),
    });
    let actor = tokio::spawn({
        let cwd = root.path().to_path_buf();
        let store = Arc::clone(&store);
        async move {
            run_session_actor(
                &journal_path,
                session_id,
                LaunchSession {
                    request_id: Uuid::new_v4(),
                    cwd,
                    provider: CodingProvider::OpenRouter,
                    model: None,
                    effort: None,
                    fast: Some(false),
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::Manual,
                    name: None,
                    initial_prompt: None,
                    capabilities: Default::default(),
                    subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                executor,
                store,
            )
            .await
        }
    });
    let team_prompt = |text: &str| HostCommand::TeamPrompt {
        session_id,
        message_id: Uuid::new_v4(),
        text: text.to_string(),
        attachments: Vec::new(),
        output_schema: None,
        delivery: PromptDelivery::Steer,
    };
    command_tx.send(team_prompt("first task")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), turn_started.notified())
        .await
        .expect("the first task runs");
    command_tx
        .send(HostCommand::Interrupt { session_id })
        .await
        .unwrap();
    command_tx
        .send(team_prompt("background wake"))
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(500), turn_started.notified())
            .await
            .is_err(),
        "an interrupt still holds against an ordinary team wake"
    );
    command_tx
        .send(HostCommand::ResumeFromInterrupt { session_id })
        .await
        .unwrap();
    command_tx.send(team_prompt("resume")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), turn_started.notified())
        .await
        .expect("the interrupting parent's follow-up starts a turn");
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(30), actor).await;
    scratch.discard().await;
}
