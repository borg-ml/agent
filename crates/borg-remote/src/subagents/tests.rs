use super::*;
use crate::{PermissionMode, SessionEventKind};
use std::sync::Mutex as StdMutex;
use tempfile::tempdir;

#[test]
fn agent_mcp_executable_uses_existing_current_binary() {
    let directory = tempdir().unwrap();
    let executable = directory.path().join("borg");
    std::fs::write(&executable, b"borg").unwrap();

    assert_eq!(
        resolve_agent_mcp_executable(&executable).unwrap(),
        executable
    );
}

#[cfg(target_os = "linux")]
#[test]
fn agent_mcp_executable_recovers_path_after_atomic_upgrade() {
    let directory = tempdir().unwrap();
    let executable = directory.path().join("borg");
    std::fs::write(&executable, b"replacement").unwrap();
    let deleted_identity = PathBuf::from(format!("{} (deleted)", executable.display()));

    assert_eq!(
        resolve_agent_mcp_executable(&deleted_identity).unwrap(),
        executable
    );
}

#[derive(Clone, Default)]
struct RecordingPeerExecutor {
    prompts: Arc<StdMutex<Vec<String>>>,
}

#[async_trait::async_trait]
impl crate::AgentTurnExecutor for RecordingPeerExecutor {
    async fn execute(
        &self,
        turn: crate::AgentTurn,
        events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<crate::AgentTurnControl>>,
    ) -> Result<crate::AgentTurnResult> {
        let response = {
            let mut prompts = self.prompts.lock().expect("peer prompt lock");
            let response = format!("persistent peer reply {}", prompts.len() + 1);
            prompts.push(turn.prompt);
            response
        };
        events
            .send(SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::Assistant,
                text: response.clone(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            })
            .await
            .map_err(|_| anyhow::anyhow!("peer event receiver closed"))?;
        Ok(crate::AgentTurnResult {
            provider_session_id: Some("persistent-peer-session".to_string()),
            final_text: response,
        })
    }
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
    })
    .collect()
}

fn launch() -> LaunchSession {
    LaunchSession {
        request_id: Uuid::new_v4(),
        cwd: PathBuf::from("/workspace"),
        provider: CodingProvider::Codex,
        model: Some("gpt-test".into()),
        effort: Some("high".into()),
        fast: Some(false),
        response_language: crate::ResponseLanguage::Auto,
        permission_mode: PermissionMode::Manual,
        name: None,
        initial_prompt: None,
        capabilities: crate::SessionCapabilities {
            provider_capabilities: test_provider_capabilities(),
            ..crate::SessionCapabilities::default()
        },
        subagent_concurrency_limit: None,
        extension_skill_roots: Vec::new(),
        team_policy: None,
    }
}

async fn bind_test_team(
    directory: &Path,
    store: &crate::SqliteSessionStore,
    root: Uuid,
    children: &[Uuid],
) {
    let workspace = store
        .workspace_store()
        .await
        .unwrap()
        .expect("SQLite session store exposes the canonical workspace projection");
    let human = crate::local_human_participant_id("Human");
    workspace
        .ensure_execution_workspace(root, "test team", human, "Human", root, "Director")
        .await
        .unwrap();
    for child in children {
        let lock_path = child_lock_path(directory, *child);
        let _writer = crate::SessionWriterLease::acquire(&lock_path).unwrap();
        store.register_child_session(root, *child).await.unwrap();
        workspace
            .ensure_execution_workspace(root, "test team", human, "Human", *child, "Worker")
            .await
            .unwrap();
    }
}

#[test]
fn child_identity_is_stable_and_inherits_execution_context() {
    let root = Uuid::new_v4();
    let mut table = SubagentTable {
        root_session_id: root,
        max_children: 2,
        entries: HashMap::new(),
        task_names: HashMap::new(),
    };
    let child = table.reserve("review_api", &launch()).unwrap();
    assert_eq!(child.parent_session_id, root);
    assert_eq!(child.task_name, "/root/review_api");
    assert_eq!(child.provider, CodingProvider::Codex);
    assert_eq!(child.model.as_deref(), Some("gpt-test"));
    assert_eq!(table.resolve("review_api").unwrap(), child.session_id);
    assert_eq!(table.resolve("/root/review_api").unwrap(), child.session_id);
}

#[tokio::test]
async fn ensuring_a_sidecar_reuses_one_idle_provider_session() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let store = Arc::new(
        crate::SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
            .await
            .unwrap(),
    );
    store.create_session(root).await.unwrap();
    let mut root_launch = launch();
    root_launch.capabilities.multiplayer = false;
    let coordinator = SubagentCoordinator::new_with_store_and_executor(
        directory.path(),
        root,
        root_launch,
        3,
        Arc::new(crate::LocalAgentTurnExecutor::default()),
        store,
    )
    .unwrap();

    let first = coordinator
        .ensure_sidecar(
            "claude",
            CodingProvider::Claude,
            Some("claude-opus-5".to_string()),
            Some("high".to_string()),
        )
        .await
        .unwrap();
    let second = coordinator
        .ensure_sidecar(
            "claude",
            CodingProvider::Claude,
            Some("claude-opus-5".to_string()),
            Some("high".to_string()),
        )
        .await
        .unwrap();

    assert_eq!(first.session_id, second.session_id);
    assert_eq!(second.task_name, "/root/claude");
    assert_eq!(second.provider, CodingProvider::Claude);
    assert_eq!(second.model.as_deref(), Some("claude-opus-5"));
    assert_eq!(second.effort.as_deref(), Some("high"));

    coordinator.stop("/root/claude").await.unwrap();
    let resumed = coordinator
        .ensure_sidecar(
            "claude",
            CodingProvider::Claude,
            Some("claude-opus-5".to_string()),
            Some("high".to_string()),
        )
        .await
        .unwrap();
    assert_eq!(resumed.session_id, first.session_id);
    coordinator.stop("/root/claude").await.unwrap();
}

#[tokio::test]
async fn subagent_admission_rejects_a_provider_without_host_authentication() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let store = Arc::new(
        crate::SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
            .await
            .unwrap(),
    );
    store.create_session(root).await.unwrap();
    let mut root_launch = launch();
    let claude = root_launch
        .capabilities
        .provider_capabilities
        .iter_mut()
        .find(|capability| capability.provider == CodingProvider::Claude)
        .unwrap();
    claude.authenticated = false;
    claude.auth_methods.clear();
    claude.can_spawn = false;
    claude.auth_detail = Some("Claude subscription is not authenticated".to_string());
    let coordinator = SubagentCoordinator::new_with_store_and_executor(
        directory.path(),
        root,
        root_launch,
        2,
        Arc::new(crate::LocalAgentTurnExecutor::default()),
        store,
    )
    .unwrap();

    let error = coordinator
        .ensure_sidecar(
            "claude",
            CodingProvider::Claude,
            Some("claude-opus-5".to_string()),
            Some("high".to_string()),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("Claude cannot spawn"));
    assert!(error.contains("not authenticated"));
}

#[tokio::test]
async fn persistent_peer_consultation_reuses_the_sidecar_and_returns_to_the_primary() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let store = Arc::new(
        crate::SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
            .await
            .unwrap(),
    );
    store.create_session(root).await.unwrap();
    let prompts = Arc::new(StdMutex::new(Vec::new()));
    let executor = RecordingPeerExecutor {
        prompts: Arc::clone(&prompts),
    };
    let mut root_launch = launch();
    root_launch.capabilities.multiplayer = false;
    root_launch.cwd = directory.path().to_path_buf();
    let coordinator = SubagentCoordinator::new_with_store_and_executor(
        directory.path(),
        root,
        root_launch,
        3,
        Arc::new(executor),
        store,
    )
    .unwrap();

    let first = coordinator
        .consult_peer(CodingProvider::Codex, None, "Compare the API boundaries.")
        .await
        .unwrap();
    let first_thread = first["thread"].as_str().unwrap().to_string();
    let second = coordinator
        .consult_peer(
            CodingProvider::Codex,
            None,
            "Now revisit the cancellation edge case.",
        )
        .await
        .unwrap();
    let reverse = coordinator
        .consult_peer(
            CodingProvider::Claude,
            None,
            "As the Claude primary, ask GPT to challenge the conclusion.",
        )
        .await
        .unwrap();

    assert_eq!(first["persistent"], true);
    assert_eq!(first["provider"], "claude");
    assert_eq!(first["response"], "persistent peer reply 1");
    assert_eq!(second["response"], "persistent peer reply 2");
    assert_eq!(second["thread"], first_thread);
    assert_eq!(reverse["provider"], "codex");
    assert_eq!(reverse["thread"], "/root/gpt");
    assert_eq!(reverse["response"], "persistent peer reply 3");
    let prompts = prompts.lock().unwrap().clone();
    assert_eq!(prompts.len(), 3);
    assert!(prompts[0].contains("Compare the API boundaries."));
    assert!(prompts[1].contains("cancellation edge case"));
    assert!(prompts[0].contains("persistent private Claude peer"));
    assert!(prompts[2].contains("As the Claude primary"));
    coordinator.stop("/root/claude").await.unwrap();
    coordinator.stop("/root/gpt").await.unwrap();
}

#[test]
fn cross_provider_peer_does_not_inherit_an_incompatible_model_or_effort() {
    assert_eq!(
        default_model_for_cross_provider_peer(CodingProvider::Claude),
        None
    );
    assert_eq!(
        default_effort_for_cross_provider_peer(CodingProvider::Claude),
        None
    );
    assert_eq!(
        default_model_for_cross_provider_peer(CodingProvider::Codex).as_deref(),
        Some(borg_provider::codex_product_model())
    );
    assert_eq!(
        default_model_for_cross_provider_peer(CodingProvider::OpenRouter).as_deref(),
        Some(borg_provider::openrouter_product_model())
    );
    assert_eq!(
        default_effort_for_cross_provider_peer(CodingProvider::OpenRouter),
        None
    );
}

#[test]
fn persistent_peer_defaults_to_the_opposite_provider_and_stable_sidecar_profile() {
    let (provider, model, effort) =
        resolve_persistent_peer_profile(CodingProvider::Codex, None).unwrap();
    assert_eq!(provider, CodingProvider::Claude);
    assert_eq!(model.as_deref(), Some("claude-opus-5"));
    assert_eq!(effort.as_deref(), Some("high"));

    let (provider, model, effort) =
        resolve_persistent_peer_profile(CodingProvider::Claude, None).unwrap();
    assert_eq!(provider, CodingProvider::Codex);
    assert_eq!(model.as_deref(), Some(borg_provider::codex_product_model()));
    assert_eq!(
        effort.as_deref(),
        Some(borg_provider::codex_default_effort())
    );

    let (provider, model, effort) =
        resolve_persistent_peer_profile(CodingProvider::Codex, Some("claude-opus-5@high")).unwrap();
    assert_eq!(provider, CodingProvider::Claude);
    assert_eq!(model.as_deref(), Some("claude-opus-5"));
    assert_eq!(effort.as_deref(), Some("high"));
}

#[test]
fn live_child_limit_and_task_names_are_enforced() {
    let mut table = SubagentTable {
        root_session_id: Uuid::new_v4(),
        max_children: 1,
        entries: HashMap::new(),
        task_names: HashMap::new(),
    };
    let child = table.reserve("first", &launch()).unwrap();
    assert!(table.reserve("second", &launch()).is_err());
    table
        .entries
        .get_mut(&child.session_id)
        .unwrap()
        .snapshot
        .status = SubagentStatus::Stopped;
    assert!(table.reserve("second", &launch()).is_ok());
    assert!(table.reserve("SECOND", &launch()).is_err());
}

#[tokio::test]
async fn child_messages_are_team_scoped_and_can_report_to_root() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let store = Arc::new(
        crate::SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
            .await
            .unwrap(),
    );
    store.create_session(root).await.unwrap();
    let coordinator = SubagentCoordinator::new_with_store_and_executor(
        directory.path(),
        root,
        launch(),
        3,
        Arc::new(crate::LocalAgentTurnExecutor::default()),
        store.clone(),
    )
    .unwrap();
    let worker = coordinator
        .table
        .lock()
        .await
        .reserve("worker", &launch())
        .unwrap();
    bind_test_team(directory.path(), store.as_ref(), root, &[worker.session_id]).await;
    let mut wake = coordinator.subscribe_root_messages();
    let mut activity = coordinator.subscribe();

    coordinator
        .send_message_as(worker.session_id, "/root", "blocked on an API decision")
        .await
        .unwrap();
    let projected = activity.recv().await.unwrap();
    assert!(matches!(
        projected,
        SubagentActivity::SessionEvent {
            event: SessionEvent {
                kind: SessionEventKind::Message {
                    actor: crate::EventActor::Assistant,
                    status: MessageStatus::Complete,
                    ref text,
                    ..
                },
                ..
            },
            ..
        } if text == "blocked on an API decision"
    ));
    assert!(matches!(
        wake.try_recv(),
        Ok(message)
            if message.delivery == PromptDelivery::Steer
                && message.text.contains("blocked on an API decision")
    ));

    coordinator
        .followup_task_as(worker.session_id, "/root", "please review")
        .await
        .unwrap();
    let followup = wake.recv().await.unwrap();
    assert!(followup.text.contains("please review"));
    assert!(coordinator.take_root_inbox().await.is_empty());
}

#[tokio::test]
async fn durable_root_inbox_poll_replays_a_report_when_wake_delivery_was_missed() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let store = Arc::new(
        crate::SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
            .await
            .unwrap(),
    );
    store.create_session(root).await.unwrap();
    let coordinator = SubagentCoordinator::new_with_store_and_executor(
        directory.path(),
        root,
        launch(),
        3,
        Arc::new(crate::LocalAgentTurnExecutor::default()),
        store.clone(),
    )
    .unwrap();
    let worker = coordinator
        .table
        .lock()
        .await
        .reserve("worker", &launch())
        .unwrap();
    bind_test_team(directory.path(), store.as_ref(), root, &[worker.session_id]).await;

    // No root receiver exists. The durable inbox must retain the wake and
    // replay it when the root session reconnects.
    coordinator
        .send_message_as(worker.session_id, "/root", "durable result")
        .await
        .unwrap();

    let mut wake = coordinator.subscribe_root_messages();
    let reports = coordinator.refresh_root_inbox_reports().await.unwrap();
    assert_eq!(reports.len(), 1);
    let (message_id, SubagentActivity::SessionEvent { event, .. }) = &reports[0] else {
        panic!("expected a durable child report projection");
    };
    assert!(matches!(
        &event.kind,
        SessionEventKind::Message {
            actor: crate::EventActor::Assistant,
            text,
            status: MessageStatus::Complete,
            ..
        } if text == "durable result"
    ));
    coordinator.mark_root_message_projected(*message_id).await;
    coordinator.wake_pending_root_messages().await;
    let wake = wake.recv().await.unwrap();
    assert_eq!(wake.delivery, PromptDelivery::Steer);
    assert!(wake.text.contains("durable result"));
    assert!(coordinator.take_root_inbox().await.is_empty());
}

#[tokio::test]
async fn durable_root_wake_retries_after_the_receiver_is_lost() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let store = Arc::new(
        crate::SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
            .await
            .unwrap(),
    );
    store.create_session(root).await.unwrap();
    let coordinator = SubagentCoordinator::new_with_store_and_executor(
        directory.path(),
        root,
        launch(),
        3,
        Arc::new(crate::LocalAgentTurnExecutor::default()),
        store.clone(),
    )
    .unwrap();
    let worker = coordinator
        .table
        .lock()
        .await
        .reserve("worker", &launch())
        .unwrap();
    bind_test_team(directory.path(), store.as_ref(), root, &[worker.session_id]).await;

    // A broadcast can report success even when the receiver disappears
    // before reading it. The durable poll must retry after the grace
    // interval instead of treating that send as an acknowledgement.
    let first_wake = coordinator.subscribe_root_messages();
    coordinator
        .send_message_as(worker.session_id, "/root", "retry this report")
        .await
        .unwrap();
    drop(first_wake);

    tokio::time::sleep(ROOT_MESSAGE_RETRY_INTERVAL + Duration::from_millis(25)).await;
    let mut wake = coordinator.subscribe_root_messages();
    let reports = coordinator.refresh_root_inbox_reports().await.unwrap();
    assert_eq!(reports.len(), 1);
    let (message_id, _) = &reports[0];
    coordinator.mark_root_message_projected(*message_id).await;
    coordinator.wake_pending_root_messages().await;

    let message = wake.recv().await.unwrap();
    assert_eq!(message.delivery, PromptDelivery::Steer);
    assert!(message.text.contains("retry this report"));
}

#[tokio::test]
async fn sibling_messages_use_the_shared_team_directory() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let store = Arc::new(
        crate::SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
            .await
            .unwrap(),
    );
    store.create_session(root).await.unwrap();
    let coordinator = SubagentCoordinator::new_with_store_and_executor(
        directory.path(),
        root,
        launch(),
        3,
        Arc::new(crate::LocalAgentTurnExecutor::default()),
        store.clone(),
    )
    .unwrap();
    let mut table = coordinator.table.lock().await;
    let sender = table.reserve("sender", &launch()).unwrap();
    let recipient = table.reserve("recipient", &launch()).unwrap();
    let (commands, mut received) = mpsc::channel(1);
    let entry = table.entries.get_mut(&recipient.session_id).unwrap();
    entry.snapshot.status = SubagentStatus::Running;
    entry.commands = Some(commands);
    drop(table);
    bind_test_team(
        directory.path(),
        store.as_ref(),
        root,
        &[sender.session_id, recipient.session_id],
    )
    .await;

    coordinator
        .send_message_as(sender.session_id, "recipient", "share the benchmark")
        .await
        .unwrap();
    let HostCommand::Prompt {
        session_id,
        text,
        delivery,
        ..
    } = received.recv().await.unwrap()
    else {
        panic!("expected prompt");
    };
    assert_eq!(session_id, recipient.session_id);
    assert_eq!(delivery, PromptDelivery::Queue);
    assert!(text.contains("Team message from /root/sender"));
    assert!(text.contains("share the benchmark"));

    let broadcast_id = coordinator
        .broadcast_message_as(sender.session_id, "team checkpoint")
        .await
        .unwrap();
    let HostCommand::Prompt { message_id, .. } = received.recv().await.unwrap() else {
        panic!("expected broadcast prompt");
    };
    assert_eq!(message_id, broadcast_id);
    assert_eq!(
        coordinator.take_root_inbox().await[0].message_id,
        broadcast_id
    );
    let workspace = store
        .workspace_store()
        .await
        .unwrap()
        .expect("SQLite session store exposes the canonical workspace projection");
    let binding = store
        .workspace_binding(recipient.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        workspace
            .deliveries_after(binding.workspace_id, binding.participant_id, 0, 10)
            .await
            .unwrap()
            .iter()
            .filter(|delivery| delivery.sequence > 0)
            .count(),
        2
    );
    coordinator
        .acknowledge_message_for_session(recipient.session_id, broadcast_id)
        .await
        .unwrap();
    assert!(
        coordinator
            .unread_messages_for_session(recipient.session_id)
            .await
            .unwrap()
            .iter()
            .all(|message| message.message_id != broadcast_id)
    );
}

#[tokio::test]
async fn focused_human_prompt_and_recall_target_the_exact_child_actor() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let store = Arc::new(
        crate::SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
            .await
            .unwrap(),
    );
    store.create_session(root).await.unwrap();
    let coordinator = SubagentCoordinator::new_with_store_and_executor(
        directory.path(),
        root,
        launch(),
        3,
        Arc::new(crate::LocalAgentTurnExecutor::default()),
        store,
    )
    .unwrap();
    let mut table = coordinator.table.lock().await;
    let child = table.reserve("worker", &launch()).unwrap();
    let (commands, mut received) = mpsc::channel(2);
    let entry = table.entries.get_mut(&child.session_id).unwrap();
    entry.snapshot.status = SubagentStatus::Running;
    entry.commands = Some(commands);
    drop(table);

    let message_id = Uuid::new_v4();
    coordinator
        .prompt_child(
            "worker",
            message_id,
            "inspect the scheduler".to_string(),
            vec![PathBuf::from("/workspace/trace.txt")],
            PromptDelivery::Steer,
        )
        .await
        .unwrap();
    let HostCommand::Prompt {
        session_id,
        message_id: received_id,
        text,
        attachments,
        delivery,
        ..
    } = received.recv().await.unwrap()
    else {
        panic!("expected direct child prompt");
    };
    assert_eq!(session_id, child.session_id);
    assert_eq!(received_id, message_id);
    assert_eq!(text, "inspect the scheduler");
    assert_eq!(attachments, [PathBuf::from("/workspace/trace.txt")]);
    assert_eq!(delivery, PromptDelivery::Steer);

    coordinator
        .recall_child_prompt("worker", Some(message_id))
        .await
        .unwrap();
    let HostCommand::RecallQueuedPrompt {
        session_id,
        message_id: recalled_id,
    } = received.recv().await.unwrap()
    else {
        panic!("expected exact child prompt recall");
    };
    assert_eq!(session_id, child.session_id);
    assert_eq!(recalled_id, Some(message_id));
}

#[tokio::test]
async fn broadcast_is_rejected_when_multiplayer_is_disabled() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let store = Arc::new(
        crate::SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
            .await
            .unwrap(),
    );
    let mut disabled = launch();
    disabled.capabilities.multiplayer = false;
    let coordinator = SubagentCoordinator::new_with_store_and_executor(
        directory.path(),
        root,
        disabled,
        1,
        Arc::new(crate::LocalAgentTurnExecutor::default()),
        store,
    )
    .unwrap();
    assert!(
        coordinator
            .broadcast_message_as(root, "blocked")
            .await
            .is_err()
    );
}

#[test]
fn tool_catalog_exposes_one_complete_lifecycle() {
    let names = subagent_tool_specs(CodingProvider::Codex)
        .into_iter()
        .filter_map(|tool| tool["name"].as_str().map(str::to_string))
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        [
            "spawn_agent",
            "list_agents",
            "send_message",
            "followup_task",
            "broadcast_team",
            "list_unread_team_messages",
            "acknowledge_team_message",
            "interrupt_agent",
            "wait_agent"
        ]
    );
}

#[test]
fn shared_work_tools_are_absent_when_the_capability_is_disabled() {
    let names = agent_tool_specs_with_capabilities(CodingProvider::Codex, false, false, None)
        .into_iter()
        .filter_map(|tool| tool["name"].as_str().map(str::to_string))
        .collect::<Vec<_>>();
    assert!(!names.iter().any(|name| is_shared_work_tool(name)));
    assert!(!names.iter().any(|name| name == "spawn_agent"));

    let enabled = agent_tool_specs_with_capabilities(CodingProvider::Codex, false, true, None)
        .into_iter()
        .filter_map(|tool| tool["name"].as_str().map(str::to_string))
        .collect::<Vec<_>>();
    assert!(enabled.iter().any(|name| name == "create_shared_work"));
    assert!(enabled.iter().any(|name| name == "request_work_review"));
}

#[test]
fn every_execution_lane_exposes_the_same_borg_control_plane() {
    let common = [
        "consult_model",
        "get_goal",
        "get_plan",
        "update_plan",
        "lsp_diagnostics",
        "list_plugins",
        "read_plugin",
        "get_agent_settings",
        "update_agent_settings",
        "create_plugin",
        "create_extension",
        "list_workflows",
        "run_workflow",
    ];
    for provider in [
        CodingProvider::Codex,
        CodingProvider::Claude,
        CodingProvider::OpenRouter,
        CodingProvider::OpenAiCompatible,
    ] {
        let names = agent_tool_specs_with_capabilities(provider, false, false, None)
            .into_iter()
            .filter_map(|tool| tool["name"].as_str().map(str::to_owned))
            .collect::<Vec<_>>();
        for required in common {
            assert!(
                names.iter().any(|name| name == required),
                "{provider:?} lane is missing Borg tool {required}"
            );
        }
    }
}

#[test]
fn persistent_peer_tool_is_root_only_and_not_recursive() {
    let root_names = agent_tool_specs_with_capabilities_and_consultation(
        CodingProvider::Codex,
        true,
        false,
        None,
        true,
    )
    .into_iter()
    .filter_map(|tool| tool["name"].as_str().map(str::to_owned))
    .collect::<Vec<_>>();
    assert!(root_names.iter().any(|name| name == "consult_peer"));

    let child_names = agent_tool_specs_with_capabilities_and_consultation(
        CodingProvider::Claude,
        true,
        false,
        None,
        false,
    )
    .into_iter()
    .filter_map(|tool| tool["name"].as_str().map(str::to_owned))
    .collect::<Vec<_>>();
    assert!(!child_names.iter().any(|name| name == "consult_peer"));
    assert!(!child_names.iter().any(|name| name == "consult_model"));
}

#[tokio::test]
async fn shared_work_tools_are_idempotent_atomic_and_replayable() {
    let directory = tempdir().unwrap();
    let workspace_id = Uuid::new_v4();
    let human_id = Uuid::new_v4();
    let agent_id = Uuid::new_v4();
    let store = SqliteWorkspaceStore::open(directory.path().join("sessions.sqlite3"))
        .await
        .unwrap();
    store
        .ensure_execution_workspace(
            workspace_id,
            "shared tools",
            human_id,
            "Human",
            agent_id,
            "Agent",
        )
        .await
        .unwrap();
    let tools = SharedWorkToolContext::new(store, workspace_id, agent_id);

    let create_args = json!({
        "title": "Verify boundary delivery",
        "detail": "Exercise the real provider boundary.",
        "idempotency_key": "work:boundary-delivery"
    });
    let created = tools
        .call("create_shared_work", create_args.clone())
        .await
        .unwrap();
    let retried = tools.call("create_shared_work", create_args).await.unwrap();
    assert_eq!(created, retried);
    assert!(
        tools
            .call(
                "create_shared_work",
                json!({
                    "title": "Conflicting payload",
                    "idempotency_key": "work:boundary-delivery"
                }),
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("idempotency conflict")
    );

    let work_id: Uuid = serde_json::from_value(created["kind"]["work"]["id"].clone()).unwrap();
    let claim_args = json!({
        "work_id": work_id,
        "idempotency_key": "claim:boundary-delivery"
    });
    let claim = tools
        .call("claim_shared_work", claim_args.clone())
        .await
        .unwrap();
    assert_eq!(
        claim,
        tools.call("claim_shared_work", claim_args).await.unwrap()
    );
    tools
        .call(
            "request_work_review",
            json!({
                "work_id": work_id,
                "requested_reviewer_id": human_id,
                "instructions": "Review the boundary trace.",
                "idempotency_key": "review-request:boundary-delivery"
            }),
        )
        .await
        .unwrap();

    let replay = tools
        .call("list_shared_work", json!({ "limit": 20 }))
        .await
        .unwrap();
    let events = replay["events"].as_array().unwrap();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0]["kind"]["type"], "work_created");
    assert_eq!(events[1]["kind"]["type"], "work_claimed");
    assert_eq!(events[2]["kind"]["type"], "review_requested");
}

#[test]
fn autonomous_team_defaults_workers_to_low_without_overriding_tool_input() {
    let mut team_launch = launch();
    team_launch.team_policy = Some(crate::TeamPreset::XhighDirectorLowWorkers.policy(
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        std::iter::empty(),
        crate::ProviderId("codex".into()),
    ));
    assert_eq!(
        effective_worker_effort(&team_launch, None).as_deref(),
        Some("low")
    );
    assert_eq!(
        effective_worker_effort(&team_launch, Some("high".into())).as_deref(),
        Some("high")
    );
    team_launch.team_policy = None;
    assert_eq!(
        effective_worker_effort(&team_launch, None).as_deref(),
        Some("high")
    );
}

#[test]
fn autonomous_team_policy_is_visible_in_spawn_tool_metadata() {
    let policy = crate::TeamPreset::XhighDirectorLowWorkers.policy(
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        std::iter::empty(),
        crate::ProviderId("codex".into()),
    );
    let spawn = agent_tool_specs_with_team_policy(CodingProvider::Codex, true, Some(&policy))
        .into_iter()
        .find(|tool| tool["name"] == "spawn_agent")
        .unwrap();
    assert!(
        spawn["description"]
            .as_str()
            .unwrap()
            .contains("Effective autonomous-team policy")
    );
}

#[test]
fn disabled_catalog_omits_subagent_tools() {
    let names = agent_tool_specs_with_subagents(CodingProvider::Codex, false)
        .into_iter()
        .filter_map(|tool| tool["name"].as_str().map(str::to_string))
        .collect::<Vec<_>>();
    assert!(!names.iter().any(|name| name == "spawn_agent"));
    assert!(!names.iter().any(|name| name == "send_message"));
}

#[test]
fn subagent_tool_and_validation_use_the_provider_model_catalog() {
    let catalog = CodingProvider::Codex
        .model_catalog()
        .expect("Codex catalog");
    let spawn = subagent_tool_specs(CodingProvider::Codex)
        .into_iter()
        .find(|tool| tool["name"] == "spawn_agent")
        .expect("spawn_agent tool");
    let description = spawn["description"].as_str().expect("description");
    for (model, _) in catalog.selectable_models {
        assert!(
            description.contains(model),
            "agent-facing description omitted {model}"
        );
        validate_subagent_overrides(CodingProvider::Codex, Some(model), None)
            .expect("catalog model should be accepted");
    }
    assert!(description.contains("gpt-5.6-luna"));
    assert!(
        validate_subagent_overrides(CodingProvider::Codex, Some("not-a-codex-model"), None)
            .is_err()
    );
}

#[test]
fn every_parent_model_can_see_codex_luna_as_a_subagent_option() {
    for parent in [
        CodingProvider::Codex,
        CodingProvider::Claude,
        CodingProvider::OpenRouter,
    ] {
        let spawn = subagent_tool_specs(parent)
            .into_iter()
            .find(|tool| tool["name"] == "spawn_agent")
            .expect("spawn_agent tool");
        assert!(
            spawn["description"]
                .as_str()
                .is_some_and(|description| description.contains("gpt-5.6-luna (Luna)")),
            "parent {parent:?} omitted Luna from its orchestration instructions"
        );
        assert!(
            spawn["inputSchema"]["properties"]["model"]["description"]
                .as_str()
                .is_some_and(|description| description.contains("gpt-5.6-luna")),
            "parent {parent:?} omitted Luna from the model argument metadata"
        );
    }
}

#[tokio::test]
async fn durable_parent_activity_restores_child_topology() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let child_id = Uuid::new_v4();
    let now = Utc::now();
    let mut snapshot = SubagentSnapshot {
        session_id: child_id,
        parent_session_id: root,
        task_name: "/root/review_api".into(),
        status: SubagentStatus::Starting,
        provider: CodingProvider::Codex,
        model: Some("gpt-test".into()),
        effort: Some("high".into()),
        cwd: PathBuf::from("/workspace"),
        created_at: now,
        updated_at: now,
        detail: None,
        final_text: None,
        usage: SubagentUsage::default(),
    };
    let started = SessionEvent::new(
        root,
        1,
        SessionEventKind::SubagentActivity {
            activity: SubagentActivityKind::Started,
            agent: snapshot.clone(),
            event: None,
        },
    );
    snapshot.status = SubagentStatus::Stopped;
    snapshot.detail = Some("done".into());
    let stopped = SessionEvent::new(
        root,
        2,
        SessionEventKind::SubagentActivity {
            activity: SubagentActivityKind::Stopped,
            agent: snapshot.clone(),
            event: None,
        },
    );
    let store = Arc::new(
        crate::SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
            .await
            .unwrap(),
    );
    store.create_session(root).await.unwrap();
    let coordinator = SubagentCoordinator::new_with_store_and_executor(
        directory.path(),
        root,
        launch(),
        3,
        Arc::new(crate::LocalAgentTurnExecutor::default()),
        store,
    )
    .unwrap();
    coordinator
        .restore_from_events(&[started, stopped])
        .await
        .unwrap();

    assert_eq!(
        coordinator
            .resolve_snapshot(child_id.to_string().as_str())
            .await
            .unwrap()
            .status,
        SubagentStatus::Stopped
    );
    assert_eq!(coordinator.list(None).await.len(), 1);

    let message_id = Uuid::new_v4();
    let partial = SessionEvent::new(
        root,
        3,
        SessionEventKind::SubagentActivity {
            activity: SubagentActivityKind::Updated,
            agent: snapshot.clone(),
            event: Some(Box::new(SessionEvent::new(
                child_id,
                0,
                SessionEventKind::Message {
                    message_id,
                    actor: crate::EventActor::Assistant,
                    text: "I".into(),
                    attachments: Vec::new(),
                    status: MessageStatus::InProgress,
                    delivery: None,
                },
            ))),
        },
    );
    coordinator
        .restore_from_events(std::slice::from_ref(&partial))
        .await
        .unwrap();
    assert!(!coordinator.root_message_is_projected(message_id).await);

    let completed = SessionEvent::new(
        root,
        4,
        SessionEventKind::SubagentActivity {
            activity: SubagentActivityKind::Completed,
            agent: snapshot,
            event: Some(Box::new(SessionEvent::new(
                child_id,
                8,
                SessionEventKind::Message {
                    message_id,
                    actor: crate::EventActor::Assistant,
                    text: "I am complete".into(),
                    attachments: Vec::new(),
                    status: MessageStatus::Complete,
                    delivery: None,
                },
            ))),
        },
    );
    coordinator
        .restore_from_events(&[partial, completed])
        .await
        .unwrap();
    assert!(coordinator.root_message_is_projected(message_id).await);
}

#[tokio::test]
async fn restore_mirrors_a_child_stop_journaled_before_the_parent_crashed() {
    let directory = tempdir().unwrap();
    let workspace = directory.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let root = Uuid::new_v4();
    let child_id = Uuid::new_v4();
    let now = Utc::now();
    let parent_event = SessionEvent::new(
        root,
        1,
        SessionEventKind::SubagentActivity {
            activity: SubagentActivityKind::Updated,
            agent: SubagentSnapshot {
                session_id: child_id,
                parent_session_id: root,
                task_name: "/root/review_api".into(),
                status: SubagentStatus::Running,
                provider: CodingProvider::Codex,
                model: Some("gpt-test".into()),
                effort: Some("high".into()),
                cwd: workspace.clone(),
                created_at: now,
                updated_at: now,
                detail: Some("turn phase: provider active".into()),
                final_text: None,
                usage: SubagentUsage::default(),
            },
            event: None,
        },
    );
    let child_path = child_lock_path(directory.path(), child_id);
    let store = Arc::new(
        crate::SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
            .await
            .unwrap(),
    );
    store.create_session(root).await.unwrap();
    store.create_session(child_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        SessionEventKind::SessionConfigured {
            cwd: workspace,
            provider: CodingProvider::Codex,
            model: Some("gpt-test".into()),
            effort: Some("high".into()),
            fast: false,
            response_language: crate::ResponseLanguage::Auto,
            permission_mode: PermissionMode::Manual,
        },
        SessionEventKind::StatusChanged {
            status: SessionStatus::Stopped,
            detail: Some("crash cleanup completed".into()),
        },
    ] {
        store
            .append(SessionEvent::new(child_id, 0, kind))
            .await
            .unwrap();
    }
    let session_store: Arc<dyn SessionStore> = store;
    let coordinator = SubagentCoordinator::new_with_store_and_executor(
        directory.path(),
        root,
        launch(),
        3,
        Arc::new(crate::LocalAgentTurnExecutor::default()),
        session_store,
    )
    .unwrap();
    let updates = coordinator
        .restore_from_events(&[parent_event])
        .await
        .unwrap();

    assert_eq!(
        coordinator
            .resolve_snapshot(&child_id.to_string())
            .await
            .unwrap()
            .status,
        SubagentStatus::Stopped
    );
    assert!(matches!(
        updates.as_slice(),
        [SubagentActivity::Stopped { agent }] if agent.session_id == child_id
    ));
    let idle_writer = crate::SessionWriterLease::try_acquire(&child_path)
        .unwrap()
        .expect("a reconciled stopped child remains dormant");
    drop(idle_writer);
}

#[tokio::test]
async fn restored_live_child_stays_dormant_and_stops_with_its_root() {
    let directory = tempdir().unwrap();
    let workspace = directory.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let root = Uuid::new_v4();
    let child_id = Uuid::new_v4();
    let now = Utc::now();
    let snapshot = SubagentSnapshot {
        session_id: child_id,
        parent_session_id: root,
        task_name: "/root/review_api".into(),
        status: SubagentStatus::Ready,
        provider: CodingProvider::Codex,
        model: Some("gpt-test".into()),
        effort: Some("high".into()),
        cwd: workspace.clone(),
        created_at: now,
        updated_at: now,
        detail: None,
        final_text: Some("ready".into()),
        usage: SubagentUsage::default(),
    };
    let parent_event = SessionEvent::new(
        root,
        1,
        SessionEventKind::SubagentActivity {
            activity: SubagentActivityKind::Completed,
            agent: snapshot,
            event: None,
        },
    );
    let store = Arc::new(
        crate::SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
            .await
            .unwrap(),
    );
    store.create_session(root).await.unwrap();
    store.create_session(child_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        SessionEventKind::SessionConfigured {
            cwd: workspace,
            provider: CodingProvider::Codex,
            model: Some("gpt-test".into()),
            effort: Some("high".into()),
            fast: false,
            response_language: crate::ResponseLanguage::Auto,
            permission_mode: PermissionMode::Manual,
        },
        SessionEventKind::StatusChanged {
            status: SessionStatus::Ready,
            detail: None,
        },
    ] {
        store
            .append(SessionEvent::new(child_id, 0, kind))
            .await
            .unwrap();
    }
    let session_store: Arc<dyn SessionStore> = store.clone();
    let coordinator = SubagentCoordinator::new_with_store_and_executor(
        directory.path(),
        root,
        launch(),
        3,
        Arc::new(crate::LocalAgentTurnExecutor::default()),
        session_store,
    )
    .unwrap();
    let mut activity_rx = coordinator.subscribe();
    coordinator
        .restore_from_events(&[parent_event])
        .await
        .unwrap();
    let child_path = child_lock_path(directory.path(), child_id);
    assert!(store.contains_session(child_id).await.unwrap());
    let restored = coordinator
        .resolve_snapshot(&child_id.to_string())
        .await
        .unwrap();
    assert_eq!(restored.status, SubagentStatus::Ready);
    assert!(
        restored
            .detail
            .as_deref()
            .unwrap()
            .contains("follow up to wake")
    );
    assert_eq!(
        store
            .list_sessions(10)
            .await
            .unwrap()
            .into_iter()
            .map(|session| session.session_id)
            .collect::<Vec<_>>(),
        vec![root]
    );
    coordinator.ensure_child_actor(child_id).await.unwrap();
    assert!(
        crate::SessionWriterLease::try_acquire(&child_path)
            .unwrap()
            .is_none(),
        "explicit wake should own the child writer"
    );
    let terminal_updates = coordinator.stop_all().await;
    assert!(matches!(
        terminal_updates.as_slice(),
        [SubagentActivity::Stopped { agent }] if agent.session_id == child_id
    ));
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if matches!(
                activity_rx.recv().await.unwrap(),
                SubagentActivity::Stopped { .. }
            ) {
                break;
            }
        }
    })
    .await
    .expect("restored child emits stop activity");
    let released_writer = crate::SessionWriterLease::try_acquire(&child_path)
        .unwrap()
        .expect("root stop must release the child writer");
    drop(released_writer);
}
