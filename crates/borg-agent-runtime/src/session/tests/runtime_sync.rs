use super::*;
use sqlx::postgres::PgPoolOptions;

const TOOL_ID: &str = "completed-before-stop";

struct SyncExecutor {
    armed: Arc<AtomicBool>,
    before_output: Option<Arc<Notify>>,
    emitted: Arc<Notify>,
}

#[async_trait::async_trait]
impl AgentTurnExecutor for SyncExecutor {
    async fn execute(
        &self,
        _turn: AgentTurn,
        events: mpsc::Sender<SessionEventKind>,
        controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<crate::AgentTurnResult> {
        self.armed.store(true, Ordering::SeqCst);
        if let Some(ready) = &self.before_output {
            ready.notified().await;
        }
        events
            .send(SessionEventKind::ToolStarted {
                tool_call_id: TOOL_ID.into(),
                name: "read_file".into(),
                input: json!({"path": "already-read"}),
                input_ref: None,
                parent_tool_call_id: None,
            })
            .await
            .unwrap();
        events
            .send(SessionEventKind::ToolCompleted {
                tool_call_id: TOOL_ID.into(),
                output: "finished".into(),
                output_ref: None,
                is_error: false,
                input: None,
                input_ref: None,
                parent_tool_call_id: None,
            })
            .await
            .unwrap();
        self.emitted.notify_one();
        let mut controls = controls.unwrap();
        while let Some(control) = controls.recv().await {
            if matches!(control, AgentTurnControl::Interrupt) {
                break;
            }
        }
        Ok(crate::AgentTurnResult {
            provider_session_id: None,
            final_text: String::new(),
        })
    }
}

async fn start_actor(
    root: &Path,
    session_id: Uuid,
    store: Arc<dyn SessionStore>,
    executor: Arc<SyncExecutor>,
    actor_id: &Arc<Mutex<Option<tokio::task::Id>>>,
) -> (
    AbortTask<Result<()>>,
    mpsc::Sender<HostCommand>,
    mpsc::Receiver<SessionEvent>,
) {
    let root_path = root.to_path_buf();
    let writer = SessionWriterLease::acquire(root.join(format!("{session_id}.lock"))).unwrap();
    let (commands, command_rx) = mpsc::channel(8);
    let (events, mut event_rx) = mpsc::channel(128);
    let actor = AbortTask(tokio::spawn(async move {
        run_agent_session_with_store_and_writer(
            &root_path,
            session_id,
            LaunchSession {
                request_id: Uuid::new_v4(),
                cwd: root_path.clone(),
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: None,
                ultrafast: None,
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::FullAccess,
                name: None,
                initial_prompt: None,
                capabilities: crate::SessionCapabilities {
                    resume_paused_goal_on_message: false,
                    ..Default::default()
                },
                subagent_concurrency_limit: None,
                extension_skill_roots: Vec::new(),
                team_policy: None,
            },
            command_rx,
            events,
            executor,
            store,
            writer,
        )
        .await
    }));
    *actor_id.lock().unwrap() = Some(actor.0.id());
    next_matching(&mut event_rx, |kind| {
        matches!(
            kind,
            SessionEventKind::StatusChanged {
                status: SessionStatus::Ready,
                ..
            }
        )
    })
    .await;
    commands
        .send(HostCommand::Prompt {
            session_id,
            message_id: Uuid::new_v4(),
            text: "read the file".into(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Queue,
        })
        .await
        .unwrap();
    (actor, commands, event_rx)
}

async fn next_matching(
    events: &mut mpsc::Receiver<SessionEvent>,
    predicate: impl Fn(&SessionEventKind) -> bool,
) -> SessionEvent {
    loop {
        let event = events.recv().await.expect("actor remains available");
        if predicate(&event.kind) {
            return event;
        }
    }
}

#[tokio::test]
async fn slow_inbox_sweeps_do_not_withhold_completed_tools_until_stop() {
    let root = tempdir().unwrap();
    let (scratch, session_id, postgres, workspace, binding, _) = team_delivery_fixture().await;
    let mut goal = SessionGoal::new("paused goal".into(), None);
    goal.status = GoalStatus::Paused;
    postgres
        .append_batch(vec![
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
                    ultrafast: false,
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::FullAccess,
                    speed_support: Default::default(),
                },
            ),
            SessionEvent::new(session_id, 0, SessionEventKind::GoalUpdated { goal }),
        ])
        .await
        .unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let actor_id = Arc::new(Mutex::new(None));
    let sweep_entered = Arc::new(Notify::new());
    let emitted = Arc::new(Notify::new());
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .before_acquire({
            let armed = armed.clone();
            let actor_id = actor_id.clone();
            let sweep_entered = sweep_entered.clone();
            move |_, _| {
                let delay = armed.load(Ordering::SeqCst)
                    && *actor_id.lock().unwrap() == tokio::task::try_id();
                let sweep_entered = sweep_entered.clone();
                Box::pin(async move {
                    if delay {
                        sweep_entered.notify_one();
                        tokio::time::sleep(ROOT_INBOX_REFRESH_INTERVAL + Duration::from_millis(25))
                            .await;
                    }
                    Ok(true)
                })
            }
        })
        .connect_with((*postgres.pool().connect_options()).clone())
        .await
        .unwrap();
    let store: Arc<dyn SessionStore> = Arc::new(postgres.as_ref().clone().with_workspace_store(
        Arc::new(crate::workspace_postgres::PostgresWorkspaceStore::from_pool(pool.clone())),
    ));
    let executor = Arc::new(SyncExecutor {
        armed: armed.clone(),
        before_output: Some(sweep_entered),
        emitted: emitted.clone(),
    });
    let (mut actor, commands, mut events) =
        start_actor(root.path(), session_id, store.clone(), executor, &actor_id).await;
    tokio::time::timeout(Duration::from_secs(5), emitted.notified())
        .await
        .unwrap();
    commands
        .send(HostCommand::Goal {
            session_id,
            action: crate::GoalAction::Clear,
        })
        .await
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        next_matching(&mut events, |kind| {
            matches!(kind, SessionEventKind::GoalCleared { .. })
        }),
    )
    .await
    .unwrap();
    let completed = tokio::time::timeout(
        Duration::from_secs(3),
        next_matching(&mut events, |kind| {
            matches!(kind, SessionEventKind::ToolCompleted { tool_call_id, .. }
            if tool_call_id == TOOL_ID)
        }),
    )
    .await;
    let mail = if completed.is_ok() {
        let message_id = append_team_message(
            &workspace,
            &binding,
            "inbox still works",
            crate::DeliveryMode::Notify,
        )
        .await;
        Some(
            tokio::time::timeout(
                Duration::from_secs(5),
                next_matching(&mut events, |kind| {
                    matches!(kind, SessionEventKind::AgentMessageReceived { message_id: id, .. }
                if *id == message_id)
                }),
            )
            .await,
        )
    } else {
        None
    };
    armed.store(false, Ordering::SeqCst);
    commands
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), &mut actor.0)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let history = store.read(session_id).await.unwrap();
    pool.close().await;
    scratch.discard().await;
    completed.expect(
        "a completed tool is journaled and delivered before Stop despite slow inbox sweeps",
    );
    assert!(history.iter().any(|event| matches!(&event.kind,
        SessionEventKind::ToolCompleted { tool_call_id, .. } if tool_call_id == TOOL_ID)));
    mail.unwrap()
        .expect("inbox reports still reach the actor after a slow sweep");
}

#[tokio::test]
async fn mid_batch_interrupt_preserves_the_popped_tool_completion() {
    let root = tempdir().unwrap();
    let (scratch, session_id, postgres, _, _, _) = team_delivery_fixture().await;
    let armed = Arc::new(AtomicBool::new(false));
    let actor_id = Arc::new(Mutex::new(None));
    let held = Arc::new(AtomicBool::new(false));
    let first_recorded = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let pool = PgPoolOptions::new().max_connections(1).before_acquire({
        let armed = armed.clone();
        let actor_id = actor_id.clone();
        let held = held.clone();
        let first_recorded = first_recorded.clone();
        let release = release.clone();
        move |connection, _| {
            let inspect = armed.load(Ordering::SeqCst)
                && *actor_id.lock().unwrap() == tokio::task::try_id()
                && !held.load(Ordering::SeqCst);
            let held = held.clone();
            let first_recorded = first_recorded.clone();
            let release = release.clone();
            Box::pin(async move {
                if inspect {
                    let started: bool = sqlx::query_scalar(
                        "select exists(select 1 from session_events where session_id=$1 \
                         and event_kind='tool_started' and event_json #>> '{kind,tool_call_id}'=$2)")
                        .bind(session_id).bind(TOOL_ID).fetch_one(connection).await?;
                    if started && !held.swap(true, Ordering::SeqCst) {
                        first_recorded.notify_one();
                        release.notified().await;
                    }
                }
                Ok(true)
            })
        }
    }).connect_with((*postgres.pool().connect_options()).clone()).await.unwrap();
    let store: Arc<dyn SessionStore> = Arc::new(postgres.as_ref().clone().with_workspace_store(
        Arc::new(crate::workspace_postgres::PostgresWorkspaceStore::from_pool(pool.clone())),
    ));
    let executor = Arc::new(SyncExecutor {
        armed: armed.clone(),
        before_output: None,
        emitted: Arc::new(Notify::new()),
    });
    let (mut actor, commands, mut events) =
        start_actor(root.path(), session_id, store.clone(), executor, &actor_id).await;
    tokio::time::timeout(Duration::from_secs(5), first_recorded.notified())
        .await
        .unwrap();
    commands
        .send(HostCommand::Interrupt { session_id })
        .await
        .unwrap();
    release.notify_one();
    tokio::time::timeout(
        Duration::from_secs(5),
        next_matching(&mut events, |kind| {
            matches!(kind, SessionEventKind::TurnCompleted { .. })
        }),
    )
    .await
    .unwrap();
    armed.store(false, Ordering::SeqCst);
    commands
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), &mut actor.0)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let history = store.read(session_id).await.unwrap();
    let started = history.iter().position(|event| {
        matches!(&event.kind,
        SessionEventKind::ToolStarted { tool_call_id, .. } if tool_call_id == TOOL_ID)
    });
    let completed = history.iter().position(|event| {
        matches!(&event.kind,
        SessionEventKind::ToolCompleted { tool_call_id, .. } if tool_call_id == TOOL_ID)
    });
    let terminal = history
        .iter()
        .position(|event| matches!(event.kind, SessionEventKind::TurnCompleted { .. }));
    pool.close().await;
    scratch.discard().await;
    assert!(started.is_some());
    assert!(
        completed.is_some(),
        "cancellation preserves the event already popped from the batch"
    );
    assert!(started.unwrap() < completed.unwrap() && completed.unwrap() < terminal.unwrap());
}
