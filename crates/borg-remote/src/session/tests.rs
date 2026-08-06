use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::json;
use tempfile::tempdir;
use tokio::sync::Notify;

use super::*;
use crate::{AgentCompaction, AgentTurnResult, CodingProvider, PermissionMode};

type RecordedTurns = Arc<Mutex<Vec<(PathBuf, Option<serde_json::Value>)>>>;
type RecordedPromptTurns = Arc<Mutex<Vec<(String, Vec<PathBuf>)>>>;
type RecordedContextTurns = Arc<Mutex<Vec<(String, Option<String>)>>>;
type RecordedProviderTurns =
    Arc<Mutex<Vec<(CodingProvider, Option<String>, Option<String>, String)>>>;
type RecordedCompactionTurns = Arc<Mutex<Vec<(CodingProvider, Option<String>, String)>>>;
type SeenConsultProvider = Arc<Mutex<Vec<(CodingProvider, Option<String>, String)>>>;

fn subscription_prompt_ends_with(prompt: &str, text: &str) -> bool {
    let frame = format_subscription_frame(&format_subscription_actor_value(EventActor::User, text));
    prompt.ends_with(&frame)
}

async fn sqlite_runtime_store(
    root: &tempfile::TempDir,
    session_id: Uuid,
) -> (Arc<dyn SessionStore>, RuntimeSessionStore) {
    let sqlite = Arc::new(
        SqliteSessionStore::open(root.path().join("sessions.sqlite3"))
            .await
            .unwrap(),
    );
    sqlite.create_session(session_id).await.unwrap();
    let store: Arc<dyn SessionStore> = sqlite;
    let runtime = RuntimeSessionStore::new(Arc::clone(&store), Vec::new());
    (store, runtime)
}

#[tokio::test]
async fn durable_session_events_project_once_into_the_bound_workspace() {
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let session_store = Arc::new(
        SqliteSessionStore::open(root.path().join("sessions.sqlite3"))
            .await
            .unwrap(),
    );
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
    let mut runtime = RuntimeSessionStore::new(store.clone(), Vec::new())
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
    let mut runtime = RuntimeSessionStore::new(store.clone(), Vec::new())
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
}

#[tokio::test]
async fn projection_delivery_failure_is_durable_and_does_not_fail_the_session_append() {
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let session_store = Arc::new(
        SqliteSessionStore::open(root.path().join("sessions.sqlite3"))
            .await
            .unwrap(),
    );
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
        RuntimeSessionStore::new(store, Vec::new()).with_workspace_projection(projection);

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
}

/// A rewind forks the session into the parent's workspace under a brand new
/// participant.  Replaying the inherited ancestry there would re-append
/// every parent event and then fail on the first message the new
/// participant was never an audience of.
#[tokio::test]
async fn a_forked_session_never_reprojects_the_inherited_ancestry() {
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let session_store = Arc::new(
        SqliteSessionStore::open(root.path().join("sessions.sqlite3"))
            .await
            .unwrap(),
    );
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
    let mut runtime = RuntimeSessionStore::new(store.clone(), Vec::new())
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
                    "profile": "claude-opus-5@high",
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

struct InterruptibleQueueExecutor {
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
}

struct CommittingSteerExecutor {
    provider: CodingProvider,
    turn_started: Arc<Notify>,
    steer_accepted: Arc<Notify>,
    release_commit: Arc<Notify>,
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

struct HungProviderExecutor;

struct CleanupBarrierExecutor {
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
impl AgentTurnExecutor for CleanupBarrierExecutor {
    async fn execute(
        &self,
        _turn: AgentTurn,
        events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        events
            .send(SessionEventKind::ReasoningDelta {
                text: "provider active".to_string(),
            })
            .await
            .ok();
        self.started.notify_one();
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
    async fn execute(
        &self,
        turn: AgentTurn,
        _events: mpsc::Sender<SessionEventKind>,
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
        if subscription_prompt_ends_with(&turn.prompt, "first") {
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
        events: mpsc::Sender<SessionEventKind>,
        controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        self.turn_started.notify_one();
        let mut controls = controls.expect("active turn has controls");
        if let Some(AgentTurnControl::Steer {
            message_id, ack, ..
        }) = controls.recv().await
        {
            let _ = ack.send(Ok(()));
            self.steer_accepted.notify_one();
            self.release_commit.notified().await;
            let (kind, payload) = match self.provider {
                CodingProvider::Codex => (
                    "item/completed:userMessage".to_string(),
                    json!({
                        "item_type": "userMessage",
                        "client_id": message_id.to_string(),
                    }),
                ),
                CodingProvider::Claude => (
                    "claude.command_lifecycle".to_string(),
                    json!({
                        "type": "command_lifecycle",
                        "command_uuid": Uuid::new_v4().to_string(),
                        "state": "started",
                        "client_user_message_id": message_id.to_string(),
                    }),
                ),
                provider => panic!("unsupported committing steer provider: {provider:?}"),
            };
            events
                .send(SessionEventKind::ProviderEvent {
                    provider: self.provider,
                    kind,
                    payload,
                })
                .await
                .unwrap();
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

        let Some(AgentTurnControl::Steer {
            message_id, ack, ..
        }) = controls.recv().await
        else {
            panic!("boundary retry");
        };
        let _ = ack.send(Ok(()));
        events
            .send(SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "item/completed:userMessage".to_string(),
                payload: json!({
                    "item_type": "userMessage",
                    "client_id": message_id.to_string(),
                }),
            })
            .await
            .unwrap();
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
        _events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        self.turns
            .lock()
            .unwrap()
            .push((turn.prompt.clone(), turn.attachments));
        if subscription_prompt_ends_with(&turn.prompt, "first") {
            self.first_started.notify_one();
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
        _events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        self.prompts
            .lock()
            .unwrap()
            .push((turn.prompt, turn.attachments));
        if self.calls.fetch_add(1, Ordering::AcqRel) == 0 {
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
    let message_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(128);
    let calls = Arc::new(AtomicUsize::new(0));
    let prompts = Arc::new(Mutex::new(Vec::new()));
    let executor = Arc::new(EmptyThenSuccessExecutor {
        calls: Arc::clone(&calls),
        prompts: Arc::clone(&prompts),
    });
    let actor = tokio::spawn({
        let journal_path = journal_path.clone();
        let cwd = root.path().to_path_buf();
        async move {
            run_agent_session_with_executor(
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
            )
            .await
        }
    });

    let mut transitions = Vec::new();
    let mut completed = 0;
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

    let store = SqliteSessionStore::open(root.path().join("sessions.sqlite3"))
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
    assert_eq!(
        transitions,
        [
            (MessageStatus::InProgress, PromptDelivery::Steer),
            (MessageStatus::Queued, PromptDelivery::Queue),
            (MessageStatus::InProgress, PromptDelivery::Queue),
            (MessageStatus::Complete, PromptDelivery::Queue),
        ]
    );
    assert_eq!(prompts.lock().unwrap().len(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn ready_is_emitted_only_after_all_queued_turn_events_are_complete() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let cwd = root.path().to_path_buf();
    let session_id = Uuid::new_v4();
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
    let actor = tokio::spawn(async move {
        run_agent_session_with_executor(
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
}

#[tokio::test(flavor = "current_thread")]
async fn provider_setup_stall_has_a_durable_terminal_boundary() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(64);
    let actor = tokio::spawn({
        let journal_path = journal_path.clone();
        let cwd = root.path().to_path_buf();
        async move {
            run_agent_session_with_executor(
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
            )
            .await
        }
    });

    let mut observed = Vec::new();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(2), event_rx.recv())
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

    let durable = SqliteSessionStore::open(root.path().join("sessions.sqlite3"))
        .await
        .unwrap()
        .read(session_id)
        .await
        .unwrap();
    assert!(durable.iter().any(|event| matches!(
        &event.kind,
        SessionEventKind::TurnCompleted {
            message_id: completed,
            error: Some(error),
            ..
        } if *completed == message_id
            && error.contains("liveness timeout while awaiting provider")
    )));
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
    let actor = tokio::spawn({
        let cwd = root.path().to_path_buf();
        async move {
            run_agent_session_with_executor(
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
            )
            .await
        }
    });

    let session_store = SqliteSessionStore::open(root.path().join("sessions.sqlite3"))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let durable = session_store.read(session_id).await.unwrap_or_default();
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
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("detached consumer can recover the durable timeout boundary");
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), actor)
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
}

#[tokio::test(flavor = "current_thread")]
async fn all_queued_prompts_can_be_recalled_at_the_turn_completion_boundary() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
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
    let actor = tokio::spawn(async move {
        run_agent_session_with_executor(
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
}

#[tokio::test(flavor = "current_thread")]
async fn multiple_queue_mode_prompts_drain_fifo_after_a_natural_turn_boundary() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
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
    let actor = tokio::spawn(async move {
        run_agent_session_with_executor(
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

    // Releasing the first turn must batch all queue-mode prompts at the
    // natural boundary. They are one provider input, in FIFO text order.
    release_first.notify_one();
    tokio::time::timeout(Duration::from_secs(1), async {
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
}

#[tokio::test]
async fn interrupted_turn_reaches_fifo_drain_boundary() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(32);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let provider_sessions = Arc::new(Mutex::new(Vec::new()));
    let called = Arc::new(Notify::new());
    let executor = Arc::new(InterruptibleQueueExecutor {
        seen: Arc::clone(&seen),
        provider_sessions: Arc::clone(&provider_sessions),
        called: Arc::clone(&called),
    });
    let actor = tokio::spawn(async move {
        run_agent_session_with_executor(
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
    tokio::time::timeout(Duration::from_secs(1), called.notified())
        .await
        .expect("first turn starts");
    command_tx
        .send(HostCommand::Interrupt { session_id })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), called.notified())
        .await
        .expect("queued turn starts after interruption");
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();

    let events = std::iter::from_fn(|| event_rx.try_recv().ok()).collect::<Vec<_>>();
    assert!(events.iter().any(|event| matches!(
        &event.kind,
        SessionEventKind::TurnCompleted {
            error: Some(error),
            ..
        } if error == "turn interrupted"
    )));
    assert!(!events.iter().any(|event| matches!(
        &event.kind,
        SessionEventKind::Error { message }
            if message.contains("provider completed without a visible response")
    )));

    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert_eq!(
            seen[0],
            (
                "Borg canonical provider context v2. The history below is a read-only, provider-neutral projection of durable Borg state; answer the current request normally.\n<borg-message>{\"content\":\"first\",\"role\":\"user\"}</borg-message>".to_string(),
                Vec::new()
            )
        );
    assert_eq!(
        seen[1].0,
        "Borg canonical provider context v2. The history below is a read-only, provider-neutral projection of durable Borg state; answer the current request normally.\n<borg-message>{\"content\":\"second [Image 1]\\n\\nthird\",\"role\":\"user\"}</borg-message>"
    );
    assert_eq!(
        seen[1].1,
        [PathBuf::from("/tmp/queued-image.png")],
        "queued image attachments must stay on their FIFO prompt"
    );
    assert_eq!(
        provider_sessions.lock().unwrap().as_slice(),
        [None, Some("provider-session".to_string())],
        "interrupting a Codex turn must preserve its provider thread"
    );
}

#[tokio::test]
async fn interrupt_timeout_cannot_publish_ready_before_provider_cleanup_finishes() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(64);
    let started = Arc::new(Notify::new());
    let cleanup_started = Arc::new(Notify::new());
    let release_cleanup = Arc::new(Notify::new());
    let executor = Arc::new(CleanupBarrierExecutor {
        started: Arc::clone(&started),
        cleanup_started: Arc::clone(&cleanup_started),
        release_cleanup: Arc::clone(&release_cleanup),
        cleanup_calls: AtomicUsize::new(0),
    });
    let actor = tokio::spawn(async move {
        run_agent_session_with_executor(
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
    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("provider starts");
    while event_rx.try_recv().is_ok() {}

    command_tx
        .send(HostCommand::Interrupt { session_id })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(4), cleanup_started.notified())
        .await
        .expect("interrupt timeout enters provider cleanup");

    let before_cleanup = std::iter::from_fn(|| event_rx.try_recv().ok()).collect::<Vec<_>>();
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
}

#[tokio::test]
async fn rejected_multimodal_steer_falls_back_to_the_front_of_the_fifo() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
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
    let actor = tokio::spawn(async move {
        run_agent_session_with_executor(
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

#[tokio::test]
async fn accepted_codex_steer_stays_pending_until_the_user_message_commits() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let followup_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(64);
    let turn_started = Arc::new(Notify::new());
    let steer_accepted = Arc::new(Notify::new());
    let release_commit = Arc::new(Notify::new());
    let executor = Arc::new(CommittingSteerExecutor {
        provider: CodingProvider::Codex,
        turn_started: Arc::clone(&turn_started),
        steer_accepted: Arc::clone(&steer_accepted),
        release_commit: Arc::clone(&release_commit),
    });
    let actor = tokio::spawn(async move {
        run_agent_session_with_executor(
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
        "transport acknowledgement must not hide an uncommitted steer"
    );

    release_commit.notify_one();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .expect("committed steer event arrives")
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
        .send(HostCommand::Interrupt { session_id })
        .await
        .unwrap();
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();
}

#[tokio::test]
async fn accepted_claude_steer_stays_pending_until_the_user_message_commits() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let followup_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(64);
    let turn_started = Arc::new(Notify::new());
    let steer_accepted = Arc::new(Notify::new());
    let release_commit = Arc::new(Notify::new());
    let executor = Arc::new(CommittingSteerExecutor {
        provider: CodingProvider::Claude,
        turn_started: Arc::clone(&turn_started),
        steer_accepted: Arc::clone(&steer_accepted),
        release_commit: Arc::clone(&release_commit),
    });
    let actor = tokio::spawn(async move {
        run_agent_session_with_executor(
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
        "Claude transport acknowledgement must not hide an uncommitted steer"
    );

    release_commit.notify_one();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(5), event_rx.recv())
            .await
            .expect("Claude committed steer event arrives")
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
        .send(HostCommand::Interrupt { session_id })
        .await
        .unwrap();
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();
}

#[tokio::test]
async fn rejected_codex_steer_retries_at_the_next_tool_boundary() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
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
    let actor = tokio::spawn(async move {
        run_agent_session_with_executor(
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
    loop {
        let event = tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .expect("committed retry event arrives")
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
        .send(HostCommand::Interrupt { session_id })
        .await
        .unwrap();
    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();
}

#[tokio::test]
async fn unacknowledged_steer_does_not_block_interrupt_or_fifo_fallback() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(32);
    let turns = Arc::new(Mutex::new(Vec::new()));
    let turn_started = Arc::new(Notify::new());
    let steer_seen = Arc::new(Notify::new());
    let executor = Arc::new(HoldingSteerExecutor {
        turns: Arc::clone(&turns),
        turn_started: Arc::clone(&turn_started),
        steer_seen: Arc::clone(&steer_seen),
    });
    let actor = tokio::spawn(async move {
        run_agent_session_with_executor(
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
    tokio::time::timeout(Duration::from_secs(1), steer_seen.notified())
        .await
        .expect("provider receives steer");

    command_tx
        .send(HostCommand::Interrupt { session_id })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), turn_started.notified())
        .await
        .expect("unacknowledged steer falls back to the FIFO");
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

    let mut transitions = Vec::new();
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
}

#[tokio::test]
async fn recalling_unacknowledged_active_steer_emits_prompt_recalled() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(32);
    let turns = Arc::new(Mutex::new(Vec::new()));
    let turn_started = Arc::new(Notify::new());
    let steer_seen = Arc::new(Notify::new());
    let executor = Arc::new(HoldingSteerExecutor {
        turns,
        turn_started: Arc::clone(&turn_started),
        steer_seen: Arc::clone(&steer_seen),
    });
    let actor = tokio::spawn(async move {
        run_agent_session_with_executor(
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

    let followup_id = Uuid::new_v4();
    command_tx
        .send(HostCommand::Prompt {
            session_id,
            message_id: followup_id,
            text: "recall this follow-up".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), steer_seen.notified())
        .await
        .expect("provider has received the unacknowledged steer");

    command_tx
        .send(HostCommand::RecallQueuedPrompt {
            session_id,
            message_id: Some(followup_id),
        })
        .await
        .unwrap();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .expect("recall event arrives")
            .expect("session remains open");
        if matches!(
            event.kind,
            SessionEventKind::PromptRecalled { message_id, .. } if message_id == followup_id
        ) {
            break;
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
}

#[tokio::test]
async fn session_semantics_are_independent_of_turn_execution_location() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    std::fs::create_dir_all(root.path().join("managed-workspace")).unwrap();
    let session_id = Uuid::new_v4();
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
    let actor = tokio::spawn(async move {
        run_agent_session_with_executor(
            &journal_path,
            session_id,
            launch,
            command_rx,
            event_tx,
            executor,
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
}

#[tokio::test]
async fn compaction_after_provider_switch_rehydrates_the_new_provider_session() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(64);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let compacted = Arc::new(Notify::new());
    let executor = Arc::new(CrossProviderCompactionExecutor {
        seen: Arc::clone(&seen),
        compacted: Arc::clone(&compacted),
    });
    let actor = tokio::spawn({
        let cwd = root.path().to_path_buf();
        async move {
            run_agent_session_with_executor(
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
                    "Borg canonical provider context v2. The history below is a read-only, provider-neutral projection of durable Borg state; answer the current request normally.\n<borg-message>{\"content\":\"Previous conversation summary:\\n\\nretained summary\",\"role\":\"user\"}</borg-message>\n<borg-message>{\"content\":\"continue\",\"role\":\"user\"}</borg-message>".to_string(),
                ),
            ]
        );
}

#[tokio::test]
async fn clear_context_starts_the_next_turn_without_provider_or_retained_context() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(32);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let executor = Arc::new(ContextRecordingExecutor {
        seen: Arc::clone(&seen),
    });
    let actor = tokio::spawn(async move {
        run_agent_session_with_executor(
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

    run_agent_session(
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
    )
    .await
    .unwrap();

    let mut observed = Vec::new();
    while let Some(event) = event_rx.recv().await {
        observed.push(event);
    }
    assert_eq!(observed.len(), 4);
    assert!(matches!(observed[0].kind, SessionEventKind::SessionStarted));
    assert!(matches!(
        observed[1].kind,
        SessionEventKind::SessionConfigured { .. }
    ));
    assert!(matches!(
        observed[2].kind,
        SessionEventKind::StatusChanged {
            status: SessionStatus::Ready,
            ..
        }
    ));
    assert!(matches!(
        observed[3].kind,
        SessionEventKind::StatusChanged {
            status: SessionStatus::Stopped,
            ..
        }
    ));
    let journal_events = SqliteSessionStore::open(root.path().join("sessions.sqlite3"))
        .await
        .unwrap()
        .read(session_id)
        .await
        .unwrap();
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
}

#[tokio::test]
async fn sqlite_store_runs_the_canonical_session_actor() {
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let writer =
        SessionWriterLease::acquire(root.path().join(format!("{session_id}.lock"))).unwrap();
    let store = Arc::new(
        crate::SqliteSessionStore::open(root.path().join("sessions.sqlite3"))
            .await
            .unwrap(),
    );
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
    assert_eq!(stored.len(), 4);
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
}

#[tokio::test]
async fn sqlite_autonomy_job_runs_through_the_session_turn_boundary() {
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let writer =
        SessionWriterLease::acquire(root.path().join(format!("{session_id}.lock"))).unwrap();
    let store = Arc::new(
        crate::SqliteSessionStore::open(root.path().join("sessions.sqlite3"))
            .await
            .unwrap(),
    );
    let autonomy = store.autonomy_store().await.unwrap();
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
}

#[tokio::test]
async fn sqlite_blu_workflow_job_runs_without_blocking_the_session_actor() {
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let writer =
        SessionWriterLease::acquire(root.path().join(format!("{session_id}.lock"))).unwrap();
    let store = Arc::new(
        crate::SqliteSessionStore::open(root.path().join("sessions.sqlite3"))
            .await
            .unwrap(),
    );
    let autonomy = store.autonomy_store().await.unwrap();
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
}

#[tokio::test]
async fn crash_reconciled_child_stop_is_durable_before_resumed_ready() {
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let child_id = Uuid::new_v4();
    let cwd = root.path().to_path_buf();
    let store = Arc::new(
        crate::SqliteSessionStore::open(root.path().join("sessions.sqlite3"))
            .await
            .unwrap(),
    );
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
}

#[tokio::test]
async fn initial_mixed_provider_peer_starts_with_isolated_provider_configuration() {
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let writer =
        SessionWriterLease::acquire(root.path().join(format!("{session_id}.lock"))).unwrap();
    let store = Arc::new(
        SqliteSessionStore::open(root.path().join("sessions.sqlite3"))
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
        *provider == CodingProvider::Claude
            && model.is_none()
            && effort.is_none()
            && subscription_prompt_ends_with(prompt, "peer topic")
    }));

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();
}

#[tokio::test]
async fn model_consultation_dispatches_a_freeform_briefing_to_an_isolated_provider() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
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
        capabilities: Default::default(),
        subagent_concurrency_limit: None,
        extension_skill_roots: Vec::new(),
        team_policy: None,
    };
    let actor = tokio::spawn(async move {
        run_agent_session_with_executor(
            &journal_path,
            session_id,
            launch,
            command_rx,
            event_tx,
            executor,
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
    tokio::time::timeout(Duration::from_secs(1), called.notified())
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
}

#[tokio::test]
async fn goal_state_is_recoverable_from_the_session_journal() {
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let (store, mut journal) = sqlite_runtime_store(&root, session_id).await;
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
    account_goal_tokens(
        &mut journal,
        &event_tx,
        session_id,
        &mut goal,
        &mut active_since,
        100,
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
}

#[tokio::test]
async fn model_can_mark_an_active_goal_blocked() {
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let (store, mut journal) = sqlite_runtime_store(&root, session_id).await;
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
    assert!(!provider_error_is_usage_limited(
        r#"claude SDK API error: overloaded "kind":"overloaded" "status":529"#
    ));
}

#[tokio::test]
async fn usage_limit_failure_stops_an_active_goal() {
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let (store, mut journal) = sqlite_runtime_store(&root, session_id).await;
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
fn codex_app_server_user_message_client_id_commits_a_steer() {
    let message_id = Uuid::new_v4();
    let event = SessionEventKind::ProviderEvent {
        provider: CodingProvider::Codex,
        kind: "item/completed:userMessage".to_string(),
        payload: json!({
            "method": "item/completed",
            "params": {
                "item": {
                    "type": "userMessage",
                    "clientId": message_id.to_string()
                }
            }
        }),
    };

    assert_eq!(committed_codex_user_message_id(&event), Some(message_id));
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

/// ↑ on an empty composer must give back exactly the pending work the
/// provider has not committed. Both an in-flight and transport-accepted
/// steer are still recallable; only the provider's committed user-message
/// event makes it owned by the active turn.
#[test]
fn only_an_uncommitted_steer_is_withdrawable_from_the_active_turn() {
    let rejected_id = Uuid::new_v4();
    let awaiting_id = Uuid::new_v4();
    let accepted_id = Uuid::new_v4();
    let steer = |message_id: Uuid, state: PendingSteerState| PendingSteer {
        prompt: QueuedPrompt {
            message_id,
            text: "steer".to_string(),
            actor: EventActor::User,
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Steer,
            visible: true,
            interrupt_batch: true,
        },
        state,
    };
    let mut pending_steers = VecDeque::from([
        steer(awaiting_id, PendingSteerState::AwaitingAcknowledgement),
        steer(
            rejected_id,
            PendingSteerState::RetryAtBoundary {
                error: "provider refused the steer".to_string(),
            },
        ),
        steer(accepted_id, PendingSteerState::Accepted),
    ]);

    let recalled = recall_withdrawable_steers(&mut pending_steers, Some(accepted_id));
    assert_eq!(
        recalled
            .iter()
            .map(|prompt| prompt.message_id)
            .collect::<Vec<_>>(),
        [accepted_id]
    );
    assert_eq!(pending_steers.len(), 2);

    let recalled = recall_withdrawable_steers(&mut pending_steers, None);
    assert_eq!(
        recalled
            .iter()
            .map(|prompt| prompt.message_id)
            .collect::<Vec<_>>(),
        [awaiting_id, rejected_id]
    );
    assert!(pending_steers.is_empty());
}

#[test]
fn escape_batch_coalesces_queued_prompts_in_fifo_order() {
    let first_image = PathBuf::from("/tmp/first.png");
    let last_image = PathBuf::from("/tmp/last.png");
    let last_id = Uuid::new_v4();
    let mut pending = VecDeque::from([
        QueuedPrompt {
            message_id: Uuid::new_v4(),
            text: "first [Image 1]".to_string(),
            actor: EventActor::User,
            attachments: vec![first_image.clone()],
            output_schema: None,
            delivery: PromptDelivery::Queue,
            visible: true,
            interrupt_batch: true,
        },
        QueuedPrompt {
            message_id: Uuid::new_v4(),
            text: "second".to_string(),
            actor: EventActor::User,
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Queue,
            visible: true,
            interrupt_batch: true,
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
            HostCommand::Prompt { message_id, .. } => Some(*message_id),
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
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let (store, mut runtime) = sqlite_runtime_store(&root, session_id).await;
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
    }]);

    settle_inactive_team_notifications(&mut runtime, &event_tx, session_id, &mut pending)
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
}

#[tokio::test]
async fn inactive_wake_report_is_retained_for_the_root_provider_turn() {
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let (_store, mut runtime) = sqlite_runtime_store(&root, session_id).await;
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
    }]);

    settle_inactive_team_notifications(&mut runtime, &event_tx, session_id, &mut pending)
        .await
        .unwrap();

    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].message_id, message_id);
    assert!(event_rx.try_recv().is_err());
}

#[tokio::test]
async fn turn_boundary_collects_all_emitted_prompts_before_escape() {
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let (_store, mut journal) = sqlite_runtime_store(&root, session_id).await;
    let (event_tx, _event_rx) = mpsc::channel(8);
    let (command_tx, mut command_rx) = mpsc::channel(8);
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
    let mut deferred = VecDeque::new();
    let mut team_message_ids = HashSet::new();
    let interrupted = collect_input_at_turn_boundary(
        &mut journal,
        &event_tx,
        session_id,
        &mut pending,
        &mut command_rx,
        &mut deferred,
        &mut team_message_ids,
    )
    .await
    .unwrap();

    assert!(interrupted);
    assert!(deferred.is_empty());
    assert_eq!(pending.len(), 2);
    coalesce_queued_prompts(&mut pending);
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].message_id, last_id);
    assert_eq!(pending[0].text, "first follow-up\n\nsecond follow-up");
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
        assert!(steers_active_provider_turn(provider, PromptDelivery::Steer));
        assert!(!steers_active_provider_turn(
            provider,
            PromptDelivery::Queue
        ));
    }
}

#[test]
fn claude_steer_commits_only_on_correlated_command_start() {
    let message_id = Uuid::new_v4();
    let provider_event = |payload| SessionEventKind::ProviderEvent {
        provider: CodingProvider::Claude,
        kind: "claude.command_lifecycle".to_string(),
        payload,
    };

    assert_eq!(
        committed_claude_user_message_id(&provider_event(json!({
            "state": "queued",
            "client_user_message_id": message_id,
        }))),
        None
    );
    assert_eq!(
        committed_claude_user_message_id(&SessionEventKind::ProviderEvent {
            provider: CodingProvider::Claude,
            kind: "claude.user".to_string(),
            payload: json!({
                "type": "user",
                "content_block_types": ["text"],
            }),
        }),
        None
    );
    assert_eq!(
        committed_claude_user_message_id(&provider_event(json!({
            "state": "started",
            "client_user_message_id": message_id.to_string(),
        }))),
        Some(message_id)
    );
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

#[test]
fn only_empty_provider_responses_are_eligible_for_automatic_retry() {
    assert!(is_safe_automatic_retry_error(
        "codex returned an empty response"
    ));
    assert!(!is_safe_automatic_retry_error("turn interrupted"));
    assert!(!is_safe_automatic_retry_error("tool execution failed"));
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
fn native_replay_discards_an_interrupted_incomplete_tool_round() {
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
    assert_eq!(replay.len(), 3);
    assert!(matches!(replay[0], ModelMessage::User { .. }));
    assert!(matches!(replay[2], ModelMessage::Tool { .. }));
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

    assert!(subscription_context_needs_compaction(
        &context,
        EventActor::User,
        "continue",
        false
    ));
    assert!(!subscription_context_needs_compaction(
        &context,
        EventActor::User,
        "continue",
        true
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
fn subscription_input_budget_counts_characters_not_utf8_bytes() {
    let text = "🛠️".repeat(SUBSCRIPTION_INPUT_BUDGET_CHARS / 4);
    let prompt = format_subscription_provider_prompt(None, EventActor::User, &text);

    assert!(prompt.len() > SUBSCRIPTION_INPUT_BUDGET_CHARS);
    assert!(prompt.chars().count() <= SUBSCRIPTION_INPUT_BUDGET_CHARS);
}

#[tokio::test]
async fn reusable_subscription_pool_does_not_compact_large_durable_replay() {
    let root = tempdir().unwrap();
    let journal_path = root.path().join("session.lock");
    let session_id = Uuid::new_v4();
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
    let actor = tokio::spawn(async move {
        run_agent_session_with_executor(
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
    assert!(prompt_lengths[1] > SUBSCRIPTION_INPUT_BUDGET_CHARS);
    assert!(
        !provider_input_compacted,
        "a healthy pooled subscription must not compact the full durable replay"
    );

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .unwrap();
    actor.await.unwrap().unwrap();
}

#[test]
fn retained_context_chunking_preserves_every_byte_and_utf8() {
    let context = format!("first αβγ\n{}\nlast", "🛠️".repeat(2_000));
    let chunks = split_retained_context(&context, 97);

    assert!(chunks.iter().all(|chunk| chunk.len() <= 97));
    assert_eq!(chunks.concat(), context);
    assert!(chunks.iter().any(|chunk| chunk.contains("αβγ")));
    assert!(chunks.iter().any(|chunk| chunk.contains("🛠️")));
}

#[test]
fn native_auto_compaction_starts_at_ten_percent_effective_context_remaining() {
    let state = |context_tokens, context_window_tokens| SessionState {
        usage: crate::SessionUsage {
            context_tokens: Some(context_tokens),
            context_window_tokens: Some(context_window_tokens),
            ..crate::SessionUsage::default()
        },
        ..SessionState::default()
    };
    assert!(!native_auto_compaction_needed(&state(89_999, 100_000)));
    assert!(native_auto_compaction_needed(&state(90_000, 100_000)));
    assert!(native_auto_compaction_needed(&state(100_000, 100_000)));
    assert!(!native_auto_compaction_needed(&SessionState::default()));
}

#[test]
fn consultation_profiles_resolve_aliases_and_catalog_models() {
    assert_eq!(
        resolve_consultation_profile("claude").unwrap(),
        (
            CodingProvider::Claude,
            Some("claude-sonnet-5".to_string()),
            None
        )
    );
    assert_eq!(
        resolve_consultation_profile("gpt").unwrap(),
        (
            CodingProvider::Codex,
            Some("gpt-5.6-luna".to_string()),
            None
        )
    );
    assert_eq!(
        resolve_consultation_profile("claude/claude-opus-5").unwrap(),
        (
            CodingProvider::Claude,
            Some("claude-opus-5".to_string()),
            None
        )
    );
    assert_eq!(
        resolve_consultation_profile("claude-opus-5@high").unwrap(),
        (
            CodingProvider::Claude,
            Some("claude-opus-5".to_string()),
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
async fn cancelling_a_turn_resolves_its_pending_approval_as_denied() {
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let (_store, mut journal) = sqlite_runtime_store(&root, session_id).await;
    let (events, mut event_rx) = mpsc::channel(4);
    let mut pending = Some("approval-1".to_string());

    deny_pending_approval(&mut journal, &events, session_id, &mut pending)
        .await
        .unwrap();

    assert!(pending.is_none());
    let event = event_rx.recv().await.unwrap();
    assert!(matches!(
        event.kind,
        SessionEventKind::ApprovalResolved {
            ref approval_id,
            decision: crate::ApprovalDecision::Deny,
        } if approval_id == "approval-1"
    ));
}

#[tokio::test]
async fn cancelling_a_turn_resolves_its_pending_provider_interaction() {
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let (_store, mut journal) = sqlite_runtime_store(&root, session_id).await;
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
}

#[tokio::test]
async fn parent_stream_preserves_full_child_transcript_events() {
    let root = tempdir().unwrap();
    let parent_id = Uuid::new_v4();
    let child_id = Uuid::new_v4();
    let sqlite = Arc::new(
        SqliteSessionStore::open(root.path().join("sessions.sqlite3"))
            .await
            .unwrap(),
    );
    sqlite.create_session(parent_id).await.unwrap();
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
        sqlite.clone(),
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

    let store: Arc<dyn SessionStore> = sqlite;
    let mut journal = RuntimeSessionStore::new(store, Vec::new());
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

    record_subagent_activity(
        &mut journal,
        &events,
        parent_id,
        &coordinator,
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
        child_message(0, "I", MessageStatus::InProgress),
    )
    .await
    .unwrap();
    record_subagent_activity(
        &mut journal,
        &events,
        parent_id,
        &coordinator,
        child_message(8, "I am complete", MessageStatus::Complete),
    )
    .await
    .unwrap();

    let partial = event_rx.recv().await.unwrap();
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

    record_subagent_activity(
        &mut journal,
        &events,
        parent_id,
        &coordinator,
        child_message(8, "I am complete", MessageStatus::Complete),
    )
    .await
    .unwrap();
    assert!(event_rx.try_recv().is_err());
}
