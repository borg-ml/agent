//! Failure mode (seen live): after a host restart the director's Team panel
//! showed 2 working children while `list_agents` reported 8 running. Parking,
//! restore and wakes journal snapshot-only rows, so recovery lost each child's
//! delivery cursor and the restarted parent re-journaled every child's whole
//! history, one row per event, before the resumed children's state reached the
//! journal the panel projects. This drives the real parent actor through
//! pause -> restart -> resume and checks the panel's counts on its journal.
use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};

use borg_agent_runtime::{
    AgentTurn, AgentTurnControl, AgentTurnExecutor, AgentTurnResult, CodingProvider, HostCommand,
    LaunchSession, PermissionMode, ResponseLanguage, SessionEvent, SessionEventKind, SessionStatus,
    SessionStore, SessionWriterLease, SubagentActivityKind, SubagentLifecycle, SubagentSnapshot,
    SubagentSourceCursor, SubagentStatus, run_agent_session_with_store_and_writer,
    session_store::postgres::testing,
};
use borg_view::ViewReducer;
use chrono::Utc;
use tokio::sync::mpsc;
use uuid::Uuid;

const HISTORY: usize = 40;

/// A provider that never answers: a resumed child stays mid-turn.
struct HungProvider;

#[async_trait::async_trait]
impl AgentTurnExecutor for HungProvider {
    async fn execute(
        &self,
        _turn: AgentTurn,
        _events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> anyhow::Result<AgentTurnResult> {
        std::future::pending().await
    }
}

fn configured(cwd: &Path) -> SessionEventKind {
    SessionEventKind::SessionConfigured {
        cwd: cwd.to_path_buf(),
        provider: CodingProvider::Codex,
        model: Some("gpt-test".into()),
        effort: Some("high".into()),
        fast: false,
        ultrafast: false,
        response_language: ResponseLanguage::Auto,
        permission_mode: PermissionMode::Manual,
        speed_support: Default::default(),
    }
}

fn snapshot(root: Uuid, child: Uuid, name: &str, cwd: &Path) -> SubagentSnapshot {
    SubagentSnapshot {
        session_id: child,
        parent_session_id: root,
        task_name: format!("/root/{name}"),
        status: SubagentStatus::Running,
        lifecycle: Some(SubagentLifecycle::Running),
        source: None,
        provider: CodingProvider::Codex,
        model: Some("gpt-test".into()),
        effort: Some("high".into()),
        fast: false,
        ultrafast: false,
        desktop: false,
        cwd: cwd.to_path_buf(),
        created_at: Utc::now(),
        updated_at: Utc::now(),
        detail: Some("turn phase: provider active".into()),
        final_text: None,
        usage: Default::default(),
        interrupted_by: None,
    }
}

async fn append(store: &dyn SessionStore, session: Uuid, kind: SessionEventKind) -> SessionEvent {
    store
        .append(SessionEvent::new(session, 0, kind))
        .await
        .unwrap()
}

/// What the previous host left: the child's own journal, every event of it
/// mirrored into the parent, then the snapshot-only row that parked it.
async fn paused_child(
    store: &dyn SessionStore,
    root: Uuid,
    name: &str,
    cwd: &Path,
    mid_task: bool,
) -> (Uuid, u64) {
    let child = Uuid::new_v4();
    store.create_session(child).await.unwrap();
    append(store, child, SessionEventKind::SessionStarted).await;
    append(store, child, configured(cwd)).await;
    let mut agent = snapshot(root, child, name, cwd);
    let mut mirrored = 0;
    for index in 0..HISTORY {
        let idle = !mid_task && index + 1 == HISTORY;
        let event = append(
            store,
            child,
            SessionEventKind::StatusChanged {
                status: if idle {
                    SessionStatus::Ready
                } else {
                    SessionStatus::Running
                },
                lifecycle: None,
                detail: (!idle).then(|| "turn phase: provider active".into()),
            },
        )
        .await;
        mirrored = event.sequence;
        if idle {
            agent.status = SubagentStatus::Ready;
            agent.lifecycle = Some(SubagentLifecycle::Ready);
            agent.detail = None;
        }
        agent.source = Some(SubagentSourceCursor {
            child_sequence: mirrored,
            revision: mirrored,
        });
        append(
            store,
            root,
            SessionEventKind::SubagentActivity {
                activity: SubagentActivityKind::Updated,
                agent: agent.clone(),
                event: Some(Box::new(event)),
            },
        )
        .await;
    }
    agent.status = SubagentStatus::Ready;
    agent.lifecycle = Some(if mid_task {
        SubagentLifecycle::DormantInterrupted
    } else {
        SubagentLifecycle::Dormant
    });
    agent.detail = Some("Paused with the parent session".into());
    agent.source = Some(SubagentSourceCursor {
        child_sequence: mirrored,
        revision: mirrored + 1,
    });
    append(
        store,
        root,
        SessionEventKind::SubagentActivity {
            activity: SubagentActivityKind::Completed,
            agent,
            event: None,
        },
    )
    .await;
    (child, mirrored)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn children_resumed_after_a_restart_count_as_working() {
    let directory = tempfile::tempdir().unwrap();
    let cwd = directory.path().join("workspace");
    std::fs::create_dir_all(&cwd).unwrap();
    let (scratch, store) = testing::session_store().await;
    let store: Arc<dyn SessionStore> = Arc::new(store);
    let root = Uuid::new_v4();
    store.create_session(root).await.unwrap();
    append(store.as_ref(), root, SessionEventKind::SessionStarted).await;
    append(store.as_ref(), root, configured(&cwd)).await;
    let mut mirrored = HashMap::new();
    let mut working = Vec::new();
    for (name, mid_task) in [
        ("build_speed", true),
        ("perf_architect", true),
        ("near_terrain_fast", true),
        ("island_cost_audit", false),
    ] {
        let (child, through) = paused_child(store.as_ref(), root, name, &cwd, mid_task).await;
        mirrored.insert(child, through);
        if mid_task {
            working.push(child);
        }
    }

    // Restart: the parent restores its parked team and resumes the children
    // that were cut off mid-task.
    let writer =
        SessionWriterLease::acquire(directory.path().join(format!("{root}.lock"))).unwrap();
    let (commands, command_rx) = mpsc::channel(4);
    let (event_tx, mut events) = mpsc::channel(1024);
    let actor = tokio::spawn({
        let store = Arc::clone(&store);
        let session_root = directory.path().to_path_buf();
        let cwd = cwd.clone();
        async move {
            run_agent_session_with_store_and_writer(
                &session_root,
                root,
                LaunchSession {
                    request_id: Uuid::new_v4(),
                    cwd,
                    provider: CodingProvider::Codex,
                    model: Some("gpt-test".into()),
                    effort: Some("high".into()),
                    fast: Some(false),
                    ultrafast: None,
                    response_language: ResponseLanguage::Auto,
                    permission_mode: PermissionMode::Manual,
                    name: None,
                    initial_prompt: None,
                    capabilities: Default::default(),
                    subagent_concurrency_limit: Some(8),
                    extension_skill_roots: Vec::new(),
                    team_policy: None,
                },
                command_rx,
                event_tx,
                Arc::new(HungProvider),
                store,
                writer,
            )
            .await
        }
    });

    // Resume: every child cut off mid-task reports a new turn to its parent.
    let mut replayed = Vec::new();
    let mut resumed = Vec::new();
    tokio::time::timeout(Duration::from_secs(60), async {
        while resumed.len() < working.len() {
            let event = events.recv().await.expect("parent actor is running");
            let SessionEventKind::SubagentActivity {
                agent,
                event: Some(child_event),
                ..
            } = &event.kind
            else {
                continue;
            };
            if child_event.sequence <= mirrored[&agent.session_id] {
                replayed.push((agent.task_name.clone(), child_event.sequence));
            } else if event.sequence > 0
                && agent.lifecycle() == SubagentLifecycle::Running
                && !resumed.contains(&agent.session_id)
            {
                resumed.push(agent.session_id);
            }
        }
    })
    .await
    .expect("resumed children reach the parent journal");
    assert!(
        replayed.is_empty(),
        "a restart re-journaled child history the parent already had: {replayed:?}"
    );

    // What the Team panel shows, from the parent's durable journal (as a
    // reattached frontend reads it).
    let mut view = ViewReducer::new(root, "restart".into());
    for event in store.read(root).await.unwrap() {
        view.apply(event);
    }
    assert_eq!(view.core.active_subagent_count(), working.len());
    assert_eq!(view.core.waiting_subagent_count(), 0);
    assert_eq!(view.core.inactive_subagent_count(), 1);

    let drain = tokio::spawn(async move { while events.recv().await.is_some() {} });
    commands
        .send(HostCommand::Stop { session_id: root })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(60), actor)
        .await
        .expect("parent stops")
        .unwrap()
        .unwrap();
    drain.await.unwrap();
    scratch.discard().await;
}
