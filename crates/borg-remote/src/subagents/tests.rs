use super::*;
use crate::persistent_runtime::{PersistentRuntimeRegistry, RuntimeHost};
use crate::{PermissionMode, SessionEventKind};
use std::collections::BTreeMap;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicUsize, Ordering};
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
                text: "I am checking the supplied evidence before deciding.".to_string(),
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

#[derive(Clone)]
struct ControlledPeerExecutor {
    calls: Arc<AtomicUsize>,
    first_started: Arc<tokio::sync::Notify>,
    release_first: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl crate::AgentTurnExecutor for ControlledPeerExecutor {
    async fn execute(
        &self,
        turn: crate::AgentTurn,
        events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<crate::AgentTurnControl>>,
    ) -> Result<crate::AgentTurnResult> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        events
            .send(SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::Assistant,
                text: format!("peer progress {call}: {}", turn.prompt.len()),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            })
            .await
            .map_err(|_| anyhow::anyhow!("peer event receiver closed"))?;
        if call == 1 {
            self.first_started.notify_waiters();
            self.release_first.notified().await;
        }
        Ok(crate::AgentTurnResult {
            provider_session_id: Some("controlled-peer-session".to_string()),
            final_text: format!("peer final {call}"),
        })
    }
}

#[derive(Clone, Default)]
struct EmptyPeerExecutor;

#[async_trait::async_trait]
impl crate::AgentTurnExecutor for EmptyPeerExecutor {
    async fn execute(
        &self,
        _turn: crate::AgentTurn,
        _events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<crate::AgentTurnControl>>,
    ) -> Result<crate::AgentTurnResult> {
        Ok(crate::AgentTurnResult {
            provider_session_id: Some("empty-peer-session".to_string()),
            final_text: String::new(),
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

#[tokio::test]
async fn persistent_runtime_supports_a_surf_calibration_notebook() {
    let command = std::env::var("BORG_PYTHON_RUNTIME").unwrap_or_else(|_| {
        if cfg!(windows) {
            "python".to_string()
        } else {
            "python3".to_string()
        }
    });
    if !tokio::process::Command::new(command)
        .arg("--version")
        .output()
        .await
        .is_ok_and(|output| output.status.success())
    {
        return;
    }
    let directory = tempdir().unwrap();
    std::fs::write(
        directory.path().join("reference.jsonl"),
        "{\"tick\":0,\"position\":[0.0,0.0],\"speed\":100.0}\n{\"tick\":1,\"position\":[1.0,0.0],\"speed\":101.0}\n",
    )
    .unwrap();
    let session_id = Uuid::new_v4();
    let dispatcher = AgentToolDispatcher::new(
        SessionGoalTools::disconnected(),
        SessionTodoTools::disconnected(),
        None,
        crate::LspService::new(directory.path()),
        CodingProvider::Codex,
        session_id,
        false,
        None,
        None,
        directory.path().to_path_buf(),
        None,
        None,
        Vec::new(),
        None,
        crate::native_process::ProcessManager::default(),
        PermissionMode::FullAccess,
    );

    dispatcher
        .call(
            "runtime_exec",
            serde_json::json!({
                "code": "import json\nreference = [json.loads(line) for line in borg.read('reference.jsonl')['text'].splitlines()]\ndef compare(candidate):\n    errors = [((row['position'][0] - ref['position'][0]) ** 2 + (row['position'][1] - ref['position'][1]) ** 2) ** 0.5 for row, ref in zip(candidate, reference)]\n    return {'ticks': len(errors), 'position_rmse': (sum(error * error for error in errors) / len(errors)) ** 0.5, 'position_max': max(errors), 'first_divergence': next((index for index, error in enumerate(errors) if error > 0.001), None)}"
            }),
        )
        .await
        .unwrap();
    let metrics = dispatcher
        .call(
            "runtime_exec",
            serde_json::json!({
                "code": "candidate = [{'position': [0.0, 0.0]}, {'position': [1.25, 0.0]}]\ncompare(candidate)"
            }),
        )
        .await
        .unwrap();

    assert_eq!(metrics["persistent"], true);
    assert_eq!(metrics["value"]["ticks"], 2);
    assert_eq!(metrics["value"]["first_divergence"], 1);
    assert!(metrics["value"]["position_max"].as_f64().unwrap() > 0.24);
}

#[tokio::test]
async fn dispatcher_can_select_the_optional_bun_javascript_runtime() {
    let command = std::env::var("BORG_BUN_RUNTIME").unwrap_or_else(|_| "bun".to_string());
    if !tokio::process::Command::new(command)
        .arg("--version")
        .output()
        .await
        .is_ok_and(|output| output.status.success())
    {
        return;
    }
    let directory = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let dispatcher = AgentToolDispatcher::new(
        SessionGoalTools::disconnected(),
        SessionTodoTools::disconnected(),
        None,
        crate::LspService::new(directory.path()),
        CodingProvider::Codex,
        session_id,
        false,
        None,
        None,
        directory.path().to_path_buf(),
        None,
        None,
        Vec::new(),
        None,
        crate::native_process::ProcessManager::default(),
        PermissionMode::FullAccess,
    );
    let result = dispatcher
        .call(
            "runtime_exec",
            json!({
                "runtime": "javascript",
                "code": "answer = 40\nanswer = answer + 2\nanswer"
            }),
        )
        .await
        .unwrap();
    assert_eq!(result["runtime"], "javascript");
    assert_eq!(result["value"], 42);
}

#[tokio::test]
async fn dispatcher_and_python_share_the_canonical_lossless_history_query() {
    let command = std::env::var("BORG_PYTHON_RUNTIME").unwrap_or_else(|_| {
        if cfg!(windows) {
            "python".to_string()
        } else {
            "python3".to_string()
        }
    });
    if !tokio::process::Command::new(command)
        .arg("--version")
        .output()
        .await
        .is_ok_and(|output| output.status.success())
    {
        return;
    }
    let directory = tempdir().unwrap();
    let store = crate::SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
        .await
        .unwrap();
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::User,
                text: "durable alpha evidence".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        ))
        .await
        .unwrap();
    let autonomy = store.autonomy_store().await.unwrap();
    let dispatcher = AgentToolDispatcher::new(
        SessionGoalTools::disconnected(),
        SessionTodoTools::disconnected(),
        None,
        crate::LspService::new(directory.path()),
        CodingProvider::Codex,
        session_id,
        false,
        None,
        None,
        directory.path().to_path_buf(),
        None,
        Some(autonomy),
        Vec::new(),
        None,
        crate::native_process::ProcessManager::default(),
        PermissionMode::FullAccess,
    );

    let direct = dispatcher
        .call("query_history", json!({ "text": "alpha evidence" }))
        .await
        .unwrap();
    assert_eq!(direct["backend"], "sqlite_fts5");
    assert_eq!(direct["hits"].as_array().unwrap().len(), 1);

    let direct_index = dispatcher
        .call("history_index", json!({ "after_sequence": 0, "limit": 10 }))
        .await
        .unwrap();
    assert_eq!(direct_index["documents"].as_array().unwrap().len(), 1);
    assert_eq!(direct_index["next_after_sequence"], 1);
    assert_eq!(direct_index["page_truncated"], false);
    assert!(direct_index["page_bytes"].as_u64().unwrap() > 0);

    let through_python = dispatcher
        .call(
            "runtime_exec",
            json!({ "code": "borg.history('alpha evidence')['hits'][0]['event']['sequence']" }),
        )
        .await
        .unwrap();
    assert_eq!(through_python["value"], 1);

    let through_history_index = dispatcher
        .call(
            "runtime_exec",
            json!({
                "code": "page = borg.history_index(0, 10)\npage['documents'][0]['content']"
            }),
        )
        .await
        .unwrap();
    assert!(
        through_history_index["value"]
            .as_str()
            .is_some_and(|content| content.contains("alpha evidence"))
    );
}

#[tokio::test]
async fn history_index_reports_oversized_documents_without_exceeding_runtime_budget() {
    let directory = tempdir().unwrap();
    let store = crate::SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
        .await
        .unwrap();
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    let event = SessionEvent::new(
        session_id,
        0,
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::User,
            text: "x".repeat(800_000),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
    );
    let event_id = event.id;
    store.append(event).await.unwrap();

    let page = history_index_response(
        &store,
        session_id,
        HistoryIndexArgs {
            after_sequence: Some(0),
            limit: Some(10),
        },
    )
    .await
    .unwrap();

    assert!(page["documents"].as_array().unwrap().is_empty());
    assert_eq!(page["next_after_sequence"], 0);
    assert_eq!(page["has_more"], true);
    assert_eq!(page["page_truncated"], true);
    assert!(page["page_bytes"].as_u64().unwrap() < 768 * 1024);
    assert_eq!(page["oversized_document"]["event_id"], event_id.to_string());
    assert!(
        page["oversized_document"]["content_bytes"]
            .as_u64()
            .is_some_and(|bytes| bytes > 768 * 1024)
    );
}

#[tokio::test]
async fn persistent_runtime_can_call_only_the_granted_external_mcp_tools() {
    let command = std::env::var("BORG_PYTHON_RUNTIME").unwrap_or_else(|_| {
        if cfg!(windows) {
            "python".to_string()
        } else {
            "python3".to_string()
        }
    });
    if !tokio::process::Command::new(&command)
        .arg("--version")
        .output()
        .await
        .is_ok_and(|output| output.status.success())
    {
        return;
    }
    let directory = tempdir().unwrap();
    let dispatcher = AgentToolDispatcher::new(
        SessionGoalTools::disconnected(),
        SessionTodoTools::disconnected(),
        None,
        crate::LspService::new(directory.path()),
        CodingProvider::Codex,
        Uuid::new_v4(),
        false,
        None,
        None,
        directory.path().to_path_buf(),
        None,
        None,
        Vec::new(),
        None,
        crate::native_process::ProcessManager::default(),
        PermissionMode::FullAccess,
    );
    let script = r#"
read _initialize
printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2024-11-05","capabilities":{},"serverInfo":{"name":"fake","version":"1"}}}'
read _initialized
read _list
printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"tools":[{"name":"lookup","description":"Lookup","inputSchema":{"type":"object"}}]}}'
read _call
printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"content":[{"type":"text","text":"retrieved"}]}}'
read _call
printf '%s\n' '{"jsonrpc":"2.0","id":4,"result":{"content":[{"type":"text","text":"retrieved"}]}}'
"#;
    dispatcher
        .configure_runtime_mcp(vec![borg_provider::mcp::ExternalMcpServer {
            name: "retrieval".to_string(),
            command: "sh".to_string(),
            args: vec!["-c".to_string(), script.to_string()],
            env: BTreeMap::new(),
            allowed_tools: vec!["lookup".to_string()],
        }])
        .await
        .unwrap();

    let result = dispatcher
        .call(
            "runtime_exec",
            json!({
                "code": "tools = borg.mcp_tools()\nresponse = borg.mcp('mcp__retrieval__lookup', {'query': 'alpha'})\n{'tool': tools[0]['name'], 'text': response['content'][0]['text']}"
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        result["value"],
        json!({"tool": "mcp__retrieval__lookup", "text": "retrieved"})
    );

    dispatcher
        .call(
            "create_retrieval_adapter",
            json!({
                "id": "mcp-ranker",
                "description": "Use the granted product search boundary",
                "source": "def retrieve(query):\n    response = borg.mcp('mcp__retrieval__lookup', {'query': query})\n    return {'query': query, 'text': response['content'][0]['text']}\n"
            }),
        )
        .await
        .unwrap();
    let adapter_result = dispatcher
        .call(
            "runtime_exec",
            json!({
                "code": "borg.retrieval_adapter('mcp-ranker', 'beta')"
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        adapter_result["value"],
        json!({"query": "beta", "text": "retrieved"})
    );
}

#[tokio::test]
async fn extension_mcp_grant_is_available_through_the_persistent_environment_binding() {
    let command = std::env::var("BORG_PYTHON_RUNTIME").unwrap_or_else(|_| {
        if cfg!(windows) {
            "python".to_string()
        } else {
            "python3".to_string()
        }
    });
    if !tokio::process::Command::new(&command)
        .arg("--version")
        .output()
        .await
        .is_ok_and(|output| output.status.success())
    {
        return;
    }
    let directory = tempdir().unwrap();
    let dispatcher = AgentToolDispatcher::new(
        SessionGoalTools::disconnected(),
        SessionTodoTools::disconnected(),
        None,
        crate::LspService::new(directory.path()),
        CodingProvider::Codex,
        Uuid::new_v4(),
        false,
        None,
        None,
        directory.path().to_path_buf(),
        None,
        None,
        Vec::new(),
        None,
        crate::native_process::ProcessManager::default(),
        PermissionMode::FullAccess,
    );
    dispatcher.configure_runtime_mcp(Vec::new()).await.unwrap();
    let script = r#"
read _initialize
printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2024-11-05","capabilities":{},"serverInfo":{"name":"surf","version":"1"}}}'
read _initialized
read _list
printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"tools":[{"name":"map.generate","description":"Generate a map","inputSchema":{"type":"object"}}]}}'
read _call
printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"content":[{"type":"text","text":"tick advanced"}]}}'
"#;
    dispatcher
        .configure_runtime_mcp_extensions(vec![borg_provider::mcp::ExternalMcpServer {
            name: "surf-lab__lab".to_string(),
            command: "sh".to_string(),
            args: vec!["-c".to_string(), script.to_string()],
            env: BTreeMap::new(),
            allowed_tools: vec!["map.generate".to_string()],
        }])
        .await
        .unwrap();

    let result = dispatcher
        .call(
            "runtime_exec",
            json!({
                "code": "env = borg.environment('surf-lab', 'lab')\ntools = env.tools()\nresponse = env.call('map.generate', {'seed': 7})\n{'tool': tools[0]['name'], 'text': response['content'][0]['text']}"
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        result["value"],
        json!({"tool": "mcp__surf_lab__lab__map_generate", "text": "tick advanced"})
    );
}

#[tokio::test]
async fn read_only_runtime_allows_scoped_semantic_search_only() {
    let command = std::env::var("BORG_PYTHON_RUNTIME").unwrap_or_else(|_| {
        if cfg!(windows) {
            "python".to_string()
        } else {
            "python3".to_string()
        }
    });
    if !tokio::process::Command::new(&command)
        .arg("--version")
        .output()
        .await
        .is_ok_and(|output| output.status.success())
    {
        return;
    }
    let directory = tempdir().unwrap();
    let dispatcher = AgentToolDispatcher::new(
        SessionGoalTools::disconnected(),
        SessionTodoTools::disconnected(),
        None,
        crate::LspService::new(directory.path()),
        CodingProvider::Codex,
        Uuid::new_v4(),
        false,
        None,
        None,
        directory.path().to_path_buf(),
        None,
        None,
        Vec::new(),
        None,
        crate::native_process::ProcessManager::default(),
        PermissionMode::Manual,
    );
    let script = r#"
read _initialize
printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2024-11-05","capabilities":{},"serverInfo":{"name":"fake","version":"1"}}}'
read _initialized
read _list
printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"tools":[{"name":"search_documents","description":"Search","inputSchema":{"type":"object"}}]}}'
read _call
printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"content":[{"type":"text","text":"semantic hit"}]}}'
"#;
    dispatcher
        .configure_runtime_mcp(vec![borg_provider::mcp::ExternalMcpServer {
            name: "borg".to_string(),
            command: "sh".to_string(),
            args: vec!["-c".to_string(), script.to_string()],
            env: BTreeMap::new(),
            allowed_tools: vec!["search_documents".to_string()],
        }])
        .await
        .unwrap();

    let runtime = PersistentRuntimeRegistry::default()
        .python_for_session(Uuid::new_v4(), directory.path(), None)
        .await;
    let host: Arc<dyn RuntimeHost> = Arc::new(DispatcherRuntimeHost {
        session_id: Uuid::new_v4(),
        root: directory.path().to_path_buf(),
        allow_effects: false,
        dispatcher: dispatcher.clone(),
        processes: crate::native_process::ProcessManager::default(),
        session_store: None,
        runtime_worker_id: runtime.worker_id(),
    });
    let result = runtime
        .execute("borg.semantic_search('alpha')", None, Arc::clone(&host))
        .await
        .unwrap();
    assert_eq!(result.value["content"][0]["text"], "semantic hit");

    let denied = runtime
        .execute("borg.mcp('mcp__borg__read_document', {})", None, host)
        .await
        .expect_err("unapproved MCP calls must remain permission-gated");
    assert!(
        denied
            .to_string()
            .contains("persistent runtime host mutation")
    );
}

#[tokio::test]
async fn persistent_runtime_rehydrates_explicit_checkpoint_after_worker_restart() {
    let command = std::env::var("BORG_PYTHON_RUNTIME").unwrap_or_else(|_| {
        if cfg!(windows) {
            "python".to_string()
        } else {
            "python3".to_string()
        }
    });
    if !tokio::process::Command::new(command)
        .arg("--version")
        .output()
        .await
        .is_ok_and(|output| output.status.success())
    {
        return;
    }
    let directory = tempdir().unwrap();
    let store = crate::SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
        .await
        .unwrap();
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    let first_autonomy = store.autonomy_store().await.unwrap();
    let first = AgentToolDispatcher::new(
        SessionGoalTools::disconnected(),
        SessionTodoTools::disconnected(),
        None,
        crate::LspService::new(directory.path()),
        CodingProvider::Codex,
        session_id,
        false,
        None,
        None,
        directory.path().to_path_buf(),
        None,
        Some(first_autonomy),
        Vec::new(),
        None,
        crate::native_process::ProcessManager::default(),
        PermissionMode::FullAccess,
    );
    let first_result = first
        .call(
            "runtime_exec",
            json!({
                "code": "borg.checkpoint('v1', {'answer': 42, 'cursor': 7})"
            }),
        )
        .await
        .unwrap();
    assert_eq!(first_result["recovered_from_manifest"], false);
    assert_eq!(first_result["execution_count"], 1);
    drop(first);

    let second_autonomy = store.autonomy_store().await.unwrap();
    let second = AgentToolDispatcher::new(
        SessionGoalTools::disconnected(),
        SessionTodoTools::disconnected(),
        None,
        crate::LspService::new(directory.path()),
        CodingProvider::Codex,
        session_id,
        false,
        None,
        None,
        directory.path().to_path_buf(),
        None,
        Some(second_autonomy),
        Vec::new(),
        None,
        crate::native_process::ProcessManager::default(),
        PermissionMode::FullAccess,
    );
    let status = second
        .call("runtime_exec", json!({ "code": "borg.runtime_status()" }))
        .await
        .unwrap();
    assert_eq!(status["recovered_from_manifest"], true);
    assert_eq!(status["value"]["manifest"]["status"], "running");
    assert_eq!(status["value"]["checkpoints"][0]["key"], "v1");

    let automatically_rehydrated = second
        .call("runtime_exec", json!({ "code": "answer + cursor" }))
        .await
        .unwrap();
    assert_eq!(automatically_rehydrated["value"], 49);

    let restored = second
        .call(
            "runtime_exec",
            json!({ "code": "borg.restore('v1')['state']['answer']" }),
        )
        .await
        .unwrap();
    assert_eq!(restored["value"], 42);
}

#[derive(Clone)]
struct CanonicalDogfoodExecutor {
    phase: Arc<AtomicUsize>,
}

impl CanonicalDogfoodExecutor {
    fn new() -> Self {
        Self {
            phase: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait::async_trait]
impl crate::AgentTurnExecutor for CanonicalDogfoodExecutor {
    async fn execute(
        &self,
        turn: crate::AgentTurn,
        events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<crate::AgentTurnControl>>,
    ) -> Result<crate::AgentTurnResult> {
        let phase = self.phase.fetch_add(1, Ordering::SeqCst);
        let final_text = match phase {
            0 => {
                let goal = turn
                    .agent_tools
                    .call(
                        "create_goal",
                        json!({
                            "objective": "dogfood the canonical persistent workspace",
                            "token_budget": 10_000
                        }),
                    )
                    .await?;
                let goal_id = goal["goal"]["id"]
                    .as_str()
                    .context("created goal did not return an id")?
                    .to_string();

                let plan = turn
                    .agent_tools
                    .call(
                        "update_plan",
                        json!({
                            "plan": [{
                                "content": "retrieve, verify, checkpoint, and recover durable state",
                                "status": "in_progress"
                            }]
                        }),
                    )
                    .await?;
                let plan_id = plan["items"][0]["id"]
                    .as_str()
                    .context("created plan did not return an id")?
                    .to_string();

                let index = turn
                    .agent_tools
                    .call("history_index", json!({"after_sequence": 0, "limit": 100}))
                    .await?;
                let indexed_event_id = index["documents"]
                    .as_array()
                    .and_then(|documents| {
                        documents.iter().find_map(|document| {
                            document["content"]
                                .as_str()
                                .filter(|content| content.contains("lossless-dogfood"))
                                .and_then(|_| document["event_id"].as_str())
                        })
                    })
                    .context("canonical history index did not contain the prompt")?
                    .to_string();

                let adapter = turn
                    .agent_tools
                    .call(
                        "create_retrieval_adapter",
                        json!({
                            "id": "canonical-dogfood",
                            "description": "Find durable session events through the canonical history index",
                            "source": "def retrieve(query):\n    page = borg.history_index(0, 100)\n    return {'query': query, 'event_ids': [row['event_id'] for row in page['documents'] if query and query in row['content']]}\n",
                            "tests": "def test(retrieve, borg):\n    result = retrieve('lossless-dogfood')\n    assert result['event_ids'], result\n    return {'found': len(result['event_ids'])}\n"
                        }),
                    )
                    .await?;
                let adapter_revision = adapter["revision"]
                    .as_str()
                    .context("created retrieval adapter did not return a revision")?
                    .to_string();

                let tested = turn
                    .agent_tools
                    .call(
                        "runtime_exec",
                        json!({
                            "runtime": "python",
                            "code": "borg.test_retrieval_adapter('canonical-dogfood')"
                        }),
                    )
                    .await?;
                anyhow::ensure!(
                    tested["value"]["passed"] == true,
                    "model-authored retrieval adapter test failed: {tested}"
                );

                let retrieved = turn
                    .agent_tools
                    .call(
                        "runtime_exec",
                        json!({
                            "runtime": "python",
                            "code": "borg.retrieval_adapter('canonical-dogfood', 'lossless-dogfood')"
                        }),
                    )
                    .await?;
                let retrieved_event_id = retrieved["value"]["event_ids"]
                    .as_array()
                    .and_then(|ids| ids.first())
                    .and_then(Value::as_str)
                    .context("retrieval adapter returned no canonical event id")?
                    .to_string();
                anyhow::ensure!(
                    retrieved_event_id == indexed_event_id,
                    "adapter did not preserve the indexed canonical locator"
                );

                let resolved = turn
                    .agent_tools
                    .call(
                        "query_history",
                        json!({"event_id": retrieved_event_id, "limit": 1}),
                    )
                    .await?;
                anyhow::ensure!(
                    resolved["hits"]
                        .as_array()
                        .is_some_and(|hits| !hits.is_empty()),
                    "retrieved history locator did not resolve canonically"
                );

                let checkpoint_state = json!({
                    "goal_id": goal_id,
                    "plan_id": plan_id,
                    "adapter_revision": adapter_revision,
                    "history_event_id": retrieved_event_id,
                    "cursor": index["next_after_sequence"]
                });
                let checkpoint_code = format!(
                    "borg.checkpoint('canonical-dogfood', {})",
                    serde_json::to_string(&checkpoint_state)?
                );
                let checkpoint = turn
                    .agent_tools
                    .call(
                        "runtime_exec",
                        json!({"runtime": "python", "code": checkpoint_code}),
                    )
                    .await?;
                anyhow::ensure!(
                    checkpoint["value"]["key"] == "canonical-dogfood",
                    "runtime checkpoint was not persisted"
                );
                serde_json::to_string(&json!({
                    "phase": 1,
                    "goal_id": goal_id,
                    "plan_id": plan_id,
                    "event_id": retrieved_event_id
                }))?
            }
            1 => {
                let goal = turn.agent_tools.call("get_goal", json!({})).await?;
                anyhow::ensure!(
                    goal["goal"]["status"] == "active",
                    "goal was not recovered as active: {goal}"
                );
                let plan = turn.agent_tools.call("get_plan", json!({})).await?;
                let plan_id = plan["items"][0]["id"]
                    .as_str()
                    .context("recovered plan did not contain its id")?
                    .to_string();

                let status = turn
                    .agent_tools
                    .call(
                        "runtime_exec",
                        json!({
                            "runtime": "python",
                            "code": "borg.runtime_status()"
                        }),
                    )
                    .await?;
                anyhow::ensure!(
                    status["recovered_from_manifest"] == true,
                    "runtime did not report recovery from the prior worker: {status}"
                );
                let automatically_restored = turn
                    .agent_tools
                    .call(
                        "runtime_exec",
                        json!({
                            "runtime": "python",
                            "code": "plan_id"
                        }),
                    )
                    .await?;
                anyhow::ensure!(
                    automatically_restored["value"] == plan_id,
                    "checkpoint namespace was not automatically rehydrated: {automatically_restored}"
                );
                let restored = turn
                    .agent_tools
                    .call(
                        "runtime_exec",
                        json!({
                            "runtime": "python",
                            "code": "borg.restore('canonical-dogfood')"
                        }),
                    )
                    .await?;
                anyhow::ensure!(
                    restored["value"]["state"]["plan_id"] == plan_id,
                    "checkpoint did not restore the durable plan identity: {restored}"
                );

                let retrieved = turn
                    .agent_tools
                    .call(
                        "runtime_exec",
                        json!({
                            "runtime": "python",
                            "code": "borg.retrieval_adapter('canonical-dogfood', 'lossless-dogfood')"
                        }),
                    )
                    .await?;
                anyhow::ensure!(
                    retrieved["value"]["event_ids"]
                        .as_array()
                        .is_some_and(|ids| !ids.is_empty()),
                    "persisted retrieval adapter did not work after restart"
                );

                turn.agent_tools
                    .call(
                        "update_plan",
                        json!({
                            "plan": [{
                                "id": plan_id,
                                "content": "retrieve, verify, checkpoint, and recover durable state",
                                "status": "completed"
                            }]
                        }),
                    )
                    .await?;
                let completed = turn
                    .agent_tools
                    .call("update_goal", json!({"status": "complete"}))
                    .await?;
                anyhow::ensure!(
                    completed["goal"]["status"] == "complete",
                    "goal did not reach its terminal state: {completed}"
                );
                "{\"phase\":2,\"recovered\":true}".to_string()
            }
            other => anyhow::bail!("unexpected dogfood executor phase {other}"),
        };

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
            .context("dogfood executor event receiver closed")?;
        Ok(crate::AgentTurnResult {
            provider_session_id: Some("canonical-dogfood".to_string()),
            final_text,
        })
    }
}

async fn run_canonical_dogfood_actor(
    directory: &Path,
    session_id: Uuid,
    store: Arc<crate::SqliteSessionStore>,
    executor: Arc<dyn crate::AgentTurnExecutor>,
    send_prompt: bool,
) -> Result<Vec<crate::SessionEvent>> {
    let mut session_launch = launch();
    session_launch.cwd = directory.to_path_buf();
    session_launch.permission_mode = PermissionMode::FullAccess;
    session_launch.capabilities.subagents = false;
    session_launch.capabilities.multiplayer = false;

    let lock_path = directory.join(format!("{session_id}.lock"));
    let writer = crate::SessionWriterLease::acquire(&lock_path)?;
    let (command_tx, command_rx) = mpsc::channel(8);
    let (event_tx, mut event_rx) = mpsc::channel(1_024);
    let session_root = directory.to_path_buf();
    let actor = tokio::spawn(async move {
        crate::run_agent_session_with_store_and_writer(
            &session_root,
            session_id,
            session_launch,
            command_rx,
            event_tx,
            executor,
            store,
            writer,
        )
        .await
    });

    let message_id = send_prompt.then(Uuid::new_v4);
    if let Some(message_id) = message_id {
        command_tx
            .send(HostCommand::Prompt {
                session_id,
                message_id,
                text: "lossless-dogfood needle".to_string(),
                attachments: Vec::new(),
                output_schema: None,
                delivery: PromptDelivery::Queue,
            })
            .await
            .context("send canonical dogfood prompt")?;
    }

    let mut delivered = Vec::new();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(30), event_rx.recv())
            .await
            .context("timed out waiting for canonical dogfood turn")?
            .context("canonical dogfood actor closed its event stream")?;
        let completed = matches!(
            &event.kind,
            SessionEventKind::TurnCompleted {
                message_id: completed_id,
                error: None,
                ..
            } if message_id.is_none_or(|message_id| *completed_id == message_id)
        );
        delivered.push(event);
        if completed {
            break;
        }
    }

    command_tx
        .send(HostCommand::Stop { session_id })
        .await
        .context("stop canonical dogfood actor")?;
    drop(command_tx);
    tokio::time::timeout(Duration::from_secs(30), actor)
        .await
        .context("timed out stopping canonical dogfood actor")??
        .context("canonical dogfood actor task failed")?;
    Ok(delivered)
}

#[tokio::test]
async fn canonical_runtime_dogfood_completes_goal_through_restart() {
    let command = std::env::var("BORG_PYTHON_RUNTIME").unwrap_or_else(|_| {
        if cfg!(windows) {
            "python".to_string()
        } else {
            "python3".to_string()
        }
    });
    if !tokio::process::Command::new(command)
        .arg("--version")
        .output()
        .await
        .is_ok_and(|output| output.status.success())
    {
        return;
    }

    let directory = tempdir().unwrap();
    let store = Arc::new(
        crate::SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
            .await
            .unwrap(),
    );
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    let executor = Arc::new(CanonicalDogfoodExecutor::new());

    let first_events = run_canonical_dogfood_actor(
        directory.path(),
        session_id,
        store.clone(),
        executor.clone(),
        true,
    )
    .await
    .unwrap();
    assert!(first_events.iter().any(|event| matches!(
        event.kind,
        SessionEventKind::TurnCompleted { error: None, .. }
    )));

    let second_events =
        run_canonical_dogfood_actor(directory.path(), session_id, store.clone(), executor, false)
            .await
            .unwrap();
    assert!(second_events.iter().any(|event| matches!(
        event.kind,
        SessionEventKind::TurnCompleted { error: None, .. }
    )));

    let state = store.state(session_id).await.unwrap();
    assert_eq!(state.goal.unwrap().status, crate::GoalStatus::Complete);
    assert_eq!(state.todos.len(), 1);
    assert_eq!(state.todos[0].status, crate::PlanItemStatus::Completed);
    assert!(
        store
            .runtime_checkpoint(session_id, Some("canonical-dogfood"))
            .await
            .unwrap()
            .unwrap()
            .state["cursor"]
            .as_u64()
            .is_some_and(|cursor| cursor > 0)
    );
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

#[tokio::test]
async fn canceled_peer_consultation_is_queued_privately_and_cannot_satisfy_the_next_call() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let store = Arc::new(
        crate::SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
            .await
            .unwrap(),
    );
    store.create_session(root).await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let first_started = Arc::new(tokio::sync::Notify::new());
    let release_first = Arc::new(tokio::sync::Notify::new());
    let executor = ControlledPeerExecutor {
        calls: Arc::clone(&calls),
        first_started: Arc::clone(&first_started),
        release_first: Arc::clone(&release_first),
    };
    let mut root_launch = launch();
    root_launch.capabilities.multiplayer = false;
    root_launch.cwd = directory.path().to_path_buf();
    let coordinator = SubagentCoordinator::new_with_store_and_executor(
        directory.path(),
        root,
        root_launch,
        2,
        Arc::new(executor),
        store.clone(),
    )
    .unwrap();

    let started = first_started.notified();
    tokio::pin!(started);
    let first = tokio::spawn({
        let coordinator = coordinator.clone();
        async move {
            coordinator
                .consult_peer(CodingProvider::Codex, None, "first consultation")
                .await
        }
    });
    started.await;
    let sidecar = coordinator.resolve_snapshot("/root/claude").await.unwrap();
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());

    let second = tokio::spawn({
        let coordinator = coordinator.clone();
        async move {
            coordinator
                .consult_peer(CodingProvider::Codex, None, "second consultation")
                .await
        }
    });
    let mut queued_second = false;
    for _ in 0..200 {
        queued_second = store
            .read(sidecar.session_id)
            .await
            .unwrap()
            .iter()
            .any(|event| {
                matches!(
                    &event.kind,
                    SessionEventKind::Message {
                        actor: EventActor::User,
                        text,
                        status: MessageStatus::Queued,
                        ..
                    } if text.contains("second consultation")
                )
            });
        if queued_second {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        queued_second,
        "the second consultation was not safely queued"
    );

    release_first.notify_one();
    let second = tokio::time::timeout(Duration::from_secs(3), second)
        .await
        .expect("second consultation should finish")
        .unwrap()
        .unwrap();
    assert_eq!(second["response"], "peer final 2");
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    let mut abandoned_result = None;
    for _ in 0..200 {
        abandoned_result = coordinator
            .take_root_inbox()
            .await
            .into_iter()
            .find(|message| message.text.contains("peer final 1"));
        if abandoned_result.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let abandoned_result = abandoned_result.expect("abandoned result should reach the director");
    assert_eq!(abandoned_result.delivery, PromptDelivery::Queue);
    assert!(abandoned_result.text.contains("original tool call ended"));
    coordinator.stop("/root/claude").await.unwrap();
}

#[tokio::test]
async fn persistent_peer_empty_turn_fails_at_its_correlated_completion_boundary() {
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
    root_launch.cwd = directory.path().to_path_buf();
    let coordinator = SubagentCoordinator::new_with_store_and_executor(
        directory.path(),
        root,
        root_launch,
        2,
        Arc::new(EmptyPeerExecutor),
        store,
    )
    .unwrap();

    let error = tokio::time::timeout(
        Duration::from_secs(2),
        coordinator.consult_peer(CodingProvider::Codex, None, "return no answer"),
    )
    .await
    .expect("empty peer turn should fail immediately")
    .unwrap_err()
    .to_string();
    assert!(error.contains("empty response"));
    coordinator.stop("/root/claude").await.unwrap();
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

    let (provider, model, effort) =
        resolve_persistent_peer_profile(CodingProvider::Codex, Some("claude-sonnet-5@low"))
            .unwrap();
    assert_eq!(provider, CodingProvider::Claude);
    assert_eq!(model.as_deref(), Some("claude-sonnet-5"));
    assert_eq!(effort.as_deref(), Some("low"));
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

    let broadcast = coordinator
        .broadcast_message_as(sender.session_id, "team checkpoint")
        .await
        .unwrap();
    let HostCommand::Prompt { message_id, .. } = received.recv().await.unwrap() else {
        panic!("expected broadcast prompt");
    };
    assert_eq!(message_id, broadcast.message_id);
    assert_eq!(
        coordinator.take_root_inbox().await[0].message_id,
        broadcast.message_id
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
        .acknowledge_message_for_session(recipient.session_id, broadcast.message_id)
        .await
        .unwrap();
    assert!(
        coordinator
            .unread_messages_for_session(recipient.session_id)
            .await
            .unwrap()
            .iter()
            .all(|message| message.message_id != broadcast.message_id)
    );
}

#[tokio::test]
async fn independent_sessions_share_workspace_broadcasts_exactly_once() {
    let directory = tempdir().unwrap();
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    let store = Arc::new(
        crate::SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
            .await
            .unwrap(),
    );
    store
        .create_session_in_workspace(first, workspace_id)
        .await
        .unwrap();
    store
        .create_session_in_workspace(second, workspace_id)
        .await
        .unwrap();
    let workspace = store.workspace_store().await.unwrap().unwrap();
    let human = crate::local_human_participant_id("Human");
    workspace
        .ensure_execution_workspace(
            workspace_id,
            "shared project",
            human,
            "Human",
            first,
            "First root",
        )
        .await
        .unwrap();
    workspace
        .ensure_execution_workspace(
            workspace_id,
            "shared project",
            human,
            "Human",
            second,
            "Second root",
        )
        .await
        .unwrap();
    let first_coordinator = SubagentCoordinator::new_with_store_and_executor(
        directory.path(),
        first,
        launch(),
        1,
        Arc::new(crate::LocalAgentTurnExecutor::default()),
        store.clone(),
    )
    .unwrap();
    let second_coordinator = SubagentCoordinator::new_with_store_and_executor(
        directory.path(),
        second,
        launch(),
        1,
        Arc::new(crate::LocalAgentTurnExecutor::default()),
        store,
    )
    .unwrap();

    let receipt = first_coordinator
        .broadcast_message_as(first, "workspace checkpoint")
        .await
        .unwrap();
    assert!(receipt.recipient_ids.contains(&second));
    assert_eq!(
        receipt.recipient_ids.len(),
        2,
        "the other root and human participant receive the workspace broadcast"
    );
    let unread = second_coordinator
        .unread_messages_for_session(second)
        .await
        .unwrap();
    assert_eq!(unread.len(), 1);
    assert_eq!(unread[0].message_id, receipt.message_id);
    second_coordinator
        .acknowledge_message_for_session(second, receipt.message_id)
        .await
        .unwrap();
    assert!(
        second_coordinator
            .unread_messages_for_session(second)
            .await
            .unwrap()
            .is_empty()
    );

    let roster = first_coordinator
        .call_tool_as(first, "list_workspace_participants", json!({}))
        .await
        .unwrap();
    assert!(
        roster["participants"]
            .as_array()
            .is_some_and(|participants| {
                participants
                    .iter()
                    .any(|entry| entry["participant"]["id"] == second.to_string())
            })
    );
    let direct = first_coordinator
        .call_tool_as(
            first,
            "send_message",
            json!({
                "target": format!("participant:{second}"),
                "message": "participant-addressed checkpoint"
            }),
        )
        .await
        .unwrap();
    assert_eq!(direct["recipient_count"], 1);
    let direct_id: Uuid = serde_json::from_value(direct["message_id"].clone()).unwrap();
    assert!(
        second_coordinator
            .unread_messages_for_session(second)
            .await
            .unwrap()
            .iter()
            .any(|message| message.message_id == direct_id)
    );
}

#[tokio::test]
async fn explicitly_addressed_sessions_get_an_authorized_cross_workspace_channel() {
    let directory = tempdir().unwrap();
    let sender = Uuid::new_v4();
    let recipient = Uuid::new_v4();
    let store = Arc::new(
        crate::SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
            .await
            .unwrap(),
    );
    store.create_session(sender).await.unwrap();
    store.create_session(recipient).await.unwrap();
    let workspace = store.workspace_store().await.unwrap().unwrap();
    let human = crate::local_human_participant_id("Human");
    workspace
        .ensure_execution_workspace(sender, "sender", human, "Human", sender, "Sender")
        .await
        .unwrap();
    workspace
        .ensure_execution_workspace(
            recipient,
            "recipient",
            human,
            "Human",
            recipient,
            "Recipient",
        )
        .await
        .unwrap();
    let sender_coordinator = SubagentCoordinator::new_with_store_and_executor(
        directory.path(),
        sender,
        launch(),
        1,
        Arc::new(crate::LocalAgentTurnExecutor::default()),
        store.clone(),
    )
    .unwrap();
    let recipient_coordinator = SubagentCoordinator::new_with_store_and_executor(
        directory.path(),
        recipient,
        launch(),
        1,
        Arc::new(crate::LocalAgentTurnExecutor::default()),
        store,
    )
    .unwrap();

    let result = sender_coordinator
        .call_tool_as(
            sender,
            "send_message",
            json!({
                "target": format!("session:{recipient}"),
                "message": "cross-workspace handoff"
            }),
        )
        .await
        .unwrap();
    assert_eq!(result["queued"], true);
    assert_eq!(result["recipient_count"], 1);
    assert_eq!(result["dispatched_locally"], false);
    let message_id: Uuid = serde_json::from_value(result["message_id"].clone()).unwrap();
    let unread = recipient_coordinator
        .unread_messages_for_session(recipient)
        .await
        .unwrap();
    assert_eq!(unread.len(), 1);
    assert_eq!(unread[0].message_id, message_id);
    recipient_coordinator
        .acknowledge_message_for_session(recipient, message_id)
        .await
        .unwrap();
    assert!(
        recipient_coordinator
            .unread_messages_for_session(recipient)
            .await
            .unwrap()
            .is_empty()
    );

    let unknown = Uuid::new_v4();
    assert!(
        sender_coordinator
            .call_tool_as(
                sender,
                "send_message",
                json!({"target": format!("session:{unknown}"), "message": "not authorized"}),
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("unknown session message target")
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
            "list_workspace_participants",
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
        "runtime_exec",
        "query_history",
        "history_index",
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
fn update_plan_accepts_legacy_aliases_but_advertises_the_canonical_contract() {
    let args: UpdatePlanArgs = serde_json::from_value(json!({
        "steps": [{"step": "Inspect the release path", "status": "done"}]
    }))
    .unwrap();

    assert_eq!(args.plan.len(), 1);
    assert_eq!(args.plan[0].content, "Inspect the release path");
    assert_eq!(args.plan[0].status, crate::PlanItemStatus::Completed);

    let update_plan = agent_tool_specs(CodingProvider::Codex)
        .into_iter()
        .find(|tool| tool["name"] == "update_plan")
        .expect("update_plan tool spec");
    let schema = &update_plan["inputSchema"];
    assert_eq!(schema["required"], json!(["plan"]));
    assert_eq!(
        schema["properties"]["plan"]["maxItems"],
        crate::session::MAX_PLAN_ITEMS
    );
    assert_eq!(
        schema["properties"]["plan"]["items"]["properties"]["content"]["maxLength"],
        crate::session::MAX_PLAN_ITEM_CONTENT_CHARS
    );
    assert_eq!(
        schema["properties"]["plan"]["items"]["required"],
        json!(["content", "status"])
    );
    let description = update_plan["description"].as_str().unwrap();
    assert!(description.contains("Exact call:"));
    assert!(description.contains("500 characters"));
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
