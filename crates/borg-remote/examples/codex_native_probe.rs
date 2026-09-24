//! Explicit Codex (default) or Claude (`--claude`) subscription integration probe. Uses a temporary Borg session and
//! approves only `cat probe.txt`, then resumes the same durable session.
//! `--controls` instead checks steering and interruption of a temporary process.
use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use borg_remote::{
    AgentTurnExecutor, ApprovalDecision, CodingProvider, ConsultationRequest, HostCommand,
    LaunchSession, LocalAgentTurnExecutor, MessageStatus, ModelAccessContext, PermissionMode,
    PromptDelivery, ResponseLanguage, SessionEventKind, SessionWriterLease,
    run_agent_session_with_store_and_writer,
};
use tokio::sync::mpsc;
use uuid::Uuid;

fn main() -> Result<()> {
    // Match the CLI's worker stack for the full session/subagent runtime.
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(8 * 1024 * 1024)
        .build()?
        .block_on(run())
}

async fn run() -> Result<()> {
    if selected_provider().0 == CodingProvider::Codex {
        ensure!(
            borg_provider::provider::CodexModelProvider::account_identity()
                .await?
                .starts_with("sha256:"),
            "this probe requires an existing ChatGPT subscription login"
        );
    }
    tokio::time::timeout(Duration::from_secs(240), probe())
        .await
        .context("native session probe timed out")?
}

fn selected_provider() -> (CodingProvider, &'static str, &'static str) {
    if std::env::args().any(|arg| arg == "--claude") {
        (CodingProvider::Claude, "claude-sonnet-5", "low")
    } else {
        (
            CodingProvider::Codex,
            borg_provider::codex_product_model(),
            borg_provider::codex_default_effort(),
        )
    }
}

async fn probe() -> Result<()> {
    let (provider, model, effort) = selected_provider();
    if std::env::args().any(|arg| arg == "--children") {
        return child_probe().await;
    }
    if std::env::args().any(|arg| arg == "--controls") {
        ensure!(
            !std::env::args().any(|arg| arg == "--fast"),
            "control probe verifies standard routing only"
        );
        return control_probe().await;
    }
    let fast = std::env::args().any(|arg| arg == "--fast");
    let automatic = std::env::args().any(|arg| arg == "--auto");
    let root = tempfile::tempdir()?;
    let nonce = Uuid::new_v4().to_string();
    tokio::fs::write(root.path().join("probe.txt"), &nonce).await?;
    let session_id = Uuid::new_v4();
    // The probe runs against this machine's configured journal, the same one
    // every other entry point opens.
    let store = Arc::clone(
        borg_remote::session_store::factory::open_resolved(
            &borg_remote::session_store::factory::SessionStoreConfig::from_env(),
        )
        .await?
        .session(),
    );
    for resumed in [false, true] {
        let (commands, command_rx) = mpsc::channel(8);
        let (events, mut event_rx) = mpsc::channel(128);
        let cwd = root.path().to_path_buf();
        let message_id = Uuid::new_v4();
        let actor_store = Arc::clone(&store);
        let writer = SessionWriterLease::acquire(root.path().join("session.lock"))?;
        let actor = tokio::spawn(async move {
            run_agent_session_with_store_and_writer(
                &cwd, session_id,
                LaunchSession {
                    request_id: message_id, cwd: cwd.clone(), provider,
                    model: Some(model.into()),
                    effort: Some(effort.into()), fast: Some(fast),
                    response_language: ResponseLanguage::Auto,
                    permission_mode: if automatic { PermissionMode::Auto } else { PermissionMode::Manual },
                    name: None, initial_prompt: Some(if resumed {
                        "Without using any tools, repeat the exact probe value you read in the previous turn."
                    } else {
                        "This is a read-only integration probe. Use exec exactly once with cmd exactly `cat probe.txt` and action `read probe`. Do not run any other command, discover tools, or change files. Then reply with the exact file contents. Preserve this exact value for the next turn, including through any context compaction."
                    }.into()), capabilities: Default::default(), subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(), team_policy: None,
                }, command_rx, events,
                Arc::new(LocalAgentTurnExecutor::default()),
                actor_store,
                writer,
            ).await
        });
        let result: Result<()> = async {
            let mut generated = false;
            let mut approved = false;
            let mut tool_completed = false;
            let mut native_state = false;
            let mut compacting = false;
            while let Some(event) = event_rx.recv().await {
                match event.kind {
                    SessionEventKind::ProviderEvent { kind, payload, .. }
                        if kind == "native_approval_review" && automatic =>
                    {
                        ensure!(payload["decision"] == "allow", "automatic review rejected the allowed read");
                        approved = true;
                        println!("PASS: structured automatic approval review");
                    }
                    SessionEventKind::ProviderEvent { kind, payload, .. }
                        if kind == "context_compaction" && payload["status"] == "completed" =>
                    {
                        ensure!(
                            compacting && payload["native"] == true,
                            "unexpected compaction route"
                        );
                        ensure!(
                            payload["summary"]
                                .as_str()
                                .unwrap_or_default()
                                .contains(&nonce),
                            "compaction lost the exact probe value"
                        );
                        println!("PASS: Borg-owned account-bound compaction");
                        return Ok(());
                    }
                    SessionEventKind::ProviderEvent { kind, payload, .. }
                        if kind == "context_compaction_failed" =>
                    {
                        anyhow::bail!("native compaction failed: {payload}");
                    }
                    SessionEventKind::ProviderEvent { kind, payload, .. }
                        if kind == "action/preparing" =>
                    {
                        generated = true;
                        ensure!(!resumed, "resumed model unexpectedly requested a tool");
                        println!("Borg generation: {}", payload["label"]);
                    }
                    SessionEventKind::ProviderEvent { kind, payload, .. }
                        if kind == "native_model_message" =>
                    {
                        native_state |= payload.get("provider_state").is_some();
                    }
                    SessionEventKind::ApprovalRequested {
                        approval_id,
                        command,
                        ..
                    } => {
                        let allowed = !automatic && !resumed && command.as_deref() == Some("cat probe.txt");
                        commands
                            .send(HostCommand::Approve {
                                session_id,
                                approval_id,
                                decision: if allowed {
                                    ApprovalDecision::AllowOnce
                                } else {
                                    ApprovalDecision::Deny
                                },
                            })
                            .await?;
                        ensure!(
                            allowed,
                            "probe requested a command outside its read-only allowlist"
                        );
                        ensure!(generated, "approval preceded generation feedback");
                        approved = true;
                    }
                    SessionEventKind::ToolCompleted {
                        is_error, output, ..
                    } => {
                        ensure!(
                            approved && !is_error && output.contains(&nonce),
                            "Borg tool result failed"
                        );
                        tool_completed = true;
                    }
                    SessionEventKind::UsageUpdated {
                        total_tokens,
                        cached_input_tokens,
                        cost_basis,
                        context_window_tokens,
                        ..
                    } => {
                        ensure!(
                            cost_basis == "subscription_equivalent",
                            "native subscription usage lost its cost classification"
                        );
                        ensure!(
                            context_window_tokens.is_some_and(|window| window > 0),
                            "native subscription usage lost the catalog context limit"
                        );
                        println!(
                            "Borg usage: {total_tokens} tokens, {cached_input_tokens} cached input, {context_window_tokens:?} usable context limit"
                        );
                    }
                    SessionEventKind::TurnCompleted {
                        message_id: id,
                        provider_session_id,
                        final_text,
                        error,
                    } if id == message_id => {
                        ensure!(
                            error.is_none(),
                            "native turn failed: {}",
                            error.unwrap_or_default()
                        );
                        ensure!(
                            provider_session_id.is_none(),
                            "unexpected provider-owned conversation"
                        );
                        ensure!(
                            native_state && final_text.contains(&nonce),
                            "durable native replay lost probe state"
                        );
                        ensure!(
                            resumed || tool_completed,
                            "first turn did not execute the approved Borg tool"
                        );
                        if resumed {
                            return Ok(());
                        }
                        compacting = true;
                        commands.send(HostCommand::Compact { session_id }).await?;
                    }
                    _ => {}
                }
            }
            anyhow::bail!("session closed without completing probe")
        }
        .await;
        let _ = commands.send(HostCommand::Stop { session_id }).await;
        actor.await??;
        result?;
        let journal = store.read(session_id).await?;
        let native_outputs = journal
            .iter()
            .filter(|event| {
                matches!(&event.kind,
                    SessionEventKind::ProviderEvent { kind, payload, .. }
                    if kind == "native_model_message" && payload.get("provider_state").is_some()
                )
            })
            .count();
        ensure!(
            native_outputs >= if resumed { 2 } else { 1 },
            "opaque native model state was not persisted in the journal"
        );
        ensure!(journal.iter().any(|event| matches!(&event.kind,
            SessionEventKind::ProviderEvent { kind, .. } if kind == "native_tool_round_completed"
        )), "native tool-round boundary was not persisted");
        if resumed {
            let consultation = LocalAgentTurnExecutor::default()
                .for_session(session_id, store.as_ref(), None)
                .await?
                .context("local executor did not resolve the session route")?
                .consult(ConsultationRequest {
                    access: ModelAccessContext {
                        session_id,
                        store: Some(Arc::clone(&store)),
                        provider_context: None,
                        parent_session_id: None,
                        request_prefix: None,
                    },
                    message_id: Uuid::new_v4(),
                    provider,
                    model: Some(model.into()),
                    effort: Some(effort.into()),
                    cwd: root.path().to_path_buf(),
                    response_language: ResponseLanguage::Auto,
                    prompt: format!(
                        "Reply with exactly this probe value and nothing else: {nonce}"
                    ),
                })
                .await?;
            ensure!(
                consultation.final_text.trim() == nonce,
                "consultation lost its isolated briefing"
            );
            println!("PASS: isolated account-bound native consultation");
        }
        println!(
            "PASS: {}",
            if resumed {
                "durable session restart"
            } else if automatic {
                "Borg tool execution and automatic approval"
            } else {
                "Borg tool execution and manual approval"
            }
        );
    }
    Ok(())
}

async fn control_probe() -> Result<()> {
    let (provider, model, effort) = selected_provider();
    const COMMAND: &str = "/bin/sh -c 'echo $$ > probe.pid; exec sleep 10'";
    for interrupt in [false, true] {
        let root = tempfile::tempdir()?;
        let session_id = Uuid::new_v4();
        let message_id = Uuid::new_v4();
        let steer_id = Uuid::new_v4();
        let nonce = Uuid::new_v4().to_string();
        let (commands, command_rx) = mpsc::channel(8);
        let (events, mut event_rx) = mpsc::channel(128);
        let cwd = root.path().to_path_buf();
        let store = Arc::clone(
            borg_remote::session_store::factory::open_resolved(
                &borg_remote::session_store::factory::SessionStoreConfig::from_env(),
            )
            .await?
            .session(),
        );
        let writer = SessionWriterLease::acquire(root.path().join("session.lock"))?;
        let actor = tokio::spawn(async move {
            run_agent_session_with_store_and_writer(
                &cwd, session_id,
                LaunchSession {
                    request_id: message_id, cwd: cwd.clone(), provider,
                    model: Some(model.into()),
                    effort: Some(effort.into()), fast: Some(false),
                    response_language: ResponseLanguage::Auto, permission_mode: PermissionMode::Manual,
                    name: None, initial_prompt: Some(format!(
                        "This is a control integration probe in a disposable directory. Call exec exactly once with action `wait probe`, cmd exactly `{COMMAND}`, yield_time_ms 10000, and no workdir. Do not request any other tool or command. Afterwards reply DONE, unless the user steers you to a different response."
                    )), capabilities: Default::default(), subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(), team_policy: None,
                }, command_rx, events,
                Arc::new(LocalAgentTurnExecutor::default()),
                store,
                writer,
            ).await
        });
        let result = tokio::time::timeout(Duration::from_secs(90), async {
            let mut generated = false;
            let mut tool_started = false;
            let mut approved = false;
            let mut tool_completed = false;
            let mut queued = false;
            let mut accepted = false;
            let mut process_id = None;
            let mut sent_at: Option<std::time::Instant> = None;
            let mut tick = tokio::time::interval(Duration::from_millis(50));
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        if let Some(sent) = sent_at {
                            ensure!(interrupt || queued || sent.elapsed() < Duration::from_secs(3),
                                "durable steering queue waited for the running command");
                            ensure!(!interrupt || sent.elapsed() < Duration::from_secs(6),
                                "interrupt waited for the running command");
                        } else if approved
                            && let Ok(pid) = tokio::fs::read_to_string(root.path().join("probe.pid")).await
                            && let Ok(pid) = pid.trim().parse::<u32>()
                        {
                            ensure!(process_running(pid).await?, "probe process did not start");
                            process_id = Some(pid);
                            sent_at = Some(std::time::Instant::now());
                            commands.send(if interrupt {
                                HostCommand::Interrupt { session_id }
                            } else {
                                HostCommand::Prompt {
                                    session_id, message_id: steer_id,
                                    text: format!("After the current command finishes, do not use any more tools. Reply exactly {nonce} instead of DONE."),
                                    attachments: Vec::new(), output_schema: None,
                                    delivery: PromptDelivery::Steer,
                                }
                            }).await?;
                        }
                    }
                    event = event_rx.recv() => {
                        let event = event.context("session closed before the control result")?;
                        match event.kind {
                            SessionEventKind::ProviderEvent { kind, .. } if kind == "action/preparing" => {
                                generated = true;
                            }
                            SessionEventKind::ToolStarted { name, input, .. } => {
                                ensure!(generated && !tool_started && name == "exec"
                                    && input["cmd"] == COMMAND && input["yield_time_ms"] == 10000
                                    && input.get("workdir").is_none_or(|value| value.is_null()),
                                    "probe requested a tool outside its temporary-process allowlist");
                                tool_started = true;
                            }
                            SessionEventKind::ApprovalRequested { approval_id, command, .. } => {
                                let allowed = tool_started && !approved && command.as_deref() == Some(COMMAND);
                                commands.send(HostCommand::Approve {
                                    session_id, approval_id,
                                    decision: if allowed { ApprovalDecision::AllowOnce } else { ApprovalDecision::Deny },
                                }).await?;
                                ensure!(allowed, "unexpected process approval");
                                approved = true;
                            }
                            SessionEventKind::Message { message_id: id, status: MessageStatus::Queued,
                                delivery: Some(PromptDelivery::Steer), .. } if id == steer_id => {
                                ensure!(!interrupt && !tool_completed && process_running(process_id.context("steer preceded process start")?).await?,
                                    "steering was not durably queued during process execution");
                                queued = true;
                            }
                            SessionEventKind::Message { message_id: id, status: MessageStatus::Complete,
                                delivery: Some(PromptDelivery::Steer), .. } if id == steer_id => {
                                ensure!(queued && tool_completed, "steer completed before its tool-boundary fold");
                                accepted = true;
                            }
                            SessionEventKind::ToolCompleted { is_error, output, .. } => {
                                ensure!(sent_at.is_some(), "command finished before control delivery (error={is_error}): {output}");
                                tool_completed = true;
                            }
                            SessionEventKind::TurnCompleted { message_id: id, provider_session_id, final_text, error } if id == message_id => {
                                ensure!(provider_session_id.is_none() && sent_at.is_some(), "unexpected turn boundary");
                                if interrupt {
                                    ensure!(!process_running(process_id.context("missing process id")?).await?,
                                        "interrupted turn left its process alive (turn error: {error:?})");
                                    ensure!(matches!(error.as_deref(), Some("turn interrupted" | "native provider turn interrupted")),
                                        "interrupt did not stop the turn: {error:?}");
                                } else {
                                    ensure!(accepted && tool_completed && error.is_none() && final_text.trim() == nonce,
                                        "steering did not affect the same native turn");
                                }
                                return Ok::<_, anyhow::Error>(());
                            }
                            _ => {}
                        }
                    }
                }
            }
        }).await.context("control probe timed out").and_then(|result| result);
        let _ = commands.send(HostCommand::Stop { session_id }).await;
        tokio::time::timeout(Duration::from_secs(5), actor)
            .await
            .context("probe actor cleanup timed out")???;
        result?;
        println!(
            "PASS: live native {}",
            if interrupt {
                "interrupt reaps the running process"
            } else {
                "steering is durably queued during execution and changes the same turn"
            }
        );
    }
    Ok(())
}

async fn child_probe() -> Result<()> {
    use borg_remote::{EventActor, SessionCapabilities, SubagentAction, SubagentControlOutcome};
    let (provider, model, effort) = selected_provider();
    if provider == CodingProvider::Claude {
        borg_provider::provider::ClaudeModelProvider::account_identity(None).await?;
    }
    let capability = borg_remote::ProviderCapability {
        provider,
        installed: true,
        version: None,
        authenticated: true,
        auth_detail: Some("subscription verified by the model adapter".into()),
        auth_methods: vec![borg_remote::ProviderAuthMethod::Subscription],
        can_spawn: true,
        usage: None,
        billing: Some(borg_remote::BillingLane::Subscription),
    };
    let root = tempfile::tempdir()?;
    let session_id = Uuid::new_v4();
    let (commands, command_rx) = mpsc::channel(16);
    let (events, mut event_rx) = mpsc::channel(256);
    let store = Arc::clone(
        borg_remote::session_store::factory::open_resolved(
            &borg_remote::session_store::factory::SessionStoreConfig::from_env(),
        )
        .await?
        .session(),
    );
    let actor_store = Arc::clone(&store);
    let cwd = root.path().to_path_buf();
    let writer = SessionWriterLease::acquire(root.path().join("session.lock"))?;
    let actor = tokio::spawn(async move {
        run_agent_session_with_store_and_writer(
            &cwd,
            session_id,
            LaunchSession {
                request_id: Uuid::new_v4(),
                cwd: cwd.clone(),
                provider,
                model: Some(model.into()),
                effort: Some(effort.into()),
                fast: Some(false),
                response_language: ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
                name: Some("Subscription child control probe".into()),
                initial_prompt: None,
                capabilities: SessionCapabilities {
                    autonomous_team: false,
                    provider_capabilities: vec![capability],
                    ..Default::default()
                },
                subagent_concurrency_limit: Some(2),
                extension_skill_roots: vec![],
                team_policy: None,
            },
            command_rx,
            events,
            Arc::new(LocalAgentTurnExecutor::default()),
            actor_store,
            writer,
        )
        .await
    });
    let result: Result<()> = async {
        for name in ["left", "right"] {
            commands.send(HostCommand::Subagent { session_id, action: SubagentAction::Ensure {
                request_id: Uuid::new_v4(), task_name: name.into(), provider,
                model: Some(model.into()), effort: Some(effort.into()),
            }}).await?;
        }
        let mut agents = std::collections::BTreeMap::new();
        let markers = [Uuid::new_v4().to_string(), Uuid::new_v4().to_string()];
        let steered = Uuid::new_v4().to_string();
        let mut streaming = [false; 2];
        let mut completed = [false; 2];
        let mut prompted = false;
        let mut controlled = false;
        let mut recovered = false;
        let mut helper_pids = std::collections::BTreeSet::new();
        while let Some(event) = event_rx.recv().await {
            match event.kind {
                SessionEventKind::SubagentControl { outcome: SubagentControlOutcome::Failed { message }, .. } => anyhow::bail!("child control failed: {message}"),
                SessionEventKind::SubagentControl { outcome: SubagentControlOutcome::Accepted { agent }, .. } => {
                    agents.insert(agent.task_name.clone(), agent.session_id);
                    if agents.len() == 2 && !prompted {
                        for (index, target) in agents.keys().enumerate() {
                            commands.send(HostCommand::Subagent { session_id, action: SubagentAction::Prompt {
                                request_id: Uuid::new_v4(), target: target.clone(), message_id: Uuid::new_v4(),
                                text: format!("This is a bounded streaming-control probe. Remember my private marker {}. Do not use any tools or send messages. Output the integers 1 through 500, one per line, then DONE. A later user message may change the task.", markers[index]),
                                attachments: vec![], delivery: PromptDelivery::Steer,
                            }}).await?;
                        }
                        prompted = true;
                    }
                }
                SessionEventKind::SubagentActivity { agent, event: Some(child), .. } => {
                    let Some(index) = agents.keys().position(|name| name == &agent.task_name) else { continue; };
                    ensure!(agent.parent_session_id == session_id, "child identity lost its Borg parent");
                    match child.kind {
                        SessionEventKind::ProviderEvent { kind, payload, .. } if kind == "native_model_request" && provider == CodingProvider::Claude => {
                            ensure!(payload["parent_agent_id"] == session_id.to_string() && payload["session_id"] == agent.session_id.to_string(), "model request misattributed its child");
                            let pid = payload["connector_pid"].as_u64().context("missing shared helper pid")?;
                            if helper_pids.insert(pid) { println!("CLAUDE_HELPER_PID {pid}"); }
                        }
                        SessionEventKind::ToolStarted { .. } | SessionEventKind::ApprovalRequested { .. } => anyhow::bail!("child requested a tool in the no-tool probe"),
                        SessionEventKind::MessageDelta { .. } | SessionEventKind::Message { actor: EventActor::Assistant, .. } => streaming[index] = true,
                        SessionEventKind::TurnCompleted { final_text, error, provider_session_id, .. } => {
                            ensure!(controlled && provider_session_id.is_none(), "child ended before independent controls were exercised");
                            if recovered {
                                ensure!(error.is_none() && final_text.trim() == markers[index], "child lost its isolated durable context: {error:?}");
                            } else if index == 0 {
                                ensure!(error.is_none() && final_text.trim() == steered, "child steer did not reach the same Borg turn: {error:?}");
                            } else {
                                ensure!(error.as_deref().is_some_and(|error| error.contains("interrupted")), "selected child did not stop: {error:?}");
                            }
                            completed[index] = true;
                        }
                        _ => {}
                    }
                    if streaming.iter().all(|seen| *seen) && !controlled {
                        let targets = agents.keys().cloned().collect::<Vec<_>>();
                        commands.send(HostCommand::Subagent { session_id, action: SubagentAction::Prompt {
                            request_id: Uuid::new_v4(), target: targets[0].clone(), message_id: Uuid::new_v4(),
                            text: format!("Stop counting. Do not use tools. Reply exactly {steered}."), attachments: vec![], delivery: PromptDelivery::Steer,
                        }}).await?;
                        commands.send(HostCommand::Subagent { session_id, action: SubagentAction::Interrupt {
                            request_id: Uuid::new_v4(), target: targets[1].clone(),
                        }}).await?;
                        controlled = true;
                    }
                    if completed.iter().all(|done| *done) {
                        if recovered { break; }
                        println!("PASS: concurrent child streams, independent steer and interruption");
                        recovered = true;
                        completed = [false; 2];
                        for target in agents.keys() {
                            commands.send(HostCommand::Subagent { session_id, action: SubagentAction::Prompt {
                                request_id: Uuid::new_v4(), target: target.clone(), message_id: Uuid::new_v4(),
                                text: "Without tools, reply with only the private marker I gave you before the counting task.".into(), attachments: vec![], delivery: PromptDelivery::Steer,
                            }}).await?;
                        }
                    }
                }
                _ => {}
            }
        }
        ensure!(recovered && completed.iter().all(|done| *done), "child probe ended prematurely");
        if provider == CodingProvider::Claude { ensure!(helper_pids.len() == 1, "concurrent children used different helpers"); }
        for child in agents.values() {
            let journal = store.read(*child).await?;
            ensure!(journal.iter().any(|event| matches!(&event.kind, SessionEventKind::ProviderEvent { kind, .. } if kind == "native_model_usage")), "child usage audit is missing");
        }
        println!("PASS: child journal recovery, usage attribution, isolated markers, and one shared helper");
        Ok(())
    }.await;
    let _ = commands.send(HostCommand::Stop { session_id }).await;
    tokio::time::timeout(Duration::from_secs(10), actor)
        .await
        .context("child probe cleanup timed out")???;
    result
}

async fn process_running(pid: u32) -> Result<bool> {
    ensure!(pid > 1, "invalid probe process id");
    Ok(tokio::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await?
        .success())
}
