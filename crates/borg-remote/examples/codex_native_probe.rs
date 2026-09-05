//! Explicit subscription integration probe. Uses a temporary Borg session and
//! approves only `cat probe.txt`, then resumes the same durable session.
//! `--controls` instead checks steering and interruption of a temporary process.
use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use borg_remote::{
    AgentTurnExecutor, ApprovalDecision, CodingProvider, ConsultationRequest, HostCommand,
    LaunchSession, LocalAgentTurnExecutor, MessageStatus, ModelAccessContext, PermissionMode,
    PromptDelivery, ResponseLanguage, SessionEventKind, SessionStore, SqliteSessionStore,
    run_agent_session_with_executor,
};
use tokio::sync::mpsc;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(240), probe())
        .await
        .context("native session probe timed out")?
}

async fn probe() -> Result<()> {
    if std::env::args().any(|arg| arg == "--controls") {
        ensure!(
            !std::env::args().any(|arg| arg == "--fast"),
            "control probe verifies standard routing only"
        );
        return control_probe().await;
    }
    let fast = std::env::args().any(|arg| arg == "--fast");
    let root = tempfile::tempdir()?;
    let nonce = Uuid::new_v4().to_string();
    tokio::fs::write(root.path().join("probe.txt"), &nonce).await?;
    let session_id = Uuid::new_v4();
    for resumed in [false, true] {
        let (commands, command_rx) = mpsc::channel(8);
        let (events, mut event_rx) = mpsc::channel(128);
        let cwd = root.path().to_path_buf();
        let message_id = Uuid::new_v4();
        let actor = tokio::spawn(async move {
            run_agent_session_with_executor(
                &cwd.join("session.lock"), session_id,
                LaunchSession {
                    request_id: message_id, cwd: cwd.clone(), provider: CodingProvider::Codex,
                    model: Some(borg_provider::codex_product_model().into()),
                    effort: Some(borg_provider::codex_default_effort().into()), fast: Some(fast),
                    response_language: ResponseLanguage::Auto, permission_mode: PermissionMode::Manual,
                    name: None, initial_prompt: Some(if resumed {
                        "Without using any tools, repeat the exact probe value you read in the previous turn."
                    } else {
                        "This is a read-only integration probe. Use exec exactly once with cmd exactly `cat probe.txt` and action `read probe`. Do not run any other command, discover tools, or change files. Then reply with the exact file contents. Preserve this exact value for the next turn, including through any context compaction."
                    }.into()), capabilities: Default::default(), subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(), team_policy: None,
                }, command_rx, events,
                Arc::new(LocalAgentTurnExecutor::default()),
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
                        let allowed = !resumed && command.as_deref() == Some("cat probe.txt");
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
        let store = SqliteSessionStore::open(root.path().join("sessions.sqlite3")).await?;
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
                .for_session(session_id, &store)
                .await?
                .context("local executor did not resolve the session route")?
                .consult(ConsultationRequest {
                    access: ModelAccessContext {
                        session_id,
                        store: Some(store.clone()),
                    },
                    message_id: Uuid::new_v4(),
                    provider: CodingProvider::Codex,
                    model: Some(borg_provider::codex_product_model().to_string()),
                    effort: Some(borg_provider::codex_default_effort().to_string()),
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
            } else {
                "Borg tool execution and manual approval"
            }
        );
    }
    Ok(())
}

async fn control_probe() -> Result<()> {
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
        let actor = tokio::spawn(async move {
            run_agent_session_with_executor(
                &cwd.join("session.lock"), session_id,
                LaunchSession {
                    request_id: message_id, cwd: cwd.clone(), provider: CodingProvider::Codex,
                    model: Some(borg_provider::codex_product_model().into()),
                    effort: Some(borg_provider::codex_default_effort().into()), fast: Some(false),
                    response_language: ResponseLanguage::Auto, permission_mode: PermissionMode::Manual,
                    name: None, initial_prompt: Some(format!(
                        "This is a control integration probe in a disposable directory. Call exec exactly once with action `wait probe`, cmd exactly `{COMMAND}`, yield_time_ms 10000, and no workdir. Do not request any other tool or command. Afterwards reply DONE, unless the user steers you to a different response."
                    )), capabilities: Default::default(), subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(), team_policy: None,
                }, command_rx, events,
                Arc::new(LocalAgentTurnExecutor::default()),
            ).await
        });
        let result = tokio::time::timeout(Duration::from_secs(90), async {
            let mut generated = false;
            let mut tool_started = false;
            let mut approved = false;
            let mut tool_completed = false;
            let mut accepted = false;
            let mut process_id = None;
            let mut sent_at: Option<std::time::Instant> = None;
            let mut tick = tokio::time::interval(Duration::from_millis(50));
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        if let Some(sent) = sent_at {
                            ensure!(interrupt || accepted || sent.elapsed() < Duration::from_secs(3),
                                "steering acknowledgement waited for the running command");
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
                            SessionEventKind::Message { message_id: id, status: MessageStatus::Complete,
                                delivery: Some(PromptDelivery::Steer), .. } if id == steer_id => {
                                ensure!(!interrupt && !tool_completed && process_running(process_id.context("steer preceded process start")?).await?,
                                    "steering was not accepted during process execution");
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
                "steering is accepted during execution and changes the same turn"
            }
        );
    }
    Ok(())
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
