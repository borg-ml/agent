//! Explicit subscription integration probe. Uses a temporary Borg session and
//! approves only `cat probe.txt`, then resumes the same durable session.
use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use borg_remote::{
    AgentTurnExecutor, ApprovalDecision, CodingProvider, ConsultationRequest, HostCommand,
    LaunchSession, LocalAgentTurnExecutor, ModelAccessContext, PermissionMode, ResponseLanguage,
    SessionEventKind, SessionStore, SqliteSessionStore, run_agent_session_with_executor,
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
                    effort: Some(borg_provider::codex_default_effort().into()), fast: Some(false),
                    response_language: ResponseLanguage::Auto, permission_mode: PermissionMode::Manual,
                    name: None, initial_prompt: Some(if resumed {
                        "Without using any tools, repeat the exact probe value you read in the previous turn."
                    } else {
                        "This is a read-only integration probe. Use exec exactly once with cmd exactly `cat probe.txt` and action `read probe`. Do not run any other command, discover tools, or change files. Then reply with the exact file contents. Preserve this exact value for the next turn, including through any context compaction."
                    }.into()), capabilities: Default::default(), subagent_concurrency_limit: None,
                    extension_skill_roots: Vec::new(), team_policy: None,
                }, command_rx, events,
                Arc::new(LocalAgentTurnExecutor::default().with_codex_model_only()),
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
                        ..
                    } => {
                        println!(
                            "Borg usage: {total_tokens} tokens, {cached_input_tokens} cached input"
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
                .with_codex_model_only()
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
