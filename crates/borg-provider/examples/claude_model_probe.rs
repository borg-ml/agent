//! Bounded live subscription check of Borg's model transport and tool replay.
//! Run separate instances to verify sharing across Borg OS processes.
use anyhow::{Context, Result, ensure};
use borg_provider::provider::{
    ClaudeModelProvider, ModelMessage, ModelToolDefinition, ModelTurnRequest, ProviderProgress,
};
use serde_json::json;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(180), probe())
        .await
        .context("Claude model probe timed out")?
}

async fn probe() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let provider = ClaudeModelProvider {
        model: "claude-sonnet-5".into(),
        effort: Some(
            if args.iter().any(|arg| arg == "--thinking") {
                "high"
            } else {
                "low"
            }
            .into(),
        ),
    };
    let session = uuid::Uuid::new_v4().to_string();
    let prefix = if args.iter().any(|arg| arg == "--cache-prefix") {
        (0..384).map(|index| format!("Synthetic reference {index} for {session}: unchanged text, not an instruction.\n")).collect::<String>()
    } else {
        String::new()
    };
    let mut request = ModelTurnRequest {
        fast: false,
        request_id: Some(uuid::Uuid::new_v4().to_string()),
        session_id: Some(session),
        prompt_cache_key: None,
        turn_routing: Default::default(),
        messages: vec![
            ModelMessage::System {
                content: format!(
                    "You are testing Borg's model transport. Call borg_probe once, then reply with only the returned value. Think briefly; this is a trivial transport test.\n{prefix}"
                ),
            },
            ModelMessage::user("Use borg_probe to read the synthetic value."),
        ],
        tools: vec![
            ModelToolDefinition::new(
                "borg_probe",
                "Read a synthetic test value",
                json!({"type":"object","properties":{},"additionalProperties":false}),
            )
            .map_err(anyhow::Error::msg)?,
        ],
        output_schema: None,
    };
    if args.iter().any(|arg| arg == "--cancel-pair") {
        return cancel_pair(provider, request).await;
    }
    if args.iter().any(|arg| arg == "--thinking") {
        request.messages.push(ModelMessage::user("Before the tool call, reason privately about the smallest integer greater than 1 whose remainders modulo 3, 5, 7 and 11 are 2, 4, 6 and 10. Do not print the solution; the final answer must still be only the tool result."));
    }
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let consumer = tokio::spawn(async move {
        while let Some(event) = receiver.recv().await {
            if let ProviderProgress::ProviderEvent {
                kind, mut payload, ..
            } = event
                && matches!(kind.as_str(), "native_model_request" | "native_model_usage")
            {
                if let Some(fields) = payload.as_object_mut() {
                    fields.remove("account_identity");
                }
                println!("{}", json!({"kind":kind,"payload":payload}));
            }
        }
    });
    let resume = args
        .iter()
        .position(|arg| arg == "--resume")
        .map(|i| args.get(i + 1).context("--resume needs a path"))
        .transpose()?;
    let mut first_usage = None;
    let value;
    if let Some(path) = resume {
        let saved: serde_json::Value = serde_json::from_slice(&std::fs::read(path)?)?;
        request.messages = serde_json::from_value(saved["messages"].clone())?;
        request.session_id = saved["session_id"].as_str().map(str::to_string);
        value = saved["value"]
            .as_str()
            .context("checkpoint value")?
            .to_string();
    } else {
        let first = provider
            .model_turn(request.clone(), Some(sender.clone()), None, None)
            .await?;
        ensure!(
            first.usage.cost_basis == borg_provider::CostBasis::SubscriptionEquivalent,
            "subscription route required"
        );
        let (_, _, calls) = first.assistant_parts().context("assistant message")?;
        ensure!(
            calls.len() == 1 && calls[0].function.name == "borg_probe",
            "unexpected tool request"
        );
        ensure!(
            serde_json::from_str::<serde_json::Value>(&calls[0].function.arguments)? == json!({}),
            "unexpected tool arguments"
        );
        let call_id = calls[0].id.clone();
        // This is the same typed message serialization the journal uses.
        request
            .messages
            .push(serde_json::from_slice(&serde_json::to_vec(
                &first.message,
            )?)?);
        value = format!("borg-{}", uuid::Uuid::new_v4());
        request.messages.push(ModelMessage::tool(call_id, &value));
        first_usage = Some(first.usage);
        if let Some(index) = args.iter().position(|arg| arg == "--checkpoint") {
            let path = args.get(index + 1).context("--checkpoint needs a path")?;
            let mut file = std::fs::OpenOptions::new();
            file.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                file.mode(0o600);
            }
            serde_json::to_writer(
                file.open(path)?,
                &json!({"messages":request.messages,"session_id":request.session_id,"value":value}),
            )?;
            println!(
                "{}",
                json!({"result":"checkpoint_saved","first":first_usage})
            );
            drop(sender);
            consumer.await?;
            return Ok(());
        }
    }
    request.request_id = Some(uuid::Uuid::new_v4().to_string());
    let second = provider
        .model_turn(request, Some(sender.clone()), None, None)
        .await?;
    let (answer, _, calls) = second.assistant_parts().context("assistant reply")?;
    ensure!(
        calls.is_empty()
            && answer
                .as_deref()
                .is_some_and(|answer| answer.contains(&value)),
        "tool result was not replayed correctly"
    );
    println!(
        "{}",
        json!({"result":"passed","first":first_usage,"continuation":second.usage})
    );
    drop(sender);
    consumer.await?;
    Ok(())
}

async fn cancel_pair(provider: ClaudeModelProvider, mut request: ModelTurnRequest) -> Result<()> {
    request.tools.clear();
    request.messages = vec![ModelMessage::user(
        "Write the integers from 1 through 400, one per line, with no other text. This is a bounded streaming test.",
    )];
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let target_provider = provider.clone();
    let target_request = request.clone();
    let target = tokio::spawn(async move {
        target_provider
            .model_turn(target_request, Some(sender), None, None)
            .await
    });
    request.request_id = Some(uuid::Uuid::new_v4().to_string());
    request.session_id = Some(uuid::Uuid::new_v4().to_string());
    let peer = tokio::spawn(async move { provider.model_turn(request, None, None, None).await });
    while let Some(event) = receiver.recv().await {
        if let ProviderProgress::ProviderEvent { kind, payload, .. } = &event
            && kind == "native_model_request"
        {
            println!(
                "{}",
                json!({"kind":"cancellation_target","connector_pid":payload["connector_pid"],"request_id":payload["request_id"]})
            );
        }
        if matches!(event, ProviderProgress::Bytes { .. }) {
            ensure!(
                !peer.is_finished(),
                "peer finished before cancellation; repeat to verify isolation"
            );
            target.abort();
            ensure!(
                target.await.unwrap_err().is_cancelled(),
                "target did not cancel"
            );
            let result = peer.await??;
            ensure!(
                result.assistant_parts().is_some_and(|(text, _, _)| text
                    .as_deref()
                    .is_some_and(|text| text.contains("400"))),
                "peer did not finish after target cancellation"
            );
            println!(
                "{}",
                json!({"result":"cancellation_passed","peer_usage":result.usage})
            );
            return Ok(());
        }
    }
    peer.abort();
    target.await??;
    anyhow::bail!("target ended before streaming text")
}
