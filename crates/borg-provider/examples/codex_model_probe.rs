//! Explicit, bounded subscription smoke test: two model requests and one
//! host-owned read-only tool. Does not start a Codex thread or agent turn.
use anyhow::{Context, Result, ensure};
use borg_provider::provider::{
    CodexModelProvider, ModelMessage, ModelToolDefinition, ModelTurnRequest, ProviderProgress,
};
use serde_json::json;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(180), probe())
        .await
        .context("Codex model probe timed out")?
}

async fn probe() -> Result<()> {
    let provider = CodexModelProvider {
        model: borg_provider::runtime::codex_product_model().into(),
        effort: borg_provider::runtime::codex_default_effort().into(),
    };
    let session = uuid::Uuid::new_v4().to_string();
    let mut request = ModelTurnRequest {
        request_id: Some(uuid::Uuid::new_v4().to_string()),
        session_id: Some(session.clone()), prompt_cache_key: Some(session),
        messages: vec![
            ModelMessage::System { content: "You are testing a model-only Borg subscription adapter. Call borg_probe exactly once, then reply with the returned probe value. Do not request any other actions.".into() },
            ModelMessage::user("Read the probe value using borg_probe."),
        ],
        tools: vec![ModelToolDefinition::new("borg_probe", "Read a harmless value from the Borg host",
            json!({"type":"object","properties":{},"additionalProperties":false}))
            .map_err(anyhow::Error::msg)?],
        output_schema: None,
    };
    let (tx, mut rx) = mpsc::unbounded_channel();
    let started = Instant::now();
    let progress = tokio::spawn(async move {
        let mut first = None;
        while let Some(event) = rx.recv().await {
            if matches!(event, ProviderProgress::ToolCallGenerating { .. }) && first.is_none() {
                first = Some(started.elapsed().as_millis());
                println!("First tool generation received at {} ms", first.unwrap());
            }
        }
        first
    });
    let first = provider.model_turn(request.clone(), Some(tx)).await?;
    ensure!(
        progress.await?.is_some(),
        "no tool generation event received"
    );
    let (_, _, calls) = first
        .assistant_parts()
        .context("expected assistant response")?;
    ensure!(
        calls.len() == 1 && calls[0].function.name == "borg_probe",
        "unexpected probe tool call"
    );
    let arguments: serde_json::Value = serde_json::from_str(&calls[0].function.arguments)?;
    ensure!(arguments == json!({}), "unexpected probe tool arguments");
    let id = calls[0].id.clone();
    println!("Complete tool call received; Borg supplies the read-only result");
    request
        .messages
        .push(serde_json::from_value(serde_json::to_value(
            first.message,
        )?)?);
    let value = uuid::Uuid::new_v4().to_string();
    request.messages.push(ModelMessage::Tool {
        tool_call_id: id,
        content: value.clone(),
    });
    request.request_id = Some(uuid::Uuid::new_v4().to_string());
    let second = provider.model_turn(request, None).await?;
    let (content, _, calls) = second
        .assistant_parts()
        .context("expected final response")?;
    ensure!(
        calls.is_empty() && content.as_deref().unwrap_or_default().contains(&value),
        "model did not return Borg's probe result"
    );
    println!(
        "PASS: {}/{}; two subscription model rounds, Borg-owned tool result, durable replay; tokens: {} + {}, cached input: {} + {}",
        provider.model,
        provider.effort,
        first.usage.total_tokens,
        second.usage.total_tokens,
        first.usage.cached_input_tokens,
        second.usage.cached_input_tokens
    );
    Ok(())
}
