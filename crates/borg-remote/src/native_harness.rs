use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use borg_provider::provider::{
    ModelGateway, ModelMessage, ModelToolCall, ModelToolDefinition, ModelTurnRequest,
    ModelTurnResult, OpenAiCompatibleProfile, OpenAiCompatibleProvider, ProviderAttemptTrace,
    ProviderCallError, ProviderInvocation, ProviderProgress, ProviderProgressStream,
};
use borg_provider::{CostBasis, ProviderCallUsage};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{
    AgentTurn, AgentTurnControl, AgentTurnResult, ApprovalDecision, EventActor, MessageStatus,
    PermissionMode, SessionEventKind, SessionStatus, WorkspaceCommandOutcome,
    WorkspaceCommandRequest, WorkspaceFilesystemOperation, WorkspaceFilesystemOutcome,
    WorkspaceFilesystemRequest, execute_workspace_command, execute_workspace_filesystem,
};

const MAX_TOOL_ROUNDS: usize = 32;
const MAX_TOOL_RESULT_BYTES: usize = 1024 * 1024;
const MAX_APPROVAL_DETAIL_BYTES: usize = 8 * 1024;
const DEFAULT_FILE_BYTES: u64 = 256 * 1024;
const MAX_FILE_BYTES: u64 = 8 * 1024 * 1024;
const DEFAULT_COMMAND_TIMEOUT_MS: u64 = 120_000;
const MAX_COMMAND_TIMEOUT_MS: u64 = 30 * 60 * 1000;
const DEFAULT_COMMAND_OUTPUT_BYTES: u64 = 256 * 1024;
const MAX_COMMAND_OUTPUT_BYTES: u64 = 1024 * 1024;

#[derive(Clone)]
pub(crate) struct NativeHarness {
    full_access_sessions: Arc<Mutex<HashSet<Uuid>>>,
    model_client: Arc<dyn NativeModelClient>,
}

impl std::fmt::Debug for NativeHarness {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeHarness")
            .field("full_access_sessions", &"[session-scoped]")
            .field("model_client", &"[provider adapter]")
            .finish()
    }
}

impl Default for NativeHarness {
    fn default() -> Self {
        Self {
            full_access_sessions: Arc::default(),
            model_client: Arc::new(CompatibleModelClient::default()),
        }
    }
}

impl NativeHarness {
    pub(crate) fn with_model_gateway(model_gateway: ModelGateway) -> Self {
        Self {
            model_client: Arc::new(CompatibleModelClient {
                gateway: Some(model_gateway),
            }),
            ..Self::default()
        }
    }

    pub(crate) async fn run(
        &self,
        turn: AgentTurn,
        events: mpsc::Sender<SessionEventKind>,
        mut controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        send(
            &events,
            SessionEventKind::StatusChanged {
                status: SessionStatus::Running,
                detail: None,
            },
        )
        .await;

        let model = turn
            .model
            .clone()
            .context("native provider sessions require an explicit model")?;
        if turn.provider == crate::CodingProvider::Kimi {
            anyhow::ensure!(
                model == borg_provider::kimi_product_model(),
                "native Kimi sessions require model `{}`",
                borg_provider::kimi_product_model()
            );
        }
        let runtime = NativeToolRuntime::start(
            turn.session_id,
            turn.cwd.clone(),
            turn.permission_mode,
            turn.agent_tools.clone(),
            turn.external_mcp_servers.clone(),
        )
        .await?;
        let tools = runtime.tool_definitions()?;
        let mut messages = Vec::with_capacity(turn.conversation.len().saturating_add(3));
        let mut system_prompt = super::agent::CODING_SYSTEM_PROMPT.to_string();
        if let Some(instruction) = turn.response_language.instruction() {
            system_prompt.push_str("\n\n");
            system_prompt.push_str(instruction);
        }
        messages.push(ModelMessage::System {
            content: system_prompt,
        });
        messages.extend(turn.conversation);
        let user_message = ModelMessage::User {
            content: prompt_with_attachments(&turn.cwd, &turn.prompt, &turn.attachments),
        };
        record_native_message(&events, turn.provider, &user_message).await?;
        messages.push(user_message);

        let mut usage = ProviderCallUsage::default();
        let mut assistant_message_id = Uuid::new_v4();
        let mut model_round = 0_usize;
        let mut tool_round = 0_usize;
        while model_round < MAX_TOOL_ROUNDS {
            model_round += 1;
            let result = match self
                .call_model(
                    turn.provider,
                    &model,
                    turn.effort.as_deref(),
                    ModelTurnRequest {
                        request_id: Some(format!("{}:{model_round}", turn.message_id)),
                        messages: messages.clone(),
                        tools: tools.clone(),
                        output_schema: turn.output_schema.clone(),
                    },
                    ModelStreamContext {
                        coding_provider: turn.provider,
                        assistant_message_id,
                        events: &events,
                        controls: &mut controls,
                    },
                )
                .await?
            {
                NativeModelOutcome::Completed(result) => result,
                NativeModelOutcome::Steered(steer) => {
                    let message = ModelMessage::User {
                        content: prompt_with_attachments(
                            &turn.cwd,
                            &steer.text,
                            &steer.attachments,
                        ),
                    };
                    record_native_message(&events, turn.provider, &message).await?;
                    messages.push(message);
                    assistant_message_id = Uuid::new_v4();
                    send(
                        &events,
                        SessionEventKind::ProviderEvent {
                            provider: turn.provider,
                            kind: "native_steer_applied".to_string(),
                            payload: json!({ "model_round": model_round }),
                        },
                    )
                    .await;
                    continue;
                }
            };
            absorb_usage(&mut usage, &result.usage);
            let ModelMessage::Assistant {
                content,
                reasoning_content: _,
                reasoning_details: _,
                tool_calls,
            } = &result.message
            else {
                bail!("native provider returned a non-assistant model turn")
            };
            record_native_message(&events, turn.provider, &result.message).await?;
            messages.push(result.message.clone());

            if result.finish_reason == "length" {
                bail!("native provider response was truncated at the completion-token limit");
            }
            if tool_calls.is_empty() {
                if result.finish_reason != "stop" {
                    bail!(
                        "native provider ended the turn with unexpected finish reason `{}`",
                        result.finish_reason
                    );
                }
                let final_text = content.clone().unwrap_or_default();
                if final_text.trim().is_empty() {
                    bail!("native provider ended the turn without a final response");
                }
                send(
                    &events,
                    SessionEventKind::Message {
                        message_id: assistant_message_id,
                        actor: EventActor::Assistant,
                        text: final_text.clone(),
                        attachments: Vec::new(),
                        status: MessageStatus::Complete,
                        delivery: None,
                    },
                )
                .await;
                send_usage(&events, &usage).await;
                send(
                    &events,
                    SessionEventKind::StatusChanged {
                        status: SessionStatus::Ready,
                        detail: None,
                    },
                )
                .await;
                return Ok(AgentTurnResult {
                    provider_session_id: None,
                    final_text,
                });
            }
            if result.finish_reason != "tool_calls" {
                bail!(
                    "native provider returned tool calls with inconsistent finish reason `{}`",
                    result.finish_reason
                );
            }
            if let Some(narration) = content.as_ref().filter(|text| !text.trim().is_empty()) {
                send(
                    &events,
                    SessionEventKind::Message {
                        message_id: assistant_message_id,
                        actor: EventActor::Assistant,
                        text: narration.clone(),
                        attachments: Vec::new(),
                        status: MessageStatus::Complete,
                        delivery: None,
                    },
                )
                .await;
            }
            assistant_message_id = Uuid::new_v4();

            let mut pending_steer = None;
            for tool_call in tool_calls {
                let input = parse_tool_arguments(tool_call);
                send(
                    &events,
                    SessionEventKind::ToolStarted {
                        tool_call_id: tool_call.id.clone(),
                        name: tool_call.function.name.clone(),
                        input: input.clone().unwrap_or_else(|error| {
                            json!({
                                "malformed_arguments": tool_call.function.arguments,
                                "error": error
                            })
                        }),
                        input_ref: None,
                    },
                )
                .await;
                let (output, is_error, steer) = match input {
                    Ok(input) => {
                        execute_tool(self, &runtime, tool_call, input, &events, &mut controls)
                            .await?
                    }
                    Err(error) => (json!({ "error": error }).to_string(), true, None),
                };
                pending_steer = pending_steer.or(steer);
                let output = bounded_tool_content(output);
                send(
                    &events,
                    SessionEventKind::ToolCompleted {
                        tool_call_id: tool_call.id.clone(),
                        output: output.clone(),
                        output_ref: None,
                        is_error,
                        input: None,
                        input_ref: None,
                    },
                )
                .await;
                let tool_message = ModelMessage::Tool {
                    tool_call_id: tool_call.id.clone(),
                    content: output,
                };
                record_native_message(&events, turn.provider, &tool_message).await?;
                messages.push(tool_message);
            }

            if let Some(steer) = pending_steer {
                let message = ModelMessage::User {
                    content: prompt_with_attachments(&turn.cwd, &steer.text, &steer.attachments),
                };
                record_native_message(&events, turn.provider, &message).await?;
                messages.push(message);
            }
            tool_round += 1;
            send(
                &events,
                SessionEventKind::ProviderEvent {
                    provider: turn.provider,
                    kind: "native_tool_round_completed".to_string(),
                    payload: json!({ "round": tool_round }),
                },
            )
            .await;
        }
        bail!("native provider exceeded the harness limit of {MAX_TOOL_ROUNDS} tool rounds")
    }

    pub(crate) async fn compact(
        &self,
        provider: crate::CodingProvider,
        model: &str,
        effort: Option<&str>,
        conversation: Vec<ModelMessage>,
    ) -> Result<(String, ProviderCallUsage)> {
        anyhow::ensure!(
            provider.uses_native_harness(),
            "{provider:?} does not use Borg's native harness"
        );
        anyhow::ensure!(
            !conversation.is_empty(),
            "there is no native conversation to compact yet"
        );
        let mut messages = Vec::with_capacity(conversation.len().saturating_add(2));
        messages.push(ModelMessage::System {
            content: "Summarize the conversation for another agent that will continue the work. Preserve user requirements, decisions, files changed, commands and tests run, unresolved errors, approvals, and next steps. Be compact but do not omit details needed to continue safely. Return only the summary.".to_string(),
        });
        messages.extend(conversation);
        messages.push(ModelMessage::User {
            content: "Create the continuation summary now.".to_string(),
        });
        let result = self
            .model_client
            .model_turn(
                provider,
                model,
                effort,
                ModelTurnRequest {
                    request_id: Some(format!("compact:{}", Uuid::new_v4())),
                    messages,
                    tools: Vec::new(),
                    output_schema: None,
                },
                None,
            )
            .await
            .map_err(|error| anyhow::anyhow!(error.message))?;
        let ModelMessage::Assistant {
            content,
            tool_calls,
            ..
        } = result.message
        else {
            bail!("native compaction returned a non-assistant message")
        };
        anyhow::ensure!(
            tool_calls.is_empty(),
            "native compaction unexpectedly requested a tool"
        );
        let summary = content.unwrap_or_default();
        anyhow::ensure!(
            !summary.trim().is_empty(),
            "native compaction returned an empty summary"
        );
        Ok((summary, result.usage))
    }

    fn has_full_access(&self, session_id: Uuid) -> bool {
        self.full_access_sessions
            .lock()
            .expect("native approval-state lock poisoned")
            .contains(&session_id)
    }

    fn allow_session(&self, session_id: Uuid) {
        self.full_access_sessions
            .lock()
            .expect("native approval-state lock poisoned")
            .insert(session_id);
    }

    async fn call_model(
        &self,
        provider: crate::CodingProvider,
        model: &str,
        effort: Option<&str>,
        request: ModelTurnRequest,
        context: ModelStreamContext<'_>,
    ) -> Result<NativeModelOutcome> {
        call_model_streaming(
            self.model_client.as_ref(),
            provider,
            model,
            effort,
            request,
            context,
        )
        .await
    }
}

struct NativeSteer {
    text: String,
    attachments: Vec<PathBuf>,
}

enum NativeModelOutcome {
    Completed(ModelTurnResult),
    Steered(NativeSteer),
}

#[async_trait]
trait NativeModelClient: Send + Sync {
    async fn model_turn(
        &self,
        provider: crate::CodingProvider,
        model: &str,
        effort: Option<&str>,
        request: ModelTurnRequest,
        progress: Option<mpsc::UnboundedSender<ProviderProgress>>,
    ) -> std::result::Result<ModelTurnResult, ProviderCallError>;
}

#[derive(Debug, Clone, Default)]
struct CompatibleModelClient {
    gateway: Option<ModelGateway>,
}

#[async_trait]
impl NativeModelClient for CompatibleModelClient {
    async fn model_turn(
        &self,
        provider: crate::CodingProvider,
        model: &str,
        effort: Option<&str>,
        request: ModelTurnRequest,
        progress: Option<mpsc::UnboundedSender<ProviderProgress>>,
    ) -> std::result::Result<ModelTurnResult, ProviderCallError> {
        let profile = match provider {
            crate::CodingProvider::Kimi => OpenAiCompatibleProfile::Kimi,
            crate::CodingProvider::OpenRouter => OpenAiCompatibleProfile::OpenRouter,
            crate::CodingProvider::OpenAiCompatible => OpenAiCompatibleProfile::Generic,
            crate::CodingProvider::Codex
            | crate::CodingProvider::Claude
            | crate::CodingProvider::OpenCode => {
                return Err(ProviderCallError {
                    message: format!("{provider:?} does not use Borg's native model client"),
                    trace: ProviderAttemptTrace {
                        invocation: ProviderInvocation {
                            provider_label: "native".to_string(),
                            executable: String::new(),
                            args: Vec::new(),
                            cwd: None,
                            model: Some(model.to_string()),
                            effort: effort.map(str::to_string),
                        },
                        exit_status: Some(1),
                        stdout: String::new(),
                        stderr: "invalid native provider".to_string(),
                    },
                    session_id: None,
                });
            }
        };
        OpenAiCompatibleProvider {
            model: model.to_string(),
            effort: effort.map(str::to_string),
            system_prompt: "",
        }
        .model_turn_via_profile(request, progress, self.gateway.as_ref(), profile)
        .await
    }
}

struct NativeToolRuntime {
    session_id: Uuid,
    root: PathBuf,
    permission: PermissionMode,
    agent_tools: crate::AgentToolDispatcher,
    mcp: crate::native_mcp::NativeMcpRuntime,
}

impl NativeToolRuntime {
    async fn start(
        session_id: Uuid,
        root: PathBuf,
        permission: PermissionMode,
        agent_tools: crate::AgentToolDispatcher,
        external_mcp_servers: Vec<borg_provider::mcp::ExternalMcpServer>,
    ) -> Result<Self> {
        Ok(Self {
            session_id,
            root,
            permission,
            agent_tools,
            mcp: crate::native_mcp::NativeMcpRuntime::start(external_mcp_servers).await?,
        })
    }

    fn tool_definitions(&self) -> Result<Vec<ModelToolDefinition>> {
        let mut definitions = builtin_tool_specs()
            .into_iter()
            .chain(self.agent_tools.specs())
            .map(|spec| ModelToolDefinition::from_mcp_spec(&spec).map_err(anyhow::Error::msg))
            .collect::<Result<Vec<_>>>()?;
        definitions.extend_from_slice(self.mcp.definitions());
        let mut names = HashSet::with_capacity(definitions.len());
        for definition in &definitions {
            if !names.insert(definition.name.as_str()) {
                bail!("duplicate native harness tool name `{}`", definition.name);
            }
        }
        Ok(definitions)
    }

    async fn call(&self, name: &str, arguments: Value) -> Result<Value> {
        match name {
            "list_files" => {
                let args: ListFilesArgs = serde_json::from_value(arguments)?;
                self.filesystem(WorkspaceFilesystemOperation::List {
                    path: PathBuf::from(args.path.unwrap_or_else(|| ".".to_string())),
                    limit: args.limit.unwrap_or(200).clamp(1, 2_000),
                })
                .await
            }
            "read_file" => {
                let args: ReadFileArgs = serde_json::from_value(arguments)?;
                self.filesystem(WorkspaceFilesystemOperation::ReadText {
                    path: PathBuf::from(args.path),
                    max_bytes: args
                        .max_bytes
                        .unwrap_or(DEFAULT_FILE_BYTES)
                        .clamp(1, MAX_FILE_BYTES),
                })
                .await
            }
            "search_files" => {
                let args: SearchFilesArgs = serde_json::from_value(arguments)?;
                let command = vec![
                    "rg".to_string(),
                    "--line-number".to_string(),
                    "--no-heading".to_string(),
                    "--color".to_string(),
                    "never".to_string(),
                    "--".to_string(),
                    args.pattern,
                    args.path.unwrap_or_else(|| ".".to_string()),
                ];
                self.command(command, 30_000, DEFAULT_COMMAND_OUTPUT_BYTES)
                    .await
            }
            "write_file" => {
                self.require_workspace_write()?;
                let args: WriteFileArgs = serde_json::from_value(arguments)?;
                self.filesystem(WorkspaceFilesystemOperation::WriteText {
                    path: PathBuf::from(args.path),
                    text: args.content,
                    overwrite: args.overwrite.unwrap_or(false),
                    create_parent_dirs: args.create_parent_dirs.unwrap_or(true),
                })
                .await
            }
            "edit_file" => {
                self.require_workspace_write()?;
                let args: EditFileArgs = serde_json::from_value(arguments)?;
                self.edit_file(args).await
            }
            "exec_command" => {
                let args: ExecCommandArgs = serde_json::from_value(arguments)?;
                self.command(
                    vec!["bash".to_string(), "-lc".to_string(), args.cmd],
                    args.timeout_ms
                        .unwrap_or(DEFAULT_COMMAND_TIMEOUT_MS)
                        .clamp(1, MAX_COMMAND_TIMEOUT_MS),
                    args.output_max_bytes
                        .unwrap_or(DEFAULT_COMMAND_OUTPUT_BYTES)
                        .clamp(1, MAX_COMMAND_OUTPUT_BYTES),
                )
                .await
            }
            other if self.mcp.contains(other) => self.mcp.call(other, arguments).await,
            other => self.agent_tools.call(other, arguments).await,
        }
    }

    fn require_workspace_write(&self) -> Result<()> {
        if self.permission == PermissionMode::ReadOnly {
            bail!("session permission mode is read-only")
        }
        Ok(())
    }

    async fn filesystem(&self, operation: WorkspaceFilesystemOperation) -> Result<Value> {
        let response = execute_workspace_filesystem(
            std::slice::from_ref(&self.root),
            WorkspaceFilesystemRequest {
                request_id: Uuid::new_v4(),
                workspace_id: self.session_id,
                root_path: self.root.clone(),
                timeout_ms: 30_000,
                operation,
            },
        )
        .await;
        match response.outcome {
            WorkspaceFilesystemOutcome::Success { output } => Ok(serde_json::to_value(output)?),
            WorkspaceFilesystemOutcome::Failure {
                code,
                message,
                retryable,
            } => bail!("{code:?}: {message} (retryable={retryable})"),
        }
    }

    async fn edit_file(&self, args: EditFileArgs) -> Result<Value> {
        if args.old_text.is_empty() {
            bail!("edit_file old_text must not be empty");
        }
        let path = PathBuf::from(&args.path);
        let read = self
            .filesystem(WorkspaceFilesystemOperation::ReadText {
                path: path.clone(),
                max_bytes: MAX_FILE_BYTES,
            })
            .await?;
        let current = read
            .get("text")
            .and_then(Value::as_str)
            .context("workspace read did not return text")?;
        let matches = current.matches(&args.old_text).count();
        if matches == 0 {
            bail!("edit_file old_text was not found in {}", args.path);
        }
        if matches > 1 && !args.replace_all.unwrap_or(false) {
            bail!(
                "edit_file old_text matched {matches} locations in {}; set replace_all=true or provide more context",
                args.path
            );
        }
        let updated = if args.replace_all.unwrap_or(false) {
            current.replace(&args.old_text, &args.new_text)
        } else {
            current.replacen(&args.old_text, &args.new_text, 1)
        };
        self.filesystem(WorkspaceFilesystemOperation::WriteText {
            path,
            text: updated,
            overwrite: true,
            create_parent_dirs: false,
        })
        .await
    }

    async fn command(
        &self,
        command: Vec<String>,
        timeout_ms: u64,
        output_max_bytes: u64,
    ) -> Result<Value> {
        let response = execute_workspace_command(
            std::slice::from_ref(&self.root),
            WorkspaceCommandRequest {
                request_id: Uuid::new_v4(),
                workspace_id: self.session_id,
                root_path: self.root.clone(),
                cwd: PathBuf::from("."),
                command,
                timeout_ms,
                output_max_bytes,
            },
        )
        .await;
        match response.outcome {
            WorkspaceCommandOutcome::Success { output } => Ok(json!({
                "exit_code": output.exit_code,
                "timed_out": output.timed_out,
                "stdout": output.stdout,
                "stderr": output.stderr,
                "stdout_truncated": output.stdout_truncated,
                "stderr_truncated": output.stderr_truncated,
            })),
            WorkspaceCommandOutcome::Failure {
                code,
                message,
                retryable,
            } => bail!("{code:?}: {message} (retryable={retryable})"),
        }
    }
}

struct ModelStreamContext<'a> {
    coding_provider: crate::CodingProvider,
    assistant_message_id: Uuid,
    events: &'a mpsc::Sender<SessionEventKind>,
    controls: &'a mut Option<mpsc::Receiver<AgentTurnControl>>,
}

async fn call_model_streaming(
    model_client: &dyn NativeModelClient,
    provider: crate::CodingProvider,
    model: &str,
    effort: Option<&str>,
    request: ModelTurnRequest,
    context: ModelStreamContext<'_>,
) -> Result<NativeModelOutcome> {
    let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
    let call = model_client.model_turn(provider, model, effort, request, Some(progress_tx));
    tokio::pin!(call);
    let mut text = String::new();
    let mut last_text_emit = Instant::now() - Duration::from_millis(50);
    let mut progress_open = true;
    loop {
        tokio::select! {
            result = &mut call => {
                return result
                    .map(NativeModelOutcome::Completed)
                    .map_err(|error| anyhow::anyhow!(error.message));
            }
            progress = progress_rx.recv(), if progress_open => match progress {
                Some(ProviderProgress::Bytes {
                    stream: ProviderProgressStream::Stdout,
                    chunk,
                }) => {
                    text.push_str(&String::from_utf8_lossy(&chunk));
                    if last_text_emit.elapsed() >= Duration::from_millis(40)
                        || chunk.ends_with(b"\n")
                    {
                        send(context.events, SessionEventKind::Message {
                            message_id: context.assistant_message_id,
                            actor: EventActor::Assistant,
                            text: text.clone(),
                            attachments: Vec::new(),
                            status: MessageStatus::InProgress,
                            delivery: None,
                        }).await;
                        last_text_emit = Instant::now();
                    }
                }
                Some(ProviderProgress::ProviderEvent {
                    kind,
                    payload,
                    content_text,
                    ..
                }) if kind == "reasoning_delta" => {
                    if let Some(text) = content_text
                        .or_else(|| payload.get("text").and_then(Value::as_str).map(str::to_string))
                    {
                        send(context.events, SessionEventKind::ReasoningDelta { text }).await;
                    }
                }
                Some(ProviderProgress::ProviderEvent { kind, payload, .. }) => {
                    send(context.events, SessionEventKind::ProviderEvent {
                        provider: context.coding_provider,
                        kind,
                        payload,
                    }).await;
                }
                Some(_) => {}
                None => progress_open = false,
            },
            control = next_control(context.controls) => match control {
                Some(AgentTurnControl::Interrupt) => bail!("native provider turn interrupted"),
                Some(AgentTurnControl::Steer {
                    text,
                    attachments,
                    ack,
                    ..
                }) => {
                    let _ = ack.send(Ok(()));
                    return Ok(NativeModelOutcome::Steered(NativeSteer {
                        text,
                        attachments,
                    }));
                }
                Some(AgentTurnControl::Approval { .. })
                | Some(AgentTurnControl::ProviderInteractionResponse { .. }) => {}
                None => {}
            }
        }
    }
}

async fn execute_tool(
    harness: &NativeHarness,
    runtime: &NativeToolRuntime,
    tool_call: &ModelToolCall,
    input: Value,
    events: &mpsc::Sender<SessionEventKind>,
    controls: &mut Option<mpsc::Receiver<AgentTurnControl>>,
) -> Result<(String, bool, Option<NativeSteer>)> {
    if tool_call.function.name == "exec_command" && runtime.permission == PermissionMode::ReadOnly {
        return Ok((
            json!({
                "error": "command execution is disabled in read-only sessions"
            })
            .to_string(),
            true,
            None,
        ));
    }
    let external_mcp = runtime.mcp.contains(&tool_call.function.name);
    if (tool_call.function.name == "exec_command" || external_mcp)
        && runtime.permission != PermissionMode::FullAccess
        && !harness.has_full_access(runtime.session_id)
    {
        let command = (tool_call.function.name == "exec_command").then(|| {
            input
                .get("cmd")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string()
        });
        let (title, detail) = if let Some(command) = command.as_deref() {
            ("Run command", command.to_string())
        } else {
            (
                "Use external tool",
                format!(
                    "{} {}",
                    tool_call.function.name,
                    bounded_text(input.to_string(), MAX_APPROVAL_DETAIL_BYTES)
                ),
            )
        };
        match request_tool_approval(
            harness,
            runtime.session_id,
            title,
            &detail,
            command,
            events,
            controls,
        )
        .await?
        {
            ApprovalDecision::Deny => {
                return Ok((
                    json!({ "error": "tool execution was denied by the user" }).to_string(),
                    true,
                    None,
                ));
            }
            ApprovalDecision::AllowOnce | ApprovalDecision::AllowSession => {}
        }
    }

    let call = runtime.call(&tool_call.function.name, input);
    tokio::pin!(call);
    loop {
        tokio::select! {
            result = &mut call => return Ok(match result {
                Ok(value) => (serde_json::to_string(&value)?, false, None),
                Err(error) => (
                    json!({ "error": format!("{error:#}") }).to_string(),
                    true,
                    None,
                ),
            }),
            control = next_control(controls) => match control {
                Some(AgentTurnControl::Interrupt) => bail!("native provider turn interrupted"),
                Some(AgentTurnControl::Steer {
                    text,
                    attachments,
                    ack,
                    ..
                }) => {
                    let _ = ack.send(Ok(()));
                    let result = (&mut call).await;
                    return Ok(match result {
                        Ok(value) => (
                            serde_json::to_string(&value)?,
                            false,
                            Some(NativeSteer { text, attachments }),
                        ),
                        Err(error) => (
                            json!({ "error": format!("{error:#}") }).to_string(),
                            true,
                            Some(NativeSteer { text, attachments }),
                        ),
                    });
                }
                Some(AgentTurnControl::Approval { .. })
                | Some(AgentTurnControl::ProviderInteractionResponse { .. }) => {}
                None => {}
            }
        }
    }
}

async fn request_tool_approval(
    harness: &NativeHarness,
    session_id: Uuid,
    title: &str,
    detail: &str,
    command: Option<String>,
    events: &mpsc::Sender<SessionEventKind>,
    controls: &mut Option<mpsc::Receiver<AgentTurnControl>>,
) -> Result<ApprovalDecision> {
    let approval_id = Uuid::new_v4().to_string();
    send(
        events,
        SessionEventKind::StatusChanged {
            status: SessionStatus::WaitingForApproval,
            detail: None,
        },
    )
    .await;
    send(
        events,
        SessionEventKind::ApprovalRequested {
            approval_id: approval_id.clone(),
            title: title.to_string(),
            detail: detail.to_string(),
            command,
        },
    )
    .await;
    loop {
        match next_control(controls).await {
            Some(AgentTurnControl::Approval {
                approval_id: received,
                decision,
            }) if received == approval_id => {
                if decision == ApprovalDecision::AllowSession {
                    harness.allow_session(session_id);
                }
                send(
                    events,
                    SessionEventKind::StatusChanged {
                        status: SessionStatus::Running,
                        detail: None,
                    },
                )
                .await;
                return Ok(decision);
            }
            Some(AgentTurnControl::Interrupt) => bail!("native provider turn interrupted"),
            Some(AgentTurnControl::Steer { ack, .. }) => {
                let _ = ack.send(Err(
                    "resolve the pending tool approval before steering the turn".to_string(),
                ));
            }
            Some(AgentTurnControl::Approval { .. })
            | Some(AgentTurnControl::ProviderInteractionResponse { .. }) => {}
            None => bail!("tool approval channel closed before a decision was received"),
        }
    }
}

async fn next_control(
    controls: &mut Option<mpsc::Receiver<AgentTurnControl>>,
) -> Option<AgentTurnControl> {
    match controls {
        Some(controls) => controls.recv().await,
        None => std::future::pending().await,
    }
}

async fn record_native_message(
    events: &mpsc::Sender<SessionEventKind>,
    provider: crate::CodingProvider,
    message: &ModelMessage,
) -> Result<()> {
    let payload = serde_json::to_value(message)?;
    events
        .send(SessionEventKind::ProviderEvent {
            provider,
            kind: "native_model_message".to_string(),
            payload,
        })
        .await
        .map_err(|_| anyhow::anyhow!("session actor stopped while recording native conversation"))
}

async fn send(events: &mpsc::Sender<SessionEventKind>, event: SessionEventKind) {
    let _ = events.send(event).await;
}

async fn send_usage(events: &mpsc::Sender<SessionEventKind>, usage: &ProviderCallUsage) {
    send(
        events,
        SessionEventKind::UsageUpdated {
            provider_duration_ms: usage.duration_ms,
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
            cached_input_tokens: usage.cached_input_tokens,
            cache_creation_input_tokens: usage.cache_creation_input_tokens,
            total_tokens: usage.total_tokens,
            cost_microusd: usage.cost_microusd,
            cost_basis: usage.cost_basis.to_string(),
            cost_usd: None,
            context_tokens: usage.context_tokens,
            context_window_tokens: usage.context_window_tokens,
        },
    )
    .await;
}

fn absorb_usage(total: &mut ProviderCallUsage, usage: &ProviderCallUsage) {
    let had_usage = total.total_tokens > 0;
    total.duration_ms = total.duration_ms.saturating_add(usage.duration_ms);
    total.input_tokens = total.input_tokens.saturating_add(usage.input_tokens);
    total.cached_input_tokens = total
        .cached_input_tokens
        .saturating_add(usage.cached_input_tokens);
    total.cache_creation_input_tokens = total
        .cache_creation_input_tokens
        .saturating_add(usage.cache_creation_input_tokens);
    total.output_tokens = total.output_tokens.saturating_add(usage.output_tokens);
    total.total_tokens = total.total_tokens.saturating_add(usage.total_tokens);
    total.context_tokens = usage.context_tokens.or(total.context_tokens);
    total.context_window_tokens = usage.context_window_tokens.or(total.context_window_tokens);
    total.cost_microusd = match (total.cost_microusd, usage.cost_microusd, had_usage) {
        (Some(left), Some(right), _) => Some(left.saturating_add(right)),
        (None, Some(right), false) => Some(right),
        _ => None,
    };
    total.cost_basis = if total.cost_microusd.is_some() {
        CostBasis::EstimatedFromPricing
    } else {
        CostBasis::Unavailable
    };
}

fn parse_tool_arguments(tool_call: &ModelToolCall) -> std::result::Result<Value, String> {
    let arguments = tool_call.function.arguments.trim();
    let value = if arguments.is_empty() {
        json!({})
    } else {
        serde_json::from_str(arguments)
            .map_err(|error| format!("tool arguments are not valid JSON: {error}"))?
    };
    if !value.is_object() {
        return Err("tool arguments must be a JSON object".to_string());
    }
    Ok(value)
}

fn bounded_tool_content(output: String) -> String {
    if output.len() <= MAX_TOOL_RESULT_BYTES {
        return output;
    }
    let mut boundary = MAX_TOOL_RESULT_BYTES;
    while !output.is_char_boundary(boundary) {
        boundary -= 1;
    }
    format!(
        "{}\n\n[tool output truncated at {} bytes]",
        &output[..boundary],
        MAX_TOOL_RESULT_BYTES
    )
}

fn bounded_text(mut output: String, max_bytes: usize) -> String {
    if output.len() <= max_bytes {
        return output;
    }
    let mut boundary = max_bytes;
    while !output.is_char_boundary(boundary) {
        boundary -= 1;
    }
    output.truncate(boundary);
    output.push('…');
    output
}

fn prompt_with_attachments(cwd: &Path, prompt: &str, attachments: &[PathBuf]) -> String {
    if attachments.is_empty() {
        return prompt.to_string();
    }
    let paths = attachments
        .iter()
        .map(|path| {
            path.strip_prefix(cwd)
                .unwrap_or(path)
                .to_string_lossy()
                .to_string()
        })
        .collect::<Vec<_>>()
        .join("\n- ");
    format!("{prompt}\n\nAttached workspace paths:\n- {paths}")
}

fn builtin_tool_specs() -> Vec<Value> {
    vec![
        tool(
            "list_files",
            "List one workspace directory without following symlinks.",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "default": "." },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 2000 }
                },
                "additionalProperties": false
            }),
        ),
        tool(
            "read_file",
            "Read a UTF-8 text file under the workspace root.",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "minLength": 1 },
                    "max_bytes": { "type": "integer", "minimum": 1, "maximum": MAX_FILE_BYTES }
                },
                "required": ["path"],
                "additionalProperties": false
            }),
        ),
        tool(
            "search_files",
            "Search workspace text with ripgrep and return file, line, and matching text.",
            json!({
                "type": "object",
                "properties": {
                    "pattern": { "type": "string", "minLength": 1 },
                    "path": { "type": "string", "default": "." }
                },
                "required": ["pattern"],
                "additionalProperties": false
            }),
        ),
        tool(
            "write_file",
            "Create or deliberately overwrite a UTF-8 workspace file.",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "minLength": 1 },
                    "content": { "type": "string" },
                    "overwrite": { "type": "boolean", "default": false },
                    "create_parent_dirs": { "type": "boolean", "default": true }
                },
                "required": ["path", "content"],
                "additionalProperties": false
            }),
        ),
        tool(
            "edit_file",
            "Replace an exact text span in one workspace file; ambiguous matches fail unless replace_all is explicit.",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "minLength": 1 },
                    "old_text": { "type": "string", "minLength": 1 },
                    "new_text": { "type": "string" },
                    "replace_all": { "type": "boolean", "default": false }
                },
                "required": ["path", "old_text", "new_text"],
                "additionalProperties": false
            }),
        ),
        tool(
            "exec_command",
            "Run one shell command in the workspace with bounded time and output. Non-full-access sessions require user approval.",
            json!({
                "type": "object",
                "properties": {
                    "cmd": { "type": "string", "minLength": 1 },
                    "timeout_ms": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": MAX_COMMAND_TIMEOUT_MS
                    },
                    "output_max_bytes": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": MAX_COMMAND_OUTPUT_BYTES
                    }
                },
                "required": ["cmd"],
                "additionalProperties": false
            }),
        ),
    ]
}

fn tool(name: &str, description: &str, input_schema: Value) -> Value {
    json!({ "name": name, "description": description, "inputSchema": input_schema })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListFilesArgs {
    path: Option<String>,
    limit: Option<usize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadFileArgs {
    path: String,
    max_bytes: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchFilesArgs {
    pattern: String,
    path: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteFileArgs {
    path: String,
    content: String,
    overwrite: Option<bool>,
    create_parent_dirs: Option<bool>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EditFileArgs {
    path: String,
    old_text: String,
    new_text: String,
    replace_all: Option<bool>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecCommandArgs {
    cmd: String,
    timeout_ms: Option<u64>,
    output_max_bytes: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_arguments_require_an_object() {
        let call = ModelToolCall::function(
            "call".to_string(),
            "read_file".to_string(),
            "[]".to_string(),
        );
        assert_eq!(
            parse_tool_arguments(&call).unwrap_err(),
            "tool arguments must be a JSON object"
        );
    }

    #[test]
    fn bounded_tool_results_preserve_utf8_boundaries() {
        let output = "é".repeat(MAX_TOOL_RESULT_BYTES);
        let bounded = bounded_tool_content(output);
        assert!(bounded.is_char_boundary(MAX_TOOL_RESULT_BYTES));
        assert!(bounded.contains("tool output truncated"));
    }
}
