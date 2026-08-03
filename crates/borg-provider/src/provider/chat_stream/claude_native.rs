//! Borg integration for the standalone [`claude_agents`] runtime.
//!
//! This module owns Borg-specific binary discovery, authentication, MCP
//! preparation, and provider environment setup. The stream-json protocol,
//! control channel, and pooled process lifecycle live in the MIT-licensed
//! `claude-agents` crate.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use claude_agents::{
    ChatStreamControl as NativeControl, ChatStreamEvent as NativeEvent,
    ChatStreamRequest as NativeRequest, CommandSpec, RuntimeDirectory,
};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::mpsc;

use super::{
    ChatApprovalDecision, ChatStreamControl, ChatStreamEvent, ChatStreamRequest,
    LocalAgentPermission, ProviderAuthProvider,
};
use crate::ProviderChannel;

const BASE_ARGS: &[&str] = &[
    "--output-format",
    "stream-json",
    "--verbose",
    "--input-format",
    "stream-json",
];
const STDIO_PERMISSION_ARGS: &[&str] = &["--permission-prompt-tool", "stdio"];
const SUPPORTED_HARNESS_SCHEMAS: &[u64] = &[1];

#[derive(Debug, Clone)]
struct ClaudeBinary {
    path: PathBuf,
    sdk_version: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SdkManifest {
    #[serde(default)]
    sdk_compat: Option<SdkCompat>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SdkCompat {
    #[serde(default)]
    harness_schema: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct WrapperManifest {
    #[serde(default)]
    version: Option<String>,
}

fn platform_binary_name() -> &'static str {
    if cfg!(windows) {
        "claude.exe"
    } else {
        "claude"
    }
}

fn resolve_claude_binary() -> Result<ClaudeBinary> {
    let mut attempted = Vec::new();
    if let Some(raw) = std::env::var_os("BORG_CLAUDE_BIN") {
        let path = PathBuf::from(raw);
        if !path.exists() {
            bail!(
                "BORG_CLAUDE_BIN points at {} which does not exist",
                path.display()
            );
        }
        return describe_binary(path);
    }

    let binary = platform_binary_name();
    let mut candidates = Vec::new();
    let mut push = |path: PathBuf| {
        if !candidates.contains(&path) {
            candidates.push(path);
        }
    };
    if let Ok(executable) = std::env::current_exe()
        && let Some(parent) = executable.parent()
    {
        push(parent.join("providers/claude").join(binary));
    }
    if let Some(home) = std::env::var_os("BORG_HOME") {
        push(PathBuf::from(home).join("providers/claude").join(binary));
    }
    if let Some(home) = std::env::var_os("HOME") {
        push(
            PathBuf::from(home)
                .join(".borg/providers/claude")
                .join(binary),
        );
    }
    if let Ok(cwd) = std::env::current_dir() {
        push(cwd.join("providers/claude").join(binary));
    }
    for candidate in candidates {
        if candidate.exists() {
            return describe_binary(candidate);
        }
        attempted.push(candidate.display().to_string());
    }
    if let Some(path) = std::env::var_os("PATH") {
        for candidate in std::env::split_paths(&path).map(|dir| dir.join(binary)) {
            if candidate.is_file() {
                return describe_binary(candidate);
            }
        }
    }
    attempted.push(format!("{binary} on PATH"));
    bail!(
        "the claude binary was not found; set BORG_CLAUDE_BIN or install Claude Code. Looked in:\n  {}",
        attempted.join("\n  ")
    )
}

fn describe_binary(path: PathBuf) -> Result<ClaudeBinary> {
    let path = path
        .canonicalize()
        .with_context(|| format!("failed to canonicalize {}", path.display()))?;
    let sdk_version = read_manifest_metadata(&path)?;
    Ok(ClaudeBinary { path, sdk_version })
}

fn read_manifest_metadata(binary: &Path) -> Result<Option<String>> {
    let Some(dir) = binary.parent() else {
        return Ok(None);
    };
    let manifest_path = dir.join("manifest.json");
    if !manifest_path.exists() {
        return Ok(None);
    }
    let raw = match std::fs::read_to_string(&manifest_path) {
        Ok(raw) => raw,
        Err(_) => return Ok(None),
    };
    let Ok(manifest) = serde_json::from_str::<SdkManifest>(&raw) else {
        return Ok(None);
    };
    if let Some(schema) = manifest.sdk_compat.and_then(|compat| compat.harness_schema)
        && !SUPPORTED_HARNESS_SCHEMAS.contains(&schema)
    {
        bail!(
            "claude binary at {} declares harnessSchema {schema}, but this Borg build supports {:?}",
            binary.display(),
            SUPPORTED_HARNESS_SCHEMAS,
        );
    }
    let package_path = manifest_path
        .parent()
        .map(|dir| dir.join("package.json"))
        .filter(|path| path.exists());
    Ok(package_path
        .and_then(|path| std::fs::read_to_string(path).ok())
        .and_then(|raw| serde_json::from_str::<WrapperManifest>(&raw).ok())
        .and_then(|manifest| manifest.version))
}

fn permission_mode(permission: LocalAgentPermission) -> &'static str {
    match permission {
        LocalAgentPermission::FullAccess => "bypassPermissions",
        LocalAgentPermission::Auto => "acceptEdits",
        LocalAgentPermission::Manual => "default",
    }
}

fn fast_settings() -> String {
    serde_json::json!({
        "fastMode": true,
        "fastModePerSessionOptIn": true,
    })
    .to_string()
}

fn set_env(
    environment: &mut Vec<(String, String)>,
    key: impl Into<String>,
    value: impl Into<String>,
) {
    environment.push((key.into(), value.into()));
}

fn channel_environment(channel: ProviderChannel) -> Result<Vec<(String, String)>> {
    let mut environment = Vec::new();
    match channel {
        ProviderChannel::Direct => {}
        ProviderChannel::Vertex => {
            let project_id = std::env::var("BORG_VERTEX_PROJECT_ID")
                .or_else(|_| std::env::var("ANTHROPIC_VERTEX_PROJECT_ID"))
                .context(
                    "Vertex channel selected but neither BORG_VERTEX_PROJECT_ID nor \
                     ANTHROPIC_VERTEX_PROJECT_ID is set",
                )?;
            let region = std::env::var("BORG_VERTEX_REGION")
                .or_else(|_| std::env::var("CLOUD_ML_REGION"))
                .unwrap_or_else(|_| "global".to_string());
            set_env(&mut environment, "CLAUDE_CODE_USE_VERTEX", "1");
            set_env(&mut environment, "ANTHROPIC_VERTEX_PROJECT_ID", project_id);
            set_env(&mut environment, "CLOUD_ML_REGION", region);
            if let Ok(creds) = std::env::var("BORG_VERTEX_CREDENTIALS_PATH")
                && !creds.trim().is_empty()
            {
                set_env(&mut environment, "GOOGLE_APPLICATION_CREDENTIALS", creds);
            }
        }
        ProviderChannel::Bedrock => {
            let region = std::env::var("BORG_BEDROCK_REGION")
                .or_else(|_| std::env::var("AWS_REGION"))
                .context(
                    "Bedrock channel selected but neither BORG_BEDROCK_REGION nor \
                     AWS_REGION is set",
                )?;
            set_env(&mut environment, "CLAUDE_CODE_USE_BEDROCK", "1");
            set_env(&mut environment, "AWS_REGION", region);
        }
        ProviderChannel::AzureOpenAi => {
            bail!(
                "AzureOpenAi channel is not supported for Claude; this channel is reserved for future Codex/OpenAI routing"
            );
        }
    }
    Ok(environment)
}

fn native_lifecycle_key(req: &ChatStreamRequest, permission: LocalAgentPermission) -> String {
    serde_json::json!({
        "workspace": req.working_directory,
        "model": req.model.as_ref().filter(|model| !model.trim().is_empty()),
        "effort": req.effort.clone().unwrap_or_else(|| "medium".to_string()),
        "fast": req.fast,
        "system_prompt": req.system_prompt,
        "output_schema": req.output_schema,
        "mcp_owner_id": req.mcp_owner_id,
        "mcp_allowed_scopes": req.mcp_allowed_scopes,
        "mcp_user_id": req.mcp_user_id,
        "mcp_external_servers": req.mcp_external_servers.iter().map(|server| serde_json::json!({
            "name": server.name,
            "command": server.command,
            "args": server.args,
            "env": server.env,
            "allowed_tools": server.allowed_tools,
        })).collect::<Vec<_>>(),
        "mcp_api_token": req.mcp_api_token,
        "permission": permission_mode(permission),
        "provider_channel": req.provider_channel.as_str(),
        "persist_session": req.persist_session != Some(false),
    })
    .to_string()
}

fn build_native_request(
    req: &ChatStreamRequest,
    local_auth: bool,
    permission: LocalAgentPermission,
) -> Result<NativeRequest> {
    let runtime_directory =
        RuntimeDirectory::new().context("failed to create Claude provider home")?;
    let provider_home = runtime_directory.path();
    if let Some(auth) = req.provider_auth.as_ref()
        && auth.provider == ProviderAuthProvider::Claude
    {
        crate::provider_auth::restore_bundle(
            ProviderAuthProvider::Claude,
            &auth.bundle,
            provider_home,
        )
        .context("failed to restore Claude provider auth bundle")?;
    }
    let workspace_dir = req
        .working_directory
        .clone()
        .unwrap_or_else(|| provider_home.to_path_buf());
    std::fs::create_dir_all(&workspace_dir).with_context(|| {
        format!(
            "failed to create Claude workspace directory {}",
            workspace_dir.display()
        )
    })?;
    let mcp_setup = super::prepare_request_mcp(provider_home, req, local_auth)?;
    let mcp_servers = super::read_mcp_servers_from_config(mcp_setup.claude_config_path.as_deref())?
        .unwrap_or(Value::Null);
    let git_env = super::prepare_git_credential_env(provider_home, &req.git_credentials)?;
    let binary = resolve_claude_binary()?;

    let mut args = BASE_ARGS
        .iter()
        .map(|arg| (*arg).to_string())
        .collect::<Vec<_>>();
    args.extend(STDIO_PERMISSION_ARGS.iter().map(|arg| (*arg).to_string()));
    args.extend([
        "--permission-mode".to_string(),
        permission_mode(permission).to_string(),
        "--include-partial-messages".to_string(),
    ]);
    if matches!(permission, LocalAgentPermission::FullAccess) {
        args.push("--allow-dangerously-skip-permissions".to_string());
    }
    let model = req
        .model
        .clone()
        .or_else(|| {
            (!local_auth)
                .then(|| super::default_model_for_backend("claude"))
                .flatten()
        })
        .filter(|model| !model.trim().is_empty());
    if let Some(model) = model {
        args.extend(["--model".to_string(), model]);
    }
    args.extend([
        "--effort".to_string(),
        req.effort.clone().unwrap_or_else(|| "medium".to_string()),
    ]);
    if req.fast {
        args.extend(["--settings".to_string(), fast_settings()]);
    }
    if let Some(schema) = req.output_schema.as_ref() {
        args.extend(["--json-schema".to_string(), serde_json::to_string(schema)?]);
    }
    if !mcp_servers.is_null() {
        args.extend([
            "--mcp-config".to_string(),
            serde_json::to_string(&serde_json::json!({"mcpServers": mcp_servers}))?,
        ]);
    }
    if !mcp_setup.allowed_tools.is_empty() {
        args.extend(["--allowedTools".to_string(), mcp_setup.allowed_tools]);
    }
    if let Some(session_id) = req.session_id.as_deref().filter(|id| !id.trim().is_empty()) {
        args.push(format!("--resume={session_id}"));
    }
    if req.persist_session == Some(false) {
        args.push("--no-session-persistence".to_string());
    }

    let mut environment = Vec::new();
    if !local_auth {
        set_env(
            &mut environment,
            "HOME",
            provider_home.display().to_string(),
        );
    }
    environment.extend(git_env);
    if std::env::var_os("CLAUDE_CODE_ENTRYPOINT").is_none() {
        set_env(&mut environment, "CLAUDE_CODE_ENTRYPOINT", "sdk-ts");
    }
    if std::env::var_os("CLAUDE_AGENT_SDK_VERSION").is_none()
        && let Some(version) = binary.sdk_version
    {
        set_env(&mut environment, "CLAUDE_AGENT_SDK_VERSION", version);
    }
    if std::env::var("ENABLE_TOOL_SEARCH").is_err() {
        set_env(&mut environment, "ENABLE_TOOL_SEARCH", "auto:5");
    }
    if std::env::var_os("ANTHROPIC_API_KEY").is_none()
        && let Some(key) =
            crate::credentials::stored_api_key(crate::credentials::ApiKeyCredential::Anthropic)
    {
        set_env(&mut environment, "ANTHROPIC_API_KEY", key);
    }
    environment.extend(channel_environment(req.provider_channel)?);

    Ok(NativeRequest {
        prompt: req.prompt.clone(),
        attachments: req.attachments.clone(),
        system_prompt: req.system_prompt.clone(),
        command: CommandSpec {
            program: binary.path,
            args,
            current_dir: workspace_dir,
            environment,
            environment_remove: vec!["NODE_OPTIONS".to_string()],
        },
        runtime_directory: Some(runtime_directory),
        lifecycle_key: native_lifecycle_key(req, permission),
    })
}

fn to_native_control(control: ChatStreamControl) -> NativeControl {
    match control {
        ChatStreamControl::Steer {
            text,
            attachments,
            ack,
            ..
        } => NativeControl::Steer {
            text,
            attachments,
            ack,
        },
        ChatStreamControl::Approval {
            approval_id,
            decision,
        } => NativeControl::Approval {
            approval_id,
            decision: match decision {
                ChatApprovalDecision::ApproveOnce => {
                    claude_agents::ChatApprovalDecision::ApproveOnce
                }
                ChatApprovalDecision::ApproveSession => {
                    claude_agents::ChatApprovalDecision::ApproveSession
                }
                ChatApprovalDecision::Reject => claude_agents::ChatApprovalDecision::Reject,
            },
        },
        ChatStreamControl::ProviderInteractionResponse {
            interaction_id,
            response,
        } => NativeControl::ProviderInteractionResponse {
            interaction_id,
            response,
        },
        ChatStreamControl::Interrupt => NativeControl::Interrupt,
    }
}

fn native_controls(
    controls: Option<mpsc::Receiver<ChatStreamControl>>,
) -> (
    Option<mpsc::Receiver<NativeControl>>,
    Option<tokio::task::JoinHandle<()>>,
) {
    let Some(mut controls) = controls else {
        return (None, None);
    };
    let (tx, rx) = mpsc::channel(64);
    let task = tokio::spawn(async move {
        while let Some(control) = controls.recv().await {
            if tx.send(to_native_control(control)).await.is_err() {
                break;
            }
        }
    });
    (Some(rx), Some(task))
}

fn native_usage(usage: claude_agents::ProviderCallUsage) -> crate::runtime::ProviderCallUsage {
    crate::runtime::ProviderCallUsage {
        duration_ms: usage.duration_ms,
        input_tokens: usage.input_tokens,
        cached_input_tokens: usage.cached_input_tokens,
        cache_creation_input_tokens: usage.cache_creation_input_tokens,
        output_tokens: usage.output_tokens,
        total_tokens: usage.total_tokens,
        context_tokens: usage.context_tokens,
        context_window_tokens: usage.context_window_tokens,
        cost_microusd: usage.cost_microusd,
        cost_basis: if usage.cost_microusd.is_some() {
            crate::runtime::CostBasis::ProviderReported
        } else {
            crate::runtime::CostBasis::Unavailable
        },
    }
}

fn native_event(event: NativeEvent) -> ChatStreamEvent {
    match event {
        NativeEvent::ProviderEvent {
            kind,
            payload,
            raw_payload,
            stream_channel,
            content_text,
            provider_item_id,
            tool_use_id,
            tool_name,
        } => ChatStreamEvent::ProviderEvent {
            kind,
            payload,
            raw_payload,
            stream_channel,
            content_text,
            provider_item_id,
            tool_use_id,
            tool_name,
        },
        NativeEvent::Delta(text) => ChatStreamEvent::Delta(text),
        NativeEvent::ReasoningDelta(text) => ChatStreamEvent::ReasoningDelta(text),
        NativeEvent::Narration { text } => ChatStreamEvent::Narration { text },
        NativeEvent::Phase { name, input } => ChatStreamEvent::Phase { name, input },
        NativeEvent::ToolCall { id, name, input } => ChatStreamEvent::ToolCall { id, name, input },
        NativeEvent::ToolResult {
            tool_use_id,
            output,
            is_error,
            input,
        } => ChatStreamEvent::ToolResult {
            tool_use_id,
            output,
            is_error,
            input,
        },
        NativeEvent::ApprovalRequested {
            approval_id,
            title,
            detail,
            command,
        } => ChatStreamEvent::ApprovalRequested {
            approval_id,
            title,
            detail,
            command,
        },
        NativeEvent::ProviderInteractionRequested {
            interaction_id,
            kind,
            title,
            detail,
            payload,
        } => ChatStreamEvent::ProviderInteractionRequested {
            interaction_id,
            kind,
            title,
            detail,
            payload,
        },
        NativeEvent::Done {
            final_text,
            usage,
            session_id,
        } => ChatStreamEvent::Done {
            final_text,
            usage: usage.map(native_usage),
            session_id,
        },
        NativeEvent::Failed { error } => ChatStreamEvent::Failed { error },
    }
}

async fn bridge_run(
    request: NativeRequest,
    controls: Option<mpsc::Receiver<ChatStreamControl>>,
    tx: mpsc::Sender<ChatStreamEvent>,
    pooled: Option<claude_agents::ClaudePool>,
) -> Result<()> {
    let (native_tx, mut native_events) = mpsc::channel(64);
    let (native_controls, controls_task) = native_controls(controls);
    let runner = tokio::spawn(async move {
        match pooled {
            Some(pool) => {
                claude_agents::run_pooled(request, native_tx, native_controls, pool).await
            }
            None => claude_agents::run(request, native_tx, native_controls).await,
        }
    });
    while let Some(event) = native_events.recv().await {
        if tx.send(native_event(event)).await.is_err() {
            runner.abort();
            if let Some(task) = controls_task {
                task.abort();
            }
            return Ok(());
        }
    }
    let runner_result = runner.await.context("claude-agents runtime task panicked");
    if let Some(task) = controls_task {
        task.abort();
    }
    runner_result??;
    Ok(())
}

pub(super) async fn run(
    req: ChatStreamRequest,
    tx: mpsc::Sender<ChatStreamEvent>,
    controls: Option<mpsc::Receiver<ChatStreamControl>>,
    local_auth: bool,
    permission: LocalAgentPermission,
) -> Result<()> {
    let native_request = build_native_request(&req, local_auth, permission)?;
    bridge_run(native_request, controls, tx, None).await
}

pub(super) async fn run_pooled(
    req: ChatStreamRequest,
    tx: mpsc::Sender<ChatStreamEvent>,
    controls: Option<mpsc::Receiver<ChatStreamControl>>,
    permission: LocalAgentPermission,
    pool: claude_agents::ClaudePool,
) -> Result<()> {
    anyhow::ensure!(
        req.provider_auth.is_none() && req.git_credentials.is_empty(),
        "credential-scoped Claude requests cannot reuse a pooled native process"
    );
    let native_request = build_native_request(&req, true, permission)?;
    bridge_run(native_request, controls, tx, Some(pool)).await
}
