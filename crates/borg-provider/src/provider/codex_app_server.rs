use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc as std_mpsc,
};
use std::time::{Duration, Instant};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const CONTROL_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const INTERRUPT_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
const REQUEST_READ_POLL_INTERVAL: Duration = Duration::from_millis(100);
const MAX_QUARANTINED_RESPONSES: usize = 64;
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

use super::chat_stream::LocalAgentPermission;
use super::chat_stream::{ChatApprovalDecision, ChatStreamControl};

#[derive(Serialize)]
struct JsonRpcRequest {
    jsonrpc: &'static str,
    id: u64,
    method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<Value>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct JsonRpcMessage {
    #[serde(default)]
    pub id: Option<u64>,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub result: Option<Value>,
    #[serde(default)]
    pub error: Option<Value>,
    #[serde(default)]
    pub params: Option<Value>,
}

enum ReaderMessage {
    Message(JsonRpcMessage),
    Failed(String),
}

pub struct CodexAppServerClient {
    child: Child,
    stdin: Option<ChildStdin>,
    reader: std_mpsc::Receiver<ReaderMessage>,
    next_id: u64,
    workspace_id: Option<String>,
    network_access: bool,
    web_search_allowed: bool,
    deferred_notifications: Vec<JsonRpcMessage>,
    quarantined_responses: VecDeque<JsonRpcMessage>,
    cancellation: Option<Arc<AtomicBool>>,
    _shell_env: crate::shell_env::CleanShellEnv,
    _managed_codex_home: Option<tempfile::TempDir>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TokenUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cached_input_tokens: u64,
    pub cache_write_input_tokens: u64,
    pub reasoning_output_tokens: u64,
    pub total_tokens: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexWeeklyUsage {
    pub used_percent: u8,
    pub resets_at: Option<i64>,
}

impl CodexWeeklyUsage {
    pub fn remaining_percent(&self) -> u8 {
        100 - self.used_percent
    }
}

#[derive(Debug)]
pub struct TurnResult {
    pub workspace_id: String,
    pub output_text: String,
    pub reasoning_text: String,
    pub raw_notifications: Vec<String>,
    pub turn_token_usage: Option<TokenUsage>,
    pub total_token_usage: Option<TokenUsage>,
    pub model_context_window: Option<u64>,
}

impl CodexAppServerClient {
    pub fn workspace_id(&self) -> Option<&str> {
        self.workspace_id.as_deref()
    }

    pub fn start(
        network_access: bool,
        web_search_allowed: bool,
        codex_home: Option<&Path>,
        use_managed_openai_api_key: bool,
        extra_env: &[(String, String)],
    ) -> Result<Self> {
        Self::start_with_cancellation(
            network_access,
            web_search_allowed,
            codex_home,
            use_managed_openai_api_key,
            extra_env,
            None,
        )
    }

    pub(crate) fn start_with_cancellation(
        network_access: bool,
        web_search_allowed: bool,
        codex_home: Option<&Path>,
        use_managed_openai_api_key: bool,
        extra_env: &[(String, String)],
        cancellation: Option<Arc<AtomicBool>>,
    ) -> Result<Self> {
        let started_at = Instant::now();
        let shell_env = crate::shell_env::CleanShellEnv::new()?;
        tracing::debug!(
            target: "borg_ttft",
            stage = "codex_clean_shell_ready",
            elapsed_ms = started_at.elapsed().as_millis(),
            "Codex startup stage"
        );
        let mut command = Command::new("codex");
        let managed_api_key = if use_managed_openai_api_key {
            Some(super::managed_openai_api_key().ok_or_else(|| {
                anyhow::anyhow!(
                    "managed OpenAI access requires BORG_OPENAI_API_KEY or OPENAI_API_KEY"
                )
            })?)
        } else {
            None
        };
        let managed_codex_home = if managed_api_key.is_some() && codex_home.is_none() {
            Some(
                tempfile::Builder::new()
                    .prefix("borg-codex-managed-")
                    .tempdir()
                    .context("failed to create managed Codex home")?,
            )
        } else {
            None
        };
        let mut codex_args = shell_env.codex_config_args();
        codex_args.push("-c".to_string());
        codex_args.push(if web_search_allowed {
            "web_search=\"live\"".to_string()
        } else {
            "web_search=\"disabled\"".to_string()
        });
        command
            .arg("app-server")
            .args(codex_args)
            .args(["--listen", "stdio://"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            // App-server emits structured diagnostics on stderr, including
            // multi-line auth failures. Inheriting that stream corrupts any
            // terminal UI owned by the caller, so drain it behind the typed
            // app-server boundary instead.
            .stderr(Stdio::piped());
        shell_env.apply(&mut command);
        for (key, value) in extra_env {
            command.env(key, value);
        }
        if managed_api_key.is_none() {
            clear_managed_codex_env(&mut command);
        }
        let effective_codex_home =
            codex_home.or_else(|| managed_codex_home.as_ref().map(|dir| dir.path()));
        if let Some(codex_home) = effective_codex_home {
            fs::create_dir_all(codex_home)
                .with_context(|| format!("failed to create {}", codex_home.display()))?;
            if let Some(openai_api_key) = managed_api_key.as_deref() {
                write_managed_codex_auth(codex_home, openai_api_key)?;
            }
            command.env("CODEX_HOME", codex_home);
        }
        crate::subprocess::isolate_std_process_from_terminal(&mut command);
        let mut child = command
            .spawn()
            .context("failed to spawn codex app-server")?;
        tracing::debug!(
            target: "borg_ttft",
            stage = "codex_app_server_spawned",
            elapsed_ms = started_at.elapsed().as_millis(),
            "Codex startup stage"
        );

        let stdin = child
            .stdin
            .take()
            .context("codex app-server stdin not available")?;
        let stdout = child
            .stdout
            .take()
            .context("codex app-server stdout not available")?;
        let reader = spawn_reader(stdout);
        let mut stderr = child
            .stderr
            .take()
            .context("codex app-server stderr not available")?;
        std::thread::spawn(move || {
            for line in BufReader::new(&mut stderr)
                .lines()
                .map_while(std::result::Result::ok)
            {
                tracing::debug!(message = %line, "codex app-server stderr");
            }
        });

        let mut client = Self {
            child,
            stdin: Some(stdin),
            reader,
            next_id: 1,
            workspace_id: None,
            network_access,
            web_search_allowed,
            deferred_notifications: Vec::new(),
            quarantined_responses: VecDeque::new(),
            cancellation,
            _shell_env: shell_env,
            _managed_codex_home: managed_codex_home,
        };

        client.send_request(
            "initialize",
            Some(serde_json::json!({
                "clientInfo": {
                    "name": "borg",
                    "version": "0.1.0",
                },
                "capabilities": {
                    "experimentalApi": true,
                    "mcpServerOpenaiFormElicitation": true,
                    "requestAttestation": false,
                },
                "protocolVersion": "2025-01-01",
            })),
        )?;
        client.send_notification("initialized", None)?;
        tracing::debug!(
            target: "borg_ttft",
            stage = "codex_app_server_initialized",
            elapsed_ms = started_at.elapsed().as_millis(),
            "Codex startup stage"
        );
        Ok(client)
    }

    /// Read the authenticated ChatGPT account's current Codex weekly bucket.
    ///
    /// This is the same app-server account snapshot used by Codex's own
    /// `/usage` surface. The returned percentage is provider-reported, not
    /// inferred from local session token telemetry.
    pub fn account_weekly_usage(&mut self) -> Result<CodexWeeklyUsage> {
        let response = self.send_request("account/rateLimits/read", None)?;
        parse_codex_weekly_usage(&response)
    }

    pub fn thread_start(
        &mut self,
        developer_instructions: &str,
        model: Option<&str>,
        reasoning_effort: Option<&str>,
        mcp_config_path: Option<&str>,
        fast: bool,
        persist_session: bool,
    ) -> Result<String> {
        self.thread_start_with_permission(
            developer_instructions,
            model,
            reasoning_effort,
            mcp_config_path,
            fast,
            persist_session,
            LocalAgentPermission::FullAccess,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn thread_start_with_permission(
        &mut self,
        developer_instructions: &str,
        model: Option<&str>,
        reasoning_effort: Option<&str>,
        mcp_config_path: Option<&str>,
        fast: bool,
        persist_session: bool,
        permission: LocalAgentPermission,
    ) -> Result<String> {
        let (approval_policy, sandbox, sandbox_policy, approvals_reviewer) =
            permission.codex_policy();
        let mut params = serde_json::json!({
            "approvalPolicy": approval_policy,
            "approvalsReviewer": approvals_reviewer,
            "sandbox": sandbox,
            "sandboxPolicy": sandbox_policy,
            "networkAccess": self.network_access,
            "config": thread_config(self.web_search_allowed, reasoning_effort),
            "ephemeral": !persist_session,
            "personality": "none",
        });

        if !developer_instructions.is_empty() {
            params["developerInstructions"] = Value::String(developer_instructions.to_string());
        }
        if let Some(model) = model {
            params["model"] = Value::String(model.to_string());
        }
        if fast {
            // Codex CLI /fast maps to service_tier="fast" on the OpenAI
            // Responses API. App-server accepts `fast` or `flex`.
            params["serviceTier"] = Value::String("fast".to_string());
        }
        inject_mcp_servers_config(&mut params, mcp_config_path);

        let response = self.send_request("thread/start", Some(params))?;
        validate_reasoning_effort(&response, reasoning_effort)?;
        let workspace_id = response
            .get("threadId")
            .and_then(Value::as_str)
            .or_else(|| response.pointer("/thread/id").and_then(Value::as_str))
            .unwrap_or("")
            .to_string();
        self.workspace_id = Some(workspace_id.clone());
        Ok(workspace_id)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn thread_resume(
        &mut self,
        thread_id: &str,
        developer_instructions: &str,
        model: Option<&str>,
        reasoning_effort: Option<&str>,
        mcp_config_path: Option<&str>,
        fast: bool,
        working_directory: &str,
    ) -> Result<String> {
        self.thread_resume_with_permission(
            thread_id,
            developer_instructions,
            model,
            reasoning_effort,
            mcp_config_path,
            fast,
            working_directory,
            LocalAgentPermission::FullAccess,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn thread_resume_with_permission(
        &mut self,
        thread_id: &str,
        developer_instructions: &str,
        model: Option<&str>,
        reasoning_effort: Option<&str>,
        mcp_config_path: Option<&str>,
        fast: bool,
        working_directory: &str,
        permission: LocalAgentPermission,
    ) -> Result<String> {
        self.thread_resume_request(
            thread_id,
            developer_instructions,
            model,
            reasoning_effort,
            mcp_config_path,
            fast,
            working_directory,
            permission,
        )
        .map(|(workspace_id, _)| workspace_id)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn thread_resume_with_permission_streaming<F>(
        &mut self,
        thread_id: &str,
        developer_instructions: &str,
        model: Option<&str>,
        reasoning_effort: Option<&str>,
        mcp_config_path: Option<&str>,
        fast: bool,
        working_directory: &str,
        permission: LocalAgentPermission,
        output_schema_requested: bool,
        mut on_notification: F,
    ) -> Result<(String, Option<TurnResult>)>
    where
        F: FnMut(&JsonRpcMessage) -> Result<()>,
    {
        let params = self.thread_resume_params(
            thread_id,
            developer_instructions,
            model,
            reasoning_effort,
            mcp_config_path,
            fast,
            working_directory,
            permission,
        );
        let mut resumed_turn: Option<TurnState> = None;
        let mut completed_turn: Option<TurnResult> = None;
        let (response, _) = self.send_request_inner_with_timeout_observing(
            "thread/resume",
            Some(params),
            false,
            REQUEST_TIMEOUT,
            |message| {
                on_notification(message)?;
                if completed_turn.is_some() {
                    return Ok(());
                }
                let Some(turn_id) = notification_turn_id(message) else {
                    return Ok(());
                };
                let state = resumed_turn.get_or_insert_with(|| {
                    TurnState::new(
                        thread_id.to_string(),
                        turn_id.to_string(),
                        output_schema_requested,
                    )
                });
                if state.turn_id != turn_id {
                    return Ok(());
                }
                if let Some(result) = state.handle_message(message.clone(), &mut |_| Ok(()))? {
                    completed_turn = Some(result);
                }
                Ok(())
            },
        )?;
        validate_reasoning_effort(&response, reasoning_effort)?;
        let workspace_id = response
            .get("threadId")
            .and_then(Value::as_str)
            .or_else(|| response.pointer("/thread/id").and_then(Value::as_str))
            .unwrap_or(thread_id)
            .to_string();
        self.workspace_id = Some(workspace_id.clone());
        Ok((workspace_id, completed_turn))
    }

    #[allow(clippy::too_many_arguments)]
    fn thread_resume_request(
        &mut self,
        thread_id: &str,
        developer_instructions: &str,
        model: Option<&str>,
        reasoning_effort: Option<&str>,
        mcp_config_path: Option<&str>,
        fast: bool,
        working_directory: &str,
        permission: LocalAgentPermission,
    ) -> Result<(String, Option<TurnResult>)> {
        let params = self.thread_resume_params(
            thread_id,
            developer_instructions,
            model,
            reasoning_effort,
            mcp_config_path,
            fast,
            working_directory,
            permission,
        );
        let response = self.send_request("thread/resume", Some(params))?;
        validate_reasoning_effort(&response, reasoning_effort)?;
        let workspace_id = response
            .get("threadId")
            .and_then(Value::as_str)
            .or_else(|| response.pointer("/thread/id").and_then(Value::as_str))
            .unwrap_or(thread_id)
            .to_string();
        self.workspace_id = Some(workspace_id.clone());
        Ok((workspace_id, None))
    }

    #[allow(clippy::too_many_arguments)]
    fn thread_resume_params(
        &self,
        thread_id: &str,
        developer_instructions: &str,
        model: Option<&str>,
        reasoning_effort: Option<&str>,
        mcp_config_path: Option<&str>,
        fast: bool,
        working_directory: &str,
        permission: LocalAgentPermission,
    ) -> Value {
        let (approval_policy, sandbox, sandbox_policy, approvals_reviewer) =
            permission.codex_policy();
        let mut params = serde_json::json!({
            "threadId": thread_id,
            "approvalPolicy": approval_policy,
            "approvalsReviewer": approvals_reviewer,
            "sandbox": sandbox,
            "sandboxPolicy": sandbox_policy,
            "networkAccess": self.network_access,
            "config": thread_config(self.web_search_allowed, reasoning_effort),
            "cwd": working_directory,
            "personality": "none",
        });

        if !developer_instructions.is_empty() {
            params["developerInstructions"] = Value::String(developer_instructions.to_string());
        }
        if let Some(model) = model {
            params["model"] = Value::String(model.to_string());
        }
        if fast {
            params["serviceTier"] = Value::String("fast".to_string());
        }
        inject_mcp_servers_config(&mut params, mcp_config_path);
        params
    }

    pub fn thread_compact(&mut self, thread_id: &str) -> Result<()> {
        self.send_request(
            "thread/compact/start",
            Some(serde_json::json!({ "threadId": thread_id })),
        )?;
        Ok(())
    }

    pub fn turn_execute_streaming<F>(
        &mut self,
        prompt: &str,
        working_directory: &str,
        on_notification: F,
    ) -> Result<TurnResult>
    where
        F: FnMut(&JsonRpcMessage) -> Result<()>,
    {
        self.turn_execute_streaming_with_schema(prompt, working_directory, None, on_notification)
    }

    pub fn turn_execute_streaming_with_schema<F>(
        &mut self,
        prompt: &str,
        working_directory: &str,
        output_schema: Option<&Value>,
        on_notification: F,
    ) -> Result<TurnResult>
    where
        F: FnMut(&JsonRpcMessage) -> Result<()>,
    {
        self.turn_execute_streaming_with_schema_and_steering(
            prompt,
            working_directory,
            output_schema,
            None,
            on_notification,
        )
    }

    pub fn turn_execute_streaming_with_schema_and_steering<F>(
        &mut self,
        prompt: &str,
        working_directory: &str,
        output_schema: Option<&Value>,
        control_rx: Option<&mut tokio::sync::mpsc::Receiver<super::chat_stream::ChatStreamControl>>,
        on_notification: F,
    ) -> Result<TurnResult>
    where
        F: FnMut(&JsonRpcMessage) -> Result<()>,
    {
        self.turn_execute_streaming_with_schema_steering_and_attachments(
            prompt,
            &[],
            working_directory,
            output_schema,
            control_rx,
            on_notification,
        )
    }

    pub fn turn_execute_streaming_with_schema_steering_and_attachments<F>(
        &mut self,
        prompt: &str,
        attachments: &[PathBuf],
        working_directory: &str,
        output_schema: Option<&Value>,
        mut control_rx: Option<
            &mut tokio::sync::mpsc::Receiver<super::chat_stream::ChatStreamControl>,
        >,
        mut on_notification: F,
    ) -> Result<TurnResult>
    where
        F: FnMut(&JsonRpcMessage) -> Result<()>,
    {
        let workspace_id = self
            .workspace_id
            .clone()
            .context("no active thread - call thread_start or thread_resume first")?;
        let mut params = serde_json::json!({
            "threadId": workspace_id,
            "input": turn_user_input(prompt, attachments),
            "cwd": working_directory,
            "summary": "detailed",
        });
        if let Some(schema) = output_schema {
            // Codex app-server: per-turn JSON schema enforcement. Response
            // will be constrained to match.
            params["outputSchema"] = schema.clone();
        }

        let (turn_start_result, early_notifications) =
            self.send_request_inner("turn/start", Some(params), true)?;
        let turn_id = extract_turn_id(&turn_start_result)
            .context("Codex turn/start response did not include an active turn id")?;

        let mut state = TurnState::new(
            workspace_id.clone(),
            turn_id.clone(),
            output_schema.is_some(),
        );

        for msg in early_notifications {
            if let Some(result) = state.handle_message(msg, &mut on_notification)? {
                return Ok(result);
            }
        }
        if let Some(result) = self.drain_pending_steers(
            &workspace_id,
            &turn_id,
            &mut control_rx,
            &mut state,
            &mut on_notification,
        )? {
            return Ok(result);
        }

        loop {
            if let Some(result) = self.drain_pending_steers(
                &workspace_id,
                &turn_id,
                &mut control_rx,
                &mut state,
                &mut on_notification,
            )? {
                return Ok(result);
            }
            let Some(msg) = self.read_message_timeout(Duration::from_millis(50))? else {
                continue;
            };
            if self.answer_server_request(
                &msg,
                &mut control_rx,
                &mut state,
                &mut on_notification,
            )? {
                continue;
            }
            if let Some(result) = state.handle_message(msg, &mut on_notification)? {
                return Ok(result);
            }
        }
    }

    fn answer_server_request<F>(
        &mut self,
        message: &JsonRpcMessage,
        control_rx: &mut Option<
            &mut tokio::sync::mpsc::Receiver<super::chat_stream::ChatStreamControl>,
        >,
        state: &mut TurnState,
        on_notification: &mut F,
    ) -> Result<bool>
    where
        F: FnMut(&JsonRpcMessage) -> Result<()>,
    {
        if !state.accepts_message(message) {
            if let Some(request_id) = message.id {
                if let Some(response) = unattended_server_request_response(message) {
                    self.send_response(request_id, response)?;
                } else {
                    self.send_error_response(
                        request_id,
                        -32600,
                        "request belongs to an inactive turn".to_string(),
                    )?;
                }
            }
            tracing::debug!(
                method = message.method.as_deref().unwrap_or_default(),
                active_turn_id = %state.turn_id,
                message_turn_id = notification_turn_id(message).unwrap_or_default(),
                "discarded Codex request from an inactive turn"
            );
            return Ok(true);
        }
        let method = message.method.as_deref().unwrap_or("");
        if method == "currentTime/read" {
            let request_id = message
                .id
                .context("Codex current-time request did not include a JSON-RPC id")?;
            let current_time_at: i64 = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .context("system clock is before the Unix epoch")?
                .as_secs()
                .try_into()
                .context("current Unix time does not fit app-server's i64 response")?;
            self.send_response(
                request_id,
                serde_json::json!({ "currentTimeAt": current_time_at }),
            )?;
            return Ok(true);
        }
        if method == "item/tool/call" {
            let request_id = message
                .id
                .context("Codex dynamic-tool request did not include a JSON-RPC id")?;
            self.send_response(
                request_id,
                serde_json::json!({
                    "success": false,
                    "contentItems": [{
                        "type": "inputText",
                        "text": "Borg did not register this dynamic client tool; use the advertised Codex or MCP tools instead."
                    }]
                }),
            )?;
            return Ok(true);
        }
        if matches!(
            method,
            "item/tool/requestUserInput" | "mcpServer/elicitation/request"
        ) {
            let request_id = message
                .id
                .context("Codex interaction request did not include a JSON-RPC id")?;
            state.handle_message(message.clone(), on_notification)?;
            let fallback = if method == "item/tool/requestUserInput" {
                serde_json::json!({ "answers": {} })
            } else {
                serde_json::json!({ "action": "cancel" })
            };
            let deadline = message
                .params
                .as_ref()
                .and_then(|params| params.get("autoResolutionMs"))
                .and_then(Value::as_u64)
                .map(|milliseconds| Instant::now() + Duration::from_millis(milliseconds));
            let response = loop {
                let Some(receiver) = control_rx.as_deref_mut() else {
                    break fallback;
                };
                match recv_control_until(receiver, deadline) {
                    Some(ChatStreamControl::ProviderInteractionResponse {
                        interaction_id,
                        response,
                    }) if interaction_id == request_id.to_string() => break response,
                    Some(ChatStreamControl::Steer { ack, .. }) => {
                        let _ = ack.send(Err(
                            "answer the pending provider request before steering this turn"
                                .to_string(),
                        ));
                    }
                    Some(ChatStreamControl::Interrupt) => break fallback,
                    Some(ChatStreamControl::Approval { .. })
                    | Some(ChatStreamControl::ProviderInteractionResponse { .. }) => {}
                    None => break fallback,
                }
            };
            self.send_response(request_id, response)?;
            return Ok(true);
        }
        if !matches!(
            method,
            "item/commandExecution/requestApproval"
                | "item/fileChange/requestApproval"
                | "item/permissions/requestApproval"
                | "execCommandApproval"
                | "applyPatchApproval"
        ) {
            return Ok(false);
        }
        let request_id = message
            .id
            .context("Codex approval request did not include a JSON-RPC id")?;
        state.handle_message(message.clone(), on_notification)?;
        let decision = loop {
            let Some(receiver) = control_rx.as_deref_mut() else {
                break "decline";
            };
            match receiver.blocking_recv() {
                Some(ChatStreamControl::Approval {
                    approval_id,
                    decision,
                }) if approval_id == request_id.to_string() => {
                    break match decision {
                        ChatApprovalDecision::ApproveOnce => "accept",
                        ChatApprovalDecision::ApproveSession => "acceptForSession",
                        ChatApprovalDecision::Reject => "decline",
                    };
                }
                Some(ChatStreamControl::Steer { ack, .. }) => {
                    let _ = ack.send(Err(
                        "answer the pending approval before steering this turn".to_string()
                    ));
                }
                Some(ChatStreamControl::Interrupt) => break "cancel",
                Some(ChatStreamControl::Approval { .. })
                | Some(ChatStreamControl::ProviderInteractionResponse { .. }) => {}
                None => break "decline",
            }
        };
        let response = match method {
            "item/permissions/requestApproval" => {
                let permissions = if matches!(decision, "accept" | "acceptForSession") {
                    message
                        .params
                        .as_ref()
                        .and_then(|params| params.get("permissions"))
                        .cloned()
                        .unwrap_or_else(|| serde_json::json!({}))
                } else {
                    serde_json::json!({})
                };
                serde_json::json!({
                    "permissions": permissions,
                    "scope": if decision == "acceptForSession" { "session" } else { "turn" },
                })
            }
            "execCommandApproval" | "applyPatchApproval" => {
                let legacy_decision = match decision {
                    "accept" => serde_json::json!("approved"),
                    "acceptForSession" => serde_json::json!("approved_for_session"),
                    "decline" => serde_json::json!({ "denied": { "rejection": "denied by user" } }),
                    _ => serde_json::json!("abort"),
                };
                serde_json::json!({ "decision": legacy_decision })
            }
            _ => serde_json::json!({ "decision": decision }),
        };
        self.send_response(request_id, response)?;
        Ok(true)
    }

    fn drain_pending_steers<F>(
        &mut self,
        workspace_id: &str,
        turn_id: &str,
        control_rx: &mut Option<
            &mut tokio::sync::mpsc::Receiver<super::chat_stream::ChatStreamControl>,
        >,
        state: &mut TurnState,
        on_notification: &mut F,
    ) -> Result<Option<TurnResult>>
    where
        F: FnMut(&JsonRpcMessage) -> Result<()>,
    {
        let Some(rx) = control_rx.as_deref_mut() else {
            return Ok(None);
        };
        loop {
            let command = match rx.try_recv() {
                Ok(command) => command,
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => return Ok(None),
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => return Ok(None),
            };
            match command {
                super::chat_stream::ChatStreamControl::Steer {
                    client_user_message_id,
                    text,
                    attachments,
                    ack,
                } => {
                    if text.trim().is_empty() && attachments.is_empty() {
                        let _ = ack.send(Err("cannot steer with empty input".to_string()));
                        continue;
                    }
                    let mut params = serde_json::json!({
                        "threadId": workspace_id,
                        "input": turn_user_input(&text, &attachments),
                        "expectedTurnId": turn_id,
                    });
                    if let Some(client_id) = client_user_message_id.as_deref() {
                        params["clientUserMessageId"] = Value::String(client_id.to_string());
                    }
                    let result = self.send_request_inner("turn/steer", Some(params), true);
                    match result {
                        Ok((_response, notifications)) => {
                            let _ = ack.send(Ok(()));
                            for msg in notifications {
                                if let Some(result) = state.handle_message(msg, on_notification)? {
                                    return Ok(Some(result));
                                }
                            }
                        }
                        Err(err) => {
                            let _ = ack.send(Err(format!("{err:#}")));
                            if let Some(result) =
                                self.drain_deferred_notifications(state, on_notification)?
                            {
                                return Ok(Some(result));
                            }
                        }
                    }
                }
                super::chat_stream::ChatStreamControl::Approval { .. } => {
                    // Approval decisions are consumed only while a matching
                    // server-initiated approval request is pending.
                }
                super::chat_stream::ChatStreamControl::ProviderInteractionResponse { .. } => {
                    // Provider interaction responses are consumed only while
                    // a matching server-initiated request is pending.
                }
                super::chat_stream::ChatStreamControl::Interrupt => {
                    let params = serde_json::json!({
                        "threadId": workspace_id,
                        "turnId": turn_id,
                    });
                    let (_, notifications) =
                        match self.send_request_inner("turn/interrupt", Some(params), true) {
                            Ok(result) => result,
                            Err(error) if expected_inactive_turn_interrupt(&error) => {
                                // The provider can finish between the session actor
                                // receiving Interrupt and this request reaching the
                                // app-server. In that race there is no active turn
                                // left to interrupt; ending this stream is the
                                // correct idempotent result, not a provider failure.
                                tracing::debug!(
                                    %error,
                                    turn_id,
                                    "Codex turn was already inactive when interrupt arrived"
                                );
                                if let Some(result) =
                                    self.drain_deferred_notifications(state, on_notification)?
                                {
                                    return Ok(Some(result));
                                }
                                return Ok(Some(TurnResult {
                                    workspace_id: state.workspace_id.clone(),
                                    output_text: state.output_text.clone(),
                                    reasoning_text: state.reasoning_text.clone(),
                                    raw_notifications: state.raw_notifications.clone(),
                                    turn_token_usage: state.turn_token_usage.clone(),
                                    total_token_usage: state.total_token_usage.clone(),
                                    model_context_window: state.model_context_window,
                                }));
                            }
                            Err(error) => return Err(error),
                        };
                    for msg in notifications {
                        if let Some(result) = state.handle_message(msg, on_notification)? {
                            return Ok(Some(result));
                        }
                    }
                }
            }
        }
    }

    fn drain_deferred_notifications<F>(
        &mut self,
        state: &mut TurnState,
        on_notification: &mut F,
    ) -> Result<Option<TurnResult>>
    where
        F: FnMut(&JsonRpcMessage) -> Result<()>,
    {
        let notifications = std::mem::take(&mut self.deferred_notifications);
        for message in notifications {
            if let Some(result) = state.handle_message(message, on_notification)? {
                return Ok(Some(result));
            }
        }
        Ok(None)
    }

    pub fn shutdown(&mut self) -> Result<()> {
        self.stdin.take();
        let deadline = Instant::now() + SHUTDOWN_TIMEOUT;
        let status = loop {
            if self
                .cancellation
                .as_ref()
                .is_some_and(|flag| flag.load(Ordering::Acquire))
            {
                tracing::debug!("cancelling Codex app-server shutdown with its owning turn");
                self.child
                    .kill()
                    .context("failed killing cancelled codex app-server")?;
                break self
                    .child
                    .wait()
                    .context("failed waiting for cancelled codex app-server")?;
            }
            if let Some(status) = self
                .child
                .try_wait()
                .context("failed checking codex app-server process during shutdown")?
            {
                break status;
            }
            if Instant::now() >= deadline {
                tracing::warn!(
                    timeout_ms = SHUTDOWN_TIMEOUT.as_millis(),
                    "codex app-server did not exit during graceful shutdown; killing it"
                );
                self.child
                    .kill()
                    .context("failed killing codex app-server during shutdown")?;
                break self
                    .child
                    .wait()
                    .context("failed waiting for killed codex app-server")?;
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        if !status.success() {
            tracing::warn!(
                ?status,
                "codex app-server process exited unsuccessfully during shutdown"
            );
        }
        Ok(())
    }

    fn send_request(&mut self, method: &str, params: Option<Value>) -> Result<Value> {
        self.send_request_inner(method, params, false)
            .map(|(result, _)| result)
    }

    fn send_request_inner(
        &mut self,
        method: &str,
        params: Option<Value>,
        collect_notifications: bool,
    ) -> Result<(Value, Vec<JsonRpcMessage>)> {
        let timeout = if method == "turn/interrupt" {
            INTERRUPT_REQUEST_TIMEOUT
        } else if method == "turn/steer" {
            CONTROL_REQUEST_TIMEOUT
        } else {
            REQUEST_TIMEOUT
        };
        self.send_request_inner_with_timeout(method, params, collect_notifications, timeout)
    }

    fn send_request_inner_with_timeout(
        &mut self,
        method: &str,
        params: Option<Value>,
        collect_notifications: bool,
        timeout: Duration,
    ) -> Result<(Value, Vec<JsonRpcMessage>)> {
        self.send_request_inner_with_timeout_observing(
            method,
            params,
            collect_notifications,
            timeout,
            |_| Ok(()),
        )
    }

    fn send_request_inner_with_timeout_observing<F>(
        &mut self,
        method: &str,
        params: Option<Value>,
        collect_notifications: bool,
        timeout: Duration,
        mut observe_notification: F,
    ) -> Result<(Value, Vec<JsonRpcMessage>)>
    where
        F: FnMut(&JsonRpcMessage) -> Result<()>,
    {
        self.ensure_not_cancelled()?;
        let id = self.next_id;
        self.next_id += 1;
        let mut notifications = Vec::new();
        let deadline = Instant::now() + timeout;

        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id,
            method: method.to_string(),
            params,
        };

        let line = serde_json::to_string(&request)?;
        self.ensure_not_cancelled()?;
        let stdin = self
            .stdin
            .as_mut()
            .context("codex app-server stdin closed")?;
        writeln!(stdin, "{line}")?;
        stdin.flush()?;

        loop {
            self.ensure_not_cancelled()?;
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                if collect_notifications && matches!(method, "turn/interrupt" | "turn/steer") {
                    self.deferred_notifications.extend(notifications);
                }
                bail!(
                    "timed out after {} ms waiting for codex app-server response to {method} (request id {id})",
                    timeout.as_millis()
                );
            }
            let Some(msg) = self.read_message_timeout(remaining.min(REQUEST_READ_POLL_INTERVAL))?
            else {
                continue;
            };
            // A server-initiated request has an id and a method. Handle that
            // direction before matching client responses so an app-server
            // request that happens to reuse our numeric id can never be
            // mistaken for the response to our outstanding request.
            if let (Some(server_request_id), Some(server_method)) = (msg.id, msg.method.as_deref())
            {
                if let Some(response) = unattended_server_request_response(&msg) {
                    self.send_response(server_request_id, response)?;
                } else {
                    self.send_error_response(
                        server_request_id,
                        -32601,
                        format!("Borg does not provide the optional host service {server_method}"),
                    )?;
                }
                observe_notification(&msg)?;
                if collect_notifications {
                    notifications.push(msg);
                }
                continue;
            }
            if msg.id == Some(id) {
                if let Some(error) = msg.error {
                    if collect_notifications && matches!(method, "turn/interrupt" | "turn/steer") {
                        self.deferred_notifications.extend(notifications);
                    }
                    // Preserve the structured JSON-RPC error so callers can
                    // classify rate-limit / overload / auth without string
                    // matching. The wrapper's classifier looks for
                    // `"code":429` / `"code":503` / `"code":529` substrings.
                    bail!(
                        "codex app-server error for {method}: {}",
                        serde_json::to_string(&error).unwrap_or_else(|_| error.to_string())
                    );
                }
                return Ok((msg.result.unwrap_or(Value::Null), notifications));
            }
            if msg.id.is_some() {
                self.quarantine_response(msg, method, id);
                continue;
            }
            observe_notification(&msg)?;
            if collect_notifications {
                notifications.push(msg);
            }
        }
    }

    fn quarantine_response(&mut self, response: JsonRpcMessage, method: &str, expected_id: u64) {
        tracing::debug!(
            response_id = ?response.id,
            expected_id,
            method,
            "quarantined unmatched Codex app-server response"
        );
        if self.quarantined_responses.len() == MAX_QUARANTINED_RESPONSES {
            self.quarantined_responses.pop_front();
        }
        self.quarantined_responses.push_back(response);
    }

    fn send_notification(&mut self, method: &str, params: Option<Value>) -> Result<()> {
        let notification = serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params.unwrap_or(Value::Null),
        });
        let line = serde_json::to_string(&notification)?;
        let stdin = self
            .stdin
            .as_mut()
            .context("codex app-server stdin closed")?;
        writeln!(stdin, "{line}")?;
        stdin.flush()?;
        Ok(())
    }

    fn send_response(&mut self, id: u64, result: Value) -> Result<()> {
        let response = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": result,
        });
        let line = serde_json::to_string(&response)?;
        let stdin = self
            .stdin
            .as_mut()
            .context("codex app-server stdin closed")?;
        writeln!(stdin, "{line}")?;
        stdin.flush()?;
        Ok(())
    }

    fn send_error_response(&mut self, id: u64, code: i64, message: String) -> Result<()> {
        let response = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {
                "code": code,
                "message": message,
            },
        });
        let line = serde_json::to_string(&response)?;
        let stdin = self
            .stdin
            .as_mut()
            .context("codex app-server stdin closed")?;
        writeln!(stdin, "{line}")?;
        stdin.flush()?;
        Ok(())
    }

    fn read_message_timeout(&mut self, timeout: Duration) -> Result<Option<JsonRpcMessage>> {
        self.ensure_not_cancelled()?;
        match self.reader.recv_timeout(timeout) {
            Ok(ReaderMessage::Message(message)) => Ok(Some(message)),
            Ok(ReaderMessage::Failed(error)) => bail!("{error}"),
            Err(std_mpsc::RecvTimeoutError::Timeout) => Ok(None),
            Err(std_mpsc::RecvTimeoutError::Disconnected) => {
                bail!("codex app-server reader stopped unexpectedly")
            }
        }
    }

    fn ensure_not_cancelled(&self) -> Result<()> {
        if self
            .cancellation
            .as_ref()
            .is_some_and(|flag| flag.load(Ordering::Acquire))
        {
            bail!("Codex app-server request cancelled with its owning turn")
        }
        Ok(())
    }

    pub(crate) fn detach_cancellation(&mut self) {
        self.cancellation = None;
    }

    pub(crate) fn attach_cancellation(&mut self, cancellation: Arc<AtomicBool>) {
        self.cancellation = Some(cancellation);
    }
}

/// Own stdout reads on one dedicated thread so no JSON-RPC call can block the
/// provider executor inside `BufRead::read_line`. The client consumes parsed
/// frames through a timeout-capable channel, which bounds request waits even
/// when the child writes a partial or unterminated frame.
fn spawn_reader(stdout: ChildStdout) -> std_mpsc::Receiver<ReaderMessage> {
    let (sender, receiver) = std_mpsc::channel();
    std::thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => {
                    let _ = sender.send(ReaderMessage::Failed(
                        "codex app-server stdout closed unexpectedly".to_string(),
                    ));
                    break;
                }
                Ok(_) => {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    match serde_json::from_str::<JsonRpcMessage>(trimmed) {
                        Ok(message) => {
                            if sender.send(ReaderMessage::Message(message)).is_err() {
                                break;
                            }
                        }
                        Err(error) => {
                            let _ = sender.send(ReaderMessage::Failed(format!(
                                "failed to parse app-server message: {trimmed}: {error}"
                            )));
                            break;
                        }
                    }
                }
                Err(error) => {
                    let _ = sender.send(ReaderMessage::Failed(format!(
                        "failed to read from codex app-server: {error}"
                    )));
                    break;
                }
            }
        }
    });
    receiver
}

fn unattended_server_request_response(message: &JsonRpcMessage) -> Option<Value> {
    match message.method.as_deref()? {
        "currentTime/read" => {
            let current_time_at = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .ok()?
                .as_secs();
            let current_time_at = i64::try_from(current_time_at).ok()?;
            Some(serde_json::json!({ "currentTimeAt": current_time_at }))
        }
        "item/tool/call" => Some(serde_json::json!({
            "success": false,
            "contentItems": [{
                "type": "inputText",
                "text": "Borg did not register this dynamic client tool; use the advertised Codex or MCP tools instead."
            }]
        })),
        "item/tool/requestUserInput" => Some(serde_json::json!({ "answers": {} })),
        "mcpServer/elicitation/request" => Some(serde_json::json!({ "action": "cancel" })),
        "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" => {
            Some(serde_json::json!({ "decision": "decline" }))
        }
        "item/permissions/requestApproval" => Some(serde_json::json!({
            "permissions": {},
            "scope": "turn",
        })),
        "execCommandApproval" | "applyPatchApproval" => Some(serde_json::json!({
            "decision": {
                "denied": {
                    "rejection": "interactive approval was unavailable"
                }
            }
        })),
        _ => None,
    }
}

fn recv_control_until(
    receiver: &mut tokio::sync::mpsc::Receiver<ChatStreamControl>,
    deadline: Option<Instant>,
) -> Option<ChatStreamControl> {
    let Some(deadline) = deadline else {
        return receiver.blocking_recv();
    };
    loop {
        match receiver.try_recv() {
            Ok(control) => return Some(control),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => return None,
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return None;
                }
                std::thread::sleep(remaining.min(Duration::from_millis(10)));
            }
        }
    }
}

fn parse_codex_weekly_usage(response: &Value) -> Result<CodexWeeklyUsage> {
    const WEEKLY_WINDOW_MINS: u64 = 7 * 24 * 60;

    let snapshot = response
        .pointer("/rateLimitsByLimitId/codex")
        .or_else(|| response.get("rateLimits"))
        .context("Codex did not return its account rate-limit bucket")?;
    let window = ["primary", "secondary"]
        .into_iter()
        .filter_map(|name| snapshot.get(name))
        .find(|window| {
            window.get("windowDurationMins").and_then(Value::as_u64) == Some(WEEKLY_WINDOW_MINS)
        })
        .context("Codex did not return a seven-day usage window")?;
    let used_percent = window
        .get("usedPercent")
        .and_then(Value::as_u64)
        .context("Codex weekly usage is missing usedPercent")?;
    anyhow::ensure!(
        used_percent <= 100,
        "Codex returned an invalid weekly usedPercent of {used_percent}"
    );

    Ok(CodexWeeklyUsage {
        used_percent: used_percent as u8,
        resets_at: window.get("resetsAt").and_then(Value::as_i64),
    })
}

fn turn_user_input(prompt: &str, attachments: &[PathBuf]) -> Vec<Value> {
    let mut input = vec![serde_json::json!({
        "type": "text",
        "text": prompt,
    })];
    input.extend(attachments.iter().map(|path| {
        serde_json::json!({
            "type": "localImage",
            "path": path,
        })
    }));
    input
}

impl LocalAgentPermission {
    fn codex_policy(self) -> (&'static str, &'static str, &'static str, &'static str) {
        match self {
            Self::FullAccess => ("never", "danger-full-access", "dangerFullAccess", "user"),
            Self::Auto => (
                "untrusted",
                "workspace-write",
                "workspaceWrite",
                "auto_review",
            ),
            Self::Manual => ("untrusted", "workspace-write", "workspaceWrite", "user"),
        }
    }
}

/// Extract the active turn only from the `turn/start` response.
///
/// Notifications are read from a pooled app-server stdout stream and may
/// include queued events from an earlier turn. Inferring the active turn from
/// a buffered `turn/started` notification can therefore make that old turn
/// look current before notification filtering begins.
fn extract_turn_id(result: &Value) -> Option<String> {
    result
        .pointer("/turn/id")
        .and_then(Value::as_str)
        .or_else(|| result.get("turnId").and_then(Value::as_str))
        .map(str::to_string)
}

/// Return the provider turn that owns a notification or server request.
///
/// Codex app-server keeps a single stdout stream across pooled turns. Messages
/// from the previous turn can therefore already be buffered when `turn/start`
/// for the next turn is acknowledged. They must never be projected into the
/// new Borg turn merely because they were read while that turn was active.
fn notification_turn_id(message: &JsonRpcMessage) -> Option<&str> {
    let params = message.params.as_ref()?;
    params
        .get("turnId")
        .and_then(Value::as_str)
        .or_else(|| params.pointer("/turn/id").and_then(Value::as_str))
}

/// Current app-server protocol notifications that describe model work always
/// carry a turn id. Treat a missing id as untrusted rather than silently
/// attaching stale buffered work to whichever turn happens to read it.
/// `error` is intentionally not included: structured turn errors carry an id,
/// but global/auth/transport errors may not and must still surface to callers.
fn notification_requires_turn_id(method: Option<&str>) -> bool {
    let Some(method) = method else {
        return false;
    };
    method == "thread/tokenUsage/updated"
        || method.starts_with("turn/")
        || method.starts_with("item/")
}

fn clear_managed_codex_env(command: &mut Command) {
    for key in [
        "BORG_OPENAI_AUTH_JSON_B64",
        "BORG_OPENAI_CODEX_ACCESS_TOKEN",
        "OPENAI_CODEX_ACCESS_TOKEN",
    ] {
        command.env_remove(key);
    }
}

struct TurnState {
    workspace_id: String,
    turn_id: String,
    output_text: String,
    reasoning_text: String,
    raw_notifications: Vec<String>,
    turn_token_usage: Option<TokenUsage>,
    total_token_usage: Option<TokenUsage>,
    model_context_window: Option<u64>,
    agent_message_text: HashMap<String, String>,
    emitted_agent_message_ids: HashSet<String>,
    emitted_any_agent_message: bool,
    output_schema_requested: bool,
}

impl TurnState {
    fn new(workspace_id: String, turn_id: String, output_schema_requested: bool) -> Self {
        Self {
            workspace_id,
            turn_id,
            output_text: String::new(),
            reasoning_text: String::new(),
            raw_notifications: Vec::new(),
            turn_token_usage: None,
            total_token_usage: None,
            model_context_window: None,
            agent_message_text: HashMap::new(),
            emitted_agent_message_ids: HashSet::new(),
            emitted_any_agent_message: false,
            output_schema_requested,
        }
    }

    fn accepts_message(&self, message: &JsonRpcMessage) -> bool {
        match notification_turn_id(message) {
            Some(turn_id) => turn_id == self.turn_id,
            None => !notification_requires_turn_id(message.method.as_deref()),
        }
    }

    fn handle_message<F>(
        &mut self,
        msg: JsonRpcMessage,
        on_notification: &mut F,
    ) -> Result<Option<TurnResult>>
    where
        F: FnMut(&JsonRpcMessage) -> Result<()>,
    {
        if !self.accepts_message(&msg) {
            tracing::debug!(
                method = msg.method.as_deref().unwrap_or_default(),
                active_turn_id = %self.turn_id,
                message_turn_id = notification_turn_id(&msg).unwrap_or_default(),
                "quarantined Codex notification outside the active turn"
            );
            return Ok(None);
        }
        on_notification(&msg)?;
        self.raw_notifications.push(serde_json::to_string(&msg)?);

        match msg.method.as_deref().unwrap_or("") {
            "error" => {
                bail!("codex turn failed: {}", codex_error_message(&msg));
            }
            "item/agentMessage/delta" => {
                let item_id = msg
                    .params
                    .as_ref()
                    .and_then(|params| params.get("itemId"))
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let delta = msg
                    .params
                    .as_ref()
                    .and_then(|params| params.get("delta"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if !item_id.is_empty() {
                    self.begin_agent_message(&item_id);
                    self.agent_message_text
                        .entry(item_id.clone())
                        .or_default()
                        .push_str(delta);
                } else {
                    self.begin_agent_message("");
                }
                self.output_text.push_str(delta);
            }
            "item/reasoning/summaryTextDelta" | "item/reasoning/textDelta" => {
                if let Some(delta) = msg
                    .params
                    .as_ref()
                    .and_then(|params| params.get("delta"))
                    .and_then(Value::as_str)
                {
                    self.reasoning_text.push_str(delta);
                }
            }
            "item/completed" => {
                if let Some(item) = msg.params.as_ref().and_then(|params| params.get("item")) {
                    let item_type = item.get("type").and_then(Value::as_str).unwrap_or("");
                    if matches_codex_type(item_type, &["agentMessage"]) {
                        self.merge_completed_agent_message(item);
                    }
                }
            }
            "thread/tokenUsage/updated" => {
                if let Some(usage) = msg
                    .params
                    .as_ref()
                    .and_then(|params| params.get("tokenUsage"))
                {
                    self.turn_token_usage = parse_token_usage(usage.get("last"));
                    self.total_token_usage = parse_token_usage(usage.get("total"));
                    self.model_context_window =
                        usage.get("modelContextWindow").and_then(Value::as_u64);
                }
            }
            "turn/completed" => {
                if let Some(params) = &msg.params {
                    let status = params
                        .get("status")
                        .or_else(|| params.pointer("/turn/status"))
                        .and_then(Value::as_str)
                        .unwrap_or("unknown");
                    if status == "failed" {
                        let error = params
                            .get("error")
                            .and_then(error_value_text)
                            .unwrap_or_else(|| "unknown error".to_string());
                        bail!("codex turn failed: {error}");
                    }
                }
                let output_text = if self.output_schema_requested {
                    msg.params
                        .as_ref()
                        .and_then(extract_last_agent_message_text_from_turn_params)
                        .unwrap_or_else(|| self.output_text.clone())
                } else {
                    self.output_text.clone()
                };
                return Ok(Some(TurnResult {
                    workspace_id: self.workspace_id.clone(),
                    output_text,
                    reasoning_text: self.reasoning_text.clone(),
                    raw_notifications: self.raw_notifications.clone(),
                    turn_token_usage: self.turn_token_usage.clone(),
                    total_token_usage: self.total_token_usage.clone(),
                    model_context_window: self.model_context_window,
                }));
            }
            _ => {}
        }

        Ok(None)
    }

    fn merge_completed_agent_message(&mut self, item: &Value) {
        let Some(text) = extract_agent_message_text(item) else {
            return;
        };
        let item_id = item.get("id").and_then(Value::as_str).unwrap_or("");
        if item_id.is_empty() {
            self.begin_agent_message("");
            self.output_text.push_str(&text);
            return;
        }
        let existing = self
            .agent_message_text
            .get(item_id)
            .cloned()
            .unwrap_or_default();
        if let Some(suffix) = text.strip_prefix(existing.as_str()) {
            self.agent_message_text
                .entry(item_id.to_string())
                .or_default()
                .push_str(suffix);
            self.output_text.push_str(suffix);
        } else if existing.is_empty() {
            self.begin_agent_message(item_id);
            self.agent_message_text
                .insert(item_id.to_string(), text.clone());
            self.output_text.push_str(&text);
        }
    }

    fn begin_agent_message(&mut self, item_id: &str) {
        let is_new = if item_id.is_empty() {
            true
        } else {
            self.emitted_agent_message_ids.insert(item_id.to_string())
        };
        if !is_new {
            return;
        }
        if self.emitted_any_agent_message
            && !self.output_text.is_empty()
            && !self.output_text.ends_with(char::is_whitespace)
        {
            self.output_text.push_str("\n\n");
        }
        self.emitted_any_agent_message = true;
    }
}

fn extract_last_agent_message_text_from_turn_params(params: &Value) -> Option<String> {
    let items = params.pointer("/turn/items")?.as_array()?;
    items
        .iter()
        .rev()
        .find(|item| item.get("type").and_then(Value::as_str) == Some("agentMessage"))
        .and_then(|item| item.get("text").and_then(Value::as_str))
        .filter(|text| !text.is_empty())
        .map(str::to_string)
}

fn extract_agent_message_text(item: &Value) -> Option<String> {
    if let Some(text) = item.get("text").and_then(Value::as_str)
        && !text.is_empty()
    {
        return Some(text.to_string());
    }
    let blocks = item.get("content").and_then(Value::as_array)?;
    let text = blocks
        .iter()
        .filter_map(|block| {
            if let Some(text) = block.get("text").and_then(Value::as_str) {
                return Some(text.to_string());
            }
            block.as_str().map(ToString::to_string)
        })
        .collect::<String>();
    if text.is_empty() { None } else { Some(text) }
}

fn matches_codex_type(item_type: &str, expected: &[&str]) -> bool {
    expected.contains(&item_type)
}

fn parse_token_usage(value: Option<&Value>) -> Option<TokenUsage> {
    let value = value?;
    Some(TokenUsage {
        input_tokens: value
            .get("inputTokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        output_tokens: value
            .get("outputTokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        cached_input_tokens: value
            .get("cachedInputTokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        cache_write_input_tokens: value
            .get("cacheWriteInputTokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        reasoning_output_tokens: value
            .get("reasoningOutputTokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        total_tokens: value
            .get("totalTokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
    })
}

fn codex_error_message(message: &JsonRpcMessage) -> String {
    if let Some(text) = message
        .message
        .as_deref()
        .filter(|text| !text.trim().is_empty())
    {
        return text.to_string();
    }
    if let Some(error) = message.error.as_ref().and_then(error_value_text) {
        return error;
    }
    let Some(params) = message.params.as_ref() else {
        return "codex app-server emitted an error notification".to_string();
    };
    for key in ["message", "error", "detail", "details", "reason"] {
        if let Some(text) = params.get(key).and_then(error_value_text) {
            return text;
        }
    }
    "codex app-server emitted an error notification".to_string()
}

fn expected_inactive_turn_interrupt(error: &anyhow::Error) -> bool {
    let text = error.to_string().to_ascii_lowercase();
    text.contains("codex app-server error for turn/interrupt")
        && text.contains("\"code\":-32600")
        && text.contains("expected active turn id")
}

fn error_value_text(value: &Value) -> Option<String> {
    if let Some(text) = value.as_str().filter(|text| !text.trim().is_empty()) {
        return Some(text.to_string());
    }
    if let Some(object) = value.as_object() {
        for key in ["message", "error", "detail", "details", "reason"] {
            if let Some(text) = object.get(key).and_then(error_value_text) {
                return Some(text);
            }
        }
    }
    None
}

fn write_managed_codex_auth(codex_home: &Path, openai_api_key: &str) -> Result<()> {
    let auth_path = codex_home.join("auth.json");
    let auth = serde_json::json!({
        "auth_mode": "apikey",
        "OPENAI_API_KEY": openai_api_key,
    });
    let raw = serde_json::to_vec_pretty(&auth).context("failed to encode managed Codex auth")?;
    fs::write(&auth_path, raw)
        .with_context(|| format!("failed to write {}", auth_path.display()))?;
    Ok(())
}

fn thread_config(web_search_allowed: bool, reasoning_effort: Option<&str>) -> Value {
    let mut config = serde_json::json!({
        "web_search": if web_search_allowed { "live" } else { "disabled" },
        // Borg owns the durable subagent lifecycle. Leaving Codex's parallel
        // collaboration runtime enabled would expose a second model catalog
        // and a second child-session authority to the same acting model.
        "features": {
            "multi_agent": false,
            "multi_agent_v2": false,
        },
    });
    if let Some(effort) = reasoning_effort {
        // `reasoningEffort` was removed from ThreadStartParams and
        // ThreadResumeParams in app-server v2. The thread-level override now
        // travels through Codex's canonical config field.
        config["model_reasoning_effort"] = Value::String(effort.to_string());
    }
    config
}

fn validate_reasoning_effort(response: &Value, requested: Option<&str>) -> Result<()> {
    let Some(requested) = requested else {
        return Ok(());
    };
    if let Some(actual) = response.get("reasoningEffort").and_then(Value::as_str) {
        anyhow::ensure!(
            actual == requested,
            "codex app-server started the thread with reasoning effort `{actual}`, expected `{requested}`"
        );
    }
    Ok(())
}

fn inject_mcp_servers_config(params: &mut Value, mcp_config_path: Option<&str>) {
    let Some(config_path) = mcp_config_path else {
        return;
    };
    let Ok(raw) = super::read_provider_mcp_config_text(Path::new(config_path)) else {
        return;
    };
    let Ok(config) = serde_json::from_str::<Value>(&raw) else {
        return;
    };
    let Some(servers) = config.get("mcpServers").cloned() else {
        return;
    };
    params["config"]["mcp_servers"] = servers;
}

impl Drop for CodexAppServerClient {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    fn scripted_client(script: &str) -> CodexAppServerClient {
        let shell_env = crate::shell_env::CleanShellEnv::new().expect("clean shell environment");
        let mut child = Command::new("sh")
            .args(["-c", script])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("scripted app-server");
        let stdin = child.stdin.take().expect("script stdin");
        let stdout = child.stdout.take().expect("script stdout");
        CodexAppServerClient {
            child,
            stdin: Some(stdin),
            reader: spawn_reader(stdout),
            next_id: 1,
            workspace_id: None,
            network_access: false,
            web_search_allowed: false,
            deferred_notifications: Vec::new(),
            quarantined_responses: VecDeque::new(),
            cancellation: None,
            _shell_env: shell_env,
            _managed_codex_home: None,
        }
    }

    #[cfg(unix)]
    #[test]
    fn request_wait_quarantines_mismatched_response_ids() {
        let mut client = scripted_client(
            r#"read request
printf '%s\n' '{"jsonrpc":"2.0","id":99,"result":{"stale":true}}'
printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"matched":true}}'"#,
        );

        let (result, notifications) = client
            .send_request_inner_with_timeout("test/request", None, true, Duration::from_secs(1))
            .expect("matched response");

        assert_eq!(result, serde_json::json!({"matched": true}));
        assert!(notifications.is_empty());
        assert_eq!(client.quarantined_responses.len(), 1);
        assert_eq!(client.quarantined_responses[0].id, Some(99));
    }

    #[cfg(unix)]
    #[test]
    fn request_wait_exposes_a_bounded_timeout() {
        let mut client = scripted_client("read request\nsleep 1");

        let error = client
            .send_request_inner_with_timeout("turn/start", None, true, Duration::from_millis(25))
            .expect_err("request must time out");

        let message = error.to_string();
        assert!(message.contains("timed out after 25 ms"), "{message}");
        assert!(message.contains("turn/start"), "{message}");
        assert!(message.contains("request id 1"), "{message}");
    }

    #[cfg(unix)]
    #[test]
    fn partial_provider_frame_cannot_block_a_request_forever() {
        let mut client = scripted_client("printf '%s' '{\"jsonrpc\":\"2.0\",\"id\":1'; sleep 1");

        let error = client
            .send_request_inner_with_timeout("turn/start", None, true, Duration::from_millis(25))
            .expect_err("unterminated frame must time out");

        assert!(error.to_string().contains("timed out after 25 ms"));
    }

    #[cfg(unix)]
    #[test]
    fn resume_streams_notifications_from_the_resume_request() {
        let mut client = scripted_client(
            r#"read request
printf '%s\n' '{"jsonrpc":"2.0","method":"turn/started","params":{"turn":{"id":"turn-1"}}}'
printf '%s\n' '{"jsonrpc":"2.0","method":"item/reasoning/summaryTextDelta","params":{"turnId":"turn-1","delta":"thinking"}}'
printf '%s\n' '{"jsonrpc":"2.0","method":"item/agentMessage/delta","params":{"turnId":"turn-1","itemId":"message-1","delta":"reply"}}'
printf '%s\n' '{"jsonrpc":"2.0","method":"turn/completed","params":{"turnId":"turn-1","status":"completed"}}'
printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"threadId":"thread-1"}}'"#,
        );
        let mut seen = Vec::new();

        let (workspace_id, result) = client
            .thread_resume_with_permission_streaming(
                "thread-1",
                "",
                None,
                None,
                None,
                false,
                "/tmp",
                LocalAgentPermission::FullAccess,
                false,
                |message| {
                    seen.push(message.method.clone().unwrap_or_default());
                    Ok(())
                },
            )
            .expect("resume response");

        assert_eq!(workspace_id, "thread-1");
        let result = result.expect("resumed turn result");
        assert_eq!(result.output_text, "reply");
        assert_eq!(result.reasoning_text, "thinking");
        assert_eq!(
            seen,
            [
                "turn/started",
                "item/reasoning/summaryTextDelta",
                "item/agentMessage/delta",
                "turn/completed",
            ]
        );
    }

    #[cfg(unix)]
    #[test]
    fn request_wait_honors_owner_cancellation() {
        let mut client = scripted_client("read request\nsleep 1");
        let cancellation = Arc::new(AtomicBool::new(false));
        client.cancellation = Some(Arc::clone(&cancellation));
        let cancel_later = Arc::clone(&cancellation);
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(25));
            cancel_later.store(true, Ordering::Release);
        });

        let error = client
            .send_request_inner_with_timeout("turn/start", None, true, Duration::from_secs(1))
            .expect_err("owner cancellation must end a synchronous request wait");

        assert!(error.to_string().contains("cancelled with its owning turn"));
    }

    #[test]
    fn bounded_control_wait_observes_cancellation() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        tx.blocking_send(ChatStreamControl::Interrupt)
            .expect("queue cancellation");

        assert!(matches!(
            recv_control_until(&mut rx, Some(Instant::now() + Duration::from_millis(50))),
            Some(ChatStreamControl::Interrupt)
        ));
    }

    #[test]
    fn command_modes_select_the_expected_codex_reviewer() {
        assert_eq!(
            LocalAgentPermission::FullAccess.codex_policy(),
            ("never", "danger-full-access", "dangerFullAccess", "user")
        );
        assert_eq!(
            LocalAgentPermission::Auto.codex_policy(),
            (
                "untrusted",
                "workspace-write",
                "workspaceWrite",
                "auto_review"
            )
        );
        assert_eq!(
            LocalAgentPermission::Manual.codex_policy(),
            ("untrusted", "workspace-write", "workspaceWrite", "user")
        );
    }

    #[test]
    fn unattended_server_requests_are_resolved_without_granting_authority() {
        let permission_request = JsonRpcMessage {
            id: Some(7),
            method: Some("item/permissions/requestApproval".to_string()),
            message: None,
            result: None,
            error: None,
            params: Some(serde_json::json!({
                "permissions": {"network": {"enabled": true}}
            })),
        };
        assert_eq!(
            unattended_server_request_response(&permission_request),
            Some(serde_json::json!({
                "permissions": {},
                "scope": "turn",
            }))
        );

        let interaction_request = JsonRpcMessage {
            id: Some(8),
            method: Some("item/tool/requestUserInput".to_string()),
            message: None,
            result: None,
            error: None,
            params: None,
        };
        assert_eq!(
            unattended_server_request_response(&interaction_request),
            Some(serde_json::json!({"answers": {}}))
        );
    }

    #[test]
    fn account_usage_selects_the_real_codex_weekly_bucket() {
        let usage = parse_codex_weekly_usage(&serde_json::json!({
            "rateLimits": {
                "primary": {
                    "usedPercent": 99,
                    "windowDurationMins": 300
                }
            },
            "rateLimitsByLimitId": {
                "codex": {
                    "planType": "pro",
                    "primary": {
                        "usedPercent": 62,
                        "windowDurationMins": 10080,
                        "resetsAt": 1_785_611_894_i64
                    }
                },
                "another-model": {
                    "primary": {
                        "usedPercent": 5,
                        "windowDurationMins": 10080
                    }
                }
            }
        }))
        .expect("weekly usage");

        assert_eq!(usage.used_percent, 62);
        assert_eq!(usage.remaining_percent(), 38);
        assert_eq!(usage.resets_at, Some(1_785_611_894));
    }

    #[test]
    fn account_usage_does_not_mislabel_a_nonweekly_bucket() {
        let error = parse_codex_weekly_usage(&serde_json::json!({
            "rateLimits": {
                "primary": {
                    "usedPercent": 62,
                    "windowDurationMins": 300
                }
            }
        }))
        .expect_err("five-hour bucket must not be presented as weekly");

        assert!(error.to_string().contains("seven-day"));
    }

    #[test]
    fn an_interrupt_for_a_turn_that_already_finished_is_idempotent() {
        let race = anyhow::anyhow!(
            "codex app-server error for turn/interrupt: {{\"code\":-32600,\"message\":\"expected active turn id\"}}"
        );
        assert!(expected_inactive_turn_interrupt(&race));

        let unrelated = anyhow::anyhow!(
            "codex app-server error for turn/interrupt: {{\"code\":-32000,\"message\":\"permission denied\"}}"
        );
        assert!(!expected_inactive_turn_interrupt(&unrelated));
    }

    #[test]
    fn turn_start_identity_comes_from_the_response_not_buffered_notifications() {
        let stale_turn_started = JsonRpcMessage {
            id: None,
            method: Some("turn/started".to_string()),
            message: None,
            result: None,
            error: None,
            params: Some(serde_json::json!({
                "threadId": "thread-1",
                "turn": {"id": "turn-old", "status": "inProgress"},
            })),
        };

        // `turn/start` always returns the turn identity. A queued notification
        // is not an acceptable substitute because it may belong to a prior
        // pooled turn.
        assert_eq!(extract_turn_id(&Value::Null), None);
        assert_eq!(
            extract_turn_id(&serde_json::json!({"turn": {"id": "turn-new"}})),
            Some("turn-new".to_string())
        );
        assert_eq!(notification_turn_id(&stale_turn_started), Some("turn-old"));
    }

    fn test_turn_state() -> TurnState {
        TurnState {
            workspace_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            output_text: String::new(),
            reasoning_text: String::new(),
            raw_notifications: Vec::new(),
            turn_token_usage: None,
            total_token_usage: None,
            model_context_window: None,
            agent_message_text: HashMap::new(),
            emitted_agent_message_ids: HashSet::new(),
            emitted_any_agent_message: false,
            output_schema_requested: false,
        }
    }

    #[test]
    fn turn_state_separates_distinct_agent_messages() {
        let mut state = test_turn_state();
        let first = JsonRpcMessage {
            id: None,
            method: Some("item/agentMessage/delta".to_string()),
            message: None,
            result: None,
            error: None,
            params: Some(serde_json::json!({
                "turnId": "turn-1",
                "itemId": "agent-1",
                "delta": "First sentence.",
            })),
        };
        let second = JsonRpcMessage {
            id: None,
            method: Some("item/agentMessage/delta".to_string()),
            message: None,
            result: None,
            error: None,
            params: Some(serde_json::json!({
                "turnId": "turn-1",
                "itemId": "agent-2",
                "delta": "Second sentence.",
            })),
        };

        state.handle_message(first, &mut |_| Ok(())).unwrap();
        state.handle_message(second, &mut |_| Ok(())).unwrap();

        assert_eq!(state.output_text, "First sentence.\n\nSecond sentence.");
    }

    #[test]
    fn stale_pooled_notifications_cannot_complete_or_emit_into_the_next_turn() {
        let mut state = test_turn_state();
        let mut projected = Vec::new();
        let stale_notifications = [
            JsonRpcMessage {
                id: None,
                method: Some("turn/started".to_string()),
                message: None,
                result: None,
                error: None,
                params: Some(serde_json::json!({
                    "threadId": "thread-1",
                    "turn": {"id": "turn-old", "status": "inProgress"},
                })),
            },
            JsonRpcMessage {
                id: None,
                method: Some("item/started".to_string()),
                message: None,
                result: None,
                error: None,
                params: Some(serde_json::json!({
                    "threadId": "thread-1",
                    "turnId": "turn-old",
                    "item": {"id": "tool-old", "type": "commandExecution"},
                })),
            },
            JsonRpcMessage {
                id: None,
                method: Some("item/agentMessage/delta".to_string()),
                message: None,
                result: None,
                error: None,
                params: Some(serde_json::json!({
                    "threadId": "thread-1",
                    "turnId": "turn-old",
                    "itemId": "agent-old",
                    "delta": "stale output",
                })),
            },
            JsonRpcMessage {
                id: None,
                method: Some("item/reasoning/textDelta".to_string()),
                message: None,
                result: None,
                error: None,
                params: Some(serde_json::json!({
                    "threadId": "thread-1",
                    "turnId": "turn-old",
                    "itemId": "reasoning-old",
                    "delta": "stale reasoning",
                })),
            },
            JsonRpcMessage {
                id: None,
                method: Some("item/completed".to_string()),
                message: None,
                result: None,
                error: None,
                params: Some(serde_json::json!({
                    "threadId": "thread-1",
                    "turnId": "turn-old",
                    "item": {
                        "id": "tool-old",
                        "type": "commandExecution",
                        "status": "completed",
                        "aggregatedOutput": "stale result"
                    },
                })),
            },
            JsonRpcMessage {
                id: None,
                method: Some("thread/tokenUsage/updated".to_string()),
                message: None,
                result: None,
                error: None,
                params: Some(serde_json::json!({
                    "threadId": "thread-1",
                    "turnId": "turn-old",
                    "tokenUsage": {"last": {"totalTokens": 999}},
                })),
            },
            JsonRpcMessage {
                id: None,
                method: Some("error".to_string()),
                message: None,
                result: None,
                error: None,
                params: Some(serde_json::json!({
                    "threadId": "thread-1",
                    "turnId": "turn-old",
                    "message": "stale error",
                })),
            },
            JsonRpcMessage {
                id: None,
                method: Some("turn/completed".to_string()),
                message: None,
                result: None,
                error: None,
                params: Some(serde_json::json!({
                    "threadId": "thread-1",
                    "turn": {"id": "turn-old", "status": "completed", "items": []},
                })),
            },
        ];

        for message in stale_notifications {
            assert!(
                state
                    .handle_message(message, &mut |message| {
                        projected.push(message.method.clone());
                        Ok(())
                    })
                    .unwrap()
                    .is_none()
            );
        }
        assert!(projected.is_empty());
        assert!(state.output_text.is_empty());
        assert!(state.reasoning_text.is_empty());
        assert!(state.raw_notifications.is_empty());

        let current_terminal = JsonRpcMessage {
            id: None,
            method: Some("turn/completed".to_string()),
            message: None,
            result: None,
            error: None,
            params: Some(serde_json::json!({
                "threadId": "thread-1",
                "turn": {"id": "turn-1", "status": "completed", "items": []},
            })),
        };
        assert!(
            state
                .handle_message(current_terminal, &mut |_| Ok(()))
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn turn_scoped_notifications_without_identity_are_quarantined() {
        let mut state = test_turn_state();
        let message = JsonRpcMessage {
            id: None,
            method: Some("item/agentMessage/delta".to_string()),
            message: None,
            result: None,
            error: None,
            params: Some(serde_json::json!({
                "itemId": "unidentified",
                "delta": "must not leak into this turn",
            })),
        };

        assert!(
            state
                .handle_message(message, &mut |_| panic!("must not project"))
                .unwrap()
                .is_none()
        );
        assert!(state.output_text.is_empty());
    }

    #[test]
    fn turn_state_keeps_provider_context_window_with_usage() {
        let mut state = test_turn_state();
        let message = JsonRpcMessage {
            id: None,
            method: Some("thread/tokenUsage/updated".to_string()),
            message: None,
            result: None,
            error: None,
            params: Some(serde_json::json!({
                "turnId": "turn-1",
                "tokenUsage": {
                    "last": {
                        "totalTokens": 4000,
                        "cacheWriteInputTokens": 750
                    },
                    "total": { "totalTokens": 64000 },
                    "modelContextWindow": 258400
                }
            })),
        };

        assert!(
            state
                .handle_message(message, &mut |_| Ok(()))
                .unwrap()
                .is_none()
        );
        assert_eq!(state.total_token_usage.unwrap().total_tokens, 64_000);
        assert_eq!(
            state.turn_token_usage.unwrap().cache_write_input_tokens,
            750
        );
        assert_eq!(state.model_context_window, Some(258_400));
    }

    #[test]
    fn turn_input_preserves_provider_native_local_images() {
        let input = turn_user_input(
            "compare these",
            &[
                PathBuf::from("/tmp/first.png"),
                PathBuf::from("/tmp/second.webp"),
            ],
        );

        assert_eq!(
            input,
            vec![
                serde_json::json!({"type": "text", "text": "compare these"}),
                serde_json::json!({"type": "localImage", "path": "/tmp/first.png"}),
                serde_json::json!({"type": "localImage", "path": "/tmp/second.webp"}),
            ]
        );
    }

    #[test]
    fn structured_turn_completion_prefers_final_agent_message() {
        let mut state = test_turn_state();
        state.output_schema_requested = true;
        state
            .handle_message(
                JsonRpcMessage {
                    id: None,
                    method: Some("item/agentMessage/delta".to_string()),
                    message: None,
                    result: None,
                    error: None,
                    params: Some(serde_json::json!({
                        "turnId": "turn-1",
                        "itemId": "agent-1",
                        "delta": "Working through the sources first.",
                    })),
                },
                &mut |_| Ok(()),
            )
            .unwrap();

        let result = state
            .handle_message(
                JsonRpcMessage {
                    id: None,
                    method: Some("turn/completed".to_string()),
                    message: None,
                    result: None,
                    error: None,
                    params: Some(serde_json::json!({
                        "status": "completed",
                        "turn": {
                            "id": "turn-1",
                            "items": [
                                {
                                    "type": "agentMessage",
                                    "id": "agent-1",
                                    "text": "Working through the sources first.",
                                    "phase": null,
                                    "memoryCitation": null
                                },
                                {
                                    "type": "agentMessage",
                                    "id": "agent-2",
                                    "text": "{\"tool_name\":\"read_file\",\"arguments\":{}}",
                                    "phase": null,
                                    "memoryCitation": null
                                }
                            ]
                        }
                    })),
                },
                &mut |_| Ok(()),
            )
            .unwrap()
            .expect("turn result");

        assert_eq!(
            result.output_text,
            "{\"tool_name\":\"read_file\",\"arguments\":{}}"
        );
    }

    #[test]
    fn managed_codex_auth_matches_codex_login_api_key_shape() {
        let dir = tempfile::tempdir().expect("tempdir");

        write_managed_codex_auth(dir.path(), "sk-test-managed").expect("write managed auth");

        let raw = std::fs::read_to_string(dir.path().join("auth.json")).expect("read auth");
        let auth: Value = serde_json::from_str(&raw).expect("parse auth");
        assert_eq!(
            auth.as_object().map(serde_json::Map::len),
            Some(2),
            "{auth}"
        );
        assert_eq!(
            auth.get("auth_mode").and_then(Value::as_str),
            Some("apikey")
        );
        assert_eq!(
            auth.get("OPENAI_API_KEY").and_then(Value::as_str),
            Some("sk-test-managed")
        );
    }

    #[test]
    fn clear_managed_codex_env_removes_token_overrides() {
        let mut command = Command::new("env");
        command
            .env("BORG_OPENAI_AUTH_JSON_B64", "stale")
            .env("BORG_OPENAI_CODEX_ACCESS_TOKEN", "stale")
            .env("OPENAI_CODEX_ACCESS_TOKEN", "stale")
            .env("CODEX_HOME", "/tmp/codex-home");

        clear_managed_codex_env(&mut command);

        let output = command.output().expect("run env");
        let rendered = String::from_utf8(output.stdout).expect("utf8");
        assert!(!rendered.contains("BORG_OPENAI_AUTH_JSON_B64="));
        assert!(!rendered.contains("BORG_OPENAI_CODEX_ACCESS_TOKEN="));
        assert!(!rendered.contains("OPENAI_CODEX_ACCESS_TOKEN="));
        assert!(rendered.contains("CODEX_HOME=/tmp/codex-home"));
    }

    #[test]
    fn inject_mcp_servers_places_servers_in_thread_config() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("mcp.json");
        std::fs::write(
            &path,
            r#"{"mcpServers":{"borg":{"command":"node","args":["server.js"],"env":{"API_BASE_URL":"http://localhost"}}}}"#,
        )
        .expect("write mcp config");
        let mut params = serde_json::json!({
            "config": thread_config(false, None),
        });

        inject_mcp_servers_config(&mut params, Some(path.to_str().unwrap()));

        assert_eq!(
            params
                .pointer("/config/mcp_servers/borg/command")
                .and_then(Value::as_str),
            Some("node")
        );
        assert!(params.get("mcpServers").is_none());
    }

    #[test]
    fn thread_config_uses_codex_v2_reasoning_effort_contract() {
        let config = thread_config(true, Some("low"));

        assert_eq!(
            config.get("model_reasoning_effort").and_then(Value::as_str),
            Some("low")
        );
        assert!(config.get("reasoningEffort").is_none());
        assert_eq!(
            config
                .pointer("/features/multi_agent")
                .and_then(Value::as_bool),
            Some(false)
        );
        assert_eq!(
            config
                .pointer("/features/multi_agent_v2")
                .and_then(Value::as_bool),
            Some(false)
        );
    }

    #[test]
    fn thread_response_rejects_an_ignored_reasoning_effort() {
        let response = serde_json::json!({"reasoningEffort": "xhigh"});

        let error =
            validate_reasoning_effort(&response, Some("low")).expect_err("effort must match");

        assert!(error.to_string().contains("expected `low`"));
    }

    #[test]
    fn turn_state_fails_on_codex_error_notification() {
        let mut state = test_turn_state();
        let error = JsonRpcMessage {
            id: None,
            method: Some("error".to_string()),
            message: None,
            result: None,
            error: None,
            params: Some(serde_json::json!({
                "error": {
                    "message": "You've hit your usage limit.",
                },
            })),
        };

        let err = state.handle_message(error, &mut |_| Ok(())).unwrap_err();

        assert!(
            format!("{err:#}").contains("You've hit your usage limit."),
            "{err:#}"
        );
    }

    #[test]
    fn turn_state_fails_on_top_level_codex_error_message() {
        let mut state = test_turn_state();
        let error = JsonRpcMessage {
            id: None,
            method: Some("error".to_string()),
            message: Some(
                "Reconnecting... 2/5 (Failed to refresh token: 400 Bad Request: Your session has ended. Please log in again.)"
                    .to_string(),
            ),
            result: None,
            error: None,
            params: None,
        };

        let err = state.handle_message(error, &mut |_| Ok(())).unwrap_err();

        assert!(
            format!("{err:#}").contains("Your session has ended. Please log in again."),
            "{err:#}"
        );
    }
}
