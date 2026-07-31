use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader, Write};
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::{Duration, Instant};

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

pub struct CodexAppServerClient {
    child: Child,
    stdin: Option<ChildStdin>,
    reader: BufReader<ChildStdout>,
    next_id: u64,
    workspace_id: Option<String>,
    network_access: bool,
    web_search_allowed: bool,
    deferred_notifications: Vec<JsonRpcMessage>,
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
            reader: BufReader::new(stdout),
            next_id: 1,
            workspace_id: None,
            network_access,
            web_search_allowed,
            deferred_notifications: Vec::new(),
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

        let response = self.send_request("thread/resume", Some(params))?;
        validate_reasoning_effort(&response, reasoning_effort)?;
        let workspace_id = response
            .get("threadId")
            .and_then(Value::as_str)
            .or_else(|| response.pointer("/thread/id").and_then(Value::as_str))
            .unwrap_or(thread_id)
            .to_string();
        self.workspace_id = Some(workspace_id.clone());
        Ok(workspace_id)
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
        let turn_id = extract_turn_id(&turn_start_result, &early_notifications)
            .context("turn/start response did not include turn id")?;

        let mut state = TurnState {
            workspace_id: workspace_id.clone(),
            output_text: String::new(),
            reasoning_text: String::new(),
            raw_notifications: Vec::new(),
            turn_token_usage: None,
            total_token_usage: None,
            model_context_window: None,
            agent_message_text: HashMap::new(),
            emitted_agent_message_ids: HashSet::new(),
            emitted_any_agent_message: false,
            output_schema_requested: output_schema.is_some(),
        };

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
        let status = self
            .child
            .wait()
            .context("failed waiting for codex app-server process to exit")?;
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
        let id = self.next_id;
        self.next_id += 1;
        let mut notifications = Vec::new();

        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id,
            method: method.to_string(),
            params,
        };

        let line = serde_json::to_string(&request)?;
        let stdin = self
            .stdin
            .as_mut()
            .context("codex app-server stdin closed")?;
        writeln!(stdin, "{line}")?;
        stdin.flush()?;

        loop {
            let msg = self.read_message()?;
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
                if collect_notifications {
                    notifications.push(msg);
                }
                continue;
            }
            if collect_notifications {
                notifications.push(msg);
            }
        }
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

    fn read_message(&mut self) -> Result<JsonRpcMessage> {
        let mut line = String::new();
        loop {
            line.clear();
            let bytes = self
                .reader
                .read_line(&mut line)
                .context("failed to read from codex app-server")?;
            if bytes == 0 {
                bail!("codex app-server stdout closed unexpectedly");
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let msg: JsonRpcMessage = serde_json::from_str(trimmed)
                .with_context(|| format!("failed to parse app-server message: {trimmed}"))?;
            return Ok(msg);
        }
    }

    fn read_message_timeout(&mut self, timeout: Duration) -> Result<Option<JsonRpcMessage>> {
        if self.reader.buffer().is_empty() && !child_stdout_ready(self.reader.get_ref(), timeout)? {
            return Ok(None);
        }
        self.read_message().map(Some)
    }
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

#[cfg(unix)]
fn child_stdout_ready(stdout: &ChildStdout, timeout: Duration) -> Result<bool> {
    let fd = stdout.as_raw_fd();
    let mut pollfd = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    let timeout_ms = timeout.as_millis().min(i32::MAX as u128) as i32;
    loop {
        let ready = unsafe { libc::poll(&mut pollfd, 1, timeout_ms) };
        if ready > 0 {
            return Ok(true);
        }
        if ready == 0 {
            return Ok(false);
        }
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::EINTR) {
            return Err(error).context("failed to poll codex app-server stdout");
        }
    }
}

#[cfg(windows)]
fn child_stdout_ready(stdout: &ChildStdout, timeout: Duration) -> Result<bool> {
    use std::os::windows::io::AsRawHandle;
    use std::time::Instant;
    use windows_sys::Win32::System::Pipes::PeekNamedPipe;

    let deadline = Instant::now() + timeout;
    loop {
        let mut available = 0_u32;
        let ready = unsafe {
            PeekNamedPipe(
                stdout.as_raw_handle(),
                std::ptr::null_mut(),
                0,
                std::ptr::null_mut(),
                &mut available,
                std::ptr::null_mut(),
            )
        };
        if ready == 0 {
            return Err(std::io::Error::last_os_error())
                .context("failed to inspect codex app-server stdout");
        }
        if available > 0 {
            return Ok(true);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(false);
        }
        std::thread::sleep(remaining.min(Duration::from_millis(5)));
    }
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

fn extract_turn_id(result: &Value, notifications: &[JsonRpcMessage]) -> Option<String> {
    result
        .pointer("/turn/id")
        .and_then(Value::as_str)
        .or_else(|| result.get("turnId").and_then(Value::as_str))
        .map(str::to_string)
        .or_else(|| {
            notifications.iter().find_map(|message| {
                if message.method.as_deref() != Some("turn/started") {
                    return None;
                }
                message
                    .params
                    .as_ref()
                    .and_then(|params| params.pointer("/turn/id").and_then(Value::as_str))
                    .or_else(|| {
                        message
                            .params
                            .as_ref()
                            .and_then(|params| params.get("turnId").and_then(Value::as_str))
                    })
                    .map(str::to_string)
            })
        })
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
    fn handle_message<F>(
        &mut self,
        msg: JsonRpcMessage,
        on_notification: &mut F,
    ) -> Result<Option<TurnResult>>
    where
        F: FnMut(&JsonRpcMessage) -> Result<()>,
    {
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
        let _ = self.child.kill();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn test_turn_state() -> TurnState {
        TurnState {
            workspace_id: "thread-1".to_string(),
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
                "itemId": "agent-2",
                "delta": "Second sentence.",
            })),
        };

        state.handle_message(first, &mut |_| Ok(())).unwrap();
        state.handle_message(second, &mut |_| Ok(())).unwrap();

        assert_eq!(state.output_text, "First sentence.\n\nSecond sentence.");
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
