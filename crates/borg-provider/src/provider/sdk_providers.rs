//! Provider impls that drive every Claude / Codex run through the
//! Claude Agent SDK and the Codex app-server. The CLI binaries (`claude
//! exec`, `codex exec`) are no longer used — these are the only paths.
//!
//! Both providers rely on server-side structured-output enforcement:
//! - Agent SDK: `outputFormat: { type: "json_schema", schema }` passed through
//!   `packages/borg-claude-sdk/src/provider.ts`.
//! - Codex app-server: `outputSchema` param on the `turn/start` JSON-RPC call.
//!
//! The SDK / app-server layer retries internally on schema-validation failure,
//! so callers see a parsed `ProviderCallResult.value` matching the schema or
//! a `ProviderCallError` — never an out-of-shape payload.

use std::path::PathBuf;

use crate::{ProviderAuthBundle, ProviderAuthProvider, ProviderChannel};
use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::mpsc::UnboundedSender;

use super::chat_stream::{
    ChatStreamRequest, run_claude_chat_stream, run_codex_chat_stream,
    run_codex_freeform_chat_stream,
};
use super::{
    Provider, ProviderCallError, ProviderCallResult, ProviderProgress, StructuredOutputDialect,
    default_effort_for_backend, default_model_for_backend,
};
use crate::mcp::BorgAgentMcpContext;

mod codex_exec;
mod request;
mod stream_result;

use codex_exec::{codex_exec_freeform_call, codex_exec_structured_call};
use request::{ChatStreamRequestOptions, build_chat_stream_request};
pub use stream_result::{await_freeform_result, await_structured_result};

#[derive(Debug, Clone)]
pub struct ClaudeSdkProvider {
    pub model: Option<String>,
    pub effort: Option<String>,
    pub fast: bool,
    pub system_prompt: &'static str,
    pub auth_bundle: Option<ProviderAuthBundle>,
    /// Network route: `Direct` (Anthropic API), `Vertex` (Google Cloud), or
    /// `Bedrock` (AWS). Activates the corresponding `CLAUDE_CODE_USE_*` env
    /// var on the bundled Claude Agent SDK binary.
    pub channel: ProviderChannel,
    /// When `Some(false)`, the bundled Agent SDK skips writing session
    /// history to `~/.claude/projects/`. `None` keeps the SDK default
    /// (persist), which lets developers and support replay sessions
    /// from the `claude` CLI for debugging. Enterprise tier sets this
    /// to `Some(false)` for the no-disk-persistence privacy guarantee
    /// (see `runner::persist_session_for_tier`).
    pub persist_session: Option<bool>,
    pub mcp: BorgAgentMcpContext,
}

#[derive(Debug, Clone)]
pub struct CodexAppServerProvider {
    pub model: Option<String>,
    pub effort: Option<String>,
    pub fast: bool,
    pub system_prompt: &'static str,
    pub auth_bundle: Option<ProviderAuthBundle>,
    pub auth_codex_home: Option<PathBuf>,
    /// Reserved for future Azure OpenAI routing. Ignored today (Direct only).
    pub channel: ProviderChannel,
    pub persist_session: Option<bool>,
    pub web_search_allowed: bool,
    pub mcp: BorgAgentMcpContext,
}

impl ClaudeSdkProvider {
    fn chat_stream_request(
        &self,
        prompt: &str,
        schema: Option<&Value>,
        session_id: Option<&str>,
    ) -> ChatStreamRequest {
        build_chat_stream_request(ChatStreamRequestOptions {
            prompt,
            schema,
            model: self.model.clone(),
            effort: self.effort.clone(),
            fast: self.fast,
            system_prompt: self.system_prompt,
            auth_bundle: self.auth_bundle.as_ref(),
            auth_codex_home: None,
            auth_provider: ProviderAuthProvider::Claude,
            provider_channel: self.channel,
            session_id: session_id.filter(|s| !s.is_empty()).map(str::to_string),
            persist_session: self.persist_session,
            web_search_allowed: false,
            resume_unavailable_prompt: None,
            mcp: self.mcp.clone(),
        })
    }
}

impl CodexAppServerProvider {
    fn product_model(&self) -> Option<String> {
        default_model_for_backend("codex")
    }

    fn product_effort(&self) -> Option<String> {
        match self.effort.as_deref().map(str::trim) {
            Some(value) if crate::codex_effort_supported(value) => Some(value.to_string()),
            _ => default_effort_for_backend("codex"),
        }
    }

    fn chat_stream_request(
        &self,
        prompt: &str,
        schema: Option<&Value>,
        session_id: Option<&str>,
    ) -> ChatStreamRequest {
        build_chat_stream_request(ChatStreamRequestOptions {
            prompt,
            schema,
            model: self.product_model(),
            effort: self.product_effort(),
            fast: self.fast,
            system_prompt: self.system_prompt,
            auth_bundle: self.auth_bundle.as_ref(),
            auth_codex_home: self.auth_codex_home.clone(),
            auth_provider: ProviderAuthProvider::Openai,
            provider_channel: self.channel,
            session_id: session_id.filter(|s| !s.is_empty()).map(str::to_string),
            persist_session: self.persist_session,
            web_search_allowed: self.web_search_allowed,
            resume_unavailable_prompt: None,
            mcp: self.mcp.clone(),
        })
    }
}

#[async_trait]
impl Provider for ClaudeSdkProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn structured_call_with_progress(
        &self,
        prompt: &str,
        schema: &Value,
        session_id: Option<&str>,
        progress: Option<UnboundedSender<ProviderProgress>>,
    ) -> std::result::Result<ProviderCallResult, ProviderCallError> {
        let request = self.chat_stream_request(prompt, Some(schema), session_id);
        let rx = run_claude_chat_stream(request);
        await_structured_result(
            rx,
            progress,
            self.label(),
            "claude-agent-sdk",
            self.model.clone(),
            self.effort.clone(),
        )
        .await
    }

    async fn freeform_call_with_progress(
        &self,
        prompt: &str,
        session_id: Option<&str>,
        progress: Option<UnboundedSender<ProviderProgress>>,
    ) -> std::result::Result<ProviderCallResult, ProviderCallError> {
        let request = self.chat_stream_request(prompt, None, session_id);
        let rx = run_claude_chat_stream(request);
        await_freeform_result(
            rx,
            progress,
            self.label(),
            "claude-agent-sdk",
            self.model.clone(),
            self.effort.clone(),
        )
        .await
    }

    fn label(&self) -> &'static str {
        "claude-sdk"
    }

    fn structured_output_dialect(&self) -> StructuredOutputDialect {
        StructuredOutputDialect::StrictObjects
    }
}

#[async_trait]
impl Provider for CodexAppServerProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn structured_call_with_progress(
        &self,
        prompt: &str,
        schema: &Value,
        session_id: Option<&str>,
        progress: Option<UnboundedSender<ProviderProgress>>,
    ) -> std::result::Result<ProviderCallResult, ProviderCallError> {
        if std::env::var("BORG_CODEX_USE_EXEC").is_ok_and(|value| value == "1") {
            return codex_exec_structured_call(
                prompt,
                schema,
                self.product_model(),
                self.product_effort(),
                self.system_prompt,
                progress,
            )
            .await;
        }
        let request = self.chat_stream_request(prompt, Some(schema), session_id);
        let rx = run_codex_chat_stream(request);
        await_structured_result(
            rx,
            progress,
            self.label(),
            "codex-app-server",
            self.product_model(),
            self.product_effort(),
        )
        .await
    }

    async fn freeform_call_with_progress(
        &self,
        prompt: &str,
        session_id: Option<&str>,
        progress: Option<UnboundedSender<ProviderProgress>>,
    ) -> std::result::Result<ProviderCallResult, ProviderCallError> {
        if std::env::var("BORG_CODEX_USE_EXEC").is_ok_and(|value| value == "1") {
            return codex_exec_freeform_call(
                prompt,
                self.product_model(),
                self.product_effort(),
                self.system_prompt,
                progress,
            )
            .await;
        }
        let request = self.chat_stream_request(prompt, None, session_id);
        let rx = run_codex_freeform_chat_stream(request);
        await_freeform_result(
            rx,
            progress,
            self.label(),
            "codex-app-server",
            self.product_model(),
            self.product_effort(),
        )
        .await
    }

    fn label(&self) -> &'static str {
        "codex-app-server"
    }

    fn structured_output_dialect(&self) -> StructuredOutputDialect {
        StructuredOutputDialect::StrictObjects
    }
}
