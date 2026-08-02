use std::path::PathBuf;

use crate::{ProviderAuthBundle, ProviderAuthProvider, ProviderChannel};
use serde_json::Value;

use crate::mcp::BorgAgentMcpContext;
use crate::provider::chat_stream::{ChatProviderAuth, ChatStreamRequest};

pub(super) struct ChatStreamRequestOptions<'a> {
    pub prompt: &'a str,
    pub schema: Option<&'a Value>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub fast: bool,
    pub system_prompt: &'a str,
    pub auth_bundle: Option<&'a ProviderAuthBundle>,
    pub auth_codex_home: Option<PathBuf>,
    pub auth_provider: ProviderAuthProvider,
    pub provider_channel: ProviderChannel,
    pub session_id: Option<String>,
    pub persist_session: Option<bool>,
    pub web_search_allowed: bool,
    pub resume_unavailable_prompt: Option<String>,
    pub mcp: BorgAgentMcpContext,
}

pub(super) fn build_chat_stream_request(
    options: ChatStreamRequestOptions<'_>,
) -> ChatStreamRequest {
    ChatStreamRequest {
        prompt: options.prompt.to_string(),
        owner_session_id: None,
        client_user_message_id: None,
        attachments: Vec::new(),
        model: options.model,
        effort: options.effort,
        fast: options.fast,
        system_prompt: options.system_prompt.to_string(),
        output_schema: options.schema.cloned(),
        mcp_owner_id: options.mcp.owner_id,
        mcp_allowed_scopes: options.mcp.allowed_scopes,
        mcp_user_id: options.mcp.user_id,
        mcp_external_servers: options.mcp.external_servers,
        mcp_api_token: options.mcp.api_token,
        provider_auth: options.auth_bundle.map(|bundle| ChatProviderAuth {
            provider: options.auth_provider,
            bundle: bundle.clone(),
            codex_home: options.auth_codex_home,
        }),
        git_credentials: Vec::new(),
        working_directory: None,
        session_id: options.session_id,
        provider_channel: options.provider_channel,
        persist_session: options.persist_session,
        web_search_allowed: options.web_search_allowed,
        resume_unavailable_prompt: options.resume_unavailable_prompt,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::{ProviderAuthProvider, ProviderChannel};

    use super::{ChatStreamRequestOptions, build_chat_stream_request};
    use crate::mcp::{BorgAgentMcpContext, ExternalMcpServer};

    #[test]
    fn chat_stream_request_preserves_borg_agent_mcp_context() {
        let mcp = BorgAgentMcpContext {
            owner_id: Some("thread:workspace-1".to_string()),
            allowed_scopes: vec![
                "thread:workspace-1".to_string(),
                "library:lib-1".to_string(),
            ],
            user_id: Some("user-1".to_string()),
            external_servers: vec![ExternalMcpServer {
                name: "example".to_string(),
                command: "example-mcp".to_string(),
                args: vec!["--stdio".to_string()],
                env: BTreeMap::new(),
                allowed_tools: vec!["mcp__example__search".to_string()],
            }],
            api_token: Some("token".to_string()),
        };

        let request = build_chat_stream_request(ChatStreamRequestOptions {
            prompt: "prompt",
            schema: None,
            model: Some("model".to_string()),
            effort: None,
            fast: false,
            system_prompt: "system",
            auth_bundle: None,
            auth_codex_home: None,
            auth_provider: ProviderAuthProvider::Openai,
            provider_channel: ProviderChannel::Direct,
            session_id: None,
            persist_session: None,
            web_search_allowed: true,
            resume_unavailable_prompt: None,
            mcp: mcp.clone(),
        });

        assert_eq!(request.mcp_owner_id, mcp.owner_id);
        assert_eq!(request.mcp_allowed_scopes, mcp.allowed_scopes);
        assert_eq!(request.mcp_user_id, mcp.user_id);
        assert_eq!(request.mcp_external_servers.len(), 1);
        assert_eq!(request.mcp_api_token, mcp.api_token);
    }
}
