use borg_provider::ProviderChannel;
use borg_provider::provider::{
    ChatStreamEvent, ChatStreamRequest, LocalAgentPermission, run_opencode_local_chat_stream,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
#[ignore = "requires an installed OpenCode runtime; uses only a local fake model"]
async fn opencode_generates_before_complete_arguments_and_finishes() -> anyhow::Result<()> {
    for permission in [
        LocalAgentPermission::Manual,
        LocalAgentPermission::Auto,
        LocalAgentPermission::FullAccess,
    ] {
        let root = tempfile::tempdir()?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        std::fs::write(
            root.path().join("opencode.json"),
            serde_json::to_vec(&serde_json::json!({
                "enabled_providers": ["borg-smoke"],
                "model": "borg-smoke/test",
                "small_model": "borg-smoke/test",
                "autoupdate": false,
                "permission": "ask",
                "provider": {"borg-smoke": {
                    "npm": "@ai-sdk/openai-compatible",
                    "name": "Borg smoke",
                    "options": {"baseURL": format!("http://{address}/v1"), "apiKey": "test"},
                    "models": {"test": {"name": "test", "limit": {"context": 32768, "output": 4096}}}
                }}
            }))?,
        )?;
        let (release_tx, release_rx) = tokio::sync::watch::channel(false);
        let mock = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut release_rx = release_rx.clone();
                tokio::spawn(async move {
                    let mut request = Vec::new();
                    let end = loop {
                        let mut chunk = [0u8; 4096];
                        let n = socket.read(&mut chunk).await.unwrap();
                        if n == 0 {
                            return;
                        }
                        request.extend_from_slice(&chunk[..n]);
                        if let Some(end) = request.windows(4).position(|x| x == b"\r\n\r\n") {
                            break end + 4;
                        }
                    };
                    let headers = String::from_utf8_lossy(&request[..end]);
                    let length: usize = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse().unwrap())
                        })
                        .unwrap_or(0);
                    while request.len() < end + length {
                        let mut chunk = [0u8; 4096];
                        let n = socket.read(&mut chunk).await.unwrap();
                        if n == 0 {
                            return;
                        }
                        request.extend_from_slice(&chunk[..n]);
                    }
                    let body: serde_json::Value = serde_json::from_slice(&request[end..]).unwrap();
                    if body["stream"] == true
                        && body["tools"]
                            .as_array()
                            .is_some_and(|tools| !tools.is_empty())
                        && !body["messages"].as_array().is_some_and(|messages| {
                            messages.iter().any(|message| message["role"] == "tool")
                        })
                    {
                        let fragment = serde_json::json!({"id":"smoke","object":"chat.completion.chunk","created":0,"model":"test","choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"smoke-tool","type":"function","function":{"name":"bash","arguments":"{"}}]},"finish_reason":null}]});
                        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\ndata: {fragment}\n\n").as_bytes()).await.unwrap();
                        release_rx.wait_for(|released| *released).await.unwrap();
                        let remainder = serde_json::json!({"id":"smoke","object":"chat.completion.chunk","created":0,"model":"test","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"command\":\"pwd\",\"description\":\"print cwd\"}"}}]},"finish_reason":"tool_calls"}]});
                        socket
                            .write_all(format!("data: {remainder}\n\ndata: [DONE]\n\n").as_bytes())
                            .await
                            .unwrap();
                    } else if body["stream"] == true {
                        let chunk = serde_json::json!({"id":"smoke","object":"chat.completion.chunk","created":0,"model":"test","choices":[{"index":0,"delta":{"role":"assistant","content":"Done."},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}});
                        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\ndata: {chunk}\n\ndata: [DONE]\n\n").as_bytes()).await.unwrap();
                    } else {
                        let body = r#"{"id":"title","object":"chat.completion","created":0,"model":"test","choices":[{"index":0,"message":{"role":"assistant","content":"Smoke"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#;
                        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
                    }
                });
            }
        });
        let request = ChatStreamRequest {
            prompt: "Run pwd.".into(),
            lifecycle_key: None,
            owner_session_id: None,
            client_user_message_id: None,
            attachments: vec![],
            model: Some("borg-smoke/test".into()),
            effort: None,
            fast: false,
            system_prompt: "".into(),
            output_schema: None,
            mcp_owner_id: None,
            mcp_allowed_scopes: vec![],
            mcp_user_id: None,
            mcp_external_servers: vec![],
            mcp_api_token: None,
            provider_auth: None,
            git_credentials: vec![],
            working_directory: Some(root.path().into()),
            session_id: None,
            fork_turn_id: None,
            provider_channel: ProviderChannel::Direct,
            persist_session: Some(false),
            web_search_allowed: false,
            resume_unavailable_prompt: None,
        };
        let mut events = run_opencode_local_chat_stream(request, permission);
        let mut generations = 0;
        let mut results = 0;
        let result = tokio::time::timeout(std::time::Duration::from_secs(45), async {
            while let Some(event) = events.recv().await {
                match event {
                    ChatStreamEvent::ToolCallGenerating { id } => {
                        assert!(id.is_some());
                        generations += 1;
                        assert_eq!(generations, 1);
                        release_tx.send(true)?;
                    }
                    ChatStreamEvent::ToolResult {
                        is_error, output, ..
                    } => {
                        assert_eq!(
                            is_error,
                            permission == LocalAgentPermission::Manual,
                            "{permission:?}: {output}"
                        );
                        results += 1;
                    }
                    ChatStreamEvent::Failed { error } => anyhow::bail!("{error}"),
                    ChatStreamEvent::Done { final_text, .. } => {
                        assert_eq!(generations, 1);
                        assert_eq!(results, 1);
                        if permission != LocalAgentPermission::Manual {
                            assert!(final_text.contains("Done."));
                        }
                        return Ok(());
                    }
                    _ => {}
                }
            }
            anyhow::bail!("event stream closed")
        })
        .await;
        drop(events);
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        mock.abort();
        result??;
    }
    Ok(())
}
