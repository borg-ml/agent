#![cfg(all(unix, feature = "codex"))]

use std::os::unix::fs::PermissionsExt;
use std::time::Duration;

use borg_provider::ProviderChannel;
use borg_provider::provider::{
    ChatStreamRequest, CodexSubscriptionPool, LocalAgentPermission, run_codex_local_chat_stream,
    run_codex_local_chat_stream_pooled,
};

// This test binary has one single-threaded test: the runtime override never
// races another test or invokes an authenticated provider.
#[tokio::test(flavor = "current_thread")]
async fn cancellation_reaps_subscription_processes_and_releases_the_pool() {
    let root = tempfile::tempdir().unwrap();
    let executable = root.path().join("codex");
    std::fs::write(
        &executable,
        r#"#!/bin/sh
if [ "$1" = "--version" ]; then echo "codex 1.0.0"; exit; fi
stage=$(cat stage)
while IFS= read -r line; do
    case "$line" in
        *initialize*) method=initialize; id=1; result="{}" ;;
        *thread/start*) method=thread/start; id=2; result='{"thread":{"id":"test-thread"}}' ;;
        *turn/start*) method=turn/start; id=3; result='{"turn":{"id":"test-turn"}}' ;;
        *) continue ;;
    esac
    if [ "$method" = "$stage" ]; then
        echo $$ > ready
        while IFS= read -r ignored; do :; done
        exit
    fi
    printf '{"id":%s,"result":%s}
' "$id" "$result"
    if [ "$method" = "turn/start" ]; then
        echo $$ > ready
        while IFS= read -r ignored; do :; done
        exit
    fi
done
"#,
    )
    .unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
    unsafe {
        std::env::set_var("BORG_CODEX_BIN", &executable);
    }
    let pool = CodexSubscriptionPool::default();
    for pooled in [false, true] {
        for stage in ["initialize", "thread/start", "turn/start", "stream"] {
            std::fs::write(root.path().join("stage"), stage).unwrap();
            let request = request(root.path());
            let stream = if pooled {
                run_codex_local_chat_stream_pooled(
                    request,
                    None,
                    LocalAgentPermission::FullAccess,
                    pool.clone(),
                )
            } else {
                run_codex_local_chat_stream(request, None, LocalAgentPermission::FullAccess)
            };
            let ready = root.path().join("ready");
            tokio::time::timeout(Duration::from_secs(5), async {
                while !ready.exists() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("fake provider never reached {stage}, pooled={pooled}"));
            let pid: i32 = std::fs::read_to_string(&ready)
                .unwrap()
                .trim()
                .parse()
                .unwrap();
            drop(stream);
            tokio::time::timeout(Duration::from_secs(3), async {
                pool.shutdown().await;
                while unsafe { libc::kill(pid, 0) } == 0 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("cancellation wedged at {stage}, pooled={pooled}"));
            std::fs::remove_file(ready).unwrap();
        }
    }
    #[cfg(feature = "claude")]
    {
        use borg_provider::provider::{
            ClaudeSubscriptionPool, run_claude_local_chat_stream,
            run_claude_local_chat_stream_pooled,
        };
        std::fs::write(
            &executable,
            r#"#!/bin/sh
if [ "$1" = "--version" ]; then echo "claude 1.0.0"; exit; fi
IFS= read -r line
echo $$ > ready
while IFS= read -r ignored; do :; done
"#,
        )
        .unwrap();
        unsafe {
            std::env::set_var("BORG_CLAUDE_BIN", &executable);
        }
        let pool = ClaudeSubscriptionPool::default();
        for pooled in [false, true, true] {
            let stream = if pooled {
                run_claude_local_chat_stream_pooled(
                    request(root.path()),
                    None,
                    LocalAgentPermission::FullAccess,
                    pool.clone(),
                )
            } else {
                run_claude_local_chat_stream(
                    request(root.path()),
                    None,
                    LocalAgentPermission::FullAccess,
                )
            };
            let ready = root.path().join("ready");
            tokio::time::timeout(Duration::from_secs(5), async {
                while !ready.exists() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("Claude runtime did not start");
            let pid: i32 = std::fs::read_to_string(&ready)
                .unwrap()
                .trim()
                .parse()
                .unwrap();
            drop(stream);
            tokio::time::timeout(Duration::from_secs(3), async {
                while unsafe { libc::kill(pid, 0) } == 0 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("Claude runtime detached on cancellation");
            std::fs::remove_file(ready).unwrap();
        }
    }
}

fn request(root: &std::path::Path) -> ChatStreamRequest {
    ChatStreamRequest {
        prompt: "hello".to_string(),
        lifecycle_key: None,
        owner_session_id: None,
        client_user_message_id: None,
        attachments: Vec::new(),
        model: None,
        effort: None,
        fast: false,
        system_prompt: "system".to_string(),
        output_schema: None,
        mcp_owner_id: None,
        mcp_allowed_scopes: Vec::new(),
        mcp_user_id: None,
        mcp_external_servers: Vec::new(),
        mcp_api_token: None,
        provider_auth: None,
        git_credentials: Vec::new(),
        working_directory: Some(root.to_path_buf()),
        session_id: None,
        fork_turn_id: None,
        provider_channel: ProviderChannel::Direct,
        persist_session: Some(false),
        web_search_allowed: false,
        resume_unavailable_prompt: None,
    }
}
