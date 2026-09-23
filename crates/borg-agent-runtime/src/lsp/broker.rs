//! A per-user LSP broker. Unix sockets are private to the OS user; the first
//! Borg process to request LSP owns the server pool, and later processes use
//! that same pool. The pool still keys clients by canonical workspace *and*
//! access policy: a language server's opened documents are not safe to share
//! across different scopes or worktrees.
use std::collections::HashMap;
use std::fs::{DirBuilder, File, OpenOptions};
use std::io::ErrorKind;
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, PermissionsExt};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant, timeout};
use uuid::Uuid;

use super::{LspPathPolicy, LspService};

const MAX_WIRE_BYTES: u64 = 64 * 1024 * 1024;
// A workspace pass can initialize a server for 120 s and then spend up to
// 90 s collecting diagnostics. Keep the IPC budget above both, but bounded.
const BROKER_TIMEOUT: Duration = Duration::from_secs(4 * 60);
const SESSION_IDLE: Duration = Duration::from_secs(30 * 60);

#[derive(Serialize, Deserialize)]
pub(super) struct Request {
    pub id: Uuid,
    pub root: PathBuf,
    pub policy: LspPathPolicy,
    pub operation: Operation,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case")]
pub(super) enum Operation {
    Status,
    Diagnostics {
        path: PathBuf,
    },
    Hover {
        path: PathBuf,
        line: u32,
        character: u32,
    },
    Definition {
        path: PathBuf,
        line: u32,
        character: u32,
    },
    References {
        path: PathBuf,
        line: u32,
        character: u32,
    },
    DocumentSymbols {
        path: PathBuf,
    },
    WorkspaceSymbols {
        query: String,
    },
    WorkspaceDiagnostics {
        path: Option<PathBuf>,
    },
}

#[derive(Serialize, Deserialize)]
struct Reply {
    result: Option<Value>,
    error: Option<String>,
}

struct Session {
    root: PathBuf,
    policy: LspPathPolicy,
    service: LspService,
    last_used: Instant,
}

type Sessions = Arc<Mutex<HashMap<Uuid, Session>>>;

pub(super) async fn request(request: Request) -> Result<Value> {
    request_on(request, socket_paths()?).await
}

async fn request_on(request: Request, (socket, lock_path): (PathBuf, PathBuf)) -> Result<Value> {
    timeout(BROKER_TIMEOUT, async {
        let mut stream = match UnixStream::connect(&socket).await {
            Ok(stream) => stream,
            Err(error) if stale_socket(&error) => {
                // Lock only the election, not the analysis. The listener is
                // bound before releasing it, so a second process will connect
                // to the leader even while the first server initializes.
                let lock = tokio::task::spawn_blocking(move || -> Result<File> {
                    let lock = OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(false)
                        .open(lock_path)?;
                    lock.lock()?;
                    Ok(lock)
                })
                .await??;
                let stream = match UnixStream::connect(&socket).await {
                    Ok(stream) => stream,
                    Err(error) if stale_socket(&error) => {
                        if let Ok(metadata) = std::fs::symlink_metadata(&socket) {
                            ensure!(
                                metadata.file_type().is_socket(),
                                "LSP broker path is not a socket"
                            );
                            std::fs::remove_file(&socket)?;
                        }
                        let listener = UnixListener::bind(&socket)
                            .with_context(|| format!("bind LSP broker at {}", socket.display()))?;
                        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
                        tokio::spawn(serve(listener));
                        UnixStream::connect(&socket).await?
                    }
                    Err(error) => {
                        return Err(error).context("LSP broker is not accepting connections");
                    }
                };
                drop(lock);
                stream
            }
            Err(error) => return Err(error).context("connect to LSP broker"),
        };
        let bytes = serde_json::to_vec(&request)?;
        ensure!(
            bytes.len() as u64 <= MAX_WIRE_BYTES,
            "LSP request exceeds broker limit"
        );
        stream.write_all(&bytes).await?;
        stream.shutdown().await?;
        let mut response = Vec::new();
        stream
            .take(MAX_WIRE_BYTES + 1)
            .read_to_end(&mut response)
            .await?;
        ensure!(
            response.len() as u64 <= MAX_WIRE_BYTES,
            "LSP broker reply exceeds limit"
        );
        let reply: Reply = serde_json::from_slice(&response).context("invalid LSP broker reply")?;
        match (reply.result, reply.error) {
            (Some(value), None) => Ok(value),
            (_, Some(error)) => bail!("{error}"),
            _ => bail!("LSP broker omitted its result"),
        }
    })
    .await
    .context("LSP broker did not answer within four minutes")?
}

fn stale_socket(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        ErrorKind::NotFound | ErrorKind::ConnectionRefused
    )
}

fn socket_paths() -> Result<(PathBuf, PathBuf)> {
    // XDG_RUNTIME_DIR and TMPDIR can differ between a login shell, a remote
    // agent and a desktop app launched by the same user. /tmp is a common
    // local rendezvous point; the child directory is mode 0700 and UID-checked.
    socket_paths_in(PathBuf::from("/tmp"))
}

fn socket_paths_in(base: PathBuf) -> Result<(PathBuf, PathBuf)> {
    // SAFETY: geteuid reads the effective UID and has no input or side effects.
    let uid = unsafe { libc::geteuid() };
    let dir = base.join(format!("borg-lsp-{uid}"));
    match DirBuilder::new().mode(0o700).create(&dir) {
        Ok(()) => (),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => (),
        Err(error) => return Err(error).with_context(|| format!("create {}", dir.display())),
    }
    let meta = std::fs::symlink_metadata(&dir)?;
    ensure!(
        meta.file_type().is_dir() && meta.uid() == uid && meta.permissions().mode() & 0o077 == 0,
        "LSP broker directory is not private to this OS user"
    );
    let socket = dir.join("pool.sock");
    ensure!(
        socket.as_os_str().len() < 104,
        "LSP broker socket path is too long"
    );
    Ok((socket, dir.join("election.lock")))
}

async fn serve(listener: UnixListener) {
    let sessions: Sessions = Arc::new(Mutex::new(HashMap::new()));
    let reaper = Arc::clone(&sessions);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(SESSION_IDLE);
        loop {
            tick.tick().await;
            reaper
                .lock()
                .await
                .retain(|_, session| session.last_used.elapsed() < SESSION_IDLE);
        }
    });
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            break;
        };
        let sessions = Arc::clone(&sessions);
        tokio::spawn(async move {
            if let Err(error) = serve_request(stream, sessions).await {
                tracing::debug!(%error, "LSP broker request failed");
            }
        });
    }
}

async fn serve_request(mut stream: UnixStream, sessions: Sessions) -> Result<()> {
    let mut bytes = Vec::new();
    (&mut stream)
        .take(MAX_WIRE_BYTES + 1)
        .read_to_end(&mut bytes)
        .await?;
    ensure!(
        bytes.len() as u64 <= MAX_WIRE_BYTES,
        "LSP request exceeds broker limit"
    );
    let request: Request = serde_json::from_slice(&bytes)?;
    let result = dispatch(request, sessions).await;
    let reply = match result {
        Ok(value) => Reply {
            result: Some(value),
            error: None,
        },
        Err(error) => Reply {
            result: None,
            error: Some(format!("{error:#}")),
        },
    };
    stream.write_all(&serde_json::to_vec(&reply)?).await?;
    stream.shutdown().await?;
    Ok(())
}

async fn dispatch(request: Request, sessions: Sessions) -> Result<Value> {
    let service = {
        let mut sessions = sessions.lock().await;
        let session = sessions.entry(request.id).or_insert_with(|| {
            let mut service = LspService::with_path_policy(&request.root, request.policy.clone());
            service.broker_local = true;
            Session {
                root: request.root.clone(),
                policy: request.policy.clone(),
                service,
                last_used: Instant::now(),
            }
        });
        // UUID identity alone is not authority to borrow an existing scope.
        ensure!(
            session.root == request.root && session.policy == request.policy,
            "LSP broker client identity changed its workspace or access policy"
        );
        session.last_used = Instant::now();
        session.service.clone()
    };
    match request.operation {
        Operation::Status => {
            let mut status = service.status_local().await;
            status["broker_pid"] = Value::from(std::process::id());
            Ok(status)
        }
        Operation::Diagnostics { path } => service.diagnostics_local(&path).await,
        Operation::Hover {
            path,
            line,
            character,
        } => {
            service
                .position_request(&path, "textDocument/hover", line, character, json!({}))
                .await
        }
        Operation::Definition {
            path,
            line,
            character,
        } => {
            service
                .position_request(&path, "textDocument/definition", line, character, json!({}))
                .await
        }
        Operation::References {
            path,
            line,
            character,
        } => {
            service
                .position_request(
                    &path,
                    "textDocument/references",
                    line,
                    character,
                    json!({ "context": { "includeDeclaration": true } }),
                )
                .await
        }
        Operation::DocumentSymbols { path } => {
            service
                .document_request(&path, "textDocument/documentSymbol", json!({}))
                .await
        }
        Operation::WorkspaceSymbols { query } => service.workspace_symbols_local(&query).await,
        Operation::WorkspaceDiagnostics { path } => {
            service.workspace_diagnostics_local(path.as_deref()).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lsp::{LspClientKey, LspScopeKey, spec_for_id};
    use serde_json::json;

    fn status_request(id: Uuid, root: PathBuf, policy: LspPathPolicy) -> Request {
        Request {
            id,
            root,
            policy,
            operation: Operation::Status,
        }
    }

    #[tokio::test]
    async fn broker_clients_only_lease_their_own_scope_and_workspace() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let sessions: Sessions = Arc::new(Mutex::new(HashMap::new()));
        let ids: Vec<_> = (0..4).map(|_| Uuid::new_v4()).collect();
        for (id, root, policy) in [
            (ids[0], first.path(), LspPathPolicy::Unrestricted),
            (ids[1], first.path(), LspPathPolicy::Unrestricted),
            (ids[2], first.path(), LspPathPolicy::SessionWorkspace),
            (ids[3], second.path(), LspPathPolicy::Unrestricted),
        ] {
            let status = dispatch(
                status_request(id, root.to_path_buf(), policy),
                Arc::clone(&sessions),
            )
            .await
            .unwrap();
            assert_eq!(status["active_workspaces"], json!([]));
        }
        let services: Vec<_> = {
            let sessions = sessions.lock().await;
            ids.iter().map(|id| sessions[id].service.clone()).collect()
        };
        let spec = spec_for_id("rust-analyzer").unwrap();
        let first_client = services[0].lease_client(spec, first.path()).await;
        let same = services[1].lease_client(spec, first.path()).await;
        let restricted = services[2].lease_client(spec, first.path()).await;
        let other_tree = services[3].lease_client(spec, second.path()).await;
        assert!(Arc::ptr_eq(&first_client, &same));
        assert!(!Arc::ptr_eq(&first_client, &restricted));
        assert!(!Arc::ptr_eq(&first_client, &other_tree));
        let first_key = LspClientKey {
            server_id: "rust-analyzer",
            workspace_root: first.path().to_path_buf(),
            scope: LspScopeKey::Unrestricted,
        };
        assert!(services[0].leased.lock().await.contains(&first_key));
        assert!(!services[2].leased.lock().await.contains(&first_key));
        assert!(
            dispatch(
                status_request(
                    ids[0],
                    second.path().to_path_buf(),
                    LspPathPolicy::Unrestricted
                ),
                sessions
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("identity changed")
        );
    }

    #[test]
    fn only_stale_socket_errors_trigger_an_election() {
        assert!(stale_socket(&std::io::Error::from(ErrorKind::NotFound)));
        assert!(stale_socket(&std::io::Error::from(
            ErrorKind::ConnectionRefused
        )));
        assert!(!stale_socket(&std::io::Error::from(ErrorKind::WouldBlock)));
        assert!(!stale_socket(&std::io::Error::from(
            ErrorKind::PermissionDenied
        )));
    }

    #[test]
    fn broker_subprocess_client() {
        let Ok(base) = std::env::var("BORG_LSP_TEST_SOCKET_BASE") else {
            return;
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        let status = rt
            .block_on(request_on(
                status_request(
                    Uuid::new_v4(),
                    PathBuf::from(&base),
                    LspPathPolicy::Unrestricted,
                ),
                socket_paths_in(PathBuf::from(base)).unwrap(),
            ))
            .unwrap();
        println!("BROKER_PID={}", status["broker_pid"].as_u64().unwrap());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn second_process_uses_the_same_broker() {
        let base = tempfile::tempdir().unwrap();
        let paths = socket_paths_in(base.path().to_path_buf()).unwrap();
        let status = request_on(
            status_request(
                Uuid::new_v4(),
                base.path().to_path_buf(),
                LspPathPolicy::Unrestricted,
            ),
            paths,
        )
        .await
        .unwrap();
        assert_eq!(status["broker_pid"], json!(std::process::id()));
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("lsp::broker::tests::broker_subprocess_client")
            .arg("--nocapture")
            .env("BORG_LSP_TEST_SOCKET_BASE", base.path())
            .output()
            .unwrap();
        assert!(
            child.status.success(),
            "{}",
            String::from_utf8_lossy(&child.stderr)
        );
        assert!(
            String::from_utf8_lossy(&child.stdout)
                .contains(&format!("BROKER_PID={}", std::process::id()))
        );
    }
}
