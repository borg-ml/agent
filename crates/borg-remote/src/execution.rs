use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use uuid::Uuid;

use crate::native_process::{ProcessManager, ProcessSnapshot};
use crate::{
    HostResourceLimits, SqliteSessionStore, WorkspaceFilesystemRequest, WorkspaceFilesystemResponse,
};

/// One command executed in an agent's execution world.
#[derive(Clone)]
pub struct ExecutionCommandRequest {
    pub owner_session_id: Uuid,
    pub root: PathBuf,
    pub command: String,
    pub workdir: Option<String>,
    pub yield_time_ms: Option<u64>,
    pub max_output_tokens: Option<usize>,
    pub timeout_ms: u64,
    pub journal: Option<SqliteSessionStore>,
}

/// One stdin interaction with a process in an agent's execution world.
#[derive(Debug, Clone)]
pub struct ExecutionStdinRequest {
    pub owner_session_id: Uuid,
    pub process_id: Uuid,
    pub chars: Option<String>,
    pub terminate: bool,
    pub yield_time_ms: Option<u64>,
    pub max_output_tokens: Option<usize>,
}

/// A bounded text read in an execution world.
#[derive(Debug, Clone)]
pub struct ExecutionReadRequest {
    pub root: PathBuf,
    pub path: PathBuf,
    pub offset_line: usize,
    pub limit_lines: usize,
    pub max_bytes: usize,
}

/// A bounded, gitignore-aware text search in an execution world.
#[derive(Debug, Clone)]
pub struct ExecutionSearchRequest {
    pub root: PathBuf,
    pub path: PathBuf,
    pub pattern: String,
    pub literal: bool,
    pub case_sensitive: bool,
    pub offset: usize,
    pub limit: usize,
}

/// Provider-neutral execution boundary used by native tools and runtimes.
///
/// Borg's default implementation is local and trusted-user execution. A
/// container, remote worker, or other execution world can implement this
/// boundary without changing model-facing tool schemas or the durable session
/// protocol.
#[async_trait]
pub trait ExecutionProvider: Send + Sync {
    async fn recover_session(&self, session_id: Uuid, store: SqliteSessionStore) -> Result<()>;

    async fn filesystem(
        &self,
        enrolled_roots: &[PathBuf],
        request: WorkspaceFilesystemRequest,
        limits: &HostResourceLimits,
    ) -> WorkspaceFilesystemResponse;

    async fn read_file(&self, request: ExecutionReadRequest) -> Result<Value> {
        Ok(serde_json::to_value(
            crate::native_io::read_text_range(
                request.root,
                request.path,
                request.offset_line,
                request.limit_lines,
                request.max_bytes,
            )
            .await?,
        )?)
    }

    async fn search_files(&self, request: ExecutionSearchRequest) -> Result<Value> {
        Ok(serde_json::to_value(
            crate::native_io::search_text(
                request.root,
                request.path,
                request.pattern,
                request.literal,
                request.case_sensitive,
                request.offset,
                request.limit,
            )
            .await?,
        )?)
    }

    async fn command(&self, request: ExecutionCommandRequest) -> Result<ProcessSnapshot>;

    async fn write_stdin(&self, request: ExecutionStdinRequest) -> Result<ProcessSnapshot>;

    async fn terminate_session(&self, session_id: Uuid);
}

/// The current local execution world. It preserves Borg's existing process,
/// filesystem, journaling, and recovery behavior behind `ExecutionProvider`.
#[derive(Debug, Clone, Default)]
pub struct LocalExecutionProvider {
    processes: ProcessManager,
}

impl LocalExecutionProvider {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn with_process_manager(processes: ProcessManager) -> Self {
        Self { processes }
    }
}

#[async_trait]
impl ExecutionProvider for LocalExecutionProvider {
    async fn recover_session(&self, session_id: Uuid, store: SqliteSessionStore) -> Result<()> {
        self.processes.recover_session(session_id, store).await
    }

    async fn filesystem(
        &self,
        enrolled_roots: &[PathBuf],
        request: WorkspaceFilesystemRequest,
        limits: &HostResourceLimits,
    ) -> WorkspaceFilesystemResponse {
        crate::execute_workspace_filesystem_with_limits(enrolled_roots, request, limits).await
    }

    async fn command(&self, request: ExecutionCommandRequest) -> Result<ProcessSnapshot> {
        self.processes
            .exec(
                request.owner_session_id,
                &request.root,
                request.command,
                request.workdir.as_deref(),
                request.yield_time_ms,
                request.max_output_tokens,
                request.timeout_ms,
                request.journal,
            )
            .await
    }

    async fn write_stdin(&self, request: ExecutionStdinRequest) -> Result<ProcessSnapshot> {
        self.processes
            .write_stdin(
                request.owner_session_id,
                request.process_id,
                request.chars.as_deref(),
                request.terminate,
                request.yield_time_ms,
                request.max_output_tokens,
            )
            .await
    }

    async fn terminate_session(&self, session_id: Uuid) {
        self.processes.terminate_session(session_id).await;
    }
}
