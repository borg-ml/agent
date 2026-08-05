use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;

use anyhow::Context;
use chrono::Utc;
use tokio::io::{AsyncRead, AsyncReadExt};
use uuid::Uuid;

use crate::{
    WorkspaceCommandErrorCode, WorkspaceCommandOutcome, WorkspaceCommandOutput,
    WorkspaceCommandRequest, WorkspaceCommandResponse,
};

const MAX_COMMAND_ARGS: usize = 128;
const MAX_COMMAND_ARG_CHARS: usize = 16_384;
const MAX_OUTPUT_BYTES: u64 = 1024 * 1024;
const MAX_TIMEOUT_MS: u64 = 30 * 60 * 1000;

pub async fn execute_workspace_command(
    enrolled_roots: &[PathBuf],
    request: WorkspaceCommandRequest,
) -> WorkspaceCommandResponse {
    let request_id = request.request_id;
    let workspace_id = request.workspace_id;
    let outcome = match execute(enrolled_roots, request).await {
        Ok(output) => WorkspaceCommandOutcome::Success {
            output: Box::new(output),
        },
        Err(outcome) => *outcome,
    };
    WorkspaceCommandResponse {
        request_id,
        workspace_id,
        outcome,
    }
}

type CommandResult<T> = Result<T, Box<WorkspaceCommandOutcome>>;

async fn execute(
    enrolled_roots: &[PathBuf],
    request: WorkspaceCommandRequest,
) -> CommandResult<WorkspaceCommandOutput> {
    validate_command(&request.command)?;
    let root = canonical_workspace_root(enrolled_roots, &request.root_path)?;
    let cwd = canonical_workspace_cwd(&root, &request.cwd)?;
    let timeout_ms = request.timeout_ms.clamp(1, MAX_TIMEOUT_MS);
    let output_max_bytes = request.output_max_bytes.clamp(1, MAX_OUTPUT_BYTES);
    let command_id = Uuid::new_v4();
    let started_at = Utc::now();
    let mut process = tokio::process::Command::new(&request.command[0]);
    process
        .args(&request.command[1..])
        .current_dir(&cwd)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = process.spawn().map_err(|error| {
        failure(
            if error.kind() == ErrorKind::PermissionDenied {
                WorkspaceCommandErrorCode::PermissionDenied
            } else {
                WorkspaceCommandErrorCode::Io
            },
            format!(
                "failed to start workspace command executable `{}`: {error}",
                request.command[0]
            ),
            matches!(
                error.kind(),
                ErrorKind::Interrupted | ErrorKind::WouldBlock | ErrorKind::TimedOut
            ),
        )
    })?;
    let stdout = child.stdout.take().ok_or_else(|| {
        failure(
            WorkspaceCommandErrorCode::Io,
            "workspace command stdout pipe missing",
            false,
        )
    })?;
    let stderr = child.stderr.take().ok_or_else(|| {
        failure(
            WorkspaceCommandErrorCode::Io,
            "workspace command stderr pipe missing",
            false,
        )
    })?;
    let stdout_task = tokio::spawn(read_limited_pipe(stdout, output_max_bytes as usize));
    let stderr_task = tokio::spawn(read_limited_pipe(stderr, output_max_bytes as usize));
    let wait =
        tokio::time::timeout(std::time::Duration::from_millis(timeout_ms), child.wait()).await;
    let (exit_code, timed_out) = match wait {
        Ok(Ok(status)) => (status.code(), false),
        Ok(Err(error)) => return Err(io_failure(error)),
        Err(_) => {
            let _ = child.kill().await;
            (None, true)
        }
    };
    let (stdout, stdout_truncated) = stdout_task
        .await
        .context("workspace command stdout task panicked")
        .and_then(|result| result)
        .map_err(anyhow_failure)?;
    let (stderr, stderr_truncated) = stderr_task
        .await
        .context("workspace command stderr task panicked")
        .and_then(|result| result)
        .map_err(anyhow_failure)?;
    let finished_at = Utc::now();
    // The complete output is persisted by the host's SQLite receipt in the
    // same transaction as the mutation identity. Keep only a stable virtual
    // locator in the wire contract; writing a second JSON manifest beside the
    // workspace would create an untracked, non-transactional source of truth.
    let manifest_path = PathBuf::from(format!("borg://artifact/command/{command_id}"));
    let output = WorkspaceCommandOutput {
        id: command_id,
        workspace_id: request.workspace_id,
        workspace_root: root,
        cwd,
        command: request.command,
        timeout_seconds: timeout_ms.div_ceil(1000),
        timed_out,
        exit_code,
        stdout,
        stderr,
        stdout_truncated,
        stderr_truncated,
        output_max_bytes,
        started_at,
        finished_at,
        manifest_path: manifest_path.clone(),
    };
    Ok(output)
}

fn validate_command(command: &[String]) -> CommandResult<()> {
    if command.is_empty() || command.len() > MAX_COMMAND_ARGS {
        return Err(failure(
            WorkspaceCommandErrorCode::InvalidCommand,
            format!("command must contain between 1 and {MAX_COMMAND_ARGS} arguments"),
            false,
        ));
    }
    for (index, argument) in command.iter().enumerate() {
        if (index == 0 && argument.is_empty())
            || argument.chars().count() > MAX_COMMAND_ARG_CHARS
            || argument
                .chars()
                .any(|character| character.is_control() && character != '\t')
        {
            return Err(failure(
                WorkspaceCommandErrorCode::InvalidCommand,
                format!("command argument {index} is invalid"),
                false,
            ));
        }
    }
    Ok(())
}

fn canonical_workspace_root(
    enrolled_roots: &[PathBuf],
    requested: &Path,
) -> CommandResult<PathBuf> {
    let requested = requested.canonicalize().map_err(io_failure)?;
    let enrolled = enrolled_roots
        .iter()
        .filter_map(|root| root.canonicalize().ok())
        .any(|root| requested.starts_with(root));
    if !enrolled {
        return Err(failure(
            WorkspaceCommandErrorCode::PermissionDenied,
            "workspace root is outside this host's enrolled roots",
            false,
        ));
    }
    Ok(requested)
}

fn canonical_workspace_cwd(root: &Path, relative: &Path) -> CommandResult<PathBuf> {
    if relative.is_absolute()
        || relative
            .components()
            .any(|component| !matches!(component, Component::Normal(_) | Component::CurDir))
    {
        return Err(failure(
            WorkspaceCommandErrorCode::InvalidCwd,
            "cwd must be relative to the workspace root without traversal",
            false,
        ));
    }
    let mut current = root.to_path_buf();
    for component in relative.components() {
        let Component::Normal(name) = component else {
            continue;
        };
        current.push(name);
        let metadata = std::fs::symlink_metadata(&current).map_err(io_failure)?;
        if metadata.file_type().is_symlink() {
            return Err(failure(
                WorkspaceCommandErrorCode::InvalidCwd,
                "workspace command cwd must not traverse symlinks",
                false,
            ));
        }
    }
    let canonical = current.canonicalize().map_err(io_failure)?;
    if !canonical.starts_with(root) || !canonical.is_dir() {
        return Err(failure(
            WorkspaceCommandErrorCode::InvalidCwd,
            "cwd must be an existing directory under the workspace root",
            false,
        ));
    }
    Ok(canonical)
}

async fn read_limited_pipe<R>(mut reader: R, max_bytes: usize) -> anyhow::Result<(String, bool)>
where
    R: AsyncRead + Unpin,
{
    let mut output = Vec::new();
    let mut truncated = false;
    let mut buffer = [0_u8; 8192];
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        let remaining = max_bytes.saturating_sub(output.len());
        let keep = remaining.min(read);
        output.extend_from_slice(&buffer[..keep]);
        truncated |= keep < read;
    }
    Ok((String::from_utf8_lossy(&output).to_string(), truncated))
}

fn io_failure(error: std::io::Error) -> Box<WorkspaceCommandOutcome> {
    failure(
        if error.kind() == ErrorKind::PermissionDenied {
            WorkspaceCommandErrorCode::PermissionDenied
        } else {
            WorkspaceCommandErrorCode::Io
        },
        error.to_string(),
        matches!(
            error.kind(),
            ErrorKind::Interrupted | ErrorKind::WouldBlock | ErrorKind::TimedOut
        ),
    )
}

fn anyhow_failure(error: anyhow::Error) -> Box<WorkspaceCommandOutcome> {
    failure(WorkspaceCommandErrorCode::Io, error.to_string(), false)
}

fn failure(
    code: WorkspaceCommandErrorCode,
    message: impl Into<String>,
    retryable: bool,
) -> Box<WorkspaceCommandOutcome> {
    Box::new(WorkspaceCommandOutcome::Failure {
        code,
        message: message.into(),
        retryable,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn command_cwd_cannot_escape_enrolled_root_through_symlink() {
        let root = tempfile::tempdir().expect("root");
        let outside = tempfile::tempdir().expect("outside");
        #[cfg(unix)]
        std::os::unix::fs::symlink(outside.path(), root.path().join("escape")).expect("symlink");
        #[cfg(windows)]
        std::os::windows::fs::symlink_dir(outside.path(), root.path().join("escape"))
            .expect("symlink");
        let response = execute_workspace_command(
            &[root.path().to_path_buf()],
            WorkspaceCommandRequest {
                request_id: Uuid::new_v4(),
                workspace_id: Uuid::new_v4(),
                root_path: root.path().to_path_buf(),
                cwd: PathBuf::from("escape"),
                command: vec!["pwd".to_string()],
                timeout_ms: 1_000,
                output_max_bytes: 1_024,
            },
        )
        .await;
        assert!(matches!(
            response.outcome,
            WorkspaceCommandOutcome::Failure {
                code: WorkspaceCommandErrorCode::InvalidCwd,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn command_output_is_bounded_and_manifested() {
        let root = tempfile::tempdir().expect("root");
        let response = execute_workspace_command(
            &[root.path().to_path_buf()],
            WorkspaceCommandRequest {
                request_id: Uuid::new_v4(),
                workspace_id: Uuid::new_v4(),
                root_path: root.path().to_path_buf(),
                cwd: PathBuf::from("."),
                command: vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    "printf 123456789".to_string(),
                ],
                timeout_ms: 1_000,
                output_max_bytes: 4,
            },
        )
        .await;
        let WorkspaceCommandOutcome::Success { output } = response.outcome else {
            panic!("command should succeed");
        };
        assert_eq!(output.stdout, "1234");
        assert!(output.stdout_truncated);
        assert_eq!(
            output.manifest_path,
            PathBuf::from(format!("borg://artifact/command/{}", output.id))
        );
    }
}
