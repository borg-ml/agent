use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};

use base64::Engine as _;
use chrono::{DateTime, Utc};
use tokio::fs;
use uuid::Uuid;

use crate::{
    WorkspaceFileEntry, WorkspaceFileKind, WorkspaceFilesystemErrorCode,
    WorkspaceFilesystemOperation, WorkspaceFilesystemOutcome, WorkspaceFilesystemOutput,
    WorkspaceFilesystemRequest, WorkspaceFilesystemResponse,
};

const MAX_TRANSFER_BYTES: usize = 8 * 1024 * 1024;

pub async fn execute_workspace_filesystem(
    enrolled_roots: &[PathBuf],
    request: WorkspaceFilesystemRequest,
) -> WorkspaceFilesystemResponse {
    let request_id = request.request_id;
    let workspace_id = request.workspace_id;
    let operation = request.operation.clone();
    let outcome = match tokio::time::timeout(
        std::time::Duration::from_millis(request.timeout_ms.clamp(1, 30 * 60 * 1000)),
        execute(enrolled_roots, &request.root_path, operation),
    )
    .await
    {
        Ok(Ok(output)) => WorkspaceFilesystemOutcome::Success { output },
        Ok(Err(error)) => *error,
        Err(_) => *failure(
            WorkspaceFilesystemErrorCode::TimedOut,
            "remote filesystem operation timed out",
            true,
        ),
    };
    WorkspaceFilesystemResponse {
        request_id,
        workspace_id,
        outcome,
    }
}

type FsResult<T> = Result<T, Box<WorkspaceFilesystemOutcome>>;

async fn execute(
    enrolled_roots: &[PathBuf],
    requested_root: &Path,
    operation: WorkspaceFilesystemOperation,
) -> FsResult<WorkspaceFilesystemOutput> {
    let root = canonical_workspace_root(enrolled_roots, requested_root)?;
    match operation {
        WorkspaceFilesystemOperation::List { path, limit } => {
            let directory = existing_path(&root, &path)?;
            let metadata = fs::metadata(&directory).await.map_err(io_failure)?;
            if !metadata.is_dir() {
                return Err(failure(
                    WorkspaceFilesystemErrorCode::NotADirectory,
                    "path must be a directory",
                    false,
                ));
            }
            let mut reader = fs::read_dir(&directory).await.map_err(io_failure)?;
            let mut entries = Vec::new();
            let mut truncated = false;
            while let Some(entry) = reader.next_entry().await.map_err(io_failure)? {
                if entries.len() >= limit {
                    truncated = true;
                    break;
                }
                let metadata = fs::symlink_metadata(entry.path())
                    .await
                    .map_err(io_failure)?;
                let relative = if path == Path::new(".") {
                    PathBuf::from(entry.file_name())
                } else {
                    path.join(entry.file_name())
                };
                entries.push(file_entry(relative, &metadata));
            }
            entries.sort_by(|left, right| left.path.cmp(&right.path));
            Ok(WorkspaceFilesystemOutput::Listed {
                path,
                entries,
                limit,
                truncated,
            })
        }
        WorkspaceFilesystemOperation::Stat { path } => {
            let target = existing_path(&root, &path)?;
            let metadata = fs::symlink_metadata(target).await.map_err(io_failure)?;
            Ok(WorkspaceFilesystemOutput::Stat {
                entry: file_entry(path, &metadata),
            })
        }
        WorkspaceFilesystemOperation::ReadText { path, max_bytes } => {
            let (bytes, metadata) = read_regular_file(&root, &path, max_bytes).await?;
            let text = String::from_utf8(bytes).map_err(|_| {
                failure(
                    WorkspaceFilesystemErrorCode::InvalidEncoding,
                    "workspace file is not valid UTF-8 text",
                    false,
                )
            })?;
            Ok(WorkspaceFilesystemOutput::Text {
                path,
                text,
                bytes: metadata.len(),
                max_bytes,
                modified_at: modified_at(&metadata),
            })
        }
        WorkspaceFilesystemOperation::ReadBytes { path, max_bytes } => {
            let (bytes, metadata) = read_regular_file(&root, &path, max_bytes).await?;
            Ok(WorkspaceFilesystemOutput::Bytes {
                path,
                content_base64: base64::engine::general_purpose::STANDARD.encode(bytes),
                bytes: metadata.len(),
                max_bytes,
                modified_at: modified_at(&metadata),
            })
        }
        WorkspaceFilesystemOperation::WriteText {
            path,
            text,
            overwrite,
            create_parent_dirs,
        } => {
            if text.len() > MAX_TRANSFER_BYTES {
                return Err(payload_too_large());
            }
            write_file(
                &root,
                &path,
                text.as_bytes(),
                overwrite,
                create_parent_dirs,
                "write_text",
            )
            .await
        }
        WorkspaceFilesystemOperation::WriteBytes {
            path,
            content_base64,
            overwrite,
            create_parent_dirs,
        } => {
            if content_base64.len() > MAX_TRANSFER_BYTES.div_ceil(3) * 4 {
                return Err(payload_too_large());
            }
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(content_base64)
                .map_err(|_| {
                    failure(
                        WorkspaceFilesystemErrorCode::InvalidEncoding,
                        "content_base64 is not valid standard base64",
                        false,
                    )
                })?;
            if bytes.len() > MAX_TRANSFER_BYTES {
                return Err(payload_too_large());
            }
            write_file(
                &root,
                &path,
                &bytes,
                overwrite,
                create_parent_dirs,
                "write_bytes",
            )
            .await
        }
        WorkspaceFilesystemOperation::Mkdir { path, recursive } => {
            validate_relative(&path, false)?;
            create_directories(&root, &path, recursive).await?;
            Ok(mutated(
                "mkdir",
                Some(path),
                None,
                None,
                None,
                None,
                None,
                true,
            ))
        }
        WorkspaceFilesystemOperation::Move {
            from_path,
            to_path,
            overwrite,
            create_parent_dirs,
        } => {
            let source = existing_path(&root, &from_path)?;
            let (target, _) = write_target(&root, &to_path, overwrite, create_parent_dirs).await?;
            if source == target {
                return Ok(mutated(
                    "move",
                    None,
                    Some(from_path),
                    Some(to_path),
                    None,
                    None,
                    None,
                    false,
                ));
            }
            remove_overwrite_target(&target).await?;
            fs::rename(source, target).await.map_err(io_failure)?;
            Ok(mutated(
                "move",
                None,
                Some(from_path),
                Some(to_path),
                None,
                None,
                None,
                true,
            ))
        }
        WorkspaceFilesystemOperation::Copy {
            from_path,
            to_path,
            overwrite,
            create_parent_dirs,
            recursive,
            max_entries,
        } => {
            let source = existing_path(&root, &from_path)?;
            let metadata = fs::metadata(&source).await.map_err(io_failure)?;
            let (target, _) = write_target(&root, &to_path, overwrite, create_parent_dirs).await?;
            if source == target {
                return Ok(mutated(
                    "copy",
                    None,
                    Some(from_path),
                    Some(to_path),
                    None,
                    Some(0),
                    None,
                    false,
                ));
            }
            if metadata.is_dir() {
                if !recursive {
                    return Err(invalid("copying a directory requires recursive=true"));
                }
                if target.starts_with(&source) {
                    return Err(invalid(
                        "directory copy target must not be inside the source directory",
                    ));
                }
                remove_overwrite_target(&target).await?;
                let stats = copy_tree(&source, &target, max_entries).await?;
                let mut output = mutated(
                    "copy",
                    None,
                    Some(from_path),
                    Some(to_path),
                    None,
                    Some(stats.bytes),
                    None,
                    true,
                );
                if let WorkspaceFilesystemOutput::Mutated {
                    files,
                    directories,
                    entries,
                    ..
                } = &mut output
                {
                    *files = Some(stats.files);
                    *directories = Some(stats.directories);
                    *entries = Some(stats.entries);
                }
                Ok(output)
            } else if metadata.is_file() {
                remove_overwrite_target(&target).await?;
                let bytes = fs::copy(source, target).await.map_err(io_failure)?;
                Ok(mutated(
                    "copy",
                    None,
                    Some(from_path),
                    Some(to_path),
                    None,
                    Some(bytes),
                    None,
                    true,
                ))
            } else {
                Err(invalid("copy supports regular files and directories only"))
            }
        }
        WorkspaceFilesystemOperation::Delete {
            path,
            archive,
            recursive,
        } => {
            let source = existing_path(&root, &path)?;
            let metadata = fs::metadata(&source).await.map_err(io_failure)?;
            if archive {
                let archive_path = PathBuf::from(".borg-archive")
                    .join("files")
                    .join(format!(
                        "{}-{}",
                        Utc::now().format("%Y%m%dT%H%M%SZ"),
                        Uuid::new_v4()
                    ))
                    .join(&path);
                let (target, _) = write_target(&root, &archive_path, false, true).await?;
                fs::rename(source, target).await.map_err(io_failure)?;
                return Ok(mutated(
                    "archive",
                    Some(path),
                    None,
                    None,
                    Some(archive_path),
                    None,
                    None,
                    true,
                ));
            }
            if metadata.is_dir() {
                if !recursive {
                    return Err(invalid("hard-deleting a directory requires recursive=true"));
                }
                fs::remove_dir_all(source).await.map_err(io_failure)?;
            } else if metadata.is_file() {
                fs::remove_file(source).await.map_err(io_failure)?;
            } else {
                return Err(invalid(
                    "hard delete supports regular files and directories only",
                ));
            }
            Ok(mutated(
                "delete",
                Some(path),
                None,
                None,
                None,
                None,
                None,
                true,
            ))
        }
    }
}

fn canonical_workspace_root(enrolled_roots: &[PathBuf], requested: &Path) -> FsResult<PathBuf> {
    let requested = requested.canonicalize().map_err(io_failure)?;
    let enrolled = enrolled_roots
        .iter()
        .filter_map(|root| root.canonicalize().ok())
        .any(|root| requested.starts_with(root));
    if !enrolled {
        return Err(failure(
            WorkspaceFilesystemErrorCode::PermissionDenied,
            "workspace root is outside this host's enrolled roots",
            false,
        ));
    }
    Ok(requested)
}

fn validate_relative(path: &Path, allow_root: bool) -> FsResult<()> {
    if path.is_absolute() {
        return Err(invalid("path must be relative to the workspace root"));
    }
    let mut normal = false;
    for component in path.components() {
        match component {
            Component::Normal(_) => normal = true,
            Component::CurDir => {}
            _ => return Err(invalid("path contains unsupported traversal components")),
        }
    }
    if !allow_root && !normal {
        return Err(invalid(
            "path must identify an item under the workspace root",
        ));
    }
    Ok(())
}

fn existing_path(root: &Path, relative: &Path) -> FsResult<PathBuf> {
    validate_relative(relative, true)?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        let Component::Normal(name) = component else {
            continue;
        };
        current.push(name);
        let metadata = std::fs::symlink_metadata(&current).map_err(io_failure)?;
        if metadata.file_type().is_symlink() {
            return Err(invalid("workspace file operations do not follow symlinks"));
        }
    }
    let canonical = current.canonicalize().map_err(io_failure)?;
    if !canonical.starts_with(root) {
        return Err(invalid("path must stay under the workspace root"));
    }
    Ok(canonical)
}

pub(crate) fn resolve_existing_workspace_path(
    root: &Path,
    relative: &Path,
) -> anyhow::Result<PathBuf> {
    existing_path(root, relative).map_err(|outcome| anyhow::anyhow!("{outcome:?}"))
}

async fn create_directories(root: &Path, relative: &Path, recursive: bool) -> FsResult<()> {
    let mut current = root.to_path_buf();
    let components = relative
        .components()
        .filter_map(|component| match component {
            Component::Normal(name) => Some(name.to_owned()),
            _ => None,
        })
        .collect::<Vec<_>>();
    for (index, name) in components.iter().enumerate() {
        current.push(name);
        match fs::symlink_metadata(&current).await {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(invalid("workspace file operations do not follow symlinks"));
            }
            Ok(metadata) if !metadata.is_dir() => {
                return Err(failure(
                    WorkspaceFilesystemErrorCode::NotADirectory,
                    "path component is not a directory",
                    false,
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {
                if !recursive && index + 1 != components.len() {
                    return Err(failure(
                        WorkspaceFilesystemErrorCode::NotFound,
                        "parent directory does not exist",
                        false,
                    ));
                }
                fs::create_dir(&current).await.map_err(io_failure)?;
            }
            Err(error) => return Err(io_failure(error)),
        }
    }
    Ok(())
}

async fn write_target(
    root: &Path,
    relative: &Path,
    overwrite: bool,
    create_parent_dirs: bool,
) -> FsResult<(PathBuf, bool)> {
    validate_relative(relative, false)?;
    let parent = relative
        .parent()
        .ok_or_else(|| invalid("target path must have a parent directory"))?;
    if create_parent_dirs {
        create_directories(root, parent, true).await?;
    }
    let canonical_parent = existing_path(root, parent)?;
    let name = relative
        .file_name()
        .ok_or_else(|| invalid("target path must include a file name"))?;
    let target = canonical_parent.join(name);
    match fs::symlink_metadata(&target).await {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(invalid(
            "workspace file operations do not overwrite symlinks",
        )),
        Ok(_) if !overwrite => Err(failure(
            WorkspaceFilesystemErrorCode::AlreadyExists,
            "target already exists",
            false,
        )),
        Ok(_) => Ok((target, false)),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok((target, true)),
        Err(error) => Err(io_failure(error)),
    }
}

async fn write_file(
    root: &Path,
    path: &Path,
    bytes: &[u8],
    overwrite: bool,
    create_parent_dirs: bool,
    operation: &str,
) -> FsResult<WorkspaceFilesystemOutput> {
    let (target, created) = write_target(root, path, overwrite, create_parent_dirs).await?;
    if target.is_dir() {
        return Err(failure(
            WorkspaceFilesystemErrorCode::NotAFile,
            "target path is a directory",
            false,
        ));
    }
    fs::write(target, bytes).await.map_err(io_failure)?;
    Ok(mutated(
        operation,
        Some(path.to_path_buf()),
        None,
        None,
        None,
        Some(bytes.len() as u64),
        Some(created),
        true,
    ))
}

async fn read_regular_file(
    root: &Path,
    path: &Path,
    max_bytes: u64,
) -> FsResult<(Vec<u8>, std::fs::Metadata)> {
    let target = existing_path(root, path)?;
    let metadata = fs::metadata(&target).await.map_err(io_failure)?;
    if !metadata.is_file() {
        return Err(failure(
            WorkspaceFilesystemErrorCode::NotAFile,
            "path must be a regular file",
            false,
        ));
    }
    let limit = max_bytes.min(MAX_TRANSFER_BYTES as u64);
    if metadata.len() > limit {
        return Err(payload_too_large());
    }
    Ok((fs::read(target).await.map_err(io_failure)?, metadata))
}

async fn remove_overwrite_target(path: &Path) -> FsResult<()> {
    match fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(invalid(
            "workspace file operations do not overwrite symlinks",
        )),
        Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(path).await.map_err(io_failure),
        Ok(_) => fs::remove_file(path).await.map_err(io_failure),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_failure(error)),
    }
}

#[derive(Default)]
struct CopyStats {
    files: u64,
    directories: u64,
    entries: u64,
    bytes: u64,
}

async fn copy_tree(source: &Path, target: &Path, max_entries: usize) -> FsResult<CopyStats> {
    fs::create_dir(target).await.map_err(io_failure)?;
    let mut stats = CopyStats {
        directories: 1,
        entries: 1,
        ..CopyStats::default()
    };
    let mut pending = vec![(source.to_path_buf(), target.to_path_buf())];
    while let Some((source_dir, target_dir)) = pending.pop() {
        let mut reader = fs::read_dir(source_dir).await.map_err(io_failure)?;
        while let Some(entry) = reader.next_entry().await.map_err(io_failure)? {
            stats.entries += 1;
            if stats.entries > max_entries as u64 {
                return Err(payload_too_large());
            }
            let metadata = fs::symlink_metadata(entry.path())
                .await
                .map_err(io_failure)?;
            if metadata.file_type().is_symlink() {
                return Err(invalid("workspace directory copy does not copy symlinks"));
            }
            let destination = target_dir.join(entry.file_name());
            if metadata.is_dir() {
                fs::create_dir(&destination).await.map_err(io_failure)?;
                stats.directories += 1;
                pending.push((entry.path(), destination));
            } else if metadata.is_file() {
                stats.bytes += fs::copy(entry.path(), destination)
                    .await
                    .map_err(io_failure)?;
                stats.files += 1;
            } else {
                return Err(invalid(
                    "workspace directory copy supports regular files and directories only",
                ));
            }
        }
    }
    Ok(stats)
}

fn file_entry(path: PathBuf, metadata: &std::fs::Metadata) -> WorkspaceFileEntry {
    let kind = if metadata.is_file() {
        WorkspaceFileKind::File
    } else if metadata.is_dir() {
        WorkspaceFileKind::Directory
    } else if metadata.file_type().is_symlink() {
        WorkspaceFileKind::Symlink
    } else {
        WorkspaceFileKind::Other
    };
    WorkspaceFileEntry {
        name: path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default()
            .to_string(),
        path,
        kind,
        bytes: metadata.is_file().then_some(metadata.len()),
        modified_at: modified_at(metadata),
    }
}

fn modified_at(metadata: &std::fs::Metadata) -> Option<DateTime<Utc>> {
    metadata.modified().ok().map(DateTime::<Utc>::from)
}

#[allow(clippy::too_many_arguments)]
fn mutated(
    operation: &str,
    path: Option<PathBuf>,
    from_path: Option<PathBuf>,
    to_path: Option<PathBuf>,
    archived_path: Option<PathBuf>,
    bytes: Option<u64>,
    created: Option<bool>,
    changed: bool,
) -> WorkspaceFilesystemOutput {
    WorkspaceFilesystemOutput::Mutated {
        operation: operation.to_string(),
        path,
        from_path,
        to_path,
        archived_path,
        bytes,
        created,
        changed,
        files: None,
        directories: None,
        entries: None,
    }
}

fn invalid(message: impl Into<String>) -> Box<WorkspaceFilesystemOutcome> {
    failure(WorkspaceFilesystemErrorCode::InvalidPath, message, false)
}

fn payload_too_large() -> Box<WorkspaceFilesystemOutcome> {
    failure(
        WorkspaceFilesystemErrorCode::PayloadTooLarge,
        "filesystem payload exceeds the bounded transfer limit",
        false,
    )
}

fn io_failure(error: std::io::Error) -> Box<WorkspaceFilesystemOutcome> {
    let (code, retryable) = match error.kind() {
        ErrorKind::NotFound => (WorkspaceFilesystemErrorCode::NotFound, false),
        ErrorKind::AlreadyExists => (WorkspaceFilesystemErrorCode::AlreadyExists, false),
        ErrorKind::PermissionDenied => (WorkspaceFilesystemErrorCode::PermissionDenied, false),
        ErrorKind::Interrupted | ErrorKind::WouldBlock | ErrorKind::TimedOut => {
            (WorkspaceFilesystemErrorCode::Io, true)
        }
        _ => (WorkspaceFilesystemErrorCode::Io, false),
    };
    failure(code, error.to_string(), retryable)
}

fn failure(
    code: WorkspaceFilesystemErrorCode,
    message: impl Into<String>,
    retryable: bool,
) -> Box<WorkspaceFilesystemOutcome> {
    Box::new(WorkspaceFilesystemOutcome::Failure {
        code,
        message: message.into(),
        retryable,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(root: &Path, operation: WorkspaceFilesystemOperation) -> WorkspaceFilesystemRequest {
        WorkspaceFilesystemRequest {
            request_id: Uuid::new_v4(),
            workspace_id: Uuid::new_v4(),
            root_path: root.to_path_buf(),
            timeout_ms: 5_000,
            operation,
        }
    }

    #[tokio::test]
    async fn rejects_traversal_and_symlink_escape() {
        let enrolled = tempfile::tempdir().unwrap();
        let root = enrolled.path().join("workspace");
        fs::create_dir(&root).await.unwrap();
        let traversal = execute_workspace_filesystem(
            &[enrolled.path().to_path_buf()],
            request(
                &root,
                WorkspaceFilesystemOperation::ReadText {
                    path: PathBuf::from("../secret"),
                    max_bytes: 1024,
                },
            ),
        )
        .await;
        assert!(matches!(
            traversal.outcome,
            WorkspaceFilesystemOutcome::Failure {
                code: WorkspaceFilesystemErrorCode::InvalidPath,
                ..
            }
        ));

        #[cfg(unix)]
        {
            let outside = tempfile::tempdir().unwrap();
            std::os::unix::fs::symlink(outside.path(), root.join("escape")).unwrap();
            let escaped = execute_workspace_filesystem(
                &[enrolled.path().to_path_buf()],
                request(
                    &root,
                    WorkspaceFilesystemOperation::WriteText {
                        path: PathBuf::from("escape/leak.txt"),
                        text: "no".to_string(),
                        overwrite: true,
                        create_parent_dirs: true,
                    },
                ),
            )
            .await;
            assert!(matches!(
                escaped.outcome,
                WorkspaceFilesystemOutcome::Failure {
                    code: WorkspaceFilesystemErrorCode::InvalidPath,
                    ..
                }
            ));
        }
    }

    #[tokio::test]
    async fn write_then_read_round_trip() {
        let enrolled = tempfile::tempdir().unwrap();
        let root = enrolled.path().join("workspace");
        fs::create_dir(&root).await.unwrap();
        let write = execute_workspace_filesystem(
            &[enrolled.path().to_path_buf()],
            request(
                &root,
                WorkspaceFilesystemOperation::WriteText {
                    path: PathBuf::from("src/lib.rs"),
                    text: "pub fn borg() {}".to_string(),
                    overwrite: true,
                    create_parent_dirs: true,
                },
            ),
        )
        .await;
        assert!(matches!(
            write.outcome,
            WorkspaceFilesystemOutcome::Success { .. }
        ));
        let read = execute_workspace_filesystem(
            &[enrolled.path().to_path_buf()],
            request(
                &root,
                WorkspaceFilesystemOperation::ReadText {
                    path: PathBuf::from("src/lib.rs"),
                    max_bytes: 1024,
                },
            ),
        )
        .await;
        assert!(matches!(
            read.outcome,
            WorkspaceFilesystemOutcome::Success {
                output: WorkspaceFilesystemOutput::Text { ref text, .. }
            } if text == "pub fn borg() {}"
        ));
    }
}
