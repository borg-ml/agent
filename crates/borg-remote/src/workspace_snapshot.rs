//! Bounded, opt-in workspace snapshots for reversible local edits.
//!
//! A snapshot is deliberately a plain file manifest. It does not claim to
//! capture processes, databases, network effects, or files outside the chosen
//! root. Symlinks and Borg's own state directories are excluded.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const WORKSPACE_SNAPSHOT_VERSION: u32 = 1;
pub const DEFAULT_MAX_SNAPSHOT_FILES: usize = 4_096;
pub const DEFAULT_MAX_SNAPSHOT_BYTES: u64 = 16 * 1024 * 1024;
pub const MAX_SNAPSHOT_FILE_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceSnapshot {
    pub version: u32,
    pub root: String,
    pub files: Vec<WorkspaceSnapshotFile>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceSnapshotFile {
    pub path: PathBuf,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceRestoreReport {
    pub restored_files: usize,
    pub removed_files: usize,
}

impl WorkspaceSnapshot {
    pub fn capture(root: impl AsRef<Path>) -> Result<Self> {
        Self::capture_with_limits(root, DEFAULT_MAX_SNAPSHOT_FILES, DEFAULT_MAX_SNAPSHOT_BYTES)
    }

    pub fn capture_with_limits(
        root: impl AsRef<Path>,
        max_files: usize,
        max_bytes: u64,
    ) -> Result<Self> {
        ensure!(
            max_files > 0,
            "workspace snapshot file limit must be positive"
        );
        ensure!(
            max_bytes > 0,
            "workspace snapshot byte limit must be positive"
        );
        let root = root
            .as_ref()
            .canonicalize()
            .with_context(|| format!("canonicalize workspace root {}", root.as_ref().display()))?;
        ensure!(root.is_dir(), "workspace snapshot root is not a directory");
        let mut files = Vec::new();
        let mut total_bytes = 0u64;
        collect_files(
            &root,
            &root,
            max_files,
            max_bytes,
            &mut total_bytes,
            &mut files,
        )?;
        Ok(Self {
            version: WORKSPACE_SNAPSHOT_VERSION,
            root: root.display().to_string(),
            files,
        })
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.version == WORKSPACE_SNAPSHOT_VERSION,
            "unsupported workspace snapshot version {}; expected {}",
            self.version,
            WORKSPACE_SNAPSHOT_VERSION
        );
        let mut paths = BTreeSet::new();
        let mut total_bytes = 0u64;
        ensure!(
            self.files.len() <= DEFAULT_MAX_SNAPSHOT_FILES,
            "workspace snapshot contains too many files"
        );
        for file in &self.files {
            validate_relative_path(&file.path)?;
            ensure!(
                u64::try_from(file.bytes.len()).unwrap_or(u64::MAX) <= MAX_SNAPSHOT_FILE_BYTES,
                "workspace snapshot file {} is too large",
                file.path.display()
            );
            total_bytes = total_bytes.saturating_add(file.bytes.len() as u64);
            ensure!(
                total_bytes <= DEFAULT_MAX_SNAPSHOT_BYTES,
                "workspace snapshot is too large"
            );
            ensure!(
                paths.insert(file.path.clone()),
                "duplicate snapshot path {}",
                file.path.display()
            );
        }
        Ok(())
    }

    /// Restore captured files. Extra files are retained unless `prune_extra`
    /// is explicitly requested, making ordinary restore non-destructive.
    pub fn restore(
        &self,
        root: impl AsRef<Path>,
        prune_extra: bool,
    ) -> Result<WorkspaceRestoreReport> {
        self.validate()?;
        let root = root
            .as_ref()
            .canonicalize()
            .with_context(|| format!("canonicalize workspace root {}", root.as_ref().display()))?;
        ensure!(root.is_dir(), "workspace restore root is not a directory");
        let mut paths = BTreeSet::new();
        let mut restored_files = 0;
        for file in &self.files {
            validate_relative_path(&file.path)?;
            let destination = safe_destination(&root, &file.path)?;
            if let Ok(metadata) = fs::symlink_metadata(&destination) {
                ensure!(
                    !metadata.file_type().is_symlink(),
                    "refusing to restore through symlink {}",
                    file.path.display()
                );
            }
            let parent = destination
                .parent()
                .context("workspace snapshot destination has no parent")?;
            fs::create_dir_all(parent)?;
            ensure_parent_inside(&root, parent)?;
            let temp = parent.join(format!(
                ".{}.borg-restore-{}.tmp",
                destination
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("file"),
                Uuid::new_v4().simple()
            ));
            fs::write(&temp, &file.bytes)?;
            fs::rename(&temp, &destination).with_context(|| {
                format!("replace restored workspace file {}", file.path.display())
            })?;
            paths.insert(file.path.clone());
            restored_files += 1;
        }
        let removed_files = if prune_extra {
            let mut current = Vec::new();
            let mut ignored_bytes = 0;
            collect_files(
                &root,
                &root,
                DEFAULT_MAX_SNAPSHOT_FILES,
                DEFAULT_MAX_SNAPSHOT_BYTES,
                &mut ignored_bytes,
                &mut current,
            )?;
            let mut removed = 0;
            for file in current {
                if !paths.contains(&file.path) {
                    let path = safe_destination(&root, &file.path)?;
                    fs::remove_file(path)?;
                    removed += 1;
                }
            }
            removed
        } else {
            0
        };
        Ok(WorkspaceRestoreReport {
            restored_files,
            removed_files,
        })
    }
}

fn collect_files(
    root: &Path,
    directory: &Path,
    max_files: usize,
    max_bytes: u64,
    total_bytes: &mut u64,
    files: &mut Vec<WorkspaceSnapshotFile>,
) -> Result<()> {
    let mut entries = fs::read_dir(directory)
        .with_context(|| format!("read workspace directory {}", directory.display()))?
        .collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let name = entry.file_name();
        if name == ".git" || name == ".borg" {
            continue;
        }
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            continue;
        }
        if metadata.is_dir() {
            collect_files(root, &path, max_files, max_bytes, total_bytes, files)?;
            continue;
        }
        if !metadata.is_file() {
            continue;
        }
        ensure!(
            files.len() < max_files,
            "workspace snapshot contains too many files"
        );
        let byte_len = metadata.len();
        ensure!(
            byte_len <= MAX_SNAPSHOT_FILE_BYTES,
            "workspace file {} exceeds the per-file snapshot limit",
            path.display()
        );
        ensure!(
            total_bytes.saturating_add(byte_len) <= max_bytes,
            "workspace snapshot exceeds its byte limit"
        );
        let bytes = fs::read(&path)?;
        ensure!(
            bytes.len() as u64 == byte_len,
            "workspace file changed while snapshotting"
        );
        let relative = path.strip_prefix(root)?.to_path_buf();
        validate_relative_path(&relative)?;
        *total_bytes = total_bytes.saturating_add(byte_len);
        files.push(WorkspaceSnapshotFile {
            path: relative,
            bytes,
        });
    }
    Ok(())
}

fn validate_relative_path(path: &Path) -> Result<()> {
    ensure!(
        !path.as_os_str().is_empty(),
        "workspace snapshot path is empty"
    );
    ensure!(
        !path.is_absolute(),
        "workspace snapshot path must be relative"
    );
    for component in path.components() {
        ensure!(
            matches!(component, Component::Normal(_)),
            "workspace snapshot path escapes its root"
        );
    }
    Ok(())
}

fn safe_destination(root: &Path, relative: &Path) -> Result<PathBuf> {
    validate_relative_path(relative)?;
    let mut current = root.to_path_buf();
    let components = relative.components().collect::<Vec<_>>();
    for (index, component) in components.iter().enumerate() {
        current.push(component.as_os_str());
        let Ok(metadata) = fs::symlink_metadata(&current) else {
            continue;
        };
        ensure!(
            !metadata.file_type().is_symlink(),
            "workspace snapshot path traverses symlink {}",
            relative.display()
        );
        if index + 1 < components.len() {
            ensure!(
                metadata.is_dir(),
                "workspace snapshot parent is not a directory: {}",
                relative.display()
            );
        }
    }
    Ok(root.join(relative))
}

fn ensure_parent_inside(root: &Path, parent: &Path) -> Result<()> {
    let canonical = parent
        .canonicalize()
        .with_context(|| format!("canonicalize workspace parent {}", parent.display()))?;
    ensure!(
        canonical.starts_with(root),
        "workspace snapshot path escapes its root"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capture_and_restore_round_trip_is_bounded_and_non_destructive() {
        let root = tempfile::tempdir().expect("tempdir");
        fs::create_dir_all(root.path().join("nested")).expect("nested");
        fs::write(root.path().join("a.txt"), b"one").expect("a");
        fs::write(root.path().join("nested/b.txt"), b"two").expect("b");
        let snapshot = WorkspaceSnapshot::capture(root.path()).expect("capture");
        fs::write(root.path().join("a.txt"), b"changed").expect("change");
        fs::write(root.path().join("extra.txt"), b"keep").expect("extra");
        let report = snapshot.restore(root.path(), false).expect("restore");
        assert_eq!(report.restored_files, 2);
        assert_eq!(fs::read(root.path().join("a.txt")).expect("read"), b"one");
        assert!(root.path().join("extra.txt").is_file());
    }

    #[test]
    fn traversal_and_limits_are_rejected() {
        let root = tempfile::tempdir().expect("tempdir");
        fs::write(root.path().join("a"), b"123").expect("file");
        assert!(WorkspaceSnapshot::capture_with_limits(root.path(), 0, 10).is_err());
        let snapshot = WorkspaceSnapshot {
            version: WORKSPACE_SNAPSHOT_VERSION,
            root: root.path().display().to_string(),
            files: vec![WorkspaceSnapshotFile {
                path: PathBuf::from("../escape"),
                bytes: Vec::new(),
            }],
        };
        assert!(snapshot.validate().is_err());
    }

    #[cfg(unix)]
    #[test]
    fn restore_rejects_symlinked_parent_components() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("outside");
        symlink(outside.path(), root.path().join("linked")).expect("symlink");
        let snapshot = WorkspaceSnapshot {
            version: WORKSPACE_SNAPSHOT_VERSION,
            root: root.path().display().to_string(),
            files: vec![WorkspaceSnapshotFile {
                path: PathBuf::from("linked/file.txt"),
                bytes: b"must stay inside".to_vec(),
            }],
        };
        assert!(snapshot.restore(root.path(), false).is_err());
        assert!(!outside.path().join("file.txt").exists());
    }
}
