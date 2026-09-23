use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use ts_rs::TS;

const MAX_BASELINE_BYTES: usize = 4 * 1024 * 1024;
const MAX_FILE_BYTES: usize = 1024 * 1024;
const MAX_CHANGE_PATHS: usize = 64;
const MAX_PATCH_PATHS: usize = 8;
const MAX_PATCH_BYTES: usize = 16 * 1024;
const MAX_TOTAL_PATCH_BYTES: usize = 32 * 1024;
const MAX_GIT_OUTPUT_BYTES: usize = 2 * 1024 * 1024;
const GIT_PROBE_TIMEOUT: Duration = Duration::from_millis(400);
const CHANGE_CAPTURE_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct CommandChange {
    pub path: String,
    pub added: usize,
    pub removed: usize,
    pub diff: String,
}

/// A bounded baseline for the worktree that a shell process actually runs in.
/// Only already-dirty text files need their bytes copied: Git can diff a path
/// that was clean at launch against its original HEAD after the process exits.
pub(crate) struct CommandChangeBaseline {
    root: PathBuf,
    head: Option<String>,
    preexisting: BTreeSet<String>,
    dirty: BTreeMap<String, Option<Vec<u8>>>,
}

impl CommandChangeBaseline {
    pub(crate) fn capture(cwd: &Path) -> Option<Self> {
        let root = git_output(cwd, &["rev-parse", "--show-toplevel"])?;
        let root = PathBuf::from(String::from_utf8(root.stdout).ok()?.trim());
        let head = git_head(&root);
        let mut dirty = BTreeMap::new();
        let mut total_bytes = 0;
        let status = git_status(&root)?;
        let tracked = status.iter().filter(|(_, code)| code.as_str() != "??");
        let untracked = status.iter().filter(|(_, code)| code.as_str() == "??");
        for (path, _) in tracked.chain(untracked) {
            if dirty.len() == MAX_CHANGE_PATHS {
                break;
            }
            let Some(contents) = text_contents(&root.join(path)) else {
                continue;
            };
            let bytes = contents.as_ref().map_or(0, Vec::len);
            if total_bytes + bytes > MAX_BASELINE_BYTES {
                continue;
            }
            total_bytes += bytes;
            dirty.insert(path.clone(), contents);
        }
        let preexisting = status.keys().cloned().collect();
        if git_head(&root) != head {
            return None;
        }
        Some(Self {
            root,
            head,
            preexisting,
            dirty,
        })
    }

    pub(crate) fn finish(self) -> Vec<CommandChange> {
        let started = Instant::now();
        let end_head = git_head(&self.root);
        let Some(after) = git_status(&self.root) else {
            return Vec::new();
        };
        let mut paths = self.dirty.keys().cloned().collect::<Vec<_>>();
        paths.extend(
            after
                .iter()
                .filter(|(path, code)| !self.preexisting.contains(*path) && code.as_str() != "??")
                .map(|(path, _)| path.clone()),
        );
        for path in committed_paths(
            &self.root,
            self.head.as_deref(),
            end_head.as_deref(),
            &self.preexisting,
        ) {
            if !paths.contains(&path) {
                paths.push(path);
            }
        }
        paths.extend(
            after
                .iter()
                .filter(|(path, code)| !self.preexisting.contains(*path) && code.as_str() == "??")
                .map(|(path, _)| path.clone()),
        );
        let mut changes = Vec::new();
        let mut patch_bytes = 0;
        for path in paths {
            if changes.len() == MAX_CHANGE_PATHS || started.elapsed() >= CHANGE_CAPTURE_TIMEOUT {
                break;
            }
            let changed = if let Some(before) = self.dirty.get(&path) {
                let Some(now) = text_contents(&self.root.join(&path)) else {
                    continue;
                };
                if before == &now {
                    continue;
                }
                Some((before.as_deref(), now))
            } else {
                (after.contains_key(&path) || self.head != end_head).then_some((None, None))
            };
            let Some((before, now)) = changed else {
                continue;
            };
            let mut patch = if changes.len() >= MAX_PATCH_PATHS {
                String::new()
            } else if self.dirty.contains_key(&path) {
                diff_contents(&self.root, &path, before, now.as_deref()).unwrap_or_default()
            } else if after.get(&path).is_some_and(|status| status == "??") {
                let Some(contents) = text_contents(&self.root.join(&path)) else {
                    continue;
                };
                diff_contents(&self.root, &path, None, contents.as_deref()).unwrap_or_default()
            } else if let Some(head) = self.head.as_deref() {
                git_output_path(&self.root, &["diff", "--no-ext-diff", head, "--"], &path)
                    .map(|output| String::from_utf8_lossy(&output.stdout).into_owned())
                    .unwrap_or_default()
            } else if end_head.is_some() {
                let Some(contents) = text_contents(&self.root.join(&path)) else {
                    continue;
                };
                diff_contents(&self.root, &path, None, contents.as_deref()).unwrap_or_default()
            } else {
                String::new()
            };
            if patch.is_empty() && changes.len() < MAX_PATCH_PATHS {
                patch =
                    format!("Diff unavailable for {path}: file status changed during command.\n");
            }
            let (added, removed) = patch_line_counts(&patch);
            let diff = truncate_patch(
                patch,
                MAX_PATCH_BYTES.min(MAX_TOTAL_PATCH_BYTES.saturating_sub(patch_bytes)),
            );
            let diff = crate::secret_scrub::scrub_secrets(&diff).into_owned();
            patch_bytes += diff.len();
            changes.push(CommandChange {
                path,
                added,
                removed,
                diff,
            });
        }
        if git_head(&self.root) == end_head {
            changes
        } else {
            Vec::new()
        }
    }
}

fn git_head(root: &Path) -> Option<String> {
    git_output(root, &["rev-parse", "--verify", "HEAD"])
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|head| head.trim().to_string())
}

fn committed_paths(
    root: &Path,
    before: Option<&str>,
    after: Option<&str>,
    preexisting: &BTreeSet<String>,
) -> Vec<String> {
    let Some(after) = after.filter(|after| Some(*after) != before) else {
        return Vec::new();
    };
    let output = if let Some(before) = before {
        git_output(
            root,
            &[
                "diff",
                "--name-only",
                "-z",
                "--no-renames",
                before,
                after,
                "--",
            ],
        )
    } else {
        git_output(
            root,
            &[
                "diff-tree",
                "--root",
                "--no-commit-id",
                "--name-only",
                "-r",
                "-z",
                "--no-renames",
                after,
            ],
        )
    };
    let Some(output) = output else {
        return Vec::new();
    };
    output
        .stdout
        .split(|byte| *byte == 0)
        .map(str::from_utf8)
        .filter_map(Result::ok)
        .filter(|path| !path.is_empty() && !preexisting.contains(*path))
        .take(MAX_CHANGE_PATHS)
        .map(str::to_string)
        .collect()
}

fn git_output(root: &Path, args: &[&str]) -> Option<Output> {
    let output = command_output(
        Command::new("git")
            .arg("--no-optional-locks")
            .arg("-C")
            .arg(root)
            .args(args),
    )?;
    output.status.success().then_some(output)
}

fn git_output_path(root: &Path, args: &[&str], path: &str) -> Option<Output> {
    let output = command_output(
        Command::new("git")
            .arg("--no-optional-locks")
            .arg("-C")
            .arg(root)
            .args(args)
            .arg(path),
    )?;
    output.status.success().then_some(output)
}

fn git_status(root: &Path) -> Option<BTreeMap<String, String>> {
    let output = git_output(
        root,
        &["status", "--porcelain=v1", "-z", "--untracked-files=all"],
    )?;
    let mut records = output.stdout.split(|byte| *byte == 0);
    let mut paths = BTreeMap::new();
    while let Some(record) = records.next() {
        if record.len() < 4 || record[2] != b' ' {
            continue;
        }
        let status = std::str::from_utf8(&record[..2]).ok()?;
        let path = std::str::from_utf8(&record[3..]).ok()?.to_string();
        if status.contains('R') || status.contains('C') {
            // Porcelain -z follows a rename's destination with its source.
            let _ = records.next();
        }
        paths.insert(path, status.to_string());
    }
    Some(paths)
}

/// `None` means the path cannot be safely diffed. `Some(None)` means missing.
fn text_contents(path: &Path) -> Option<Option<Vec<u8>>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Some(None),
        Err(_) => return None,
    };
    if !metadata.file_type().is_file() || metadata.len() > MAX_FILE_BYTES as u64 {
        return None;
    }
    let bytes = fs::read(path).ok()?;
    (bytes.len() <= MAX_FILE_BYTES && !bytes.contains(&0)).then_some(Some(bytes))
}

fn diff_contents(
    root: &Path,
    path: &str,
    before: Option<&[u8]>,
    after: Option<&[u8]>,
) -> Option<String> {
    if before.is_none() && after.is_none() {
        return None;
    }
    let mut old_file = tempfile::NamedTempFile::new().ok()?;
    if let Some(bytes) = before {
        old_file.write_all(bytes).ok()?;
        old_file.flush().ok()?;
    }
    let new_file = if after.is_none() {
        Some(tempfile::NamedTempFile::new().ok()?)
    } else {
        None
    };
    let new_path = root.join(path);
    let new = new_file
        .as_ref()
        .map_or(new_path.as_path(), |file| file.path());
    let output = command_output(
        Command::new("git")
            .arg("--no-optional-locks")
            .arg("diff")
            .arg("--no-index")
            .arg("--no-ext-diff")
            .arg("--")
            .arg(old_file.path())
            .arg(new),
    )?;
    if !matches!(output.status.code(), Some(0 | 1)) {
        return None;
    }
    let raw = String::from_utf8_lossy(&output.stdout);
    let mut patch = String::new();
    for line in raw.lines() {
        if line.starts_with("diff --git ") {
            patch.push_str(&format!("diff --git a/{path} b/{path}"));
        } else if line.starts_with("--- ") {
            patch.push_str(&format!(
                "--- {}",
                before.map_or_else(|| "/dev/null".to_string(), |_| format!("a/{path}"))
            ));
        } else if line.starts_with("+++ ") {
            patch.push_str(&format!(
                "+++ {}",
                after.map_or_else(|| "/dev/null".to_string(), |_| format!("b/{path}"))
            ));
        } else {
            patch.push_str(line);
        }
        patch.push('\n');
    }
    if patch.is_empty() {
        // Two empty temporary files are byte-identical, but creating or
        // deleting an empty file still changes the workspace.
        patch = format!(
            "diff --git a/{path} b/{path}\n--- {}\n+++ {}\n",
            before.map_or_else(|| "/dev/null".to_string(), |_| format!("a/{path}")),
            after.map_or_else(|| "/dev/null".to_string(), |_| format!("b/{path}")),
        );
    }
    Some(patch)
}

fn command_output(command: &mut Command) -> Option<Output> {
    let mut stdout = tempfile::tempfile().ok()?;
    let mut child = command
        .stdout(Stdio::from(stdout.try_clone().ok()?))
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let started = Instant::now();
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if started.elapsed() < GIT_PROBE_TIMEOUT => {
                std::thread::sleep(Duration::from_millis(5));
            }
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
        }
    };
    stdout.seek(SeekFrom::Start(0)).ok()?;
    let mut bytes = Vec::new();
    stdout
        .take((MAX_GIT_OUTPUT_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .ok()?;
    (bytes.len() <= MAX_GIT_OUTPUT_BYTES).then_some(Output {
        status,
        stdout: bytes,
        stderr: Vec::new(),
    })
}

fn patch_line_counts(patch: &str) -> (usize, usize) {
    let added = patch
        .lines()
        .filter(|line| line.starts_with('+') && !line.starts_with("+++"))
        .count();
    let removed = patch
        .lines()
        .filter(|line| line.starts_with('-') && !line.starts_with("---"))
        .count();
    (added, removed)
}

fn truncate_patch(mut patch: String, limit: usize) -> String {
    const MARKER: &str = "\n... diff truncated ...\n";
    if patch.len() > limit {
        if limit <= MARKER.len() {
            return String::new();
        }
        let mut end = limit - MARKER.len();
        while !patch.is_char_boundary(end) {
            end -= 1;
        }
        patch.truncate(end);
        patch.push_str(MARKER);
    }
    patch
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Git sees real temporary files on both sides; `/dev/null` is only a
    /// displayed diff label, so add/delete capture also works on Windows.
    #[test]
    fn add_and_delete_diffs_do_not_require_a_platform_null_device() {
        let root = tempfile::tempdir().expect("workspace");
        fs::write(root.path().join("new.rs"), "added\n").expect("new file");
        let added =
            diff_contents(root.path(), "new.rs", None, Some(b"added\n")).expect("addition diff");
        assert!(added.contains("--- /dev/null"), "{added}");
        assert!(added.contains("+++ b/new.rs"), "{added}");
        assert!(added.contains("+added"), "{added}");

        let removed =
            diff_contents(root.path(), "old.rs", Some(b"removed\n"), None).expect("deletion diff");
        assert!(removed.contains("--- a/old.rs"), "{removed}");
        assert!(removed.contains("+++ /dev/null"), "{removed}");
        assert!(removed.contains("-removed"), "{removed}");

        fs::write(root.path().join("empty.rs"), b"").expect("empty file");
        let empty =
            diff_contents(root.path(), "empty.rs", None, Some(b"")).expect("empty addition diff");
        assert!(empty.contains("--- /dev/null"), "{empty}");
        assert!(empty.contains("+++ b/empty.rs"), "{empty}");
    }
}
