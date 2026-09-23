//! Local Git worktree inventory and resource admission. Engine caches belong to adapters.
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const GIB: u64 = 1024 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceBudgets {
    pub disk_reserve_bytes: u64,
    pub ram_reserve_bytes: u64,
    pub agent_disk_limit_bytes: u64,
    pub agent_ram_limit_bytes: u64,
}

impl Default for WorkspaceBudgets {
    fn default() -> Self {
        Self {
            disk_reserve_bytes: 60 * GIB,
            ram_reserve_bytes: 8 * GIB,
            agent_disk_limit_bytes: 32 * GIB,
            agent_ram_limit_bytes: 16 * GIB,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceAdmission {
    pub admitted: bool,
    pub reason: Option<String>,
    pub disk_available_bytes: u64,
    pub ram_available_bytes: u64,
}

/// Lane callers use the same decision as worktree creation; a refusal is a queue reason,
/// never a request to delete an agent's files.
pub fn assess_admission(
    budgets: &WorkspaceBudgets,
    disk_available: u64,
    ram_available: u64,
    agent_disk_used: u64,
    agent_ram_used: u64,
    requested_disk: u64,
    requested_ram: u64,
) -> WorkspaceAdmission {
    let reasons = [
        (
            disk_available.saturating_sub(requested_disk) < budgets.disk_reserve_bytes,
            format!(
                "disk reserve: {} available, {} requested, {} reserved",
                disk_available, requested_disk, budgets.disk_reserve_bytes
            ),
        ),
        (
            ram_available.saturating_sub(requested_ram) < budgets.ram_reserve_bytes,
            format!(
                "RAM reserve: {} available, {} requested, {} reserved",
                ram_available, requested_ram, budgets.ram_reserve_bytes
            ),
        ),
        (
            agent_disk_used.saturating_add(requested_disk) > budgets.agent_disk_limit_bytes,
            format!(
                "agent disk cap: {} used + {} requested > {}",
                agent_disk_used, requested_disk, budgets.agent_disk_limit_bytes
            ),
        ),
        (
            agent_ram_used.saturating_add(requested_ram) > budgets.agent_ram_limit_bytes,
            format!(
                "agent RAM cap: {} used + {} requested > {}",
                agent_ram_used, requested_ram, budgets.agent_ram_limit_bytes
            ),
        ),
    ];
    WorkspaceAdmission {
        admitted: !reasons.iter().any(|(blocked, _)| *blocked),
        reason: reasons
            .into_iter()
            .find_map(|(blocked, reason)| blocked.then_some(reason)),
        disk_available_bytes: disk_available,
        ram_available_bytes: ram_available,
    }
}

#[cfg(unix)]
pub fn disk_available(path: &Path) -> Result<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let path = CString::new(path.as_os_str().as_bytes())?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: path is NUL terminated, stat points to a valid writable allocation.
    ensure!(
        unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } == 0,
        "statvfs failed: {}",
        std::io::Error::last_os_error()
    );
    // SAFETY: statvfs succeeded and initialized the allocation.
    let stat = unsafe { stat.assume_init() };
    Ok(stat.f_bavail.saturating_mul(stat.f_frsize))
}

#[cfg(not(unix))]
pub fn disk_available(_path: &Path) -> Result<u64> {
    anyhow::bail!("disk availability is not implemented for this platform")
}

pub fn ram_available() -> Result<u64> {
    let meminfo = fs::read_to_string("/proc/meminfo").context("read Linux memory availability")?;
    let kb = meminfo
        .lines()
        .find_map(|line| {
            line.strip_prefix("MemAvailable:")
                .and_then(|line| line.split_whitespace().next())
                .and_then(|value| value.parse::<u64>().ok())
        })
        .context("MemAvailable not present in /proc/meminfo")?;
    Ok(kb.saturating_mul(1024))
}

fn git(repo: &Path, args: &[&str]) -> Result<std::process::Output> {
    Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .with_context(|| format!("run git in {}", repo.display()))
}
fn git_text(repo: &Path, args: &[&str]) -> Result<String> {
    let output = git(repo, args)?;
    ensure!(
        output.status.success(),
        "git {}: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorktreeRecord {
    pub path: PathBuf,
    pub branch: Option<String>,
    pub owner: Option<Uuid>,
    pub owner_live: bool,
    pub owner_gone: bool,
    pub merged: bool,
    pub abandoned: bool,
    pub dirty: bool,
    pub last_activity_unix: Option<u64>,
    pub size_bytes: u64,
    pub gc_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Owner {
    session: Uuid,
}

fn git_dir(tree: &Path) -> Result<PathBuf> {
    Ok(PathBuf::from(git_text(
        tree,
        &["rev-parse", "--absolute-git-dir"],
    )?))
}

fn tree_owner(tree: &Path) -> Option<Uuid> {
    fs::read(git_dir(tree).ok()?.join("borg-worktree-owner.json"))
        .ok()
        .and_then(|data| serde_json::from_slice::<Owner>(&data).ok())
        .map(|owner| owner.session)
}

/// `active` is an authoritative list of local session owners, obtained from the
/// workspace journal. Unknown owners are not interpreted as dead.
pub fn inventory(repo: &Path, active: &[(Uuid, PathBuf)]) -> Result<Vec<WorktreeRecord>> {
    let output = git_text(repo, &["worktree", "list", "--porcelain"])?;
    let primary = PathBuf::from(git_text(
        repo,
        &["rev-parse", "--path-format=absolute", "--git-common-dir"],
    )?);
    let primary_repo = primary
        .parent()
        .context("Git common directory has no parent")?;
    let base_branch = git_text(
        primary_repo,
        &["symbolic-ref", "--quiet", "--short", "HEAD"],
    )
    .unwrap_or_else(|_| "main".into());
    let base_ref = format!("refs/heads/{base_branch}");
    let mut trees = Vec::new();
    for block in output.split("\n\n") {
        let Some(path) = block
            .lines()
            .find_map(|line| line.strip_prefix("worktree "))
        else {
            continue;
        };
        let path = PathBuf::from(path);
        if !path.is_dir() {
            continue;
        }
        let branch = block
            .lines()
            .find_map(|line| line.strip_prefix("branch refs/heads/").map(str::to_owned));
        let owner = tree_owner(&path).or_else(|| {
            active
                .iter()
                .find(|(_, cwd)| cwd == &path)
                .map(|(id, _)| *id)
        });
        let owner_live = active
            .iter()
            .any(|(id, cwd)| Some(*id) == owner || cwd == &path);
        let status = git(
            &path,
            &["status", "--porcelain=v1", "--untracked-files=normal"],
        )?;
        let dirty = !status.status.success() || !status.stdout.is_empty();
        let merged = branch
            .as_deref()
            .is_some_and(|branch| branch != base_branch)
            && git(&path, &["merge-base", "--is-ancestor", "HEAD", &base_ref])
                .is_ok_and(|out| out.status.success());
        let is_primary = git_dir(&path).is_ok_and(|dir| dir == primary);
        let owner_gone = false; // only the caller with journal exit evidence can set this
        let size_bytes = Command::new("du")
            .args(["-skx", "--"])
            .arg(&path)
            .output()
            .ok()
            .filter(|out| out.status.success())
            .and_then(|out| String::from_utf8(out.stdout).ok())
            .and_then(|text| text.split_whitespace().next()?.parse::<u64>().ok())
            .unwrap_or(0)
            .saturating_mul(1024);
        let last_activity_unix = fs::metadata(&path)
            .ok()
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs());
        // Thirty days without a worktree/index update or branch commit is a
        // *proposal*, never owner-exit proof. Git branches remain after removal.
        let index_activity = git_dir(&path)
            .ok()
            .and_then(|dir| fs::metadata(dir.join("index")).ok())
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs());
        let commit_activity = git_text(&path, &["log", "-1", "--format=%ct"])
            .ok()
            .and_then(|value| value.parse::<u64>().ok());
        let last_activity_unix = [last_activity_unix, index_activity, commit_activity]
            .into_iter()
            .flatten()
            .max();
        let abandoned = !is_primary
            && !merged
            && last_activity_unix.is_some_and(|seen| {
                now_unix().is_ok_and(|now| now.saturating_sub(seen) >= 30 * 86_400)
            });
        let gc_reason =
            if !is_primary && (merged || abandoned) && !dirty && !owner_live && owner.is_some() {
                Some(
                    if merged {
                        "merged, clean"
                    } else {
                        "abandoned 30d, clean; branch retained"
                    }
                    .to_string()
                        + "; verify owner exit before removal",
                )
            } else {
                None
            };
        trees.push(WorktreeRecord {
            path,
            branch,
            owner,
            owner_live,
            owner_gone,
            merged,
            abandoned,
            dirty,
            last_activity_unix,
            size_bytes,
            gc_reason,
        });
    }
    Ok(trees)
}

pub fn create_worktree(
    repo: &Path,
    root: &Path,
    task: &str,
    owner: Uuid,
    shared_cargo: bool,
    budgets: &WorkspaceBudgets,
) -> Result<WorktreeRecord> {
    ensure!(
        !task.is_empty()
            && task.len() <= 64
            && task
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
        "task must be 1-64 ASCII letters, digits, '-' or '_'"
    );
    fs::create_dir_all(root)?;
    let agent_disk_used = inventory(repo, &[])?
        .into_iter()
        .filter(|tree| tree.owner == Some(owner))
        .fold(0u64, |total, tree| total.saturating_add(tree.size_bytes));
    let admission = assess_admission(
        budgets,
        disk_available(root)?,
        ram_available()?,
        agent_disk_used,
        0,
        GIB,
        0,
    );
    ensure!(
        admission.admitted,
        "worktree queued: {}",
        admission.reason.unwrap_or_default()
    );
    let path = root.join(format!("{task}-{}", &owner.to_string()[..8]));
    ensure!(
        !path.exists(),
        "worktree path already exists: {}",
        path.display()
    );
    let branch = format!("agent/{task}-{}", &owner.to_string()[..8]);
    let result = git(
        repo,
        &[
            "worktree",
            "add",
            "-b",
            &branch,
            path.to_str().context("worktree path not UTF-8")?,
            "HEAD",
        ],
    )?;
    ensure!(
        result.status.success(),
        "git worktree add: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    fs::write(
        git_dir(&path)?.join("borg-worktree-owner.json"),
        serde_json::to_vec(&Owner { session: owner })?,
    )?;
    if shared_cargo {
        // Cargo already shares ~/.cargo registry; keep build products per worktree.
        let cargo_dir = path.join(".cargo");
        fs::create_dir_all(&cargo_dir)?;
        let config = cargo_dir.join("config.toml");
        if !config.exists() {
            fs::write(
                config,
                "# Registry/download cache uses the user's shared CARGO_HOME.\n[build]\ntarget-dir = \"target\"\n",
            )?;
        }
    }
    inventory(repo, &[(owner, path.clone())])?
        .into_iter()
        .find(|tree| tree.path == path)
        .context("new worktree not in Git inventory")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetUsage {
    pub tree: PathBuf,
    pub target: PathBuf,
    pub bytes: u64,
    pub cap_bytes: u64,
    pub over_cap: bool,
    pub owner: Option<Uuid>,
    pub owner_live: bool,
}

/// Read-only per-tree build-output report. A lane may queue a new build when
/// this cap is exceeded; cleaning is a separate owner-approved job.
pub fn target_usage(trees: &[WorktreeRecord], cap_bytes: u64) -> Result<Vec<TargetUsage>> {
    ensure!(cap_bytes > 0, "target cap must be positive");
    Ok(trees
        .iter()
        .map(|tree| {
            let target = tree.path.join("target");
            let bytes = if target.is_dir() {
                Command::new("du")
                    .args(["-skx", "--"])
                    .arg(&target)
                    .output()
                    .ok()
                    .filter(|out| out.status.success())
                    .and_then(|out| String::from_utf8(out.stdout).ok())
                    .and_then(|text| text.split_whitespace().next()?.parse::<u64>().ok())
                    .unwrap_or(0)
                    .saturating_mul(1024)
            } else {
                0
            };
            TargetUsage {
                tree: tree.path.clone(),
                target,
                bytes,
                cap_bytes,
                over_cap: bytes > cap_bytes,
                owner: tree.owner,
                owner_live: tree.owner_live,
            }
        })
        .collect())
}

/// Only an explicit, journal-confirmed exited owner allows removal. Dirty trees
/// require force; force does not bypass the live-session guard.
pub fn gc(
    repo: &Path,
    tree: &WorktreeRecord,
    exited_owners: &[Uuid],
    apply: bool,
    force: bool,
) -> Result<bool> {
    ensure!(
        tree.gc_reason.is_some() || (force && (tree.merged || tree.abandoned) && !tree.owner_live),
        "not eligible for GC"
    );
    ensure!(!tree.owner_live, "live session owns worktree");
    ensure!(
        tree.owner.is_some_and(|id| exited_owners.contains(&id)),
        "owner exit not confirmed by journal"
    );
    ensure!(!tree.dirty || force, "dirty worktree requires --force");
    if !apply {
        return Ok(false);
    }
    // Recheck at the point of use; Git also refuses removal of dirty trees without force.
    let fresh = inventory(repo, &[])?
        .into_iter()
        .find(|candidate| candidate.path == tree.path)
        .context("worktree disappeared before GC")?;
    ensure!(!fresh.dirty || force, "worktree became dirty");
    ensure!(
        (fresh.merged || fresh.abandoned) && fresh.owner == tree.owner,
        "worktree changed since proposal"
    );
    let output = git(
        repo,
        &[
            "worktree",
            "remove",
            if force { "--force" } else { "--" },
            tree.path.to_str().context("path not UTF-8")?,
        ],
    )?;
    ensure!(
        output.status.success(),
        "git worktree remove: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(true)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirtyOwner {
    pub path: PathBuf,
    pub session: Option<Uuid>,
    pub files: Vec<String>,
}

/// Snapshot only: does not pause or mutate any agent. Globs use `*`, `**`, `?`.
pub fn freeze_preview(
    repo: &Path,
    globs: &[String],
    active: &[(Uuid, PathBuf)],
) -> Result<Vec<DirtyOwner>> {
    ensure!(!globs.is_empty(), "at least one path glob required");
    let expressions = globs
        .iter()
        .map(|glob| {
            let mut pattern = String::from("^");
            let mut chars = glob.chars().peekable();
            while let Some(c) = chars.next() {
                match c {
                    '*' if chars.peek() == Some(&'*') => {
                        chars.next();
                        pattern.push_str(".*");
                    }
                    '*' => pattern.push_str("[^/]*"),
                    '?' => pattern.push_str("[^/]"),
                    _ => pattern.push_str(&regex::escape(&c.to_string())),
                }
            }
            pattern.push('$');
            regex::Regex::new(&pattern)
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut owners = Vec::new();
    for tree in inventory(repo, active)? {
        let mut files = Vec::new();
        for args in [
            ["diff", "--name-only", "-z", "HEAD"].as_slice(),
            ["ls-files", "--others", "--exclude-standard", "-z"].as_slice(),
        ] {
            let out = git(&tree.path, args)?;
            if !out.status.success() {
                continue;
            }
            files.extend(
                out.stdout
                    .split(|b| *b == 0)
                    .filter(|name| !name.is_empty())
                    .filter_map(|name| String::from_utf8(name.to_vec()).ok())
                    .filter(|name| expressions.iter().any(|expr| expr.is_match(name))),
            );
        }
        files.sort();
        files.dedup();
        if !files.is_empty() {
            owners.push(DirtyOwner {
                path: tree.path,
                session: tree.owner,
                files,
            });
        }
    }
    Ok(owners)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn budget_refusal_is_actionable_and_saturates() {
        let b = WorkspaceBudgets::default();
        let d = assess_admission(&b, 50 * GIB, 9 * GIB, 0, 0, 2 * GIB, 0);
        assert!(!d.admitted);
        assert!(d.reason.unwrap().contains("disk reserve"));
        assert!(!assess_admission(&b, 100 * GIB, 10 * GIB, 31 * GIB, 0, 2 * GIB, 0).admitted);
    }
}

/// A local handshake linked to the durable shared-work claim. This is not a
/// filesystem lock on other editors; adapters must acquire their project lane
/// before landing a change that conflicts with jobs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreezeState {
    pub id: Uuid,
    pub work_id: Uuid,
    pub owner: Uuid,
    pub globs: Vec<String>,
    pub reason: String,
    pub deadline_unix: u64,
    pub required_acks: Vec<Uuid>,
    pub acknowledgements: Vec<Uuid>,
    pub dirty_owners: Vec<DirtyOwner>,
    pub status: FreezeStatus,
    pub moved_note: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FreezeStatus {
    Requested,
    Landed,
    Released,
    Aborted,
}

fn now_unix() -> Result<u64> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs())
}
fn freeze_paths(repo: &Path) -> Result<(PathBuf, PathBuf)> {
    let dir = PathBuf::from(git_text(
        repo,
        &["rev-parse", "--path-format=absolute", "--git-common-dir"],
    )?);
    Ok((dir.join("borg-freeze.json"), dir.join("borg-freeze.lock")))
}

#[cfg(unix)]
fn freeze_locked<T>(repo: &Path, action: impl FnOnce(&Path) -> Result<T>) -> Result<T> {
    use std::os::fd::AsRawFd;
    let (state, lock) = freeze_paths(repo)?;
    let file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock)?;
    // SAFETY: the fd belongs to `file` and stays open until the critical section ends.
    ensure!(
        unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } == 0,
        "freeze lock: {}",
        std::io::Error::last_os_error()
    );
    action(&state)
}

#[cfg(not(unix))]
fn freeze_locked<T>(_repo: &Path, _action: impl FnOnce(&Path) -> Result<T>) -> Result<T> {
    anyhow::bail!("freeze locking is not implemented for this platform")
}

fn save_freeze(path: &Path, state: &FreezeState) -> Result<()> {
    let tmp = path.with_extension(format!("{}.tmp", Uuid::new_v4()));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)?;
    use std::io::Write;
    file.write_all(&serde_json::to_vec_pretty(state)?)?;
    file.sync_all()?;
    fs::rename(tmp, path)?;
    fs::File::open(path.parent().context("freeze path has no parent")?)?.sync_all()?;
    Ok(())
}

pub fn freeze_status(repo: &Path) -> Result<Option<FreezeState>> {
    freeze_locked(repo, |path| {
        if !path.exists() {
            return Ok(None);
        }
        Ok(Some(serde_json::from_slice(&fs::read(path)?)?))
    })
}

pub fn request_freeze(
    repo: &Path,
    work_id: Uuid,
    owner: Uuid,
    globs: Vec<String>,
    reason: String,
    deadline_secs: u64,
    active: &[(Uuid, PathBuf)],
) -> Result<FreezeState> {
    ensure!(
        !reason.trim().is_empty() && deadline_secs > 0 && deadline_secs <= 86_400,
        "reason and deadline (1..86400 seconds) required"
    );
    freeze_locked(repo, |path| {
        if path.exists() {
            let old: FreezeState = serde_json::from_slice(&fs::read(path)?)?;
            ensure!(
                matches!(old.status, FreezeStatus::Released | FreezeStatus::Aborted),
                "freeze {} still {:?}; release or abort it first",
                old.id,
                old.status
            );
        }
        let dirty_owners = freeze_preview(repo, &globs, active)?;
        let worktree_paths = inventory(repo, active)?
            .into_iter()
            .map(|tree| tree.path)
            .collect::<Vec<_>>();
        let mut required_acks = active
            .iter()
            .filter(|(_, cwd)| {
                worktree_paths
                    .iter()
                    .any(|path| cwd == path || cwd.starts_with(path))
            })
            .map(|(id, _)| *id)
            .filter(|id| id != &owner)
            .collect::<Vec<_>>();
        required_acks.sort();
        required_acks.dedup();
        let state = FreezeState {
            id: Uuid::new_v4(),
            work_id,
            owner,
            globs,
            reason,
            deadline_unix: now_unix()?.saturating_add(deadline_secs),
            required_acks,
            acknowledgements: vec![],
            dirty_owners,
            status: FreezeStatus::Requested,
            moved_note: None,
        };
        save_freeze(path, &state)?;
        Ok(state)
    })
}

fn update_freeze(
    repo: &Path,
    id: Uuid,
    action: impl FnOnce(&mut FreezeState) -> Result<()>,
) -> Result<FreezeState> {
    freeze_locked(repo, |path| {
        let mut state: FreezeState =
            serde_json::from_slice(&fs::read(path).context("no active freeze")?)?;
        ensure!(
            state.id == id,
            "freeze ID does not match current project freeze"
        );
        action(&mut state)?;
        save_freeze(path, &state)?;
        Ok(state)
    })
}

pub fn acknowledge_freeze(repo: &Path, id: Uuid, participant: Uuid) -> Result<FreezeState> {
    update_freeze(repo, id, |state| {
        ensure!(
            state.status == FreezeStatus::Requested && now_unix()? <= state.deadline_unix,
            "freeze is not pending or timed out; owner must abort/re-request"
        );
        ensure!(
            state.required_acks.contains(&participant),
            "participant not asked to acknowledge"
        );
        if !state.acknowledgements.contains(&participant) {
            state.acknowledgements.push(participant);
        }
        Ok(())
    })
}

pub fn land_freeze(
    repo: &Path,
    id: Uuid,
    owner: Uuid,
    note: String,
    active: &[(Uuid, PathBuf)],
) -> Result<FreezeState> {
    update_freeze(repo, id, |state| {
        ensure!(
            state.owner == owner && state.status == FreezeStatus::Requested,
            "only the requester can land a requested freeze"
        );
        ensure!(
            now_unix()? <= state.deadline_unix,
            "freeze timed out; abort/re-request"
        );
        ensure!(
            state
                .required_acks
                .iter()
                .all(|id| state.acknowledgements.contains(id)),
            "not all relevant sessions acknowledged the freeze"
        );
        ensure!(
            !note.trim().is_empty(),
            "a 'where did X move' note is required"
        );
        let dirty = freeze_preview(repo, &state.globs, active)?;
        ensure!(
            dirty.is_empty(),
            "dirty protected paths remain; don't overwrite or rebase an owner"
        );
        state.dirty_owners = dirty;
        state.status = FreezeStatus::Landed;
        state.moved_note = Some(note);
        Ok(())
    })
}

pub fn finish_freeze(repo: &Path, id: Uuid, owner: Uuid, abort: bool) -> Result<FreezeState> {
    update_freeze(repo, id, |state| {
        ensure!(state.owner == owner, "only the requester can end a freeze");
        ensure!(
            abort || state.status == FreezeStatus::Landed,
            "land before release or abort explicitly"
        );
        state.status = if abort {
            FreezeStatus::Aborted
        } else {
            FreezeStatus::Released
        };
        Ok(())
    })
}

#[cfg(test)]
mod safety_tests {
    use super::*;

    #[test]
    fn gc_never_removes_live_or_dirty_without_force() {
        let id = Uuid::new_v4();
        let tree = WorktreeRecord {
            path: PathBuf::from("/nonexistent"),
            branch: Some("topic".into()),
            owner: Some(id),
            owner_live: true,
            owner_gone: false,
            merged: true,
            abandoned: false,
            dirty: false,
            last_activity_unix: None,
            size_bytes: 0,
            gc_reason: None,
        };
        assert!(gc(Path::new("/nonexistent"), &tree, &[id], true, true).is_err());
        let tree = WorktreeRecord {
            owner_live: false,
            dirty: true,
            gc_reason: Some("candidate".into()),
            ..tree
        };
        assert!(gc(Path::new("/nonexistent"), &tree, &[id], true, false).is_err());
        assert!(gc(Path::new("/nonexistent"), &tree, &[], true, true).is_err());
    }

    #[test]
    fn freeze_requires_acks_and_clean_protected_paths_before_land() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let repo = temp.path();
        let status = Command::new("git")
            .args(["init", "-b", "main"])
            .arg(repo)
            .status()?;
        ensure!(status.success(), "git init failed");
        let owner = Uuid::new_v4();
        let peer = Uuid::new_v4();
        let active = vec![(peer, repo.to_owned())];
        let freeze = request_freeze(
            repo,
            Uuid::new_v4(),
            owner,
            vec!["src/**".into()],
            "refactor header".into(),
            120,
            &active,
        )?;
        assert!(freeze.required_acks.contains(&peer));
        assert!(land_freeze(repo, freeze.id, owner, "moved to src/new".into(), &active).is_err());
        acknowledge_freeze(repo, freeze.id, peer)?;
        fs::create_dir(repo.join("src"))?;
        fs::write(repo.join("src/x.rs"), "private edits")?;
        assert!(land_freeze(repo, freeze.id, owner, "moved to src/new".into(), &active).is_err());
        fs::remove_file(repo.join("src/x.rs"))?;
        let landed = land_freeze(repo, freeze.id, owner, "moved to src/new".into(), &active)?;
        assert_eq!(landed.status, FreezeStatus::Landed);
        assert_eq!(
            finish_freeze(repo, freeze.id, owner, false)?.status,
            FreezeStatus::Released
        );
        Ok(())
    }
}
