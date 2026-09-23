//! Local Git worktree inventory and resource admission. Engine caches belong to adapters.
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail, ensure};
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
        (disk_available.saturating_sub(requested_disk) < budgets.disk_reserve_bytes,
         format!("disk reserve: {} available, {} requested, {} reserved", disk_available, requested_disk, budgets.disk_reserve_bytes)),
        (ram_available.saturating_sub(requested_ram) < budgets.ram_reserve_bytes,
         format!("RAM reserve: {} available, {} requested, {} reserved", ram_available, requested_ram, budgets.ram_reserve_bytes)),
        (agent_disk_used.saturating_add(requested_disk) > budgets.agent_disk_limit_bytes,
         format!("agent disk cap: {} used + {} requested > {}", agent_disk_used, requested_disk, budgets.agent_disk_limit_bytes)),
        (agent_ram_used.saturating_add(requested_ram) > budgets.agent_ram_limit_bytes,
         format!("agent RAM cap: {} used + {} requested > {}", agent_ram_used, requested_ram, budgets.agent_ram_limit_bytes)),
    ];
    WorkspaceAdmission {
        admitted: !reasons.iter().any(|(blocked, _)| *blocked),
        reason: reasons.into_iter().find_map(|(blocked, reason)| blocked.then_some(reason)),
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
    ensure!(unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } == 0,
        "statvfs failed: {}", std::io::Error::last_os_error());
    // SAFETY: statvfs succeeded and initialized the allocation.
    let stat = unsafe { stat.assume_init() };
    Ok(stat.f_bavail.saturating_mul(stat.f_frsize))
}

#[cfg(not(unix))]
pub fn disk_available(_path: &Path) -> Result<u64> {
    bail!("disk availability is not implemented for this platform")
}

pub fn ram_available() -> Result<u64> {
    let meminfo = fs::read_to_string("/proc/meminfo").context("read Linux memory availability")?;
    let kb = meminfo.lines().find_map(|line| line.strip_prefix("MemAvailable:")
        .and_then(|line| line.split_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok()))
        .context("MemAvailable not present in /proc/meminfo")?;
    Ok(kb.saturating_mul(1024))
}

fn git(repo: &Path, args: &[&str]) -> Result<std::process::Output> {
    Command::new("git").arg("-C").arg(repo).args(args).output()
        .with_context(|| format!("run git in {}", repo.display()))
}
fn git_text(repo: &Path, args: &[&str]) -> Result<String> {
    let output = git(repo, args)?;
    ensure!(output.status.success(), "git {}: {}", args.join(" "), String::from_utf8_lossy(&output.stderr));
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
    pub dirty: bool,
    pub last_activity_unix: Option<u64>,
    pub size_bytes: u64,
    pub gc_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Owner { session: Uuid }

fn git_dir(tree: &Path) -> Result<PathBuf> {
    Ok(PathBuf::from(git_text(tree, &["rev-parse", "--absolute-git-dir"])?))
}

fn tree_owner(tree: &Path) -> Option<Uuid> {
    fs::read(git_dir(tree).ok()?.join("borg-worktree-owner.json")).ok()
        .and_then(|data| serde_json::from_slice::<Owner>(&data).ok())
        .map(|owner| owner.session)
}

/// `active` is an authoritative list of local session owners, obtained from the
/// workspace journal. Unknown owners are not interpreted as dead.
pub fn inventory(repo: &Path, active: &[(Uuid, PathBuf)]) -> Result<Vec<WorktreeRecord>> {
    let output = git_text(repo, &["worktree", "list", "--porcelain"])?;
    let primary = PathBuf::from(git_text(repo, &["rev-parse", "--path-format=absolute", "--git-common-dir"])?);
    let mut trees = Vec::new();
    for block in output.split("\n\n") {
        let Some(path) = block.lines().find_map(|line| line.strip_prefix("worktree ")) else { continue };
        let path = PathBuf::from(path);
        if !path.is_dir() { continue }
        let branch = block.lines().find_map(|line| line.strip_prefix("branch refs/heads/").map(str::to_owned));
        let owner = tree_owner(&path).or_else(|| active.iter().find(|(_, cwd)| cwd == &path).map(|(id, _)| *id));
        let owner_live = active.iter().any(|(id, cwd)| Some(*id) == owner || cwd == &path);
        let status = git(&path, &["status", "--porcelain=v1", "--untracked-files=normal"])?;
        let dirty = !status.status.success() || !status.stdout.is_empty();
        let merged = branch.as_deref().is_some_and(|branch| branch != "main")
            && git(&path, &["merge-base", "--is-ancestor", "HEAD", "refs/heads/main"])
                .is_ok_and(|out| out.status.success());
        let is_primary = git_dir(&path).is_ok_and(|dir| dir == primary);
        let owner_gone = owner.is_some() && !owner_live; // only advisory; journal exit proof required for removal
        let size_bytes = Command::new("du").args(["-skx", "--"])
            .arg(&path).output().ok().filter(|out| out.status.success())
            .and_then(|out| String::from_utf8(out.stdout).ok())
            .and_then(|text| text.split_whitespace().next()?.parse::<u64>().ok())
            .unwrap_or(0).saturating_mul(1024);
        let last_activity_unix = fs::metadata(&path).ok().and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok()).map(|d| d.as_secs());
        let gc_reason = if !is_primary && merged && !dirty && !owner_live && owner.is_some() {
            Some("merged, clean; verify owner exit before removal".into())
        } else { None };
        trees.push(WorktreeRecord { path, branch, owner, owner_live, owner_gone, merged,
            dirty, last_activity_unix, size_bytes, gc_reason });
    }
    Ok(trees)
}

pub fn create_worktree(repo: &Path, root: &Path, task: &str, owner: Uuid, shared_cargo: bool,
    budgets: &WorkspaceBudgets) -> Result<WorktreeRecord> {
    ensure!(!task.is_empty() && task.len() <= 64 && task.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'), "task must be 1-64 ASCII letters, digits, '-' or '_'");
    fs::create_dir_all(root)?;
    let admission = assess_admission(budgets, disk_available(root)?, ram_available()?, 0, 0, GIB, 0);
    ensure!(admission.admitted, "worktree queued: {}", admission.reason.unwrap_or_default());
    let path = root.join(format!("{task}-{}", &owner.to_string()[..8]));
    ensure!(!path.exists(), "worktree path already exists: {}", path.display());
    let branch = format!("agent/{task}-{}", &owner.to_string()[..8]);
    let result = git(repo, &["worktree", "add", "-b", &branch, path.to_str().context("worktree path not UTF-8")?, "HEAD"])?;
    ensure!(result.status.success(), "git worktree add: {}", String::from_utf8_lossy(&result.stderr));
    fs::write(git_dir(&path)?.join("borg-worktree-owner.json"), serde_json::to_vec(&Owner { session: owner })?)?;
    if shared_cargo {
        // Cargo already shares ~/.cargo registry; keep build products per worktree.
        let cargo_dir = path.join(".cargo");
        fs::create_dir_all(&cargo_dir)?;
        let config = cargo_dir.join("config.toml");
        if !config.exists() {
            fs::write(config, "# Registry/download cache uses the user's shared CARGO_HOME.\n[build]\ntarget-dir = \"target\"\n")?;
        }
    }
    inventory(repo, &[(owner, path.clone())])?.into_iter().find(|tree| tree.path == path)
        .context("new worktree not in Git inventory")
}

/// Only an explicit, journal-confirmed exited owner allows removal. Dirty trees
/// require force; force does not bypass the live-session guard.
pub fn gc(repo: &Path, tree: &WorktreeRecord, exited_owners: &[Uuid], apply: bool, force: bool) -> Result<bool> {
    ensure!(tree.gc_reason.is_some() || (force && tree.merged && !tree.owner_live), "not eligible for GC");
    ensure!(!tree.owner_live, "live session owns worktree");
    ensure!(tree.owner.is_some_and(|id| exited_owners.contains(&id)), "owner exit not confirmed by journal");
    ensure!(!tree.dirty || force, "dirty worktree requires --force");
    if !apply { return Ok(false) }
    // Recheck at the point of use; Git also refuses removal of dirty trees without force.
    let fresh = inventory(repo, &[])?.into_iter().find(|candidate| candidate.path == tree.path)
        .context("worktree disappeared before GC")?;
    ensure!(!fresh.dirty || force, "worktree became dirty");
    ensure!(fresh.merged && fresh.owner == tree.owner, "worktree changed since proposal");
    let output = git(repo, &["worktree", "remove", if force { "--force" } else { "--" },
        tree.path.to_str().context("path not UTF-8")?])?;
    ensure!(output.status.success(), "git worktree remove: {}", String::from_utf8_lossy(&output.stderr));
    Ok(true)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirtyOwner {
    pub path: PathBuf,
    pub session: Option<Uuid>,
    pub files: Vec<String>,
}

/// Snapshot only: does not pause or mutate any agent. Globs use `*`, `**`, `?`.
pub fn freeze_preview(repo: &Path, globs: &[String], active: &[(Uuid, PathBuf)]) -> Result<Vec<DirtyOwner>> {
    ensure!(!globs.is_empty(), "at least one path glob required");
    let expressions = globs.iter().map(|glob| {
        let mut pattern = String::from("^");
        let mut chars = glob.chars().peekable();
        while let Some(c) = chars.next() {
            match c {
                '*' if chars.peek() == Some(&'*') => { chars.next(); pattern.push_str(".*"); }
                '*' => pattern.push_str("[^/]*"),
                '?' => pattern.push_str("[^/]"),
                _ => pattern.push_str(&regex::escape(&c.to_string())),
            }
        }
        pattern.push('$');
        regex::Regex::new(&pattern)
    }).collect::<std::result::Result<Vec<_>, _>>()?;
    let mut owners = Vec::new();
    for tree in inventory(repo, active)? {
        let mut files = Vec::new();
        for args in [["diff", "--name-only", "-z", "HEAD"].as_slice(),
                     ["ls-files", "--others", "--exclude-standard", "-z"].as_slice()] {
            let out = git(&tree.path, args)?;
            if !out.status.success() { continue }
            files.extend(out.stdout.split(|b| *b == 0).filter(|name| !name.is_empty())
                .filter_map(|name| String::from_utf8(name.to_vec()).ok())
                .filter(|name| expressions.iter().any(|expr| expr.is_match(name))));
        }
        files.sort(); files.dedup();
        if !files.is_empty() { owners.push(DirtyOwner { path: tree.path, session: tree.owner, files }); }
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
