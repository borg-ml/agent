//! Worktree budgets and local freeze gates; shared-work claims stay in Borg's workspace log.

use std::path::{Path, PathBuf};
pub mod hygiene;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::lanes::{AdmissionBudget, Holder, ResourceKey};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Admission {
    pub admitted: bool,
    pub reason: String,
    pub parallelism_hint: Option<u32>,
}

/// Recheck under the lane dispatch lock; reservations are counted by the supervisor.
pub fn assess_budget(
    budget: &AdmissionBudget,
    reserved_ram: u64,
    reserved_disk: u64,
) -> Result<Admission> {
    let ram = hygiene::ram_available()?.saturating_sub(reserved_ram);
    let disk = hygiene::disk_available(&budget.disk_path)?.saturating_sub(reserved_disk);
    let required_ram = budget
        .min_available_ram_bytes
        .saturating_add(budget.reserve_ram_bytes);
    let required_disk = budget
        .min_free_disk_bytes
        .saturating_add(budget.reserve_disk_bytes);
    let reason = if disk < required_disk {
        format!(
            "disk admission queued: {disk} free after reservation, {required_disk} required on {}",
            budget.disk_path.display()
        )
    } else if ram < required_ram {
        format!("RAM admission queued: {ram} available after reservation, {required_ram} required")
    } else {
        String::new()
    };
    Ok(Admission {
        admitted: reason.is_empty(),
        reason,
        parallelism_hint: None,
    })
}

/// Jobs targeting different directories on the same filesystem compete for the
/// same free bytes. Compare devices, not path strings, when summing lane disk
/// reservations before calling `assess_budget`.
#[cfg(unix)]
pub fn same_filesystem(left: &Path, right: &Path) -> Result<bool> {
    use std::os::unix::fs::MetadataExt;
    Ok(std::fs::metadata(left)?.dev() == std::fs::metadata(right)?.dev())
}

#[cfg(not(unix))]
pub fn same_filesystem(_left: &Path, _right: &Path) -> Result<bool> {
    anyhow::bail!("filesystem identity is unavailable on this platform")
}

#[cfg(test)]
mod budget_tests {
    use super::*;

    #[test]
    fn worktrees_on_same_device_share_reservations() -> Result<()> {
        let root = tempfile::tempdir()?;
        let a = root.path().join("a");
        let b = root.path().join("b");
        std::fs::create_dir_all(&a)?;
        std::fs::create_dir_all(&b)?;
        assert!(same_filesystem(&a, &b)?);
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorktreeSpec {
    pub project: PathBuf,
    pub path: PathBuf,
    pub base_ref: String,
    pub branch: String,
    pub owner: Holder,
    pub admission: AdmissionBudget,
    pub cache: CachePolicy,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CachePolicy {
    Private,
    SharedReadOnly {
        path: PathBuf,
    },
    SharedLocked {
        path: PathBuf,
        resource: ResourceKey,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorktreeRecord {
    pub id: Uuid,
    pub spec: WorktreeSpec,
    pub estimated_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FreezeRequest {
    pub project: PathBuf,
    pub owner: Holder,
    pub shared_work_id: Uuid,
    pub reason: String,
    pub affected_paths: Vec<PathBuf>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Freeze {
    pub id: Uuid,
    pub request: FreezeRequest,
    pub acknowledged_participants: Vec<Uuid>,
}

#[async_trait]
pub trait WorkspaceCoordinator: Send + Sync {
    async fn create(&self, spec: WorktreeSpec) -> Result<WorktreeRecord>;
    async fn list(&self, project: &Path) -> Result<Vec<WorktreeRecord>>;
    async fn gc(&self, project: &Path, dry_run: bool) -> Result<Vec<PathBuf>>;
    async fn request_freeze(&self, request: FreezeRequest) -> Result<Freeze>;
    async fn acknowledge_freeze(&self, id: Uuid, participant: Uuid) -> Result<Freeze>;
    async fn release_freeze(&self, id: Uuid) -> Result<()>;
}
