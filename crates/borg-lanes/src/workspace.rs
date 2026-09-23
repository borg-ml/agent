//! Worktree budgets and local freeze gates; shared-work claims stay in Borg's workspace log.

use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::lanes::{AdmissionBudget, Holder, ResourceKey};

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
    async fn list(&self, project: &PathBuf) -> Result<Vec<WorktreeRecord>>;
    async fn gc(&self, project: &PathBuf, dry_run: bool) -> Result<Vec<PathBuf>>;
    async fn request_freeze(&self, request: FreezeRequest) -> Result<Freeze>;
    async fn acknowledge_freeze(&self, id: Uuid, participant: Uuid) -> Result<Freeze>;
    async fn release_freeze(&self, id: Uuid) -> Result<()>;
}
