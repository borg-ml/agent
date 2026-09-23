//! Declarative adapter contract. Blu validation remains the trust boundary.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::lanes::{JobSpec, ResourceKey, ResourceRequest};
use crate::services::ServiceSpec;
use crate::workspace::CachePolicy;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JobTemplate {
    pub name: String,
    pub kind: JobKind,
    pub resources: Vec<ResourceRequest>,
    pub spec: JobSpec,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum JobKind {
    Build,
    Run,
    Test,
    Import,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdapterManifest {
    pub id: String,
    pub project_markers: Vec<String>,
    pub resources: Vec<ResourceKey>,
    pub jobs: Vec<JobTemplate>,
    pub services: Vec<ServiceSpec>,
    pub caches: Vec<CachePolicy>,
    pub skill_roots: Vec<PathBuf>,
    pub mcp_servers: Vec<String>,
}
