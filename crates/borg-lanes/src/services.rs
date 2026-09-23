//! Persistent, supervised services; independent of requesting Borg sessions.

use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::lanes::{AdmissionBudget, Holder, Hook, ResourceRequest};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HealthCheck {
    pub argv: Vec<String>,
    pub interval_ms: u64,
    pub timeout_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RestartPolicy {
    pub max_restarts: u32,
    pub backoff_ms: u64,
    pub debounce_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Endpoint {
    pub listen: String,
    pub backend_ports: [u16; 2],
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServiceSpec {
    pub id: String,
    pub argv: Vec<String>,
    pub cwd: PathBuf,
    pub env: Vec<(String, String)>,
    pub resources: Vec<ResourceRequest>,
    pub memory_max_bytes: Option<u64>,
    pub admission: AdmissionBudget,
    pub health: HealthCheck,
    pub restart: RestartPolicy,
    pub endpoint: Option<Endpoint>,
    pub restore: Option<Hook>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ServiceState {
    Stopped,
    Starting,
    Healthy { backend: Option<u16> },
    Degraded { reason: String },
    Yielding,
    Yielded,
    RestartPending,
    Restarting,
    Failed { reason: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientLease {
    pub id: Uuid,
    pub service_id: String,
    pub owner: Holder,
    pub expires_at_unix_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServiceStatus {
    pub id: String,
    pub state: ServiceState,
    pub endpoint: Option<Endpoint>,
    pub clients: Vec<ClientLease>,
}

#[async_trait]
pub trait ServiceCoordinator: Send + Sync {
    async fn start(&self, spec: ServiceSpec) -> Result<ServiceStatus>;
    async fn status(&self, id: &str) -> Result<ServiceStatus>;
    async fn acquire(&self, id: &str, owner: Holder, ttl_ms: u64) -> Result<ClientLease>;
    async fn release(&self, lease: &ClientLease) -> Result<()>;
    async fn trigger_restart(&self, id: &str, reason: &str) -> Result<()>;
    async fn yield_for(&self, id: &str, resources: &[ResourceRequest]) -> Result<()>;
    async fn resume(&self, id: &str) -> Result<ServiceStatus>;
    async fn stop(&self, id: &str) -> Result<()>;
}
