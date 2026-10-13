use serde::{Deserialize, Serialize};

/// Ed25519 signature covers the exact UTF-8 claims bytes, base64url without padding.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SignedTicket { pub claims: String, pub signature: String }
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TicketClaims {
    pub version: u32,
    pub ticket_id: String,
    pub principal_id: String,
    pub device_id: String,
    pub host_id: String,
    /// Empty means host-wide; otherwise an explicit allowlist including child IDs.
    pub session_ids: Vec<String>,
    pub permission: Permission,
    pub device_public_key: String,
    pub issued_at: i64,
    pub expires_at: i64,
    pub revocation_epoch: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Permission { Read, Control }
/// First frame only. Proof signs `borg.remote.auth.v2\n{ticket_id}\n{nonce}`.
/// Ticket IDs are consumed atomically at relay admission. Direct hosts independently
/// consume ticket IDs, so route switching requires a fresh ticket.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Authentication {
    Client { ticket: SignedTicket, nonce: String, proof: String },
    Host { host_id: String, token: String },
}
/// One outbound host socket muxes devices; relay never changes the inner view frame.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TunnelFrame {
    Open { connection_id: String, ticket: SignedTicket },
    /// Private relay owner-service ingress; never accepted from viewer Client frames.
    OpenService { connection_id: String, ticket: SignedTicket, nonce: String, proof: String },
    Client { connection_id: String, frame: serde_json::Value },
    Server { connection_id: String, frame: serde_json::Value },
    Close { connection_id: String, reason: String },
    CacheBatch { snapshots: Vec<ColdSnapshot>, headlines: Vec<SessionHeadline> },
    Revoked { epoch: u64 },
    EpochUpdate { epoch: u64 },
    AuthenticationRejected { reason: String },
    /// Trusted relay owner-service ingress only; viewer Client frames cannot emit this.
    HostCommand { command_id: String, command: borg_agent_runtime::HostCommand, not_after: Option<i64> },
    HostCommandAccepted { command_id: String },
    HostCommandResult { command_id: String, outcome: serde_json::Value, error: Option<String> },
    Routes { routes: Vec<HostRoute> },
    Heartbeat { heartbeat: borg_agent_runtime::HostHeartbeat },
    RegisterSession { registration_id: String, session_id: String, request: borg_agent_runtime::LaunchSession },
    RegisteredSession { registration_id: String, session_id: String, error: Option<String> },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColdSnapshot { pub session_id: String, pub frame: serde_json::Value, pub synchronized_at: String }
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionHeadline { pub session_id: String, pub parent_session_id: Option<String>, pub name: String, pub status: String, pub updated_at: String }

/// Distinct identity contract, not an unregistered-device bypass. Issued only
/// inside the product to an independently authenticated active session owner.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceTicketClaims {
    pub version: u32,
    pub purpose: ServicePurpose,
    pub ticket_id: String,
    pub principal_id: String,
    pub host_id: String,
    pub session_id: String,
    pub service_public_key: String,
    pub issued_at: i64,
    pub expires_at: i64,
    pub revocation_epoch: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all="snake_case")]
pub enum ServicePurpose { ProductView }
// Read-only by definition: service grants cannot command/approve/configure.
// Proof: `borg.remote.service.v2\n{ticket_id}\n{nonce}` signed by the ephemeral
// service key whose public key is included in relay-signed ServiceTicketClaims.

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostRoute { pub url: String, pub server_name: String, pub protocol: u32 }
