use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// A device key pinned locally in host.json. Pinning any key makes the host
/// verify device proofs and command signatures itself instead of trusting the relay.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustedDevice {
    /// Base64url-no-pad Ed25519 public key (32 bytes).
    pub public_key: String,
    pub label: String,
    pub added_at: i64,
}

/// Owner command signed by a device key; `command` is the exact signed JSON text.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceCommandSignature {
    pub device_public_key: String,
    pub command: String,
    /// Base64url-no-pad Ed25519 signature over [`device_command_bytes`].
    pub signature: String,
}

/// Canonical signed bytes for a device-signed owner command.
pub fn device_command_bytes(
    host_id: &str,
    command_id: &str,
    not_after: Option<i64>,
    command: &str,
) -> Vec<u8> {
    let not_after = not_after.map_or_else(|| "-".to_string(), |value| value.to_string());
    format!("borg.remote.command.v1\n{host_id}\n{command_id}\n{not_after}\n{command}").into_bytes()
}

/// `XXXX-XXXX-XXXX-XXXX`: first 16 chars of base64url(SHA-256(raw key)).
pub fn device_fingerprint(public_key: &str) -> anyhow::Result<String> {
    let key: [u8; 32] = URL_SAFE_NO_PAD
        .decode(public_key)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("device public key must be 32 bytes"))?;
    let digest = URL_SAFE_NO_PAD.encode(Sha256::digest(key));
    Ok(digest.as_bytes()[..16]
        .chunks(4)
        .map(|group| std::str::from_utf8(group).expect("base64 is ASCII"))
        .collect::<Vec<_>>()
        .join("-"))
}

/// Ed25519 signature covers the exact UTF-8 claims bytes, base64url without padding.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SignedTicket {
    pub claims: String,
    pub signature: String,
}
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
pub enum Permission {
    Read,
    Control,
}
/// First frame only. Proof signs `borg.remote.auth.v2\n{ticket_id}\n{nonce}`.
/// Ticket IDs are consumed atomically at relay admission. Direct hosts independently
/// consume ticket IDs, so route switching requires a fresh ticket.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Authentication {
    Client {
        ticket: SignedTicket,
        nonce: String,
        proof: String,
    },
    Host {
        host_id: String,
        token: String,
    },
}
/// One outbound host socket muxes devices; relay never changes the inner view frame.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TunnelFrame {
    /// `nonce`/`proof` repeat the client's admission proof so a host that pins
    /// trusted devices can verify it independently of the relay.
    Open {
        connection_id: String,
        ticket: SignedTicket,
        #[serde(default)]
        nonce: Option<String>,
        #[serde(default)]
        proof: Option<String>,
    },
    /// Private relay owner-service ingress; never accepted from viewer Client frames.
    OpenService {
        connection_id: String,
        ticket: SignedTicket,
        nonce: String,
        proof: String,
    },
    Client {
        connection_id: String,
        frame: serde_json::Value,
    },
    Server {
        connection_id: String,
        frame: serde_json::Value,
    },
    Close {
        connection_id: String,
        reason: String,
    },
    SourceAttention {
        frame: serde_json::Value,
    },
    CacheBatch {
        snapshots: Vec<ColdSnapshot>,
        headlines: Vec<SessionHeadline>,
    },
    Revoked {
        epoch: u64,
    },
    EpochUpdate {
        epoch: u64,
    },
    AuthenticationRejected {
        reason: String,
    },
    /// Trusted relay owner-service ingress only; viewer Client frames cannot emit this.
    HostCommand {
        command_id: String,
        command: borg_agent_runtime::HostCommand,
        not_after: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        device_signature: Option<DeviceCommandSignature>,
    },
    HostCommandAccepted {
        command_id: String,
    },
    HostCommandResult {
        command_id: String,
        outcome: serde_json::Value,
        error: Option<String>,
    },
    Routes {
        routes: Vec<HostRoute>,
    },
    Heartbeat {
        heartbeat: borg_agent_runtime::HostHeartbeat,
    },
    RegisterSession {
        registration_id: String,
        session_id: String,
        request: Box<borg_agent_runtime::LaunchSession>,
    },
    RegisteredSession {
        registration_id: String,
        session_id: String,
        error: Option<String>,
    },
    /// Relay to host: inbox, directory or roster state for this session
    /// changed. The host fetches it now instead of polling on a timer.
    CollaborationChanged {
        session_id: String,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColdSnapshot {
    pub session_id: String,
    pub frame: serde_json::Value,
    pub synchronized_at: String,
}
pub use borg_view::wire::SessionHeadline;

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
#[serde(rename_all = "snake_case")]
pub enum ServicePurpose {
    ProductView,
}
// Read-only by definition: service grants cannot command/approve/configure.
// Proof: `borg.remote.service.v2\n{ticket_id}\n{nonce}` signed by the ephemeral
// service key whose public key is included in relay-signed ServiceTicketClaims.

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostRoute {
    pub url: String,
    pub server_name: String,
    pub protocol: u32,
}

#[cfg(test)]
mod tests {
    // Cross-repo contract: the relay and browser must derive identical
    // fingerprints and signed command bytes.
    #[test]
    fn fingerprint_and_command_bytes_match_shared_protocol() {
        assert_eq!(
            super::device_fingerprint("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA").unwrap(),
            "Zmh6-rfhi-vXds-j8GL"
        );
        assert!(super::device_fingerprint("AAAA").is_err());
        assert_eq!(
            super::device_command_bytes("h", "c", None, "{}"),
            b"borg.remote.command.v1\nh\nc\n-\n{}"
        );
        assert_eq!(
            super::device_command_bytes("h", "c", Some(42), "{}"),
            b"borg.remote.command.v1\nh\nc\n42\n{}"
        );
    }
}
