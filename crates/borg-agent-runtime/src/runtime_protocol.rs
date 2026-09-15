//! Versioned provider-neutral wire envelopes for the Borg agent runtime.
//!
//! The session actor and its durable journal remain the authority. These
//! envelopes are only the transport boundary used by a Web adapter, a local
//! host, or a future worker. The legacy remote host protocol can be adapted
//! without changing command or event semantics.

use anyhow::{Result, ensure};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::session_store::SessionState;
use crate::{HostCommand, HostCommandEnvelope, SessionEvent};

/// Stable protocol identity. Implementation crates and transports may change;
/// this identifier must not be reused for an incompatible contract.
pub const AGENT_RUNTIME_PROTOCOL: &str = "borg.agent_runtime";
pub const AGENT_RUNTIME_PROTOCOL_VERSION: u16 = 1;

/// A command sent to the canonical Borg session actor.
///
/// `idempotency_key` is mandatory at this boundary even though the legacy host
/// envelope did not carry one. The compatibility conversion derives a stable
/// key from the legacy envelope id so retries remain safe during migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentRuntimeCommandEnvelope {
    pub protocol: String,
    pub version: u16,
    pub request_id: Uuid,
    pub correlation_id: Uuid,
    pub idempotency_key: String,
    pub sequence: u64,
    pub created_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claim_token: Option<Uuid>,
    pub command: HostCommand,
}

impl AgentRuntimeCommandEnvelope {
    pub fn new(
        request_id: Uuid,
        correlation_id: Uuid,
        idempotency_key: impl Into<String>,
        command: HostCommand,
    ) -> Self {
        Self::new_at(
            request_id,
            correlation_id,
            idempotency_key,
            0,
            Utc::now(),
            command,
        )
    }

    pub fn new_at(
        request_id: Uuid,
        correlation_id: Uuid,
        idempotency_key: impl Into<String>,
        sequence: u64,
        created_at: DateTime<Utc>,
        command: HostCommand,
    ) -> Self {
        Self {
            protocol: AGENT_RUNTIME_PROTOCOL.to_string(),
            version: AGENT_RUNTIME_PROTOCOL_VERSION,
            request_id,
            correlation_id,
            idempotency_key: idempotency_key.into(),
            sequence,
            created_at,
            claim_token: None,
            command,
        }
    }

    /// Adapt the existing remote-host envelope without changing its command
    /// payload. This is the bridge used while Web and CLI converge.
    pub fn from_legacy(envelope: HostCommandEnvelope) -> Self {
        let idempotency_key = format!("legacy-host-command:{}", envelope.id);
        let mut runtime = Self::new_at(
            envelope.id,
            envelope.id,
            idempotency_key,
            envelope.sequence,
            envelope.created_at,
            envelope.command,
        );
        runtime.claim_token = envelope.claim_token;
        runtime
    }

    pub fn into_legacy(self) -> Result<HostCommandEnvelope> {
        self.validate()?;
        Ok(HostCommandEnvelope {
            id: self.request_id,
            sequence: self.sequence,
            created_at: self.created_at,
            claim_token: self.claim_token,
            command: self.command,
        })
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.protocol == AGENT_RUNTIME_PROTOCOL,
            "unsupported agent runtime protocol `{}`",
            self.protocol
        );
        ensure!(
            self.version == AGENT_RUNTIME_PROTOCOL_VERSION,
            "unsupported agent runtime protocol version {}",
            self.version
        );
        ensure!(
            !self.idempotency_key.trim().is_empty(),
            "agent runtime command idempotency key is empty"
        );
        if let Some(session_id) = self.command.session_id() {
            ensure!(
                self.correlation_id == session_id || self.correlation_id == self.request_id,
                "agent runtime command correlation id does not identify the command session"
            );
        }
        Ok(())
    }
}

/// One event from the canonical session journal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentRuntimeEventEnvelope {
    pub protocol: String,
    pub version: u16,
    pub session_id: Uuid,
    pub cursor: u64,
    pub event: SessionEvent,
}

impl AgentRuntimeEventEnvelope {
    pub fn from_event(event: SessionEvent) -> Self {
        Self {
            protocol: AGENT_RUNTIME_PROTOCOL.to_string(),
            version: AGENT_RUNTIME_PROTOCOL_VERSION,
            session_id: event.session_id,
            cursor: event.sequence,
            event,
        }
    }

    pub fn into_event(self) -> Result<SessionEvent> {
        self.validate()?;
        Ok(self.event)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.protocol == AGENT_RUNTIME_PROTOCOL,
            "unsupported agent runtime protocol `{}`",
            self.protocol
        );
        ensure!(
            self.version == AGENT_RUNTIME_PROTOCOL_VERSION,
            "unsupported agent runtime protocol version {}",
            self.version
        );
        ensure!(
            self.session_id == self.event.session_id,
            "agent runtime event session id does not match its envelope"
        );
        ensure!(
            self.cursor == self.event.sequence,
            "agent runtime event cursor does not match its event sequence"
        );
        Ok(())
    }
}

/// A reconnect/recovery snapshot for one session.
///
/// The snapshot is a projection. The lossless event journal remains the source
/// of truth and can rebuild it when a worker or product projection is lost.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentRuntimeSnapshot {
    pub protocol: String,
    pub version: u16,
    pub session_id: Uuid,
    pub cursor: u64,
    pub state: SessionState,
}

impl AgentRuntimeSnapshot {
    pub fn new(session_id: Uuid, state: SessionState) -> Self {
        Self {
            protocol: AGENT_RUNTIME_PROTOCOL.to_string(),
            version: AGENT_RUNTIME_PROTOCOL_VERSION,
            session_id,
            cursor: state.latest_sequence,
            state,
        }
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.protocol == AGENT_RUNTIME_PROTOCOL,
            "unsupported agent runtime protocol `{}`",
            self.protocol
        );
        ensure!(
            self.version == AGENT_RUNTIME_PROTOCOL_VERSION,
            "unsupported agent runtime protocol version {}",
            self.version
        );
        ensure!(
            self.cursor == self.state.latest_sequence,
            "agent runtime snapshot cursor does not match its projected sequence"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use serde_json::Value;

    use super::*;

    fn fixture(name: &str) -> Value {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("protocol-fixtures")
            .join("v1")
            .join(name);
        serde_json::from_str(
            &fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display())),
        )
        .unwrap_or_else(|error| panic!("invalid JSON fixture {}: {error}", path.display()))
    }

    #[test]
    fn canonical_fixtures_are_valid_runtime_envelopes() {
        let command: AgentRuntimeCommandEnvelope =
            serde_json::from_value(fixture("command-prompt.json")).expect("command fixture");
        command.validate().expect("command validation");

        let goal: AgentRuntimeCommandEnvelope =
            serde_json::from_value(fixture("command-goal.json")).expect("goal fixture");
        goal.validate().expect("goal validation");

        let event: AgentRuntimeEventEnvelope =
            serde_json::from_value(fixture("event-message.json")).expect("event fixture");
        event.validate().expect("event validation");

        let snapshot: AgentRuntimeSnapshot =
            serde_json::from_value(fixture("snapshot.json")).expect("snapshot fixture");
        snapshot.validate().expect("snapshot validation");
    }

    #[test]
    fn legacy_host_envelope_round_trips_through_the_compatibility_adapter() {
        let wire: AgentRuntimeCommandEnvelope =
            serde_json::from_value(fixture("command-prompt.json")).expect("command fixture");
        let legacy = wire.clone().into_legacy().expect("legacy command");
        let adapted = AgentRuntimeCommandEnvelope::from_legacy(legacy.clone());

        assert_eq!(adapted.request_id, legacy.id);
        assert_eq!(adapted.sequence, legacy.sequence);
        assert_eq!(adapted.created_at, legacy.created_at);
        assert_eq!(
            serde_json::to_value(&adapted.command).expect("adapted command JSON"),
            serde_json::to_value(&legacy.command).expect("legacy command JSON")
        );
        assert_eq!(
            adapted.idempotency_key,
            format!("legacy-host-command:{}", legacy.id)
        );
    }
}
