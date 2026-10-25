//! Host-pinned device trust. With no pinned devices the host trusts the relay
//! as before; once any key is pinned in host.json, the host itself verifies
//! device proofs and owner-command signatures and refuses everything else.
use crate::auth::{DeviceCommandSignature, TicketClaims, TrustedDevice, device_command_bytes};
use crate::{HostCommand, HostConfig};
use anyhow::{Context, Result, bail, ensure};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use ed25519_dalek::{Signature, VerifyingKey};
use std::{
    io::Write,
    path::{Path, PathBuf},
    sync::Mutex,
    time::SystemTime,
};

/// Longest validity a signed command may claim; bounds a captured signature.
const SIGNED_COMMAND_MAX_TTL: i64 = 300;

/// Decodes and re-encodes a device key so pinned keys compare canonically.
pub fn canonical_device_key(public_key: &str) -> Result<String> {
    let key: [u8; 32] = URL_SAFE_NO_PAD
        .decode(public_key.trim())
        .context("device public key must be base64url without padding")?
        .try_into()
        .map_err(|_| anyhow::anyhow!("device public key must be 32 bytes"))?;
    VerifyingKey::from_bytes(&key).context("invalid Ed25519 device public key")?;
    Ok(URL_SAFE_NO_PAD.encode(key))
}

pub fn ensure_trusted(devices: &[TrustedDevice], public_key: &str) -> Result<()> {
    ensure!(
        devices.iter().any(|device| device.public_key == public_key),
        "device {} is not trusted by this host",
        crate::auth::device_fingerprint(public_key).unwrap_or_else(|_| "invalid".into())
    );
    Ok(())
}

/// Relay-path Open: under enforcement the host re-verifies the client proof.
pub fn verify_relay_open(
    devices: &[TrustedDevice],
    claims: &TicketClaims,
    nonce: Option<&str>,
    proof: Option<&str>,
) -> Result<()> {
    if devices.is_empty() {
        return Ok(());
    }
    let (Some(nonce), Some(proof)) = (nonce, proof) else {
        bail!("host requires a device proof on relay admission");
    };
    crate::admission::verify_device_proof(claims, nonce, proof)?;
    ensure_trusted(devices, &claims.device_public_key)
}

/// Owner command admission. Returns the signing key when the command was signed.
pub fn verify_owner_command(
    devices: &[TrustedDevice],
    host_id: &str,
    command_id: &str,
    command: &HostCommand,
    not_after: Option<i64>,
    signature: Option<&DeviceCommandSignature>,
    now: i64,
) -> Result<Option<String>> {
    if devices.is_empty() {
        return Ok(None);
    }
    let Some(signature) = signature else {
        if matches!(
            command,
            HostCommand::Interrupt { .. } | HostCommand::Stop { .. }
        ) {
            return Ok(None);
        }
        bail!("host requires device-signed commands");
    };
    ensure_trusted(devices, &signature.device_public_key)?;
    let not_after = not_after.context("signed command requires not_after")?;
    ensure!(
        not_after <= now + SIGNED_COMMAND_MAX_TTL,
        "signed command not_after is too far in the future"
    );
    ensure!(not_after >= now, "signed command expired");
    ensure!(
        signature.signature.len() <= 128,
        "oversized command signature"
    );
    let key: [u8; 32] = URL_SAFE_NO_PAD
        .decode(&signature.device_public_key)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid device public key"))?;
    let parsed = Signature::from_slice(&URL_SAFE_NO_PAD.decode(&signature.signature)?)?;
    VerifyingKey::from_bytes(&key)?
        .verify_strict(
            &device_command_bytes(host_id, command_id, Some(not_after), &signature.command),
            &parsed,
        )
        .context("invalid command signature")?;
    let signed: serde_json::Value =
        serde_json::from_str(&signature.command).context("signed command is not JSON")?;
    ensure!(
        signed == serde_json::to_value(command)?,
        "signed command does not match the delivered command"
    );
    Ok(Some(signature.device_public_key.clone()))
}

/// Trusted devices from host.json, re-read whenever its mtime changes so
/// `borg remote devices trust|untrust` applies to a running host.
pub struct DeviceTrust {
    config_path: PathBuf,
    state: Mutex<(Option<SystemTime>, Vec<TrustedDevice>)>,
}

impl DeviceTrust {
    pub fn new(config_path: &Path, devices: Vec<TrustedDevice>) -> Self {
        let modified = std::fs::metadata(config_path)
            .and_then(|metadata| metadata.modified())
            .ok();
        Self {
            config_path: config_path.to_path_buf(),
            state: Mutex::new((modified, devices)),
        }
    }

    /// Current trusted devices; reports whether the list changed.
    pub fn refresh(&self) -> (Vec<TrustedDevice>, bool) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let modified = std::fs::metadata(&self.config_path)
            .and_then(|metadata| metadata.modified())
            .ok();
        if modified == state.0 {
            return (state.1.clone(), false);
        }
        let read = std::fs::read(&self.config_path)
            .map_err(anyhow::Error::from)
            .and_then(|bytes| Ok(serde_json::from_slice::<HostConfig>(&bytes)?));
        match read {
            Ok(config) => {
                state.0 = modified;
                let changed = config.trusted_devices != state.1;
                state.1 = config.trusted_devices;
                (state.1.clone(), changed)
            }
            // Keep the last good list on a torn or invalid read; retry next time.
            Err(error) => {
                tracing::warn!(%error, "host config reload failed; keeping trusted devices");
                (state.1.clone(), false)
            }
        }
    }

    pub fn devices(&self) -> Vec<TrustedDevice> {
        self.refresh().0
    }

    pub fn audit_path(&self) -> PathBuf {
        audit_path(&self.config_path)
    }
}

pub fn audit_path(config_path: &Path) -> PathBuf {
    config_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("audit.jsonl")
}

/// Appends one audit record. Never records command content or secrets.
pub fn audit(path: &Path, mut record: serde_json::Value) {
    if let Some(object) = record.as_object_mut() {
        object.insert(
            "time".into(),
            chrono::Utc::now()
                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
                .into(),
        );
    }
    let result = (|| -> std::io::Result<()> {
        let mut options = std::fs::OpenOptions::new();
        options.append(true).create(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
        let mut line = serde_json::to_vec(&record)?;
        line.push(b'\n');
        options.open(path)?.write_all(&line)
    })();
    if let Err(error) = result {
        tracing::warn!(%error, path = %path.display(), "remote audit append failed");
    }
}

pub fn audit_admission(
    path: &Path,
    route: &str,
    claims: Option<&TicketClaims>,
    rejection: Option<&anyhow::Error>,
) {
    let mut record = serde_json::json!({
        "event": "admission",
        "route": route,
        "outcome": if rejection.is_none() { "admitted" } else { "rejected" },
    });
    if let Some(claims) = claims {
        record["ticket_id"] = claims.ticket_id.clone().into();
        record["device"] = fingerprint_or_none(&claims.device_public_key);
        record["permission"] = serde_json::to_value(&claims.permission).unwrap_or_default();
        record["scope"] = if claims.session_ids.is_empty() {
            "host".into()
        } else {
            claims.session_ids.clone().into()
        };
    }
    if let Some(error) = rejection {
        record["reason"] = error.to_string().into();
    }
    audit(path, record);
}

pub fn fingerprint_or_none(public_key: &str) -> serde_json::Value {
    crate::auth::device_fingerprint(public_key)
        .map(serde_json::Value::from)
        .unwrap_or(serde_json::Value::Null)
}

/// `type` tag of a host command: the variant name, never its content.
pub fn command_kind(command: &HostCommand) -> String {
    serde_json::to_value(command)
        .ok()
        .and_then(|value| value.get("type")?.as_str().map(str::to_string))
        .unwrap_or_default()
}

pub fn trusted_devices(config_path: &Path) -> Result<Vec<TrustedDevice>> {
    Ok(crate::host::read_config(config_path)?.trusted_devices)
}

/// Pin a device key in host.json. Returns false when it was already trusted.
pub fn trust_device(config_path: &Path, public_key: &str, label: &str) -> Result<bool> {
    let public_key = canonical_device_key(public_key)?;
    let mut config = crate::host::read_config(config_path)?;
    if config
        .trusted_devices
        .iter()
        .any(|device| device.public_key == public_key)
    {
        return Ok(false);
    }
    config.trusted_devices.push(TrustedDevice {
        public_key: public_key.clone(),
        label: label.to_string(),
        added_at: chrono::Utc::now().timestamp(),
    });
    crate::host::write_config(config_path, &config)?;
    audit(
        &audit_path(config_path),
        serde_json::json!({"event":"trust","route":"local","device":fingerprint_or_none(&public_key),"label":label,"outcome":"trusted"}),
    );
    Ok(true)
}

/// Unpin by public key or fingerprint; returns the removed device.
pub fn untrust_device(config_path: &Path, key_or_fingerprint: &str) -> Result<TrustedDevice> {
    let wanted = key_or_fingerprint.trim();
    let mut config = crate::host::read_config(config_path)?;
    let index = config
        .trusted_devices
        .iter()
        .position(|device| {
            device.public_key == wanted
                || crate::auth::device_fingerprint(&device.public_key)
                    .is_ok_and(|fingerprint| fingerprint.eq_ignore_ascii_case(wanted))
        })
        .context("no trusted device matches that key or fingerprint")?;
    let removed = config.trusted_devices.remove(index);
    crate::host::write_config(config_path, &config)?;
    audit(
        &audit_path(config_path),
        serde_json::json!({"event":"trust","route":"local","device":fingerprint_or_none(&removed.public_key),"label":removed.label,"outcome":"untrusted"}),
    );
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::Permission;
    use ed25519_dalek::{Signer, SigningKey};

    fn pinned(key: &SigningKey) -> Vec<TrustedDevice> {
        vec![TrustedDevice {
            public_key: URL_SAFE_NO_PAD.encode(key.verifying_key().as_bytes()),
            label: "phone".into(),
            added_at: 0,
        }]
    }

    fn sign(key: &SigningKey, command: &HostCommand, not_after: i64) -> DeviceCommandSignature {
        let command = serde_json::to_string(command).unwrap();
        let bytes = device_command_bytes("h", "c", Some(not_after), &command);
        DeviceCommandSignature {
            device_public_key: URL_SAFE_NO_PAD.encode(key.verifying_key().as_bytes()),
            command,
            signature: URL_SAFE_NO_PAD.encode(key.sign(&bytes).to_bytes()),
        }
    }

    // Security boundary: once a device is pinned, the relay alone must not be
    // able to run owner commands; only stop/interrupt may arrive unsigned.
    #[test]
    fn enforcing_host_accepts_only_trusted_signed_owner_commands() {
        let trusted = SigningKey::from_bytes(&[1; 32]);
        let stranger = SigningKey::from_bytes(&[2; 32]);
        let devices = pinned(&trusted);
        let session_id = uuid::Uuid::nil();
        let compact = HostCommand::Compact { session_id };
        let verify = |signature: Option<&DeviceCommandSignature>, not_after| {
            verify_owner_command(&devices, "h", "c", &compact, not_after, signature, 1000)
        };
        assert!(verify_owner_command(&[], "h", "c", &compact, None, None, 1000).is_ok());
        assert!(verify(None, Some(1100)).is_err());
        let interrupt = HostCommand::Interrupt { session_id };
        assert!(verify_owner_command(&devices, "h", "c", &interrupt, None, None, 1000).is_ok());

        let good = sign(&trusted, &compact, 1100);
        assert!(verify(Some(&good), Some(1100)).unwrap().is_some());
        assert!(verify(Some(&good), None).is_err());
        assert!(verify(Some(&sign(&trusted, &compact, 1400)), Some(1400)).is_err());
        assert!(verify(Some(&sign(&stranger, &compact, 1100)), Some(1100)).is_err());
        // Signature over a different deadline does not verify for this one.
        let mut tampered = good.clone();
        tampered.signature = sign(&trusted, &compact, 1099).signature;
        assert!(verify(Some(&tampered), Some(1100)).is_err());
        let other = sign(&trusted, &HostCommand::ClearContext { session_id }, 1100);
        assert!(verify(Some(&other), Some(1100)).is_err());
        assert!(
            verify_owner_command(
                &devices,
                "other-host",
                "c",
                &compact,
                Some(1100),
                Some(&good),
                1000
            )
            .is_err()
        );
    }

    // Security boundary: an enforcing host needs the device's own proof on the
    // relay path, so the relay cannot admit a device it invented.
    #[test]
    fn enforcing_host_requires_trusted_device_proof_on_relay_open() {
        let trusted = SigningKey::from_bytes(&[1; 32]);
        let claims = |key: &SigningKey| TicketClaims {
            version: 2,
            ticket_id: "t".into(),
            principal_id: "p".into(),
            device_id: "d".into(),
            host_id: "h".into(),
            session_ids: vec![],
            permission: Permission::Control,
            device_public_key: URL_SAFE_NO_PAD.encode(key.verifying_key().as_bytes()),
            issued_at: 0,
            expires_at: 100,
            revocation_epoch: 0,
        };
        let proof = |key: &SigningKey| {
            URL_SAFE_NO_PAD.encode(key.sign(b"borg.remote.auth.v2\nt\nn").to_bytes())
        };
        let devices = pinned(&trusted);
        assert!(verify_relay_open(&[], &claims(&trusted), None, None).is_ok());
        assert!(verify_relay_open(&devices, &claims(&trusted), None, None).is_err());
        assert!(
            verify_relay_open(
                &devices,
                &claims(&trusted),
                Some("n"),
                Some(&proof(&trusted))
            )
            .is_ok()
        );
        let stranger = SigningKey::from_bytes(&[2; 32]);
        assert!(
            verify_relay_open(
                &devices,
                &claims(&stranger),
                Some("n"),
                Some(&proof(&stranger))
            )
            .is_err()
        );
        assert!(
            verify_relay_open(
                &devices,
                &claims(&trusted),
                Some("n"),
                Some(&proof(&stranger))
            )
            .is_err()
        );
    }
}
