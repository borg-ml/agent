//! Cryptographic admission checks shared by direct and relay connections.
use crate::auth::{Permission, SignedTicket, TicketClaims};
use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use ed25519_dalek::{Signature, VerifyingKey};

pub fn verify_ticket(
    ticket: &SignedTicket,
    issuer: &VerifyingKey,
    host_id: &str,
    epoch: u64,
    now: i64,
) -> Result<TicketClaims> {
    ensure!(
        ticket.claims.len() <= 16_384 && ticket.signature.len() <= 128,
        "oversized admission ticket"
    );
    let bytes = URL_SAFE_NO_PAD.decode(&ticket.claims)?;
    let signature = Signature::from_slice(&URL_SAFE_NO_PAD.decode(&ticket.signature)?)?;
    issuer
        .verify_strict(&bytes, &signature)
        .context("invalid ticket signature")?;
    let claims: TicketClaims = serde_json::from_slice(&bytes)?;
    ensure!(
        claims.version == 2 && claims.host_id == host_id,
        "invalid ticket audience/version"
    );
    ensure!(claims.revocation_epoch == epoch, "revoked ticket");
    ensure!(
        claims.issued_at <= now
            && claims.expires_at > now
            && claims
                .expires_at
                .checked_sub(claims.issued_at)
                .is_some_and(|ttl| ttl > 0 && ttl <= 300),
        "expired or invalid ticket lifetime"
    );
    ensure!(
        !claims.ticket_id.is_empty()
            && !claims.principal_id.is_empty()
            && !claims.device_id.is_empty(),
        "missing admission identity"
    );
    Ok(claims)
}

pub fn verify_device_proof(claims: &TicketClaims, nonce: &str, proof: &str) -> Result<()> {
    ensure!(
        !nonce.is_empty()
            && nonce.len() <= 256
            && !nonce.contains('\n')
            && !claims.ticket_id.contains('\n'),
        "invalid proof nonce"
    );
    ensure!(proof.len() <= 128, "oversized device proof");
    let key: [u8; 32] = URL_SAFE_NO_PAD
        .decode(&claims.device_public_key)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid device public key"))?;
    let signature = Signature::from_slice(&URL_SAFE_NO_PAD.decode(proof)?)?;
    VerifyingKey::from_bytes(&key)?
        .verify_strict(
            format!("borg.remote.auth.v2\n{}\n{}", claims.ticket_id, nonce).as_bytes(),
            &signature,
        )
        .context("invalid device proof")
}

pub fn authorize_scope(
    claims: &TicketClaims,
    session_id: Option<&str>,
    operation: bool,
) -> Result<()> {
    ensure!(
        !operation || claims.permission == Permission::Control,
        "read-only admission"
    );
    ensure!(
        claims.session_ids.is_empty()
            || session_id.is_some_and(|id| claims.session_ids.iter().any(|allowed| allowed == id)),
        "scope outside ticket authority"
    );
    Ok(())
}

/// Direct admission consumes once before allowing any viewer operation. The
/// unique key survives process restart; expired records can safely be pruned.
pub async fn consume_ticket(pool: &sqlx::PgPool, claims: &TicketClaims, now: i64) -> Result<()> {
    ensure!(claims.expires_at > now, "expired admission ticket");
    sqlx::query("create table if not exists remote_admission_tickets(host_id text not null,ticket_id text not null,expires_at bigint not null,primary key(host_id,ticket_id))").execute(pool).await?;
    sqlx::query("delete from remote_admission_tickets where expires_at <= $1")
        .bind(now)
        .execute(pool)
        .await?;
    let inserted =
        sqlx::query("insert into remote_admission_tickets values($1,$2,$3) on conflict do nothing")
            .bind(&claims.host_id)
            .bind(&claims.ticket_id)
            .bind(claims.expires_at)
            .execute(pool)
            .await?
            .rows_affected();
    ensure!(inserted == 1, "admission ticket already consumed");
    Ok(())
}

pub fn issuer_key(encoded: &str) -> Result<VerifyingKey> {
    let bytes: [u8; 32] = URL_SAFE_NO_PAD
        .decode(encoded)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid relay public key"))?;
    Ok(VerifyingKey::from_bytes(&bytes)?)
}

pub fn verify_service_ticket(
    ticket: &SignedTicket,
    issuer: &VerifyingKey,
    host_id: &str,
    epoch: u64,
    now: i64,
) -> Result<crate::auth::ServiceTicketClaims> {
    ensure!(
        ticket.claims.len() <= 16_384 && ticket.signature.len() <= 128,
        "oversized service admission"
    );
    let bytes = URL_SAFE_NO_PAD.decode(&ticket.claims)?;
    let signature = Signature::from_slice(&URL_SAFE_NO_PAD.decode(&ticket.signature)?)?;
    issuer
        .verify_strict(&bytes, &signature)
        .context("invalid service ticket signature")?;
    let claims: crate::auth::ServiceTicketClaims = serde_json::from_slice(&bytes)?;
    ensure!(
        claims.version == 2
            && claims.host_id == host_id
            && claims.revocation_epoch == epoch
            && claims.purpose == crate::auth::ServicePurpose::ProductView,
        "invalid service authority"
    );
    ensure!(
        claims.issued_at <= now
            && claims.expires_at > now
            && claims
                .expires_at
                .checked_sub(claims.issued_at)
                .is_some_and(|ttl| ttl > 0 && ttl <= 300),
        "invalid service ticket lifetime"
    );
    ensure!(
        !claims.ticket_id.is_empty()
            && !claims.principal_id.is_empty()
            && uuid::Uuid::parse_str(&claims.session_id).is_ok(),
        "invalid service identity/scope"
    );
    Ok(claims)
}
pub fn verify_service_proof(
    claims: &crate::auth::ServiceTicketClaims,
    nonce: &str,
    proof: &str,
) -> Result<()> {
    ensure!(
        !nonce.is_empty()
            && nonce.len() <= 256
            && !nonce.contains('\n')
            && !claims.ticket_id.contains('\n')
            && proof.len() <= 128,
        "invalid service proof nonce"
    );
    let key = issuer_key(&claims.service_public_key)?;
    let signature = Signature::from_slice(&URL_SAFE_NO_PAD.decode(proof)?)?;
    key.verify_strict(
        format!("borg.remote.service.v2\n{}\n{}", claims.ticket_id, nonce).as_bytes(),
        &signature,
    )
    .context("invalid service proof")
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};

    // Compiler checks cannot guard audience/expiry/scope enforcement or prove
    // modified signed claims are refused: these are admission security contracts.
    #[test]
    fn signed_admission_rejects_tampering_expiry_wrong_host_and_scope() {
        let signer = SigningKey::from_bytes(&[7; 32]);
        let claims = TicketClaims {
            version: 2,
            ticket_id: "t".into(),
            principal_id: "p".into(),
            device_id: "d".into(),
            host_id: "h".into(),
            session_ids: vec!["s".into()],
            permission: Permission::Read,
            device_public_key: URL_SAFE_NO_PAD.encode(signer.verifying_key().as_bytes()),
            issued_at: 100,
            expires_at: 200,
            revocation_epoch: 3,
        };
        let bytes = serde_json::to_vec(&claims).unwrap();
        let mut ticket = SignedTicket {
            claims: URL_SAFE_NO_PAD.encode(&bytes),
            signature: URL_SAFE_NO_PAD.encode(signer.sign(&bytes).to_bytes()),
        };
        let accepted = verify_ticket(&ticket, &signer.verifying_key(), "h", 3, 150).unwrap();
        assert!(authorize_scope(&accepted, Some("s"), false).is_ok());
        assert!(authorize_scope(&accepted, Some("other"), false).is_err());
        assert!(authorize_scope(&accepted, Some("s"), true).is_err());
        assert!(verify_ticket(&ticket, &signer.verifying_key(), "other", 3, 150).is_err());
        assert!(verify_ticket(&ticket, &signer.verifying_key(), "h", 4, 150).is_err());
        assert!(verify_ticket(&ticket, &signer.verifying_key(), "h", 3, 200).is_err());
        ticket.claims = URL_SAFE_NO_PAD.encode(b"{}");
        assert!(verify_ticket(&ticket, &signer.verifying_key(), "h", 3, 150).is_err());
    }
}
