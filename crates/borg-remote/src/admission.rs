//! Cryptographic admission checks shared by direct and relay connections.
use anyhow::{Result, ensure, Context};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use ed25519_dalek::{Signature, VerifyingKey};
use crate::auth::{SignedTicket, TicketClaims, Permission};

pub fn verify_ticket(ticket: &SignedTicket, issuer: &VerifyingKey, host_id: &str, epoch: u64, now: i64) -> Result<TicketClaims> {
    ensure!(ticket.claims.len() <= 16_384 && ticket.signature.len() <= 128, "oversized admission ticket");
    let bytes = URL_SAFE_NO_PAD.decode(&ticket.claims)?;
    let signature = Signature::from_slice(&URL_SAFE_NO_PAD.decode(&ticket.signature)?)?;
    issuer.verify_strict(&bytes, &signature).context("invalid ticket signature")?;
    let claims: TicketClaims = serde_json::from_slice(&bytes)?;
    ensure!(claims.version == 2 && claims.host_id == host_id, "invalid ticket audience/version");
    ensure!(claims.revocation_epoch == epoch, "revoked ticket");
    ensure!(claims.issued_at <= now && claims.expires_at > now && claims.expires_at.checked_sub(claims.issued_at).is_some_and(|ttl| ttl > 0 && ttl <= 300), "expired or invalid ticket lifetime");
    ensure!(!claims.ticket_id.is_empty() && !claims.principal_id.is_empty() && !claims.device_id.is_empty(), "missing admission identity");
    Ok(claims)
}

pub fn verify_device_proof(claims: &TicketClaims, nonce: &str, proof: &str) -> Result<()> {
    ensure!(!nonce.is_empty() && nonce.len() <= 256 && !nonce.contains('\n') && !claims.ticket_id.contains('\n'), "invalid proof nonce");
    ensure!(proof.len() <= 128, "oversized device proof");
    let key: [u8;32] = URL_SAFE_NO_PAD.decode(&claims.device_public_key)?.try_into().map_err(|_| anyhow::anyhow!("invalid device public key"))?;
    let signature = Signature::from_slice(&URL_SAFE_NO_PAD.decode(proof)?)?;
    VerifyingKey::from_bytes(&key)?.verify_strict(format!("borg.remote.auth.v2\n{}\n{}", claims.ticket_id, nonce).as_bytes(), &signature).context("invalid device proof")
}

pub fn authorize_scope(claims: &TicketClaims, session_id: Option<&str>, operation: bool) -> Result<()> {
    ensure!(!operation || claims.permission == Permission::Control, "read-only admission");
    ensure!(claims.session_ids.is_empty() || session_id.is_some_and(|id| claims.session_ids.iter().any(|allowed| allowed == id)), "scope outside ticket authority");
    Ok(())
}

/// Direct admission consumes once before allowing any viewer operation. The
/// unique key survives process restart; expired records can safely be pruned.
pub async fn consume_ticket(pool: &sqlx::PgPool, claims: &TicketClaims, now: i64) -> Result<()> {
    ensure!(claims.expires_at > now, "expired admission ticket");
    sqlx::query("create table if not exists remote_admission_tickets(host_id text not null,ticket_id text not null,expires_at bigint not null,primary key(host_id,ticket_id))").execute(pool).await?;
    let inserted = sqlx::query("insert into remote_admission_tickets values($1,$2,$3) on conflict do nothing")
        .bind(&claims.host_id).bind(&claims.ticket_id).bind(claims.expires_at).execute(pool).await?.rows_affected();
    ensure!(inserted == 1, "admission ticket already consumed");
    Ok(())
}
