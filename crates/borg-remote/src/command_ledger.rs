//! Durable viewer/owner command admission. Started effects are never implicitly
//! replayed; prompt retries use canonical prompt admission with the same UUID.
use anyhow::{Result, ensure};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use uuid::Uuid;

/// Answer receipts must not be offline password/PIN verifiers. The relay's
/// private authority keys this digest; raw answers and unkeyed hashes are not
/// persisted. A token rotation deliberately makes old answer retries fail closed.
pub(crate) fn private_answer_intent(encoded: &[u8], authority: &str) -> Result<Vec<u8>> {
    use hmac::{Hmac, KeyInit, Mac};
    ensure!(
        !authority.is_empty(),
        "private answer authority is unavailable"
    );
    let mut mac =
        Hmac::<Sha256>::new_from_slice(authority.as_bytes()).expect("HMAC accepts any key length");
    mac.update(b"borg:private-answer-intent:v1\0");
    mac.update(encoded);
    let mut intent = b"private-answer-v1:".to_vec();
    intent.extend_from_slice(&mac.finalize().into_bytes());
    Ok(intent)
}

pub enum Admission {
    New,
    InDoubt,
    Terminal(Vec<u8>),
}
#[derive(Clone)]
pub struct CommandLedger {
    pool: PgPool,
}
impl CommandLedger {
    pub async fn open(pool: PgPool) -> Result<Self> {
        sqlx::query("create table if not exists remote_command_receipts(host_id uuid not null,command_id uuid not null,scope text not null,principal text not null,intent bytea not null,result bytea,primary key(host_id,command_id))").execute(&pool).await?;
        sqlx::query("create table if not exists remote_prompt_commands(host_id uuid not null,command_id uuid not null,session_id uuid not null,message_id uuid not null,primary key(host_id,command_id),foreign key(host_id,command_id) references remote_command_receipts(host_id,command_id))").execute(&pool).await?;
        sqlx::query("create index if not exists remote_prompt_message on remote_prompt_commands(host_id,session_id,message_id)").execute(&pool).await?;
        Ok(Self { pool })
    }
    pub async fn admit(
        &self,
        host: Uuid,
        id: Uuid,
        scope: &str,
        principal: &str,
        encoded_command: &[u8],
    ) -> Result<Admission> {
        ensure!(
            !principal.is_empty()
                && principal.len() <= 1024
                && !scope.is_empty()
                && scope.len() <= 1024
                && encoded_command.len() <= 1_048_576,
            "invalid command admission"
        );
        let digest = Sha256::digest(encoded_command).to_vec();
        let mut tx = self.pool.begin().await?;
        let changed=sqlx::query("insert into remote_command_receipts(host_id,command_id,scope,principal,intent) values($1,$2,$3,$4,$5) on conflict do nothing")
            .bind(host).bind(id).bind(scope).bind(principal).bind(&digest).execute(&mut *tx).await?.rows_affected();
        if changed == 1 {
            tx.commit().await?;
            return Ok(Admission::New);
        }
        let row=sqlx::query("select scope,principal,intent,result from remote_command_receipts where host_id=$1 and command_id=$2")
            .bind(host).bind(id).fetch_one(&mut *tx).await?;
        ensure!(
            row.try_get::<String, _>("scope")? == scope
                && row.try_get::<String, _>("principal")? == principal
                && row.try_get::<Vec<u8>, _>("intent")? == digest,
            "command id reused for a different intent/authority"
        );
        let result: Option<Vec<u8>> = row.try_get("result")?;
        tx.commit().await?;
        Ok(match result {
            Some(result) => Admission::Terminal(result),
            None => Admission::InDoubt,
        })
    }
    pub async fn bind_prompt(
        &self,
        host: Uuid,
        command: Uuid,
        session: Uuid,
        message: Uuid,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "insert into remote_prompt_commands values($1,$2,$3,$4) on conflict do nothing",
        )
        .bind(host)
        .bind(command)
        .bind(session)
        .bind(message)
        .execute(&mut *tx)
        .await?;
        let pair:(Uuid,Uuid)=sqlx::query_as("select session_id,message_id from remote_prompt_commands where host_id=$1 and command_id=$2")
            .bind(host).bind(command).fetch_one(&mut *tx).await?;
        ensure!(
            pair == (session, message),
            "command prompt identity conflict"
        );
        tx.commit().await?;
        Ok(())
    }
    pub async fn prompt_commands(
        &self,
        host: Uuid,
        session: Uuid,
        message: Uuid,
    ) -> Result<Vec<Uuid>> {
        Ok(sqlx::query_scalar("select command_id from remote_prompt_commands where host_id=$1 and session_id=$2 and message_id=$3 order by command_id limit 128")
            .bind(host).bind(session).bind(message).fetch_all(&self.pool).await?)
    }
    pub async fn prompt_identity(&self, host: Uuid, command: Uuid) -> Result<Option<(Uuid, Uuid)>> {
        Ok(sqlx::query_as("select session_id,message_id from remote_prompt_commands where host_id=$1 and command_id=$2")
            .bind(host).bind(command).fetch_optional(&self.pool).await?)
    }
    pub async fn terminal(
        &self,
        host: Uuid,
        id: Uuid,
        scope: &str,
        principal: &str,
        encoded_command: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let digest = Sha256::digest(encoded_command).to_vec();
        let row=sqlx::query("select scope,principal,intent,result from remote_command_receipts where host_id=$1 and command_id=$2")
            .bind(host).bind(id).fetch_optional(&self.pool).await?;
        let Some(row) = row else { return Ok(None) };
        ensure!(
            row.try_get::<String, _>("scope")? == scope
                && row.try_get::<String, _>("principal")? == principal
                && row.try_get::<Vec<u8>, _>("intent")? == digest,
            "command id reused for a different intent/authority"
        );
        Ok(row.try_get("result")?)
    }
    pub async fn finish(&self, host: Uuid, id: Uuid, result: &[u8]) -> Result<()> {
        ensure!(result.len() <= 1_048_576, "command result exceeds budget");
        let changed=sqlx::query("update remote_command_receipts set result=$3 where host_id=$1 and command_id=$2 and result is null")
            .bind(host).bind(id).bind(result).execute(&self.pool).await?.rows_affected();
        if changed == 0 {
            let existing: Option<Option<Vec<u8>>> = sqlx::query_scalar(
                "select result from remote_command_receipts where host_id=$1 and command_id=$2",
            )
            .bind(host)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
            ensure!(
                existing.flatten().as_deref() == Some(result),
                "different terminal result or admission missing"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod private_answer_tests {
    use super::*;
    #[test]
    fn answer_intents_require_private_authority_and_do_not_store_plain_hashes() {
        let input = br#"{"answer":"1234"}"#;
        let intent = private_answer_intent(input, "test-authority").unwrap();
        assert_eq!(
            intent,
            private_answer_intent(input, "test-authority").unwrap()
        );
        assert_ne!(
            intent,
            private_answer_intent(input, "other-authority").unwrap()
        );
        assert_ne!(
            intent,
            private_answer_intent(br#"{"answer":"1235"}"#, "test-authority").unwrap()
        );
        assert!(!intent.windows(4).any(|bytes| bytes == b"1234"));
        assert!(private_answer_intent(input, "").is_err());
    }
}
