//! Durable viewer/owner command admission. Started effects are never implicitly
//! replayed; prompt retries use canonical prompt admission with the same UUID.
use anyhow::{Result, ensure};
use sqlx::{PgPool, Row};
use sha2::{Digest,Sha256};
use uuid::Uuid;

pub enum Admission { New, InDoubt, Terminal(Vec<u8>) }
#[derive(Clone)]
pub struct CommandLedger { pool: PgPool }
impl CommandLedger {
    pub async fn open(pool: PgPool) -> Result<Self> {
        sqlx::query("create table if not exists remote_command_receipts(host_id uuid not null,command_id uuid not null,scope text not null,principal text not null,intent bytea not null,result bytea,primary key(host_id,command_id))").execute(&pool).await?;
        Ok(Self{pool})
    }
    pub async fn admit(&self,host:Uuid,id:Uuid,scope:&str,principal:&str,encoded_command:&[u8])->Result<Admission> {
        ensure!(!principal.is_empty() && principal.len()<=1024 && !scope.is_empty() && scope.len()<=1024 && encoded_command.len()<=1_048_576,"invalid command admission");
        let digest=Sha256::digest(encoded_command).to_vec();
        let mut tx=self.pool.begin().await?;
        let changed=sqlx::query("insert into remote_command_receipts(host_id,command_id,scope,principal,intent) values($1,$2,$3,$4,$5) on conflict do nothing")
            .bind(host).bind(id).bind(scope).bind(principal).bind(&digest).execute(&mut *tx).await?.rows_affected();
        if changed==1 { tx.commit().await?; return Ok(Admission::New) }
        let row=sqlx::query("select scope,principal,intent,result from remote_command_receipts where host_id=$1 and command_id=$2")
            .bind(host).bind(id).fetch_one(&mut *tx).await?;
        ensure!(row.try_get::<String,_>("scope")?==scope && row.try_get::<String,_>("principal")?==principal && row.try_get::<Vec<u8>,_>("intent")?==digest,"command id reused for a different intent/authority");
        let result:Option<Vec<u8>>=row.try_get("result")?;
        tx.commit().await?;
        Ok(match result { Some(result)=>Admission::Terminal(result), None=>Admission::InDoubt })
    }
    pub async fn finish(&self,host:Uuid,id:Uuid,result:&[u8])->Result<()> {
        ensure!(result.len()<=1_048_576,"command result exceeds budget");
        let changed=sqlx::query("update remote_command_receipts set result=$3 where host_id=$1 and command_id=$2 and result is null")
            .bind(host).bind(id).bind(result).execute(&self.pool).await?.rows_affected();
        ensure!(changed==1,"command result already completed or admission missing");
        Ok(())
    }
}
