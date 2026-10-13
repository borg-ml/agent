use super::*;
use borg_view::wire::{SessionHeadline,HostOp,AttentionKind};
use crate::projection::ProjectionCheckpoint;

impl SyncAuthority {
    pub(super) async fn initialize_host_index(&self)->Result<()> {
        sqlx::query("create table if not exists remote_host_headlines(scope text not null,session_id uuid not null,headline bytea not null,primary key(scope,session_id))").execute(&self.pg.pool).await?;
        let key=scope_key(self.host,None);
        if self.projection.load(&key).await?.is_none() {
            let state=ProjectionCheckpoint{epoch:Uuid::new_v4().to_string(),revision:0,source_sequence:0,source_live_revision:0,reducer:Vec::new()};
            self.projection.initialize(&key,&state).await?;
        }
        Ok(())
    }
    pub(super) async fn publish_headline(&self,session:Uuid)->Result<()> {
        let parent:Option<Uuid>=sqlx::query_scalar("select owner_session_id from sessions where id=$1").bind(session).fetch_one(&self.pg.pool).await?;
        let state=self.store.state(session).await?;
        let mut attention=Vec::new();
        if state.pending_approval_id.is_some(){attention.push(AttentionKind::Approval);}
        if state.pending_provider_interaction_id.is_some(){attention.push(AttentionKind::Question);}
        let summary=crate::SessionSummary{session_id:session,parent_session_id:None,parent_cut_sequence:None,inherited_event_count:0,state};
        let headline=SessionHeadline::from_summary(&summary,parent,attention);
        let encoded=serde_json::to_vec(&headline)?;
        let key=scope_key(self.host,None);
        let mut tx=self.pg.pool.begin().await?;
        let prior:Option<Vec<u8>>=sqlx::query_scalar("select headline from remote_host_headlines where scope=$1 and session_id=$2").bind(&key).bind(session).fetch_optional(&mut *tx).await?;
        if prior.as_ref()==Some(&encoded){return Ok(())}
        let head=sqlx::query("select epoch,revision from remote_view_checkpoints where scope=$1 for update").bind(&key).fetch_one(&mut *tx).await?;
        use sqlx::Row;
        let epoch:String=head.try_get("epoch")?;
        let revision:i64=head.try_get("revision")?;
        let next=revision.checked_add(1).ok_or_else(||anyhow::anyhow!("host revision exhausted"))?;
        let base=Cursor{epoch:epoch.clone(),revision:revision.to_string()};
        let cursor=Cursor{epoch,revision:next.to_string()};
        let frame=ServerFrame::HostPatch{scope:Scope::Host,base,cursor:cursor.clone(),ops:vec![HostOp::SessionUpsert{session:headline.clone()}]};
        let bytes=serde_json::to_vec(&frame)?;
        sqlx::query("insert into remote_host_headlines values($1,$2,$3) on conflict(scope,session_id) do update set headline=excluded.headline").bind(&key).bind(session).bind(encoded).execute(&mut *tx).await?;
        sqlx::query("update remote_view_checkpoints set revision=$2 where scope=$1").bind(&key).bind(next).execute(&mut *tx).await?;
        sqlx::query("insert into remote_view_replay values($1,$2,$3,$4)").bind(&key).bind(&cursor.epoch).bind(next).bind(bytes).execute(&mut *tx).await?;
        sqlx::query("delete from remote_view_replay where scope=$1 and revision in(select revision from(select revision,row_number() over(order by revision desc)n,sum(octet_length(frame))over(order by revision desc)bytes from remote_view_replay where scope=$1)r where n>128 or bytes>1048576)").bind(&key).execute(&mut *tx).await?;
        tx.commit().await?;
        let mut connections=self.connections.lock().await;
        for (id,conn) in connections.iter_mut(){
            if let Some(delivery)=conn.subscriptions.get_mut(&key){
                let filtered=if conn.claims.allows(session){frame.clone()}else{match &frame {ServerFrame::HostPatch{scope,base,cursor,..}=>ServerFrame::HostPatch{scope:scope.clone(),base:base.clone(),cursor:cursor.clone(),ops:Vec::new()},_=>unreachable!()}};
                self.emit_scope(id,delivery,filtered)?;
            }
        }
        Ok(())
    }
    pub(super) async fn host_page(&self,claims:&ConnectionClaims,before:Option<Uuid>)->Result<Vec<SessionHeadline>> {
        let key=scope_key(self.host,None);
        let allow=claims.allowlist()?;
        let rows:Vec<Vec<u8>>=sqlx::query_scalar("select headline from remote_host_headlines where scope=$1 and($2::uuid is null or session_id<$2) and($3::uuid[] is null or session_id=any($3)) order by session_id desc limit 64")
            .bind(key).bind(before).bind(allow).fetch_all(&self.pg.pool).await?;
        rows.into_iter().map(|row|Ok(serde_json::from_slice(&row)?)).collect()
    }
    pub(super) async fn subscribe_host(&self,id:&str,claims:&ConnectionClaims,after:Option<Cursor>)->Result<()> {
        self.authorize(claims,&Scope::Host,false).await?;
        let key=scope_key(self.host,None);
        let mut connections=self.connections.lock().await;
        let conn=connections.get_mut(id).ok_or_else(||anyhow::anyhow!("viewer closed"))?;
        let row=sqlx::query("select c.epoch,c.revision,(select coalesce(jsonb_agg(convert_from(page.headline,'UTF8')::jsonb),'[]'::jsonb) from(select headline from remote_host_headlines where scope=$1 and($2::uuid[] is null or session_id=any($2)) order by session_id desc limit 64)page)sessions from remote_view_checkpoints c where c.scope=$1")
            .bind(&key).bind(claims.allowlist()?).fetch_one(&self.pg.pool).await?;
        use sqlx::Row;
        let cursor=Cursor{epoch:row.try_get("epoch")?,revision:row.try_get::<i64,_>("revision")?.to_string()};
        let sessions:Vec<SessionHeadline>=serde_json::from_value(row.try_get("sessions")?)?;
        let history_anchor=sessions.last().map(|s|s.session_id.clone());
        let initial=after.filter(|after|after.epoch==cursor.epoch).unwrap_or(Cursor{epoch:cursor.epoch.clone(),revision:"0".into()});
        let mut delivery=Delivery::new(initial);
        self.emit_scope(id,&mut delivery,ServerFrame::HostSnapshot{scope:Scope::Host,cursor:cursor.clone(),sessions,history_anchor})?;
        conn.subscriptions.insert(key,delivery);
        self.emit(id,ServerFrame::Synchronized{scope:Scope::Host,cursor})?;
        Ok(())
    }
}
