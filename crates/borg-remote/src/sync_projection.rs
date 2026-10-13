use super::*;
use borg_view::{ViewReducer,wire::{ViewOp,RowBody}};
use crate::projection::ProjectionCheckpoint;

pub(super) struct Projector {pub reducer:ViewReducer,pub state:ProjectionCheckpoint,text_heads:HashMap<(String,String),(String,usize)>}
fn field_name(field:&borg_view::checkpoint::CheckpointTextField)->Result<String> {
    Ok(serde_json::to_value(field)?.as_str().ok_or_else(||anyhow::anyhow!("invalid text field"))?.to_owned())
}
fn preview(text:&mut String) {
    if text.len()>4096 {let mut end=4096;while !text.is_char_boundary(end) {end-=1;}text.truncate(end);}
}
impl SyncAuthority {
    pub(super) async fn projector(&self,session:Uuid)->Result<Projector> {
        let key=scope_key(self.host,Some(session));
        if let Some(state)=self.projection.load(&key).await? {
            let refs=ViewReducer::checkpoint_text_refs(&state.reducer)?;
            let mut resolved=HashMap::new();
            let mut text_heads=HashMap::new();
            for reference in refs {
                let field=field_name(&reference.field)?;
                let text=crate::row_index::resolve_text(&self.pg.pool,&key,&reference.row_id,&field,&reference.revision,reference.bytes).await?;
                text_heads.insert((reference.row_id.clone(),field.clone()),(reference.revision,reference.bytes));
                resolved.insert((reference.row_id,field),text);
            }
            let reducer=ViewReducer::restore(&state.reducer,|reference| resolved.remove(&(reference.row_id.clone(),field_name(&reference.field)?)).ok_or_else(||anyhow::anyhow!("unresolved checkpoint body")))?;
            return Ok(Projector{reducer,state,text_heads})
        }
        let mut reducer=ViewReducer::new(session,Uuid::new_v4().to_string());
        let state=ProjectionCheckpoint {epoch:reducer.cursor().epoch.clone(),revision:0,source_sequence:0,source_live_revision:0,reducer:reducer.checkpoint()?};
        ensure!(self.projection.initialize(&key,&state).await?,"concurrent projection initializer");
        Ok(Projector{reducer,state,text_heads:HashMap::new()})
    }
    pub(super) async fn project(&self,projector:&mut Projector,event:crate::SessionEvent,live_revision:Option<u64>)->Result<()> {
        let base=projector.reducer.cursor().clone();
        let sequence=event.sequence;
        let event_id=event.id;
        let source_session=event.session_id;
        let turn=match &event.kind {
            crate::SessionEventKind::TurnStarted{message_id,..}=>Some((*message_id,None)),
            crate::SessionEventKind::TurnCompleted{message_id,provider_session_id,final_text,error}=>Some((*message_id,Some((provider_session_id.clone(),final_text.clone(),error.clone())))),
            _=>None,
        };
        let attention_event=event.kind.clone();
        let ops=projector.reducer.apply(event);
        self.persist_ops(projector,source_session,event_id,sequence,live_revision,base,ops,turn).await?;
        self.publish_attention(source_session,&attention_event).await
    }
    async fn publish_attention(&self,source:Uuid,event:&crate::SessionEventKind)->Result<()> {
        use borg_view::wire::AttentionKind;
        let scope=Scope::Session{session_id:source.to_string()};
        let state=self.store.state(source).await?;
        let mut frames=Vec::new();
        match event {
            crate::SessionEventKind::ApprovalRequested{title,..}=>frames.push(ServerFrame::Attention{scope:scope.clone(),source_session_id:source.to_string(),kind:AttentionKind::Approval,label:title.clone()}),
            crate::SessionEventKind::ProviderInteractionRequested{title,..}=>frames.push(ServerFrame::Attention{scope:scope.clone(),source_session_id:source.to_string(),kind:AttentionKind::Question,label:title.clone()}),
            crate::SessionEventKind::ApprovalResolved{..} if state.pending_approval_id.is_none()=>frames.push(ServerFrame::AttentionCleared{scope:scope.clone(),source_session_id:source.to_string(),kind:AttentionKind::Approval}),
            crate::SessionEventKind::ProviderInteractionResolved{..} if state.pending_provider_interaction_id.is_none()=>frames.push(ServerFrame::AttentionCleared{scope:scope.clone(),source_session_id:source.to_string(),kind:AttentionKind::Question}),
            crate::SessionEventKind::TurnCompleted{error,..}=>frames.push(ServerFrame::Attention{scope:scope.clone(),source_session_id:source.to_string(),kind:if error.is_some(){AttentionKind::Error}else{AttentionKind::Done},label:if error.is_some(){"Turn failed".into()}else{"Turn complete".into()}}),
            crate::SessionEventKind::TurnStarted{..}=>for kind in [AttentionKind::Done,AttentionKind::Error] {frames.push(ServerFrame::AttentionCleared{scope:scope.clone(),source_session_id:source.to_string(),kind});},
            _=>{}
        }
        if frames.is_empty(){return Ok(())}
        let key=scope_key(self.host,Some(source));
        let connections=self.connections.lock().await;
        for(id,connection)in connections.iter(){
            if connection.subscriptions.contains_key(&key) {
                for frame in &frames {self.emit(id,frame.clone())?;}
            }
        }
        Ok(())
    }
    async fn persist_ops(&self,projector:&mut Projector,source_session:Uuid,event_id:Uuid,sequence:u64,live_revision:Option<u64>,base:Cursor,ops:Vec<ViewOp>,turn:Option<(Uuid,Option<(Option<String>,String,Option<String>)>)>)->Result<()> {
        let scope=Scope::Session{session_id:source_session.to_string()};
        let key=scope_key(self.host,Some(source_session));
        let cursor=projector.reducer.cursor().clone();
        let closed_rows=ops.iter().filter_map(|op|match op {
            ViewOp::RowUpsert{row}=>match &row.body {
                RowBody::Message{status,..}|RowBody::Reasoning{status,..}|RowBody::Tool{status,..}|RowBody::Diff{status,..} if matches!(status,borg_view::wire::Lifecycle::Complete|borg_view::wire::Lifecycle::Stopped|borg_view::wire::Lifecycle::Failed)=>Some(row.id.as_str()),
                _=>None,
            },_=>None,
        }).collect::<std::collections::HashSet<_>>();
        let checkpoint=projector.reducer.checkpoint()?;
        let references=projector.reducer.checkpoint_texts();
        let text_fields=references.iter().map(|(reference,_)|field_name(&reference.field)).collect::<Result<Vec<_>>>()?;
        let texts=references.iter().zip(&text_fields).filter(|((r,_),field)|projector.text_heads.get(&(r.row_id.clone(),(*field).clone()))!=Some(&(r.revision.clone(),r.bytes))).map(|((r,text),field)|(r.row_id.as_str(),field.as_str(),r.revision.as_str(),*text,closed_rows.contains(r.row_id.as_str()))).collect();
        let next_heads=references.iter().zip(&text_fields).map(|((r,_),field)|((r.row_id.clone(),field.clone()),(r.revision.clone(),r.bytes))).collect();
        let mut metadata=Vec::new();
        for op in &ops {
            if let ViewOp::RowUpsert{row}=op {
                let mut row=row.clone();
                match &mut row.body {
                    RowBody::Message{text,..}|RowBody::Reasoning{text,..}=>text.clear(),
                    RowBody::Tool{input,output_preview,..}=>{if let Some(text)=input {preview(text);}if let Some(text)=output_preview {preview(text);}}
                    RowBody::TeamReport{text,..}=>text.clear(),
                    RowBody::Diff{patch,..}=>patch.clear(),
                    RowBody::ActionGroup{actions,..}=>for action in actions {
                        use borg_view::wire::ActionRow;
                        match action {
                            ActionRow::Lifecycle{body,..}=>if let Some(text)=body {preview(text);},
                            ActionRow::Information{text,..}|ActionRow::Activity{text}=>preview(text),
                            ActionRow::Compaction{summary,..}=>preview(summary),
                            _=>{}
                        }
                    },
                    _=>{}
                }
                let position=row.history_before.split(':').collect::<Vec<_>>();
                let source=position.first().and_then(|p|p.parse::<i64>().ok()).unwrap_or(sequence as i64);
                let ordinal=position.get(1).and_then(|p|p.parse::<i64>().ok()).unwrap_or(0);
                metadata.push((row.id.clone(),row.revision.parse::<i64>()?,source,ordinal,row.history_before.clone(),serde_json::to_vec(&row)?));
            }
        }
        let mut rows=metadata.iter().map(|(id,revision,source,ordinal,anchor,body)|crate::row_index::RowMutation::Upsert{id,revision:*revision,source_sequence:*source,ordinal:*ordinal,anchor,metadata:body}).collect::<Vec<_>>();
        for op in &ops {match op {
            ViewOp::RowRemove{id}=>rows.push(crate::row_index::RowMutation::Remove{id}),
            ViewOp::RowAppend{id,expected_revision,revision,offset,text,..}=>rows.push(crate::row_index::RowMutation::Advance{id,expected_revision:expected_revision.parse()?,revision:revision.parse()?,bytes:offset.checked_add(text.len()).ok_or_else(||anyhow::anyhow!("row text exhausted"))?}),
            _=>{}
        }}
        let mut frame=ServerFrame::Patch{scope:scope.clone(),base,cursor:cursor.clone(),ops:ops.clone()};
        let mut encoded=serde_json::to_vec(&frame)?;
        if encoded.len()>1_048_576 {frame=ServerFrame::Snapshot{scope:scope.clone(),view:projector.reducer.snapshot()};encoded=serde_json::to_vec(&frame)?;}
        let state=ProjectionCheckpoint {epoch:cursor.epoch.clone(),revision:cursor.revision.parse()?,source_sequence:projector.state.source_sequence.max(sequence as i64),source_live_revision:live_revision.map(|r|r as i64).unwrap_or(projector.state.source_live_revision),reducer:checkpoint};
        let mut turn_frames=Vec::new();
        let mut turn_encoded=None;
        let mut final_payload=None;
        if let Some((message,completion))=turn {
            for command in self.ledger.prompt_commands(self.host,Uuid::parse_str(match &scope {Scope::Session{session_id}=>session_id,Scope::Host=>unreachable!()})?,message).await? {
                let marker=match &completion {
                    None=>ServerFrame::TurnStarted{scope:scope.clone(),command_id:command.to_string(),message_id:message.to_string()},
                    Some((provider,final_text,error))=>{
                        let final_text_asset=if final_text.len()>512*1024 {
                            let (reference,payload)=assets::final_asset(source_session,event_id,final_text);
                            final_payload=Some(payload);Some(reference)
                        } else {None};
                        ServerFrame::TurnCompleted{scope:scope.clone(),command_id:command.to_string(),message_id:message.to_string(),provider_session_id:provider.clone(),final_text:if final_text_asset.is_some(){String::new()}else{final_text.clone()},final_text_asset,error:error.clone(),usage:projector.reducer.turn_usage(&message).cloned()}
                    },
                };
                turn_encoded=Some((message.to_string(),serde_json::to_vec(&marker)?));
                turn_frames.push(marker);
            }
        }
        self.projection.commit_full(&key,projector.state.revision,&state,&encoded,rows,texts,turn_encoded.as_ref().map(|(id,body)|(id.as_str(),body.as_slice())),final_payload.as_ref().map(|(id,session,event,body)|(*id,*session,*event,body.as_slice()))).await?;
        projector.state=state;
        projector.text_heads=next_heads;
        let mut connections=self.connections.lock().await;
        let mut failed=Vec::new();
        for (id,connection) in connections.iter_mut() {
            if let Some(delivery)=connection.subscriptions.get_mut(&key) {
                if self.emit_scope(id,delivery,frame.clone()).is_err() {failed.push(id.clone());continue;}
                for turn in &turn_frames {if self.emit(id,turn.clone()).is_err() {failed.push(id.clone());break;}}
            }
        }
        for id in failed {connections.remove(&id);let _=self.outbound.try_send(TunnelFrame::Close{connection_id:id,reason:"projection delivery gap".into()});}
        Ok(())
    }
}

impl SyncAuthority {
    async fn catch_up(&self,session:Uuid)->Result<()> {
        let owner=self.store.owning_root_session(session).await?.unwrap_or(session);
        if !matches!(self.store.host_launch_owner(owner).await?,Some((host,_)) if host==self.host) {return Ok(())}
        if let Some(request)=crate::host::scope_registration(self.store.as_ref(),session).await? {
            if self.relay_online.load(Ordering::Acquire) {
                if let Err(error)=crate::host::register_child(self.host,session,request).await {
                    tracing::warn!(%session,%error,"relay registration retained for reconnect; projecting local authority");
                    self.catalog_recovery.store(true,Ordering::Release);
                }
            }
        } else {return Ok(())}
        loop {
            let mut projectors=self.projectors.lock().await;
            if !projectors.contains_key(&session) {projectors.insert(session,self.projector(session).await?);}
            let projector=projectors.get_mut(&session).expect("initialized projector");
            let events=self.store.events_after(session,projector.state.source_sequence as u64,128).await?;
            let count=events.len();
            for event in events {self.project(projector,event,None).await?;}
            drop(projectors);
            if count<128 {break}
            tokio::task::yield_now().await;
        }
        loop {
            let mut projectors=self.projectors.lock().await;
            let projector=projectors.get_mut(&session).ok_or_else(||anyhow::anyhow!("scope projector missing"))?;
            let deltas=self.pg.text_deltas_after(session,projector.state.source_live_revision as u64).await?;
            let count=deltas.len();
            for (revision,event) in deltas {self.project(projector,event,Some(revision)).await?;}
            if count<128 {break}
        }
        self.lift_child_metadata(session).await?;
        self.publish_headline(session).await?;
        // Bound idle reducer memory independently from roster/thread count.
        let mut projectors=self.projectors.lock().await;
        if projectors.len()>128 {
            let connections=self.connections.lock().await;
            let idle=projectors.keys().copied().find(|id| {
                let key=scope_key(self.host,Some(*id));
                !connections.values().any(|connection|connection.subscriptions.contains_key(&key))
            });
            if let Some(id)=idle {projectors.remove(&id);}
        }
        Ok(())
    }
    async fn lift_child_metadata(&self,child:Uuid)->Result<()> {
        let parent:Option<Uuid>=sqlx::query_scalar("select owner_session_id from sessions where id=$1").bind(child).fetch_one(&self.pg.pool).await?;
        let Some(parent)=parent else{return Ok(())};
        let mut projectors=self.projectors.lock().await;
        let (child_cursor,child_meta)={let child=projectors.get(&child).ok_or_else(||anyhow::anyhow!("child projection missing"))?;(child.reducer.cursor().clone(),child.reducer.snapshot().meta)};
        if !projectors.contains_key(&parent){projectors.insert(parent,self.projector(parent).await?);}
        let projector=projectors.get_mut(&parent).expect("parent initialized");
        if !projector.reducer.core.subagent_snapshots.contains_key(&child) {
            let sequence:Option<i64>=sqlx::query_scalar("select sequence from session_events where session_id=$1 and subagent_session_id=$2 and event_kind='subagent_activity' order by sequence desc limit 1")
                .bind(parent).bind(child).fetch_optional(&self.pg.pool).await?;
            if let Some(sequence)=sequence {
                for event in self.store.events_after(parent,sequence.saturating_sub(1) as u64,1).await? {
                    if let crate::SessionEventKind::SubagentActivity{agent,..}=event.kind {
                        let status=agent.status;
                        projector.reducer.core.upsert_subagent_snapshot_with_status(&agent,status);
                    }
                }
            } else {return Ok(())} // Parent creation notification will reconcile refs.
        }
        let base=projector.reducer.cursor().clone();
        let ops=projector.reducer.apply_child_meta(child,child_cursor,&child_meta)?;
        if !ops.is_empty() {
            self.persist_ops(projector,parent,Uuid::nil(),projector.state.source_sequence as u64,None,base,ops,None).await?;
        }
        Ok(())
    }
    async fn reconcile_catalog(&self)->Result<()> {
        let mut root_after=Uuid::nil();
        loop {
            let roots:Vec<Uuid>=sqlx::query_scalar("select session_id from host_launch_owners where host_id=$1 and session_id>$2 order by session_id limit 64")
                .bind(self.host).bind(root_after).fetch_all(&self.pg.pool).await?;
            if roots.is_empty() {break}
            root_after=*roots.last().expect("nonempty page");
            let count=roots.len();
            for root in roots {
                self.catch_up(root).await?;
                let mut after=None;
                loop {
                    let ids=self.store.descendant_sessions(root,after,64).await?;
                    if ids.is_empty() {break}
                    after=ids.last().copied();
                    let count=ids.len();
                    for child in ids {self.catch_up(child).await?;}
                    if count<64 {break}
                }
            }
            if count<64 {break}
        }
        Ok(())
    }
}

pub(super) async fn run_publisher(weak:std::sync::Weak<SyncAuthority>,mut listener:sqlx::postgres::PgListener) {
    use std::sync::atomic::Ordering;
    let (hints,mut inbox)=mpsc::channel(128);
    let recovery=match weak.upgrade() {Some(authority)=>Arc::clone(&authority.catalog_recovery),None=>return};
    let reader_recovery=Arc::clone(&recovery);
    let reader_authority=weak.clone();
    let _reader=crate::session::AbortTask(crate::host::spawn_transport_task(async move {
        loop {
            match listener.try_recv().await {
                Ok(Some(notification))=>{
                    if let Ok(session)=Uuid::parse_str(notification.payload()) {
                        if hints.try_send(session).is_err() {reader_recovery.store(true,Ordering::Release);}
                    }
                }
                Ok(None)=>{reader_recovery.store(true,Ordering::Release);let _=hints.try_send(Uuid::nil());}
                Err(error)=>{
                    tracing::warn!(%error,"publisher listener lost; durable catalog recovery required");
                    reader_recovery.store(true,Ordering::Release);
                    let Some(authority)=reader_authority.upgrade() else{return};
                    match authority.pg.subscribe_committed().await {
                        Ok(reconnected)=>{listener=reconnected;let _=hints.try_send(Uuid::nil());}
                        Err(error)=>{tracing::error!(%error,"publisher reconnect failed");tokio::time::sleep(std::time::Duration::from_secs(1)).await;}
                    }
                }
            }
            if hints.is_closed() {return}
        }
    }));
    loop {
        let Some(authority)=weak.upgrade() else{return};
        if recovery.swap(false,Ordering::AcqRel) {
            if let Err(error)=authority.reconcile_catalog().await {
                tracing::error!(%error,"catalog projection recovery failed; retrying durable cursors");
                recovery.store(true,Ordering::Release);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
        }
        let wake=authority.publisher_wake.notified();
        let session=tokio::select! {
            session=inbox.recv()=>match session {Some(session)=>session,None=>return},
            _=wake=>continue,
        };
        drop(authority);
        if session.is_nil() {continue}
        let Some(authority)=weak.upgrade() else{return};
        if let Err(error)=authority.catch_up(session).await {
            tracing::error!(%session,%error,"projection commit failed; durable cursor retained");
            authority.projectors.lock().await.remove(&session);
            recovery.store(true,Ordering::Release);
        }
    }
}

impl SyncAuthority {
    pub(super) async fn subscribe(&self,id:&str,claims:&ConnectionClaims,subscription:borg_view::wire::Subscription)->Result<()> {
        if matches!(subscription.scope,Scope::Host){return self.subscribe_host(id,claims,subscription.after).await}
        let session=self.authorize(claims,&subscription.scope,false).await?.ok_or_else(||anyhow::anyhow!("session scope required"))?;
        let key=scope_key(self.host,Some(session));
        // Publisher and subscription both lock projector before connections.
        // Register before reading tail, while publication for this scope is held.
        let mut projectors=self.projectors.lock().await;
        if !projectors.contains_key(&session) {projectors.insert(session,self.projector(session).await?);}
        let (cursor,snapshot)={
            let projector=projectors.get(&session).expect("initialized projector");
            (projector.reducer.cursor().clone(),projector.reducer.snapshot())
        };
        let mut connections=self.connections.lock().await;
        let connection=connections.get_mut(id).ok_or_else(||anyhow::anyhow!("viewer closed"))?;
        ensure!(connection.subscriptions.len()<16 || connection.subscriptions.contains_key(&key),"subscription capacity");
        let initial=subscription.after.clone().filter(|after|after.epoch==cursor.epoch).unwrap_or(Cursor{epoch:cursor.epoch.clone(),revision:"0".into()});
        connection.subscriptions.insert(key.clone(),Delivery::new(initial));
        let delivery=connection.subscriptions.get_mut(&key).expect("registered subscription");
        let replay=if let Some(after)=subscription.after {self.projection.replay(&key,&after.epoch,after.revision.parse()?).await?} else {None};
        match replay {
            Some(frames)=>for frame in frames {self.emit_scope(id,delivery,serde_json::from_slice(&frame)?)?;},
            None=>self.emit_scope(id,delivery,ServerFrame::Snapshot{scope:subscription.scope.clone(),view:snapshot})?,
        }
        let source_head=self.store.state(session).await?.latest_sequence;
        let indexed_head=projectors.get(&session).expect("initialized projector").state.source_sequence as u64;
        if indexed_head>=source_head {self.emit(id,ServerFrame::Synchronized{scope:subscription.scope,cursor})?;}
        Ok(())
    }
    pub(super) async fn history(&self,id:&str,claims:&ConnectionClaims,scope:Scope,before:String,limit:usize)->Result<()> {
        if matches!(scope,Scope::Host) {
            self.authorize(claims,&scope,false).await?;
            let sessions=self.host_page(claims,Some(Uuid::parse_str(&before)?)).await?;
            let before=sessions.last().map(|session|session.session_id.clone());
            return self.emit(id,ServerFrame::HostHistoryPage{scope,sessions,before});
        }
        let session=self.authorize(claims,&scope,false).await?.ok_or_else(||anyhow::anyhow!("history requires session scope"))?;
        ensure!((1..=75).contains(&limit),"history limit exceeds budget");
        let key=scope_key(self.host,Some(session));
        // The authority's own opaque anchor is looked up, never compared by
        // clients or trusted as a raw SQL cursor supplied by a viewer.
        let position:Option<(i64,i64,String)>=sqlx::query_as("select source_sequence,ordinal,row_id from remote_view_rows where scope=$1 and history_anchor=$2 order by source_sequence,ordinal,row_id limit 1")
            .bind(&key).bind(&before).fetch_optional(&self.pg.pool).await?;
        let position=position.ok_or_else(||anyhow::anyhow!("unknown history anchor"))?;
        let page=crate::row_index::page(&self.pg.pool,&key,(position.0,position.1,position.2.as_str()),limit).await?;
        let mut rows=Vec::with_capacity(page.len());
        for (_,_,_,metadata) in page {
            let mut row:borg_view::wire::Row=serde_json::from_slice(&metadata)?;
            let field=match &row.body {
                RowBody::Message{..}=>Some("message"),RowBody::Reasoning{..}=>Some("tool_input"),
                RowBody::TeamReport{..}=>Some("action_body"),RowBody::Diff{..}=>Some("tool_input"),_=>None,
            };
            if let Some(field)=field {
                if row.text_state.as_ref().is_none_or(|state|state.bytes!="0") {
                    let (bytes,_)=crate::row_index::field_page(&self.pg.pool,&key,&row.id,field,0,4096).await?;
                    let text=match std::str::from_utf8(&bytes) {Ok(text)=>text.to_owned(),Err(error) if error.error_len().is_none()=>std::str::from_utf8(&bytes[..error.valid_up_to()])?.to_owned(),Err(error)=>return Err(error.into())};
                    match &mut row.body {RowBody::Message{text:body,..}|RowBody::Reasoning{text:body,..}|RowBody::TeamReport{text:body,..}=>*body=text,RowBody::Diff{patch,..}=>*patch=text,_=>{}}
                }
            }
            rows.push(row);
        }
        rows.reverse();
        let before=rows.first().map(|row|row.history_before.clone());
        self.emit(id,ServerFrame::HistoryPage{scope,rows,before})?;
        Ok(())
    }
}
