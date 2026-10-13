//! Authenticated scope ownership and ordered subscription admission. Projection
//! restoration and publication share the selected journal's durable authority.
use crate::{
    SessionStore,
    auth::{TicketClaims, TunnelFrame},
    projection::{ProjectionStore, scope_key},
};
use anyhow::{Result, ensure};
use borg_view::wire::{ClientFrame, Command, Cursor, Scope, ServerFrame};
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::sync::{Mutex, mpsc};
use uuid::Uuid;

pub struct ViewerCommand {
    pub connection_id: String,
    pub command_id: Uuid,
    pub scope: Scope,
    pub command: Command,
    pub principal: String,
}
#[path = "sync_assets.rs"]
mod assets;
#[path = "sync_host.rs"]
mod host_index;
#[path = "sync_projection.rs"]
mod projector;

#[derive(Clone)]
enum ConnectionClaims {
    Device(TicketClaims),
    Service(crate::auth::ServiceTicketClaims),
}
impl ConnectionClaims {
    fn expiry(&self) -> i64 {
        match self {
            Self::Device(c) => c.expires_at,
            Self::Service(c) => c.expires_at,
        }
    }
    fn epoch(&self) -> u64 {
        match self {
            Self::Device(c) => c.revocation_epoch,
            Self::Service(c) => c.revocation_epoch,
        }
    }
    fn host(&self) -> &str {
        match self {
            Self::Device(c) => &c.host_id,
            Self::Service(c) => &c.host_id,
        }
    }
    fn principal(&self) -> &str {
        match self {
            Self::Device(c) => &c.principal_id,
            Self::Service(c) => &c.principal_id,
        }
    }
    fn allows(&self, session: Uuid) -> bool {
        match self {
            Self::Device(c) => {
                c.session_ids.is_empty()
                    || c.session_ids.iter().any(|id| id == &session.to_string())
            }
            Self::Service(c) => c.session_id == session.to_string(),
        }
    }
    fn allowlist(&self) -> Result<Option<Vec<Uuid>>> {
        match self {
            Self::Device(c) if c.session_ids.is_empty() => Ok(None),
            Self::Device(c) => Ok(Some(
                c.session_ids
                    .iter()
                    .map(|id| Uuid::parse_str(id))
                    .collect::<std::result::Result<Vec<_>, _>>()?,
            )),
            Self::Service(c) => Ok(Some(vec![Uuid::parse_str(&c.session_id)?])),
        }
    }
    fn device(&self) -> Option<&str> {
        match self {
            Self::Device(c) => Some(&c.device_id),
            Self::Service(_) => None,
        }
    }
}
struct Delivery {
    sent: Cursor,
    pending: VecDeque<(u64, usize)>,
    bytes: usize,
}
impl Delivery {
    fn new(cursor: Cursor) -> Self {
        Self {
            sent: cursor,
            pending: VecDeque::new(),
            bytes: 0,
        }
    }
}
struct Connection {
    claims: ConnectionClaims,
    subscriptions: HashMap<String, Delivery>,
}
pub struct SyncAuthority {
    host: Uuid,
    epoch: AtomicU64,
    stopped: AtomicBool,
    relay_online: AtomicBool,
    catalog_recovery: Arc<AtomicBool>,
    publisher_wake: tokio::sync::Notify,
    store: Arc<dyn SessionStore>,
    pg: crate::session_store::postgres::publication::PostgresAuthority,
    projectors: Mutex<HashMap<Uuid, projector::Projector>>,
    projection: ProjectionStore,
    ledger: crate::command_ledger::CommandLedger,
    outbound: mpsc::Sender<TunnelFrame>,
    commands: mpsc::Sender<ViewerCommand>,
    direct_outputs: std::sync::RwLock<HashMap<String, mpsc::Sender<ServerFrame>>>,
    connections: Mutex<HashMap<String, Connection>>,
}
impl SyncAuthority {
    pub async fn open(
        host: Uuid,
        epoch: u64,
        store: Arc<dyn SessionStore>,
        outbound: mpsc::Sender<TunnelFrame>,
        commands: mpsc::Sender<ViewerCommand>,
    ) -> Result<Arc<Self>> {
        let authority = store
            .postgres_authority()
            .ok_or_else(|| anyhow::anyhow!("canonical host sync requires PostgreSQL authority"))?;
        let listener = authority.subscribe_committed().await?;
        let this = Arc::new(Self {
            host,
            epoch: AtomicU64::new(epoch),
            stopped: AtomicBool::new(false),
            relay_online: AtomicBool::new(false),
            catalog_recovery: Arc::new(AtomicBool::new(true)),
            publisher_wake: tokio::sync::Notify::new(),
            projection: ProjectionStore::open(authority.pool.clone()).await?,
            ledger: crate::command_ledger::CommandLedger::open(authority.pool.clone()).await?,
            pg: authority,
            projectors: Mutex::new(HashMap::new()),
            store,
            outbound,
            commands,
            direct_outputs: std::sync::RwLock::new(HashMap::new()),
            connections: Mutex::new(HashMap::new()),
        });
        this.initialize_host_index().await?;
        sqlx::query("create table if not exists remote_source_attention(host_id uuid not null,source_session_id uuid not null,kind text not null,source_sequence bigint not null,frame bytea not null check(octet_length(frame)<=16384),primary key(host_id,source_session_id,kind))")
            .execute(&this.pg.pool).await?;
        let weak = Arc::downgrade(&this);
        crate::host::spawn_transport_task(async move {
            projector::run_publisher(weak, listener).await;
        });
        Ok(this)
    }
    pub async fn open_connection(self: &Arc<Self>, id: String, claims: TicketClaims) -> Result<()> {
        self.open_authority(id, ConnectionClaims::Device(claims))
            .await
    }
    pub async fn open_service_connection(
        self: &Arc<Self>,
        id: String,
        claims: crate::auth::ServiceTicketClaims,
    ) -> Result<()> {
        ensure!(
            claims.purpose == crate::auth::ServicePurpose::ProductView,
            "unsupported service authority"
        );
        self.open_authority(id, ConnectionClaims::Service(claims))
            .await
    }
    async fn open_authority(self: &Arc<Self>, id: String, claims: ConnectionClaims) -> Result<()> {
        ensure!(
            !self.stopped.load(Ordering::Acquire),
            "host authority stopped"
        );
        ensure!(
            claims.host() == self.host.to_string()
                && claims.epoch() == self.epoch.load(Ordering::Acquire)
                && claims.expiry() > chrono::Utc::now().timestamp(),
            "invalid scope authority"
        );
        let expiry = claims.expiry();
        let mut connections = self.connections.lock().await;
        ensure!(
            connections.len() < 128 && !connections.contains_key(&id),
            "viewer connection capacity/conflict"
        );
        self.emit(
            &id,
            ServerFrame::Welcome {
                protocol: 2,
                host_id: self.host.to_string(),
                commands: vec![
                    "prompt".into(),
                    "interrupt".into(),
                    "compact".into(),
                    "clear_context".into(),
                    "approval".into(),
                    "slash".into(),
                ],
            },
        )?;
        connections.insert(
            id.clone(),
            Connection {
                claims,
                subscriptions: HashMap::new(),
            },
        );
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let delay =
                (expiry.saturating_mul(1000) - chrono::Utc::now().timestamp_millis()).max(0) as u64;
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
            if let Some(authority) = weak.upgrade() {
                authority.close_connection(&id).await;
                let _ = authority.outbound.try_send(TunnelFrame::Close {
                    connection_id: id,
                    reason: "ticket expired".into(),
                });
            }
        });
        Ok(())
    }
    pub(crate) fn emit(&self, id: &str, frame: ServerFrame) -> Result<()> {
        ensure!(
            serde_json::to_vec(&frame)?.len() <= 1_048_576,
            "server frame exceeds budget"
        );
        if let Some(output) = self
            .direct_outputs
            .read()
            .map_err(|_| anyhow::anyhow!("direct route poisoned"))?
            .get(id)
        {
            output
                .try_send(frame)
                .map_err(|_| anyhow::anyhow!("direct delivery gap"))?;
            return Ok(());
        }
        let frame = serde_json::to_value(frame)?;
        ensure!(
            serde_json::to_vec(&frame)?.len() <= 1_048_576,
            "server frame exceeds budget"
        );
        self.outbound
            .try_send(TunnelFrame::Server {
                connection_id: id.into(),
                frame,
            })
            .map_err(|_| anyhow::anyhow!("viewer outbound gap; reconnect required"))?;
        Ok(())
    }
    pub async fn close_connection(&self, id: &str) {
        self.connections.lock().await.remove(id);
        if let Ok(mut outputs) = self.direct_outputs.write() {
            outputs.remove(id);
        }
    }
    fn emit_scope(&self, id: &str, delivery: &mut Delivery, frame: ServerFrame) -> Result<()> {
        let cursor = match &frame {
            ServerFrame::Patch { base, cursor, .. }
            | ServerFrame::HostPatch { base, cursor, .. } => {
                ensure!(base == &delivery.sent, "scope delivery cursor gap");
                cursor.clone()
            }
            ServerFrame::Snapshot { view, .. } => view.cursor.clone(),
            ServerFrame::HostSnapshot { cursor, .. } => cursor.clone(),
            _ => return self.emit(id, frame),
        };
        let bytes = serde_json::to_vec(&frame)?.len();
        ensure!(
            delivery.pending.len() < 128
                && delivery
                    .bytes
                    .checked_add(bytes)
                    .is_some_and(|n| n <= 1_048_576),
            "unacknowledged scope budget exceeded"
        );
        let revision = cursor.revision.parse::<u64>()?;
        self.emit(id, frame)?;
        delivery.bytes += bytes;
        delivery.pending.push_back((revision, bytes));
        delivery.sent = cursor;
        Ok(())
    }
    pub async fn relay_reconnected(&self) -> Result<()> {
        ensure!(
            !self.stopped.load(Ordering::Acquire),
            "host authority stopped"
        );
        self.relay_online.store(true, Ordering::Release);
        self.catalog_recovery.store(true, Ordering::Release);
        self.publisher_wake.notify_one();
        Ok(())
    }
    pub async fn relay_disconnected(&self) {
        self.relay_online.store(false, Ordering::Release);
        let mut connections = self.connections.lock().await;
        let direct = self.direct_outputs.read().ok();
        connections.retain(|id, _| {
            direct
                .as_ref()
                .is_some_and(|outputs| outputs.contains_key(id))
        });
    }
    pub async fn shutdown(&self) {
        self.stopped.store(true, Ordering::Release);
        self.publisher_wake.notify_one();
        let mut connections = self.connections.lock().await;
        for id in connections.keys() {
            let _ = self.emit(
                id,
                ServerFrame::AuthRevoked {
                    reason: "host authority stopped".into(),
                },
            );
        }
        connections.clear();
        if let Ok(mut outputs) = self.direct_outputs.write() {
            outputs.clear();
        }
    }
    pub fn current_epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }
    pub async fn consume_client_ticket(&self, claims: &TicketClaims) -> Result<()> {
        ensure!(
            claims.host_id == self.host.to_string()
                && claims.revocation_epoch == self.current_epoch(),
            "ticket authority mismatch"
        );
        crate::admission::consume_ticket(&self.pg.pool, claims, chrono::Utc::now().timestamp())
            .await
    }
    pub async fn open_direct_connection(
        self: &Arc<Self>,
        id: String,
        claims: TicketClaims,
        output: mpsc::Sender<ServerFrame>,
    ) -> Result<()> {
        self.direct_outputs
            .write()
            .map_err(|_| anyhow::anyhow!("direct route poisoned"))?
            .insert(id.clone(), output);
        if let Err(error) = self.open_connection(id.clone(), claims).await {
            if let Ok(mut outputs) = self.direct_outputs.write() {
                outputs.remove(&id);
            }
            return Err(error);
        }
        Ok(())
    }
    pub async fn update_epoch(&self, epoch: u64) {
        self.epoch.store(epoch, Ordering::Release);
        let mut connections = self.connections.lock().await;
        for id in connections.keys() {
            let _ = self.outbound.try_send(TunnelFrame::Close {
                connection_id: id.clone(),
                reason: "admission revoked".into(),
            });
        }
        connections.clear();
        if let Ok(mut outputs) = self.direct_outputs.write() {
            outputs.clear();
        }
    }
    async fn authorize(
        &self,
        claims: &ConnectionClaims,
        scope: &Scope,
        operate: bool,
    ) -> Result<Option<Uuid>> {
        ensure!(
            !self.stopped.load(Ordering::Acquire)
                && claims.expiry() > chrono::Utc::now().timestamp()
                && claims.epoch() == self.epoch.load(Ordering::Acquire),
            "expired/revoked authority"
        );
        let session = match scope {
            Scope::Host => None,
            Scope::Session { session_id } => Some(Uuid::parse_str(session_id)?),
        };
        match claims {
            ConnectionClaims::Device(ticket) => {
                if session.is_some() || operate {
                    crate::admission::authorize_scope(
                        ticket,
                        session.as_ref().map(|s| s.to_string()).as_deref(),
                        operate,
                    )?
                }
            }
            ConnectionClaims::Service(ticket) => ensure!(
                !operate
                    && session.map(|s| s.to_string()).as_deref()
                        == Some(ticket.session_id.as_str()),
                "service authority is read-only exact-session"
            ),
        }
        if let Some(session) = session {
            let owner = self
                .store
                .owning_root_session(session)
                .await?
                .unwrap_or(session);
            ensure!(
                self.store
                    .host_launch_owner(owner)
                    .await?
                    .map(|(host, _)| host)
                    == Some(self.host),
                "session outside host authority"
            );
        }
        Ok(session)
    }
    async fn validate_request(
        &self,
        claims: &ConnectionClaims,
        scope: &Scope,
        source: &str,
        id: &str,
        revision: &str,
        question: bool,
    ) -> Result<()> {
        let source = Uuid::parse_str(source)?;
        self.authorize(
            claims,
            &Scope::Session {
                session_id: source.to_string(),
            },
            true,
        )
        .await?;
        if let Scope::Session { session_id } = scope {
            let parent = Uuid::parse_str(session_id)?;
            ensure!(
                source == parent || self.store.is_descendant(parent, source).await?,
                "approval source outside selected scope"
            );
        }
        let state = self.store.state(source).await?;
        let pending = if question {
            state.pending_provider_interaction_id
        } else {
            state.pending_approval_id
        };
        ensure!(pending.as_deref() == Some(id), "request no longer pending");
        let kind = if question {
            "provider_interaction_requested"
        } else {
            "approval_requested"
        };
        let latest:Option<Uuid>=sqlx::query_scalar("select event_id from session_events where session_id=$1 and event_kind=$2 order by sequence desc limit 1")
            .bind(source).bind(kind).fetch_optional(&self.pg.pool).await?;
        ensure!(
            latest == Some(Uuid::parse_str(revision)?),
            "stale request revision"
        );
        Ok(())
    }
    pub async fn handle(&self, id: &str, frame: ClientFrame) -> Result<Vec<ServerFrame>> {
        let claims = self
            .connections
            .lock()
            .await
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("unknown viewer"))?
            .claims
            .clone();
        ensure!(
            claims.expiry() > chrono::Utc::now().timestamp(),
            "expired viewer"
        );
        match frame {
            ClientFrame::Command {
                command_id,
                scope,
                command,
            } => {
                let session = self.authorize(&claims, &scope, true).await?;
                let id_uuid = Uuid::parse_str(&command_id)?;
                let key = scope_key(self.host, session);
                if let Some(bytes) = self
                    .ledger
                    .terminal(
                        self.host,
                        id_uuid,
                        &key,
                        claims.principal(),
                        &serde_json::to_vec(&command)?,
                    )
                    .await?
                {
                    let state = self
                        .projection
                        .load(&key)
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("missing receipt scope projection"))?;
                    self.emit(
                        id,
                        ServerFrame::Result {
                            command_id,
                            scope,
                            cursor: Cursor {
                                epoch: state.epoch,
                                revision: state.revision.to_string(),
                            },
                            outcome: serde_json::from_slice(&bytes)?,
                        },
                    )?;
                    return Ok(Vec::new());
                }
                match &command {
                    Command::Approval {
                        approval_id,
                        source_session_id,
                        target_revision,
                        ..
                    } => {
                        self.validate_request(
                            &claims,
                            &scope,
                            source_session_id,
                            approval_id,
                            target_revision,
                            false,
                        )
                        .await?
                    }
                    Command::RespondToQuestion {
                        approval_id,
                        source_session_id,
                        target_revision,
                        ..
                    } => {
                        self.validate_request(
                            &claims,
                            &scope,
                            source_session_id,
                            approval_id,
                            target_revision,
                            true,
                        )
                        .await?
                    }
                    _ => {}
                }
                self.commands
                    .try_send(ViewerCommand {
                        connection_id: id.into(),
                        command_id: Uuid::parse_str(&command_id)?,
                        scope,
                        command,
                        principal: claims.principal().to_owned(),
                    })
                    .map_err(|_| anyhow::anyhow!("command ingress capacity"))?;
            }
            ClientFrame::Unsubscribe { scope } => {
                let session = self.authorize(&claims, &scope, false).await?;
                if let Some(conn) = self.connections.lock().await.get_mut(id) {
                    conn.subscriptions.remove(&scope_key(self.host, session));
                }
            }
            ClientFrame::Ack { scope, cursor } => {
                let session = self.authorize(&claims, &scope, false).await?;
                let key = scope_key(self.host, session);
                let head = self
                    .projection
                    .load(&key)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("missing scope projection"))?;
                ensure!(
                    cursor.epoch == head.epoch && cursor.revision.parse::<i64>()? <= head.revision,
                    "invalid future/epoch ACK"
                );
                let mut connections = self.connections.lock().await;
                let conn = connections
                    .get_mut(id)
                    .ok_or_else(|| anyhow::anyhow!("viewer closed"))?;
                let delivery = conn
                    .subscriptions
                    .get_mut(&key)
                    .ok_or_else(|| anyhow::anyhow!("ACK on unsubscribed scope"))?;
                let revision = cursor.revision.parse::<u64>()?;
                ensure!(
                    cursor.epoch == delivery.sent.epoch
                        && revision <= delivery.sent.revision.parse::<u64>()?,
                    "ACK exceeds actually sent cursor"
                );
                while delivery
                    .pending
                    .front()
                    .is_some_and(|(pending, _)| *pending <= revision)
                {
                    let (_, bytes) = delivery.pending.pop_front().expect("nonempty pending");
                    delivery.bytes -= bytes;
                }
            }
            ClientFrame::Hello {
                protocol,
                device,
                subscriptions,
            } => {
                ensure!(
                    protocol == 2 && claims.device().is_none_or(|expected| device == expected),
                    "invalid viewer hello"
                );
                ensure!(subscriptions.len() <= 16, "subscription capacity");
                for subscription in subscriptions {
                    self.subscribe(id, &claims, subscription).await?;
                }
            }
            ClientFrame::Subscribe { subscription } => {
                self.subscribe(id, &claims, subscription).await?
            }
            ClientFrame::Resync { scope, .. } => {
                self.subscribe(
                    id,
                    &claims,
                    borg_view::wire::Subscription { scope, after: None },
                )
                .await?
            }
            ClientFrame::History {
                scope,
                before,
                limit,
            } => self.history(id, &claims, scope, before, limit).await?,
            ClientFrame::TurnResult {
                scope,
                command_id,
                message_id,
            } => {
                let session = self
                    .authorize(&claims, &scope, false)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("turn result requires session"))?;
                let command = Uuid::parse_str(&command_id)?;
                ensure!(
                    self.ledger.prompt_identity(self.host, command).await?
                        == Some((session, Uuid::parse_str(&message_id)?)),
                    "turn result identity mismatch"
                );
                let key = scope_key(self.host, Some(session));
                let bytes: Option<Vec<u8>> = sqlx::query_scalar(
                    "select frame from remote_turn_results where scope=$1 and message_id=$2",
                )
                .bind(key)
                .bind(message_id)
                .fetch_optional(&self.pg.pool)
                .await?;
                if let Some(bytes) = bytes {
                    let mut frame: ServerFrame = serde_json::from_slice(&bytes)?;
                    match &mut frame {
                        ServerFrame::TurnStarted { command_id: id, .. }
                        | ServerFrame::TurnCompleted { command_id: id, .. } => *id = command_id,
                        _ => return Err(anyhow::anyhow!("invalid durable turn result")),
                    }
                    self.emit(id, frame)?;
                }
            }
            ClientFrame::AssetPage {
                scope,
                request_id,
                asset_id,
                offset,
                max_bytes,
            } => {
                self.asset_page(id, &claims, scope, request_id, asset_id, offset, max_bytes)
                    .await?
            }
            ClientFrame::SubscribeInbox {} => return Err(anyhow::anyhow!("inbox is relay-only")),
            ClientFrame::Presence { scope, .. } => {
                self.authorize(&claims, &scope, false).await?;
            }
        }
        Ok(Vec::new()) // All emission uses the single ordered outbound queue.
    }
}
