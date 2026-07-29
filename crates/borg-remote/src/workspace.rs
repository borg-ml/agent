//! Provider-neutral local multiplayer workspace kernel.
//!
//! Execution remains in `SessionEvent`; this module only records references to
//! it. Participants are global identities; workspace membership is durable.

use std::{path::Path, str::FromStr, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Row, Sqlite, SqlitePool, Transaction};
use uuid::Uuid;

const BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const WRITE_TRANSACTION: &str = "BEGIN IMMEDIATE";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Participant {
    pub id: Uuid,
    pub display_name: String,
    pub kind: ParticipantKind,
    pub created_at: DateTime<Utc>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParticipantKind {
    Human,
    Agent,
    Service,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceRole {
    Owner,
    Admin,
    Editor,
    Contributor,
    Viewer,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceMembership {
    pub workspace_id: Uuid,
    pub participant_id: Uuid,
    pub role: WorkspaceRole,
    pub joined_at: DateTime<Utc>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Workspace {
    pub id: Uuid,
    pub name: String,
    pub created_at: DateTime<Utc>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Thread {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub title: String,
    pub created_at: DateTime<Utc>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructuredMention {
    pub participant_id: Uuid,
    pub start: u32,
    pub end: u32,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceMessageBody {
    pub text: String,
    #[serde(default)]
    pub mentions: Vec<StructuredMention>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Audience {
    Workspace,
    Participants { participants: Vec<Uuid> },
    Role { role: WorkspaceRole },
    Direct { participant: Uuid },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceMessage {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub thread_id: Option<Uuid>,
    pub reply_to_message_id: Option<Uuid>,
    pub author_id: Uuid,
    pub body: WorkspaceMessageBody,
    pub audience: Audience,
    pub created_at: DateTime<Utc>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryMode {
    Boundary,
    Wake,
    NextTurn,
    Notify,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryState {
    Pending,
    Admitted,
    Acknowledged,
    Failed,
    Recalled,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryAttempt {
    pub attempted_at: DateTime<Utc>,
    pub detail: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecipientDelivery {
    pub workspace_id: Uuid,
    pub sequence: u64,
    pub recipient_id: Uuid,
    pub mode: DeliveryMode,
    pub state: DeliveryState,
    pub attempts: u32,
    pub last_attempt: Option<DeliveryAttempt>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryCursor {
    pub workspace_id: Uuid,
    pub participant_id: Uuid,
    pub admitted_sequence: u64,
    pub acknowledged_sequence: u64,
}
/// An expiring client or host lease. Absence/expiry means no presence; it is never an offline event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PresenceLease {
    pub workspace_id: Uuid,
    pub participant_id: Uuid,
    pub client_id: Uuid,
    pub host_id: Option<Uuid>,
    pub expires_at: DateTime<Utc>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostIdentity {
    pub id: Uuid,
    pub name: String,
    pub capabilities: WorkspaceHostCapabilities,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkspaceHostCapabilities {
    pub delivery_modes: Vec<DeliveryMode>,
    pub attachments: bool,
    pub max_attachment_bytes: Option<u64>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostAttachment {
    pub host_id: Uuid,
    pub workspace_id: Uuid,
    pub attached_at: DateTime<Utc>,
}
#[async_trait]
pub trait WorkspaceHost: Send + Sync {
    async fn event_appended(&self, _: WorkspaceEvent) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceEvent {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub sequence: u64,
    pub author_id: Uuid,
    pub idempotency_key: String,
    pub created_at: DateTime<Utc>,
    pub kind: WorkspaceEventKind,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkspaceEventKind {
    Message {
        message: WorkspaceMessage,
        mode: DeliveryMode,
    },
    SessionEvent {
        session_id: Uuid,
        session_event_id: Uuid,
        session_sequence: u64,
        mode: DeliveryMode,
    },
}

#[async_trait]
pub trait WorkspaceStore: Send + Sync {
    async fn create_participant(&self, participant: Participant) -> Result<()>;
    async fn create_workspace(&self, workspace: Workspace) -> Result<()>;
    async fn add_member(&self, membership: WorkspaceMembership) -> Result<()>;
    async fn create_thread(&self, thread: Thread) -> Result<()>;
    async fn append(&self, event: WorkspaceEvent) -> Result<WorkspaceEvent>;
    async fn replay(
        &self,
        workspace_id: Uuid,
        viewer_id: Uuid,
        after_sequence: u64,
        limit: usize,
    ) -> Result<Vec<WorkspaceEvent>>;
    async fn deliveries_after(
        &self,
        workspace_id: Uuid,
        recipient_id: Uuid,
        after_sequence: u64,
        limit: usize,
    ) -> Result<Vec<RecipientDelivery>>;
    async fn transition_delivery(
        &self,
        workspace_id: Uuid,
        sequence: u64,
        recipient_id: Uuid,
        state: DeliveryState,
        attempt: Option<DeliveryAttempt>,
    ) -> Result<RecipientDelivery>;
    async fn acquire_presence_lease(&self, lease: PresenceLease) -> Result<()>;
    async fn active_presence(
        &self,
        workspace_id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<Vec<PresenceLease>>;
}

#[derive(Clone)]
pub struct SqliteWorkspaceStore {
    pool: SqlitePool,
}
impl SqliteWorkspaceStore {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let opts = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(BUSY_TIMEOUT)
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(opts)
            .await
            .with_context(|| format!("failed to open SQLite workspace store {}", path.display()))?;
        let store = Self { pool };
        store.schema().await?;
        Ok(store)
    }
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
    async fn write(&self) -> Result<Transaction<'static, Sqlite>, sqlx::Error> {
        self.pool.begin_with(WRITE_TRANSACTION).await
    }
    async fn schema(&self) -> Result<()> {
        sqlx::raw_sql(r#"
      create table if not exists workspace_participants (id text primary key, display_name text not null, kind text not null, created_at text not null);
      create table if not exists workspaces (id text primary key, name text not null, next_sequence integer not null default 1, created_at text not null);
      create table if not exists workspace_members (workspace_id text not null references workspaces(id) on delete cascade, participant_id text not null references workspace_participants(id), role text not null, joined_at text not null, primary key(workspace_id,participant_id));
      create table if not exists workspace_threads (id text primary key, workspace_id text not null references workspaces(id) on delete cascade, title text not null, created_at text not null);
      create table if not exists workspace_events (workspace_id text not null references workspaces(id) on delete cascade, sequence integer not null, id text not null, author_id text not null references workspace_participants(id), idempotency_key text not null, canonical_json text not null, event_json text not null, created_at text not null, primary key(workspace_id,sequence), unique(workspace_id,author_id,idempotency_key));
      create table if not exists workspace_deliveries (workspace_id text not null, sequence integer not null, recipient_id text not null references workspace_participants(id), mode text not null, state text not null, attempts integer not null default 0, last_attempt_json text, primary key(workspace_id,sequence,recipient_id), foreign key(workspace_id,sequence) references workspace_events(workspace_id,sequence) on delete cascade);
      create table if not exists workspace_presence_leases (workspace_id text not null references workspaces(id) on delete cascade, participant_id text not null references workspace_participants(id), client_id text not null, host_id text, expires_at text not null, primary key(workspace_id,participant_id,client_id));
      create index if not exists idx_workspace_delivery_recipient on workspace_deliveries(workspace_id,recipient_id,sequence);
    "#).execute(&self.pool).await?;
        Ok(())
    }
    async fn members(
        &self,
        tx: &mut Transaction<'static, Sqlite>,
        w: Uuid,
    ) -> Result<Vec<(Uuid, WorkspaceRole)>> {
        let rows =
            sqlx::query("select participant_id,role from workspace_members where workspace_id=?")
                .bind(w.to_string())
                .fetch_all(&mut **tx)
                .await?;
        rows.into_iter()
            .map(|r| {
                Ok((
                    Uuid::parse_str(r.get("participant_id"))?,
                    serde_json::from_str(r.get("role"))?,
                ))
            })
            .collect()
    }
    fn recipients(a: &Audience, members: &[(Uuid, WorkspaceRole)]) -> Result<Vec<Uuid>> {
        let mut ids = match a {
            Audience::Workspace => members.iter().map(|(id, _)| *id).collect(),
            Audience::Participants { participants } => participants.clone(),
            Audience::Role { role } => members
                .iter()
                .filter_map(|(id, r)| (r == role).then_some(*id))
                .collect(),
            Audience::Direct { participant } => vec![*participant],
        };
        ids.sort_unstable();
        ids.dedup();
        ensure!(!ids.is_empty(), "audience resolves to no members");
        ensure!(
            ids.iter().all(|p| members.iter().any(|(id, _)| id == p)),
            "audience contains a non-member"
        );
        Ok(ids)
    }
}

#[async_trait]
impl WorkspaceStore for SqliteWorkspaceStore {
    async fn create_participant(&self, p: Participant) -> Result<()> {
        sqlx::query("insert into workspace_participants values(?,?,?,?)")
            .bind(p.id.to_string())
            .bind(p.display_name)
            .bind(serde_json::to_string(&p.kind)?)
            .bind(p.created_at.to_rfc3339())
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    async fn create_workspace(&self, w: Workspace) -> Result<()> {
        sqlx::query("insert into workspaces(id,name,created_at) values(?,?,?)")
            .bind(w.id.to_string())
            .bind(w.name)
            .bind(w.created_at.to_rfc3339())
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    async fn add_member(&self, m: WorkspaceMembership) -> Result<()> {
        sqlx::query("insert into workspace_members values(?,?,?,?)")
            .bind(m.workspace_id.to_string())
            .bind(m.participant_id.to_string())
            .bind(serde_json::to_string(&m.role)?)
            .bind(m.joined_at.to_rfc3339())
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    async fn create_thread(&self, t: Thread) -> Result<()> {
        sqlx::query("insert into workspace_threads values(?,?,?,?)")
            .bind(t.id.to_string())
            .bind(t.workspace_id.to_string())
            .bind(t.title)
            .bind(t.created_at.to_rfc3339())
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    async fn append(&self, mut e: WorkspaceEvent) -> Result<WorkspaceEvent> {
        ensure!(
            !e.idempotency_key.is_empty(),
            "idempotency key must not be empty"
        );
        let mut tx = self.write().await?;
        let mut canonical = e.clone();
        canonical.sequence = 0;
        let canonical = serde_json::to_string(&canonical)?;
        if let Some(row)=sqlx::query("select canonical_json,event_json from workspace_events where workspace_id=? and author_id=? and idempotency_key=?").bind(e.workspace_id.to_string()).bind(e.author_id.to_string()).bind(&e.idempotency_key).fetch_optional(&mut *tx).await?{if row.get::<String,_>("canonical_json")!=canonical {bail!("idempotency conflict: key was used with a different payload");}return Ok(serde_json::from_str(row.get("event_json"))?)}
        let members = self.members(&mut tx, e.workspace_id).await?;
        ensure!(
            members.iter().any(|(id, _)| *id == e.author_id),
            "author is not a workspace member"
        );
        let (mode, audience) = match &e.kind {
            WorkspaceEventKind::Message { message, mode } => {
                ensure!(
                    message.workspace_id == e.workspace_id && message.author_id == e.author_id,
                    "message workspace/author mismatch"
                );
                if let Some(thread) = message.thread_id {
                    let found:i64=sqlx::query_scalar("select exists(select 1 from workspace_threads where id=? and workspace_id=?)").bind(thread.to_string()).bind(e.workspace_id.to_string()).fetch_one(&mut *tx).await?;
                    ensure!(found != 0, "thread is not in workspace");
                }
                (*mode, &message.audience)
            }
            WorkspaceEventKind::SessionEvent { mode, .. } => (*mode, &Audience::Workspace),
        };
        let recipients = Self::recipients(audience, &members)?;
        let seq:i64=sqlx::query_scalar("update workspaces set next_sequence=next_sequence+1 where id=? returning next_sequence-1").bind(e.workspace_id.to_string()).fetch_one(&mut *tx).await?;
        e.sequence = u64::try_from(seq)?;
        let json = serde_json::to_string(&e)?;
        sqlx::query("insert into workspace_events values(?,?,?,?,?,?,?,?)")
            .bind(e.workspace_id.to_string())
            .bind(seq)
            .bind(e.id.to_string())
            .bind(e.author_id.to_string())
            .bind(&e.idempotency_key)
            .bind(canonical)
            .bind(json)
            .bind(e.created_at.to_rfc3339())
            .execute(&mut *tx)
            .await?;
        for recipient in recipients {
            sqlx::query("insert into workspace_deliveries(workspace_id,sequence,recipient_id,mode,state) values(?,?,?,?,?)").bind(e.workspace_id.to_string()).bind(seq).bind(recipient.to_string()).bind(serde_json::to_string(&mode)?).bind(serde_json::to_string(&DeliveryState::Pending)?).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(e)
    }
    async fn replay(
        &self,
        w: Uuid,
        viewer: Uuid,
        after: u64,
        limit: usize,
    ) -> Result<Vec<WorkspaceEvent>> {
        let member:i64=sqlx::query_scalar("select exists(select 1 from workspace_members where workspace_id=? and participant_id=?)").bind(w.to_string()).bind(viewer.to_string()).fetch_one(&self.pool).await?;
        ensure!(member != 0, "viewer is not a workspace member");
        let rows=sqlx::query("select e.event_json from workspace_events e join workspace_deliveries d on d.workspace_id=e.workspace_id and d.sequence=e.sequence where d.workspace_id=? and d.recipient_id=? and d.sequence>? order by d.sequence limit ?").bind(w.to_string()).bind(viewer.to_string()).bind(i64::try_from(after)?).bind(i64::try_from(limit)?).fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|r| Ok(serde_json::from_str(r.get("event_json"))?))
            .collect()
    }
    async fn deliveries_after(
        &self,
        w: Uuid,
        p: Uuid,
        after: u64,
        limit: usize,
    ) -> Result<Vec<RecipientDelivery>> {
        let rows=sqlx::query("select sequence,recipient_id,mode,state,attempts,last_attempt_json from workspace_deliveries where workspace_id=? and recipient_id=? and sequence>? order by sequence limit ?").bind(w.to_string()).bind(p.to_string()).bind(i64::try_from(after)?).bind(i64::try_from(limit)?).fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|r| {
                Ok(RecipientDelivery {
                    workspace_id: w,
                    sequence: u64::try_from(r.get::<i64, _>("sequence"))?,
                    recipient_id: Uuid::parse_str(r.get("recipient_id"))?,
                    mode: serde_json::from_str(r.get("mode"))?,
                    state: serde_json::from_str(r.get("state"))?,
                    attempts: u32::try_from(r.get::<i64, _>("attempts"))?,
                    last_attempt: r
                        .get::<Option<String>, _>("last_attempt_json")
                        .map(|x| serde_json::from_str(&x))
                        .transpose()?,
                })
            })
            .collect()
    }
    async fn transition_delivery(
        &self,
        w: Uuid,
        s: u64,
        p: Uuid,
        next: DeliveryState,
        attempt: Option<DeliveryAttempt>,
    ) -> Result<RecipientDelivery> {
        let mut tx = self.write().await?;
        let row=sqlx::query("select mode,state,attempts,last_attempt_json from workspace_deliveries where workspace_id=? and sequence=? and recipient_id=?").bind(w.to_string()).bind(i64::try_from(s)?).bind(p.to_string()).fetch_optional(&mut *tx).await?.context("recipient has no delivery")?;
        let current: DeliveryState = serde_json::from_str(row.get("state"))?;
        let allowed = matches!(
            (current, next),
            (
                DeliveryState::Pending,
                DeliveryState::Admitted | DeliveryState::Failed | DeliveryState::Recalled
            ) | (
                DeliveryState::Admitted,
                DeliveryState::Acknowledged | DeliveryState::Failed | DeliveryState::Recalled
            ) | (
                DeliveryState::Failed,
                DeliveryState::Pending | DeliveryState::Recalled
            )
        );
        ensure!(
            allowed || current == next,
            "invalid non-monotonic delivery transition"
        );
        let attempts: i64 = row.get("attempts");
        let attempts = attempts + if attempt.is_some() { 1 } else { 0 };
        sqlx::query("update workspace_deliveries set state=?,attempts=?,last_attempt_json=coalesce(?,last_attempt_json) where workspace_id=? and sequence=? and recipient_id=?").bind(serde_json::to_string(&next)?).bind(attempts).bind(attempt.as_ref().map(serde_json::to_string).transpose()?).bind(w.to_string()).bind(i64::try_from(s)?).bind(p.to_string()).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(RecipientDelivery {
            workspace_id: w,
            sequence: s,
            recipient_id: p,
            mode: serde_json::from_str(row.get("mode"))?,
            state: next,
            attempts: u32::try_from(attempts)?,
            last_attempt: attempt.or(row
                .get::<Option<String>, _>("last_attempt_json")
                .map(|x| serde_json::from_str(&x))
                .transpose()?),
        })
    }
    async fn acquire_presence_lease(&self, l: PresenceLease) -> Result<()> {
        ensure!(
            l.expires_at > Utc::now(),
            "presence lease must expire in the future"
        );
        sqlx::query("insert into workspace_presence_leases values(?,?,?,?,?) on conflict(workspace_id,participant_id,client_id) do update set host_id=excluded.host_id,expires_at=excluded.expires_at").bind(l.workspace_id.to_string()).bind(l.participant_id.to_string()).bind(l.client_id.to_string()).bind(l.host_id.map(|x|x.to_string())).bind(l.expires_at.to_rfc3339()).execute(&self.pool).await?;
        Ok(())
    }
    async fn active_presence(&self, w: Uuid, now: DateTime<Utc>) -> Result<Vec<PresenceLease>> {
        sqlx::query("delete from workspace_presence_leases where expires_at<=?")
            .bind(now.to_rfc3339())
            .execute(&self.pool)
            .await?;
        let rows=sqlx::query("select participant_id,client_id,host_id,expires_at from workspace_presence_leases where workspace_id=? and expires_at>?").bind(w.to_string()).bind(now.to_rfc3339()).fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|r| {
                Ok(PresenceLease {
                    workspace_id: w,
                    participant_id: Uuid::parse_str(r.get("participant_id"))?,
                    client_id: Uuid::parse_str(r.get("client_id"))?,
                    host_id: r
                        .get::<Option<String>, _>("host_id")
                        .map(|x| Uuid::parse_str(&x))
                        .transpose()?,
                    expires_at: DateTime::parse_from_rfc3339(r.get("expires_at"))?
                        .with_timezone(&Utc),
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    async fn fixture() -> (
        SqliteWorkspaceStore,
        Workspace,
        Participant,
        Participant,
        Participant,
    ) {
        let file = tempfile::NamedTempFile::new().unwrap();
        let s = SqliteWorkspaceStore::open(file.path()).await.unwrap();
        std::mem::forget(file);
        let w = Workspace {
            id: Uuid::new_v4(),
            name: "w".into(),
            created_at: Utc::now(),
        };
        let a = Participant {
            id: Uuid::new_v4(),
            display_name: "a".into(),
            kind: ParticipantKind::Human,
            created_at: Utc::now(),
        };
        let b = Participant {
            id: Uuid::new_v4(),
            display_name: "b".into(),
            kind: ParticipantKind::Agent,
            created_at: Utc::now(),
        };
        let c = Participant {
            id: Uuid::new_v4(),
            display_name: "c".into(),
            kind: ParticipantKind::Service,
            created_at: Utc::now(),
        };
        s.create_workspace(w.clone()).await.unwrap();
        for p in [&a, &b, &c] {
            s.create_participant(p.clone()).await.unwrap();
        }
        for (p, r) in [(&a, WorkspaceRole::Owner), (&b, WorkspaceRole::Editor)] {
            s.add_member(WorkspaceMembership {
                workspace_id: w.id,
                participant_id: p.id,
                role: r,
                joined_at: Utc::now(),
            })
            .await
            .unwrap();
        }
        (s, w, a, b, c)
    }
    fn event(w: Uuid, a: Uuid, audience: Audience, key: &str) -> WorkspaceEvent {
        let now = Utc::now();
        WorkspaceEvent {
            id: Uuid::new_v4(),
            workspace_id: w,
            sequence: 0,
            author_id: a,
            idempotency_key: key.into(),
            created_at: now,
            kind: WorkspaceEventKind::Message {
                mode: DeliveryMode::Notify,
                message: WorkspaceMessage {
                    id: Uuid::new_v4(),
                    workspace_id: w,
                    thread_id: None,
                    reply_to_message_id: None,
                    author_id: a,
                    body: WorkspaceMessageBody {
                        text: "hi".into(),
                        mentions: vec![],
                    },
                    audience,
                    created_at: now,
                },
            },
        }
    }
    #[tokio::test]
    async fn conflicting_retry_is_rejected() {
        let (s, w, a, b, _) = fixture().await;
        s.append(event(
            w.id,
            a.id,
            Audience::Direct { participant: b.id },
            "k",
        ))
        .await
        .unwrap();
        assert!(
            s.append(event(w.id, a.id, Audience::Workspace, "k"))
                .await
                .unwrap_err()
                .to_string()
                .contains("idempotency conflict")
        );
    }
    #[tokio::test]
    async fn delivery_outcomes_are_independent() {
        let (s, w, a, b, _) = fixture().await;
        let e = s
            .append(event(w.id, a.id, Audience::Workspace, "k"))
            .await
            .unwrap();
        s.transition_delivery(w.id, e.sequence, a.id, DeliveryState::Admitted, None)
            .await
            .unwrap();
        s.transition_delivery(
            w.id,
            e.sequence,
            b.id,
            DeliveryState::Failed,
            Some(DeliveryAttempt {
                attempted_at: Utc::now(),
                detail: Some("offline".into()),
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            s.deliveries_after(w.id, a.id, 0, 2).await.unwrap()[0].state,
            DeliveryState::Admitted
        );
        assert_eq!(
            s.deliveries_after(w.id, b.id, 0, 2).await.unwrap()[0].state,
            DeliveryState::Failed
        );
    }
    #[tokio::test]
    async fn unauthorized_membership_and_replay_fail() {
        let (s, w, a, b, c) = fixture().await;
        assert!(
            s.add_member(WorkspaceMembership {
                workspace_id: w.id,
                participant_id: Uuid::new_v4(),
                role: WorkspaceRole::Viewer,
                joined_at: Utc::now()
            })
            .await
            .is_err()
        );
        assert!(
            s.append(event(w.id, c.id, Audience::Workspace, "x"))
                .await
                .is_err()
        );
        s.append(event(
            w.id,
            a.id,
            Audience::Direct { participant: b.id },
            "ok",
        ))
        .await
        .unwrap();
        assert!(s.replay(w.id, a.id, 0, 10).await.unwrap().is_empty());
        assert!(s.replay(w.id, c.id, 0, 10).await.is_err());
    }
    #[tokio::test]
    async fn concurrent_sequences_are_gap_free() {
        let (s, w, a, b, _) = fixture().await;
        let (l, r) = tokio::join!(
            s.append(event(
                w.id,
                a.id,
                Audience::Direct { participant: b.id },
                "l"
            )),
            s.append(event(
                w.id,
                b.id,
                Audience::Direct { participant: a.id },
                "r"
            ))
        );
        let mut q = vec![l.unwrap().sequence, r.unwrap().sequence];
        q.sort_unstable();
        assert_eq!(q, vec![1, 2]);
    }
}
