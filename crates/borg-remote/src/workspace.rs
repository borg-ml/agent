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
const WORKSPACE_SCHEMA_VERSION: i64 = 2;

/// Stable identity for the local OS user across all personal workspaces in one
/// Borg installation. Authenticated cloud workspaces replace this projection
/// with the product user participant ID.
pub fn local_human_participant_id(display_name: &str) -> Uuid {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("borg://local-human/{}", display_name.trim()).as_bytes(),
    )
}

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
pub struct WorkspaceRosterEntry {
    pub participant: Participant,
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
pub struct SharedWork {
    pub id: Uuid,
    pub title: String,
    pub detail: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceArtifact {
    pub id: Uuid,
    pub work_id: Option<Uuid>,
    pub name: String,
    pub media_type: Option<String>,
    pub uri: String,
    pub content_hash: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceDecision {
    pub id: Uuid,
    pub subject: String,
    pub outcome: String,
    pub rationale: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AtomicWorkClaim {
    pub work_id: Uuid,
    pub claimant_id: Uuid,
    /// The claim id observed by the claimant, or `None` when the work was unclaimed.
    pub expected_claim_id: Option<Uuid>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkDependency {
    pub work_id: Uuid,
    pub depends_on_work_id: Uuid,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceReviewRequest {
    pub id: Uuid,
    pub work_id: Uuid,
    pub requested_reviewer_id: Option<Uuid>,
    pub instructions: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkReview {
    pub work_id: Uuid,
    pub reviewer_id: Uuid,
    pub verdict: String,
    pub detail: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceReference {
    pub id: Uuid,
    pub label: String,
    pub target: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Provenance {
    pub subject_id: Uuid,
    pub source_kind: String,
    pub source_id: String,
    pub detail: Option<String>,
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
    WorkCreated {
        work: SharedWork,
        mode: DeliveryMode,
    },
    ArtifactPublished {
        artifact: WorkspaceArtifact,
        mode: DeliveryMode,
    },
    DecisionRecorded {
        decision: WorkspaceDecision,
        mode: DeliveryMode,
    },
    WorkClaimed {
        claim: AtomicWorkClaim,
        mode: DeliveryMode,
    },
    DependencyDeclared {
        dependency: WorkDependency,
        mode: DeliveryMode,
    },
    ReviewRequested {
        request: WorkspaceReviewRequest,
        mode: DeliveryMode,
    },
    ReviewRecorded {
        review: WorkReview,
        mode: DeliveryMode,
    },
    ReferenceAdded {
        reference: WorkspaceReference,
        mode: DeliveryMode,
    },
    ProvenanceRecorded {
        provenance: Provenance,
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
    async fn append_session_event_batch(&self, events: &[WorkspaceEvent]) -> Result<()> {
        let _ = events;
        bail!("batch session-event projection is unavailable for this workspace store")
    }
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
            .synchronous(SqliteSynchronous::Full)
            .busy_timeout(BUSY_TIMEOUT)
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(opts)
            .await
            .with_context(|| format!("failed to open SQLite workspace store {}", path.display()))?;
        Self::from_pool(pool).await
    }

    /// Attach the workspace projection to an already-open canonical SQLite
    /// authority. Production sessions use this path so workspace membership,
    /// multiplayer delivery, events, actions, and provider history share one
    /// WAL and one transaction boundary.
    pub async fn from_pool(pool: SqlitePool) -> Result<Self> {
        let store = Self { pool };
        store.schema().await?;
        Ok(store)
    }
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub async fn list_workspaces_for_participant(
        &self,
        participant_id: Uuid,
    ) -> Result<Vec<Workspace>> {
        let rows = sqlx::query(
            "select w.id,w.name,w.created_at from workspaces w \
             join workspace_members m on m.workspace_id=w.id \
             where m.participant_id=? order by w.created_at,w.id",
        )
        .bind(participant_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(Workspace {
                    id: Uuid::parse_str(row.try_get("id")?)?,
                    name: row.try_get("name")?,
                    created_at: DateTime::parse_from_rfc3339(row.try_get("created_at")?)?
                        .with_timezone(&Utc),
                })
            })
            .collect()
    }

    pub async fn workspace_roster(
        &self,
        workspace_id: Uuid,
        viewer_id: Uuid,
    ) -> Result<Vec<WorkspaceRosterEntry>> {
        self.require_member(workspace_id, viewer_id).await?;
        let rows = sqlx::query(
            "select p.id,p.display_name,p.kind,p.created_at,m.role,m.joined_at \
             from workspace_members m \
             join workspace_participants p on p.id=m.participant_id \
             where m.workspace_id=? order by m.joined_at,p.id",
        )
        .bind(workspace_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(WorkspaceRosterEntry {
                    participant: Participant {
                        id: Uuid::parse_str(row.try_get("id")?)?,
                        display_name: row.try_get("display_name")?,
                        kind: serde_json::from_str(row.try_get("kind")?)?,
                        created_at: DateTime::parse_from_rfc3339(row.try_get("created_at")?)?
                            .with_timezone(&Utc),
                    },
                    role: serde_json::from_str(row.try_get("role")?)?,
                    joined_at: DateTime::parse_from_rfc3339(row.try_get("joined_at")?)?
                        .with_timezone(&Utc),
                })
            })
            .collect()
    }

    pub async fn workspace_threads(
        &self,
        workspace_id: Uuid,
        viewer_id: Uuid,
    ) -> Result<Vec<Thread>> {
        self.require_member(workspace_id, viewer_id).await?;
        let rows = sqlx::query(
            "select id,title,created_at from workspace_threads \
             where workspace_id=? order by created_at,id",
        )
        .bind(workspace_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(Thread {
                    id: Uuid::parse_str(row.try_get("id")?)?,
                    workspace_id,
                    title: row.try_get("title")?,
                    created_at: DateTime::parse_from_rfc3339(row.try_get("created_at")?)?
                        .with_timezone(&Utc),
                })
            })
            .collect()
    }

    /// Idempotently materialize the stable local identities for one execution
    /// session. Session events remain authoritative; this only establishes the
    /// workspace projection they attach to.
    pub async fn ensure_execution_workspace(
        &self,
        workspace_id: Uuid,
        workspace_name: &str,
        human_participant_id: Uuid,
        human_display_name: &str,
        agent_participant_id: Uuid,
        agent_display_name: &str,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let mut transaction = self.write().await?;
        sqlx::query(
            "insert into workspaces(id,name,created_at) values(?,?,?) \
             on conflict(id) do nothing",
        )
        .bind(workspace_id.to_string())
        .bind(workspace_name)
        .bind(&now)
        .execute(&mut *transaction)
        .await?;
        for (participant_id, display_name, kind) in [
            (
                human_participant_id,
                human_display_name,
                ParticipantKind::Human,
            ),
            (
                agent_participant_id,
                agent_display_name,
                ParticipantKind::Agent,
            ),
        ] {
            sqlx::query(
                "insert into workspace_participants(id,display_name,kind,created_at) \
                 values(?,?,?,?) on conflict(id) do update set display_name=excluded.display_name",
            )
            .bind(participant_id.to_string())
            .bind(display_name)
            .bind(serde_json::to_string(&kind)?)
            .bind(&now)
            .execute(&mut *transaction)
            .await?;
        }
        for (participant_id, role) in [
            (human_participant_id, WorkspaceRole::Owner),
            (agent_participant_id, WorkspaceRole::Editor),
        ] {
            sqlx::query(
                "insert into workspace_members(workspace_id,participant_id,role,joined_at) \
                 values(?,?,?,?) on conflict(workspace_id,participant_id) \
                 do update set role=excluded.role",
            )
            .bind(workspace_id.to_string())
            .bind(participant_id.to_string())
            .bind(serde_json::to_string(&role)?)
            .bind(&now)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    pub async fn transition_message_delivery(
        &self,
        workspace_id: Uuid,
        message_id: Uuid,
        recipient_id: Uuid,
        state: DeliveryState,
        attempt: Option<DeliveryAttempt>,
    ) -> Result<Option<RecipientDelivery>> {
        let sequence: Option<i64> = sqlx::query_scalar(
            "select sequence from workspace_events \
             where workspace_id=? and id=? \
               and json_extract(event_json, '$.kind.type')='message'",
        )
        .bind(workspace_id.to_string())
        .bind(message_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        let Some(sequence) = sequence else {
            return Ok(None);
        };
        // A participant outside the message's audience simply has nothing to
        // transition; that is a quiet no-op, not a session-fatal error.
        let addressed: i64 = sqlx::query_scalar(
            "select exists(select 1 from workspace_deliveries \
             where workspace_id=? and sequence=? and recipient_id=?)",
        )
        .bind(workspace_id.to_string())
        .bind(sequence)
        .bind(recipient_id.to_string())
        .fetch_one(&self.pool)
        .await?;
        if addressed == 0 {
            return Ok(None);
        }
        Ok(Some(
            <Self as WorkspaceStore>::transition_delivery(
                self,
                workspace_id,
                u64::try_from(sequence)?,
                recipient_id,
                state,
                attempt,
            )
            .await?,
        ))
    }

    /// Highest session sequence already projected into this workspace.
    ///
    /// The projection is append-only and strictly ordered, so a restart only
    /// has to replay above this watermark.  Walking the whole transcript to
    /// re-prove idempotency costs one round trip per event, which on a long
    /// session dominates startup.
    pub async fn latest_projected_session_sequence(
        &self,
        workspace_id: Uuid,
        session_id: Uuid,
    ) -> Result<u64> {
        let sequence: Option<i64> = sqlx::query_scalar(
            "select max(json_extract(event_json, '$.kind.session_sequence')) \
             from workspace_events \
             where workspace_id=? \
               and json_extract(event_json, '$.kind.type')='session_event' \
               and json_extract(event_json, '$.kind.session_id')=?",
        )
        .bind(workspace_id.to_string())
        .bind(session_id.to_string())
        .fetch_one(&self.pool)
        .await?;
        Ok(sequence
            .and_then(|sequence| u64::try_from(sequence).ok())
            .unwrap_or(0))
    }

    pub async fn contains_idempotent_event(
        &self,
        workspace_id: Uuid,
        author_id: Uuid,
        idempotency_key: &str,
    ) -> Result<bool> {
        let exists: i64 = sqlx::query_scalar(
            "select exists(select 1 from workspace_events \
             where workspace_id=? and author_id=? and idempotency_key=?)",
        )
        .bind(workspace_id.to_string())
        .bind(author_id.to_string())
        .bind(idempotency_key)
        .fetch_one(&self.pool)
        .await?;
        Ok(exists != 0)
    }

    pub async fn pending_message_events(
        &self,
        workspace_id: Uuid,
        recipient_id: Uuid,
        limit: usize,
    ) -> Result<Vec<(WorkspaceEvent, RecipientDelivery)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        // Session projections also create workspace deliveries and normally
        // remain pending forever. Keep the message bit on the delivery row so
        // this 50 ms inbox poll can seek directly to the pending-message
        // intersection instead of scanning either all recipient deliveries or
        // every settled workspace message.
        let rows = sqlx::query(
            "select d.sequence,d.recipient_id,d.mode,d.state,d.attempts,d.last_attempt_json,e.event_json \
             from workspace_deliveries d indexed by idx_workspace_pending_message_deliveries \
             join workspace_events e \
               on e.workspace_id=d.workspace_id and e.sequence=d.sequence \
             where d.workspace_id=? and d.recipient_id=? \
               and d.is_message=1 and d.state='\"pending\"' \
             order by d.sequence limit ?",
        )
        .bind(workspace_id.to_string())
        .bind(recipient_id.to_string())
        .bind(i64::try_from(limit)?)
        .fetch_all(&self.pool)
        .await?;
        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let sequence = u64::try_from(row.get::<i64, _>("sequence"))?;
            let event_json: String = row.get("event_json");
            let event = serde_json::from_str(&event_json)?;
            let delivery = RecipientDelivery {
                workspace_id,
                sequence,
                recipient_id: Uuid::parse_str(row.get("recipient_id"))?,
                mode: serde_json::from_str(row.get("mode"))?,
                state: serde_json::from_str(row.get("state"))?,
                attempts: u32::try_from(row.get::<i64, _>("attempts"))?,
                last_attempt: row
                    .get::<Option<String>, _>("last_attempt_json")
                    .map(|value| serde_json::from_str(&value))
                    .transpose()?,
            };
            result.push((event, delivery));
        }
        Ok(result)
    }
    async fn write(&self) -> Result<Transaction<'static, Sqlite>, sqlx::Error> {
        self.pool.begin_with(WRITE_TRANSACTION).await
    }
    async fn schema(&self) -> Result<()> {
        sqlx::raw_sql(r#"
      create table if not exists borg_workspace_schema (id integer primary key check(id=1), version integer not null);
      create table if not exists workspace_participants (id text primary key, display_name text not null, kind text not null, created_at text not null);
      create table if not exists workspaces (id text primary key, name text not null, next_sequence integer not null default 1, created_at text not null);
      create table if not exists workspace_members (workspace_id text not null references workspaces(id) on delete cascade, participant_id text not null references workspace_participants(id), role text not null, joined_at text not null, primary key(workspace_id,participant_id));
      create table if not exists workspace_threads (id text primary key, workspace_id text not null references workspaces(id) on delete cascade, title text not null, created_at text not null);
      create table if not exists workspace_events (workspace_id text not null references workspaces(id) on delete cascade, sequence integer not null, id text not null, author_id text not null references workspace_participants(id), idempotency_key text not null, canonical_json text not null, event_json text not null, created_at text not null, primary key(workspace_id,sequence), unique(workspace_id,author_id,idempotency_key));
      create table if not exists workspace_work_items (workspace_id text not null references workspaces(id) on delete cascade, work_id text not null, created_sequence integer not null, primary key(workspace_id,work_id));
      create table if not exists workspace_work_claims (workspace_id text not null references workspaces(id) on delete cascade, work_id text not null, claim_id text not null, claimant_id text not null references workspace_participants(id), sequence integer not null, primary key(workspace_id,work_id));
      create table if not exists workspace_work_dependencies (workspace_id text not null references workspaces(id) on delete cascade, work_id text not null, depends_on_work_id text not null, sequence integer not null, primary key(workspace_id,work_id,depends_on_work_id));
      create table if not exists workspace_deliveries (workspace_id text not null, sequence integer not null, recipient_id text not null references workspace_participants(id), mode text not null, state text not null, attempts integer not null default 0, last_attempt_json text, is_message integer not null default 0, primary key(workspace_id,sequence,recipient_id), foreign key(workspace_id,sequence) references workspace_events(workspace_id,sequence) on delete cascade);
      create table if not exists workspace_presence_leases (workspace_id text not null references workspaces(id) on delete cascade, participant_id text not null references workspace_participants(id), client_id text not null, host_id text, expires_at text not null, primary key(workspace_id,participant_id,client_id));
      create index if not exists idx_workspace_delivery_recipient on workspace_deliveries(workspace_id,recipient_id,sequence);
      create index if not exists idx_workspace_events_id on workspace_events(workspace_id,id);
      create index if not exists idx_workspace_events_messages on workspace_events(workspace_id,sequence) where json_extract(event_json, '$.kind.type')='message';
    "#).execute(&self.pool).await?;
        let columns = sqlx::query("pragma table_info(workspace_deliveries)")
            .fetch_all(&self.pool)
            .await?;
        ensure!(
            columns
                .iter()
                .any(|column| column.get::<String, _>("name") == "is_message"),
            "workspace database is stale: workspace_deliveries.is_message is missing; recreate or explicitly export/import this database"
        );
        let version: Option<i64> =
            sqlx::query_scalar("select version from borg_workspace_schema where id=1")
                .fetch_optional(&self.pool)
                .await?;
        match version {
            Some(version) => ensure!(
                version == WORKSPACE_SCHEMA_VERSION,
                "workspace database schema version {version} is unsupported; expected {WORKSPACE_SCHEMA_VERSION}"
            ),
            None => {
                sqlx::query("insert into borg_workspace_schema(id,version) values(1,?)")
                    .bind(WORKSPACE_SCHEMA_VERSION)
                    .execute(&self.pool)
                    .await?;
            }
        }
        sqlx::raw_sql(
            r#"create index if not exists idx_workspace_pending_message_deliveries
               on workspace_deliveries(workspace_id,recipient_id,sequence)
               where is_message=1 and state='"pending"';"#,
        )
        .execute(&self.pool)
        .await?;
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

    async fn work_exists(
        tx: &mut Transaction<'static, Sqlite>,
        workspace_id: Uuid,
        work_id: Uuid,
    ) -> Result<bool> {
        let found: i64 = sqlx::query_scalar(
            "select exists(select 1 from workspace_work_items \
             where workspace_id=? and work_id=?)",
        )
        .bind(workspace_id.to_string())
        .bind(work_id.to_string())
        .fetch_one(&mut **tx)
        .await?;
        Ok(found != 0)
    }

    async fn require_member(&self, workspace_id: Uuid, participant_id: Uuid) -> Result<()> {
        let found: i64 = sqlx::query_scalar(
            "select exists(select 1 from workspace_members \
             where workspace_id=? and participant_id=?)",
        )
        .bind(workspace_id.to_string())
        .bind(participant_id.to_string())
        .fetch_one(&self.pool)
        .await?;
        ensure!(found != 0, "viewer is not a workspace member");
        Ok(())
    }

    fn canonical_event(mut event: WorkspaceEvent) -> Result<String> {
        let epoch = DateTime::<Utc>::from_timestamp(0, 0).expect("Unix epoch is valid");
        event.id = Uuid::nil();
        event.sequence = 0;
        event.created_at = epoch;
        if let WorkspaceEventKind::Message { message, .. } = &mut event.kind {
            message.id = Uuid::nil();
            message.created_at = epoch;
        }
        Ok(serde_json::to_string(&event)?)
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
        let canonical = Self::canonical_event(e.clone())?;
        if let Some(row) = sqlx::query(
            "select canonical_json,event_json from workspace_events \
             where workspace_id=? and author_id=? and idempotency_key=?",
        )
        .bind(e.workspace_id.to_string())
        .bind(e.author_id.to_string())
        .bind(&e.idempotency_key)
        .fetch_optional(&mut *tx)
        .await?
        {
            if row.get::<String, _>("canonical_json") != canonical {
                bail!("idempotency conflict: key was used with a different payload");
            }
            return Ok(serde_json::from_str(row.get("event_json"))?);
        }
        let members = self.members(&mut tx, e.workspace_id).await?;
        ensure!(
            members.iter().any(|(id, _)| *id == e.author_id),
            "author is not a workspace member"
        );
        if let WorkspaceEventKind::WorkCreated { work, .. } = &e.kind {
            ensure!(
                !work.title.trim().is_empty(),
                "work title must not be empty"
            );
            ensure!(
                !Self::work_exists(&mut tx, e.workspace_id, work.id).await?,
                "work already exists in workspace"
            );
        }
        if let WorkspaceEventKind::ArtifactPublished { artifact, .. } = &e.kind {
            ensure!(
                !artifact.name.trim().is_empty() && !artifact.uri.trim().is_empty(),
                "artifact name and URI must not be empty"
            );
            if let Some(work_id) = artifact.work_id {
                ensure!(
                    Self::work_exists(&mut tx, e.workspace_id, work_id).await?,
                    "artifact work item is not in workspace"
                );
            }
        }
        if let WorkspaceEventKind::DecisionRecorded { decision, .. } = &e.kind {
            ensure!(
                !decision.subject.trim().is_empty() && !decision.outcome.trim().is_empty(),
                "decision subject and outcome must not be empty"
            );
        }
        if let WorkspaceEventKind::WorkClaimed { claim, .. } = &e.kind {
            ensure!(
                Self::work_exists(&mut tx, e.workspace_id, claim.work_id).await?,
                "claimed work item is not in workspace"
            );
            ensure!(
                members.iter().any(|(id, _)| *id == claim.claimant_id),
                "claimant is not a workspace member"
            );
            let current: Option<String> = sqlx::query_scalar(
                "select claim_id from workspace_work_claims where workspace_id=? and work_id=?",
            )
            .bind(e.workspace_id.to_string())
            .bind(claim.work_id.to_string())
            .fetch_optional(&mut *tx)
            .await?;
            ensure!(
                current.as_deref()
                    == claim
                        .expected_claim_id
                        .as_ref()
                        .map(Uuid::to_string)
                        .as_deref(),
                "atomic claim conflict"
            );
        }
        if let WorkspaceEventKind::DependencyDeclared { dependency, .. } = &e.kind {
            ensure!(
                dependency.work_id != dependency.depends_on_work_id,
                "work cannot depend on itself"
            );
            ensure!(
                Self::work_exists(&mut tx, e.workspace_id, dependency.work_id).await?
                    && Self::work_exists(&mut tx, e.workspace_id, dependency.depends_on_work_id,)
                        .await?,
                "dependency work item is not in workspace"
            );
        }
        if let WorkspaceEventKind::ReviewRequested { request, .. } = &e.kind {
            ensure!(
                Self::work_exists(&mut tx, e.workspace_id, request.work_id).await?,
                "reviewed work item is not in workspace"
            );
            if let Some(reviewer_id) = request.requested_reviewer_id {
                ensure!(
                    members.iter().any(|(id, _)| *id == reviewer_id),
                    "requested reviewer is not a workspace member"
                );
            }
        }
        if let WorkspaceEventKind::ReviewRecorded { review, .. } = &e.kind {
            ensure!(
                Self::work_exists(&mut tx, e.workspace_id, review.work_id).await?,
                "reviewed work item is not in workspace"
            );
            ensure!(
                members.iter().any(|(id, _)| *id == review.reviewer_id),
                "reviewer is not a workspace member"
            );
            ensure!(
                !review.verdict.trim().is_empty(),
                "review verdict must not be empty"
            );
        }
        if let WorkspaceEventKind::ReferenceAdded { reference, .. } = &e.kind {
            ensure!(
                !reference.label.trim().is_empty() && !reference.target.trim().is_empty(),
                "reference label and target must not be empty"
            );
        }
        if let WorkspaceEventKind::ProvenanceRecorded { provenance, .. } = &e.kind {
            ensure!(
                !provenance.source_kind.trim().is_empty()
                    && !provenance.source_id.trim().is_empty(),
                "provenance source kind and source id must not be empty"
            );
        }
        let (mode, audience) = match &e.kind {
            WorkspaceEventKind::Message { message, mode } => {
                ensure!(
                    message.workspace_id == e.workspace_id && message.author_id == e.author_id,
                    "message workspace/author mismatch"
                );
                if let Some(thread) = message.thread_id {
                    let found: i64 = sqlx::query_scalar(
                        "select exists(select 1 from workspace_threads \
                         where id=? and workspace_id=?)",
                    )
                    .bind(thread.to_string())
                    .bind(e.workspace_id.to_string())
                    .fetch_one(&mut *tx)
                    .await?;
                    ensure!(found != 0, "thread is not in workspace");
                }
                if let Some(reply_to) = message.reply_to_message_id {
                    let found: i64 = sqlx::query_scalar(
                        "select exists(\
                            select 1 from workspace_events \
                            where workspace_id=? \
                              and json_extract(event_json, '$.kind.message.id')=?\
                         )",
                    )
                    .bind(e.workspace_id.to_string())
                    .bind(reply_to.to_string())
                    .fetch_one(&mut *tx)
                    .await?;
                    ensure!(found != 0, "reply target is not in workspace");
                }
                (*mode, &message.audience)
            }
            WorkspaceEventKind::SessionEvent { mode, .. }
            | WorkspaceEventKind::WorkCreated { mode, .. }
            | WorkspaceEventKind::ArtifactPublished { mode, .. }
            | WorkspaceEventKind::DecisionRecorded { mode, .. }
            | WorkspaceEventKind::WorkClaimed { mode, .. }
            | WorkspaceEventKind::DependencyDeclared { mode, .. }
            | WorkspaceEventKind::ReviewRequested { mode, .. }
            | WorkspaceEventKind::ReviewRecorded { mode, .. }
            | WorkspaceEventKind::ReferenceAdded { mode, .. }
            | WorkspaceEventKind::ProvenanceRecorded { mode, .. } => (*mode, &Audience::Workspace),
        };
        let mut recipients = Self::recipients(audience, &members)?;
        let visible_participants = recipients
            .iter()
            .copied()
            .chain(std::iter::once(e.author_id))
            .collect::<Vec<_>>();
        if let WorkspaceEventKind::Message { message, .. } = &e.kind {
            for mention in &message.body.mentions {
                let start = usize::try_from(mention.start)?;
                let end = usize::try_from(mention.end)?;
                ensure!(
                    start < end
                        && end <= message.body.text.len()
                        && message.body.text.is_char_boundary(start)
                        && message.body.text.is_char_boundary(end),
                    "mention range is not a valid UTF-8 text range"
                );
                ensure!(
                    visible_participants.contains(&mention.participant_id),
                    "mentioned participant is not visible in the message audience"
                );
            }
        }
        recipients.retain(|recipient| *recipient != e.author_id);
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
        if let WorkspaceEventKind::WorkCreated { work, .. } = &e.kind {
            sqlx::query("insert into workspace_work_items values(?,?,?)")
                .bind(e.workspace_id.to_string())
                .bind(work.id.to_string())
                .bind(seq)
                .execute(&mut *tx)
                .await?;
        }
        if let WorkspaceEventKind::WorkClaimed { claim, .. } = &e.kind {
            sqlx::query(
                "insert into workspace_work_claims values(?,?,?,?,?) \
                 on conflict(workspace_id,work_id) do update set \
                 claim_id=excluded.claim_id, claimant_id=excluded.claimant_id, sequence=excluded.sequence",
            )
            .bind(e.workspace_id.to_string())
            .bind(claim.work_id.to_string())
            .bind(e.id.to_string())
            .bind(claim.claimant_id.to_string())
            .bind(seq)
            .execute(&mut *tx)
            .await?;
        }
        if let WorkspaceEventKind::DependencyDeclared { dependency, .. } = &e.kind {
            sqlx::query("insert into workspace_work_dependencies values(?,?,?,?)")
                .bind(e.workspace_id.to_string())
                .bind(dependency.work_id.to_string())
                .bind(dependency.depends_on_work_id.to_string())
                .bind(seq)
                .execute(&mut *tx)
                .await?;
        }
        let is_message = matches!(&e.kind, WorkspaceEventKind::Message { .. });
        for recipient in recipients {
            sqlx::query("insert into workspace_deliveries(workspace_id,sequence,recipient_id,mode,state,is_message) values(?,?,?,?,?,?)").bind(e.workspace_id.to_string()).bind(seq).bind(recipient.to_string()).bind(serde_json::to_string(&mode)?).bind(serde_json::to_string(&DeliveryState::Pending)?).bind(is_message).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(e)
    }

    /// Append session-event projections in one durable transaction.
    ///
    /// Session events are already validated by the canonical session journal;
    /// this path only materializes the workspace reference rows. Keeping the
    /// idempotency check and all inserts in one transaction makes repair fast
    /// without weakening the workspace journal's atomicity.
    async fn append_session_event_batch(&self, events: &[WorkspaceEvent]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let workspace_id = events[0].workspace_id;
        anyhow::ensure!(
            events.iter().all(|event| {
                event.workspace_id == workspace_id
                    && matches!(&event.kind, WorkspaceEventKind::SessionEvent { .. })
            }),
            "session projection batch contains an incompatible workspace event"
        );
        let mut tx = self.write().await?;
        let members = self.members(&mut tx, workspace_id).await?;
        let member_ids = members.into_iter().map(|(id, _)| id).collect::<Vec<_>>();

        for source in events {
            let WorkspaceEventKind::SessionEvent { mode, .. } = &source.kind else {
                unreachable!("session projection batch was validated above")
            };
            anyhow::ensure!(
                member_ids.contains(&source.author_id),
                "author is not a workspace member"
            );
            let canonical = Self::canonical_event(source.clone())?;
            if let Some(row) = sqlx::query(
                "select canonical_json from workspace_events \
                 where workspace_id=? and author_id=? and idempotency_key=?",
            )
            .bind(workspace_id.to_string())
            .bind(source.author_id.to_string())
            .bind(&source.idempotency_key)
            .fetch_optional(&mut *tx)
            .await?
            {
                anyhow::ensure!(
                    row.get::<String, _>("canonical_json") == canonical,
                    "idempotency conflict: key was used with a different payload"
                );
                continue;
            }

            let sequence: i64 = sqlx::query_scalar(
                "update workspaces set next_sequence=next_sequence+1 \
                 where id=? returning next_sequence-1",
            )
            .bind(workspace_id.to_string())
            .fetch_one(&mut *tx)
            .await?;
            let mut event = source.clone();
            event.sequence = u64::try_from(sequence)?;
            let event_json = serde_json::to_string(&event)?;
            sqlx::query(
                "insert into workspace_events \
                 (workspace_id,sequence,id,author_id,idempotency_key,canonical_json,event_json,created_at) \
                 values(?,?,?,?,?,?,?,?)",
            )
            .bind(workspace_id.to_string())
            .bind(sequence)
            .bind(event.id.to_string())
            .bind(event.author_id.to_string())
            .bind(&event.idempotency_key)
            .bind(canonical)
            .bind(event_json)
            .bind(event.created_at.to_rfc3339())
            .execute(&mut *tx)
            .await?;

            let mode_json = serde_json::to_string(mode)?;
            let pending_json = serde_json::to_string(&DeliveryState::Pending)?;
            for recipient in member_ids
                .iter()
                .copied()
                .filter(|id| *id != event.author_id)
            {
                sqlx::query(
                    "insert into workspace_deliveries \
                     (workspace_id,sequence,recipient_id,mode,state,is_message) \
                     values(?,?,?,?,?,0)",
                )
                .bind(workspace_id.to_string())
                .bind(sequence)
                .bind(recipient.to_string())
                .bind(&mode_json)
                .bind(&pending_json)
                .execute(&mut *tx)
                .await?;
            }
        }
        tx.commit().await?;
        Ok(())
    }

    async fn replay(
        &self,
        w: Uuid,
        viewer: Uuid,
        after: u64,
        limit: usize,
    ) -> Result<Vec<WorkspaceEvent>> {
        let member: i64 = sqlx::query_scalar(
            "select exists(select 1 from workspace_members \
             where workspace_id=? and participant_id=?)",
        )
        .bind(w.to_string())
        .bind(viewer.to_string())
        .fetch_one(&self.pool)
        .await?;
        ensure!(member != 0, "viewer is not a workspace member");
        let rows = sqlx::query(
            "select e.event_json from workspace_events e \
             where e.workspace_id=? and e.sequence>? \
               and (e.author_id=? or exists(\
                    select 1 from workspace_deliveries d \
                    where d.workspace_id=e.workspace_id and d.sequence=e.sequence \
                      and d.recipient_id=?\
               )) \
             order by e.sequence limit ?",
        )
        .bind(w.to_string())
        .bind(i64::try_from(after)?)
        .bind(viewer.to_string())
        .bind(viewer.to_string())
        .bind(i64::try_from(limit)?)
        .fetch_all(&self.pool)
        .await?;
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
        if current == next {
            tx.commit().await?;
            return Ok(RecipientDelivery {
                workspace_id: w,
                sequence: s,
                recipient_id: p,
                mode: serde_json::from_str(row.get("mode"))?,
                state: current,
                attempts: u32::try_from(row.get::<i64, _>("attempts"))?,
                last_attempt: row
                    .get::<Option<String>, _>("last_attempt_json")
                    .map(|value| serde_json::from_str(&value))
                    .transpose()?,
            });
        }
        let allowed = matches!(
            (current, next),
            (
                DeliveryState::Pending,
                DeliveryState::Admitted | DeliveryState::Failed | DeliveryState::Recalled
            ) | (
                DeliveryState::Admitted,
                DeliveryState::Acknowledged | DeliveryState::Failed
            ) | (DeliveryState::Failed, DeliveryState::Pending)
        );
        ensure!(allowed, "invalid non-monotonic delivery transition");
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
        let member: i64 = sqlx::query_scalar(
            "select exists(select 1 from workspace_members \
             where workspace_id=? and participant_id=?)",
        )
        .bind(l.workspace_id.to_string())
        .bind(l.participant_id.to_string())
        .fetch_one(&self.pool)
        .await?;
        ensure!(
            member != 0,
            "presence participant is not a workspace member"
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

    #[test]
    fn local_human_identity_is_stable_across_workspaces() {
        assert_eq!(
            local_human_participant_id("shulgin"),
            local_human_participant_id(" shulgin ")
        );
        assert_ne!(
            local_human_participant_id("shulgin"),
            local_human_participant_id("teammate")
        );
    }

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

    #[tokio::test]
    async fn workspace_listing_roster_and_threads_are_member_scoped() {
        let (store, workspace, owner, agent, outsider) = fixture().await;
        store
            .create_thread(Thread {
                id: Uuid::new_v4(),
                workspace_id: workspace.id,
                title: "Coordination".into(),
                created_at: Utc::now(),
            })
            .await
            .unwrap();

        assert_eq!(
            store
                .list_workspaces_for_participant(owner.id)
                .await
                .unwrap(),
            vec![workspace.clone()]
        );
        let roster = store
            .workspace_roster(workspace.id, owner.id)
            .await
            .unwrap();
        assert_eq!(roster.len(), 2);
        assert!(roster.iter().any(|entry| entry.participant.id == agent.id));
        assert_eq!(
            store
                .workspace_threads(workspace.id, owner.id)
                .await
                .unwrap()[0]
                .title,
            "Coordination"
        );
        assert!(
            store
                .workspace_roster(workspace.id, outsider.id)
                .await
                .unwrap_err()
                .to_string()
                .contains("not a workspace member")
        );
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
    async fn session_event_projection_batch_is_idempotent_and_visible() {
        let (store, workspace, human, agent, _) = fixture().await;
        let session_id = Uuid::new_v4();
        let events = (1..=1024)
            .map(|session_sequence| WorkspaceEvent {
                id: Uuid::new_v4(),
                workspace_id: workspace.id,
                sequence: 0,
                author_id: agent.id,
                idempotency_key: format!("session-projection-{session_sequence}"),
                created_at: Utc::now(),
                kind: WorkspaceEventKind::SessionEvent {
                    session_id,
                    session_event_id: Uuid::new_v4(),
                    session_sequence,
                    mode: DeliveryMode::Notify,
                },
            })
            .collect::<Vec<_>>();

        store.append_session_event_batch(&events).await.unwrap();
        // A retried repair batch must be a no-op, including its deliveries.
        store.append_session_event_batch(&events).await.unwrap();

        let visible_to_agent = store.replay(workspace.id, agent.id, 0, 2048).await.unwrap();
        assert_eq!(visible_to_agent.len(), events.len());
        assert!(
            visible_to_agent
                .iter()
                .enumerate()
                .all(|(index, event)| { event.sequence == u64::try_from(index + 1).unwrap() })
        );

        let visible_to_human = store.replay(workspace.id, human.id, 0, 2048).await.unwrap();
        assert_eq!(visible_to_human.len(), events.len());
    }

    #[tokio::test]
    async fn mentions_and_replies_are_validated_by_the_single_workspace_authority() {
        let (s, w, a, b, c) = fixture().await;
        let first = s
            .append(event(
                w.id,
                a.id,
                Audience::Direct { participant: b.id },
                "first",
            ))
            .await
            .unwrap();
        let WorkspaceEventKind::Message {
            message: first_message,
            ..
        } = first.kind
        else {
            panic!("message event")
        };
        let mut reply = event(w.id, a.id, Audience::Direct { participant: b.id }, "reply");
        let WorkspaceEventKind::Message { message, .. } = &mut reply.kind else {
            panic!("message event")
        };
        message.reply_to_message_id = Some(first_message.id);
        message.body = WorkspaceMessageBody {
            text: "hé".into(),
            mentions: vec![StructuredMention {
                participant_id: b.id,
                start: 0,
                end: 3,
            }],
        };
        s.append(reply).await.unwrap();

        let mut invalid = event(
            w.id,
            a.id,
            Audience::Direct { participant: b.id },
            "invalid",
        );
        let WorkspaceEventKind::Message { message, .. } = &mut invalid.kind else {
            panic!("message event")
        };
        message.body.mentions = vec![StructuredMention {
            participant_id: c.id,
            start: 1,
            end: 2,
        }];
        assert!(s.append(invalid).await.is_err());
    }

    #[tokio::test]
    async fn exact_idempotent_retry_returns_the_original_event() {
        let (s, w, a, b, _) = fixture().await;
        let original = event(w.id, a.id, Audience::Direct { participant: b.id }, "same");
        let first = s.append(original.clone()).await.unwrap();
        let mut retry_envelope = event(w.id, a.id, Audience::Direct { participant: b.id }, "same");
        retry_envelope.created_at = original.created_at + chrono::Duration::seconds(1);
        if let WorkspaceEventKind::Message { message, .. } = &mut retry_envelope.kind {
            message.created_at = retry_envelope.created_at;
        }
        let retry = s.append(retry_envelope).await.unwrap();
        assert_eq!(retry, first);
        assert_eq!(s.replay(w.id, a.id, 0, 10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn delivery_outcomes_are_independent() {
        let (s, w, a, b, c) = fixture().await;
        s.add_member(WorkspaceMembership {
            workspace_id: w.id,
            participant_id: c.id,
            role: WorkspaceRole::Contributor,
            joined_at: Utc::now(),
        })
        .await
        .unwrap();
        let e = s
            .append(event(w.id, a.id, Audience::Workspace, "k"))
            .await
            .unwrap();
        s.transition_delivery(w.id, e.sequence, b.id, DeliveryState::Admitted, None)
            .await
            .unwrap();
        s.transition_delivery(
            w.id,
            e.sequence,
            c.id,
            DeliveryState::Failed,
            Some(DeliveryAttempt {
                attempted_at: Utc::now(),
                detail: Some("offline".into()),
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            s.deliveries_after(w.id, b.id, 0, 2).await.unwrap()[0].state,
            DeliveryState::Admitted
        );
        assert_eq!(
            s.deliveries_after(w.id, c.id, 0, 2).await.unwrap()[0].state,
            DeliveryState::Failed
        );
        assert!(
            s.transition_delivery(w.id, e.sequence, b.id, DeliveryState::Recalled, None)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn recalled_pending_message_is_not_redelivered() {
        let (store, workspace, author, recipient, _) = fixture().await;
        let event = store
            .append(event(
                workspace.id,
                author.id,
                Audience::Direct {
                    participant: recipient.id,
                },
                "recall-before-admission",
            ))
            .await
            .unwrap();
        let message_id = match &event.kind {
            WorkspaceEventKind::Message { message, .. } => message.id,
            _ => unreachable!(),
        };
        assert_eq!(
            store
                .pending_message_events(workspace.id, recipient.id, 10)
                .await
                .unwrap()[0]
                .0
                .id,
            event.id
        );
        store
            .transition_delivery(
                workspace.id,
                event.sequence,
                recipient.id,
                DeliveryState::Recalled,
                None,
            )
            .await
            .unwrap();
        assert!(
            store
                .pending_message_events(workspace.id, recipient.id, 10)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store
                .deliveries_after(workspace.id, recipient.id, 0, 10)
                .await
                .unwrap()[0]
                .state,
            DeliveryState::Recalled
        );
        assert_ne!(message_id, Uuid::nil());
    }

    #[tokio::test]
    async fn pending_message_limit_ignores_older_non_message_deliveries() {
        let (store, workspace, author, recipient, _) = fixture().await;
        store
            .append(WorkspaceEvent {
                id: Uuid::new_v4(),
                workspace_id: workspace.id,
                sequence: 0,
                author_id: author.id,
                idempotency_key: "older-session-event".into(),
                created_at: Utc::now(),
                kind: WorkspaceEventKind::SessionEvent {
                    session_id: Uuid::new_v4(),
                    session_event_id: Uuid::new_v4(),
                    session_sequence: 1,
                    mode: DeliveryMode::Notify,
                },
            })
            .await
            .unwrap();
        let message = store
            .append(event(
                workspace.id,
                author.id,
                Audience::Direct {
                    participant: recipient.id,
                },
                "newer-message",
            ))
            .await
            .unwrap();
        let pending = store
            .pending_message_events(workspace.id, recipient.id, 1)
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0.id, message.id);
        assert_eq!(pending[0].1.sequence, message.sequence);
    }

    #[tokio::test]
    async fn pending_message_limit_ignores_older_settled_messages() {
        let (store, workspace, author, recipient, _) = fixture().await;
        let settled = store
            .append(event(
                workspace.id,
                author.id,
                Audience::Direct {
                    participant: recipient.id,
                },
                "settled-message",
            ))
            .await
            .unwrap();
        store
            .transition_delivery(
                workspace.id,
                settled.sequence,
                recipient.id,
                DeliveryState::Admitted,
                None,
            )
            .await
            .unwrap();
        store
            .transition_delivery(
                workspace.id,
                settled.sequence,
                recipient.id,
                DeliveryState::Acknowledged,
                None,
            )
            .await
            .unwrap();
        let pending_message = store
            .append(event(
                workspace.id,
                author.id,
                Audience::Direct {
                    participant: recipient.id,
                },
                "pending-message",
            ))
            .await
            .unwrap();

        let pending = store
            .pending_message_events(workspace.id, recipient.id, 1)
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0.id, pending_message.id);
        assert_eq!(pending[0].1.sequence, pending_message.sequence);
    }

    #[tokio::test]
    async fn failed_message_retry_reappears_without_a_new_workspace_event() {
        let (store, workspace, author, recipient, _) = fixture().await;
        let message = store
            .append(event(
                workspace.id,
                author.id,
                Audience::Direct {
                    participant: recipient.id,
                },
                "retry-message",
            ))
            .await
            .unwrap();
        store
            .transition_delivery(
                workspace.id,
                message.sequence,
                recipient.id,
                DeliveryState::Failed,
                None,
            )
            .await
            .unwrap();
        assert!(
            store
                .pending_message_events(workspace.id, recipient.id, 1)
                .await
                .unwrap()
                .is_empty()
        );

        store
            .transition_delivery(
                workspace.id,
                message.sequence,
                recipient.id,
                DeliveryState::Pending,
                None,
            )
            .await
            .unwrap();
        let pending = store
            .pending_message_events(workspace.id, recipient.id, 1)
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0.id, message.id);
        assert_eq!(pending[0].1.state, DeliveryState::Pending);
    }

    #[tokio::test]
    async fn pending_message_query_seeks_the_delivery_intersection_index() {
        let (store, workspace, _, recipient, _) = fixture().await;
        let rows = sqlx::query(
            "explain query plan \
             select d.sequence,d.recipient_id,d.mode,d.state,d.attempts,d.last_attempt_json,e.event_json \
             from workspace_deliveries d indexed by idx_workspace_pending_message_deliveries \
             join workspace_events e \
               on e.workspace_id=d.workspace_id and e.sequence=d.sequence \
             where d.workspace_id=? and d.recipient_id=? \
               and d.is_message=1 and d.state='\"pending\"' \
             order by d.sequence limit ?",
        )
        .bind(workspace.id.to_string())
        .bind(recipient.id.to_string())
        .bind(10_i64)
        .fetch_all(store.pool())
        .await
        .unwrap();
        let plan = rows
            .iter()
            .map(|row| row.get::<String, _>("detail"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            plan.contains("idx_workspace_pending_message_deliveries"),
            "unexpected query plan: {plan}"
        );
    }

    #[tokio::test]
    async fn stale_workspace_store_is_rejected_instead_of_migrated() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let opts = SqliteConnectOptions::from_str(&format!("sqlite://{}", file.path().display()))
            .unwrap()
            .journal_mode(SqliteJournalMode::Wal)
            .foreign_keys(false);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(opts)
            .await
            .unwrap();
        sqlx::raw_sql(
            r#"
            create table workspace_deliveries (
              workspace_id text not null,
              sequence integer not null,
              recipient_id text not null,
              mode text not null,
              state text not null,
              attempts integer not null default 0,
              last_attempt_json text,
              primary key(workspace_id,sequence,recipient_id)
            );
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        let error = match SqliteWorkspaceStore::open(file.path()).await {
            Ok(_) => panic!("stale workspace schema was silently accepted"),
            Err(error) => error,
        };
        assert!(
            format!("{error:#}").contains("workspace_deliveries.is_message"),
            "unexpected stale-schema error: {error:#}"
        );
    }

    #[tokio::test]
    async fn boundary_and_wake_messages_preserve_independent_order_and_ids() {
        let (store, workspace, author, recipient, _) = fixture().await;
        let mut boundary = event(
            workspace.id,
            author.id,
            Audience::Direct {
                participant: recipient.id,
            },
            "boundary",
        );
        let boundary_id = match &boundary.kind {
            WorkspaceEventKind::Message { message, .. } => message.id,
            _ => unreachable!(),
        };
        if let WorkspaceEventKind::Message { mode, .. } = &mut boundary.kind {
            *mode = DeliveryMode::Boundary;
        }
        let mut wake = event(
            workspace.id,
            author.id,
            Audience::Direct {
                participant: recipient.id,
            },
            "wake",
        );
        let wake_id = match &wake.kind {
            WorkspaceEventKind::Message { message, .. } => message.id,
            _ => unreachable!(),
        };
        if let WorkspaceEventKind::Message { mode, .. } = &mut wake.kind {
            *mode = DeliveryMode::Wake;
        }
        store.append(boundary).await.unwrap();
        store.append(wake).await.unwrap();
        let pending = store
            .pending_message_events(workspace.id, recipient.id, 10)
            .await
            .unwrap();
        assert_eq!(pending.len(), 2);
        let ids = pending
            .iter()
            .map(|(event, _)| match &event.kind {
                WorkspaceEventKind::Message { message, .. } => message.id,
                _ => unreachable!(),
            })
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![boundary_id, wake_id]);
        assert_eq!(pending[0].1.mode, DeliveryMode::Boundary);
        assert_eq!(pending[1].1.mode, DeliveryMode::Wake);
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
        assert_eq!(s.replay(w.id, a.id, 0, 10).await.unwrap().len(), 1);
        assert_eq!(s.replay(w.id, b.id, 0, 10).await.unwrap().len(), 1);
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

    fn coordination_event(w: Uuid, a: Uuid, key: &str, kind: WorkspaceEventKind) -> WorkspaceEvent {
        WorkspaceEvent {
            id: Uuid::new_v4(),
            workspace_id: w,
            sequence: 0,
            author_id: a,
            idempotency_key: key.into(),
            created_at: Utc::now(),
            kind,
        }
    }

    #[tokio::test]
    async fn coordination_payloads_persist_and_replay() {
        let (s, w, a, b, _) = fixture().await;
        let work = SharedWork {
            id: Uuid::new_v4(),
            title: "implement kernel".into(),
            detail: None,
        };
        let dependency_work = SharedWork {
            id: Uuid::new_v4(),
            title: "define contracts".into(),
            detail: None,
        };
        let artifact = WorkspaceArtifact {
            id: Uuid::new_v4(),
            work_id: Some(work.id),
            name: "design.md".into(),
            media_type: Some("text/markdown".into()),
            uri: "workspace://design.md".into(),
            content_hash: Some("abc".into()),
        };
        let decision = WorkspaceDecision {
            id: Uuid::new_v4(),
            subject: "storage".into(),
            outcome: "sqlite".into(),
            rationale: Some("local durability".into()),
        };
        let variants = vec![
            WorkspaceEventKind::WorkCreated {
                work: work.clone(),
                mode: DeliveryMode::Notify,
            },
            WorkspaceEventKind::WorkCreated {
                work: dependency_work.clone(),
                mode: DeliveryMode::Notify,
            },
            WorkspaceEventKind::ArtifactPublished {
                artifact,
                mode: DeliveryMode::Notify,
            },
            WorkspaceEventKind::DecisionRecorded {
                decision,
                mode: DeliveryMode::Notify,
            },
            WorkspaceEventKind::WorkClaimed {
                claim: AtomicWorkClaim {
                    work_id: work.id,
                    claimant_id: b.id,
                    expected_claim_id: None,
                },
                mode: DeliveryMode::Notify,
            },
            WorkspaceEventKind::DependencyDeclared {
                dependency: WorkDependency {
                    work_id: work.id,
                    depends_on_work_id: dependency_work.id,
                },
                mode: DeliveryMode::Notify,
            },
            WorkspaceEventKind::ReviewRequested {
                request: WorkspaceReviewRequest {
                    id: Uuid::new_v4(),
                    work_id: work.id,
                    requested_reviewer_id: Some(b.id),
                    instructions: Some("check the evidence".into()),
                },
                mode: DeliveryMode::Notify,
            },
            WorkspaceEventKind::ReviewRecorded {
                review: WorkReview {
                    work_id: work.id,
                    reviewer_id: b.id,
                    verdict: "approved".into(),
                    detail: None,
                },
                mode: DeliveryMode::Notify,
            },
            WorkspaceEventKind::ReferenceAdded {
                reference: WorkspaceReference {
                    id: Uuid::new_v4(),
                    label: "spec".into(),
                    target: "https://example.invalid/spec".into(),
                },
                mode: DeliveryMode::Notify,
            },
            WorkspaceEventKind::ProvenanceRecorded {
                provenance: Provenance {
                    subject_id: work.id,
                    source_kind: "import".into(),
                    source_id: "legacy-7".into(),
                    detail: None,
                },
                mode: DeliveryMode::Notify,
            },
        ];
        for (index, kind) in variants.into_iter().enumerate() {
            s.append(coordination_event(
                w.id,
                a.id,
                &format!("coord-{index}"),
                kind,
            ))
            .await
            .unwrap();
        }
        let replay = s.replay(w.id, b.id, 0, 20).await.unwrap();
        assert_eq!(replay.len(), 10);
        assert!(matches!(
            replay[0].kind,
            WorkspaceEventKind::WorkCreated { .. }
        ));
        assert!(matches!(
            replay[9].kind,
            WorkspaceEventKind::ProvenanceRecorded { .. }
        ));
        assert!(
            s.append(coordination_event(
                w.id,
                a.id,
                "claim-conflict",
                WorkspaceEventKind::WorkClaimed {
                    claim: AtomicWorkClaim {
                        work_id: work.id,
                        claimant_id: b.id,
                        expected_claim_id: None
                    },
                    mode: DeliveryMode::Notify
                }
            ))
            .await
            .unwrap_err()
            .to_string()
            .contains("atomic claim conflict")
        );
    }
}
