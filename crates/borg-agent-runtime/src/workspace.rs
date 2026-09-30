//! Provider-neutral local multiplayer workspace kernel.
//!
//! Execution remains in `SessionEvent`; this module only records references to
//! it. Participants are global identities; workspace membership is durable.

use std::path::Path;

use anyhow::{Result, bail, ensure};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use ts_rs::TS;
use uuid::Uuid;

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

/// One entry of an authenticated directory sync: what the owning host reports
/// about an instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryInstance {
    pub participant: Participant,
    pub host_id: Option<Uuid>,
    pub workspace_id: Option<Uuid>,
    pub cwd: Option<String>,
    pub status: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentInstance {
    #[serde(flatten)]
    pub participant: Participant,
    pub host_id: Option<Uuid>,
    pub workspace_id: Option<Uuid>,
    /// When this installation last saw the instance in local registration or
    /// an authenticated directory sync. Not a liveness guarantee.
    pub seen_at: Option<DateTime<Utc>>,
    /// The working directory the instance was launched against. `display_name`
    /// is only the workspace basename, so several sessions in different
    /// checkouts are otherwise indistinguishable; this is what tells them
    /// apart. Recorded locally at launch, and for an instance on another host
    /// reported by that host in the directory sync.
    pub cwd: Option<String>,
    /// The owning host's lifecycle state for this instance as last reported by
    /// the directory sync: `running`, `ready`, `starting`, or `stopped`.
    /// Absent for a row no directory sync has covered. A `stopped` instance is
    /// tombstoned the moment it is reported, because nothing on this
    /// installation can reach it and a listing that keeps advertising it buries
    /// the peers that are running.
    pub status: Option<String>,
    /// The owning OS process, recorded locally at launch. Absent for remote
    /// entries, whose pids are meaningless on this machine.
    pub pid: Option<i64>,
    /// When this installation observed the local owner gone. Set by reaping so
    /// a dead row stops being advertised without discarding its history.
    pub exited_at: Option<DateTime<Utc>>,
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
/// A durable reference to an image forwarded with a team message.
///
/// Carries a digest, never a path. The bytes are captured into a
/// content-addressed store when the message is sent, so a recipient resolves
/// the digest inside its own store and never opens a filesystem location the
/// sender chose -- a forwarded image cannot double as a request to read an
/// arbitrary file on the recipient's machine. It is also what makes replay
/// honest: the captured bytes are the artifact, so a message still delivers
/// the real image after the sender has deleted the original.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageAttachment {
    /// Final path component of the sender's file, kept as a human label only.
    pub name: String,
    /// Sniffed from the leading bytes, never from the file extension.
    pub media_type: String,
    pub byte_len: u64,
    /// Lowercase hex SHA-256 of the captured bytes; the store address.
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceMessageBody {
    pub text: String,
    #[serde(default)]
    pub mentions: Vec<StructuredMention>,
    /// Defaulted so journals written before image forwarding replay unchanged
    /// rather than failing to deserialize, and skipped when empty so a plain
    /// text message serializes exactly as it did before this field existed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<MessageAttachment>,
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
/// Provider-neutral input to the single durable workspace message router.
///
/// Callers choose an authorized workspace and audience. The store owns the
/// message ID, sequence, audience validation, and recipient delivery rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewWorkspaceMessage {
    pub workspace_id: Uuid,
    pub author_id: Uuid,
    pub text: String,
    #[serde(default)]
    pub mentions: Vec<StructuredMention>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<MessageAttachment>,
    pub audience: Audience,
    pub mode: DeliveryMode,
    pub thread_id: Option<Uuid>,
    pub reply_to_message_id: Option<Uuid>,
    pub idempotency_key: String,
}
/// Truthful durable acceptance receipt. A successful route always names at
/// least one recipient delivery; immediate wake/steer is an optional layer on
/// top of this receipt, never a second mailbox.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceMessageReceipt {
    pub message_id: Uuid,
    pub workspace_id: Uuid,
    pub sequence: u64,
    pub recipient_ids: Vec<Uuid>,
    pub mode: DeliveryMode,
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
    /// Accepted by the remote relay for a recipient on another host; the
    /// recipient's own host reports admission and acknowledgement.
    Relayed,
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
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedWork {
    pub id: Uuid,
    pub title: String,
    pub detail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assignee_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "WorkStatus::is_pending")]
    pub status: WorkStatus,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub position: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked_reason: Option<String>,
}
fn is_zero(value: &i64) -> bool {
    *value == 0
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum WorkStatus {
    #[default]
    Pending,
    InProgress,
    Blocked,
    AwaitingReview,
    Completed,
}
impl<'de> Deserialize<'de> for WorkStatus {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        match value.trim().to_ascii_lowercase().as_str() {
            "pending" | "todo" | "not_started" | "not-started" => Ok(Self::Pending),
            "in_progress" | "in-progress" | "in progress" | "inprogress" | "active" | "working" => {
                Ok(Self::InProgress)
            }
            "blocked" => Ok(Self::Blocked),
            "awaiting_review" | "awaiting-review" => Ok(Self::AwaitingReview),
            "completed" | "complete" | "done" | "finished" => Ok(Self::Completed),
            _ => Err(serde::de::Error::unknown_variant(
                &value,
                &[
                    "pending",
                    "in_progress",
                    "blocked",
                    "awaiting_review",
                    "completed",
                ],
            )),
        }
    }
}
impl WorkStatus {
    fn is_pending(&self) -> bool {
        *self == Self::Pending
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkItem {
    pub work: SharedWork,
    pub revision: Uuid,
    pub assignment_id: Option<Uuid>,
    pub created_sequence: u64,
    pub updated_sequence: u64,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkSnapshot {
    pub items: Vec<WorkItem>,
    pub revision: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkPlanUpdate {
    pub id: Option<Uuid>,
    pub content: String,
    pub status: WorkStatus,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkPatch {
    pub title: Option<String>,
    #[serde(
        default,
        deserialize_with = "present_optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub detail: Option<Option<String>>,
    pub status: Option<WorkStatus>,
    #[serde(
        default,
        deserialize_with = "present_optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub parent_id: Option<Option<Uuid>>,
    #[serde(
        default,
        deserialize_with = "present_optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub blocked_reason: Option<Option<String>>,
}
fn present_optional<'de, D, T>(deserializer: D) -> std::result::Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
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
    WorkAssigned {
        work_id: Uuid,
        assignee_id: Option<Uuid>,
        expected_assignment_id: Option<Uuid>,
        mode: DeliveryMode,
    },
    WorkUpdated {
        work_id: Uuid,
        expected_revision: Uuid,
        patch: WorkPatch,
        mode: DeliveryMode,
    },
    WorkPlanUpdated {
        assignee_id: Uuid,
        expected_revision: u64,
        items: Vec<WorkPlanUpdate>,
        mode: DeliveryMode,
    },
    WorkPlanMigrated {
        source_session_id: Uuid,
        assignee_id: Uuid,
        items: Vec<WorkPlanUpdate>,
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
    async fn work_items(
        &self,
        workspace_id: Uuid,
        viewer_id: Uuid,
        assignee_id: Option<Uuid>,
    ) -> Result<WorkSnapshot>;
    async fn update_work_plan(
        &self,
        workspace_id: Uuid,
        actor_id: Uuid,
        assignee_id: Uuid,
        idempotency_key: String,
        expected_revision: u64,
        items: Vec<WorkPlanUpdate>,
    ) -> Result<WorkSnapshot>;
    async fn assign_work(
        &self,
        workspace_id: Uuid,
        actor_id: Uuid,
        work_id: Uuid,
        assignee_id: Option<Uuid>,
        expected_assignment_id: Option<Uuid>,
        idempotency_key: String,
    ) -> Result<WorkItem>;
    async fn update_work(
        &self,
        workspace_id: Uuid,
        actor_id: Uuid,
        work_id: Uuid,
        expected_revision: Uuid,
        patch: WorkPatch,
        idempotency_key: String,
    ) -> Result<WorkItem>;
    async fn legacy_plan_migrated(
        &self,
        workspace_id: Uuid,
        viewer_id: Uuid,
        source_session_id: Uuid,
    ) -> Result<bool>;
    async fn ensure_legacy_plan_migrated(
        &self,
        workspace_id: Uuid,
        actor_id: Uuid,
        assignee_id: Uuid,
        source_session_id: Uuid,
        items: Vec<crate::PlanItem>,
    ) -> Result<WorkSnapshot>;
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

    // Reads that callers outside this module depend on. They were inherent on
    // the concrete store, which forced every caller to name that type;
    // declaring them here keeps callers on the trait.
    async fn contains_message(&self, message_id: Uuid) -> Result<bool>;
    async fn contains_idempotent_event(
        &self,
        workspace_id: Uuid,
        author_id: Uuid,
        idempotency_key: &str,
    ) -> Result<bool>;
    /// Highest session sequence already projected into this workspace, so a
    /// restart replays only above the watermark.
    async fn latest_projected_session_sequence(
        &self,
        workspace_id: Uuid,
        session_id: Uuid,
    ) -> Result<u64>;
    async fn participant(&self, participant_id: Uuid) -> Result<Option<Participant>>;
    async fn workspace_name(&self, workspace_id: Uuid) -> Result<Option<String>>;
    async fn workspace_roster(
        &self,
        workspace_id: Uuid,
        viewer_id: Uuid,
    ) -> Result<Vec<WorkspaceRosterEntry>>;
    async fn list_workspaces_for_participant(&self, participant_id: Uuid)
    -> Result<Vec<Workspace>>;

    /// The event a given author already admitted under this idempotency key.
    async fn event_by_idempotency_key(
        &self,
        workspace_id: Uuid,
        author_id: Uuid,
        idempotency_key: &str,
    ) -> Result<Option<WorkspaceEvent>>;

    /// Who a delivered event was fanned out to, in recipient order.
    async fn delivery_recipients(&self, workspace_id: Uuid, sequence: u64) -> Result<Vec<Uuid>>;

    // Instance discovery. Enumeration spans workspaces -- "which instances
    // exist on this machine" is not a question any single workspace's audience
    // can answer -- so it belongs on the store rather than on the message path.
    /// Agent instances known to this installation.
    ///
    /// `include_exited` is a storage-level filter: reaped rows accumulate and
    /// discovery pays a binding lookup per row it materialises, so excluding
    /// them in Rust would pay the full per-row cost before truncating.
    async fn list_instances(&self, include_exited: bool) -> Result<Vec<AgentInstance>>;

    /// Record this process as a live instance, clearing any exit tombstone.
    async fn register_local_instance(
        &self,
        participant_id: Uuid,
        workspace_id: Option<Uuid>,
        cwd: &Path,
        pid: u32,
    ) -> Result<()>;

    /// Create or refresh the workspace one execution session runs in.
    ///
    /// Idempotent by construction: the workspace is inserted on-conflict-nothing
    /// while participants and memberships are upserted, so relaunching a session
    /// refreshes display names and roles without duplicating anything.
    async fn ensure_execution_workspace(
        &self,
        workspace_id: Uuid,
        workspace_name: &str,
        human_participant_id: Uuid,
        human_display_name: &str,
        agent_participant_id: Uuid,
        agent_display_name: &str,
    ) -> Result<()>;

    /// The deterministic two-party workspace for a direct message.
    ///
    /// The id is derived from the sorted participant pair, so both directions
    /// resolve to the same workspace instead of creating two.
    async fn ensure_direct_workspace(
        &self,
        left_participant_id: Uuid,
        right_participant_id: Uuid,
    ) -> Result<Uuid>;

    /// Undelivered messages for one recipient, oldest first.
    async fn pending_message_events(
        &self,
        workspace_id: Uuid,
        recipient_id: Uuid,
        limit: usize,
    ) -> Result<Vec<(WorkspaceEvent, RecipientDelivery)>>;

    /// Canonical messages addressed to a recipient, including already consumed
    /// deliveries, in bounded sequence pages. This read does not admit input.
    async fn message_events_after(
        &self,
        workspace_id: Uuid,
        recipient_id: Uuid,
        after_sequence: u64,
        limit: usize,
    ) -> Result<Vec<WorkspaceEvent>>;

    /// Every recipient delivery row for one message, across all workspaces.
    async fn message_deliveries(&self, message_id: Uuid) -> Result<Vec<RecipientDelivery>>;

    /// Project an authenticated relay delivery into the same inbox as local
    /// messages.
    ///
    /// Thread and reply references stay cloud identities and are NOT validated
    /// locally: the conversation they belong to may never have been
    /// materialised on this machine, and rejecting the message for that would
    /// drop mail that the relay already accepted.
    async fn import_relay_message(
        &self,
        message: WorkspaceMessage,
        author_name: &str,
        recipient_id: Uuid,
        mode: DeliveryMode,
    ) -> Result<WorkspaceEvent>;

    /// The sequence of a message event in this workspace, if it exists.
    async fn message_sequence(&self, workspace_id: Uuid, message_id: Uuid) -> Result<Option<u64>>;

    /// Cache authenticated instance discovery without granting membership.
    async fn upsert_instance(
        &self,
        participant: Participant,
        host_id: Option<Uuid>,
        workspace_id: Option<Uuid>,
    ) -> Result<()>;

    /// Cache one authenticated directory sync, atomically.
    ///
    /// A sync reports every instance the directory knows, stopped ones
    /// included, so it is written as one transaction: a commit per entry made
    /// each sync hundreds of synchronous disk flushes.
    ///
    /// Separate from [`WorkspaceStore::upsert_instance`] because it carries what
    /// the owning host reports about the instance: where it runs, and whether
    /// it runs at all. `upsert_instance` provisions a participant this
    /// installation is arranging to reach and knows none of that.
    ///
    /// A `stopped` instance is tombstoned on sight, so the default listing stops
    /// advertising a peer that cannot answer; any running state clears the
    /// tombstone again. Fields the directory leaves empty never erase what was
    /// learned locally, because a directory entry is a thin mirror -- it even
    /// omits the workspace most of the time.
    async fn upsert_directory_instances(&self, instances: &[DirectoryInstance]) -> Result<()>;

    /// Transition a delivery addressed by message id rather than sequence.
    ///
    /// A DEFAULT METHOD: resolving the message and checking that the recipient
    /// was actually addressed are rules, not storage. Returns `None` rather
    /// than erroring for an unknown message or an unaddressed participant --
    /// a peer outside the audience simply has nothing to transition, and
    /// treating that as fatal would kill a session over someone else's mail.
    async fn transition_message_delivery(
        &self,
        workspace_id: Uuid,
        message_id: Uuid,
        recipient_id: Uuid,
        state: DeliveryState,
        attempt: Option<DeliveryAttempt>,
    ) -> Result<Option<RecipientDelivery>> {
        let Some(sequence) = self.message_sequence(workspace_id, message_id).await? else {
            return Ok(None);
        };
        let addressed = self
            .delivery_recipients(workspace_id, sequence)
            .await?
            .contains(&recipient_id);
        if !addressed {
            return Ok(None);
        }
        Ok(Some(
            self.transition_delivery(workspace_id, sequence, recipient_id, state, attempt)
                .await?,
        ))
    }

    /// Apply one authenticated relay roster projection idempotently.
    ///
    /// A cache of cloud membership, not a local authority grant: the workspace
    /// must already exist locally or there is nothing to project onto.
    async fn upsert_relay_roster_entry(
        &self,
        workspace_id: Uuid,
        participant: Participant,
        role: WorkspaceRole,
    ) -> Result<()>;

    /// Tombstone instances whose owner is gone, returning how many moved.
    ///
    /// Tombstoned rather than deleted: `workspace_events.author_id` and the
    /// delivery rows reference the participant, so removing it would break
    /// history that is still being replayed.
    async fn mark_local_instances_exited(&self, participant_ids: &[Uuid]) -> Result<u64>;

    /// Admit one authored message and return its delivery receipt.
    ///
    /// A DEFAULT METHOD ON PURPOSE. Everything it decides -- that the audience
    /// resolves to someone other than the author, that a repeated idempotency
    /// key carries identical content, that a lost race is resolved semantically
    /// rather than reported as a conflict -- is a rule, not a storage detail.
    /// Written once, both backends obey it; written twice, they eventually
    /// disagree about whether a message was already sent.
    async fn append_message(&self, input: NewWorkspaceMessage) -> Result<WorkspaceMessageReceipt> {
        ensure!(!input.text.trim().is_empty(), "workspace message is empty");
        ensure!(
            !input.idempotency_key.trim().is_empty(),
            "workspace message idempotency key is empty"
        );
        let roster = self
            .workspace_roster(input.workspace_id, input.author_id)
            .await?;
        let members = roster
            .iter()
            .map(|entry| (entry.participant.id, entry.role))
            .collect::<Vec<_>>();
        let mut expected_recipients = resolve_recipients(&input.audience, &members)?;
        expected_recipients.retain(|recipient| *recipient != input.author_id);
        ensure!(
            !expected_recipients.is_empty(),
            "message audience resolves only to its author"
        );

        if let Some(event) = self.admitted_message_event(&input).await? {
            return self.message_receipt(&event, &expected_recipients).await;
        }

        let message_id = Uuid::new_v4();
        let created_at = Utc::now();
        let mode = input.mode;
        let candidate = WorkspaceEvent {
            id: message_id,
            workspace_id: input.workspace_id,
            sequence: 0,
            author_id: input.author_id,
            idempotency_key: input.idempotency_key.clone(),
            created_at,
            kind: WorkspaceEventKind::Message {
                message: WorkspaceMessage {
                    id: message_id,
                    workspace_id: input.workspace_id,
                    thread_id: input.thread_id,
                    reply_to_message_id: input.reply_to_message_id,
                    author_id: input.author_id,
                    body: WorkspaceMessageBody {
                        text: input.text.clone(),
                        mentions: input.mentions.clone(),
                        attachments: input.attachments.clone(),
                    },
                    audience: input.audience.clone(),
                    created_at,
                },
                mode,
            },
        };
        let event = match self.append(candidate).await {
            Ok(event) => event,
            Err(error) => {
                // Two exact retries can race between the optimistic lookup
                // above and the unique idempotency constraint. Resolve the
                // winner semantically before reporting a conflict.
                if let Some(event) = self.admitted_message_event(&input).await? {
                    return self.message_receipt(&event, &expected_recipients).await;
                }
                return Err(error);
            }
        };
        self.message_receipt(&event, &expected_recipients).await
    }

    /// The already-admitted event for this key, proven to carry identical
    /// content. A matching key with different content is a caller bug.
    async fn admitted_message_event(
        &self,
        input: &NewWorkspaceMessage,
    ) -> Result<Option<WorkspaceEvent>> {
        let Some(event) = self
            .event_by_idempotency_key(input.workspace_id, input.author_id, &input.idempotency_key)
            .await?
        else {
            return Ok(None);
        };
        let matches = matches!(
            &event.kind,
            WorkspaceEventKind::Message { message, mode }
                if event.workspace_id == input.workspace_id
                    && event.author_id == input.author_id
                    && message.workspace_id == input.workspace_id
                    && message.author_id == input.author_id
                    && message.thread_id == input.thread_id
                    && message.reply_to_message_id == input.reply_to_message_id
                    && message.body.text == input.text
                    && message.body.mentions == input.mentions
                    && message.body.attachments == input.attachments
                    && message.audience == input.audience
                    && *mode == input.mode
        );
        ensure!(
            matches,
            "idempotency conflict: key was used with a different payload"
        );
        Ok(Some(event))
    }

    /// The receipt for an admitted message, checked against the audience the
    /// caller was authorised for.
    async fn message_receipt(
        &self,
        event: &WorkspaceEvent,
        expected_recipients: &[Uuid],
    ) -> Result<WorkspaceMessageReceipt> {
        let WorkspaceEventKind::Message { message, mode } = &event.kind else {
            bail!("workspace message receipt references a non-message event");
        };
        let recipient_ids = self
            .delivery_recipients(event.workspace_id, event.sequence)
            .await?;
        // Delivery rows are the ground truth for who was reached; if they
        // disagree with the authorised audience the receipt is not safe to
        // hand back.
        ensure!(
            recipient_ids == expected_recipients,
            "workspace delivery receipt does not match the authorized audience"
        );
        Ok(WorkspaceMessageReceipt {
            message_id: message.id,
            workspace_id: event.workspace_id,
            sequence: event.sequence,
            recipient_ids,
            mode: *mode,
        })
    }
}

/// The canonical form an idempotency key is checked against.
///
/// Identity and timestamps are zeroed so a retry of the same logical event
/// canonicalises identically. Shared by every backend on purpose: two stores
/// that disagreed here would accept the same message twice.
pub(crate) fn canonical_event(mut event: WorkspaceEvent) -> Result<String> {
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

/// Resolve an audience to the participants who must receive the event.
///
/// Also shared: a backend that resolved an audience differently would deliver
/// one workspace's message to a different set of people.
pub(crate) fn resolve_recipients(
    audience: &Audience,
    members: &[(Uuid, WorkspaceRole)],
) -> Result<Vec<Uuid>> {
    let mut ids = match audience {
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

impl WorkSnapshot {
    pub(crate) fn apply(&mut self, event: &WorkspaceEvent) -> Result<()> {
        let mut touched = Vec::new();
        match &event.kind {
            WorkspaceEventKind::WorkCreated { work, .. } => {
                self.items.push(WorkItem {
                    work: work.clone(),
                    revision: event.id,
                    assignment_id: work.assignee_id.map(|_| event.id),
                    created_sequence: event.sequence,
                    updated_sequence: event.sequence,
                });
            }
            WorkspaceEventKind::WorkClaimed { claim, .. } => {
                let item = self.item_mut(claim.work_id)?;
                item.work.assignee_id = Some(claim.claimant_id);
                item.assignment_id = Some(event.id);
                touched.push(claim.work_id);
            }
            WorkspaceEventKind::WorkAssigned {
                work_id,
                assignee_id,
                ..
            } => {
                let item = self.item_mut(*work_id)?;
                item.work.assignee_id = *assignee_id;
                if assignee_id.is_none() && item.work.status == WorkStatus::InProgress {
                    item.work.status = WorkStatus::Pending;
                }
                item.assignment_id = Some(event.id);
                touched.push(*work_id);
            }
            WorkspaceEventKind::WorkUpdated { work_id, patch, .. } => {
                let item = self.item_mut(*work_id)?;
                if let Some(title) = &patch.title {
                    item.work.title = title.clone();
                }
                if let Some(detail) = &patch.detail {
                    item.work.detail = detail.clone();
                }
                if let Some(status) = patch.status {
                    item.work.status = status;
                }
                if let Some(parent) = patch.parent_id {
                    item.work.parent_id = parent;
                }
                if let Some(reason) = &patch.blocked_reason {
                    item.work.blocked_reason = reason.clone();
                }
                touched.push(*work_id);
            }
            WorkspaceEventKind::WorkPlanUpdated {
                assignee_id, items, ..
            }
            | WorkspaceEventKind::WorkPlanMigrated {
                assignee_id, items, ..
            } => {
                let migration = matches!(event.kind, WorkspaceEventKind::WorkPlanMigrated { .. });
                let mut kept = Vec::new();
                for (position, update) in items.iter().enumerate() {
                    let mut id = update.id.unwrap_or_else(|| {
                        Uuid::new_v5(&event.id, position.to_string().as_bytes())
                    });
                    if migration
                        && let Some(existing) = self.items.iter().find(|item| item.work.id == id)
                    {
                        // A legacy plan may already reference the same canonical task.
                        // Do not replace its newer status, ownership token, or links.
                        if existing.work.assignee_id == Some(*assignee_id) {
                            continue;
                        }
                        id = Uuid::new_v5(&event.id, id.as_bytes());
                        ensure!(
                            !self.items.iter().any(|item| item.work.id == id),
                            "legacy mapped work id conflicts"
                        );
                    }
                    kept.push(id);
                    if let Some(item) = self.items.iter_mut().find(|item| item.work.id == id) {
                        item.work.title = update.content.clone();
                        item.work.status = update.status;
                        item.work.position = position as i64;
                        touched.push(id);
                    } else {
                        self.items.push(WorkItem {
                            work: SharedWork {
                                id,
                                title: update.content.clone(),
                                status: update.status,
                                assignee_id: Some(*assignee_id),
                                position: position as i64,
                                ..SharedWork::default()
                            },
                            revision: event.id,
                            assignment_id: Some(event.id),
                            created_sequence: event.sequence,
                            updated_sequence: event.sequence,
                        });
                    }
                }
                if !migration {
                    for item in &mut self.items {
                        if item.work.assignee_id == Some(*assignee_id)
                            && !kept.contains(&item.work.id)
                        {
                            item.work.assignee_id = None;
                            if item.work.status == WorkStatus::InProgress {
                                item.work.status = WorkStatus::Pending;
                            }
                            item.assignment_id = Some(event.id);
                            touched.push(item.work.id);
                        }
                    }
                }
            }
            WorkspaceEventKind::DependencyDeclared { dependency, .. } => {
                touched.push(dependency.work_id)
            }
            WorkspaceEventKind::ReviewRequested { request, .. } => touched.push(request.work_id),
            WorkspaceEventKind::ReviewRecorded { review, .. } => touched.push(review.work_id),
            _ => return Ok(()),
        }
        for id in touched {
            let item = self.item_mut(id)?;
            item.revision = event.id;
            item.updated_sequence = event.sequence;
        }
        self.revision = event.sequence;
        self.items
            .sort_by_key(|item| (item.work.position, item.created_sequence, item.work.id));
        Ok(())
    }

    fn item_mut(&mut self, id: Uuid) -> Result<&mut WorkItem> {
        self.items
            .iter_mut()
            .find(|item| item.work.id == id)
            .ok_or_else(|| anyhow::anyhow!("work item is not in workspace"))
    }
}
