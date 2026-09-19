//! `WorkspaceStore` on PostgreSQL.
//!
//! The workspace tier is the second-largest writer in the journal file (1.6M
//! events on this machine), so leaving it on SQLite would keep every Borg
//! process queueing on that file's single write lock even after the session
//! journal became contention-free. Here the sequence allocator takes a row lock
//! on the workspace it is appending to, so two workspaces never contend.
//!
//! Canonicalisation and audience resolution are NOT reimplemented here: they
//! are shared with the SQLite store, because two backends that disagreed on
//! either would accept a message twice or deliver it to the wrong people.

use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, Utc};
use sqlx::postgres::PgPool;
use sqlx::{Postgres, Row, Transaction};
use uuid::Uuid;

use std::path::Path;

use crate::workspace::{
    AgentInstance, Audience, DeliveryAttempt, DeliveryMode, DeliveryState, Participant,
    ParticipantKind, PresenceLease, RecipientDelivery, Thread, Workspace, WorkspaceEvent,
    WorkspaceEventKind, WorkspaceMembership, WorkspaceMessage, WorkspaceRole, WorkspaceRosterEntry,
    WorkspaceStore, canonical_event, resolve_recipients,
};

/// The durable workspace projection, backed by PostgreSQL.
#[derive(Debug, Clone)]
pub struct PostgresWorkspaceStore {
    pool: PgPool,
}

impl PostgresWorkspaceStore {
    /// Adopt a pool that already has the satellite schema applied.
    ///
    /// The schema is owned by `PostgresSessionStore::ensure_schema`, so this
    /// never creates tables: one bootstrap path keeps concurrent cold starts
    /// serialised behind a single advisory lock.
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn members(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        workspace_id: Uuid,
    ) -> Result<Vec<(Uuid, WorkspaceRole)>> {
        let rows = sqlx::query(
            "select participant_id, role from workspace_members where workspace_id = $1",
        )
        .bind(workspace_id.to_string())
        .fetch_all(&mut **transaction)
        .await?;
        rows.iter()
            .map(|row| {
                Ok((
                    Uuid::parse_str(row.try_get("participant_id")?)?,
                    serde_json::from_str(row.try_get("role")?)?,
                ))
            })
            .collect()
    }

    async fn work_exists(
        transaction: &mut Transaction<'_, Postgres>,
        workspace_id: Uuid,
        work_id: Uuid,
    ) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "select exists(select 1 from workspace_work_items \
             where workspace_id = $1 and work_id = $2)",
        )
        .bind(workspace_id.to_string())
        .bind(work_id.to_string())
        .fetch_one(&mut **transaction)
        .await?)
    }

    async fn require_member(&self, workspace_id: Uuid, participant_id: Uuid) -> Result<()> {
        let found: bool = sqlx::query_scalar(
            "select exists(select 1 from workspace_members \
             where workspace_id = $1 and participant_id = $2)",
        )
        .bind(workspace_id.to_string())
        .bind(participant_id.to_string())
        .fetch_one(&self.pool)
        .await?;
        ensure!(found, "viewer is not a workspace member");
        Ok(())
    }

    /// Claim the next sequence for one workspace.
    ///
    /// `update ... returning` takes a row lock on that workspace only, which is
    /// the workspace-tier equivalent of the journal's per-session allocator.
    async fn next_sequence(
        transaction: &mut Transaction<'_, Postgres>,
        workspace_id: Uuid,
    ) -> Result<i64> {
        sqlx::query_scalar(
            "update workspaces set next_sequence = next_sequence + 1 \
             where id = $1 returning next_sequence - 1",
        )
        .bind(workspace_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .with_context(|| format!("workspace {workspace_id} does not exist"))
    }

    /// Look up an already-admitted event for this idempotency key.
    async fn existing_event(
        transaction: &mut Transaction<'_, Postgres>,
        event: &WorkspaceEvent,
        canonical: &str,
    ) -> Result<Option<WorkspaceEvent>> {
        let row = sqlx::query(
            "select canonical_json, event_json from workspace_events \
             where workspace_id = $1 and author_id = $2 and idempotency_key = $3",
        )
        .bind(event.workspace_id.to_string())
        .bind(event.author_id.to_string())
        .bind(&event.idempotency_key)
        .fetch_optional(&mut **transaction)
        .await?;
        let Some(row) = row else { return Ok(None) };
        // Same key, different payload is a bug in the caller, not a retry.
        ensure!(
            row.try_get::<String, _>("canonical_json")? == canonical,
            "idempotency conflict: key was used with a different payload"
        );
        Ok(Some(serde_json::from_str(row.try_get("event_json")?)?))
    }

    async fn insert_event(
        transaction: &mut Transaction<'_, Postgres>,
        event: &WorkspaceEvent,
        canonical: &str,
    ) -> Result<()> {
        // Columns are named rather than positional: `is_message` is generated
        // and cannot be written.
        sqlx::query(
            "insert into workspace_events \
             (workspace_id, sequence, id, author_id, idempotency_key, canonical_json, \
              event_json, created_at) values ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(event.workspace_id.to_string())
        .bind(i64::try_from(event.sequence)?)
        .bind(event.id.to_string())
        .bind(event.author_id.to_string())
        .bind(&event.idempotency_key)
        .bind(canonical)
        .bind(serde_json::to_string(event)?)
        .bind(event.created_at.to_rfc3339())
        .execute(&mut **transaction)
        .await?;
        Ok(())
    }

    async fn fan_out(
        transaction: &mut Transaction<'_, Postgres>,
        workspace_id: Uuid,
        sequence: i64,
        recipients: &[Uuid],
        mode: &DeliveryMode,
        is_message: bool,
    ) -> Result<()> {
        let mode_json = serde_json::to_string(mode)?;
        let pending = serde_json::to_string(&DeliveryState::Pending)?;
        for recipient in recipients {
            sqlx::query(
                "insert into workspace_deliveries \
                 (workspace_id, sequence, recipient_id, mode, state, is_message) \
                 values ($1, $2, $3, $4, $5, $6)",
            )
            .bind(workspace_id.to_string())
            .bind(sequence)
            .bind(recipient.to_string())
            .bind(&mode_json)
            .bind(&pending)
            .bind(is_message)
            .execute(&mut **transaction)
            .await?;
        }
        Ok(())
    }

    /// Append one event, optionally skipping local reference validation.
    ///
    /// `validate_references` is false only for relay imports: a cloud thread or
    /// reply id need not exist on this machine, and refusing the message for
    /// that would drop mail the relay already accepted.
    pub(crate) async fn append_event(
        &self,
        mut event: WorkspaceEvent,
        validate_references: bool,
    ) -> Result<WorkspaceEvent> {
        ensure!(
            !event.idempotency_key.is_empty(),
            "idempotency key must not be empty"
        );
        let canonical = canonical_event(event.clone())?;
        let mut transaction = self.pool.begin().await?;
        if let Some(existing) = Self::existing_event(&mut transaction, &event, &canonical).await? {
            transaction.commit().await?;
            return Ok(existing);
        }
        let members = self.members(&mut transaction, event.workspace_id).await?;
        ensure!(
            members.iter().any(|(id, _)| *id == event.author_id),
            "author is not a workspace member"
        );
        Self::validate(&mut transaction, &event, &members, validate_references).await?;

        // The mode is what a delivery records; the audience only decides who
        // receives it. Only a message carries its own audience.
        let (mode, audience) = match &event.kind {
            WorkspaceEventKind::Message { message, mode } => (*mode, message.audience.clone()),
            WorkspaceEventKind::SessionEvent { mode, .. }
            | WorkspaceEventKind::WorkCreated { mode, .. }
            | WorkspaceEventKind::ArtifactPublished { mode, .. }
            | WorkspaceEventKind::DecisionRecorded { mode, .. }
            | WorkspaceEventKind::WorkClaimed { mode, .. }
            | WorkspaceEventKind::DependencyDeclared { mode, .. }
            | WorkspaceEventKind::ReviewRequested { mode, .. }
            | WorkspaceEventKind::ReviewRecorded { mode, .. }
            | WorkspaceEventKind::ReferenceAdded { mode, .. }
            | WorkspaceEventKind::ProvenanceRecorded { mode, .. } => (*mode, Audience::Workspace),
        };
        let recipients = resolve_recipients(&audience, &members)?;

        let sequence = Self::next_sequence(&mut transaction, event.workspace_id).await?;
        event.sequence = u64::try_from(sequence)?;
        Self::insert_event(&mut transaction, &event, &canonical).await?;
        Self::apply_side_effects(&mut transaction, &event, sequence).await?;
        let is_message = matches!(&event.kind, WorkspaceEventKind::Message { .. });
        // The author is a member but not a recipient of their own event.
        let recipients: Vec<Uuid> = recipients
            .into_iter()
            .filter(|id| *id != event.author_id)
            .collect();
        Self::fan_out(
            &mut transaction,
            event.workspace_id,
            sequence,
            &recipients,
            &mode,
            is_message,
        )
        .await?;
        transaction.commit().await?;
        Ok(event)
    }

    /// Validate an event against the workspace's current state.
    ///
    /// Mirrors the SQLite store's checks. Anywhere this drifts, the shared
    /// workspace conformance tests fail on one backend and not the other.
    async fn validate(
        transaction: &mut Transaction<'_, Postgres>,
        event: &WorkspaceEvent,
        members: &[(Uuid, WorkspaceRole)],
        validate_references: bool,
    ) -> Result<()> {
        let workspace_id = event.workspace_id;
        match &event.kind {
            WorkspaceEventKind::WorkCreated { work, .. } => {
                ensure!(
                    !work.title.trim().is_empty(),
                    "work title must not be empty"
                );
                ensure!(
                    !Self::work_exists(transaction, workspace_id, work.id).await?,
                    "work already exists in workspace"
                );
            }
            WorkspaceEventKind::ArtifactPublished { artifact, .. } => {
                ensure!(
                    !artifact.name.trim().is_empty() && !artifact.uri.trim().is_empty(),
                    "artifact name and URI must not be empty"
                );
                if let Some(work_id) = artifact.work_id {
                    ensure!(
                        Self::work_exists(transaction, workspace_id, work_id).await?,
                        "artifact work item is not in workspace"
                    );
                }
            }
            WorkspaceEventKind::Message { message, .. } => {
                ensure!(
                    message.workspace_id == workspace_id && message.author_id == event.author_id,
                    "message workspace/author mismatch"
                );
                // A thread or reply target from another workspace would let a
                // message graft itself onto a conversation it is not part of.
                if let Some(thread) = message.thread_id.filter(|_| validate_references) {
                    let found: bool = sqlx::query_scalar(
                        "select exists(select 1 from workspace_threads \
                         where id = $1 and workspace_id = $2)",
                    )
                    .bind(thread.to_string())
                    .bind(workspace_id.to_string())
                    .fetch_one(&mut **transaction)
                    .await?;
                    ensure!(found, "thread is not in workspace");
                }
                if let Some(reply_to) = message.reply_to_message_id.filter(|_| validate_references)
                {
                    let found: bool = sqlx::query_scalar(
                        "select exists(select 1 from workspace_events \
                         where workspace_id = $1 \
                           and (event_json::jsonb #>> '{kind,message,id}') = $2)",
                    )
                    .bind(workspace_id.to_string())
                    .bind(reply_to.to_string())
                    .fetch_one(&mut **transaction)
                    .await?;
                    ensure!(found, "reply target is not in workspace");
                }
            }
            WorkspaceEventKind::DecisionRecorded { decision, .. } => {
                ensure!(
                    !decision.subject.trim().is_empty() && !decision.outcome.trim().is_empty(),
                    "decision subject and outcome must not be empty"
                );
            }
            WorkspaceEventKind::WorkClaimed { claim, .. } => {
                ensure!(
                    Self::work_exists(transaction, workspace_id, claim.work_id).await?,
                    "claimed work item is not in workspace"
                );
                ensure!(
                    members.iter().any(|(id, _)| *id == claim.claimant_id),
                    "claimant is not a workspace member"
                );
                // Compare-and-set: the caller states which claim it believes is
                // current, so two claimants cannot both win.
                let current: Option<String> = sqlx::query_scalar(
                    "select claim_id from workspace_work_claims \
                     where workspace_id = $1 and work_id = $2",
                )
                .bind(workspace_id.to_string())
                .bind(claim.work_id.to_string())
                .fetch_optional(&mut **transaction)
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
            WorkspaceEventKind::DependencyDeclared { dependency, .. } => {
                ensure!(
                    dependency.work_id != dependency.depends_on_work_id,
                    "work cannot depend on itself"
                );
                ensure!(
                    Self::work_exists(transaction, workspace_id, dependency.work_id).await?
                        && Self::work_exists(
                            transaction,
                            workspace_id,
                            dependency.depends_on_work_id
                        )
                        .await?,
                    "dependency work item is not in workspace"
                );
            }
            WorkspaceEventKind::ReviewRequested { request, .. } => {
                ensure!(
                    Self::work_exists(transaction, workspace_id, request.work_id).await?,
                    "reviewed work item is not in workspace"
                );
                if let Some(reviewer_id) = request.requested_reviewer_id {
                    ensure!(
                        members.iter().any(|(id, _)| *id == reviewer_id),
                        "requested reviewer is not a workspace member"
                    );
                }
            }
            WorkspaceEventKind::ReviewRecorded { review, .. } => {
                ensure!(
                    Self::work_exists(transaction, workspace_id, review.work_id).await?,
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
            _ => {}
        }
        Ok(())
    }

    /// Record the side tables an event implies: work items, claims, edges.
    async fn apply_side_effects(
        transaction: &mut Transaction<'_, Postgres>,
        event: &WorkspaceEvent,
        sequence: i64,
    ) -> Result<()> {
        let workspace_id = event.workspace_id.to_string();
        match &event.kind {
            WorkspaceEventKind::WorkCreated { work, .. } => {
                sqlx::query(
                    "insert into workspace_work_items \
                     (workspace_id, work_id, created_sequence) values ($1, $2, $3)",
                )
                .bind(&workspace_id)
                .bind(work.id.to_string())
                .bind(sequence)
                .execute(&mut **transaction)
                .await?;
            }
            WorkspaceEventKind::WorkClaimed { claim, .. } => {
                sqlx::query(
                    "insert into workspace_work_claims \
                     (workspace_id, work_id, claim_id, claimant_id, sequence) \
                     values ($1, $2, $3, $4, $5) \
                     on conflict (workspace_id, work_id) do update set \
                     claim_id = excluded.claim_id, claimant_id = excluded.claimant_id, \
                     sequence = excluded.sequence",
                )
                .bind(&workspace_id)
                .bind(claim.work_id.to_string())
                .bind(event.id.to_string())
                .bind(claim.claimant_id.to_string())
                .bind(sequence)
                .execute(&mut **transaction)
                .await?;
            }
            WorkspaceEventKind::DependencyDeclared { dependency, .. } => {
                sqlx::query(
                    "insert into workspace_work_dependencies \
                     (workspace_id, work_id, depends_on_work_id, sequence) \
                     values ($1, $2, $3, $4) \
                     on conflict (workspace_id, work_id, depends_on_work_id) do nothing",
                )
                .bind(&workspace_id)
                .bind(dependency.work_id.to_string())
                .bind(dependency.depends_on_work_id.to_string())
                .bind(sequence)
                .execute(&mut **transaction)
                .await?;
            }
            _ => {}
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl WorkspaceStore for PostgresWorkspaceStore {
    async fn create_participant(&self, participant: Participant) -> Result<()> {
        sqlx::query(
            "insert into workspace_participants (id, display_name, kind, created_at) \
             values ($1, $2, $3, $4)",
        )
        .bind(participant.id.to_string())
        .bind(participant.display_name)
        .bind(serde_json::to_string(&participant.kind)?)
        .bind(participant.created_at.to_rfc3339())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn create_workspace(&self, workspace: Workspace) -> Result<()> {
        sqlx::query("insert into workspaces (id, name, created_at) values ($1, $2, $3)")
            .bind(workspace.id.to_string())
            .bind(workspace.name)
            .bind(workspace.created_at.to_rfc3339())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn add_member(&self, membership: WorkspaceMembership) -> Result<()> {
        sqlx::query(
            "insert into workspace_members (workspace_id, participant_id, role, joined_at) \
             values ($1, $2, $3, $4)",
        )
        .bind(membership.workspace_id.to_string())
        .bind(membership.participant_id.to_string())
        .bind(serde_json::to_string(&membership.role)?)
        .bind(membership.joined_at.to_rfc3339())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn create_thread(&self, thread: Thread) -> Result<()> {
        sqlx::query(
            "insert into workspace_threads (id, workspace_id, title, created_at) \
             values ($1, $2, $3, $4)",
        )
        .bind(thread.id.to_string())
        .bind(thread.workspace_id.to_string())
        .bind(thread.title)
        .bind(thread.created_at.to_rfc3339())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn append(&self, event: WorkspaceEvent) -> Result<WorkspaceEvent> {
        self.append_event(event, true).await
    }

    async fn append_session_event_batch(&self, events: &[WorkspaceEvent]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let workspace_id = events[0].workspace_id;
        ensure!(
            events.iter().all(|event| {
                event.workspace_id == workspace_id
                    && matches!(&event.kind, WorkspaceEventKind::SessionEvent { .. })
            }),
            "session projection batch contains an incompatible workspace event"
        );
        // One transaction for the whole batch: these rows are a projection of
        // events the session journal already accepted, so a partial batch would
        // leave the projection describing a history that never happened.
        let mut transaction = self.pool.begin().await?;
        let members = self.members(&mut transaction, workspace_id).await?;
        let member_ids: Vec<Uuid> = members.into_iter().map(|(id, _)| id).collect();

        for source in events {
            let WorkspaceEventKind::SessionEvent { mode, .. } = &source.kind else {
                bail!("session projection batch was validated above");
            };
            ensure!(
                member_ids.contains(&source.author_id),
                "author is not a workspace member"
            );
            let canonical = canonical_event(source.clone())?;
            if Self::existing_event(&mut transaction, source, &canonical)
                .await?
                .is_some()
            {
                continue;
            }
            let sequence = Self::next_sequence(&mut transaction, workspace_id).await?;
            let mut event = source.clone();
            event.sequence = u64::try_from(sequence)?;
            Self::insert_event(&mut transaction, &event, &canonical).await?;
            let recipients: Vec<Uuid> = member_ids
                .iter()
                .copied()
                .filter(|id| *id != event.author_id)
                .collect();
            Self::fan_out(
                &mut transaction,
                workspace_id,
                sequence,
                &recipients,
                mode,
                false,
            )
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    async fn replay(
        &self,
        workspace_id: Uuid,
        viewer_id: Uuid,
        after_sequence: u64,
        limit: usize,
    ) -> Result<Vec<WorkspaceEvent>> {
        self.require_member(workspace_id, viewer_id).await?;
        // A viewer sees what they authored plus what was delivered to them --
        // never another participant's private audience.
        let rows = sqlx::query(
            "select e.event_json from workspace_events e \
             where e.workspace_id = $1 and e.sequence > $2 \
               and (e.author_id = $3 or exists( \
                    select 1 from workspace_deliveries d \
                    where d.workspace_id = e.workspace_id and d.sequence = e.sequence \
                      and d.recipient_id = $3)) \
             order by e.sequence limit $4",
        )
        .bind(workspace_id.to_string())
        .bind(i64::try_from(after_sequence)?)
        .bind(viewer_id.to_string())
        .bind(i64::try_from(limit)?)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| Ok(serde_json::from_str(row.try_get("event_json")?)?))
            .collect()
    }

    async fn deliveries_after(
        &self,
        workspace_id: Uuid,
        recipient_id: Uuid,
        after_sequence: u64,
        limit: usize,
    ) -> Result<Vec<RecipientDelivery>> {
        let rows = sqlx::query(
            "select sequence, recipient_id, mode, state, attempts, last_attempt_json \
             from workspace_deliveries \
             where workspace_id = $1 and recipient_id = $2 and sequence > $3 \
             order by sequence limit $4",
        )
        .bind(workspace_id.to_string())
        .bind(recipient_id.to_string())
        .bind(i64::try_from(after_sequence)?)
        .bind(i64::try_from(limit)?)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| {
                Ok(RecipientDelivery {
                    workspace_id,
                    sequence: u64::try_from(row.try_get::<i64, _>("sequence")?)?,
                    recipient_id: Uuid::parse_str(row.try_get("recipient_id")?)?,
                    mode: serde_json::from_str(row.try_get("mode")?)?,
                    state: serde_json::from_str(row.try_get("state")?)?,
                    attempts: u32::try_from(row.try_get::<i64, _>("attempts")?)?,
                    last_attempt: row
                        .try_get::<Option<String>, _>("last_attempt_json")?
                        .map(|value| serde_json::from_str(&value))
                        .transpose()?,
                })
            })
            .collect()
    }

    async fn transition_delivery(
        &self,
        workspace_id: Uuid,
        sequence: u64,
        recipient_id: Uuid,
        next: DeliveryState,
        attempt: Option<DeliveryAttempt>,
    ) -> Result<RecipientDelivery> {
        let mut transaction = self.pool.begin().await?;
        // `for update` is required here, not optional: two relays transitioning
        // the same delivery would otherwise both read `pending` and one update
        // would be lost.
        let row = sqlx::query(
            "select mode, state, attempts, last_attempt_json from workspace_deliveries \
             where workspace_id = $1 and sequence = $2 and recipient_id = $3 for update",
        )
        .bind(workspace_id.to_string())
        .bind(i64::try_from(sequence)?)
        .bind(recipient_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .context("recipient has no delivery")?;

        let current: DeliveryState = serde_json::from_str(row.try_get("state")?)?;
        let attempts: i64 = row.try_get("attempts")?;
        if current == next {
            transaction.commit().await?;
            return Ok(RecipientDelivery {
                workspace_id,
                sequence,
                recipient_id,
                mode: serde_json::from_str(row.try_get("mode")?)?,
                state: current,
                attempts: u32::try_from(attempts)?,
                last_attempt: row
                    .try_get::<Option<String>, _>("last_attempt_json")?
                    .map(|value| serde_json::from_str(&value))
                    .transpose()?,
            });
        }
        let allowed = matches!(
            (current, next),
            (
                DeliveryState::Pending,
                DeliveryState::Relayed
                    | DeliveryState::Admitted
                    | DeliveryState::Failed
                    | DeliveryState::Recalled
            ) | (
                DeliveryState::Relayed,
                DeliveryState::Relayed | DeliveryState::Acknowledged | DeliveryState::Failed
            ) | (
                DeliveryState::Admitted,
                DeliveryState::Acknowledged | DeliveryState::Failed
            ) | (DeliveryState::Failed, DeliveryState::Pending)
        );
        ensure!(allowed, "invalid non-monotonic delivery transition");
        let attempts = attempts + i64::from(attempt.is_some());
        sqlx::query(
            "update workspace_deliveries set state = $1, attempts = $2, \
             last_attempt_json = coalesce($3, last_attempt_json) \
             where workspace_id = $4 and sequence = $5 and recipient_id = $6",
        )
        .bind(serde_json::to_string(&next)?)
        .bind(attempts)
        .bind(attempt.as_ref().map(serde_json::to_string).transpose()?)
        .bind(workspace_id.to_string())
        .bind(i64::try_from(sequence)?)
        .bind(recipient_id.to_string())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(RecipientDelivery {
            workspace_id,
            sequence,
            recipient_id,
            mode: serde_json::from_str(row.try_get("mode")?)?,
            state: next,
            attempts: u32::try_from(attempts)?,
            last_attempt: attempt.or(row
                .try_get::<Option<String>, _>("last_attempt_json")?
                .map(|value| serde_json::from_str(&value))
                .transpose()?),
        })
    }

    async fn acquire_presence_lease(&self, lease: PresenceLease) -> Result<()> {
        ensure!(
            lease.expires_at > Utc::now(),
            "presence lease must expire in the future"
        );
        let mut transaction = self.pool.begin().await?;
        let member: bool = sqlx::query_scalar(
            "select exists(select 1 from workspace_members \
             where workspace_id = $1 and participant_id = $2)",
        )
        .bind(lease.workspace_id.to_string())
        .bind(lease.participant_id.to_string())
        .fetch_one(&mut *transaction)
        .await?;
        ensure!(member, "presence participant is not a workspace member");
        sqlx::query(
            "insert into workspace_presence_leases \
             (workspace_id, participant_id, client_id, host_id, expires_at) \
             values ($1, $2, $3, $4, $5) \
             on conflict (workspace_id, participant_id, client_id) do update set \
             host_id = excluded.host_id, expires_at = excluded.expires_at",
        )
        .bind(lease.workspace_id.to_string())
        .bind(lease.participant_id.to_string())
        .bind(lease.client_id.to_string())
        .bind(lease.host_id.map(|id| id.to_string()))
        .bind(lease.expires_at.to_rfc3339())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn event_by_idempotency_key(
        &self,
        workspace_id: Uuid,
        author_id: Uuid,
        idempotency_key: &str,
    ) -> Result<Option<WorkspaceEvent>> {
        let row = sqlx::query(
            "select event_json from workspace_events \
             where workspace_id = $1 and author_id = $2 and idempotency_key = $3",
        )
        .bind(workspace_id.to_string())
        .bind(author_id.to_string())
        .bind(idempotency_key)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref()
            .map(|row| Ok(serde_json::from_str(row.try_get("event_json")?)?))
            .transpose()
    }

    async fn delivery_recipients(&self, workspace_id: Uuid, sequence: u64) -> Result<Vec<Uuid>> {
        // Ordered by recipient so a receipt can be compared to the audience
        // the caller was authorised for.
        let rows = sqlx::query(
            "select recipient_id from workspace_deliveries \
             where workspace_id = $1 and sequence = $2 order by recipient_id",
        )
        .bind(workspace_id.to_string())
        .bind(i64::try_from(sequence)?)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| Ok(Uuid::parse_str(row.try_get("recipient_id")?)?))
            .collect()
    }

    async fn ensure_execution_workspace(
        &self,
        workspace_id: Uuid,
        workspace_name: &str,
        human_participant_id: Uuid,
        human_display_name: &str,
        agent_participant_id: Uuid,
        agent_display_name: &str,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let mut transaction = self.pool.begin().await?;
        // The workspace itself is never overwritten; participants and roles are
        // refreshed, so relaunching a session updates display names without
        // duplicating membership.
        sqlx::query(
            "insert into workspaces (id, name, created_at) values ($1, $2, $3) \
             on conflict (id) do nothing",
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
                "insert into workspace_participants (id, display_name, kind, created_at) \
                 values ($1, $2, $3, $4) \
                 on conflict (id) do update set display_name = excluded.display_name",
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
                "insert into workspace_members (workspace_id, participant_id, role, joined_at) \
                 values ($1, $2, $3, $4) \
                 on conflict (workspace_id, participant_id) do update set role = excluded.role",
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

    async fn ensure_direct_workspace(
        &self,
        left_participant_id: Uuid,
        right_participant_id: Uuid,
    ) -> Result<Uuid> {
        ensure!(
            left_participant_id != right_participant_id,
            "direct message recipient must differ from its author"
        );
        // Sorted before hashing so both directions name the same workspace.
        let mut participant_ids = [left_participant_id, right_participant_id];
        participant_ids.sort_unstable();
        let workspace_id = Uuid::new_v5(
            &Uuid::NAMESPACE_URL,
            format!(
                "borg://local-direct/{}/{}",
                participant_ids[0], participant_ids[1]
            )
            .as_bytes(),
        );
        let now = Utc::now().to_rfc3339();
        let mut transaction = self.pool.begin().await?;
        for participant_id in participant_ids {
            let exists: bool = sqlx::query_scalar(
                "select exists(select 1 from workspace_participants where id = $1)",
            )
            .bind(participant_id.to_string())
            .fetch_one(&mut *transaction)
            .await?;
            ensure!(
                exists,
                "direct message participant {participant_id} is unknown"
            );
        }
        sqlx::query(
            "insert into workspaces (id, name, created_at) values ($1, $2, $3) \
             on conflict (id) do nothing",
        )
        .bind(workspace_id.to_string())
        .bind("Borg direct message")
        .bind(&now)
        .execute(&mut *transaction)
        .await?;
        for participant_id in participant_ids {
            sqlx::query(
                "insert into workspace_members (workspace_id, participant_id, role, joined_at) \
                 values ($1, $2, $3, $4) \
                 on conflict (workspace_id, participant_id) do nothing",
            )
            .bind(workspace_id.to_string())
            .bind(participant_id.to_string())
            .bind(serde_json::to_string(&WorkspaceRole::Editor)?)
            .bind(&now)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(workspace_id)
    }

    async fn pending_message_events(
        &self,
        workspace_id: Uuid,
        recipient_id: Uuid,
        limit: usize,
    ) -> Result<Vec<(WorkspaceEvent, RecipientDelivery)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        // Session projections also create deliveries and stay pending forever,
        // so the message bit is what lets this poll seek to the pending-message
        // intersection instead of scanning every delivery for the recipient.
        let rows = sqlx::query(
            "select d.sequence, d.recipient_id, d.mode, d.state, d.attempts, \
                    d.last_attempt_json, e.event_json \
             from workspace_deliveries d \
             join workspace_events e \
               on e.workspace_id = d.workspace_id and e.sequence = d.sequence \
             where d.workspace_id = $1 and d.recipient_id = $2 \
               and d.is_message and d.state = '\"pending\"' \
             order by d.sequence limit $3",
        )
        .bind(workspace_id.to_string())
        .bind(recipient_id.to_string())
        .bind(i64::try_from(limit)?)
        .fetch_all(&self.pool)
        .await?;
        let mut result = Vec::with_capacity(rows.len());
        for row in &rows {
            let sequence = u64::try_from(row.try_get::<i64, _>("sequence")?)?;
            let event: WorkspaceEvent =
                serde_json::from_str(row.try_get::<&str, _>("event_json")?)?;
            let delivery = RecipientDelivery {
                workspace_id,
                sequence,
                recipient_id: Uuid::parse_str(row.try_get("recipient_id")?)?,
                mode: serde_json::from_str(row.try_get("mode")?)?,
                state: serde_json::from_str(row.try_get("state")?)?,
                attempts: u32::try_from(row.try_get::<i64, _>("attempts")?)?,
                last_attempt: row
                    .try_get::<Option<String>, _>("last_attempt_json")?
                    .map(|value| serde_json::from_str(&value))
                    .transpose()?,
            };
            result.push((event, delivery));
        }
        Ok(result)
    }

    async fn import_relay_message(
        &self,
        message: WorkspaceMessage,
        author_name: &str,
        recipient_id: Uuid,
        mode: DeliveryMode,
    ) -> Result<WorkspaceEvent> {
        ensure!(
            message.author_id != recipient_id,
            "relay sender cannot be its recipient"
        );
        ensure!(
            !message.body.text.trim().is_empty(),
            "relay message is empty"
        );
        ensure!(
            message.audience
                == Audience::Direct {
                    participant: recipient_id
                },
            "relay delivery recipient mismatch"
        );

        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "insert into workspaces (id, name, created_at) values ($1, $2, $3) \
             on conflict (id) do nothing",
        )
        .bind(message.workspace_id.to_string())
        .bind("Borg instance conversation")
        .bind(message.created_at.to_rfc3339())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "insert into workspace_participants (id, display_name, kind, created_at) \
             values ($1, $2, $3, $4) on conflict (id) do nothing",
        )
        .bind(message.author_id.to_string())
        .bind(author_name)
        .bind(serde_json::to_string(&ParticipantKind::Agent)?)
        .bind(message.created_at.to_rfc3339())
        .execute(&mut *transaction)
        .await?;
        // A relay identity must not silently attach itself to a local human
        // participant that happens to share the id.
        let author_kind: String =
            sqlx::query_scalar("select kind from workspace_participants where id = $1")
                .bind(message.author_id.to_string())
                .fetch_one(&mut *transaction)
                .await?;
        ensure!(
            serde_json::from_str::<ParticipantKind>(&author_kind)? == ParticipantKind::Agent,
            "relay agent identity conflicts with a local human participant"
        );
        for participant_id in [message.author_id, recipient_id] {
            sqlx::query(
                "insert into workspace_members (workspace_id, participant_id, role, joined_at) \
                 values ($1, $2, $3, $4) \
                 on conflict (workspace_id, participant_id) do nothing",
            )
            .bind(message.workspace_id.to_string())
            .bind(participant_id.to_string())
            .bind(serde_json::to_string(&WorkspaceRole::Viewer)?)
            .bind(message.created_at.to_rfc3339())
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;

        let event = WorkspaceEvent {
            id: message.id,
            workspace_id: message.workspace_id,
            sequence: 0,
            author_id: message.author_id,
            idempotency_key: format!("relay-message:{}", message.id),
            created_at: message.created_at,
            kind: WorkspaceEventKind::Message { message, mode },
        };
        // Reference validation is skipped: the thread or reply this message
        // belongs to may only exist in the cloud.
        self.append_event(event, false).await
    }

    async fn upsert_relay_roster_entry(
        &self,
        workspace_id: Uuid,
        participant: Participant,
        role: WorkspaceRole,
    ) -> Result<()> {
        let mut transaction = self.pool.begin().await?;
        // A projection, not an authority grant: there must already be a local
        // workspace to project onto.
        let workspace_exists: bool =
            sqlx::query_scalar("select exists(select 1 from workspaces where id = $1)")
                .bind(workspace_id.to_string())
                .fetch_one(&mut *transaction)
                .await?;
        ensure!(
            workspace_exists,
            "relay workspace is not materialized locally"
        );
        sqlx::query(
            "insert into workspace_participants (id, display_name, kind, created_at) \
             values ($1, $2, $3, $4) on conflict (id) do update set \
             display_name = excluded.display_name, kind = excluded.kind",
        )
        .bind(participant.id.to_string())
        .bind(participant.display_name)
        .bind(serde_json::to_string(&participant.kind)?)
        .bind(participant.created_at.to_rfc3339())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "insert into workspace_members (workspace_id, participant_id, role, joined_at) \
             values ($1, $2, $3, $4) \
             on conflict (workspace_id, participant_id) do update set role = excluded.role",
        )
        .bind(workspace_id.to_string())
        .bind(participant.id.to_string())
        .bind(serde_json::to_string(&role)?)
        .bind(Utc::now().to_rfc3339())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn message_sequence(&self, workspace_id: Uuid, message_id: Uuid) -> Result<Option<u64>> {
        // `is_message` is the stored generated column, so this stays an index
        // lookup rather than a JSON extraction per row.
        let sequence: Option<i64> = sqlx::query_scalar(
            "select sequence from workspace_events \
             where workspace_id = $1 and id = $2 and is_message",
        )
        .bind(workspace_id.to_string())
        .bind(message_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        sequence
            .map(|sequence| Ok(u64::try_from(sequence)?))
            .transpose()
    }

    async fn upsert_instance(
        &self,
        participant: Participant,
        host_id: Option<Uuid>,
        workspace_id: Option<Uuid>,
    ) -> Result<()> {
        ensure!(
            participant.kind == ParticipantKind::Agent,
            "instance must be an agent"
        );
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "insert into workspace_participants (id, display_name, kind, created_at) \
             values ($1, $2, $3, $4) on conflict (id) do update set \
             display_name = excluded.display_name, kind = excluded.kind",
        )
        .bind(participant.id.to_string())
        .bind(&participant.display_name)
        .bind(serde_json::to_string(&participant.kind)?)
        .bind(participant.created_at.to_rfc3339())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "insert into agent_instances (participant_id, host_id, workspace_id, seen_at) \
             values ($1, $2, $3, $4) on conflict (participant_id) do update set \
             host_id = excluded.host_id, workspace_id = excluded.workspace_id, \
             seen_at = excluded.seen_at",
        )
        .bind(participant.id.to_string())
        .bind(host_id.map(|id| id.to_string()))
        .bind(workspace_id.map(|id| id.to_string()))
        .bind(Utc::now().to_rfc3339())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn message_deliveries(&self, message_id: Uuid) -> Result<Vec<RecipientDelivery>> {
        let rows = sqlx::query(
            "select d.workspace_id, d.sequence, d.recipient_id, d.mode, d.state, d.attempts, \
                    d.last_attempt_json \
             from workspace_events e \
             join workspace_deliveries d \
               on d.workspace_id = e.workspace_id and d.sequence = e.sequence \
             where e.id = $1 order by d.recipient_id",
        )
        .bind(message_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| {
                Ok(RecipientDelivery {
                    workspace_id: Uuid::parse_str(row.try_get("workspace_id")?)?,
                    sequence: u64::try_from(row.try_get::<i64, _>("sequence")?)?,
                    recipient_id: Uuid::parse_str(row.try_get("recipient_id")?)?,
                    mode: serde_json::from_str(row.try_get("mode")?)?,
                    state: serde_json::from_str(row.try_get("state")?)?,
                    attempts: u32::try_from(row.try_get::<i64, _>("attempts")?)?,
                    last_attempt: row
                        .try_get::<Option<String>, _>("last_attempt_json")?
                        .map(|value| serde_json::from_str(&value))
                        .transpose()?,
                })
            })
            .collect()
    }

    async fn list_instances(&self, include_exited: bool) -> Result<Vec<AgentInstance>> {
        // The exit filter is applied in SQL, not in Rust: reaped rows are the
        // bulk of a long-lived installation and discovery pays a lookup per row
        // it materialises.
        let rows = sqlx::query(
            "select p.id, p.display_name, p.kind, p.created_at, i.host_id, i.workspace_id, \
                    i.seen_at, i.cwd, i.pid, i.exited_at \
             from workspace_participants p \
             left join agent_instances i on i.participant_id = p.id \
             where p.kind = $1 and ($2 or i.exited_at is null) \
             order by p.created_at, p.id",
        )
        .bind(serde_json::to_string(&ParticipantKind::Agent)?)
        .bind(include_exited)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| {
                let pid: Option<i64> = row.try_get("pid")?;
                Ok(AgentInstance {
                    participant: Participant {
                        id: Uuid::parse_str(row.try_get("id")?)?,
                        display_name: row.try_get("display_name")?,
                        kind: serde_json::from_str(row.try_get("kind")?)?,
                        created_at: DateTime::parse_from_rfc3339(row.try_get("created_at")?)?
                            .with_timezone(&Utc),
                    },
                    host_id: row
                        .try_get::<Option<String>, _>("host_id")?
                        .map(|id| Uuid::parse_str(&id))
                        .transpose()?,
                    workspace_id: row
                        .try_get::<Option<String>, _>("workspace_id")?
                        .map(|id| Uuid::parse_str(&id))
                        .transpose()?,
                    seen_at: row
                        .try_get::<Option<String>, _>("seen_at")?
                        .map(|seen| DateTime::parse_from_rfc3339(&seen))
                        .transpose()?
                        .map(|seen| seen.with_timezone(&Utc)),
                    cwd: row.try_get("cwd")?,
                    pid,
                    exited_at: row
                        .try_get::<Option<String>, _>("exited_at")?
                        .map(|exited| DateTime::parse_from_rfc3339(&exited))
                        .transpose()?
                        .map(|exited| exited.with_timezone(&Utc)),
                })
            })
            .collect()
    }

    async fn register_local_instance(
        &self,
        participant_id: Uuid,
        workspace_id: Option<Uuid>,
        cwd: &Path,
        pid: u32,
    ) -> Result<()> {
        // Re-registering clears the exit tombstone: a process that is running
        // again is not exited, and coalesce keeps a previously known workspace
        // when this launch does not name one.
        sqlx::query(
            "insert into agent_instances \
             (participant_id, host_id, workspace_id, seen_at, cwd, pid, exited_at) \
             values ($1, null, $2, $3, $4, $5, null) \
             on conflict (participant_id) do update set \
             workspace_id = coalesce(excluded.workspace_id, agent_instances.workspace_id), \
             seen_at = excluded.seen_at, cwd = excluded.cwd, pid = excluded.pid, \
             exited_at = null",
        )
        .bind(participant_id.to_string())
        .bind(workspace_id.map(|id| id.to_string()))
        .bind(Utc::now().to_rfc3339())
        .bind(cwd.to_string_lossy().to_string())
        .bind(i64::from(pid))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn mark_local_instances_exited(&self, participant_ids: &[Uuid]) -> Result<u64> {
        if participant_ids.is_empty() {
            return Ok(0);
        }
        // Tombstoned, never deleted: workspace_events.author_id and delivery
        // rows reference the participant, so removing it would break history.
        // `exited_at is null` keeps the first reaping time rather than moving
        // it on every sweep.
        let ids: Vec<String> = participant_ids.iter().map(Uuid::to_string).collect();
        let affected = sqlx::query(
            "update agent_instances set exited_at = $1 \
             where participant_id = any($2) and exited_at is null",
        )
        .bind(Utc::now().to_rfc3339())
        .bind(&ids)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(affected)
    }

    async fn contains_message(&self, message_id: Uuid) -> Result<bool> {
        // `is_message` is a stored generated column here, so this is an index
        // lookup rather than a per-row JSON extraction.
        Ok(sqlx::query_scalar(
            "select exists(select 1 from workspace_events where id = $1 and is_message)",
        )
        .bind(message_id.to_string())
        .fetch_one(&self.pool)
        .await?)
    }

    async fn contains_idempotent_event(
        &self,
        workspace_id: Uuid,
        author_id: Uuid,
        idempotency_key: &str,
    ) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "select exists(select 1 from workspace_events \
             where workspace_id = $1 and author_id = $2 and idempotency_key = $3)",
        )
        .bind(workspace_id.to_string())
        .bind(author_id.to_string())
        .bind(idempotency_key)
        .fetch_one(&self.pool)
        .await?)
    }

    async fn latest_projected_session_sequence(
        &self,
        workspace_id: Uuid,
        session_id: Uuid,
    ) -> Result<u64> {
        // Workspace bodies are plain text and never compressed, so reading
        // inside them in SQL is safe here -- unlike the session journal, where
        // a cold row would be invisible to this predicate.
        let sequence: Option<i64> = sqlx::query_scalar(
            "select max((event_json::jsonb #>> '{kind,session_sequence}')::bigint) \
             from workspace_events \
             where workspace_id = $1 \
               and (event_json::jsonb #>> '{kind,type}') = 'session_event' \
               and (event_json::jsonb #>> '{kind,session_id}') = $2",
        )
        .bind(workspace_id.to_string())
        .bind(session_id.to_string())
        .fetch_one(&self.pool)
        .await?;
        Ok(sequence
            .and_then(|sequence| u64::try_from(sequence).ok())
            .unwrap_or(0))
    }

    async fn participant(&self, participant_id: Uuid) -> Result<Option<Participant>> {
        let row = sqlx::query(
            "select id, display_name, kind, created_at from workspace_participants \
             where id = $1",
        )
        .bind(participant_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref()
            .map(|row| {
                Ok(Participant {
                    id: Uuid::parse_str(row.try_get("id")?)?,
                    display_name: row.try_get("display_name")?,
                    kind: serde_json::from_str(row.try_get("kind")?)?,
                    created_at: DateTime::parse_from_rfc3339(row.try_get("created_at")?)?
                        .with_timezone(&Utc),
                })
            })
            .transpose()
    }

    async fn workspace_name(&self, workspace_id: Uuid) -> Result<Option<String>> {
        Ok(
            sqlx::query_scalar("select name from workspaces where id = $1")
                .bind(workspace_id.to_string())
                .fetch_optional(&self.pool)
                .await?,
        )
    }

    async fn workspace_roster(
        &self,
        workspace_id: Uuid,
        viewer_id: Uuid,
    ) -> Result<Vec<WorkspaceRosterEntry>> {
        // A roster is membership information, so only a member may read it.
        self.require_member(workspace_id, viewer_id).await?;
        let rows = sqlx::query(
            "select p.id, p.display_name, p.kind, p.created_at, m.role, m.joined_at \
             from workspace_members m \
             join workspace_participants p on p.id = m.participant_id \
             where m.workspace_id = $1 order by m.joined_at, p.id",
        )
        .bind(workspace_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
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

    async fn list_workspaces_for_participant(
        &self,
        participant_id: Uuid,
    ) -> Result<Vec<Workspace>> {
        let rows = sqlx::query(
            "select w.id, w.name, w.created_at from workspaces w \
             join workspace_members m on m.workspace_id = w.id \
             where m.participant_id = $1 order by w.created_at, w.id",
        )
        .bind(participant_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
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

    async fn active_presence(
        &self,
        workspace_id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<Vec<PresenceLease>> {
        sqlx::query("delete from workspace_presence_leases where expires_at <= $1")
            .bind(now.to_rfc3339())
            .execute(&self.pool)
            .await?;
        let rows = sqlx::query(
            "select participant_id, client_id, host_id, expires_at \
             from workspace_presence_leases where workspace_id = $1 and expires_at > $2",
        )
        .bind(workspace_id.to_string())
        .bind(now.to_rfc3339())
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| {
                Ok(PresenceLease {
                    workspace_id,
                    participant_id: Uuid::parse_str(row.try_get("participant_id")?)?,
                    client_id: Uuid::parse_str(row.try_get("client_id")?)?,
                    host_id: row
                        .try_get::<Option<String>, _>("host_id")?
                        .map(|id| Uuid::parse_str(&id))
                        .transpose()?,
                    expires_at: DateTime::parse_from_rfc3339(row.try_get("expires_at")?)?
                        .with_timezone(&Utc),
                })
            })
            .collect()
    }
}
