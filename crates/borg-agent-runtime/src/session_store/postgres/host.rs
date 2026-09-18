//! Host launch admission, journal cursors, bootstraps and workspace bindings.
//!
//! These rows decide which host is allowed to drive a session and how much of
//! its journal that host has already seen. The invariants are ownership ones,
//! so most of the code here is refusal: a launch may not change after
//! admission, ownership may not move between hosts, and a cursor may not move
//! backwards.

use anyhow::{Context, Result, ensure};
use chrono::Utc;
use sqlx::Row;
use std::collections::HashMap;
use uuid::Uuid;

use super::PostgresSessionStore;
use crate::session_store::{
    MAX_HOST_LAUNCH_METADATA_BYTES, SessionActionState, SessionState, SessionWorkspaceBinding,
};

impl PostgresSessionStore {
    pub async fn contains_session(&self, session_id: Uuid) -> Result<bool> {
        Ok(
            sqlx::query_scalar("select exists(select 1 from sessions where id = $1)")
                .bind(session_id)
                .fetch_one(self.pool())
                .await?,
        )
    }

    pub async fn persist_host_launch_metadata(
        &self,
        session_id: Uuid,
        metadata: &serde_json::Value,
    ) -> Result<()> {
        self.persist_host_launch(session_id, metadata, None).await
    }

    pub async fn persist_owned_host_launch_metadata(
        &self,
        session_id: Uuid,
        metadata: &serde_json::Value,
        host_id: Uuid,
        relay_origin: &str,
    ) -> Result<()> {
        self.persist_host_launch(session_id, metadata, Some((host_id, relay_origin)))
            .await
    }

    /// Admit a launch, optionally claiming ownership of it.
    ///
    /// Launch metadata is immutable once admitted: re-admitting the same
    /// session with different metadata is a different launch request wearing
    /// the same id, and is refused rather than silently replacing the original.
    async fn persist_host_launch(
        &self,
        session_id: Uuid,
        metadata: &serde_json::Value,
        owner: Option<(Uuid, &str)>,
    ) -> Result<()> {
        ensure!(
            metadata.is_object(),
            "host launch metadata must be an object"
        );
        let metadata_json = serde_json::to_string(metadata)?;
        ensure!(
            metadata_json.len() <= MAX_HOST_LAUNCH_METADATA_BYTES,
            "host launch metadata exceeds {MAX_HOST_LAUNCH_METADATA_BYTES} bytes"
        );
        let mut transaction = self.pool().begin().await?;
        if let Some((host_id, _)) = owner {
            let bound: Option<Option<Uuid>> = sqlx::query_scalar(
                "select host_id from session_workspace_bindings where session_id = $1",
            )
            .bind(session_id)
            .fetch_optional(&mut *transaction)
            .await?;
            if let Some(bound) = bound.flatten() {
                ensure!(bound == host_id, "session binding belongs to another host");
            }
            // The launch can name the host it was created for; a mismatch means
            // one host is trying to admit another host's launch.
            if let Some(identity) = metadata.pointer("/attachment/host_identity/host_id") {
                let identity = identity
                    .as_str()
                    .context("invalid launch host identity")?
                    .parse::<Uuid>()
                    .context("invalid launch host identity")?;
                ensure!(
                    identity == host_id,
                    "launch attachment belongs to another host"
                );
            }
        }
        let existing: Option<serde_json::Value> = sqlx::query_scalar(
            "select metadata_json from host_launches where session_id = $1 for update",
        )
        .bind(session_id)
        .fetch_optional(&mut *transaction)
        .await?;
        let was_existing = existing.is_some();
        if let Some(existing) = existing {
            ensure!(
                serde_json::to_string(&existing)? == metadata_json,
                "session launch metadata already exists for a different launch request"
            );
        } else {
            let now = Utc::now();
            sqlx::query(
                "insert into host_launches (session_id, metadata_json, created_at, updated_at) \
                 values ($1, $2, $3, $3)",
            )
            .bind(session_id)
            .bind(metadata)
            .bind(now)
            .execute(&mut *transaction)
            .await?;
        }
        if let Some((host_id, relay_origin)) = owner {
            let existing_owner = sqlx::query(
                "select host_id, relay_origin from host_launch_owners where session_id = $1 \
                 for update",
            )
            .bind(session_id)
            .fetch_optional(&mut *transaction)
            .await?;
            if let Some(row) = existing_owner {
                ensure!(
                    row.try_get::<Uuid, _>("host_id")? == host_id
                        && row.try_get::<String, _>("relay_origin")? == relay_origin,
                    "host launch already belongs to another host or relay"
                );
            } else {
                // An already-admitted launch with no owner predates ownership
                // tracking; claiming it needs the relay-verified path, not a
                // plain re-admission.
                ensure!(
                    !was_existing,
                    "legacy launch requires relay-verified ownership before admission"
                );
                sqlx::query(
                    "insert into host_launch_owners (session_id, host_id, relay_origin) \
                     values ($1, $2, $3)",
                )
                .bind(session_id)
                .bind(host_id)
                .bind(relay_origin)
                .execute(&mut *transaction)
                .await?;
            }
        }
        transaction.commit().await?;
        Ok(())
    }

    pub async fn host_launch_owner(&self, session_id: Uuid) -> Result<Option<(Uuid, String)>> {
        let row = sqlx::query(
            "select host_id, relay_origin from host_launch_owners where session_id = $1",
        )
        .bind(session_id)
        .fetch_optional(self.pool())
        .await?;
        row.map(|row| Ok((row.try_get("host_id")?, row.try_get("relay_origin")?)))
            .transpose()
    }

    /// Claim a legacy, unowned launch. The caller must already have verified
    /// this session with the host-authenticated relay.
    pub async fn claim_legacy_host_launch_owner(
        &self,
        session_id: Uuid,
        host_id: Uuid,
        relay_origin: &str,
    ) -> Result<()> {
        let mut transaction = self.pool().begin().await?;
        let metadata: serde_json::Value =
            sqlx::query_scalar("select metadata_json from host_launches where session_id = $1")
                .bind(session_id)
                .fetch_one(&mut *transaction)
                .await?;
        let bound: Option<Option<Uuid>> = sqlx::query_scalar(
            "select host_id from session_workspace_bindings where session_id = $1",
        )
        .bind(session_id)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(bound) = bound.flatten() {
            ensure!(
                bound == host_id,
                "legacy session binding belongs to another host"
            );
        }
        if let Some(owner) = metadata.pointer("/attachment/host_identity/host_id") {
            let owner = owner
                .as_str()
                .context("invalid legacy launch host identity")?
                .parse::<Uuid>()
                .context("invalid legacy launch host identity")?;
            ensure!(owner == host_id, "legacy launch belongs to another host");
        }
        sqlx::query(
            "insert into host_launch_owners (session_id, host_id, relay_origin) \
             values ($1, $2, $3) on conflict (session_id) do nothing",
        )
        .bind(session_id)
        .bind(host_id)
        .bind(relay_origin)
        .execute(&mut *transaction)
        .await?;
        // Re-read rather than trust the insert: another host may have claimed
        // this launch first, and the conflict would have been silent.
        let row = sqlx::query(
            "select host_id, relay_origin from host_launch_owners where session_id = $1",
        )
        .bind(session_id)
        .fetch_one(&mut *transaction)
        .await?;
        ensure!(
            row.try_get::<Uuid, _>("host_id")? == host_id
                && row.try_get::<String, _>("relay_origin")? == relay_origin,
            "host launch already belongs to another host or relay"
        );
        transaction.commit().await?;
        Ok(())
    }

    /// Load immutable host launch authorization, rejecting malformed rows
    /// rather than treating them as a missing session.
    pub async fn load_host_launch_metadata(
        &self,
        session_id: Uuid,
    ) -> Result<Option<serde_json::Value>> {
        let metadata: Option<serde_json::Value> =
            sqlx::query_scalar("select metadata_json from host_launches where session_id = $1")
                .bind(session_id)
                .fetch_optional(self.pool())
                .await?;
        let Some(metadata) = metadata else {
            return Ok(None);
        };
        ensure!(
            serde_json::to_string(&metadata)?.len() <= MAX_HOST_LAUNCH_METADATA_BYTES,
            "host launch metadata exceeds {MAX_HOST_LAUNCH_METADATA_BYTES} bytes"
        );
        Ok(Some(metadata))
    }

    /// Sessions whose journal has moved past what their host has acknowledged.
    pub async fn pending_host_journals(
        &self,
        after: Option<Uuid>,
        limit: usize,
    ) -> Result<Vec<Uuid>> {
        Ok(sqlx::query_scalar(
            "select h.session_id from host_launches h \
             left join sessions s on s.id = h.session_id \
             left join host_launch_owners o on o.session_id = h.session_id \
             left join host_journal_cursors c on c.session_id = h.session_id \
             where ($1::uuid is null or h.session_id > $1::uuid) \
               and (o.session_id is null \
                 or s.next_sequence - 1 > coalesce(c.event_cursor, 0) \
                 or exists(select 1 from session_live_state l \
                   where l.session_id = h.session_id \
                     and l.revision > coalesce(c.live_revision, 0))) \
             order by h.session_id limit $2",
        )
        .bind(after)
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(self.pool())
        .await?)
    }

    pub async fn acknowledge_host_journal(
        &self,
        session_id: Uuid,
        event_cursor: u64,
        live_revision: u64,
    ) -> Result<()> {
        // Cursors only move forward: an out-of-order acknowledgement must not
        // rewind delivery and replay journal the host already consumed.
        sqlx::query(
            "insert into host_journal_cursors (session_id, event_cursor, live_revision) \
             values ($1, $2, $3) on conflict (session_id) do update set \
             event_cursor = greatest(host_journal_cursors.event_cursor, excluded.event_cursor), \
             live_revision = greatest(host_journal_cursors.live_revision, excluded.live_revision)",
        )
        .bind(session_id)
        .bind(i64::try_from(event_cursor).context("host event cursor exceeds a bigint")?)
        .bind(i64::try_from(live_revision).context("host live revision exceeds a bigint")?)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    pub async fn begin_host_bootstrap(&self, session_id: Uuid) -> Result<()> {
        sqlx::query(
            "insert into host_bootstraps (session_id) values ($1) \
             on conflict (session_id) do nothing",
        )
        .bind(session_id)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    pub async fn finish_host_bootstrap(&self, session_id: Uuid) -> Result<()> {
        sqlx::query("delete from host_bootstraps where session_id = $1")
            .bind(session_id)
            .execute(self.pool())
            .await?;
        Ok(())
    }

    /// Cancel work left behind by a session that ended terminally.
    ///
    /// The caller must hold the session writer lease. Work is cancelled in
    /// bounded batches so settling a session with a long queue does not hold
    /// one transaction open across the whole sweep.
    pub async fn settle_terminal_host_session(&self, session_id: Uuid) -> Result<()> {
        const BATCH: usize = 128;
        loop {
            let mut transaction = self.pool().begin().await?;
            let state_json: Option<String> =
                sqlx::query_scalar("select state_json from sessions where id = $1")
                    .bind(session_id)
                    .fetch_optional(&mut *transaction)
                    .await?;
            let terminal = state_json
                .as_deref()
                .map(serde_json::from_str::<SessionState>)
                .transpose()?
                .and_then(|state| state.status)
                .is_some_and(|status| {
                    matches!(
                        status,
                        crate::SessionStatus::Failed
                            | crate::SessionStatus::Stopped
                            | crate::SessionStatus::Completed
                    )
                });
            ensure!(terminal, "cannot settle a non-terminal host session");

            let pending: Vec<Uuid> = sqlx::query_scalar(
                "select action_id from session_actions \
                 where session_id = $1 and state not in ('completed', 'failed', 'cancelled') \
                 order by action_id limit $2",
            )
            .bind(session_id)
            .bind(i64::try_from(BATCH).unwrap_or(i64::MAX))
            .fetch_all(&mut *transaction)
            .await?;
            for action_id in &pending {
                self.cancel_action_in_transaction(
                    &mut transaction,
                    session_id,
                    *action_id,
                    "host session is terminal",
                )
                .await?;
            }
            if pending.len() < BATCH {
                sqlx::query("delete from host_bootstraps where session_id = $1")
                    .bind(session_id)
                    .execute(&mut *transaction)
                    .await?;
                transaction.commit().await?;
                return Ok(());
            }
            transaction.commit().await?;
        }
    }

    /// Host-owned launches with unfinished bootstrap or session actions.
    ///
    /// A host calls this after a process restart so an acknowledged prompt
    /// cannot stay stranded until another command happens to arrive.
    pub async fn pending_host_launch_metadata(
        &self,
        limit: usize,
    ) -> Result<Vec<(Uuid, serde_json::Value)>> {
        self.pending_host_launch_metadata_for_host(0, None, limit)
            .await
    }

    pub async fn pending_host_launch_metadata_for_host(
        &self,
        offset: usize,
        owner: Option<(Uuid, &str)>,
        limit: usize,
    ) -> Result<Vec<(Uuid, serde_json::Value)>> {
        let (host_id, relay_origin) = match owner {
            Some((host_id, relay_origin)) => (Some(host_id), Some(relay_origin)),
            None => (None, None),
        };
        let rows = sqlx::query(
            "select h.session_id, h.metadata_json from host_launches h \
             left join host_launch_owners o on o.session_id = h.session_id \
             left join session_workspace_bindings w on w.session_id = h.session_id \
             where (exists (select 1 from host_bootstraps b where b.session_id = h.session_id) \
                or exists (select 1 from session_actions a \
                 where a.session_id = h.session_id \
                   and a.state not in ('completed', 'failed', 'cancelled'))) \
               and ($1::uuid is null \
                 or ((o.session_id is null \
                      or (o.host_id = $1::uuid and o.relay_origin = $2)) \
                   and (w.host_id is null or w.host_id = $1::uuid) \
                   and (h.metadata_json #>> '{attachment,host_identity,host_id}' is null \
                     or h.metadata_json #>> '{attachment,host_identity,host_id}' \
                        = $1::uuid::text))) \
             order by case when $1::uuid is null then false else o.session_id is null end, \
                      h.created_at asc, h.session_id asc \
             limit $3 offset $4",
        )
        .bind(host_id)
        .bind(relay_origin)
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .bind(i64::try_from(offset).unwrap_or(0))
        .fetch_all(self.pool())
        .await?;
        rows.iter()
            .map(|row| Ok((row.try_get("session_id")?, row.try_get("metadata_json")?)))
            .collect()
    }

    pub async fn attach_session_workspace(
        &self,
        binding: SessionWorkspaceBinding,
    ) -> Result<SessionWorkspaceBinding> {
        let mut transaction = self.pool().begin().await?;
        let exists: bool =
            sqlx::query_scalar("select exists(select 1 from sessions where id = $1)")
                .bind(binding.session_id)
                .fetch_one(&mut *transaction)
                .await?;
        ensure!(exists, "session {} does not exist", binding.session_id);
        let existing = sqlx::query(
            "select workspace_id, participant_id from session_workspace_bindings \
             where session_id = $1 for update",
        )
        .bind(binding.session_id)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(existing) = existing {
            // Only the host and attachment time may change. Moving a session
            // to a different workspace or identity would silently re-home its
            // history.
            let workspace_id: Uuid = existing.try_get("workspace_id")?;
            let participant_id: Uuid = existing.try_get("participant_id")?;
            ensure!(
                workspace_id == binding.workspace_id && participant_id == binding.participant_id,
                "session {} is already attached to workspace {} as participant {}",
                binding.session_id,
                workspace_id,
                participant_id
            );
        }
        sqlx::query(
            "insert into session_workspace_bindings \
             (session_id, workspace_id, participant_id, host_id, attached_at) \
             values ($1, $2, $3, $4, $5) \
             on conflict (session_id) do update set \
             host_id = excluded.host_id, attached_at = excluded.attached_at",
        )
        .bind(binding.session_id)
        .bind(binding.workspace_id)
        .bind(binding.participant_id)
        .bind(binding.host_id)
        .bind(binding.attached_at)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(binding)
    }

    pub async fn session_workspace_binding(
        &self,
        session_id: Uuid,
    ) -> Result<Option<SessionWorkspaceBinding>> {
        let row = sqlx::query(
            "select workspace_id, participant_id, host_id, attached_at \
             from session_workspace_bindings where session_id = $1",
        )
        .bind(session_id)
        .fetch_optional(self.pool())
        .await?;
        row.map(|row| {
            Ok(SessionWorkspaceBinding {
                session_id,
                workspace_id: row.try_get("workspace_id")?,
                participant_id: row.try_get("participant_id")?,
                host_id: row.try_get("host_id")?,
                attached_at: row.try_get("attached_at")?,
            })
        })
        .transpose()
    }

    /// Record `session_id` as a child of `owner_session_id`.
    ///
    /// A child shares its owner's conversation, so it also shares the harness
    /// route that owns it; an owner whose route is undecided leaves the child
    /// free to resolve its own.
    pub async fn register_child_session(
        &self,
        owner_session_id: Uuid,
        session_id: Uuid,
    ) -> Result<()> {
        ensure!(
            self.contains_session(owner_session_id).await?,
            "owner session {owner_session_id} does not exist"
        );
        if !self.contains_session(session_id).await? {
            crate::SessionStore::create_session(self, session_id).await?;
        }
        let mut transaction = self.pool().begin().await?;
        let existing_owner: Option<Uuid> =
            sqlx::query_scalar("select owner_session_id from sessions where id = $1 for update")
                .bind(session_id)
                .fetch_one(&mut *transaction)
                .await?;
        if let Some(existing_owner) = existing_owner {
            ensure!(
                existing_owner == owner_session_id,
                "child session {session_id} already belongs to {existing_owner}"
            );
        } else {
            sqlx::query("update sessions set owner_session_id = $1 where id = $2")
                .bind(owner_session_id)
                .bind(session_id)
                .execute(&mut *transaction)
                .await?;
        }
        // A child shares its owner's conversation, so it must share the route
        // that owns it. Resolving with the owner's route rather than copying
        // means a child that already pinned a different route is refused
        // instead of being silently re-homed.
        Self::inherit_harness_routes(&mut transaction, owner_session_id, session_id).await?;
        let owner_workspace: Option<Uuid> = sqlx::query_scalar(
            "select workspace_id from session_workspace_bindings where session_id = $1",
        )
        .bind(owner_session_id)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(workspace_id) = owner_workspace {
            sqlx::query(
                "insert into session_workspace_bindings \
                 (session_id, workspace_id, participant_id, attached_at) \
                 values ($1, $2, $1, $3) on conflict (session_id) do nothing",
            )
            .bind(session_id)
            .bind(workspace_id)
            .bind(Utc::now())
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    pub async fn host_workspace_cursor_map(
        &self,
        host_id: Uuid,
        session_id: Uuid,
    ) -> Result<HashMap<Uuid, u64>> {
        crate::SessionStore::host_workspace_cursors(self, host_id, session_id).await
    }
}

/// Cancel one action inside an existing transaction, recording the audit
/// transition. Lives here rather than in `actions` because settling is the only
/// caller that cancels work it does not own a lease for.
impl PostgresSessionStore {
    async fn cancel_action_in_transaction(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        session_id: Uuid,
        action_id: Uuid,
        reason: &str,
    ) -> Result<()> {
        super::actions::transition_action_in_transaction(
            transaction,
            session_id,
            action_id,
            None,
            SessionActionState::Cancelled,
            Some(reason.to_string()),
        )
        .await
        .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::super::testing::{ScratchDatabase, test_url};
    use super::*;
    use crate::session_store::SessionStore;
    use crate::{SessionEvent, SessionEventKind, SessionStatus};

    fn metadata(host_id: Option<Uuid>) -> serde_json::Value {
        match host_id {
            Some(host_id) => serde_json::json!({
                "cwd": "/home/shulgin/agent",
                "attachment": {"host_identity": {"host_id": host_id}},
            }),
            None => serde_json::json!({"cwd": "/home/shulgin/agent"}),
        }
    }

    async fn store(url: &str) -> (ScratchDatabase, PostgresSessionStore) {
        let scratch = ScratchDatabase::create(url).await;
        let store = PostgresSessionStore::connect(&scratch.url)
            .await
            .expect("connect");
        (scratch, store)
    }

    #[tokio::test]
    async fn a_launch_is_immutable_once_admitted() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store) = store(&url).await;
        let session_id = Uuid::new_v4();
        let host_id = Uuid::new_v4();
        store
            .persist_owned_host_launch_metadata(
                session_id,
                &metadata(Some(host_id)),
                host_id,
                "wss://relay.example",
            )
            .await
            .expect("admit");

        // Re-admitting the identical launch is idempotent.
        store
            .persist_owned_host_launch_metadata(
                session_id,
                &metadata(Some(host_id)),
                host_id,
                "wss://relay.example",
            )
            .await
            .expect("re-admit");

        // A different launch wearing the same session id is refused.
        let mut changed = metadata(Some(host_id));
        changed["cwd"] = serde_json::json!("/somewhere/else");
        assert!(
            store
                .persist_owned_host_launch_metadata(
                    session_id,
                    &changed,
                    host_id,
                    "wss://relay.example"
                )
                .await
                .is_err()
        );

        assert_eq!(
            store.load_host_launch_metadata(session_id).await.unwrap(),
            Some(metadata(Some(host_id)))
        );
        assert_eq!(
            store.host_launch_owner(session_id).await.unwrap(),
            Some((host_id, "wss://relay.example".to_string()))
        );
        scratch.discard().await;
    }

    #[tokio::test]
    async fn ownership_cannot_move_between_hosts_or_relays() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store) = store(&url).await;
        let session_id = Uuid::new_v4();
        let host_id = Uuid::new_v4();
        let other_host = Uuid::new_v4();
        store
            .persist_owned_host_launch_metadata(
                session_id,
                &metadata(Some(host_id)),
                host_id,
                "wss://relay.example",
            )
            .await
            .expect("admit");

        // Another host must not take over an owned launch...
        assert!(
            store
                .persist_owned_host_launch_metadata(
                    session_id,
                    &metadata(Some(host_id)),
                    other_host,
                    "wss://relay.example"
                )
                .await
                .is_err()
        );
        // ...nor may the same host arrive through a different relay.
        assert!(
            store
                .persist_owned_host_launch_metadata(
                    session_id,
                    &metadata(Some(host_id)),
                    host_id,
                    "wss://other-relay.example"
                )
                .await
                .is_err()
        );
        // A launch naming one host cannot be admitted by another.
        assert!(
            store
                .persist_owned_host_launch_metadata(
                    Uuid::new_v4(),
                    &metadata(Some(other_host)),
                    host_id,
                    "wss://relay.example"
                )
                .await
                .is_err()
        );
        scratch.discard().await;
    }

    #[tokio::test]
    async fn an_unowned_legacy_launch_needs_the_relay_verified_claim() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store) = store(&url).await;
        let session_id = Uuid::new_v4();
        let host_id = Uuid::new_v4();
        store
            .persist_host_launch_metadata(session_id, &metadata(None))
            .await
            .expect("legacy admit");
        assert_eq!(store.host_launch_owner(session_id).await.unwrap(), None);

        // Ordinary re-admission must not quietly become an ownership claim.
        assert!(
            store
                .persist_owned_host_launch_metadata(
                    session_id,
                    &metadata(None),
                    host_id,
                    "wss://relay.example"
                )
                .await
                .is_err(),
            "a legacy launch must be claimed through the verified path"
        );

        store
            .claim_legacy_host_launch_owner(session_id, host_id, "wss://relay.example")
            .await
            .expect("claim");
        assert_eq!(
            store.host_launch_owner(session_id).await.unwrap(),
            Some((host_id, "wss://relay.example".to_string()))
        );
        // A second host loses the race rather than overwriting the winner.
        assert!(
            store
                .claim_legacy_host_launch_owner(session_id, Uuid::new_v4(), "wss://relay.example")
                .await
                .is_err()
        );
        scratch.discard().await;
    }

    #[tokio::test]
    async fn journal_cursors_track_progress_and_never_rewind() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store) = store(&url).await;
        let session_id = Uuid::new_v4();
        let host_id = Uuid::new_v4();
        store.create_session(session_id).await.expect("create");
        store
            .persist_owned_host_launch_metadata(
                session_id,
                &metadata(Some(host_id)),
                host_id,
                "wss://relay.example",
            )
            .await
            .expect("admit");
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::SessionStarted,
            ))
            .await
            .expect("started");

        let pending = store.pending_host_journals(None, 10).await.expect("pending");
        assert!(pending.contains(&session_id), "unsent journal must be listed");

        store
            .acknowledge_host_journal(session_id, 1, 0)
            .await
            .expect("ack");
        assert!(
            !store
                .pending_host_journals(None, 10)
                .await
                .unwrap()
                .contains(&session_id),
            "an acknowledged journal is no longer pending"
        );

        // A stale acknowledgement must not rewind delivery and replay journal
        // the host already consumed.
        store
            .acknowledge_host_journal(session_id, 0, 0)
            .await
            .expect("stale ack");
        assert!(
            !store
                .pending_host_journals(None, 10)
                .await
                .unwrap()
                .contains(&session_id)
        );
        scratch.discard().await;
    }

    #[tokio::test]
    async fn a_bootstrapping_launch_is_reported_until_it_finishes() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store) = store(&url).await;
        let session_id = Uuid::new_v4();
        let host_id = Uuid::new_v4();
        store.create_session(session_id).await.expect("create");
        store
            .persist_owned_host_launch_metadata(
                session_id,
                &metadata(Some(host_id)),
                host_id,
                "wss://relay.example",
            )
            .await
            .expect("admit");
        assert!(
            store
                .pending_host_launch_metadata(10)
                .await
                .unwrap()
                .is_empty()
        );

        store.begin_host_bootstrap(session_id).await.expect("begin");
        let pending = store.pending_host_launch_metadata(10).await.expect("pending");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0, session_id);

        // Scoped to the owning host: another host must not be handed this work.
        let scoped = store
            .pending_host_launch_metadata_for_host(
                0,
                Some((Uuid::new_v4(), "wss://relay.example")),
                10,
            )
            .await
            .expect("scoped");
        assert!(scoped.is_empty());

        store.finish_host_bootstrap(session_id).await.expect("finish");
        assert!(
            store
                .pending_host_launch_metadata(10)
                .await
                .unwrap()
                .is_empty()
        );
        scratch.discard().await;
    }

    #[tokio::test]
    async fn settling_refuses_a_live_session_and_cancels_a_terminal_ones_work() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store) = store(&url).await;
        let session_id = Uuid::new_v4();
        let host_id = Uuid::new_v4();
        store.create_session(session_id).await.expect("create");
        // A bootstrap marker references an admitted launch, so the launch has
        // to exist first -- the schema enforces that, not just convention.
        store
            .persist_owned_host_launch_metadata(
                session_id,
                &metadata(Some(host_id)),
                host_id,
                "wss://relay.example",
            )
            .await
            .expect("admit");
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::SessionStarted,
            ))
            .await
            .expect("started");
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::StatusChanged {
                    status: SessionStatus::Running,
                    detail: None,
                },
            ))
            .await
            .expect("running");
        let action = crate::session_action::SessionAction::new(
            Uuid::new_v4(),
            session_id,
            crate::session_action::SessionActionKind::Prompt,
            crate::session_action::ActionDeliveryPolicy::NextTurnBoundary,
            crate::session_action::ActionWakePolicy::OnLowerBoundary,
            serde_json::json!({"text": "unfinished"}),
        );
        let action_id = store.enqueue_action(action).await.expect("enqueue").action_id;

        // Live work must not be swept away by a settle.
        assert!(
            store.settle_terminal_host_session(session_id).await.is_err(),
            "a running session is not settleable"
        );

        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::StatusChanged {
                    status: SessionStatus::Stopped,
                    detail: None,
                },
            ))
            .await
            .expect("stopped");
        store.begin_host_bootstrap(session_id).await.expect("begin");
        store
            .settle_terminal_host_session(session_id)
            .await
            .expect("settle");

        let settled = store.action(session_id, action_id).await.unwrap().unwrap();
        assert_eq!(settled.state, crate::session_action::SessionActionState::Cancelled);
        assert_eq!(settled.error.as_deref(), Some("host session is terminal"));
        assert!(store.pending_actions(session_id, 10).await.unwrap().is_empty());
        assert!(
            store
                .pending_host_launch_metadata(10)
                .await
                .unwrap()
                .is_empty(),
            "settling must clear the bootstrap marker"
        );
        scratch.discard().await;
    }

    #[tokio::test]
    async fn a_binding_may_change_host_but_not_workspace_or_identity() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store) = store(&url).await;
        let session_id = Uuid::new_v4();
        store.create_session(session_id).await.expect("create");
        let binding = store
            .workspace_binding(session_id)
            .await
            .expect("binding")
            .expect("create_session binds a workspace");

        let host_id = Uuid::new_v4();
        let rebound = store
            .attach_workspace(SessionWorkspaceBinding {
                host_id: Some(host_id),
                attached_at: Utc::now(),
                ..binding.clone()
            })
            .await
            .expect("attach");
        assert_eq!(rebound.host_id, Some(host_id));

        // Re-homing a session's history to another workspace is refused.
        assert!(
            store
                .attach_workspace(SessionWorkspaceBinding {
                    workspace_id: Uuid::new_v4(),
                    ..binding.clone()
                })
                .await
                .is_err()
        );
        assert!(
            store
                .attach_workspace(SessionWorkspaceBinding {
                    participant_id: Uuid::new_v4(),
                    ..binding
                })
                .await
                .is_err()
        );
        scratch.discard().await;
    }

    #[tokio::test]
    async fn a_child_session_belongs_to_exactly_one_owner() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store) = store(&url).await;
        let owner = Uuid::new_v4();
        let child = Uuid::new_v4();
        store.create_session(owner).await.expect("create owner");

        // An unknown owner cannot adopt anything.
        assert!(
            PostgresSessionStore::register_child_session(&store, Uuid::new_v4(), child)
                .await
                .is_err()
        );

        // Registering creates the child if it does not exist yet.
        PostgresSessionStore::register_child_session(&store, owner, child)
            .await
            .expect("register");
        assert!(store.contains_session(child).await.unwrap());
        // Repeating is idempotent; a second owner is refused.
        PostgresSessionStore::register_child_session(&store, owner, child)
            .await
            .expect("re-register");
        let other_owner = Uuid::new_v4();
        store.create_session(other_owner).await.expect("create");
        assert!(
            PostgresSessionStore::register_child_session(&store, other_owner, child)
                .await
                .is_err(),
            "a child must not be adopted away from its owner"
        );

        // A child is not a root session, so it must not appear in listings.
        let roots: Vec<Uuid> = store
            .list_sessions(50)
            .await
            .unwrap()
            .into_iter()
            .map(|summary| summary.session_id)
            .collect();
        assert!(roots.contains(&owner));
        assert!(!roots.contains(&child));
        scratch.discard().await;
    }
}
