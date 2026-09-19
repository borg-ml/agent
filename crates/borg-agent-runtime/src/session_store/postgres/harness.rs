//! Which harness owns a session's transcript.
//!
//! A conversation has exactly one owner: either Borg's own harness or the
//! provider's CLI. The route is decided once and then pinned, because a
//! transcript cannot migrate between owners in either direction -- only the
//! owner can replay its own history. Everything here exists to make that
//! decision once, durably, and never silently revise it.

use anyhow::{Context, Result, ensure};
use sqlx::{Postgres, Row, Transaction};
use uuid::Uuid;

use super::PostgresSessionStore;
use crate::SessionEventKind;
use crate::session_store::OPENCODE_GO_MODEL_PREFIX;

/// Does this session's lineage contain a native model message for `provider`?
///
/// Narrowed in SQL by the lifted `provider_event_kind` column and confirmed in
/// Rust, because the provider lives inside the body and a cold body is opaque
/// to SQL. Deciding "no native history" from a filter that cannot see aged rows
/// would hand a Borg-owned conversation to the CLI.
async fn has_native_history(
    transaction: &mut Transaction<'_, Postgres>,
    session_id: Uuid,
    provider: &str,
) -> Result<bool> {
    let rows = sqlx::query(
        "with recursive lineage(id) as ( \
             select id from sessions where id = $1 \
             union all \
             select s.parent_session_id from sessions s join lineage l on s.id = l.id \
             where s.parent_session_id is not null \
         ) \
         select e.event_json, e.event_body, e.dict_id from lineage l \
         join session_events e on e.session_id = l.id \
         where e.event_kind = 'provider_event' \
           and e.provider_event_kind = 'native_model_message' \
         limit 256",
    )
    .bind(session_id)
    .fetch_all(&mut **transaction)
    .await?;
    for row in &rows {
        let json: Option<serde_json::Value> = row.try_get("event_json")?;
        let body: Option<Vec<u8>> = row.try_get("event_body")?;
        let dict_id: Option<i32> = row.try_get("dict_id")?;
        // Hot rows answer directly; a cold row is decoded rather than skipped.
        let value = match json {
            Some(value) => value,
            None => {
                let bytes = body.unwrap_or_default();
                let dictionary = match dict_id {
                    Some(dict_id) => Some(super::body::EventDictionary {
                        dict_id,
                        bytes: sqlx::query_scalar(
                            "select dict_bytes from session_event_dicts where dict_id = $1",
                        )
                        .bind(dict_id)
                        .fetch_one(&mut **transaction)
                        .await?,
                    }),
                    None => None,
                };
                serde_json::from_slice(&super::body::decompress(&bytes, dictionary.as_ref())?)?
            }
        };
        let event: crate::SessionEvent = serde_json::from_value(value)?;
        if let SessionEventKind::ProviderEvent {
            provider: event_provider,
            ..
        } = &event.kind
            && serde_json::to_value(event_provider)?.as_str() == Some(provider)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

impl PostgresSessionStore {
    /// Resolve and pin this session's Codex harness route.
    pub(super) async fn resolve_codex_harness(
        transaction: &mut Transaction<'_, Postgres>,
        session_id: Uuid,
        inherited: Option<bool>,
    ) -> Result<bool> {
        let existing: Option<bool> = sqlx::query_scalar(
            "select native from session_harness_routes \
             where session_id = $1 and provider = 'codex'",
        )
        .bind(session_id)
        .fetch_optional(&mut **transaction)
        .await?;
        let native = if let Some(existing) = existing {
            existing
        } else {
            // Existing native history stays on Borg's harness, including
            // history predating account tags; never fall back to a CLI-owned
            // conversation.
            let tagged: bool = sqlx::query_scalar(
                "select exists(select 1 from session_model_access \
                 where session_id = $1 and provider = 'codex')",
            )
            .bind(session_id)
            .fetch_one(&mut **transaction)
            .await?;
            let native_history =
                tagged || has_native_history(transaction, session_id, "codex").await?;
            let row = sqlx::query(
                "select next_sequence, parent_session_id, owner_session_id \
                 from sessions where id = $1",
            )
            .bind(session_id)
            .fetch_one(&mut **transaction)
            .await?;
            let empty_root = row.try_get::<i64, _>("next_sequence")? == 1
                && row
                    .try_get::<Option<Uuid>, _>("parent_session_id")?
                    .is_none();
            let owner: Option<Uuid> = row.try_get("owner_session_id")?;
            let native = native_history || (empty_root && inherited.unwrap_or(owner.is_none()));
            sqlx::query(
                "insert into session_harness_routes (session_id, provider, native) \
                 values ($1, 'codex', $2) on conflict (session_id, provider) do nothing",
            )
            .bind(session_id)
            .bind(native)
            .execute(&mut **transaction)
            .await?;
            native
        };
        ensure!(
            inherited.is_none_or(|owner| owner == native),
            "child Codex harness differs from its owner's durable route; start a new child session"
        );
        Ok(native)
    }

    /// Resolve and pin the OpenCode harness route.
    ///
    /// Unlike Codex this route is *model-aware*: only the `opencode-go` aliases
    /// expose an endpoint Borg can drive itself, so every other OpenCode model
    /// stays on the CLI. Returns `None` while the route is still undecidable --
    /// a fresh session with no history and no model. Pinning that case would
    /// strand the session on a route before its model was ever known.
    pub(super) async fn resolve_opencode_harness(
        transaction: &mut Transaction<'_, Postgres>,
        session_id: Uuid,
        model: Option<&str>,
        inherited: Option<bool>,
    ) -> Result<Option<bool>> {
        let existing: Option<bool> = sqlx::query_scalar(
            "select native from session_harness_routes \
             where session_id = $1 and provider = 'open_code'",
        )
        .bind(session_id)
        .fetch_optional(&mut **transaction)
        .await?;
        if let Some(existing) = existing {
            ensure!(
                inherited.is_none_or(|owner| owner == existing),
                "child OpenCode harness differs from its owner's durable route; \
                 start a new child session"
            );
            return Ok(Some(existing));
        }

        let native_history = has_native_history(transaction, session_id, "open_code").await?;
        // Anything else on an OpenCode session -- a linked CLI thread, or
        // simply existing events -- means the CLI owns the transcript and only
        // it can replay that history. A model switch clears
        // `provider_session_id`, hence the second signal.
        let row = sqlx::query(
            "with recursive lineage(id) as ( \
                 select id from sessions where id = $1 \
                 union all \
                 select s.parent_session_id from sessions s join lineage l on s.id = l.id \
                 where s.parent_session_id is not null \
             ) \
             select exists(select 1 from lineage l join sessions s on s.id = l.id \
                 where s.state_json::jsonb #>> '{configuration,provider}' = 'open_code' \
                   and (s.state_json::jsonb #> '{provider_session_id}' is not null \
                     or (s.state_json::jsonb #>> '{latest_sequence}')::bigint > 0)) \
                 as legacy_history, \
             (select state_json::jsonb #>> '{configuration,model}' from sessions where id = $1) \
                 as durable_model",
        )
        .bind(session_id)
        .fetch_one(&mut **transaction)
        .await?;
        let legacy_history: bool = row.try_get("legacy_history")?;
        let durable_model: Option<String> = row.try_get("durable_model")?;

        if let Some(inherited) = inherited {
            ensure!(
                !(inherited && legacy_history) && !(!inherited && native_history),
                "child OpenCode harness differs from its owner's durable route; \
                 start a new child session"
            );
        }

        let selected = model
            .map(str::trim)
            .filter(|model| !model.is_empty())
            .map(str::to_owned)
            .or(durable_model);
        let native = if let Some(inherited) = inherited {
            // A child or fork shares its owner's history, so a split route
            // would give one conversation two different owners.
            inherited
        } else if native_history {
            true
        } else if legacy_history {
            false
        } else {
            match selected.as_deref() {
                // Must agree with the adapter's own alias check: pinning a
                // route the gateway then refuses would strand the session.
                Some(selected) => selected
                    .trim()
                    .strip_prefix(OPENCODE_GO_MODEL_PREFIX)
                    .is_some_and(|upstream| !upstream.trim().is_empty()),
                None => return Ok(None),
            }
        };
        sqlx::query(
            "insert into session_harness_routes (session_id, provider, native) \
             values ($1, 'open_code', $2) on conflict (session_id, provider) do nothing",
        )
        .bind(session_id)
        .bind(native)
        .execute(&mut **transaction)
        .await?;
        Ok(Some(native))
    }

    /// Resolve, and durably pin, this session's OpenCode route.
    pub async fn uses_native_opencode_harness(
        &self,
        session_id: Uuid,
        model: Option<&str>,
    ) -> Result<bool> {
        let mut transaction = self.pool().begin().await?;
        let native =
            Self::resolve_opencode_harness(&mut transaction, session_id, model, None).await?;
        transaction.commit().await?;
        Ok(native.unwrap_or(false))
    }

    /// Resolve, and durably pin, this session's Codex route.
    pub async fn uses_native_codex_harness(&self, session_id: Uuid) -> Result<bool> {
        let mut transaction = self.pool().begin().await?;
        let native = Self::resolve_codex_harness(&mut transaction, session_id, None).await?;
        transaction.commit().await?;
        Ok(native)
    }

    /// Record which account last drove this session for `provider`.
    ///
    /// Refuses a Codex write when the session is pinned to the CLI route: the
    /// account tag is also the signal `resolve_codex_harness` reads to decide a
    /// route for an untagged session, so tagging a CLI-owned conversation would
    /// later flip it to Borg's harness and strand a transcript only the CLI can
    /// replay.
    pub async fn record_model_access(
        &self,
        session_id: Uuid,
        provider: crate::CodingProvider,
        account_identity: &str,
    ) -> Result<()> {
        ensure!(
            !account_identity.is_empty(),
            "model account identity is empty"
        );
        let mut transaction = self.pool().begin().await?;
        if provider == crate::CodingProvider::Codex {
            let native: Option<bool> = sqlx::query_scalar(
                "select native from session_harness_routes \
                 where session_id = $1 and provider = 'codex'",
            )
            .bind(session_id)
            .fetch_optional(&mut *transaction)
            .await?;
            ensure!(
                native != Some(false),
                "this session retains its Codex compatibility route; \
                 start a new session for Borg-owned execution"
            );
        }
        let provider = serde_json::to_value(provider)?
            .as_str()
            .context("provider is not a string")?
            .to_owned();
        // This records the last selected access, not ownership of the session.
        sqlx::query(
            "insert into session_model_access (session_id, provider, account_identity) \
             values ($1, $2, $3) on conflict (session_id, provider) \
             do update set account_identity = excluded.account_identity",
        )
        .bind(session_id)
        .bind(&provider)
        .bind(account_identity)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Give `session_id` the same routes as `source_session_id`.
    ///
    /// Used when a fork or child adopts an existing conversation: the inherited
    /// route is passed to the resolver rather than copied, so a conflicting
    /// pinned route is refused instead of silently overwritten.
    pub(super) async fn inherit_harness_routes(
        transaction: &mut Transaction<'_, Postgres>,
        source_session_id: Uuid,
        session_id: Uuid,
    ) -> Result<()> {
        let source_codex =
            Self::resolve_codex_harness(transaction, source_session_id, None).await?;
        Self::resolve_codex_harness(transaction, session_id, Some(source_codex)).await?;
        // An undecided source leaves the child free to resolve its own route.
        let source_opencode =
            Self::resolve_opencode_harness(transaction, source_session_id, None, None).await?;
        Self::resolve_opencode_harness(transaction, session_id, None, source_opencode).await?;
        Ok(())
    }
}
