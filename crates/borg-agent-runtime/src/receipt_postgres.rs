//! Receipts and the relay's durable command queue, on PostgreSQL.
//!
//! A receipt is how a host proves a mutation happened exactly once across a
//! crash: `receipt_records` is the current projection and `receipt_transitions`
//! is its append-only audit. The validation that decides whether an incoming
//! request is a replay or a conflict is imported from `receipt`, not rewritten,
//! because a backend that judged that differently would either repeat a
//! mutation or refuse a legitimate retry.

use anyhow::{Context, Result, ensure};
use sqlx::postgres::{PgPool, PgRow};
use sqlx::{Postgres, Row, Transaction};
use uuid::Uuid;

use crate::receipt::{
    MAX_SQLITE_RECEIPT_TRANSITIONS, RECEIPT_STATE_STARTED, RECEIPT_STATE_TERMINAL, RECEIPT_VERSION,
    ReceiptRecord, ReceiptState, ReceiptTransition, bounded_json_value, validate_existing_request,
    validate_receipt_projection,
};

/// Receipt persistence and the host command queue, backed by PostgreSQL.
#[derive(Debug, Clone)]
pub struct PostgresReceiptStore {
    pool: PgPool,
}

fn decode_record(row: &PgRow) -> Result<ReceiptRecord> {
    Ok(ReceiptRecord {
        version: row.try_get("version")?,
        state: row.try_get("state")?,
        request: serde_json::from_str(row.try_get("request_json")?)
            .context("decode receipt request")?,
        response: row
            .try_get::<Option<String>, _>("response_json")?
            .map(|value| serde_json::from_str(&value).context("decode receipt response"))
            .transpose()?,
    })
}

fn decode_transition(row: &PgRow) -> Result<ReceiptTransition> {
    Ok(ReceiptTransition {
        sequence: row.try_get("sequence")?,
        version: row.try_get("version")?,
        state: row.try_get("state")?,
        request: serde_json::from_str(row.try_get("request_json")?)
            .context("decode receipt transition request")?,
        response: row
            .try_get::<Option<String>, _>("response_json")?
            .map(|value| serde_json::from_str(&value).context("decode receipt transition response"))
            .transpose()?,
    })
}

async fn insert_record(
    transaction: &mut Transaction<'_, Postgres>,
    request_id: Uuid,
    state: &str,
    request_json: &str,
    response_json: Option<&str>,
) -> Result<()> {
    let now = chrono::Utc::now().to_rfc3339();
    sqlx::query(
        "insert into receipt_records \
         (request_id, version, state, request_json, response_json, created_at, updated_at) \
         values ($1, $2, $3, $4, $5, $6, $6)",
    )
    .bind(request_id.to_string())
    .bind(i64::from(RECEIPT_VERSION))
    .bind(state)
    .bind(request_json)
    .bind(response_json)
    .bind(&now)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn append_transition(
    transaction: &mut Transaction<'_, Postgres>,
    request_id: Uuid,
    sequence: i64,
    state: &str,
    request_json: &str,
    response_json: Option<&str>,
) -> Result<()> {
    sqlx::query(
        "insert into receipt_transitions \
         (request_id, sequence, version, state, request_json, response_json, created_at) \
         values ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(request_id.to_string())
    .bind(sequence)
    .bind(i64::from(RECEIPT_VERSION))
    .bind(state)
    .bind(request_json)
    .bind(response_json)
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

impl PostgresReceiptStore {
    /// Adopt a pool whose satellite schema the session store already applied.
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn record_in_transaction(
        transaction: &mut Transaction<'_, Postgres>,
        request_id: Uuid,
    ) -> Result<Option<ReceiptRecord>> {
        // `for update` holds the receipt while its audit is validated, so two
        // hosts replaying the same mutation cannot both decide they are first.
        sqlx::query(
            "select version, state, request_json, response_json \
             from receipt_records where request_id = $1 for update",
        )
        .bind(request_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .as_ref()
        .map(decode_record)
        .transpose()
    }

    async fn validate_audit(
        transaction: &mut Transaction<'_, Postgres>,
        request_id: Uuid,
        record: &ReceiptRecord,
    ) -> Result<()> {
        let rows = sqlx::query(
            "select sequence, version, state, request_json, response_json \
             from receipt_transitions where request_id = $1 order by sequence limit $2",
        )
        .bind(request_id.to_string())
        .bind(i64::try_from(MAX_SQLITE_RECEIPT_TRANSITIONS + 1)?)
        .fetch_all(&mut **transaction)
        .await?;
        let transitions: Vec<ReceiptTransition> =
            rows.iter().map(decode_transition).collect::<Result<_>>()?;
        // The projection must be explainable by its audit: a record with no
        // matching history is corruption, not a fast path.
        validate_receipt_projection(record, &transitions)
    }

    /// Classify a receipt against the request a caller is about to perform.
    ///
    /// Returns a state rather than an error: "this record contradicts its own
    /// audit" is an answer the caller must act on (refuse the mutation), not a
    /// transport failure to retry.
    pub async fn load_value(
        &self,
        request_id: Uuid,
        request: &serde_json::Value,
    ) -> Result<ReceiptState<serde_json::Value>> {
        let request = bounded_json_value(request, "receipt request")?;
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(
            "select version, state, request_json, response_json \
             from receipt_records where request_id = $1",
        )
        .bind(request_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(row) = row else {
            // Transitions with no projection mean the record was lost while its
            // audit survived: absent and corrupt are different answers.
            let orphaned: bool = sqlx::query_scalar(
                "select exists(select 1 from receipt_transitions where request_id = $1)",
            )
            .bind(request_id.to_string())
            .fetch_one(&mut *transaction)
            .await?;
            transaction.commit().await?;
            return Ok(if orphaned {
                ReceiptState::Corrupt
            } else {
                ReceiptState::Missing
            });
        };
        let Ok(record) = decode_record(&row) else {
            transaction.commit().await?;
            return Ok(ReceiptState::Corrupt);
        };
        let rows = sqlx::query(
            "select sequence, version, state, request_json, response_json \
             from receipt_transitions where request_id = $1 order by sequence limit $2",
        )
        .bind(request_id.to_string())
        .bind(i64::try_from(MAX_SQLITE_RECEIPT_TRANSITIONS + 1)?)
        .fetch_all(&mut *transaction)
        .await?;
        let Ok(transitions) = rows
            .iter()
            .map(decode_transition)
            .collect::<Result<Vec<ReceiptTransition>>>()
        else {
            transaction.commit().await?;
            return Ok(ReceiptState::Corrupt);
        };
        if validate_receipt_projection(&record, &transitions).is_err() {
            transaction.commit().await?;
            return Ok(ReceiptState::Corrupt);
        }
        // A different request under the same id is a conflict, not a replay.
        let state = if record.version != i64::from(RECEIPT_VERSION) || record.request != request {
            ReceiptState::Conflict
        } else {
            match record.state.as_str() {
                RECEIPT_STATE_STARTED => ReceiptState::Started,
                // As in the SQLite backend: hand back the stored body and let
                // the generic wrapper decide whether it fits the caller's type.
                RECEIPT_STATE_TERMINAL => match record.response {
                    Some(response) => ReceiptState::Terminal(response),
                    None => ReceiptState::Corrupt,
                },
                _ => ReceiptState::Corrupt,
            }
        };
        transaction.commit().await?;
        Ok(state)
    }

    /// Record the intent to perform a mutation.
    ///
    /// Repeating the same intent is a no-op; repeating the id with a different
    /// request is refused, because the receipt is the mutation's identity.
    pub async fn begin_value(&self, request_id: Uuid, request: &serde_json::Value) -> Result<()> {
        let request = bounded_json_value(request, "receipt request")?;
        let request_json = serde_json::to_string(&request)?;
        let mut transaction = self.pool.begin().await?;
        let existing = Self::record_in_transaction(&mut transaction, request_id).await?;
        let Some(existing) = existing else {
            insert_record(
                &mut transaction,
                request_id,
                RECEIPT_STATE_STARTED,
                &request_json,
                None,
            )
            .await?;
            append_transition(
                &mut transaction,
                request_id,
                1,
                RECEIPT_STATE_STARTED,
                &request_json,
                None,
            )
            .await?;
            transaction.commit().await?;
            return Ok(());
        };
        validate_existing_request(&existing, &request)?;
        Self::validate_audit(&mut transaction, request_id, &existing).await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Publish the terminal response, keeping the intent transition as evidence
    /// that the mutation was authorised before it ran.
    pub async fn finish_value(
        &self,
        request_id: Uuid,
        request: &serde_json::Value,
        response: &serde_json::Value,
    ) -> Result<()> {
        let request = bounded_json_value(request, "receipt request")?;
        let response = bounded_json_value(response, "receipt response")?;
        let request_json = serde_json::to_string(&request)?;
        let response_json = serde_json::to_string(&response)?;
        let mut transaction = self.pool.begin().await?;
        let existing = Self::record_in_transaction(&mut transaction, request_id).await?;
        let Some(existing) = existing else {
            // A mutation that finished without a recorded intent still gets a
            // complete audit rather than a bare projection row.
            insert_record(
                &mut transaction,
                request_id,
                RECEIPT_STATE_TERMINAL,
                &request_json,
                Some(&response_json),
            )
            .await?;
            append_transition(
                &mut transaction,
                request_id,
                1,
                RECEIPT_STATE_TERMINAL,
                &request_json,
                Some(&response_json),
            )
            .await?;
            transaction.commit().await?;
            return Ok(());
        };
        validate_existing_request(&existing, &request)?;
        Self::validate_audit(&mut transaction, request_id, &existing).await?;
        match existing.state.as_str() {
            RECEIPT_STATE_STARTED => {
                sqlx::query(
                    "update receipt_records set state = $1, response_json = $2, \
                     updated_at = $3 where request_id = $4",
                )
                .bind(RECEIPT_STATE_TERMINAL)
                .bind(&response_json)
                .bind(chrono::Utc::now().to_rfc3339())
                .bind(request_id.to_string())
                .execute(&mut *transaction)
                .await?;
                append_transition(
                    &mut transaction,
                    request_id,
                    2,
                    RECEIPT_STATE_TERMINAL,
                    &request_json,
                    Some(&response_json),
                )
                .await?;
            }
            RECEIPT_STATE_TERMINAL => {
                // Already published. Re-finishing with a different response
                // would rewrite the outcome a caller may already have acted on.
                ensure!(
                    existing.response.as_ref() == Some(&response),
                    "receipt {request_id} already completed with a different response"
                );
            }
            state => anyhow::bail!("receipt {request_id} has unknown state {state}"),
        }
        transaction.commit().await?;
        Ok(())
    }

    /// Queue a command for a host to execute.
    pub async fn enqueue_host_operation(
        &self,
        host_id: Uuid,
        request_id: Uuid,
        command: &serde_json::Value,
    ) -> Result<()> {
        let command_json = serde_json::to_string(&bounded_json_value(command, "host operation")?)?;
        let command_json = command_json.as_str();
        let mut transaction = self.pool.begin().await?;
        let existing = sqlx::query(
            "select host_id, command_json from host_operation_queue where request_id = $1 \
             for update",
        )
        .bind(request_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(row) = existing {
            // The request id IS the command's identity, so the same id must
            // never name different work for a different host.
            ensure!(
                row.try_get::<String, _>("host_id")? == host_id.to_string()
                    && row.try_get::<String, _>("command_json")? == command_json,
                "queued host operation {request_id} already names a different command"
            );
            transaction.commit().await?;
            return Ok(());
        }
        sqlx::query(
            "insert into host_operation_queue (request_id, host_id, command_json) \
             values ($1, $2, $3)",
        )
        .bind(request_id.to_string())
        .bind(host_id.to_string())
        .bind(command_json)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// The oldest live command for a host, skipping quarantined rows.
    pub async fn next_host_operation(
        &self,
        host_id: Uuid,
    ) -> Result<Option<(Uuid, serde_json::Value)>> {
        let row = sqlx::query(
            "select sequence, request_id, command_json from host_operation_queue \
             where host_id = $1 and quarantine_reason is null order by sequence limit 1",
        )
        .bind(host_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else { return Ok(None) };
        let sequence: i64 = row.try_get("sequence")?;
        let request_id: String = row.try_get("request_id")?;
        let command_json: String = row.try_get("command_json")?;
        let decoded = Uuid::parse_str(&request_id).ok().and_then(|request_id| {
            serde_json::from_str::<serde_json::Value>(&command_json)
                .ok()
                .map(|command| (request_id, command))
        });
        match decoded {
            Some(decoded) => Ok(Some(decoded)),
            None => {
                // An undecodable row would block the queue head forever, so it
                // is quarantined rather than retried.
                sqlx::query(
                    "update host_operation_queue set quarantine_reason = $1 where sequence = $2",
                )
                .bind("invalid queued operation encoding")
                .bind(sequence)
                .execute(&self.pool)
                .await?;
                Ok(None)
            }
        }
    }

    pub async fn quarantine_host_operation(&self, host_id: Uuid, request_id: Uuid) -> Result<()> {
        sqlx::query(
            "update host_operation_queue set quarantine_reason = $1 \
             where host_id = $2 and request_id = $3",
        )
        .bind("unsupported command or mismatched identity")
        .bind(host_id.to_string())
        .bind(request_id.to_string())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn queued_host_operation(
        &self,
        host_id: Uuid,
        request_id: Uuid,
    ) -> Result<Option<serde_json::Value>> {
        let json: Option<String> = sqlx::query_scalar(
            "select command_json from host_operation_queue \
             where host_id = $1 and request_id = $2 and quarantine_reason is null",
        )
        .bind(host_id.to_string())
        .bind(request_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        // An undecodable row reads as absent rather than propagating a parse
        // error to a caller that only asked whether work is queued.
        Ok(json.and_then(|json| serde_json::from_str(&json).ok()))
    }

    pub async fn finish_host_operation(&self, host_id: Uuid, request_id: Uuid) -> Result<()> {
        sqlx::query("delete from host_operation_queue where host_id = $1 and request_id = $2")
            .bind(host_id.to_string())
            .bind(request_id.to_string())
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl crate::receipt::ReceiptBackend for PostgresReceiptStore {
    async fn load_value(
        &self,
        request_id: Uuid,
        request: &serde_json::Value,
    ) -> Result<ReceiptState<serde_json::Value>> {
        Self::load_value(self, request_id, request).await
    }

    async fn begin_value(&self, request_id: Uuid, request: &serde_json::Value) -> Result<()> {
        Self::begin_value(self, request_id, request).await
    }

    async fn finish_value(
        &self,
        request_id: Uuid,
        request: &serde_json::Value,
        response: &serde_json::Value,
    ) -> Result<()> {
        Self::finish_value(self, request_id, request, response).await
    }

    async fn enqueue_host_operation(
        &self,
        host_id: Uuid,
        request_id: Uuid,
        command: &serde_json::Value,
    ) -> Result<()> {
        Self::enqueue_host_operation(self, host_id, request_id, command).await
    }

    async fn next_host_operation(
        &self,
        host_id: Uuid,
    ) -> Result<Option<(Uuid, serde_json::Value)>> {
        Self::next_host_operation(self, host_id).await
    }

    async fn quarantine_host_operation(&self, host_id: Uuid, request_id: Uuid) -> Result<()> {
        Self::quarantine_host_operation(self, host_id, request_id).await
    }

    async fn queued_host_operation(
        &self,
        host_id: Uuid,
        request_id: Uuid,
    ) -> Result<Option<serde_json::Value>> {
        Self::queued_host_operation(self, host_id, request_id).await
    }

    async fn finish_host_operation(&self, host_id: Uuid, request_id: Uuid) -> Result<()> {
        Self::finish_host_operation(self, host_id, request_id).await
    }
}
