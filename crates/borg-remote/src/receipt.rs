use anyhow::{Context, Result, bail, ensure};
use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::sqlite::SqliteRow;
use sqlx::{Row, Sqlite, SqlitePool, Transaction};
use uuid::Uuid;

const RECEIPT_VERSION: u8 = 1;

#[derive(Debug)]
pub(crate) enum ReceiptState<T> {
    Missing,
    Started,
    Terminal(T),
    Conflict,
    Corrupt,
}

const RECEIPT_STATE_STARTED: &str = "started";
const RECEIPT_STATE_TERMINAL: &str = "terminal";
/// Bounds metadata and response payloads stored in one receipt row.
pub(crate) const MAX_SQLITE_RECEIPT_JSON_BYTES: usize = 1024 * 1024;
/// A receipt may have one intent transition and one terminal transition.
pub(crate) const MAX_SQLITE_RECEIPT_TRANSITIONS: usize = 2;

#[derive(Debug, Clone)]
struct SqliteReceiptRecord {
    version: i64,
    state: String,
    request: serde_json::Value,
    response: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
struct SqliteReceiptTransition {
    sequence: i64,
    version: i64,
    state: String,
    request: serde_json::Value,
    response: Option<serde_json::Value>,
}

/// SQLite-backed receipt persistence for hosts that need crash-safe mutation
/// identity without depending on the filesystem receipt implementation.
///
/// The supplied pool remains owned by the caller. `receipt_records` is the
/// current projection; `receipt_transitions` is append-only audit history.
pub(crate) struct SqliteReceiptStore {
    pool: SqlitePool,
}

impl SqliteReceiptStore {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub(crate) async fn open(pool: SqlitePool) -> Result<Self> {
        let store = Self::new(pool);
        store.ensure_schema().await?;
        Ok(store)
    }

    #[cfg(test)]
    pub(crate) fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub(crate) async fn load<Request, Response>(
        &self,
        request_id: Uuid,
        request: &Request,
    ) -> Result<ReceiptState<Response>>
    where
        Request: Serialize,
        Response: DeserializeOwned,
    {
        let request = bounded_json_value(request, "receipt request")?;
        self.ensure_schema().await?;
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(
            "select version,state,request_json,response_json \
             from receipt_records where request_id=?",
        )
        .bind(request_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(row) = row else {
            let orphaned: i64 = sqlx::query_scalar(
                "select exists(select 1 from receipt_transitions where request_id=?)",
            )
            .bind(request_id.to_string())
            .fetch_one(&mut *transaction)
            .await?;
            transaction.commit().await?;
            return Ok(if orphaned == 0 {
                ReceiptState::Missing
            } else {
                ReceiptState::Corrupt
            });
        };

        let record = match decode_receipt_record(&row) {
            Ok(record) => record,
            Err(_) => {
                transaction.commit().await?;
                return Ok(ReceiptState::Corrupt);
            }
        };
        let transitions = sqlx::query(
            "select sequence,version,state,request_json,response_json \
             from receipt_transitions where request_id=? \
             order by sequence limit ?",
        )
        .bind(request_id.to_string())
        .bind(i64::try_from(MAX_SQLITE_RECEIPT_TRANSITIONS + 1)?)
        .fetch_all(&mut *transaction)
        .await?;
        let transitions: Result<Vec<_>> =
            transitions.iter().map(decode_receipt_transition).collect();
        let transitions = match transitions {
            Ok(transitions) => transitions,
            Err(_) => {
                transaction.commit().await?;
                return Ok(ReceiptState::Corrupt);
            }
        };
        if validate_receipt_projection(&record, &transitions).is_err() {
            transaction.commit().await?;
            return Ok(ReceiptState::Corrupt);
        }
        let state = if record.version != i64::from(RECEIPT_VERSION) || record.request != request {
            ReceiptState::Conflict
        } else {
            match record.state.as_str() {
                RECEIPT_STATE_STARTED => ReceiptState::Started,
                RECEIPT_STATE_TERMINAL => match record.response {
                    Some(response) => match serde_json::from_value(response) {
                        Ok(response) => ReceiptState::Terminal(response),
                        Err(_) => ReceiptState::Corrupt,
                    },
                    None => ReceiptState::Corrupt,
                },
                _ => ReceiptState::Corrupt,
            }
        };
        transaction.commit().await?;
        Ok(state)
    }

    /// Durably records intent before a mutation may begin.
    pub(crate) async fn begin<Request: Serialize>(
        &self,
        request_id: Uuid,
        request: &Request,
    ) -> Result<()> {
        let request = bounded_json_value(request, "receipt request")?;
        let request_json = serde_json::to_string(&request)?;
        self.ensure_schema().await?;
        let mut transaction = self.begin_write().await?;
        let existing = self
            .record_in_transaction(&mut transaction, request_id)
            .await?;
        let Some(existing) = existing else {
            insert_receipt_record(
                &mut transaction,
                request_id,
                RECEIPT_STATE_STARTED,
                &request_json,
                None,
            )
            .await?;
            append_receipt_transition(
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
        validate_existing_audit(&mut transaction, request_id, &existing).await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Atomically publishes a terminal response while retaining the intent
    /// transition as evidence that the mutation was authorized.
    pub(crate) async fn finish<Request, Response>(
        &self,
        request_id: Uuid,
        request: &Request,
        response: &Response,
    ) -> Result<()>
    where
        Request: Serialize,
        Response: Serialize,
    {
        let request = bounded_json_value(request, "receipt request")?;
        let response = bounded_json_value(response, "receipt response")?;
        let request_json = serde_json::to_string(&request)?;
        let response_json = serde_json::to_string(&response)?;
        self.ensure_schema().await?;
        let mut transaction = self.begin_write().await?;
        let existing = self
            .record_in_transaction(&mut transaction, request_id)
            .await?;
        let Some(existing) = existing else {
            insert_receipt_record(
                &mut transaction,
                request_id,
                RECEIPT_STATE_TERMINAL,
                &request_json,
                Some(&response_json),
            )
            .await?;
            append_receipt_transition(
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
        validate_existing_audit(&mut transaction, request_id, &existing).await?;
        match existing.state.as_str() {
            RECEIPT_STATE_STARTED => {
                sqlx::query(
                    "update receipt_records set state=?,response_json=?,updated_at=? \
                     where request_id=?",
                )
                .bind(RECEIPT_STATE_TERMINAL)
                .bind(&response_json)
                .bind(chrono::Utc::now().to_rfc3339())
                .bind(request_id.to_string())
                .execute(&mut *transaction)
                .await?;
                append_receipt_transition(
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
                ensure!(
                    existing.response.as_ref() == Some(&response),
                    "receipt {request_id} was finished with a different response"
                );
            }
            _ => bail!("receipt {request_id} is corrupt"),
        }
        transaction.commit().await?;
        Ok(())
    }

    async fn begin_write(&self) -> Result<Transaction<'static, Sqlite>> {
        Ok(crate::SqliteSessionStore::begin_sqlite_write(&self.pool).await?)
    }

    async fn ensure_schema(&self) -> Result<()> {
        sqlx::query(
            "create table if not exists receipt_records (\
                request_id text primary key not null,\
                version integer not null,\
                state text not null,\
                request_json text not null,\
                response_json text,\
                created_at text not null,\
                updated_at text not null\
            )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "create table if not exists receipt_transitions (\
                request_id text not null,\
                sequence integer not null,\
                version integer not null,\
                state text not null,\
                request_json text not null,\
                response_json text,\
                created_at text not null,\
                primary key(request_id, sequence),\
                foreign key(request_id) references receipt_records(request_id)\
            )",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn record_in_transaction(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        request_id: Uuid,
    ) -> Result<Option<SqliteReceiptRecord>> {
        sqlx::query(
            "select version,state,request_json,response_json \
             from receipt_records where request_id=?",
        )
        .bind(request_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .map(|row| decode_receipt_record(&row))
        .transpose()
    }
}

fn bounded_json_value<T: Serialize>(value: &T, label: &str) -> Result<serde_json::Value> {
    let value = serde_json::to_value(value).with_context(|| format!("failed to encode {label}"))?;
    let bytes = serde_json::to_vec(&value)?.len();
    ensure!(
        bytes <= MAX_SQLITE_RECEIPT_JSON_BYTES,
        "{label} exceeds {MAX_SQLITE_RECEIPT_JSON_BYTES} bytes"
    );
    Ok(value)
}

fn decode_receipt_record(row: &SqliteRow) -> Result<SqliteReceiptRecord> {
    let request_json: String = row.try_get("request_json")?;
    ensure!(
        request_json.len() <= MAX_SQLITE_RECEIPT_JSON_BYTES,
        "receipt request JSON exceeds bound"
    );
    let request = serde_json::from_str(&request_json).context("invalid receipt request JSON")?;
    let response_json: Option<String> = row.try_get("response_json")?;
    if let Some(response_json) = &response_json {
        ensure!(
            response_json.len() <= MAX_SQLITE_RECEIPT_JSON_BYTES,
            "receipt response JSON exceeds bound"
        );
    }
    let response = response_json
        .map(|json| serde_json::from_str(&json).context("invalid receipt response JSON"))
        .transpose()?;
    Ok(SqliteReceiptRecord {
        version: row.try_get("version")?,
        state: row.try_get("state")?,
        request,
        response,
    })
}

fn decode_receipt_transition(row: &SqliteRow) -> Result<SqliteReceiptTransition> {
    let request_json: String = row.try_get("request_json")?;
    let response_json: Option<String> = row.try_get("response_json")?;
    ensure!(
        request_json.len() <= MAX_SQLITE_RECEIPT_JSON_BYTES,
        "receipt audit request JSON exceeds bound"
    );
    if let Some(response_json) = &response_json {
        ensure!(
            response_json.len() <= MAX_SQLITE_RECEIPT_JSON_BYTES,
            "receipt audit response JSON exceeds bound"
        );
    }
    Ok(SqliteReceiptTransition {
        sequence: row.try_get("sequence")?,
        version: row.try_get("version")?,
        state: row.try_get("state")?,
        request: serde_json::from_str(&request_json)
            .context("invalid receipt audit request JSON")?,
        response: response_json
            .map(|json| serde_json::from_str(&json).context("invalid receipt audit response JSON"))
            .transpose()?,
    })
}

fn validate_existing_request(
    existing: &SqliteReceiptRecord,
    request: &serde_json::Value,
) -> Result<()> {
    ensure!(
        existing.version == i64::from(RECEIPT_VERSION),
        "receipt version is incompatible"
    );
    ensure!(
        existing.request == *request,
        "receipt request identity conflict"
    );
    Ok(())
}

fn validate_receipt_projection(
    record: &SqliteReceiptRecord,
    transitions: &[SqliteReceiptTransition],
) -> Result<()> {
    ensure!(!transitions.is_empty(), "receipt has no audit transitions");
    ensure!(
        transitions.len() <= MAX_SQLITE_RECEIPT_TRANSITIONS,
        "receipt has too many audit transitions"
    );
    for (index, transition) in transitions.iter().enumerate() {
        ensure!(
            transition.sequence == i64::try_from(index + 1)?,
            "receipt audit sequence is not contiguous"
        );
        ensure!(
            transition.version == record.version && transition.request == record.request,
            "receipt audit identity does not match current projection"
        );
    }
    match record.state.as_str() {
        RECEIPT_STATE_STARTED => {
            ensure!(
                transitions.len() == 1,
                "started receipt has invalid audit history"
            );
            ensure!(
                transitions[0].state == RECEIPT_STATE_STARTED
                    && transitions[0].response.is_none()
                    && record.response.is_none(),
                "started receipt audit is inconsistent"
            );
        }
        RECEIPT_STATE_TERMINAL => {
            let terminal = transitions.last().expect("non-empty transitions");
            ensure!(
                terminal.state == RECEIPT_STATE_TERMINAL,
                "terminal audit is missing"
            );
            ensure!(
                terminal.response.is_some() && terminal.response == record.response,
                "terminal receipt response audit is inconsistent"
            );
            if transitions.len() == 2 {
                ensure!(
                    transitions[0].state == RECEIPT_STATE_STARTED
                        && transitions[0].response.is_none(),
                    "receipt intent audit is inconsistent"
                );
            }
        }
        _ => bail!("receipt state is invalid"),
    }
    Ok(())
}

async fn validate_existing_audit(
    transaction: &mut Transaction<'_, Sqlite>,
    request_id: Uuid,
    record: &SqliteReceiptRecord,
) -> Result<()> {
    let rows = sqlx::query(
        "select sequence,version,state,request_json,response_json \
         from receipt_transitions where request_id=? order by sequence limit ?",
    )
    .bind(request_id.to_string())
    .bind(i64::try_from(MAX_SQLITE_RECEIPT_TRANSITIONS + 1)?)
    .fetch_all(&mut **transaction)
    .await?;
    let transitions: Vec<_> = rows
        .iter()
        .map(decode_receipt_transition)
        .collect::<Result<_>>()?;
    validate_receipt_projection(record, &transitions)
}

async fn insert_receipt_record(
    transaction: &mut Transaction<'_, Sqlite>,
    request_id: Uuid,
    state: &str,
    request_json: &str,
    response_json: Option<&str>,
) -> Result<()> {
    let now = chrono::Utc::now().to_rfc3339();
    sqlx::query(
        "insert into receipt_records \
         (request_id,version,state,request_json,response_json,created_at,updated_at) \
         values(?,?,?,?,?,?,?)",
    )
    .bind(request_id.to_string())
    .bind(i64::from(RECEIPT_VERSION))
    .bind(state)
    .bind(request_json)
    .bind(response_json)
    .bind(&now)
    .bind(&now)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn append_receipt_transition(
    transaction: &mut Transaction<'_, Sqlite>,
    request_id: Uuid,
    sequence: i64,
    state: &str,
    request_json: &str,
    response_json: Option<&str>,
) -> Result<()> {
    sqlx::query(
        "insert into receipt_transitions \
         (request_id,sequence,version,state,request_json,response_json,created_at) \
         values(?,?,?,?,?,?,?)",
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use sqlx::sqlite::SqlitePoolOptions;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Response {
        value: String,
    }

    async fn sqlite_store() -> SqliteReceiptStore {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        SqliteReceiptStore::open(pool).await.unwrap()
    }

    #[tokio::test]
    async fn sqlite_receipt_started_state_survives_restart_and_terminal_replays() {
        let store = sqlite_store().await;
        let request_id = Uuid::new_v4();
        let request = serde_json::json!({"operation": "delete", "path": "work"});
        let response = Response {
            value: "accepted".to_string(),
        };

        store.begin(request_id, &request).await.unwrap();
        let reopened = SqliteReceiptStore::new(store.pool().clone());
        assert!(matches!(
            reopened
                .load::<_, Response>(request_id, &request)
                .await
                .unwrap(),
            ReceiptState::Started
        ));
        store.finish(request_id, &request, &response).await.unwrap();
        assert!(matches!(
            reopened
                .load::<_, Response>(request_id, &request)
                .await
                .unwrap(),
            ReceiptState::Terminal(replayed) if replayed == response
        ));

        let transitions: i64 =
            sqlx::query_scalar("select count(*) from receipt_transitions where request_id=?")
                .bind(request_id.to_string())
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(transitions, 2);
    }

    #[tokio::test]
    async fn sqlite_receipt_identity_conflicts_and_exact_retries_are_safe() {
        let store = sqlite_store().await;
        let request_id = Uuid::new_v4();
        let request = serde_json::json!({"operation": "move", "to": "done"});
        let different_request = serde_json::json!({"operation": "delete", "path": "done"});
        let response = Response {
            value: "accepted".to_string(),
        };

        store.begin(request_id, &request).await.unwrap();
        store.begin(request_id, &request).await.unwrap();
        assert!(matches!(
            store
                .load::<_, Response>(request_id, &different_request)
                .await
                .unwrap(),
            ReceiptState::Conflict
        ));
        assert!(store.begin(request_id, &different_request).await.is_err());

        store.finish(request_id, &request, &response).await.unwrap();
        store.finish(request_id, &request, &response).await.unwrap();
        let different_response = Response {
            value: "different".to_string(),
        };
        assert!(
            store
                .finish(request_id, &request, &different_response)
                .await
                .is_err()
        );

        let transitions: i64 =
            sqlx::query_scalar("select count(*) from receipt_transitions where request_id=?")
                .bind(request_id.to_string())
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(transitions, 2);
    }

    #[tokio::test]
    async fn sqlite_receipt_corruption_is_not_reported_as_missing() {
        let store = sqlite_store().await;
        let request_id = Uuid::new_v4();
        let request = serde_json::json!({"operation": "write"});
        store.begin(request_id, &request).await.unwrap();
        sqlx::query("update receipt_records set request_json=? where request_id=?")
            .bind("{")
            .bind(request_id.to_string())
            .execute(store.pool())
            .await
            .unwrap();

        assert!(matches!(
            store
                .load::<_, Response>(request_id, &request)
                .await
                .unwrap(),
            ReceiptState::Corrupt
        ));
        assert!(matches!(
            store
                .load::<_, Response>(Uuid::new_v4(), &request)
                .await
                .unwrap(),
            ReceiptState::Missing
        ));
    }

    #[tokio::test]
    async fn sqlite_receipt_schema_is_created_and_payloads_are_bounded() {
        let store = sqlite_store().await;
        let table_count: i64 = sqlx::query_scalar(
            "select count(*) from sqlite_master where type='table' \
             and name in ('receipt_records','receipt_transitions')",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(table_count, 2);

        let oversized = "x".repeat(MAX_SQLITE_RECEIPT_JSON_BYTES);
        assert!(store.begin(Uuid::new_v4(), &oversized).await.is_err());
    }
}
