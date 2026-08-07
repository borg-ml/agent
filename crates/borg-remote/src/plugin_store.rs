//! Extension-scoped durable state and artifact receipts.
//!
//! Plugins may keep high-rate or ecosystem-specific files in their workspace,
//! but correctness-critical metadata comes through this host-owned boundary.
//! The database is the authority for revisions, idempotency, provenance, and
//! artifact hashes; plugin code never receives a SQLite handle.

use std::path::Path;

use anyhow::{Context, Result, bail, ensure};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{Row, Sqlite, SqlitePool, Transaction};
use tokio::io::AsyncReadExt;
use uuid::Uuid;

use crate::session_store::SqliteSessionStore;

const MAX_EXTENSION_ID_BYTES: usize = 64;
const MAX_SCOPE_BYTES: usize = 16;
const MAX_KEY_BYTES: usize = 256;
const MAX_PREFIX_BYTES: usize = 256;
const MAX_IDEMPOTENCY_KEY_BYTES: usize = 256;
const MAX_PLUGIN_VALUE_BYTES: usize = 512 * 1024;
const MAX_PLUGIN_BATCH_ITEMS: usize = 64;
const MAX_PLUGIN_METADATA_BYTES: usize = 32 * 1024;
const MAX_ARTIFACT_ID_BYTES: usize = 256;
const MAX_ARTIFACT_NAME_BYTES: usize = 256;
const MAX_ARTIFACT_PATH_BYTES: usize = 4096;
const MAX_ARTIFACT_MEDIA_TYPE_BYTES: usize = 128;
const MAX_ARTIFACT_BYTES: u64 = 64 * 1024 * 1024;
const MAX_LIST_ITEMS: usize = 200;
const DELETED_CONTENT_HASH: &str = "sha256:deleted";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PluginScope {
    Session,
    Project,
}

impl PluginScope {
    fn parse(value: Option<&str>) -> Result<Self> {
        match value.unwrap_or("session") {
            "session" => Ok(Self::Session),
            "project" => Ok(Self::Project),
            other => bail!("plugin storage scope must be `session` or `project`, got `{other}`"),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::Project => "project",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum PluginWrite {
    Put {
        key: String,
        value: Value,
        #[serde(default)]
        expected_revision: Option<u64>,
    },
    Delete {
        key: String,
        #[serde(default)]
        expected_revision: Option<u64>,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ArtifactInput {
    artifact_id: String,
    path: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    run_id: Option<String>,
    #[serde(default)]
    media_type: Option<String>,
    #[serde(default = "empty_object")]
    metadata: Value,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct PluginCall {
    #[serde(default)]
    extension_id: Option<String>,
    #[serde(default)]
    scope: Option<String>,
    op: String,
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    prefix: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    idempotency_key: Option<String>,
    #[serde(default)]
    writes: Vec<PluginWrite>,
    #[serde(default)]
    artifacts: Vec<ArtifactInput>,
    #[serde(default)]
    artifact_id: Option<String>,
    #[serde(default = "empty_object")]
    provenance: Value,
}

#[derive(Debug, Clone, Serialize)]
struct PluginStateEntry {
    extension_id: String,
    scope: String,
    scope_id: String,
    key: String,
    value: Option<Value>,
    revision: u64,
    content_hash: String,
    provenance: Value,
    updated_at: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct PluginArtifactReceipt {
    extension_id: String,
    scope: String,
    scope_id: String,
    artifact_id: String,
    path: String,
    name: Option<String>,
    run_id: Option<String>,
    media_type: Option<String>,
    byte_len: u64,
    content_hash: String,
    metadata: Value,
    provenance: Value,
    created_at: String,
}

#[derive(Debug, Clone, Serialize)]
struct PreparedArtifact {
    input: ArtifactInput,
    byte_len: u64,
    content_hash: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct CommitResult {
    extension_id: String,
    scope: String,
    scope_id: String,
    idempotency_key: String,
    request_hash: String,
    replayed: bool,
    writes: Vec<Value>,
    artifacts: Vec<PluginArtifactReceipt>,
}

#[derive(Debug, Clone)]
pub(crate) struct SqlitePluginStore {
    pool: SqlitePool,
}

pub(crate) async fn ensure_schema(pool: &SqlitePool) -> Result<()> {
    sqlx::raw_sql(
        r#"
        create table if not exists plugin_state (
            extension_id text not null,
            scope text not null,
            scope_id text not null,
            key text not null,
            value_json text,
            deleted integer not null default 0,
            content_hash text not null,
            revision integer not null,
            provenance_json text not null,
            created_at text not null,
            updated_at text not null,
            primary key (extension_id, scope, scope_id, key)
        );

        create index if not exists idx_plugin_state_scope
            on plugin_state (extension_id, scope, scope_id, deleted, key);

        create table if not exists plugin_artifacts (
            extension_id text not null,
            scope text not null,
            scope_id text not null,
            artifact_id text not null,
            path text not null,
            name text,
            run_id text,
            media_type text,
            byte_len integer not null,
            content_hash text not null,
            metadata_json text not null,
            provenance_json text not null,
            created_at text not null,
            primary key (extension_id, scope, scope_id, artifact_id)
        );

        create index if not exists idx_plugin_artifacts_run
            on plugin_artifacts (extension_id, scope, scope_id, run_id, created_at);

        create table if not exists plugin_mutation_receipts (
            extension_id text not null,
            scope text not null,
            scope_id text not null,
            idempotency_key text not null,
            request_hash text not null,
            result_json text not null,
            created_at text not null,
            primary key (extension_id, scope, scope_id, idempotency_key)
        );
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

impl SqlitePluginStore {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    async fn existing_commit(
        &self,
        extension_id: &str,
        scope: PluginScope,
        scope_id: &str,
        idempotency_key: &str,
        request_hash: &str,
    ) -> Result<Option<CommitResult>> {
        let row = sqlx::query(
            "select request_hash,result_json from plugin_mutation_receipts \
             where extension_id=? and scope=? and scope_id=? and idempotency_key=?",
        )
        .bind(extension_id)
        .bind(scope.label())
        .bind(scope_id)
        .bind(idempotency_key)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let existing_hash: String = row.try_get("request_hash")?;
        ensure!(
            existing_hash == request_hash,
            "plugin storage idempotency key was reused with different content"
        );
        let result_json: String = row.try_get("result_json")?;
        let mut result: CommitResult = serde_json::from_str(&result_json)
            .context("stored plugin commit receipt is invalid")?;
        result.replayed = true;
        Ok(Some(result))
    }

    pub(crate) async fn call(
        &self,
        session_id: Uuid,
        root: &Path,
        default_extension_id: Option<&str>,
        arguments: Value,
    ) -> Result<Value> {
        ensure_schema(&self.pool).await?;
        let request: PluginCall = serde_json::from_value(arguments)?;
        let raw_extension_id = request.extension_id.as_deref().or(default_extension_id);
        let extension_id = raw_extension_id.context("plugin storage extension_id is required")?;
        validate_extension_id(extension_id)?;
        if let Some(default) = default_extension_id {
            ensure!(
                request.extension_id.is_none() || request.extension_id.as_deref() == Some(default),
                "plugin storage extension_id does not match the active extension"
            );
        }
        validate_plugin_call(&request)?;
        let scope = PluginScope::parse(request.scope.as_deref())?;
        let scope_id = scope_id(scope, session_id);
        match request.op.as_str() {
            "get" => {
                let key = request
                    .key
                    .as_deref()
                    .context("plugin storage key is required")?;
                let entry = self.get_entry(extension_id, scope, &scope_id, key).await?;
                Ok(json!({"entry": entry}))
            }
            "list" => {
                let entries = self
                    .list_entries(
                        extension_id,
                        scope,
                        &scope_id,
                        request.prefix.as_deref(),
                        request.limit.unwrap_or(MAX_LIST_ITEMS),
                    )
                    .await?;
                Ok(json!({"entries": entries}))
            }
            "commit" => {
                let idempotency_key = request
                    .idempotency_key
                    .as_deref()
                    .context("plugin storage commit idempotency_key is required")?;
                let request_hash = mutation_request_hash(
                    extension_id,
                    scope,
                    &scope_id,
                    idempotency_key,
                    &request.writes,
                    &request.artifacts,
                    &request.provenance,
                )?;
                if let Some(result) = self
                    .existing_commit(
                        extension_id,
                        scope,
                        &scope_id,
                        idempotency_key,
                        &request_hash,
                    )
                    .await?
                {
                    return Ok(serde_json::to_value(result)?);
                }
                let prepared = self
                    .prepare_artifacts(root, &request.artifacts)
                    .await
                    .context("prepare plugin artifact receipts")?;
                Ok(serde_json::to_value(
                    self.commit(
                        extension_id,
                        scope,
                        &scope_id,
                        idempotency_key,
                        &request_hash,
                        &request.writes,
                        &prepared,
                        &request.provenance,
                    )
                    .await?,
                )?)
            }
            "verify_artifact" => {
                let artifact_id = request
                    .artifact_id
                    .as_deref()
                    .context("plugin artifact_id is required")?;
                Ok(self
                    .verify_artifact(extension_id, scope, &scope_id, artifact_id, root)
                    .await?)
            }
            other => bail!("unknown plugin storage operation `{other}`"),
        }
    }

    async fn get_entry(
        &self,
        extension_id: &str,
        scope: PluginScope,
        scope_id: &str,
        key: &str,
    ) -> Result<Option<PluginStateEntry>> {
        validate_key(key, "plugin storage key")?;
        let row = sqlx::query(
            "select value_json,deleted,content_hash,revision,provenance_json,updated_at \
             from plugin_state where extension_id=? and scope=? and scope_id=? and key=?",
        )
        .bind(extension_id)
        .bind(scope.label())
        .bind(scope_id)
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| decode_state_entry(row, extension_id, scope, scope_id, key))
            .transpose()
    }

    async fn list_entries(
        &self,
        extension_id: &str,
        scope: PluginScope,
        scope_id: &str,
        prefix: Option<&str>,
        limit: usize,
    ) -> Result<Vec<PluginStateEntry>> {
        if let Some(prefix) = prefix {
            ensure!(
                prefix.len() <= MAX_PREFIX_BYTES,
                "plugin storage prefix is too long"
            );
        }
        let limit = limit.clamp(1, MAX_LIST_ITEMS);
        let rows = if let Some(prefix) = prefix {
            let pattern = format!("{}%", escape_like_prefix(prefix));
            sqlx::query(
                "select key,value_json,deleted,content_hash,revision,provenance_json,updated_at \
                 from plugin_state where extension_id=? and scope=? and scope_id=? and deleted=0 \
                 and key like ? escape '\\' order by key limit ?",
            )
            .bind(extension_id)
            .bind(scope.label())
            .bind(scope_id)
            .bind(pattern)
            .bind(i64::try_from(limit)?)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                "select key,value_json,deleted,content_hash,revision,provenance_json,updated_at \
                 from plugin_state where extension_id=? and scope=? and scope_id=? and deleted=0 \
                 order by key limit ?",
            )
            .bind(extension_id)
            .bind(scope.label())
            .bind(scope_id)
            .bind(i64::try_from(limit)?)
            .fetch_all(&self.pool)
            .await?
        };
        rows.into_iter()
            .map(|row| {
                let key: String = row.try_get("key")?;
                decode_state_entry(row, extension_id, scope, scope_id, &key)
            })
            .collect()
    }

    async fn prepare_artifacts(
        &self,
        root: &Path,
        inputs: &[ArtifactInput],
    ) -> Result<Vec<PreparedArtifact>> {
        let mut prepared = Vec::with_capacity(inputs.len());
        for input in inputs {
            let path =
                crate::filesystem::resolve_existing_workspace_path(root, Path::new(&input.path))?;
            let metadata = tokio::fs::metadata(&path).await?;
            ensure!(
                metadata.is_file(),
                "plugin artifact path is not a regular file"
            );
            ensure!(
                metadata.len() <= MAX_ARTIFACT_BYTES,
                "plugin artifact exceeds {MAX_ARTIFACT_BYTES} bytes"
            );
            let (byte_len, content_hash) = hash_file(&path).await?;
            prepared.push(PreparedArtifact {
                input: input.clone(),
                byte_len,
                content_hash,
            });
        }
        Ok(prepared)
    }

    async fn commit(
        &self,
        extension_id: &str,
        scope: PluginScope,
        scope_id: &str,
        idempotency_key: &str,
        request_hash: &str,
        writes: &[PluginWrite],
        artifacts: &[PreparedArtifact],
        provenance: &Value,
    ) -> Result<CommitResult> {
        validate_idempotency_key(idempotency_key)?;
        validate_metadata(provenance)?;
        let mut transaction = SqliteSessionStore::begin_sqlite_write(&self.pool).await?;
        if let Some(existing) = sqlx::query(
            "select request_hash,result_json from plugin_mutation_receipts \
             where extension_id=? and scope=? and scope_id=? and idempotency_key=?",
        )
        .bind(extension_id)
        .bind(scope.label())
        .bind(scope_id)
        .bind(idempotency_key)
        .fetch_optional(&mut *transaction)
        .await?
        {
            let existing_hash: String = existing.try_get("request_hash")?;
            ensure!(
                existing_hash == request_hash,
                "plugin storage idempotency key was reused with different content"
            );
            let result_json: String = existing.try_get("result_json")?;
            let mut result: CommitResult = serde_json::from_str(&result_json)
                .context("stored plugin commit receipt is invalid")?;
            result.replayed = true;
            transaction.commit().await?;
            return Ok(result);
        }

        let mut write_results = Vec::with_capacity(writes.len());
        for write in writes {
            write_results.push(
                apply_write(
                    &mut transaction,
                    extension_id,
                    scope,
                    scope_id,
                    write,
                    provenance,
                )
                .await?,
            );
        }
        let mut artifact_results = Vec::with_capacity(artifacts.len());
        for artifact in artifacts {
            artifact_results.push(
                record_artifact(
                    &mut transaction,
                    extension_id,
                    scope,
                    scope_id,
                    artifact,
                    provenance,
                )
                .await?,
            );
        }
        let result = CommitResult {
            extension_id: extension_id.to_string(),
            scope: scope.label().to_string(),
            scope_id: scope_id.to_string(),
            idempotency_key: idempotency_key.to_string(),
            request_hash: request_hash.to_string(),
            replayed: false,
            writes: write_results,
            artifacts: artifact_results,
        };
        sqlx::query(
            "insert into plugin_mutation_receipts \
             (extension_id,scope,scope_id,idempotency_key,request_hash,result_json,created_at) \
             values(?,?,?,?,?,?,?)",
        )
        .bind(extension_id)
        .bind(scope.label())
        .bind(scope_id)
        .bind(idempotency_key)
        .bind(request_hash)
        .bind(serde_json::to_string(&result)?)
        .bind(Utc::now().to_rfc3339())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(result)
    }

    async fn verify_artifact(
        &self,
        extension_id: &str,
        scope: PluginScope,
        scope_id: &str,
        artifact_id: &str,
        root: &Path,
    ) -> Result<Value> {
        validate_text(artifact_id, MAX_ARTIFACT_ID_BYTES, "artifact_id")?;
        let row = sqlx::query(
            "select path,byte_len,content_hash from plugin_artifacts \
             where extension_id=? and scope=? and scope_id=? and artifact_id=?",
        )
        .bind(extension_id)
        .bind(scope.label())
        .bind(scope_id)
        .bind(artifact_id)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(json!({"artifact_id": artifact_id, "found": false, "valid": false}));
        };
        let path: String = row.try_get("path")?;
        let expected_len: i64 = row.try_get("byte_len")?;
        let expected_hash: String = row.try_get("content_hash")?;
        let result = match crate::filesystem::resolve_existing_workspace_path(
            root,
            Path::new(&path),
        ) {
            Ok(path) => match hash_file(&path).await {
                Ok((byte_len, content_hash)) => json!({
                    "artifact_id": artifact_id,
                    "found": true,
                    "valid": i64::try_from(byte_len)? == expected_len && content_hash == expected_hash,
                    "path": path,
                    "byte_len": byte_len,
                    "content_hash": content_hash,
                    "expected_byte_len": expected_len,
                    "expected_content_hash": expected_hash,
                }),
                Err(error) => json!({
                    "artifact_id": artifact_id,
                    "found": true,
                    "valid": false,
                    "path": path,
                    "error": error.to_string(),
                }),
            },
            Err(error) => json!({
                "artifact_id": artifact_id,
                "found": true,
                "valid": false,
                "path": path,
                "error": error.to_string(),
            }),
        };
        Ok(result)
    }
}

async fn apply_write(
    transaction: &mut Transaction<'_, Sqlite>,
    extension_id: &str,
    scope: PluginScope,
    scope_id: &str,
    write: &PluginWrite,
    provenance: &Value,
) -> Result<Value> {
    let (key, value, expected_revision) = match write {
        PluginWrite::Put {
            key,
            value,
            expected_revision,
        } => {
            validate_key(key, "plugin storage key")?;
            validate_value(value)?;
            (key, Some(value), *expected_revision)
        }
        PluginWrite::Delete {
            key,
            expected_revision,
        } => {
            validate_key(key, "plugin storage key")?;
            (key, None, *expected_revision)
        }
    };
    let existing = sqlx::query(
        "select revision,deleted,content_hash from plugin_state \
         where extension_id=? and scope=? and scope_id=? and key=?",
    )
    .bind(extension_id)
    .bind(scope.label())
    .bind(scope_id)
    .bind(key)
    .fetch_optional(&mut **transaction)
    .await?;
    let current_revision = existing
        .as_ref()
        .map(|row| row.try_get::<i64, _>("revision"))
        .transpose()?
        .unwrap_or(0);
    ensure!(current_revision >= 0, "plugin storage revision is negative");
    if let Some(expected_revision) = expected_revision {
        ensure!(
            i64::try_from(expected_revision)? == current_revision,
            "plugin storage revision conflict for key `{key}`: expected {expected_revision}, current {current_revision}"
        );
    }
    let next_revision = current_revision + 1;
    let now = Utc::now().to_rfc3339();
    let value_json = value.map(serde_json::to_string).transpose()?;
    let content_hash = match &value_json {
        Some(value_json) => format!("sha256:{:x}", Sha256::digest(value_json.as_bytes())),
        None => DELETED_CONTENT_HASH.to_string(),
    };
    let provenance_json = serde_json::to_string(provenance)?;
    if existing.is_some() {
        sqlx::query(
            "update plugin_state set value_json=?,deleted=?,content_hash=?,revision=?, \
             provenance_json=?,updated_at=? where extension_id=? and scope=? and scope_id=? and key=?",
        )
        .bind(&value_json)
        .bind(i64::from(value.is_none()))
        .bind(&content_hash)
        .bind(next_revision)
        .bind(&provenance_json)
        .bind(&now)
        .bind(extension_id)
        .bind(scope.label())
        .bind(scope_id)
        .bind(key)
        .execute(&mut **transaction)
        .await?;
    } else {
        sqlx::query(
            "insert into plugin_state \
             (extension_id,scope,scope_id,key,value_json,deleted,content_hash,revision,provenance_json,created_at,updated_at) \
             values(?,?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(extension_id)
        .bind(scope.label())
        .bind(scope_id)
        .bind(key)
        .bind(&value_json)
        .bind(i64::from(value.is_none()))
        .bind(&content_hash)
        .bind(next_revision)
        .bind(&provenance_json)
        .bind(&now)
        .bind(&now)
        .execute(&mut **transaction)
        .await?;
    }
    Ok(json!({
        "key": key,
        "deleted": value.is_none(),
        "revision": next_revision,
        "content_hash": content_hash,
    }))
}

async fn record_artifact(
    transaction: &mut Transaction<'_, Sqlite>,
    extension_id: &str,
    scope: PluginScope,
    scope_id: &str,
    prepared: &PreparedArtifact,
    provenance: &Value,
) -> Result<PluginArtifactReceipt> {
    let input = &prepared.input;
    let now = Utc::now().to_rfc3339();
    let receipt = PluginArtifactReceipt {
        extension_id: extension_id.to_string(),
        scope: scope.label().to_string(),
        scope_id: scope_id.to_string(),
        artifact_id: input.artifact_id.clone(),
        path: input.path.clone(),
        name: input.name.clone(),
        run_id: input.run_id.clone(),
        media_type: input.media_type.clone(),
        byte_len: prepared.byte_len,
        content_hash: prepared.content_hash.clone(),
        metadata: input.metadata.clone(),
        provenance: provenance.clone(),
        created_at: now,
    };
    let existing = sqlx::query(
        "select path,name,run_id,media_type,byte_len,content_hash,metadata_json,provenance_json,created_at \
         from plugin_artifacts where extension_id=? and scope=? and scope_id=? and artifact_id=?",
    )
    .bind(extension_id)
    .bind(scope.label())
    .bind(scope_id)
    .bind(&input.artifact_id)
    .fetch_optional(&mut **transaction)
    .await?;
    if let Some(existing) = existing {
        let existing_receipt =
            decode_artifact(existing, extension_id, scope, scope_id, &input.artifact_id)?;
        ensure!(
            existing_receipt.path == receipt.path
                && existing_receipt.name == receipt.name
                && existing_receipt.run_id == receipt.run_id
                && existing_receipt.media_type == receipt.media_type
                && existing_receipt.byte_len == receipt.byte_len
                && existing_receipt.content_hash == receipt.content_hash
                && existing_receipt.metadata == receipt.metadata
                && existing_receipt.provenance == receipt.provenance,
            "plugin artifact_id `{}` was reused with different content",
            input.artifact_id
        );
        return Ok(existing_receipt);
    }
    sqlx::query(
        "insert into plugin_artifacts \
         (extension_id,scope,scope_id,artifact_id,path,name,run_id,media_type,byte_len,content_hash,metadata_json,provenance_json,created_at) \
         values(?,?,?,?,?,?,?,?,?,?,?,?,?)",
    )
    .bind(extension_id)
    .bind(scope.label())
    .bind(scope_id)
    .bind(&receipt.artifact_id)
    .bind(&receipt.path)
    .bind(&receipt.name)
    .bind(&receipt.run_id)
    .bind(&receipt.media_type)
    .bind(i64::try_from(receipt.byte_len)?)
    .bind(&receipt.content_hash)
    .bind(serde_json::to_string(&receipt.metadata)?)
    .bind(serde_json::to_string(&receipt.provenance)?)
    .bind(&receipt.created_at)
    .execute(&mut **transaction)
    .await?;
    Ok(receipt)
}

fn decode_state_entry(
    row: sqlx::sqlite::SqliteRow,
    extension_id: &str,
    scope: PluginScope,
    scope_id: &str,
    key: &str,
) -> Result<PluginStateEntry> {
    let deleted: i64 = row.try_get("deleted")?;
    let value_json: Option<String> = row.try_get("value_json")?;
    let value = if deleted == 0 {
        Some(
            serde_json::from_str(
                value_json
                    .as_deref()
                    .context("plugin state value is missing")?,
            )
            .context("plugin state value is invalid JSON")?,
        )
    } else {
        None
    };
    Ok(PluginStateEntry {
        extension_id: extension_id.to_string(),
        scope: scope.label().to_string(),
        scope_id: scope_id.to_string(),
        key: key.to_string(),
        value,
        revision: u64::try_from(row.try_get::<i64, _>("revision")?)?,
        content_hash: row.try_get("content_hash")?,
        provenance: serde_json::from_str(&row.try_get::<String, _>("provenance_json")?)?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn decode_artifact(
    row: sqlx::sqlite::SqliteRow,
    extension_id: &str,
    scope: PluginScope,
    scope_id: &str,
    artifact_id: &str,
) -> Result<PluginArtifactReceipt> {
    Ok(PluginArtifactReceipt {
        extension_id: extension_id.to_string(),
        scope: scope.label().to_string(),
        scope_id: scope_id.to_string(),
        artifact_id: artifact_id.to_string(),
        path: row.try_get("path")?,
        name: row.try_get("name")?,
        run_id: row.try_get("run_id")?,
        media_type: row.try_get("media_type")?,
        byte_len: u64::try_from(row.try_get::<i64, _>("byte_len")?)?,
        content_hash: row.try_get("content_hash")?,
        metadata: serde_json::from_str(&row.try_get::<String, _>("metadata_json")?)?,
        provenance: serde_json::from_str(&row.try_get::<String, _>("provenance_json")?)?,
        created_at: row.try_get("created_at")?,
    })
}

async fn hash_file(path: &Path) -> Result<(u64, String)> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut bytes = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        bytes = bytes
            .checked_add(u64::try_from(read)?)
            .context("plugin artifact size overflow")?;
        ensure!(
            bytes <= MAX_ARTIFACT_BYTES,
            "plugin artifact exceeds {MAX_ARTIFACT_BYTES} bytes"
        );
        hasher.update(&buffer[..read]);
    }
    Ok((bytes, format!("sha256:{:x}", hasher.finalize())))
}

fn validate_plugin_call(request: &PluginCall) -> Result<()> {
    ensure!(
        request.op.len() <= MAX_SCOPE_BYTES,
        "plugin storage operation is too long"
    );
    if let Some(key) = &request.key {
        validate_key(key, "plugin storage key")?;
    }
    if let Some(prefix) = &request.prefix {
        ensure!(
            prefix.len() <= MAX_PREFIX_BYTES,
            "plugin storage prefix is too long"
        );
    }
    ensure!(
        request.writes.len() <= MAX_PLUGIN_BATCH_ITEMS,
        "too many plugin state writes"
    );
    ensure!(
        request.artifacts.len() <= MAX_PLUGIN_BATCH_ITEMS,
        "too many plugin artifacts"
    );
    for write in &request.writes {
        match write {
            PluginWrite::Put { value, .. } => validate_value(value)?,
            PluginWrite::Delete { .. } => {}
        }
    }
    let mut artifact_ids = std::collections::HashSet::new();
    for artifact in &request.artifacts {
        validate_text(&artifact.artifact_id, MAX_ARTIFACT_ID_BYTES, "artifact_id")?;
        ensure!(
            artifact_ids.insert(&artifact.artifact_id),
            "duplicate plugin artifact_id"
        );
        validate_text(&artifact.path, MAX_ARTIFACT_PATH_BYTES, "artifact path")?;
        ensure!(
            !Path::new(&artifact.path).is_absolute(),
            "plugin artifact path must be relative"
        );
        if let Some(name) = &artifact.name {
            validate_text(name, MAX_ARTIFACT_NAME_BYTES, "artifact name")?;
        }
        if let Some(run_id) = &artifact.run_id {
            validate_text(run_id, MAX_ARTIFACT_ID_BYTES, "artifact run_id")?;
        }
        if let Some(media_type) = &artifact.media_type {
            validate_text(
                media_type,
                MAX_ARTIFACT_MEDIA_TYPE_BYTES,
                "artifact media_type",
            )?;
        }
        validate_metadata(&artifact.metadata)?;
    }
    validate_metadata(&request.provenance)
}

fn mutation_request_hash(
    extension_id: &str,
    scope: PluginScope,
    scope_id: &str,
    idempotency_key: &str,
    writes: &[PluginWrite],
    artifacts: &[ArtifactInput],
    provenance: &Value,
) -> Result<String> {
    let request = json!({
        "extension_id": extension_id,
        "scope": scope.label(),
        "scope_id": scope_id,
        "idempotency_key": idempotency_key,
        "writes": writes,
        "artifacts": artifacts,
        "provenance": provenance,
    });
    Ok(format!(
        "sha256:{:x}",
        Sha256::digest(serde_json::to_vec(&request)?)
    ))
}

fn escape_like_prefix(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn validate_extension_id(value: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= MAX_EXTENSION_ID_BYTES
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')),
        "invalid plugin extension_id"
    );
    Ok(())
}

fn validate_key(value: &str, label: &str) -> Result<()> {
    validate_text(value, MAX_KEY_BYTES, label)?;
    ensure!(
        !value.starts_with('/'),
        "{label} must be namespaced and relative"
    );
    ensure!(
        !value
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == ".."),
        "{label} contains an invalid path component"
    );
    Ok(())
}

fn validate_idempotency_key(value: &str) -> Result<()> {
    validate_text(value, MAX_IDEMPOTENCY_KEY_BYTES, "idempotency_key")
}

fn validate_value(value: &Value) -> Result<()> {
    ensure!(
        serde_json::to_vec(value)?.len() <= MAX_PLUGIN_VALUE_BYTES,
        "plugin storage value exceeds {MAX_PLUGIN_VALUE_BYTES} bytes"
    );
    Ok(())
}

fn validate_metadata(value: &Value) -> Result<()> {
    ensure!(value.is_object(), "plugin metadata must be a JSON object");
    ensure!(
        serde_json::to_vec(value)?.len() <= MAX_PLUGIN_METADATA_BYTES,
        "plugin metadata exceeds {MAX_PLUGIN_METADATA_BYTES} bytes"
    );
    Ok(())
}

fn validate_text(value: &str, max_bytes: usize, label: &str) -> Result<()> {
    ensure!(!value.trim().is_empty(), "{label} is empty");
    ensure!(
        value.len() <= max_bytes,
        "{label} exceeds {max_bytes} bytes"
    );
    Ok(())
}

fn scope_id(scope: PluginScope, session_id: Uuid) -> String {
    match scope {
        PluginScope::Session => session_id.to_string(),
        PluginScope::Project => "project".to_string(),
    }
}

fn empty_object() -> Value {
    json!({})
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use sqlx::sqlite::SqlitePoolOptions;
    use tempfile::tempdir;

    use super::*;

    async fn store() -> (SqlitePluginStore, tempfile::TempDir) {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        ensure_schema(&pool).await.unwrap();
        (SqlitePluginStore::new(pool), tempdir().unwrap())
    }

    #[tokio::test]
    async fn commit_is_atomic_cas_protected_and_replayable() {
        let (store, root) = store().await;
        let session_id = Uuid::new_v4();
        let first = store
            .call(
                session_id,
                root.path(),
                None,
                json!({
                    "extension_id": "harvey-lab",
                    "op": "commit",
                    "idempotency_key": "run-1-admit",
                    "writes": [{"op": "put", "key": "runs/run-1", "value": {"state": "queued"}}],
                    "provenance": {"workflow_id": "wf-1"}
                }),
            )
            .await
            .unwrap();
        assert_eq!(first["writes"][0]["revision"], 1);
        let replay = store
            .call(
                session_id,
                root.path(),
                None,
                json!({
                    "extension_id": "harvey-lab",
                    "op": "commit",
                    "idempotency_key": "run-1-admit",
                    "writes": [{"op": "put", "key": "runs/run-1", "value": {"state": "queued"}}],
                    "provenance": {"workflow_id": "wf-1"}
                }),
            )
            .await
            .unwrap();
        assert_eq!(replay["replayed"], true);
        assert!(store
            .call(
                session_id,
                root.path(),
                None,
                json!({
                    "extension_id": "harvey-lab",
                    "op": "commit",
                    "idempotency_key": "run-1-finish",
                    "writes": [
                        {"op": "put", "key": "runs/run-1", "value": {"state": "done"}, "expected_revision": 99},
                        {"op": "put", "key": "runs/run-1/score", "value": {"all_pass": true}}
                    ],
                    "provenance": {}
                }),
            )
            .await
            .is_err());
        let state = store
            .call(
                session_id,
                root.path(),
                None,
                json!({
                    "extension_id": "harvey-lab",
                    "op": "get",
                    "key": "runs/run-1"
                }),
            )
            .await
            .unwrap();
        assert_eq!(state["entry"]["value"]["state"], "queued");
        assert!(
            store
                .call(
                    session_id,
                    root.path(),
                    None,
                    json!({
                        "extension_id": "harvey-lab",
                        "op": "get",
                        "key": "runs/run-1/score"
                    }),
                )
                .await
                .unwrap()["entry"]
                .is_null()
        );
    }

    #[tokio::test]
    async fn artifact_receipts_hash_and_verify_workspace_files() {
        let (store, root) = store().await;
        tokio::fs::write(root.path().join("scores.json"), br#"{"score":1}"#)
            .await
            .unwrap();
        let session_id = Uuid::new_v4();
        let result = store
            .call(
                session_id,
                root.path(),
                Some("harvey-lab"),
                json!({
                    "op": "commit",
                    "idempotency_key": "run-1-score",
                    "artifacts": [{
                        "artifact_id": "run-1-scores",
                        "path": "scores.json",
                        "run_id": "run-1",
                        "media_type": "application/json",
                        "metadata": {"kind": "score"}
                    }],
                    "provenance": {"task_id": "task-1"}
                }),
            )
            .await
            .unwrap();
        assert_eq!(result["artifacts"][0]["byte_len"], 11);
        let verified = store
            .call(
                session_id,
                root.path(),
                Some("harvey-lab"),
                json!({"op": "verify_artifact", "artifact_id": "run-1-scores"}),
            )
            .await
            .unwrap();
        assert_eq!(verified["valid"], true);
        tokio::fs::write(root.path().join("scores.json"), br#"{"score":0}"#)
            .await
            .unwrap();
        let changed = store
            .call(
                session_id,
                root.path(),
                Some("harvey-lab"),
                json!({"op": "verify_artifact", "artifact_id": "run-1-scores"}),
            )
            .await
            .unwrap();
        assert_eq!(changed["valid"], false);
        let replay = store
            .call(
                session_id,
                root.path(),
                Some("harvey-lab"),
                json!({
                    "op": "commit",
                    "idempotency_key": "run-1-score",
                    "artifacts": [{
                        "artifact_id": "run-1-scores",
                        "path": "scores.json",
                        "run_id": "run-1",
                        "media_type": "application/json",
                        "metadata": {"kind": "score"}
                    }],
                    "provenance": {"task_id": "task-1"}
                }),
            )
            .await
            .unwrap();
        assert_eq!(replay["replayed"], true);
    }

    #[tokio::test]
    async fn prefix_listing_applies_before_limit() {
        let (store, root) = store().await;
        let session_id = Uuid::new_v4();
        store
            .call(
                session_id,
                root.path(),
                None,
                json!({
                    "extension_id": "harvey-lab",
                    "op": "commit",
                    "idempotency_key": "prefix-list",
                    "writes": [
                        {"op": "put", "key": "a/other", "value": 1},
                        {"op": "put", "key": "runs/one", "value": 2}
                    ],
                    "provenance": {}
                }),
            )
            .await
            .unwrap();
        let listed = store
            .call(
                session_id,
                root.path(),
                None,
                json!({
                    "extension_id": "harvey-lab",
                    "op": "list",
                    "prefix": "runs/",
                    "limit": 1
                }),
            )
            .await
            .unwrap();
        assert_eq!(listed["entries"].as_array().unwrap().len(), 1);
        assert_eq!(listed["entries"][0]["key"], "runs/one");
    }

    #[test]
    fn plugin_keys_cannot_escape_their_namespace() {
        assert!(validate_key("runs/run-1", "key").is_ok());
        assert!(validate_key("../escape", "key").is_err());
        assert!(validate_key("runs//bad", "key").is_err());
        assert!(PathBuf::from("/absolute").is_absolute());
    }
}
