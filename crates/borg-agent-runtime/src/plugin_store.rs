//! Extension-scoped durable state and artifact receipts.
//!
//! Plugins may keep high-rate or ecosystem-specific files in their workspace,
//! but correctness-critical metadata comes through this host-owned boundary.
//! The database is the authority for revisions, idempotency, provenance, and
//! artifact hashes; plugin code never receives a database handle.

use std::path::Path;

use anyhow::{Context, Result, bail, ensure};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::Row;
use tokio::io::AsyncReadExt;
use uuid::Uuid;

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
pub enum PluginScope {
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
pub enum PluginWrite {
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
pub struct ArtifactInput {
    pub(crate) artifact_id: String,
    pub(crate) path: String,
    #[serde(default)]
    pub(crate) name: Option<String>,
    #[serde(default)]
    pub(crate) run_id: Option<String>,
    #[serde(default)]
    pub(crate) media_type: Option<String>,
    #[serde(default = "empty_object")]
    pub(crate) metadata: Value,
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
pub struct PluginStateEntry {
    pub(crate) extension_id: String,
    pub(crate) scope: String,
    pub(crate) scope_id: String,
    pub(crate) key: String,
    pub(crate) value: Option<Value>,
    pub(crate) revision: u64,
    pub(crate) content_hash: String,
    pub(crate) provenance: Value,
    pub(crate) updated_at: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PluginArtifactReceipt {
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
pub struct PreparedArtifact {
    pub(crate) input: ArtifactInput,
    pub(crate) byte_len: u64,
    pub(crate) content_hash: String,
}

pub struct CommitScope<'a> {
    pub(crate) extension_id: &'a str,
    pub(crate) scope: PluginScope,
    pub(crate) scope_id: &'a str,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CommitResult {
    pub(crate) extension_id: String,
    pub(crate) scope: String,
    pub(crate) scope_id: String,
    pub(crate) idempotency_key: String,
    pub(crate) request_hash: String,
    pub(crate) replayed: bool,
    pub(crate) writes: Vec<Value>,
    pub(crate) artifacts: Vec<PluginArtifactReceipt>,
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
    Ok((bytes, format!("sha256:{}", hex::encode(hasher.finalize()))))
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
        "sha256:{}",
        hex::encode(Sha256::digest(serde_json::to_vec(&request)?))
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

/// The five database operations plugin storage actually needs.
///
/// Everything else in this module -- request parsing, validation, hashing,
/// artifact preparation from the filesystem -- is backend-agnostic. Isolating
/// the storage operations behind a trait is what lets a second engine reuse all
/// of that rather than reimplementing the rules that decide whether a mutation
/// is a replay, a conflict, or a revision violation.
#[async_trait::async_trait]
pub trait PluginBackend: Send + Sync {
    async fn get_entry(
        &self,
        extension_id: &str,
        scope: PluginScope,
        scope_id: &str,
        key: &str,
    ) -> Result<Option<PluginStateEntry>>;

    async fn list_entries(
        &self,
        extension_id: &str,
        scope: PluginScope,
        scope_id: &str,
        prefix: Option<&str>,
        limit: usize,
    ) -> Result<Vec<PluginStateEntry>>;

    async fn existing_commit(
        &self,
        extension_id: &str,
        scope: PluginScope,
        scope_id: &str,
        idempotency_key: &str,
        request_hash: &str,
    ) -> Result<Option<CommitResult>>;

    async fn commit(
        &self,
        commit_scope: CommitScope<'_>,
        idempotency_key: &str,
        request_hash: &str,
        writes: &[PluginWrite],
        artifacts: &[PreparedArtifact],
        provenance: &Value,
    ) -> Result<CommitResult>;

    async fn verify_artifact(
        &self,
        extension_id: &str,
        scope: PluginScope,
        scope_id: &str,
        artifact_id: &str,
        root: &Path,
    ) -> Result<Value>;
}

/// Plugin storage on PostgreSQL.
///
/// A child module so it can use the private request and receipt vocabulary
/// above without widening any of it. Only the five storage operations live
/// here; parsing, validation, hashing and artifact preparation are shared
/// with the module above.
pub mod postgres {
    use super::*;
    use sqlx::postgres::{PgPool, PgRow};
    use sqlx::{Postgres, Transaction};

    pub struct PostgresPluginStore {
        pool: PgPool,
    }

    impl PostgresPluginStore {
        /// Adopt a pool whose satellite schema the session store already applied.
        pub fn new(pool: PgPool) -> Self {
            Self { pool }
        }
    }

    fn decode_entry(
        row: &PgRow,
        extension_id: &str,
        scope: PluginScope,
        scope_id: &str,
        key: &str,
    ) -> Result<PluginStateEntry> {
        let deleted: bool = row.try_get("deleted")?;
        let value_json: Option<String> = row.try_get("value_json")?;
        // A live row must carry its value; a tombstone must not.
        let value = if deleted {
            None
        } else {
            Some(
                serde_json::from_str(
                    value_json
                        .as_deref()
                        .context("plugin state value is missing")?,
                )
                .context("plugin state value is invalid JSON")?,
            )
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

    /// Apply one write under compare-and-set.
    ///
    /// `for update` is required rather than decorative: without it two
    /// concurrent commits would both read revision N, both write N+1, and the
    /// caller's `expected_revision` guarantee would be silently broken.
    async fn apply_write(
        transaction: &mut Transaction<'_, Postgres>,
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
            } => (key, Some(value), *expected_revision),
            PluginWrite::Delete {
                key,
                expected_revision,
            } => (key, None, *expected_revision),
        };
        let existing = sqlx::query(
            "select revision, deleted, content_hash from plugin_state \
             where extension_id = $1 and scope = $2 and scope_id = $3 and key = $4 for update",
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
            Some(value_json) => format!(
                "sha256:{}",
                hex::encode(Sha256::digest(value_json.as_bytes())),
            ),
            None => DELETED_CONTENT_HASH.to_string(),
        };
        let provenance_json = serde_json::to_string(provenance)?;
        sqlx::query(
            "insert into plugin_state \
             (extension_id, scope, scope_id, key, value_json, deleted, content_hash, revision, \
              provenance_json, created_at, updated_at) \
             values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10) \
             on conflict (extension_id, scope, scope_id, key) do update set \
             value_json = excluded.value_json, deleted = excluded.deleted, \
             content_hash = excluded.content_hash, revision = excluded.revision, \
             provenance_json = excluded.provenance_json, updated_at = excluded.updated_at",
        )
        .bind(extension_id)
        .bind(scope.label())
        .bind(scope_id)
        .bind(key)
        .bind(&value_json)
        .bind(value.is_none())
        .bind(&content_hash)
        .bind(next_revision)
        .bind(&provenance_json)
        .bind(&now)
        .execute(&mut **transaction)
        .await?;
        Ok(json!({
            "key": key,
            "revision": next_revision,
            "content_hash": content_hash,
            "deleted": value.is_none(),
        }))
    }

    async fn record_artifact(
        transaction: &mut Transaction<'_, Postgres>,
        extension_id: &str,
        scope: PluginScope,
        scope_id: &str,
        prepared: &PreparedArtifact,
        provenance: &Value,
    ) -> Result<PluginArtifactReceipt> {
        let input = &prepared.input;
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
            created_at: Utc::now().to_rfc3339(),
        };
        // An artifact id is write-once: re-publishing identical content is a
        // replay, but changing what an id points at would rewrite evidence.
        let existing = sqlx::query(
            "select path, name, run_id, media_type, byte_len, content_hash, metadata_json, \
                    provenance_json, created_at from plugin_artifacts \
             where extension_id = $1 and scope = $2 and scope_id = $3 and artifact_id = $4 \
             for update",
        )
        .bind(extension_id)
        .bind(scope.label())
        .bind(scope_id)
        .bind(&receipt.artifact_id)
        .fetch_optional(&mut **transaction)
        .await?;
        if let Some(row) = existing {
            let existing_path: String = row.try_get("path")?;
            let existing_len: i64 = row.try_get("byte_len")?;
            let existing_hash: String = row.try_get("content_hash")?;
            ensure!(
                existing_path == receipt.path
                    && u64::try_from(existing_len)? == receipt.byte_len
                    && existing_hash == receipt.content_hash,
                "plugin artifact {} already exists with different content",
                receipt.artifact_id
            );
            return Ok(receipt);
        }
        sqlx::query(
            "insert into plugin_artifacts \
             (extension_id, scope, scope_id, artifact_id, path, name, run_id, media_type, \
              byte_len, content_hash, metadata_json, provenance_json, created_at) \
             values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
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

    #[async_trait::async_trait]
    impl PluginBackend for PostgresPluginStore {
        async fn get_entry(
            &self,
            extension_id: &str,
            scope: PluginScope,
            scope_id: &str,
            key: &str,
        ) -> Result<Option<PluginStateEntry>> {
            let row = sqlx::query(
                "select value_json, deleted, content_hash, revision, provenance_json, updated_at \
                 from plugin_state \
                 where extension_id = $1 and scope = $2 and scope_id = $3 and key = $4",
            )
            .bind(extension_id)
            .bind(scope.label())
            .bind(scope_id)
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;
            row.as_ref()
                .map(|row| decode_entry(row, extension_id, scope, scope_id, key))
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
            let rows = if let Some(prefix) = prefix {
                ensure!(
                    prefix.len() <= MAX_PREFIX_BYTES,
                    "plugin storage prefix exceeds {MAX_PREFIX_BYTES} bytes"
                );
                // `like` with an escaped prefix: a caller's `%` must match a
                // literal percent, not every key in the scope.
                let pattern = format!(
                    "{}%",
                    prefix
                        .replace('\\', "\\\\")
                        .replace('%', "\\%")
                        .replace('_', "\\_")
                );
                sqlx::query(
                    "select key, value_json, deleted, content_hash, revision, provenance_json, \
                            updated_at from plugin_state \
                     where extension_id = $1 and scope = $2 and scope_id = $3 \
                       and not deleted and key like $4 escape '\\' \
                     order by key limit $5",
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
                    "select key, value_json, deleted, content_hash, revision, provenance_json, \
                            updated_at from plugin_state \
                     where extension_id = $1 and scope = $2 and scope_id = $3 and not deleted \
                     order by key limit $4",
                )
                .bind(extension_id)
                .bind(scope.label())
                .bind(scope_id)
                .bind(i64::try_from(limit)?)
                .fetch_all(&self.pool)
                .await?
            };
            rows.iter()
                .map(|row| {
                    let key: String = row.try_get("key")?;
                    decode_entry(row, extension_id, scope, scope_id, &key)
                })
                .collect()
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
                "select request_hash, result_json from plugin_mutation_receipts \
                 where extension_id = $1 and scope = $2 and scope_id = $3 \
                   and idempotency_key = $4",
            )
            .bind(extension_id)
            .bind(scope.label())
            .bind(scope_id)
            .bind(idempotency_key)
            .fetch_optional(&self.pool)
            .await?;
            let Some(row) = row else { return Ok(None) };
            ensure!(
                row.try_get::<String, _>("request_hash")? == request_hash,
                "plugin storage idempotency key was reused with different content"
            );
            let mut result: CommitResult = serde_json::from_str(row.try_get("result_json")?)
                .context("stored plugin commit receipt is invalid")?;
            result.replayed = true;
            Ok(Some(result))
        }

        async fn commit(
            &self,
            commit_scope: CommitScope<'_>,
            idempotency_key: &str,
            request_hash: &str,
            writes: &[PluginWrite],
            artifacts: &[PreparedArtifact],
            provenance: &Value,
        ) -> Result<CommitResult> {
            let CommitScope {
                extension_id,
                scope,
                scope_id,
            } = commit_scope;
            let mut transaction = self.pool.begin().await?;
            // Re-check the receipt inside the transaction: two callers racing
            // the same idempotency key must not both apply the writes.
            let existing = sqlx::query(
                "select request_hash, result_json from plugin_mutation_receipts \
                 where extension_id = $1 and scope = $2 and scope_id = $3 \
                   and idempotency_key = $4 for update",
            )
            .bind(extension_id)
            .bind(scope.label())
            .bind(scope_id)
            .bind(idempotency_key)
            .fetch_optional(&mut *transaction)
            .await?;
            if let Some(existing) = existing {
                ensure!(
                    existing.try_get::<String, _>("request_hash")? == request_hash,
                    "plugin storage idempotency key was reused with different content"
                );
                let mut result: CommitResult =
                    serde_json::from_str(existing.try_get("result_json")?)
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
                 (extension_id, scope, scope_id, idempotency_key, request_hash, result_json, \
                  created_at) values ($1, $2, $3, $4, $5, $6, $7)",
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
            let row = sqlx::query(
                "select path, byte_len, content_hash from plugin_artifacts \
                 where extension_id = $1 and scope = $2 and scope_id = $3 and artifact_id = $4",
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
            // A recorded artifact whose file no longer matches is reported as
            // invalid rather than erroring: that is the question being asked.
            let result =
                match crate::filesystem::resolve_existing_workspace_path(root, Path::new(&path)) {
                    Ok(path) => match hash_file(&path).await {
                        Ok((byte_len, content_hash)) => json!({
                            "artifact_id": artifact_id,
                            "found": true,
                            "valid": i64::try_from(byte_len)? == expected_len
                                && content_hash == expected_hash,
                        }),
                        Err(_) => {
                            json!({"artifact_id": artifact_id, "found": true, "valid": false})
                        }
                    },
                    Err(_) => json!({"artifact_id": artifact_id, "found": true, "valid": false}),
                };
            Ok(result)
        }
    }
}

/// Execute one plugin-storage operation against any backend.
///
/// Free function rather than a method because everything here -- request
/// validation, scope resolution, idempotency hashing, artifact preparation --
/// is backend-agnostic policy. Only the five `PluginBackend` calls it makes
/// touch storage. Keeping the policy in one place is what stops a second
/// engine from quietly reimplementing the rules that decide whether a mutation
/// is a replay, a conflict, or a revision violation.
pub(crate) async fn call(
    backend: &dyn PluginBackend,
    session_id: Uuid,
    root: &Path,
    default_extension_id: Option<&str>,
    arguments: Value,
) -> Result<Value> {
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
            let entry = backend
                .get_entry(extension_id, scope, &scope_id, key)
                .await?;
            Ok(json!({"entry": entry}))
        }
        "list" => {
            let entries = backend
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
            if let Some(result) = backend
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
            let prepared = prepare_artifacts(root, &request.artifacts)
                .await
                .context("prepare plugin artifact receipts")?;
            Ok(serde_json::to_value(
                backend
                    .commit(
                        CommitScope {
                            extension_id,
                            scope,
                            scope_id: &scope_id,
                        },
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
            Ok(backend
                .verify_artifact(extension_id, scope, &scope_id, artifact_id, root)
                .await?)
        }
        other => bail!("unknown plugin storage operation `{other}`"),
    }
}

/// Hash and stat every artifact a commit references, before any storage is
/// touched, so a commit either records all of them or none.
async fn prepare_artifacts(root: &Path, inputs: &[ArtifactInput]) -> Result<Vec<PreparedArtifact>> {
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
