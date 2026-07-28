use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const RECEIPT_VERSION: u8 = 1;

#[derive(Debug)]
pub(crate) enum ReceiptState<T> {
    Missing,
    Started,
    Terminal(T),
    Legacy(T),
    Conflict,
    Corrupt,
}

#[derive(Serialize, Deserialize)]
struct StartedReceipt {
    version: u8,
    request: serde_json::Value,
}

#[derive(Serialize, Deserialize)]
struct TerminalReceipt<T> {
    version: u8,
    request: serde_json::Value,
    response: T,
}

pub(crate) struct ReceiptStore {
    directory: PathBuf,
}

impl ReceiptStore {
    pub(crate) fn new(directory: PathBuf) -> Self {
        Self { directory }
    }

    pub(crate) fn load<Request, Response>(
        &self,
        request_id: Uuid,
        request: &Request,
    ) -> Result<ReceiptState<Response>>
    where
        Request: Serialize,
        Response: DeserializeOwned,
    {
        let request = serde_json::to_value(request)?;
        let terminal_path = self.terminal_path(request_id);
        if terminal_path.exists() {
            let receipt = match read_json::<TerminalReceipt<Response>>(&terminal_path) {
                Ok(receipt) => receipt,
                Err(_) => return Ok(ReceiptState::Corrupt),
            };
            if receipt.version != RECEIPT_VERSION || receipt.request != request {
                return Ok(ReceiptState::Conflict);
            }
            return Ok(ReceiptState::Terminal(receipt.response));
        }

        // Receipts written before the crash-safe state machine stored only the
        // response. They remain replayable, but cannot prove request identity.
        let legacy_path = self.legacy_path(request_id);
        if legacy_path.exists() {
            return Ok(match read_json(&legacy_path) {
                Ok(response) => ReceiptState::Legacy(response),
                Err(_) => ReceiptState::Corrupt,
            });
        }

        let started_path = self.started_path(request_id);
        if !started_path.exists() {
            return Ok(ReceiptState::Missing);
        }
        let receipt = match read_json::<StartedReceipt>(&started_path) {
            Ok(receipt) => receipt,
            Err(_) => return Ok(ReceiptState::Corrupt),
        };
        if receipt.version != RECEIPT_VERSION || receipt.request != request {
            return Ok(ReceiptState::Conflict);
        }
        Ok(ReceiptState::Started)
    }

    /// Durably records intent before a mutation may begin.
    pub(crate) fn begin<Request: Serialize>(
        &self,
        request_id: Uuid,
        request: &Request,
    ) -> Result<()> {
        secure_directory(&self.directory)?;
        let receipt = StartedReceipt {
            version: RECEIPT_VERSION,
            request: serde_json::to_value(request)?,
        };
        let bytes = serde_json::to_vec(&receipt)?;
        let path = self.started_path(request_id);
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&path)
            .with_context(|| format!("failed to create {}", path.display()))?;
        file.write_all(&bytes)
            .with_context(|| format!("failed to write {}", path.display()))?;
        file.sync_all()
            .with_context(|| format!("failed to sync {}", path.display()))?;
        sync_directory(&self.directory)?;
        Ok(())
    }

    /// Atomically publishes the terminal response. The started marker remains
    /// as evidence that execution was authorized before the mutation.
    pub(crate) fn finish<Request, Response>(
        &self,
        request_id: Uuid,
        request: &Request,
        response: &Response,
    ) -> Result<()>
    where
        Request: Serialize,
        Response: Serialize,
    {
        secure_directory(&self.directory)?;
        let receipt = TerminalReceipt {
            version: RECEIPT_VERSION,
            request: serde_json::to_value(request)?,
            response,
        };
        atomic_write_secure(
            &self.terminal_path(request_id),
            &serde_json::to_vec(&receipt)?,
        )
    }

    fn started_path(&self, request_id: Uuid) -> PathBuf {
        self.directory.join(format!("{request_id}.started.json"))
    }

    fn terminal_path(&self, request_id: Uuid) -> PathBuf {
        self.directory.join(format!("{request_id}.terminal.json"))
    }

    fn legacy_path(&self, request_id: Uuid) -> PathBuf {
        self.directory.join(format!("{request_id}.json"))
    }
}

pub(crate) fn atomic_write_secure(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .context("atomic file path must have a parent directory")?;
    secure_directory(parent)?;
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("borg"),
        Uuid::new_v4()
    ));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&temporary)
        .with_context(|| format!("failed to create {}", temporary.display()))?;
    if let Err(error) = file.write_all(bytes).and_then(|_| file.sync_all()) {
        let _ = fs::remove_file(&temporary);
        return Err(error).with_context(|| format!("failed to sync {}", temporary.display()));
    }
    if let Err(error) = fs::rename(&temporary, path) {
        let _ = fs::remove_file(&temporary);
        return Err(error).with_context(|| format!("failed to publish {}", path.display()));
    }
    sync_directory(parent)
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    serde_json::from_slice(
        &fs::read(path).with_context(|| format!("failed to read {}", path.display()))?,
    )
    .with_context(|| format!("invalid receipt {}", path.display()))
}

fn secure_directory(path: &Path) -> Result<()> {
    fs::create_dir_all(path).with_context(|| format!("failed to create {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .with_context(|| format!("failed to secure {}", path.display()))?;
    }
    Ok(())
}

fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        File::open(path)
            .with_context(|| format!("failed to open {}", path.display()))?
            .sync_all()
            .with_context(|| format!("failed to sync {}", path.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use tempfile::tempdir;

    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Response {
        value: String,
    }

    #[test]
    fn started_mutation_is_indeterminate_after_restart() {
        let root = tempdir().unwrap();
        let store = ReceiptStore::new(root.path().join("receipts"));
        let request_id = Uuid::new_v4();
        let request = serde_json::json!({"operation": "delete", "path": "work"});

        store.begin(request_id, &request).unwrap();

        let reopened = ReceiptStore::new(root.path().join("receipts"));
        assert!(matches!(
            reopened.load::<_, Response>(request_id, &request).unwrap(),
            ReceiptState::Started
        ));
    }

    #[test]
    fn terminal_response_replays_and_request_id_reuse_is_rejected() {
        let root = tempdir().unwrap();
        let store = ReceiptStore::new(root.path().join("receipts"));
        let request_id = Uuid::new_v4();
        let request = serde_json::json!({"operation": "move", "to": "done"});
        let response = Response {
            value: "accepted".to_string(),
        };
        store.begin(request_id, &request).unwrap();
        store.finish(request_id, &request, &response).unwrap();

        assert!(matches!(
            store
                .load::<_, Response>(request_id, &request)
                .unwrap(),
            ReceiptState::Terminal(replayed) if replayed == response
        ));
        assert!(matches!(
            store
                .load::<_, Response>(
                    request_id,
                    &serde_json::json!({"operation": "delete", "path": "done"})
                )
                .unwrap(),
            ReceiptState::Conflict
        ));
    }

    #[test]
    fn corrupt_started_marker_never_becomes_missing() {
        let root = tempdir().unwrap();
        let store = ReceiptStore::new(root.path().join("receipts"));
        let request_id = Uuid::new_v4();
        secure_directory(&store.directory).unwrap();
        fs::write(store.started_path(request_id), b"{").unwrap();

        assert!(matches!(
            store
                .load::<_, Response>(request_id, &serde_json::json!({"operation": "write"}))
                .unwrap(),
            ReceiptState::Corrupt
        ));
    }

    #[test]
    fn atomic_metadata_publish_replaces_only_with_complete_bytes() {
        let root = tempdir().unwrap();
        let path = root.path().join("session.launch.json");
        atomic_write_secure(&path, br#"{"request":"first"}"#).unwrap();
        atomic_write_secure(&path, br#"{"request":"second"}"#).unwrap();

        let value: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(value, serde_json::json!({"request": "second"}));
        assert!(fs::read_dir(root.path()).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp")
        }));
    }
}
