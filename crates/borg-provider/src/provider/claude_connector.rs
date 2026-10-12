//! Subscription capabilities and bounded account-verification observation.
//! No executable download, provider process, upstream modules, or local RPC.

use anyhow::{Context, Result};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Debug, Deserialize)]
pub(super) struct Capabilities {
    pub model: String,
    pub context_window: u64,
    pub default_output_tokens: u64,
    pub max_output_tokens: u64,
    pub thinking: bool,
    pub adaptive_thinking: bool,
    pub thinking_required: bool,
    pub fast: bool,
    pub efforts: Vec<String>,
    pub betas: Vec<String>,
}

fn failure_path(selected: Option<&Path>) -> Result<PathBuf> {
    let authority = crate::claude_subscription::AuthAuthority::resolve(selected)?;
    let home = crate::provider_bin::home_directory().context("Claude metadata home unavailable")?;
    let key = hex::encode(Sha256::digest(
        format!(
            "{}\0{:?}",
            authority.config_dir().display(),
            authority.store()
        )
        .as_bytes(),
    ));
    Ok(home
        .join(".cache/borg/native-claude")
        .join(key)
        .join("account-verification-failed"))
}

pub(super) fn recent_account_verification_failure(selected: Option<&Path>) -> Result<bool> {
    let path = failure_path(selected)?;
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return Ok(false);
    };
    if !metadata.is_file() {
        return Ok(false);
    }
    let modified = metadata.modified()?;
    let authority = crate::claude_subscription::AuthAuthority::resolve(selected)?;
    if fs::metadata(authority.credential_file())
        .and_then(|m| m.modified())
        .is_ok_and(|login| login > modified)
    {
        return Ok(false);
    }
    Ok(modified.elapsed().unwrap_or_default() < Duration::from_secs(90))
}

pub(super) fn record_account_verification(selected: Option<&Path>, success: bool) {
    let Ok(path) = failure_path(selected) else {
        return;
    };
    if success {
        let _ = fs::remove_file(path);
        return;
    }
    let Some(directory) = path.parent() else {
        return;
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
        let mut builder = fs::DirBuilder::new();
        builder.recursive(true).mode(0o700);
        if builder.create(directory).is_err() {
            return;
        }
        let Ok(meta) = fs::symlink_metadata(directory) else {
            return;
        };
        if !meta.is_dir() || meta.uid() != unsafe { libc::geteuid() } {
            return;
        }
        let _ = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path);
    }
    #[cfg(not(unix))]
    {
        let _ = (directory, path);
    }
}
