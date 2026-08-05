use std::fs::{self, File, OpenOptions};
use std::path::PathBuf;

use anyhow::{Context, Result};

/// Exclusive ownership of a session actor.
///
/// Session state itself lives in SQLite. This file is only the process-level
/// ownership boundary that prevents two actors from driving the same session
/// concurrently.
#[derive(Debug)]
pub struct SessionWriterLease {
    _file: File,
}

impl SessionWriterLease {
    pub fn try_acquire(lock_path: impl Into<PathBuf>) -> Result<Option<Self>> {
        let lock_path = lock_path.into();
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(parent, fs::Permissions::from_mode(0o700))
                    .with_context(|| format!("failed to secure {}", parent.display()))?;
            }
        }
        let mut options = OpenOptions::new();
        options.create(true).read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options
            .open(&lock_path)
            .with_context(|| format!("failed to open session lock {}", lock_path.display()))?;
        match file.try_lock() {
            Ok(()) => Ok(Some(Self { _file: file })),
            Err(std::fs::TryLockError::WouldBlock) => Ok(None),
            Err(std::fs::TryLockError::Error(error)) => Err(error)
                .with_context(|| format!("failed to lock session {}", lock_path.display())),
        }
    }

    pub(crate) fn acquire(lock_path: impl Into<PathBuf>) -> Result<Self> {
        let lock_path = lock_path.into();
        Self::try_acquire(lock_path.clone())?.with_context(|| {
            format!(
                "session is already active in another Borg process ({})",
                lock_path.display()
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn only_one_session_actor_can_own_the_lock() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("session.lock");
        let lease = SessionWriterLease::try_acquire(&path).unwrap().unwrap();

        assert!(SessionWriterLease::try_acquire(&path).unwrap().is_none());

        drop(lease);
        assert!(SessionWriterLease::try_acquire(&path).unwrap().is_some());
    }
}
