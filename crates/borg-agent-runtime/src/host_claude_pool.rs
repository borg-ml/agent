//! Volatile host-wide admission for idle Claude subscription processes.

use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(super) const MAX_IDLE_POOLS: usize = 4;

#[derive(Clone)]
pub(super) struct HostClaudePoolRegistry {
    directory: PathBuf,
}

#[derive(Default, Serialize, Deserialize)]
struct RegistryState {
    sequence: u64,
    leases: Vec<LeaseRecord>,
}

#[derive(Serialize, Deserialize)]
struct LeaseRecord {
    token: Uuid,
    session_id: Uuid,
    sequence: u64,
}

pub(super) struct HostIdleLease {
    pub token: Uuid,
    path: PathBuf,
    file: Option<File>,
}

impl Drop for HostIdleLease {
    fn drop(&mut self) {
        self.file.take();
        let _ = fs::remove_file(&self.path);
    }
}

impl HostClaudePoolRegistry {
    #[cfg(not(test))]
    pub fn for_host() -> Self {
        Self::new(crate::host_paths::host_home().join("runtime/claude-idle-pools"))
    }

    pub(super) fn new(directory: PathBuf) -> Self {
        Self { directory }
    }

    pub fn register(&self, session_id: Uuid) -> Result<HostIdleLease> {
        let token = Uuid::new_v4();
        let path = self.lease_path(token);
        self.with_state(move |state| {
            let file = private_file(&path, true)?;
            file.lock()
                .with_context(|| format!("failed to lock {}", path.display()))?;
            let lease = HostIdleLease {
                token,
                path,
                file: Some(file),
            };
            state.sequence = state
                .sequence
                .checked_add(1)
                .context("Claude idle lease sequence exhausted")?;
            state.leases.push(LeaseRecord {
                token,
                session_id,
                sequence: state.sequence,
            });
            state
                .leases
                .sort_by_key(|entry| std::cmp::Reverse(entry.sequence));
            state.leases.truncate(MAX_IDLE_POOLS);
            Ok((lease, true))
        })
    }

    pub fn allowed_tokens(&self) -> Result<HashSet<Uuid>> {
        self.with_state(|state| {
            Ok((
                state.leases.iter().map(|entry| entry.token).collect(),
                false,
            ))
        })
    }

    fn with_state<T>(
        &self,
        update: impl FnOnce(&mut RegistryState) -> Result<(T, bool)>,
    ) -> Result<T> {
        self.ensure_directory()?;
        let lock_path = self.directory.join("registry.lock");
        let lock = private_file(&lock_path, false)?;
        lock.lock()
            .with_context(|| format!("failed to lock {}", lock_path.display()))?;
        let state_path = self.directory.join("registry.json");
        let mut state: RegistryState = match fs::read(&state_path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .with_context(|| format!("failed to parse {}", state_path.display()))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => RegistryState::default(),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to read {}", state_path.display()));
            }
        };
        let mut live = Vec::with_capacity(state.leases.len());
        let mut changed = false;
        for entry in state.leases.drain(..) {
            if self.lease_is_locked(entry.token)? {
                live.push(entry);
            } else {
                changed = true;
            }
        }
        state.leases = live;
        self.remove_unreferenced_leases(&state)?;
        let (result, updated) = update(&mut state)?;
        if changed || updated {
            self.write_state(&state_path, &state)?;
        }
        drop(lock);
        Ok(result)
    }

    fn remove_unreferenced_leases(&self, state: &RegistryState) -> Result<()> {
        let referenced = state
            .leases
            .iter()
            .map(|entry| entry.token)
            .collect::<HashSet<_>>();
        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("lease") {
                continue;
            }
            let Some(token) = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .and_then(|stem| Uuid::parse_str(stem).ok())
            else {
                continue;
            };
            if !referenced.contains(&token) && !self.lease_is_locked(token)? {
                let _ = fs::remove_file(&path);
            }
        }
        Ok(())
    }

    fn lease_is_locked(&self, token: Uuid) -> Result<bool> {
        let path = self.lease_path(token);
        let file = match OpenOptions::new().read(true).write(true).open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => {
                return Err(error).with_context(|| format!("failed to inspect {}", path.display()));
            }
        };
        match file.try_lock() {
            Ok(()) => Ok(false),
            Err(std::fs::TryLockError::WouldBlock) => Ok(true),
            Err(std::fs::TryLockError::Error(error)) => {
                Err(error).with_context(|| format!("failed to inspect {}", path.display()))
            }
        }
    }

    fn write_state(&self, state_path: &Path, state: &RegistryState) -> Result<()> {
        let temp_path = self
            .directory
            .join(format!("registry-{}.tmp", Uuid::new_v4()));
        let result = (|| {
            let mut file = private_file(&temp_path, true)?;
            file.write_all(&serde_json::to_vec(state)?)?;
            file.sync_all()?;
            fs::rename(&temp_path, state_path)
                .with_context(|| format!("failed to replace {}", state_path.display()))
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temp_path);
        }
        result
    }

    fn lease_path(&self, token: Uuid) -> PathBuf {
        self.directory.join(format!("{token}.lease"))
    }

    fn ensure_directory(&self) -> Result<()> {
        fs::create_dir_all(&self.directory)
            .with_context(|| format!("failed to create {}", self.directory.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&self.directory, fs::Permissions::from_mode(0o700))
                .with_context(|| format!("failed to secure {}", self.directory.display()))?;
        }
        Ok(())
    }
}

fn private_file(path: &Path, create_new: bool) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    if create_new {
        options.create_new(true);
    } else {
        options.create(true);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(path)
        .with_context(|| format!("failed to open {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::process::{Child, Command, Stdio};
    use std::time::Duration;

    struct ChildGuard(Child);

    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    #[test]
    fn independent_owners_share_the_four_newest_idle_leases() {
        let directory = tempfile::tempdir().unwrap();
        let first_owner = HostClaudePoolRegistry::new(directory.path().to_path_buf());
        let second_owner = HostClaudePoolRegistry::new(directory.path().to_path_buf());
        let first = first_owner.register(Uuid::new_v4()).unwrap();
        let mut newer = Vec::new();
        for _ in 0..MAX_IDLE_POOLS {
            newer.push(second_owner.register(Uuid::new_v4()).unwrap());
        }
        let allowed = first_owner.allowed_tokens().unwrap();
        assert!(!allowed.contains(&first.token));
        assert!(newer.iter().all(|lease| allowed.contains(&lease.token)));
        drop(newer);
        assert!(second_owner.allowed_tokens().unwrap().is_empty());
    }

    #[test]
    fn crashed_owner_lease_is_pruned_across_processes() {
        const HELPER_DIR: &str = "BORG_CLAUDE_POOL_LEASE_TEST_HELPER_DIR";
        if let Ok(directory) = std::env::var(HELPER_DIR) {
            let registry = HostClaudePoolRegistry::new(PathBuf::from(&directory));
            let lease = registry.register(Uuid::new_v4()).unwrap();
            fs::write(Path::new(&directory).join("ready"), lease.token.to_string()).unwrap();
            let _ = std::io::stdin().read(&mut [0u8; 1]);
            return;
        }

        let directory = tempfile::tempdir().unwrap();
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "agent::host_claude_pool::tests::crashed_owner_lease_is_pruned_across_processes",
                "--nocapture",
            ])
            .env(HELPER_DIR, directory.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let mut child = ChildGuard(child);
        let ready = directory.path().join("ready");
        for _ in 0..500 {
            if ready.exists() {
                break;
            }
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "lease helper exited early"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let token = Uuid::parse_str(&fs::read_to_string(&ready).unwrap()).unwrap();
        let other_owner = HostClaudePoolRegistry::new(directory.path().to_path_buf());
        assert!(other_owner.allowed_tokens().unwrap().contains(&token));

        child.0.kill().unwrap();
        child.0.wait().unwrap();
        assert!(!other_owner.allowed_tokens().unwrap().contains(&token));
        assert!(!directory.path().join(format!("{token}.lease")).exists());
    }
}
