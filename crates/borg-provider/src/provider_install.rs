//! Installation of external provider runtimes using their official scripts.
use crate::provider_bin::{InstallStrategy, Runtime, auto_install_enabled, resolve_uncached};
use anyhow::{Context, Result, bail};
use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Healed {
    AlreadyWorking,
    Installed { version: String },
}

pub async fn ensure(runtime: Runtime) -> Result<(PathBuf, Healed)> {
    let first_error = match resolve_uncached(runtime).await {
        Ok(path) => return Ok((path, Healed::AlreadyWorking)),
        Err(error) => error,
    };
    if !auto_install_enabled() {
        bail!(
            "{first_error}
Automatic installation is disabled by {}.",
            crate::provider_bin::AUTO_INSTALL_ENV
        );
    }
    let _lock = InstallLock::acquire();
    if let Ok(path) = resolve_uncached(runtime).await {
        return Ok((path, Healed::AlreadyWorking));
    }
    let version = install_for(runtime)
        .await
        .with_context(|| format!("Borg could not install {}: {first_error}", runtime.label()))?;
    let path = resolve_uncached(runtime)
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    Ok((path, Healed::Installed { version }))
}

async fn install_for(runtime: Runtime) -> Result<String> {
    match runtime.install_strategy() {
        InstallStrategy::Script { url } => {
            run_install_script(runtime, url).await?;
            let path = resolve_uncached(runtime)
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            Ok(installed_version(&path).await)
        }
    }
}

// Keep the legacy shared lock location so older Borg processes still serialize installs.
fn standalone_root() -> Option<PathBuf> {
    let home = std::env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .or_else(|| crate::provider_bin::home_directory().map(|home| home.join(".codex")))?;
    Some(home.join("packages").join("standalone"))
}

struct InstallLock {
    directory: Option<PathBuf>,
}

/// A lock older than this is assumed to belong to a process that died.
const LOCK_STALE_AFTER: Duration = Duration::from_secs(600);

impl InstallLock {
    fn acquire() -> Self {
        let Some(root) = standalone_root() else {
            return Self { directory: None };
        };
        if fs::create_dir_all(&root).is_err() {
            return Self { directory: None };
        }
        let directory = root.join("install.lock.d");

        for _ in 0..60 {
            match fs::create_dir(&directory) {
                Ok(()) => {
                    return Self {
                        directory: Some(directory),
                    };
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    if lock_is_stale(&directory) {
                        tracing::warn!("clearing a stale Codex install lock");
                        let _ = fs::remove_dir_all(&directory);
                        continue;
                    }
                    std::thread::sleep(Duration::from_secs(1));
                }
                // An unwritable directory must not block the install: proceed
                // unlocked rather than refuse to heal.
                Err(_) => return Self { directory: None },
            }
        }
        // Waited long enough. Proceed anyway; the publish step is atomic.
        Self { directory: None }
    }
}

impl Drop for InstallLock {
    fn drop(&mut self) {
        if let Some(directory) = self.directory.take() {
            let _ = fs::remove_dir_all(directory);
        }
    }
}

fn lock_is_stale(directory: &Path) -> bool {
    fs::metadata(directory)
        .and_then(|metadata| metadata.modified())
        .map(|modified| {
            modified
                .elapsed()
                .is_ok_and(|elapsed| elapsed > LOCK_STALE_AFTER)
        })
        .unwrap_or(false)
}

async fn run_install_script(runtime: Runtime, url: &str) -> Result<()> {
    eprintln!(
        "Borg: installing {} via its official installer...",
        runtime.label()
    );
    #[cfg(unix)]
    let mut command = {
        let mut command = tokio::process::Command::new("sh");
        command.arg("-c").arg(format!(
            "set -e; curl -fsSL {url} | CODEX_NON_INTERACTIVE=1 sh"
        ));
        command
    };
    #[cfg(windows)]
    let mut command = {
        let mut command = tokio::process::Command::new("powershell.exe");
        command.args([
            "-NoProfile",
            "-Command",
            &format!("$env:CODEX_NON_INTERACTIVE=1; irm {url} | iex"),
        ]);
        command
    };
    command.stdin(std::process::Stdio::null());

    let status = tokio::time::timeout(Duration::from_secs(900), command.status())
        .await
        .with_context(|| format!("the {} installer timed out", runtime.label()))?
        .with_context(|| format!("failed to run the {} installer", runtime.label()))?;
    anyhow::ensure!(
        status.success(),
        "the {} installer exited with {status}",
        runtime.label()
    );
    Ok(())
}

/// Best-effort version string for a working executable, for reporting only.
async fn installed_version(executable: &Path) -> String {
    tokio::process::Command::new(executable)
        .arg("--version")
        .output()
        .await
        .ok()
        .filter(|output| output.status.success())
        .map(|output| {
            String::from_utf8_lossy(&output.stdout)
                .split_whitespace()
                .last()
                .unwrap_or("")
                .to_string()
        })
        .filter(|version| !version.is_empty())
        .unwrap_or_else(|| "(unknown version)".to_string())
}
