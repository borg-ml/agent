//! Resolution, health-checking and self-healing for the external provider CLIs
//! Borg drives (`codex`, `claude`, `opencode`).
//!
//! Why this exists:
//!   - Borg used to spawn bare `Command::new("codex")` and friends, trusting the
//!     first match on `PATH`. That is fragile on a fresh machine and fails in a
//!     way users cannot diagnose.
//!   - Real example: macOS XProtect quarantined the npm `@openai/codex`
//!     package's native binary and moved it to the Bin, leaving a launcher on
//!     `PATH` that exits non-zero while a perfectly good notarized standalone
//!     build sat unused in `~/.local/bin`.
//!   - `curl -fsSL https://borg.ml/install | sh` has to be the only thing a user
//!     ever runs, so when a runtime is missing or broken Borg repairs it rather
//!     than printing homework.
//!
//! The design is deliberately table-driven. Everything that matters — probing,
//! candidate discovery, de-duplication, caching, the healing ladder, the error
//! text — is shared. A runtime contributes only three facts: what its
//! executable is called, where else to look for it, and how to install it. That
//! keeps adding the next provider a one-line change rather than a new subsystem.
//!
//! Nothing here runs eagerly. A runtime is resolved the first time Borg actually
//! needs it, so a user who never touches Codex never downloads Codex.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{Result, anyhow};
use tokio::process::Command;
use tokio::sync::Mutex;

use crate::codex_install;

/// How long a candidate gets to answer `--version`.
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// Set to `0`/`false`/`no`/`off` to forbid Borg from installing anything.
pub const AUTO_INSTALL_ENV: &str = "BORG_AUTO_INSTALL";

/// An external CLI that Borg drives as a provider backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Runtime {
    Codex,
    Claude,
    OpenCode,
    /// xAI's CLI. Grok's coding plan is only reachable through this binary:
    /// xAI publishes no Anthropic-compatible endpoint, so no other agent CLI
    /// can drive the subscription.
    Grok,
    /// Meta's CLI, for Muse Code. Like Grok, the subscription is reached
    /// through the vendor's own binary rather than a base-URL redirect.
    Muse,
    /// Moonshot's CLI. Optional for the Kimi coding plan — `claude`, `codex` and
    /// `opencode` can all drive it via a base-URL redirect — but it is the
    /// vendor's own first-party client.
    Kimi,
}

/// How a runtime is installed when it is missing or broken.
#[derive(Debug, Clone, Copy)]
pub(crate) enum InstallStrategy {
    /// Download the signed release package directly and verify its checksum
    /// before installing. Preferred: it is auditable and needs no shell.
    CodexPackage,
    /// Pipe the vendor's official installer. Used where the vendor publishes no
    /// checksummed package manifest, so their script is the canonical channel.
    Script { url: &'static str },
}

impl Runtime {
    pub const ALL: [Runtime; 6] = [
        Runtime::Codex,
        Runtime::Claude,
        Runtime::OpenCode,
        Runtime::Grok,
        Runtime::Muse,
        Runtime::Kimi,
    ];

    /// Human-facing name, also used in log and error text.
    pub fn label(self) -> &'static str {
        match self {
            Runtime::Codex => "Codex",
            Runtime::Claude => "Claude Code",
            Runtime::OpenCode => "OpenCode",
            Runtime::Grok => "Grok",
            Runtime::Muse => "Muse Code",
            Runtime::Kimi => "Kimi",
        }
    }

    /// The executable's base name, without any platform extension.
    pub fn program(self) -> &'static str {
        match self {
            Runtime::Codex => "codex",
            Runtime::Claude => "claude",
            Runtime::OpenCode => "opencode",
            Runtime::Grok => "grok",
            Runtime::Muse => "muse",
            Runtime::Kimi => "kimi",
        }
    }

    /// Environment variable that pins this runtime to an explicit path.
    pub fn pin_env(self) -> &'static str {
        match self {
            Runtime::Codex => "BORG_CODEX_BIN",
            Runtime::Claude => "BORG_CLAUDE_BIN",
            Runtime::OpenCode => "BORG_OPENCODE_BIN",
            Runtime::Grok => "BORG_GROK_BIN",
            Runtime::Muse => "BORG_MUSE_BIN",
            Runtime::Kimi => "BORG_KIMI_BIN",
        }
    }

    pub(crate) fn install_strategy(self) -> InstallStrategy {
        match self {
            Runtime::Codex => InstallStrategy::CodexPackage,
            Runtime::Claude => InstallStrategy::Script {
                url: "https://claude.ai/install.sh",
            },
            Runtime::OpenCode => InstallStrategy::Script {
                url: "https://opencode.ai/install",
            },
            Runtime::Grok => InstallStrategy::Script {
                url: "https://x.ai/cli/install.sh",
            },
            Runtime::Muse => InstallStrategy::Script {
                url: "https://dev.meta.ai/install.sh",
            },
            Runtime::Kimi => InstallStrategy::Script {
                url: "https://code.kimi.com/kimi-code/install.sh",
            },
        }
    }

    /// The executable's file name on this platform.
    pub(crate) fn executable_file_name(self) -> String {
        if cfg!(windows) {
            format!("{}.exe", self.program())
        } else {
            self.program().to_string()
        }
    }

    /// Locations this runtime's official installer uses that may not be on
    /// `PATH` in a non-login shell, a launchd job, or a GUI-spawned process.
    fn install_dirs(self) -> Vec<PathBuf> {
        let mut dirs: Vec<PathBuf> = Vec::new();
        if let Some(home) = home_directory() {
            match self {
                Runtime::Codex => {
                    dirs.push(home.join(".local").join("bin"));
                    dirs.push(
                        home.join(".codex")
                            .join("packages")
                            .join("standalone")
                            .join("current")
                            .join("bin"),
                    );
                }
                Runtime::Claude => {
                    dirs.push(home.join(".local").join("bin"));
                    dirs.push(home.join(".claude").join("local"));
                }
                Runtime::OpenCode => {
                    dirs.push(home.join(".opencode").join("bin"));
                    dirs.push(home.join(".local").join("bin"));
                }
                Runtime::Grok => {
                    dirs.push(home.join(".grok").join("bin"));
                    dirs.push(home.join(".local").join("bin"));
                }
                Runtime::Muse => {
                    dirs.push(home.join(".muse").join("bin"));
                    dirs.push(home.join(".local").join("bin"));
                }
                Runtime::Kimi => {
                    dirs.push(home.join(".kimi").join("bin"));
                    dirs.push(home.join(".local").join("bin"));
                }
            }
        }
        dirs.push(PathBuf::from("/opt/homebrew/bin"));
        dirs.push(PathBuf::from("/usr/local/bin"));
        dirs
    }
}

/// A candidate that failed, and why. Used to build an actionable error.
#[derive(Debug, Clone)]
pub(crate) struct Rejected {
    pub(crate) path: PathBuf,
    pub(crate) reason: String,
}

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

type Cache = HashMap<Runtime, Result<PathBuf, String>>;

fn cache() -> &'static Mutex<Cache> {
    static CACHE: OnceLock<Mutex<Cache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Serializes healing so two backends starting at once cannot both install.
fn heal_gate() -> &'static Mutex<()> {
    static GATE: OnceLock<Mutex<()>> = OnceLock::new();
    GATE.get_or_init(|| Mutex::new(()))
}

/// Resolve `runtime`, repairing or installing it if needed.
///
/// The result is cached per runtime, so the heal is attempted at most once per
/// process. This is the only function provider code should call.
pub async fn executable(runtime: Runtime) -> Result<PathBuf> {
    if let Some(cached) = cache().lock().await.get(&runtime) {
        return cached.clone().map_err(|message| anyhow!(message));
    }

    let _gate = heal_gate().lock().await;
    // Re-check: another task may have resolved this runtime while we queued.
    if let Some(cached) = cache().lock().await.get(&runtime) {
        return cached.clone().map_err(|message| anyhow!(message));
    }

    let resolved = codex_install::ensure(runtime)
        .await
        .map(|(path, _)| path)
        .map_err(|error| format!("{error:#}"));
    cache().lock().await.insert(runtime, resolved.clone());
    resolved.map_err(|message| anyhow!(message))
}

/// A `tokio` `Command` for a resolved runtime.
pub async fn command(runtime: Runtime) -> Result<Command> {
    Ok(Command::new(executable(runtime).await?))
}

/// Resolve the Codex CLI. Convenience wrapper over [`executable`].
pub async fn codex_executable() -> Result<PathBuf> {
    executable(Runtime::Codex).await
}

/// A `tokio` `Command` for the Codex CLI.
pub async fn codex_command() -> Result<Command> {
    command(Runtime::Codex).await
}

/// Whether Borg is permitted to install runtimes on its own.
pub fn auto_install_enabled() -> bool {
    match std::env::var(AUTO_INSTALL_ENV) {
        Ok(value) => !is_falsey(&value),
        Err(_) => true,
    }
}

fn is_falsey(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "0" | "false" | "no" | "off"
    )
}

// ---------------------------------------------------------------------------
// Resolution
// ---------------------------------------------------------------------------

/// Resolve without consulting or populating the cache, and without installing.
///
/// Used by the healing ladder between steps, and by `borg doctor` so a repair
/// can be verified in the same process that reported the fault.
pub async fn resolve_uncached(runtime: Runtime) -> Result<PathBuf, String> {
    // An explicit pin is authoritative: if it is set and broken, say so rather
    // than silently falling back to something the user did not choose.
    if let Some(value) = std::env::var_os(runtime.pin_env())
        && !value.is_empty()
    {
        let path = PathBuf::from(&value);
        return match probe_path(runtime, &path).await {
            Ok(()) => Ok(path),
            Err(reason) => Err(format!(
                "{} is set to `{}`, but it does not work: {reason}.\nUnset {} to let Borg find a working `{}`.",
                runtime.pin_env(),
                path.display(),
                runtime.pin_env(),
                runtime.program()
            )),
        };
    }

    let mut rejected: Vec<Rejected> = Vec::new();
    for path in candidates(runtime) {
        match probe_path(runtime, &path).await {
            Ok(()) => return Ok(path),
            Err(reason) => rejected.push(Rejected { path, reason }),
        }
    }
    Err(unresolved_message(runtime, &rejected))
}

/// Every plausible install of `runtime`, in preference order, de-duplicated.
///
/// `PATH` comes first so an explicitly managed install still wins, but the
/// well-known install directories are always appended as a fallback. A broken
/// entry earlier in `PATH` must never mask a working install elsewhere, which
/// is why probing decides the winner rather than ordering alone.
fn candidates(runtime: Runtime) -> Vec<PathBuf> {
    let name = runtime.executable_file_name();
    let mut out: Vec<PathBuf> = Vec::new();
    let mut push = |path: PathBuf| {
        let key = std::fs::canonicalize(&path).unwrap_or_else(|_| path.clone());
        if !out.iter().any(|existing| {
            std::fs::canonicalize(existing).unwrap_or_else(|_| existing.clone()) == key
        }) {
            out.push(path);
        }
    };

    for dir in path_dirs().into_iter().chain(runtime.install_dirs()) {
        let candidate = dir.join(&name);
        if is_executable_file(&candidate) {
            push(candidate);
        }
    }
    out
}

fn path_dirs() -> Vec<PathBuf> {
    std::env::var_os("PATH")
        .map(|value| std::env::split_paths(&value).collect())
        .unwrap_or_default()
}

pub(crate) fn home_directory() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn is_executable_file(path: &Path) -> bool {
    // `metadata` follows symlinks, so a dangling link fails here — which is
    // exactly right: a link to a deleted binary is not a candidate.
    let Ok(metadata) = std::fs::metadata(path) else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

/// Run `<candidate> --version` and require a clean exit.
///
/// This is the check that catches the gutted-launcher case: the file exists and
/// is executable, but exits non-zero because its real payload is gone.
pub(crate) async fn probe_path(runtime: Runtime, path: &Path) -> Result<(), String> {
    if !is_executable_file(path) {
        return Err("not an executable file".to_string());
    }

    let mut command = Command::new(path);
    command
        .arg("--version")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);

    let output = match tokio::time::timeout(PROBE_TIMEOUT, command.output()).await {
        Ok(Ok(output)) => output,
        Ok(Err(error)) => return Err(format!("could not be started ({error})")),
        Err(_) => {
            return Err(format!(
                "did not respond to `--version` within {}s",
                PROBE_TIMEOUT.as_secs()
            ));
        }
    };
    if output.status.success() {
        return Ok(());
    }
    let _ = runtime;
    let detail = first_meaningful_line(&output.stderr)
        .or_else(|| first_meaningful_line(&output.stdout))
        .unwrap_or_else(|| format!("exited with {}", output.status));
    Err(format!("`--version` failed: {detail}"))
}

fn first_meaningful_line(bytes: &[u8]) -> Option<String> {
    String::from_utf8_lossy(bytes)
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(|line| line.chars().take(200).collect())
}

// ---------------------------------------------------------------------------
// Diagnostics
// ---------------------------------------------------------------------------

/// Build the message a user sees when nothing works and healing is off or has
/// failed. It has to name what was tried, why each attempt failed, and what to
/// do next.
pub(crate) fn unresolved_message(runtime: Runtime, rejected: &[Rejected]) -> String {
    let program = runtime.program();
    let mut message = format!(
        "Borg could not find a working `{program}` executable ({}).\n",
        runtime.label()
    );

    if rejected.is_empty() {
        message.push_str(&format!(
            "No `{program}` was found on PATH or in the standard install locations.\n"
        ));
    } else {
        message.push_str("Checked, and none of these worked:\n");
        for entry in rejected {
            message.push_str(&format!("  {} — {}\n", entry.path.display(), entry.reason));
        }
    }

    let install_hint = match runtime.install_strategy() {
        InstallStrategy::CodexPackage => {
            "curl -fsSL https://chatgpt.com/codex/install.sh | sh".to_string()
        }
        InstallStrategy::Script { url } => format!("curl -fsSL {url} | sh"),
    };
    message.push_str(&format!(
        "\nInstall or repair {}, then run `borg doctor` to confirm:\n  {install_hint}\n",
        runtime.label()
    ));

    if runtime == Runtime::Codex
        && cfg!(target_os = "macos")
        && rejected.iter().any(is_probably_gutted_npm_shim)
    {
        message.push_str(
            "\nOne of the entries above is the npm/Homebrew global install of `@openai/codex`. \
             macOS XProtect quarantines that package's native binary on some systems, leaving \
             a launcher that cannot start. Remove it so Borg can use a working install:\n  \
             npm uninstall -g @openai/codex\n",
        );
    }

    message.push_str(&format!(
        "\nTo pin a specific build instead, set {} to its full path.\n",
        runtime.pin_env()
    ));
    message
}

fn is_probably_gutted_npm_shim(entry: &Rejected) -> bool {
    let path = entry.path.to_string_lossy();
    path.contains("node_modules") || path.contains("/homebrew/") || path.contains("/usr/local/bin/")
}

/// One-line health summary per runtime, for `borg doctor`. Read-only: this
/// never installs anything.
pub async fn diagnose(runtime: Runtime) -> Result<PathBuf, String> {
    resolve_uncached(runtime).await
}

/// Present the candidate list without probing. Exposed for diagnostics.
pub fn candidate_paths(runtime: Runtime) -> Vec<PathBuf> {
    candidates(runtime)
}

// Kept so existing call sites and tests keep a stable name for the pin env.
pub const CODEX_BIN_ENV: &str = "BORG_CODEX_BIN";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_runtime_has_distinct_identity() {
        let mut programs: Vec<&str> = Runtime::ALL.iter().map(|r| r.program()).collect();
        programs.sort_unstable();
        programs.dedup();
        assert_eq!(programs.len(), Runtime::ALL.len());

        let mut pins: Vec<&str> = Runtime::ALL.iter().map(|r| r.pin_env()).collect();
        pins.sort_unstable();
        pins.dedup();
        assert_eq!(pins.len(), Runtime::ALL.len());
    }

    #[test]
    fn codex_pin_env_matches_the_documented_constant() {
        assert_eq!(Runtime::Codex.pin_env(), CODEX_BIN_ENV);
    }

    #[test]
    fn missing_file_is_not_executable() {
        assert!(!is_executable_file(Path::new("/definitely/not/here/codex")));
    }

    #[test]
    fn directory_is_not_executable() {
        assert!(!is_executable_file(Path::new("/tmp")));
    }

    #[tokio::test]
    async fn probe_rejects_a_command_that_exits_non_zero() {
        assert!(
            probe_path(Runtime::Codex, Path::new("/usr/bin/false"))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn probe_accepts_a_command_that_exits_zero() {
        assert!(
            probe_path(Runtime::Codex, Path::new("/usr/bin/true"))
                .await
                .is_ok()
        );
    }

    #[test]
    fn unresolved_message_names_every_rejected_candidate() {
        let message = unresolved_message(
            Runtime::Codex,
            &[Rejected {
                path: PathBuf::from("/opt/homebrew/bin/codex"),
                reason: "`--version` failed: boom".to_string(),
            }],
        );
        assert!(message.contains("/opt/homebrew/bin/codex"));
        assert!(message.contains("boom"));
        assert!(message.contains("borg doctor"));
    }

    #[test]
    fn each_runtime_suggests_its_own_installer() {
        assert!(unresolved_message(Runtime::Claude, &[]).contains("claude.ai/install.sh"));
        assert!(unresolved_message(Runtime::OpenCode, &[]).contains("opencode.ai/install"));
        assert!(unresolved_message(Runtime::Codex, &[]).contains("chatgpt.com/codex/install.sh"));
        assert!(unresolved_message(Runtime::Grok, &[]).contains("x.ai/cli/install.sh"));
        assert!(unresolved_message(Runtime::Kimi, &[]).contains("code.kimi.com"));
        assert!(unresolved_message(Runtime::Muse, &[]).contains("dev.meta.ai/install.sh"));
    }

    #[test]
    fn the_npm_hint_is_codex_only() {
        let entry = Rejected {
            path: PathBuf::from("/opt/homebrew/lib/node_modules/@openai/codex/bin/codex.js"),
            reason: "`--version` failed".to_string(),
        };
        assert!(is_probably_gutted_npm_shim(&entry));
        assert!(!unresolved_message(Runtime::Claude, &[entry]).contains("npm uninstall"));
    }

    #[test]
    fn candidates_are_deduplicated() {
        for runtime in Runtime::ALL {
            let paths = candidate_paths(runtime);
            let mut seen = std::collections::HashSet::new();
            for path in &paths {
                assert!(seen.insert(path.clone()), "duplicate candidate {path:?}");
            }
        }
    }

    #[test]
    fn auto_install_parses_falsey_values() {
        for value in ["0", "false", "no", "off", "FALSE", " Off "] {
            assert!(is_falsey(value), "{value} should disable auto-install");
        }
        for value in ["1", "true", "yes", ""] {
            assert!(!is_falsey(value), "{value} should not disable auto-install");
        }
    }
}
