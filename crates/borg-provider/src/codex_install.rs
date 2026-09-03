//! Self-healing installation of the external `codex` runtime.
//!
//! Why this exists:
//!   - `curl -fsSL https://borg.ml/install | sh` has to be the only thing a
//!     user ever runs. If Borg's Codex backend is missing or broken, Borg
//!     repairs it itself rather than printing homework.
//!   - The failure modes are varied and none of them are the user's fault:
//!     Codex was never installed; macOS XProtect quarantined the npm package's
//!     native binary and left a launcher that cannot start; a release directory
//!     survived but its `current` symlink dangles; `~/.local/bin/codex` points
//!     at something deleted. Each of those has a different, cheap repair.
//!
//! The ladder, cheapest first, stopping at the first thing that works:
//!   1. Probe every candidate on disk (see [`crate::provider_bin`]).
//!   2. Offline repair: an intact release directory is already present, only
//!      the symlinks that publish it are wrong. Relink and re-probe.
//!   3. Download the official `codex-package` archive from GitHub, verify it
//!      against the published `SHA256SUMS` manifest, and install it into the
//!      same layout Codex's own updater uses so `codex update` keeps working.
//!
//! Installing into `~/.codex/packages/standalone` deliberately mirrors the
//! upstream layout (`codex-package.json` declares `layoutVersion`, `version`,
//! `target` and `entrypoint`). We are a peer of the official installer, not a
//! competing one.

use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::Client;
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::provider_bin::{
    InstallStrategy, Runtime, auto_install_enabled, probe_path, resolve_uncached,
};

/// Upstream repository publishing the Codex releases. Used as the fallback
/// metadata source; the GitHub API rate-limits unauthenticated callers to 60
/// requests an hour, which is not a foundation for an auto-installer.
const REPOSITORY: &str = "openai/codex";

/// Primary release metadata, and what the official installer prefers.
const RELEASES_CHANNEL_URL: &str = "https://releases.openai.com/codex/channels/latest";

/// The official installer, used as the last-resort fallback.
pub const INSTALL_SCRIPT_URL: &str = "https://chatgpt.com/codex/install.sh";

/// Manifest listing the SHA-256 of every `codex-package-*` asset.
const CHECKSUM_ASSET: &str = "codex-package_SHA256SUMS";

/// Layout version of `codex-package.json` this installer understands.
const SUPPORTED_LAYOUT_VERSION: u32 = 1;

/// The package archives are ~110 MiB; leave generous headroom but stay bounded.
const MAX_DOWNLOAD_BYTES: u64 = 512 * 1024 * 1024;

/// Set to `0`/`false`/`no` to forbid Borg from installing Codex on its own.
pub const AUTO_INSTALL_ENV: &str = "BORG_CODEX_AUTO_INSTALL";

/// Overrides the release metadata endpoint. Exists for tests.
pub const RELEASE_API_ENV: &str = "BORG_CODEX_RELEASE_API_URL";

#[derive(Debug, Deserialize)]
struct GithubRelease {
    tag_name: String,
    assets: Vec<GithubAsset>,
}

/// Both metadata sources share this shape. `releases.openai.com` additionally
/// carries a per-asset `digest` (`sha256:<hex>`), which lets us verify without
/// a second round trip for the manifest.
#[derive(Debug, Deserialize)]
struct GithubAsset {
    name: String,
    browser_download_url: String,
    #[serde(default)]
    digest: Option<String>,
}

/// What [`ensure`] had to do. Reported so callers can tell the user
/// something honest about a multi-second pause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Healed {
    /// A working `codex` was already installed and wired up.
    AlreadyWorking,
    /// An intact release directory was present; only its symlinks were wrong.
    Relinked,
    /// A fresh package was downloaded and installed.
    Installed { version: String },
}

/// Resolve `codex`, repairing or installing it if needed.
///
/// Returns the executable path and what had to be done to get it.
pub async fn ensure(runtime: Runtime) -> Result<(PathBuf, Healed)> {
    let first_error = match resolve_uncached(runtime).await {
        Ok(path) => return Ok((path, Healed::AlreadyWorking)),
        Err(error) => error,
    };

    // Step 2: the binary may already be on disk, just not published correctly.
    // This is free, works offline, and covers the common XProtect aftermath
    // where the PATH copy died but a good release directory survived.
    if runtime == Runtime::Codex
        && let Some(path) = repair_symlinks(runtime).await
    {
        return Ok((path, Healed::Relinked));
    }

    if !auto_install_enabled() {
        bail!(
            "{first_error}\nAutomatic installation is disabled by {}. \
             Unset it to let Borg repair this itself.",
            crate::provider_bin::AUTO_INSTALL_ENV
        );
    }

    // Step 3: install from upstream, holding the same lock the official
    // installer uses so the two can never interleave on one machine.
    let _lock = InstallLock::acquire();

    // Another process may have finished installing while we waited.
    if let Ok(path) = resolve_uncached(runtime).await {
        return Ok((path, Healed::AlreadyWorking));
    }

    let version = match install_for(runtime).await {
        Ok(version) => version,
        Err(direct) => {
            // Step 4: last resort. If our own download path fails for a reason
            // we did not anticipate — an upstream layout change, a proxy that
            // mangles the API — defer to the official installer, which is the
            // one thing guaranteed to track upstream.
            tracing::warn!(error = %format!("{direct:#}"), "direct Codex install failed; trying the official installer");
            match run_install_script(runtime, INSTALL_SCRIPT_URL).await {
                Ok(()) => match resolve_uncached(runtime).await {
                    Ok(path) => {
                        let version = installed_version(&path).await;
                        return Ok((path, Healed::Installed { version }));
                    }
                    Err(error) => bail!(
                        "Borg ran the official Codex installer, but Codex still does not run: {error}"
                    ),
                },
                Err(script) => bail!(
                    "Borg tried to install Codex automatically and could not.\n\
                     Direct install: {direct:#}\n\
                     Official installer: {script:#}\n\n{first_error}"
                ),
            }
        }
    };

    match resolve_uncached(runtime).await {
        Ok(path) => Ok((path, Healed::Installed { version })),
        Err(error) => bail!("Borg installed Codex {version}, but it still does not run: {error}"),
    }
}

// ---------------------------------------------------------------------------
// Step 2: offline repair
// ---------------------------------------------------------------------------

/// Find an already-installed release whose `bin/codex` actually runs, and
/// republish it through `current` and `~/.local/bin`.
///
/// Returns the working executable path if a repair succeeded.
async fn repair_symlinks(runtime: Runtime) -> Option<PathBuf> {
    let releases = standalone_root()?.join("releases");
    let mut candidates: Vec<PathBuf> = fs::read_dir(&releases)
        .ok()?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect();

    // Newest name last in lexical order is a decent proxy for newest version;
    // probing protects us if the guess is wrong.
    candidates.sort();
    candidates.reverse();

    for release in candidates {
        let executable = release
            .join("bin")
            .join(Runtime::Codex.executable_file_name());
        if probe_path(runtime, &executable).await.is_err() {
            continue;
        }
        if let Err(error) = activate(&release) {
            tracing::warn!(%error, release = %release.display(), "failed to relink Codex release");
            continue;
        }
        return Some(executable);
    }
    None
}

// ---------------------------------------------------------------------------
// Step 3: install from upstream
// ---------------------------------------------------------------------------

/// Install `runtime` using whichever channel its strategy names. This is the
/// only place the providers differ; everything around it is shared.
async fn install_for(runtime: Runtime) -> Result<String> {
    match runtime.install_strategy() {
        InstallStrategy::CodexPackage => install_latest().await,
        InstallStrategy::Script { url } => {
            run_install_script(runtime, url).await?;
            let path = resolve_uncached(runtime)
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            Ok(installed_version(&path).await)
        }
    }
}

/// Download, verify and install the latest Codex package. Returns its version.
async fn install_latest() -> Result<String> {
    let client = Client::builder()
        .user_agent(format!("borg/{}", env!("CARGO_PKG_VERSION")))
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(900))
        .build()
        .context("failed to initialize the Codex download client")?;

    let release = fetch_release(&client).await?;
    let version = version_from_tag(&release.tag_name);
    let target = package_target()?;
    let asset_name = format!("codex-package-{target}.tar.gz");

    let asset = release
        .assets
        .iter()
        .find(|asset| asset.name == asset_name)
        .with_context(|| format!("Codex release {} has no {asset_name}", release.tag_name))?;
    eprintln!("Borg: installing Codex {version} for {target}...");

    // Prefer the digest carried in the metadata; fall back to the published
    // SHA256SUMS manifest. One of the two must verify — unverified bytes are
    // never installed.
    let archive = download(&client, &asset.browser_download_url).await?;
    match asset.digest.as_deref().and_then(parse_sha256_digest) {
        Some(expected) => verify_digest(&archive, &expected)?,
        None => {
            let checksums = release
                .assets
                .iter()
                .find(|asset| asset.name == CHECKSUM_ASSET)
                .with_context(|| {
                    format!("Codex release {} has no {CHECKSUM_ASSET}", release.tag_name)
                })?;
            let manifest = download(&client, &checksums.browser_download_url).await?;
            verify_against_manifest(&asset_name, &archive, &manifest)?;
        }
    }

    let root = standalone_root().context("could not determine the Codex install directory")?;
    let releases = root.join("releases");
    fs::create_dir_all(&releases).with_context(|| {
        format!(
            "failed to create the Codex release directory {}",
            releases.display()
        )
    })?;

    // Stage beside the destination so the final publish is an atomic rename on
    // the same filesystem, and a crash mid-extract never leaves a half-written
    // release where `current` can find it.
    let staging = releases.join(format!(".staging-{}", std::process::id()));
    if staging.exists() {
        fs::remove_dir_all(&staging).context("failed to clear stale Codex staging")?;
    }
    let staged = extract_and_validate(Runtime::Codex, &archive, &staging, &version, target).await;
    let staged = match staged {
        Ok(()) => staging,
        Err(error) => {
            let _ = fs::remove_dir_all(&staging);
            return Err(error);
        }
    };

    let destination = releases.join(format!("{version}-{target}"));
    if destination.exists() {
        // A concurrent installer, or a previous partial run. Ours is verified,
        // so replace it, but keep the swap atomic from a reader's point of view.
        let retired = releases.join(format!(".retired-{}", std::process::id()));
        let _ = fs::remove_dir_all(&retired);
        fs::rename(&destination, &retired)
            .context("failed to move the existing Codex release aside")?;
        let published = fs::rename(&staged, &destination);
        if published.is_err() {
            let _ = fs::rename(&retired, &destination);
            published.context("failed to publish the Codex release")?;
        }
        let _ = fs::remove_dir_all(&retired);
    } else {
        fs::rename(&staged, &destination).context("failed to publish the Codex release")?;
    }

    activate(&destination)?;
    eprintln!("Borg: Codex {version} installed.");
    Ok(version)
}

/// Resolve release metadata, preferring `releases.openai.com` and falling back
/// to the GitHub API. Either source alone is a single point of failure; an
/// installer that heals itself should not depend on exactly one host.
async fn fetch_release(client: &Client) -> Result<GithubRelease> {
    if let Ok(url) = std::env::var(RELEASE_API_ENV) {
        return fetch_release_from(client, &url).await;
    }

    let primary = fetch_release_from(client, RELEASES_CHANNEL_URL).await;
    let primary_error = match primary {
        Ok(release) => return Ok(release),
        Err(error) => error,
    };
    tracing::warn!(%primary_error, "Codex release channel unavailable; trying GitHub");

    let github = format!("https://api.github.com/repos/{REPOSITORY}/releases/latest");
    fetch_release_from(client, &github)
        .await
        .with_context(|| format!("the Codex release channel was also unavailable: {primary_error}"))
}

async fn fetch_release_from(client: &Client, url: &str) -> Result<GithubRelease> {
    client
        .get(url)
        .send()
        .await
        .with_context(|| format!("failed to reach {url}"))?
        .error_for_status()
        .with_context(|| format!("{url} returned an error"))?
        .json()
        .await
        .with_context(|| format!("{url} returned invalid Codex release metadata"))
}

async fn download(client: &Client, url: &str) -> Result<Vec<u8>> {
    let response = client
        .get(url)
        .send()
        .await
        .context("failed to download the Codex package")?
        .error_for_status()
        .context("the Codex package download returned an error")?;
    if response
        .content_length()
        .is_some_and(|length| length > MAX_DOWNLOAD_BYTES)
    {
        bail!("the Codex package exceeds the {MAX_DOWNLOAD_BYTES} byte safety limit");
    }
    let bytes = response
        .bytes()
        .await
        .context("failed to read the Codex package")?;
    anyhow::ensure!(
        bytes.len() as u64 <= MAX_DOWNLOAD_BYTES,
        "the Codex package exceeds the {MAX_DOWNLOAD_BYTES} byte safety limit"
    );
    Ok(bytes.to_vec())
}

/// Parse a `sha256:<hex>` digest as published in the release metadata.
pub(crate) fn parse_sha256_digest(digest: &str) -> Option<String> {
    let hex = digest.trim().strip_prefix("sha256:")?;
    (hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .then(|| hex.to_ascii_lowercase())
}

/// Compare `archive` against an expected lowercase hex SHA-256.
pub(crate) fn verify_digest(archive: &[u8], expected: &str) -> Result<()> {
    let actual = hex::encode(Sha256::digest(archive));
    anyhow::ensure!(
        actual.eq_ignore_ascii_case(expected),
        "the Codex package checksum did not match; refusing to install"
    );
    Ok(())
}

/// Check `archive` against the line naming `asset_name` in a `sha256sum`-style
/// manifest. An absent line is a hard failure: never install unverified bytes.
pub(crate) fn verify_against_manifest(
    asset_name: &str,
    archive: &[u8],
    manifest: &[u8],
) -> Result<()> {
    let text = std::str::from_utf8(manifest).context("Codex checksum manifest is not UTF-8")?;
    let expected = text
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let digest = fields.next()?;
            let name = fields.next()?.trim_start_matches('*');
            (name == asset_name).then_some(digest)
        })
        .next()
        .with_context(|| format!("Codex checksum manifest does not list {asset_name}"))?;
    anyhow::ensure!(
        expected.len() == 64 && expected.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "Codex checksum for {asset_name} is not SHA-256"
    );
    let actual = hex::encode(Sha256::digest(archive));
    anyhow::ensure!(
        actual.eq_ignore_ascii_case(expected),
        "the Codex package checksum did not match; refusing to install"
    );
    Ok(())
}

/// Unpack the package into `destination` and prove it runs before it is
/// allowed anywhere near the published layout.
async fn extract_and_validate(
    runtime: Runtime,
    archive: &[u8],
    destination: &Path,
    version: &str,
    target: &str,
) -> Result<()> {
    fs::create_dir_all(destination).context("failed to create the Codex staging directory")?;
    unpack(archive, destination)?;

    let manifest_path = destination.join("codex-package.json");
    let manifest: PackageManifest = serde_json::from_slice(
        &fs::read(&manifest_path).context("the Codex package has no codex-package.json")?,
    )
    .context("the Codex package manifest is invalid")?;
    anyhow::ensure!(
        manifest.layout_version == SUPPORTED_LAYOUT_VERSION,
        "the Codex package uses layout version {} which this version of Borg does not understand; \
         run `borg update`",
        manifest.layout_version
    );
    anyhow::ensure!(
        manifest.version == version && manifest.target == target,
        "the Codex package describes {}-{} but {version}-{target} was requested",
        manifest.version,
        manifest.target
    );

    let entrypoint = destination.join(&manifest.entrypoint);
    anyhow::ensure!(
        entrypoint.is_file(),
        "the Codex package is missing its entrypoint {}",
        manifest.entrypoint
    );
    probe_path(runtime, &entrypoint)
        .await
        .map_err(|reason| anyhow::anyhow!("the downloaded Codex does not run: {reason}"))
}

#[derive(Debug, Deserialize)]
struct PackageManifest {
    #[serde(rename = "layoutVersion")]
    layout_version: u32,
    version: String,
    target: String,
    entrypoint: String,
}

/// Extract a gzipped tar, refusing any entry that would escape `destination`.
fn unpack(archive: &[u8], destination: &Path) -> Result<()> {
    use flate2::read::GzDecoder;
    let mut tar = tar::Archive::new(GzDecoder::new(Cursor::new(archive)));
    tar.set_preserve_permissions(true);
    for entry in tar
        .entries()
        .context("the Codex package is not a valid archive")?
    {
        let mut entry = entry.context("the Codex package has an unreadable entry")?;
        let path = entry
            .path()
            .context("the Codex package has an invalid entry path")?
            .into_owned();
        ensure_safe_entry(&path)?;
        entry
            .unpack(destination.join(&path))
            .with_context(|| format!("failed to extract {}", path.display()))?;
    }
    Ok(())
}

/// Reject absolute paths, `..` traversal, and Windows path/drive tricks.
pub(crate) fn ensure_safe_entry(path: &Path) -> Result<()> {
    use std::path::Component;
    anyhow::ensure!(
        !path.is_absolute(),
        "the Codex package contains an absolute path: {}",
        path.display()
    );
    for component in path.components() {
        match component {
            Component::Normal(part) => {
                let text = part.to_string_lossy();
                anyhow::ensure!(
                    !text.contains(':') && !text.contains('\\'),
                    "the Codex package contains an unsafe path component: {}",
                    path.display()
                );
            }
            Component::CurDir => {}
            _ => bail!(
                "the Codex package contains an unsafe path: {}",
                path.display()
            ),
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Publishing a release
// ---------------------------------------------------------------------------

/// Point `current`, the package's own `codex` shortcut, and `~/.local/bin` at
/// `release`. Every symlink swap is atomic so a concurrent reader sees either
/// the old target or the new one, never a missing file.
fn activate(release: &Path) -> Result<()> {
    let root = release
        .parent()
        .and_then(Path::parent)
        .context("the Codex release directory has no standalone root")?;

    // The package ships `bin/codex`; upstream's installer adds a top-level
    // `codex` shortcut beside it. Recreate it so both layouts resolve.
    let entry = release.join(Runtime::Codex.executable_file_name());
    if !entry.exists() {
        let target = Path::new("bin").join(Runtime::Codex.executable_file_name());
        let _ = replace_symlink(&target, &entry);
    }

    replace_symlink(release, &root.join("current"))
        .context("failed to point the Codex `current` symlink at the new release")?;

    // Publishing into ~/.local/bin is best effort: Borg always calls the
    // resolved absolute path, so a read-only or absent bin directory must not
    // fail the install. It only affects what the user's own shell finds.
    if let Some(bin) = user_bin_dir()
        && fs::create_dir_all(&bin).is_ok()
    {
        for name in [
            Runtime::Codex.executable_file_name(),
            codex_code_mode_host_name().to_string(),
        ] {
            let source = release.join("bin").join(&name);
            if source.exists()
                && let Err(error) = replace_symlink(&source, &bin.join(&name))
            {
                tracing::warn!(%error, "failed to link {name} into {}", bin.display());
            }
        }
    }
    Ok(())
}

/// Create or atomically replace the symlink at `link` so it points to `target`.
fn replace_symlink(target: &Path, link: &Path) -> Result<()> {
    let parent = link.parent().context("symlink has no parent directory")?;
    fs::create_dir_all(parent).with_context(|| format!("failed to create {}", parent.display()))?;
    let staging = parent.join(format!(
        ".{}-{}.tmp",
        link.file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| "link".to_string()),
        std::process::id()
    ));
    let _ = fs::remove_file(&staging);
    symlink(target, &staging)
        .with_context(|| format!("failed to create the symlink {}", staging.display()))?;
    // `rename` over an existing symlink is atomic and replaces it.
    if let Err(error) = fs::rename(&staging, link) {
        let _ = fs::remove_file(&staging);
        return Err(error).with_context(|| format!("failed to publish {}", link.display()));
    }
    Ok(())
}

#[cfg(unix)]
fn symlink(target: &Path, link: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
fn symlink(target: &Path, link: &Path) -> std::io::Result<()> {
    // Symlinks need a privilege Windows does not grant by default; a hard link
    // gives the same resolution for a file and needs no privilege.
    if target.is_dir() {
        std::os::windows::fs::symlink_dir(target, link)
    } else {
        fs::hard_link(target, link).or_else(|_| std::os::windows::fs::symlink_file(target, link))
    }
}

// ---------------------------------------------------------------------------
// Paths and platform
// ---------------------------------------------------------------------------

fn standalone_root() -> Option<PathBuf> {
    let home = std::env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .or_else(|| crate::provider_bin::home_directory().map(|home| home.join(".codex")))?;
    Some(home.join("packages").join("standalone"))
}

fn user_bin_dir() -> Option<PathBuf> {
    crate::provider_bin::home_directory().map(|home| home.join(".local").join("bin"))
}

fn codex_code_mode_host_name() -> &'static str {
    if cfg!(windows) {
        "codex-code-mode-host.exe"
    } else {
        "codex-code-mode-host"
    }
}

/// The Rust target triple naming the Codex package for this machine.
pub(crate) fn package_target() -> Result<&'static str> {
    // A process running under Rosetta reports x86_64 on Apple silicon. Installing
    // the translated build there would work but be needlessly slow, and the
    // official installer makes the same correction.
    let arch = if std::env::consts::OS == "macos"
        && std::env::consts::ARCH == "x86_64"
        && running_under_rosetta()
    {
        "aarch64"
    } else {
        std::env::consts::ARCH
    };
    let target = match (arch, std::env::consts::OS) {
        ("aarch64", "macos") => "aarch64-apple-darwin",
        ("x86_64", "macos") => "x86_64-apple-darwin",
        // Codex publishes musl builds for Linux; they are statically linked and
        // run on glibc distributions too, so there is one Linux asset per arch.
        ("aarch64", "linux") => "aarch64-unknown-linux-musl",
        ("x86_64", "linux") => "x86_64-unknown-linux-musl",
        ("aarch64", "windows") => "aarch64-pc-windows-msvc",
        ("x86_64", "windows") => "x86_64-pc-windows-msvc",
        (arch, os) => bail!("Codex does not publish a build for {os} on {arch}"),
    };
    Ok(target)
}

/// Whether this process is an x86_64 binary translated by Rosetta.
fn running_under_rosetta() -> bool {
    #[cfg(target_os = "macos")]
    {
        std::process::Command::new("/usr/sbin/sysctl")
            .args(["-n", "sysctl.proc_translated"])
            .output()
            .ok()
            .filter(|output| output.status.success())
            .is_some_and(|output| String::from_utf8_lossy(&output.stdout).trim() == "1")
    }
    #[cfg(not(target_os = "macos"))]
    {
        false
    }
}

/// Codex tags releases as `rust-v0.152.1`; the package directory and manifest
/// use the bare `0.152.1`.
pub(crate) fn version_from_tag(tag: &str) -> String {
    tag.trim()
        .trim_start_matches("rust-")
        .trim_start_matches('v')
        .to_string()
}

/// Advisory cross-process lock over the standalone root.
///
/// Uses the same `install.lock.d` directory the official installer creates, so
/// Borg and a concurrently running `install.sh` cannot both write the layout.
/// `create_dir` is atomic on every supported filesystem, which is exactly the
/// primitive a lock needs.
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

/// Run a vendor's official installer non-interactively.
///
/// Piping a vendor script is not the first choice — the Codex path verifies a
/// checksummed package instead — but for runtimes that publish no such manifest
/// it is the canonical channel, and it is the one thing guaranteed to track
/// upstream when a layout changes underneath us.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_strips_the_rust_tag_prefix() {
        assert_eq!(version_from_tag("rust-v0.152.1"), "0.152.1");
        assert_eq!(version_from_tag("v0.152.1"), "0.152.1");
        assert_eq!(version_from_tag("0.152.1"), "0.152.1");
    }

    #[test]
    fn target_is_known_for_this_machine() {
        assert!(package_target().is_ok());
    }

    #[test]
    fn manifest_verification_accepts_the_matching_line() {
        let archive = b"codex bytes";
        let digest = hex::encode(Sha256::digest(archive));
        let manifest = format!(
            "0000000000000000000000000000000000000000000000000000000000000000  other.tar.gz\n\
             {digest}  codex-package-aarch64-apple-darwin.tar.gz\n"
        );
        verify_against_manifest(
            "codex-package-aarch64-apple-darwin.tar.gz",
            archive,
            manifest.as_bytes(),
        )
        .expect("checksum should verify");
    }

    #[test]
    fn manifest_verification_rejects_a_wrong_digest() {
        let manifest = "0000000000000000000000000000000000000000000000000000000000000000  codex-package-aarch64-apple-darwin.tar.gz\n";
        let error = verify_against_manifest(
            "codex-package-aarch64-apple-darwin.tar.gz",
            b"codex bytes",
            manifest.as_bytes(),
        )
        .expect_err("a mismatched checksum must fail");
        assert!(error.to_string().contains("did not match"));
    }

    #[test]
    fn manifest_verification_rejects_an_unlisted_asset() {
        let manifest = "0000000000000000000000000000000000000000000000000000000000000000  something-else.tar.gz\n";
        assert!(
            verify_against_manifest(
                "codex-package-x86_64-apple-darwin.tar.gz",
                b"x",
                manifest.as_bytes()
            )
            .is_err(),
            "an asset missing from the manifest must never install"
        );
    }

    #[test]
    fn unsafe_archive_entries_are_rejected() {
        assert!(ensure_safe_entry(Path::new("bin/codex")).is_ok());
        assert!(ensure_safe_entry(Path::new("./codex-package.json")).is_ok());
        assert!(ensure_safe_entry(Path::new("../escape")).is_err());
        assert!(ensure_safe_entry(Path::new("/etc/passwd")).is_err());
        assert!(ensure_safe_entry(Path::new("bin/../../escape")).is_err());
    }

    #[test]
    fn auto_install_is_on_unless_explicitly_disabled() {
        // Guarded: the environment is process-global, so assert the parsing
        // rather than mutating it under a parallel test runner.
        for value in ["0", "false", "no", "off", "FALSE"] {
            assert!(
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "0" | "false" | "no" | "off"
                ),
                "{value} should disable auto-install"
            );
        }
    }

    #[tokio::test]
    async fn replace_symlink_swaps_an_existing_link() {
        let root = tempfile::tempdir().expect("temporary directory");
        let first = root.path().join("first");
        let second = root.path().join("second");
        fs::write(&first, b"1").unwrap();
        fs::write(&second, b"2").unwrap();
        let link = root.path().join("current");

        replace_symlink(&first, &link).expect("initial link");
        assert_eq!(fs::read(&link).unwrap(), b"1");

        replace_symlink(&second, &link).expect("swap link");
        assert_eq!(fs::read(&link).unwrap(), b"2");
    }
}
