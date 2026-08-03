use std::fs;
use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use reqwest::Client;
use semver::Version;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::agent_config::UpdateConfig;
use crate::cli::UpdateArgs;

const REPOSITORY: &str = "borg-ml/cli";
const MAX_DOWNLOAD_BYTES: usize = 128 * 1024 * 1024;
static BACKGROUND_STARTED: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Deserialize)]
struct GithubRelease {
    tag_name: String,
    assets: Vec<GithubAsset>,
}

#[derive(Debug, Deserialize)]
struct GithubAsset {
    name: String,
    browser_download_url: String,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct UpdateState {
    last_checked_unix: u64,
}

pub(crate) async fn run(args: UpdateArgs) -> Result<()> {
    match update(args.check, false).await? {
        UpdateOutcome::Current(version) => println!("Borg {version} is up to date."),
        UpdateOutcome::Available(version) => {
            println!("Borg {version} is available. Run `borg update` to install it.")
        }
        UpdateOutcome::Installed(version) => {
            #[cfg(unix)]
            println!("Updated Borg to {version}. The next launch will use it.");
            #[cfg(windows)]
            println!("Borg {version} will be installed when this process exits.");
        }
    }
    Ok(())
}

pub(crate) fn spawn_background(config: UpdateConfig) {
    if cfg!(debug_assertions)
        || !config.auto_install
        || BACKGROUND_STARTED.swap(true, Ordering::AcqRel)
        || !check_is_due(config.check_interval_hours)
    {
        return;
    }
    tokio::spawn(async {
        if let Err(error) = update(false, true).await {
            tracing::debug!(%error, "background Borg update skipped");
        }
    });
}

#[derive(Debug, PartialEq, Eq)]
enum UpdateOutcome {
    Current(Version),
    Available(Version),
    Installed(Version),
}

async fn update(check_only: bool, quiet: bool) -> Result<UpdateOutcome> {
    let client = Client::builder()
        .user_agent(format!("borg/{}", env!("CARGO_PKG_VERSION")))
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(60))
        .build()
        .context("failed to initialize the update client")?;
    if quiet {
        // Throttle unattended attempts even when GitHub is temporarily unavailable.
        record_check();
    }
    let release = fetch_release(&client).await?;
    if !quiet {
        record_check();
    }
    let latest = parse_version(&release.tag_name)?;
    let current = Version::parse(env!("CARGO_PKG_VERSION")).context("invalid installed version")?;
    if latest <= current {
        return Ok(UpdateOutcome::Current(current));
    }
    if check_only {
        return Ok(UpdateOutcome::Available(latest));
    }

    let asset_name = platform_asset()?;
    let asset = release
        .assets
        .iter()
        .find(|asset| asset.name == asset_name)
        .with_context(|| format!("release {} has no {asset_name}", release.tag_name))?;
    let checksum = release
        .assets
        .iter()
        .find(|candidate| candidate.name == format!("{asset_name}.sha256"))
        .with_context(|| {
            format!(
                "release {} has no checksum for {asset_name}",
                release.tag_name
            )
        })?;
    if !quiet {
        eprintln!("Downloading Borg {latest} for {}...", platform_target()?);
    }
    let (archive, checksum_bytes) = tokio::try_join!(
        download(&client, &asset.browser_download_url),
        download(&client, &checksum.browser_download_url)
    )?;
    verify_checksum(&asset_name, &archive, &checksum_bytes)?;
    let temporary = tempfile::tempdir().context("failed to create update staging directory")?;
    let candidate = temporary.path().join(executable_name());
    let provider_candidate = temporary.path().join("providers/claude");
    extract_executable(&archive, &candidate)?;
    extract_native_provider(&archive, &provider_candidate)?;
    validate_candidate(&candidate, &latest)?;
    validate_native_provider(&provider_candidate)?;
    install_candidate(&candidate, &provider_candidate, &latest)?;
    Ok(UpdateOutcome::Installed(latest))
}

async fn fetch_release(client: &Client) -> Result<GithubRelease> {
    let url = std::env::var("BORG_UPDATE_API_URL")
        .unwrap_or_else(|_| format!("https://api.github.com/repos/{REPOSITORY}/releases/latest"));
    client
        .get(url)
        .send()
        .await
        .context("failed to check for Borg updates")?
        .error_for_status()
        .context("Borg update server returned an error")?
        .json()
        .await
        .context("invalid Borg release metadata")
}

async fn download(client: &Client, url: &str) -> Result<Vec<u8>> {
    let response = client
        .get(url)
        .send()
        .await
        .context("failed to download Borg update")?
        .error_for_status()
        .context("Borg release download returned an error")?;
    if response
        .content_length()
        .is_some_and(|length| length > MAX_DOWNLOAD_BYTES as u64)
    {
        bail!("Borg update exceeds the {MAX_DOWNLOAD_BYTES} byte safety limit");
    }
    let bytes = response
        .bytes()
        .await
        .context("failed to read Borg update")?;
    anyhow::ensure!(
        bytes.len() <= MAX_DOWNLOAD_BYTES,
        "Borg update exceeds the {MAX_DOWNLOAD_BYTES} byte safety limit"
    );
    Ok(bytes.to_vec())
}

fn verify_checksum(asset_name: &str, archive: &[u8], checksum_file: &[u8]) -> Result<()> {
    let text = std::str::from_utf8(checksum_file).context("checksum is not UTF-8")?;
    let mut fields = text.split_whitespace();
    let expected = fields.next().context("checksum file is empty")?;
    let named_asset = fields
        .next()
        .map(|name| name.trim_start_matches('*'))
        .context("checksum file does not name its asset")?;
    anyhow::ensure!(
        fields.next().is_none(),
        "checksum file has unexpected fields"
    );
    anyhow::ensure!(named_asset == asset_name, "checksum names the wrong asset");
    anyhow::ensure!(
        expected.len() == 64 && expected.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "checksum is not SHA-256"
    );
    let actual = format!("{:x}", Sha256::digest(archive));
    anyhow::ensure!(
        actual.eq_ignore_ascii_case(expected),
        "Borg update checksum did not match"
    );
    Ok(())
}

#[cfg(unix)]
fn extract_executable(archive: &[u8], destination: &Path) -> Result<()> {
    use flate2::read::GzDecoder;
    let mut tar = tar::Archive::new(GzDecoder::new(Cursor::new(archive)));
    for entry in tar.entries().context("invalid Borg release archive")? {
        let mut entry = entry.context("invalid Borg release entry")?;
        if entry.path().context("invalid Borg release path")?.as_ref() == Path::new("borg") {
            let mut output =
                fs::File::create(destination).context("failed to stage Borg update")?;
            std::io::copy(&mut entry, &mut output).context("failed to extract Borg update")?;
            output.sync_all().context("failed to sync Borg update")?;
            return Ok(());
        }
    }
    bail!("Borg release archive does not contain `borg`")
}

#[cfg(unix)]
fn extract_native_provider(archive: &[u8], destination: &Path) -> Result<()> {
    use flate2::read::GzDecoder;
    let mut tar = tar::Archive::new(GzDecoder::new(Cursor::new(archive)));
    fs::create_dir_all(destination)
        .context("failed to create native provider staging directory")?;
    let mut found_binary = false;
    for entry in tar.entries().context("invalid Borg release archive")? {
        let mut entry = entry.context("invalid Borg release entry")?;
        let path = entry.path().context("invalid Borg release path")?;
        let Ok(relative) = path.strip_prefix(Path::new("providers/claude")) else {
            continue;
        };
        let Some(name) = relative.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if relative.components().count() != 1
            || !matches!(name, "claude" | "manifest.json" | "package.json")
        {
            continue;
        }
        let name = name.to_string();
        let output = destination.join(&name);
        let mut file = fs::File::create(&output).with_context(|| {
            format!("failed to stage native provider file {}", output.display())
        })?;
        std::io::copy(&mut entry, &mut file).with_context(|| {
            format!(
                "failed to extract native provider file {}",
                output.display()
            )
        })?;
        file.sync_all()
            .context("failed to sync native provider file")?;
        if name == "claude" {
            found_binary = true;
        }
    }
    anyhow::ensure!(
        found_binary,
        "Borg release archive does not contain providers/claude/claude"
    );
    Ok(())
}

#[cfg(windows)]
fn extract_executable(archive: &[u8], destination: &Path) -> Result<()> {
    let mut zip = zip::ZipArchive::new(Cursor::new(archive)).context("invalid Borg release zip")?;
    let mut entry = zip
        .by_name("borg.exe")
        .context("Borg release zip does not contain `borg.exe`")?;
    let mut output = fs::File::create(destination).context("failed to stage Borg update")?;
    std::io::copy(&mut entry, &mut output).context("failed to extract Borg update")?;
    output.sync_all().context("failed to sync Borg update")
}

#[cfg(windows)]
fn extract_native_provider(archive: &[u8], destination: &Path) -> Result<()> {
    let mut zip = zip::ZipArchive::new(Cursor::new(archive)).context("invalid Borg release zip")?;
    fs::create_dir_all(destination)
        .context("failed to create native provider staging directory")?;
    for name in ["claude.exe", "manifest.json", "package.json"] {
        let archive_name = format!("providers/claude/{name}");
        let mut entry = zip
            .by_name(&archive_name)
            .with_context(|| format!("Borg release zip does not contain `{archive_name}`"))?;
        let output = destination.join(name);
        let mut file = fs::File::create(&output).with_context(|| {
            format!("failed to stage native provider file {}", output.display())
        })?;
        std::io::copy(&mut entry, &mut file).with_context(|| {
            format!(
                "failed to extract native provider file {}",
                output.display()
            )
        })?;
        file.sync_all()
            .context("failed to sync native provider file")?;
    }
    Ok(())
}

fn validate_candidate(candidate: &Path, expected: &Version) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(candidate, fs::Permissions::from_mode(0o755))
            .context("failed to make staged Borg executable")?;
    }
    let output = std::process::Command::new(candidate)
        .arg("--version")
        .stdin(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .context("failed to run staged Borg update")?;
    anyhow::ensure!(output.status.success(), "staged Borg update did not run");
    let version = String::from_utf8(output.stdout).context("invalid staged Borg version output")?;
    anyhow::ensure!(
        version.trim() == format!("borg {expected}"),
        "staged Borg version did not match release metadata"
    );
    Ok(())
}

fn validate_native_provider(provider: &Path) -> Result<()> {
    for name in [
        native_provider_executable_name(),
        "manifest.json",
        "package.json",
    ] {
        anyhow::ensure!(
            provider.join(name).is_file(),
            "staged native provider is missing `{name}`"
        );
    }
    Ok(())
}

#[cfg(unix)]
fn install_candidate(candidate: &Path, provider: &Path, _version: &Version) -> Result<()> {
    let executable = std::env::current_exe().context("failed to locate installed Borg")?;
    install_candidate_at(candidate, &executable)?;
    install_native_provider_at(provider, &executable)
}

#[cfg(unix)]
fn install_candidate_at(candidate: &Path, executable: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let parent = executable
        .parent()
        .context("installed Borg has no parent")?;
    let staged = parent.join(format!(".borg-update-{}", std::process::id()));
    fs::copy(candidate, &staged).context("failed to stage update beside installed Borg")?;
    let mode = fs::metadata(executable)
        .map(|metadata| metadata.permissions().mode())
        .unwrap_or(0o755);
    fs::set_permissions(&staged, fs::Permissions::from_mode(mode))
        .context("failed to preserve Borg executable permissions")?;
    fs::File::open(&staged)
        .and_then(|file| file.sync_all())
        .context("failed to sync staged Borg update")?;
    fs::rename(&staged, executable).context("failed to atomically replace installed Borg")
}

fn install_native_provider_at(provider: &Path, executable: &Path) -> Result<()> {
    validate_native_provider(provider)?;
    let target = std::env::var_os("BORG_HOME")
        .map(PathBuf::from)
        .map(|home| home.join("providers/claude"))
        .or_else(|| {
            executable
                .parent()
                .map(|parent| parent.join("providers/claude"))
        })
        .context("installed Borg has no provider destination")?;
    install_native_provider_to(provider, &target)
}

fn install_native_provider_to(provider: &Path, target: &Path) -> Result<()> {
    let providers_parent = target
        .parent()
        .context("native provider destination has no parent")?;
    fs::create_dir_all(&providers_parent)
        .context("failed to create installed provider directory")?;
    let staged = providers_parent.join(format!(".claude-update-{}", std::process::id()));
    let backup = providers_parent.join(format!(".claude-backup-{}", std::process::id()));
    if staged.exists() {
        fs::remove_dir_all(&staged).context("failed to clear stale provider staging")?;
    }
    if backup.exists() {
        fs::remove_dir_all(&backup).context("failed to clear stale provider backup")?;
    }
    fs::create_dir(&staged).context("failed to stage native provider")?;
    for name in [
        native_provider_executable_name(),
        "manifest.json",
        "package.json",
    ] {
        fs::copy(provider.join(name), staged.join(name))
            .with_context(|| format!("failed to copy native provider `{name}`"))?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(
            staged.join(native_provider_executable_name()),
            fs::Permissions::from_mode(0o700),
        )
        .context("failed to make native provider executable")?;
    }
    if target.exists() {
        fs::rename(&target, &backup).context("failed to stage the installed native provider")?;
    }
    if let Err(error) = fs::rename(&staged, &target) {
        if backup.exists() {
            let _ = fs::rename(&backup, &target);
        }
        return Err(error).context("failed to atomically install native provider");
    }
    if backup.exists() {
        fs::remove_dir_all(&backup).context("failed to remove old native provider")?;
    }
    Ok(())
}

#[cfg(windows)]
fn install_candidate(candidate: &Path, provider: &Path, version: &Version) -> Result<()> {
    use std::os::windows::process::CommandExt;
    let executable = std::env::current_exe().context("failed to locate installed Borg")?;
    let parent = executable
        .parent()
        .context("installed Borg has no parent")?;
    let id = std::process::id();
    let staged = parent.join(format!(".borg-update-{id}.exe"));
    let helper = parent.join(format!(".borg-update-{id}.ps1"));
    fs::copy(candidate, &staged).context("failed to stage update beside installed Borg")?;
    install_native_provider_at(provider, &executable)?;
    let script = r#"param([int]$ParentId,[string]$Source,[string]$Destination,[string]$Helper)
Wait-Process -Id $ParentId -ErrorAction SilentlyContinue
for ($i = 0; $i -lt 120; $i++) {
  try { Move-Item -Force $Source $Destination; break } catch { Start-Sleep -Milliseconds 500 }
}
Remove-Item -Force -ErrorAction SilentlyContinue $Source
Remove-Item -Force -ErrorAction SilentlyContinue $Helper
"#;
    fs::write(&helper, script).context("failed to write Windows update helper")?;
    std::process::Command::new("powershell")
        .args([
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
        ])
        .arg(&helper)
        .args(["-ParentId", &id.to_string(), "-Source"])
        .arg(&staged)
        .arg("-Destination")
        .arg(&executable)
        .arg("-Helper")
        .arg(&helper)
        .creation_flags(0x0800_0000)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("failed to schedule Borg {version} replacement"))?;
    Ok(())
}

fn parse_version(tag: &str) -> Result<Version> {
    Version::parse(tag.trim().trim_start_matches('v'))
        .with_context(|| format!("invalid Borg release tag `{tag}`"))
}

fn platform_target() -> Result<&'static str> {
    match (std::env::consts::ARCH, std::env::consts::OS) {
        ("x86_64", "linux") => Ok("x86_64-unknown-linux-gnu"),
        ("aarch64", "linux") => Ok("aarch64-unknown-linux-gnu"),
        ("x86_64", "macos") => Ok("x86_64-apple-darwin"),
        ("aarch64", "macos") => Ok("aarch64-apple-darwin"),
        ("x86_64", "windows") => Ok("x86_64-pc-windows-msvc"),
        ("aarch64", "windows") => Ok("aarch64-pc-windows-msvc"),
        (arch, os) => bail!("Borg self-update is not supported on {arch}-{os}"),
    }
}

fn platform_asset() -> Result<String> {
    let suffix = if cfg!(windows) { ".zip" } else { ".tar.gz" };
    Ok(format!("borg-{}{suffix}", platform_target()?))
}

fn executable_name() -> &'static str {
    if cfg!(windows) { "borg.exe" } else { "borg" }
}

fn native_provider_executable_name() -> &'static str {
    if cfg!(windows) {
        "claude.exe"
    } else {
        "claude"
    }
}

fn state_path() -> Option<PathBuf> {
    let root = std::env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".cache")))
        .or_else(|| std::env::var_os("LOCALAPPDATA").map(PathBuf::from))?;
    Some(root.join("borg").join("update.json"))
}

fn check_is_due(interval_hours: u64) -> bool {
    let Some(path) = state_path() else {
        return false;
    };
    let state = fs::read(&path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<UpdateState>(&bytes).ok())
        .unwrap_or_default();
    unix_now().saturating_sub(state.last_checked_unix) >= interval_hours.saturating_mul(3600)
}

fn record_check() {
    let Some(path) = state_path() else {
        return;
    };
    let Some(parent) = path.parent() else {
        return;
    };
    if fs::create_dir_all(parent).is_err() {
        return;
    }
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    let state = UpdateState {
        last_checked_unix: unix_now(),
    };
    if serde_json::to_vec(&state)
        .ok()
        .and_then(|bytes| {
            fs::File::create(&temporary)
                .ok()
                .and_then(|mut file| file.write_all(&bytes).ok())
        })
        .is_some()
    {
        let _ = fs::rename(temporary, path);
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checksum_binds_hash_and_asset_name() {
        let bytes = b"release";
        let hash = format!("{:x}", Sha256::digest(bytes));
        verify_checksum(
            "borg-test.tar.gz",
            bytes,
            format!("{hash}  borg-test.tar.gz\n").as_bytes(),
        )
        .unwrap();
        assert!(
            verify_checksum(
                "borg-test.tar.gz",
                bytes,
                format!("{hash}  another.tar.gz\n").as_bytes()
            )
            .is_err()
        );
    }

    #[test]
    fn release_versions_are_semantic() {
        assert_eq!(parse_version("v1.2.3").unwrap(), Version::new(1, 2, 3));
        assert!(parse_version("latest").is_err());
    }

    #[test]
    fn supported_target_has_matching_archive() {
        let target = platform_target().unwrap();
        let asset = platform_asset().unwrap();
        assert!(asset.contains(target));
        assert!(asset.ends_with(if cfg!(windows) { ".zip" } else { ".tar.gz" }));
    }

    #[cfg(unix)]
    #[test]
    fn unix_release_archive_extracts_only_the_expected_binary() {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        let mut archive = tar::Builder::new(GzEncoder::new(Vec::new(), Compression::fast()));
        let bytes = b"borg-binary";
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o755);
        header.set_cksum();
        archive
            .append_data(&mut header, "borg", Cursor::new(bytes))
            .unwrap();
        let compressed = archive.into_inner().unwrap().finish().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("borg");
        extract_executable(&compressed, &destination).unwrap();
        assert_eq!(fs::read(destination).unwrap(), bytes);
    }

    #[cfg(unix)]
    #[test]
    fn unix_release_archive_extracts_the_native_provider_payload() {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        let mut archive = tar::Builder::new(GzEncoder::new(Vec::new(), Compression::fast()));
        for (name, bytes) in [
            ("providers/claude/claude", b"claude-binary".as_slice()),
            (
                "providers/claude/manifest.json",
                br#"{"sdkCompat":{"harnessSchema":1}}"#,
            ),
            ("providers/claude/package.json", br#"{"version":"1.0.0"}"#),
        ] {
            let mut header = tar::Header::new_gnu();
            header.set_size(bytes.len() as u64);
            header.set_mode(0o700);
            header.set_cksum();
            archive
                .append_data(&mut header, name, Cursor::new(bytes))
                .unwrap();
        }
        let compressed = archive.into_inner().unwrap().finish().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("providers/claude");
        extract_native_provider(&compressed, &destination).unwrap();
        validate_native_provider(&destination).unwrap();
        assert_eq!(
            fs::read(destination.join("claude")).unwrap(),
            b"claude-binary"
        );
    }

    #[cfg(unix)]
    #[test]
    fn unix_install_replaces_the_binary_atomically() {
        let directory = tempfile::tempdir().unwrap();
        let installed = directory.path().join("borg");
        let candidate = directory.path().join("candidate");
        fs::write(&installed, b"old").unwrap();
        fs::write(&candidate, b"new").unwrap();
        install_candidate_at(&candidate, &installed).unwrap();
        assert_eq!(fs::read(installed).unwrap(), b"new");
    }

    #[cfg(unix)]
    #[test]
    fn unix_install_replaces_the_native_provider_atomically() {
        let directory = tempfile::tempdir().unwrap();
        let installed = directory.path().join("borg");
        let provider = directory.path().join("staged-provider");
        fs::write(&installed, b"borg").unwrap();
        fs::create_dir(&provider).unwrap();
        fs::write(provider.join("claude"), b"new-claude").unwrap();
        fs::write(provider.join("manifest.json"), b"{}").unwrap();
        fs::write(provider.join("package.json"), b"{}").unwrap();
        install_native_provider_to(&provider, &directory.path().join("providers/claude")).unwrap();
        let installed_provider = directory.path().join("providers/claude");
        assert_eq!(
            fs::read(installed_provider.join("claude")).unwrap(),
            b"new-claude"
        );
    }
}
