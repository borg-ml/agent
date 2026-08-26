use std::fs::{self, File, OpenOptions};
use std::io;
#[cfg(windows)]
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use futures_util::StreamExt;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tempfile::TempPath;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};
use tokio::time::{Instant, sleep, timeout};

const DEFAULT_LOCAL_DICTATION_BASE_URL: &str = "http://127.0.0.1:5092";
const DEFAULT_DICTATION_MODEL: &str = "whisper-1";
const PARAKEET_MODEL_NAME: &str = "parakeet-tdt-0.6b-v2";
const PARAKEET_CACHE_KEY: &str = "parakeet-v2";
const PARAKEET_MODEL_FILE: &str = "tdt-0.6b-v2-q4_k.gguf";
const PARAKEET_MODEL_URL: &str = "https://huggingface.co/mudler/parakeet-cpp-gguf/resolve/5dd91ce04815f74f50f86a4ceb56a2a76fd46bf4/tdt-0.6b-v2-q4_k.gguf";
const PARAKEET_MODEL_SIZE: u64 = 638_373_152;
const PARAKEET_MODEL_SHA256: &str =
    "417e8a8e994ec4bcce7010ab1e205f8b88291a4535ddd3152d24d0e19517bfc8";
const DICTATION_TIMEOUT: Duration = Duration::from_secs(120);
const DICTATION_SETUP_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const DICTATION_STARTUP_TIMEOUT: Duration = Duration::from_secs(120);
const DICTATION_SETUP_POLL_INTERVAL: Duration = Duration::from_millis(250);
const DICTATION_CACHE_LOCK_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const DICTATION_CACHE_LOCK_STALE_AFTER: Duration = Duration::from_secs(2 * 60 * 60);
const RECORDER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_RECORDER_STDERR_BYTES: usize = 16 * 1024;
const MAX_AUDIO_BYTES: u64 = 128 * 1024 * 1024;
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy)]
struct RuntimeAsset {
    archive_name: &'static str,
    sha256: &'static str,
    size: u64,
}

struct InstalledDictation {
    server_bin: PathBuf,
    model_path: PathBuf,
}

#[derive(Clone)]
pub(crate) struct LocalDictationConfig {
    base_url: String,
    model: String,
    api_key: Option<String>,
    record_command: Option<String>,
    base_url_explicit: bool,
    model_explicit: bool,
    auto_setup: bool,
}

impl LocalDictationConfig {
    pub(crate) fn from_env() -> Self {
        let base_url = env_value("BORG_CLI_DICTATION_BASE_URL")
            .or_else(|| env_value("BORG_DICTATION_BASE_URL"));
        let model =
            env_value("BORG_CLI_DICTATION_MODEL").or_else(|| env_value("BORG_DICTATION_MODEL"));
        Self {
            base_url: base_url
                .clone()
                .unwrap_or_else(|| DEFAULT_LOCAL_DICTATION_BASE_URL.to_string())
                .trim_end_matches('/')
                .to_string(),
            model: model
                .clone()
                .unwrap_or_else(|| DEFAULT_DICTATION_MODEL.to_string()),
            api_key: env_value("BORG_CLI_DICTATION_API_KEY")
                .or_else(|| env_value("BORG_DICTATION_API_KEY")),
            record_command: env_value("BORG_CLI_DICTATION_RECORD_COMMAND"),
            base_url_explicit: base_url.is_some(),
            model_explicit: model.is_some(),
            auto_setup: env_bool("BORG_CLI_DICTATION_AUTO_SETUP").unwrap_or(true),
        }
    }

    pub(crate) fn requires_setup(&self) -> bool {
        self.auto_setup && !self.base_url_explicit && !self.model_explicit && self.api_key.is_none()
    }

    fn for_managed_backend(&self) -> Self {
        Self {
            base_url: DEFAULT_LOCAL_DICTATION_BASE_URL.to_string(),
            model: PARAKEET_MODEL_NAME.to_string(),
            api_key: None,
            record_command: self.record_command.clone(),
            base_url_explicit: false,
            model_explicit: false,
            auto_setup: true,
        }
    }
}

pub(crate) struct LocalDictationBackend {
    config: LocalDictationConfig,
    _service: Option<LocalDictationService>,
}

impl LocalDictationBackend {
    pub(crate) fn config(&self) -> LocalDictationConfig {
        self.config.clone()
    }
}

struct LocalDictationService {
    _child: Child,
}

/// Prepare a local Parakeet service when Borg is using its default dictation
/// endpoint. Explicit endpoint/model settings remain externally managed.
pub(crate) async fn ensure_backend(config: LocalDictationConfig) -> Result<LocalDictationBackend> {
    if !config.requires_setup() {
        return Ok(LocalDictationBackend {
            config,
            _service: None,
        });
    }

    if endpoint_reachable(&config.base_url).await {
        return Ok(LocalDictationBackend {
            config,
            _service: None,
        });
    }

    let installed = timeout(DICTATION_SETUP_TIMEOUT, ensure_installed())
        .await
        .context("timed out installing the local Parakeet dictation backend")??;
    let managed_config = config.for_managed_backend();
    let mut child = Command::new(&installed.server_bin)
        .arg("--model")
        .arg(&installed.model_path)
        .arg("--host")
        .arg("127.0.0.1")
        .arg("--port")
        .arg("5092")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| {
            format!(
                "failed to start the local Parakeet server {}",
                installed.server_bin.display()
            )
        })?;

    if let Err(error) = wait_for_endpoint(&managed_config.base_url, &mut child).await {
        child.kill().await.ok();
        child.wait().await.ok();
        return Err(error);
    }

    Ok(LocalDictationBackend {
        config: managed_config,
        _service: Some(LocalDictationService { _child: child }),
    })
}

async fn ensure_installed() -> Result<InstalledDictation> {
    let asset = runtime_asset()?;
    let cache_dir = dictation_cache_dir()?;
    fs::create_dir_all(&cache_dir)
        .with_context(|| format!("failed to create dictation cache {}", cache_dir.display()))?;
    let _lock = CacheLock::acquire(&cache_dir).await?;

    let archive_path = cache_dir.join(asset.archive_name);
    let server_dir = cache_dir.join("runtime");
    let server_bin = find_file(&server_dir, runtime_binary_name())?;
    if server_bin.is_none() {
        let runtime_url = format!("{PARAKEET_RUNTIME_URL_PREFIX}{}", asset.archive_name);
        download_verified(
            &runtime_url,
            &archive_path,
            asset.size,
            asset.sha256,
            "Parakeet runtime",
        )
        .await?;
        extract_runtime(&archive_path, &server_dir)?;
    }
    let server_bin = find_file(&server_dir, runtime_binary_name())?
        .context("the downloaded Parakeet runtime did not contain parakeet-server")?;

    let model_path = cache_dir.join(PARAKEET_MODEL_FILE);
    download_verified(
        PARAKEET_MODEL_URL,
        &model_path,
        PARAKEET_MODEL_SIZE,
        PARAKEET_MODEL_SHA256,
        "Parakeet V2 model",
    )
    .await?;

    Ok(InstalledDictation {
        server_bin,
        model_path,
    })
}

const PARAKEET_RUNTIME_URL_PREFIX: &str =
    "https://github.com/mudler/parakeet.cpp/releases/download/v0.5.0/";

#[allow(unreachable_code)]
fn runtime_asset() -> Result<RuntimeAsset> {
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    {
        return Ok(RuntimeAsset {
            archive_name: "parakeet-v0.5.0-bin-linux-cpu-x64.tar.gz",
            sha256: "636a9fc48ac023096037790f9b77d7e5043b200dd6399ec0438bd648c35d79b9",
            size: 2_103_219,
        });
    }
    #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
    {
        return Ok(RuntimeAsset {
            archive_name: "parakeet-v0.5.0-bin-linux-cpu-arm64.tar.gz",
            sha256: "a7c9064c64b84f6b041252d5d2334d4a47693636e9c7c6ab2c535fcef11cf88b",
            size: 1_931_531,
        });
    }
    #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
    {
        return Ok(RuntimeAsset {
            archive_name: "parakeet-v0.5.0-bin-macos-cpu-x64.tar.gz",
            sha256: "7acddf9cc47684f6e3fba54d50768f8b301947fcb6a9ec65c64443704cc4896f",
            size: 2_159_847,
        });
    }
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    {
        return Ok(RuntimeAsset {
            archive_name: "parakeet-v0.5.0-bin-macos-metal-arm64.tar.gz",
            sha256: "819999afb74cfcbb2c8bf4cfff398ef35616c016bca1a311e0ef9660bb4708ee",
            size: 2_128_797,
        });
    }
    #[cfg(all(target_os = "windows", target_arch = "x86_64"))]
    {
        return Ok(RuntimeAsset {
            archive_name: "parakeet-v0.5.0-bin-win-cpu-x64.zip",
            sha256: "df25af4095807d83957f6e135950120e7954fd2d4aca8ad0a5de248ada6287e0",
            size: 1_421_017,
        });
    }
    bail!("automatic Parakeet dictation setup is not supported on this platform")
}

#[cfg(windows)]
fn runtime_binary_name() -> &'static str {
    "parakeet-server.exe"
}

#[cfg(not(windows))]
fn runtime_binary_name() -> &'static str {
    "parakeet-server"
}

#[cfg(any(unix, windows))]
fn dictation_cache_dir() -> Result<PathBuf> {
    dirs::cache_dir()
        .map(|path| path.join("borg").join("dictation").join(PARAKEET_CACHE_KEY))
        .context("unable to determine a cache directory for local dictation")
}

pub(crate) fn parakeet_is_installed() -> bool {
    let Ok(cache_dir) = dictation_cache_dir() else {
        return false;
    };
    let model_path = cache_dir.join(PARAKEET_MODEL_FILE);
    let marker_path = verification_marker(&model_path);
    fs::metadata(&model_path).is_ok_and(|metadata| metadata.len() == PARAKEET_MODEL_SIZE)
        && fs::read_to_string(marker_path)
            .is_ok_and(|marker| marker.trim() == PARAKEET_MODEL_SHA256)
        && find_file(&cache_dir.join("runtime"), runtime_binary_name())
            .is_ok_and(|path| path.is_some())
}

#[cfg(not(any(unix, windows)))]
fn dictation_cache_dir() -> Result<PathBuf> {
    bail!("automatic Parakeet dictation setup is not supported on this platform")
}

async fn endpoint_reachable(base_url: &str) -> bool {
    let Ok(client) = reqwest::Client::builder()
        .timeout(Duration::from_secs(1))
        .build()
    else {
        return false;
    };
    endpoint_reachable_with_client(&client, base_url).await
}

async fn endpoint_reachable_with_client(client: &reqwest::Client, base_url: &str) -> bool {
    client
        .get(format!("{base_url}/v1/audio/transcriptions"))
        .send()
        .await
        .is_ok()
}

async fn wait_for_endpoint(base_url: &str, child: &mut Child) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(1))
        .build()
        .context("failed to create the local Parakeet health client")?;
    let deadline = Instant::now() + DICTATION_STARTUP_TIMEOUT;
    loop {
        if let Some(status) = child
            .try_wait()
            .context("failed to inspect the local Parakeet server")?
        {
            bail!("local Parakeet server exited before becoming ready (status {status})");
        }
        if endpoint_reachable_with_client(&client, base_url).await {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("local Parakeet server did not become ready before the startup timeout");
        }
        sleep(DICTATION_SETUP_POLL_INTERVAL).await;
    }
}

struct CacheLock {
    path: PathBuf,
}

impl CacheLock {
    async fn acquire(cache_dir: &Path) -> Result<Self> {
        let path = cache_dir.join(".install.lock");
        let deadline = Instant::now() + DICTATION_CACHE_LOCK_TIMEOUT;
        loop {
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(file) => {
                    drop(file);
                    return Ok(Self { path });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    if fs::metadata(&path)
                        .and_then(|metadata| metadata.modified())
                        .and_then(|modified| modified.elapsed().map_err(io::Error::other))
                        .is_ok_and(|age| age > DICTATION_CACHE_LOCK_STALE_AFTER)
                    {
                        let _ = fs::remove_file(&path);
                        continue;
                    }
                    if Instant::now() >= deadline {
                        bail!("another Borg process is still installing Parakeet");
                    }
                    sleep(DICTATION_SETUP_POLL_INTERVAL).await;
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("failed to acquire dictation cache lock {}", path.display())
                    });
                }
            }
        }
    }
}

impl Drop for CacheLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

async fn download_verified(
    url: &str,
    destination: &Path,
    expected_size: u64,
    expected_sha256: &str,
    label: &str,
) -> Result<()> {
    let marker = verification_marker(destination);
    if cache_marker_matches(destination, &marker, expected_size, expected_sha256).await? {
        return Ok(());
    }
    if file_matches(destination, expected_size, expected_sha256).await? {
        write_verification_marker(&marker, expected_sha256).await?;
        return Ok(());
    }
    let _ = tokio::fs::remove_file(destination).await;
    let _ = tokio::fs::remove_file(&marker).await;
    let partial = PathBuf::from(format!("{}.part", destination.display()));
    let _ = tokio::fs::remove_file(&partial).await;
    let result = download_to_partial(url, &partial, expected_size, label).await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&partial).await;
    }
    result?;
    ensure_file_hash(&partial, expected_size, expected_sha256, label).await?;
    tokio::fs::rename(&partial, destination)
        .await
        .with_context(|| format!("failed to install {label} into {}", destination.display()))?;
    write_verification_marker(&marker, expected_sha256).await?;
    Ok(())
}

fn verification_marker(path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.sha256", path.display()))
}

async fn cache_marker_matches(
    path: &Path,
    marker: &Path,
    expected_size: u64,
    expected_sha256: &str,
) -> Result<bool> {
    let Ok(metadata) = tokio::fs::metadata(path).await else {
        return Ok(false);
    };
    if metadata.len() != expected_size {
        return Ok(false);
    }
    let Ok(contents) = tokio::fs::read_to_string(marker).await else {
        return Ok(false);
    };
    Ok(contents.trim().eq_ignore_ascii_case(expected_sha256))
}

async fn write_verification_marker(marker: &Path, expected_sha256: &str) -> Result<()> {
    tokio::fs::write(marker, format!("{expected_sha256}\n"))
        .await
        .with_context(|| {
            format!(
                "failed to write cache verification marker {}",
                marker.display()
            )
        })
}

async fn download_to_partial(
    url: &str,
    destination: &Path,
    expected_size: u64,
    label: &str,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(DICTATION_SETUP_TIMEOUT)
        .build()
        .context("failed to create the dictation download client")?;
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("failed to download {label}"))?;
    ensure!(
        response.status().is_success(),
        "failed to download {label}: HTTP {}",
        response.status()
    );
    if let Some(length) = response.content_length() {
        ensure!(
            length == expected_size,
            "downloaded {label} has unexpected size: {length} bytes"
        );
    }
    let mut output = tokio::fs::File::create(destination)
        .await
        .with_context(|| format!("failed to create partial {label} download"))?;
    let mut stream = response.bytes_stream();
    let mut size = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.with_context(|| format!("failed while downloading {label}"))?;
        size = size.saturating_add(chunk.len() as u64);
        ensure!(
            size <= expected_size,
            "downloaded {label} exceeds the expected size"
        );
        output.write_all(&chunk).await?;
    }
    output.flush().await?;
    output.sync_all().await?;
    ensure!(
        size == expected_size,
        "downloaded {label} is incomplete: expected {expected_size} bytes, got {size}"
    );
    Ok(())
}

async fn file_matches(path: &Path, expected_size: u64, expected_sha256: &str) -> Result<bool> {
    if !tokio::fs::try_exists(path).await? {
        return Ok(false);
    }
    match ensure_file_hash(
        path,
        expected_size,
        expected_sha256,
        "cached dictation file",
    )
    .await
    {
        Ok(()) => Ok(true),
        Err(_) => Ok(false),
    }
}

async fn ensure_file_hash(
    path: &Path,
    expected_size: u64,
    expected_sha256: &str,
    label: &str,
) -> Result<()> {
    let metadata = tokio::fs::metadata(path)
        .await
        .with_context(|| format!("failed to inspect {label}"))?;
    ensure!(
        metadata.len() == expected_size,
        "{label} has unexpected size: {} bytes",
        metadata.len()
    );
    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("failed to open {label}"))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let actual = format!("{:x}", hasher.finalize());
    ensure!(
        actual.eq_ignore_ascii_case(expected_sha256),
        "{label} failed SHA-256 verification"
    );
    Ok(())
}

fn find_file(root: &Path, name: &str) -> Result<Option<PathBuf>> {
    if !root.is_dir() {
        return Ok(None);
    }
    for entry in fs::read_dir(root).with_context(|| format!("failed to read {}", root.display()))? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            if let Some(found) = find_file(&path, name)? {
                return Ok(Some(found));
            }
        } else if path.file_name().and_then(|name| name.to_str()) == Some(name) {
            return Ok(Some(path));
        }
    }
    Ok(None)
}

fn extract_runtime(archive_path: &Path, runtime_dir: &Path) -> Result<()> {
    let staging_dir = runtime_dir.with_extension("partial");
    if staging_dir.exists() {
        fs::remove_dir_all(&staging_dir)
            .with_context(|| format!("failed to clear {}", staging_dir.display()))?;
    }
    fs::create_dir_all(&staging_dir)
        .with_context(|| format!("failed to create {}", staging_dir.display()))?;

    #[cfg(unix)]
    {
        let file = File::open(archive_path)
            .with_context(|| format!("failed to open {}", archive_path.display()))?;
        let decoder = flate2::read::GzDecoder::new(file);
        let mut archive = tar::Archive::new(decoder);
        for entry in archive.entries()? {
            let mut entry = entry?;
            let path = entry.path()?.into_owned();
            ensure_safe_archive_path(&path)?;
            entry.unpack(staging_dir.join(path))?;
        }
    }

    #[cfg(windows)]
    {
        let file = File::open(archive_path)
            .with_context(|| format!("failed to open {}", archive_path.display()))?;
        let mut archive = zip::ZipArchive::new(file).context("failed to read Parakeet zip")?;
        for index in 0..archive.len() {
            let mut entry = archive.by_index(index)?;
            let path = Path::new(entry.name());
            ensure_safe_archive_path(path)?;
            let destination = staging_dir.join(path);
            if entry.is_dir() {
                fs::create_dir_all(destination)?;
                continue;
            }
            if let Some(parent) = destination.parent() {
                fs::create_dir_all(parent)?;
            }
            let mut output = File::create(destination)?;
            io::copy(&mut entry, &mut output)?;
            output.flush()?;
        }
    }

    ensure!(
        find_file(&staging_dir, runtime_binary_name())?.is_some(),
        "the downloaded Parakeet archive did not contain {}",
        runtime_binary_name()
    );
    if runtime_dir.exists() {
        fs::remove_dir_all(runtime_dir)
            .with_context(|| format!("failed to replace {}", runtime_dir.display()))?;
    }
    fs::rename(&staging_dir, runtime_dir)
        .with_context(|| format!("failed to install runtime into {}", runtime_dir.display()))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let server = find_file(runtime_dir, runtime_binary_name())?
            .context("installed runtime binary disappeared")?;
        fs::set_permissions(server, fs::Permissions::from_mode(0o755))?;
    }
    Ok(())
}

fn ensure_safe_archive_path(path: &Path) -> Result<()> {
    ensure!(
        !path.is_absolute()
            && !path.components().any(|component| {
                matches!(
                    component,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            }),
        "Parakeet runtime archive contains an unsafe path"
    );
    Ok(())
}

pub(crate) struct LocalDictationRecorder {
    child: Child,
    audio_path: TempPath,
    stderr_task: tokio::task::JoinHandle<String>,
}

impl LocalDictationRecorder {
    pub(crate) fn start(config: &LocalDictationConfig) -> Result<Self> {
        let audio = recording_tempfile()?;
        let audio_path = audio.into_temp_path();
        let mut command = recorder_command(config, audio_path.as_ref())?;
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .context(
                "failed to start local microphone recorder; install ffmpeg or set \
                 BORG_CLI_DICTATION_RECORD_COMMAND with an {output} placeholder",
            )?;
        let stderr = child
            .stderr
            .take()
            .context("microphone recorder stderr pipe was unavailable")?;
        let stderr_task = tokio::spawn(read_recorder_stderr(stderr));
        Ok(Self {
            child,
            audio_path,
            stderr_task,
        })
    }

    pub(crate) async fn finish_and_transcribe(
        mut self,
        config: LocalDictationConfig,
    ) -> Result<String> {
        if let Some(mut stdin) = self.child.stdin.take() {
            stdin.write_all(b"q\n").await.ok();
            stdin.shutdown().await.ok();
        }
        let status = match tokio::time::timeout(RECORDER_SHUTDOWN_TIMEOUT, self.child.wait()).await
        {
            Ok(status) => status.context("failed waiting for microphone recorder")?,
            Err(_) => {
                self.child.kill().await.ok();
                self.child
                    .wait()
                    .await
                    .context("failed to stop microphone recorder")?
            }
        };
        let stderr = self.stderr_task.await.unwrap_or_default();
        if !status.success() {
            bail!("{}", recorder_failure_message(status, &stderr));
        }
        transcribe(&config, self.audio_path.as_ref()).await
    }
}

fn recording_tempfile() -> Result<tempfile::NamedTempFile> {
    let cache_result = dictation_cache_dir().and_then(|cache_dir| {
        let recordings_dir = cache_dir.join("recordings");
        fs::create_dir_all(&recordings_dir).with_context(|| {
            format!(
                "failed to create dictation recording directory {}",
                recordings_dir.display()
            )
        })?;
        tempfile::Builder::new()
            .prefix("borg-dictation-")
            .suffix(".wav")
            .tempfile_in(&recordings_dir)
            .with_context(|| format!("failed to create recording in {}", recordings_dir.display()))
    });
    match cache_result {
        Ok(audio) => Ok(audio),
        Err(cache_error) => {
            let audio = tempfile::Builder::new()
                .prefix("borg-dictation-")
                .suffix(".wav")
                .tempfile();
            audio.with_context(|| {
                format!(
                    "failed to create temporary dictation audio (cache attempt failed: {cache_error:#})"
                )
            })
        }
    }
}

async fn read_recorder_stderr(mut reader: impl AsyncRead + Unpin) -> String {
    let mut output = Vec::new();
    let mut truncated = false;
    let mut buffer = [0_u8; 4096];
    while let Ok(read) = reader.read(&mut buffer).await {
        if read == 0 {
            break;
        }
        let remaining = MAX_RECORDER_STDERR_BYTES.saturating_sub(output.len());
        let keep = remaining.min(read);
        output.extend_from_slice(&buffer[..keep]);
        truncated |= keep < read;
    }
    let mut output = String::from_utf8_lossy(&output).into_owned();
    if truncated {
        output.push_str(" [stderr truncated]");
    }
    output
}

fn recorder_failure_message(status: ExitStatus, stderr: &str) -> String {
    let detail = stderr.split_whitespace().collect::<Vec<_>>().join(" ");
    if detail.is_empty() {
        return format!("microphone recording failed with {status}");
    }
    let detail = detail.chars().take(512).collect::<String>();
    format!("microphone recording failed with {status}: {detail}")
}

fn recorder_command(config: &LocalDictationConfig, output: &std::path::Path) -> Result<Command> {
    if let Some(template) = config.record_command.as_deref() {
        let mut parts = shlex::split(template)
            .context("BORG_CLI_DICTATION_RECORD_COMMAND contains invalid shell-style quoting")?;
        anyhow::ensure!(
            parts.iter().any(|part| part.contains("{output}")),
            "BORG_CLI_DICTATION_RECORD_COMMAND must contain an {{output}} placeholder"
        );
        for part in &mut parts {
            *part = part.replace("{output}", &output.to_string_lossy());
        }
        let program = parts
            .first()
            .context("BORG_CLI_DICTATION_RECORD_COMMAND cannot be empty")?
            .clone();
        let mut command = Command::new(program);
        command.args(&parts[1..]);
        return Ok(command);
    }

    let mut command = Command::new("ffmpeg");
    command.args(["-hide_banner", "-loglevel", "error", "-y"]);
    #[cfg(target_os = "linux")]
    command.args(["-f", "pulse", "-i", "default"]);
    #[cfg(target_os = "macos")]
    command.args(["-f", "avfoundation", "-i", ":0"]);
    #[cfg(target_os = "windows")]
    command.args(["-f", "dshow", "-i", "audio=default"]);
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    bail!(
        "automatic microphone capture is not supported on this platform; set \
         BORG_CLI_DICTATION_RECORD_COMMAND with an {{output}} placeholder"
    );
    command.args(["-ac", "1", "-ar", "16000", "-c:a", "pcm_s16le"]);
    command.arg(output);
    Ok(command)
}

async fn transcribe(config: &LocalDictationConfig, path: &std::path::Path) -> Result<String> {
    let metadata = tokio::fs::metadata(path)
        .await
        .context("failed to inspect recorded dictation audio")?;
    anyhow::ensure!(metadata.len() > 44, "dictation recording is empty");
    anyhow::ensure!(
        metadata.len() <= MAX_AUDIO_BYTES,
        "dictation recording exceeds {} MiB",
        MAX_AUDIO_BYTES / (1024 * 1024)
    );
    let bytes = tokio::fs::read(path)
        .await
        .context("failed to read recorded dictation audio")?;
    let part = reqwest::multipart::Part::bytes(bytes)
        .file_name("dictation.wav")
        .mime_str("audio/wav")
        .context("failed to prepare dictation audio")?;
    let form = reqwest::multipart::Form::new()
        .part("file", part)
        .text("model", config.model.clone())
        .text("response_format", "json");
    let client = reqwest::Client::builder()
        .timeout(DICTATION_TIMEOUT)
        .build()
        .context("failed to create local dictation client")?;
    let mut request = client
        .post(format!("{}/v1/audio/transcriptions", config.base_url))
        .multipart(form);
    if let Some(api_key) = config.api_key.as_deref() {
        request = request.bearer_auth(api_key);
    }
    let response = request.send().await.with_context(|| {
        format!(
            "local dictation model is unavailable at {}; retry dictation setup or set BORG_CLI_DICTATION_BASE_URL",
            config.base_url
        )
    })?;
    let status = response.status();
    let body = response
        .bytes()
        .await
        .context("failed to read local dictation response")?;
    anyhow::ensure!(
        body.len() <= MAX_RESPONSE_BYTES,
        "local dictation response is too large"
    );
    let body = String::from_utf8_lossy(&body);
    anyhow::ensure!(
        status.is_success(),
        "local dictation model returned {status}: {}",
        body.trim()
    );
    Ok(transcription_response_text(&body))
}

fn transcription_response_text(body: &str) -> String {
    if serde_json::from_str::<Value>(body).is_ok() {
        transcription_text(body).unwrap_or_default()
    } else {
        body.trim().to_string()
    }
}

fn transcription_text(body: &str) -> Option<String> {
    serde_json::from_str::<Value>(body).ok().and_then(|value| {
        value
            .get("text")
            .or_else(|| value.get("transcription"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(str::to_string)
    })
}

fn env_value(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_bool(name: &str) -> Option<bool> {
    match env_value(name)?.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{
        LocalDictationConfig, LocalDictationRecorder, ensure_safe_archive_path,
        transcription_response_text, transcription_text,
    };

    fn default_config() -> LocalDictationConfig {
        LocalDictationConfig {
            base_url: "http://127.0.0.1:5092".to_string(),
            model: "whisper-1".to_string(),
            api_key: None,
            record_command: None,
            base_url_explicit: false,
            model_explicit: false,
            auto_setup: true,
        }
    }

    #[test]
    fn explicit_dictation_settings_skip_automatic_setup() {
        let mut config = default_config();
        assert!(config.requires_setup());

        config.base_url_explicit = true;
        assert!(!config.requires_setup());

        config.base_url_explicit = false;
        config.model_explicit = true;
        assert!(!config.requires_setup());
    }

    #[test]
    fn runtime_archive_paths_cannot_escape_the_cache() {
        assert!(ensure_safe_archive_path(Path::new("runtime/parakeet-server")).is_ok());
        assert!(ensure_safe_archive_path(Path::new("../parakeet-server")).is_err());
        assert!(ensure_safe_archive_path(Path::new("runtime/../../parakeet-server")).is_err());
    }

    #[test]
    fn transcription_response_accepts_supported_local_shapes() {
        assert_eq!(
            transcription_text(r#"{"text":"  hello borg  "}"#).as_deref(),
            Some("hello borg")
        );
        assert_eq!(
            transcription_text(r#"{"transcription":"hello"}"#).as_deref(),
            Some("hello")
        );
        assert_eq!(transcription_text(r#"{"text":""}"#), None);
        assert_eq!(transcription_response_text(r#"{"text":""}"#), "");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn recorder_failure_includes_the_child_diagnostic() {
        let mut config = default_config();
        config.record_command =
            Some("sh -c 'printf recorder-disk-quota-failure >&2; exit 134' {output}".to_string());
        let recorder = LocalDictationRecorder::start(&config).expect("recorder starts");
        let recordings_dir = super::dictation_cache_dir()
            .expect("unix tests have a user cache directory")
            .join("recordings");
        assert!(
            recorder.audio_path.starts_with(&recordings_dir),
            "recording path should avoid the shared temp directory: {}",
            recorder.audio_path.display()
        );
        let error = recorder
            .finish_and_transcribe(config)
            .await
            .expect_err("recorder should fail");
        let message = error.to_string();
        assert!(message.contains("134"), "{message}");
        assert!(message.contains("recorder-disk-quota-failure"), "{message}");
    }
}
