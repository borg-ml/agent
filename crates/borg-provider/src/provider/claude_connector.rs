//! One authenticated model helper per host credential authority, shared across
//! Borg processes. The helper has no conversation or agent lifecycle state.

use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, OnceLock},
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

const VERSION: &str = "2.1.281";
const PROTOCOL: u32 = 1;
const SCRIPT: &str = include_str!("claude_connector.js");
const RELEASE_ROOT: &str = "https://storage.googleapis.com/claude-code-dist-86c565f3-f756-42ad-8dfa-d59b1c096819/claude-code-releases";
pub(super) const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
pub(super) const MAX_RESPONSE_BYTES: usize = 128 * 1024 * 1024;

/// Contains a local transport credential. Never Debug or include in a trace.
#[derive(Clone, Serialize, Deserialize)]
struct Endpoint {
    port: u16,
    pid: u32,
    secret: String,
    revision: String,
}

#[derive(Clone)]
pub(super) struct Connector {
    endpoint: Arc<Endpoint>,
    http: reqwest::Client,
}

#[derive(Debug, Deserialize)]
pub(super) struct ConnectorInfo {
    protocol: u32,
    version: String,
    pub pid: u32,
    pub account_identity: String,
    auth: String,
    agent_loop: bool,
    pub capabilities: Option<Capabilities>,
}

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

/// Canonical credential directory, independent of session/worktree and refresh
/// token rotation. A selected controller authority supplies its persistent path.
pub(super) fn auth_directory(selected: Option<&Path>) -> Result<PathBuf> {
    let directory = selected
        .map(Path::to_path_buf)
        .or_else(|| crate::env::nonempty_var("CLAUDE_CONFIG_DIR").map(PathBuf::from))
        .or_else(|| crate::provider_bin::home_directory().map(|home| home.join(".claude")))
        .context("cannot locate Claude subscription credentials")?;
    directory.canonicalize().with_context(|| {
        format!(
            "cannot open Claude credential authority {}; run claude auth login",
            directory.display()
        )
    })
}

fn host_root() -> Result<PathBuf> {
    Ok(crate::provider_bin::home_directory()
        .context("cannot locate the host's Claude connector directory")?
        .join(".cache/borg/claude-model"))
}

fn private_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(path)?;
        let metadata = fs::symlink_metadata(path)?;
        ensure!(
            metadata.is_dir() && metadata.uid() == unsafe { libc::geteuid() },
            "unsafe Claude connector directory"
        );
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    #[cfg(not(unix))]
    {
        fs::create_dir_all(path)?;
    }
    Ok(())
}

fn private_file(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    Ok(options.open(path)?)
}

fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file =
        tempfile::NamedTempFile::new_in(path.parent().context("missing connector directory")?)?;
    file.write_all(bytes)?;
    file.as_file().sync_all()?;
    file.persist(path).map_err(|error| error.error)?;
    Ok(())
}

async fn lock(path: &Path) -> Result<File> {
    let file = private_file(path)?;
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            match file.try_lock() {
                Ok(()) => return Ok(()),
                Err(std::fs::TryLockError::WouldBlock) => {
                    tokio::time::sleep(Duration::from_millis(25)).await
                }
                Err(error) => return Err(anyhow::anyhow!(error)),
            }
        }
    })
    .await
    .context("timed out waiting for the shared Claude connector")??;
    Ok(file)
}

fn checksum(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(hex::encode(digest.finalize()))
}

fn platform_release() -> Result<(&'static str, &'static str)> {
    // Private module bindings are validated against this exact release. New
    // upstream versions require a reviewed binding update, not just --version.
    match (
        std::env::consts::OS,
        std::env::consts::ARCH,
        cfg!(target_env = "musl"),
    ) {
        ("linux", "x86_64", false) => Ok((
            "linux-x64",
            "56fe3da88458465fb27d7e9299dddb3fead55750fb9c2de795f233b5eea6dce1",
        )),
        _ => bail!("the Claude shared model connector has no validated runtime for this platform"),
    }
}

async fn executable(root: &Path) -> Result<PathBuf> {
    static EXECUTABLE: tokio::sync::OnceCell<PathBuf> = tokio::sync::OnceCell::const_new();
    EXECUTABLE.get_or_try_init(|| async {
        let (platform, expected) = platform_release()?;
        if let Some(pin) = crate::env::nonempty_var("BORG_CLAUDE_BIN") {
            let path = PathBuf::from(pin).canonicalize().context("invalid BORG_CLAUDE_BIN")?;
            let checking = path.clone();
            ensure!(tokio::task::spawn_blocking(move || checksum(&checking)).await?? == expected,
                "BORG_CLAUDE_BIN is incompatible with the shared connector; select the official Claude {VERSION} {platform} binary or unset the override");
            return Ok(path);
        }
        let directory = root.join(VERSION).join(platform);
        private_directory(&directory)?;
        let _lock = lock(&directory.join("install.lock")).await?;
        let path = directory.join("claude");
        if path.exists() {
            let checking = path.clone();
            ensure!(tokio::task::spawn_blocking(move || checksum(&checking)).await?? == expected,
                "the pinned Claude model runtime failed its checksum; remove {} and retry", path.display());
            return Ok(path);
        }
        ensure!(crate::provider_bin::auto_install_enabled(),
            "Claude {VERSION} is required for the shared connector; automatic installation is disabled");
        let mut file = tempfile::NamedTempFile::new_in(&directory)?;
        let response = reqwest::Client::builder().connect_timeout(Duration::from_secs(30)).build()?
            .get(format!("{RELEASE_ROOT}/{VERSION}/{platform}/claude"))
            .timeout(Duration::from_secs(180)).send().await?.error_for_status()?;
        let mut stream = response.bytes_stream();
        let mut count = 0usize;
        let mut digest = Sha256::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            count = count.saturating_add(chunk.len());
            ensure!(count <= 300 * 1024 * 1024, "Claude runtime download exceeds its size limit");
            digest.update(&chunk);
            file.write_all(&chunk)?;
        }
        ensure!(hex::encode(digest.finalize()) == expected, "Claude runtime download failed checksum validation");
        #[cfg(unix)] {
            use std::os::unix::fs::PermissionsExt;
            file.as_file().set_permissions(fs::Permissions::from_mode(0o700))?;
        }
        file.as_file().sync_all()?;
        file.persist(&path).map_err(|error| error.error)?;
        Ok(path)
    }).await.cloned()
}

fn local_http() -> reqwest::Client {
    static HTTP: OnceLock<reqwest::Client> = OnceLock::new();
    HTTP.get_or_init(|| {
        reqwest::Client::builder()
            .no_proxy()
            .connect_timeout(Duration::from_secs(2))
            .build()
            .expect("local HTTP client")
    })
    .clone()
}

impl Connector {
    fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint: Arc::new(endpoint),
            http: local_http(),
        }
    }

    fn post(&self, path: &str) -> reqwest::RequestBuilder {
        self.http
            .post(format!("http://127.0.0.1:{}{path}", self.endpoint.port))
            .bearer_auth(&self.endpoint.secret)
    }

    pub async fn info(&self, model: Option<&str>) -> Result<ConnectorInfo> {
        let response = self
            .post("/info")
            .json(&json!({"model": model}))
            .timeout(Duration::from_secs(45))
            .send()
            .await?
            .error_for_status()?;
        let text = super::read_provider_response_text_with_limit(
            response,
            "Claude connector handshake",
            64 * 1024,
        )
        .await?;
        let info: ConnectorInfo = serde_json::from_str(&text)?;
        ensure!(
            info.protocol == PROTOCOL
                && info.version == VERSION
                && info.pid == self.endpoint.pid
                && info.auth == "subscription_oauth"
                && !info.agent_loop
                && info.account_identity.len() == 64,
            "incompatible Claude connector handshake"
        );
        Ok(info)
    }

    async fn existing(endpoint_path: &Path, revision: &str) -> Result<Option<Self>> {
        if endpoint_path.exists() {
            let file = File::open(endpoint_path)?;
            let bytes = crate::bounded_io::read_open_file_bytes_with_limit(
                endpoint_path,
                "Claude connector endpoint",
                file,
                4096,
            )?;
            if let Ok(endpoint) = serde_json::from_slice::<Endpoint>(&bytes) {
                ensure!(
                    endpoint.revision == revision,
                    "Claude connector revision mismatch"
                );
                let connector = Self::new(endpoint);
                match connector.info(None).await {
                    Ok(_) => return Ok(Some(connector)),
                    // Only an unreachable listener permits replacement. An
                    // auth or compatibility error must not create a peer helper.
                    Err(error)
                        if error
                            .downcast_ref::<reqwest::Error>()
                            .is_some_and(|error| error.is_connect()) => {}
                    Err(error) => {
                        return Err(error).context("shared Claude connector handshake failed");
                    }
                }
            } else {
                bail!(
                    "invalid shared Claude connector endpoint; remove {} and retry",
                    endpoint_path.display()
                );
            }
        }
        Ok(None)
    }

    pub async fn connect(selected: Option<&Path>) -> Result<Self> {
        for name in [
            "ANTHROPIC_API_KEY",
            "ANTHROPIC_AUTH_TOKEN",
            "ANTHROPIC_BASE_URL",
            "ANTHROPIC_CUSTOM_HEADERS",
            "ANTHROPIC_UNIX_SOCKET",
            "ANTHROPIC_BETAS",
            "CLAUDE_CODE_OAUTH_TOKEN",
            "CLAUDE_CODE_OAUTH_TOKEN_FILE_DESCRIPTOR",
            "CCR_OAUTH_TOKEN_FILE",
            "CLAUDE_CODE_USE_BEDROCK",
            "CLAUDE_CODE_USE_VERTEX",
            "CLAUDE_CODE_USE_FOUNDRY",
        ] {
            ensure!(
                crate::env::nonempty_var(name).is_none(),
                "{name} conflicts with the selected Claude subscription login; unset it for this route"
            );
        }
        let authority = auth_directory(selected)?;
        let root = host_root()?;
        private_directory(&root)?;
        let pinned = if crate::env::nonempty_var("BORG_CLAUDE_BIN").is_some() {
            Some(executable(&root).await?)
        } else {
            None
        };
        let revision = hex::encode(Sha256::digest(SCRIPT.as_bytes()));
        let key = hex::encode(Sha256::digest(
            format!("{}\0{VERSION}\0{revision}", authority.display()).as_bytes(),
        ));
        let directory = root.join("authorities").join(key);
        private_directory(&directory)?;
        let endpoint_path = directory.join("endpoint.json");
        let start_lock = private_file(&directory.join("start.lock"))?;
        let existing = tokio::time::timeout(Duration::from_secs(90), async {
            loop {
                if let Some(connector) = Self::existing(&endpoint_path, &revision).await? {
                    return Ok::<_, anyhow::Error>(Some(connector));
                }
                match start_lock.try_lock() {
                    Ok(()) => return Self::existing(&endpoint_path, &revision).await,
                    Err(std::fs::TryLockError::WouldBlock) => {
                        tokio::time::sleep(Duration::from_millis(50)).await
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        })
        .await
        .context("timed out waiting for the shared Claude connector")??;
        if let Some(connector) = existing {
            return Ok(connector);
        }
        let binary = match pinned {
            Some(path) => path,
            None => executable(&root).await?,
        };
        let script = directory.join("connector.js");
        write_private(&script, SCRIPT.as_bytes())?;
        let mut bytes = [0u8; 32];
        getrandom::fill(&mut bytes).context("generate Claude connector transport credential")?;
        let secret = hex::encode(bytes);
        let mut command = Command::new(binary);
        command
            .arg("--version")
            .current_dir(&directory)
            .env("BUN_OPTIONS", "--preload=./connector.js")
            .env("CLAUDE_CODE_ENTRYPOINT", "borg")
            .env("CLAUDE_AGENT_SDK_CLIENT_APP", "borg-model-connector")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        if selected.is_some() {
            command.env("CLAUDE_CONFIG_DIR", &authority);
        }
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            command.process_group(0);
            let descriptor = start_lock.as_raw_fd();
            // The helper holds the same open-file-description lock after this
            // Borg process exits, including a crash during startup.
            unsafe {
                command.pre_exec(move || {
                    if libc::dup2(descriptor, 3) < 0 || libc::fcntl(3, libc::F_SETFD, 0) < 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
        }
        let mut startup = StartupGuard(Some(
            command
                .spawn()
                .context("start shared Claude model connector")?,
        ));
        let child = startup.0.as_mut().expect("starting child");
        let mut input = child
            .stdin
            .take()
            .context("missing Claude connector input")?;
        input
            .write_all(&serde_json::to_vec(
                &json!({"protocol": PROTOCOL, "secret": secret, "revision": revision, "endpoint_path": endpoint_path}),
            )?)
            .await?;
        input.shutdown().await?;
        drop(input);
        let output = child
            .stdout
            .take()
            .context("missing Claude connector output")?;
        let mut line = String::new();
        tokio::time::timeout(
            Duration::from_secs(60),
            BufReader::new(output).take(4096).read_line(&mut line),
        )
        .await
        .context("Claude connector startup timed out")??;
        let ready: Value = serde_json::from_str(&line)
            .context("Claude connector exited before its ready handshake")?;
        ensure!(
            ready["type"] == "ready",
            "Claude connector could not start: {}",
            ready["message"].as_str().unwrap_or("incompatible runtime")
        );
        let endpoint = Endpoint {
            port: ready["port"]
                .as_u64()
                .and_then(|port| u16::try_from(port).ok())
                .context("invalid connector port")?,
            pid: child.id().context("Claude connector already exited")?,
            secret,
            revision,
        };
        let connector = Self::new(endpoint);
        connector.info(None).await?;
        // Reap the child if this Borg process lives long enough; its independent
        // idle timer retires it even when the launching Borg process exits.
        let mut child = startup.0.take().expect("ready child");
        tokio::spawn(async move {
            let _ = child.wait().await;
        });
        Ok(connector)
    }

    pub async fn infer(&self, request: &Value) -> Result<reqwest::Response> {
        let serialized = serde_json::to_vec(request)?;
        ensure!(
            serialized.len() <= 64 * 1024 * 1024,
            "Claude model request exceeds connector limit"
        );
        Ok(super::apply_provider_request_timeout(
            self.post("/infer")
                .header("content-type", "application/json")
                .body(serialized),
        )
        .send()
        .await?
        .error_for_status()?)
    }

    pub fn cancellation_guard(&self, id: String) -> CancelOnDrop {
        CancelOnDrop {
            connector: self.clone(),
            id,
            armed: true,
        }
    }
}

struct StartupGuard(Option<tokio::process::Child>);
impl Drop for StartupGuard {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            let _ = child.start_kill();
        }
    }
}

pub(super) struct CancelOnDrop {
    connector: Connector,
    id: String,
    armed: bool,
}
impl CancelOnDrop {
    pub fn complete(&mut self) {
        self.armed = false;
    }
}
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if self.armed
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            let request = self
                .connector
                .post("/cancel")
                .json(&json!({"id": self.id}))
                .timeout(Duration::from_secs(2));
            runtime.spawn(async move {
                let _ = request.send().await;
            });
        }
    }
}
