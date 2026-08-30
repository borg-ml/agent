use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant, timeout};
use url::Url;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const LSP_IDLE_TIMEOUT: Duration = Duration::from_secs(2 * 60);
const LSP_REAPER_INTERVAL: Duration = Duration::from_secs(30);
const MAX_LSP_MESSAGE_BYTES: usize = 8 * 1024 * 1024;
const MAX_WORKSPACE_DIAGNOSTIC_FILES: usize = 4096;

#[derive(Clone)]
pub struct LspService {
    root: PathBuf,
    path_policy: LspPathPolicy,
    clients: std::sync::Arc<Mutex<HashMap<LspClientKey, LspClient>>>,
}

#[derive(Clone, Debug)]
pub(crate) enum LspPathPolicy {
    /// Local trusted sessions already have the host's normal filesystem
    /// access, so LSP should not impose a narrower artificial boundary.
    Unrestricted,
    /// Isolated hosts keep LSP inside the attached session workspace.
    SessionWorkspace,
    /// Trusted enrolled hosts may inspect any path inside their enrolled
    /// roots, matching the host's other workspace tools.
    AuthorizedRoots(Vec<PathBuf>),
}

impl LspPathPolicy {
    pub(crate) fn unrestricted() -> Self {
        Self::Unrestricted
    }

    pub(crate) fn session_workspace() -> Self {
        Self::SessionWorkspace
    }

    pub(crate) fn authorized_roots(roots: Vec<PathBuf>) -> Self {
        Self::AuthorizedRoots(roots)
    }

    fn allows(&self, path: &Path, session_root: &Path) -> bool {
        match self {
            Self::Unrestricted => true,
            Self::SessionWorkspace => path.starts_with(session_root),
            Self::AuthorizedRoots(roots) => roots.iter().any(|root| path.starts_with(root)),
        }
    }

    fn scope_root(&self, path: &Path, session_root: &Path) -> Option<PathBuf> {
        match self {
            Self::Unrestricted => None,
            Self::SessionWorkspace => Some(session_root.to_path_buf()),
            Self::AuthorizedRoots(roots) => roots
                .iter()
                .filter(|root| path.starts_with(root))
                .max_by_key(|root| root.components().count())
                .cloned(),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct LspClientKey {
    server_id: &'static str,
    workspace_root: PathBuf,
}

struct ServerSpec {
    id: &'static str,
    command: &'static str,
    args: &'static [&'static str],
    language_id: &'static str,
    extensions: &'static [&'static str],
}

struct LspClient {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
    opened_versions: HashMap<PathBuf, i32>,
    published_diagnostics: HashMap<String, Value>,
    last_used: Instant,
}

impl LspService {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self::with_path_policy(root, LspPathPolicy::unrestricted())
    }

    pub(crate) fn with_path_policy(root: impl Into<PathBuf>, path_policy: LspPathPolicy) -> Self {
        let clients = std::sync::Arc::new(Mutex::new(HashMap::new()));
        spawn_idle_reaper(&clients);
        Self {
            root: root.into(),
            path_policy,
            clients,
        }
    }

    pub async fn status(&self) -> Value {
        let clients = self.clients.lock().await;
        let mut active = clients
            .keys()
            .map(|key| key.server_id)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        active.sort_unstable();
        let mut active_workspaces = clients
            .keys()
            .map(|key| json!({ "server": key.server_id, "root": key.workspace_root }))
            .collect::<Vec<_>>();
        active_workspaces.sort_by_key(|workspace| workspace.to_string());
        json!({
            "root": self.root,
            "active_servers": active,
            "active_workspaces": active_workspaces,
            "supported_servers": supported_server_status()
        })
    }

    pub fn supported_status() -> Value {
        json!(supported_server_status())
    }

    pub async fn diagnostics(&self, path: &Path) -> Result<Value> {
        let (path, uri, spec, workspace_root) = self.resolve_document(path).await?;
        let mut clients = self.clients.lock().await;
        let client = ensure_client(&mut clients, spec, &workspace_root).await?;
        client
            .document_diagnostics(&path, &uri, spec.language_id)
            .await
    }

    pub async fn hover(&self, path: &Path, line: u32, character: u32) -> Result<Value> {
        self.position_request(path, "textDocument/hover", line, character, json!({}))
            .await
    }

    pub async fn definition(&self, path: &Path, line: u32, character: u32) -> Result<Value> {
        self.position_request(path, "textDocument/definition", line, character, json!({}))
            .await
    }

    pub async fn references(&self, path: &Path, line: u32, character: u32) -> Result<Value> {
        self.position_request(
            path,
            "textDocument/references",
            line,
            character,
            json!({ "context": { "includeDeclaration": true } }),
        )
        .await
    }

    pub async fn document_symbols(&self, path: &Path) -> Result<Value> {
        self.document_request(path, "textDocument/documentSymbol", json!({}))
            .await
    }

    pub async fn workspace_symbols(&self, query: &str) -> Result<Value> {
        let mut clients = self.clients.lock().await;
        if clients.is_empty() {
            bail!("no language server is active; inspect a supported source file first");
        }
        let mut server_counts = HashMap::new();
        for key in clients.keys() {
            *server_counts.entry(key.server_id).or_insert(0usize) += 1;
        }
        let mut results = serde_json::Map::new();
        for (key, client) in clients.iter_mut() {
            let value = client
                .request("workspace/symbol", json!({ "query": query }))
                .await
                .with_context(|| {
                    format!(
                        "{} workspace symbol request failed",
                        workspace_label(key, server_counts[key.server_id])
                    )
                })?;
            results.insert(workspace_label(key, server_counts[key.server_id]), value);
        }
        Ok(Value::Object(results))
    }

    /// Request diagnostics for every document known to each active language
    /// server workspace. An optional source path can bootstrap the matching
    /// language server when this service has not been used yet.
    pub async fn workspace_diagnostics(&self, path: Option<&Path>) -> Result<Value> {
        if let Some(path) = path {
            let (path, uri, spec, workspace_root) = self.resolve_document(path).await?;
            let mut clients = self.clients.lock().await;
            let client = ensure_client(&mut clients, spec, &workspace_root).await?;
            client.open_document(&path, &uri, spec.language_id).await?;
        }

        let mut clients = self.clients.lock().await;
        if clients.is_empty() {
            bail!(
                "no language server is active; provide a representative source path to initialize one"
            );
        }
        let mut server_counts = HashMap::new();
        for key in clients.keys() {
            *server_counts.entry(key.server_id).or_insert(0usize) += 1;
        }
        let mut results = serde_json::Map::new();
        for (key, client) in clients.iter_mut() {
            let value = match client.workspace_diagnostics().await {
                Ok(value) => value,
                Err(error) if is_unknown_workspace_diagnostics_request(&error) => {
                    let spec = spec_for_id(key.server_id)
                        .with_context(|| format!("unknown language server `{}`", key.server_id))?;
                    client
                        .document_workspace_diagnostics(&key.workspace_root, spec)
                        .await
                        .with_context(|| {
                            format!(
                                "{} document diagnostics fallback failed ({error:#})",
                                workspace_label(key, server_counts[key.server_id])
                            )
                        })?
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!(
                            "{} workspace diagnostics request failed",
                            workspace_label(key, server_counts[key.server_id])
                        )
                    });
                }
            };
            results.insert(workspace_label(key, server_counts[key.server_id]), value);
        }
        Ok(Value::Object(results))
    }

    async fn document_request(&self, path: &Path, method: &str, extra: Value) -> Result<Value> {
        let (path, uri, spec, workspace_root) = self.resolve_document(path).await?;
        let mut clients = self.clients.lock().await;
        let client = ensure_client(&mut clients, spec, &workspace_root).await?;
        client.open_document(&path, &uri, spec.language_id).await?;
        let mut params = extra.as_object().cloned().unwrap_or_default();
        params.insert("textDocument".to_string(), json!({ "uri": uri }));
        client.request(method, Value::Object(params)).await
    }

    async fn position_request(
        &self,
        path: &Path,
        method: &str,
        line: u32,
        character: u32,
        extra: Value,
    ) -> Result<Value> {
        let (path, uri, spec, workspace_root) = self.resolve_document(path).await?;
        let mut clients = self.clients.lock().await;
        let client = ensure_client(&mut clients, spec, &workspace_root).await?;
        client.open_document(&path, &uri, spec.language_id).await?;
        let mut params = extra.as_object().cloned().unwrap_or_default();
        params.insert("textDocument".to_string(), json!({ "uri": uri }));
        params.insert(
            "position".to_string(),
            json!({
                "line": line.saturating_sub(1),
                "character": character.saturating_sub(1)
            }),
        );
        client.request(method, Value::Object(params)).await
    }

    async fn resolve_document(
        &self,
        requested: &Path,
    ) -> Result<(PathBuf, String, &'static ServerSpec, PathBuf)> {
        let joined = if requested.is_absolute() {
            requested.to_path_buf()
        } else {
            self.root.join(requested)
        };
        let path = tokio::fs::canonicalize(&joined)
            .await
            .with_context(|| format!("cannot resolve {}", joined.display()))?;
        let root = tokio::fs::canonicalize(&self.root)
            .await
            .with_context(|| format!("cannot resolve workspace root {}", self.root.display()))?;
        if !self.path_policy.allows(&path, &root) {
            bail!("LSP path must stay inside an authorized workspace root");
        }
        let spec = spec_for_path(&path).ok_or_else(|| {
            anyhow::anyhow!(
                "no configured language server for {}",
                path.extension()
                    .and_then(|value| value.to_str())
                    .unwrap_or("this file type")
            )
        })?;
        let uri = Url::from_file_path(&path)
            .map_err(|_| anyhow::anyhow!("cannot convert {} to a file URI", path.display()))?
            .to_string();
        let scope_root = self.path_policy.scope_root(&path, &root);
        let fallback_root = if path.starts_with(&root) {
            root.clone()
        } else {
            path.parent().unwrap_or(&path).to_path_buf()
        };
        let workspace_root =
            discover_project_root(&path, scope_root.as_deref(), fallback_root).await;
        Ok((path, uri, spec, workspace_root))
    }
}

impl LspClient {
    async fn start(spec: &ServerSpec, root: &Path) -> Result<Self> {
        let mut child = Command::new(spec.command)
            .args(spec.args)
            .current_dir(root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| {
                format!(
                    "{} is not available; install it on PATH to enable {} LSP support",
                    spec.command, spec.id
                )
            })?;
        let stdin = child
            .stdin
            .take()
            .context("language server stdin unavailable")?;
        let stdout = child
            .stdout
            .take()
            .context("language server stdout unavailable")?;
        let mut client = Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            next_id: 1,
            opened_versions: HashMap::new(),
            published_diagnostics: HashMap::new(),
            last_used: Instant::now(),
        };
        let root_uri = Url::from_directory_path(root)
            .map_err(|_| anyhow::anyhow!("cannot convert workspace root to URI"))?
            .to_string();
        let mut initialize = json!({
            "processId": std::process::id(),
            "rootUri": root_uri,
            "capabilities": {
                "textDocument": {
                    "hover": { "contentFormat": ["markdown", "plaintext"] },
                    "definition": { "linkSupport": true },
                    "documentSymbol": { "hierarchicalDocumentSymbolSupport": true },
                    "diagnostic": {}
                },
                "workspace": { "symbol": { "resolveSupport": { "properties": [] } } }
            },
            "clientInfo": { "name": "borg", "version": env!("CARGO_PKG_VERSION") }
        });
        if let Some(options) = server_initialization_options(spec) {
            initialize["initializationOptions"] = options;
        }
        client.request("initialize", initialize).await?;
        client.notify("initialized", json!({})).await?;
        Ok(client)
    }

    async fn open_document(&mut self, path: &Path, uri: &str, language_id: &str) -> Result<()> {
        let text = tokio::fs::read_to_string(path)
            .await
            .with_context(|| format!("cannot read {}", path.display()))?;
        let version = self.opened_versions.entry(path.to_path_buf()).or_insert(0);
        *version += 1;
        let method = if *version == 1 {
            "textDocument/didOpen"
        } else {
            "textDocument/didChange"
        };
        let params = if *version == 1 {
            json!({
                "textDocument": {
                    "uri": uri,
                    "languageId": language_id,
                    "version": *version,
                    "text": text
                }
            })
        } else {
            json!({
                "textDocument": { "uri": uri, "version": *version },
                "contentChanges": [{ "text": text }]
            })
        };
        self.notify(method, params).await
    }

    async fn close_document(&mut self, path: &Path, uri: &str) -> Result<()> {
        if self.opened_versions.remove(path).is_none() {
            return Ok(());
        }
        self.published_diagnostics.remove(uri);
        self.notify(
            "textDocument/didClose",
            json!({ "textDocument": { "uri": uri } }),
        )
        .await
    }

    async fn document_diagnostics(
        &mut self,
        path: &Path,
        uri: &str,
        language_id: &str,
    ) -> Result<Value> {
        self.open_document(path, uri, language_id).await?;
        match self
            .request(
                "textDocument/diagnostic",
                json!({ "textDocument": { "uri": uri } }),
            )
            .await
        {
            Ok(result) => Ok(result),
            Err(pull_error) => self
                .wait_for_published_diagnostics(uri)
                .await
                .with_context(|| format!("pull diagnostics failed ({pull_error:#})")),
        }
    }

    async fn document_workspace_diagnostics(
        &mut self,
        workspace_root: &Path,
        spec: &ServerSpec,
    ) -> Result<Value> {
        let scan = discover_workspace_documents(workspace_root, spec.extensions).await;
        let mut items = Vec::new();
        for path in scan.paths {
            let uri = Url::from_file_path(&path)
                .map_err(|_| anyhow::anyhow!("cannot convert {} to a file URI", path.display()))?
                .to_string();
            self.close_document(&path, &uri)
                .await
                .with_context(|| format!("failed to reset {}", path.display()))?;
            let report = self
                .document_diagnostics(&path, &uri, spec.language_id)
                .await;
            let close = self.close_document(&path, &uri).await;
            let report =
                report.with_context(|| format!("diagnostics failed for {}", path.display()))?;
            close.with_context(|| format!("failed to close {}", path.display()))?;
            items.push(workspace_document_report(&uri, report));
        }
        let mut result = json!({ "kind": "full", "items": items });
        if scan.truncated {
            result["partial"] = Value::Bool(true);
            result["partialReason"] = Value::String(format!(
                "workspace scan limited to {MAX_WORKSPACE_DIAGNOSTIC_FILES} files"
            ));
        }
        Ok(result)
    }

    async fn workspace_diagnostics(&mut self) -> Result<Value> {
        self.request("workspace/diagnostic", json!({ "previousResultIds": [] }))
            .await
    }

    async fn notify(&mut self, method: &str, params: Value) -> Result<()> {
        self.write_message(&json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params
        }))
        .await
    }

    async fn request(&mut self, method: &str, params: Value) -> Result<Value> {
        let id = self.next_id;
        self.next_id += 1;
        self.write_message(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params
        }))
        .await?;
        timeout(REQUEST_TIMEOUT, async {
            loop {
                let message = self.read_message().await?;
                self.capture_diagnostics(&message);
                if message.get("method").is_some()
                    && let Some(server_request_id) = message.get("id").cloned()
                {
                    self.write_message(&json!({
                        "jsonrpc": "2.0",
                        "id": server_request_id,
                        "result": Value::Null
                    }))
                    .await?;
                    continue;
                }
                if message.get("id").and_then(Value::as_u64) != Some(id) {
                    continue;
                }
                if let Some(error) = message.get("error") {
                    bail!("{method} failed: {error}");
                }
                return Ok(message.get("result").cloned().unwrap_or(Value::Null));
            }
        })
        .await
        .with_context(|| format!("{method} timed out"))?
    }

    fn capture_diagnostics(&mut self, message: &Value) {
        if message.get("method").and_then(Value::as_str) != Some("textDocument/publishDiagnostics")
        {
            return;
        }
        let Some(uri) = message.pointer("/params/uri").and_then(Value::as_str) else {
            return;
        };
        let diagnostics = message
            .pointer("/params/diagnostics")
            .cloned()
            .unwrap_or_else(|| json!([]));
        self.published_diagnostics
            .insert(uri.to_string(), diagnostics);
    }

    async fn wait_for_published_diagnostics(&mut self, uri: &str) -> Result<Value> {
        if let Some(items) = self.published_diagnostics.remove(uri) {
            return Ok(json!({ "kind": "full", "items": items }));
        }
        timeout(Duration::from_secs(3), async {
            loop {
                let message = self.read_message().await?;
                self.capture_diagnostics(&message);
                if let Some(server_request_id) = message.get("id").cloned()
                    && message.get("method").is_some()
                {
                    self.write_message(&json!({
                        "jsonrpc": "2.0",
                        "id": server_request_id,
                        "result": Value::Null
                    }))
                    .await?;
                }
                if let Some(items) = self.published_diagnostics.remove(uri) {
                    return Ok(json!({ "kind": "full", "items": items }));
                }
            }
        })
        .await
        .context("language server did not publish diagnostics")?
    }

    async fn write_message(&mut self, message: &Value) -> Result<()> {
        let body = serde_json::to_vec(message)?;
        self.stdin
            .write_all(format!("Content-Length: {}\r\n\r\n", body.len()).as_bytes())
            .await?;
        self.stdin.write_all(&body).await?;
        self.stdin.flush().await?;
        Ok(())
    }

    async fn read_message(&mut self) -> Result<Value> {
        let mut content_length = None;
        loop {
            let mut header = String::new();
            let bytes = self.stdout.read_line(&mut header).await?;
            if bytes == 0 {
                let status = self.child.try_wait()?;
                bail!("language server closed stdout (status: {status:?})");
            }
            if header == "\r\n" || header == "\n" {
                break;
            }
            if let Some(value) = header
                .strip_prefix("Content-Length:")
                .and_then(|value| value.trim().parse::<usize>().ok())
            {
                content_length = Some(value);
            }
        }
        let length = content_length.context("LSP response omitted Content-Length")?;
        if length > MAX_LSP_MESSAGE_BYTES {
            bail!("LSP response exceeds {} bytes", MAX_LSP_MESSAGE_BYTES);
        }
        let mut body = vec![0; length];
        self.stdout.read_exact(&mut body).await?;
        serde_json::from_slice(&body).context("language server returned invalid JSON")
    }
}

async fn ensure_client<'a>(
    clients: &'a mut HashMap<LspClientKey, LspClient>,
    spec: &'static ServerSpec,
    root: &Path,
) -> Result<&'a mut LspClient> {
    let key = LspClientKey {
        server_id: spec.id,
        workspace_root: root.to_path_buf(),
    };
    if !clients.contains_key(&key) {
        clients.insert(key.clone(), LspClient::start(spec, root).await?);
    }
    let client = clients.get_mut(&key).expect("inserted LSP client");
    client.last_used = Instant::now();
    Ok(client)
}

fn spawn_idle_reaper(clients: &std::sync::Arc<Mutex<HashMap<LspClientKey, LspClient>>>) {
    let clients = std::sync::Arc::downgrade(clients);
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        return;
    };
    runtime.spawn(async move {
        let mut interval = tokio::time::interval(LSP_REAPER_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;
        loop {
            interval.tick().await;
            let Some(clients) = clients.upgrade() else {
                return;
            };
            clients
                .lock()
                .await
                .retain(|key, client| !lsp_client_should_reap(key, client.last_used.elapsed()));
        }
    });
}

fn lsp_client_is_expired(idle_for: Duration) -> bool {
    idle_for >= LSP_IDLE_TIMEOUT
}

fn lsp_client_should_reap(key: &LspClientKey, idle_for: Duration) -> bool {
    lsp_client_is_expired(idle_for) || !key.workspace_root.is_dir()
}

fn server_initialization_options(spec: &ServerSpec) -> Option<Value> {
    (spec.id == "rust-analyzer").then(|| {
        json!({
            "cachePriming": { "enable": false },
            "cargo": { "allTargets": false },
            "checkOnSave": false,
            "check": { "allTargets": false }
        })
    })
}

fn workspace_label(key: &LspClientKey, server_count: usize) -> String {
    if server_count == 1 {
        key.server_id.to_string()
    } else {
        format!("{}@{}", key.server_id, key.workspace_root.display())
    }
}

fn is_unknown_workspace_diagnostics_request(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    message.contains("\"code\":-32601") || message.contains("unknown request")
}

fn workspace_document_report(uri: &str, report: Value) -> Value {
    match report {
        Value::Object(mut report) => {
            report.insert("uri".to_string(), Value::String(uri.to_string()));
            Value::Object(report)
        }
        Value::Array(items) => json!({
            "kind": "full",
            "uri": uri,
            "items": items
        }),
        Value::Null => json!({
            "kind": "full",
            "uri": uri,
            "items": []
        }),
        other => json!({
            "kind": "full",
            "uri": uri,
            "items": [],
            "report": other
        }),
    }
}

struct WorkspaceDocumentScan {
    paths: Vec<PathBuf>,
    truncated: bool,
}

async fn discover_workspace_documents(root: &Path, extensions: &[&str]) -> WorkspaceDocumentScan {
    let mut pending = vec![root.to_path_buf()];
    let mut paths = Vec::new();
    let mut truncated = false;
    while let Some(directory) = pending.pop() {
        let Ok(mut entries) = tokio::fs::read_dir(&directory).await else {
            continue;
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            let Ok(file_type) = entry.file_type().await else {
                continue;
            };
            if file_type.is_dir() {
                if !ignored_workspace_directory(&path) {
                    pending.push(path);
                }
                continue;
            }
            if file_type.is_file()
                && path
                    .extension()
                    .and_then(|extension| extension.to_str())
                    .is_some_and(|extension| {
                        extensions
                            .iter()
                            .any(|candidate| candidate.eq_ignore_ascii_case(extension))
                    })
            {
                if paths.len() == MAX_WORKSPACE_DIAGNOSTIC_FILES {
                    truncated = true;
                    break;
                }
                paths.push(path);
            }
        }
        if truncated {
            break;
        }
    }
    paths.sort();
    WorkspaceDocumentScan { paths, truncated }
}

fn ignored_workspace_directory(path: &Path) -> bool {
    matches!(
        path.file_name().and_then(|name| name.to_str()),
        Some(
            ".git"
                | ".hg"
                | ".svn"
                | "target"
                | "node_modules"
                | "vendor"
                | "build"
                | "dist"
                | ".venv"
                | "__pycache__"
        )
    )
}

async fn discover_project_root(path: &Path, boundary: Option<&Path>, fallback: PathBuf) -> PathBuf {
    let mut current = path.parent().unwrap_or(path).to_path_buf();
    loop {
        if has_project_marker(&current).await {
            return current;
        }
        if boundary.is_some_and(|boundary| current == boundary) {
            break;
        }
        let Some(parent) = current.parent() else {
            break;
        };
        if parent == current {
            break;
        }
        current = parent.to_path_buf();
    }
    boundary.map(Path::to_path_buf).unwrap_or(fallback)
}

async fn has_project_marker(path: &Path) -> bool {
    const PROJECT_MARKERS: &[&str] = &[
        ".git",
        "Cargo.toml",
        "package.json",
        "pyproject.toml",
        "go.mod",
        "CMakeLists.txt",
        "pom.xml",
        "build.gradle",
        "build.gradle.kts",
        "Package.swift",
        "Gemfile",
        "composer.json",
        ".luarc.json",
    ];
    for marker in PROJECT_MARKERS {
        if tokio::fs::metadata(path.join(marker)).await.is_ok() {
            return true;
        }
    }
    false
}

fn spec_for_path(path: &Path) -> Option<&'static ServerSpec> {
    let extension = path.extension()?.to_str()?.to_ascii_lowercase();
    server_specs()
        .iter()
        .find(|spec| spec.extensions.contains(&extension.as_str()))
}

fn spec_for_id(id: &str) -> Option<&'static ServerSpec> {
    server_specs().iter().find(|spec| spec.id == id)
}

fn supported_server_status() -> Vec<Value> {
    server_specs()
        .iter()
        .map(|spec| {
            json!({
                "id": spec.id,
                "command": spec.command,
                "language": spec.language_id,
                "extensions": spec.extensions,
                "available": command_available(spec.command),
            })
        })
        .collect()
}

fn command_available(command: &str) -> bool {
    let path = Path::new(command);
    if path.components().count() > 1 {
        return path.is_file();
    }
    std::env::var_os("PATH").is_some_and(|paths| {
        std::env::split_paths(&paths).any(|directory| directory.join(command).is_file())
    })
}

fn server_specs() -> &'static [ServerSpec] {
    &[
        ServerSpec {
            id: "rust-analyzer",
            command: "rust-analyzer",
            args: &[],
            language_id: "rust",
            extensions: &["rs"],
        },
        ServerSpec {
            id: "typescript-language-server",
            command: "typescript-language-server",
            args: &["--stdio"],
            language_id: "typescript",
            extensions: &["ts", "tsx", "js", "jsx", "mjs", "cjs"],
        },
        ServerSpec {
            id: "pyright",
            command: "pyright-langserver",
            args: &["--stdio"],
            language_id: "python",
            extensions: &["py", "pyi"],
        },
        ServerSpec {
            id: "gopls",
            command: "gopls",
            args: &[],
            language_id: "go",
            extensions: &["go"],
        },
        ServerSpec {
            id: "clangd",
            command: "clangd",
            args: &["--pch-storage=memory"],
            language_id: "cpp",
            extensions: &["c", "h", "cc", "cpp", "cxx", "hpp"],
        },
        ServerSpec {
            id: "jdtls",
            command: "jdtls",
            args: &[],
            language_id: "java",
            extensions: &["java"],
        },
        ServerSpec {
            id: "kotlin-language-server",
            command: "kotlin-language-server",
            args: &[],
            language_id: "kotlin",
            extensions: &["kt", "kts"],
        },
        ServerSpec {
            id: "sourcekit-lsp",
            command: "sourcekit-lsp",
            args: &[],
            language_id: "swift",
            extensions: &["swift"],
        },
        ServerSpec {
            id: "csharp-ls",
            command: "csharp-ls",
            args: &[],
            language_id: "csharp",
            extensions: &["cs"],
        },
        ServerSpec {
            id: "solargraph",
            command: "solargraph",
            args: &["stdio"],
            language_id: "ruby",
            extensions: &["rb", "rake"],
        },
        ServerSpec {
            id: "intelephense",
            command: "intelephense",
            args: &["--stdio"],
            language_id: "php",
            extensions: &["php"],
        },
        ServerSpec {
            id: "lua-language-server",
            command: "lua-language-server",
            args: &[],
            language_id: "lua",
            extensions: &["lua"],
        },
        ServerSpec {
            id: "bash-language-server",
            command: "bash-language-server",
            args: &["start"],
            language_id: "shellscript",
            extensions: &["sh", "bash", "zsh"],
        },
        ServerSpec {
            id: "yaml-language-server",
            command: "yaml-language-server",
            args: &["--stdio"],
            language_id: "yaml",
            extensions: &["yaml", "yml"],
        },
        ServerSpec {
            id: "vscode-json-language-server",
            command: "vscode-json-language-server",
            args: &["--stdio"],
            language_id: "json",
            extensions: &["json", "jsonc"],
        },
        ServerSpec {
            id: "vscode-html-language-server",
            command: "vscode-html-language-server",
            args: &["--stdio"],
            language_id: "html",
            extensions: &["html", "htm"],
        },
        ServerSpec {
            id: "vscode-css-language-server",
            command: "vscode-css-language-server",
            args: &["--stdio"],
            language_id: "css",
            extensions: &["css", "scss", "less"],
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn common_language_servers_are_advertised_before_startup() {
        let supported = LspService::supported_status();
        let languages = supported
            .as_array()
            .expect("server status is an array")
            .iter()
            .filter_map(|server| server.get("language").and_then(Value::as_str))
            .collect::<Vec<_>>();

        for language in [
            "rust",
            "typescript",
            "python",
            "go",
            "cpp",
            "java",
            "kotlin",
            "swift",
            "csharp",
            "ruby",
            "php",
            "lua",
            "shellscript",
            "yaml",
            "json",
            "html",
            "css",
        ] {
            assert!(languages.contains(&language), "missing {language}");
        }
    }

    #[test]
    fn inactive_language_servers_expire_at_the_idle_boundary() {
        assert!(!lsp_client_is_expired(
            LSP_IDLE_TIMEOUT - Duration::from_millis(1)
        ));
        assert!(lsp_client_is_expired(LSP_IDLE_TIMEOUT));
    }

    #[test]
    fn missing_lsp_workspaces_are_reaped_even_when_recently_used() {
        let workspace = tempfile::tempdir().expect("workspace");
        let key = LspClientKey {
            server_id: "rust-analyzer",
            workspace_root: workspace.path().to_path_buf(),
        };
        assert!(!lsp_client_should_reap(&key, Duration::ZERO));

        drop(workspace);
        assert!(lsp_client_should_reap(&key, Duration::ZERO));
    }

    #[test]
    fn rust_analyzer_avoids_eager_workspace_builds() {
        let spec = spec_for_id("rust-analyzer").expect("rust-analyzer spec");
        let options = server_initialization_options(spec).expect("rust-analyzer options");

        assert_eq!(options.pointer("/cachePriming/enable"), Some(&json!(false)));
        assert_eq!(options.pointer("/cargo/allTargets"), Some(&json!(false)));
        assert_eq!(options.get("checkOnSave"), Some(&json!(false)));
        assert_eq!(options.pointer("/check/allTargets"), Some(&json!(false)));
    }

    #[test]
    fn clangd_keeps_preambles_out_of_shared_temporary_storage() {
        let spec = spec_for_id("clangd").expect("clangd spec");

        assert!(spec.args.contains(&"--pch-storage=memory"));
    }

    #[tokio::test]
    async fn trusted_lsp_resolves_external_files_against_their_project_root() {
        let session_root = tempfile::tempdir().expect("session workspace");
        let external_project = tempfile::tempdir().expect("external project");
        let source_dir = external_project.path().join("src/bin");
        tokio::fs::create_dir_all(&source_dir)
            .await
            .expect("create source directory");
        tokio::fs::write(
            external_project.path().join("Cargo.toml"),
            "[package]\nname = \"surf\"\nversion = \"0.1.0\"\n",
        )
        .await
        .expect("write project marker");
        let source = source_dir.join("surf_lab.rs");
        tokio::fs::write(&source, "fn main() {}\n")
            .await
            .expect("write source file");

        let service = LspService::new(session_root.path());
        let (resolved, _, spec, project_root) = service
            .resolve_document(&source)
            .await
            .expect("trusted LSP accepts an external source file");

        assert_eq!(resolved, tokio::fs::canonicalize(&source).await.unwrap());
        assert_eq!(spec.id, "rust-analyzer");
        assert_eq!(
            project_root,
            tokio::fs::canonicalize(external_project.path())
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn restricted_lsp_keeps_external_files_outside_the_session_workspace() {
        let session_root = tempfile::tempdir().expect("session workspace");
        let external_project = tempfile::tempdir().expect("external project");
        let source = external_project.path().join("main.rs");
        tokio::fs::write(&source, "fn main() {}\n")
            .await
            .expect("write source file");

        let service =
            LspService::with_path_policy(session_root.path(), LspPathPolicy::session_workspace());
        let error = match service.resolve_document(&source).await {
            Ok(_) => panic!("restricted LSP must reject an external source file"),
            Err(error) => error,
        };
        assert_eq!(
            error.to_string(),
            "LSP path must stay inside an authorized workspace root"
        );
    }

    #[tokio::test]
    async fn rust_analyzer_answers_a_real_diagnostic_request_when_available() {
        if !Command::new("rust-analyzer")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .is_ok_and(|status| status.success())
        {
            return;
        }
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let result = LspService::new(&root)
            .diagnostics(Path::new("src/lib.rs"))
            .await
            .expect("rust-analyzer diagnostic request");
        assert!(result.is_object() || result.is_array() || result.is_null());

        let workspace = LspService::new(&root)
            .workspace_diagnostics(Some(Path::new("src/lib.rs")))
            .await
            .expect("rust-analyzer workspace diagnostic request");
        let items = workspace["rust-analyzer"]["items"]
            .as_array()
            .expect("workspace diagnostic report items");
        assert!(items.iter().any(|item| {
            item["uri"]
                .as_str()
                .is_some_and(|uri| uri.ends_with("/src/lib.rs"))
        }));
    }

    #[tokio::test]
    async fn clangd_publish_diagnostics_are_returned_when_available() {
        if !Command::new("clangd")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .is_ok_and(|status| status.success())
        {
            return;
        }
        let root = tempfile::tempdir().expect("temp workspace");
        tokio::fs::write(
            root.path().join("broken.c"),
            "int main() { return missing; }\n",
        )
        .await
        .expect("write C source");
        let service = LspService::new(root.path());
        let result = service
            .diagnostics(Path::new("broken.c"))
            .await
            .expect("clangd diagnostics");
        assert!(
            result
                .get("items")
                .and_then(Value::as_array)
                .is_some_and(|items| !items.is_empty())
        );

        service
            .workspace_diagnostics(Some(Path::new("broken.c")))
            .await
            .expect("clangd workspace diagnostics");
        let clients = service.clients.lock().await;
        let client = clients.values().next().expect("active clangd client");
        assert!(client.opened_versions.is_empty());
    }
}
