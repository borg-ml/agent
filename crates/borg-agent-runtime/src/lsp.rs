use std::collections::HashMap;
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
/// `initialize` is the one request that legitimately takes long: jdtls, gopls
/// and rust-analyzer index the workspace before answering. Every other
/// request keeps the short timeout so a wedged server is noticed quickly.
const INITIALIZE_TIMEOUT: Duration = Duration::from_secs(120);
/// Servers publish diagnostics only after their first analysis pass; waiting
/// three seconds produced empty results on any non-trivial workspace.
const PUBLISHED_DIAGNOSTICS_TIMEOUT: Duration = Duration::from_secs(15);
/// An agent's gap between two tool calls is routinely minutes, so a short
/// idle timeout reaped servers between a targeted request and the follow-up
/// workspace pass and forced a full (for clangd and rust-analyzer, very
/// expensive) restart. Keep warm servers long enough to span normal turns.
const LSP_IDLE_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const LSP_REAPER_INTERVAL: Duration = Duration::from_secs(30);
const MAX_LSP_MESSAGE_BYTES: usize = 8 * 1024 * 1024;
const MAX_WORKSPACE_DIAGNOSTIC_FILES: usize = 4096;
/// Whole-call wall clock for a workspace diagnostics pass. Without it, the
/// per-document fallback multiplies its own timeout by the file count, which
/// on a large C++ workspace is hours; callers get a bounded partial report
/// instead of a hang.
const WORKSPACE_DIAGNOSTICS_BUDGET: Duration = Duration::from_secs(90);
/// Remembered reaps, so a later "nothing is active" answer can say a server
/// *was* active rather than implying one never started.
const MAX_REMEMBERED_REAPS: usize = 8;
/// Coverage scanning reads the compilation database directly; large
/// generated databases are checked only up to these bounds, and report
/// `unknown` rather than guessing beyond them.
const MAX_COMPILATION_DATABASE_SCAN_BYTES: u64 = 32 * 1024 * 1024;
const COMPILATION_DATABASE_SCAN_BUDGET: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub struct LspService {
    root: PathBuf,
    path_policy: LspPathPolicy,
    clients: std::sync::Arc<Mutex<HashMap<LspClientKey, LspClientSlot>>>,
    reaped: std::sync::Arc<Mutex<Vec<ReapRecord>>>,
    /// Keys this service has leased.
    ///
    /// The pool behind `clients` belongs to the whole process, so without this
    /// a session's `status` and workspace-wide passes would report every
    /// server the host happens to be running, including workspaces this
    /// session never opened. Sharing the process is the point; sharing the
    /// view is not.
    leased: std::sync::Arc<Mutex<std::collections::BTreeSet<LspClientKey>>>,
}

#[derive(Clone, Debug)]
pub enum LspPathPolicy {
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

    pub fn session_workspace() -> Self {
        Self::SessionWorkspace
    }

    pub fn authorized_roots(roots: Vec<PathBuf>) -> Self {
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

    /// This policy's identity for pooling. Two services share a server only
    /// when these are equal, which means their access rules are the same rule
    /// and not merely similar.
    fn scope_key(&self, session_root: &Path) -> LspScopeKey {
        match self {
            Self::Unrestricted => LspScopeKey::Unrestricted,
            Self::SessionWorkspace => LspScopeKey::SessionWorkspace(session_root.to_path_buf()),
            Self::AuthorizedRoots(roots) => {
                let mut roots = roots.clone();
                roots.sort();
                roots.dedup();
                LspScopeKey::AuthorizedRoots(roots)
            }
        }
    }
}

/// The access rules a pooled server was started for.
///
/// Part of the pool key, because a shared server shares more than a process:
/// the documents opened in it and the diagnostics it has already published
/// live on the client. Without this, a session restricted to one workspace
/// would receive results for files a broader session opened in the same
/// server. The per-request `allows` check cannot catch that -- it guards the
/// path a caller names, not the state the server already holds.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum LspScopeKey {
    Unrestricted,
    SessionWorkspace(PathBuf),
    AuthorizedRoots(Vec<PathBuf>),
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct LspClientKey {
    server_id: &'static str,
    workspace_root: PathBuf,
    scope: LspScopeKey,
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
}

/// One language server per (server, workspace root). The map lock is held
/// only to find or create the slot; the slot's own lock serialises traffic
/// to that server, so a slow rust-analyzer start or request never blocks a
/// hover against gopls in the same session. The first caller to lock the
/// slot starts the server.
struct LspClientSlot {
    client: SharedLspClient,
    last_used: Instant,
}

/// A leased slot is not evidence that a server runs there. Recording the
/// distinction keeps `status` and workspace results from presenting a server
/// that failed to start as an active one answering with no diagnostics.
enum LspClientState {
    NotStarted,
    Ready(Box<LspClient>),
    Failed(String),
}

impl LspClientState {
    fn ready_mut(&mut self) -> Option<&mut LspClient> {
        match self {
            Self::Ready(client) => Some(client),
            _ => None,
        }
    }

    fn label(&self) -> &'static str {
        match self {
            Self::NotStarted => "not_started",
            Self::Ready(_) => "ready",
            Self::Failed(_) => "failed",
        }
    }

    /// The report for a workspace whose server is not answering. Returning
    /// this instead of skipping the slot keeps an unstarted server out of a
    /// result set that would otherwise read as "no problems found".
    fn unavailable_report(&self) -> Value {
        let mut report = json!({
            "status": self.label(),
            "items": [],
            "unavailable": true
        });
        match self {
            Self::Failed(error) => {
                report["error"] = Value::String(error.clone());
            }
            Self::NotStarted => {
                report["error"] = Value::String(
                    "language server has not started for this workspace yet".to_string(),
                );
            }
            Self::Ready(_) => {}
        }
        report
    }
}

type SharedLspClient = std::sync::Arc<Mutex<LspClientState>>;

#[derive(Clone)]
struct ReapRecord {
    key: LspClientKey,
    reason: &'static str,
}

impl ReapRecord {
    fn describe(&self) -> String {
        format!(
            "{} for {} ({})",
            self.key.server_id,
            self.key.workspace_root.display(),
            self.reason
        )
    }
}

/// One client pool for the process.
///
/// A language server is identified by the workspace it indexes, so two
/// sessions looking at the same workspace want the same server. Before this,
/// each `LspService` owned its own pool and every session -- root and subagent
/// alike -- started its own rust-analyzer for the same repository. That is not
/// a small inefficiency: seven parallel workers on one Cargo workspace ran
/// seven copies of the same index and tens of gigabytes of resident memory.
struct LspPool {
    clients: std::sync::Arc<Mutex<HashMap<LspClientKey, LspClientSlot>>>,
    reaped: std::sync::Arc<Mutex<Vec<ReapRecord>>>,
}

fn pool() -> &'static LspPool {
    static POOL: std::sync::OnceLock<LspPool> = std::sync::OnceLock::new();
    POOL.get_or_init(|| LspPool {
        clients: std::sync::Arc::new(Mutex::new(HashMap::new())),
        reaped: std::sync::Arc::new(Mutex::new(Vec::new())),
    })
}

static IDLE_REAPER: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>> =
    std::sync::Mutex::new(None);

/// Whether the pool still has a reaper that can actually run.
///
/// A reaper belongs to the Tokio runtime that spawned it and dies with it, and
/// a handle whose runtime is gone reports finished. That is precisely what a
/// "started once" flag cannot see: it would stay true over a dead task and the
/// pool would never sweep again.
fn reaper_is_live(existing: Option<&tokio::task::JoinHandle<()>>) -> bool {
    existing.is_some_and(|handle| !handle.is_finished())
}

/// Keep exactly one live idle reaper for the pool.
///
/// Re-spawned whenever the one we hold has stopped, because runtimes do not
/// always outlive the process: every `#[tokio::test]` builds and drops its
/// own, and an embedded host that restarts its runtime would otherwise be left
/// with a pool nothing ever sweeps. A service constructed outside a runtime
/// still starts nothing, exactly as before.
fn ensure_idle_reaper(pool: &'static LspPool) {
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        return;
    };
    // The guard covers an Option<JoinHandle> and is never held across an
    // await, so a poisoned lock is recoverable rather than a reason to leave
    // the pool unswept.
    let mut reaper = match IDLE_REAPER.lock() {
        Ok(reaper) => reaper,
        Err(poisoned) => poisoned.into_inner(),
    };
    if reaper_is_live(reaper.as_ref()) {
        return;
    }
    *reaper = Some(spawn_idle_reaper(runtime, &pool.clients, &pool.reaped));
}

impl LspService {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self::with_path_policy(root, LspPathPolicy::unrestricted())
    }

    pub(crate) fn with_path_policy(root: impl Into<PathBuf>, path_policy: LspPathPolicy) -> Self {
        let pool = pool();
        ensure_idle_reaper(pool);
        Self {
            root: root.into(),
            path_policy,
            clients: std::sync::Arc::clone(&pool.clients),
            reaped: std::sync::Arc::clone(&pool.reaped),
            leased: std::sync::Arc::new(Mutex::new(std::collections::BTreeSet::new())),
        }
    }

    pub async fn status(&self) -> Value {
        let slots = self.active_clients().await;
        let mut active = Vec::new();
        let mut active_workspaces = Vec::new();
        for (key, shared) in &slots {
            let mut workspace = json!({ "server": key.server_id, "root": key.workspace_root });
            // Never block on a slot: status must stay answerable while a
            // long workspace pass holds the server.
            match shared.try_lock() {
                Ok(state) => {
                    if matches!(*state, LspClientState::Ready(_)) {
                        active.push(key.server_id);
                    }
                    workspace["state"] = Value::String(state.label().to_string());
                    if let LspClientState::Failed(error) = &*state {
                        workspace["error"] = Value::String(error.clone());
                    }
                }
                Err(_) => {
                    active.push(key.server_id);
                    workspace["state"] = Value::String("busy".to_string());
                }
            }
            active_workspaces.push(workspace);
        }
        active.sort_unstable();
        active.dedup();
        active_workspaces.sort_by_key(|workspace| workspace.to_string());
        let mut status = json!({
            "root": self.root,
            "active_servers": active,
            "active_workspaces": active_workspaces,
            "supported_servers": supported_server_status()
        });
        // This session's own reaps only: the list is process-wide, and another
        // session's workspace path is not this session's to report.
        let leased = self.leased.lock().await.clone();
        let reaped = self.reaped.lock().await;
        let recently_stopped = reaped
            .iter()
            .filter(|record| leased.contains(&record.key))
            .map(|record| {
                json!({
                    "server": record.key.server_id,
                    "root": record.key.workspace_root,
                    "reason": record.reason
                })
            })
            .collect::<Vec<_>>();
        if !recently_stopped.is_empty() {
            status["recently_stopped"] = Value::Array(recently_stopped);
        }
        status
    }

    pub fn supported_status() -> Value {
        json!(supported_server_status())
    }

    pub async fn diagnostics(&self, path: &Path) -> Result<Value> {
        let (path, uri, spec, workspace_root) = self.resolve_document(path).await?;
        let shared = self.lease_client(spec, &workspace_root).await;
        let mut slot = shared.lock().await;
        let client = ready_client(&mut slot, spec, &workspace_root).await?;
        let mut report = client
            .document_diagnostics(&path, &uri, spec.language_id)
            .await?;
        drop(slot);
        if let Some(context) = compilation_context_status(&workspace_root, spec, Some(&path)).await
            && let Value::Object(report) = &mut report
        {
            report.insert("compilationContext".to_string(), context);
        }
        Ok(report)
    }

    /// Find or create the slot for `(spec, root)` under the map lock, without
    /// starting the server there.
    async fn lease_client(&self, spec: &'static ServerSpec, root: &Path) -> SharedLspClient {
        // Revive the sweep on use, not only on construction: a service built
        // under a runtime that later went away would otherwise keep leasing
        // servers into a pool nothing reaps.
        ensure_idle_reaper(pool());
        let session_root = tokio::fs::canonicalize(&self.root)
            .await
            .unwrap_or_else(|_| self.root.clone());
        let key = LspClientKey {
            server_id: spec.id,
            workspace_root: root.to_path_buf(),
            scope: self.path_policy.scope_key(&session_root),
        };
        self.leased.lock().await.insert(key.clone());
        let client = {
            let mut clients = self.clients.lock().await;
            let slot = clients.entry(key).or_insert_with(|| LspClientSlot {
                client: std::sync::Arc::new(Mutex::new(LspClientState::NotStarted)),
                last_used: Instant::now(),
            });
            slot.last_used = Instant::now();
            slot.client.clone()
        };
        discard_dead_client(&client).await;
        client
    }

    /// Snapshot of every slot, taken under the map lock and released before
    /// any server is spoken to. Taking the snapshot counts as use: a long
    /// workspace pass must not leave its own servers looking idle.
    async fn active_clients(&self) -> Vec<(LspClientKey, SharedLspClient)> {
        let leased = self.leased.lock().await.clone();
        let snapshot = {
            let mut clients = self.clients.lock().await;
            let now = Instant::now();
            clients
                .iter_mut()
                .filter(|(key, _)| leased.contains(*key))
                .map(|(key, slot)| {
                    slot.last_used = now;
                    (key.clone(), slot.client.clone())
                })
                .collect::<Vec<_>>()
        };
        // A slot that says `Ready` is not evidence the process behind it is
        // still running. Reconcile before anyone reads the state, so `status`
        // and workspace passes report a killed server as stopped instead of
        // presenting it as one answering with no diagnostics.
        for (_, client) in &snapshot {
            discard_dead_client(client).await;
        }
        snapshot
    }

    /// Why a workspace-wide request has nothing to talk to. Distinguishes
    /// "never started one" from "started one and stopped it while idle".
    async fn no_active_server_error(&self) -> anyhow::Error {
        let leased = self.leased.lock().await.clone();
        let reaped = self.reaped.lock().await;
        // Only this session's own reaps. The list is process-wide now, and
        // another session's workspace path is not this session's to report.
        let mine = reaped
            .iter()
            .filter(|record| leased.contains(&record.key))
            .collect::<Vec<_>>();
        let Some(last) = mine.last() else {
            return anyhow::anyhow!(
                "no language server is active; provide a representative source path to initialize one"
            );
        };
        let stopped = mine
            .iter()
            .map(|record| record.describe())
            .collect::<Vec<_>>()
            .join(", ");
        anyhow::anyhow!(
            "no language server is active; {stopped} stopped earlier in this session. \
             Provide a representative source path (for example one under {}) to start it again",
            last.key.workspace_root.display()
        )
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
        let clients = self.active_clients().await;
        if clients.is_empty() {
            return Err(self.no_active_server_error().await);
        }
        let server_counts = server_counts(&clients);
        let mut results = serde_json::Map::new();
        for (key, shared) in &clients {
            let label = workspace_label(key, server_counts[key.server_id]);
            let mut slot = shared.lock().await;
            let Some(client) = slot.ready_mut() else {
                results.insert(label, slot.unavailable_report());
                continue;
            };
            let value = client
                .request("workspace/symbol", json!({ "query": query }))
                .await
                .with_context(|| format!("{label} workspace symbol request failed"))?;
            results.insert(label, value);
        }
        Ok(Value::Object(results))
    }

    /// Request diagnostics for every document known to each active language
    /// server workspace. An optional source path can bootstrap the matching
    /// language server when this service has not been used yet.
    pub async fn workspace_diagnostics(&self, path: Option<&Path>) -> Result<Value> {
        if let Some(path) = path {
            let (path, uri, spec, workspace_root) = self.resolve_document(path).await?;
            let shared = self.lease_client(spec, &workspace_root).await;
            let mut slot = shared.lock().await;
            let client = ready_client(&mut slot, spec, &workspace_root).await?;
            client.open_document(&path, &uri, spec.language_id).await?;
        }

        let clients = self.active_clients().await;
        if clients.is_empty() {
            return Err(self.no_active_server_error().await);
        }
        let server_counts = server_counts(&clients);
        let deadline = Instant::now() + WORKSPACE_DIAGNOSTICS_BUDGET;
        let mut results = serde_json::Map::new();
        for (key, shared) in &clients {
            let label = workspace_label(key, server_counts[key.server_id]);
            let mut slot = shared.lock().await;
            let Some(client) = slot.ready_mut() else {
                results.insert(label, slot.unavailable_report());
                continue;
            };
            // The budget is shared across workspaces, so one slow server
            // cannot consume the whole call and leave the rest unreported.
            if Instant::now() >= deadline {
                results.insert(label, skipped_for_budget_report());
                continue;
            }
            let value = match client.workspace_diagnostics().await {
                Ok(value) => value,
                Err(error) if is_unknown_workspace_diagnostics_request(&error) => {
                    let spec = spec_for_id(key.server_id)
                        .with_context(|| format!("unknown language server `{}`", key.server_id))?;
                    client
                        .document_workspace_diagnostics(&key.workspace_root, spec, deadline)
                        .await
                        .with_context(|| {
                            format!("{label} document diagnostics fallback failed ({error:#})")
                        })?
                }
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("{label} workspace diagnostics request failed"));
                }
            };
            results.insert(label, value);
        }
        Ok(Value::Object(results))
    }

    async fn document_request(&self, path: &Path, method: &str, extra: Value) -> Result<Value> {
        let (path, uri, spec, workspace_root) = self.resolve_document(path).await?;
        let shared = self.lease_client(spec, &workspace_root).await;
        let mut slot = shared.lock().await;
        let client = ready_client(&mut slot, spec, &workspace_root).await?;
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
        let shared = self.lease_client(spec, &workspace_root).await;
        let mut slot = shared.lock().await;
        let client = ready_client(&mut slot, spec, &workspace_root).await?;
        let text = client.open_document(&path, &uri, spec.language_id).await?;
        // Callers count columns in characters; the wire position is UTF-16
        // code units (the encoding negotiated in `initialize`).
        let line_index = line.saturating_sub(1);
        let character = utf16_column(
            text.lines().nth(line_index as usize).unwrap_or_default(),
            character.saturating_sub(1),
        );
        let mut params = extra.as_object().cloned().unwrap_or_default();
        params.insert("textDocument".to_string(), json!({ "uri": uri }));
        params.insert(
            "position".to_string(),
            json!({
                "line": line_index,
                "character": character
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
        };
        let root_uri = Url::from_directory_path(root)
            .map_err(|_| anyhow::anyhow!("cannot convert workspace root to URI"))?
            .to_string();
        let mut initialize = json!({
            "processId": std::process::id(),
            "rootUri": root_uri,
            "capabilities": {
                "general": { "positionEncodings": ["utf-16"] },
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
        client
            .request_with_timeout("initialize", initialize, INITIALIZE_TIMEOUT)
            .await?;
        client.notify("initialized", json!({})).await?;
        Ok(client)
    }

    async fn open_document(&mut self, path: &Path, uri: &str, language_id: &str) -> Result<String> {
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
                    "text": text.clone()
                }
            })
        } else {
            json!({
                "textDocument": { "uri": uri, "version": *version },
                "contentChanges": [{ "text": text.clone() }]
            })
        };
        self.notify(method, params).await?;
        Ok(text)
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

    /// Walk the workspace one document at a time for servers without
    /// `workspace/diagnostic`. Stops at `deadline` and reports how far it
    /// got, because the alternative on a large C++ tree is an unbounded call
    /// that the caller can only kill.
    async fn document_workspace_diagnostics(
        &mut self,
        workspace_root: &Path,
        spec: &ServerSpec,
        deadline: Instant,
    ) -> Result<Value> {
        let scan = discover_workspace_documents(workspace_root, spec.extensions).await;
        let total = scan.paths.len();
        let mut items = Vec::new();
        let mut failed = 0usize;
        let mut exhausted = false;
        for path in scan.paths {
            if Instant::now() >= deadline {
                exhausted = true;
                break;
            }
            let uri = Url::from_file_path(&path)
                .map_err(|_| anyhow::anyhow!("cannot convert {} to a file URI", path.display()))?
                .to_string();
            self.close_document(&path, &uri)
                .await
                .with_context(|| format!("failed to reset {}", path.display()))?;
            let report = self
                .document_diagnostics(&path, &uri, spec.language_id)
                .await;
            self.close_document(&path, &uri)
                .await
                .with_context(|| format!("failed to close {}", path.display()))?;
            // One unreadable or slow document used to abort the whole pass
            // and discard every diagnostic already collected.
            match report {
                Ok(report) => items.push(workspace_document_report(&uri, report)),
                Err(error) => {
                    failed += 1;
                    items.push(json!({
                        "uri": uri,
                        "kind": "full",
                        "items": [],
                        "error": format!("{error:#}")
                    }));
                }
            }
        }
        let scanned = items.len();
        let mut result = json!({ "kind": "full", "items": items });
        if failed > 0 {
            result["failedDocuments"] = json!(failed);
        }
        let mut partial_reasons = Vec::new();
        if scan.truncated {
            partial_reasons.push(format!(
                "workspace scan limited to {MAX_WORKSPACE_DIAGNOSTIC_FILES} files"
            ));
        }
        if exhausted {
            partial_reasons.push(format!(
                "time budget of {}s exhausted after {scanned} of {total} discovered files",
                WORKSPACE_DIAGNOSTICS_BUDGET.as_secs()
            ));
        }
        if !partial_reasons.is_empty() {
            result["partial"] = Value::Bool(true);
            result["partialReason"] = Value::String(partial_reasons.join("; "));
            result["documentsScanned"] = json!(scanned);
            result["documentsDiscovered"] = json!(total);
        }
        if let Some(context) = compilation_context_status(workspace_root, spec, None).await {
            result["compilationContext"] = context;
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
        self.request_with_timeout(method, params, REQUEST_TIMEOUT)
            .await
    }

    async fn request_with_timeout(
        &mut self,
        method: &str,
        params: Value,
        deadline: Duration,
    ) -> Result<Value> {
        let id = self.next_id;
        self.next_id += 1;
        self.write_message(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params
        }))
        .await?;
        timeout(deadline, async {
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
        timeout(PUBLISHED_DIAGNOSTICS_TIMEOUT, async {
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

/// Forget a server that is no longer running, so the next request starts a
/// fresh one instead of writing into a closed pipe.
///
/// The child is spawned `kill_on_drop`, so a server killed from outside stays
/// in its slot as a `Ready` client over a dead pipe, and the process stays a
/// zombie because nobody ever waits on it. `try_wait` reaps it and returns the
/// slot to `NotStarted`, which is the state `ready_client` restarts from.
///
/// Non-blocking on purpose: a slot that is already locked is mid-request, and
/// that is itself proof the server is alive.
async fn discard_dead_client(client: &SharedLspClient) {
    let Ok(mut state) = client.try_lock() else {
        return;
    };
    // An error from `try_wait` means the handle can no longer answer for the
    // child, which is not a state to keep serving requests from either.
    let exited = match &mut *state {
        LspClientState::Ready(running) => !matches!(running.child.try_wait(), Ok(None)),
        _ => return,
    };
    if exited {
        *state = LspClientState::NotStarted;
    }
}

/// Start the server for a slot on first use. Runs under the slot lock, so
/// concurrent callers for the same workspace wait for one start instead of
/// racing, while other servers stay reachable.
/// A previous failure is recorded but not sticky: installing the server and
/// retrying is the normal fix, so the next caller attempts a fresh start.
async fn ready_client<'a>(
    slot: &'a mut LspClientState,
    spec: &'static ServerSpec,
    root: &Path,
) -> Result<&'a mut LspClient> {
    if !matches!(slot, LspClientState::Ready(_)) {
        match LspClient::start(spec, root).await {
            Ok(client) => *slot = LspClientState::Ready(Box::new(client)),
            Err(error) => {
                *slot = LspClientState::Failed(format!("{error:#}"));
                return Err(error);
            }
        }
    }
    Ok(slot.ready_mut().expect("started LSP client"))
}

/// The UTF-16 code-unit column for a character (code point) column on `line`.
/// A column past the end of the line maps to the end of the line.
fn utf16_column(line: &str, character_column: u32) -> u32 {
    line.chars()
        .take(character_column as usize)
        .map(|character| character.len_utf16() as u32)
        .sum::<u32>()
}

fn spawn_idle_reaper(
    runtime: tokio::runtime::Handle,
    clients: &std::sync::Arc<Mutex<HashMap<LspClientKey, LspClientSlot>>>,
    reaped: &std::sync::Arc<Mutex<Vec<ReapRecord>>>,
) -> tokio::task::JoinHandle<()> {
    let clients = std::sync::Arc::downgrade(clients);
    let reaped = std::sync::Arc::downgrade(reaped);
    runtime.spawn(async move {
        let mut interval = tokio::time::interval(LSP_REAPER_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;
        loop {
            interval.tick().await;
            let (Some(clients), Some(reaped)) = (clients.upgrade(), reaped.upgrade()) else {
                return;
            };
            let mut records = Vec::new();
            clients.lock().await.retain(|key, slot| {
                let Some(reason) = lsp_client_reap_reason(key, slot.last_used.elapsed()) else {
                    return true;
                };
                // A slot whose server is mid-request is not idle, whatever
                // its timestamp says.
                if slot.client.try_lock().is_err() {
                    return true;
                }
                records.push(ReapRecord {
                    key: key.clone(),
                    reason,
                });
                false
            });
            if records.is_empty() {
                continue;
            }
            let mut reaped = reaped.lock().await;
            reaped.extend(records);
            let overflow = reaped.len().saturating_sub(MAX_REMEMBERED_REAPS);
            reaped.drain(..overflow);
        }
    })
}

fn lsp_client_is_expired(idle_for: Duration) -> bool {
    idle_for >= LSP_IDLE_TIMEOUT
}

fn lsp_client_reap_reason(key: &LspClientKey, idle_for: Duration) -> Option<&'static str> {
    if !key.workspace_root.is_dir() {
        return Some("workspace root no longer exists");
    }
    lsp_client_is_expired(idle_for).then_some("stopped after being idle")
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

fn server_counts(clients: &[(LspClientKey, SharedLspClient)]) -> HashMap<&'static str, usize> {
    let mut counts = HashMap::new();
    for (key, _) in clients {
        *counts.entry(key.server_id).or_insert(0usize) += 1;
    }
    counts
}

fn skipped_for_budget_report() -> Value {
    json!({
        "status": "skipped",
        "items": [],
        "partial": true,
        "partialReason": format!(
            "time budget of {}s was consumed by earlier workspaces; query this one directly",
            WORKSPACE_DIAGNOSTICS_BUDGET.as_secs()
        )
    })
}

/// Servers that read a compilation database answer from default flags when
/// it is absent, producing confident errors (unknown includes, undefined
/// types) that describe the missing configuration rather than the code.
/// Reporting that state keeps callers from acting on those diagnostics.
fn uses_compilation_database(spec: &ServerSpec) -> bool {
    spec.id == "clangd"
}

async fn compilation_context_status(
    workspace_root: &Path,
    spec: &ServerSpec,
    file: Option<&Path>,
) -> Option<Value> {
    if !uses_compilation_database(spec) {
        return None;
    }
    let search_start = file
        .and_then(Path::parent)
        .unwrap_or(workspace_root)
        .to_path_buf();
    let Some(database) = find_compilation_database(&search_start, workspace_root).await else {
        return Some(json!({
            "status": "missing",
            "searchedFrom": search_start,
            "warning": "no compile_commands.json or compile_flags.txt was found; \
        clangd is using fallback flags, so missing-include and unknown-type diagnostics \
        likely describe absent build configuration rather than defects in the code"
        }));
    };
    let mut status = json!({
        "status": "present",
        "kind": database.kind,
        "path": database.path
    });
    let Ok(metadata) = tokio::fs::metadata(&database.path).await else {
        return Some(status);
    };
    status["sizeBytes"] = json!(metadata.len());
    if database.kind != "compile_commands.json" {
        return Some(status);
    }
    if let Some(file) = file {
        match compilation_database_covers(&database.path, file).await {
            Coverage::Covered => {
                status["coversFile"] = Value::Bool(true);
            }
            Coverage::Absent => {
                status["coversFile"] = Value::Bool(false);
                status["status"] = Value::String("stale".to_string());
                status["warning"] = Value::String(format!(
                    "{} has no entry for this file; clangd is inferring flags from a \
different translation unit, so its diagnostics may be misleading. Regenerate the \
compilation database to include this file",
                    database.path.display()
                ));
            }
            Coverage::Unknown => {
                status["coversFile"] = Value::String("unknown".to_string());
                status["coverageNote"] = Value::String(format!(
                    "compilation database exceeds the {} MiB / {}s inspection budget; \
coverage was not determined",
                    MAX_COMPILATION_DATABASE_SCAN_BYTES / (1024 * 1024),
                    COMPILATION_DATABASE_SCAN_BUDGET.as_secs()
                ));
            }
        }
        if let Ok(source) = tokio::fs::metadata(file).await
            && let (Ok(source_time), Ok(database_time)) = (source.modified(), metadata.modified())
            && source_time > database_time
        {
            status["staleAgainstSource"] = Value::Bool(true);
        }
    }
    Some(status)
}

struct CompilationDatabase {
    path: PathBuf,
    kind: &'static str,
}

/// clangd looks in each ancestor directory and in its `build` subdirectory;
/// mirror that rather than inventing a broader search.
async fn find_compilation_database(start: &Path, root: &Path) -> Option<CompilationDatabase> {
    let mut current = Some(start.to_path_buf());
    let mut reached_root = false;
    while let Some(directory) = current {
        for (relative, kind) in [
            (
                PathBuf::from("compile_commands.json"),
                "compile_commands.json",
            ),
            (
                PathBuf::from("build").join("compile_commands.json"),
                "compile_commands.json",
            ),
            (PathBuf::from("compile_flags.txt"), "compile_flags.txt"),
        ] {
            let candidate = directory.join(relative);
            if tokio::fs::metadata(&candidate).await.is_ok() {
                return Some(CompilationDatabase {
                    path: candidate,
                    kind,
                });
            }
        }
        if reached_root {
            break;
        }
        if directory == root {
            reached_root = true;
        }
        current = directory.parent().map(Path::to_path_buf);
    }
    None
}

enum Coverage {
    Covered,
    Absent,
    Unknown,
}

/// Generated databases for large projects reach hundreds of megabytes, so
/// this scans for the file path within a byte and time budget and reports
/// `Unknown` rather than parsing the whole document or guessing.
async fn compilation_database_covers(database: &Path, file: &Path) -> Coverage {
    let Some(needle) = file.to_str() else {
        return Coverage::Unknown;
    };
    let Ok(contents) = tokio::fs::read(database).await else {
        return Coverage::Unknown;
    };
    if contents.len() as u64 > MAX_COMPILATION_DATABASE_SCAN_BYTES {
        return Coverage::Unknown;
    }
    let started = Instant::now();
    let Ok(text) = std::str::from_utf8(&contents) else {
        return Coverage::Unknown;
    };
    if text.contains(needle) {
        return Coverage::Covered;
    }
    if started.elapsed() > COMPILATION_DATABASE_SCAN_BUDGET {
        return Coverage::Unknown;
    }
    // A database may record the file under a different but equivalent path;
    // the file name alone is a weaker signal, so absence of both is what
    // justifies reporting the file as uncovered.
    match file.file_name().and_then(|name| name.to_str()) {
        Some(name) if text.contains(name) => Coverage::Unknown,
        _ => Coverage::Absent,
    }
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
                // Generated and derived trees in large C++/Unreal projects
                // hold far more source-shaped files than the project itself,
                // and diagnostics on them are noise.
                | "Intermediate"
                | "Binaries"
                | "DerivedDataCache"
                | "Saved"
        )
    )
}

async fn discover_project_root(path: &Path, boundary: Option<&Path>, fallback: PathBuf) -> PathBuf {
    let mut current = path.parent().unwrap_or(path).to_path_buf();
    loop {
        if has_project_marker(&current).await {
            return cargo_workspace_root(current, boundary).await;
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

/// Widen a Cargo member crate to the workspace that owns it.
///
/// `Cargo.toml` is a project marker, so the upward walk stops at the first
/// member crate it meets. rust-analyzer started there indexes the whole
/// workspace anyway, so touching three member crates of one workspace started
/// three servers that each held the same index. One workspace is one root.
///
/// The walk never rises above `boundary`, which is the path policy's scope
/// root. Widening a root is a read-scope decision: a restricted session must
/// not end up with a server rooted somewhere it is not allowed to read, so the
/// boundary is checked before each step up rather than after.
async fn cargo_workspace_root(start: PathBuf, boundary: Option<&Path>) -> PathBuf {
    if !declares_cargo_package_only(&start).await {
        return start;
    }
    let mut current = start.clone();
    loop {
        if boundary.is_some_and(|boundary| current == boundary) {
            return start;
        }
        let Some(parent) = current.parent() else {
            return start;
        };
        if parent == current {
            return start;
        }
        current = parent.to_path_buf();
        if declares_cargo_workspace(&current).await {
            return current;
        }
    }
}

/// A manifest that declares a package and no workspace: a member crate, or a
/// standalone crate that is its own root.
async fn declares_cargo_package_only(directory: &Path) -> bool {
    read_cargo_manifest(directory)
        .await
        .is_some_and(|manifest| {
            manifest.get("package").is_some() && manifest.get("workspace").is_none()
        })
}

async fn declares_cargo_workspace(directory: &Path) -> bool {
    read_cargo_manifest(directory)
        .await
        .is_some_and(|manifest| manifest.get("workspace").is_some())
}

/// Read `Cargo.toml`, bounded in size and tolerant of failure.
///
/// A manifest that cannot be read or parsed simply does not widen anything:
/// the narrower root still works, there are just more of them. Guessing from
/// an unparsable file would be the worse answer.
async fn read_cargo_manifest(directory: &Path) -> Option<toml::Table> {
    const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;
    let path = directory.join("Cargo.toml");
    let metadata = tokio::fs::metadata(&path).await.ok()?;
    if !metadata.is_file() || metadata.len() > MAX_MANIFEST_BYTES {
        return None;
    }
    tokio::fs::read_to_string(&path)
        .await
        .ok()?
        .parse::<toml::Table>()
        .ok()
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
    fn character_columns_are_converted_to_utf16_units() {
        assert_eq!(utf16_column("let x = 1;", 4), 4);
        // 'é' is one code point and one UTF-16 unit; '😀' is one code point
        // and two UTF-16 units.
        assert_eq!(utf16_column("é😀x", 0), 0);
        assert_eq!(utf16_column("é😀x", 1), 1);
        assert_eq!(utf16_column("é😀x", 2), 3);
        assert_eq!(utf16_column("é😀x", 3), 4);
        assert_eq!(
            utf16_column("é😀x", 99),
            4,
            "past the end clamps to the line end"
        );
        assert_eq!(utf16_column("", 5), 0);
    }

    #[test]
    fn missing_lsp_workspaces_are_reaped_even_when_recently_used() {
        let workspace = tempfile::tempdir().expect("workspace");
        let key = LspClientKey {
            server_id: "rust-analyzer",
            workspace_root: workspace.path().to_path_buf(),
            scope: LspScopeKey::Unrestricted,
        };
        assert!(lsp_client_reap_reason(&key, Duration::ZERO).is_none());

        drop(workspace);
        assert!(lsp_client_reap_reason(&key, Duration::ZERO).is_some());
    }

    /// Each member crate of a Cargo workspace used to resolve as its own
    /// project root, because `Cargo.toml` is a project marker and the walk
    /// stopped at the first one. Every member crate touched therefore started
    /// its own rust-analyzer, and each of those indexed the whole workspace
    /// anyway -- the observed seven processes holding tens of gigabytes. This
    /// spawns nothing; it pins the root arithmetic that decides how many
    /// servers exist.
    #[tokio::test]
    async fn one_cargo_workspace_resolves_to_one_root() {
        let workspace = tempfile::tempdir().expect("workspace");
        let root = workspace.path();
        tokio::fs::write(
            root.join("Cargo.toml"),
            "[workspace]\nmembers = [\"crates/*\"]\n",
        )
        .await
        .expect("workspace manifest");

        let mut sources = Vec::new();
        for member in ["alpha", "beta"] {
            let crate_dir = root.join("crates").join(member);
            tokio::fs::create_dir_all(crate_dir.join("src"))
                .await
                .expect("member tree");
            tokio::fs::write(
                crate_dir.join("Cargo.toml"),
                format!("[package]\nname = \"{member}\"\n"),
            )
            .await
            .expect("member manifest");
            let source = crate_dir.join("src").join("lib.rs");
            tokio::fs::write(&source, "").await.expect("member source");
            sources.push(source);
        }

        let alpha = discover_project_root(&sources[0], None, root.to_path_buf()).await;
        let beta = discover_project_root(&sources[1], None, root.to_path_buf()).await;
        assert_eq!(alpha, root);
        assert_eq!(
            alpha, beta,
            "two member crates of one workspace are one language server, not two"
        );

        // Widening is a read-scope decision, so it stops at the policy scope
        // root. A session confined to one member crate must not end up with a
        // server rooted above it.
        let member = root.join("crates").join("alpha");
        assert_eq!(
            discover_project_root(&sources[0], Some(&member), root.to_path_buf()).await,
            member,
            "a policy boundary outranks the workspace it sits inside"
        );

        // A crate that belongs to no workspace is still its own root.
        let solo = tempfile::tempdir().expect("solo");
        tokio::fs::write(
            solo.path().join("Cargo.toml"),
            "[package]\nname = \"solo\"\n",
        )
        .await
        .expect("solo manifest");
        tokio::fs::create_dir_all(solo.path().join("src"))
            .await
            .expect("solo tree");
        let solo_source = solo.path().join("src").join("main.rs");
        tokio::fs::write(&solo_source, "")
            .await
            .expect("solo source");
        assert_eq!(
            discover_project_root(&solo_source, None, solo.path().to_path_buf()).await,
            solo.path()
        );
    }

    /// The pool is process-wide, which is the whole memory fix, so the thing
    /// worth pinning is what it refuses to share. A shared client also shares
    /// the documents opened in it and the diagnostics it has published, so two
    /// sessions may only share a server when their access rules are the same
    /// rule. Leasing starts no server: the slot is created `NotStarted`.
    #[tokio::test]
    async fn one_workspace_shares_one_server_unless_the_rules_differ() {
        let root = tempfile::tempdir().expect("workspace");
        let spec = spec_for_id("rust-analyzer").expect("rust-analyzer spec");

        let first = LspService::new(root.path());
        let second = LspService::new(root.path());
        let shared = first.lease_client(spec, root.path()).await;
        let reused = second.lease_client(spec, root.path()).await;
        assert!(
            std::sync::Arc::ptr_eq(&shared, &reused),
            "a second session on the same workspace must reuse the running server"
        );

        let restricted =
            LspService::with_path_policy(root.path(), LspPathPolicy::session_workspace());
        let separate = restricted.lease_client(spec, root.path()).await;
        assert!(
            !std::sync::Arc::ptr_eq(&shared, &separate),
            "a session with narrower access must not inherit a broader session's server"
        );

        assert_eq!(
            restricted.active_clients().await.len(),
            1,
            "a session reports the servers it leased, not every server on the host"
        );
    }

    /// A reaper belongs to the runtime that spawned it. The first cut of this
    /// pool latched a "started" flag on success, which stayed true after that
    /// runtime went away: every `#[tokio::test]` after the first, and any host
    /// that restarts an embedded runtime, would have been left with a pool
    /// nothing ever swept. Deliberately exercises the decision rather than the
    /// shared slot, so it is deterministic and cannot pass by happening to
    /// observe another test's live reaper.
    #[test]
    fn a_reaper_whose_runtime_is_gone_is_not_live() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let handle = runtime.spawn(std::future::pending::<()>());
        runtime.block_on(async { tokio::task::yield_now().await });
        assert!(
            reaper_is_live(Some(&handle)),
            "a task on a running runtime is a live reaper"
        );

        drop(runtime);
        assert!(
            !reaper_is_live(Some(&handle)),
            "a reaper whose runtime is gone must be respawned, not counted as running"
        );
        assert!(!reaper_is_live(None), "no reaper is not a live reaper");
    }

    /// The child is spawned `kill_on_drop`, so a server killed from outside
    /// stayed in its slot as a `Ready` client over a closed pipe while the
    /// process sat as a zombie nobody waited on. Two things had to be wrong at
    /// once: the next request wrote into the dead pipe, and `status` -- which
    /// only reads -- presented the corpse as a server answering with no
    /// diagnostics. `cat` stands in for a language server here: this needs a
    /// process with piped stdio that exits when killed, and nothing about
    /// rust-analyzer in particular.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_killed_server_is_reaped_and_stops_being_reported_as_active() {
        let root = tempfile::tempdir().expect("workspace");
        let service = LspService::new(root.path());
        let key = LspClientKey {
            server_id: "rust-analyzer",
            workspace_root: root.path().to_path_buf(),
            scope: LspScopeKey::Unrestricted,
        };

        let mut child = Command::new("cat")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("cat");
        let stdin = child.stdin.take().expect("stdin");
        let stdout = child.stdout.take().expect("stdout");
        let shared: SharedLspClient =
            std::sync::Arc::new(Mutex::new(LspClientState::Ready(Box::new(LspClient {
                child,
                stdin,
                stdout: BufReader::new(stdout),
                next_id: 1,
                opened_versions: HashMap::new(),
                published_diagnostics: HashMap::new(),
            }))));
        service.leased.lock().await.insert(key.clone());
        service.clients.lock().await.insert(
            key,
            LspClientSlot {
                client: std::sync::Arc::clone(&shared),
                last_used: Instant::now(),
            },
        );

        assert_eq!(
            service.status().await["active_servers"],
            json!(["rust-analyzer"]),
            "a running server is reported and left alone"
        );

        shared
            .lock()
            .await
            .ready_mut()
            .expect("ready")
            .child
            .start_kill()
            .expect("kill the server the way an outside process would");

        // The exit is observed, not assumed after a fixed sleep.
        let mut status = service.status().await;
        for _ in 0..200 {
            if status["active_servers"] == json!([]) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            status = service.status().await;
        }
        assert_eq!(
            status["active_servers"],
            json!([]),
            "a read-only status must not present a killed server as one answering"
        );
        assert_eq!(
            status["active_workspaces"][0]["state"],
            json!("not_started")
        );
        assert!(
            matches!(&*shared.lock().await, LspClientState::NotStarted),
            "the slot returns to NotStarted so the next request restarts it"
        );
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
        let shared = service
            .clients
            .lock()
            .await
            .values()
            .next()
            .expect("active clangd client")
            .client
            .clone();
        let mut client = shared.lock().await;
        assert!(
            client
                .ready_mut()
                .expect("started clangd client")
                .opened_versions
                .is_empty()
        );
    }

    /// A leased-but-unstarted server used to appear in `active_servers` and
    /// contribute nothing to workspace results, so a failed start read as a
    /// clean workspace.
    #[tokio::test]
    async fn a_server_that_failed_to_start_is_never_reported_as_active() {
        let root = tempfile::tempdir().expect("workspace");
        let service = LspService::new(root.path());
        let key = LspClientKey {
            server_id: "clangd",
            workspace_root: root.path().to_path_buf(),
            scope: LspScopeKey::Unrestricted,
        };
        // The pool is process-wide, so a service reports the slots it leased.
        service.leased.lock().await.insert(key.clone());
        service.clients.lock().await.insert(
            key,
            LspClientSlot {
                client: std::sync::Arc::new(Mutex::new(LspClientState::Failed(
                    "clangd is not available; install it on PATH".to_string(),
                ))),
                last_used: Instant::now(),
            },
        );

        let status = service.status().await;
        assert_eq!(status["active_servers"], json!([]));
        assert_eq!(status["active_workspaces"][0]["state"], json!("failed"));

        let report = service
            .workspace_diagnostics(None)
            .await
            .expect("an unavailable server still reports its state");
        assert_eq!(report["clangd"]["unavailable"], json!(true));
        assert_eq!(report["clangd"]["status"], json!("failed"));
        assert!(
            report["clangd"]["error"]
                .as_str()
                .is_some_and(|error| error.contains("not available")),
            "the reason a workspace has no diagnostics must survive into the report"
        );
    }

    /// Without a compilation database clangd answers from fallback flags, so
    /// its confident errors describe build configuration, not the code.
    #[tokio::test]
    async fn clangd_diagnostics_report_their_compilation_context() {
        let root = tempfile::tempdir().expect("workspace");
        let source = root.path().join("main.cpp");
        tokio::fs::write(&source, "int main() { return 0; }\n")
            .await
            .expect("write source");
        let clangd = spec_for_id("clangd").expect("clangd spec");

        let missing = compilation_context_status(root.path(), clangd, Some(&source))
            .await
            .expect("clangd reports compilation context");
        assert_eq!(missing["status"], json!("missing"));
        assert!(
            missing["warning"]
                .as_str()
                .is_some_and(|warning| warning.contains("fallback flags")),
            "a missing database must say why the diagnostics are suspect"
        );

        let database = root.path().join("compile_commands.json");
        tokio::fs::write(
            &database,
            r#"[{"directory":"/other","file":"/other/unrelated.cpp","command":"clang++ unrelated.cpp"}]"#,
        )
        .await
        .expect("write database");
        let uncovered = compilation_context_status(root.path(), clangd, Some(&source))
            .await
            .expect("clangd reports compilation context");
        assert_eq!(uncovered["status"], json!("stale"));
        assert_eq!(uncovered["coversFile"], json!(false));

        tokio::fs::write(
            &database,
            format!(
                r#"[{{"directory":"{}","file":"{}","command":"clang++ main.cpp"}}]"#,
                root.path().display(),
                source.display()
            ),
        )
        .await
        .expect("write database");
        let covered = compilation_context_status(root.path(), clangd, Some(&source))
            .await
            .expect("clangd reports compilation context");
        assert_eq!(covered["status"], json!("present"));
        assert_eq!(covered["coversFile"], json!(true));

        let rust = spec_for_id("rust-analyzer").expect("rust-analyzer spec");
        assert!(
            compilation_context_status(root.path(), rust, Some(&source))
                .await
                .is_none(),
            "servers that do not read a compilation database stay unannotated"
        );
    }

    /// The per-document fallback multiplied its own timeout by the file
    /// count, so a large workspace produced an unbounded call. It must stop
    /// at the deadline and say how far it got.
    #[tokio::test]
    async fn workspace_document_diagnostics_stop_at_their_time_budget() {
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
        let root = tempfile::tempdir().expect("workspace");
        for index in 0..4 {
            tokio::fs::write(
                root.path().join(format!("unit{index}.cpp")),
                "int main() { return 0; }\n",
            )
            .await
            .expect("write source");
        }
        let service = LspService::new(root.path());
        service
            .diagnostics(Path::new("unit0.cpp"))
            .await
            .expect("clangd diagnostics");
        let shared = service
            .clients
            .lock()
            .await
            .values()
            .next()
            .expect("active clangd client")
            .client
            .clone();
        let mut state = shared.lock().await;
        let client = state.ready_mut().expect("started clangd client");
        let clangd = spec_for_id("clangd").expect("clangd spec");

        let report = client
            .document_workspace_diagnostics(root.path(), clangd, Instant::now())
            .await
            .expect("an exhausted budget returns a partial report, not an error");

        assert_eq!(report["partial"], json!(true));
        assert_eq!(report["documentsScanned"], json!(0));
        assert_eq!(report["documentsDiscovered"], json!(4));
        assert!(
            report["partialReason"]
                .as_str()
                .is_some_and(|reason| reason.contains("time budget")),
            "the caller must learn the result was cut short by time"
        );
    }
}
