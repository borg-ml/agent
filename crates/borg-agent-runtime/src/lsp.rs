use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

#[cfg(unix)]
mod broker;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{Mutex, MutexGuard, watch};
use tokio::time::{Duration, Instant, timeout, timeout_at};
use url::Url;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const CANCEL_TIMEOUT: Duration = Duration::from_millis(100);

tokio::task_local! {
    // Scoped to one broker dispatch, never to a pooled client or session.
    static REQUEST_CANCELLED: watch::Receiver<bool>;
}

async fn request_cancelled() {
    if let Ok(mut receiver) = REQUEST_CANCELLED.try_with(Clone::clone) {
        let _ = receiver.wait_for(|cancelled| *cancelled).await;
    } else {
        std::future::pending::<()>().await;
    }
}

async fn lock_client(
    shared: &SharedLspClient,
    deadline: Option<Instant>,
) -> Result<MutexGuard<'_, LspClientState>> {
    let end = Instant::now() + REQUEST_TIMEOUT;
    let end = deadline.map_or(end, |deadline| deadline.min(end));
    tokio::select! {
        biased;
        _ = request_cancelled() => bail!("LSP request cancelled while waiting for server slot"),
        result = timeout_at(end, shared.lock()) => {
            result.context("language server slot is busy; timed out waiting for another LSP request")
        }
    }
}
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
// Reclaim only request-idle servers under host memory pressure, never a busy
// slot or a queued request holding a client lease. Normal warm TTL stays 10m.
const LSP_PRESSURE_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const LSP_MIN_AVAILABLE_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const MAX_OPEN_DOCUMENTS: usize = 8;
const LSP_REAPER_INTERVAL: Duration = Duration::from_secs(30);
const MAX_LSP_MESSAGE_BYTES: usize = 8 * 1024 * 1024;
const MAX_WORKSPACE_DIAGNOSTIC_FILES: usize = 4096;
/// Whole-pass budget including startup and slot waits. Without it, document
/// fallback multiplies its timeout by the file count. At expiry, allow at most
/// CANCEL_TIMEOUT more for best-effort cancellation, then return a partial report.
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
    #[cfg(unix)]
    broker_id: uuid::Uuid,
    #[cfg(unix)]
    broker_local: bool,
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

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
    last_activity: Instant,
    // Reads and writes may be interrupted by tool cancellation, including in
    // the middle of a frame. Keep all consumed/emitted bytes on the client.
    incoming: Vec<u8>,
    outgoing: Vec<u8>,
    outgoing_written: usize,
    pending_request: Option<u64>,
    supports_pull_diagnostics: bool,
    compilation_configuration: Vec<(PathBuf, u64, std::time::SystemTime)>,
    opened_versions: HashMap<PathBuf, i32>,
    open_order: VecDeque<PathBuf>,
    // Retain generations after close so late publications cannot match a reopen.
    last_versions: HashMap<PathBuf, i32>,
    published_diagnostics: HashMap<String, Value>,
    // Dropped after the child; only this client owns the directory.
    _private_preamble_storage: Option<tempfile::TempDir>,
}

/// One language server per (server, canonical workspace root, access scope).
/// The map lock is held
/// only to find or create the slot; the slot's own lock serialises traffic
/// to that server, so a slow rust-analyzer start or request never blocks a
/// hover against gopls in the same session. The first caller to lock the
/// slot starts the server.
struct LspClientSlot {
    client: SharedLspClient,
    last_used: Instant,
    owners: Vec<std::sync::Weak<Mutex<std::collections::BTreeSet<LspClientKey>>>>,
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

/// One client pool for the process. On Unix the first process to bind the
/// per-user LSP broker owns this pool; other Borg processes forward calls to
/// it. If the owner exits, the next caller binds a replacement broker.
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
            #[cfg(unix)]
            broker_id: uuid::Uuid::new_v4(),
            #[cfg(unix)]
            broker_local: cfg!(test),
            clients: std::sync::Arc::clone(&pool.clients),
            reaped: std::sync::Arc::clone(&pool.reaped),
            leased: std::sync::Arc::new(Mutex::new(std::collections::BTreeSet::new())),
        }
    }

    #[cfg(unix)]
    async fn shared_request(&self, operation: broker::Operation) -> Result<Option<Value>> {
        if self.broker_local {
            return Ok(None);
        }
        broker::request(broker::Request {
            id: self.broker_id,
            root: self.root.clone(),
            policy: self.path_policy.clone(),
            operation,
        })
        .await
        .map(Some)
    }

    pub async fn status(&self) -> Value {
        #[cfg(unix)]
        if !self.broker_local {
            return match self.shared_request(broker::Operation::Status).await {
                Ok(Some(status)) => status,
                Ok(None) => unreachable!("shared requests are enabled"),
                Err(error) => json!({ "root": self.root, "broker_error": format!("{error:#}"),
                    "active_servers": [], "active_workspaces": [],
                    "supported_servers": supported_server_status() }),
            };
        }
        self.status_local().await
    }

    async fn status_local(&self) -> Value {
        let slots = self.active_clients(false).await;
        let mut active = Vec::new();
        let mut active_workspaces = Vec::new();
        for (key, shared) in &slots {
            let mut workspace = json!({ "server": key.server_id, "root": key.workspace_root });
            let last_used = self
                .clients
                .lock()
                .await
                .get(key)
                .map(|slot| slot.last_used);
            if let Some(last_used) = last_used {
                workspace["idle_seconds"] = json!(last_used.elapsed().as_secs());
            }
            // Never block on a slot: status must stay answerable while a
            // long workspace pass holds the server.
            match shared.try_lock() {
                Ok(state) => {
                    if matches!(*state, LspClientState::Ready(_)) {
                        active.push(key.server_id);
                    }
                    workspace["state"] = Value::String(state.label().to_string());
                    if let LspClientState::Ready(client) = &*state {
                        if let Some(last_used) = last_used {
                            workspace["idle_seconds"] =
                                json!(last_used.max(client.last_activity).elapsed().as_secs());
                        }
                        workspace["pid"] = json!(client.child.id());
                        workspace["open_documents"] = json!(client.opened_versions.len());
                        workspace["rss_bytes"] =
                            json!(client.child.id().and_then(process_rss_bytes));
                    }
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

    /// Explicit release affects only this service's exclusively leased idle
    /// slots. Busy/queued requests and other live service owners are preserved.
    pub async fn release_idle(&self, idle_seconds: u64) -> Result<Value> {
        #[cfg(unix)]
        if let Some(value) = self
            .shared_request(broker::Operation::ReleaseIdle { idle_seconds })
            .await?
        {
            return Ok(value);
        }
        Ok(self.release_idle_local(idle_seconds).await)
    }

    async fn release_idle_local(&self, idle_seconds: u64) -> Value {
        let mine = self.leased.lock().await.clone();
        let mut released = Vec::new();
        let mut skipped = Vec::new();
        let mut clients = self.clients.lock().await;
        clients.retain(|key, slot| {
            if !mine.contains(key) {
                return true;
            }
            let shared_owner = slot
                .owners
                .iter()
                .filter_map(|owner| owner.upgrade())
                .any(|owner| !std::sync::Arc::ptr_eq(&owner, &self.leased));
            let idle_for = slot
                .client
                .try_lock()
                .ok()
                .map(|state| slot_idle_for(slot, &state));
            let reason = if shared_owner {
                Some("shared with another session")
            } else if idle_for.is_some_and(|idle| idle < Duration::from_secs(idle_seconds)) {
                Some("recent analysis")
            } else if std::sync::Arc::strong_count(&slot.client) != 1 || idle_for.is_none() {
                Some("active or queued request")
            } else {
                None
            };
            let record =
                json!({"server": key.server_id, "root": key.workspace_root, "reason": reason});
            if reason.is_some() {
                skipped.push(record);
                true
            } else {
                released.push(record);
                false
            } // kill_on_drop owns exactly this child
        });
        json!({"released": released, "skipped": skipped, "minimum_idle_seconds": idle_seconds})
    }

    pub fn supported_status() -> Value {
        json!(supported_server_status())
    }

    pub async fn diagnostics(&self, path: &Path) -> Result<Value> {
        #[cfg(unix)]
        if let Some(value) = self
            .shared_request(broker::Operation::Diagnostics {
                path: path.to_path_buf(),
            })
            .await?
        {
            return Ok(value);
        }
        self.diagnostics_local(path).await
    }

    async fn diagnostics_local(&self, path: &Path) -> Result<Value> {
        let (path, uri, spec, workspace_root) = self.resolve_document(path).await?;
        let shared = self.lease_client(spec, &workspace_root).await;
        let mut slot = lock_client(&shared, None).await?;
        let client = ready_client(&mut slot, spec, &workspace_root, &path, None).await?;
        let mut report = client
            .document_diagnostics(&path, &uri, spec.language_id, None)
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
                owners: Vec::new(),
            });
            let owner = std::sync::Arc::downgrade(&self.leased);
            slot.owners.retain(|owner| owner.strong_count() != 0);
            if !slot.owners.iter().any(|known| known.ptr_eq(&owner)) {
                slot.owners.push(owner);
            }
            slot.last_used = Instant::now();
            slot.client.clone()
        };
        discard_dead_client(&client).await;
        client
    }

    /// Snapshot of every slot, taken under the map lock and released before
    /// any server is spoken to. Analysis snapshots count as use; status does
    /// not renew TTL, or monitoring would retain idle servers indefinitely.
    async fn active_clients(&self, touch: bool) -> Vec<(LspClientKey, SharedLspClient)> {
        let leased = self.leased.lock().await.clone();
        let snapshot = {
            let mut clients = self.clients.lock().await;
            let now = Instant::now();
            clients
                .iter_mut()
                .filter(|(key, _)| leased.contains(*key))
                .map(|(key, slot)| {
                    if touch {
                        slot.last_used = now;
                    }
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
        #[cfg(unix)]
        if let Some(value) = self
            .shared_request(broker::Operation::Hover {
                path: path.to_path_buf(),
                line,
                character,
            })
            .await?
        {
            return Ok(value);
        }
        self.position_request(path, "textDocument/hover", line, character, json!({}))
            .await
    }

    pub async fn definition(&self, path: &Path, line: u32, character: u32) -> Result<Value> {
        #[cfg(unix)]
        if let Some(value) = self
            .shared_request(broker::Operation::Definition {
                path: path.to_path_buf(),
                line,
                character,
            })
            .await?
        {
            return Ok(value);
        }
        self.position_request(path, "textDocument/definition", line, character, json!({}))
            .await
    }

    pub async fn references(&self, path: &Path, line: u32, character: u32) -> Result<Value> {
        #[cfg(unix)]
        if let Some(value) = self
            .shared_request(broker::Operation::References {
                path: path.to_path_buf(),
                line,
                character,
            })
            .await?
        {
            return Ok(value);
        }
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
        #[cfg(unix)]
        if let Some(value) = self
            .shared_request(broker::Operation::DocumentSymbols {
                path: path.to_path_buf(),
            })
            .await?
        {
            return Ok(value);
        }
        self.document_request(path, "textDocument/documentSymbol", json!({}))
            .await
    }

    pub async fn workspace_symbols(&self, query: &str) -> Result<Value> {
        #[cfg(unix)]
        if let Some(value) = self
            .shared_request(broker::Operation::WorkspaceSymbols {
                query: query.to_owned(),
            })
            .await?
        {
            return Ok(value);
        }
        self.workspace_symbols_local(query).await
    }

    async fn workspace_symbols_local(&self, query: &str) -> Result<Value> {
        let clients = self.active_clients(true).await;
        if clients.is_empty() {
            return Err(self.no_active_server_error().await);
        }
        let server_counts = server_counts(&clients);
        let mut results = serde_json::Map::new();
        for (key, shared) in &clients {
            let label = workspace_label(key, server_counts[key.server_id]);
            let mut slot = lock_client(shared, None).await?;
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
        #[cfg(unix)]
        if let Some(value) = self
            .shared_request(broker::Operation::WorkspaceDiagnostics {
                path: path.map(Path::to_path_buf),
            })
            .await?
        {
            return Ok(value);
        }
        self.workspace_diagnostics_local(path).await
    }

    /// Model-facing projection; the raw LSP/broker API remains lossless.
    pub async fn workspace_diagnostics_for_tool(&self, path: Option<&Path>) -> Result<Value> {
        let report = self.workspace_diagnostics(path).await?;
        workspace_diagnostics_tool_report(
            report,
            &crate::host_paths::host_home().join("diagnostics"),
        )
        .await
    }

    async fn workspace_diagnostics_local(&self, path: Option<&Path>) -> Result<Value> {
        // Include path discovery, bootstrap and slot contention in the SAME
        // budget as analysis, not another 120 seconds before the clock starts.
        let deadline = Instant::now() + WORKSPACE_DIAGNOSTICS_BUDGET;
        let bootstrap = if let Some(path) = path {
            match timeout_at(deadline, async {
                let resolved = self.resolve_document(path).await?;
                self.lease_client(resolved.2, &resolved.3).await;
                Ok::<_, anyhow::Error>(resolved)
            })
            .await
            {
                Ok(result) => Some(result?),
                Err(_) => return Ok(json!({ "bootstrap": skipped_for_budget_report() })),
            }
        } else {
            None
        };

        let clients = self.active_clients(true).await;
        if clients.is_empty() {
            return Err(self.no_active_server_error().await);
        }
        let server_counts = server_counts(&clients);
        let mut results = serde_json::Map::new();
        for (key, shared) in &clients {
            let label = workspace_label(key, server_counts[key.server_id]);
            if Instant::now() >= deadline {
                results.insert(label, skipped_for_budget_report());
                continue;
            }
            let mut slot = match lock_client(shared, Some(deadline)).await {
                Ok(slot) => slot,
                Err(error) => {
                    results.insert(label, workspace_diagnostic_result(Err(error)));
                    continue;
                }
            };
            if let Some((path, uri, spec, root)) = &bootstrap
                && spec.id == key.server_id
                && root == &key.workspace_root
            {
                let started = timeout_at(deadline, async {
                    let client = ready_client(&mut slot, spec, root, path, Some(deadline)).await?;
                    client.open_document(path, uri, spec.language_id).await?;
                    Ok::<_, anyhow::Error>(())
                })
                .await
                .context("workspace diagnostic time budget exhausted during bootstrap")
                .and_then(|result| result);
                if let Err(error) = started {
                    results.insert(label, workspace_diagnostic_result(Err(error)));
                    continue;
                }
            }
            let Some(client) = slot.ready_mut() else {
                results.insert(label, slot.unavailable_report());
                continue;
            };
            let report = timeout_at(deadline, client.workspace_diagnostics(deadline)).await;
            if report.is_err() {
                // The outer timer may drop the nested request before its own
                // cancellation cleanup runs. Do not leave it running until reuse.
                let _ = timeout(CANCEL_TIMEOUT, client.cancel_pending_request()).await;
            }
            let value = workspace_diagnostic_result(
                match report
                    .context("workspace diagnostic time budget exhausted")
                    .and_then(|result| result)
                {
                    Ok(value) => Ok(value),
                    Err(error) if is_unknown_workspace_diagnostics_request(&error) => {
                        let spec = spec_for_id(key.server_id).with_context(|| {
                            format!("unknown language server `{}`", key.server_id)
                        })?;
                        client
                            .document_workspace_diagnostics(&key.workspace_root, spec, deadline)
                            .await
                            .with_context(|| {
                                format!("{label} document diagnostics fallback failed ({error:#})")
                            })
                    }
                    Err(error) => Err(error)
                        .with_context(|| format!("{label} workspace diagnostics request failed")),
                },
            );
            results.insert(label, value);
        }
        Ok(Value::Object(results))
    }

    async fn document_request(&self, path: &Path, method: &str, extra: Value) -> Result<Value> {
        let (path, uri, spec, workspace_root) = self.resolve_document(path).await?;
        let shared = self.lease_client(spec, &workspace_root).await;
        let mut slot = lock_client(&shared, None).await?;
        let client = ready_client(&mut slot, spec, &workspace_root, &path, None).await?;
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
        let mut slot = lock_client(&shared, None).await?;
        let client = ready_client(&mut slot, spec, &workspace_root, &path, None).await?;
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

fn configure_private_preamble_storage(
    spec: &ServerSpec,
    command: &mut Command,
) -> Result<Option<tempfile::TempDir>> {
    if spec.id != "clangd" {
        return Ok(None);
    }
    let parent = crate::host_paths::host_home().join("lsp-preambles");
    std::fs::create_dir_all(&parent).context("create clangd preamble storage root")?;
    let directory = tempfile::Builder::new()
        .prefix("clangd-")
        .tempdir_in(parent)
        .context("create private clangd preamble storage")?;
    for name in ["TMPDIR", "TMP", "TEMP"] {
        command.env(name, directory.path());
    }
    Ok(Some(directory))
}

impl LspClient {
    async fn start(spec: &ServerSpec, root: &Path, deadline: Option<Instant>) -> Result<Self> {
        let mut command = Command::new(spec.command);
        let private_preamble_storage = configure_private_preamble_storage(spec, &mut command)?;
        let mut child = command
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
            last_activity: Instant::now(),
            incoming: Vec::new(),
            outgoing: Vec::new(),
            outgoing_written: 0,
            pending_request: None,
            supports_pull_diagnostics: false,
            compilation_configuration: compilation_configuration_stamp(spec, root).await,
            opened_versions: HashMap::new(),
            open_order: VecDeque::new(),
            last_versions: HashMap::new(),
            published_diagnostics: HashMap::new(),
            _private_preamble_storage: private_preamble_storage,
        };
        let root_uri = Url::from_directory_path(root)
            .map_err(|_| anyhow::anyhow!("cannot convert workspace root to URI"))?
            .to_string();
        let mut initialize = json!({
            "processId": std::process::id(),
            "rootUri": root_uri,
            "workspaceFolders": [{
                "uri": root_uri,
                "name": root.file_name().and_then(|name| name.to_str()).unwrap_or("workspace")
            }],
            "capabilities": {
                "general": { "positionEncodings": ["utf-16"] },
                "textDocument": {
                    "hover": { "contentFormat": ["markdown", "plaintext"] },
                    "definition": { "linkSupport": true },
                    "documentSymbol": { "hierarchicalDocumentSymbolSupport": true },
                    "diagnostic": {},
                    "publishDiagnostics": { "versionSupport": true }
                },
                "workspace": { "symbol": { "resolveSupport": { "properties": [] } } }
            },
            "clientInfo": { "name": "borg", "version": env!("CARGO_PKG_VERSION") }
        });
        if let Some(options) = server_initialization_options(spec) {
            initialize["initializationOptions"] = options;
        }
        let initialized = client
            .request_with_timeout(
                "initialize",
                initialize,
                deadline.map_or(INITIALIZE_TIMEOUT, |end| {
                    INITIALIZE_TIMEOUT.min(end.saturating_duration_since(Instant::now()))
                }),
            )
            .await?;
        client.supports_pull_diagnostics = initialized
            .pointer("/capabilities/diagnosticProvider")
            .is_some_and(Value::is_object);
        client.notify("initialized", json!({})).await?;
        Ok(client)
    }

    async fn open_document(&mut self, path: &Path, uri: &str, language_id: &str) -> Result<String> {
        let text = tokio::fs::read_to_string(path)
            .await
            .with_context(|| format!("cannot read {}", path.display()))?;
        let was_open = self.opened_versions.contains_key(path);
        if !was_open
            && self.opened_versions.len() >= MAX_OPEN_DOCUMENTS
            && let Some(oldest) = self.open_order.front().cloned()
        {
            let old_uri = Url::from_file_path(&oldest)
                .map_err(|_| anyhow::anyhow!("invalid LSP document path"))?;
            self.close_document(&oldest, old_uri.as_str()).await?;
        }
        self.open_order.retain(|opened| opened != path);
        self.open_order.push_back(path.to_path_buf());
        let version = self
            .last_versions
            .get(path)
            .copied()
            .unwrap_or(0)
            .checked_add(1)
            .context("LSP document version exhausted")?;
        self.last_versions.insert(path.to_path_buf(), version);
        self.opened_versions.insert(path.to_path_buf(), version);
        // Do not return a cached earlier generation after didChange/didOpen.
        self.published_diagnostics.remove(uri);
        let method = if was_open {
            "textDocument/didChange"
        } else {
            "textDocument/didOpen"
        };
        let params = if !was_open {
            json!({
                "textDocument": {
                    "uri": uri,
                    "languageId": language_id,
                    "version": version,
                    "text": text.clone()
                }
            })
        } else {
            json!({
                "textDocument": { "uri": uri, "version": version },
                "contentChanges": [{ "text": text.clone() }],
                // clangd may suppress diagnostics for an unchanged buffer,
                // even though this client waits for the new version. Its
                // protocol extension requests publication for this generation;
                // other servers ignore the additional field.
                "wantDiagnostics": true
            })
        };
        self.notify(method, params).await?;
        Ok(text)
    }

    async fn close_document(&mut self, path: &Path, uri: &str) -> Result<()> {
        if self.opened_versions.remove(path).is_none() {
            return Ok(());
        }
        self.open_order.retain(|opened| opened != path);
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
        deadline: Option<Instant>,
    ) -> Result<Value> {
        // didOpen triggers the first parse/preamble build independently of
        // initialize. Large C++ units can need more than the warm 15-second
        // budget even after initialize has answered. Bound BOTH the pull and
        // push fallback by the workspace's remaining budget: checking only
        // between files allowed one cold file to add another 120 seconds.
        let publish_timeout = if self.last_versions.contains_key(path) {
            PUBLISHED_DIAGNOSTICS_TIMEOUT
        } else {
            INITIALIZE_TIMEOUT
        };
        let remaining = |maximum: Duration| {
            deadline.map_or(maximum, |end| {
                maximum.min(end.saturating_duration_since(Instant::now()))
            })
        };
        // clangd can suppress an identical diagnostic payload even with
        // wantDiagnostics=true. Reopen for an explicit diagnostic request so
        // the publication carries this generation instead of timing out.
        if spec_for_path(path).is_some_and(|spec| spec.id == "clangd") {
            self.close_document(path, uri).await?;
        }
        self.open_document(path, uri, language_id).await?;
        // Push-only servers (including clangd) do not implement this request.
        // Preserve the publication timeout/error instead of hiding it behind
        // an expected MethodNotFound from an unadvertised pull capability.
        if !self.supports_pull_diagnostics {
            return self
                .wait_for_published_diagnostics(uri, remaining(publish_timeout))
                .await;
        }
        let pull_timeout = remaining(REQUEST_TIMEOUT);
        if pull_timeout.is_zero() {
            bail!("workspace diagnostic time budget exhausted");
        }
        match self
            .request_with_timeout(
                "textDocument/diagnostic",
                json!({ "textDocument": { "uri": uri } }),
                pull_timeout,
            )
            .await
        {
            Ok(result) => Ok(result),
            Err(pull_error) => self
                .wait_for_published_diagnostics(uri, remaining(publish_timeout))
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
        let scan = match timeout_at(
            deadline,
            discover_workspace_documents(workspace_root, spec.extensions),
        )
        .await
        {
            Ok(scan) => scan,
            Err(_) => return Ok(skipped_for_budget_report()),
        };
        let total = scan.paths.len();
        let mut items = Vec::new();
        let mut failed = 0usize;
        let mut exhausted = false;
        let mut interrupted = None;
        for path in scan.paths {
            if Instant::now() >= deadline {
                exhausted = true;
                break;
            }
            let uri = Url::from_file_path(&path)
                .map_err(|_| anyhow::anyhow!("cannot convert {} to a file URI", path.display()))?
                .to_string();
            let report = timeout_at(
                deadline,
                self.document_diagnostics(&path, &uri, spec.language_id, Some(deadline)),
            )
            .await;
            if report.is_err() {
                let _ = timeout(CANCEL_TIMEOUT, self.cancel_pending_request()).await;
            }
            let report = report
                .context("workspace diagnostic time budget exhausted")
                .and_then(|result| result);
            let close_error = timeout_at(deadline, self.close_document(&path, &uri))
                .await
                .context("workspace diagnostic time budget exhausted during document cleanup")
                .and_then(|result| result)
                .err();
            let diagnostic_failed = report.is_err();
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
            if let Some(error) = close_error {
                let error = format!("failed to close {}: {error:#}", path.display());
                if let Some(document) = items.last_mut() {
                    document["closeError"] = json!(error);
                }
                failed += usize::from(!diagnostic_failed);
                interrupted = Some(error);
                break;
            }
            // The last discovered file may itself exhaust the deadline. In
            // that case there is no next loop iteration to mark this partial.
            if Instant::now() >= deadline {
                exhausted = true;
                break;
            }
        }
        let scanned = items.len();
        let mut result = json!({ "kind": "full", "items": items });
        if failed > 0 {
            result["failedDocuments"] = json!(failed);
        }
        let mut partial_reasons = Vec::new();
        if let Some(error) = interrupted {
            partial_reasons.push(error);
        }
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
        if let Ok(Some(context)) = timeout_at(
            deadline,
            compilation_context_status(workspace_root, spec, None),
        )
        .await
        {
            result["compilationContext"] = context;
        }
        Ok(result)
    }

    async fn workspace_diagnostics(&mut self, deadline: Instant) -> Result<Value> {
        self.request_with_timeout(
            "workspace/diagnostic",
            json!({ "previousResultIds": [] }),
            REQUEST_TIMEOUT.min(deadline.saturating_duration_since(Instant::now())),
        )
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
        let result = tokio::select! {
            biased;
            _ = request_cancelled() => Err(anyhow::anyhow!("LSP request cancelled")),
            result = timeout(deadline, async {
                self.cancel_pending_request().await?;
                let id = self.next_id;
                self.next_id += 1;
                self.pending_request = Some(id);
                self.write_message(&json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "method": method,
                    "params": params
                })).await?;
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
                        })).await?;
                        continue;
                    }
                    if message.get("id").and_then(Value::as_u64) != Some(id) {
                        continue;
                    }
                    self.pending_request = None;
                    if let Some(error) = message.get("error") {
                        bail!("{method} failed: {error}");
                    }
                    return Ok(message.get("result").cloned().unwrap_or(Value::Null));
                }
            }) => result.with_context(|| format!("{method} timed out")).and_then(|result| result),
        };
        self.last_activity = Instant::now();
        if result.is_err() {
            // Best effort now; an interrupted write/cancel stays queued and
            // MUST finish before any later request is emitted on this client.
            let _ = timeout(CANCEL_TIMEOUT, self.cancel_pending_request()).await;
        }
        result
    }

    async fn cancel_pending_request(&mut self) -> Result<()> {
        if let Some(id) = self.pending_request.take() {
            // write_message queues the complete frame before its first await.
            // Retrying an interrupted cancel must flush it, not enqueue it twice.
            self.notify("$/cancelRequest", json!({ "id": id })).await?;
        }
        self.flush_outgoing().await
    }

    fn capture_diagnostics(&mut self, message: &Value) {
        if message.get("method").and_then(Value::as_str) != Some("textDocument/publishDiagnostics")
        {
            return;
        }
        let Some(uri) = message.pointer("/params/uri").and_then(Value::as_str) else {
            return;
        };
        let Some(path) = Url::parse(uri).ok().and_then(|uri| uri.to_file_path().ok()) else {
            return;
        };
        let Some(current) = self.opened_versions.get(&path).copied() else {
            // Includes didClose's empty publication: it is not a source check.
            return;
        };
        let version = message.pointer("/params/version");
        if let Some(version) = version.filter(|value| !value.is_null()) {
            if version.as_i64() != Some(i64::from(current)) {
                return;
            }
        } else if current != 1 {
            // An unversioned publication after edit/reopen cannot establish
            // freshness. Keep first-open compatibility, but fail closed later
            // (pull diagnostics may still work). Version-aware servers were
            // explicitly told to include versions in initialize.
            return;
        }
        let diagnostics = message
            .pointer("/params/diagnostics")
            .cloned()
            .unwrap_or_else(|| json!([]));
        self.published_diagnostics
            .insert(uri.to_string(), diagnostics);
    }

    async fn wait_for_published_diagnostics(
        &mut self,
        uri: &str,
        publish_timeout: Duration,
    ) -> Result<Value> {
        if let Some(items) = self.published_diagnostics.remove(uri) {
            return Ok(json!({ "kind": "full", "items": items }));
        }
        tokio::select! {
            biased;
            _ = request_cancelled() => bail!("LSP diagnostic wait cancelled"),
            result = timeout(publish_timeout, async {
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
        }) => result.context("language server did not publish diagnostics")?,
        }
    }

    async fn write_message(&mut self, message: &Value) -> Result<()> {
        self.last_activity = Instant::now();
        let body = serde_json::to_vec(message)?;
        // Queue the complete frame before awaiting: didOpen/didClose have
        // already updated document state, even if an older write was paused.
        self.outgoing
            .extend_from_slice(format!("Content-Length: {}\r\n\r\n", body.len()).as_bytes());
        self.outgoing.extend_from_slice(&body);
        timeout(REQUEST_TIMEOUT, self.flush_outgoing())
            .await
            .context("timed out writing to language server")?
    }

    async fn flush_outgoing(&mut self) -> Result<()> {
        while self.outgoing_written < self.outgoing.len() {
            // write (unlike write_all) is cancellation safe. Commit each
            // successful offset before the next await, even if a tool drops us.
            let written = self
                .stdin
                .write(&self.outgoing[self.outgoing_written..])
                .await?;
            if written == 0 {
                bail!("language server closed stdin");
            }
            self.outgoing_written += written;
        }
        self.stdin.flush().await?;
        self.outgoing.clear();
        self.outgoing_written = 0;
        Ok(())
    }

    async fn read_message(&mut self) -> Result<Value> {
        const MAX_HEADER_BYTES: usize = 8192;
        loop {
            // Search only the bounded header prefix, not the growing body.
            let header = &self.incoming[..self.incoming.len().min(MAX_HEADER_BYTES)];
            let header_end = header
                .windows(4)
                .position(|bytes| bytes == b"\r\n\r\n")
                .map(|end| end + 4)
                .into_iter()
                .chain(
                    header
                        .windows(2)
                        .position(|bytes| bytes == b"\n\n")
                        .map(|end| end + 2),
                )
                .min();
            if let Some(end) = header_end.filter(|end| *end <= MAX_HEADER_BYTES) {
                let header = std::str::from_utf8(&self.incoming[..end])?;
                let length = header
                    .lines()
                    .find_map(|line| {
                        line.strip_prefix("Content-Length:")
                            .and_then(|value| value.trim().parse::<usize>().ok())
                    })
                    .context("LSP response omitted Content-Length")?;
                if length > MAX_LSP_MESSAGE_BYTES {
                    bail!("LSP response exceeds {} bytes", MAX_LSP_MESSAGE_BYTES);
                }
                if self.incoming.len() >= end + length {
                    let message = serde_json::from_slice(&self.incoming[end..end + length])
                        .context("language server returned invalid JSON");
                    self.incoming.drain(..end + length);
                    self.last_activity = Instant::now();
                    return message;
                }
            } else if self.incoming.len() >= MAX_HEADER_BYTES {
                bail!("LSP response header exceeds {MAX_HEADER_BYTES} bytes");
            }
            let mut bytes = [0; 8192];
            let read = self.stdout.read(&mut bytes).await?;
            if read == 0 {
                let status = self.child.try_wait()?;
                bail!("language server closed stdout (status: {status:?})");
            }
            self.incoming.extend_from_slice(&bytes[..read]);
        }
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
// clangd caches fallback compile commands for already open documents. A newly
// supplied database/config must restart the owned server, not merely relabel
// old diagnostics as having a present compilation context.
async fn compilation_configuration_stamp(
    spec: &ServerSpec,
    root: &Path,
) -> Vec<(PathBuf, u64, std::time::SystemTime)> {
    if spec.id != "clangd" {
        return Vec::new();
    }
    let mut stamp = Vec::new();
    for relative in [
        "compile_commands.json",
        "build/compile_commands.json",
        "build/compile_flags.txt",
        "compile_flags.txt",
        ".clangd",
    ] {
        let path = root.join(relative);
        if let Ok(metadata) = tokio::fs::metadata(&path).await {
            stamp.push((
                path,
                metadata.len(),
                metadata.modified().unwrap_or(std::time::UNIX_EPOCH),
            ));
        }
    }
    // clangd inherits .clangd files above a nested CMake/project root too.
    // Keep those in the stamp so a parent override change cannot leave a
    // warm client serving diagnostics from a different compilation context.
    for ancestor in root.ancestors().skip(1) {
        let path = ancestor.join(".clangd");
        if let Ok(metadata) = tokio::fs::metadata(&path).await {
            stamp.push((
                path,
                metadata.len(),
                metadata.modified().unwrap_or(std::time::UNIX_EPOCH),
            ));
        }
    }
    stamp
}

async fn ready_client<'a>(
    slot: &'a mut LspClientState,
    spec: &'static ServerSpec,
    root: &Path,
    document: &Path,
    deadline: Option<Instant>,
) -> Result<&'a mut LspClient> {
    let mut configuration = compilation_configuration_stamp(spec, root).await;
    // A document may use a nearer .clangd/compile_flags.txt than the workspace
    // root. Refresh those configurations too; otherwise old fallback errors
    // can be returned alongside metadata claiming the new flags are present.
    if spec.id == "clangd" {
        let mut directories = std::collections::BTreeSet::new();
        let mut documents = vec![document.to_path_buf()];
        if let Some(client) = slot.ready_mut() {
            documents.extend(client.opened_versions.keys().cloned());
        }
        for path in documents {
            let mut parent = path.parent();
            while let Some(directory) = parent {
                if directory == root || !directory.starts_with(root) {
                    break;
                }
                directories.insert(directory.to_path_buf());
                parent = directory.parent();
            }
        }
        for directory in directories {
            configuration.extend(compilation_configuration_stamp(spec, &directory).await);
        }
    }
    // Ancestors may be reached from several opened documents. Compare the
    // configuration set, not duplicate counts or the order of document visits.
    configuration.sort();
    configuration.dedup();
    let changed = slot
        .ready_mut()
        .is_some_and(|client| client.compilation_configuration != configuration);
    if changed {
        // Dropping only this slot kills its own child (kill_on_drop). Other
        // workspaces, providers and user-owned language servers are untouched.
        *slot = LspClientState::NotStarted;
    }
    if !matches!(slot, LspClientState::Ready(_)) {
        match LspClient::start(spec, root, deadline).await {
            Ok(mut client) => {
                client.compilation_configuration = configuration;
                *slot = LspClientState::Ready(Box::new(client));
            }
            Err(error) => {
                *slot = LspClientState::Failed(format!("{error:#}"));
                return Err(error);
            }
        }
    }
    let client = slot.ready_mut().expect("started LSP client");
    timeout(REQUEST_TIMEOUT, client.cancel_pending_request())
        .await
        .context("timed out cancelling an abandoned LSP request")??;
    Ok(client)
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
            let pressure = host_memory_pressure();
            clients.lock().await.retain(|key, slot| {
                // External Arcs include queued requests not yet holding the lock.
                if std::sync::Arc::strong_count(&slot.client) != 1 {
                    return true;
                }
                let Ok(state) = slot.client.try_lock() else {
                    return true;
                };
                let idle_for = slot_idle_for(slot, &state);
                let reason = lsp_client_reap_reason(key, idle_for).or_else(|| {
                    (pressure && idle_for >= LSP_PRESSURE_IDLE_TIMEOUT)
                        .then_some("stopped while idle under host memory pressure")
                });
                let Some(reason) = reason else {
                    return true;
                };
                drop(state);
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

fn slot_idle_for(slot: &LspClientSlot, state: &LspClientState) -> Duration {
    let last_activity = match state {
        LspClientState::Ready(client) => slot.last_used.max(client.last_activity),
        _ => slot.last_used,
    };
    last_activity.elapsed()
}

fn host_memory_pressure() -> bool {
    std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|text| {
            text.lines().find_map(|line| {
                line.strip_prefix("MemAvailable:")?
                    .split_whitespace()
                    .next()?
                    .parse::<u64>()
                    .ok()
            })
        })
        .is_some_and(|kib| kib.saturating_mul(1024) < LSP_MIN_AVAILABLE_BYTES)
}

fn process_rss_bytes(pid: u32) -> Option<u64> {
    std::fs::read_to_string(format!("/proc/{pid}/status"))
        .ok()?
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmRSS:")?
                .split_whitespace()
                .next()?
                .parse::<u64>()
                .ok()
        })
        .map(|kib| kib.saturating_mul(1024))
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
            "cargo": { "allTargets": false, "buildScripts": { "enable": false } },
            "procMacro": { "enable": false },
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
            "time budget of {}s exhausted during workspace diagnostics; query this workspace directly",
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

// clangd configuration may choose a different database per path. Finding a
// nearby database is not proof that clangd uses it; avoid a false "present" /
// coversFile claim until configuration selection can be resolved faithfully.
async fn clangd_database_override(search_start: &Path) -> Option<PathBuf> {
    // A discovered nested project root does not stop clangd's configuration
    // inheritance. Its parent .clangd may override a nearer compile database.
    for directory in search_start.ancestors() {
        let path = directory.join(".clangd");
        if let Ok(contents) = tokio::fs::read_to_string(&path).await
            && contents.lines().any(|line| {
                let line = line.trim();
                !line.starts_with('#')
                    && line.split_once(':').is_some_and(|(key, _)| {
                        key.trim().trim_matches(['\'', '"']) == "CompilationDatabase"
                    })
            })
        {
            return Some(path);
        }
    }
    None
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
    if let Some(configuration) = clangd_database_override(&search_start).await {
        return Some(json!({
            "status": "unverified",
            "diagnosticTrust": "clangd_database_override_not_code_error_proof",
            "clangdConfiguration": configuration,
            "coversFile": "unknown",
            "warning": "clangd configuration overrides CompilationDatabase (possibly per path); \
                a nearby database does not establish which flags clangd actually used. \
                Configuration selection must be verified before treating these diagnostics as code-error proof"
        }));
    }
    let Some(database) = find_compilation_database(&search_start, workspace_root).await else {
        return Some(json!({
            "status": "missing",
            "diagnosticTrust": "fallback_flags_not_code_error_proof",
            "searchedFrom": search_start,
            "warning": "no compile_commands.json or compile_flags.txt was found; \
        clangd is using fallback flags, so missing-include and unknown-type diagnostics \
        likely describe absent build configuration rather than defects in the code"
        }));
    };
    let mut status = json!({
        "status": "present",
        "kind": database.kind,
        "path": database.path,
        "provenance": "filesystem_database_discovery",
        "effectiveFlagsVerified": false,
        "effectiveFlagsCaveat": "Database presence and file-entry coverage do not establish \
            clangd's selected effective command. Conditional, inherited or user CompileFlags \
            may modify it; the effective command has not been observed."
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
                status["diagnosticTrust"] = json!("inferred_flags_not_code_error_proof");
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
            status["sourceNewerThanDatabase"] = Value::Bool(true);
            status["staleAgainstSource"] = Value::Bool(true);
            status["timestampCaveat"] = json!(
                "Source mtime alone does not prove compilation flags are stale; source edits normally postdate the database."
            );
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
            (
                PathBuf::from("build").join("compile_flags.txt"),
                "compile_flags.txt",
            ),
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

// One failed server must not hide diagnostics from healthy workspaces. Keep
// failure explicit: an empty item array here is NOT a clean diagnostic pass.
fn workspace_diagnostic_result(result: Result<Value>) -> Value {
    result.unwrap_or_else(|error| {
        json!({
            "items": [],
            "partial": true,
            "partialReason": format!("{error:#}")
        })
    })
}

fn is_unknown_workspace_diagnostics_request(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    message.contains("\"code\":-32601") || message.contains("unknown request")
}

const MAX_WORKSPACE_TOOL_REPORT_BYTES: usize = 32 * 1024;
const MAX_WORKSPACE_TOOL_SAMPLES: usize = 12;

async fn workspace_diagnostics_tool_report(report: Value, directory: &Path) -> Result<Value> {
    let full = serde_json::to_vec(&report)?;
    if full.len() <= MAX_WORKSPACE_TOOL_REPORT_BYTES {
        return Ok(report);
    }
    tokio::fs::create_dir_all(directory).await?;
    let path = directory.join(format!("{}.json", uuid::Uuid::new_v4()));
    let mut file = tokio::fs::File::create(&path).await?;
    file.write_all(&full).await?;
    file.sync_all().await?;
    let mut counts = [0usize; 5];
    let mut documents = 0usize;
    let mut samples = Vec::new();
    let mut servers = serde_json::Map::new();
    if let Some(reports) = report.as_object() {
        for (server, report) in reports {
            collect_workspace_diagnostics(report, "", &mut documents, &mut counts, &mut samples);
            if servers.len() < 16 {
                let mut metadata = serde_json::Map::new();
                for key in [
                    "kind",
                    "unavailable",
                    "partial",
                    "partialReason",
                    "failedDocuments",
                    "documentsScanned",
                    "documentsDiscovered",
                    "compilationContext",
                    "error",
                    "message",
                ] {
                    if let Some(value) = report.get(key) {
                        let value = if serde_json::to_vec(value)?.len() > 1024 {
                            json!({"truncated": true, "detail": value.to_string().chars().take(512).collect::<String>()})
                        } else {
                            value.clone()
                        };
                        metadata.insert(key.to_owned(), value);
                    }
                }
                servers.insert(server.chars().take(256).collect(), Value::Object(metadata));
            }
        }
    }
    samples.sort_by_key(|sample| sample["severity"].as_u64().unwrap_or(5));
    samples.truncate(MAX_WORKSPACE_TOOL_SAMPLES);
    let mut projected = json!({
        "summarized": true,
        "full_report_path": path,
        "full_report_bytes": full.len(),
        "documents": documents,
        "diagnostic_counts": {"errors": counts[1], "warnings": counts[2],
            "information": counts[3], "hints": counts[4], "unspecified": counts[0]},
        "servers": servers,
        "samples": samples,
        "instruction": "Full diagnostics are preserved in full_report_path. Samples prioritize errors; use targeted lsp_diagnostics or read the file for complete details."
    });
    while serde_json::to_vec(&projected)?.len() > MAX_WORKSPACE_TOOL_REPORT_BYTES {
        projected["summary_truncated"] = json!(true);
        if !projected["samples"].as_array_mut().unwrap().is_empty() {
            projected["samples"].as_array_mut().unwrap().pop();
        } else if let Some(key) = projected["servers"]
            .as_object()
            .unwrap()
            .keys()
            .next_back()
            .cloned()
        {
            projected["servers"].as_object_mut().unwrap().remove(&key);
        } else {
            break;
        }
    }
    Ok(projected)
}

fn collect_workspace_diagnostics(
    report: &Value,
    uri: &str,
    documents: &mut usize,
    counts: &mut [usize; 5],
    samples: &mut Vec<Value>,
) {
    let uri = report["uri"].as_str().unwrap_or(uri);
    if report.get("uri").is_some() {
        *documents += 1;
    }
    if let Some(items) = report["items"].as_array() {
        for item in items {
            if item.get("message").is_some() && item.get("range").is_some() {
                let severity = item["severity"]
                    .as_u64()
                    .filter(|v| (1..=4).contains(v))
                    .unwrap_or(0) as usize;
                counts[severity] += 1;
                let rank = if severity == 0 { 5 } else { severity as u64 };
                if samples.len() < MAX_WORKSPACE_TOOL_SAMPLES
                    || samples
                        .iter()
                        .any(|sample| sample["severity"].as_u64().unwrap_or(5) > rank)
                {
                    let sample = json!({
                        "uri": uri.chars().take(1024).collect::<String>(),
                        "severity": if severity == 0 { None } else { Some(severity) },
                        "range": {"start": {"line": item["range"]["start"]["line"].as_u64(),
                            "character": item["range"]["start"]["character"].as_u64()},
                            "end": {"line": item["range"]["end"]["line"].as_u64(),
                            "character": item["range"]["end"]["character"].as_u64()}},
                        "message": item["message"].as_str().unwrap_or("").chars().take(512).collect::<String>()
                    });
                    if !samples.contains(&sample) {
                        samples.push(sample);
                    }
                    samples.sort_by_key(|sample| sample["severity"].as_u64().unwrap_or(5));
                    samples.truncate(MAX_WORKSPACE_TOOL_SAMPLES);
                }
            } else {
                collect_workspace_diagnostics(item, uri, documents, counts, samples);
            }
        }
    }
    if let Some(related) = report["relatedDocuments"].as_object() {
        for (uri, report) in related {
            collect_workspace_diagnostics(report, uri, documents, counts, samples);
        }
    }
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
                | ".worktrees"
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
            args: &["--pch-storage=disk"],
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

    #[tokio::test]
    async fn oversized_workspace_tool_reports_preserve_full_data_and_prioritize_errors() {
        let directory = tempfile::tempdir().expect("report directory");
        let warning = json!({"severity": 2, "range": {"start": {"line": 1, "character": 2},
            "end": {"line": 1, "character": 3}}, "message": "warning".repeat(200)});
        let mut diagnostics = vec![warning; 1000];
        diagnostics.push(
            json!({"severity": 1, "range": {"start": {"line": 9, "character": 0},
            "end": {"line": 9, "character": 1}}, "message": "late error"}),
        );
        let full = json!({"clangd": {"partial": true, "partialReason": "budget exhausted",
            "compilationContext": {"coverage": "partial"},
            "items": [{"uri": "file:///broken.cpp", "kind": "full", "items": diagnostics}]},
            "offline": {"unavailable": true, "error": "server unavailable"}});
        let projected = workspace_diagnostics_tool_report(full.clone(), directory.path())
            .await
            .expect("projection");
        assert!(serde_json::to_vec(&projected).unwrap().len() <= MAX_WORKSPACE_TOOL_REPORT_BYTES);
        assert_eq!(projected["diagnostic_counts"]["errors"], 1);
        assert_eq!(projected["diagnostic_counts"]["warnings"], 1000);
        assert_eq!(projected["samples"][0]["message"], "late error");
        assert_eq!(projected["samples"].as_array().unwrap().len(), 2);
        assert_eq!(projected["servers"]["clangd"]["partial"], true);
        assert_eq!(projected["servers"]["offline"]["unavailable"], true);
        assert_eq!(
            projected["servers"]["clangd"]["compilationContext"],
            full["clangd"]["compilationContext"]
        );
        let saved = tokio::fs::read(projected["full_report_path"].as_str().unwrap())
            .await
            .unwrap();
        assert_eq!(serde_json::from_slice::<Value>(&saved).unwrap(), full);
        let small = json!({"clangd": {"items": []}});
        assert_eq!(
            workspace_diagnostics_tool_report(small.clone(), directory.path())
                .await
                .unwrap(),
            small
        );
    }

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

    // Product regression: repeatedly observing status must not keep multi-GB
    // idle indexers alive forever. These pool tests start NO child servers.
    #[tokio::test]
    async fn status_observation_does_not_renew_idle_lease() {
        let root = tempfile::tempdir().unwrap();
        let service = LspService::new(root.path());
        let shared = service
            .lease_client(spec_for_id("clangd").unwrap(), root.path())
            .await;
        drop(shared);
        let key = service.leased.lock().await.iter().next().unwrap().clone();
        let before = Instant::now() - Duration::from_secs(120);
        service
            .clients
            .lock()
            .await
            .get_mut(&key)
            .unwrap()
            .last_used = before;
        let _ = service.status_local().await;
        assert_eq!(
            service.clients.lock().await.get(&key).unwrap().last_used,
            before
        );
        let released = service.release_idle_local(60).await;
        assert_eq!(released["released"].as_array().unwrap().len(), 1);
    }

    // An explicit emergency release must free a recently used but unowned
    // idle slot, without weakening the separate busy/shared ownership checks.
    #[tokio::test]
    async fn zero_idle_release_frees_recent_exclusively_owned_slot() {
        let root = tempfile::tempdir().unwrap();
        let service = LspService::new(root.path());
        drop(
            service
                .lease_client(spec_for_id("clangd").unwrap(), root.path())
                .await,
        );
        let released = service.release_idle_local(0).await;
        assert_eq!(released["minimum_idle_seconds"], 0);
        assert_eq!(released["released"].as_array().unwrap().len(), 1);
    }

    // Product boundary: a cleanup request cannot evict another session's
    // shared workspace or a request queued on the same server mutex.
    #[tokio::test]
    async fn explicit_idle_release_preserves_queued_and_shared_clients() {
        let root = tempfile::tempdir().unwrap();
        let first = LspService::new(root.path());
        let held = first
            .lease_client(spec_for_id("clangd").unwrap(), root.path())
            .await;
        let key = first.leased.lock().await.iter().next().unwrap().clone();
        first.clients.lock().await.get_mut(&key).unwrap().last_used =
            Instant::now() - Duration::from_secs(120);
        assert!(
            first.release_idle_local(0).await["released"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        drop(held);
        let second = LspService::new(root.path());
        drop(
            second
                .lease_client(spec_for_id("clangd").unwrap(), root.path())
                .await,
        );
        first.clients.lock().await.get_mut(&key).unwrap().last_used =
            Instant::now() - Duration::from_secs(120);
        assert!(
            first.release_idle_local(0).await["released"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        drop(second);
        assert_eq!(
            first.release_idle_local(0).await["released"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
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

    // Nested checkout sources can consume the whole scan budget before any
    // active project source is checked. An explicit worktree root is different.
    #[tokio::test]
    async fn workspace_discovery_excludes_nested_worktrees_but_accepts_an_explicit_one() {
        let root = tempfile::tempdir().expect("workspace");
        let current = root.path().join("src/lib.rs");
        let worktree = root.path().join(".worktrees/archive");
        let archived = worktree.join("src/lib.rs");
        for source in [&current, &archived] {
            tokio::fs::create_dir_all(source.parent().expect("source parent"))
                .await
                .expect("source directory");
            tokio::fs::write(source, "fn example() {}\n")
                .await
                .expect("source");
        }
        let scan = discover_workspace_documents(root.path(), &["rs"]).await;
        assert_eq!(scan.paths, vec![current]);
        assert!(!scan.truncated);
        let explicit = discover_workspace_documents(&worktree, &["rs"]).await;
        assert_eq!(explicit.paths, vec![archived]);
        assert!(!explicit.truncated);
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
            restricted.active_clients(true).await.len(),
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
                last_activity: Instant::now(),
                incoming: Vec::new(),
                outgoing: Vec::new(),
                outgoing_written: 0,
                pending_request: None,
                supports_pull_diagnostics: false,
                compilation_configuration: Vec::new(),
                opened_versions: HashMap::new(),
                open_order: VecDeque::new(),
                last_versions: HashMap::new(),
                published_diagnostics: HashMap::new(),
                _private_preamble_storage: None,
            }))));
        service.leased.lock().await.insert(key.clone());
        service.clients.lock().await.insert(
            key,
            LspClientSlot {
                client: std::sync::Arc::clone(&shared),
                last_used: Instant::now(),
                owners: Vec::new(),
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
        // Disabling checkOnSave alone still permits rust-analyzer to compile
        // build.rs helpers/proc macros during indexing (violates source-only work).
        assert_eq!(
            options.pointer("/cargo/buildScripts/enable"),
            Some(&json!(false))
        );
        assert_eq!(options.pointer("/procMacro/enable"), Some(&json!(false)));
        assert_eq!(options.get("checkOnSave"), Some(&json!(false)));
        assert_eq!(options.pointer("/check/allTargets"), Some(&json!(false)));
    }

    #[test]
    fn clangd_keeps_preambles_out_of_shared_temporary_storage() {
        let spec = spec_for_id("clangd").expect("clangd spec");

        assert!(spec.args.contains(&"--pch-storage=disk"));
        let mut command = Command::new(spec.command);
        let storage = configure_private_preamble_storage(spec, &mut command)
            .expect("private storage")
            .expect("clangd storage");
        let path = storage.path().to_path_buf();
        assert_eq!(
            path.parent(),
            Some(
                crate::host_paths::host_home()
                    .join("lsp-preambles")
                    .as_path()
            )
        );
        for name in ["TMPDIR", "TMP", "TEMP"] {
            assert!(
                command
                    .as_std()
                    .get_envs()
                    .any(|(key, value)| { key == name && value == Some(path.as_os_str()) })
            );
        }
        drop(storage);
        assert!(!path.exists());
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

    // rootUri alone leaves modern Pyright in a nonexistent default workspace:
    // sibling imports become Unknown and real cross-module type errors disappear.
    #[tokio::test]
    async fn pyright_resolves_project_imports_and_reports_cross_module_errors_when_available() {
        if !command_available("pyright-langserver") {
            return;
        }
        let root = tempfile::tempdir().expect("Python workspace");
        for directory in ["pkg", "src"] {
            tokio::fs::create_dir(root.path().join(directory))
                .await
                .expect("source directory");
        }
        for (path, source) in [
            (
                "pyproject.toml",
                "[tool.pyright]\ntypeCheckingMode = \"basic\"\n",
            ),
            ("pkg/__init__.py", ""),
            ("pkg/library.py", "def number() -> int:\n    return 1\n"),
            (
                "src/main.py",
                "from pkg.library import number\nvalue: str = number()\n",
            ),
        ] {
            tokio::fs::write(root.path().join(path), source)
                .await
                .expect("Python project source");
        }
        let result = LspService::new(root.path())
            .diagnostics(Path::new("src/main.py"))
            .await
            .expect("Pyright diagnostics");
        let items = result["items"].as_array().expect("full diagnostics");
        assert!(
            items
                .iter()
                .any(|item| item["code"] == "reportAssignmentType"),
            "resolved imports must expose the real type mismatch: {result}"
        );
        assert!(
            !items
                .iter()
                .any(|item| item["code"] == "reportMissingImports"),
            "project sibling imports must resolve: {result}"
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

        // Repeating an unchanged-buffer request must still publish the current
        // generation; clangd otherwise suppresses it and the call times out.
        let repeated = service
            .diagnostics(Path::new("broken.c"))
            .await
            .expect("repeated unchanged clangd diagnostics");
        assert_eq!(repeated["items"], result["items"]);

        tokio::fs::write(root.path().join("compile_flags.txt"), "-Dmissing=0\n")
            .await
            .expect("replace fallback compilation flags");
        let configured = service
            .diagnostics(Path::new("broken.c"))
            .await
            .expect("diagnostics after configuration change");
        assert_eq!(configured["items"], json!([]));

        service
            .workspace_diagnostics(Some(Path::new("broken.c")))
            .await
            .expect("clangd workspace diagnostics");
        let shared = service
            .clients
            .lock()
            .await
            .iter()
            .find(|(key, _)| key.server_id == "clangd" && key.workspace_root == root.path())
            .map(|(_, slot)| slot)
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

    #[test]
    fn workspace_server_failure_is_explicit_partial_report() {
        let failed = workspace_diagnostic_result(Err(anyhow::anyhow!("workspace request timeout")));
        assert_eq!(failed["partial"], true);
        assert!(
            failed["partialReason"]
                .as_str()
                .unwrap()
                .contains("timeout")
        );
        let healthy = json!({ "items": [{ "uri": "file:///healthy.c", "items": [] }] });
        assert_eq!(workspace_diagnostic_result(Ok(healthy.clone())), healthy);
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
                owners: Vec::new(),
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
    async fn clangd_configuration_stamp_changes_when_fallback_flags_are_replaced() {
        let root = tempfile::tempdir().expect("workspace");
        let clangd = spec_for_id("clangd").expect("clangd");
        let before = compilation_configuration_stamp(clangd, root.path()).await;
        tokio::fs::write(root.path().join("compile_flags.txt"), "-std=c++20\n")
            .await
            .expect("write flags");
        let after = compilation_configuration_stamp(clangd, root.path()).await;
        assert_ne!(before, after);
        tokio::fs::write(root.path().join("main.cpp"), "int value;\n")
            .await
            .expect("write source");
        assert_eq!(
            after,
            compilation_configuration_stamp(clangd, root.path()).await
        );
    }

    // Prevent false fallback warnings and stale diagnostics when clangd reads
    // flags in the standard build directory (also selected by .clangd).
    #[tokio::test]
    async fn clangd_build_directory_flags_are_reported_and_invalidate_cache() {
        let root = tempfile::tempdir().expect("workspace");
        let clangd = spec_for_id("clangd").expect("clangd");
        let before = compilation_configuration_stamp(clangd, root.path()).await;
        tokio::fs::create_dir(root.path().join("build"))
            .await
            .expect("build directory");
        let flags = root.path().join("build/compile_flags.txt");
        tokio::fs::write(&flags, "-std=c++20\n")
            .await
            .expect("flags");
        let status = compilation_context_status(root.path(), clangd, None)
            .await
            .expect("context");
        assert_eq!(status["status"], json!("present"));
        assert_eq!(status["path"], json!(flags));
        assert_ne!(
            before,
            compilation_configuration_stamp(clangd, root.path()).await
        );
    }

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
        // A matching database entry remains valid evidence of coverage, not
        // proof of the effective command after clangd configuration applies.
        assert_eq!(
            covered["provenance"],
            json!("filesystem_database_discovery")
        );
        assert_eq!(covered["effectiveFlagsVerified"], json!(false));
        tokio::fs::write(
            root.path().join(".clangd"),
            "If:\n  PathMatch: .*\nCompileFlags:\n  Add: [-DCONFIGURED]\n",
        )
        .await
        .expect("write conditional flag configuration");
        let configured = compilation_context_status(root.path(), clangd, Some(&source))
            .await
            .expect("database coverage with unobserved effective flags");
        assert_eq!(configured["status"], json!("present"));
        assert_eq!(configured["coversFile"], json!(true));
        assert_eq!(configured["effectiveFlagsVerified"], json!(false));
        assert!(configured["effectiveFlagsCaveat"].is_string());

        tokio::fs::write(
            root.path().join(".clangd"),
            "CompileFlags:\n  CompilationDatabase: build\n---\nIf:\n  PathMatch: Source/Engine/.*\nCompileFlags:\n  CompilationDatabase: Intermediate/ClangDatabase\n",
        )
        .await
        .expect("write per-path clangd configuration");
        let overridden = compilation_context_status(root.path(), clangd, Some(&source))
            .await
            .expect("clangd reports uncertain configuration selection");
        assert_eq!(overridden["status"], json!("unverified"));
        assert_eq!(overridden["coversFile"], json!("unknown"));
        assert_eq!(
            overridden["diagnosticTrust"],
            json!("clangd_database_override_not_code_error_proof")
        );

        let rust = spec_for_id("rust-analyzer").expect("rust-analyzer spec");
        assert!(
            compilation_context_status(root.path(), rust, Some(&source))
                .await
                .is_none(),
            "servers that do not read a compilation database stay unannotated"
        );
    }

    // Native compilation cannot guard this metadata contract: clangd may
    // inherit a parent database override while the nested target builds with
    // its own flags. Never certify those diagnostics as covered by that target.
    #[tokio::test]
    async fn nested_clangd_context_inherits_parent_database_override() {
        let parent = tempfile::tempdir().expect("parent project");
        let nested = parent.path().join("nested");
        tokio::fs::create_dir(&nested).await.unwrap();
        let source = nested.join("unit.cpp");
        tokio::fs::write(&source, "int value;\n").await.unwrap();
        tokio::fs::write(nested.join("compile_flags.txt"), "-std=c++20\n")
            .await
            .unwrap();
        let clangd = spec_for_id("clangd").unwrap();
        let before = compilation_configuration_stamp(clangd, &nested).await;
        let inherited = parent.path().join(".clangd");
        tokio::fs::write(&inherited, "CompileFlags:\n  CompilationDatabase: build\n")
            .await
            .unwrap();

        let report = compilation_context_status(&nested, clangd, Some(&source))
            .await
            .unwrap();
        assert_eq!(report["status"], json!("unverified"));
        assert_eq!(report["coversFile"], json!("unknown"));
        assert_eq!(report["clangdConfiguration"], json!(inherited));
        assert_ne!(
            before,
            compilation_configuration_stamp(clangd, &nested).await
        );
    }

    /// A late didClose clear used to refill the diagnostic cache and then
    /// masquerade as a fresh, empty workspace report. Source/type checking does
    /// not exercise this protocol ordering; test real open/change/close state
    /// with injected publications instead of depending on clangd timing.
    #[cfg(unix)]
    #[tokio::test]
    async fn published_diagnostics_must_match_the_live_document_generation() {
        let root = tempfile::tempdir().expect("workspace");
        let path = root.path().join("unit.cpp");
        tokio::fs::write(&path, "int value;\n")
            .await
            .expect("source");
        let uri = Url::from_file_path(&path).expect("uri").to_string();
        let mut child = Command::new("sh")
            .args(["-c", "cat >/dev/null"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("owned silent server");
        let mut client = LspClient {
            stdin: child.stdin.take().expect("stdin"),
            stdout: BufReader::new(child.stdout.take().expect("stdout")),
            child,
            next_id: 1,
            last_activity: Instant::now(),
            incoming: Vec::new(),
            outgoing: Vec::new(),
            outgoing_written: 0,
            pending_request: None,
            supports_pull_diagnostics: false,
            compilation_configuration: Vec::new(),
            opened_versions: HashMap::new(),
            open_order: VecDeque::new(),
            last_versions: HashMap::new(),
            published_diagnostics: HashMap::new(),
            _private_preamble_storage: None,
        };
        let publication = |version: Value, diagnostics: Value| {
            json!({
                "method": "textDocument/publishDiagnostics",
                "params": {"uri": uri, "version": version, "diagnostics": diagnostics}
            })
        };
        let error = json!([{"severity": 1, "message": "fixture source error"}]);
        client
            .open_document(&path, &uri, "cpp")
            .await
            .expect("open");
        client.capture_diagnostics(&publication(Value::Null, error.clone()));
        assert_eq!(client.published_diagnostics.get(&uri), Some(&error));
        client
            .open_document(&path, &uri, "cpp")
            .await
            .expect("change");
        assert!(!client.published_diagnostics.contains_key(&uri));
        client.capture_diagnostics(&publication(json!(1), json!([])));
        assert!(!client.published_diagnostics.contains_key(&uri));
        client.capture_diagnostics(&publication(json!(2), error.clone()));
        assert_eq!(client.published_diagnostics.get(&uri), Some(&error));
        client.close_document(&path, &uri).await.expect("close");
        client.capture_diagnostics(&publication(Value::Null, json!([])));
        client.capture_diagnostics(&publication(json!(2), error.clone()));
        assert!(!client.published_diagnostics.contains_key(&uri));
        client
            .open_document(&path, &uri, "cpp")
            .await
            .expect("reopen");
        assert_eq!(client.opened_versions.get(&path), Some(&3));
        client.capture_diagnostics(&publication(Value::Null, json!([])));
        client.capture_diagnostics(&publication(json!(2), json!([])));
        assert!(!client.published_diagnostics.contains_key(&uri));
        client.capture_diagnostics(&publication(json!(3), error.clone()));
        assert_eq!(client.published_diagnostics.get(&uri), Some(&error));
        let report = client
            .wait_for_published_diagnostics(&uri, Duration::ZERO)
            .await
            .expect("current publication");
        assert_eq!(report["items"], error);
        client.child.kill().await.expect("stop owned server");
    }

    /// A scan with a nonzero budget must bound an in-flight diagnostic too,
    /// not just notice expiration before the NEXT file. A compiler check and
    /// the already-expired-budget test cannot catch a 120-second fallback wait.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_silent_document_server_cannot_overrun_the_workspace_budget() {
        let root = tempfile::tempdir().expect("workspace");
        tokio::fs::write(root.path().join("unit.cpp"), "int value;\n")
            .await
            .expect("source");
        // The shell holds stdout open while cat consumes requests without
        // answering. No installed language server or timing its parse is needed.
        let mut child = Command::new("sh")
            .args(["-c", "cat >/dev/null"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("silent server");
        let mut client = LspClient {
            stdin: child.stdin.take().expect("stdin"),
            stdout: BufReader::new(child.stdout.take().expect("stdout")),
            child,
            next_id: 1,
            last_activity: Instant::now(),
            incoming: Vec::new(),
            outgoing: Vec::new(),
            outgoing_written: 0,
            pending_request: None,
            supports_pull_diagnostics: false,
            compilation_configuration: Vec::new(),
            opened_versions: HashMap::new(),
            open_order: VecDeque::new(),
            last_versions: HashMap::new(),
            published_diagnostics: HashMap::new(),
            _private_preamble_storage: None,
        };
        let clangd = spec_for_id("clangd").expect("spec");
        let result = timeout(
            Duration::from_secs(2),
            client.document_workspace_diagnostics(
                root.path(),
                clangd,
                Instant::now() + Duration::from_millis(100),
            ),
        )
        .await;
        // Closing a sweep's document must not turn the next explicit check
        // back into a cold 120-second publication wait.
        assert!(
            client
                .last_versions
                .contains_key(&root.path().join("unit.cpp")),
            "the sweep checked this document"
        );
        tokio::time::pause();
        let warm_started = Instant::now();
        let path = root.path().join("unit.cpp");
        let uri = Url::from_file_path(&path).unwrap().to_string();
        assert!(
            client
                .document_diagnostics(&path, &uri, "cpp", None)
                .await
                .is_err()
        );
        assert!(
            warm_started.elapsed() < PUBLISHED_DIAGNOSTICS_TIMEOUT + Duration::from_secs(1),
            "a previously checked, closed document is still warm: {:?}",
            warm_started.elapsed()
        );
        client
            .close_document(&path, &uri)
            .await
            .expect("close warm check");
        tokio::time::resume();
        client.child.kill().await.expect("stop owned mock server");
        let report = result
            .expect("the scan must not wait for the ordinary request/publish timeout")
            .expect("budget exhaustion is a partial report");
        assert_eq!(report["partial"], json!(true));
        assert_eq!(report["documentsDiscovered"], json!(1));
        let scanned = report["documentsScanned"].as_u64().expect("scanned count");
        assert!(scanned <= 1);
        if scanned == 1 {
            assert_eq!(report["failedDocuments"], json!(1));
            assert!(report["items"][0]["error"].is_string());
        }
        assert!(
            client.opened_versions.is_empty(),
            "close the timed-out document"
        );
    }

    /// A dead server's didClose error must not replace the original document
    /// failure or turn a workspace sweep into an empty, unauditable report.
    #[cfg(unix)]
    #[tokio::test]
    async fn dead_workspace_server_preserves_document_and_cleanup_failures() {
        let root = tempfile::tempdir().expect("workspace");
        for name in ["first.cpp", "second.cpp"] {
            tokio::fs::write(root.path().join(name), "int value;\n")
                .await
                .expect("source");
        }
        let mut child = Command::new("sh")
            .args(["-c", "exit 0"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("owned server");
        let stdin = child.stdin.take().expect("stdin");
        let stdout = BufReader::new(child.stdout.take().expect("stdout"));
        child.wait().await.expect("closed server pipes");
        let mut client = LspClient {
            child,
            stdin,
            stdout,
            next_id: 1,
            last_activity: Instant::now(),
            incoming: Vec::new(),
            outgoing: Vec::new(),
            outgoing_written: 0,
            pending_request: None,
            supports_pull_diagnostics: false,
            compilation_configuration: Vec::new(),
            opened_versions: HashMap::new(),
            open_order: VecDeque::new(),
            last_versions: HashMap::new(),
            published_diagnostics: HashMap::new(),
            _private_preamble_storage: None,
        };
        let report = client
            .document_workspace_diagnostics(
                root.path(),
                spec_for_id("clangd").expect("spec"),
                Instant::now() + Duration::from_secs(2),
            )
            .await
            .expect("server loss returns a partial report");
        assert_eq!(report["partial"], json!(true));
        assert_eq!(report["documentsScanned"], json!(1));
        assert_eq!(report["documentsDiscovered"], json!(2));
        assert_eq!(report["failedDocuments"], json!(1));
        assert!(
            report["items"][0]["uri"]
                .as_str()
                .is_some_and(|uri| uri.ends_with("first.cpp"))
        );
        assert!(report["items"][0]["error"].is_string());
        assert!(
            report["items"][0]["closeError"]
                .as_str()
                .is_some_and(|error| error.contains("failed to close"))
        );
        assert!(client.opened_versions.is_empty());
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
            .iter()
            .find(|(key, _)| key.server_id == "clangd" && key.workspace_root == root.path())
            .map(|(_, slot)| slot)
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
        assert_eq!(report["items"], json!([]));
        // Discovery shares the expired deadline; it finishes inside the first
        // timer tick only on an unloaded host, otherwise the pass is skipped.
        if report.get("status") != Some(&json!("skipped")) {
            assert_eq!(report["documentsScanned"], json!(0));
            assert_eq!(report["documentsDiscovered"], json!(4));
        }
        assert!(
            report["partialReason"]
                .as_str()
                .is_some_and(|reason| reason.contains("time budget")),
            "the caller must learn the result was cut short by time"
        );
    }

    // A timeout can land in either a header or body. Losing those bytes used
    // to corrupt the next request, and dropping a request did not cancel the
    // server's obsolete work. A compiler check cannot exercise this wire order.
    #[cfg(unix)]
    #[tokio::test]
    async fn cancelled_requests_preserve_frames_and_cancel_before_reuse() {
        for split_header in [true, false] {
            for mode in ["timeout", "drop", "broker_cancel"] {
                let script = r#"
import sys,json
r,w=sys.stdin.buffer,sys.stdout.buffer
def read():
    n=0
    while True:
        h=r.readline()
        if not h: raise EOFError()
        if h==b'\r\n': break
        if h.startswith(b'Content-Length:'): n=int(h.split(b':')[1])
    return json.loads(r.read(n))
def frame(v):
    b=json.dumps(v).encode()
    return b'Content-Length: '+str(len(b)).encode()+b'\r\n\r\n'+b
first=read()
body=json.dumps({'jsonrpc':'2.0','id':first['id'],'result':'obsolete'}).encode()
b=b'Content-Length: '+str(len(body)).encode()+b'\n\n'+body
cut=9 if sys.argv[1]=='header' else len(b)-5
w.write(b[:cut]);w.flush()
cancel=read()
assert cancel['method']=='$/cancelRequest' and cancel['params']['id']==first['id'],cancel
# Coalesce an LF-framed response with a CRLF-framed successor. Choosing a
# later CRLF boundary instead of the earlier LF boundary corrupts both frames.
w.write(b[cut:]+frame({'jsonrpc':'2.0','method':'fixture/late'}));w.flush()
second=read()
w.write(frame({'jsonrpc':'2.0','id':second['id'],'result':'fresh'}));w.flush()
"#;
                let mut child = Command::new("python3")
                    .args([
                        "-u",
                        "-c",
                        script,
                        if split_header { "header" } else { "body" },
                    ])
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .kill_on_drop(true)
                    .spawn()
                    .expect("owned stdio server");
                let mut client = LspClient {
                    stdin: child.stdin.take().unwrap(),
                    stdout: BufReader::new(child.stdout.take().unwrap()),
                    child,
                    next_id: 1,
                    last_activity: Instant::now(),
                    incoming: Vec::new(),
                    outgoing: Vec::new(),
                    outgoing_written: 0,
                    pending_request: None,
                    supports_pull_diagnostics: false,
                    compilation_configuration: Vec::new(),
                    opened_versions: HashMap::new(),
                    open_order: VecDeque::new(),
                    last_versions: HashMap::new(),
                    published_diagnostics: HashMap::new(),
                    _private_preamble_storage: None,
                };
                if mode == "broker_cancel" {
                    let (cancel, cancelled) = watch::channel(false);
                    let trigger = async move {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        cancel.send(true).unwrap();
                    };
                    let (result, ()) = tokio::join!(
                        REQUEST_CANCELLED
                            .scope(cancelled, client.request("fixture/first", json!({}))),
                        trigger,
                    );
                    assert!(result.is_err());
                    assert!(
                        client.pending_request.is_none(),
                        "send cancellation without waiting for reuse"
                    );
                } else if mode == "drop" {
                    assert!(
                        timeout(
                            Duration::from_millis(500),
                            client.request("fixture/first", json!({}))
                        )
                        .await
                        .is_err()
                    );
                } else {
                    assert!(
                        client
                            .request_with_timeout(
                                "fixture/first",
                                json!({}),
                                Duration::from_millis(500)
                            )
                            .await
                            .is_err()
                    );
                }
                assert!(
                    !client.incoming.is_empty(),
                    "fixture must interrupt a consumed frame"
                );
                let result = client.request("fixture/second", json!({})).await;
                let _ = client.child.kill().await;
                assert_eq!(
                    result.unwrap(),
                    json!("fresh"),
                    "header={split_header}, mode={mode}"
                );
            }
        }
    }

    // Waiting behind another session's scan must not hang unrelated callers
    // forever, or turn an unexamined workspace into an empty clean report.
    #[tokio::test(start_paused = true)]
    async fn busy_workspace_slots_have_bounded_explicit_results() {
        let root = tempfile::tempdir().unwrap();
        let service = LspService::new(root.path());
        let shared = service
            .lease_client(spec_for_id("rust-analyzer").unwrap(), root.path())
            .await;
        let _busy = shared.lock().await;
        let started = Instant::now();
        let (symbols, diagnostics) = timeout(REQUEST_TIMEOUT + Duration::from_secs(1), async {
            tokio::join!(
                service.workspace_symbols_local("value"),
                service.workspace_diagnostics_local(None)
            )
        })
        .await
        .expect("slot waits must be bounded, not wait for a broker deadline");
        assert!(symbols.is_err());
        let report = diagnostics.unwrap();
        assert_eq!(report["rust-analyzer"]["partial"], json!(true));
        assert!(
            report["rust-analyzer"]["partialReason"]
                .as_str()
                .is_some_and(|reason| reason.contains("slot"))
        );
        assert!(started.elapsed() <= REQUEST_TIMEOUT + Duration::from_secs(1));
    }
    // Initialization is allowed to be slow, but not to start a second clock
    // after the caller has spent its workspace budget. No real analyzer runs.
    #[cfg(unix)]
    #[tokio::test]
    async fn workspace_bootstrap_respects_the_callers_remaining_budget() {
        static SILENT: ServerSpec = ServerSpec {
            id: "fixture",
            command: "sh",
            args: &["-c", "cat >/dev/null"],
            language_id: "fixture",
            extensions: &[],
        };
        let root = tempfile::tempdir().unwrap();
        let mut state = LspClientState::NotStarted;
        let result = timeout(
            Duration::from_secs(2),
            ready_client(
                &mut state,
                &SILENT,
                root.path(),
                root.path(),
                Some(Instant::now() + Duration::from_millis(100)),
            ),
        )
        .await;
        assert!(
            result.is_ok(),
            "do not wait for the ordinary 120-second initialize timeout"
        );
        assert!(result.unwrap().is_err());
        assert!(matches!(state, LspClientState::Failed(_)));
    }
    // The OS pipe can stop a request/cancel partway through writing. Retrying
    // cleanup must neither lose bytes nor enqueue another cancel. The control
    // socket makes backpressure deterministic without a real language server.
    #[cfg(unix)]
    #[tokio::test]
    async fn interrupted_writes_resume_without_duplicate_cancellation() {
        let root = tempfile::tempdir().unwrap();
        let control_path = root.path().join("control.sock");
        let listener = tokio::net::UnixListener::bind(&control_path).unwrap();
        let script = r#"
import sys,json,socket
c=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
c.connect(sys.argv[1]);c.sendall(b'R');assert c.recv(1)==b'R'
r,w=sys.stdin.buffer,sys.stdout.buffer
def read():
    n=0
    while True:
        h=r.readline()
        if not h: raise EOFError()
        if h==b'\r\n': break
        if h.startswith(b'Content-Length:'): n=int(h.split(b':')[1])
    return json.loads(r.read(n))
first=read()
assert len(first['params']['text'])==2*1024*1024
cancel=read()
assert cancel['method']=='$/cancelRequest' and cancel['params']['id']==first['id'],cancel
second=read()
assert second['method']=='fixture/second',second
body=json.dumps({'jsonrpc':'2.0','id':second['id'],'result':'fresh'}).encode()
w.write(b'Content-Length: '+str(len(body)).encode()+b'\r\n\r\n'+body);w.flush()
# A workspace pass must cancel its expired pull WITHOUT another reuse call.
while True:
    third=read()
    if 'id' in third: break
assert third['method']=='textDocument/diagnostic',third
cancel=read()
assert cancel['method']=='$/cancelRequest' and cancel['params']['id']==third['id'],cancel
c.sendall(b'C')
read() # didClose cleanup
"#;
        let mut child = Command::new("python3")
            .args(["-u", "-c", script])
            .arg(&control_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("owned blocked server");
        let mut client = LspClient {
            stdin: child.stdin.take().unwrap(),
            stdout: BufReader::new(child.stdout.take().unwrap()),
            child,
            next_id: 1,
            last_activity: Instant::now(),
            incoming: Vec::new(),
            outgoing: Vec::new(),
            outgoing_written: 0,
            pending_request: None,
            supports_pull_diagnostics: false,
            compilation_configuration: Vec::new(),
            opened_versions: HashMap::new(),
            open_order: VecDeque::new(),
            last_versions: HashMap::new(),
            published_diagnostics: HashMap::new(),
            _private_preamble_storage: None,
        };
        let (mut control, _) = timeout(Duration::from_secs(2), listener.accept())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(control.read_u8().await.unwrap(), b'R');
        let result = client
            .request_with_timeout(
                "fixture/first",
                json!({ "text": "x".repeat(2*1024*1024) }),
                Duration::from_millis(100),
            )
            .await;
        assert!(result.is_err());
        assert!(
            timeout(Duration::from_millis(100), client.cancel_pending_request())
                .await
                .is_err()
        );
        control.write_all(b"R").await.unwrap();
        let result = client.request("fixture/second", json!({})).await;
        assert_eq!(result.unwrap(), json!("fresh"));
        std::fs::write(root.path().join("unit.cpp"), "int value;\n").unwrap();
        client.supports_pull_diagnostics = true;
        let report = client
            .document_workspace_diagnostics(
                root.path(),
                spec_for_id("clangd").unwrap(),
                Instant::now() + Duration::from_millis(100),
            )
            .await
            .unwrap();
        assert_eq!(report["partial"], json!(true));
        assert_eq!(
            timeout(Duration::from_secs(2), control.read_u8())
                .await
                .unwrap()
                .unwrap(),
            b'C',
            "the workspace deadline must cancel immediately, not wait for reuse"
        );
        let _ = client.child.kill().await;
    }
}
