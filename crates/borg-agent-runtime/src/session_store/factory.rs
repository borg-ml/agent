//! Chooses this process's durable backend, and refuses to start on a partial one.
//!
//! WHY THIS EXISTS: Borg now has two session-store implementations, and every
//! process must pick exactly one. That choice cannot be made lazily per call
//! site, because the two backends are not interchangeable at runtime -- they
//! are different databases holding different data. One decision, made once at
//! startup, from one environment variable.
//!
//! THE FAIL-FAST RULE IS THE POINT. A `SessionStore` exposes its satellite
//! tiers as `Result<Option<_>>`, and `None` means "this backend does not offer
//! that tier". The runtime treats a missing tier as a transient fault and
//! retries: `run_host_workspace_recovery_loop` in borg-remote loops on
//! `Ok(None)` every two seconds, forever, logging a warning each time. A
//! backend that is wired up for the journal but not for workspaces therefore
//! does not fail -- it hangs, quietly, with hosted message recovery dead and
//! nothing but a repeating warning to show for it.
//!
//! So this module resolves the tiers eagerly and returns an error naming the
//! backend and the missing tier. A process that cannot serve every tier must
//! die at startup, where the operator is watching, rather than degrade into a
//! silent retry loop hours later. `open` is the only supported way to obtain a
//! store from configuration; constructing a backend directly is for tests and
//! for tools that deliberately want one specific backend.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};

use super::postgres::{PostgresSessionStore, SESSIONS_URL_ENV};
use super::{SessionStore, SqliteSessionStore};

/// Which durable backend a process resolved to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionBackend {
    /// The historical single-file journal. One writer per file, machine-wide.
    Sqlite,
    /// The contention-free journal: writers serialise per session row.
    Postgres,
}

impl SessionBackend {
    /// The name used in diagnostics and startup errors.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::Postgres => "postgres",
        }
    }
}

impl std::fmt::Display for SessionBackend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Which journal a process was told to use.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Selection {
    /// An explicit `BORG_SESSIONS_URL`, or a URL pinned in code.
    Url(String),
    /// An explicit opt-out to the single-writer journal.
    Sqlite,
    /// The default: Borg's own PostgreSQL cluster on this machine.
    Managed,
}

/// Where a process should look for its journal.
///
/// Held as a value rather than read from the environment at each call site so
/// that tests can drive the factory without mutating process-global state --
/// `std::env::set_var` is not sound to call while other threads run, and the
/// test suite is threaded.
#[derive(Debug, Clone)]
pub struct SessionStoreConfig {
    /// What the environment or the caller asked for.
    selection: Selection,
    /// Where the SQLite journal lives when SQLite was selected.
    sqlite_path: PathBuf,
    /// The Borg home directory that hosts the managed cluster.
    home: PathBuf,
    /// Interactive processes wait longer for the SQLite write lock instead of
    /// failing a user-visible command. Ignored by Postgres, which has no
    /// machine-wide write lock to wait on.
    interactive: bool,
}

/// Opt out of the managed cluster and use the single-writer journal.
pub const SESSIONS_BACKEND_ENV: &str = "BORG_SESSIONS_BACKEND";

/// The Borg home that hosts the managed cluster.
///
/// Machine-scoped rather than derived from the journal path, because there is
/// one managed cluster per Borg installation and several call sites pass a
/// scratch or non-canonical journal path. Deriving from those produced a
/// cluster directory at an arbitrary root; anchoring to `BORG_HOME` means every
/// process on a machine agrees on one cluster, and a test or a second
/// installation scopes itself by setting `BORG_HOME` as it already does for
/// every other piece of durable state.
fn default_home() -> PathBuf {
    crate::default_host_config_path()
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from(".borg"))
}

impl SessionStoreConfig {
    /// Resolve from the environment.
    ///
    /// The default is the managed PostgreSQL cluster. Borg's normal posture is
    /// several agents running at once, and SQLite admits one writer per file
    /// machine-wide, so a SQLite default silently serialises every agent behind
    /// one lock. SQLite remains available, but only when asked for.
    pub fn from_env(sqlite_path: impl Into<PathBuf>) -> Self {
        let sqlite_path = sqlite_path.into();
        let selection = select(
            PostgresSessionStore::url_from_env(),
            std::env::var(SESSIONS_BACKEND_ENV).ok().as_deref(),
            journal_is_the_default_one(&sqlite_path),
        );
        Self {
            selection,
            home: default_home(),
            sqlite_path,
            interactive: false,
        }
    }

    /// Pin a specific Postgres URL, ignoring the environment.
    pub fn with_url(url: impl Into<String>, sqlite_path: impl Into<PathBuf>) -> Self {
        let sqlite_path = sqlite_path.into();
        Self {
            selection: Selection::Url(url.into()),
            home: default_home(),
            sqlite_path,
            interactive: false,
        }
    }

    /// Pin SQLite, ignoring the environment.
    pub fn sqlite(sqlite_path: impl Into<PathBuf>) -> Self {
        let sqlite_path = sqlite_path.into();
        Self {
            selection: Selection::Sqlite,
            home: default_home(),
            sqlite_path,
            interactive: false,
        }
    }

    /// Pin the managed cluster under a specific home, ignoring the environment.
    pub fn managed(home: impl Into<PathBuf>, sqlite_path: impl Into<PathBuf>) -> Self {
        Self {
            selection: Selection::Managed,
            home: home.into(),
            sqlite_path: sqlite_path.into(),
            interactive: false,
        }
    }

    /// Wait longer for the SQLite write lock; see the field comment.
    pub fn interactive(mut self, interactive: bool) -> Self {
        self.interactive = interactive;
        self
    }

    /// Where this configuration points, for diagnostics and for telling two
    /// configurations apart.
    ///
    /// A connection string can carry a password, so this is the URL as
    /// configured and is fit for an error message the operator sees, not for a
    /// log that leaves the machine.
    pub fn describe(&self) -> String {
        match &self.selection {
            Selection::Url(url) => url.clone(),
            Selection::Sqlite => self.sqlite_path.display().to_string(),
            Selection::Managed => self.cluster().data_dir().display().to_string(),
        }
    }

    /// Which backend this configuration selects, without connecting.
    ///
    /// There is no auto-detection and no fallback in either direction: an
    /// unreachable Postgres does not quietly become SQLite, because that would
    /// split one machine's history across two databases, and the split stays
    /// invisible until someone goes looking for a session that was written
    /// somewhere else.
    pub fn backend(&self) -> SessionBackend {
        match self.selection {
            Selection::Url(_) | Selection::Managed => SessionBackend::Postgres,
            Selection::Sqlite => SessionBackend::Sqlite,
        }
    }

    /// The managed cluster this configuration would use.
    pub fn cluster(&self) -> super::cluster::ManagedCluster {
        super::cluster::ManagedCluster::in_home(&self.home)
    }
}

/// Decide the journal from configuration values.
///
/// Pure so the precedence rule can be tested directly: `std::env::set_var` is
/// not sound to call while other threads run, and this suite is threaded.
fn select(url: Option<String>, backend: Option<&str>, default_journal: bool) -> Selection {
    // An explicit URL wins over everything, including an explicit backend
    // name, because it is the more specific instruction.
    if let Some(url) = url {
        return Selection::Url(url);
    }
    if backend
        .map(str::trim)
        .is_some_and(|name| name.eq_ignore_ascii_case("sqlite"))
    {
        return Selection::Sqlite;
    }
    // The managed cluster serves the installation's own journal. A caller that
    // pointed somewhere else -- a scratch directory, a second journal being
    // read for migration -- has deliberately left that installation, and there
    // is no cluster there to reach for. Provisioning one beside every scratch
    // path would turn a temp directory into a database server.
    if default_journal {
        Selection::Managed
    } else {
        Selection::Sqlite
    }
}

/// Whether this journal path is the one the local Borg installation owns.
fn journal_is_the_default_one(sqlite_path: &Path) -> bool {
    sqlite_path.starts_with(default_home())
}

/// A session store whose satellite tiers are known to be present.
///
/// The tiers are resolved once here rather than re-fetched per use, so the
/// "every tier or nothing" guarantee is established at construction and cannot
/// be weakened by a later caller that forgets to check.
#[derive(Clone)]
pub struct ResolvedSessionStore {
    backend: SessionBackend,
    session: Arc<dyn SessionStore>,
    workspace: Arc<dyn crate::WorkspaceStore>,
    autonomy: Arc<dyn crate::autonomy::AutonomyStore>,
    receipts: Arc<dyn crate::receipt::ReceiptBackend>,
}

impl ResolvedSessionStore {
    /// The backend this process resolved to.
    pub fn backend(&self) -> SessionBackend {
        self.backend
    }

    /// The session journal.
    pub fn session(&self) -> &Arc<dyn SessionStore> {
        &self.session
    }

    /// The workspace tier on the same durable authority as the journal.
    pub fn workspace(&self) -> &Arc<dyn crate::WorkspaceStore> {
        &self.workspace
    }

    /// The durable runtime-job tier.
    pub fn autonomy(&self) -> &Arc<dyn crate::autonomy::AutonomyStore> {
        &self.autonomy
    }

    /// The receipt tier, which makes relayed mutations replay-safe.
    pub fn receipts(&self) -> &Arc<dyn crate::receipt::ReceiptBackend> {
        &self.receipts
    }
}

impl std::fmt::Debug for ResolvedSessionStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResolvedSessionStore")
            .field("backend", &self.backend)
            .finish_non_exhaustive()
    }
}

/// A journal that has been opened but whose tiers are not yet proven.
///
/// Separate from `ResolvedSessionStore` so the type says which guarantee you
/// hold. Resolving a tier CONSTRUCTS it, and on a new database that means
/// creating its schema -- real work that takes SQLite's single writer lock. A
/// process that is about to discover it lost an ownership race and exit must
/// not pay for tiers it will never use, or it blocks behind whatever large
/// session currently holds the writer.
///
/// So the journal opens first, the caller makes its ownership decision, and
/// only a process that commits to running calls [`OpenSessionStore::resolve`].
/// Every long-running path does, which is what keeps the all-or-nothing
/// guarantee where it matters: before any loop that would otherwise retry a
/// missing tier forever.
pub struct OpenSessionStore {
    backend: SessionBackend,
    session: Arc<dyn SessionStore>,
}

impl std::fmt::Debug for OpenSessionStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenSessionStore")
            .field("backend", &self.backend)
            .finish_non_exhaustive()
    }
}

impl OpenSessionStore {
    /// The backend this process resolved to.
    pub fn backend(&self) -> SessionBackend {
        self.backend
    }

    /// The journal, before its satellite tiers have been proven present.
    pub fn session(&self) -> &Arc<dyn SessionStore> {
        &self.session
    }

    /// Prove every satellite tier is present, or fail.
    pub async fn resolve(self) -> Result<ResolvedSessionStore> {
        resolve_tiers(self.backend, self.session).await
    }
}

/// Open the configured backend.
///
/// The tiers are NOT resolved here; call [`OpenSessionStore::resolve`] once the
/// process has committed to running. See that type for why the two steps are
/// separate.
pub async fn open(config: &SessionStoreConfig) -> Result<OpenSessionStore> {
    let backend = config.backend();
    let session: Arc<dyn SessionStore> = match &config.selection {
        Selection::Url(url) => {
            let store = PostgresSessionStore::connect(url).await.with_context(|| {
                format!("{SESSIONS_URL_ENV} is set, so Borg requires PostgreSQL")
            })?;
            // Before anything reads or writes. A journal reached by
            // connection string can be shared by accident in a way a SQLite
            // file could not, and what would be shared is a trust boundary.
            // Sharing stays possible, but only deliberately.
            store.ensure_single_owner().await?;
            Arc::new(store)
        }
        Selection::Managed => {
            // Provisioning happens here rather than in `from_env` because it
            // starts a server: a configuration value must stay cheap to build,
            // and only a process that actually opens the journal should pay
            // for one.
            let cluster = config.cluster();
            let url = cluster.ensure_running().await?;
            let store = PostgresSessionStore::connect(&url)
                .await
                .context("could not open the Borg session cluster's journal")?;
            // The managed cluster is this machine's own, so a second OS user
            // reaching it is the same accident the configured-URL path guards
            // against.
            store.ensure_single_owner().await?;
            Arc::new(store)
        }
        Selection::Sqlite => {
            let path = config.sqlite_path.clone();
            if config.interactive {
                Arc::new(SqliteSessionStore::open_interactive(path).await?)
            } else {
                Arc::new(SqliteSessionStore::open(path).await?)
            }
        }
    };
    Ok(OpenSessionStore { backend, session })
}

/// Open the configured backend and prove every tier in one step.
///
/// For callers with no ownership race to settle first -- tests, tools, and any
/// process that is going to run regardless.
pub async fn open_resolved(config: &SessionStoreConfig) -> Result<ResolvedSessionStore> {
    open(config).await?.resolve().await
}

/// Resolve the satellite tiers of an already-open store.
///
/// Split out so a caller that built a backend itself -- a test, or a tool that
/// deliberately wants one specific backend -- still gets the same all-or-
/// nothing guarantee as a configured startup.
pub async fn resolve_tiers(
    backend: SessionBackend,
    session: Arc<dyn SessionStore>,
) -> Result<ResolvedSessionStore> {
    let workspace = session
        .workspace_store()
        .await
        .with_context(|| missing_tier_context(backend, "workspace"))?
        .ok_or_else(|| anyhow::anyhow!(missing_tier_context(backend, "workspace")))?;
    let autonomy = session
        .autonomy_store()
        .await
        .with_context(|| missing_tier_context(backend, "autonomy"))?
        .ok_or_else(|| anyhow::anyhow!(missing_tier_context(backend, "autonomy")))?;
    let receipts = session
        .receipt_store()
        .await
        .with_context(|| missing_tier_context(backend, "receipt"))?;
    Ok(ResolvedSessionStore {
        backend,
        session,
        workspace,
        autonomy,
        receipts,
    })
}

/// The startup error for a backend that cannot serve `tier`.
///
/// Spelled out at length on purpose: the failure this replaces was a two-second
/// retry loop that never terminated, so the message has to make clear that
/// refusing to start is the intended behaviour and not a transient fault worth
/// restarting into.
fn missing_tier_context(backend: SessionBackend, tier: &str) -> String {
    format!(
        "the {backend} session store does not provide the {tier} tier, so this process \
         refuses to start; a partially wired backend does not fail at the point of use, \
         it retries forever (see run_host_workspace_recovery_loop). Either finish porting \
         the {tier} tier to {backend}, or unset {SESSIONS_URL_ENV} to use SQLite."
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CodingProvider;

    /// Borg's default posture is several agents at once, and SQLite admits one
    /// writer per file machine-wide. So the default must be Postgres, and
    /// SQLite must still be reachable for anyone who wants it.
    #[test]
    fn postgres_is_the_default_and_sqlite_is_opt_in() {
        assert_eq!(select(None, None, true), Selection::Managed);
        assert_eq!(select(None, Some("sqlite"), true), Selection::Sqlite);
        assert_eq!(select(None, Some("  SQLite  "), true), Selection::Sqlite);
        // An unrecognised name must not silently become SQLite.
        assert_eq!(select(None, Some("postgres"), true), Selection::Managed);
        assert_eq!(select(None, Some(""), true), Selection::Managed);
    }

    /// The managed cluster belongs to the installation's own journal. A caller
    /// pointing at a scratch path has left that installation, and must not
    /// have a database server provisioned beside it.
    #[test]
    fn a_journal_outside_the_borg_home_does_not_get_a_cluster() {
        assert_eq!(select(None, None, false), Selection::Sqlite);
        // An explicit URL still reaches whatever it names.
        assert_eq!(
            select(Some("postgres://x@y/z".to_string()), None, false),
            Selection::Url("postgres://x@y/z".to_string())
        );
    }

    /// A configured URL is the more specific instruction, so it outranks a
    /// backend name rather than being contradicted by one.
    #[test]
    fn an_explicit_url_outranks_every_other_selection() {
        let url = "postgres://borg@localhost:5433/borg_sessions";
        assert_eq!(
            select(Some(url.to_string()), None, true),
            Selection::Url(url.to_string())
        );
        assert_eq!(
            select(Some(url.to_string()), Some("sqlite"), true),
            Selection::Url(url.to_string())
        );
    }

    #[test]
    fn each_selection_reports_the_backend_it_will_open() {
        assert_eq!(
            SessionStoreConfig::sqlite("/tmp/borg/sessions.sqlite3").backend(),
            SessionBackend::Sqlite
        );
        assert_eq!(
            SessionStoreConfig::managed("/tmp/borg", "/tmp/borg/sessions.sqlite3").backend(),
            SessionBackend::Postgres
        );
        assert_eq!(
            SessionStoreConfig::with_url("postgres://x@y/z", "/tmp/borg/sessions.sqlite3")
                .backend(),
            SessionBackend::Postgres
        );
    }

    use crate::session_store::{
        RawSessionEvent, SessionLineage, SessionStoreCompaction, SessionStoreHealth,
        SessionWorkspaceBinding,
    };
    use crate::session_store::{
        RuntimeCheckpoint, RuntimeManifest, RuntimeManifestActivation, SessionHistoryIndexDocument,
        SessionHistoryPage, SessionHistoryQuery,
    };
    use crate::{
        ClaimedActionTransition, SessionAction, SessionActionState, SessionActionTransition,
        SessionEvent, SessionLiveEvent, SessionPayloadRef, SessionRecovery, SessionState,
        SessionStoreFork, SessionSummary,
    };
    use chrono::{DateTime, Utc};
    use std::time::Duration;
    use uuid::Uuid;

    /// A store whose satellite tiers can be selectively withheld -- the shape
    /// a half-ported backend has.
    ///
    /// Configurable rather than fixed so one stub covers every "tier N is
    /// missing" case. Two hand-written stubs would drift, and each would need
    /// all forty required methods restated.
    #[derive(Default)]
    struct TierlessStore {
        workspace: Option<Arc<dyn crate::WorkspaceStore>>,
    }

    impl std::fmt::Debug for TierlessStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("TierlessStore")
        }
    }

    #[async_trait::async_trait]
    impl SessionStore for TierlessStore {
        /// The tier under test: withheld unless the fixture supplies one.
        async fn workspace_store(&self) -> Result<Option<Arc<dyn crate::WorkspaceStore>>> {
            Ok(self.workspace.clone())
        }
        async fn create_session(&self, _session_id: Uuid) -> Result<()> {
            unimplemented!("journal access is not part of this test")
        }
        async fn append(&self, _event: SessionEvent) -> Result<SessionEvent> {
            unimplemented!("journal access is not part of this test")
        }
        async fn enqueue_action(&self, _action: SessionAction) -> Result<SessionAction> {
            unimplemented!("journal access is not part of this test")
        }
        async fn transition_action(
            &self,
            _session_id: Uuid,
            _action_id: Uuid,
            _expected: Option<SessionActionState>,
            _next: SessionActionState,
            _error: Option<String>,
        ) -> Result<SessionAction> {
            unimplemented!("journal access is not part of this test")
        }
        async fn claim_action(
            &self,
            _session_id: Uuid,
            _action_id: Uuid,
            _lease_owner: &str,
            _lease_duration: Duration,
        ) -> Result<Option<SessionAction>> {
            unimplemented!("journal access is not part of this test")
        }
        async fn heartbeat_action(
            &self,
            _session_id: Uuid,
            _action_id: Uuid,
            _lease_owner: &str,
            _lease_token: Uuid,
            _lease_duration: Duration,
        ) -> Result<SessionAction> {
            unimplemented!("journal access is not part of this test")
        }
        async fn transition_claimed_action(
            &self,
            _transition: ClaimedActionTransition,
        ) -> Result<SessionAction> {
            unimplemented!("journal access is not part of this test")
        }
        async fn recover_expired_actions(
            &self,
            _session_id: Uuid,
            _now: DateTime<Utc>,
            _limit: usize,
        ) -> Result<Vec<SessionAction>> {
            unimplemented!("journal access is not part of this test")
        }
        async fn action(
            &self,
            _session_id: Uuid,
            _action_id: Uuid,
        ) -> Result<Option<SessionAction>> {
            unimplemented!("journal access is not part of this test")
        }
        async fn action_transitions(
            &self,
            _session_id: Uuid,
            _action_id: Uuid,
        ) -> Result<Vec<SessionActionTransition>> {
            unimplemented!("journal access is not part of this test")
        }
        async fn pending_actions(
            &self,
            _session_id: Uuid,
            _limit: usize,
        ) -> Result<Vec<SessionAction>> {
            unimplemented!("journal access is not part of this test")
        }
        async fn read(&self, _session_id: Uuid) -> Result<Vec<SessionEvent>> {
            unimplemented!("journal access is not part of this test")
        }
        async fn events_after(
            &self,
            _session_id: Uuid,
            _sequence: u64,
            _limit: usize,
        ) -> Result<Vec<SessionEvent>> {
            unimplemented!("journal access is not part of this test")
        }
        async fn latest_completed_context_compaction(
            &self,
            _session_id: Uuid,
        ) -> Result<Option<SessionEvent>> {
            unimplemented!("journal access is not part of this test")
        }
        async fn state(&self, _session_id: Uuid) -> Result<SessionState> {
            unimplemented!("journal access is not part of this test")
        }
        async fn recovery(&self, _session_id: Uuid) -> Result<SessionRecovery> {
            unimplemented!("journal access is not part of this test")
        }
        async fn live_events_after(
            &self,
            _session_id: Uuid,
            _revision: u64,
        ) -> Result<Vec<SessionLiveEvent>> {
            unimplemented!("journal access is not part of this test")
        }
        async fn load_payload(&self, _payload: &SessionPayloadRef) -> Result<Vec<u8>> {
            unimplemented!("journal access is not part of this test")
        }
        async fn contains_message(&self, _session_id: Uuid, _message_id: Uuid) -> Result<bool> {
            unimplemented!("journal access is not part of this test")
        }
        async fn fork_before(
            &self,
            _parent_session_id: Uuid,
            _session_id: Uuid,
            _sequence: u64,
        ) -> Result<SessionStoreFork> {
            unimplemented!("journal access is not part of this test")
        }
        async fn list_sessions(&self, _limit: usize) -> Result<Vec<SessionSummary>> {
            unimplemented!("journal access is not part of this test")
        }
        async fn activate_runtime_manifest(
            &self,
            _session_id: Uuid,
            _runtime: &str,
            _root: &str,
            _command: &str,
            _worker_id: Uuid,
        ) -> Result<RuntimeManifestActivation> {
            unimplemented!("journal access is not part of this test")
        }
        async fn record_runtime_execution(
            &self,
            _session_id: Uuid,
            _worker_id: Uuid,
            _code_hash: &str,
            _worker_failed: bool,
            _error: Option<&str>,
        ) -> Result<()> {
            unimplemented!("journal access is not part of this test")
        }
        async fn stop_runtime_manifest(&self, _session_id: Uuid, _worker_id: Uuid) -> Result<()> {
            unimplemented!("journal access is not part of this test")
        }
        async fn runtime_manifest(&self, _session_id: Uuid) -> Result<Option<RuntimeManifest>> {
            unimplemented!("journal access is not part of this test")
        }
        async fn save_runtime_checkpoint(
            &self,
            _session_id: Uuid,
            _worker_id: Uuid,
            _key: &str,
            _state: &serde_json::Value,
        ) -> Result<RuntimeCheckpoint> {
            unimplemented!("journal access is not part of this test")
        }
        async fn runtime_checkpoint(
            &self,
            _session_id: Uuid,
            _key: Option<&str>,
        ) -> Result<Option<RuntimeCheckpoint>> {
            unimplemented!("journal access is not part of this test")
        }
        async fn list_runtime_checkpoints(
            &self,
            _session_id: Uuid,
            _limit: usize,
        ) -> Result<Vec<RuntimeCheckpoint>> {
            unimplemented!("journal access is not part of this test")
        }
        async fn load_harness_state(&self, _session_id: Uuid) -> Result<Option<serde_json::Value>> {
            unimplemented!("journal access is not part of this test")
        }
        async fn save_harness_state(
            &self,
            _session_id: Uuid,
            _state: &serde_json::Value,
        ) -> Result<()> {
            unimplemented!("journal access is not part of this test")
        }
        async fn rollback_harness_state(
            &self,
            _session_id: Uuid,
            _steps: usize,
        ) -> Result<serde_json::Value> {
            unimplemented!("journal access is not part of this test")
        }
        async fn history_index_documents_after(
            &self,
            _session_id: Uuid,
            _sequence: u64,
            _limit: usize,
        ) -> Result<Vec<SessionHistoryIndexDocument>> {
            unimplemented!("journal access is not part of this test")
        }
        async fn ensure_workflow_action(
            &self,
            _session_id: Uuid,
            _workflow_id: Uuid,
            _payload: &serde_json::Value,
        ) -> Result<SessionAction> {
            unimplemented!("journal access is not part of this test")
        }
        async fn ensure_workflow_started(
            &self,
            _event: SessionEvent,
            _workflow_id: Uuid,
        ) -> Result<SessionEvent> {
            unimplemented!("journal access is not part of this test")
        }
        async fn append_with_action_lease(
            &self,
            _event: SessionEvent,
            _action_id: Uuid,
            _lease_owner: &str,
            _lease_token: Uuid,
        ) -> Result<SessionEvent> {
            unimplemented!("journal access is not part of this test")
        }
        async fn contains_session(&self, _session_id: Uuid) -> Result<bool> {
            unimplemented!("journal access is not part of this test")
        }
        async fn create_session_in_workspace(
            &self,
            _session_id: Uuid,
            _workspace_id: Uuid,
        ) -> Result<SessionWorkspaceBinding> {
            unimplemented!("journal access is not part of this test")
        }
        async fn discard_empty_session(&self, _session_id: Uuid) -> Result<bool> {
            unimplemented!("journal access is not part of this test")
        }
        async fn load_host_launch_metadata(
            &self,
            _session_id: Uuid,
        ) -> Result<Option<serde_json::Value>> {
            unimplemented!("journal access is not part of this test")
        }
        async fn acknowledge_host_journal(
            &self,
            _session_id: Uuid,
            _event_cursor: u64,
            _live_revision: u64,
        ) -> Result<()> {
            unimplemented!("journal access is not part of this test")
        }
        async fn begin_host_bootstrap(&self, _session_id: Uuid) -> Result<()> {
            unimplemented!("journal access is not part of this test")
        }
        async fn claim_legacy_host_launch_owner(
            &self,
            _session_id: Uuid,
            _host_id: Uuid,
            _relay_origin: &str,
        ) -> Result<()> {
            unimplemented!("journal access is not part of this test")
        }
        async fn create_session_in_workspace_as(
            &self,
            _session_id: Uuid,
            _workspace_id: Uuid,
            _participant_id: Uuid,
        ) -> Result<SessionWorkspaceBinding> {
            unimplemented!("journal access is not part of this test")
        }
        async fn finish_host_bootstrap(&self, _session_id: Uuid) -> Result<()> {
            unimplemented!("journal access is not part of this test")
        }
        async fn host_launch_owner(&self, _session_id: Uuid) -> Result<Option<(Uuid, String)>> {
            unimplemented!("journal access is not part of this test")
        }
        async fn pending_host_journals(
            &self,
            _after: Option<Uuid>,
            _limit: usize,
        ) -> Result<Vec<Uuid>> {
            unimplemented!("journal access is not part of this test")
        }
        async fn pending_host_launch_metadata(
            &self,
            _limit: usize,
        ) -> Result<Vec<(Uuid, serde_json::Value)>> {
            unimplemented!("journal access is not part of this test")
        }
        async fn pending_host_launch_metadata_for_host(
            &self,
            _offset: usize,
            _owner: Option<(Uuid, &str)>,
            _limit: usize,
        ) -> Result<Vec<(Uuid, serde_json::Value)>> {
            unimplemented!("journal access is not part of this test")
        }
        async fn persist_host_launch_metadata(
            &self,
            _session_id: Uuid,
            _metadata: &serde_json::Value,
        ) -> Result<()> {
            unimplemented!("journal access is not part of this test")
        }
        async fn persist_owned_host_launch_metadata(
            &self,
            _session_id: Uuid,
            _metadata: &serde_json::Value,
            _host_id: Uuid,
            _relay_origin: &str,
        ) -> Result<()> {
            unimplemented!("journal access is not part of this test")
        }
        async fn settle_terminal_host_session(&self, _session_id: Uuid) -> Result<()> {
            unimplemented!("journal access is not part of this test")
        }
        async fn pending_host_workspace_messages(
            &self,
            _host_id: Uuid,
            _after: Option<Uuid>,
            _limit: usize,
        ) -> Result<Vec<Uuid>> {
            unimplemented!("journal access is not part of this test")
        }
        async fn receipt_store(
            &self,
        ) -> Result<std::sync::Arc<dyn crate::receipt::ReceiptBackend>> {
            unimplemented!("journal access is not part of this test")
        }
        async fn compact(&self, _vacuum: bool) -> Result<SessionStoreCompaction> {
            unimplemented!("journal access is not part of this test")
        }
        async fn readiness(&self) -> Result<SessionStoreHealth> {
            unimplemented!("journal access is not part of this test")
        }
        async fn health(&self) -> Result<SessionStoreHealth> {
            unimplemented!("journal access is not part of this test")
        }
        async fn import_session_events(
            &self,
            _session_id: Uuid,
            _events: Vec<SessionEvent>,
        ) -> Result<bool> {
            unimplemented!("journal access is not part of this test")
        }
        async fn session_payload_refs(
            &self,
            _session_id: Uuid,
        ) -> Result<Vec<(Uuid, SessionPayloadRef)>> {
            unimplemented!("journal access is not part of this test")
        }
        async fn import_payload(
            &self,
            _session_id: Uuid,
            _event_id: Uuid,
            _payload: &SessionPayloadRef,
            _bytes: &[u8],
        ) -> Result<()> {
            unimplemented!("journal access is not part of this test")
        }
        async fn session_lineage_page(
            &self,
            _after: Option<Uuid>,
            _limit: usize,
        ) -> Result<Vec<SessionLineage>> {
            unimplemented!("journal access is not part of this test")
        }
        async fn append_batch(&self, _events: Vec<SessionEvent>) -> Result<u64> {
            unimplemented!("journal access is not part of this test")
        }
        async fn raw_event_page(
            &self,
            _session_id: Uuid,
            _after_sequence: u64,
            _limit: usize,
        ) -> Result<Vec<RawSessionEvent>> {
            unimplemented!("journal access is not part of this test")
        }
        async fn import_raw_events(
            &self,
            _session_id: Uuid,
            _events: Vec<RawSessionEvent>,
        ) -> Result<u64> {
            unimplemented!("journal access is not part of this test")
        }
        async fn finish_imported_session(
            &self,
            _session_id: Uuid,
            _state: &SessionState,
            _inherited_event_count: u64,
        ) -> Result<()> {
            unimplemented!("journal access is not part of this test")
        }
        async fn search_all_sessions(
            &self,
            _query: SessionHistoryQuery,
        ) -> Result<SessionHistoryPage> {
            unimplemented!("journal access is not part of this test")
        }
        fn plugin_backend(&self) -> std::sync::Arc<dyn crate::plugin_store::PluginBackend> {
            unimplemented!("journal access is not part of this test")
        }
        async fn query_history(
            &self,
            _session_id: Uuid,
            _query: SessionHistoryQuery,
        ) -> Result<SessionHistoryPage> {
            unimplemented!("journal access is not part of this test")
        }
        async fn uses_native_codex_harness(&self, _session_id: Uuid) -> Result<bool> {
            unimplemented!("journal access is not part of this test")
        }
        async fn uses_native_opencode_harness(
            &self,
            _session_id: Uuid,
            _model: Option<&str>,
        ) -> Result<bool> {
            unimplemented!("journal access is not part of this test")
        }
        #[cfg(any(feature = "subscription-adapters", test))]
        async fn record_model_access(
            &self,
            _session_id: Uuid,
            _provider: CodingProvider,
            _account_identity: &str,
        ) -> Result<()> {
            unimplemented!("journal access is not part of this test")
        }
    }

    /// The central guarantee: a backend missing a tier fails here, loudly,
    /// rather than at the point of use, silently and forever.
    #[tokio::test]
    async fn a_backend_without_a_workspace_tier_is_refused_at_startup() {
        let error = resolve_tiers(SessionBackend::Postgres, Arc::new(TierlessStore::default()))
            .await
            .expect_err("a store with no workspace tier must not resolve");
        let message = format!("{error:#}");
        assert!(
            message.contains("workspace") && message.contains("postgres"),
            "the error must name the backend and the missing tier, got: {message}"
        );
        assert!(
            message.contains("retries forever"),
            "the error must explain why this is fatal rather than transient, got: {message}"
        );
    }

    /// The migration's actual claim: Postgres serves every tier too, so a
    /// process pointed at `BORG_SESSIONS_URL` starts rather than refusing.
    /// Without this, `a_backend_without_a_workspace_tier_is_refused_at_startup`
    /// would be satisfied by a factory that rejects Postgres unconditionally.
    #[tokio::test]
    async fn postgres_resolves_every_tier() {
        let Some(url) = crate::session_store::postgres::testing::test_url() else {
            eprintln!("factory: skipping postgres, BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let scratch = crate::session_store::postgres::testing::ScratchDatabase::create(&url).await;
        let config = SessionStoreConfig::with_url(&scratch.url, "/unused/sessions.sqlite3");
        assert_eq!(config.backend(), SessionBackend::Postgres);
        let resolved = open_resolved(&config)
            .await
            .expect("postgres must serve every tier");
        assert_eq!(resolved.backend(), SessionBackend::Postgres);
        resolved
            .workspace()
            .create_workspace(crate::Workspace {
                id: Uuid::new_v4(),
                name: "factory-tier-check".to_string(),
                created_at: Utc::now(),
            })
            .await
            .expect("the resolved workspace tier must be usable");
        assert!(
            resolved
                .autonomy()
                .get(Uuid::new_v4())
                .await
                .expect("the resolved autonomy tier must be usable")
                .is_none(),
            "an unknown job id has no row"
        );
    }

    /// SQLite serves every tier today, so it must resolve -- otherwise the
    /// check above would be vacuously true and would pass on any input.
    #[tokio::test]
    async fn sqlite_resolves_every_tier() {
        let directory = tempfile::tempdir().expect("tempdir");
        let config = SessionStoreConfig::sqlite(directory.path().join("sessions.sqlite3"));
        let resolved = open_resolved(&config)
            .await
            .expect("sqlite must serve every tier");
        assert_eq!(resolved.backend(), SessionBackend::Sqlite);
        // Prove the handle is usable, not merely non-null.
        resolved
            .workspace()
            .create_workspace(crate::Workspace {
                id: Uuid::new_v4(),
                name: "factory-tier-check".to_string(),
                created_at: Utc::now(),
            })
            .await
            .expect("the resolved workspace tier must be usable");
        assert!(
            resolved
                .autonomy()
                .get(Uuid::new_v4())
                .await
                .expect("the resolved autonomy tier must be usable")
                .is_none(),
            "an unknown job id has no row"
        );
    }

    /// Opening the journal must not construct the satellite tiers.
    ///
    /// This is the ordering that a competing host depends on: resolving a tier
    /// creates its schema, which takes SQLite's single writer lock, so doing it
    /// during `open` makes a process that is about to lose an ownership race
    /// block behind whatever session currently holds the writer. The tables
    /// below are the proof -- they exist only after `resolve`.
    #[tokio::test]
    async fn opening_the_journal_does_not_build_the_satellite_tiers() {
        use sqlx::Row;

        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("sessions.sqlite3");
        let config = SessionStoreConfig::sqlite(&path);

        let opened = open(&config).await.expect("open the journal");
        let probe = SqliteSessionStore::open(&path).await.expect("probe");
        let tier_tables = |pool: &sqlx::SqlitePool| {
            let pool = pool.clone();
            async move {
                sqlx::query(
                    "select name from sqlite_master where type='table' \
                     and name in ('borg_autonomy_schema', 'borg_workspace_schema')",
                )
                .fetch_all(&pool)
                .await
                .expect("probe tier tables")
                .iter()
                .map(|row| row.get::<String, _>("name"))
                .collect::<Vec<_>>()
            }
        };
        assert!(
            tier_tables(probe.pool()).await.is_empty(),
            "opening the journal must not create tier schemas"
        );

        let resolved = opened.resolve().await.expect("resolve tiers");
        let mut after = tier_tables(probe.pool()).await;
        after.sort();
        assert_eq!(
            after,
            vec![
                "borg_autonomy_schema".to_string(),
                "borg_workspace_schema".to_string()
            ],
            "resolving is what builds the tiers"
        );
        assert_eq!(resolved.backend(), SessionBackend::Sqlite);
    }

    /// A journal that acquires a SECOND owner is refused unless sharing was
    /// asked for.
    ///
    /// The owner is this machine and OS user, which is exactly the boundary
    /// workspace membership and `/broadcast` are scoped to. A SQLite journal
    /// could not be shared by accident because it was a file; a connection
    /// string can be, and the sharing would be invisible until someone noticed
    /// a stranger in their workspace. Simulated here by writing a second owner
    /// row directly, because a test cannot become a different OS user.
    #[tokio::test]
    async fn a_second_owner_is_refused_unless_sharing_is_explicit() {
        let Some(url) = crate::session_store::postgres::testing::test_url() else {
            eprintln!("factory: skipping postgres, BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let scratch = crate::session_store::postgres::testing::ScratchDatabase::create(&url).await;
        let config = SessionStoreConfig::with_url(&scratch.url, "/unused/sessions.sqlite3");

        // First owner: this process. Opening records it and succeeds.
        let opened = open(&config).await.expect("a fresh journal has one owner");
        let store = crate::session_store::postgres::PostgresSessionStore::connect_with_pool_size(
            &scratch.url,
            2,
        )
        .await
        .expect("probe connection");
        drop(opened);

        // A different machine and user reaches the same database.
        sqlx::query("insert into borg_journal_owners (fingerprint, display) values ($1, $2)")
            .bind("someone@another-machine")
            .bind("someone on another-machine")
            .execute(store.pool())
            .await
            .expect("record a second owner");

        let error = open(&config)
            .await
            .expect_err("a silently shared journal must not open");
        let message = format!("{error:#}");
        assert!(
            message.contains("another-machine"),
            "the error must name the other owner, got: {message}"
        );
        assert!(
            message.contains("trust boundary") && message.contains("BORG_SESSIONS_SHARED"),
            "the error must say what is at stake and how to opt in, got: {message}"
        );

        scratch.discard().await;
    }

    /// The missing-tier check must fire for EVERY tier, not just the first one
    /// that happens to be checked. A backend that served workspaces but not
    /// autonomy would otherwise slip through the guard above.
    #[tokio::test]
    async fn a_backend_missing_only_the_autonomy_tier_is_also_refused() {
        let directory = tempfile::tempdir().expect("tempdir");
        let sqlite = SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
            .await
            .expect("sqlite");
        let workspace = sqlite
            .workspace_store()
            .await
            .expect("workspace tier")
            .expect("workspace tier");

        let error = resolve_tiers(
            SessionBackend::Postgres,
            Arc::new(TierlessStore {
                workspace: Some(workspace),
            }),
        )
        .await
        .expect_err("a store with no autonomy tier must not resolve");
        let message = format!("{error:#}");
        assert!(
            message.contains("autonomy"),
            "the error must name the autonomy tier, got: {message}"
        );
    }
}
