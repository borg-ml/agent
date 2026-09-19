//! Plugin storage tier conformance.
//!
//! Plugin state is compare-and-set storage with idempotent commits. The rules
//! that matter decide whether a write is applied, rejected as a revision
//! conflict, or replayed, so they are asserted here.

use serde_json::{Value, json};
use sha2::Digest;
use uuid::Uuid;

use crate::plugin_store::postgres::PostgresPluginStore;
use crate::plugin_store::{
    ArtifactInput, CommitScope, PluginBackend, PluginScope, PluginWrite, PreparedArtifact,
};
use crate::session_store::postgres::PostgresSessionStore;
use crate::session_store::postgres::testing::{ScratchDatabase, test_url};

struct Harness {
    name: &'static str,
    store: Box<dyn PluginBackend>,
    scratch: ScratchDatabase,
}

impl Harness {
    async fn discard(self) {
        self.scratch.discard().await;
    }
}

async fn harnesses() -> Vec<Harness> {
    let Some(url) = test_url() else {
        eprintln!("plugin conformance: skipping postgres, BORG_TEST_SESSIONS_URL is not set");
        return Vec::new();
    };
    let scratch = ScratchDatabase::create(&url).await;
    // The session store owns schema bootstrap for the whole database,
    // including the satellite tiers this store reads.
    let session = PostgresSessionStore::connect_with_pool_size(&scratch.url, 4)
        .await
        .expect("bootstrap postgres schema");
    vec![Harness {
        name: "postgres",
        store: Box::new(PostgresPluginStore::new(session.pool().clone())),
        scratch,
    }]
}

const EXTENSION: &str = "conformance-extension";

fn scope_id() -> String {
    Uuid::new_v4().to_string()
}

fn put(key: &str, value: Value, expected_revision: Option<u64>) -> PluginWrite {
    PluginWrite::Put {
        key: key.to_string(),
        value,
        expected_revision,
    }
}

async fn commit_writes(
    harness: &Harness,
    scope_id: &str,
    key: &str,
    writes: &[PluginWrite],
) -> anyhow::Result<()> {
    harness
        .store
        .commit(
            CommitScope {
                extension_id: EXTENSION,
                scope: PluginScope::Session,
                scope_id,
            },
            &format!("{key}-{}", Uuid::new_v4()),
            "request-hash",
            writes,
            &[],
            &json!({"source": "conformance"}),
        )
        .await
        .map(|_| ())
}

#[tokio::test]
async fn a_write_is_readable_and_carries_its_revision() {
    for harness in harnesses().await {
        let name = harness.name;
        let scope_id = scope_id();

        assert!(
            harness
                .store
                .get_entry(EXTENSION, PluginScope::Session, &scope_id, "missing")
                .await
                .expect("get")
                .is_none(),
            "[{name}] an unwritten key is absent"
        );

        commit_writes(&harness, &scope_id, "k", &[put("k", json!({"v": 1}), None)])
            .await
            .expect("commit");
        let entry = harness
            .store
            .get_entry(EXTENSION, PluginScope::Session, &scope_id, "k")
            .await
            .expect("get")
            .expect("entry exists");
        assert_eq!(entry.value, Some(json!({"v": 1})), "[{name}]");
        assert_eq!(entry.revision, 1, "[{name}] a first write is revision one");

        let listed = harness
            .store
            .list_entries(EXTENSION, PluginScope::Session, &scope_id, None, 50)
            .await
            .expect("list");
        assert_eq!(listed.len(), 1, "[{name}]");
        harness.discard().await;
    }
}

#[tokio::test]
async fn compare_and_set_rejects_a_stale_expected_revision() {
    for harness in harnesses().await {
        let name = harness.name;
        let scope_id = scope_id();
        commit_writes(&harness, &scope_id, "k", &[put("k", json!(1), None)])
            .await
            .expect("first write");

        // Writing against the revision we actually observed succeeds...
        commit_writes(&harness, &scope_id, "k", &[put("k", json!(2), Some(1))])
            .await
            .expect("cas write");
        let entry = harness
            .store
            .get_entry(EXTENSION, PluginScope::Session, &scope_id, "k")
            .await
            .expect("get")
            .expect("entry");
        assert_eq!(entry.revision, 2, "[{name}]");
        assert_eq!(entry.value, Some(json!(2)), "[{name}]");

        // ...and writing against a revision that has moved on does not. This is
        // the whole point of the mechanism: a lost update must be refused.
        assert!(
            commit_writes(&harness, &scope_id, "k", &[put("k", json!(3), Some(1))])
                .await
                .is_err(),
            "[{name}] a stale expected revision must be refused"
        );
        let entry = harness
            .store
            .get_entry(EXTENSION, PluginScope::Session, &scope_id, "k")
            .await
            .expect("get")
            .expect("entry");
        assert_eq!(
            entry.value,
            Some(json!(2)),
            "[{name}] a refused write must not have been applied"
        );
        harness.discard().await;
    }
}

#[tokio::test]
async fn a_delete_tombstones_the_key_without_erasing_its_history() {
    for harness in harnesses().await {
        let name = harness.name;
        let scope_id = scope_id();
        commit_writes(&harness, &scope_id, "k", &[put("k", json!("live"), None)])
            .await
            .expect("write");
        commit_writes(
            &harness,
            &scope_id,
            "k",
            &[PluginWrite::Delete {
                key: "k".to_string(),
                expected_revision: Some(1),
            }],
        )
        .await
        .expect("delete");

        // The key still exists as a tombstone, carrying its next revision, so a
        // later compare-and-set can reason about it.
        let entry = harness
            .store
            .get_entry(EXTENSION, PluginScope::Session, &scope_id, "k")
            .await
            .expect("get")
            .expect("tombstone exists");
        assert_eq!(entry.value, None, "[{name}] a deleted key has no value");
        assert_eq!(entry.revision, 2, "[{name}]");

        // But listing is a live view and must not surface it.
        assert!(
            harness
                .store
                .list_entries(EXTENSION, PluginScope::Session, &scope_id, None, 50)
                .await
                .expect("list")
                .is_empty(),
            "[{name}] a deleted key must not appear in a listing"
        );
        harness.discard().await;
    }
}

#[tokio::test]
async fn a_commit_replays_instead_of_applying_twice() {
    for harness in harnesses().await {
        let name = harness.name;
        let scope_id = scope_id();
        let idempotency_key = "commit-once";
        let writes = vec![put("counter", json!(1), None)];

        let first = harness
            .store
            .commit(
                CommitScope {
                    extension_id: EXTENSION,
                    scope: PluginScope::Session,
                    scope_id: &scope_id,
                },
                idempotency_key,
                "hash-a",
                &writes,
                &[],
                &json!({}),
            )
            .await
            .expect("commit");
        assert!(!first.replayed, "[{name}] a first commit is not a replay");

        let replay = harness
            .store
            .commit(
                CommitScope {
                    extension_id: EXTENSION,
                    scope: PluginScope::Session,
                    scope_id: &scope_id,
                },
                idempotency_key,
                "hash-a",
                &writes,
                &[],
                &json!({}),
            )
            .await
            .expect("replay");
        assert!(replay.replayed, "[{name}] a repeated commit replays");

        // The write must have been applied exactly once.
        let entry = harness
            .store
            .get_entry(EXTENSION, PluginScope::Session, &scope_id, "counter")
            .await
            .expect("get")
            .expect("entry");
        assert_eq!(
            entry.revision, 1,
            "[{name}] a replayed commit must not apply its writes again"
        );

        // The same key with different content is a caller bug, not a replay.
        assert!(
            harness
                .store
                .existing_commit(
                    EXTENSION,
                    PluginScope::Session,
                    &scope_id,
                    idempotency_key,
                    "hash-b"
                )
                .await
                .is_err(),
            "[{name}] a reused idempotency key with new content must be refused"
        );
        harness.discard().await;
    }
}

#[tokio::test]
async fn a_recorded_artifact_is_verified_against_the_file_on_disk() {
    for harness in harnesses().await {
        let name = harness.name;
        let scope_id = scope_id();
        let root = tempfile::tempdir().expect("workspace root");
        let contents = b"artifact bytes";
        std::fs::write(root.path().join("out.bin"), contents).expect("write artifact");
        let content_hash = format!("sha256:{}", hex::encode(sha2::Sha256::digest(contents)));
        let prepared = PreparedArtifact {
            input: ArtifactInput {
                artifact_id: "artifact-1".to_string(),
                path: "out.bin".to_string(),
                name: None,
                run_id: None,
                media_type: None,
                metadata: json!({}),
            },
            byte_len: contents.len() as u64,
            content_hash: content_hash.clone(),
        };
        harness
            .store
            .commit(
                CommitScope {
                    extension_id: EXTENSION,
                    scope: PluginScope::Session,
                    scope_id: &scope_id,
                },
                "artifact-commit",
                "hash-a",
                &[],
                std::slice::from_ref(&prepared),
                &json!({}),
            )
            .await
            .expect("commit artifact");

        let verified = harness
            .store
            .verify_artifact(
                EXTENSION,
                PluginScope::Session,
                &scope_id,
                "artifact-1",
                root.path(),
            )
            .await
            .expect("verify");
        assert_eq!(verified["found"], json!(true), "[{name}]");
        assert_eq!(verified["valid"], json!(true), "[{name}]");

        // Tampering with the file must be detected rather than trusted.
        std::fs::write(root.path().join("out.bin"), b"different").expect("tamper");
        let tampered = harness
            .store
            .verify_artifact(
                EXTENSION,
                PluginScope::Session,
                &scope_id,
                "artifact-1",
                root.path(),
            )
            .await
            .expect("verify");
        assert_eq!(tampered["valid"], json!(false), "[{name}]");

        // An id that was never recorded is absent, not invalid-by-accident.
        let missing = harness
            .store
            .verify_artifact(
                EXTENSION,
                PluginScope::Session,
                &scope_id,
                "no-such-artifact",
                root.path(),
            )
            .await
            .expect("verify");
        assert_eq!(missing["found"], json!(false), "[{name}]");
        harness.discard().await;
    }
}

/// The dispatch entry point, not just the storage methods underneath it.
///
/// `plugin_store::call` carries all the backend-agnostic policy -- request
/// validation, scope resolution, idempotency hashing -- and sits above the
/// five storage primitives. Driving that exact entry point is what keeps the
/// rules built on top of them from going untested.
#[tokio::test]
async fn the_call_entry_point_applies_the_shared_policy() {
    for harness in harnesses().await {
        let name = harness.name;
        let root = tempfile::tempdir().expect("temp root");
        let session_id = Uuid::new_v4();

        let committed = crate::plugin_store::call(
            harness.store.as_ref(),
            session_id,
            root.path(),
            Some(EXTENSION),
            json!({
                "op": "commit",
                "scope": "session",
                "idempotency_key": "call-entry-1",
                "writes": [{"op": "put", "key": "a/b", "value": {"n": 1}}],
            }),
        )
        .await
        .unwrap_or_else(|error| panic!("[{name}] commit: {error:#}"));
        assert_eq!(committed["replayed"], json!(false), "[{name}]");

        // The same key replays rather than applying twice.
        let replayed = crate::plugin_store::call(
            harness.store.as_ref(),
            session_id,
            root.path(),
            Some(EXTENSION),
            json!({
                "op": "commit",
                "scope": "session",
                "idempotency_key": "call-entry-1",
                "writes": [{"op": "put", "key": "a/b", "value": {"n": 1}}],
            }),
        )
        .await
        .unwrap_or_else(|error| panic!("[{name}] replay: {error:#}"));
        assert_eq!(replayed["replayed"], json!(true), "[{name}]");

        let fetched = crate::plugin_store::call(
            harness.store.as_ref(),
            session_id,
            root.path(),
            Some(EXTENSION),
            json!({"op": "get", "scope": "session", "key": "a/b"}),
        )
        .await
        .unwrap_or_else(|error| panic!("[{name}] get: {error:#}"));
        assert_eq!(fetched["entry"]["value"], json!({"n": 1}), "[{name}]");

        let listed = crate::plugin_store::call(
            harness.store.as_ref(),
            session_id,
            root.path(),
            Some(EXTENSION),
            json!({"op": "list", "scope": "session", "prefix": "a/"}),
        )
        .await
        .unwrap_or_else(|error| panic!("[{name}] list: {error:#}"));
        assert_eq!(
            listed["entries"].as_array().map(Vec::len),
            Some(1),
            "[{name}] {listed}"
        );

        // Policy failures must come from the shared layer, not the backend.
        let unknown = crate::plugin_store::call(
            harness.store.as_ref(),
            session_id,
            root.path(),
            Some(EXTENSION),
            json!({"op": "nonsense", "scope": "session"}),
        )
        .await;
        assert!(unknown.is_err(), "[{name}] unknown op must be refused");

        let mismatched = crate::plugin_store::call(
            harness.store.as_ref(),
            session_id,
            root.path(),
            Some(EXTENSION),
            json!({"op": "get", "scope": "session", "key": "a/b",
                   "extension_id": "some-other-extension"}),
        )
        .await;
        assert!(
            mismatched.is_err(),
            "[{name}] a request must not read another extension's state"
        );

        harness.discard().await;
    }
}

/// Postgres is the only backend, so an unset or broken `BORG_TEST_SESSIONS_URL`
/// leaves `harnesses()` empty and every test above passes without asserting
/// anything. This is the guard that makes that vacuum visible instead of green.
#[tokio::test]
async fn postgres_coverage_follows_its_configuration() {
    let configured = test_url().is_some();
    let harnesses = harnesses().await;
    let names: Vec<&str> = harnesses.iter().map(|harness| harness.name).collect();
    assert_eq!(
        names.contains(&"postgres"),
        configured,
        "postgres coverage must follow BORG_TEST_SESSIONS_URL, got {names:?}"
    );
    for harness in harnesses {
        harness.discard().await;
    }
}
