//! Plugin storage tier: one suite, both backends.
//!
//! Plugin state is compare-and-set storage with idempotent commits. The rules
//! that matter are the ones that decide whether a write is applied, rejected as
//! a revision conflict, or replayed -- so they are asserted against both
//! engines with identical inputs.

use serde_json::{Value, json};
use sha2::Digest;
use uuid::Uuid;

use crate::plugin_store::postgres::PostgresPluginStore;
use crate::plugin_store::{
    ArtifactInput, CommitScope, PluginBackend, PluginScope, PluginWrite, PreparedArtifact,
    SqlitePluginStore,
};
use crate::session_store::postgres::PostgresSessionStore;
use crate::session_store::postgres::testing::{ScratchDatabase, test_url};

struct Harness {
    name: &'static str,
    store: Box<dyn PluginBackend>,
    _directory: Option<tempfile::TempDir>,
    scratch: Option<ScratchDatabase>,
}

impl Harness {
    async fn discard(self) {
        if let Some(scratch) = self.scratch {
            scratch.discard().await;
        }
    }
}

async fn harnesses() -> Vec<Harness> {
    let mut harnesses = Vec::new();
    let directory = tempfile::tempdir().expect("temp dir");
    let session = crate::SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
        .await
        .expect("open sqlite session store");
    let sqlite = SqlitePluginStore::new(session.pool().clone());
    crate::plugin_store::ensure_schema(session.pool())
        .await
        .expect("plugin schema");
    harnesses.push(Harness {
        name: "sqlite",
        store: Box::new(sqlite),
        _directory: Some(directory),
        scratch: None,
    });

    if let Some(url) = test_url() {
        let scratch = ScratchDatabase::create(&url).await;
        let session = PostgresSessionStore::connect(&scratch.url)
            .await
            .expect("bootstrap postgres schema");
        harnesses.push(Harness {
            name: "postgres",
            store: Box::new(PostgresPluginStore::new(session.pool().clone())),
            _directory: None,
            scratch: Some(scratch),
        });
    } else {
        eprintln!("plugin conformance: skipping postgres, BORG_TEST_SESSIONS_URL is not set");
    }
    harnesses
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
        let content_hash = format!(
            "sha256:{}",
            hex::encode(sha2::Sha256::digest(contents))
        );
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

#[tokio::test]
async fn both_plugin_backends_are_exercised_when_configured() {
    let configured = test_url().is_some();
    let harnesses = harnesses().await;
    let names: Vec<&str> = harnesses.iter().map(|harness| harness.name).collect();
    assert!(names.contains(&"sqlite"));
    assert_eq!(
        names.contains(&"postgres"),
        configured,
        "postgres coverage must follow BORG_TEST_SESSIONS_URL, got {names:?}"
    );
    for harness in harnesses {
        harness.discard().await;
    }
}
