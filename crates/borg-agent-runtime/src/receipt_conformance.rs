//! Receipt tier: one suite, both backends.
//!
//! A receipt is the identity of a mutation. If the two backends disagree about
//! what counts as a replay, a host either repeats a mutation it already made or
//! refuses a legitimate retry -- so the rules are asserted against both.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::receipt::{ReceiptBackend, ReceiptState, SqliteReceiptStore};
use crate::receipt_postgres::PostgresReceiptStore;
use crate::session_store::postgres::PostgresSessionStore;
use crate::session_store::postgres::testing::{ScratchDatabase, test_url};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Request {
    action: String,
    target: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Response {
    ok: bool,
}

/// Both backends are driven through `dyn ReceiptBackend`, the same dispatch
/// production uses. This suite previously matched on an enum because no shared
/// trait existed; testing through the trait also covers the typed `load`/
/// `begin`/`finish` wrappers, which are where the request and response types
/// are erased and restored -- the part most likely to differ between engines.
struct Harness {
    name: &'static str,
    store: Box<dyn ReceiptBackend>,
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
    let sqlite = SqliteReceiptStore::open(session.pool().clone())
        .await
        .expect("open sqlite receipt store");
    harnesses.push(Harness {
        name: "sqlite",
        store: Box::new(sqlite),
        _directory: Some(directory),
        scratch: None,
    });

    if let Some(url) = test_url() {
        let scratch = ScratchDatabase::create(&url).await;
        let session = PostgresSessionStore::connect_with_pool_size(&scratch.url, 4)
            .await
            .expect("bootstrap postgres schema");
        harnesses.push(Harness {
            name: "postgres",
            store: Box::new(PostgresReceiptStore::from_pool(session.pool().clone())),
            _directory: None,
            scratch: Some(scratch),
        });
    } else {
        eprintln!("receipt conformance: skipping postgres, BORG_TEST_SESSIONS_URL is not set");
    }
    harnesses
}

fn request_for(target: &str) -> Request {
    Request {
        action: "delete".to_string(),
        target: target.to_string(),
    }
}

#[tokio::test]
async fn a_mutation_is_recorded_once_and_replays_its_response() {
    for harness in harnesses().await {
        let name = harness.name;
        let request_id = Uuid::new_v4();
        let request = request_for("/tmp/thing");

        assert!(
            matches!(
                harness
                    .store
                    .load::<Request, Response>(request_id, &request)
                    .await
                    .expect("load"),
                ReceiptState::Missing
            ),
            "[{name}] an unknown receipt is absent, not an error"
        );

        harness
            .store
            .begin(request_id, &request)
            .await
            .expect("begin");
        assert!(
            matches!(
                harness
                    .store
                    .load::<Request, Response>(request_id, &request)
                    .await
                    .expect("load"),
                ReceiptState::Started
            ),
            "[{name}] a recorded intent has no response yet"
        );

        // Repeating the intent is a no-op, so a crash between begin and finish
        // is recoverable rather than fatal.
        harness
            .store
            .begin(request_id, &request)
            .await
            .expect("re-begin");

        let response = Response { ok: true };
        harness
            .store
            .finish(request_id, &request, &response)
            .await
            .expect("finish");
        assert!(
            matches!(
                harness.store.load::<Request, Response>(request_id, &request).await
                    .expect("load"),
                ReceiptState::Terminal(ref stored) if *stored == response
            ),
            "[{name}] a finished receipt replays its response"
        );

        // A different request under the same id is a conflict, not a replay.
        let different = request_for("/tmp/other");
        assert!(
            matches!(
                harness
                    .store
                    .load::<Request, Response>(request_id, &different)
                    .await
                    .expect("load"),
                ReceiptState::Conflict
            ),
            "[{name}]"
        );

        // Re-finishing identically is a replay, not a second mutation.
        harness
            .store
            .finish(request_id, &request, &response)
            .await
            .expect("re-finish");
        harness.discard().await;
    }
}

#[tokio::test]
async fn a_receipt_id_cannot_be_reused_for_different_work() {
    for harness in harnesses().await {
        let name = harness.name;
        let request_id = Uuid::new_v4();
        let request = request_for("/tmp/original");
        harness
            .store
            .begin(request_id, &request)
            .await
            .expect("begin");

        // The receipt IS the mutation's identity, so the same id describing a
        // different action must be refused rather than silently accepted.
        let other = request_for("/tmp/something-else");
        assert!(
            harness.store.begin(request_id, &other).await.is_err(),
            "[{name}] a reused receipt id with a different request must be refused"
        );

        let response = Response { ok: true };
        harness
            .store
            .finish(request_id, &request, &response)
            .await
            .expect("finish");
        // And a published outcome must not be rewritten: a caller may already
        // have acted on it.
        assert!(
            harness
                .store
                .finish(request_id, &request, &Response { ok: false })
                .await
                .is_err(),
            "[{name}] a completed receipt must not be re-published with a new response"
        );
        harness.discard().await;
    }
}

#[tokio::test]
async fn finishing_without_a_recorded_intent_still_leaves_a_complete_audit() {
    for harness in harnesses().await {
        let name = harness.name;
        let request_id = Uuid::new_v4();
        let request = request_for("/tmp/direct");
        let response = Response { ok: true };
        harness
            .store
            .finish(request_id, &request, &response)
            .await
            .expect("finish");

        assert!(
            matches!(
                harness.store.load::<Request, Response>(request_id, &request).await
                    .expect("load"),
                ReceiptState::Terminal(ref stored) if *stored == response
            ),
            "[{name}] a direct finish is still a complete, replayable receipt"
        );
        harness.discard().await;
    }
}

#[tokio::test]
async fn the_host_queue_is_ordered_idempotent_and_quarantinable() {
    for harness in harnesses().await {
        let name = harness.name;
        let host = Uuid::new_v4();
        let other_host = Uuid::new_v4();
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();

        harness
            .store
            .enqueue_host_operation(host, first, &serde_json::json!({"cmd": "one"}))
            .await
            .expect("enqueue");
        harness
            .store
            .enqueue_host_operation(host, second, &serde_json::json!({"cmd": "two"}))
            .await
            .expect("enqueue");

        // Re-enqueueing the same command is a retry; the same id naming
        // different work is a bug.
        harness
            .store
            .enqueue_host_operation(host, first, &serde_json::json!({"cmd": "one"}))
            .await
            .expect("re-enqueue");
        assert!(
            harness
                .store
                .enqueue_host_operation(host, first, &serde_json::json!({"cmd": "different"}))
                .await
                .is_err(),
            "[{name}]"
        );

        // FIFO: the oldest live command comes first.
        let (next_id, command) = harness
            .store
            .next_host_operation(host)
            .await
            .expect("next")
            .expect("queued command");
        assert_eq!(next_id, first, "[{name}] the queue is ordered");
        assert_eq!(command, serde_json::json!({"cmd": "one"}), "[{name}]");

        // Another host sees none of it.
        assert!(
            harness
                .store
                .next_host_operation(other_host)
                .await
                .expect("next")
                .is_none(),
            "[{name}] a queue is scoped to its host"
        );
        assert!(
            harness
                .store
                .queued_host_operation(other_host, first)
                .await
                .expect("queued")
                .is_none(),
            "[{name}]"
        );

        harness
            .store
            .finish_host_operation(host, first)
            .await
            .expect("finish");
        let (next_id, _) = harness
            .store
            .next_host_operation(host)
            .await
            .expect("next")
            .expect("queued command");
        assert_eq!(next_id, second, "[{name}] finishing advances the queue");

        // A quarantined command stops blocking the head without being lost.
        harness
            .store
            .quarantine_host_operation(host, second)
            .await
            .expect("quarantine");
        assert!(
            harness
                .store
                .next_host_operation(host)
                .await
                .expect("next")
                .is_none(),
            "[{name}] a quarantined command is not served"
        );
        assert!(
            harness
                .store
                .queued_host_operation(host, second)
                .await
                .expect("queued")
                .is_none(),
            "[{name}]"
        );
        harness.discard().await;
    }
}

#[tokio::test]
async fn both_receipt_backends_are_exercised_when_configured() {
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
