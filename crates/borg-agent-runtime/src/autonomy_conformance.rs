//! Autonomy tier: one suite, both backends.
//!
//! This queue decides whether durable background work runs once, twice, or is
//! abandoned. Those rules are asserted here against SQLite and PostgreSQL with
//! identical inputs, because a disagreement means real work is lost or
//! duplicated rather than merely reported differently.

use std::time::Duration;

use chrono::Utc;
use uuid::Uuid;

use crate::autonomy::{
    AutonomyJob, AutonomyJobState, AutonomyLease, AutonomyStore, EnqueueAutonomyJob,
    SaveAutonomyCheckpoint, SqliteAutonomyStore,
};
use crate::autonomy_postgres::PostgresAutonomyStore;
use crate::session_store::postgres::PostgresSessionStore;
use crate::session_store::postgres::testing::{ScratchDatabase, test_url};

/// Both backends are driven through `dyn AutonomyStore` -- the same dynamic
/// dispatch production uses. This suite previously matched on an enum because
/// no shared trait existed; now that one does, testing through it means the
/// object-safe surface itself is covered, not just the two inherent impls.
struct Harness {
    name: &'static str,
    store: Box<dyn AutonomyStore>,
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
    // The autonomy store shares the session journal's pool, exactly as it does
    // in production: it must never become a second database authority.
    let session = crate::SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
        .await
        .expect("open sqlite session store");
    let sqlite = SqliteAutonomyStore::open(session.pool().clone())
        .await
        .expect("open sqlite autonomy store");
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
            store: Box::new(PostgresAutonomyStore::from_pool(session.pool().clone())),
            _directory: None,
            scratch: Some(scratch),
        });
    } else {
        eprintln!("autonomy conformance: skipping postgres, BORG_TEST_SESSIONS_URL is not set");
    }
    harnesses
}

fn job(key: &str, max_attempts: u32) -> EnqueueAutonomyJob {
    EnqueueAutonomyJob {
        job_id: None,
        idempotency_key: key.to_string(),
        kind: "probe".to_string(),
        payload: serde_json::json!({"work": key}),
        due_at: Utc::now(),
        max_attempts,
        session_id: None,
        goal_id: None,
    }
}

fn lease_of(job: &AutonomyJob) -> AutonomyLease {
    AutonomyLease {
        owner: job.lease_owner.clone().expect("owner"),
        token: job.lease_token.expect("token"),
    }
}

#[tokio::test]
async fn enqueue_is_idempotent_by_key() {
    for harness in harnesses().await {
        let name = harness.name;
        // A genuine retry is byte-identical, including its due time.
        let request = job("key-1", 3);
        let first = harness
            .store
            .enqueue(request.clone())
            .await
            .expect("enqueue");
        assert_eq!(first.state, AutonomyJobState::Queued, "[{name}]");
        assert_eq!(first.attempt, 0, "[{name}]");

        let repeated = harness
            .store
            .enqueue(request.clone())
            .await
            .expect("enqueue");
        assert_eq!(repeated.job_id, first.job_id, "[{name}]");

        // The same key describing different work is a caller bug: returning the
        // original would silently discard the new request.
        let mut conflicting = request;
        conflicting.payload = serde_json::json!({"work": "something else"});
        assert!(
            harness.store.enqueue(conflicting).await.is_err(),
            "[{name}] a reused key with different work must be refused"
        );

        let fetched = harness
            .store
            .get(first.job_id)
            .await
            .expect("get")
            .expect("job exists");
        assert_eq!(fetched.job_id, first.job_id, "[{name}]");
        assert!(
            harness
                .store
                .get(Uuid::new_v4())
                .await
                .expect("get")
                .is_none(),
            "[{name}]"
        );
        harness.discard().await;
    }
}

#[tokio::test]
async fn a_claim_is_exclusive_and_the_audit_records_every_step() {
    for harness in harnesses().await {
        let name = harness.name;
        let enqueued = harness
            .store
            .enqueue(job("claimable", 3))
            .await
            .expect("enqueue");
        let now = Utc::now();
        let claimed = harness
            .store
            .claim_due(now, "worker-a", Duration::from_secs(3_600), 10)
            .await
            .expect("claim");
        assert_eq!(claimed.len(), 1, "[{name}]");
        assert_eq!(claimed[0].job_id, enqueued.job_id, "[{name}]");
        assert_eq!(claimed[0].state, AutonomyJobState::Claimed, "[{name}]");

        // Already-claimed work is not offered to a second scheduler.
        let second = harness
            .store
            .claim_due(now, "worker-b", Duration::from_secs(3_600), 10)
            .await
            .expect("claim");
        assert!(
            second.is_empty(),
            "[{name}] a claimed job must not be re-offered"
        );

        let transitions = harness
            .store
            .list_transitions(enqueued.job_id)
            .await
            .expect("transitions");
        let states: Vec<AutonomyJobState> = transitions.iter().map(|t| t.to).collect();
        assert_eq!(
            states,
            vec![AutonomyJobState::Queued, AutonomyJobState::Claimed],
            "[{name}]"
        );
        harness.discard().await;
    }
}

#[tokio::test]
async fn an_attempt_is_spent_on_running_not_on_claiming() {
    for harness in harnesses().await {
        let name = harness.name;
        let enqueued = harness
            .store
            .enqueue(job("attempts", 1))
            .await
            .expect("enqueue");
        let now = Utc::now();
        let claimed = harness
            .store
            .claim_due(now, "worker-a", Duration::from_secs(3_600), 10)
            .await
            .expect("claim");
        // Claiming must not burn the budget: a worker that dies before running
        // would otherwise fail a job that never executed once.
        assert_eq!(claimed[0].attempt, 0, "[{name}]");

        let lease = lease_of(&claimed[0]);
        let running = harness
            .store
            .transition(
                enqueued.job_id,
                AutonomyJobState::Claimed,
                AutonomyJobState::Running,
                Some(&lease),
                None,
                Utc::now(),
            )
            .await
            .expect("run");
        assert_eq!(running.attempt, 1, "[{name}] running spends the attempt");

        let completed = harness
            .store
            .complete(
                enqueued.job_id,
                &lease,
                serde_json::json!({"ok": true}),
                Utc::now(),
            )
            .await
            .expect("complete");
        assert_eq!(completed.state, AutonomyJobState::Completed, "[{name}]");
        assert!(
            completed.lease_owner.is_none(),
            "[{name}] a finished job holds no lease"
        );
        assert_eq!(
            completed.result,
            Some(serde_json::json!({"ok": true})),
            "[{name}]"
        );
        harness.discard().await;
    }
}

#[tokio::test]
async fn an_expired_lease_requeues_until_the_budget_runs_out() {
    for harness in harnesses().await {
        let name = harness.name;
        let enqueued = harness
            .store
            .enqueue(job("recoverable", 1))
            .await
            .expect("enqueue");
        let claimed = harness
            .store
            .claim_due(Utc::now(), "crashed", Duration::from_secs(1), 10)
            .await
            .expect("claim");
        assert_eq!(claimed.len(), 1, "[{name}]");

        // Nothing is recovered while the lease is still live.
        assert!(
            harness
                .store
                .recover_expired(Utc::now(), 10)
                .await
                .expect("recover")
                .is_empty(),
            "[{name}] a live lease must not be reclaimed"
        );

        // Drive the clock forward instead of sleeping: the boundary is what is
        // under test, not the scheduler's timing.
        let future = Utc::now() + chrono::Duration::hours(1);
        let recovered = harness
            .store
            .recover_expired(future, 10)
            .await
            .expect("recover");
        assert_eq!(recovered.len(), 1, "[{name}]");
        assert_eq!(recovered[0].state, AutonomyJobState::Queued, "[{name}]");
        assert!(
            recovered[0]
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("lease expired")),
            "[{name}] recovery must record why work was requeued"
        );

        // Spend the only attempt, then let the lease expire again: with no
        // budget left the job is abandoned rather than retried forever.
        let claimed = harness
            .store
            .claim_due(future, "crashed-again", Duration::from_secs(1), 10)
            .await
            .expect("claim");
        let lease = lease_of(&claimed[0]);
        harness
            .store
            .transition(
                enqueued.job_id,
                AutonomyJobState::Claimed,
                AutonomyJobState::Running,
                Some(&lease),
                None,
                future,
            )
            .await
            .expect("run");
        let later = future + chrono::Duration::hours(1);
        let recovered = harness
            .store
            .recover_expired(later, 10)
            .await
            .expect("recover");
        assert_eq!(recovered.len(), 1, "[{name}]");
        assert_eq!(
            recovered[0].state,
            AutonomyJobState::Failed,
            "[{name}] an exhausted budget abandons the job"
        );
        harness.discard().await;
    }
}

#[tokio::test]
async fn a_stale_lease_cannot_drive_a_job_another_worker_now_owns() {
    for harness in harnesses().await {
        let name = harness.name;
        let enqueued = harness
            .store
            .enqueue(job("fenced", 5))
            .await
            .expect("enqueue");
        let claimed = harness
            .store
            .claim_due(Utc::now(), "worker-a", Duration::from_secs(1), 10)
            .await
            .expect("claim");
        let stale = lease_of(&claimed[0]);

        let future = Utc::now() + chrono::Duration::hours(1);
        harness
            .store
            .recover_expired(future, 10)
            .await
            .expect("recover");
        let reclaimed = harness
            .store
            .claim_due(future, "worker-b", Duration::from_secs(3_600), 10)
            .await
            .expect("claim");
        assert_eq!(reclaimed.len(), 1, "[{name}]");
        assert_ne!(reclaimed[0].lease_token, claimed[0].lease_token, "[{name}]");

        // The original worker waking up must not be able to advance work it no
        // longer owns.
        assert!(
            harness
                .store
                .transition(
                    enqueued.job_id,
                    AutonomyJobState::Claimed,
                    AutonomyJobState::Running,
                    Some(&stale),
                    None,
                    future,
                )
                .await
                .is_err(),
            "[{name}] a stale lease token must be fenced"
        );
        // Nor may a heartbeat from the stale owner extend it.
        assert!(
            harness
                .store
                .heartbeat(enqueued.job_id, &stale, future, Duration::from_secs(60))
                .await
                .is_err(),
            "[{name}]"
        );
        harness.discard().await;
    }
}

#[tokio::test]
async fn a_checkpoint_key_is_write_once_per_job() {
    for harness in harnesses().await {
        let name = harness.name;
        let enqueued = harness
            .store
            .enqueue(job("checkpoints", 3))
            .await
            .expect("enqueue");
        let checkpoint = SaveAutonomyCheckpoint {
            checkpoint_id: None,
            job_id: enqueued.job_id,
            checkpoint_key: "step-1".to_string(),
            session_id: None,
            goal_id: None,
            kind: "progress".to_string(),
            state: serde_json::json!({"done": 1}),
            evidence: serde_json::json!({"log": "first"}),
            created_at: Utc::now(),
        };
        let saved = harness
            .store
            .save_checkpoint(checkpoint.clone())
            .await
            .expect("save");
        assert_eq!(saved.checkpoint_key, "step-1", "[{name}]");

        // Saving identical content again is a retry, and returns the original.
        let repeated = harness
            .store
            .save_checkpoint(checkpoint.clone())
            .await
            .expect("save");
        assert_eq!(repeated.checkpoint_id, saved.checkpoint_id, "[{name}]");

        // The same key with different evidence would rewrite recorded history.
        let mut conflicting = checkpoint;
        conflicting.evidence = serde_json::json!({"log": "rewritten"});
        assert!(
            harness.store.save_checkpoint(conflicting).await.is_err(),
            "[{name}] a checkpoint key must not be reused with new evidence"
        );

        let listed = harness
            .store
            .list_checkpoints(enqueued.job_id)
            .await
            .expect("list");
        assert_eq!(listed.len(), 1, "[{name}]");
        harness.discard().await;
    }
}

#[tokio::test]
async fn both_autonomy_backends_are_exercised_when_configured() {
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
