//! Workspace tier: one suite, both backends.
//!
//! The workspace store carries the rules that decide who receives a message and
//! whether a retry is a duplicate. Those are exactly the rules that must not
//! drift between engines, so they are asserted here against SQLite and
//! PostgreSQL with identical inputs.

use chrono::{Duration, Utc};
use uuid::Uuid;

use crate::session_store::postgres::PostgresSessionStore;
use crate::session_store::postgres::testing::{ScratchDatabase, test_url};
use crate::workspace::{
    Audience, DeliveryMode, DeliveryState, Participant, ParticipantKind, PresenceLease,
    SqliteWorkspaceStore, Workspace, WorkspaceEvent, WorkspaceEventKind, WorkspaceMembership,
    WorkspaceMessage, WorkspaceMessageBody, WorkspaceRole, WorkspaceStore,
};
use crate::workspace_postgres::PostgresWorkspaceStore;

struct Harness {
    name: &'static str,
    store: Box<dyn WorkspaceStore>,
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
    let sqlite = SqliteWorkspaceStore::open(directory.path().join("workspace.sqlite3"))
        .await
        .expect("open sqlite workspace store");
    harnesses.push(Harness {
        name: "sqlite",
        store: Box::new(sqlite),
        _directory: Some(directory),
        scratch: None,
    });

    if let Some(url) = test_url() {
        let scratch = ScratchDatabase::create(&url).await;
        // The session store owns schema bootstrap for the whole database,
        // including the satellite tiers this store reads.
        let session = PostgresSessionStore::connect(&scratch.url)
            .await
            .expect("bootstrap postgres schema");
        let store = PostgresWorkspaceStore::from_pool(session.pool().clone());
        harnesses.push(Harness {
            name: "postgres",
            store: Box::new(store),
            _directory: None,
            scratch: Some(scratch),
        });
    } else {
        eprintln!("workspace conformance: skipping postgres, BORG_TEST_SESSIONS_URL is not set");
    }
    harnesses
}

fn participant(id: Uuid, name: &str) -> Participant {
    Participant {
        id,
        display_name: name.to_string(),
        kind: ParticipantKind::Agent,
        created_at: Utc::now(),
    }
}

fn message_event(
    workspace_id: Uuid,
    author_id: Uuid,
    text: &str,
    audience: Audience,
    idempotency_key: &str,
) -> WorkspaceEvent {
    WorkspaceEvent {
        id: Uuid::new_v4(),
        workspace_id,
        sequence: 0,
        author_id,
        idempotency_key: idempotency_key.to_string(),
        created_at: Utc::now(),
        kind: WorkspaceEventKind::Message {
            message: WorkspaceMessage {
                id: Uuid::new_v4(),
                workspace_id,
                thread_id: None,
                reply_to_message_id: None,
                author_id,
                body: WorkspaceMessageBody {
                    text: text.to_string(),
                    mentions: Vec::new(),
                },
                audience,
                created_at: Utc::now(),
            },
            mode: DeliveryMode::Notify,
        },
    }
}

/// A workspace with three members: an author and two others.
async fn workspace_with_members(store: &dyn WorkspaceStore) -> (Uuid, Uuid, Uuid, Uuid) {
    let workspace_id = Uuid::new_v4();
    let author = Uuid::new_v4();
    let second = Uuid::new_v4();
    let third = Uuid::new_v4();
    for (id, name) in [(author, "author"), (second, "second"), (third, "third")] {
        store
            .create_participant(participant(id, name))
            .await
            .expect("create participant");
    }
    store
        .create_workspace(Workspace {
            id: workspace_id,
            name: "conformance".to_string(),
            created_at: Utc::now(),
        })
        .await
        .expect("create workspace");
    for id in [author, second, third] {
        store
            .add_member(WorkspaceMembership {
                workspace_id,
                participant_id: id,
                role: WorkspaceRole::Editor,
                joined_at: Utc::now(),
            })
            .await
            .expect("add member");
    }
    (workspace_id, author, second, third)
}

#[tokio::test]
async fn an_appended_message_is_sequenced_and_replayable_by_its_recipients() {
    for harness in harnesses().await {
        let store = harness.store.as_ref();
        let name = harness.name;
        let (workspace_id, author, second, _third) = workspace_with_members(store).await;

        let event = store
            .append(message_event(
                workspace_id,
                author,
                "hello workspace",
                Audience::Workspace,
                "key-1",
            ))
            .await
            .expect("append");
        assert_eq!(event.sequence, 1, "[{name}] sequences start at one");

        // The author sees their own event; so does a recipient.
        assert_eq!(
            store.replay(workspace_id, author, 0, 50).await.expect("replay").len(),
            1,
            "[{name}] an author sees their own event"
        );
        assert_eq!(
            store.replay(workspace_id, second, 0, 50).await.expect("replay").len(),
            1,
            "[{name}] a recipient sees a delivered event"
        );

        // A non-member cannot replay at all.
        assert!(
            store.replay(workspace_id, Uuid::new_v4(), 0, 50).await.is_err(),
            "[{name}] a non-member must not read a workspace"
        );

        let second_event = store
            .append(message_event(
                workspace_id,
                author,
                "second message",
                Audience::Workspace,
                "key-2",
            ))
            .await
            .expect("append");
        assert_eq!(second_event.sequence, 2, "[{name}]");
        harness.discard().await;
    }
}

#[tokio::test]
async fn an_idempotency_key_admits_once_and_rejects_a_changed_payload() {
    for harness in harnesses().await {
        let store = harness.store.as_ref();
        let name = harness.name;
        let (workspace_id, author, _second, _third) = workspace_with_members(store).await;

        let event = message_event(
            workspace_id,
            author,
            "only once",
            Audience::Workspace,
            "shared-key",
        );
        let first = store.append(event.clone()).await.expect("append");
        // A retry of the same logical event returns the original rather than
        // admitting a duplicate.
        let retried = store.append(event).await.expect("retry");
        assert_eq!(first.sequence, retried.sequence, "[{name}]");
        assert_eq!(
            store.replay(workspace_id, author, 0, 50).await.unwrap().len(),
            1,
            "[{name}] a retry must not create a second event"
        );

        // The same key with different content is a caller bug, not a retry.
        let conflicting = message_event(
            workspace_id,
            author,
            "something else",
            Audience::Workspace,
            "shared-key",
        );
        assert!(store.append(conflicting).await.is_err(), "[{name}]");
        harness.discard().await;
    }
}

#[tokio::test]
async fn a_direct_audience_reaches_only_its_target() {
    for harness in harnesses().await {
        let store = harness.store.as_ref();
        let name = harness.name;
        let (workspace_id, author, second, third) = workspace_with_members(store).await;

        store
            .append(message_event(
                workspace_id,
                author,
                "for your eyes only",
                Audience::Direct {
                    participant: second,
                },
                "direct-1",
            ))
            .await
            .expect("append");

        assert_eq!(
            store.deliveries_after(workspace_id, second, 0, 50).await.unwrap().len(),
            1,
            "[{name}] the target receives a direct message"
        );
        assert!(
            store.deliveries_after(workspace_id, third, 0, 50).await.unwrap().is_empty(),
            "[{name}] an uninvolved member must not receive a direct message"
        );
        assert!(
            store.replay(workspace_id, third, 0, 50).await.unwrap().is_empty(),
            "[{name}] a private audience must not leak through replay"
        );

        // An audience naming a non-member is refused outright.
        assert!(
            store
                .append(message_event(
                    workspace_id,
                    author,
                    "to a stranger",
                    Audience::Direct {
                        participant: Uuid::new_v4()
                    },
                    "direct-2",
                ))
                .await
                .is_err(),
            "[{name}]"
        );
        // So is an author who is not a member.
        assert!(
            store
                .append(message_event(
                    workspace_id,
                    Uuid::new_v4(),
                    "from a stranger",
                    Audience::Workspace,
                    "direct-3",
                ))
                .await
                .is_err(),
            "[{name}]"
        );
        harness.discard().await;
    }
}

#[tokio::test]
async fn delivery_transitions_are_monotonic_and_count_attempts() {
    for harness in harnesses().await {
        let store = harness.store.as_ref();
        let name = harness.name;
        let (workspace_id, author, second, _third) = workspace_with_members(store).await;
        let event = store
            .append(message_event(
                workspace_id,
                author,
                "deliver me",
                Audience::Workspace,
                "delivery-1",
            ))
            .await
            .expect("append");

        let delivered = store
            .transition_delivery(
                workspace_id,
                event.sequence,
                second,
                DeliveryState::Relayed,
                None,
            )
            .await
            .expect("relay");
        assert_eq!(delivered.state, DeliveryState::Relayed, "[{name}]");

        // Repeating the current state is a no-op rather than an error, so a
        // retrying relay is not punished.
        let repeated = store
            .transition_delivery(
                workspace_id,
                event.sequence,
                second,
                DeliveryState::Relayed,
                None,
            )
            .await
            .expect("repeat");
        assert_eq!(repeated.attempts, delivered.attempts, "[{name}]");

        // Going backwards is refused: delivery state only moves forward.
        assert!(
            store
                .transition_delivery(
                    workspace_id,
                    event.sequence,
                    second,
                    DeliveryState::Pending,
                    None
                )
                .await
                .is_err(),
            "[{name}] a non-monotonic delivery transition must be refused"
        );

        let acknowledged = store
            .transition_delivery(
                workspace_id,
                event.sequence,
                second,
                DeliveryState::Acknowledged,
                None,
            )
            .await
            .expect("acknowledge");
        assert_eq!(acknowledged.state, DeliveryState::Acknowledged, "[{name}]");

        // A delivery that was never created cannot be transitioned.
        assert!(
            store
                .transition_delivery(
                    workspace_id,
                    event.sequence,
                    Uuid::new_v4(),
                    DeliveryState::Relayed,
                    None
                )
                .await
                .is_err(),
            "[{name}]"
        );
        harness.discard().await;
    }
}

#[tokio::test]
async fn presence_leases_expire_and_exclude_non_members() {
    for harness in harnesses().await {
        let store = harness.store.as_ref();
        let name = harness.name;
        let (workspace_id, author, second, _third) = workspace_with_members(store).await;

        store
            .acquire_presence_lease(PresenceLease {
                workspace_id,
                participant_id: author,
                client_id: Uuid::new_v4(),
                host_id: None,
                expires_at: Utc::now() + Duration::hours(1),
            })
            .await
            .expect("acquire");
        let active = store
            .active_presence(workspace_id, Utc::now())
            .await
            .expect("active");
        assert_eq!(active.len(), 1, "[{name}]");
        assert_eq!(active[0].participant_id, author, "[{name}]");

        // A lease that has already expired is not a lease.
        assert!(
            store
                .acquire_presence_lease(PresenceLease {
                    workspace_id,
                    participant_id: second,
                    client_id: Uuid::new_v4(),
                    host_id: None,
                    expires_at: Utc::now() - Duration::minutes(1),
                })
                .await
                .is_err(),
            "[{name}]"
        );
        // Nor may a non-member hold presence in a workspace.
        assert!(
            store
                .acquire_presence_lease(PresenceLease {
                    workspace_id,
                    participant_id: Uuid::new_v4(),
                    client_id: Uuid::new_v4(),
                    host_id: None,
                    expires_at: Utc::now() + Duration::hours(1),
                })
                .await
                .is_err(),
            "[{name}]"
        );

        // Reading presence in the future sweeps what has lapsed.
        assert!(
            store
                .active_presence(workspace_id, Utc::now() + Duration::hours(2))
                .await
                .expect("active")
                .is_empty(),
            "[{name}] an expired lease must not remain active"
        );
        harness.discard().await;
    }
}

#[tokio::test]
async fn the_shared_read_surface_answers_identically() {
    for harness in harnesses().await {
        let store = harness.store.as_ref();
        let name = harness.name;
        let (workspace_id, author, second, _third) = workspace_with_members(store).await;

        assert_eq!(
            store.workspace_name(workspace_id).await.expect("name"),
            Some("conformance".to_string()),
            "[{name}]"
        );
        assert_eq!(
            store.workspace_name(Uuid::new_v4()).await.expect("name"),
            None,
            "[{name}] an unknown workspace has no name"
        );

        let found = store
            .participant(author)
            .await
            .expect("participant")
            .expect("author exists");
        assert_eq!(found.id, author, "[{name}]");
        assert!(
            store
                .participant(Uuid::new_v4())
                .await
                .expect("participant")
                .is_none(),
            "[{name}]"
        );

        let roster = store
            .workspace_roster(workspace_id, author)
            .await
            .expect("roster");
        assert_eq!(roster.len(), 3, "[{name}]");
        // A roster is membership information, so a stranger may not read it.
        assert!(
            store
                .workspace_roster(workspace_id, Uuid::new_v4())
                .await
                .is_err(),
            "[{name}] a non-member must not read the roster"
        );

        let workspaces = store
            .list_workspaces_for_participant(second)
            .await
            .expect("workspaces");
        assert_eq!(workspaces.len(), 1, "[{name}]");
        assert_eq!(workspaces[0].id, workspace_id, "[{name}]");
        assert!(
            store
                .list_workspaces_for_participant(Uuid::new_v4())
                .await
                .expect("workspaces")
                .is_empty(),
            "[{name}]"
        );

        let event = store
            .append(message_event(
                workspace_id,
                author,
                "findable",
                Audience::Workspace,
                "read-surface-1",
            ))
            .await
            .expect("append");
        assert!(
            store.contains_message(event.id).await.expect("contains"),
            "[{name}] an appended message is findable by id"
        );
        assert!(
            !store.contains_message(Uuid::new_v4()).await.expect("contains"),
            "[{name}]"
        );
        assert!(
            store
                .contains_idempotent_event(workspace_id, author, "read-surface-1")
                .await
                .expect("idempotent"),
            "[{name}]"
        );
        assert!(
            !store
                .contains_idempotent_event(workspace_id, author, "never-used")
                .await
                .expect("idempotent"),
            "[{name}]"
        );

        // No session projection yet, so the replay watermark is zero rather
        // than an error: a fresh workspace replays from the beginning.
        assert_eq!(
            store
                .latest_projected_session_sequence(workspace_id, Uuid::new_v4())
                .await
                .expect("watermark"),
            0,
            "[{name}]"
        );
        harness.discard().await;
    }
}

/// The same guard as the session conformance suite: a broken Postgres
/// connection must not turn this file green while testing only SQLite.
#[tokio::test]
async fn both_workspace_backends_are_exercised_when_configured() {
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
