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
    SqliteWorkspaceStore, Thread, Workspace, WorkspaceEvent, WorkspaceEventKind,
    WorkspaceMembership, WorkspaceMessage, WorkspaceMessageBody, WorkspaceRole, WorkspaceStore,
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
        let session = PostgresSessionStore::connect_with_pool_size(&scratch.url, 4)
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
            store
                .replay(workspace_id, author, 0, 50)
                .await
                .expect("replay")
                .len(),
            1,
            "[{name}] an author sees their own event"
        );
        assert_eq!(
            store
                .replay(workspace_id, second, 0, 50)
                .await
                .expect("replay")
                .len(),
            1,
            "[{name}] a recipient sees a delivered event"
        );

        // A non-member cannot replay at all.
        assert!(
            store
                .replay(workspace_id, Uuid::new_v4(), 0, 50)
                .await
                .is_err(),
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
            store
                .replay(workspace_id, author, 0, 50)
                .await
                .unwrap()
                .len(),
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
            store
                .deliveries_after(workspace_id, second, 0, 50)
                .await
                .unwrap()
                .len(),
            1,
            "[{name}] the target receives a direct message"
        );
        assert!(
            store
                .deliveries_after(workspace_id, third, 0, 50)
                .await
                .unwrap()
                .is_empty(),
            "[{name}] an uninvolved member must not receive a direct message"
        );
        assert!(
            store
                .replay(workspace_id, third, 0, 50)
                .await
                .unwrap()
                .is_empty(),
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
            !store
                .contains_message(Uuid::new_v4())
                .await
                .expect("contains"),
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

#[tokio::test]
async fn workspace_provisioning_is_idempotent_and_direction_independent() {
    for harness in harnesses().await {
        let store = harness.store.as_ref();
        let name = harness.name;
        let workspace_id = Uuid::new_v4();
        let human = Uuid::new_v4();
        let agent = Uuid::new_v4();

        store
            .ensure_execution_workspace(workspace_id, "project", human, "Human", agent, "Agent")
            .await
            .expect("provision");
        // Relaunching the same session must refresh names, not duplicate
        // membership or fail on the existing workspace.
        store
            .ensure_execution_workspace(
                workspace_id,
                "project",
                human,
                "Human Renamed",
                agent,
                "Agent",
            )
            .await
            .expect("re-provision");

        let roster = store
            .workspace_roster(workspace_id, human)
            .await
            .expect("roster");
        assert_eq!(roster.len(), 2, "[{name}] membership must not duplicate");
        let human_entry = roster
            .iter()
            .find(|entry| entry.participant.id == human)
            .expect("human is a member");
        assert_eq!(
            human_entry.participant.display_name, "Human Renamed",
            "[{name}] a relaunch refreshes display names"
        );
        assert_eq!(human_entry.role, WorkspaceRole::Owner, "[{name}]");

        // A direct workspace is derived from the sorted participant pair, so
        // both directions must land on the same workspace rather than two.
        let forward = store
            .ensure_direct_workspace(human, agent)
            .await
            .expect("direct");
        let reverse = store
            .ensure_direct_workspace(agent, human)
            .await
            .expect("direct");
        assert_eq!(forward, reverse, "[{name}] direct workspaces are symmetric");
        assert!(
            store.ensure_direct_workspace(human, human).await.is_err(),
            "[{name}] a direct message needs two distinct participants"
        );
        assert!(
            store
                .ensure_direct_workspace(human, Uuid::new_v4())
                .await
                .is_err(),
            "[{name}] an unknown participant cannot be direct-messaged"
        );
        harness.discard().await;
    }
}

#[tokio::test]
async fn a_pending_message_is_listed_until_its_delivery_settles() {
    for harness in harnesses().await {
        let store = harness.store.as_ref();
        let name = harness.name;
        let (workspace_id, author, second, _third) = workspace_with_members(store).await;

        let event = store
            .append(message_event(
                workspace_id,
                author,
                "please read me",
                Audience::Workspace,
                "pending-1",
            ))
            .await
            .expect("append");

        let pending = store
            .pending_message_events(workspace_id, second, 10)
            .await
            .expect("pending");
        assert_eq!(
            pending.len(),
            1,
            "[{name}] an undelivered message is pending"
        );
        assert_eq!(pending[0].0.id, event.id, "[{name}]");
        assert_eq!(pending[0].1.state, DeliveryState::Pending, "[{name}]");
        // The author is not a recipient of their own message.
        assert!(
            store
                .pending_message_events(workspace_id, author, 10)
                .await
                .expect("pending")
                .is_empty(),
            "[{name}]"
        );
        assert!(
            store
                .pending_message_events(workspace_id, second, 0)
                .await
                .expect("pending")
                .is_empty(),
            "[{name}] a zero limit asks for nothing"
        );

        // Every recipient's row is visible for the message, across workspaces.
        let deliveries = store
            .message_deliveries(event.id)
            .await
            .expect("deliveries");
        assert_eq!(deliveries.len(), 2, "[{name}]");
        assert!(
            deliveries
                .iter()
                .all(|delivery| delivery.workspace_id == workspace_id),
            "[{name}]"
        );

        // Pending cannot jump straight to acknowledged; it is admitted first.
        store
            .transition_delivery(
                workspace_id,
                event.sequence,
                second,
                DeliveryState::Admitted,
                None,
            )
            .await
            .expect("admit");
        store
            .transition_delivery(
                workspace_id,
                event.sequence,
                second,
                DeliveryState::Acknowledged,
                None,
            )
            .await
            .expect("acknowledge");
        assert!(
            store
                .pending_message_events(workspace_id, second, 10)
                .await
                .expect("pending")
                .is_empty(),
            "[{name}] a settled delivery stops being pending"
        );
        harness.discard().await;
    }
}

#[tokio::test]
async fn a_message_cannot_reference_a_thread_or_reply_outside_its_workspace() {
    for harness in harnesses().await {
        let store = harness.store.as_ref();
        let name = harness.name;
        let (workspace_id, author, _second, _third) = workspace_with_members(store).await;
        let (other_workspace, other_author, _, _) = workspace_with_members(store).await;

        // A thread that belongs to a different workspace must not be usable as
        // a parent here, or a message grafts itself onto a conversation it is
        // not part of.
        let foreign_thread = Uuid::new_v4();
        store
            .create_thread(Thread {
                id: foreign_thread,
                workspace_id: other_workspace,
                title: "elsewhere".to_string(),
                created_at: Utc::now(),
            })
            .await
            .expect("create thread");

        let mut event = message_event(
            workspace_id,
            author,
            "grafted",
            Audience::Workspace,
            "reference-1",
        );
        if let WorkspaceEventKind::Message { message, .. } = &mut event.kind {
            message.thread_id = Some(foreign_thread);
        }
        assert!(
            store.append(event).await.is_err(),
            "[{name}] a thread from another workspace must be refused"
        );

        // Same for a reply target: the message being replied to has to exist
        // in this workspace.
        let elsewhere = store
            .append(message_event(
                other_workspace,
                other_author,
                "over here",
                Audience::Workspace,
                "reference-2",
            ))
            .await
            .expect("append elsewhere");
        let WorkspaceEventKind::Message {
            message: foreign, ..
        } = &elsewhere.kind
        else {
            panic!("[{name}] message expected");
        };
        let mut event = message_event(
            workspace_id,
            author,
            "replying across a boundary",
            Audience::Workspace,
            "reference-3",
        );
        if let WorkspaceEventKind::Message { message, .. } = &mut event.kind {
            message.reply_to_message_id = Some(foreign.id);
        }
        assert!(
            store.append(event).await.is_err(),
            "[{name}] a reply target from another workspace must be refused"
        );

        // A thread in this workspace is accepted, so the check is not simply
        // rejecting every reference.
        let local_thread = Uuid::new_v4();
        store
            .create_thread(Thread {
                id: local_thread,
                workspace_id,
                title: "here".to_string(),
                created_at: Utc::now(),
            })
            .await
            .expect("create thread");
        let mut event = message_event(
            workspace_id,
            author,
            "properly threaded",
            Audience::Workspace,
            "reference-4",
        );
        if let WorkspaceEventKind::Message { message, .. } = &mut event.kind {
            message.thread_id = Some(local_thread);
        }
        store.append(event).await.expect("local thread is valid");
        harness.discard().await;
    }
}

#[tokio::test]
async fn a_relay_message_lands_locally_without_local_reference_checks() {
    for harness in harnesses().await {
        let store = harness.store.as_ref();
        let name = harness.name;
        let recipient = Uuid::new_v4();
        let remote_author = Uuid::new_v4();
        store
            .create_participant(participant(recipient, "local"))
            .await
            .expect("create recipient");
        let workspace_id = Uuid::new_v4();

        let message = WorkspaceMessage {
            id: Uuid::new_v4(),
            workspace_id,
            // Cloud identities: neither exists locally, and that must not stop
            // the message from being delivered.
            thread_id: Some(Uuid::new_v4()),
            reply_to_message_id: Some(Uuid::new_v4()),
            author_id: remote_author,
            body: WorkspaceMessageBody {
                text: "from another installation".to_string(),
                mentions: Vec::new(),
            },
            audience: Audience::Direct {
                participant: recipient,
            },
            created_at: Utc::now(),
        };
        let event = store
            .import_relay_message(
                message.clone(),
                "Remote Agent",
                recipient,
                DeliveryMode::Notify,
            )
            .await
            .expect("import relay message");
        assert_eq!(event.author_id, remote_author, "[{name}]");
        assert!(
            store.contains_message(event.id).await.expect("contains"),
            "[{name}] an imported relay message is durable locally"
        );

        // Re-importing the same message is a replay, not a second delivery.
        let repeated = store
            .import_relay_message(
                message.clone(),
                "Remote Agent",
                recipient,
                DeliveryMode::Notify,
            )
            .await
            .expect("re-import");
        assert_eq!(repeated.sequence, event.sequence, "[{name}]");

        // The guards still hold: a message whose audience disagrees with the
        // recipient it was delivered for is refused.
        let mut mismatched = message.clone();
        mismatched.id = Uuid::new_v4();
        mismatched.audience = Audience::Direct {
            participant: Uuid::new_v4(),
        };
        assert!(
            store
                .import_relay_message(mismatched, "Remote Agent", recipient, DeliveryMode::Notify)
                .await
                .is_err(),
            "[{name}] relay recipient mismatch must be refused"
        );
        let mut self_addressed = message;
        self_addressed.id = Uuid::new_v4();
        self_addressed.author_id = recipient;
        assert!(
            store
                .import_relay_message(
                    self_addressed,
                    "Remote Agent",
                    recipient,
                    DeliveryMode::Notify
                )
                .await
                .is_err(),
            "[{name}] a relay sender cannot be its own recipient"
        );
        harness.discard().await;
    }
}

#[tokio::test]
async fn a_relay_roster_entry_projects_only_onto_a_local_workspace() {
    for harness in harnesses().await {
        let store = harness.store.as_ref();
        let name = harness.name;
        let (workspace_id, author, _second, _third) = workspace_with_members(store).await;

        // Projecting onto a workspace this machine has never materialised
        // would be inventing membership rather than caching it.
        assert!(
            store
                .upsert_relay_roster_entry(
                    Uuid::new_v4(),
                    participant(Uuid::new_v4(), "cloud"),
                    WorkspaceRole::Viewer,
                )
                .await
                .is_err(),
            "[{name}]"
        );

        let cloud = Uuid::new_v4();
        store
            .upsert_relay_roster_entry(
                workspace_id,
                participant(cloud, "Cloud"),
                WorkspaceRole::Viewer,
            )
            .await
            .expect("project roster");
        let roster = store
            .workspace_roster(workspace_id, author)
            .await
            .expect("roster");
        let entry = roster
            .iter()
            .find(|entry| entry.participant.id == cloud)
            .expect("projected participant is a member");
        assert_eq!(entry.role, WorkspaceRole::Viewer, "[{name}]");

        // Re-projecting updates the cached role rather than duplicating it.
        store
            .upsert_relay_roster_entry(
                workspace_id,
                participant(cloud, "Cloud"),
                WorkspaceRole::Editor,
            )
            .await
            .expect("re-project");
        let roster = store
            .workspace_roster(workspace_id, author)
            .await
            .expect("roster");
        assert_eq!(roster.len(), 4, "[{name}] re-projection must not duplicate");
        assert_eq!(
            roster
                .iter()
                .find(|entry| entry.participant.id == cloud)
                .expect("member")
                .role,
            WorkspaceRole::Editor,
            "[{name}]"
        );
        harness.discard().await;
    }
}

/// A local instance is discoverable, tombstoned on exit, and revived on relaunch.
///
/// Discovery is how agents find each other, so a backend that disagreed here
/// would either hide live peers or advertise dead ones. The tombstone is the
/// subtle part: exiting must not DELETE the row, because the instance's
/// identity and last-known location stay useful to anyone holding a reference
/// to it -- it must stop being advertised while remaining retrievable.
#[tokio::test]
async fn instance_discovery_tombstones_and_revives_identically() {
    for harness in harnesses().await {
        let store = harness.store.as_ref();
        let name = harness.name;
        let participant_id = Uuid::new_v4();
        store
            .create_participant(participant(participant_id, "worker"))
            .await
            .expect("participant");

        let cwd = std::path::Path::new("/tmp/borg-conformance-checkout");
        store
            .register_local_instance(participant_id, None, cwd, 4242)
            .await
            .unwrap_or_else(|error| panic!("[{name}] register: {error:#}"));

        let live = store
            .list_instances(false)
            .await
            .unwrap_or_else(|error| panic!("[{name}] list: {error:#}"));
        let found = live
            .iter()
            .find(|instance| instance.participant.id == participant_id)
            .unwrap_or_else(|| panic!("[{name}] a registered instance is discoverable"));
        assert_eq!(
            found.cwd.as_deref(),
            Some("/tmp/borg-conformance-checkout"),
            "[{name}] the launch directory distinguishes checkouts"
        );
        assert_eq!(found.pid, Some(4242), "[{name}]");
        assert!(
            found.seen_at.is_some(),
            "[{name}] a local registration records when it was seen"
        );

        let reaped = store
            .mark_local_instances_exited(&[participant_id])
            .await
            .unwrap_or_else(|error| panic!("[{name}] reap: {error:#}"));
        assert_eq!(reaped, 1, "[{name}] exactly the named instance is reaped");

        let advertised = store
            .list_instances(false)
            .await
            .unwrap_or_else(|error| panic!("[{name}] list after reap: {error:#}"));
        assert!(
            !advertised
                .iter()
                .any(|instance| instance.participant.id == participant_id),
            "[{name}] an exited instance is no longer advertised"
        );

        let including_exited = store
            .list_instances(true)
            .await
            .unwrap_or_else(|error| panic!("[{name}] list incl. exited: {error:#}"));
        assert!(
            including_exited
                .iter()
                .any(|instance| instance.participant.id == participant_id),
            "[{name}] an exited instance is retained, not deleted"
        );

        // Relaunching the same participant clears the tombstone rather than
        // creating a second row.
        store
            .register_local_instance(participant_id, None, cwd, 5353)
            .await
            .unwrap_or_else(|error| panic!("[{name}] re-register: {error:#}"));
        let revived: Vec<_> = store
            .list_instances(false)
            .await
            .unwrap_or_else(|error| panic!("[{name}] list after revive: {error:#}"))
            .into_iter()
            .filter(|instance| instance.participant.id == participant_id)
            .collect();
        assert_eq!(
            revived.len(),
            1,
            "[{name}] relaunch revives the existing instance instead of duplicating it"
        );
        assert_eq!(
            revived[0].pid,
            Some(5353),
            "[{name}] the revived instance carries the new process"
        );

        // Reaping something already gone is a no-op, not an error: the reaper
        // races with ordinary exits and must be safe to run repeatedly.
        let again = store
            .mark_local_instances_exited(&[Uuid::new_v4()])
            .await
            .unwrap_or_else(|error| panic!("[{name}] reap unknown: {error:#}"));
        assert_eq!(
            again, 0,
            "[{name}] reaping an unknown instance changes nothing"
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
