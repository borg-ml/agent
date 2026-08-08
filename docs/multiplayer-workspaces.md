# Multiplayer workspaces

Status: implementation contract

## Product model

Borg is multiplayer infrastructure for humans and agents. A workspace is the
shared durable unit. Humans, agents, automation, and execution hosts attach to
the same workspace and consume the same ordered event stream from CLI or web.

An execution session is not an identity and a chat thread is not a workspace.
A participant can outlive many sessions, use more than one client, and take
part in more than one thread.

## Authorities

Each kind of fact has exactly one durable authority:

| Fact | Authority |
| --- | --- |
| Shared authored content and coordination | workspace event stream |
| Provider turns, tool calls, approvals, usage | session event stream |
| Current participant access | workspace membership projection |
| Message admission for one recipient | message delivery projection |
| Ephemeral online state | renewable presence lease |
| Local process control | attached execution host |

The product backend extends its existing `workspaces` and `chat_messages`
tables. It must not create a second cloud chat log. The local runtime keeps a
SQLite projection of the same contracts. `SessionEvent` remains the complete
execution transcript; workspace events reference session IDs and sequences
instead of copying tool deltas.

## Stable identities

`Participant` is the shared identity envelope:

- `human`: authenticated user or explicitly invited teammate;
- `agent`: persistent logical teammate that may have many execution sessions;
- `service`: trusted automation or integration.

Every authored event records an immutable `author_participant_id`. Display
names, avatars, providers, models, and presence are mutable projections and
must never replace authorization identity.

An agent's participant ID is created when the agent joins the workspace. A
restart creates or resumes a session attached to that participant; it does not
create a new teammate.

`ExecutionHost` is separate from participant identity. Local laptops, shared
machines, and cloud workers advertise capabilities and hold expiring presence
leases. A participant may request work on a host only within both its
membership authority and the host's granted capability scope.

## Ordered event envelope

Every committed workspace event has:

- globally unique event ID;
- workspace ID;
- monotonically increasing workspace sequence;
- immutable author participant ID;
- client idempotency key scoped to `(workspace, author)`;
- typed payload and schema version;
- server commit time and optional causal event ID.

The append transaction allocates a sequence and persists the event atomically.
Repeating the same idempotency key returns the original event. Reusing the key
with a different payload is a conflict. Consumers replay after a cursor and
then subscribe; reconnecting cannot create a gap between backfill and live
delivery.

## Messages and threads

A message has one body, one author, and one immutable audience. It may start a
thread or reply to an earlier message. Mentions are structured participant
references, not reparsed display text.

Audience forms:

- whole workspace;
- explicit participants;
- role or group;
- direct message.

The workspace timeline is not a mandatory group chat. Workspace broadcasts are
rendered on the shared timeline, work/artifact threads are rendered with their
subject, and direct messages appear only in the participants' private inboxes.
All three use the same ordered envelope and delivery machinery so identity,
restart recovery, and references remain consistent. A participant can
explicitly promote a useful direct-message result into a shared thread; Borg
never does so implicitly.

Agent collaboration tools are address adapters over this same machinery:

- `/root/...` addresses a member of the current in-memory team and preserves
  the immediate wake/steer fast path;
- `session:<UUID>` addresses any session in the same durable local session
  store, including another root process or project workspace;
- `participant:<UUID>` addresses a member returned by
  `list_workspace_participants`, including a participant synchronized from an
  enrolled remote host;
- `broadcast_team` uses the current workspace audience rather than the
  caller's in-memory child list.

Local cross-workspace direct messages materialize one stable private peer
workspace for the two participants. Cloud and cross-machine messages require
the sender and recipient to share an authorized cloud workspace; membership
is the routing grant, not knowledge of a UUID. Every successful send returns
the committed message ID, workspace sequence, delivery mode, and exact
recipient IDs/count. Immediate local dispatch is reported separately and is
never presented as proof of durable recipient delivery.

Visibility and delivery are different. Visibility determines who may read a
message. `MessageDelivery` records how each recipient agent should admit it:

- `boundary`: steer the active turn at the next real model/tool boundary;
- `wake`: wake an idle participant, or use boundary delivery if already busy;
- `next_turn`: save for the next separately started turn;
- `notify`: update the inbox without model admission.

Delivery state is per recipient and transitions monotonically through pending,
admitted, acknowledged, or terminal failure. Admission is exactly once even
across process restart. A message may be recalled only while that recipient's
delivery remains pending. Read cursors are per participant and thread.

## Shared work

Messages carry discussion; typed events carry coordination:

- work item created, claimed, released, blocked, completed, or reviewed;
- artifact published or revised with content hash and provenance;
- decision proposed, accepted, superseded, or rejected;
- escalation requested, accepted, resolved, or declined;
- session and host attached, detached, or capability-changed.

Claims are transactional. Dependencies reference durable work item IDs.
Artifacts reference their producing participant, session, event, and revision.
Decisions remain in history when superseded.

## Autonomous teams

Autonomous teams are a configurable topology over normal participants and work
events. They are not a separate agent runtime.

A useful preset is:

- one high-reasoning director owns decomposition, assignment, review,
  synthesis, and the team budget;
- low-cost workers execute bounded work items concurrently;
- workers report completion, uncertainty, blockers, and evidence into the
  workspace;
- the director may clarify, retry, reassign, or spawn a temporary
  high-reasoning specialist;
- stop conditions bound time, tokens, cost, failures, and fan-out.

Core policy is provider-neutral. Provider, model, reasoning effort, permission
mode, concurrency, budgets, retry limits, and escalation thresholds are
configuration. An `xhigh` Codex director with `low` Codex workers is a preset,
not a protocol requirement.

### Mixed-provider thread launch

A root agent and one provider peer can be launched into the same durable team
thread explicitly:

```console
borg agent --provider codex --peer-provider claude \
  "Compare the two approaches, challenge each other's assumptions, and implement the agreed solution."
```

`--peer-model` and `--peer-effort` override the peer independently. Borg
creates the peer before admitting the root prompt, assigns each a distinct
durable participant/session identity, and gives both the same team directory,
workspace tools, goal/plan state, and direct-message channel. Provider changes
do not inherit an incompatible model or reasoning effort from the root.

The root and peer exchange attributed messages through the canonical workspace
delivery stream. Their provider/tool transcripts remain separate session
streams and are projected into the root team UI, preventing disconnected
histories while retaining unambiguous ownership, usage, approvals, and
interrupt controls.

### Launch-readiness checks

The mixed-provider path is covered at the provider-neutral session boundary,
so CI does not require paid Codex or Claude calls:

- a fresh root creates its configured peer before the root turn is admitted;
- cross-provider children do not inherit incompatible model or effort values;
- child-to-root and sibling messages retain attributed canonical identities;
- concurrent work claims are atomic, idempotent, and reject conflicting reuse;
- child interruption, durable topology restoration, and queued-turn recovery
  use the same tested session machinery as single-provider threads;
- parent projection retains each child's complete transcript and final result.

A release smoke should additionally run one credentialed thread:

```console
borg agent --provider codex --peer-provider claude --permission full-access \
  "Exchange one attributed finding, wait for each other, then synthesize one final answer."
```

The smoke passes only when both provider turns complete, the root observes the
Claude-attributed team message, and the root produces a joint final response.

Borg admits up to 16 live child agents by default for both manual and
autonomous teams. Users can lower that ceiling with
`[team].worker_concurrency` without enabling an autonomous preset. Total
assignment, report, escalation, and specialist limits are separate policy
settings; concurrency is not a hidden lifetime work budget.

Escalation is explicit and durable. A worker reports a reason such as missing
authority, ambiguous acceptance criteria, repeated failure, low confidence,
security risk, or budget pressure, together with evidence and the work item
cursor. The director decides whether to answer, change scope, grant authority,
or start a specialist. This makes autonomous execution inspectable instead of
hiding coordination in prompts.

## Authorization

Workspace roles are capability sets, not presentation labels. The built-in
roles are owner, administrator, editor, contributor, and viewer. Agent/service
members receive explicit roles and may be further restricted to assigned work,
hosts, paths, tools, budgets, and approval modes.

Required checks:

1. authenticate the acting human, service credential, or host;
2. resolve the durable participant represented by that credential;
3. verify active workspace membership and required capability;
4. validate payload references are visible in the same workspace;
5. persist immutable actor and approval provenance.

The client cannot choose an arbitrary `author_participant_id`. Server-side
credentials and session attachment determine it.

## Local and cloud synchronization

The same cursor protocol serves local CLI, remote CLI, and web:

1. attach using workspace, participant, client, and optional host identity;
2. send idempotent commands carrying the last observed sequence;
3. replay events after the local cursor;
4. renew presence and host capability leases;
5. receive live events and advance durable read/delivery cursors;
6. reconnect from the last committed cursor.

Cloud workspaces use Postgres as authority. Local-only workspaces use SQLite as
authority. Attaching a local workspace to cloud is an explicit one-time
authority transfer/import with recorded provenance; Borg never silently merges
two writable authorities with the same workspace ID.

An enrolled host receives the workspace and participant attachment on launch,
projects the authenticated cloud roster into its local cache, and uploads
locally authored message events with their event ID as the cloud idempotency
key. Borg.ml appends them to the existing `chat_messages` authority, creates
per-recipient deliveries, and enqueues a deterministic prompt command for each
attached remote recipient. Session admission and turn-completion events move
the corresponding cloud delivery to admitted and acknowledged. Retries across
API instances, host reconnects, or machine changes therefore converge on one
message and one recipient admission.

Execution is disposable; coordination is durable. Workers, sandboxes,
worktrees, containers, and cloud hosts may be ephemeral, but their lifecycle,
authority, costs, approvals, reports, and published outputs are committed to
the workspace. Borg also supports explicitly declared scratch workspaces with
a retention TTL. A scratch workspace is still persisted during its lifetime so
restart and recovery work; expiry performs a policy-controlled purge and keeps
only the minimum tombstone or audit record required by the configured
retention policy. Memory-only autonomous teams are not a supported durability
mode.

## Compatibility and migration

Migration is additive and restart-safe:

1. add participant, membership, thread, audience, idempotency, and delivery
   storage without changing legacy reads;
2. create a deterministic human participant for each existing workspace owner
   and membership;
3. project legacy `role=user` messages to the submitting human participant and
   legacy `role=assistant` messages to a deterministic workspace Borg agent;
4. retain `role` and `actor` as compatibility projections until all clients
   consume participant identity;
5. attach existing sessions to deterministic personal workspaces locally;
6. backfill delivery only for messages that require agent admission; historical
   display-only messages are considered visible and read;
7. reject partial or contradictory backfills transactionally.

Existing event IDs, message IDs, workspace IDs, ordering, encrypted bodies,
session journals, and access grants must remain unchanged.

## Extension surface

Extensions may add typed workspace payloads, views, coordination policies,
tools, hooks, and participant groups. They register versioned schemas and
reducers. Unknown event payloads remain replayable and visible as unsupported;
they must not corrupt the core projection.

Extensions cannot bypass membership checks, forge authorship, mutate committed
audiences, weaken delivery idempotency, or obtain host capabilities not granted
by the user.

Blu workflows consume the normal participant directory and message tools. They
may choose routing policy or compose communication with work events, but they
do not own a separate inbox, relay, cursor, or delivery state machine.

### Capability composition

Optional subsystems are explicit session capabilities rather than build-time
assumptions. `multiplayer`, `subagents`, `autonomous_team`, `shared_work`,
`presence`, `cloud_sync`, `web_relay`, and `telemetry` can be configured in
`agent.toml`. Disabling a parent capability cascades to its dependents:

- `subagents = false` also makes autonomous teams inactive;
- `multiplayer = false` also makes shared work and presence inactive;
- `cloud_sync = false` also makes the web relay inactive.

The core local agent, journal, goal/plan tools, replay, authorization, and
provider adapters continue to work. A disabled capability must not initialize
its database, coordinator, network client, background task, tool catalog, or UI
queries. `borg capabilities` reports the effective state and dependency reason
for every capability; `borg capabilities --json` exposes the same
provider-neutral descriptor to scripts and extensions.

Telemetry is disabled by default. Enabling it in a future build is not itself
permission to collect arbitrary session content: each telemetry implementation
must document its fields and purpose, minimize and redact data at the source,
bound retention, avoid prompt/file content in crash payloads, and provide a
working local disable control.

## Verification matrix

Completion requires boundary-level tests for:

- concurrent append produces a gap-free sequence;
- idempotent retry returns the original event and conflicting retry fails;
- human, agent, and service authors round-trip without losing identity;
- audience and membership prevent unauthorized replay;
- one message has independent delivery outcomes for two recipients;
- boundary delivery survives tool/model transitions and process restart;
- recall succeeds only before admission;
- director delegates to low-cost workers, receives reports, escalates a blocked
  item to a specialist, and respects team budget/concurrency/stop policies;
- two human clients plus several agents converge after disconnect/reconnect;
- local and cloud hosts retain explicit approval and execution provenance;
- legacy chat/session data migrates without ID, content, access, or order loss.
