# Collaboration, ACP, and reliability contract

This document is normative for Borg adapters that expose a durable agent session
to another process or person.

## Authorities

1. `SessionEvent` is the sole execution transcript. ACP, terminal, browser, and
   collaboration clients project it; they never maintain a competing transcript.
2. `WorkspaceEvent` is the sole multiplayer coordination log. The workspace host
   assigns sequence, actor, role, and provenance after authenticating the caller.
3. A `SessionWriterLease` grants one process authority to mutate a local session.
   Adapters attach to that owner or fail closed; they never start a second actor.
4. Provider processes and workspace commands are children of the session actor.
   Cancellation, close, host loss, and process exit must reach a durable terminal
   event before resources are forgotten.

## Collaboration capabilities

A share contains three independent values:

- an opaque room identifier used for relay routing;
- an encryption key carried in the URL fragment and never sent to the relay;
- either a view or control capability, also in the fragment.

View permits ordered event replay only. Control permits prompts, interrupt, and
approval responses; it does not permit changing roots, exporting credentials,
minting links, or attributing events to another participant. The host validates
every command and records the authenticated participant and capability. A relay
stores and forwards only versioned ciphertext frames and bounded routing metadata.

Links are bearer capabilities: logs and telemetry must redact their fragments.
Revocation increments a durable room epoch. Frames from an old epoch fail closed.
Nonce reuse with one key is forbidden. Clients reject duplicate frame IDs,
regressing sequence numbers, unknown protocol versions, invalid authentication
tags, and frames larger than the configured limit.

## Transaction and acknowledgement boundaries

- Mutation IDs are globally unique and stable across retry.
- A command is acknowledged only after its canonical event commit succeeds.
- Duplicate mutation IDs return the original outcome without executing again.
- Sequence allocation, event insertion, payload insertion, projections, audit
  record, and durable delivery cursor updates occur in one database transaction
  when they describe one logical mutation.
- Network send is not part of a database transaction. A committed outbox record
  bridges that boundary; delivery is at-least-once and consumers are idempotent.
- A disconnect or timeout before the commit outcome is known is *indeterminate*,
  not failed. Recovery resolves it from the receipt/event stores before retry.

SQLite uses WAL, foreign keys, a busy timeout, and `synchronous=FULL` for durable
authorities. Startup verifies schema/projection versions and replays projections
from the canonical log when a clean checkpoint is absent.

## ACP mapping

- `session/new` creates the Borg store row before acquiring and launching the
  actor. It returns only after actor admission.
- `session/load` validates the persisted cwd, acquires the writer lease, replays
  prior user/assistant/tool/plan updates, and then accepts new prompts.
- `session/prompt` subscribes before sending `HostCommand::Prompt` and correlates
  completion by Borg `message_id`.
- assistant messages, reasoning, tool calls/results, plans, and usage become
  `session/update` notifications.
- permission requests are delegated to the ACP client and mapped to one Borg
  approval ID exactly once.
- `session/cancel` interrupts current work and the prompt response ends with
  `cancelled`. `session/close` cancels work, stops the actor, and releases its
  lease without deleting durable history.

ACP clients can supply text and resource links. Unsupported content is rejected
before a prompt is admitted; it is never silently dropped.

## Recovery matrix

| Failure | Required recovery |
| --- | --- |
| crash before commit | no canonical event; retry may execute |
| crash after commit, before reply | receipt/event lookup returns committed outcome |
| relay disconnect | reconnect from last committed cursor; deduplicate frame IDs |
| client disconnect during turn | turn continues unless policy requests cancel |
| host crash during provider/tool work | startup records interrupted/indeterminate state and reconciles child process receipts |
| projection mismatch | rebuild from canonical ordered events |
| corrupt/truncated tail | quarantine invalid tail, preserve last verified commit, report degraded health |
| stale/revoked capability | reject without touching the session actor |

## Observability

Every lifecycle transition carries `operation_id`, `session_id`, optional
`workspace_id`, component, old/new state, monotonic duration, and an outcome
class (`ok`, `rejected`, `cancelled`, `retryable`, `indeterminate`, `fatal`).
Audit events additionally carry authenticated actor/capability and command kind.

Prompts, file contents, provider reasoning, credentials, encryption material,
and URL fragments are excluded from metrics and ordinary logs. Operator health
reports include:

- database open/integrity/checkpoint and projection version;
- writer lease owner/liveness;
- actor, provider, command, and child-process state;
- outbox depth/oldest age/retry count;
- relay connection, epoch, last sent/acked/received sequence;
- rejected authentication/decryption/replay/oversize counters;
- last successful commit and last recovery action.

Readiness means the process can safely admit a mutation. Liveness only means its
event loop is making progress. A degraded dependency must not be reported healthy
merely because the CLI process is alive.

## Verification gates

Tests cover happy paths plus duplicate, reordered, delayed, truncated, corrupt,
oversize, unauthorized, revoked, cancelled, timeout, disk-full, busy database,
process crash, host crash, relay loss, and restart at every commit/ack boundary.
Property tests assert monotonic sequences, nonce uniqueness, capability
non-escalation, idempotency, and projection equivalence after arbitrary replay.

## Operator commands

Start a relay reachable by both participants:

```text
borg collab relay --listen 0.0.0.0:8787
```

The relay is deliberately stateless and opaque. Put TLS and normal connection
rate/size limits in front of it before exposing it publicly. Set
`BORG_COLLAB_RELAY=wss://relay.example/ws` in the session-owner environment,
then use `/collab` for control + view links, `/collab view` to display only the
read-only link, and `/collab stop` to revoke the live room. A terminal can join
either link with `borg collab join '<link>'`.

Editors launch the ACP adapter over stdio:

```text
borg acp --provider codex --permission manual
```

Run `borg doctor` (or `borg doctor --json`) for a non-content-bearing durability
readiness report. A degraded result exits unsuccessfully so service managers can
use it as a readiness probe. Add `--deep` when an exhaustive SQLite integrity
scan is required; that scan intentionally reads the full durable database.
