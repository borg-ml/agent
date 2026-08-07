# Borg Agent Runtime Protocol v1

Status: compatibility boundary and canonical fixture set; not yet the final
dependency-neutral Cargo crate.

The provider-neutral runtime authority is the Borg session actor and its
lossless journal. A client or product adapter sends commands and receives
normalized events through a versioned envelope. The current CLI implementation
uses `HostCommand` and `SessionEvent` as the payload types so existing local
hosts, Web relays, goals, plans, approvals, subagents, MCP tools, and
provider-specific adapters keep their behavior during convergence.

The reference JSON fixtures live in
`crates/borg-remote/protocol-fixtures/v1/`. Borg Web conformance tests consume
that directory through `BORG_AGENT_RUNTIME_FIXTURES`; the product fork is a
consumer of the fixtures, not a second editable protocol source.

Each mutating command has a request ID, correlation ID, and idempotency key.
Legacy `HostCommandEnvelope` values are adapted by deriving a stable
`legacy-host-command:<id>` idempotency key. Events carry the canonical session
cursor, while snapshots are rebuildable projections of the event journal.

The enrolled CLI host now opts into this envelope with `protocol=1` when it
polls the Web relay and uploads session events. Borg Web keeps the legacy
command rows and raw event queue as its durable storage, then emits/accepts a
v1 transport adapter at the boundary; legacy clients still receive and send
the old shape. The CLI validates command and event envelopes and adapts them
exactly once into the existing HostCommand dispatcher and session journal, so
the live session actor remains unchanged while transport migration begins.
The canonical CLI provider layer now also carries the Web-only Kimi gateway
profile and OpenCode JSON subprocess adapter. Web still owns credentials,
gateway policy, and scoped product MCP; the CLI owns their provider-neutral
execution and normalized events. The next gate is shadowing one Web
Codex/Claude/Kimi/OpenCode turn and comparing event,
usage, approval, goal, artifact, and final-output projections before moving
the actor behind a shared dependency or worker client.

Web-owned MCP context is a separate authenticated capability grant. After the
host accepts a launch, it requests
`/api/remote/host/sessions/{session_id}/runtime-context` with its enrolled host
credential. Web derives the workspace/user-scoped MCP identity, scopes,
external server configuration, and short-lived token on demand. The relay
does not persist that response in the launch command or session event journal;
the host bounds the received configuration and keeps it only in the canonical
session actor's in-memory turn context. A missing endpoint is treated as a
legacy control plane and leaves host-local MCP behavior available. This keeps
Web's MCP and identity authority while allowing the CLI provider adapter to
construct the same scoped provider MCP setup.

The protocol is not a semantic-memory authority: legal/research records,
artifacts, billing, identity, and workspace policy remain owned by their
existing systems and enter the runtime through scoped adapters and typed
events.

## Execution boundary

`HostCapabilities.execution_profile` is an explicit security claim. The
current CLI host advertises `trusted_user`: workspace-root checks, bounded
processes, permission gates, and journaled effects are useful controls, but
ordinary host processes and the persistent Python/Bun worker are not a
sandbox. `isolated_hosted` is reserved for a worker with an independently
enforced container, microVM, or equivalent boundary. It must never be inferred
from Full Access, a runtime name, or a model-authored manifest. Web deployments
can set `BORG_REQUIRE_ISOLATED_HOSTS=1` to reject launches from hosts that do
not make that explicit capability claim.

The supported Linux reference deployment is `borg remote install` on an
enrolled host whose profile is `isolated_hosted`. It refuses to generate that
service unless `BORG_HOST_ALLOWED_NETWORKS` contains explicit IP addresses or
CIDRs for Borg, provider gateways, and DNS. The generated user service applies
systemd's `NoNewPrivileges`, private temporary/device namespaces, strict system
filesystem protection with `ReadWritePaths` limited to the host state and
enrolled workspace roots, an empty capability set, restricted address
families, cgroup-backed `CPUQuota`, `MemoryMax`, and `TasksMax` ceilings, and
`IPAddressDeny=any` with only the configured allowlist. The host process
verifies the service attestation, `NoNewPrivs`, and its generated
service cgroup before accepting sessions; launching the same config directly
from a shell is rejected. This is a reference isolation profile, not a claim
that every enrolled machine is safe: the Web control plane must still keep a
server-owned isolated-host allowlist and operators must review the underlying
systemd/network policy.

Trusted-user installations remain available for personal machines and local
dogfooding. They retain path, permission, resource, and journal controls but
must not be admitted when Web requires hosted isolation.

The host resource contract includes logical ceilings for session/runtime
duration, command output, file transfer, and concurrency, plus the isolated
worker's OS ceilings: `max_memory_bytes`, `max_cpu_percent`, and
`max_processes`. `BORG_HOST_MAX_MEMORY_BYTES`,
`BORG_HOST_MAX_CPU_PERCENT`, and `BORG_HOST_MAX_PROCESSES` may only lower the
canonical host defaults; the Web admission policy also rejects values above
its server ceilings.
