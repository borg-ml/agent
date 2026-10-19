# Game development at agent scale

Borg provides an engine-neutral, host-local coordination layer for competing
builds, tests and resident editors. It is implemented, not an interface-only
preview. Engine policy lives in Blu adapters; Borg owns admission, process
lifetime, cancellation, recovery and durable state.

## Architecture

- `crates/borg-lanes/src/lanes.rs`: atomic multi-resource admission, shared
  capacity, conflicting-ticket FIFO, pending-only coalescing, job supervisors,
  hooks, budgets and verified recovery.
- `crates/borg-lanes/src/host_admission.rs`: RAM admission across participating
  lane journals and migration bridge claims on the same host. This does not
  coordinate old binaries or unwrapped compiler processes.
- `crates/borg-lanes/src/services.rs`: persistent services, health, client
  leases, restart barriers, yield/resume and a stable loopback proxy.
- `crates/borg-lanes/src/workspace/hygiene.rs`: worktree admission, protected
  GC, output reporting and cooperative freeze handshakes.
- `crates/borg-agent-runtime/src/lane_tools.rs`: session-derived, restricted
  model-facing jobs and services. The CLI remains a trusted operator interface.
- `extensions/native/` and `extensions/unreal/`: toolchain-specific policy,
  workflows, resource keys, budgets and hooks; no second lane scheduler.

Separate host, project and worktree keys permit unrelated work to proceed
concurrently. Project/Worktree paths must be existing absolute canonical
paths; aliases cannot create another lock identity. Multi-key grants are
atomic. Earlier eligible conflicting requests retain FIFO precedence; a
blocked unrelated key need not hold otherwise runnable work back.

## Authority and safety

The lane journal records transitions; held kernel locks and verified owned
scopes establish process ownership. Linux production execution uses systemd
user scopes/cgroups. Degraded process-group execution is an explicit test
mode, not equivalent recovery protection. Unknown ownership quarantines
resources; never recover by process-name matching or deleting lock files.

An exclusive job enters `Preparing`, blocks conflicting new starts, waits
for foreign client leases, and yields all bound services before grant. The
requester's own participant **and** session lease does not block. Foreign
clients wait until release, expiry or grace expiration (five minutes by
default; zero means indefinite). Grace expiration allows preemption, not a
claim of safe client drain. Backend descendants must be gone before grant.
Post-hook failure can quarantine resources without rewriting workload exit
status; service resume is tracked until Healthy or a reasoned failure.

Model-facing exclusive jobs remain disabled even though the CLI implements
the foreign-client handshake. Model service control is restricted to status,
lease, release and audited reads; raw editor MCP mutation and restart are not
exposed. A lease alone does not make an unfenced backend safe. Full Access or
approval remains the actual tool boundary, not a same-user shell sandbox.

RAM/disk reservations are estimates, not filesystem quotas. Per-agent dispatch
fairness/caps are not enforced merely because workspace budget helpers exist.
Binary asset checkout authority remains with the project's VCS lock system;
a local execution lease does not replace it. Worktree GC needs owner-exit
proof and exact interactive confirmation; a freeze is cooperative and does
not automatically acquire an exclusive execution lease.

## Usage and migration

Submit immediately, then use `lane_job` with `watch=true` or a command watcher
on `borg lane job wait ID`; do not build agent sleep/poll loops. Session and
workspace journals remain the conversation and shared-work authorities.

See [lanes](lanes.md), [interfaces](interfaces.md), [services](services.md),
[workspace hygiene](workspace-hygiene.md), [native adapter](native-adapter.md),
[regression gates](benchmark.md), and [Unreal migration](unreal-migration-parity.md).
Fake services and synthetic jobs establish contracts, not real engine parity
or a measured throughput advantage. Migrate project launch paths only after
private-tree validation and owner-approved cutover; do not mix independent
legacy and Borg queue/lock authorities in one active project tree.
