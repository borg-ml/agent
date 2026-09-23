# Shared service supervisor (v0)

Implementation: `crates/borg-lanes/src/services.rs`; CLI: `borg lane service`.
Engine-neutral: the adapter supplies argv/env/cwd, resource keys, backend-port
placeholder `{port}`, bounded health/stop/restore/idle hooks and restart policy.
Do not put Unreal-specific launch arguments in Borg core.

## Definition and commands

Minimal fake HTTP server definition (all fields other than the optional fields
below are currently required by the v0 Rust wire contract):

```json
{
  "id":"my-service", "argv":["/absolute/server","--port","{port}"],
  "cwd":"/absolute/project", "env":[],
  "resources":[{"key":{"scope":"Host","name":"my-project"},"access":"Exclusive"}],
  "memory_max_bytes":1073741824,
  "admission":{"min_available_ram_bytes":0,"reserve_ram_bytes":0,
    "min_free_disk_bytes":0,"reserve_disk_bytes":0,"disk_path":"/absolute/project"},
  "health":{"kind":"http","argv":["/health"],"interval_ms":1000,"timeout_ms":3000},
  "restart":{"max_restarts":5,"backoff_ms":500,"debounce_ms":3000},
  "endpoint":{"listen":"127.0.0.1:8231","backend_ports":[8241,8242]},
  "restore":null,
  "client_mode":"Exclusive"
}
```

`client_mode` is optional: omitted or `"Exclusive"` retains one-owner-at-a-time
editor behavior. For independent clients, opt in with
`"client_mode":{"Shared":{"max_clients":2}}` (positive limit). Each distinct
owner/session gets its own lease UUID and TTL; the same owner/session renews
its existing lease. Third distinct owners are refused at the limit; release
requires the matching owner and UUID. Expiry and release restore only that
client. Yield restores **all** clients before stopping the backend or releasing
the shared lane lease; a failed callback refuses yield or stop and retains
unrestored clients for recovery. RAM/disk admission remains **per running
service**, not per client. Shared mode does not grant raw backend MCP mutation
rights: the proxy is still deny-by-default without a validated owner/fencing
adapter. A trusted Postgres adapter may use its own per-owner database/socket
access control; CLI `--owner` alone is not authentication.
Under the v0.1 foreign-client handshake ([lanes.md](lanes.md)), every shared
owner other than an exclusive job's holder is a foreign client: the job holds in
`Preparing`, with every client and the backend live, until those leases are
released, expire or the grace ends, and new shared owners are refused meanwhile.

If a client restore callback fails, `service yield` and `service stop` refuse to
release the backend/lane lease; status retains each unrestored client and its
`reason` names both the stuck owner UUID and lease UUID. An exclusive job that
cannot yield records the same IDs in its failed journal `evidence` (inspect
`borg lane resource status --json`); it does not run its command. Repair the
callback and retry that client's owner-scoped `service release ID --owner
OWNER --lease-id UUID`, or retry `service stop` to restore every remaining
client. A failed exclusive ticket must be resubmitted after the callback is
repaired; do not wipe journal/state or force grant. If the supervisor died,
`service start ID --definition FILE` first retries persisted client restores
before it may launch a new backend; a further callback failure keeps startup
failed and the remaining clients recorded for manual repair.

`health.kind` is `command`, `http` (argv[0] is a path), or `mcp_initialize`
(argv[0] is `/mcp`; POST initialize). An `mcp_initialize` probe reads the
complete reply (Content-Length, chunked, or until EOF; 16 KiB cap) and judges
it by Content-Type:
- `application/json` is one JSON value, however it is formatted;
- `text/event-stream` is split into events, and each event's `data:` lines
  are joined before parsing;
- otherwise the probe tries whole-body JSON, then events.

A JSON-RPC `error` or a non-2xx status is unhealthy, and `result.protocolVersion`
is healthy. Every probe opens a fresh MCP session. When the reply names an
`Mcp-Session-Id`, the supervisor sends a best-effort `DELETE` for it (detached,
2 s bound, failures ignored), so repeated probes do not accumulate editor
sessions. The session is not reused, because reuse would not survive a
backend restart. The health timeout is per attempt;
`readiness_timeout_ms` bounds startup. Optional `graceful_stop`, `restore`,
`idle`, and `active` are `Hook {argv:[...],timeout_ms:number}`; restore argv
receives `{owner}` (the lease holder participant UUID). `idle_after_ms`
controls the idle hook. `adapter_enforces_leases` defaults to false. Memory
cap covers the transient systemd user unit. The service's `admission`
reservation is evaluated with active job and service RAM/per-filesystem disk
reservations under the same lane journal lock before backend spawn. A denied
service stays unavailable with the lane's RAM/disk reason and retries; yield
and stop release the reservation. `MemoryMax` is a separate per-unit ceiling,
not a substitute for the host admission check. Without systemd, start fails
closed rather than launching an orphanable process tree. The unit uses
`KillMode=control-group` and `Delegate=yes`; each backend generation runs in
its own delegated subgroup. Stop kills that subgroup and verifies
`cgroup.events: populated 0` before yield or lane-lease release. Only
in-process tests use an unscoped backend. The backend receives
`BORG_SERVICE_BACKEND_PORT`; CLI start is single-instance per ID via a stable
flock, and the supervisor outlives the starting session.

```
borg lane service start my-service --definition service.json --json
borg lane service status my-service --json
borg lane service lease my-service --owner TASK --ttl-seconds 600 --purpose capture --json
borg lane service release my-service --owner TASK --lease-id UUID --json
borg lane service restart my-service --reason rebuild --json
borg lane service yield my-service --by JOB --for-seconds 7200 --json
borg lane service resume my-service --by JOB --json
borg lane service logs my-service --lines 60 --json
borg lane service stop my-service --json
```

`--state-dir DIR` belongs to the enclosing `borg lane` command. The service
state lives at `DIR/services/ID` (default `$XDG_RUNTIME_DIR/borg/lanes/services/ID`),
with a private Unix control socket, state/spec JSON, a stable lock inode, and
output log. `BORG_LANE_DIR` overrides the lane root; `BORG_LANES_ROOT` is a
compatibility alias. State and control socket are owner-only. A client lease
holds owner/purpose/expiry; release requires its UUID and matching owner; the
restore callback must succeed before the lease disappears. Expiry/crash also
invoke restore. Multiple yield owners are independent; only the matching job
can resume its window, and the backend must stop before `yield` returns.
Requests use a Unix socket; agents never poll a file to issue a command.

The stable loopback proxy survives backend restarts. During a normal restart
it starts B while A serves, switches to B only after B passes health, then
stops A. A failed B leaves A serving. When no backend is ready, HTTP returns
503 JSON with state/reason/Retry-After. Backends alternate ports to avoid
TIME_WAIT collisions. Crash/hang recovery has bounded exponential backoff.

## Safety and integration boundaries

The generic proxy defaults to **403 on every backend request**, including
arbitrary GET and MCP initialize POST: a GET path or query can also mutate.
`read_only_paths` optionally lists exact, audited no-query GET/HEAD paths;
only these paths may be forwarded unfenced, one request per connection, with no
pipelined or later client writes. The Unreal adapter does not opt in. Merely
having a client lease does not authorize a raw MCP mutation. An adapter
may set `adapter_enforces_leases=true` only when it actually validates owner and monotone lease generation for **every** mutating
upstream call and rejects stale sessions after expiry, yield and restart.
Without that adapter, the endpoint is closed except audited status paths; do
not advertise editor MCP mutations as working. CLI `--owner` is a local operator claim, not model-facing
authorization. Service `start` accepts arbitrary argv from a local file and
must never be exposed as a model tool without a pre-registered validated spec.

The integrated model-facing `lane_service` wrapper accepts only
`status`, `lease`, `release` and audited exact-path `read` for a
pre-registered service. Its trusted dispatcher derives the holder from the
calling actor session, refuses caller-supplied owner/fence/approval fields,
and checks the session on lease release; the read tool requires a current
lease, uses only loopback GET without redirects and caps streamed responses.
Service start/stop/yield/resume, forced and ordinary restart and arbitrary
raw editor MCP mutations are **not** model-facing: restart lacks an atomic
actor fence in the supervisor, and no raw editor MCP proxy guarantees
per-owner enforcement on every mutating call. `borg lane service` remains
a host-local CLI for operators and approved Blu workflows. The MCP bridge
rejects all model-facing exclusive job templates until atomic foreign-client
preemption is proved; trusted CLI `borg lane job submit --spec <file>`
requires coordinating with the editor lease holder. This is not a same-UID
security sandbox.


**Exclusive lane gate (fake services verified):** the supervisor holds a
`LaneStore` shared lease during the backend lifetime. A lane job requesting
that resource enters `Preparing`, discovers and synchronously yields every
bound service, verifies they released their backend scopes and leases, then
grants exclusivity under the same lane lock. New/restarted services cannot
launch while that exclusive lease remains `Preparing` or `Granted`, even after
TTL expiry; post-release they resume. This was verified with two fake services,
not a live Unreal editor.

## Evidence (fake HTTP backend, not Unreal)

`CARGO_BUILD_JOBS=6 nice -n 10 cargo test -p borg-lanes services::tests -- --nocapture`:
15 passed, including independent multi-owner leases, bounded shared capacity,
per-owner TTL/release/restore, two-owner yield and failed-restore fencing,
two-service atomic RAM/disk reservation denial and
yield-to-admit handoff, crash/hang restart, A/B debounced restart, active-client
restoration before yield, expired-yield and forced-restart denial under a
`Granted` exclusive lane lease, default-deny proxy and pipelining guards,
wrong-owner resume, idle/active hooks, and readiness checks. Warm A/B restart
sampled the proxy continuously: **0 unavailable requests, ~310–410 ms** on
the fake backend (not an Unreal timing).

Production fake HTTP CLI smoke (local systemd user unit, not Unreal): start
Healthy, an active client lease and detached `setsid sleep` child inside the
delegated backend cgroup, yield returned `Yielded` with no backend/client and
its child gone, proxy returned JSON 503, and resume became Healthy on a new
PID. User-unit `MemoryCurrent` was 20.7 MiB with backend and 9.8 MiB after
yield (one fake HTTP service; not a steady-state Unreal footprint).

Crash-subtree production smoke: fake `/childexit` spawned a detached `setsid`
child in the backend subgroup and exited its leader. Recovery verified the
child gone, restarted on the alternate port (`restarts=1`), then the owned
fixture was stopped; its unit was inactive and subgroup removed.

Historical no-hook two-service scoped gate (external probe, pre-alias-fix CLI SHA256
`4022be198a99c3e8f8cac3069387f222bd2cb4e1bfdfb7a4135e8048c19a34b2`):
the test exited 0 on a real systemd-user job; both backend PIDs, detached descendants and delegated
subgroups were gone before exclusive grant, both proxies returned 503, a
client was restored, restart attempts remained fenced, and both services
auto-resumed on release. See `/tmp/gd-two-service-scoped.log`. No real Unreal
editor, engine-specific mutating MCP or model-facing registration was tested.

An owned-unit supervisor-crash smoke SIGKILLed only the test supervisor main
process; `KillMode=control-group` killed its detached backend child and the
unit became inactive. The lack of a safe non-systemd process-tree scope still
prevents a standalone `setsid` production fallback.

Production two-service disk capacity smoke on pre-alias-fix CLI SHA256
`73aed7d927988e189b245d631aa7f67830094c6b41e1f2975da29bb190c88e59`:
service A became Healthy with 10.8 GB reserved disk; separate service B on
the same filesystem stayed Degraded with no backend and the exact "disk
admission queued" reason. Yielding A admitted B to Healthy; both owned
systemd-user units stopped inactive/dead with empty control groups. See
`/tmp/gd-real-capacity-probe.log` (synthetic HTTP, not Unreal).

Pre-readiness-fix canonical-key public-CLI gate (integrated lane+service binary SHA256
`011c4ba94835500d40910d3dab6096533d245a55f0ea7f72c1112b1450e5e99d`):
Project `..` and symlink aliases, and Worktree `..` and symlink aliases, were
rejected before admission. A canonical Project same-key exclusive job then
yielded its active service before grant, restored its client, fenced restart
and returned 503 until auto-resume on release. On the same binary, two
independent services on disjoint Host keys each requested ~60% of available
RAM: the second had no backend with an explicit "RAM admission queued" reason,
then became Healthy after the first yielded. These are fake-service CLI probes,
not a live Unreal editor or authenticated mutating MCP adapter. The earlier
pre-alias-fix scoped and disk-budget smokes remain useful evidence for those
individual behaviors but do not establish canonical Project-key safety by
themselves.

Post-resume-readiness public-CLI gates on freshly rebuilt binary SHA256
`61ece6c173170b080933c821b81658a3d8ad422b1a5601d9550f8a30c4d7d4ac`:
- Real systemd-user two-service no-hook exclusive job: both detached backend
  descendants and delegated subgroups empty before grant, both proxies 503,
  client restore/restart fenced, both backends healthy after release
  (`/tmp/gd-two-service-scoped-readiness.log`).
- Distinct Host keys and disk paths on the same filesystem each reserved
  ~60% of free space: B had no backend with "disk admission queued" reason,
  then became Healthy when A yielded; both owned units stopped inactive/dead
  (`/tmp/gd-real-capacity-probe-readiness.log`, isolated probe
  `/tmp/gd-real-capacity-probe.py`).
- Project path aliases rejected before grant, canonical same-key handoff
  retained (`/tmp/gd-alias-readiness.log`).
- Stopped-supervisor resume kept `resume_pending` and journalled an error
  until recovery (`/tmp/gd-failed-resume-readiness.log`). More importantly,
  an ACKed Resume followed by failed backend health left `resume_pending`
  and a readiness error, with the yield token removed; after health returned,
  recovery cleared pending without a second Resume
  (`/tmp/gd-ack-unhealthy-readiness2.log`).

These are fake HTTP services and pinned local binaries, not a live Unreal editor.
The earlier SHA `011c4ba9…` gates do not establish the newer resume-readiness
semantics.

Shared-client real CLI gate on synthetic HTTP with the systemd-user manager:
`python3 scripts/gamedev_shared_service_probe.py --borg PATH` passed against
newly built `gamedev/services-shared` CLI SHA256
`7904ca9704209d4ea7424d29ebdc6e66332cc4ae9d8529dc9de87bbe78834939`.
Two independent owners held distinct IDs; the third was denied at
`max_clients=2`; releasing A restored only A, leaving B healthy. After A
reacquired, a no-hook exclusive Host-key job observed both remaining clients
restored, old backend PID gone, and proxy 503 **before** grant. After release
the service resumed Healthy with a new backend PID. The owned test unit
stopped inactive/dead with empty control group; see
`/tmp/gd-shared-client-cli.log`. With the optional
`--restore-failure-recovery` probe flag, an injected second-owner callback
failure kept Stop/Yield fenced, and after repair a matching owner/lease-ID
release cleared the client before Stop (`/tmp/gd-shared-recovery-cli.log`).
The CLI is a trusted local operator interface, not session-derived owner
authentication or real Unreal/Postgres mutation fencing.
