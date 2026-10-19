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

### Restart, health and memory options

All optional and serde-defaulted, so an existing definition keeps its behavior:

- `restart.mode`: `"warm"` (default; the A/B restart above) or `"cold"`. Cold
  stops the running backend first (`graceful_stop`, then its cgroup is killed
  and verified empty), the front answers 503 with state `Restarting`, and only
  then launches the replacement on the next port. For backends too large to
  run twice. The service keeps its lane lease throughout.
- `restart.transient_exit_codes`: `[int]`. An active or candidate backend
  exiting with one of these is relaunched after `backoff_ms` without counting
  toward `max_restarts`; `restarts` still counts it.
- `restart.defer_while`: `[ResourceKey]`, for example a build of the project
  tree. Before a backend launch (a warm or cold restart, a relaunch after a
  crash or failed replacement, or the first start) the supervisor takes a
  *restart barrier*: an exclusive claim on all these keys, less the service's
  own resources, in the same lane transaction that admits builds. While a
  ticket holds, is queued for or quarantines one of them the launch waits,
  and the status reason names the key and ticket (`restart deferred: build
  (host) held by ticket <id>`, `queued for`, `quarantined by`; `launch
  deferred: …` for a relaunch). A refused attempt journals nothing, so queued
  builds keep their turn. From before a cold restart stops the old backend,
  or a warm one starts the replacement, until the new backend is healthy
  (warm: and the old one is stopped) or has failed, no ticket needing one of
  the keys is admitted, not even a job that could make the service yield.
  Keys must be unique.
- **A killed supervisor** (SIGKILL, OOM, `systemctl --user stop`) leaves its
  service lease and any restart barrier behind. They are released, with the
  proof in their evidence and `recovery.jsonl`, as soon as the owner is
  provably gone: its service unit cgroup (the supervisor and its backends)
  is gone or empty, or, for a claim recorded without one, its pid has exited
  or now names another process. `lane recover` does this, as do the next
  service lease or barrier claim (so the next `service start`) and every
  build waiting for a key. Until then `lane recover` quarantines the keys,
  and it lifts that quarantine once the proof holds; no reboot is needed.
- `health.unhealthy_after_ms`: a running backend is declared hung only after
  its probes have failed continuously this long (default three
  `timeout_ms`). Each probe is still cut off at `timeout_ms`.
- `memory_swap_max_bytes`: `MemorySwapMax=` on the service unit, beside
  `MemoryMax`.

A `service` request (`stop`, `yield`, `restart`, also the yield a lane job
asks for) waits for the supervisor up to max(120 s, 3 × (`graceful_stop`
timeout + 10 s) + `restore` timeout × max clients + 30 s), so a slow graceful
stop does not time out the caller.

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
rejects all model-facing exclusive job templates even with the implemented
foreign-client handshake; trusted CLI `borg lane job submit --spec <file>`
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

## Verification reference

See [regression gates](benchmark.md) and
`crates/borg-cli/tests/lane_services.rs`. Synthetic backends exercise protocol
and lifecycle contracts, not live Unreal editor parity. Cross-journal host RAM
admission is implemented in `crates/borg-lanes/src/host_admission.rs` alongside
the local journal admission checks.
