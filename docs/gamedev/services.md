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
  "restore":null
}
```

`health.kind` is `command`, `http` (argv[0] is a path), or `mcp_initialize`
(argv[0] is `/mcp`; POST initialize). The health timeout is per attempt;
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

The planned model-facing `lane_service` wrapper accepts only
`{op:"status|lease|release|restart|yield|resume|stop", id, ttl_ms?}` for a
pre-registered service: its trusted dispatcher derives the owner from the
actual session, checks workspace access and lease generation, and never accepts
caller-supplied owner, spec, argv, or `confirmed`. Until the parent integrator
reviews and wires those checks, **there is no registered MCP tool**. `borg lane
service` remains a host-local CLI for operators and approved Blu workflows.

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
10 passed, including two-service atomic RAM/disk reservation denial and
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

Independent no-hook two-service scoped gate (bench script, SHA256
`4022be198a99c3e8f8cac3069387f222bd2cb4e1bfdfb7a4135e8048c19a34b2`):
`python3 scripts/gamedev_service_probe.py --borg PATH --atomic-descendant`
exited 0 on a real
systemd-user job; both backend PIDs, detached descendants and delegated
subgroups were gone before exclusive grant, both proxies returned 503, a
client was restored, restart attempts remained fenced, and both services
auto-resumed on release. See `/tmp/gd-two-service-scoped.log`. No real Unreal
editor, engine-specific mutating MCP or model-facing registration was tested.

An owned-unit supervisor-crash smoke SIGKILLed only the test supervisor main
process; `KillMode=control-group` killed its detached backend child and the
unit became inactive. The lack of a safe non-systemd process-tree scope still
prevents a standalone `setsid` production fallback.
