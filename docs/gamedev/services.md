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
  "cwd":"/absolute/project", "env":[], "resources":[],
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
cap covers the transient systemd user unit; no systemd means start fails closed
rather than launching an orphanable process tree. The backend receives
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
output log. `BORG_LANES_ROOT` overrides the lane root, `BORG_LANE_DIR` is a
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

The generic proxy returns **403 on POST/PUT/PATCH/DELETE** by default, including
MCP initialize: merely having a client lease does not authorize a raw MCP
mutation. An adapter may set `adapter_enforces_leases=true` only when it
actually validates owner and monotone lease generation for **every** mutating
upstream call and rejects stale sessions after expiry, yield and restart.
Without that adapter, the endpoint is read-only; do not advertise editor MCP
mutations as working. CLI `--owner` is a local operator claim, not model-facing
authorization. Service `start` accepts arbitrary argv from a local file and
must never be exposed as a model tool without a pre-registered validated spec.

The planned model-facing `lane_service` wrapper accepts only
`{op:"status|lease|release|restart|yield|resume|stop", id, ttl_ms?}` for a
pre-registered service: its trusted dispatcher derives the owner from the
actual session, checks workspace access and lease generation, and never accepts
caller-supplied owner, spec, argv, or `confirmed`. Until the parent integrator
reviews and wires those checks, **there is no registered MCP tool**. `borg lane
service` remains a host-local CLI for operators and approved Blu workflows.

**Exclusive lane gate is not yet proven:** the service-local yield stops its
backend but does not itself reserve/release a `LaneStore` shared resource.
The lane job must enter a pre-admission `Preparing` barrier, call yield before
it becomes `Granted`, and resume only after it releases exclusivity. A
post-grant pre-hook is not sufficient. Same canonical resource key and lane
root are required. No editor/exclusive mutual-exclusion claim until a real
lane-lease test passes. The lack of a safe non-systemd scope equivalent also
means the requested `setsid` fallback is deferred, not silently unsafe.

## Evidence (fake HTTP backend, not Unreal)

`CARGO_BUILD_JOBS=6 nice -n 10 cargo test -p borg-lanes services::tests -- --nocapture`:
4 passed: crash and hang restart; back-to-back restart debounce; owner-scoped
restore on lease expiry/release and refusal to yield under an active lease;
yield 503, wrong-owner resume denial, idle/active hooks, and private socket/
state permissions. Warm A/B restart sampled the proxy continuously:
**0 unavailable requests, ~310–330 ms elapsed** on this fake backend.
These numbers do not predict Unreal editor startup or memory usage. A scoped
CLI end-to-end test and `MemoryCurrent` measurement are pending integration.
