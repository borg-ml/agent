# Unreal Blu adapter (preview)

Project-agnostic Unreal policy templates for Borg's **host-local lane core**.
This package does not implement its own job queue, service supervisor, HTTP
proxy, or editor/run mutual exclusion. `adapter.json` is descriptive v0 data;
Blu v1 does **not** interpret it as a lanes manifest. Install explicitly with
`borg extensions install ./extensions/unreal --project` from the Borg worktree
(or use the user scope), after reviewing trusted code. Blu activation policy
may refuse project installs. The registered `unreal` workflow currently only
runs `discover` (Blu external workflows have no structured argv yet).

Use the direct CLI for templates (global options precede subcommand):

```sh
U=/path/to/borg/extensions/unreal/bin/unreal.py
python3 "$U" --project /path/to/Game.uproject --engine-root /path/to/UE discover
python3 "$U" --project /path/to/Game.uproject --engine-root /path/to/UE \
  --participant-id UUID --session-id UUID build --spec GameEditor Linux Development /path/to/Game.uproject
python3 "$U" --project /path/to/Game.uproject --engine-root /path/to/UE editor spec
python3 "$U" --project /path/to/Game.uproject --engine-root /path/to/UE \
  --participant-id UUID --session-id UUID run commandlet --spec -- /path/to/UnrealEditor-Cmd /path/to/Game.uproject -run=ResavePackages
```

If `--project` is omitted, discovery searches cwd's ancestors for exactly one
`.uproject`. Engine resolution accepts `--engine-root`, project configuration,
`UE_ENGINE_ROOT`, an absolute `EngineAssociation`, a sibling `UnrealEngine`, or
`/opt/unreal-engine`. Defaults are Linux, with preliminary macOS paths. No
machine-specific path or port 8231 is embedded. The CLI emits JSON conforming
to the integrating lane `JobSpec` / `ServiceSpec` (services health kind and
stop fields come from the services branch). It does not validate schemas via
Rust at runtime; actual submission requires an integrated Borg binary.

Example optional `.borg-unreal.toml` next to the `.uproject` (or `--config FILE`):

```toml
engine_root = "/path/to/UE"
[build]
memory_max_gb = 24
min_available_ram_gb = 8
reserve_ram_gb = 4
min_free_disk_gb = 20
reserve_disk_gb = 10
gb_per_action = 1.5
# ubt_start_lock = "/run/user/1000/abundance-build-lane/ubt-start.lock"
[run]
memory_max_gb = 16
[editor]
port = 8240
backend_ports = [8243, 8244]
memory_max_gb = 16
args = ["-nullrhi"]
```

`build [--spec] [--wait] [TARGET PLATFORM CONFIG [UPROJECT] [UBT flags...]]`
submits to `borg lane job submit --spec - --json` when core is available;
`--wait` uses its terminal wait. The core owns queue, worktree resource lease,
admission, memory/time limits, and coalescing of **pending** identical inputs.
The adapter calculates a source/toolchain fingerprint, a per-revision/policy UBT log (reset at job start),
RAM-derived `-MaxParallelActions`, and `-NoMutex`; a narrow UBT-start lock in
the Unreal-specific helper protects Trace.uba startup, not build scheduling.
Set `build.ubt_start_lock` in `.borg-unreal.toml` or export
`UE_UBT_START_LOCK` (environment wins) to the **same absolute lock file** used
by an existing host-wide build lane; for Abundance this is
`$XDG_RUNTIME_DIR/abundance-build-lane/ubt-start.lock` (expand `$XDG_RUNTIME_DIR`
in the shell/TOML writer). Sharing this one file serializes UBT startup
across Borg and the legacy pathway, but it does **not** merge their queues,
reservations or run locks. Do not mix active pathways in one project tree.
Each helper-launched UBT process (including a startup retry) also receives
its own private `TMPDIR` and `UBA_FILE_MAPPING_DIR`; they are removed after
that process exits. This isolates Borg jobs from concurrent Abundance-lane
UBA mappings without copying the Abundance queue into the adapter.
The helper also writes an adjacent per-revision `.metrics.json` before exiting:
UBT wall time, max RSS of any directly waited-for child (not aggregate), and
Linux scope `memory.peak` when available (includes cache, not pure RSS). A
missing peak is reported as missing, not inferred from a dead systemd unit.
When installed Linux symbol tools are present, UBT uses `-NoDumpSyms` and the
core post-hook regenerates symbols for changed libraries. The post-hook is not
an authority for job admission. Generated outputs and intermediates remain
private to each worktree; shared DDC is Unreal's native cache. Core admission
checks available RAM and output-filesystem disk; it does not evict other jobs
or safely recover unmanaged UBT processes.

`editor spec` emits a persistent service with loopback front/backend ports,
MCP-initialize health, bounded restart, and a project-run resource declaration.
The backend launcher records its own PID/start identity in a private, per-port
file and then `exec`s Unreal in that same process; the 90 s graceful-stop hook
ends PIE, sends `QUIT_EDITOR`, and waits (up to 85 s) for both the backend PID
to exit and its MCP port to close. The core still tears down the tracked
cgroup after the hook returns, so an unresponsive editor cannot run forever.
The PID file is not a lease and cannot authorize external stop actions.
`editor start|status|restart|yield|resume|stop|lease|release` forward to the
integrating `borg lane service` CLI. A disposable **fake** editor with an MCP
initialize endpoint reached Healthy through the exact combined lanes-owner
binary (SHA256 `4022be198a99…`) using this adapter's `editor start/status/stop`
entrypoints. Its unfenced front proxy denied POST with 403; the real graceful
hook sent `QUIT_EDITOR` to the fake backend and stop closed all private ports.
This does **not** validate a real Unreal editor. The architect's
`docs/gamedev/integration.md` reports a passing real-systemd two-service,
no-hook scoped descendant gate on that same binary, clearing the earlier
stale-binary fail-open. A later public CLI probe found a **Project
path-alias fail-open** on that old binary. A newly rebuilt combined binary,
SHA256 `011c4ba9…`, independently rejected aliased Project/Worktree keys and
passed a canonical Project service handoff; the final integrated binary must
repeat this gate. The adapter canonicalizes its project path and rejects
conflicting UBT project arguments. On separate pinned binaries, targeted
core probes also passed failing post-hook quarantine (`011c4ba9…`),
stopped-supervisor and ACK-then-unhealthy resume recovery plus same-device
disk capacity (`61ece6c1…`). **Foreign client leases do not atomically delay
exclusive preemption:** the selected v0 model MCP policy rejects all
model-facing exclusive templates and disables model-facing restart without an
atomic caller-lease check. Session-derived model MCP owner fencing,
final integrated-binary regression and real Unreal parity remain blockers.
These targeted passes do not enable non-spec exclusive runs.
Do not use the live shared project's editor or its port.

The stock
Unreal MCP backend lacks owner enforcement. The service spec explicitly sets
`adapter_enforces_leases=false`; `mcp ...` intentionally fails closed rather
than exposing raw backend access. Only enable client access once an adapter
proxy authenticates owner/generation on every mutating call and restores
owner-scoped PIE/cvars/camera/HUD state.

`run commandlet|import|verify|exclusive --spec -- COMMAND ARGS...` generates a
core exclusive job template. **Non-spec runs always fail closed:** the adapter
must not preempt foreign editor clients without an atomic lease policy,
owner coordination and real Unreal parity. Core fake-service handoff tests do
not resolve this. A plain job submission is not a safe substitute; model-facing
exclusive templates are disabled for v0. Never run an exclusive commandlet
against an active editor or infer safety from a JSON spec alone.

Validate without UE using `python3 -m unittest discover -s extensions/unreal/tests -v`.
Default tests use a fake engine and do not establish real Unreal build or
editor service readiness. An optional integration smoke uses only an isolated
fake project/engine and lane state: set `BORG_UNREAL_TEST_CLI` to an already-built
Borg binary with the lane job CLI, then run the same unittest command. By
default, the opt-in test sets `BORG_LANE_DEGRADED=1` and `BORG_LANE_SCOPE=0`
**only for the fake job**; do not use degraded/unscoped execution for production builds. To
exercise real scoped ownership too, set `BORG_UNREAL_TEST_SCOPED=1` plus
`XDG_RUNTIME_DIR=/run/user/$(id -u)` and
`DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/$(id -u)/bus` before running the
opt-in test. This host's systemd user manager works: the scoped fake build
finished with exit 0 and a `borg-lane-…scope` cgroup. An earlier exit 125 was
caused by setting XDG_RUNTIME_DIR to an isolated temp directory without a user
bus, not by a missing manager. This is still not real Unreal editor/build
validation. A second optional test sets `BORG_UNREAL_TEST_SERVICE_CLI` to a
Borg binary with `lane service` and requires the same working user bus. It
launches a fake MCP editor on disposable high loopback ports in isolated lane
state through `unreal.py editor start/status/stop`, checks Healthy and the
unfenced front proxy's HTTP 403 on POST, observes the default graceful hook's
`QUIT_EDITOR` request, then checks port cleanup. Ownership of a real
Unreal editor is not exercised by this test. The opt-in fake service test
also submits an adapter-generated exclusive JobSpec directly to Borg and checks
that its one fake backend is gone, proxy returns 503 during the job, and the
service auto-resumes. This **does not enable** non-spec adapter exclusive runs
or prove the two-service, failure-path or real-editor gates. Both opt-in
smokes passed on a private copy of debug binary SHA256 `61ece6c1…` (8/8 tests,
hash stable during execution). The lanes worktree had uncommitted source
changes at copy time, so this is adapter compatibility evidence, **not** a
final-source release or integrated-binary gate.
Consult `docs/gamedev/interfaces.md` for core contracts and rollout
prerequisites.

## Abundance visual-iteration pilot

Abundance's [private batch runner](../../../abundance/docs/VISUAL_ITERATION.md)
exercises the reusable-scene workflow through its existing `ab_build.sh` lane
adapter. It builds a linked worktree once, validates source/modules/fixtures,
keeps one private PIE scene, captures bounded pose/cvar combinations, and
records final evidence after cleanup. Torch settings apply live; scene placement
includes the pawn's streaming origin and aim. A Borg command watch can own the
attached start command and observe its bounded completion.

The plugin now dispatches configured project adapters directly, with literal
argv and an attached process owned by Borg's command/watch lifecycle:

```toml
# .borg-unreal.toml in the project
[visual]
adapter = "Scripts/visual_iteration.py"
[assets]
adapter = "Scripts/asset_closure.py"
```

Use `python3 /path/to/extensions/unreal/bin/unreal.py --project /path/to/Game.uproject
visual start|wait|status|batch|stop ...` or `assets snapshot|stage ...`.
The project adapter supplies fixture validation, scene readiness, camera/pawn
placement and lease restoration. Scripts must resolve inside that project.
Abundance ships both adapters and the configuration. Raw shared-editor MCP
restrictions still apply; this dispatch does not bypass them.

Borg now serializes RAM admission across independent lane journals on the host.
A native Cargo lane and an Unreal lane therefore share reservations even with
different state directories. Reservations cover future growth: private anonymous
and shared-memory residency is credited once, while reclaimable file cache and
unknown/overlapping cgroups receive no credit. The Abundance legacy bridge uses
the same host admission lock; it is a migration adapter, not another job queue.
All participating Borg processes must run the new core. Old binaries and direct
unwrapped compiler invocations cannot reserve through this protocol.

The default Unreal build reserves 12 GiB and caps compiler parallelism to fit
that estimate (including 2 GiB of overhead); the editor reserves 8 GiB. These
estimates are distinct from hard cgroup limits and should be calibrated from
measured peaks. Keep the existing launch floors and containment.
