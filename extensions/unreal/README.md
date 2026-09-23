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
When installed Linux symbol tools are present, UBT uses `-NoDumpSyms` and the
core post-hook regenerates symbols for changed libraries. The post-hook is not
an authority for job admission. Generated outputs and intermediates remain
private to each worktree; shared DDC is Unreal's native cache. Core admission
checks available RAM and output-filesystem disk; it does not evict other jobs
or safely recover unmanaged UBT processes.

`editor spec` emits a persistent service with loopback front/backend ports,
MCP-initialize health, bounded restart, and a project-run resource declaration.
`editor start|status|restart|yield|resume|stop|lease|release` forward to the
integrating `borg lane service` CLI. **No editor has been started with this
adapter**; do not use the live shared project's editor or its port. The stock
Unreal MCP backend lacks owner enforcement. The service spec explicitly sets
`adapter_enforces_leases=false`; `mcp ...` intentionally fails closed rather
than exposing raw backend access. Only enable client access once an adapter
proxy authenticates owner/generation on every mutating call and restores
owner-scoped PIE/cvars/camera/HUD state.

`run commandlet|import|verify|exclusive --spec -- COMMAND ARGS...` generates a
core exclusive job template. **Non-spec runs always fail closed:** a plain
job submission cannot atomically yield the service before admission. Core
service-yield + job admission integration and editor owner coordination are
required before executing these templates. Never run an exclusive commandlet
against an active editor or infer safety from a JSON spec alone.

Validate without UE using `python3 -m unittest discover -s extensions/unreal/tests -v`.
These tests use a fake engine and do not establish that an integrated Borg CLI,
real Unreal build, or editor service works. Consult `docs/gamedev/interfaces.md`
for core contracts and rollout prerequisites.
