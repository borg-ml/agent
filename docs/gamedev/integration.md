# Game-development branch integration review

Status: **rolling review, not a green merge approval** (updated 2026-09-23).
This file is an integration map for the parent. Implementer work is still in
progress; recheck commit hashes, diff, tests and dependencies immediately
before merging. Do not merge or change `~/agent` while its live session has
uncommitted edits. No branch here has been pushed.

## Base and merge order

`gamedev/design` started at `cua/integrated` (`3e74f31`) and was rebased
**only in its own worktree** onto the parent-finalized
`borg/agent-productivity` (`4e01e0e`), without conflict. The original
`ede51c5` interface and subsequent design commits have rewritten hashes;
do not merge the old base a second time. Lane owner rebases next on final
design, services then on the stabilized lane API, and the workspace bridge
resolves its CLI entry changes with lane owner rather than independently
merging them. Bench/adapters follow the integrated CLI; research is independent.
Parent integration order:

1. `gamedev/design`: shared crate, workspace manifests and interface/design
   docs. Rebased validation: offline `cargo check -p borg-lanes`, clippy
   `--all-targets -- -D warnings`, tests (0 skeleton tests), fmt and targeted/
   workspace rust-analyzer diagnostics (173 files, zero diagnostics) passed.
2. `gamedev/lanes`: atomic resource scheduler, job supervisor and *sole*
   owner of `borg-cli/src/{cli.rs,main.rs}` lane entry. Merge first among
   implementers because adapters/bench need stable `borg lane job` JSON API.
3. `gamedev/services`: only its `services.rs`, tests and isolated service CLI
   module; lane owner has already wired the service subcommands at checkpoint
   `9a1ed00`. Rebase services after lanes stabilizes D11; require same
   resource admission authority and stale-client fencing.
4. `gamedev/workspace`: worktree inventory, budgets, freeze and isolated
   workspace CLI module. Helper now lives in `borg-lanes/src/workspace/`;
   integrate its optional CLI/MCP bridge only after coordinating the lanes-
   owned entrypoint. Public `WorkspaceCoordinator` remains a skeleton.
5. `gamedev/unreal` and `gamedev/native`: distinct Blu packages. Ensure they
   use the real CLI schema, not speculative `[api.lanes]`, and keep Abundance
   shared tree read-only. Adapters may merge independently after lanes CLI.
6. `gamedev/bench`: Python simulation and, if available, real CLI contention
   replay. Its simulated timings are model output, not observed performance.
7. `gamedev/research`: sourced landscape doc, linked from design. Independent
   documentation; cherry-pick after author commits. Its broad competitor
   claims must retain source/verification caveats.

If lanes CLI fails to make the integration cut, merge only design plus
self-contained module implementations; mark both adapters and benchmark
integration as unverified rather than presenting prototype shims as Borg's
working first-party CLI/MCP. **That is a design-only preview, not v0 release:**
parent requires D11 atomic editor/exclusive handoff and real-CLI proof.

## Branch evidence at first review

| Branch | Observed checkpoint | Contract/API consistency and evidence | Remaining review |
| --- | --- | --- | --- |
| `gamedev/design` | rebased on final `4e01e0e` | Offline skeleton check, clippy `--all-targets -D warnings`, fmt/tests and Rust LSP (173 files, zero diagnostics) passed; public types only, no scheduler. | Conditional review remains; no merge approval until D11 cross-module proof. |
| `gamedev/lanes` | `dee1b25` scheduler/CLI auto-yield checkpoint (through `e2c92f8`) | Independently ran `cargo test -p borg-lanes lanes::tests --offline`: 6 passed (0.34 s), plus `python3 crates/borg-cli/tests/test_lane_process.py target/debug/borg`: 5 passed (2.084 s), including isolated killed-supervisor systemd scope recovery. Shared service lease, Preparing and same-lock grant recheck added. | Automatic all-active-service discovery/yield now implemented from shared service leases, but post-hook/resume ordering, resume failure observability and real-CLI two-service no-hook regression remain unproved. Do not merge as v0 handoff yet. |
| `gamedev/services` | `1c83671` atop lanes `e2c92f8` | Independently reran `CARGO_BUILD_JOBS=6 nice -n 10 cargo test -p borg-lanes services::tests --offline`: 8 passed again (1.56 s after compile); unit cfg bypasses production delegated cgroup path, including active-lease restoration, unfenced proxy default-deny and exclusive-lease restart refusal; service holds a shared lane lease during backend lifetime and scopes each backend generation under delegated supervisor unit; real cgroup cleanup not yet independently exercised. Earlier A/B probe unavailable 0 / 327 ms. MCP initialize validation, active-client restore on yield and default-deny unfenced proxy (audited allowlist pending), UnixStream control, `KillMode=control-group`, no-systemd fail-closed, raw proxy mutations denied by default. | verify supervisor-crash cgroup cleanup, every bound service discovers same lane gate and blocks startup/resume for whole exclusive lease, CLI wiring and real-CLI regression, owner/fence enforcement. |
| `gamedev/workspace` | `c4d55aa` workspace checkpoint (through `75a882a`) | Independently ran `CARGO_BUILD_JOBS=6 nice -n 10 cargo test -p borg-lanes workspace:: --offline`: 5 passed (0.08 s; 1m20s compilation wait), including dirty/live GC exclusion and ack/clean-path freeze; owner reports clippy/fmt. Moved inventory/budget/GC/freeze into `borg-lanes::workspace::hygiene`, Borg GC dry-run 14 listed/0 eligible (3.482 s); Abundance 10/0 (2.742 s); target-status two over 24 GiB, no cleanup. Freeze checks acks/clean trees but does not acquire project lane gate: advisory only (MCP validates claim; core helper/CLI require caller coordination). Trait `WorkspaceCoordinator` still skeleton; helper owns separate richer record, budget API not wired into lane dispatch. Bridge GC now static-verified dry-run-only, no model apply/confirmed fields. CLI apply requires TTY per-path confirmation and journal owner-exit recheck. Freeze now checks work_id claim; still advisory without project gate. | align public trait/record or document partial implementation; bridge CLI integration still conflicts with lanes-owned entry; wire one admission authority and enforce freeze lane gate before claiming lock. |
| `gamedev/unreal` | `fefd530` thin `extensions/unreal/` Blu adapter | Independently ran `python3 -m unittest discover -s extensions/unreal/tests -v`: 5 passed (0.948 s); stable revision/policy paths permit pending coalescing, including fake UBT/symbols tools and exclusive fail-closed. Copied scheduler/editor supervisor removed; build emits core JobSpec, exclusive run/raw MCP blocked; owner reports isolated fake Borg CLI job success in an actual systemd user scope after explicitly supplying test DBus environment (6/6 with binary); still no real Unreal runtime. | validate real CLI/spec and service entrypoint integration; no real Unreal runtime or editor parity claim. |
| `gamedev/native` | `8053c22` native Blu package (through `a293103`) | Independently ran `python3 -m unittest discover -s extensions/native/tests -v`: 10 passed (0.010 s), including nonzero response preservation. Terminal-unknown fixture keeps DB+lease, untracked source disables coalescing; owner reports real lane CMake 1/1, cargo runtime 877 passing after filtering two known failing upstream cases; service CLI still unwired. Script fails closed if CLI absent. | align exact submit JSON/async watcher semantics; independently reproduce install doctor and smoke when CLI exists; GC `--apply` needs human confirmation. |
| `gamedev/bench` | `febf229` | Pending-only coalescing is v0 default (running-only hypothetical opt-in). Independently ran `python3 -m unittest -q scripts/test_gamedev_benchmark.py`: 5 passed (0.003 s). Modeled 6×8 naive 177.7 s/FIFO 174.9 s/Borg 57.4 s (46 launches/2 pending joins); hypothetical running joins 56.4 s. Not observed throughput. Pre-fix public-CLI replay on lanes `37c5ee8` observed 3×3: 2.831 s (7 unique launches/2 joins), 6×8: 8.635 s (44/4), no conflicts/OOM; low-disk refusal exit 125. Pre-fix joins cannot be cited as v0 numbers; fake processes, not Unreal or scope recovery. Second 6×8 observed 13.081 s, status p95 20.91 ms. | two-service no-hook D11 probe correctly FAILS on old `9a1ed00` binary (queue timeout 125; both services retained until test cleanup), so rerun on post-auto-yield scoped integrated CLI with backend-scope/descendant proof; refresh contention replay after safe coalescing fix. |
| `gamedev/research` | `17e9296` sourced `docs/gamedev/landscape.md` | Survey read: Epic native UE 5.8 MCP/Horde/Zen, Unity CLI switch, VCS asset locks; thesis and caveats incorporated into design. | Preserve citation/verification qualifiers on merge. |

## Expected conflicts and resolution

- Every branch based on design may contain the same `ede51c5`; rebase on the
  final design branch once, not cherry-pick duplicate interface commits.
  `Cargo.lock` is owned by design for early shared deps, but CLI branch adds a
  `borg-lanes` dependency: regenerate lockfile with Cargo after resolving
  `Cargo.toml`, then run check. Do not copy a stale lockfile over changes.
- `cli.rs`/`main.rs` must have **one** owner (lanes). Service/workspace branches
  export isolated modules and request entry wiring by lanes owner. Reapply
  small match arms rather than taking either whole version if base's CLI moved.
- Workspace branch has uncommitted edits to `cli.rs`/`main.rs`, `agent_mcp.rs`,
  `subagents.rs` as well as its module. Owner asked to split core/hygiene from
  optional CLI/MCP wiring; lanes owner integrates CLI entry, and runtime tool
  registration must be permission-reviewed separately. Keep one public
  worktree record and one admission policy. Shared-work events still belong
  to Borg workspace journal.
- Extension manifests must not declare unknown `[api.lanes]` against Blu v1;
  use validated workflows, allowlisted MCP servers, skills, and planned
  adapter data until a schema version is implemented.
- `agents/wait-ergonomics` will be in productivity base; do not override its
  wait_agent/watch changes with older runtime files. Lanes use existing watch
  command process / goal yield and preserve explicit-stop precedence.

## Gaps / decisions for parent

1. First-party MCP tools and typed lane watchers are **specified**, not wired;
   an agent can initially use workflow-backed `borg lane job wait` through
   existing command watch. Decide whether MCP promotion is a launch gate.
2. Durable supervisor and kernel lock lifetime need hard crash/reclaim tests,
   not just a single-process queue test. Never recover by matching process
   names. Non-systemd hosts must fail safe until a supported equivalent exists.
3. Service proxy raw MCP calls need per-owner enforcement and fencing;
   A/B switch does not guarantee in-flight protocol requests survive.
4. Authoritative binary asset locks must come from P4/UVCS/Git LFS etc.; no
   adapter should claim a host lease resolves repository conflicts.
5. Worktree disk cap GC must be opt-in/destructive-action gated and avoid
   active, user-created or dirty trees. `/home` disk-full is a real acceptance
   failure even if queues get faster.
6. Unreal extension must not vendor a second long-lived queue/editor supervisor
   as its default: reusing Abundance scripts is a transitional migration
   recipe, not the core+Blu end state. Until core CLI/API parity exists, fail
   closed and report unsupported features explicitly.
7. Competitor market scan is research, not a performance benchmark or legal
   approval for hosted/shared editor licensing. Pilot and measure before a
   standalone control plane decision.

Draft CLI reminder: `--spec` accepts serialized `JobSpec`, not adapter/template
shorthand. A model-facing caller must not construct arbitrary process argv
without trusted adapter-template validation. Native/Unreal workers received
this CLI shape and should avoid synchronous model-tool waits.

Model-facing service MCP owner is **final parent integration**, not the
services branch: only status should be exposed until registered spec policy
and actual session-derived owner/fencing enforcement are audited. A tool
argument `confirmed: true` is not a durable human approval for workspace GC.

**Open fail-open regression (2026-09-23):** on the combined lane binary
`a26778e` with the older service stop path (before delegated backend subgroup
fix `6337cf5`), bench `--atomic-descendant` ran two services and a no-hook
exclusive job under a real user systemd manager. A detached backend child
(PID 1434445, verified by start ticks) survived yield, yet job `821d209b`
**started** and exited 1 only after the in-job assertion detected the child.
The test stopped its own services and cleaned only its verified fake child.
The newer service fix is *not* vindicated by this negative result: integrate
`6337cf5`/`430d2ee` and rerun the same scoped descendant test; absent a pass,
D11 blocks v0. Separate degraded two-service no-hook mechanics probe passed
independently (job `6abb6869`), but it cannot substitute for this scope proof.

**Critical v0 release gate (parent decision):** lanes enters `Preparing` for
exclusive project key R before grant; blocks new shared grants and synchronously
pre-yields **every** service bound to R until the owned backend scope/cgroup
is verified empty (leader PID exit alone is not enough), proxy returns 503,
and a fencing token acknowledges `Yielded`. Only then grant the job.
Every service start/restart/TTL auto-resume checks exclusive leases under the
**same kernel lock**, never a cached flag. Release runs post-hooks then service
resume; stale client tokens are rejected. Lanes owns Preparing/hooks/fencing;
services owns yield ack/start refusal/resume. Bench must add a real-CLI
cross-module test with an active client lease, prove backend gone before job
starts, no restart during, resume afterward. Until it passes, **do not ship
v0**; standalone fake probes and adapter fail-closed behavior are not parity.

Native adapter disk reservation is an estimate, **not** target-only quota/GC;
workspace currently considers whole-worktree cleanup with owner protection.
Process-group fallback is test/degraded mode only; if leader ownership is
unprovable, quarantine affected keys and never infer cleanup from a vanished
PID. Production Linux requires a scope/cgroup and crash tests.
