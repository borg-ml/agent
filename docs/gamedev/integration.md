# Game-development branch integration review

Status: **rolling review, not a green merge approval** (updated 2026-09-23).
This file is an integration map for the parent. Implementer work is still in
progress; recheck commit hashes, diff, tests and dependencies immediately
before merging. Do not merge or change `~/agent` while its live session has
uncommitted edits. No branch here has been pushed.

## Base and merge order

`gamedev/design` started at `cua/integrated` (`3e74f31`). Design checkpoints:
`ede51c5` (interface) → `1a1a540` (architecture and module/CLI ownership),
plus subsequent review edits. Rebase design onto `borg/agent-productivity`
**when the parent announces it**; do not merge the old base a second time.
Every implementer rebases onto the final design ref, then parent integrates:

1. `gamedev/design`: shared crate, workspace manifests and interface/design
   docs. Parent should first confirm `cargo check -p borg-lanes`, fmt and
   clippy on the rebased base.
2. `gamedev/lanes`: atomic resource scheduler, job supervisor and *sole*
   owner of `borg-cli/src/{cli.rs,main.rs}` lane entry. Merge first among
   implementers because adapters/bench need stable `borg lane job` JSON API.
3. `gamedev/services`: only its `services.rs`, tests and isolated service CLI
   module; let lanes owner wire its subcommands into CLI entry after rebasing.
   Require same resource admission authority and stale-client fencing.
4. `gamedev/workspace`: worktree inventory, budgets, freeze and isolated
   workspace CLI module. Ensure its provisional runtime helper is moved into
   `borg-lanes/src/workspace/` rather than creating a duplicate API/authority.
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
working first-party CLI/MCP.

## Branch evidence at first review

| Branch | Observed checkpoint | Contract/API consistency and evidence | Remaining review |
| --- | --- | --- | --- |
| `gamedev/design` | `1a1a540` | skeleton `cargo check -p borg-lanes --offline` and `cargo clippy -p borg-lanes --offline -- -D warnings` passed; fmt and diff check clean. Public types only, no scheduler. | Market review edits and integration doc pending commit. |
| `gamedev/lanes` | Uncommitted scheduler/CLI draft on `1a1a540` | CLI shape published: `borg lane job submit --spec SPEC.json --json`, `wait ID --json`, `status [ID] --json`. Reviewer found unsafe no-systemd process-tree fallback, running coalesce lacks revision recheck, recovery lock-error ambiguity, and async post-hook unscoped; owner notified. | do not merge until safety fixes and real FIFO/coalesce/recovery/wait tests pass. |
| `gamedev/services` | `39807be` plus uncommitted repairs | Owner reports 3/3 fake HTTP tests and 0 failed requests / 329 ms A/B restart. UnixStream control, `KillMode=control-group`, no-systemd fail-closed, raw mutating proxy denied by default unless adapter enforces lease. | verify committed repair, supervisor-crash cgroup cleanup, clippy/CLI integration, owner/fence enforcement. |
| `gamedev/workspace` | `0ef939c` plus uncommitted move to `borg-lanes/src/workspace/hygiene.rs` | Owner reports 3/3 crate tests; freeze requires relevant acks and refuses dirty trees. CLI/MCP wiring being split. Review found MCP `confirmed: true` alone is not human deletion approval; owner asked to make model-facing GC dry-run-only. | inspect core/bridge commits, GC permissions and single worktree authority. |
| `gamedev/unreal` | Uncommitted `extensions/unreal/` draft | Engine discovery and tests exist, but copied `build_lane.py` (~1,883 lines), editor lane (~741 lines) and run guard duplicate core scheduling/supervision. Owner asked to pivot to thin policy/CLI adapter. | do not merge duplicated default authority; validate trust, CLI schema, editor owner-gating and fake-engine tests. |
| `gamedev/native` | `ccfdeab` native Blu package | Three planner tests: `python3 -m unittest discover -s extensions/native/tests -q` OK (0.001 s); owner reports isolated Abundance CMake targeted build/ctest, doctor active. Script fails closed if CLI absent. | align exact submit JSON/async watcher semantics; independently reproduce install doctor and smoke when CLI exists; GC `--apply` needs human confirmation. |
| `gamedev/bench` | `b0905bc` | Four deterministic simulator tests ran with `python3 -m unittest -q scripts/test_gamedev_benchmark.py`: OK (0.002 s). Owner reports modeled 6×8 naive 177.7 s/FIFO 174.9 s/Borg 56.4 s; not observed throughput. | real CLI replay missing; inspect safety of bounded process materialization. |
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
