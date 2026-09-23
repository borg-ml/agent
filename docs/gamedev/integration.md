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
| `gamedev/lanes` | `1a1a540` shared base, no implementation commit at first look | Contract D5 gives it CLI entry, D7 requests fencing and snapshot/subscription. | inspect implementation, real FIFO/coalesce/recovery/CLI tests. |
| `gamedev/services` | `ede51c5` shared base, no implementation commit at first look | Additive health/readiness and owner state approved. | inspect proxy A/B behavior, owner lease enforcement, scope ownership and tests. |
| `gamedev/workspace` | `0ef939c` initial inventory/admission | Prototype implemented in `borg-agent-runtime/src/workspace_hygiene.rs` with its own `WorktreeRecord`; this duplicates crate authority. Owner asked to move into `borg-lanes/src/workspace/`. | review moved code, GC dry-run/dirty exclusions, freeze ack/dependency safety. |
| `gamedev/unreal` | `ede51c5` shared base, package in progress | Owner confirmed `extensions/unreal/` and Blu workflows. | inspect manifest trust, UBT startup/project gates, editor restore/proxy and read-only migration evidence. |
| `gamedev/native` | `ede51c5` shared base, package in progress | Owner confirmed `extensions/native/`, cargo/CMake templates. | inspect executable quoting, project root validation, test fixture locks and smoke tests. |
| `gamedev/bench` | `bc989be` | Four deterministic simulator tests ran with `python3 -m unittest -q scripts/test_gamedev_benchmark.py`: OK (0.002 s). Baselines are modeled, not observed throughput. | owner adding real CLI replay; inspect safety of bounded process materialization. |
| `gamedev/research` | uncommitted `docs/gamedev/landscape.md` at first look | Sourced survey (Epic/Unity/Godot and VCS locking) read and thesis applied to design; author asked to commit. | confirm refs and source caveats retained. |

## Expected conflicts and resolution

- Every branch based on design may contain the same `ede51c5`; rebase on the
  final design branch once, not cherry-pick duplicate interface commits.
  `Cargo.lock` is owned by design for early shared deps, but CLI branch adds a
  `borg-lanes` dependency: regenerate lockfile with Cargo after resolving
  `Cargo.toml`, then run check. Do not copy a stale lockfile over changes.
- `cli.rs`/`main.rs` must have **one** owner (lanes). Service/workspace branches
  export isolated modules and request entry wiring by lanes owner. Reapply
  small match arms rather than taking either whole version if base's CLI moved.
- `lib.rs` module registration and `workspace.rs` imports may collide when
  moving helper modules; keep one public worktree record and one admission
  policy. Shared-work events still belong to Borg workspace journal.
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
6. Competitor market scan is research, not a performance benchmark or legal
   approval for hosted/shared editor licensing. Pilot and measure before a
   standalone control plane decision.
