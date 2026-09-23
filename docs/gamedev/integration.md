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
do not merge the old base a second time. Lane owner has rebased on the shared design checkpoint
`9b18edf`; services then on the stabilized lane API, and the workspace bridge
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

## Branch evidence and latest verified gates

| Branch | Observed checkpoint | Contract/API consistency and evidence | Remaining review |
| --- | --- | --- | --- |
| `gamedev/design` | rebased on final `4e01e0e` | Offline skeleton check, clippy `--all-targets -D warnings`, fmt/tests and Rust LSP (173 files, zero diagnostics) passed; public types only, no scheduler. | Design-only review passed; parent owns integrated release decision and remaining post-hook/capacity gates. |
| `gamedev/lanes` | `b8ac616` service-budget + canonical-key process checkpoint | Independently ran `cargo test -p borg-lanes lanes::tests --offline`: 6 passed (0.34 s), plus `python3 crates/borg-cli/tests/test_lane_process.py target/debug/borg`: 5 passed (2.084 s), including isolated killed-supervisor systemd scope recovery. Shared service lease, Preparing and same-lock grant recheck added. | Automatic all-active-service discovery/yield implemented; independent real-systemd two-service no-hook scoped-descendant gate passed on exact built binary SHA256 `4022be198a99…` (job `2ad29092`). Independent real-systemd two-service FIFO post-hook success-path barrier passed on same pinned binary (job `27038d8c`); failure quarantine/retry still unproved. Atomic service RAM/disk reservation committed; owner public-CLI RAM deferral passed once on rebuilt SHA `011c4ba9…`, but independent repeat was nondeterministic as host available RAM rose; a deterministic capacity gate remains. |
| `gamedev/services` | `1e822c3` services + delegated subgroup/crash cleanup | Independently reran `CARGO_BUILD_JOBS=6 nice -n 10 cargo test -p borg-lanes services::tests --offline`: 8 passed again (1.56 s after compile); unit cfg bypasses production delegated cgroup path, including active-lease restoration, unfenced proxy default-deny and exclusive-lease restart refusal; service holds a shared lane lease during backend lifetime and scopes each backend generation under delegated supervisor unit; real CLI isolated explicit-yield and leader-crash smoke with detached child cleanup reported by owner; combined scoped no-hook probe independently passed on lane-integrated binary. Earlier A/B probe unavailable 0 / 327 ms. MCP initialize validation, active-client restore on yield and default-deny unfenced proxy (audited allowlist pending), UnixStream control, `KillMode=control-group`, no-systemd fail-closed, raw proxy mutations denied by default. | owner reports isolated real-systemd supervisor-crash detached-child cleanup; deterministic public-CLI atomic resident-service capacity regression and session-derived model MCP owner/fence enforcement remain. Scoped two-service CLI regression passed on recorded prebudget binary. |
| `gamedev/workspace` | `d96869e` workspace checkpoint | Independently ran `CARGO_BUILD_JOBS=6 nice -n 10 cargo test -p borg-lanes workspace:: --offline`: 7 passed (0.11 s), including dirty/live GC exclusion and ack/clean-path freeze; owner reports clippy/fmt. Moved inventory/budget/GC/freeze into `borg-lanes::workspace::hygiene`, Borg GC dry-run 14 listed/0 eligible (3.482 s); Abundance 10/0 (2.742 s); target-status two over 24 GiB, no cleanup. Freeze checks acks/clean trees but does not acquire project lane gate: advisory only (MCP validates claim; core helper/CLI require caller coordination). Trait `WorkspaceCoordinator` still skeleton; helper owns separate richer record, machine budget API wired in lanes; per-agent job cap helper exists but dispatch wiring pending. Bridge GC now static-verified dry-run-only, no model apply/confirmed fields. CLI apply requires TTY per-path confirmation and journal owner-exit recheck. Freeze now checks work_id claim; still advisory without project gate. | align public trait/record or document partial implementation; bridge CLI integration still conflicts with lanes-owned entry; wire one admission authority and enforce freeze lane gate before claiming lock. |
| `gamedev/unreal` | `3d08113` thin `extensions/unreal/` Blu adapter | Independently ran `python3 -m unittest discover -s extensions/unreal/tests -v`: 5 passed (0.948 s); stable revision/policy paths permit pending coalescing, including fake UBT/symbols tools and exclusive fail-closed. Copied scheduler/editor supervisor removed; build emits core JobSpec, exclusive run/raw MCP blocked; owner reports scoped fake build and fake MCP service tests (7/7 optional, default 5 passed/2 skipped) with no real Unreal runtime or D11 editor parity. | validate real CLI/spec and service entrypoint integration; no real Unreal runtime or editor parity claim. |
| `gamedev/native` | `3693157` native Blu package | Independently ran `python3 -m unittest discover -s extensions/native/tests -v`: 10 passed (0.010 s), including nonzero response preservation. Terminal-unknown fixture keeps DB+lease, untracked source disables coalescing; owner reports real lane CMake 1/1, cargo runtime 877 passing after filtering two known failing upstream cases; native adapter fail-closed if CLI absent; service CLI is wired in combined lane branch but native does not use it. | align exact submit JSON/async watcher semantics; independently reproduce installed doctor and real-CLI smoke; GC `--apply` needs human confirmation. |
| `gamedev/bench` | `b0d4ea6` | Pending-only coalescing is v0 default (running-only hypothetical opt-in). Independently ran `python3 -m unittest -q scripts/test_gamedev_benchmark.py`: 5 passed (0.003 s). Modeled 6×8 naive 177.7 s/FIFO 174.9 s/Borg 57.4 s (46 launches/2 pending joins); hypothetical running joins 56.4 s. Not observed throughput. Pre-fix public-CLI replay on lanes `37c5ee8` observed 3×3: 2.831 s (7 unique launches/2 joins), 6×8: 8.635 s (44/4), no conflicts/OOM; low-disk refusal exit 125. Pre-fix joins cannot be cited as v0 numbers; fake processes, not Unreal or scope recovery. Second 6×8 observed 13.081 s, status p95 20.91 ms. | two-service no-hook scoped descendant probe independently passed on fresh SHA256 `4022be198a99…` (job `2ad29092`): two backend cgroups empty before grant, both clients/proxies fenced and auto-resumed. Old binary negative test retained for regression provenance. Independent two-service ordered post-hook success-path barrier also passed (job `27038d8c`, same binary; probe script SHA256 `7644a967…` later committed at bench `b0d4ea6`). Independent targeted public-CLI running-join check passed on same binary (first `2b1701d9`, second `eb8404af`; FIFO). Test hook-failure quarantine/resume retries, capacity admission and per-agent wait fairness; refresh contention replay. |
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
8. Per-agent dispatch budget is a **documented v0 limitation**, not a release
   gate: global job+service RAM/disk admission and resource FIFO protect the
   host, but one agent can enqueue many tickets ahead of another and consume
   many FIFO slots. Workspace exposes a per-agent cap helper, not integrated
   job dispatch enforcement. Bench should report per-agent waits/fairness;
   wire an agent-aware reservation/fairness policy in v0.1.
9. The mandatory remaining v0 gates are a rebuilt combined CLI with atomic
   two-service budget admission, tested failure-path post-hook quarantine
   and failed-resume surfacing/retry, and session-derived editor owner/fencing
   at the model MCP boundary. Canonical Project/Worktree identity passed on
   pinned binary `011c4ba9…`; repeat every gate on the final integrated binary.
   Real UE runtime parity is a migration acceptance test, not a Borg v0 gate.

Draft CLI reminder: `--spec` accepts serialized `JobSpec`, not adapter/template
shorthand. A model-facing caller must not construct arbitrary process argv
without trusted adapter-template validation. Native/Unreal workers received
this CLI shape and should avoid synchronous model-tool waits.

Model-facing service MCP owner is **final parent integration**, not the
services branch: only status should be exposed until registered spec policy
and actual session-derived owner/fencing enforcement are audited. A tool
argument `confirmed: true` is not a durable human approval for workspace GC.

**D11 canonical-key regression and resolution (2026-09-23):** the committed Host-key
two-service pass below is **not** Project-key parity. On pinned binary
SHA256 `4022be198a99…`, the bench owner reported a public JSON CLI
probe binding an active service to `Project(/tmp/.../project)`, then submitted an exclusive
job for `Project(/tmp/.../project/../project)` with the same resource name.
Both resolve to the same directory, yet job `48f18f43` **started** while
service status stayed Healthy (backend PID 1585338, active client, no yield).
The in-job assertion exited 1. Isolated test state
`/tmp/borg-service-bench-_nsr0djs` was retained for diagnosis; test helper
flag `--atomic-project-alias` was in-flight. This violated canonical
project identity and blocked v0 on that executable despite Host-key passes.
Project/Worktree paths must be normalized or rejected at lane, service and
capacity entrypoints before key equality.
I independently repeated the alias probe against a newly rebuilt combined
budget CLI, SHA256
`32f8f7b24c1d250fb498ca8ee9793abc939606eb625e99c6fe80367a92767657`
(mtime 03:58:27+01, hash unchanged before/after): job
`bb635ee1-596d-4940-a497-449aac6f6a2a` started and exited 1 after
the in-job check observed a Healthy service, backend port 37519, an active
client and no yield. A private copy of the WIP probe was stable at SHA256
`b1be6d2531ac0204209f2f06ce9cf4ae8487e3e439d5f8c693d965e3c902a136`.
Owned test units stopped; isolated state `/tmp/borg-service-bench-k0tsa09m`
retained, worker output under `jobs/<job-id>/output.log` and command log
`/tmp/gd-project-alias-negative.log`. The budget rebuild does not fix alias
identity; this binary predated the alias guard `4abfab2`, distinct from
the earlier stale-cgroup executable.

**Independent post-fix pass:** binary SHA256
`011c4ba94835500d40910d3dab6096533d245a55f0ea7f72c1112b1450e5e99d`
(mtime 04:01:12+01, hash unchanged) rejected the noncanonical Project
`/project/../project` exclusive request plus Project symlink and Worktree
`..`/symlink requests, before any grant. A canonical Project job for the
**same** resource then synchronously yielded the real user-systemd service
before grant; active client cleared, proxy 503 and restart fenced, then
backend auto-resumed with a new PID. Probe exit 0, job
`47fc6ac7-121a-47ec-8ceb-00942c9701d2`, private stable probe copy
SHA256 `9ecbf073553ff2f2358fdb85ec351d05cf47ec3b1d38d1baa2fa434285fc644f`,
log `/tmp/gd-project-alias-pass.log`. Committed lane process suite (6/6)
also passed independently on that binary, covering canonical Project
submit and rejection of Project/Worktree aliases; CLI capacity paths safely
normalize to the same key. **Alias gate cleared for this binary**, with an
integrated-binary rerun still required. The probe copy was not committed at
test time.

**Capacity admission still conditional:** lane/service source now journals service
RAM and per-device disk reservations under the same dispatch lock and unit
tests cover two-service over-capacity/deferred retry. The bench owner observed
a passing public two-service RAM test on post-fix CLI SHA `011c4ba9…`:
disjoint Host keys, second queued without a backend until first yielded. My
independent repeat on that **same stable SHA** did **not** observe denial:
second became Healthy. Each fixture reserved 6,913,875,147 bytes (60% of
initial `MemAvailable` ≈11.5 GB), but host free RAM rose above 15 GB before
second admission; journal retained both reservations, so the snapshot policy
legitimately admitted both. No missing reservation is established. The
original percent-of-initial-RAM probe is nondeterministic on this host;
replace it with a stable disk-backed or measured-bound gate and rerun.
Log `/tmp/gd-service-budget-pass.log`, retained own isolated fixture
`/tmp/borg-service-bench-p8e7c0sh` (test-owned units stopped). Do not mark
the combined capacity gate green on the single volatile passing run.

**Resume-readiness gate:** service owner found that a Resume control RPC can
acknowledge `Starting` before the backend becomes `Healthy`. The existing
lane path can clear `resume_pending` on that early ACK, concealing a later
failed restart. Lanes owner is adding bounded Healthy observation and durable
status/error/recover retry before marking resume complete. The older
SHA `011c4ba9…` and earlier success-path probes do **not** prove this
failure path. Rebuild and test explicit failed-resume/recovery after the fix.

**D11 scoped evidence (2026-09-23):** a stale worktree executable
that predated delegated backend cgroups let a detached child survive yield
while an exclusive job started (`821d209b`, exited 1 on test assertion). This
is a negative regression for the *old executable*, not for the updated source.
After rebuilding, binary SHA256
`4022be198a99c3e8f8cac3069387f222bd2cb4e1bfdfb7a4135e8048c19a34b2`
passed the real-systemd, two-service, no-hook `--atomic-descendant` test
**independently** (job `2ad29092-7b43-4b32-ae50-0bd7f2961f60`): both
backend PIDs stopped; two detached descendants and owned cgroups were empty
before job start; active client restored, both proxies returned 503, restart
was fenced, and both services auto-resumed with new PIDs. Test-owned units
were cleaned. Output `/tmp/gd-two-service-scoped.log`. The older degraded
mechanics-only probe also passed (job `6abb6869`) but does not substitute for
scoped evidence. This clears the core no-hook handoff/scope gate for the
recorded binary, not the untested real Unreal editor or every recovery path.
On that same binary, an independent two-service real-systemd post-hook barrier
(job `27038d8c-26d0-47b9-bf1e-1e57ee7066ee`) held a FIFO while both
services remained fenced (no backend PID, proxy 503), then confirmed the hook
completion marker before Healthy/front 200 auto-resume. Exit 0; test script
SHA256 `7644a9674069229d6c81c1e68104f32b022878402ef9fdf2255cfb372493756d`
unchanged during run, log `/tmp/gd-two-service-posthook.log`. The bench
script was subsequently committed unchanged at bench `b0d4ea6`. This proves
successful ordered post-hook/resume, not failure quarantine or durable retry.

**Critical v0 release gate (parent decision):** lanes enters `Preparing` for
exclusive project key R before grant; blocks new shared grants and synchronously
pre-yields **every** service bound to R until the owned backend scope/cgroup
is verified empty (leader PID exit alone is not enough), proxy returns 503,
and a fencing token acknowledges `Yielded`. Only then grant the job.
Every service start/restart/TTL auto-resume checks exclusive leases under the
**same kernel lock**, never a cached flag. Release runs post-hooks then service
resume; stale client tokens are rejected. Lanes owns Preparing/hooks/fencing;
services owns yield ack/start refusal/resume. Bench has a passing real-CLI
cross-module test with an active client lease, backend gone before job start,
no restart during, and resume afterward. The scoped no-hook two-service
case above and the successful ordered post-hook barrier pass on the pinned
binary are scoped successes. The Project path-alias fail-open is fixed and
independently passed on the newer pinned binary above; finish post-hook
failure/resume-retry, combined budget admission, and session-derived editor
owner fencing, then rerun the gates on the final integrated binary before
any v0 release. Fake Unreal adapters still do not establish real UE parity.

Native adapter disk reservation is an estimate, **not** target-only quota/GC;
workspace currently considers whole-worktree cleanup with owner protection.
Process-group fallback is test/degraded mode only; if leader ownership is
unprovable, quarantine affected keys and never infer cleanup from a vanished
PID. Production Linux requires a scope/cgroup and crash tests.
