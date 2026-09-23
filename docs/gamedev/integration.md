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
| `gamedev/design` | `0643bf1`, clean, based on `4e01e0e` | Rust skeleton strict Clippy/fmt/tests and LSP passed before later documentation-only commits; public types, not a scheduler. | Parent owns integrated release decision; no integrated branch yet. |
| `gamedev/lanes` | `9c71cfa`, clean | Independent pinned CLI SHA `61ece6…` passed real-systemd scoped two-service handoff, failed post-hook quarantine, deterministic same-device disk admission, stopped-supervisor retry and ACK-then-unhealthy recovery; alias test also passed on `011c4ba9…`. Lane owner reports 26 crate tests, strict Clippy/fmt and process tests. | Foreign client leases do **not** atomically delay exclusive preemption: model-facing exclusive disabled for v0 unless optional v0.1 work lands and passes public tests/review. Final integrated binary not yet rebuilt. |
| `gamedev/services` | `e29b2af`, clean atop lane readiness source | Delegated owned backend scope/cgroup, proxy fence and Healthy-before-resume-clear. Independently passed real-systemd descendant, post-hook and recovery probes on SHA `61ece6…`; owner also reproduced disk/alias/ACK-unhealthy and reported strict checks. | Distinguish fake backend from real Unreal parity; integrate and rerun scoped gates. |
| `gamedev/workspace` | `d96869e`, clean | Workspace admission/GC/freeze policy; 7 targeted Rust tests independently passed. Fail-closed budget overrides, same-filesystem helper and per-agent cap decision exported. | Reconcile five workspace/lanes merge conflicts; per-agent fairness enforcement is v0.1, not a v0 gate. |
| `gamedev/unreal` | `03feb37`, clean | Thin Blu Unreal adapter: 8 independent Python tests run (6 passed, 2 environment-dependent skips) (log `/tmp/gd-unreal-final-premerge-tests.log`). | Fake editor smoke and design are not real UE runtime parity; migration acceptance remains separate. |
| `gamedev/native` | `3432b79`, clean | Native Blu package: 12 independent Python tests passed (log `/tmp/gd-native-final-premerge-tests.log`); owner separately reports installed Blu smoke. | Revalidate optional installed workflows with integrated CLI; GC apply still needs human confirmation. |
| `gamedev/bench` | `8b2a727` v0 range; later optional probes WIP | Six independent model tests passed (log `/tmp/gd-bench-final-premerge-tests.log`); public health-toggle, failed-resume, failed-hook and same-device probes committed. Independent pinned SHA `61ece6…` gates are recorded below with copied probe hashes; fairness findings are informational. | Cherry-pick only benchmark-specific `62bddc0..8b2a727` due older branch base; optional foreign-lease probes remain WIP outside the v0 checkpoint. |
| `gamedev/mcp-bridge` | `0bdedf1` initial checkpoint | Session-derived holder and registered template bridge unit/live checks reported by owner, not independently accepted. | **Blocked:** best-effort service snapshot allows model exclusive, restart lacks handler actor fence, response read buffers without cap; owner preparing blanket-deny/no-restart/bounded-read/per-actor follow-up. Do not merge this checkpoint. |
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
9. Pinned post-readiness CLI `61ece6…` independently passed deterministic
   two-service disk budget admission, scoped descendant handoff, bound
   post-hook failure quarantine, stopped-supervisor resume recovery, and
   ACK-then-unhealthy backend recovery. Project/Worktree alias protection
   passed on pinned `011c4ba9…`. **Remaining v0 gates:** session-derived
   actor-derived editor owner/fencing and explicit model-exclusive disablement
   at the MCP boundary (unless independently verified atomic foreign-client
   policy lands), then rerun every gate on the final integrated binary.
   Real UE runtime parity is migration acceptance, not a Borg v0 gate.


**Selected v0 model-exclusive fallback (bridge enforcement not yet verified):**
reject **all** model-facing exclusive templates with a clear
`coordinate with the editor lease holder, then use borg lane job submit --spec <trusted-JobSpec-JSON>` message, independent of
which service specs happen to be visible. Retain nonexclusive jobs and model
service lease/read; disable model-facing restart unless the supervisor itself
atomically checks the caller's active lease. The preferred shared CLI/MCP
Preparing policy would wait on foreign active client leases until release or
configurable grace (default five minutes; zero indefinite), expose the wait,
and exempt the requester's own lease. Supervisor `status.clients` is mutated
outside the lane journal lock; neither a bridge precheck nor a lane snapshot
implements that policy. Its optional v0 inclusion requires public foreign
wait, own lease, late arrival tests and independent review before integrated
release; otherwise it is v0.1. A shell-accessible CLI is not a security
sandbox. Real UE runtime parity is migration acceptance, not a Borg v0 gate.

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
replace it with a stable disk-backed or measured-bound gate and rerun
(the deterministic disk-backed pass is recorded below).
Log `/tmp/gd-service-budget-pass.log`, retained own isolated fixture
`/tmp/borg-service-bench-p8e7c0sh` (test-owned units stopped). Do not mark
the combined capacity gate green on the single volatile RAM passing run;
use the independent deterministic disk-backed result below.

I independently ran the service owner's **same-filesystem disk** fixture
`/tmp/gd-real-capacity-probe.py` (SHA256
`6552f8d4639bfa2da5c14bd50e22a14dd149d582929185d3f1bd9be541e84a39`)
on post-readiness CLI SHA256
`61ece6c173170b080933c821b81658a3d8ad422b1a5601d9550f8a30c4d7d4ac`
unchanged before/after. Exit 0: two services on **disjoint Host keys**,
separate disk paths on the **same device**, each requested 10,729,463,808
bytes from initially 17,882,439,680 free; first Healthy, second no backend
with `disk admission queued`, then Healthy after first yielded. Both
test-owned units stopped. Log `/tmp/gd-real-capacity-probe-independent.log`.
This clears the service disk-capacity gate on that binary; RAM admission is
unit-tested but the percentage-of-initial-free public probe is volatile.

**Resume-readiness gate:** service owner found Resume RPC can acknowledge
`Starting` before `Healthy`. Old SHA `011c4ba9…` could clear
`resume_pending` early and conceal failure. Lane source `2d347be` now retains
pending until bounded Healthy observation and journals retryable errors. On
rebuilt SHA `61ece6…` I independently passed the public stopped-supervisor
failure/recover probe: job `e80b3280-c867-456c-bbbc-e7749a2d6c4a`
retained `resume_pending=[bench-editor-a]` and explicit `resume_error`;
restarting the test-owned service plus `lane job recover` cleared both and
returned two Healthy backends. Copied WIP probe SHA256
`a67aca149d1b8a17774bc72952bcbc3c863f9550e69f2c5c1e2d550b90343215`,
log `/tmp/gd-resume-retry.log`. **ACK-then-unhealthy** also independently passed on **the same stable CLI SHA**:
with the test backend's health disabled, Resume removed the yield token yet
`resume_pending=[bench-editor-a]` remained and error reported `not healthy
after resume: Starting: waiting for health`. Re-enabling health and running
`lane job recover` cleared pending/error only once the service became Healthy.
Exit 0, job `825d7488-09a2-464b-b4e2-9ff8f28278c7`, copied WIP probe
SHA256 `d7365a086ff35c25eb5ca4e0c39054252fa26fc38ebfc267c79bc45d81576dd6`
and fake backend SHA256
`0e9daeac09d1e5f7e3de3c8b5e5bae8f798b71264f937e74a6e830d798afe140`,
log `/tmp/gd-ack-unhealthy.log`. The lane recovery path checks Healthy
without issuing another Resume when the yield token is already absent.
This clears the core readiness failure/retry gate for pinned SHA `61ece6…`,
not a real-Unreal parity claim.

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
On newer pinned SHA `011c4ba9…`, I independently ran an opt-in public-CLI
**failing bound post-hook** probe: job
`2491f786-15ad-42fe-839f-863fe5543d5d` workload finished exit 0,
post hook exited 42, record showed `quarantined=true` and explicit failure
evidence, and both bound services remained fenced with backend stopped and
proxy 503 rather than auto-resuming. Exit 0, binary hash stable, copied WIP
probe SHA256
`116ebd3b7e3ff06f5319053ac839b786ec3e17ebf8153c744c0ef606996871fd`,
log `/tmp/gd-post-hook-fail.log`. This clears hook-failure quarantine for
that binary; failed-resume readiness/recovery still needs a new build.

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
independently passed on the newer pinned binary above; disk admission
and stopped-supervisor resume recovery also passed on `61ece6…`. ACK-then-unhealthy resume readiness/retry also passed on `61ece6…`; finish
session-derived editor owner fencing, then rerun all gates on the final
integrated binary before
any v0 release. Fake Unreal adapters still do not establish real UE parity.

Native adapter disk reservation is an estimate, **not** target-only quota/GC;
workspace currently considers whole-worktree cleanup with owner protection.
Process-group fallback is test/degraded mode only; if leader ownership is
unprovable, quarantine affected keys and never infer cleanup from a vanished
PID. Production Linux requires a scope/cgroup and crash tests.
