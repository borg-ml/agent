# Unreal v0.2 migration parity (requirements, not rollout approval)

**Decision (2026-09-23): no default switch.** Keep Abundance `just build`,
`Scripts/ab_build.sh`, `Scripts/editor_lane.sh`, and `ab_build.sh run` on their
existing lane. The separate `tools/unreal-blu-migration` bridge is explicit
**build-only opt-in**, not a replacement for `run`, `status`, `recover`, the
editor, or MCP. Do not mix its queue with the legacy queue/locks/hooks for a
live project tree, change the shared main checkout, or expose model-facing
raw MCP/exclusive/restart on the strength of the current proof.

v0.1 acceptance established a real private Borg UBT build (698/698, exit 0),
Healthy editor with loaded Abundance modules and MCP initialize, a same-owner
client-lease D11 exclusive handoff (Yielded, empty old backend cgroup, front
503, automatic Healthy resume on a new PID), and owned teardown. It did **not**
establish wrapper feature parity. During D11 the initial detached resume
control saw `Starting` and left `resume_pending/error` despite subsequent
Healthy; `job recover` returned without clearing it, and one idempotent
`__resume_services` call reconciled the journal without a second editor
restart. Treat this as an unresolved v0.2 recovery/observability gate, not a
reason to turn on model-facing exclusive jobs. See the private Abundance
`docs/UNREAL_BLU_MIGRATION.md` for exact refs and evidence paths.

## Required parity before considering defaults

"Provider" is the intended implementation/ownership boundary; these rows are
**not** claims that the v0.2 behavior is already shipped. The build owner and
wrapper/editor owners must accept the behavior and failure policy before
changing any defaults.

| Requirement / acceptance gate | Provider | Current gap / migration rule |
| --- | --- | --- |
| One canonical Project key, tree-private build output, FIFO conflicting tickets, atomic multi-resource admission, per-tree build exclusion, host RAM/build slots and actual-output-filesystem disk admission | **Borg lanes core**, Unreal adapter supplies canonical keys/budgets; **Abundance wrapper owner** maps existing options | Prove aliases cannot bypass admission; independent legacy reservations/locks cannot constrain Borg jobs. Preserve equivalent queue timeout, status and capacity reporting. |
| Pending identical-request coalescing; running join only with input-revision recheck; same terminal log/exit for subscribers | **Borg lanes core** for join and durable job records; **Unreal adapter** for source/toolchain/output-policy fingerprint; **Abundance wrapper owner** for CLI semantics | Current adapter uses core coalescing, not legacy completed-result reuse. Test edit-during-build, `-Clean`/`-Mode=` and incompatible flags; do not claim unchanged-output success or cached compile-error reuse from coalescing alone. |
| Completed-result reuse (validated success with unchanged binaries, or unchanged UBT exit 6), `--no-reuse`, and `--no-join` equivalence if retained | **Unreal adapter + Abundance wrapper owner**, backed by **Borg core** durable results, or explicitly retire with build-owner sign-off | v0.1 does **not** replace the legacy cache policy; decide/review exact invalidation and log/exit behavior before default migration. |
| Host-wide UBT Trace.uba startup serialization *across both pathways*, known-collision bounded retry, private per-UBT `TMPDIR`/`UBA_FILE_MAPPING_DIR`, `-NoMutex` and RAM-derived parallel actions | **Unreal adapter** for UE helper/policy; **Borg core** for job admission; **Abundance build owner** coordinates transition | Adapter has a narrow Borg-only startup lock and per-UBT isolation, but legacy uses a different host-wide startup lock. Until one queue/lock domain is proven, do not run both modes simultaneously in one tree or assert cross-path startup parity. |
| Scoped process-tree lifetime, cgroup cleanup, no name-based kills, stall/timeout/orphan detection, durable recovery journal, status/log/recover CLI | **Borg lanes core**, with **Abundance wrapper owner** translating existing commands and exit codes | Preserve equivalent `build-status`, `build-recover` (dry-run/evidence), `log`, request-abandon and failure semantics. The opt-in bridge rejects status/recover; unsupported operations must fail closed, never fall back to a second queue. |
| UBA compile-cache startup/limits and safe fallback, `-NoDumpSyms`, stale-symbol removal and race-safe changed-library regeneration, debug-info flags, output isolation | **Unreal adapter** for generic UE policy/symbol helper; **Abundance build owner** for its cache service, target-specific flags and equivalence tests; **Borg core** for bounded post-hook execution | Adapter already has private UBA mappings and a symbol hook; verify ordering before notifying build waiters and under relink races, cache behavior, and per-tree generated outputs. Do not assume a Borg build reproduces every Abundance flag/hook. |
| One project-run exclusivity gate for editor, imports, commandlets, automation and captures; foreign-client wait and same-owner policy; pre-yield before exclusive grant, backend descendants gone, front 503, fence old generation, no restart during job, safe resume | **Borg lanes core + services** own Preparing/yield/lease/resume under the same lock; **Unreal adapter** supplies project/run templates; **Abundance run/editor owners** route all launch paths through it | Legacy kernel run locks and Borg Project leases are disjoint. No default exclusive until all project launchers use the same authority and real foreign-/same-owner lease tests pass; non-spec adapter runs remain disabled. |
| `pre-build`, `post-build`, `pre-exclusive`, `post-exclusive` semantics, bounded hooks, changed-output AbundanceEditor restart, settled/debounced restart and lease-aware editor restore | **Borg lanes core** for hook lifecycle, **Borg services** for restart, **Abundance build/editor owners** for project-specific hook wiring and state restoration | A build-spec symbols post-hook is **not** the legacy editor `post-build` hook. Do not silently drop hooks or run them in both lanes; prove exactly-once/ordered triggering and failure observability. |
| Session-derived owner/fence on each mutating MCP call, including backend bypass protection, PIE/cvar/camera/HUD restoration, auditable restart policy | **Borg services/core** for authentic owner/generation and proxy boundary; **Unreal adapter + Abundance editor owner** for UE operations/restore | Current adapter has `adapter_enforces_leases=false`; raw MCP and model-facing exclusive/restart stay blocked. Healthy MCP initialization and a front 403 alone do not prove mutation safety. |
| Resume journal reaches a terminal consistent state after D11 without manual internal calls, even with delayed readiness, repeated recovery or supervisor restart | **Borg lanes core + services**, **Abundance editor owner** witnesses real UE recovery | Reproduce and fix/reconcile the observed `resume_pending/error` after automatic Healthy; public status/recover must accurately reflect eventual outcome. No second job, double restart or stale client authority. |
| Stable wrapper entrypoints/exit codes and safe cutover with rollback | **Abundance build/editor wrapper owners**, with **Borg lanes/core and Unreal adapter** contract tests | Keep default legacy scripts intact until approved. Do not invent a bridge fallback or advertise one successful build as full wrapper parity. |

## Recommended single-queue path

1. Pin a reviewed integrated Borg CLI and adapter; agree the missing row-by-row
   semantics with the build and editor owners. Implement generic admission,
   recovery, service yield and journaling **once in Borg core**; put only UE
   policy in the adapter and Abundance-specific UX/hooks in Abundance. Do not
   vendor another long-lived scheduler/editor supervisor into the extension.
2. Add wrapper coverage for the missing `build`, `status`, `log`, `recover`,
   `run` and editor workflows **against the same Borg lane**; migrate *all*
   launch paths for a canonical project together. While both implementations
   exist, select one explicitly for a quiescent private tree. No automatic
   fallback between queues and no claim that legacy `flock`/reservation hooks
   protect Borg jobs (or conversely).
3. In a private tree, test queue contention, joins/reuse invalidation, startup
   collision, memory/disk admission, crash/stall recovery, symbol/cache hooks,
   post-build restart, active foreign/same-owner client leases, real exclusive
   import/commandlet yield and resume, MCP fencing/restoration, and cleanup.
   Repeat the D11 journal/recovery case using only public status/recover.
4. Only after owner review and those gates pass, arrange a coordinated,
   quiescent per-project cutover: drain legacy jobs/editor, verify no active
   legacy lock/reservation/client, switch all wrapper entrypoints to Borg,
   verify one authoritative queue/status/recovery path, and retain an explicit
   rollback procedure that drains Borg before restoring legacy. Any default
   change, model-facing promotion or shared-main rollout needs separate
   approval; this document does not authorize it.

Contracts: [interfaces.md](interfaces.md) (D7, D9–D12),
[Unreal adapter README](../../extensions/unreal/README.md), and Abundance
`docs/BUILD_LANE.md`, `docs/EDITOR_MCP_LANE.md`, `docs/UNREAL_BLU_MIGRATION.md`.
