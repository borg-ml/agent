# Game development at agent scale

Status: architecture recommendation and migration map. The interface checkpoint
is [`interfaces.md`](interfaces.md); Rust declarations alone do not provide a
working lane. Branch owners and delivery gaps are in [`integration.md`](integration.md)
when available.

## Why now

Abundance is a UE 5.8 game on a shared 24-thread/62-GB machine. Its Borg
journal recorded 137 sessions, 25,426 tool calls and 96.8 hours active turns
(25.8 hours blocked) over ~3.4 days. Agent lock/UBT/editor retry loops took
4.93 hours, blind sleeps while waiting for agents 3.90 hours, and other
sleep/poll 1.69 hours. Those are coordination costs, not compilation work.
Project-wide freezes were coordinated by hand. Per-worktree build outputs can
be 12–23 GB and filled `/home` to 100%, even though isolation is essential.

Abundance's prototype changed UBT mutex wait from 1,038 seconds against 452
seconds actual work to zero across worktrees; two simultaneous leaf-edit builds
fell from 28–31 to ~11 seconds, a leaf edit from 17.5 to ~10 seconds, viewport
capture from median 38 seconds to 1–3 seconds via a warm editor. Measured
Unreal building remains compile-bound for broad headers (300 seconds for a
widely included header); coordination is not a compiler accelerator.
Sources: Abundance `docs/BUILD_LANE.md`, `docs/EDITOR_MCP_LANE.md`,
`docs/AGENT_PRODUCTIVITY_2026-09-23.md`, and `Scripts/agent_time_report.py`.

The product objective is not "one game-agent personality". It is infrastructure
that lets many independent agents safely share machine resources, mutable
engines, worktrees and durable work while retaining ordinary Borg scheduling,
permissions, conversations and approvals.

## Architecture

```text
agent / user → Borg CLI & MCP → host-local lane supervisor
                      │             ├─ atomic resource FIFO + jobs/scopes
                      │             ├─ persistent services/front proxy
                      │             └─ worktree budget + local freeze gate
                      ├─ Borg session journal + watcher/goal yield
                      └─ workspace shared-work event log (claims/dependencies)
Blu Unreal / Unity / Godot / native adapters → validated templates & skills
```

Borg core owns the cross-engine semantics: canonical resource keys, FIFO
admission, cross-process leases backed by kernel locks, coalesced jobs,
capacity reservations, systemd-user-scope ownership and recovery, supervised
services and worktree policy. A Blu adapter supplies engine-specific commands,
project detection, cache layout, health checks, editor controls and skills.
A second cloud work queue or engine-specific lease authority would introduce
divergence; reuse Borg's existing workspace and session journals.

The host-local lane store publishes immutable ticket/job IDs and durable
transitions, while kernel lock FDs plus verified scopes determine *what is
still running*. Separate host-wide and project/worktree resources admit
unrelated jobs concurrently without putting a global mutex around every
engine. An atomic multi-resource grant avoids deadlock. Capacity is explicit
and coupled to measured RAM/disk headroom, not a thread count alone.

Waits belong to the execution layer, not to an LLM sleep loop. Submission
returns immediately. A blocking `borg lane job wait ID` observes the
supervisor's event channel and emits terminal outcome; Borg's existing
`watch` command watcher with `notify_on=exit` can observe it. After finishing
other work, an agent with an active goal can `await_watchers` (opt-in) and
wake on completion. Watching another agent requires agent watch/wait_agent,
not watching its artifact. Event registration must be snapshot-and-subscribe
without a lost wakeup; CLI waits must work after process restart. No
first-party typed lane watcher exists yet; the CLI-watch bridge costs one of
four watcher slots, so batch `wait --any` is a near-term improvement.

## Abundance migration map

| Existing piece | General primitive | Unreal adapter policy |
| --- | --- | --- |
| `Scripts/ab_build.sh`, `ab_build_lane.py` FIFO/flock | lane tickets, resource capacities, job scopes, coalescing | build output exclusive per worktree; host UBT startup lock released after log ready; UBT `-NoMutex`, private log, RAM-sized `-MaxParallelActions` |
| Build orphan/stall recovery and async `dump_syms` | scope-verified recovery, timeout, post-hook | only lane-owned scopes terminated; post-symbol hook checks library revision |
| `run --project` and install guard | exclusive lease on canonical project and host engine-install resources | headless commandlet/import/test uses gate; editor yields before exclusive grant |
| `Scripts/editor_lane.sh` editor + `lane_mcp.py` | supervised service with client leases, health, restore, yield/resume and front proxy | alternate backend ports; health before switch; idle throttle, PIE/cvar state restored by owner |
| `AGENT_PRODUCTIVITY` sleep loops | watch + active-goal yield, durable team waiting | no agent-side retry loops for running work |
| 12–23 GB worktrees; disk-full event | budgeted worktree lifecycle, cache policy, safe GC | per-tree binaries/intermediates; share native DDC; clean only inactive owned outputs |
| manual big-refactor freeze | shared-work claim + dependency + local project gate | affected paths, owner acks, drain/rebase notification; never overwrite dirty trees |

Do not hard-replace proven Abundance scripts immediately. Wrap current
`ab_build.sh` and editor lane as adapter workflows, compare outputs and timing,
then migrate one job kind at a time behind a project opt-in. Keep the
project's existing `just build` entry point and explicit `-ABWorldPath`
semantics. Finish the ongoing per-project install guard and front proxy in
Abundance first; review their final landed behavior before declaring parity.

## Safety and authority

Sub-agents remain confined by Borg's existing permissions and worktree access;
a Blu package cannot raise its own runtime access or access-policy cap. Validate
adapter-owned commands, roots, paths, environment, MCP allowlists and server
bindings under Blu policy before a supervisor accepts a template. Prefer
argument vectors to shell expansion; no arbitrary model-facing `argv` or
unbounded host executable access. A project-local package defaults to
sandboxed policy; native access needs explicit user approval. This lane
mechanism does not override human confirmation for sending, publishing,
deleting, spending money, credentials, or security controls. It cannot
convert an ordinary agent into an administrator.

Borg lane recovery kills only its own recorded, verified scope/cgroup; a
process name, file age or idle PID is not enough evidence. Never kill a
healthy unrelated Unreal/editor or a process owned by another agent's lane.
Detached process lifetime differs from requester session lifetime; state
recovery compares journal, kernel lock and scope identity. Logs must not
persist credentials, should redact declared secrets, and use per-user
permissions. The stable editor endpoint binds loopback only, and every
mutating MCP request must check an owner lease (the current raw editor MCP
server's stock tools are not all lease-gated). A/B backend switching cannot
itself imply zero loss of in-flight editor sessions.

Worktree GC is a destructive operation: default dry-run; check dirty status,
active locks, git references and owner confirmation before removal. Cache
sharing is allowlisted by ecosystem, not inferred from matching paths.
Freeze is a cooperative edit protocol involving durable participant acks,
not an implicit license to change another agent's files. The workspace
membership projection authorizes claims; the local host gate authorizes
execution. Use one authority for each fact.

## Failures and responses

| Failure | Response |
| --- | --- |
| Supervisor or requester crash | Reconcile lock FDs and owned scopes from journal, publish terminal reason, release reservations once; no broad `pkill` |
| Stalled compile vs expensive legitimate link | Check child CPU/actions/log progress, bounded hard timeout, record evidence and offer dry-run recovery |
| Lost wake between `submit` and `wait` | Atomic snapshot-then-subscribe, immediately return terminal snapshot, replay events after reconnect |
| Same fingerprint but modified source or flags | Verify start revision + argv/cwd/toolchain; fork fresh job rather than join stale work |
| Low RAM, disk full, or divergent mount | Measure `MemAvailable` and output filesystem free bytes; reserve budgets, fail with actionable gate reason and clean only owned stale outputs |
| Service crash/health check failure | Bounded restart backoff; keep healthy A backend if B fails, publish degraded status |
| Exclusive job while editor client active | Request yield, wait for client drain and resource release; cancel/time out rather than write behind editor |
| Agent interrupted/human stop | Keep job and scoped lock authority separate; watch wake must respect explicit human stop; resume with status ID |
| Disconnected/unauthorized adapter | Reject path/command/MCP escalation before launch; preserve prior healthy service where possible |
| Partial freeze ack or dirty tree | Remain proposed, no forced rebase/removal; escalate to owner or abort and release gate |

## Product shape and roll-out

Ship `borg-lanes` as a local, opt-in core primitive library and a small `borg
lane` CLI with machine-readable state. Expose safe model-facing MCP through
provider-neutral Borg dispatcher, but do not mark it complete until it is
wired, permission-checked and tested. Package Unreal as a Blu reference
adapter, native cargo/CMake as a smaller engine-agnostic counterexample;
Unity and Godot can follow from the same schema. Keep engine-specific
assumptions out of Borg core. A workflow-backed adapter is a valid first
migration bridge, not a replacement for validated declarative templates.

Roll-out stages: (1) compiled API and contract; (2) FIFO/jobs with crash
recovery and CLI, tested under contention; (3) services and workspace policy
under the same resource authority; (4) Unreal/native adapter smoke tests;
(5) MCP first-party tools with end-to-end approval and watcher tests; (6)
project-by-project opt-in with before/after metrics. Gate parity on measured
queue wait, duplicate work, successful captures, disk peak, process isolation,
and correct recovery. Do not advertise a zero-downtime editor until the A/B
proxy is verified with live in-flight requests and meaningful health checks.

### Market positioning

The market scan (`docs/gamedev/landscape.md` on `gamedev/research`, with
source register and verification caveats) recommends core+Blu before a
standalone product. Epic UE 5.8 already ships **Experimental native editor
MCP**; Horde/UBA and Zen address remote compilation and derived-data cache.
Build upon them rather than replacing the editor toolset or compiler farm.
Unity Assistant 2.18 documentation deprecates its MCP server in favor of Unity
CLI, reinforcing an adapter contract that can switch transport without
changing resource policy. Community editor bridges exist (especially Unity's
CoplayDev/unity-mcp), and Ramen/Coplay spans Unreal and Unity AI creation UX.
The surveyed Claude/Codex/Copilot worktree/parallelism docs do **not document**
host-wide, crash-safe engine/editor/GPU admission; that is a narrow claim about
the surveyed docs, not proof no competitor has such infrastructure.

P4 exclusive checkout, Unity UVCS Smart Locks, Diversion, Anchorpoint and Git
LFS locks already own binary asset edit authority. Borg must *consult and
enforce* whichever VCS lock system the project uses; its local resource lease
only coordinates execution and cannot safely supplant VCS asset locks. Private
GPU displays test visual isolation but OS/driver portability remains unverified.
Do not launch a hosted multi-tenant editor before legal review of Unreal/Unity
engine seat, hosting and redistribution terms. Pilot on one host and one studio
with p50/p90 request-to-test, duplicate builds avoided, peak disk/VRAM and
asset-lock violations. Abundance measurements prove local feasibility, not
market willingness to pay; a standalone control plane should reuse Borg's
implementation only after paid pilots, never fork its semantics.
