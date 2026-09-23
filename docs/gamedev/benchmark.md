# Contention benchmark

Reproduce from this Borg checkout (Python standard library only):

```sh
python3 scripts/gamedev_benchmark.py
```

CI-friendly regression run: `python3 -m unittest discover -s scripts -p 'test_gamedev_benchmark.py' && python3 scripts/gamedev_benchmark.py --agents 3 --jobs 3 --json > /tmp/gamedev-benchmark.json`. The model is deterministic, does not launch Unreal, and its default run takes under a second. `--seed`, `--agents`, `--jobs`, `--ram-gib`, `--cores`, `--scale`, `--policy` select the scenario. `--json` emits per-policy data and each dispatched job's timing/resource span.

## Inputs and interpretation

Source measurements: Abundance `docs/BUILD_LANE.md` (UBT 9.8–10.4 s leaf, 191–301 s wide header, 341 s clean, 1 GiB/action, 28–31 s → 11 s concurrent worktrees, 38 s → 1–3 s capture) and `docs/AGENT_PRODUCTIVITY_2026-09-23.md` (UBT/poll/ctest/capture distributions). Refreshed `Scripts/agent_time_report.py --days 1` on 2026-09-23: window 22:19–02:55, 44 sessions/5,000 calls; 2.89 h lock waits (50 calls), 1.45 h blind sleeps (46), 1.28 h UBT (232), 0.61 h captures (290), 0.22 h ctest (36). This is a short, nonrepresentative time window and tool-call counts are **not** job frequencies. Workload weights below are explicit *scenario assumptions*, not inferred job-frequency estimates. Change the weights and run sensitivity sweeps before making production capacity decisions.

Each agent issues jobs sequentially with a fixed seed and independent 0/0.2/0.4 s initial offsets. Initial builds in groups of three share source fingerprints, later edits don't. Default 6 agents × 8 jobs. Approximate sampling weights: leaf 24%, header 6%, clean 2%, run 12%, capture 18%, editor 6%, cargo 12%, ctest 14%, import 6%. Times (unscaled): leaf 10 s in lane vs 17.5 s cold; wide header 190–300 s; clean 340 s; headless Unreal run 30–220 s at 4–8 GiB; warm capture 2 s vs cold 38 s; warm editor 4 s vs cold 45 s; cargo 25 s, ctest 7 s, exclusive import 75 s. These last four non-UBT durations and task mix are **scenario assumptions**. Simulated RAM demand includes headless run 4–8 GiB and 1 GiB per parallel UBT action (header 8, clean 12); modeled budget defaults to 20 GiB and 24 logical CPU cores. Editor needs 12 s warm-up after the first start or an exclusive import yield. No filesystem or actual Unreal RAM is allocated in the default simulation.

- `naive`: one global nonblocking lock, refusals retry every 10 *unscaled* seconds. Delay from original arrival, including retry overshoot, counts as wait.
- `fifo`: one global blocking FIFO, no polling; baseline for isolating scheduling from avoiding polling.
- `borg`: per-project/worktree key FIFO, concurrent disjoint keys subject to global RAM reservation, **pending-only** same-input build coalescing, persistent editor with exclusive import yield/restart. This models safe v0 policy, **not** proof that the Borg CLI has implemented it. `--coalesce-running` models the intended future optimization only with independent revision revalidation; the v0 JobSpec fingerprint alone cannot establish this, so running joins are disabled by default. The model does not simulate crashes, path aliasing, disk pressure, compiler scaling under contention, or systemd recovery.

`agent_wait_hours` is aggregate *scaled* queue delay (including time spent waiting for a joined build's result), not actual agent billable time. `unscaled_wait_hours` in JSON extrapolates the scenario back to nominal durations. `makespan_seconds` is scaled wall clock. `cpu_utilization` integrates modeled requested CPU cores, capped at host core count, divided by makespan × host cores; actual operating-system utilization may differ. `oom` counts capacity over-admissions; `failures` counts simulated process failures (currently none are injected). `fairness_jain` is Jain's index on each agent's solo job seconds ÷ elapsed wall time, 0–1 (higher more equal), not a guarantee of no starvation. Coalesced joiners wait for the same result without a new launch. Score policies on the **same seeded jobs**, not separate random draws.


## Sample result

`python3 scripts/gamedev_benchmark.py --agents 6 --jobs 8 --seed 23 --scale 10`:

| policy | scaled wait h | scaled makespan s | modeled CPU util | OOM/fail | Jain fairness | launches/joins | polls/yields |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| naive | 0.146 | 177.7 | 9.1% | 0/0 | 0.574 | 48/0 | 525/0 |
| global FIFO | 0.228 | 174.9 | 9.2% | 0/0 | 0.821 | 48/0 | 0/0 |
| Borg lanes v0 (modeled) | 0.054 | 57.4 | 22.8% | 0/0 | 0.816 | 46/2 | 0/2 |

FIFO can accumulate *more aggregate queue time* than opportunistic poll retries while still eliminating 525 failed poll attempts and improving fairness. Borg's 3.10× modeled makespan gain here is a model prediction, not an observed performance result; policies also differ in cold-start cost. The hypothetical `--policy borg --coalesce-running` variant has 44 launches/4 joins and 56.4 s makespan, but cannot be safely implemented from v0 JobSpec alone. Dedicated real-CLI comparisons must separate scheduling, caching/warmth and CPU contention.

## Bounded fake-process replay

For an actual CPU/RSS smoke test: `python3 scripts/gamedev_benchmark.py --agents 1 --jobs 1 --scale 200 --cores 8 --policy borg --materialize`. Materialized replay permits at most 8 live workers and 8 modeled CPU cores, and caps the **sum of requested fake buffers** at 8 GiB (default 32 MiB touched per modeled GiB, maximum 64 MiB/GiB). This is not a host RSS or CPU-affinity limit: Python process overhead adds RSS, and each fake worker burns approximately 0.25 CPU, with seconds divided by `--scale`. The *analytic* per-job CPU and RAM values above remain the calibration values; the fake process intentionally shrinks both to avoid competing with live developers. The smoke run took 0.134 s and touched 64 MiB, exit failures 0. Materialization is optional and cannot validate UBT throughput or cold editor latency.

A second, bounded 3-agent × 3-job concurrency check used
`python3 scripts/gamedev_benchmark.py --agents 3 --jobs 3 --seed 23 --scale 200 --cores 8 --policy borg --ram-mib-per-gib 4 --materialize --json`.
It modeled 8 launches/1 safe pending join and 1.524 s makespan; the actual fake
workers replayed in 1.686 s with **72 MiB peak requested/touched fake buffers and zero worker
failures** (`/tmp/gd-bench-materialize-3x3.json`). This verifies bounded worker
execution/cleanup, not CLI policy correctness or an Unreal speedup; wall time
varies with shared-host load.

## Real CLI status

The CLI contract is in `docs/gamedev/interfaces.md`: `borg lane job submit|wait|status --json` and service start/status/lease/yield/resume. The lane CLI initially landed at `37c5ee8`; the lane-owner lane+service CLI and automatic handoff are verified below on the hash-pinned candidate built from source through `5429399` (**not** the final `gamedev/integrated` CLI). The drivers execute only public JSON CLI commands with fake jobs in isolated lane state directories, never direct Rust calls or hand-made supervisor state. The isolated service proxy restart/switchover and scoped two-service handoff are verified below. Project-path alias rejection, RAM admission, failed bound post-hook and failed service-resume retry have separate post-fix public-CLI results below; orphan/crash ownership recovery remains untested. Report any divergence with CLI invocation, JSON output and minimal reproduction to `gd_lanes_core`/`gd_services_core`.

### Public-CLI drivers (hash-pinned binary verified below)

After building an integrated lane+service CLI binary in this worktree:

```sh
python3 scripts/gamedev_real_benchmark.py --borg target/debug/borg --agents 3 --jobs 3 --scale 200
python3 scripts/gamedev_real_benchmark.py --borg target/debug/borg --check-budget
python3 scripts/gamedev_real_benchmark.py --borg target/debug/borg --check-running-join
python3 scripts/gamedev_service_probe.py --borg target/debug/borg
# Scope-required gates need a user systemd manager and an owner-built current binary:
python3 scripts/gamedev_service_probe.py --borg target/debug/borg --atomic-project-alias
python3 scripts/gamedev_service_probe.py --borg target/debug/borg --check-service-disk-budget
# RAM uses moving MemAvailable; informational, not a deterministic hard gate:
python3 scripts/gamedev_service_probe.py --borg target/debug/borg --check-service-budget
python3 scripts/gamedev_service_probe.py --borg target/debug/borg --atomic-post-hook-fail
python3 scripts/gamedev_service_probe.py --borg target/debug/borg --atomic-failed-resume
python3 scripts/gamedev_service_probe.py --borg target/debug/borg --atomic-unhealthy-resume
# Informational only; do not require a per-agent fairness cap for v0:
python3 scripts/gamedev_real_benchmark.py --borg target/debug/borg --burst-fairness
```

The job driver submits the **same seeded workload generator** as the simulator through `lane --json job submit --spec -`, blocks via `job wait`, and reads timing from `job status --json`. It creates only an isolated temporary project/lane directory, with a 20-slot synthetic host memory resource. Each fake job touches 16 MiB/model GiB (≤320 MiB across admitted jobs) and consumes about 0.25 CPU; the driver explicitly uses `BORG_LANE_SCOPE=0` and `BORG_LANE_DEGRADED=1` for low-impact job coordination smoke, so it does not prove scoped recovery; the D11 tests below require real user-systemd scopes. First builds have a 0.5 s minimum to permit coalescing despite CLI startup; subsequent tasks have a 0.04 s minimum. Real mode measures CLI coordination plus tiny fake jobs, **not** nominal UBT/Unreal time or systemd crash recovery. The service probe drives only the JSON CLI and a local synthetic HTTP backend, tests lease/release, stable front port across restart and exclusive yield/resume, and stops only its own service. These commands passed against the hash-pinned lane-owner candidate below, not the final integrated v0 CLI; CI must build its own current binary and fail on any invariant rather than trusting this historical result.

### Observed lane CLI (fake processes, not Unreal)

Executed 2026-09-23 using the owner-built binary from `gamedev/lanes` at `37c5ee8` (checkpoint **before** the owner disabled unsafe running joins), with `BORG_LANE_SCOPE=0`/`BORG_LANE_DEGRADED=1` in versions that require explicit unscoped testing, `--scale 200` and a fresh isolated temp directory per run. These results include per-command CLI/process startup, the 0.5 s minimum first-build time, 0.04 s later minimum and 16 MiB/model GiB touched memory, so **do not compare their makespan to the analytic model as a lane speedup**.

| workload | requests | unique launches / joins | observed wall s | aggregate wait h | peak reserved GiB / cap | measured CPU utilisation (8 cores) | failures / OOM |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 3 agents × 3 jobs | 9 | 7 / 2 | 2.831 | 0.00088 | 6 / 20 | 3.44% | 0 / 0 |
| 6 agents × 8 jobs | 48 | 44 / 4 | 8.635 | 0.00996 | 10 / 20 | 3.21% | 0 / 0 |

The pre-fix binary could join a running job by trusting a stale caller fingerprint; these observed join counts were later superseded by the fresh hash-verified scoped and targeted-running tests below. The real driver verifies no exclusive-key overlaps and no over-capacity spans using public `job status --json` timestamps; an invariant failure aborts the test and retains its `/tmp/borg-bench-*` state path for diagnosis. `cpu_utilization_8_cores` sums status `cpu_seconds` over unique jobs, divided by wall × 8; these 0.25-CPU fake jobs are deliberately light. A second 6×8 run under different host load took 13.081 s; its 44 terminal `status --json` calls took median 16.97 ms, p95 20.91 ms (subprocess startup and JSON included). Runs have timing variance from the shared host, so no single-run confidence interval is claimed.

The low-disk regression requests more free space than `/tmp` has and sets a 500 ms queue deadline. Public `job wait` exited **125**, `job status` showed no start timestamp, and the recorded reason was `waiting for disk ... free 18373660672, need 19374127616 bytes`. This is an expected budget refusal, not an OOM. The test never allocates that disk space or launches its fake worker. Crash-owned scope handling and fingerprint mutation mid-build remain untested here; exercise those only in an isolated test host with an explicit scope ownership fixture, not by killing another agent's processes.

### Atomic service/exclusive-job release gate (D11)

```sh
python3 scripts/gamedev_service_probe.py --borg target/debug/borg --atomic
```

This is a **required real-CLI integration regression, not an optional synthetic-model check**. It starts **two** fake HTTP services bound to the exact same isolated resource key R (2 shared slots), takes an active client lease, then submits an exclusive R job **without pre/post hooks**. Inside the job it demands both backend PIDs gone, both stable front proxies returning 503, client restoration, and restart denial; after the job it demands automatic resume and both healthy front endpoints. State is isolated under `/tmp/borg-service-bench-*`; it is retained on failure. The first pass uses a unique Host key to test the shared lock/handoff mechanism; actual `Project(path)` alias/canonicalization needs its own public-CLI check because `resource set-capacity --scope PATH` currently creates a Worktree key. No service on the real game or another developer's process is touched. **A no-hook two-service pass is necessary but not sufficient for full release**: also require real user-systemd delegated-descendant cleanup and post-hook-before-resume ordering (verified on the pinned candidate below), plus crash recovery and project-key alias tests separately.

#### D11 observed result: FAIL on 9a1ed00

The basic public CLI service lifecycle smoke **passed** on an own-worktree `borg` binary built from `gamedev/lanes` at `9a1ed00`: fake HTTP backend Healthy, client lease/release, stable proxy across restart, explicit yield, event-driven resume, and a new backend PID with front HTTP 200.

The required two-service **no-hook** command above **failed as expected before auto-discovery was implemented**: job `cbb19e38-d959-4a10-9f40-23dcb1b44744` had `started_ms: null`, public `job status --json` reported `wait_reason: exclusive resource bench-exclusive-13736b581f754dc6989f56ad51f353b9 busy`, and `job wait` returned 125 on queue timeout. Both test-owned services were stopped afterward; isolated state remains at `/tmp/borg-service-bench-i0b4kjyj` for owner debugging. The first attempt failed for a separate fixture setting (`BORG_LANE_SCOPE=0` also required explicit `BORG_LANE_DEGRADED=1` for an intentionally unscoped job); the reported historical failure above includes that setting and is genuinely resource contention. This pre-auto-discovery checkpoint was superseded by the fresh scoped PASS below.

#### Fresh lane-owner D11 and running-join results

After the lane owner rebuilt its binary, record its SHA-256 and mtime before making any claim. For the verified binary (`SHA-256 4022be198a99c3e8f8cac3069387f222bd2cb4e1bfdfb7a4135e8048c19a34b2`; mtime `2026-09-23 03:43:04 +0100`), the lane source through `5429399` includes automatic bound-service discovery, pending-only coalescing and delegated backend-generation cgroups; `57085ef` afterward changed docs only.

```sh
python3 scripts/gamedev_service_probe.py --borg /home/shulgin/borg-wt/gd-lanes/target/debug/borg --atomic-descendant
python3 scripts/gamedev_service_probe.py --borg /home/shulgin/borg-wt/gd-lanes/target/debug/borg --atomic-post-hook
python3 scripts/gamedev_service_probe.py --borg /home/shulgin/borg-wt/gd-lanes/target/debug/borg --atomic-project-alias
python3 scripts/gamedev_real_benchmark.py --borg /home/shulgin/borg-wt/gd-lanes/target/debug/borg --check-running-join
```

| public-CLI gate | fresh result | evidence |
| --- | --- | --- |
| D11 **scoped** two-service, no hooks | **PASS** | job `8fad95da-8101-44a5-93b3-b2521bd34107`; both yielded before grant, fronts 503, active client restored, restart fenced, 2 detached children gone and their generation cgroups empty before grant, both backends auto-resumed with new PIDs |
| Post-hook before auto-resume (scoped) | **PASS** | job `a5e0203a-7c4f-401b-b378-002bc45dd4d7`; FIFO held its post hook while both backends were absent, both yield windows remained active, both proxies returned 503; after hook-done marker both services resumed Healthy/front HTTP 200 with new PIDs |
| Safe pending-only coalescing | **PASS** | first job `7509845b-e8fe-4cf0-8a77-5116ecd52a88` already started (fake-worker marker); identical second request got distinct ID `0c4a51f2-91de-4f97-ac6b-bb1f48adf9dc` and started after the first finished |
| Project-path alias handoff (scoped, one service) | **FAIL on this hash** | exclusive `Project(/tmp/.../project/../project)` job started while shared `Project(/tmp/.../project)` service remained Healthy with active client; both paths resolve to same directory; see repro below |
| Disk admission refusal | **PASS** | impossible free-disk requirement returned exit 125 without launching a worker |

The same binary's 6×8 public CLI replay: 48 requests, 44 unique launches/4 joins, 9.77 s makespan, 13/20 GiB peak reservation, median/p95 status latency 12.06/13.28 ms, 0 job/OOM failures. A later high-contention run (binary hash checked before, not after; concurrent rebuilds possible) took 17.431 s, 0.01684 aggregate agent-wait h, 13/20 GiB peak, 0 failures/OOM, and unique-job enqueue-to-start median/p95 1381/2043 ms; terminal status CLI median/p95 46.66/62.81 ms. Treat that later run as load sensitivity, not a version comparison. Unlike the simulator's staggered arrivals, concurrent CLI submissions can coalesce **pending** requests before the first job starts, so this aggregate 44/4 count does not contradict pending-only behavior. The dedicated already-running test above is the decisive regression.

An earlier `--atomic-descendant` attempt found a surviving child under a **stale lane executable** whose mtime predates the delegated-cgroup source change. Its retained old job `821d209b-4e17-457a-a742-2e709e56fc34` and `/tmp/borg-service-bench-r4edwyx3` were evidence against that old executable only; **they do not establish a bug in the current source**. The fresh hash-and-mtime-verified scoped rerun supersedes that failure. Similarly, `9a1ed00` pre-auto-discovery correctly failed the no-hook exclusive gate with a busy-resource timeout. Keep those historical regressions, but do not use stale binaries for current release claims. The CLI status API reports backend PIDs but not cgroup membership; the fixture observes only its own fake descendant identities and kernel cgroup filesystem (no private Borg state edits).

The post-hook barrier intentionally blocks its own fake hook on a kernel FIFO, not an agent-side sleep loop. During the block a service can report `RestartPending` with an active yield window and no backend PID; the gate asserts observable fencing (both PIDs absent, both windows active, both fronts 503) rather than requiring the literal `Yielded` status label. The released hook writes a done marker before completion; auto-resume was observed only afterward. Scoped service crash recovery remains a distinct gate outside this benchmark.

### Historical Project-scope alias failure (superseded on post-fix binary)

`--atomic-project-alias` starts one fake service with a shared `Project(root/project)` lease, then submits a no-hook exclusive job with the **same resource name** at `Project(root/project/../project)`. Capacity defaults to one slot, so no direct capacity file is needed. The test uses real user-systemd scopes and observes via the public CLI. On pinned binary `4022be198a99...`, job `48f18f43-f5a6-4fb5-b4ea-aa5e7ede22eb` started while the bound service was still Healthy (`backend_pid=1585338`, active client, empty yield map) and the worker exited 1. Both paths resolve to the same real directory; CLI/resource-key canonicalization did not treat them as the same lock. The test root `/tmp/borg-service-bench-_nsr0djs` is retained for owner diagnosis. This conflicts with the interface requirement that project path aliases not create separate locks. The owner subsequently rejected noncanonical paths at the CLI boundary; the new pinned public-CLI result below supersedes this blocker. Keep this evidence scoped to the older hash. The historical Host-scoped two-service D11 result alone never covered path aliasing.

### Post-fix public-CLI gates (earlier candidate; no Unreal)

Copied the lane owner's `gamedev/lanes` binary built through `8bc11e5` (mtime `2026-09-23 04:01:12 +0100`) before each gate. The copy's SHA-256 was verified **before and after** each probe: `011c4ba94835500d40910d3dab6096533d245a55f0ea7f72c1112b1450e5e99d`. These tests use only synthetic jobs/services under isolated `/tmp` state, public JSON CLI status and user-systemd scopes; the script stops its own services. This hash was subsequently superseded by the readiness fix below. RAM MemAvailable can rise during the probe: a separate copy of the same SHA admitted both services after a ~3.7 GB host RAM increase, **a fixture nondeterminism rather than a product bug**; use same-device disk reservation for the reproducible service-budget gate.

| gate | result on `011c4ba9…` | evidence |
| --- | --- | --- |
| Project + Worktree noncanonical aliases; canonical Project handoff | **PASS** | `--atomic-project-alias`: `Project(project/../project)`, Project symlink, Worktree `..` and symlink submissions all rejected before admission (`lane resource path is not canonical`); separate canonical Project exclusive no-hook job `523e261f-57f8-46bc-87b3-4c7009b4e400` yielded its one bound fake service before grant, front 503 and client restored, then auto-resumed Healthy/front 200 with a new PID |
| Disjoint-service RAM reservation (informational; variable host RAM) | **PASS in this run** | `--check-service-budget`: each of two disjoint Host-key services reserved 9,731,090,841 bytes (~60% host MemAvailable), second queued with no backend and `RAM admission queued: 6297200231 available after reservation, 9731090841 required`; after first yielded, second reached Healthy |
| Failed bound post-hook quarantine | **PASS** | `--atomic-post-hook-fail`: two-service job `95b899f1-fbc9-49d9-b42f-84b0bed0f877` workload exit remained **0**, post hook exited 42 after FIFO release, status had `quarantined: true`, `evidence: post hook failed: post-exclusive hook exited with exit status: 42; finished`; both backends absent and front proxies 503 |
| Failed service resume and public recovery | **PASS** | `--atomic-failed-resume`: two-service job `bdbccd33-ef16-4435-bb6f-ed780832b141` stopped **only its own yielded** `bench-editor-a` via `lane service stop` inside the job. Finished exit 0; status showed `resume_pending: [bench-editor-a]` and `resume_error: service ... is not running`. Starting that test supervisor via the public CLI while its yield remained held, then `lane job recover`, cleared pending/error and restored both backends Healthy/front 200 |

The failed-resume fixture launches `service start` asynchronously: foreground start remains open while its previous yield is held, so the harness waits for public service status before calling `job recover`; no private lane-state mutation or unknown-process signal is used. The negative post-hook gate asserts fence/quarantine, **not** an automatic resume after a failed hook. Service crash/orphan ownership, RAM pressure across other users, compiler throughput, and a post-fix rerun of every older-hash gate remain outside these observations.

### Agent-level fairness context

With the *modeled* 6×8 default seed/10× scale, per-agent total queue-wait p50/p95 is naive **89.5/162.0 s**, global FIFO **137.235/159.213 s**, Borg **33.491/42.902 s** (JSON `per_agent_wait_seconds`, p95 nearest rank). The fairness Jain score above is instead on per-agent solo-work throughput, not queue wait; FIFO can score better than Borg while accumulating more waiting. These are scenario assumptions, not measured productivity or a no-starvation proof.

An informational public-CLI `--burst-fairness` run on the **older** pinned `4022be19…` hash sent six same-key requests from one burst agent beside three peer agents. Peer mean waits were **2.644–3.098 s** versus **1.236 s/request** for the burst agent (max/min **2.507×**). This does not impose a per-agent cap in v0: key-level FIFO alone cannot promise agent-level fairness, and these short fake jobs under shared-host load cannot quantify production starvation. Keep this as a diagnostic and rerun on the final integrated binary.


### Post-readiness owner candidate (current public-CLI evidence, still pre-integration)

The lane owner rebuilt `gamedev/lanes` through `2d347be` after fixing Resume RPC acknowledgment versus actual backend Healthy; mtime `2026-09-23 04:10:35 +0100`. Copied binary SHA-256 `61ece6c173170b080933c821b81658a3d8ad422b1a5601d9550f8a30c4d7d4ac` was checked before/after each gate below. These are **not** results for a later `gamedev/integrated` build. No real Unreal jobs or other agents' services ran.

| public JSON CLI gate | observed result on `61ece6c1…` |
| --- | --- |
| `--check-service-disk-budget` | **PASS**: two disjoint Host keys, separate paths on the **same filesystem**, each reserved 10,826,804,427 bytes (~60% initial free). Second had no backend, reported `disk admission queued: 7214863157 free after reservation, 10826804427 required`; after first yielded, second became Healthy. No disk was allocated. An independently run same-device fixture passed on this SHA as well. |
| `--atomic-project-alias` | **PASS**: Project `..`, Project symlink, Worktree `..` and Worktree symlink rejected at submit; separate canonical Project no-hook exclusive job `ee671230-7c45-4708-a960-d6841e58d567` yielded its bound service, restored client, front 503 in job, auto-resumed Healthy afterward. |
| `--atomic-descendant` | **PASS**: two-service scoped no-hook job `467c1a0a-44fe-45c6-a172-b37ad9899e2c` fenced both fronts and detached child cgroups before grant, automatically resumed both backends. |
| `--atomic-post-hook` | **PASS**: job `a350f7c4-9567-491a-8145-498f2de7c1ae` kept both backends fenced/503 until FIFO-bound post hook completed, then resumed Healthy. |
| `--atomic-post-hook-fail` | **PASS**: job `0af7cc00-71f2-4b74-a726-63e7fc461126` workload exit **0**, bound post hook exited 42, status `quarantined: true` with `post hook failed` evidence, both proxies stayed 503 with no backend. |
| `--atomic-failed-resume` | **PASS**: job `51fac371-8ffb-4940-9ce3-34784f16225c` stopped only its own yielded service; status had `resume_pending` and `resume_error: service ... is not running`, then public service start plus `job recover` cleared both and restored Healthy fronts. |
| `--atomic-unhealthy-resume` | **PASS**: job `a93d3ee8-6d3c-4b1f-a743-57b6468131af` toggled only its fake backend's `/health` to 503 after yield. Resume ACK removed the yield token, but service was **Starting, not Healthy** (a candidate backend PID can exist), and job status retained `resume_pending` plus `resume_error: service ... not healthy after resume: Starting: waiting for health`. Restoring `/health` yielded Healthy; public `job recover` cleared pending/error **without a second Resume**, both fronts 200. This tests post-ACK readiness, unlike the stopped-supervisor gate above. |
| `--check-running-join` and `--check-budget` | **PASS**: already-running job `7244cfd3-416a-4e4f-9b81-8a980db65981` did not absorb identical later request `281ea895-838e-4cb8-a823-cb1b67ca8e2f`; impossible job disk admission returned 125 and `started: false`. |

The bounded 6×8 public CLI replay on this same pinned binary produced 48 requests, **45 unique launches / 3 pending joins**, 14.124 s wall, 10/20 synthetic GiB peak, 0 job/OOM failures, per-agent observed waits p50/p95 **8.91/9.595 s**, Jain solo-work fairness **0.8459**. Shared-host startup/load variance makes this a regression replay, **not** a measured improvement over the simulator or older-hash runs. Model-facing exclusive dispatch with an active *foreign client lease* needs its own uniform CLI/MCP policy and regression: these lane CLI handoff tests must not be construed as clearance for a model-facing automatic exclusive. If that feature is deferred, disable model-facing exclusive explicitly and use manually coordinated `borg lane run --exclusive`.

### Final v0 integrated-main CLI gates (synthetic; 2026-09-23)

The final integrated source is `gamedev/integrated-main @fb47f53b350503d832e7837c41eb84b8a974dd1e`
(main `ed2e5f3`, release-prep 0.10 `18ecf177`, then gamedev v0). The owner
built a dev-profile `borg 0.10.0` binary, pinned read-only at
`/tmp/gd-v0/borg-fb47f53b`; SHA-256
`9a5e52c71ae4da9e1a3eebd46a6282fb95e0f4f692760178e86ad6b672f19253`.
The runner copied that binary into `/tmp/gd-bench-final-1w53Pboy/borg`, checked
its SHA before **and after every probe, including failed probes**, and archived
the four `scripts/gamedev_*.py` files plus `test_gamedev_benchmark.py` at the exact
integrated ref (each matches the accepted `8b2a727` benchmark snapshot). The
archived Python tests passed **6/6** and all scripts compiled. The integrator
also reported full Cargo workspace tests (1880 passed, 0 failed, 32 ignored),
strict clippy and fmt for that ref; the benchmark's own CLI findings are below.

Reproduce from the `fb47f53b` script snapshot with a locally built,
**source-matched** binary. Verify SHA before and after each command; `--borg`
must name a pin-copied binary, not a potentially rebuilding worktree output.
For user-systemd service gates, require `BORG_BENCH_REQUIRE_SCOPE=1` and use
only synthetic jobs and the probe's isolated `/tmp/borg-service-bench-*` state.
The separate real-driver probes deliberately use isolated, degraded unscoped
fake jobs and do **not** verify scoped crash recovery.

| final public-JSON-CLI gate | result on pinned `fb47f53b` |
| --- | --- |
| `--atomic` no-hook two-service exclusive | **PASS**; scoped handoff and automatic resume |
| `--atomic-descendant` | **PASS**; delegated descendants fenced |
| `--atomic-post-hook` | **PASS**; bound post-hook completed before resume |
| `--atomic-post-hook-fail` | **PASS**; workload finished, failed hook quarantined both services |
| `--atomic-failed-resume` | **PASS**; pending/error journaled, public recover restored Healthy |
| `--atomic-unhealthy-resume` | **PASS**; ACK did not clear pending while backend unhealthy; after health returned, public recover cleared pending/error without a second Resume |
| `--atomic-project-alias` | **PASS**; alias rejected and canonical Project path handoff completed |
| `--check-service-disk-budget` | **PASS**; same-device admission reserved 7,927,745,739 bytes per bound service |
| `--check-running-join` | **PASS**; already-running identical fingerprint did not join; same-key FIFO held |
| `--check-budget` | **PASS**; impossible disk admission caused wait exit 125 without a start |
| `--agents 6 --jobs 8 --scale 200` | **PASS**; 48 requests, 45 unique launches/3 pending joins, zero fake-job failures/OOM |
| `--burst-fairness` | **INFORMATIONAL**; max/min mean wait/request 2.427×, not a per-agent fairness cap |

On that **measured fake-process public CLI** 6×8 replay, wall time was
**15.127 s**, aggregate agent queue wait **0.01641 h**, per-agent wait p50/p95
**9.731/11.245 s**, solo-throughput Jain fairness **0.8388**, and peak synthetic
RAM reservation **11/20 GiB**. Enqueue-to-start median/p95 was **1304/1845 ms**;
terminal status CLI latency median/p95 was **33.22/63.39 ms**; measured fake CPU
utilization over eight cores was **3.31%**. The separately calibrated 6×8
*simulation* predicts **57.4 s** for modeled Borg at scale 10 and is not a
measured speedup comparison: real CLI startup, tiny fake jobs, different scaling
and shared-host load dominate this replay. These scoped tests do not validate
Unreal editor parity, arbitrary crash ownership recovery, or model-facing
foreign-client exclusive policy. Evidence: retained
`/tmp/gd-bench-final-1w53Pboy/` provenance, Python-test output, and one JSON
log per gate; runner console `/tmp/gd-bench-v0-fb47f53b-matrix.log` ends in
`FINAL INTEGRATED CLI PASS`.

### Optional v0.1 foreign-client policy (owner branch, not the integrated v0 binary)

`gamedev/lanes-v01 @7806669` adds lane-journal-locked service-client admission while an exclusive ticket is Preparing. The owner-built binary (`2026-09-23 04:51:24 +0100`) was **copied** to `target/debug/borg-bench-v01-pin`; SHA-256 `77c657df4b175d5a78a5469715c2e2b744e556879b54cfc6e0dce3406efa44e0` remained unchanged **before and after every** public-JSON-CLI probe. The final probe script SHA-256 was `5ad7a9e7a55f7490f97b9ed5d1314c69098057bd090653a28ff713d2db36e26d`. Use `BORG_BENCH_REQUIRE_SCOPE=1` to prohibit degraded jobs/services; all probes used isolated fake jobs, two owned supervised services, and real user-systemd scopes. A service lease's `--owner UUID` sets **both** Holder participant_id and session_id to that UUID; the synthetic job holder must match both for the own-holder case. The foreign-holder cases use a different UUID.

Reproduce the **historical** five-mode PASS with the exact `f5de743` fixture,
not the current working-tree probe: its tightened idle-hook gate intentionally
fails against the old binary (negative control below). From `gamedev/bench`,
with the old owner binary already copied to `target/debug/borg-bench-v01-pin`:

```sh
set -e
PIN=$(realpath target/debug/borg-bench-v01-pin)
RUN=$(mktemp -d /tmp/gd-bench-v01-historical-XXXXXXXX)
mkdir -p "$RUN/scripts"
git show f5de743:scripts/gamedev_service_probe.py > "$RUN/scripts/gamedev_service_probe.py"
git show f5de743:scripts/gamedev_fake_service.py > "$RUN/scripts/gamedev_fake_service.py"
printf '%s  %s\n' 5ad7a9e7a55f7490f97b9ed5d1314c69098057bd090653a28ff713d2db36e26d "$RUN/scripts/gamedev_service_probe.py" | sha256sum -c
check_pin() { printf '%s  %s\n' 77c657df4b175d5a78a5469715c2e2b744e556879b54cfc6e0dce3406efa44e0 "$PIN" | sha256sum -c; }
for flag in atomic-own-lease atomic-foreign-lease atomic-foreign-grace atomic-foreign-indefinite atomic-late-lease; do
  check_pin
  if ! (cd "$RUN" && BORG_BENCH_REQUIRE_SCOPE=1 python3 scripts/gamedev_service_probe.py --borg "$PIN" --"$flag"); then
    check_pin  # provenance still checked after a failed probe
    echo "historical $flag failed; do not claim PASS" >&2
    exit 1
  fi
  check_pin
done
```

| public CLI probe on `77c657df…` | observed result |
| --- | --- |
| `--atomic-own-lease` | **PASS** job `5f66f61e-a20d-4d4b-8f73-de1061bd9b12`: same participant **and** session Holder as active client; 20 s client TTL exceeds 10 s queue timeout, so waiting for lease expiry could not falsely pass; exclusive yielded and resumed both services. |
| `--atomic-foreign-lease` | **PASS** job `95e01532-1156-49a8-9634-26a60d4a6a73`: job remained Preparing with `foreign client lease:` wait reason; **both** bound services retained original Healthy backend PIDs/front 200 before release, service A status `reason` included the exact job wait notice; releasing only its test-owned foreign lease allowed the two-service handoff/front 503, then automatic Healthy resume. CLI override grace 15 s. |
| `--atomic-foreign-grace` | **PASS** job `5a5b8149-248b-41b8-8dc5-62c1f9c07df2`: both services stayed Healthy during the wait and the holder-visible notice matched; a 5 s override allowed the exclusive to yield and resume without a manual release; start minus creation was at least 3.5 s. |
| `--atomic-foreign-indefinite` | **PASS within bounded observation** job `f16c459b-a3b4-4d75-84cf-c0a2f749c3e6`: override 0 reported `grace indefinite`, both services remained Healthy with the client active and no workload start during a **2 s event-driven** observation; public lease release then permitted yield/resume. This tests the zero-grace boundary, not an arbitrary-duration liveness proof. |
| `--atomic-late-lease` | **PASS** job `da416942-7015-4b4d-b242-78e54863903b`: while the foreign client held Preparing and both original backends stayed Healthy, a new client lease for bound service B and a renewal of the existing client on A were each rejected; no ghost B client or changed A lease expiry. Releasing A allowed normal yield/resume. CLI override grace 30 s. |

The SHA `77c657df…` `--atomic-late-lease` result above covered lease refusal but
**did not arm an active hook on an idle service**. A stricter scoped negative
control against that *same, pre-fix* copied binary set `idle_after_ms: 100` on
service B, waited for its idle-hook marker, then submitted the foreign-A
Preparing job and attempted a late B lease. The lease was refused, but B's
active hook **still ran** (`/tmp/borg-service-bench-jxde75ha/bench-editor-b.active`):
`BORG_BENCH_REQUIRE_SCOPE=1 python3 scripts/gamedev_service_probe.py --borg target/debug/borg-bench-v01-pin --atomic-late-lease`
exited 1; log `/tmp/gd-bench-idle-late-prehook-fix-negative.log`. Its isolated
root was retained for diagnosis; both test-owned services were stopped with no
backend or clients. This is evidence of the **old** hook-order gap, not a test of
owner source `gamedev/lanes-v01 @384fa9e`, which moves activation after the
under-lock grant and rolls back a failed hook. The tightened idle-hook denial,
failing-active-hook rollback, and two-key per-resource grace probes remain
**unverified until a new source-matched binary is rebuilt and hash-pinned**; do
not enable model-facing exclusive from the SHA `77c657df…` table.

These are **optional owner-branch results only**: independent review and an integrated-binary rerun are required before enabling model-facing exclusive. The current v0 integrated bridge source instead rejects model-facing exclusive and instructs coordinated manual `borg lane run --exclusive`. Neither the synthetic fake-service results nor the coordinator-lease native experiment establish real Unreal editor parity or stock PostgreSQL multi-client lease support. Logs: `/tmp/gd-bench-v01-documented-atomic-*.log`; successful temp service roots and owned units were cleaned by the probe.

### v0.1 integrated CLI gates (`gamedev/v01-main`, synthetic; 2026-09-23)

Source `gamedev/v01-main @e7b64d8f` (main `bd085302` + lanes-v01, services-shared,
landscape, bench probes). Crate gates on that source: fmt, strict workspace
Clippy, `borg-lanes` 40/40, `borg-agent-runtime` 903 passed/11 ignored (134
Postgres), `borg` 240 passed/3 ignored. One dev-profile `borg 0.10.0` build,
pinned read-only at `/tmp/gd-v01/borg-e7b64d8f`, SHA-256
`ab296a7535a67446729a84cf9abb318c445b17e15584432a29f1993820ca8617`. The runner
(`/tmp/gd-bench-run-v01main.sh`) archived the scripts at that ref and copied the
binary. It checked the SHA before and after every probe and ran all of them
(no stop on first failure). The archived Python tests passed 6/6. Service
probes used `BORG_BENCH_REQUIRE_SCOPE=1`; the shared-client probes used the
user-systemd manager.

| gate | result on pinned `e7b64d8f` |
| --- | --- |
| v0 set: `--atomic`, `--atomic-descendant`, `--atomic-post-hook`, `--atomic-post-hook-fail`, `--atomic-failed-resume`, `--atomic-unhealthy-resume`, `--atomic-project-alias`, `--check-service-disk-budget`, `--check-running-join`, `--check-budget` | **PASS** (10/10) |
| `--agents 6 --jobs 8 --scale 200` | **PASS**; 48 requests, 44 launches/4 pending joins, 0 failures/OOM, 9.70 s wall, wait p50/p95 6.58/7.04 s, Jain 0.832, peak 11/20 GiB |
| `--burst-fairness` | **INFORMATIONAL**; wait p50/p95 2.47/8.16 s |
| `--atomic-own-lease` | **PASS**; holder's own client did not block |
| `--atomic-foreign-lease` | **PASS**; Preparing with both services Healthy until release |
| `--atomic-foreign-grace` | **PASS**; 5 s grace expired, then yield/resume |
| `--atomic-foreign-indefinite` | **PASS**; grace 0 held for 2 s bounded observation |
| `--atomic-late-lease` (idle-first) | **PASS**; late grant and renewal refused, idle B's active hook **not** run |
| `--atomic-active-hook-rollback` | **PASS**; exit 42 surfaced, no ghost client, backend unchanged |
| `--atomic-per-resource-grace` | **PASS**; second-key grace 0 held 7 s past the job-wide 5 s |
| `gamedev_shared_service_probe.py` | **PASS**; two foreign shared clients held the exclusive in Preparing (5 s grace), then both were restored before grant |
| `… --restore-failure-recovery` | **PASS**; failed restore fenced Stop/Yield; the failed job's evidence named owner and lease; owner release recovered |

This closes the "unverified" idle-hook, rollback and per-resource grace items
above for this integrated binary. It is still synthetic fake-service evidence,
not Unreal parity. Model-facing exclusives stay disabled in the bridge. Logs:
`/tmp/gd-bench-v01main-d2uvk7u3/` (one log per gate), runner console
`/tmp/gd-bench-v01main-e7b64d8f-matrix.log` ending in `V01 INTEGRATED CLI PASS`.
