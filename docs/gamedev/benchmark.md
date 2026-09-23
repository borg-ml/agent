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

For an actual CPU/RSS smoke test: `python3 scripts/gamedev_benchmark.py --agents 1 --jobs 1 --scale 200 --cores 8 --policy borg --materialize`. The default physical replay caps 8 live processes, 8 logical cores and 8 GiB RSS (default 32 MiB of touched RAM per modeled GiB, maximum 64 MiB/GiB); each fake process burns approximately 0.25 CPU, with seconds divided by `--scale`. The *analytic* per-job CPU and RAM values above remain the calibration values; the fake process intentionally shrinks both to avoid competing with live developers. The smoke run took 0.134 s and touched 64 MiB, exit failures 0. Materialization is optional and cannot validate UBT throughput or cold editor latency.

## Real CLI status

The CLI contract is in `docs/gamedev/interfaces.md`: `borg lane job submit|wait|status --json` and service start/status/lease/yield/resume. The lane CLI initially landed at `37c5ee8`; the integrated lane+service CLI and automatic handoff are verified below on the hash-pinned candidate built from source through `5429399`. The drivers execute only public JSON CLI commands with fake jobs in isolated lane state directories, never direct Rust calls or hand-made supervisor state. The isolated service proxy restart/switchover and scoped two-service handoff are verified below. Project-path alias rejection, RAM admission, failed bound post-hook and failed service-resume retry have separate post-fix public-CLI results below; orphan/crash ownership recovery remains untested. Report any divergence with CLI invocation, JSON output and minimal reproduction to `gd_lanes_core`/`gd_services_core`.

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

The job driver submits the **same seeded workload generator** as the simulator through `lane --json job submit --spec -`, blocks via `job wait`, and reads timing from `job status --json`. It creates only an isolated temporary project/lane directory, with a 20-slot synthetic host memory resource. Each fake job touches 16 MiB/model GiB (≤320 MiB across admitted jobs) and consumes about 0.25 CPU; the driver explicitly uses `BORG_LANE_SCOPE=0` and `BORG_LANE_DEGRADED=1` for low-impact job coordination smoke, so it does not prove scoped recovery; the D11 tests below require real user-systemd scopes. First builds have a 0.5 s minimum to permit coalescing despite CLI startup; subsequent tasks have a 0.04 s minimum. Real mode measures CLI coordination plus tiny fake jobs, **not** nominal UBT/Unreal time or systemd crash recovery. The service probe drives only the JSON CLI and a local synthetic HTTP backend, tests lease/release, stable front port across restart and exclusive yield/resume, and stops only its own service. These commands passed against the hash-pinned integrated candidate below; CI must build its own current binary and fail on any invariant rather than trusting this historical result.

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

#### Fresh integrated D11 and running-join results

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
