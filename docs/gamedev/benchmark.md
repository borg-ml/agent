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
- `borg`: per-project/worktree key FIFO, concurrent disjoint keys subject to global RAM reservation, same-input build coalescing, persistent editor with exclusive import yield/restart. This models the intended contract, **not** proof that the Borg CLI has implemented it. The model does not simulate crashes, path aliasing, disk pressure, compiler scaling under contention, or systemd recovery.

`agent_wait_hours` is aggregate *scaled* queue delay (including time spent waiting for a joined build's result), not actual agent billable time. `unscaled_wait_hours` in JSON extrapolates the scenario back to nominal durations. `makespan_seconds` is scaled wall clock. `cpu_utilization` integrates modeled requested CPU cores, capped at host core count, divided by makespan × host cores; actual operating-system utilization may differ. `oom` counts capacity over-admissions; `failures` counts simulated process failures (currently none are injected). `fairness_jain` is Jain's index on each agent's solo job seconds ÷ elapsed wall time, 0–1 (higher more equal), not a guarantee of no starvation. Coalesced joiners wait for the same result without a new launch. Score policies on the **same seeded jobs**, not separate random draws.

## Sample result

`python3 scripts/gamedev_benchmark.py --agents 6 --jobs 8 --seed 23 --scale 10`:

| policy | scaled wait h | scaled makespan s | modeled CPU util | OOM/fail | Jain fairness | launches/joins | polls/yields |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| naive | 0.146 | 177.7 | 9.1% | 0/0 | 0.574 | 48/0 | 525/0 |
| global FIFO | 0.228 | 174.9 | 9.2% | 0/0 | 0.821 | 48/0 | 0/0 |
| Borg lanes (modeled) | 0.052 | 56.4 | 22.9% | 0/0 | 0.816 | 44/4 | 0/2 |

FIFO can accumulate *more aggregate queue time* than opportunistic poll retries while still eliminating 525 failed poll attempts and improving fairness. Borg's 3.15× makespan gain here is a model prediction, not an observed performance result; policies also differ in cold-start cost. Dedicated real-CLI comparisons must separate scheduling, caching/warmth and CPU contention.

## Bounded fake-process replay

For an actual CPU/RSS smoke test: `python3 scripts/gamedev_benchmark.py --agents 1 --jobs 1 --scale 200 --cores 8 --policy borg --materialize`. The default physical replay caps 8 live processes, 8 logical cores and 8 GiB RSS (default 32 MiB of touched RAM per modeled GiB, maximum 64 MiB/GiB); each fake process burns approximately 0.25 CPU, with seconds divided by `--scale`. The *analytic* per-job CPU and RAM values above remain the calibration values; the fake process intentionally shrinks both to avoid competing with live developers. The smoke run took 0.134 s and touched 64 MiB, exit failures 0. Materialization is optional and cannot validate UBT throughput or cold editor latency.

## Real CLI status

The CLI contract is in `docs/gamedev/interfaces.md`: `borg lane job submit|wait|status --json` and service start/status/lease/yield/resume. The lane CLI landed on `gamedev/lanes` at `37c5ee8`; the service CLI is still awaiting integration. The drivers execute only public JSON CLI commands with fake jobs in isolated lane state directories, never direct Rust calls or hand-made supervisor state. Crash ownership/systemd recovery and service proxy switchover remain unverified. Report any divergence with CLI invocation, JSON output and minimal reproduction to `gd_lanes_core`/`gd_services_core`.

### Public-CLI drivers (pending binary verification)

After the lane implementation is committed and built in this worktree:

```sh
python3 scripts/gamedev_real_benchmark.py --borg target/debug/borg --agents 3 --jobs 3 --scale 200
python3 scripts/gamedev_real_benchmark.py --borg target/debug/borg --check-budget
python3 scripts/gamedev_service_probe.py --borg target/debug/borg
```

The job driver submits the **same seeded workload generator** as the simulator through `lane --json job submit --spec -`, blocks via `job wait`, and reads timing from `job status --json`. It creates only an isolated temporary project/lane directory, with a 20-slot synthetic host memory resource. Each fake job touches 16 MiB/model GiB (≤320 MiB across admitted jobs) and consumes about 0.25 CPU; disable systemd scope integration for the smoke with `BORG_LANE_SCOPE=0` so the shared host is not affected. First builds have a 0.5 s minimum to permit coalescing despite CLI startup; subsequent tasks have a 0.04 s minimum. Real mode measures CLI coordination plus tiny fake jobs, **not** nominal UBT/Unreal time or systemd crash recovery. The service probe drives only the JSON CLI and a local synthetic HTTP backend, tests lease/release, stable front port across restart and exclusive yield/resume, and stops only its own service. Do not treat pending scripts as passing CI until compiled public CLI binaries have exercised them; a failing smoke must be reported to the owning core agent with the CLI repro.

### Observed lane CLI (fake processes, not Unreal)

Executed 2026-09-23 using the owner-built binary from `gamedev/lanes` at `37c5ee8`, with `BORG_LANE_SCOPE=0`, `--scale 200` and a fresh isolated temp directory per run. These results include per-command CLI/process startup, the 0.5 s minimum first-build time, 0.04 s later minimum and 16 MiB/model GiB touched memory, so **do not compare their makespan to the analytic model as a lane speedup**.

| workload | requests | unique launches / joins | observed wall s | aggregate wait h | peak reserved GiB / cap | measured CPU utilisation (8 cores) | failures / OOM |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 3 agents × 3 jobs | 9 | 7 / 2 | 2.831 | 0.00088 | 6 / 20 | 3.44% | 0 / 0 |
| 6 agents × 8 jobs | 48 | 44 / 4 | 8.635 | 0.00996 | 10 / 20 | 3.21% | 0 / 0 |

The real driver verifies no exclusive-key overlaps and no over-capacity spans using public `job status --json` timestamps; an invariant failure aborts the test and retains its `/tmp/borg-bench-*` state path for diagnosis. `cpu_utilization_8_cores` sums status `cpu_seconds` over unique jobs, divided by wall × 8; these 0.25-CPU fake jobs are deliberately light. A second 6×8 run under different host load took 13.081 s; its 44 terminal `status --json` calls took median 16.97 ms, p95 20.91 ms (subprocess startup and JSON included). Runs have timing variance from the shared host, so no single-run confidence interval is claimed.

The low-disk regression requests more free space than `/tmp` has and sets a 500 ms queue deadline. Public `job wait` exited **125**, `job status` showed no start timestamp, and the recorded reason was `waiting for disk ... free 18373660672, need 19374127616 bytes`. This is an expected budget refusal, not an OOM. The test never allocates that disk space or launches its fake worker. Crash-owned scope handling and fingerprint mutation mid-build remain untested here; exercise those only in an isolated test host with an explicit scope ownership fixture, not by killing another agent's processes.
