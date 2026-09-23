# Native-toolchains Blu adapter

The package in `extensions/native/` is installed with `borg extensions install
./extensions/native --project`. It is pure Blu (`workflows/native.blu`,
`runtime_access = "sandboxed"`); no Python or other interpreter is needed. It
uses the same `JobSpec`, `ResourceKey` and `AdmissionBudget` as other
game-development adapters; no native-specific scheduler is added to Borg core.
Host effects go through `borg_exec`, so a run needs Full Access or an approved
workflow call.

One workflow, `native`, takes a command-line style `arguments` string. It is
exposed as the tool `ext__native__run` (which also takes a fresh `request_id`,
because identical tool arguments replay) and the command `/ext:native:run`:

```text
[--project DIR] [--target-dir DIR] [--build-dir DIR] [--dry-run]
  cargo check|build|test [-p PKG]
  cmake configure|build [--target T]
  ctest test [--exclude-label L] [--label L] [--regex R]
  postgres cargo test [-p PKG]
  service show|start [--data-dir DIR] [--port N]
  [-- raw tool flags]
```

It submits one lane job and returns the `borg lane job submit` JSON (job ID)
immediately; `--dry-run` returns the planned argv/env/JobSpec instead. To
await a job, run `borg lane job wait ID --json` in a shell, or use a Borg
command watch (`notify_on=exit`). Toolchains never run outside the lane. The
former Python CLI's `--probe-direct`, `wait` and `workspace gc` modes were
removed; use `borg lane job wait` and `borg worktree --project ROOT gc`
(dry-run) directly. The five preset commands (`/ext:native:cargo-test`, …)
are replaced by `/ext:native:run <args>`. After reviewing any local
differences, refresh an existing owned install with `borg extensions install
./extensions/native --project --force --json`; without `--force`, Borg retains
the older installed files.

Cargo `check`, `build`, `test` use a worktree-private `target`, debug profile,
`-j` sized from MemAvailable (8 GiB reserve plus 2 GiB fixed job overhead,
max six actions). Set `BORG_NATIVE_MAX_JOBS=2` (1..6) to lower concurrency and its
honest RAM reservation on a busy host; it cannot bypass the admission floor.
CMake uses a private `build` and Release profile; ctest includes `-j`, regex
and `-L`/`-LE` label selection. All output paths must remain inside the
current worktree. Jobs request an exclusive worktree output resource, memory
reserve and 60 GiB host disk floor. Cargo jobs reserve 24 GiB of additional
disk headroom, while CMake/ctest reserve 6 GiB. Source/argv/env fingerprinting
prevents a different build from joining a pending coalesced job. PostgreSQL
tests never coalesce between client databases.

`postgres cargo test ...` attaches lane hooks to the job instead of a blocking
wrapper. The pre-hook runs only when the job reaches the head of its queue:
it leases `test-postgres` under a per-job owner and creates a per-job
database, whose URL the job receives as `BORG_TEST_SESSIONS_URL`. The
post-hook drops only that database and releases that lease after the job
finishes, fails, times out, or its pre-hook fails; a queued or cancelled job
holds nothing. If the lane supervisor itself dies, the one-hour lease TTL
bounds the leak. `service start` initializes an owned throwaway `/tmp`
cluster with peer-only Unix socket authentication and starts it through
`borg lane service`; `service show` prints the definition only. Set the
printed `BORG_TEST_POSTGRES_ADMIN_URL` in the client environment. Set
`BORG_NATIVE_BORG` to a built `target/debug/borg` to test without replacing
the installed CLI. Database passwords are refused, never put in job specs.

The Blu engine differs from stock Lua in ways that matter here: a bare `-` in
a pattern is rejected (write `%-`), and long `..` chains inside a function
overflow the compiler stack on the runtime's 2 MiB worker thread, so commands
are built with `table.concat`. The runtime test
`blu_workflow::tests::native_package_refuses_output_outside_the_worktree`
executes the real package source to guard this.

## Blu port verification (2026-09-23)

- Dry-run plans from the Blu workflow matched the former `native.py` exactly
  (argv, env, JobSpec apart from random correlators and fingerprint) for
  `ctest test --regex --exclude-label -- --timeout 60`, `cmake build --target`,
  `cmake configure -- -DX=1` and `cargo check -p borg-core -- --locked`.
- Through the real Blu engine against an isolated lane (`BORG_LANE_DIR`):
  `cmake configure`, `cmake build --target probe` and `ctest test --regex
  ^probe$` on a scratch CMake project each submitted a job that finished with
  exit 0 (1/1 test passed). The 60 GiB disk floor had to be lowered in a
  scratch copy because this host had under 60 GiB free; the unmodified
  workflow correctly refused.
- A Blu-planned `postgres cargo test` spec, with its argv replaced by a probe
  that prints `current_database()` and exits non-zero, ran inside
  `borg_native_<id>`, finished with exit 1, and left `test-postgres` Healthy
  with zero clients and zero `borg_native_%` databases. With another owner
  holding the service lease, the pre-hook failed (exit 125, "pre-exclusive
  hook exited"), the workload never ran, and no database was left.
- `service start` provisioned and started a Healthy peer-auth cluster;
  a second `service start` refused because the service was already running.

## Historical probes of the former Python adapter (2026-09-23)

These were measured with the removed `native.py`/`postgres.py` (see git
history); the job-planning behaviour is unchanged by the Blu port.


- `python3 -m unittest discover -s extensions/native/tests -v`: twelve pass.
- `borg extensions install ./extensions/native --project --json`: active,
  five Blu workflows registered; `borg extensions doctor --json`: active.
  After subsequent source edits, refreshed the owned installed copy with
  `--force` after comparing directories. Its Python/skill files now match source;
  installed `native.py` dry runs emit a two-job, 13 GiB Cargo admission request
  and a private CMake build resource. Its installed unit suite passes 12/12.
- In the isolated `/home/shulgin/abundance-wt/gd-native` checkout, explicit
  `--probe-direct cmake configure`: completed, private build directory.
  `--probe-direct cmake build --target cave-network-tests`: built;
  `--probe-direct ctest test --regex '^cave-network-tests$'`: 1/1 passed, 1.09 s.
  `ctest --test-dir build -N`: 306 tests;
  `ctest --test-dir build --print-labels`: **No Labels Exist**, so this checkout
  does not register the `slow` label described in the handoff. `-LE slow`
  would run all tests, not a fast subset. The full suite was not claimed.
- Initial `cargo test -p borg-agent-runtime` direct probe using the adapter's
  three-job RAM cap and isolated throwaway Postgres: **877 passed, 2 failed,
  9 ignored**, lib test phase 34.27 s (build time separate). The failures are
  `native_mcp::tests::remembered_failure_skips_relaunch_until_config_change_or_cooldown_expiry`
  and `native_mcp::tests::reported_stderr_is_scrubbed_of_secrets`; neither Rust
  test was modified by this branch. The latter asserts both that the same
  redaction marker is absent and present in its error. Do not report the
  package suite as passing. PostgreSQL-backed test coverage did run.
- With the committed `gamedev/services` core rebased and a debug Borg CLI,
  `python3 .borg/extensions/native/native.py ctest test --regex '^cave-network-tests$'`
  submitted job `0c8183e9-...` immediately, and `borg lane job wait ID --json`
  returned `Finished(exit_code=0)`; actual CTest case 1/1 passed in 1.26 s.
  `cargo test -p borg-agent-runtime -- --skip <two pre-existing failing tests>`
  submitted job `a63f6985-...`: worktree target lease granted, 26.628 s
  runtime from lane state, 877 passed / 0 failed / 9 ignored / 2 filtered,
  PostgreSQL-backed tests included. These earlier jobs used a private throwaway
  PostgreSQL server, before the supervised service CLI was wired.
- Real supervised `test-postgres` CLI on `gamedev/lanes` @`dee1b25`:
  the peer-only cluster started Healthy with the required shared
  `Host/test-postgres` lane resource. With `BORG_LANE_DIR` pointing at our
  isolated `/tmp/gd-native-lane-smoke`, `BORG_NATIVE_BORG` pointing at that
  rebuilt CLI, `BORG_NATIVE_MAX_JOBS=2`, and
  `BORG_TEST_POSTGRES_ADMIN_URL` set to the fixture's printed Unix-socket URL,
  `python3 extensions/native/postgres.py -- python3 extensions/native/native.py cargo test -p borg-agent-runtime -- -- --skip remembered_failure_skips_relaunch_until_config_change_or_cooldown_expiry --skip reported_stderr_is_scrubbed_of_secrets`
  submitted job `e2fce75b-...`: 877 passed / 0 failed / 9 ignored / 2 filtered
  in 24.28 s. The first `--` goes to `postgres.py`, the second separates
  adapter flags, and the third passes test-harness flags through Cargo.
  The wrapper confirmed terminal status, dropped its per-client database,
  released its lease, and left the service Healthy with zero clients and zero
  `borg_native_%` databases. A prior malformed Cargo invocation returned exit
  1 but also cleaned up its lease and database.
- Read-only `borg worktree --project /home/shulgin/borg-wt/gd-native
  target-status --cap-gib 24` reported private target sizes 16.3 GiB
  (`gd-native`) and 13.3 GiB (`gd-native-bench`), neither over the 24 GiB
  reporting threshold. These sizes are logical (Btrfs reflinks may share
  physical extents); the report does not enforce a per-target cap and no GC
  was applied. The warm targets are preserved until paired runs finish.
- Rebased only nine native commits onto final compatible `gamedev/services`
  @`e29b2af` (it descends from rebased design and final productivity base).
  Native `3432b79` built its own CLI through capped lane job `631fe619-...`
  (`Finished(exit_code=0)`, 3m27s compile). The rebased CLI restarted the
  owned peer-only `test-postgres` fixture as Healthy. An initial leased Cargo
  run `785729d1-...` passed 885, failed 1, ignored 9, filtered 2: the
  unrelated, intermittent `native_mcp::tests::startup_failure_reports_the_server_exit_status_and_stderr`
  lost subprocess stderr (4/4 isolated checks passed on the rebased binary;
  1/3 failed on an older binary). A separate leased retry `97ddd78f-...`
  passed 886/0, ignored 9, filtered 2 in 28.70 s. Each run confirmed terminal
  status and left zero client leases and zero `borg_native_%` databases. The
  rebased CTest lane job `20e24604-...` passed `cave-network-tests` 1/1 in
  1.20 s. The two pre-existing filtered tests remain unclaimed.
- Borg SQLx Postgres coverage through peer-compatible socket URL
  `postgresql://shulgin@localhost/postgres?host=/tmp/gd-native-pg-benchmark&port=55471`:
  `workspace_conformance::the_shared_read_surface_answers_identically`
  passed (1/1, 0.25 s), confirming the generated service URL format.
- Throwaway PostgreSQL 18.6, `pg_ctl -w start`, three independent clusters vs
  one shared cluster plus three `CREATE DATABASE` operations (PSS summed over
  each server's process tree): independent starts 0.323 s total / 58.52 MiB
  PSS; shared start 0.108 s plus 0.072 s client database creation / 27.53 MiB
  PSS. Warm local/tmp startup, not a claim for a cold host or supervisor startup.
  The probe cluster is ours; it is not the other agent's `/tmp/bd/pg`.

## Remaining integration/decisions

- The present services core accepts one **distinct lease owner** per service:
  while client A holds `test-postgres`, the postgres job for
  client B (a different UUID owner) is rejected with `service lease held by
  another owner`, not queued. Therefore sequential leased test coverage above
  does **not** demonstrate two concurrent independent Postgres clients. Do not
  share an owner between separate wrappers: the first release can invalidate
  the other active client. A bounded two-client scheduling benchmark may use a
  single coordinator lease held across both distinct client databases and both
  terminal jobs, but this is a benchmark-only bypass of independent leases,
  not product support for simultaneous leased wrappers. A future opt-in
  multi-client service contract belongs to services core/architect.

- Actual coordinated two-agent `cargo test -p borg-agent-runtime` contention
  numbers require two warm private targets; the second target warm-up was queued
  by the real lane with an actionable RAM budget reason and cancelled before
  execution when host MemAvailable dropped below its admission threshold. The initial
  direct-probe compile is explicitly **not** a coordinated result.
- Workspace core currently provides a 32 GiB **per-agent** disk cap,
  read-only `borg worktree --project ROOT target-status` (after workspace-core
  integration), and safe **whole-worktree** GC (`borg worktree --project ROOT gc`, dry-run). A hard
  per-target byte cap or target-only GC is **not implemented**; use `cargo
  clean` only on your own finished worktree and request a workspace-core
  enhancement before claiming that feature.
- Never invoke GC apply from this package. `borg worktree gc --apply` must be
  a separately reviewed/human-confirmed operation after ownership checks.
- `cargo test -p borg-agent-runtime` without `postgres` requires a leased or
  otherwise configured `BORG_TEST_SESSIONS_URL`; without one, the runtime
  test suite is refused rather than silently skipping its Postgres tests.
