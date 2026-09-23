# Native-toolchains Blu adapter

The package in `extensions/native/` is installed with `borg extensions install
./extensions/native --project`. It uses the same `JobSpec`, `ResourceKey` and
`AdmissionBudget` as other game-development adapters; no native-specific
scheduler is added to Borg core. Its five workflow commands submit jobs and
return IDs immediately. To await a submitted job, run `borg lane job wait ID
--json` in a shell, or use a Borg command watch (`notify_on=exit`). The adapter
CLI works from the source package (`python3 extensions/native/native.py ...`)
or the installed package (`python3 .borg/extensions/native/native.py ...`).

Cargo `check`, `build`, `test` use a worktree-private `target`, debug profile,
`-j` sized from MemAvailable (8 GiB reserve plus 2 GiB fixed job overhead,
max six actions). Set `BORG_NATIVE_MAX_JOBS=2` to lower concurrency and its
honest RAM reservation on a busy host; it cannot bypass the admission floor.
CMake uses a private `build` and Release profile; ctest includes `-j`, regex
and `-L`/`-LE`
label selection. All output paths must remain inside the current worktree.
Jobs request an exclusive worktree output resource, memory reserve and 60 GiB
host disk floor. Cargo jobs reserve 24 GiB of additional disk headroom, while
CMake/ctest reserve 6 GiB. Source/argv/env fingerprinting prevents a different
build from joining a pending coalesced job. PostgreSQL tests never coalesce
between client databases.

The wrapper `extensions/native/postgres.py` leases the supervised `test-postgres`
service, creates a client database and gives `BORG_TEST_SESSIONS_URL` to the
Cargo job; after `borg lane job wait` it drops only that database and releases
its own lease even if tests fail. `python3 extensions/native/services/test_postgres.py --start` initializes
an owned throwaway `/tmp` cluster with peer-only Unix socket authentication and
starts it through `borg lane service`. Set the printed
`BORG_TEST_POSTGRES_ADMIN_URL` in the client environment. This requires the
lane CLI built from the integration branch; the old `borg 0.9.5` executable
cannot serve these commands. Set `BORG_NATIVE_BORG` to the built
`target/debug/borg` to test without replacing the installed CLI. Do not put
database passwords in job specs.

## Reproducible probes (2026-09-23)

- `python3 -m unittest discover -s extensions/native/tests -v`: ten pass.
- `borg extensions install ./extensions/native --project --json`: active,
  five Blu workflows registered; `borg extensions doctor --json`: active.
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
  PostgreSQL-backed tests included. These jobs used a private throwaway
  PostgreSQL server, not the not-yet-wired supervised service.
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
- The `cargo_test` Blu workflow requires a leased or otherwise configured
  `BORG_TEST_SESSIONS_URL`; the shell wrapper holds the database lease across
  the job wait. Without a configured URL, the runtime test suite is refused
  rather than silently skipping its Postgres tests.
