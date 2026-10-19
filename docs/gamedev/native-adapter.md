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
[--project DIR] [--target-dir DIR] [--build-dir DIR] [--reserve-disk-gib N] [--dry-run]
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
disk headroom, while CMake/ctest reserve 6 GiB. Admission needs the floor
**plus** this job's reserve and other active reservations on the same filesystem:
a Cargo job can pass the adapter's 60 GiB preflight yet queue with 79 GiB free,
because its default request needs at least 84 GiB. Inspect
`borg lane job status ID --json` for `wait_reason`, or use
`borg lane job wait ID --timeout 60 --json` for a bounded receipt with the current
state and wait reason (timeout exits 124; it does not cancel the job).

For a cached check with known smaller output growth, explicitly set
`--reserve-disk-gib 4` on that invocation. It accepts integer GiB from 1 to 1024,
changes only that job's growth reservation, and cannot lower the 60 GiB floor.
Defaults remain Cargo 24 GiB and CMake/ctest 6 GiB; do not use a cached-growth
estimate for a cold/full build. This is an admission estimate, not a disk quota.

```text
/ext:native:run --reserve-disk-gib 4 cargo check -p borg-core
```

Source/argv/env fingerprinting
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

## Limits

Services default to Exclusive clients. Core also supports opt-in
`Shared {max_clients}` mode, but the native PostgreSQL workflow does not
configure that mode; independent per-job owners can therefore conflict rather
than queue. Do not share one owner across unrelated test jobs.

Disk reservations are estimates, not per-target quotas. `borg worktree
--project ROOT target-status --cap-gib 24` reports usage; whole-worktree GC
is separately protected and dry-run by default. Never invoke GC apply from
this package. Per-agent workspace budget helpers do not enforce lane job
fairness or dispatch caps. Runtime Postgres tests require a configured
`BORG_TEST_SESSIONS_URL`; use the PostgreSQL workflow when appropriate.

Package source: `extensions/native/workflows/native.blu` and `blu.toml`.
Historical Python adapter probes are not evidence for the current package.
