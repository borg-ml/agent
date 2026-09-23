---
name: cargo-lane
description: "Build and test Cargo worktrees with bounded RAM, private targets and a shared PostgreSQL fixture."
---

# Cargo native lane

Run from the worktree root, not the canonical dirty checkout. The adapter is
`extensions/native/native.py` (source) or `.borg/extensions/native/native.py` (installed) and uses `cargo check|build|test`
with `-j` (at most 6, from MemAvailable with an 8 GiB reserve plus 2 GiB fixed job overhead). It sets
`CARGO_TARGET_DIR` to this worktree's `target/` and does not use `--release`.
On a busy host, `BORG_NATIVE_MAX_JOBS=2` lowers both `-j` and its lane RAM
reservation; it cannot lower the 8 GiB admission floor.
Pass additional Cargo args **after** `--` to avoid mixing adapter flags with
Cargo flags:

```sh
python3 .borg/extensions/native/native.py cargo test -p borg-agent-runtime
python3 .borg/extensions/native/native.py cargo check -- --workspace
python3 .borg/extensions/native/native.py workspace gc  # dry-run
```

The `cargo_test` workflow requires a pre-leased `BORG_TEST_SESSIONS_URL` and submits only; it does not hold a service lease while waiting. The shell PostgreSQL wrapper below holds the lease across the job wait. The workflow and `/ext:native:cargo-test` run the Borg
runtime package. Submit independent operations as core jobs; `borg lane job wait
ID` blocks on the lane's notification, or use Borg `watch` on that command if
other work can proceed. No `sleep`/status polling. Engine-neutral resources:
host CPU/RAM slots and a worktree-private target; only shared fixtures take a
host/service lease. Run `borg worktree --project "$PWD" budget` and preview `borg worktree --project "$PWD" gc` before any human-confirmed GC; core currently caps per-agent disk, not individual targets. Never manually delete a live agent's target.

Borg runtime tests require `BORG_TEST_SESSIONS_URL` pointing at an admin
PostgreSQL database with CREATEDB privileges; each test creates a UUID scratch
database. Run `python3 .borg/extensions/native/postgres.py -- python3 .borg/extensions/native/native.py cargo test -p borg-agent-runtime`. The wrapper leases the `test-postgres` shared service, creates a per-client database and provides `BORG_TEST_SESSIONS_URL`; it drops only its own database and releases its lease in a finally block. A service administrator supplies `BORG_TEST_POSTGRES_ADMIN_URL` (do not commit credentials). Never stop another agent's service. Never claim PostgreSQL coverage with
this URL missing (the suite intentionally fails).

Normal execution requires a built CLI exposing `borg lane job submit`.
`--dry-run` prints argv/env for verification; `--probe-direct` runs
**uncoordinated**, only for explicit bootstrap/benchmark probes, not routine
multi-agent work.
