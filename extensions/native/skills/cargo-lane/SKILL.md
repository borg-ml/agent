---
name: cargo-lane
description: "Build and test Cargo worktrees with bounded RAM, private targets and a shared PostgreSQL fixture."
---

# Cargo native lane

Run from the worktree root, not the canonical dirty checkout. The adapter is
`extensions/native/native.py` and uses `cargo check|build|test`
with `-j` (at most 6, from MemAvailable with an 8 GiB reserve). It sets
`CARGO_TARGET_DIR` to this worktree's `target/` and does not use `--release`.
Pass additional Cargo args **after** `--` to avoid mixing adapter flags with
Cargo flags:

```sh
python3 extensions/native/native.py cargo test -p borg-agent-runtime
python3 extensions/native/native.py cargo check -- --workspace
python3 extensions/native/native.py workspace gc  # dry-run
```

The `cargo_test` workflow and `/ext:native:cargo-test` run the Borg
runtime package. Submit independent operations as core jobs; `borg lane job wait
ID` blocks on the lane's notification, or use Borg `watch` on that command if
other work can proceed. No `sleep`/status polling. Engine-neutral resources:
host CPU/RAM slots and a worktree-private target; only shared fixtures take a
host/service lease. Ask workspace core for per-worktree disk budgets and
**dry-run GC before applying**; never manually delete a live agent's target.

Borg runtime tests require `BORG_TEST_SESSIONS_URL` pointing at an admin
PostgreSQL database with CREATEDB privileges; each test creates a UUID scratch
database. A service client receives its own database/URL from the
`test-postgres` shared service, releases that client lease in a finally block,
and never stops another agent's service. Never claim PostgreSQL coverage with
this URL missing (the suite intentionally fails).

Until the core CLI lands, normal execution fails closed. `--dry-run` prints
argv/env for verification; `--probe-direct` runs **uncoordinated**, only for
explicit bootstrap/benchmark probes, not routine multi-agent work.
