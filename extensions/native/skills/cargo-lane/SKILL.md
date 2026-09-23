---
name: cargo-lane
description: "Build and test Cargo worktrees with bounded RAM, private targets and a shared PostgreSQL fixture."
---

# Cargo native lane

Run from the worktree root, not the canonical dirty checkout. The adapter is
the `native` Blu workflow (`extensions/native/workflows/native.blu`), exposed
as the tool `ext__native__run` and the command `/ext:native:run`. It takes one
`arguments` string and needs no Python. As a tool, also pass a fresh
`request_id`: identical tool arguments replay the earlier result.

It plans `cargo check|build|test` with `-j` (at most 6, from MemAvailable with
an 8 GiB reserve plus 2 GiB fixed job overhead), sets `CARGO_TARGET_DIR` to
this worktree's `target/` (a `--target-dir` outside the worktree is refused)
and does not use `--release`. `BORG_NATIVE_MAX_JOBS=2` lowers both `-j` and its
lane RAM reservation; it cannot lower the 8 GiB admission floor. Raw Cargo
flags go after `--`; test-harness flags need a second `--`.

```text
/ext:native:run cargo test -p borg-agent-runtime -- -- --skip NAME
/ext:native:run cargo check -- --workspace
/ext:native:run --dry-run cargo build          # print the JobSpec only
/ext:native:run postgres cargo test -p borg-agent-runtime
```

The workflow submits one `borg lane job` and returns its ID immediately. Toolchains
run only inside that lane job; there is no uncoordinated mode. Await it with
`borg lane job wait ID --json` in a shell, or a Borg `watch` on that command
if other work can proceed. No `sleep`/status polling. Resources: host CPU/RAM
slots and a worktree-private target; only shared fixtures take a service
lease. Run `borg worktree --project "$PWD" budget` and preview
`borg worktree --project "$PWD" gc` (dry-run) before any human-confirmed GC.
Never manually delete a live agent's target.

`cargo test -p borg-agent-runtime` requires PostgreSQL: it is refused unless
`BORG_TEST_SESSIONS_URL` is set or the `postgres` prefix is used. With
`postgres`, the job's lane pre-hook leases the `test-postgres` service and
creates a per-job database, and the job receives its URL as
`BORG_TEST_SESSIONS_URL`. The post-hook drops that database and releases the
lease after the job ends, including after failure, timeout, or a failed
pre-hook. A queued job holds nothing. A service administrator supplies
`BORG_TEST_POSTGRES_ADMIN_URL` (no passwords; they are refused).
`/ext:native:run service start` provisions an owned peer-auth `/tmp` cluster
and prints that URL. Never stop another agent's service. Service core admits
one distinct lease owner at a time, so a second concurrent postgres job fails
its pre-hook instead of queueing. See `docs/gamedev/native-adapter.md`.
