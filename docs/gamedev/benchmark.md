# Lane regression gates

Current evidence is maintained as Rust integration tests that drive the public
`borg` CLI with synthetic jobs/services, rather than dated branch binaries,
Python policy models or retained `/tmp` logs. Source:
`crates/borg-cli/tests/lane_process.rs` and `lane_services.rs`.

```sh
cargo test -p borg --test lane_process --test lane_services
# Includes cases requiring a working systemd user manager:
cargo test -p borg --test lane_process --test lane_services -- --include-ignored --test-threads=1
```

These are instructions, not a claim that the tests were run by this doc audit.

## Coverage

`lane_process` covers FIFO/pending coalescing, shared capacity and exclusive
ordering, canonical Project/Worktree paths, RAM/disk queue refusal, pre-hook
failure, owned-scope crash recovery, detached supervisor lifetime, cancellation,
leftover cleanup, abandonment, bounded/start waits, FIFO relief, journal access
rate, retention, whole-workload stall detection, swap limits, finish hooks,
concurrent contention and refusal to join a running job.

`lane_services` uses its own test executable as fake HTTP/MCP backend and
workload. It covers stable proxy lifecycle, killed-supervisor restart barriers,
cold restart cleanup, swap limits, MCP health parsing/session cleanup,
two-service exclusive handoff and descendants, post-hook ordering/quarantine,
failed/unhealthy/slow resume and recovery, foreign client wait/grace/indefinite
wait/late renewals, active-hook rollback, per-resource grace, path aliases,
RAM/disk reservations and shared-client restore failure fencing. Some scoped
cases are ignored by default; inspect the test annotations for host needs.

Use isolated lane state and disposable resources. Degraded execution is for
fake tests only, not production recovery proof. Tests do not authorize killing
another session's processes or using a live project's editor.

## Measurement limits

These tests protect coordination invariants, not throughput targets or real
Unreal parity. No historical fake-job/model speedup is a current performance
claim. A new benchmark should record source/binary identity, host load,
request-to-start distribution, makespan, joins, per-agent waits, failures,
peak RAM/disk and isolation/recovery outcomes. Compare equivalent workloads
and distinguish admission estimates from actual usage. Global FIFO/capacity
does not guarantee equal per-agent shares.
