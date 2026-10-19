# Performance profiles

Run the end-to-end responsiveness stress harness with:

```bash
just tui-stress
```

The opt-in harness combines the large-session frontend profile with a real
Borg process running in a 120×40 pseudo-terminal at 165 FPS. A local mock
OpenAI-compatible provider streams 1,500 reasoning updates while a bounded
copy-on-write file workload creates storage pressure. The harness types 24
unique input probes into the live composer and measures input-to-paint p50,
p95, and maximum latency from the PTY output. It also reports bytes written
and the Linux full-I/O-pressure delta. The interactive contract is p95 at or
below 100 ms and every probe at or below 250 ms.

This test intentionally writes and synchronizes a temporary 256 MiB file under
`target/tui-responsiveness`; it is ignored by the ordinary test suite and
removes the fixture after the run.

Run the repeatable terminal profile with:

```bash
./scripts/profile-terminal-ui.sh
```

The profile exercises 14,000 resume/projection events, 10,000 conversation
messages, 4,000 child-report updates across 32 agents, first paint, cached
rendering, width reflow, and viewport scrolling. The script prints timings for the current
checkout. Treat historical local
measurements as evidence for that run, not current release guarantees. For
sampled counters, install `perf` and use the command printed by the script;
`cargo-flamegraph` is another option for CPU profiles.

## Opt-in live runtime profile

The normal Borg build does not collect runtime profile data. To profile an
owned local session, build the separate profiling variant and opt it in for
that launch:

    cargo build --release -p borg --features profiling
    BORG_PROFILE=1 target/release/borg agent

From another terminal, inspect every local session owner:

    borg inspect live
    borg inspect live --session SESSION_ID --json

The profiling variant writes one bounded, content-free snapshot to
.borg/remote/sessions/<session-id>.profile.json. It records turn duration,
provider setup, provider wait, first model output, tool execution, and
aggregated count/average/p95/max timings. The snapshot is not part of the
durable session journal and is only created when BORG_PROFILE is enabled.
`borg inspect live` also ranks the top three measured phases by wall-time
share, so a dominant provider wait or tool phase is immediately visible.

This semantic profile explains where a turn spends wall time. For CPU-only
hotspots, attach the platform profiler separately (for example perf record on
Linux) to the profiled process and its provider children.
