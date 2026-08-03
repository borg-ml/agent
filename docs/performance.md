# Performance profiles

Run the repeatable terminal profile with:

```bash
./scripts/profile-terminal-ui.sh
```

The profile exercises 14,000 resume/projection events, 10,000 conversation
messages, 4,000 child-report updates across 32 agents, first paint, cached
rendering, width reflow, and viewport scrolling. A release-mode run on the
development workstation measured:

| Path | Time |
| --- | ---: |
| Event ingest | 7.6 ms |
| First full render | 68.8 ms |
| Same-width cached render | 32.8 ms |
| Six width reflows | 433.0 ms |
| Viewport slicing/scroll | 10.4 ms |

The urgent hotspot is full-history layout/Markdown work when the transcript
width changes. Normal viewport scrolling is not the bottleneck. Completed
message/tool caches are effective: the existing 200-message live-tail gate
measured about 1 ms cached p95 versus 62 ms uncached p95.

For sampled counters, install `perf` and run the command printed by the
profile script. `cargo-flamegraph` can be used on the same release test binary
when available.
