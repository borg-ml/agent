# Performance profiles

Run the repeatable terminal profile with:

```bash
./scripts/profile-terminal-ui.sh
```

The profile exercises 14,000 resume/projection events, 10,000 conversation
messages, 4,000 child-report updates across 32 agents, first paint, cached
rendering, width reflow, and viewport scrolling. The median optimized
release-mode run on the development workstation measured:

| Path | Time |
| --- | ---: |
| Event ingest | 76.4 ms |
| First full render | 30.6 ms |
| Same-width cached render | 23.4 ms |
| Six width reflows | 157.7 ms |
| Viewport slicing/scroll | 8.0 ms |

Parallel cold-cache Markdown rendering reduced first paint from 62.1 ms to
30.6 ms and six width reflows from 380.8 ms to 157.7 ms on the same checkout.
The largest remaining transcript cost is full-history row assembly after the
content changes. Normal viewport scrolling is not the bottleneck. Completed
message/tool caches remain effective: the existing 200-message live-tail gate
measured about 1 ms cached p95 versus 62 ms uncached p95.

For sampled counters, install `perf` and run the command printed by the
profile script. `cargo-flamegraph` can be used on the same release test binary
when available.
