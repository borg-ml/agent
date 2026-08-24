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
| History display ordering | 1.7 ms |
| Event ingest | 75.5 ms |
| First full render | 16.2 ms |
| Same-width cached render | 11.0 ms |
| Six width reflows | 101.1 ms |
| Viewport slicing/scroll | 1.9 ms |

Parallel cold-cache Markdown rendering reduced first paint from 62.1 ms to
16.2 ms and six width reflows from 380.8 ms to 101.1 ms on the same checkout.
Completed-message backgrounds are now painted only for visible rows, and the
current-date display prefix is computed once per render instead of once per
entry. The largest remaining transcript cost is full-history row assembly
after the content changes. Normal viewport scrolling is not the bottleneck.
Completed message/tool caches remain effective: the existing 200-message
live-tail gate measured about 1 ms cached p95 versus 62 ms uncached p95.

The ignored `large_session_history_query_p95_gate` test profiles lossless
history retrieval over 25,000 events. Replacing a full event/search-table
anti-join on every query with the store's atomic projection-count invariant
reduced canonical FTS query p95 from 38.4 ms to 0.74 ms on the same checkout.
The bounded regular-expression fallback measured 14.5 ms p95.

The ignored `large_session_recent_prompt_recall_p95_gate` test measures the
rich TUI's bounded prompt-history load over 25,000 message events. Applying
the actor/status filter, reverse sequence order, and result limit in SQLite
instead of deserializing and sorting the entire message history reduced p95
from 88.0 ms to 0.42 ms on the same checkout.

The ignored `large_cleared_context_recovery_p95_gate` test profiles resume
after an explicit context reset with 25,000 obsolete events and a 100-event
retained suffix. Indexed boundary, legacy-queue, and latest-subagent reads
reduced recovery p95 from 54.7 ms to 0.76 ms on the same checkout.

For sampled counters, install `perf` and run the command printed by the
profile script. `cargo-flamegraph` can be used on the same release test binary
when available.
