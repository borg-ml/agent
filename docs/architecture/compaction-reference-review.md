# Compaction reference review

2026-10-18: cloned shallow public upstreams into
`/tmp/borg-compaction-reference` for source inspection:

- Codex `8c076942cb70fba8e5c4732c23f1faabc4aed484`:
  `codex-rs/core/src/compact.rs` sends current history in one compaction
  request; its context-overflow branch removes oldest input and retries.
- Pi `482c1381a00d8d00bc81697e4a980c593db6803e`:
  `packages/coding-agent/src/core/compaction/compaction.ts` serializes selected
  history into one summary prompt, or separate history/turn-prefix summaries
  for a split turn. Configuration reserves 16,384 tokens.
- OpenCode `c38d83c0c4f92466ff9c8e12b8a741698e811012`:
  `packages/core/src/session/compaction.ts` calculates a token ceiling from
  input/context limits and reserved output, with bounded overflow shrink
  steps `[0.7, 0.5, 0.35]`.

Borg's Abundance journal established a concrete failure: automatic compaction
began at 03:45:18 UTC; summary requests completed approximately every twenty
seconds, yet the turn watchdog cancelled it at 04:05:33 as having no output.
Goal continuation then began compaction again from unchanged history.

Two mechanisms caused excessive work: the fallback used `window / 2` as a
**character** budget, and each retained image forced a separate fold. The
correction uses the existing text-token estimator with explicit headroom,
packs consecutive frames/attachments within the request budget, and retries
context-length refusal with a bounded smaller budget. Durable history is not
deleted to make the request fit. Actual pass completion must refresh liveness;
start notices and zero-progress heartbeats must not keep a stuck pass alive.

This is not a reason to change the human's selected reasoning effort, route,
subscription, or billing. Running old binaries do not acquire these source
changes automatically. The active Abundance process was not interrupted or
reconfigured during diagnosis.
