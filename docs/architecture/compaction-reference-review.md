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

## Native acceleration

Current Codex V2 (`compact_remote_v2_attempt.rs`) appends a
`compaction_trigger` item to the normal subscription Responses transport. Borg
uses this only for in-turn automatic compaction of history bound to the same
selected subscription account and model. It retains the encrypted compaction
item and recent tail through the existing durable boundary, never converts the
encrypted continuation to a text summary. Manual/session-idle compaction still
uses the explicit text-summary path. Unsupported capability alone falls back;
authentication, quota and malformed-result failures preserve history and surface
normally, without provider/billing changes.

Claude's official on-demand API describes `compact-2026-09-04`,
`compaction: {type: summarize}`, a signed compaction block, and model capability
discovery with that beta. This does not prove support on Borg's subscription
OAuth endpoint. Its current verified beta allowlist/capability source and stream
validation do not implement that contract. Leave native Claude compaction off
until account/model capability and exact signed-block replay are verified. The
older threshold strategy `compact_20260112` is not the on-demand operation.
