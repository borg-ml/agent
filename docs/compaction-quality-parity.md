# Compaction quality: Borg vs Pi vs OpenCode

Read-only comparative audit. No Borg source was modified.

**2026-09-23 update:** The native path now bounds summarization input by folding
provider-neutral chunks. A failed native summary preserves the journal and
stops the turn instead of writing a degraded compaction boundary. Replay treats
older degraded boundaries as failed summaries and restores their source history.
The failure and provenance rows below describe the 2026-09-19 audit baseline.

## 0. Corrections — read first

An earlier draft of this report made two claims that verification disproved.
Both are retracted here rather than silently edited, because they bound how much
the rest should be trusted.

**Retraction 1 — the headline gap was wrong for the native path.** The earlier
draft claimed Borg does not keep a durable verbatim tail across a compaction
boundary, and cited the fact that both replay builders call `conversation.clear()`
at the boundary. That reasoning was invalid. Clearing the builder does not prove
tail loss, because the native harness **re-journals the retained tail after the
boundary**:

```rust
// The verbatim tail is re-journaled after the boundary so a
// replayed conversation carries the same recent evidence the
// live turn continued with.
for message in &retained {
    record_native_message(&events, turn.provider, message).await?;
}
```

`native_harness.rs` (in the `needs_auto_compaction` block of the turn loop);
those events are consumed on replay by `native_conversation` in `session.rs`
(`kind == "native_model_message"`). **The native path has no gap.** This
mechanism was already present in the pinned baseline `601bcdf` — it is not a
change made during the audit. I read a truncated window of the function and drew
a conclusion from an absence I had created. Credit to the reviewing agent for
catching it.

**Retraction 2 — the trigger-headroom gap was also wrong.** The draft
recommended raising Borg's "5%" auto-compaction headroom. Borg has two
constants: the tool-round trigger is `NATIVE_AUTO_COMPACT_REMAINING_PERCENT = 15`
(more headroom than Pi's 16 384 or OpenCode's 20 000 tokens), and its doc comment
records that 5% was tried and deliberately raised after it caused length
failures. `AUTO_COMPACT_REMAINING_PERCENT = 5` applies only at a turn boundary.
No action needed.

Net effect: **Borg is in better shape than the first draft claimed.** One
narrower gap survives (§3.1) and it is scoped to the subscription path only.

## 1. Scope and method

| System | Source | Pinned |
|---|---|---|
| Borg | this repo | `601bcdf` baseline; claims re-verified against the live tree |
| Pi | `github.com/badlogic/pi-mono` | `c596d09`; `@earendil-works/pi-agent-core` 0.85.1 |
| OpenCode | `github.com/sst/opencode` | `fee476b` |

Upstream was shallow-cloned and read directly, not inferred from docs. The clones
were deleted afterwards (disk was at 98%), so **Pi/OpenCode claims below are
pinned to those commits and were not re-verifiable at write time**; Borg claims
were re-verified symbol-by-symbol against the live tree.

Citations are **symbol-based, not line numbers**: `session.rs` was edited by
another agent during the audit and line numbers shifted twice. The re-verification
pass behind every Borg claim below was run against tree `86ed7db` (2026-09-19);
`native_harness.rs` and `session.rs` have both moved since `601bcdf`, so re-check
the symbols if reading this at a later commit.

## 2. Verified findings

Every Borg claim in this table was checked against the current source.

| Axis | Borg | Pi | OpenCode |
|---|---|---|---|
| Mid-turn trigger | 15% window remaining (`NATIVE_AUTO_COMPACT_REMAINING_PERCENT`) | reserve 16 384 tok | reserve 20 000 tok (`COMPACTION_BUFFER`) |
| Turn-boundary trigger | 5% (`AUTO_COMPACT_REMAINING_PERCENT`) | — | — |
| Subscription trigger | replay > 1 MiB chars (`SUBSCRIPTION_INPUT_BUDGET_CHARS`) | — | — |
| Missing usage data | estimates from transcript, assumes 128k window (`native_context_budget`) | relies on provider | disabled if `limit.context == 0` |
| Verbatim tail, native | **10% of window, re-journaled and replayed** (`NATIVE_COMPACT_RETAIN_PERCENT`, `record_native_message`) | ~20 000 tok, stored on the compaction entry | 2k–15k tok via `tail_start_id` |
| Verbatim tail, subscription | **none** (see §3.1) | n/a | n/a |
| Old tool output | cleared, tool call kept; 40 000 tok protected; `skill` protected | truncated to 2 000 chars | cleared; `PRUNE_PROTECT = 40_000`; `skill` protected |
| High-value tool output | **8 000 chars, content-aware** (`error`/`panic`/`permission denied`/… via `compaction_tool_is_high_value`) | uniform 2 000 | uniform 2 000 |
| Summarization failure | classifies provider-side vs structural; refuses to drop history on provider failure (`compaction_failure_is_provider_side`); degrades to 40% verbatim window and continues | returns `CompactionError` | `ContextOverflowError`, turn stops |
| Provenance | `context_source`, `context_window_source`, `retained_messages`, `degraded_to`, token/duration usage on durable events | `CompactionDetails` | Started/Ended events |
| Prompt: injection boundary | **yes** — "text outside those boundaries is compaction control, not a user request" | no | no |
| Prompt: stop vs backlog | **yes** — "explicitly stopped/paused", "side requests added to the backlog" | no | no |
| Prompt: file section | **no mandatory section** | deterministic `<read-files>`/`<modified-files>` from tool-call args | mandatory `Relevant Files` |

Borg leads on failure recovery, provenance, content-aware tool retention,
injection-resistant framing, stop/backlog preservation, and missing-usage
robustness. Those are the defensible advantages; each maps to a symbol above.

## 3. Remaining gaps

### 3.1 Subscription replay keeps no verbatim tail (narrow, real)

Scoped claim, verified: `record_native_message` is called **only** from
`native_harness.rs`. The subscription fold
(`compact_subscription_context_for_budget`) journals no verbatim messages — its
caller records a `context_compaction` event carrying only the summary, then
rebuilds context from the journal. So for subscription providers Borg turns
around, post-compaction context is summary-only, where Pi and OpenCode both keep
a recent tail.

Mitigation already present: failed and interrupted user prompts are kept as an
exact durable tail and re-appended after every summary (`failed_prompts` in
`native_conversation`), so an unanswered user request cannot be summarized away.

Suggested fix, if wanted: re-journal a bounded tail in the subscription path the
same way the native path already does. The mechanism exists; only the call site
is missing. **Not implemented — no source edits.**

*Bound on this claim: established by reading code, not measured. Its practical
cost is unquantified.*

### 3.2 No mandatory file inventory (minor)

Borg's prompt names Goal / Constraints / Current state / Verification / Next
steps, with no Files section. In the evaluation, Borg's two summaries were the
only ones with no file list; the judge noted a resuming agent must re-parse prose
to find what was touched. Pi derives file lists *mechanically* from tool-call
arguments, which no prompt instruction can match for reliability.

### 3.3 Fold carry-forward wording (minor, speculative)

Borg says "supersede… omit obsolete details"; OpenCode warns "anything you do not
carry into the new summary is lost". Borg applies its weaker instruction once per
fold chunk. **This did not fail under test** — Borg's fold preserved all 15
planted facts — so this is a theoretical concern only.

## 4. Measured evaluation, and what it does not establish

A 15-fact fixture (hard prohibitions, exact identifier, evidence locator, user
stop, backlog-vs-main-assignment, conditional approval, verified-vs-pending) was
run through each system's real prompt construction, single-pass and fold, on the
authorized Claude subscription. Blind-scored: **7 of 8 samples 15/15; one 14.5**
(Pi's single pass promoted a backlog item into active Next Steps — the failure
Borg's prompt is specifically hardened against).

**This does not establish parity.** Bounds:

- One fixture, at ceiling — it discriminates nothing. It supports "no evidence
  Borg is worse", not "Borg is equal".
- Borg's fold arm is **n=1**; Pi and OpenCode are n=2.
- **One judge, partially blinded**: its session had itself produced `sample_07`.
- No independent corroboration: two second-judge routes (OpenRouter, a local
  OpenAI-compatible endpoint) were attempted, are **prohibited and excluded**,
  and produced **zero output**. No reported number depends on them.
- Prompt-level only — it holds the model and serialization constant, so it is
  structurally blind to §3.1.
- 7 contaminated fold runs (reused workers that had already seen the early
  transcript) were quarantined and excluded.
- No end-to-end agent runs: nobody measured whether an agent *resumed correctly*,
  only whether facts survived.

Evidence: fixture, fact list, per-arm prompts, raw outputs, blind mapping
(`blind_mapping.json`; shuffle seed 20260919, recorded in the superseded draft,
not in the mapping file itself), judge report (`blind_scores.md`), and the 7
quarantined runs are in `/tmp/cmpaudit/eval/`. The superseded first draft is kept
there as `superseded_v1_report.md`. **That directory is tmpfs and does not
survive a reboot** — the §4 numbers are not independently re-checkable once it is
gone, and nothing in it has been copied into the repo.

## 5. Bottom line

On **capability**, Borg is ahead of Pi and OpenCode on failure recovery,
provenance, content-aware tool retention, injection-resistant prompt framing,
stop/backlog preservation, and trigger headroom. After correcting two errors of
my own (§0), the only surviving gap is narrow: **the subscription replay path
keeps no verbatim tail, while the native path does and both competitors do.**
Two minor prompt-level gaps remain (file inventory, fold wording).

On **measured quality**, nothing here establishes parity. The single fixture sat
at ceiling, the judge was partially blinded, and the test could not see the one
real gap. Treat §4 as a floor, not a ranking.
