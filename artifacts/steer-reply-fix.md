# Claude narration misclassified as reasoning

## Root cause and correction of earlier attribution

The `whats lookpass2 off` steer was admitted at canonical sequences
147374–147376. The following assistant record (147378) contained two
Anthropic `thinking` blocks and an `exec` call, but no visible assistant text.
The original raw wire was not retained. Calling this model misbehavior from
that already-parsed record was wrong.

The checksum-verified Claude 2.1.285 runtime's **own** classifier identifies
index 1 of that original record as narration, not reasoning. Its SDK documents
`narration_block_indexes` as display-only indexes for server summaries of prose
between tool calls, distinct from the model's own reasoning. Our native adapter
forwarded raw SDK events but lost this semantic distinction: every thinking
block became reasoning, and only text blocks became assistant messages.

The text previously quoted is the exact stored narration **summary**, not
necessarily the original model's verbatim prose. No encrypted state is decoded
to recover or display private reasoning.

Commit d09c8a0d requested `display: summarized` specifically to restore Reasoned
rows. A bounded same-history/account/model comparison removing that flag still
returned no text blocks; this model requires thinking. Therefore simply disabling
thinking or removing the display setting is not the demonstrated fix.

## Actual fix

- The connector uses the pinned runtime's existing narration classifier. There
  is no custom signature decoder and no heuristic based on answer-shaped prose.
- Native classification travels outside the untouched API event/content blocks.
- Subscription thinking summaries wait until the complete signature and native
  classification arrive. Confirmed narration goes to visible assistant text;
  actual or unclassified thinking remains reasoning.
- Native signed blocks, signatures, order, and tool arguments remain unchanged
  for replay. Generic API-key parsing keeps its existing behavior.
- No reply tool, extra model round, or tool-execution policy is added.

## Withdrawn experiments

The 0e535126 reply-enforcement guard did not recover text in a real corrective
retry. The human rejected that workaround. Its 280 added native_harness lines
have been removed with a path-specific inverse patch, preserving other work.
No SendUserMessage implementation was added. The unrelated type-mismatch
hardening experiment was withdrawn too. Local experiment logs remain available
but are not evidence that the rejected approaches solved this issue.

## Evidence and verification

Private captures are under `/tmp/borg-steer-wire-9cae37ce/`; they are not committed.
No proposed diagnostic tool calls were executed and no billing route changed.

- Original record and all bounded replay variants: native classifier indexes `[1]`.
- The exact production forwarding loop replayed 64 captured events offline:
  all API event payloads stayed unchanged; stop 0 classified no narration,
  stops 1 and 2 carried narration index 1.
- New routing regression: red before the channel fix, green after. It checks
  buffering, public narration vs actual reasoning, conservative missing metadata,
  and identical signed replay. Local logs: `claude-narration-before.log` and
  `claude-narration-after.log`.
- Both private-fixture runs passed: the real captured stream and original stored
  response route index 1 to narration, index 0 to reasoning, and replay all signed
  blocks unchanged. No new model inference was needed for these checks.
- Final provider suite: 156 passed, 0 failed; 3 existing live tests and the new
  optional private-fixture test ignored in the default run. The fixture test was
  explicitly run twice as described above.
- Native harness suite after guard removal: 66 passed, 0 failed.
- Targeted provider Rust LSP diagnostics clean. Native harness LSP timed out;
  its 66 tests passed. JS syntax verified with Bun; TypeScript language server
  is unavailable. Workspace diagnostic pass was partial due to
  its 90-second budget, so this is not a claim of workspace-wide cleanliness.

This is a source fix. The running Borg 0.13.5 session has not been restarted or
upgraded. Normal compiled rollout and verified activation remain separate.
