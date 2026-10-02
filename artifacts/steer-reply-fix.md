# Reasoning-only replies to human steers

## Observed failure

The human's `whats lookpass2 off` steer was folded successfully at canonical
sequences 147374–147376 (2026-10-02 01:42 UTC). The next model response,
sequence 147378, had no visible assistant content: the answer existed only in
an Anthropic thinking block. The harness executed the proposed `exec` anyway.
There was no assistant Message delivering that answer. The later claim that
it was merely buried between tools was incorrect.

This is not a lost user input or a renderer dropping a text block. The provider
returned reasoning rather than assistant text. Reasoning must remain separate:
neither answer-shaped wording nor opaque signatures safely identify public text.

## Fix

An admitted human steer now requires visible assistant text before subsequent
tool calls execute. A reasoning-only/empty response gets explicit non-executed
tool results (so replay remains well-formed), closed preparation rows, and one
corrective model round. If the retry has no visible reply either, the harness
surfaces an error instead of silently continuing. It never promotes reasoning
to a public message. Ordinary background notifications do not create this gate.

A tool batch generated before the steer is folded still runs exactly once,
as required by the existing steering contract. New input resets the reply gate.
Tools and the cached system prefix are not rewritten to force the retry.

## Evidence

The new regression first failed on unmodified runtime logic: the command marker
was `original\nresumed\n` rather than `original\n`. This proves that a
post-steer reasoning-only answer resumed side effects before any visible reply.
See local `artifacts/steer-reply-before.log`.

The regression also checks correction to visible text, reply-before-tool event
ordering, a bounded failed retry, durable non-execution results, and that private
reasoning is never surfaced. Verification and deployment status follow.


## Verification and activation

- `cargo test -p borg-agent-runtime --lib a_human_steer_requires_visible_text_before_more_tools -j 1`: passed after the guard, failed before it.
- `cargo test -p borg-agent-runtime --lib native_harness::tests -j 1 -- --test-threads=2`: 67 passed, 0 failed.
- Targeted rust-analyzer diagnostics: no errors (only inactive-feature hints).
- Workspace diagnostics were attempted; no errors were found in the scanned files,
  but the 90-second workspace budget expired after 54/202 files.
- Targeted rustfmt and git diff whitespace checks passed.
- The first post-edit rerun was blocked by a concurrent worker's unrelated
  recovery-contract comparator; that worker repaired their own change and the
  rerun passed. No unrelated edits were included in this fix.

This is a source fix. The running session still uses Borg 0.13.5 without this
new guard; a compiled rollout and activation are separate from the test result.
No active human session was killed or silently restarted. Heavy builds are
not to run while the human is playing Abundance.
