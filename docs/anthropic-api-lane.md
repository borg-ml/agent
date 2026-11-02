# Anthropic API-key lane

The explicitly selected `anthropic` provider uses Borg's native harness and the
Anthropic Messages HTTP API. It is distinct from the `claude` subscription route,
even when the model name is the same. It reads `ANTHROPIC_API_KEY` or the stored
`anthropic_api_key`; it never substitutes subscription credentials or silently
changes billing.

`crates/borg-provider/src/provider/anthropic_messages.rs` translates Borg's
provider-neutral messages, tools and attachments into Messages requests. System
instructions are top-level, tools use `input_schema`, and `max_tokens` is always
set. Streaming text, thinking, tool-input fragments, finish reasons and usage
are normalized into Borg's model contract. Context-length failures let the
harness compact; authentication and invalid requests are not quota fallbacks.

Explicit cache breakpoints cover system instructions, tools and conversation
content. Cache reads, cache writes and uncached input are separate usage buckets.
A missing price or usage counter is unavailable, not zero. See
[Prompt cache posture](prompt-cache-posture.md) for cache and warming policy and
[Model fallback chains](model-fallback.md) for explicit API-billing admission.

Borg owns tool execution, permissions, steering, cancellation, compaction and
journal recovery on this lane, just as on native subscription lanes.

## Message threads (opt-in beta)

Set `BORG_CLAUDE_MESSAGE_THREADS=1` in Borg's environment to use
[message threads](https://platform.claude.com/docs/en/build-with-claude/message-threads)
on the native `anthropic` API-key and `claude` subscription routes. This is
explicitly opt-in because threads are **not ZDR or HIPAA eligible**. Anthropic
holds their contents encrypted in memory, expires them after five idle minutes,
and deletes them no more than one hour after last use. Organization beta/HIPAA
policy is still enforced on the subscription route.

Borg sends the full conversation to create a thread, then sends only new
messages with the previous completed response ID. System instructions, tools,
model settings, beta headers, conversation prefix and account/endpoint/session
scope must match before Borg uses a continuation. Context edits, compaction or
settings changes start a new thread. After system/tool edits, create replays
omit old signed thinking blocks that the API would reject, without changing the
local journal. Only a complete stream can publish a cursor;
interrupted requests can recover through replay without accepting partial turns.
Refusals and server fallback responses do not publish a continuation cursor.
Borg does not discard or rewrite refused user turns automatically.

Expired, already-used or in-flight cursors trigger one full-history create replay;
unrelated 404 errors do not. Unsupported requests or a thread that cannot be
stored immediately after replay switch to full-history, unthreaded requests for
the same settings/scope. This never changes provider, model or billing route.
Cache breakpoints from earlier messages are removed on replay; prompt caching
and full-context token accounting remain independent of thread upload savings.
Borg's local journal remains authoritative and contains everything needed to
resume, even after the server forgets the thread. Prompt-cache warming does not
consume a conversation's single-use continuation cursor.
