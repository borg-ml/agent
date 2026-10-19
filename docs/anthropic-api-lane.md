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
