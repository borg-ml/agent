# Prompt cache posture

How Borg maximises cache hits per route, what each lane actually sends, and what
is still missing. Written after comparing Borg against the Z.ai open-sourced
ZCode harness, whose cache discipline is the most explicit of the harnesses
available to read.

## What each route sends

| Route | Vendor mechanism | Borg wiring |
| --- | --- | --- |
| Anthropic API | Explicit `cache_control` breakpoints, at most four per request | One on the system block, which also covers the tool definitions that precede it, and one on the newest text so the marker advances with the conversation |
| Codex (ChatGPT subscription, Responses API) | Implicit prefix cache, keyed by `prompt_cache_key` | Stable per-session key, deterministic `instructions`, `store: false`, reasoning items replayed through `provider_state` so the prefix is reused rather than resynthesised |
| OpenAI-compatible family: Go gateway, Kimi, GLM, Qwen, OpenRouter, configured endpoints | Implicit prefix cache, keyed by `prompt_cache_key` for vendors that honour it | Same stable key; some profiles also send the session id |
| Claude subscription (Claude Code binary) | The binary owns its own cache | Borg appends volatile status and keeps that section out of the pooled-process lifecycle key |

## Invariants Borg already holds

- **Append-only conversation.** Volatile per-turn status is recorded as durable
  trailing context rather than prepended, so the prefix stays byte-identical
  across turns. The comment in `native_harness.rs` records why: placing it ahead
  of the conversation invalidated the provider prefix cache on every tick.
- **A cache key that does not rotate.** `native_prompt_cache_key` deliberately
  ignores context generation, system prompt and tools. A key that changed when
  the prefix changed would defeat the affinity it exists to provide.
- **Declaration transport.** `declaration_transport` states whether a system
  change moves the head: `InPlace` for chat completions, `Collapsed` for
  Responses and Messages, which hoist `System` into a top-level field.
- **Deterministic separators.** Audited across every adapter: the only
  whitespace introduced on the request path is constant (a double newline
  between hoisted system parts and between instruction blocks). No timestamp,
  counter or randomness reaches the prompt. Tool definitions are sorted before
  the request.
- **Marker budget.** The Anthropic route spends two of the four available
  breakpoints, leaving room to split stable from volatile system content later.

## Gaps, in the order they are worth closing

1. **Micro-compaction.** Borg has only one way to shrink context: a rewriting
   summarisation, which loses detail and invalidates the whole cached prefix.
   ZCode clears the contents of old tool results in place first, keeping the most
   recent few and the message structure intact, and only reaches for a summary
   when that is not enough. The safe place for this in Borg is the projection
   rather than durable history: the transcript keeps every byte and only the
   replayed view is trimmed, with a boundary event recorded so the trim is
   visible and attributable.
2. **Warming where no lifetime is documented.** Prices come from the catalog
   (below), so warming fires wherever a documented lifetime exists, which today
   is the direct Anthropic route. Every other vendor is left without a lifetime
   on purpose, and pi reaches the same conclusion: it annotates only direct
   Anthropic rather than assuming a proxy behaves the same, and refuses OpenAI
   lifetimes until observed expiry and billing show a documented TTL means full
   cache loss. An operator who knows a vendor retention can still declare
   `prompt_cache_ttl_seconds` for a configured provider. Making warming fire on a
   subscription route because its own cached-token usage shows a saving, without
   knowing dollars, is a product decision rather than a missing capability.
3. **Pricing source.** Done: prices are read from the models.dev document Borg
   already downloads for context windows, per provider and model, instead of the
   hand-written table that knew two ids, one of them obsolete. A model the
   catalog omits has no price, so an estimate stays unavailable rather than
   invented.
4. **Request-body recording for cache forensics.** Borg classifies misses after
   the fact from usage. ZCode records exact request bodies per turn and asserts
   the append-only invariant, ignoring `cache_control` drift when comparing,
   because a rolling marker legitimately moves. Mirroring that would let Borg
   prove a prefix stayed stable instead of inferring it. Keep such a recorder out
   of the shipped path, as ZCode keeps its own out of packaging.
5. **Stable versus volatile system blocks.** Borg keeps the system prefix stable
   by trailing volatile material, which captures most of the benefit. Splitting
   the system prompt into separately marked stable and volatile blocks, as ZCode
   does, would additionally protect the stable prefix if a future change has to
   put something volatile ahead of the conversation.
