# Prompt cache posture

How Borg maximises cache hits per route, what each lane actually sends, and what
is still missing. Measured against the reference clients: Codex CLI (open
source) and Claude Code (recovered bundle), plus the ZCode harness, whose cache
discipline is the most explicit of the harnesses available to read.

## What each route sends

| Route | Vendor mechanism | Borg wiring |
| --- | --- | --- |
| Anthropic API | Explicit `cache_control` breakpoints, at most four per request | One on the system block, one on the last tool definition, and one on the newest text so the marker advances with the conversation |
| Codex (ChatGPT subscription, Responses API) | Implicit prefix cache, keyed by `prompt_cache_key`, with turn-scoped sticky routing | Stable per-session key, deterministic `instructions`, `store: false`, reasoning items replayed through `provider_state`, and the `x-codex-turn-state` token from a turn's first response replayed on the rest of that turn |
| OpenAI-compatible family: Go gateway, Kimi, GLM, Qwen, OpenRouter, configured endpoints | Implicit prefix cache, keyed by `prompt_cache_key` for vendors that honour it | Same stable key; some profiles also send the session id |
| Claude subscription (Claude Code binary) | The binary owns its own breakpoints: a global-scope block before `__SYSTEM_PROMPT_DYNAMIC_BOUNDARY__`, an org-scope block after it, and a rolling message marker with a one-hour TTL | Borg supplies the system prompt, the pooled process and the prompt text; a reused process appends only the new turn. An idle process is kept for the cache's hour, four at most across the host, and model or effort changes are applied to the live process |

## Invariants

- **Append-only conversation.** Runtime context that varies between turns
  (skills and MCP availability, harness state, provider usage status) trails the
  prompt as `native_prompt_context`. It is durable conversation content, replayed
  where it was sent, so turn N+1's request extends turn N's byte for byte. Each
  slot is appended again only when its text changes, as Codex does with its
  reference context item. The prefix test in `native_harness.rs` rebuilds history
  through the journal's persistence and context rules; bypassing them is how an
  ephemeral classification broke this invariant unseen.
- **A cache key that does not rotate.** `native_prompt_cache_key` deliberately
  ignores context generation, system prompt and tools. A key that changed when
  the prefix changed would defeat the affinity it exists to provide.
- **One request shape per turn.** Every request a native turn sends, compaction
  included, is built from one template: system prompt, tools, cache key, session
  identity and turn routing. Compaction first asks for its checkpoint on the
  request the provider last cached, with the instruction appended, and falls back
  to the bounded text fold only after a length refusal, when the history leaves
  no room for the checkpoint, or when the reply is unusable.
- **Declaration transport.** `declaration_transport` states whether a system
  change moves the head: `InPlace` for chat completions, `Collapsed` for
  Responses and Messages, which hoist `System` into a top-level field.
- **Deterministic separators.** Audited across every adapter: the only
  whitespace introduced on the request path is constant (a double newline
  between hoisted system parts and between instruction blocks). No timestamp,
  counter or randomness reaches the prompt. Tool definitions are sorted before
  the request.
- **Cheapest reduction first.** Micro-compaction clears old tool results in the
  replayed view before any summary rewrites history.

## Measurement

Journal usage is per turn: `usage_updated` carries uncached, cached and cache
creation tokens. Codex CLI rollouts in `~/.codex/sessions` carry per-request
`token_count` events.

Before the fixes above (30 days to 2026-09-23), Borg's Codex lane cached 93.0% of
input against Codex CLI's 95.6%. Short follow-up turns cached only 80.7%, because
each turn dropped the previous turn's prompt context and re-read that turn
uncached, and 134 compactions re-read 16.1M tokens as text under a different
head. After them, a no-tool follow-up to a turn that read a 13k-token file cached
14,848 of 14,993 input tokens.

The Claude subscription lane cached 99.1% of reads; cache writes were 0.9% of
input. A cold process replays the whole history as one message, which cannot
match the previous process's cached layout; the avoidable cold turns clustered
at Borg restarts and cost about 5M write tokens over the period.

## Gaps, in the order they are worth closing

1. **Cold Claude processes after a restart or eviction.** The replacement
   process gets the canonical projection, so its first request rewrites a
   history the previous process still has cached. Beyond the four newest idle
   processes this happens to any session left idle. Resuming Claude Code's own session record would
   reuse it, at the cost of a provider-owned durable state Borg does not keep
   today.
2. **Volatile status in the Claude system prompt.** Provider usage percentages
   sit in the system block every new Claude process shares, so a changed figure
   rewrites that block for the next subagent or cold turn. Small today; it would
   move to trailing context as on the native lanes.
3. **Responses over WebSocket.** Codex CLI sends incremental input with
   `previous_response_id` over a persistent socket. Borg stays on HTTP, which
   the backend still caches by prefix; the socket mainly saves upload and
   latency.
4. **Per-request usage in the journal.** Usage is folded per turn, so a miss on
   one request is only visible as a lower turn ratio. Recording the provider's
   cached count per request would locate misses directly.
5. **Warming where no lifetime is documented.** Warming fires wherever a
   documented lifetime exists, which today is the direct Anthropic route. An
   operator who knows a vendor retention can declare `prompt_cache_ttl_seconds`
   for a configured provider.
