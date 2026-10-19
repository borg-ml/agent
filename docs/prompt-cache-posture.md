# Prompt cache posture

Borg keeps stable request prefixes and preserves provider-native continuation
state without delegating context policy to an upstream agent loop.

## What each route sends

| Route | Vendor mechanism | Borg wiring |
| --- | --- | --- |
| Anthropic API | Explicit `cache_control` breakpoints, at most four per request | One on the system block, one on the last tool definition, and one on the newest text so the marker advances with the conversation |
| Codex (ChatGPT subscription, Responses API) | Implicit prefix cache, keyed by `prompt_cache_key`, with turn-scoped sticky routing | Stable per-session key, deterministic `instructions`, `store: false`, reasoning items replayed through `provider_state`, and the `x-codex-turn-state` token from a turn's first response replayed on the rest of that turn |
| OpenAI-compatible family: Go gateway, Kimi, GLM, Qwen, OpenRouter, configured endpoints | Implicit prefix cache, keyed by `prompt_cache_key` for vendors that honour it | Same stable key; some profiles also send the session id |
| Claude subscription (native HTTP/SSE) | Explicit Messages cache breakpoints with one-hour retention; the native Rust transport supplies subscription authentication and required request attribution | Borg constructs stable instructions, sorted tools, breakpoint placement, lossless native history and thinking settings. Restart replays Borg's journal without a provider-owned session |

## Automatic warming

`[warming] mode` selects `off`, `streaming` (the default), or `idle`;
`BORG_CACHE_WARMING` overrides it. Refreshes replay the last request without
adding messages or executing generated tool calls. They stop on cancellation,
route/context changes, errors, or fixed one-hour active / thirty-minute idle
horizons. A refresh does not extend those horizons.

Claude subscription refreshes use the same pinned account, credential directory,
and native transport as real turns, with a one-token output cap and the original
one-hour cache markers. Adaptive thinking settings remain unchanged; budget-based
thinking is rejected before inference because reducing its output budget cannot
faithfully preserve the cached request. Subscription warming consumes quota, not
API billing: decision events label catalog-price comparisons as API-equivalent
estimates, and usage retains its subscription cost basis.

Codex automatic warming remains ineligible without established cache-expiry
semantics, matching Pi's current policy. Normal Codex prefix-cache reuse is
unaffected. We do not invent a retention timer or fall back to API credentials.

A bounded live Claude check is available through `claude_model_probe
--cache-refresh --cache-prefix`: two one-token refreshes verify subscription
accounting and a cached read on the second request. Run it deliberately; it uses
subscription allowance.

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
  replayed view before any summary rewrites history. Cleared tool IDs are durable
  and fork-inheritable, so a new turn or process reconstructs the same view while
  the original transcript remains available. Manual and between-turn compaction
  likewise recover the previous system/tools/cache key from `native_request_prefix`.

## Usage and verification

`input_tokens` is uncached input, excluding cache writes. Cache reads and writes
are separate counters; `total_tokens` includes all input categories plus output.
Reasoning is a subset of output. These are processed tokens, not measured
subscription-allowance debits or dollar savings.

`native_model_usage` journals raw request usage. Snapshots sharing a request and
response ID are cumulative: use the latest and check `complete`. A new response
ID is a separate call. Interrupted streams can have partial usage; absent
counters are unknown, not zero. Audit events do not increment turn totals again.

Warm-cache checks must match the model, account, instructions, tools and workload.
Byte-stable replay is necessary but does not guarantee a service-side hit.
Restart, fork, effort changes, long contexts and expiry need separate checks;
a bounded successful continuation does not establish general provider parity.
Codex uses full-history HTTP replay rather than incremental WebSockets.

Configured endpoints may declare `prompt_cache_ttl_seconds` when the operator
knows the vendor retention. Never invent an expiry timer or switch to API
credentials to warm a subscription request.
