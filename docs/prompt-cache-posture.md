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

## Measurement and token definitions (verified 2026-09-24)

Borg's `input_tokens` means input that was neither read from nor written to the
cache. Cached reads and cache writes are separate counters. `total_tokens` is
all three input categories plus output. Reasoning is a subset of output, not an
additional charge to add to this total. These are processed tokens, not a
measurement of subscription allowance consumed. Codex CLI also presents a
blended total that subtracts cached input; comparing that display directly with
Borg's processed total exaggerates the difference.

`native_model_usage` now journals raw provider usage per request, including
reasoning and cache-write breakdowns where supplied. Records sharing a request
and provider response ID are cumulative snapshots: use the latest for that
pair, and check `complete`. A retry with a new response ID is a separate call. Interrupted
Claude streams can retain partial counters. A failed Codex response can retain
terminal usage even when its output is rejected. An absent counter is unknown,
not a measured zero. These audit events do not increment the existing
`usage_updated` turn totals and are not inherited into a fork's request audit.
The child's own journal holds its audit; its parent does not duplicate it.

Fresh matched adapter probes against Codex 0.156.1 did not establish a general
Borg cache failure. The simple warm follow-ups cached 98.00% in Borg and 98.92%
in Codex; the absolute uncached difference across those two follow-ups was 18
tokens. A controlled restart preserved warm caching in both. Earlier cold
Codex results were confounded by changed workspace context. Cache writes were
zero in these small matched samples; a nonzero-write SSE regression fixture
checks their normalization separately. These probes do not cover long coding
sessions, compaction, production tool catalogs, or subscription allowance.

The current Codex subscription adapter also follows the model catalog for
Responses Lite, reasoning summaries, verbosity, and supported effort updates.
Lite instructions and tool declarations have deterministic IDs. Borg persists
the original request effort and places trusted effort updates at their original
conversation positions, including the initial selection. Account-scoped HTTP
clients retain only allowlisted infrastructure cookies; they never retain
authentication cookies or send routing cookies to the API-key endpoint.

Three subsequent seven-request probes exercised a larger prefix, a tool
continuation, warm turns, an effort change, process recovery, and a fork. With
Codex's effort-update feature enabled, Borg reported 59,293 input tokens,
41,600 cached input, and 58 output; Codex reported 102,525 input, 72,576 cached,
and 61 output. Both reported zero cache writes. Their prompt overhead differed
by about 6,000 tokens per request, so raw totals are not an allowance comparison.
Borg had one unexplained warm cache miss; Codex missed on the fork. Earlier
runs used Codex's default-disabled effort-update feature and are not a clean
comparison for that behavior. These probes preceded the final initial-effort
marker correction, which has replay/serialization test coverage. They establish
successful continuation and recovery, not cache parity or an allowance saving.

The unmatched historical cohort (2026-08-25 through 2026-09-23 UTC) cached
93.231% of Borg Codex input and 97.536% of local Codex input. The local records
were predominantly VS Code sessions and used different workloads and models;
this is not a causal CLI comparison. The prior 95.6% baseline and 80.7%
short-follow-up figure could not be reproduced with an explicit matching
cohort. The 134 legacy compactions did reread 16,072,256 noncached input tokens.
One recorded compaction using the cached request instead read 220,032 cached
and 1,121 noncached tokens. The earlier follow-up with 14,848 cached of 14,993
input tokens was also reproduced from the journal.

Claude's unmatched historical input was 99.101% cached. The prior attribution
of roughly 5M writes to cold restarts was not independently established.
The shared-connector measurements and activation status are recorded in
[Claude shared model connector](claude-shared-model-connector.md).

Raw measurements, source versions, cohort queries, and the full audit are kept
in `/home/shulgin/.local/share/borg/assessments/2026-09-24-subscriptions/`.

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
4. **Codex intermittent warm misses.** Catalog-driven request conformance and
   infrastructure cookies are implemented. Some controlled warm calls still
   missed completely. The final effort-marker correction and transport effects
   have not been isolated as causes; byte-stable replay alone does not prove
   that the service will return a cache hit.
5. **Warming where no lifetime is documented.** Warming fires wherever a
   documented lifetime exists, which today is the direct Anthropic route. An
   operator who knows a vendor retention can declare `prompt_cache_ttl_seconds`
   for a configured provider.
