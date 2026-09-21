# Anthropic API-key lane

## Purpose and boundary

Add a native Anthropic route: Borg agent loop driving the Anthropic Messages
API with the user own API key, billed per token to that key.

This is a new, explicitly selected billing lane. It does not touch the existing
Claude lane, which stays exactly as it is: the unmodified Claude Code binary,
signed in with the user own subscription. Anthropic published policy permits
that binary path and forbids third-party routing through Free/Pro/Max
credentials, so subscription tokens are never replayed by this lane, and the two
routes must never be selected implicitly for one another.

Backing for treating these as separate lanes rather than one Claude entry:
CodingProvider::Claude is a CLI-route provider in Borg today and every session on
it carries the CLI provider session id. Switching its route per model would need
the durable route machinery OpenCode has, for a provider with live sessions, and
would put a billing-lane decision behind a model switch. A distinct provider
keeps the choice explicit, the way Kimi, GLM, Qwen and OpenRouter are already
native-only entries.

## Verified current state

- The adapter exists as crates/borg-provider/src/provider/anthropic_messages.rs
  and is routed: CodingProvider::Anthropic resolves to
  NativeRoute::AnthropicMessages, which no other provider shares. Before this
  change the repository had no Anthropic HTTP client at all.
- Claude is the CLI subscription lane only: run_claude_chat_stream and
  run_claude_chat_stream_with_control reach run_subscription_stream(...,
  SubscriptionProvider::Claude, ...) and the pooled claude_agents CLI.
- The credential kind already exists: ApiKeyCredential::Anthropic reads
  ANTHROPIC_API_KEY and stores under anthropic_api_key.
- prompt_cache_lifetime already matches claude- model ids and returns Anthropic
  documented 300s retention, so cache warming needs no new policy.
- Pricing is OpenAI-only: openai_model_pricing covers gpt-5.5 and the Codex
  product model. Anthropic prices must be added for cost_basis to report.
- No Anthropic key is configured on this host, so a live smoke cannot run until
  one is.

## Insertion points

| Concern | Where |
| --- | --- |
| Wire adapter (new module) | crates/borg-provider/src/provider/anthropic_messages.rs |
| Route selection | ProviderModelClient::route in borg-provider, new NativeRoute::AnthropicMessages |
| Turn dispatch | impl NativeModelClient for ProviderModelClient in crates/borg-agent-runtime/src/native_harness.rs |
| Provider entry | CodingProvider in contract.rs: variant, label, uses_native_harness, supports_active_turn_steer |
| Cost basis | anthropic_model_pricing beside openai_model_pricing in borg-provider provider/mod.rs |
| Auth | ApiKeyCredential::Anthropic; the capability entry must report billing api_key |
| Login | the API-key login path the other keyed providers use, plus the /login prompt |

context_window and declaration_transport in the same dispatch impl each need an
arm for the new route: the first so the context meter and auto-compaction work,
the second because the Messages API collects System into a top-level system
field, which is the Collapsed shape.

## Wire mapping

| Anthropic | Borg |
| --- | --- |
| system | system prompt, top level rather than a message |
| messages content blocks | ModelMessage text, tool_use, tool_result |
| tools input_schema | ModelToolDefinition |
| max_tokens | required by the API; set from the output budget, never omitted |
| thinking | effort mapping; thinking blocks stream as reasoning |
| content_block_delta text, thinking, input_json | text delta, reasoning delta, tool-call assembly |
| message_start and message_delta usage | input, cache_creation, cache_read, output tokens |
| stop_reason | finish reason |
| overloaded_error and 5xx | transient, retry |
| invalid_request_error | fatal, retrying cannot help |
| prompt-too-long refusal | ProviderErrorKind::ContextLength so the harness compacts |

Prompt caching uses explicit cache_control breakpoints; the 300s lifetime above
already describes what a request can write.

## Test strategy

Unit tests with a local SSE fixture, the way the compatible adapter tests use
serve_sse_body: request shape, text and thinking deltas, tool-call assembly
across input_json_delta, usage mapping, and error classification. These need no
key and run in CI. A live two-turn smoke, checking a cache read on the second
turn, tools and nonzero usage, needs a configured key and belongs in the
credentialed smoke set.

## Implementation status

Landed, in this order:

1. The Messages adapter, with fixture tests over the request shape, attachments,
   the thinking budget, the stream mapping, frame splitting across a chunk
   boundary, and refusal classification.
2. The route, the four dispatch arms, the provider variant and its metadata,
   plus the capability snapshot that always reports the API-key lane and never a
   subscription on this provider.
3. Selection end to end: the CLI provider argument, the reserved config id, and
   a login path that stores the key.

Remaining, each with what it would take:

- **Credentialed smoke.** Needs a key on the host; none is configured. Two turns
  in one session, tools, nonzero usage.
- **Pricing.** The lane reports tokens with an unavailable cost basis rather than
  a dollar figure nobody verified. Adding it means transcribing current published
  rates, which this environment could not check.
- **A pinned default model.** Deliberately absent: naming a model id this code
  cannot confirm would fail on the first call instead of asking the operator.
- **Prompt-cache breakpoints.** The adapter sends none, so the route is refused
  for cache warming with CacheWriteUnsupported. Sending cache_control breakpoints
  is what would make warming meaningful here.
- **Vendor OpenAI-compatibility shim.** Not chosen. It would be far less code,
  but it is a reduced-fidelity path and its current support for streaming, tool
  calls and prompt caching was not verifiable when this was written.

## Open questions

- Native Messages adapter as above, versus pointing the existing generic
  OpenAI-compatible profile at Anthropic OpenAI compatibility shim. The shim is
  far less code but is a reduced-fidelity path, and its current support for
  streaming, tool calls and prompt caching needs checking against Anthropic live
  docs before choosing it. That check was not possible in the session that wrote
  this note, because the search tool was unavailable.
- Which Claude API model ids to catalog, rated at the current published prices.
