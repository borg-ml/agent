# Kimi K3 and OpenRouter native harness launch contract

Borg runs Kimi K3 through its provider-neutral native harness and the model's
OpenAI-compatible chat-completions API.

## Setup

Set `BORG_KIMI_API_KEY` (or `MOONSHOT_API_KEY`). The default API base is
`https://api.moonshot.ai/v1`; override it with `BORG_KIMI_BASE_URL` for another
compatible K3 deployment.

Start a local thread with:

```console
borg agent --provider kimi
```

The product model is `kimi-k3`. Supported reasoning efforts are `low`, `high`,
and `max`; Borg defaults to `max`. `BORG_KIMI_MAX_COMPLETION_TOKENS` can
override the 131,072-token completion allowance.

## Preserved-thinking contract

Kimi K3 requires prior assistant messages to be replayed intact, including
`reasoning_content` and `tool_calls`. Borg journals each native model message
and replays that typed message unchanged on later turns and tool rounds.
Interrupted incomplete tool rounds are discarded. Manual compaction replaces
older replay context with a durable continuation summary.

Automatic compaction is enabled for the native Kimi and OpenRouter harness.
Borg compacts when 10% of the model's effective input context remains, both
before a new user turn and after a completed tool round. The effective input
window subtracts the provider's advertised maximum completion allowance from
the raw context window, leaving room for the next response as well as the 10%
working margin. Kimi uses its known 1M-token raw window and configured
completion allowance. OpenRouter model limits are resolved from its live model
metadata API, with `BORG_OPENROUTER_CONTEXT_WINDOW_TOKENS` and
`BORG_OPENROUTER_MAX_COMPLETION_TOKENS` available for compatible gateways.

Each successful automatic boundary durably records the continuation summary,
trigger, pre-compaction occupancy, effective window, latency, and compaction
token usage. Replay restarts from that boundary without rolling truncation.
Failed compaction never replaces history and produces a normalized failure
event; a failure between tool rounds stops before another oversized request.

The 10% threshold follows Codex's current 90%-used auto-compaction ceiling.
For comparison, pi reserves 16,384 tokens, while OpenCode reserves the model
output allowance (or a 20,000-token compaction buffer). Borg uses an effective
window so large-output models receive at least their advertised completion
reserve rather than relying on a percentage alone.

The native harness supports:

- streaming text and reasoning;
- multimodal image attachments;
- built-in, Borg-owned, and external MCP tools;
- permission approvals and session-scoped grants;
- steering and interruption;
- structured output;
- retry/error telemetry, usage, cache, cost, and 1M-token context reporting;
- durable replay across CLI reconnects.

API credentials stay in the executing host environment. Enrolled hosts may
also use Borg's managed Kimi gateway; raw credentials are not persisted in the
thread journal.

## OpenRouter

Set `OPENROUTER_API_KEY`, then select any OpenRouter model slug:

```console
borg agent --provider open-router --model google/gemini-3-flash-preview
```

Borg defaults to OpenRouter's capability-aware `openrouter/auto` router when
`--model` is omitted. It does not maintain a stale allowlist: any current or
future OpenRouter `author/model` slug can be supplied with `--model`.

Borg does not force Kimi semantics onto OpenRouter models. It uses
OpenRouter's normalized `reasoning` parameter only when an effort is selected,
preserves both plain and provider-native reasoning blocks across tool rounds,
and accepts arbitrary model IDs. When tools, reasoning, or structured output
are requested, Borg asks OpenRouter to route only to endpoints supporting
those parameters.

The harness can launch every OpenRouter model, but agent actions inherently
require a model whose OpenRouter metadata includes `tools`. Image attachments
likewise require an image-capable model, and strict structured output requires
`structured_outputs` or `response_format`. A model without a requested
capability returns an actionable provider error; Borg does not silently drop
tools, images, reasoning, or schema constraints.

Optional routing controls:

- `BORG_OPENROUTER_BASE_URL`: OpenRouter-compatible API base, primarily for
  gateways and deterministic integration testing;
- `BORG_OPENROUTER_PROVIDER_ORDER`: comma-separated provider preference;
- `BORG_OPENROUTER_ALLOW_FALLBACKS`: whether OpenRouter may use later
  providers;
- `BORG_OPENROUTER_RESPONSE_FORMAT`: `json_schema`, `json_object`, or `none`.
