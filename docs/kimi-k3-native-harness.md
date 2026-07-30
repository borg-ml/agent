# Kimi K3 native harness launch contract

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
