# Model fallback chains

A session can run on an ordered chain of models. When the model it is on
reaches a usage limit (a 5-hour window, a weekly cap or a billing limit), Borg
records when that limit resets, switches to the next model in the chain that
has quota, and continues the same turn at once. At later turn boundaries it
returns to the earliest model whose limit has reset. Only when every model in
the chain is limited does the session wait, and then for the one that resets
first. This is what lets a `/goal` run unattended around the clock.

```toml
[models]
fallback = ["claude-opus-5-5@max", "gpt-6-sol@xhigh", "opencode-go/deepseek-v4.1"]
```

## Routes

Each entry is a route: `model`, `model@effort`, `provider/model@effort` or
`provider@effort`.

- `claude-opus-5-5@max` runs Claude (subscription) at `max` effort.
- `gpt-6-sol@xhigh` runs Codex at `xhigh` effort.
- `opencode-go/deepseek-v4.1` runs OpenCode Go's DeepSeek model.
- `anthropic/claude-opus-5-5` names the Anthropic API lane explicitly, which is
  a different route from the Claude subscription even for the same model.

## Composing chains

Named chains live under `[models.chains]` and are included with
`chain:<name>`, anywhere in `fallback` or in another chain. Includes may nest;
a chain that includes itself is rejected when the config loads, and a route
listed twice keeps its first position.

```toml
[models]
fallback = ["claude-opus-5-5@max", "chain:subscriptions", "chain:cheap"]

[models.chains]
subscriptions = ["gpt-6-sol@xhigh"]
cheap = ["opencode-go/deepseek-v4.1"]
```

## Billing

A route never spends pay-as-you-go API credit unless it says so. When the host
reports that a provider is only reachable with an API key, a plain route on it
is skipped. Opt a route in with a table entry:

```toml
[models.chains]
paid = [{ route = "anthropic/claude-opus-5-5@high", allow_api_billing = true }]
```

## What you see

Each switch is journaled and shown in the status line ("… reached its usage
limit; continuing this turn on …"), and the model shown in the status row
follows the session. Subagents inherit the chain from their root session. The
reset deadlines are journaled too, so a restart does not retry a model that is
still exhausted.
