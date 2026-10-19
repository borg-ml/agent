# Provider ownership

## Boundary

Borg owns the agent loop, context policy, tool execution, permissions, steering,
cancellation, subagents and durable state across providers. Access adapters own
only model transport, authentication, account selection, entitlement reporting,
and the continuation/cache state needed for subscription UX. A provider-owned
conversation must not become authoritative over Borg's journal.

## Current routes

Claude and Codex use Borg's native harness over direct HTTP/SSE model transports.
Neither route runs a Claude Code CLI, Codex CLI, app server, shared JavaScript
helper or upstream agent loop. Native model access also serves Anthropic, Kimi,
GLM, Qwen, OpenRouter, Vercel and configured OpenAI-compatible endpoints.
OpenCode Go models use the native route; other OpenCode routes, Grok Build and
Muse Code retain separate compatibility transports. Do not treat every OpenCode
credential as a Go subscription, or infer feature parity from catalog presence.

The runtime resolves the actual session route in
`crates/borg-agent-runtime/src/agent.rs`; provider defaults alone are not a
complete route inventory. Claude's subscription transport is in
`crates/borg-provider/src/provider/claude_native_client.rs` and Codex's in
`crates/borg-provider/src/provider/codex_model.rs`.

## Claude login

Run `borg login claude`, or `/login` in an idle Claude thread. Borg performs the
browser sign-in itself, normally using `~/.borg/claude`, separate from Claude
Code's rotating credentials. `BORG_CLAUDE_CONFIG_DIR` overrides the selected
directory. Borg persists that choice as `claude_subscription_config_dir` only
after successful sign-in;
abandoning login does not replace the working selection. Existing saved API
credentials are retained, not selected as a subscription fallback.

Login occurs on the executing host. Remote credentials are not copied through
Borg Remote. Refresh and replay remain bound to the selected account; opaque
continuations cannot be reused under a different account. Missing entitlement
or authentication must surface explicitly, never silently switch billing.

## Model and replay contract

Adapters normalize text, reasoning, complete tool calls, usage and errors.
Tool-input fragments may drive generation feedback before arguments are complete;
execution waits for a validated call. Usage and transport heartbeats are not
model progress. Cancellation must stop the addressed invocation without replaying
completed tool side effects or cancelling unrelated peers.

Native replay preserves signed thinking and encrypted Responses continuation
state. Context estimates charge known encrypted Responses reasoning overhead;
unrelated opaque signatures are not guessed. Provider-reported context is
authoritative where available. Catalog default, usable and maximum windows are
distinct: advertised maxima do not silently enlarge the selected window.

Compaction changes the model-facing projection, not the canonical journal.
See [Model context](model-context.md), [Provider parity](provider-parity.md), and
[Prompt cache posture](prompt-cache-posture.md). Subscription support must retain
login/recovery, model capabilities, streaming, cache behavior and usage visibility
without API-key substitution or an upstream agent loop.

## Tool execution policy

Every tool action starts synchronously and yields a background `watch_id` after
10 seconds if it is still running. `async: true` opts into immediate background
execution; `blocking: true` opts out of the automatic deadline. Setting both to
true is an error. `async: false` is equivalent to the default, not an unbounded
wait. These controls are shared across tools, including explicit waits, shell
commands, persistent runtimes, and external MCP calls; there is no eligibility
allowlist. Approval still gates execution.

Left-clicking a running foreground action yields it immediately, even when
`blocking: true`. This changes only where it is awaited: the same execution
continues without cancellation or restart. Background rows retain their running
state until their watcher completes, and later clicks inspect details instead
of toggling the execution back. Retrieve background results with `list_watchers`
and cancel only with `stop_watcher`.
