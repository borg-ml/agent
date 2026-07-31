# Claude native protocol (stream-json + control_request)

Reference for the native Rust port of the Claude Agent SDK integration, replacing the
Node sidecar at `packages/borg-claude-sdk`.

Extracted from `@anthropic-ai/claude-agent-sdk@0.3.220` (`sdk.d.ts`, `sdk.mjs`) and
**verified live** against `claude` 2.1.220 (`manifest.json` `sdkCompat.harnessSchema: 1`).

> The `@anthropic-ai/claude-agent-sdk` npm package is proprietary ("© Anthropic PBC.
> All rights reserved"). This document records the observable wire protocol only —
> the framing, flags, and message shapes needed to talk to the binary. It contains no
> copied source. The MIT-licensed `anthropics/claude-agent-sdk-python` wrapper is a
> secondary reference for the same protocol.

## Transport

Line-delimited JSON both directions over the child's stdin/stdout. One JSON value per
line. Stderr is diagnostics only.

The stream is a **multiplexed demux**, not a pure message stream. Frame types on stdout:

| `type` | Direction | Meaning |
|---|---|---|
| `system`, `assistant`, `user`, `result`, `stream_event`, `rate_limit_event` | CLI → us | SDK messages — already parsed by `ClaudeStreamState` |
| `control_response` | CLI → us | Reply to a request we sent; correlate on `response.request_id` |
| `control_request` | CLI → us | CLI asking *us* (permissions, elicitation, hooks) |
| `control_cancel_request` | CLI → us | Abort an in-flight inbound `control_request` by `request_id` |
| `keep_alive` | CLI → us | Ignore |
| `transcript_mirror` | CLI → us | Ignore unless `--session-mirror` is passed |

Anything unrecognized must be ignored, not treated as an error — new frame types are
added without a protocol bump.

## Invocation

Base argv (always):

```
--output-format stream-json --verbose --input-format stream-json
```

`--verbose` is mandatory; the CLI rejects `stream-json` output without it.

Flags the sidecar's `Options` currently map to:

| Sidecar option | Flag |
|---|---|
| `model` | `--model <m>` |
| `effort` | `--effort <e>` |
| `permissionMode` | `--permission-mode <m>` |
| `allowDangerouslySkipPermissions` | `--allow-dangerously-skip-permissions` |
| `canUseTool` present | `--permission-prompt-tool stdio` |
| `outputFormat: {type:"json_schema"}` | `--json-schema <json>` |
| `mcpServers` | `--mcp-config <json>` — inline `{"mcpServers":{…}}` |
| `allowedTools` | `--allowedTools a,b,c` |
| `resume` | `--resume=<session_id>` (note: `=` form) |
| `persistSession: false` | `--no-session-persistence` |
| `includePartialMessages` | `--include-partial-messages` |
| `cwd` | process `cwd`, not a flag |

`--permission-prompt-tool stdio` is the mechanism that routes permission decisions to
us as inbound `can_use_tool` control requests. It is mutually exclusive with a named
permission-prompt tool.

`systemPrompt` is **not** a flag — it goes in the `initialize` handshake.

Env: the SDK sets `CLAUDE_CODE_ENTRYPOINT` (`sdk-ts`) and clears `NODE_OPTIONS`.
See open question below.

## The initialize handshake

Client-originated `control_request`, sent before or alongside the first user message:

```json
{"type":"control_request","request_id":"<opaque>",
 "request":{"subtype":"initialize","systemPrompt":["…"]}}
```

`request_id` is opaque and caller-generated — the CLI echoes it back. A UUID is fine.

Fields carried here rather than argv: `hooks`, `sdkMcpServers`, `jsonSchema`,
`systemPrompt` (array), `appendSystemPrompt`, `planModeInstructions`, `toolAliases`,
`excludeDynamicSections`, `agents`, `title`, `skills`, `promptSuggestions`,
`agentProgressSummaries`, `forwardSubagentText`, `supportedDialogKinds`.

The response (verified) carries `commands`, `agents`, `output_style`,
`available_output_styles`, `models`, `account`, `pid`.

Of these Borg only needs `systemPrompt` today; `jsonSchema` is passed via the flag.

## Control frames

**Outbound request:**
```json
{"type":"control_request","request_id":"…","request":{"subtype":"…", …}}
```

**Response (both directions) — success:**
```json
{"type":"control_response","response":{"subtype":"success","request_id":"…","response":{…}}}
```

**Response — error:**
```json
{"type":"control_response","response":{"subtype":"error","request_id":"…","error":"…"}}
```

Responses may carry `pending_permission_requests` / `pending_user_dialog_requests`
(prompt redelivery). Strip and ignore them.

### Subtypes Borg actually uses

Five of the 36 declared variants:

- `initialize` (out) — handshake above.
- `interrupt` (out) — response `{still_queued: string[]}`, plus `cancelled[]` when the
  request sets `cancel_queued: true`. Gated by capability `interrupt_receipt_v1` /
  `interrupt_cancel_queued_v1`.
- `get_context_usage` (out) — `{totalTokens, maxTokens, rawMaxTokens, model, categories}`.
- `can_use_tool` (**in**) — see below.
- MCP elicitation (**in**) — routed to `onElicitation` in the sidecar.

### `can_use_tool` (inbound) — verified live

Request fields observed: `subtype`, `tool_name`, `input`, `tool_use_id`,
`display_name`, `description`, `permission_suggestions`. Declared but situational:
`blocked_path`, `decision_reason`, `agent_id`, `matched_ask_rule`.

`display_name` and `description` are **normally present** — verified live. The
synthesized `"Use <tool>"` / `"Claude requested permission to use <tool>."` wording is
a degraded path, not the common one. Test fixtures that omit these fields exercise
only the fallback and will disagree with the binary.

Reply payload is the permission result plus `toolUseID`:

```json
{"type":"control_response","response":{"subtype":"success","request_id":"…",
  "response":{"behavior":"allow","updatedInput":{…},"toolUseID":"…"}}}
```

Deny: `{"behavior":"deny","message":"…","interrupt":true?}`.
Session-scoped approval: `updatedPermissions` carrying the request's
`permission_suggestions`.

Note `updatedInput` — the current sidecar never sets it, so "approve with edits" is
not expressible today. The native port should thread it through.

Every inbound request must be answered or the turn hangs. `control_cancel_request`
withdraws one; drop the pending entry and do not reply.

## Capability negotiation

`system/init` advertises a `capabilities` array — observed on 2.1.220:
`interrupt_receipt_v1`, `interrupt_cancel_queued_v1`, `msg_lifecycle_v1`. Gate optional
behavior on membership rather than on binary version.

`manifest.json` carries `sdkCompat.harnessSchema` (currently `1`) — check it at
binary-resolution time and refuse to run an unknown schema.

## Verified behavior

Spike (`/tmp/spike_control.py`, HOME sandboxed to defeat the local allowlist):

- Direct spawn, no Node, no wrapper.
- Subscription auth works — `system/init` reported `apiKeySource: "none"`, reading
  `~/.claude/.credentials.json` from the sandboxed HOME.
- `initialize` → `control_response success` with the full payload.
- `Write` tool → inbound `can_use_tool` → our `allow` → tool executed, file written.
- `result` subtype `success`, exit 0.

Note: `echo` via Bash is auto-approved by the CLI's built-in safe-command
classification and does **not** produce a `can_use_tool`. Permission-path tests must
use a genuinely gated tool (e.g. `Write`).

## Scope notes

- **No in-process MCP bridging needed.** Borg's MCP servers are stdio `command`+`args`
  (`crates/borg-provider/src/mcp.rs`); the CLI spawns them itself. The `sdkMcpServers` /
  `mcp_message` machinery — the hardest part of the protocol — is not on our path.
- The message-stream half is already reimplemented in `claude_stream.rs`. The port adds
  the control half; it does not introduce a new dependency on undocumented behavior.
- The ~270 MB `claude` binary remains either way — it ships as the platform
  optionalDependency `@anthropic-ai/claude-agent-sdk-<platform>`. The port removes the
  Node runtime, the npm wrapper dep, the `dist/provider.js` build step, and
  wrapper-vs-binary version skew.

## Open questions

1. **`CLAUDE_CODE_ENTRYPOINT`.** The SDK sets `sdk-ts`. The spike used `sdk-rs` with no
   observable difference, but the value may gate features (`fast_mode_disabled_reason`
   came back `sdk_opt_in_required`). Decide deliberately before shipping.
2. **Fast mode.** The sidecar sets `settings.fastMode` / `fastModePerSessionOptIn`,
   which reach the CLI through the generic settings passthrough, not a dedicated flag.
   Confirm the flag spelling used by `--settings`.
3. **`--json-schema` vs `initialize.jsonSchema`.** Both exist. The SDK passes the flag;
   confirm precedence and whether structured-output retries behave identically.
