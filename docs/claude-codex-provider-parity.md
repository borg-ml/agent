# Claude Agent SDK and Codex app-server parity

This document defines practical parity for Borg's coding-provider transports.
Parity means equivalent user-visible behavior and reliability; it does not
require identical provider protocols.

## Capability matrix

| Capability | Codex app-server | Claude Agent SDK | Borg status |
| --- | --- | --- | --- |
| Stream text, reasoning, tools, results | Native notifications | Native SDK messages | Implemented |
| Structured output | `outputSchema` | `outputFormat.json_schema` | Implemented |
| MCP tools | Dynamic app-server config | SDK `mcpServers` | Implemented |
| Session continuation | Thread resume and pooled thread | SDK `resume` | Implemented through the retained adapter/query runtime, with resume fallback |
| Process/session reuse | Pooled app-server | Streaming `Query` supports multi-turn input | Implemented with a keyed local SDK pool |
| Steer active turn | `turn/steer` | Streaming input supports additional user messages | Implemented |
| Interrupt active turn | App-server interrupt | `Query.interrupt()` | Implemented |
| Permission responses | App-server control responses | `canUseTool` callback and dynamic permission mode | Implemented, including session grants |
| Provider interactions | App-server interaction requests | SDK MCP elicitation callback | Implemented |
| Context telemetry | Token notifications | `getContextUsage()` and usage messages | Implemented |
| Manual compaction | `thread/compact` | No public direct compact method; `/compact` can be sent as session input | Implemented through resumed `/compact` turn |
| Cancellation cleanup | Interrupt plus app-server shutdown | `interrupt()` and `close()` | Implemented; incomplete pooled turns are discarded |
| Prewarm | Local app-server prewarm | SDK module/runtime can be kept alive | Node/SDK runtime is retained after the first turn |
| Adapter packaging | Codex binary is discovered directly | Borg TypeScript adapter plus pinned SDK | Added in-tree and included in release archives |

## Acceptance criteria

Claude reaches practical parity when:

1. A pinned, type-checked adapter is installed by supported Borg install and
   release flows without relying on an untracked local file.
2. A local session keeps one controllable SDK query runtime alive across turns
   when its workspace, model, permissions, and routing configuration remain
   compatible.
3. steer, interrupt, permission decisions, and shutdown have acknowledgements
   and deterministic terminal behavior.
4. provider session IDs resume safely, with a full-context fallback rather
   than silently losing the durable Borg conversation.
5. live context usage, final usage, tool activity, SDK errors, and auth errors
   map into the same provider-neutral event contract used by Codex.
6. manual compaction either uses a supported SDK control or sends `/compact`
   through the active streaming session and confirms its compact boundary.
7. unit and integration tests cover messages, tools, structured output,
   attachments, MCP, permissions, resume, control races, cancellation, and
   malformed/provider-error events.

The only currently known protocol difference is that the Claude SDK does not
expose a dedicated public compaction method equivalent to Codex
`thread/compact`; Borg must use the provider's `/compact` session command and
observe the resulting boundary.

## Claude authentication and recovery

Claude uses the Claude CLI's account login rather than storing credentials in
Borg:

1. Run `borg remote login claude`, or enter `/login` in an idle Claude thread.
2. Borg runs `claude auth login` in a normal terminal so the interactive flow
   can complete.
3. Borg then verifies `claude auth status --json` reports `loggedIn: true`.
4. Retry the message. Provider authentication failures are normalized to an
   actionable `/login` prompt.

The same flow runs on the machine executing the agent. For an enrolled remote
host, log in on that host; credentials are not copied through Borg Remote.
Requests carrying scoped provider or git credentials bypass the reusable
Claude process pool so credentials cannot leak into a later turn.
