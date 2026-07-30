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

## Verification map

Release verification is split between the in-tree TypeScript adapter and the
provider-neutral Rust runtime:

| Acceptance area | Regression evidence |
| --- | --- |
| Adapter protocol and pinned SDK | `npm ci`, `npm run check`, and `npm run build` in `packages/borg-claude-sdk` |
| Process reuse across turns | `pooled_claude_adapter_reuses_one_process_across_turns` |
| Resume, attachments, schema, permissions | `pooled_claude_config_preserves_resume_attachments_schema_and_permissions` |
| Steer and interrupt delivery | `active_provider_steer_uses_turn_control_for_codex_and_claude` plus adapter control tests |
| Permission and MCP interactions | `claude_adapter_permission_request_maps_to_provider_neutral_approval` and `claude_adapter_elicitation_maps_to_provider_neutral_interaction` |
| Context and final usage | `claude_adapter_context_usage_maps_to_transient_provider_telemetry`, `claude_context_usage_is_available_before_turn_completion`, and `claude_usage_accepts_assistant_and_result_envelopes` |
| Authentication recovery | `claude_auth_status_requires_an_authenticated_json_state` and `claude_auth_failures_have_one_actionable_terminal_message` |
| Reconnect reasoning visibility | `attached_reasoning_snapshots_become_incremental_deltas` and `attached_reasoning_accepts_a_new_snapshot_after_live_state_reset` |
| Cancellation and durable recovery | session interruption, queued-turn recovery, and incomplete-turn discard tests shared with Codex |

The release gate is:

```console
just claude-sdk
cargo check --workspace
cargo test --workspace
```

A credentialed smoke must then run two turns in one Claude thread, verify the
second turn reuses the adapter/session, run `/compact`, and confirm nonzero
final usage. This smoke covers upstream authentication and service behavior
that deterministic tests cannot emulate.

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
