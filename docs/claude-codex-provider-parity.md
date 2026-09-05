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
| Session continuation | Thread resume and pooled thread | Native `--resume` and persistent stream-json process | Implemented |
| Process/session reuse | Pooled app-server | Persistent native Claude stream-json process | Implemented with a keyed local Rust pool |
| Steer active turn | `turn/steer` | Streaming input supports additional user messages | Implemented |
| Interrupt active turn | App-server interrupt | `Query.interrupt()` | Implemented |
| Permission responses | App-server control responses | `canUseTool` callback and dynamic permission mode | Implemented, including session grants |
| Provider interactions | App-server interaction requests | SDK MCP elicitation callback | Implemented |
| Context telemetry | Token notifications | `getContextUsage()` and usage messages | Implemented |
| Manual compaction | `thread/compact` | No public direct compact method; `/compact` can be sent as session input | Implemented through resumed `/compact` turn |
| Cancellation cleanup | Interrupt plus app-server shutdown | `interrupt()` and `close()` | Implemented; receipt/cancellation cleanup is confirmed before pool reuse |
| Prewarm | Local app-server prewarm | Native Claude process can be kept alive | Native process is retained after the first turn |
| Runtime packaging | Codex binary is discovered directly | Claude binary plus `claude-agents` Rust runtime | Native binary is discovered from installed/release locations; Rust runtime is imported from the standalone crate |

## Acceptance criteria

Claude reaches practical parity when:

1. A pinned Claude binary is installed or packaged by supported Borg install and
   release flows without relying on an untracked local file.
2. A local session keeps one controllable native Claude runtime alive across turns
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
   attachments, MCP, permissions, resume, control races, cancellation,
   malformed/provider-error events, and packaged native-payload resolution.

Known protocol differences include Claude's lack of a dedicated public
compaction method equivalent to Codex `thread/compact`, and Codex app-server's
lack of generic tool-input fragments before a complete call. Claude compaction
uses `/compact` and observes its boundary; Codex generation feedback must not
be inferred from reasoning or tool completion. See
[Provider ownership](provider-ownership.md) for the intended model-level boundary.

## Verification map

Release verification covers the native Rust runtime and the standalone
`claude-agents` crate:

| Acceptance area | Regression evidence |
| --- | --- |
| Native command/protocol and pooled reuse | `claude-agents` fake-runner integration tests |
| Resume, attachments, schema, permissions | `claude-agents` request construction and control-channel tests |
| Steer and interrupt delivery | `active_provider_steer_uses_turn_control_for_codex_and_claude` plus `claude-agents` control tests |
| Permission and MCP interactions | `claude-agents` approval and elicitation normalization tests |
| Context and final usage | `claude-agents` usage tests, `claude_context_usage_is_available_before_turn_completion`, and `claude-agents::extract_usage` |
| Authentication recovery | `claude_auth_status_requires_an_authenticated_json_state` and `claude_auth_failures_have_one_actionable_terminal_message` |
| Reconnect reasoning visibility | `attached_reasoning_snapshots_become_incremental_deltas` and `attached_reasoning_accepts_a_new_snapshot_after_live_state_reset` |
| Cancellation and durable recovery | session interruption, queued-turn recovery, and incomplete-turn discard tests shared with Codex |

The release gate is:

```console
cargo check --workspace
cargo test --workspace
git clone https://github.com/borg-ml/claude-agents.git
cargo test --manifest-path claude-agents/Cargo.toml --locked
```

The native path needs the packaged or installed `claude` binary. The
`packages/claude-native-runtime` npm manifest is only used to fetch that
upstream platform binary; it is not a runtime adapter.

A credentialed smoke must then run two turns in one Claude thread, verify the
second turn reuses the native process/session, run `/compact`, and confirm nonzero
final usage. These smokes cover upstream authentication and service behavior
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
