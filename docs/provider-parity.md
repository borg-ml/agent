# Provider parity contract

Borg keeps provider-specific wire protocols behind one durable session contract.
The target ownership boundary is defined in
[Provider ownership](provider-ownership.md). The table below describes today's
execution paths, including the external agent runtimes still being reduced.

Codex no longer runs an app server: the app-server adapters were removed, so the
Codex column below records the historical transport rather than a current path.
Codex now executes on the native harness and direct model transport like the
other native lanes.
Parity is checked at the boundaries where regressions matter:

| Contract | Codex app server | Claude Agent SDK | Native harness |
| --- | --- | --- | --- |
| Borg goal/plan/LSP/settings/plugin tools | external MCP catalog | external MCP catalog | `borg tools` / `borg call` over the dispatcher |
| Tool call/result normalization | `ChatStreamEvent` mapper | `ChatStreamEvent` mapper | one shell-first `exec` boundary |
| Steering | active-turn control | active-turn control | active model-round control |
| Queue/recovery | durable session actor | durable session actor | durable session actor + replay |
| Usage/context projection | normalized provider usage | normalized provider usage | normalized model usage |
| Compaction boundary | provider phase events | provider/session compaction path | native summary + replay boundary |

The tests deliberately use representative protocol fixtures and the durable
session actor, rather than asserting that each provider emits a non-empty string.
Provider adapter tests verify that equivalent MCP calls become the same
`ToolCall`/`ToolResult` shape and that Codex/Claude usage is projected into the
same billing buckets. Session tests verify that all active-turn lanes share
steering and queue semantics, while recovery/replay tests exercise FIFO queue
admission, compaction summaries, and native tool-round boundaries. Catalog
tests ensure the same Borg control plane is available whether tools are
transported over MCP or reached through the session-scoped Borg CLI. Live
provider smoke tests remain opt-in and are not required for the deterministic parity
suite because they depend on credentials, network availability, and mutable
provider behavior.

The focused regression commands are:

```text
cargo test -p borg-provider provider::chat_stream::tests::codex_and_claude_normalize_mcp_tool_lifecycle_identically
cargo test -p borg-provider provider::chat_stream::tests::codex_and_claude_usage_maps_share_billing_buckets
cargo test -p borg-remote subagents::tests::every_execution_lane_exposes_the_same_borg_control_plane
cargo test -p borg-remote session::tests::active_provider_steer_uses_turn_control_across_provider_lanes
```

## Runtime supervision

The session actor owns progress supervision for root and child turns across
providers. After one minute without model progress it reports the quiet state;
at five minutes it reports “possibly stalled” without terminating the turn.
A silent model fails after twenty minutes by default. Set
`BORG_PROVIDER_STALL_TIMEOUT_SECS` to change that budget (`0` disables model-stall
failure, not status reporting). Usage counters and provider metadata do not count
as model progress. Startup and drain remain bounded separately.

In-flight tools retain a separate two-hour silence budget. Human approvals and
provider questions pause the watchdog until answered. Both monotonic and wall
clocks are checked so suspend/resume cannot hide an already-expired budget.
After detected suspension or a long watchdog scheduling pause, startup and active
provider waits get a 60-second reconnection grace period before the original
deadline is enforced. Normal polls do not renew this grace; real progress resets
the silence timer. Cancellation and drain deadlines are not extended.
Timeouts publish a durable failed turn; they do not blindly replay a possibly
side-effecting tool. The session remains available for recovery, and cleanup
failure is reported rather than leaving the turn indefinitely running.

Dropping a subscription stream cancels the entire provider invocation, including
handshakes and nested runtime tasks. The CLI sleep-prevention setting also covers
active children when the parent is idle; disabling the setting still wins.

The deterministic subprocess regression needs no credentials or network:

```text
cargo test -p borg-provider --features subscription-adapters --test subscription_cancellation
```
