# Provider parity contract

Borg keeps provider-specific wire protocols behind one durable session contract.
Parity is checked at the boundaries where regressions matter:

| Contract | Codex app server | Claude Agent SDK | Native harness |
| --- | --- | --- | --- |
| Borg goal/plan/LSP/settings/plugin tools | external MCP catalog | external MCP catalog | direct dispatcher catalog |
| Tool call/result normalization | `ChatStreamEvent` mapper | `ChatStreamEvent` mapper | native tool runtime |
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
transported over MCP or called in-process by the native harness. Live provider
smoke tests remain opt-in and are not required for the deterministic parity
suite because they depend on credentials, network availability, and mutable
provider behavior.

The focused regression commands are:

```text
cargo test -p borg-provider provider::chat_stream::tests::codex_and_claude_normalize_mcp_tool_lifecycle_identically
cargo test -p borg-provider provider::chat_stream::tests::codex_and_claude_usage_maps_share_billing_buckets
cargo test -p borg-remote subagents::tests::every_execution_lane_exposes_the_same_borg_control_plane
cargo test -p borg-remote session::tests::active_provider_steer_uses_turn_control_across_provider_lanes
```
