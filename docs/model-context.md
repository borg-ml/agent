# Read-only model context

`context` lets an agent inspect its own **model-facing projection** on
Borg-owned native model turns. It does not rewrite the canonical journal, undo
external actions, rerun historical tools, or change runtime permissions, account
credentials or billing. Compatibility CLI turns whose inner loop is owned by
another program cannot use this API.

The normal interface is message IDs and plain text, not reconstructed provider
JSON. Untouched messages retain attachments, tool arguments and opaque provider
replay state internally.

## Read

Invoke the model tool directly, or re-enter from an approved `exec` /
`runtime_exec` tool batch with the CLI:

```sh
borg call context '{"op":"read","limit":20,"max_chars":2000}'
```

The result contains `revision`, `entries`, `message_count`, `next_offset`,
`locked_tail_messages` and `estimated_tokens`. Each entry has an `id`, `index`,
`role`, `text`, `group_id`, attachment count, opaque-reasoning flag and tool-call
IDs/names. Estimates are not provider-measured usage. The protected tail contains
the response and tools currently executing and is not editable.

Page entries with `offset` / `next_offset`. Read one entry with `id`. Long text
has `text_length`, `text_offset` and `next_text_offset`; use the latter as
`text_offset` in the next read of that ID. Offsets count Unicode characters,
not bytes. `max_chars:0` reads metadata only. Limits are 1–100 entries and
0–64,000 characters per entry. Read fresh IDs after restart or compaction.

## Compaction-only history reduction

The agent-facing tool is read-only. `op:edit` is refused even under Full Access,
explicit workflow approval, or re-entry through `exec` / `runtime_exec`.
Agents cannot drop, replace, reorder or insert conversation messages or override
the system prompt through this tool.

Borg reduces conversation history through its normal compaction path. Summaries
must preserve whether human requests have already been answered or completed,
along with unfinished work and stop/pause constraints. A failed subscription
compaction keeps the full replay rather than falling back to selective message
omission; the provider can then surface the failure or input-size limit.

Older journals can contain `native_context_edit` checkpoints. They remain
replayable for recovery and forks; this compatibility does not permit new
agent-facing edits. The canonical journal and searchable evidence are unchanged.
