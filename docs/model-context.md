# Agent-controlled model context

`context` lets an agent inspect and edit its own **model-facing projection** on
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

## Edit

Edits require Full Access or explicit approval, a revision from a read, and an
active model tool batch. Requests during model generation or compaction fail.
A batch of 1–100 operations is atomic: invalid IDs, stale revisions or invalid
message structure leave the context unchanged.

```json
{
  "op": "edit",
  "revision": "<revision from read>",
  "edits": [
    {"op": "replace", "id": "<old message ID>", "text": "Concise retained evidence"},
    {"op": "drop", "ids": ["<obsolete message ID>"]},
    {"op": "insert", "role": "user", "text": "Current working notes", "after": "<kept ID>"},
    {"op": "move", "ids": ["<message ID>"], "before": "<destination ID>"}
  ]
}
```

- `replace` changes text while retaining the message role and attachments.
  Replacing assistant text clears its now-stale signed/opaque reasoning, but
  preserves its tool calls.
- `drop` and `move` expand any selected tool call/result to its complete group.
  Selected groups keep their relative order. Tool calls and all results must
  remain adjacent, with unique call IDs and no orphan results.
- `insert` accepts text with role `user` (default), `assistant` or `system`.
  Omit an anchor to append; supply only one of `before` / `after`. Extra system
  rows may be hoisted into instructions by provider adapters.
- The leading system slot stays first: replace its text, including with `""`
  to disable it, rather than dropping or moving it. Explicitly changing this
  slot overrides the full generated system prompt until context clear/revert
  supersedes it. History-only edits leave dynamically generated Borg guidance
  alone and preserve any previously chosen explicit system override.
- Keep a user message before assistant history. The editable prefix must end
  with a user message or tool result when an assistant response is protected;
  do not insert assistant text that would merge into signed thinking. Insert
  a user note when replacing the whole working history.

A successful edit returns a new revision and applies to the **next model
request**. It preserves the in-flight tool batch and later results/steers.
Adjacent user text may be canonicalized; reread the resulting IDs before further
editing. Normal automatic compaction can subsequently reshape the projection.

## Advanced raw messages and durability

`replace` / `insert` may supply a typed provider-neutral `message` instead of
`text`; for example `{"role":"user","content":"notes","attachments":[]}`.
This escape hatch is optional and still validates roles, tool groups and IDs,
including collisions with protected calls. It is not a provider-wire request
or a way to alter tool declarations, permissions or authentication.

Borg journals `native_context_edit` with the materialized edited prefix and a
`preserve_tail_from` marker. Recovery takes the suffix from the canonical
journal, not a possibly lagging tool snapshot. Multiple edits, session forks
and completed compaction boundaries retain the chosen projection and explicit
system override. Context edits invalidate stale context-usage/continuation
anchors; accounting for already-billed model work is not erased.

The transcript and searchable evidence remain intact. If an external action's
outcome is uncertain, inspect that evidence before retrying; context editing
cannot make arbitrary network or shell effects exactly-once.
