# Import threads and memory

Open `/import` in Borg, or run `borg import`. Pick a source, review the counts,
then select **Import**. **Threads** and **Memory** are both selected by default;
you can turn either off. Escape cancels the terminal preview without copying.
Import is also available from `/settings`.

| Source | Input |
| --- | --- |
| Codex CLI / Desktop | Shared Codex home (`CODEX_HOME`, otherwise `~/.codex`): session and archived-session JSONL, memory Markdown, memory database, and instructions |
| Claude Code | Claude home (`CLAUDE_CONFIG_DIR`, otherwise `~/.claude`): project transcripts, project memories, and instructions |
| Claude Desktop | Downloaded export ZIP or extracted export directory, including `conversations.json` and `memories.json` when available |
| Other apps | Portable JSON described below, optionally packaged with attachments in a ZIP |

Claude Desktop exports are available under **Settings → Privacy → Export data**.
See [Claude's export instructions](https://support.claude.com/en/articles/9450526-export-your-claude-data).
For Desktop and portable sources, the terminal prompts for an export path.
Quote paths containing spaces, for example:

```sh
borg import claude-desktop --path "$HOME/Downloads/Claude export.zip"
borg import codex --no-memory
borg import claude-code --no-threads
borg import codex --preview --json
borg import portable --path ./archive.json --yes --json
```

`--path` overrides local discovery too. `--preview` performs no import writes.
`--yes` skips the interactive preview. Without `--yes`, `--json` only reports
what was found. No model calls or network downloads are needed for import.

## What is copied

Threads appear in `/resume` with their source, original title, and message dates.
Borg copies available attachments into its own storage. Tool records become
historical text; they do not execute. Provider credentials, permissions, pending
approvals, and running processes are not imported. Resuming opens a new provider
conversation using the copied history, with manual permission mode initially.

Imports are snapshots. Stable source IDs make repeat imports skip existing
threads and memories, preserving any subsequent Borg edits. New source threads
are still copied. An existing thread is not silently replaced or merged when
its source changes. Local transcripts are staged one at a time, so large histories
do not have to fit in memory. Each thread is committed atomically; interrupted imports can
be rerun. Originals remain untouched.

Memory includes source memory files and account/project instructions. Copies are
stored as editable JSON under `$BORG_HOME/remote/imports/memory` (by default,
`~/.borg/remote/imports/memory`). Each entry retains its source and optional
project directory in `cwd`. Global entries apply to all projects. Entries with a
`cwd` apply only there. If a source project has no known local directory, its
`project` is preserved and it stays inactive until you set its `cwd` to an
absolute local directory. Delete an imported memory file to stop using it.
Current user instructions take precedence over imported context.

All copied memory remains on disk. Borg includes a bounded excerpt in the model
context with paths for reading additional entries. The importer reports missing
attachments, unreadable data, and unsupported formats. It does not claim to
recover cloud-only content absent from the export. Sources over the import size
limits are reported rather than silently truncated.

## Portable format

Use version 1. Source thread IDs must remain stable across exports. Message roles
are `user`, `assistant`, or `tool`; other roles are preserved as labelled
historical text. Dates use RFC 3339. Both arrays may be empty or omitted.

```json
{
  "version": 1,
  "threads": [
    {
      "id": "original-thread-id",
      "title": "Project discussion",
      "cwd": "/absolute/project/path",
      "messages": [
        {
          "role": "user",
          "text": "Continue the project",
          "created_at": "2026-09-01T12:00:00Z",
          "attachments": [{"name": "notes.txt", "path": "notes.txt"}]
        },
        {
          "role": "assistant",
          "text": "The next task is the settings screen.",
          "created_at": "2026-09-01T12:00:01Z"
        }
      ]
    }
  ],
  "memory": [
    {
      "source": "other-assistant",
      "source_id": "preference-1",
      "title": "Response preference",
      "content": "Prefer concise explanations.",
      "updated_at": "2026-09-01T12:00:00Z"
    }
  ]
}
```

Attachments may contain `data_base64` instead of `path`. File paths must remain
inside the export directory, including after resolving symbolic links. ZIP
entries with unsafe paths or links are rejected or reported. Files are limited
to 256 MiB each, archives to 1 GiB, individual attachments
to 64 MiB, and individual memory entries to 4 MiB.
