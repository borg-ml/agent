# LSP resource lifecycle and idle cleanup

Borg's shared LSP broker owns disposable language-server children. Do not
kill or restart the session host to reclaim an indexer.

- `lsp_status` reports session-scoped PID, RSS when available, idle seconds,
  and open-document count without renewing idle time.
- Targeted document operations retain an eight-document LRU working set and
  send `didClose` on eviction. This bounds open-document ASTs, not total index RSS.
- Ordinary idle eviction is ten minutes. On Linux with MemAvailable below
  8 GiB, request-idle clients can be evicted after 60 seconds. Running and queued
  requests are protected; idle age includes protocol activity and completion.
- `lsp_release_idle({"idle_seconds":60})` releases clients leased by the calling
  session that are not shared with another live service/session. Busy and queued
  clients are skipped. The minimum is 60 seconds; the reply explains skips.

Cleanup uses owned child lifecycle handling, not process-name scans. A later
query recreates the language server. Background indexing and simultaneously
active workspaces can still consume substantial memory.

The broker protocol/socket is v3. Old brokers are not terminated or taken over;
installing a new binary does not upgrade an already-running session host.
