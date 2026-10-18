# LSP resource lifecycle and idle cleanup

Observed on the shared Abundance build host: independent worktree clangd
children of the shared Borg session host retained roughly 2–8 GB RSS each while
a director build waited for RAM admission. CPU use alone did not prove these
servers were abandoned: clangd can background-index after a request completes.
The broker is embedded in the elected Agent host, not a restartable daemon.
Do not kill/restart that host to reclaim an indexer.

## Confirmed defects and source fixes

* `lsp_status` used the analysis snapshot helper and refreshed every leased
  server's last-used timestamp. Polling status could prevent the existing
  ten-minute idle sweep indefinitely. Observation now does not renew TTL.
* Targeted document operations retained every opened document. There is now an
  eight-document LRU working set with didClose on eviction. Workspace fallback
  already closed its temporary documents; this is not an omitted workspace
  didClose diagnosis. This bounds open-document ASTs, NOT total index RSS.
* Under Linux host memory pressure (MemAvailable below 8 GiB), request-idle
  clients can be evicted after 60 seconds instead of ten minutes. Ordinary warm
  TTL stays ten minutes. Other hosts retain ordinary TTL when /proc is absent.
  Running or queued requests are protected by mutex state AND external lease
  references. Idle age accounts for protocol activity/completion, not merely
  request start; a long diagnostic does not become instantly expired at return.
* Session-scoped status exposes PID, RSS when available, idle seconds and open
  document count. It still does not expose another session's workspace list.

## Explicit cleanup (new hosts only)

`lsp_release_idle({"idle_seconds":60})` releases only clients leased by the
requesting session, unused for at least 60 seconds, and **not** shared with any
other live service/session. Busy requests and queued lease holders are skipped.
The reply lists released/skipped workspace keys and reasons. Child lifecycle
uses the same owned kill-on-drop as ordinary automatic eviction; it does not
stop the broker, scan process names, change authorization, or kill other tools.
A new query lazily recreates that workspace's disposable language server.

New clients use broker protocol/socket v3 so they cannot silently attach to a
v2 broker lacking cleanup/resource semantics. Existing v2 brokers are neither
terminated nor taken over. Source landing is not hot deployment: running old
session hosts and their server pools remain old until normally upgraded.
Starting a new pool is not proof the old pool was reclaimed.

## Current old-host operational boundary

The v2 broker has no unload RPC. Do not invent one or repeatedly poll its status
(the poll itself renews its idle TTL). Let it age naturally without queries.
For urgent resource admission, only the actual owning director/worker can
explicitly release their **own confirmed idle** child: check no pending query,
its workspace cwd, command, broker parent, and PID start identity, then use an
exact pidfd signal. Never use killall, a guessed PID, or stop another worker's
indexer based solely on RSS/CPU. This implementing worker signalled no process.
The director separately reported reclaiming only its own main-workspace server
by verified pidfd; that is operational evidence, not proof of this new source.

## Verification limitations

Rustfmt parses the edited sources; source-contract inspection and targeted
regression tests were added. The regression pool tests spawn no language server
and cover non-renewing status plus queued/shared-owner exclusion. Tests/builds
were NOT run. LSP requests were deliberately deferred while the director build
was blocked on memory, to avoid starting more expensive indexers. This is not a
claim of runtime verification or a fixed total RSS budget. Background index
memory and actively requested simultaneous workspaces can still be large.
