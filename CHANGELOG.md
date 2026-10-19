# Changelog

User-visible changes. Release pages use these highlights and link to the full
Git comparison.

## Unreleased (since 0.18.0)

## 0.18.0 (2026-10-19)

- Load a user-wide `~/.borg/AGENTS.md` (or `CLAUDE.md`) in every session, ahead
  of the project's own guidance, like Codex's `~/.codex/AGENTS.md` and Claude
  Code's `~/.claude/CLAUDE.md`.
- Harness memory shared by a project's sessions now uses the scope name
  `project` (was `global`, which suggested user-wide) and is stored in
  `~/.borg/projects/` instead of a `.borg/` folder inside the repository.
- Add `show_image`: any agent or subagent can show PNG, JPEG, GIF or WebP
  images, with an optional caption, as a message in its transcript. Images are
  kept in the session store, so they still render after the source file is
  gone, and an image without a caption is no longer dropped.
- `borg login claude` signs in to a Borg-owned login (`~/.borg/claude`)
  instead of sharing Claude Code's, so the two no longer refresh one login and
  invalidate each other's tokens. The selection is saved only after the
  sign-in succeeds; until then the existing login keeps working.
- Keep project guidance, the session goal and the plan in context after a
  mid-turn compaction. Previously a long turn could lose AGENTS.md, skills and
  harness state after compacting, and the goal and plan survived only if the
  summary mentioned them. The unfinished goal now also accompanies every turn,
  not only goal continuations, and nested AGENTS.md files a session reached are
  restated on every request.
- Refuse `while`/`until` loops that wait on `pgrep -f`: they match the shell
  running the command and never end.
- `list_watchers` returns watcher status only; pass `watch_ids` to get those
  watchers' results. A long session's finished watchers no longer flood the
  context, and completion notices name the exact call. The retired
  `list_watches` and `stop_watch` aliases are removed; use `list_watchers` and
  `stop_watcher`.
- Stop bundling Claude Code: release archives drop the ~270 MB binary, since
  Claude runs on Borg's native subscription transport. `borg update` no longer
  installs `providers/claude`; an existing copy is left alone and can be
  deleted. Archives keep tiny placeholder files there only so older Borg
  versions can still update.
- Preserve images emitted before a shell command exits, including synchronous
  exec calls collected through a watcher, instead of intermittently losing
  `borg image` and screenshot attachments.

- Make context compaction recover from dead provider connections. Compaction
  requests now report streamed output, so a slow but working compaction stays
  alive and visible. A lost connection is retried up to three attempts with
  backoff, and an attempt silent for 15 minutes counts as lost. Auth, quota and
  other permanent errors still fail immediately, with history preserved.
- Let `inspect_agent` and `get_agent_goal` read any session on the host,
  including other instances' subagents, and show compaction status in
  inspection. Control actions remain limited to the caller's own tree.
- Deliver child interrupt and stop through a priority channel, so cancellation
  works even when a child's message queue is full.
- Keep reading process output while the session journal is slow, instead of
  stalling or misreporting an inherited pipe.
- TUI: show waiting agents with a distinct glyph, and count waiting subagents
  in the agents total; the label says "waiting" only when none are running.

## 0.17.1 (2026-10-18)

- Upgrade ZIP extraction to fix privileged permission and symlink handling vulnerabilities.
- Restore CI advisory checks with documented exceptions for unmaintained zbus 5 components required by AT-SPI and GPUI.

## 0.17.0 (2026-10-18)

- Animate active blue background actions with spinners. On actual completion,
  stop the spinner, restore the normal black background, and use past-tense
  labels. Keep nested background actions live until settlement, and prevent
  loading an old receipt from reviving their completed state.

- Prefer Codex subscription server-side compaction for eligible in-turn
  automatic compaction, preserving its encrypted continuation and recent turns
  through replay. Unsupported routes keep the text-summary fallback; no
  provider or billing route is changed.

- Fail over web search between configured Exa, Parallel, Firecrawl, and Brave
  backends when a provider is rate-limited or unavailable. Preserve query
  filters, result provenance, and useful errors without treating provider
  failures as empty results or silently using a keyless route.
- Avoid excessively fragmented context compaction: size requests against the
  model window, pack retained images together, and retry context-size refusals
  with bounded smaller requests without discarding history or lowering the
  selected reasoning effort.
- Count completed automatic-compaction passes as real progress, preventing the
  turn watchdog from cancelling productive compaction and restarting it from
  scratch. Repeated pass notices and empty heartbeats still cannot keep a stuck
  request alive.

- Unify subagent lifecycle and timeline semantics across TUI, native GUI, and
  remote views. Worker state is independent of report and operation completion;
  stale updates cannot roll back current state or approvals.
- Make child admission, stopped-worker configuration, and recovery durable.
  Repair missed activity without crossing parent boundaries or implicitly
  waking stopped workers.

- Settle Escape at turn boundaries so its stop flag cannot cancel the next
  message or leave it waiting for a provider that never starts. An Escape racing
  task completion now follows interrupt cleanup instead of reporting a cancelled
  task as a provider failure.

- Keep a provider/model switch made during a running turn from inheriting the old
  route's later usage-limit cooldown. Preserved work and fresh director tasks
  proceed on the selected route without waiting for the old provider to reset;
  explicit interrupts still cancel automatic retries.

- Make Escape in the TUI stop a running turn exactly like clicking the Running
  button. With queued follow-ups it used a separate soft "flush pending input"
  path; it now sends the same immediate interrupt. Menus and overlays still
  consume Escape first.

- Show an idle subagent that is parked on live watchers it started (a build,
  render or test it wakes itself for) as "waiting on 1 job: <watch label>"
  instead of "inactive", in the TUI roster and status line, the GUI team panel
  and the remote/web child summaries. Waiting agents count as active, are
  coloured apart from running ones, and the live subagent count line now reports
  `waiting=` separately. Ready-with-nothing-pending, failed, stopped and dormant
  agents stay inactive; `list_agents` status values are unchanged.

- Fix OpenCode children spawned without a model failing their first turn with
  "Model not found: opencode-go/beam-free". The default is now `BORG_OPENCODE_MODEL`
  or a preferred stable model validated against the live OpenCode Go catalog,
  never a free/preview id, and it is resolved at spawn time so an unresolvable
  model fails the spawn with a clear error.

- Prevent repeated human follow-ups from continually restarting Claude
  subscription thinking. The first steer remains instant; further input coalesces
  until a response completes, before unstarted tools run. Stop remains immediate,
  and OpenAI/Codex steering is unchanged.

- Let the director opt a child into user-desktop `computer_use` access with
  `spawn_agent {desktop: true, ...}`. Children remain private-display-only by
  default; grants persist across resume, are not inherited, and do not bypass
  approval or consequential-action confirmation.

## 0.16.8 (2026-10-16)

### Actions

- Preserve standalone MCP shell yield behavior and queue immediate stdin until
  an explicitly asynchronous shell has started. Its watcher/session identity,
  working directory, cancellation, and result retrieval stay intact.

- All tool actions now start synchronously and automatically continue in the
  background after ten seconds, without cancellation or restart. Use
  `async: true` to background immediately or `blocking: true` to wait until
  completion; the two opt-ins cannot be combined.
- Left-click a running foreground action to send it to the background
  immediately, including a blocking action. Hover shows the shortcut;
  background actions remain visible as running and subsequent clicks inspect
  details rather than toggling them back. Completed watcher actions no longer
  remain stuck showing “Running in background”.

### Streaming

- Preserve hidden reply anchors so tool rows cannot reorder the surrounding
  answer, and ignore stale snapshots without hiding real text repetitions.
- Bound receiver draining and compact retained live text across resize/replay
  without changing ordered events or rendered output.
- Show provider refusals as nonfatal error cards instead of invented assistant
  prose; preserve native replay and the existing continuation behavior.

- Keep reconnect preview snapshots current with raw text fragments so receiver
  lag no longer drops text or delays it until a later full snapshot.

- Append Claude text, thinking, and signature fragments in place instead of
  rebuilding the entire buffer on every delta. The same 1 MiB/16,384-fragment
  decoder fixture is 13–15× faster; provider output and progress are unchanged.
- Replay native signed Claude narration exactly once instead of synthesizing
  a second text copy. Preserve genuine text repetitions, edited context,
  signatures, tool repairs, and subscription continuation.

### Team control

- Use `/promote` in the focused child chat to hand over durable logical director
  authority and default input/stream routing. The physical host and stable
  paths stay fixed; context, model settings, permissions, goals, and plans stay
  with each session. Stale or ineligible targets fail without stopping work.
- Refresh authority at execution and preserve subscription tool discovery across
  promotion without restarting providers. Ordinary workers retain control of
  their own children but cannot control the director's protected subtree.

- Retry explicitly queued human prompts across transient owner IPC/ACK loss
  with the original IDs and FIFO order. Cancellation and settled-input guards
  prevent duplicate turns or reviving stopped work; controls retain priority.
  Notifications, promotion, and other controls are not automatically retried.

- Keep canonical team inspection scoped to persisted membership, including
  dormant sessions after a director handoff, without requiring a live roster.

### Performance verification

- Add an opt-in owned-PTY gate measuring emission-to-visible age of the latest
  assistant tokens during sustained streaming, including before/after binary
  comparison. Retained-history replay remains covered by rendering parity.

### Planning

- Allow multiple in-progress todos for the same assignee in both session and
  shared-work plans. Keep revision, assignment, identity, and size validation.

### Composer

- Preserve drafts rejected before send or before any agent output, including
  attached images, and validate image limits before taking the draft.
- Add configurable Ctrl+Z undo and Ctrl+Y / Ctrl+Shift+Z redo for the composer;
  save asynchronous pastes, dictation, and returned drafts to the sidecar too.

- Copy image attachments as image links without also copying their composer
  placeholder labels.

### Language tools

- Bound LSP queue waits and count startup and queueing against the workspace
  diagnostic budget. Abandoned requests cancel instead of blocking later work;
  incomplete analysis remains explicitly partial, never a false clean result.
- Preserve partial protocol reads and writes across cancellation, and use warm
  diagnostic waits after closing a document rather than repeatedly waiting for
  cold analysis.
- Updated processes use a versioned LSP broker without restarting existing
  agent hosts or changing analyzer features.

### Build reliability

- Restore release manifests and changelog on interruption instead of leaving
  a half-applied version bump after a cancelled or resource-limited build.

- Unblock queued lane work after recovery proves a quarantined job's workload
  is gone. Jobs with insufficient evidence remain quarantined rather than
  risking a second workload using the same resources.
- Verify and retry native Claude payload installation in CI so incomplete
  downloads do not leave release and platform builds without a working binary.

## 0.16.7 (2026-10-15)

### Claude subscription

- Fix Claude login failing with "temporarily throttled (HTTP 429)" and the
  subscription dropping out after a few hours. Anthropic's token endpoint
  rate-limits requests that identify as Claude Code, so Borg could neither
  finish a browser login nor refresh an expiring token on its own, and only
  stayed signed in while Claude Code itself was refreshing the shared login.
  Token requests now carry the same headers Claude Code sends.

## 0.16.6 (2026-10-15)

### Claude subscription

- Fix Claude browser login sometimes not staying signed in. Borg now saves the
  new credentials as soon as the browser callback's code is exchanged, before
  checking the account, so a rate-limited or failed account check (or another
  process touching the credential file) can no longer throw away a completed
  login. The account is verified on first use instead.

### Web search

- Fix keyless web search returning no results when Exa's free route is rate
  limited. The rate-limit notice is now an error that names the keyed backends
  configured on the host so the agent can retry with one of them.
- Switch the search provider without restarting. `web_search` takes an optional
  `backend` (auto, exa, firecrawl, parallel, brave, exa-mcp), and the
  `[search] backend` user setting changes the default from the next call; ask
  the agent or edit `agent.toml`. A keyed backend without its key fails with
  the missing variable named instead of switching providers.

### Storage

- Fix the Postgres session database growing without bound: every session
  projection and shared-work event wrote a never-read delivery row per
  workspace member (565 million rows, 283 GB on one machine). Only messages
  write delivery rows now; workspace-wide visibility comes from each member's
  join point. After upgrading every Borg process, remove the old rows with
  `borg session prune-deliveries --apply` (online, resumable) and/or return the
  space with `borg session reclaim-deliveries --apply` while sessions are idle.
  Both are dry runs without `--apply`.
- Add `borg session drop-test-databases` for leftover `borg_test_*` databases,
  and drop test scratch databases even when a test fails.
- Fix "sorry, too many clients already" and "opening the Borg session cluster
  timed out" when many sessions run at once or `borg update --force` overlaps
  old and new workers. The managed session cluster now starts with
  `max_connections=1000`, and a full server is waited out like a restart
  instead of failing startup. An already running cluster picks up the new
  limit the next time Borg starts it.
- Fix `borg update --force` reporting "replacement has not published the
  installed executable identity" after Ctrl-C while a new worker was still
  starting. It now says the session keeps starting in the background and to
  rerun the command for the remaining sessions.

## 0.16.5 (2026-10-14)

### Providers

- Fix "Claude subscription account/organization changed; reconnect explicitly"
  blocking a valid login after switching Claude accounts or organizations.
  Connecting now adopts the account the current token belongs to instead of
  failing on a stale saved binding.
- Allow continuing Claude conversations on a different account or organization
  than they started on, for example after hitting a usage limit. Borg no
  longer refuses to replay provider state across accounts.

## 0.16.4 (2026-10-14)

### Subagents

- Fix `wait_agent` and subagent counts treating ready children that are parked
  on a watcher as idle. They now count as running work.

### Security

- Update rustls to 0.23.46 for RUSTSEC-2026-0346 through RUSTSEC-2026-0349.

## 0.16.3 (2026-10-14)

### Providers

- Fix "Claude refresh lock busy" and "Claude account check temporarily
  throttled (HTTP 429)" errors interrupting turns. Each Claude token's account
  is now verified once per process instead of five to seven times per turn,
  and the refresh lock is no longer held during that check.
- Keep using a still-valid Claude token while another process refreshes it or
  a proactive refresh fails, and recover refresh locks left by a crashed
  process after 60 seconds instead of requiring manual recovery. This matches
  Claude Code.
- Restore fast time to first token on Claude subscriptions: request
  preparation dropped from about 2.5 s per turn to under 1 ms once the session
  is warm. Claude bootstrap and policy data and the Codex model catalog now
  refresh in the background instead of delaying a turn.
- Verify the subscription and load model metadata when a session starts, so
  the first turn does not wait on account checks either.

### Terminal UI

- Fix resume freezing for minutes, and using gigabytes of memory, in sessions
  with many subagents. Resume now loads only a few active subagents. Other
  subagents load their recent history when you first open them.
- Scroll back through a subagent's earlier history, as in the main
  transcript.

### Sessions

- Fix reverting a session with an active `/goal` immediately starting a turn
  on the goal. The reverted session now waits for your next message.

## 0.16.2 (2026-10-14)

### Terminal UI

- Fix a wall of "done · waiting for input" agent cards appearing at the end of
  the transcript after resuming a large team or recovering from a UI stall:
  child metadata now refreshes existing cards instead of adding one per
  settled child.
- Fix the status bar's team counts: the inactive count no longer stops at 64
  for larger teams, and a child blocked in a long tool call now shows as
  running. Child status is resynced from each child's journal every few
  seconds because status transitions are mirrored to the parent live only.

## 0.16.1 (2026-10-14)

### Remote

- Fix `borg` refusing to start with "this session is still owned by the
  background Borg remote host" on enrolled machines: sessions a local terminal
  registers for mirroring are no longer treated as relay-launched.
- Fix the `borg-remote` service crash-looping on its first TLS connection
  ("Could not automatically determine the process-level CryptoProvider").

## 0.16.0 (2026-10-14)

### Providers

- Add first-class Muse Wonder and Wonder Full selection with subscription-aware
  authentication and streaming.
- Use Borg-owned Rust HTTP/SSE transport for Claude subscriptions instead of
  the JavaScript connector, keeping model turns inside Borg without API-key
  fallback.
- Handle Claude subscription browser login, credential refresh, authentication
  status, and usage natively, without requiring the Claude CLI.
- Draft Claude commit messages through the model-only subscription provider
  rather than an external agent loop.
- Show Claude subscription usage reliably in `/usage` and `/status`: refresh it
  from every Claude response and keep the last known values, with their age,
  when Claude throttles a usage refresh.
- Retry throttled Claude account checks (HTTP 429/5xx), honouring Retry-After,
  and then reconnect instead of failing turns as if the subscription were
  unavailable.
- Move past Claude subscription quota refusals to the configured fallback
  instead of retrying them as broken connections.
- Finish native Claude refusals gracefully while preserving replay state.
- Stop suggesting reauthentication when a Claude account lookup merely fails.
- Give each retried model request its own identity and keep the cause of a
  dropped connection.
- Bound stalled native reasoning streams while still counting hidden transport
  activity as progress.

### Remote

- Rebuild remote sessions on one shared view: the terminal, the web client and
  the relay render the same projection, resume from cursors with bounded
  snapshots, and keep subagent reports private to their parent.
- Run hosts over a single authenticated tunnel. Commands, results and session
  registration flow through a durable command ledger, and hosts wake on relay
  and database notifications instead of polling.
- Answer approvals against the exact request revision, with acknowledgement
  and optional edited input, so a stale or replayed approval cannot run work.
- Store streamed text as append-only chunks, so long replies and reasoning no
  longer rewrite the whole message on every update.
- Serve optional direct TLS viewer sessions verified against native roots, and
  admit service tunnel connections with a signed purpose and proof.
- Reject insecure relay configuration before a daemon starts, and reconnect an
  overloaded host without cancelling its running sessions.
- Create remote sessions with the host's current provider, model and speed
  settings.

### Agents and tools

- Share background execution across safe tool calls, persistent runtime and
  shell commands. Slow calls detach after a short wait, expose retrievable and
  cancellable jobs in the UI, and group completion wakes. Optional `async`
  controls detachment while ordered mutations stay synchronous.
- Run message batches to distinct local recipients concurrently, without
  reordering messages sent to one agent under different names.
- Keep team plan reads off the workspace write lock.
- Let team reports and Borg's own notices wait for the tools already queued
  instead of cancelling them as "user steering"; only a person's message
  redirects a turn before its tools start.
- Keep shell and runtime batches that have already started running when the
  turn is steered; an explicit stop still cancels them.
- Keep finished watcher results retrievable for a bounded time after they end.
- Deliver peer messages to hosted children through their real owner, and return
  queued peer reports at explicit `wait_agent` boundaries.
- Let a parent control the goals of its own descendants.
- Keep subagent starts alive when the caller is cancelled, keep the command
  sender returned at startup, and report children that stop during startup.
- Restore declared argument types for direct capability calls and accept
  untyped JSON arguments.
- Resume admitted native work after a restart without duplicating its prompt.

### Terminal

- Recall pending input into the composer immediately and atomically, preserving
  attachments. Sending the edited text moves it back to pending on the first
  Enter, with recovery on failure and protection for text typed meanwhile.
- Preserve interrupted and redirected partial replies and abandoned tool-call
  outcomes across resume.
- Associate shell-edit receipts with the correct command when process events
  arrive before tool-start events.
- Make Esc flush pending input and interrupt as quickly as Esc stop.
- Color the compaction marker and “Compacted context” title orange, and render
  the compaction summary in a darker grey.
- Separate the compaction preview from its title with a wider gap, strip Markdown
  markers from the preview, and render Markdown in expanded summaries.
- Fold repeated action groups by default once a different action follows them
  or they reach five actions. The newest group under four actions stays open,
  and a group with a still-running action stays open.
- Avoid redundant immediate redraws during event bursts, and update elapsed-time
  labels without re-rendering the unrelated transcript tail.
- Use progressive and past-tense labels for more actions, including builds,
  compilation, verification, audits, and patch application.
- Show the "borging" status on 1 in 1000 runs instead of 1 in 100.

### Desktop

- Downscale oversized private-display screenshots instead of refusing them, and
  report the applied image scale.
- Keep private displays across launch cancellation and supervise launched app
  groups.
- Guard private-display rendering against GPU context resets, and advertise
  text-input-v3 so SDL Wayland apps accept typed text.

### Reliability

- Project team workspace events in background batches so busy subagent teams
  cannot hold back the root session's streamed output.
- Add explicit `borg update --force` handoff of verified obsolete local workers,
  including same-version local rebuilds. Plain updates do not interrupt work.
- Acknowledge stop requests and abort generation independently of slow journal
  writes and full command queues. Save interruption state and clean up tools
  separately, without waiting indefinitely for an aborted turn.
- Cancel a running tool when its turn is aborted, while preserving commands
  intentionally left running after a successful background yield. A stop no
  longer latches beyond the turn it cancelled.
- Preserve acknowledged journal commits and interrupted assistant text.
- Bound native process output stored in journal previews.

### Reasoning

- Preserve reasoning-only encrypted GPT Responses and signed or redacted Claude
  output through continuation and replay repair, rather than dropping assistant
  messages with no visible text or tool calls.
- Keep opaque GPT reasoning during replay without resurrecting native tool calls
  that request repair removed.

### Tooling

- Preserve repeated Cargo package selectors in native workflows and check every
  selected runtime package for the PostgreSQL fixture requirement.
- Preserve out-of-memory evidence from lane jobs before their scope is
  collected.
- Update `quinn-proto` to the patched 0.11.18.

## 0.15.7 (2026-10-11)

### Terminal

- Keep single reasoning blocks as solo rows rather than expandable groups.
- Match expandable action-group labels to solo action styling, including the
  purple emphasis for agent waits.
- Show the same context budget in the footer that automatic compaction checks,
  including after resume and tool-result trimming.

### Reliability

- Preserve context checkpoint accounting across JSON persistence and resume,
  preventing unchanged history from falling back to an inflated estimate and
  compacting prematurely. Keep display estimates separate from measured usage.

## 0.15.6 (2026-10-11)

### Terminal

- Show timestamps only on outer action groups, not their nested tool or reasoning
  rows, and omit singleton reasoning counts.
- Offer right-aligned action-group completion times in Settings or `/done-at on`,
  off by default.

### Reliability

- Keep session hosts alive and prevent stale-owner handoff while root processes
  or command watches are running, preserving background work after viewers close.
- Store clangd preambles in private disk-backed client directories instead of RAM
  to reduce language-server memory use.

## 0.15.5 (2026-10-10)

### Terminal

- Keep consecutive calls of the same tool or action type in nested collapsible
  groups inside Actions, with independent toggles and preserved scrolling and
  selection behavior.
  Standalone reasoning groups remain top-level.
- Accent only “Compacted context” in orange; render its details grey without a colon.
- Restore transcript images when menus and tooltips close, move, or resize.
  Team, Todos, and other overlays share the same image-safe repaint handling.
- Keep consecutive hidden reasoning chunks in one row, preserving elapsed time
  and separate tool and model-request boundaries.

### Reasoning

- Budget encrypted reasoning in compacted history and count generated model
  output when checking context pressure, so retained tails stay within budget.
- Match Codex Responses Lite headers and tool-call mode, use standard thread
  attribution without changing cache affinity, honor model-default reasoning
  summaries, and record raw, usable, and maximum context limits.
- Update finalized reasoning summaries in their original rows rather than
  reopening a second reasoning phase for terminal summary fragments.
- Avoid duplicate completion events when Codex's final response replays finished
  reasoning items, while retaining newly delivered summary text.

### Discovery

- List instances with attached UIs first, ahead of detached or legacy owners,
  so open sessions remain visible when discovery results are truncated.

### Reliability

- Avoid cross-process host-admission test stalls during single-threaded release
  validation by keeping worker protocol output separate from test-runner chrome.

## 0.15.4 (2026-10-10)

### Terminal

- Jump to the first ten agents in Team with number keys 1–9 and 0.
- Keep action details pinned to the selected action during optimistic goal
  changes, child updates, and history replay. Close the inspector if that
  action disappears rather than showing an unrelated replacement.

### Subagents

- Let directors resume their own recorded interruptions directly by default.
  Approvals and budgets stay protected; Stop cancels the current turn.
- Honor authorized resume while an interrupted turn is still winding down.

### Edits

- Exclude untracked top-level `artifacts/` and `tmp/` harness scratch from command
  edit diffs, while retaining tracked fixtures and new source files.

## 0.15.3 (2026-10-10)

### Steering

- Preserve already-visible response text and pending-message order when human
  input interrupts generation. Mixed human/background batches retain instant
  human priority; recalled messages no longer interrupt generation.
- Keep completed model responses and usage when steering races with stream
  closure, and skip tool calls that have not started rather than losing them.

### Reasoning

- Request detailed GPT summaries using Codex's normal delivery mode, instead of
  forcing its experimental concurrent-summary cutoff.

### Terminal

- Group consecutive reasoning blocks into a collapsed, expandable mini-group,
  labelled “Reasoning (N)” while active and “Reasoned (N)” when complete.
- Keep interleaved reasoning and actions in one coherent action group, and
  respect auto-expand thinking for live reasoning groups.

### Monitors

- Stop command monitors attributing unrelated concurrent workspace edits to
  themselves. Ordinary shell actions continue to record their observed edits.

## 0.15.2 (2026-10-09)

### Subagents

- Fix `/team` steering failing after the first recipient by giving each worker
  its own durable prompt ID.
- Show subagent names instead of session IDs in action summaries, expanded
  action bodies, message labels, and focused-agent notices. Names refresh when
  the roster arrives; routing continues to use the original internal IDs.

### Terminal

- Keep completed actions attached to their original rows when late tool start
  or update events arrive, rather than switching the action being inspected.

### Diagnostics

- Preserve the original document error and any collected diagnostics when a
  language server disconnects during a workspace scan. Cleanup failures now
  produce an explicitly partial report instead of discarding the evidence.

### CI

- Make the cold-restart service regression deterministic by holding replacement
  startup until the test observes the front's unavailable response.

## 0.15.1 (2026-10-09)

### Steering

- Human messages immediately interrupt native model generation and continue in
  the same harness, without restarting an external agent. Already-running tools
  are preserved, background messages remain queued, and abandoned tool previews
  are closed.

### Terminal

- Keep empty side-by-side diff panes neutral, without phantom change markers or
  background highlights.
- Show side-by-side diffs from 120 columns and for uneven replacements, while
  keeping one-sided changes and lines too wide for the panes in unified view.

## 0.15.0 (2026-10-08)

### Terminal

- Hide the transcript scrollbar track so only the draggable thumb is visible.

- Show per-model-call elapsed time and last received provider activity without
  treating replayed history as fresh progress.
- Preserve GPT reasoning summaries delivered as atomic summary parts or only in
  final model responses, without duplicating streamed text.
- Rename the subagent menu model column from “Model now” to “Model”.

- Model, effort, and speed controls and status metadata stay synchronized with
  the viewed subagent, including live roster updates and history hydration.
  Model and effort edits target that child rather than changing the director.
  Director-scoped access and response-language controls explicitly name their
  target and show the director's current values.

### Usage

- Usage-limit notices distinguish scheduled retries from provider resets, no
  longer show expired deadlines as future resumptions, and clear when a retry
  is explicitly released.
- Fill missing model-call costs from published prices so subagent totals show
  API-equivalent estimates instead of remaining unavailable. Cache reads,
  writes, and context pricing tiers are included; provider-reported amounts
  are preserved and subscription equivalents remain clearly labelled.
- Pricing catalog requests identify Borg explicitly, avoiding catalog requests
  being refused for a missing user-agent.

### Subagents

- Messaging tools require an explicit wake choice, expose canonical message IDs
  for replies, and report bounded linked-reply evidence separately from delivery
  acknowledgements. Use `followup_task` for actionable requests.
- Coordination waits return new deltas instead of repeating the full roster.
  Nested-agent watches seed current owned-child state and receive lifecycle
  updates, including parked children and resumptions.
- Ordered `team_batch` handoffs support director-authorized goal actions with
  replay-safe receipts and explicit-stop protection. Queued commands remain
  distinct from verified application; goal identity and accounting are preserved.
- Canonical goal/execution warnings and recorded owner versions make stale
  assignments and loaded-binary uncertainty visible without guessing a Git SHA.
- Saturated child command queues no longer block message delivery indefinitely;
  timed-out messages stay queued and callers are warned not to resend them.
- Already-acknowledged queued team requests no longer reopen completed work;
  unacknowledged and uncertain deliveries remain preserved.
- Instance discovery probes an answering control server rather than merely a
  listening socket, and includes session title/activity and attached/detached
  viewer state when available. Reachable detached hosts remain discoverable.
- Local command delivery has a bounded acknowledgement timeout; uncertain
  delivery is not misreported as confirmed or retried as a duplicate command.

- Spawning a task always creates a new child, preserving idle workers' names,
  context, answers and follow-up addresses.
- Shared-work creation returns explicit work and event IDs; revision conflicts
  report the expected and current revisions.
- Keep autonomous-team policy metadata on `spawn_agent` when scout is enabled.

- Add `scout`, a first-class tool for bounded exploration in a fresh subagent
  with its own context window. Findings return to the parent without dumping
  the child's transcript into the parent model's context.
- Scout defaults to Haiku 5.5 at `xhigh`, with GPT-6 Luna at `xhigh` as fallback.
  Model routes, effort, fallback, and API billing opt-in are configurable through
  live `[scout]` settings; changes apply to the next scout call.

### Agent runtime

- Explicit human or director-authorized goal resume releases stale local usage
  cooldowns and reuses the saved retry rather than adding a duplicate
  continuation. Provider quota limits remain enforced.
- Secret scrubbing preserves ordinary task identifiers instead of mistaking
  their suffixes for provider keys, while retaining credential redaction.

### Native workflows

- Bounded lane waits return admission state and wait reasons instead of only a
  timeout. Native checks can explicitly reserve less disk growth while retaining
  the free-space floor and conservative default reservation.
- Common nested Rust test, Rayon and OpenMP thread pools default to the bounded
  job count only when unset; explicit environment and CLI choices are preserved.
- Document cooperative Git landing locks, preservation of foreign staged work,
  and compact evidence handoffs using existing shared-work/artifact APIs.

### Language servers

- Compilation-context metadata distinguishes database presence/file coverage
  from unobserved clangd effective flags, including conditional configuration.
- Nested projects correctly account for inherited `.clangd` compilation-database
  overrides. Ancestor configuration changes invalidate cached metadata, while
  opening another document with the same effective configuration avoids an
  unnecessary server restart.

## 0.14.7 (2026-10-07)

### Terminal

- Tool inputs and results that are JSON render as an indented key/value outline
  with real line breaks instead of escaped source, and plain-text results drop
  the second line-number gutter. Details headers keep the action's summary, an
  orphaned result names the command it belongs to, and command-shaped results
  are shown as output whatever the tool was called.
- Action labels use consistent wording and running/completed forms, including
  waiting for agents, delegation, messages, follow-ups, and image generation.
  Capability wrappers show the same action label as direct tool calls.
- Rich-terminal resumes and reconnects no longer scan and replay the complete
  journal before the first frame. A bounded recent page keeps the latest prompt
  and reply visible, while older events load on demand without skipping history
  when new messages arrive.
- Manually expanded action groups and their scroll offsets survive history
  paging and reconnects.
- The active goal is shown again between loop turns.
- Resumed sessions keep transcript row positions and reasoning blocks, preserve
  prompt admission markers when paging history, and live GPT reasoning stays
  bound to its archived block with cumulative visible summaries.

### Subagents

- Model requests now include live running-subagent counts, shared with the
  terminal UI so both report the same lifecycle state.
- Incoming agent messages retain their sender name and pink identity when
  user-message snapshots arrive before or after their receipt, including history
  replay. Independently authored human messages retain their own identity.

### Agent runtime

- Claude replay retains visible assistant narration as text even when the native
  response stores it in thinking blocks, while preserving signed thinking
  unchanged. Existing stored messages benefit without rewriting session history.
- Native tools capture queued steering when they complete.
- Live subagent-count updates preserve canonical request prefixes through replay
  and compaction, and unchanged counts are not restored after explicit context
  edits.
- Builds without Claude subscription support no longer reference the optional
  Claude connector when checking recent account-verification failures.

### Performance

- Claude login status is read once per 60 seconds across all sessions instead of
  spawning `claude auth status` for every session's 3-second capability refresh,
  which kept several cores busy on hosts with many sessions.

## 0.14.6 (2026-10-06)

### Live settings

- `configure_agent` now handles self, the current session UUID, and managed
  children through the same validated, persisted live configuration path.
  Self-configuration preserves the conversation; only the director may
  configure another agent, and parked children are not implicitly resumed.

### Watchers

- Completed command watchers release their active slot even when the session's
  notification queue is full. Pending exit reports remain available and are
  delivered once the session can receive them.

## 0.14.5 (2026-10-05)

### Terminal

- The agent roster groups every non-running descendant under an expandable,
  scrollable Inactive section, including normally completed Ready agents.
  Delegated agents retain their requesting parent and nested team path across
  restarts; assignment badges show the originating agent instead of director.

- Clicking a sent-message or follow-up card opens the recipient's agent thread
  with its history and a Back button, rather than only the tool details.
  Unavailable recipients retain the inspector; right-click still copies details.

## 0.14.4 (2026-10-03)

### Release reliability

- Update release-tooling smoke tests for the strict Clippy preflight so validated
  platform archives can proceed to publication.

## 0.14.3 (2026-10-03)

### Release reliability

- Fix the cross-platform lint failure in Windows Claude login discovery.
- Local release checks now run the strict Clippy gate before creating a tag.

## 0.14.2 (2026-10-03)

### Windows and cross-platform reliability

- Claude subscription login is detected on Windows when the home directory is
  supplied by `USERPROFILE` rather than `HOME`.
- The pinned Claude model connector loads the correct embedded modules and
  bindings for each supported platform, including Windows' Bun virtual paths.
- Terminal status paths and window titles hide Windows extended-path prefixes;
  home-directory shortening also works when only `USERPROFILE` is set.

## 0.14.1 (2026-10-03)

### Terminal

- Expanding sent-message details no longer crashes when the status bar clips
  an off-screen status target.

## 0.14.0 (2026-10-02)

### Terminal

- Diff additions and deletions once again colour the full transcript row,
  including its margins and scrollbar gutter. Edit previews and inline diffs
  share the same paint path without changing wrapping, selection or copied text.
- The subagent roster shows a compact Mode column: blank normally, `fast` or
  `ultrafast` when configured, with ultrafast taking precedence.

- Reverting a session preserves the position of native mid-turn steers and keeps
  batched prompts together before their replies, including in existing forks.
- Collapsed plan updates reserve space for open steps, so completed leading rows
  cannot hide all remaining work. Expanded plans still show every item.

### Agent runtime

- Python/Bun SDK and persistent-code calls discover and invoke the same native,
  skill, workflow, extension and configured MCP tools. Runtime ancestry survives
  background watchers and validated command-to-SDK calls, permitting cross-
  runtime calls while rejecting cycles without discarding the caller's state.

- Agents on Borg-owned model loops can inspect and edit their model context
  with message IDs and plain text: replace system/history text, insert notes,
  or move/drop complete tool groups. Edits preserve in-flight results, opaque
  state of untouched messages, canonical evidence and runtime permissions,
  and survive recovery, forks and compaction.

### Reliability

- Local session hosts wait for working subagents, including child background
  processes and watches, before idle shutdown or automatic upgrades. Losing
  terminal input no longer cancels a busy team just because its director is idle.
- GUI team rosters are restored separately from the bounded transcript history,
  so quiet workers remain visible when reopening long sessions or viewing a child.
- Open GUI windows count as attached viewers, keeping their session host alive.
  Closing the view releases its presence without sending a stop command.

## 0.13.6 (2026-10-02)

### Terminal UI

- Plan cards return to the plain checklist with status markers, without diff
  backgrounds or superseded task rows. Empty new-chat plans remain hidden.
- Pasting a PNG from a Wayland clipboard preserves its encoded bytes instead
  of decoding and re-encoding the image. Image validation and size limits stay
  in place, with existing clipboard fallbacks retained.
- The composer caret stays steady instead of blinking. Redraws and cursor
  placement use synchronized terminal updates to prevent intermediate flashes
  under load; underline, bar and block shapes remain available.

### Reliability

- Large-session reverts reuse state checkpoints and bounded, indexed inherited
  history reads instead of repeatedly decoding full ancestor histories. Fork
  recovery respects compaction boundaries while preserving queued prompts and
  team state.
- Native Claude narration summaries are routed to visible assistant text using
  the pinned upstream runtime's classification; actual or unclassified thinking
  stays in Reasoning. Signed content and raw event payloads remain unchanged for
  replay, without extra model retries or tool-execution guards.
- Persistent Python host-tool calls are serialized across threads, preventing
  background callers from consuming each other's replies.

### Collaboration

- Team members delegating work always get a fresh worker, preserving the
  director's idle workers and the requester's own context. The new worker
  receives the requester's address for questions and its final report;
  directors can still reuse idle workers.

## 0.13.5 (2026-10-01)

### Terminal UI

- Resumed sessions recover missing journal updates when the live connection
  goes quiet, without reordering streamed previews. Host handoff preserves the
  unsent composer draft and no longer flashes the underlying shell.
- Expanded actions stay attached to the selected action across live updates
  and history reloads, including child sessions. Removing that action closes
  the inspector instead of switching it to another row.
- File diffs appear beside the command that made them, even when it finishes
  after later actions. Long diff lines wrap instead of clipping, wrapped copy
  preserves spacing, and trailing padding no longer forms solid colour bars.
- Status-only plan updates show one row instead of duplicating unchanged text.
  Edits to the task text still show the before-and-after pair.
- User messages keep composer-entered line breaks and paragraph gaps while
  retaining Markdown formatting, including after history cache warm-up.

### Reliability

- Maintenance sweeps no longer starve provider output. Stopping a turn retains
  output already received instead of dropping its cancellation tail.
- Process completion and output notifications cannot be missed between a
  status check and a wait, avoiding unnecessary waits for the full timeout.
- PostgreSQL history search falls back to bounded canonical scanning when a
  large payload exceeds the full-text index size limit. Payloads stay intact,
  and event filters, sequence paging and payload expansion remain available.

### Models

- Long screenshot sessions on Claude no longer exceed the API's 32 MB request
  limit. The newest images are kept within the budget and older ones are
  replaced with a note, instead of the oversized request being misread as a
  full context window and forcing an early compaction. When a provider refuses
  a request as too long, the refusal text is recorded, and the compaction notice
  no longer reports Borg's local estimate as the context window.

## 0.13.4 (2026-10-01)

### Models

- Fast and Ultrafast are offered only when the selected model and account
  confirm support. Standard remains available; changing models clears
  unsupported speeds. Unknown capability data does not advertise faster tiers,
  and remote frontends use the runtime host's capabilities, not local accounts.

### Terminal UI

- Rich plan updates appear beside the action that made them instead of updating
  an off-screen card in old scrollback. Live updates and rebuilt history retain
  a single plan card.
- Web-search action summaries lead with the search query, including searches
  made through the capability wrapper.
- The retry footer puts the provider error first, followed by the retry attempt
  and the Esc-to-cancel hint.
- Normal and Ctrl-click open message artifact links relative to the local
  session's working directory. Wrapped web links retain their full destination.
  Remote file paths are not opened as local files; missing files are reported,
  and executable or special files are refused. Link hovering no longer checks
  the filesystem, and opening links does not use a command shell.

### Collaboration

- Message images can travel between enrolled hosts as captured, verified bytes,
  with encrypted relay storage and recipient-only access. Pending transfers can
  be retried without duplicates; failed or recalled deliveries stay terminal.
  This requires updated agents on both hosts and an updated relay server.
- Team participant addresses resolve against the live local session before stale
  directory metadata can incorrectly reject an active recipient as exited.

### Reliability

- Network retries preserve the conversation's structured continuation even when
  a queued follow-up changes the input batch, rather than restarting the request
  as a fresh prompt.
- Native local execution preserves the session's process registry, so watchers
  can follow shells started with `exec`. Empty process input is treated as a poll
  instead of an attempted write to a closed stdin.

## 0.13.3 (2026-09-30)

### Models

- Fresh sessions honor the saved last-used model. The background host no longer
  receives an implicit `--provider claude` that bypasses the saved choice, such
  as Sol 6.1. Explicit provider choices and resumed-session models are unchanged.
- Borg's native Claude adapter aligns with the Claude Code 2.1.286 protocol
  through a pinned `claude-agents` revision. This updates protocol handling;
  it does not install or upgrade the Claude binary.

### Claude reliability

- Claude turns wait for the session's idle event before finishing, including
  follow-up turns woken by background agents. Ambient tasks no longer hold a
  turn open, and tracked background tasks survive pooled-session reuse. Older
  CLIs without session-state events retain result/task-based completion.
- Failed interrupt requests and results marked `is_error`, even with a success
  subtype, are reported as failures rather than silently accepted.
- Waiting between background follow-up turns is bounded to ten minutes by
  default. `CLAUDE_CODE_PRINT_BG_WAIT_CEILING_MS` changes the limit; `0` disables
  it. Expiry fails the turn and discards the process instead of pooling a
  potentially active session. The limit does not time out active model work.
- Priority SDK steering retains its separate-turn behavior. Claude Code
  2.1.286's join-the-running-turn behavior is reserved for human-origin messages;
  SDK steers do not claim that origin.

### Terminal UI

- The watcher menu closes and clears its hover and keyboard focus when the
  last running watcher stops or exits, even when exited watchers remain in
  history. It no longer stays open after its footer control disappears.
- Moving into or out of watcher controls and menu rows triggers a redraw, so
  watcher highlights and popups no longer remain stale until another event.

### Collaboration

- `list_instances` lists only live sessions by default. Sessions from this
  machine whose process has exited no longer come back from the relay
  directory as `running`, `ready` or `starting`; they appear only with
  `include_exited`. Sending to an exited session now fails with a clear error
  instead of reporting `queued_offline`.
- Every top-level session is `/root` of its own team, so messages from another
  session's tree are labelled "from /root of another session (participant:…)",
  and each session's context states its own team path, address and parent.

### Embedding

- Stores that implement `WorkspaceStore` outside this crate can reuse the
  shared work rules (`WorkSnapshot::validate` and
  `apply_with_plan_assignees`) and run the backend-neutral workspace
  conformance cases (`workspace_conformance::CASES`, `test-support` feature).

## 0.13.2 (2026-09-30)

### Runtime responsiveness

- Shared-work plan projection collection runs off the session actor so workspace
  scans cannot block provider events, steering or watchdogs. Revision-fenced
  application preserves newer direct plan updates.

- Python and Bun actions still running after 60 seconds continue as background
  watchers. Completion wakes the session; `list_watchers` retrieves the result
  and `stop_watcher` cancels execution.

### Models and reasoning

- Claude Sonnet 5.5 sessions use its 1M-token context window instead of 200k.
  Borg's Claude runtime moves to 2.1.285, the first to list Sonnet 5.5, so
  long Sonnet sessions compact far less often.
- New GPT sessions default to `gpt-6.1-sol` at medium reasoning effort.
- Codex subscription requests now select sequential reasoning-summary delivery,
  and completed summary events are displayed even when no text deltas arrive.
  Summaries already streamed as deltas are not duplicated. Models may still
  omit summaries; Borg cannot display text the provider does not send.

### Watchers and context

- Watchers are lifecycle triggers. A command watch wakes the agent once, when
  the command exits, with its exit code and output tail; the per-output and
  regex (`notify_pattern`) modes are gone. `watch` can also follow a shell
  already started with `exec` by its `session_id`, and stopping that watch
  leaves the shell running.
- `exec` results no longer repeat the command back to the model. Every poll of
  a running process echoed the full command, with the remembered `cd`
  prefixed; in one subagent that was over half its tool-result text. The UI and
  journal still show the command.

### Terminal UI

- Dragging the scrollbar follows the pointer again. The one-line-per-row drag
  from 0.13.0 is reverted: the thumb stays under the pointer and its place on
  the track is its place in the transcript.
- A mouse-wheel notch scrolls an eighth of the transcript view, up to 9 lines,
  instead of a sixth, up to 12. Scrolling inside an action group eases in the
  same way.
- Scrolling up during a long turn loads older history. Pages fetched while a
  reply was streaming were held back until the turn ended, so a long goal turn
  showed "Loading thread history…" and never anything older.

### Reliability

- A session cluster wedged after running out of disk recovers on its own. A
  PostgreSQL child blocked on a full disk could hold the postmaster in its
  crash reset indefinitely, refusing every connection "in recovery mode" even
  after space returned. Borg now restarts a cluster that stays there for 10
  seconds, and it replays its log as after any crash.
- Agent reports wake their director even when its goal is paused. A report
  reaching a paused or goal-less director was filed without a turn, so after an
  interrupt its agents' branches waited for a manual `/goal resume` to be merged
  or re-evaluated. Escape still holds reports until your next message, and an
  explicit watcher yield still holds queued ones. The goal itself stays paused.
- A question asked while a goal is running gets an answer as a message, not
  only as a Reasoned row. When the agent parked on watchers, the turn ended the
  moment the wait began, so a model that meant to answer after that tool call
  never could, and its answer survived only inside its thinking summary. Every
  unanswered human turn in four days of sessions (61 of 61) ended this way. A
  parked turn that still owes its human a reply now gets one more response to
  write it, and `await_watchers` tells the model its turn ends when it returns.
- A Claude thread whose history lost a tool result no longer fails every turn.
  History repair dropped the unanswered call, but the native reply blocks were
  replayed unchanged, so each retry sent the same `tool_use` without a
  `tool_result` and was refused.

### Windows

- Windows foreground sessions no longer launch an unreachable detached session
  host. Detached local hosts require Unix local session control; explicit
  requests on Windows now fail promptly with foreground advice.
- PostgreSQL server discovery now finds `.exe` binaries on PATH and standard
  `ProgramFiles/PostgreSQL` installations, including PostgreSQL 18. Installation
  advice names the exact `PostgreSQL.PostgreSQL.18` winget package.
- The Windows installer and `borg update` accept ZIPs with a containing folder
  as well as flat archives, and install the native provider alongside Borg.

## 0.13.1 (2026-09-29)

### Models and CI

- Added `gpt-6.1-sol` to the Codex model picker and subagent choices; removed
  `gpt-5.6-sol` from current choices and moved GPT peer and `/codex` defaults
  to 6.1 Sol. Existing sessions using older model IDs remain readable.
- Fixed two Clippy warnings that blocked release validation, without changing
  tool behavior.

### Terminal UI

- Ctrl/Cmd+1-9/0 opens a status menu you can actually drive. Focus reached the
  control, but the arrow keys fell through to the composer, which recalled chat
  history or scrolled the transcript behind a menu that was still open. The
  arrows were given back whenever the focused control was no longer drawn -- so
  it broke most on the subagents roster, which disappears from the status line
  the moment its last agent stops -- and on the first Up, before the menu had
  been laid out. Up and Down now belong to an open menu, and scroll the
  transcript only when none is.
- An action group held open by a running action stays open once that action
  finishes. It folded the instant the work stopped, taking the finished run
  with it at the moment you were waiting on it. A group now ends when a new
  message ends it, not when the last running action stops.

### Reliability

- A turn that stops responding is cancelled instead of running forever. Once a
  model has shown reasoning and then produces nothing at all, the stream is
  dead rather than the model thinking, but the turn stayed open with no way out.
  Three minutes of silence ends it and says so. A turn that keeps making
  progress is never cut off, and a model that works silently is left alone.

## 0.13.0 (2026-09-29)

### Tools

- The goal, plan, subagent and cross-agent messaging capabilities are their own
  tools, and one `capability` tool reaches everything else by name or by search.
  Both are on the default surface, not only on the opt-in one. A promoted name is
  refused through the generic tool and points at its own, and an unknown name is
  answered with the closest real capabilities rather than a bare error.
- `query_history` is a tool, so which of its retrieval modes answers a question
  arrives with the guidance instead of after a wrong guess.
- `search_files` is a tool backed by ripgrep's own engine, so it needs no
  external executable and is the thing to reach for rather than grep. A file with
  undecodable bytes no longer costs you every match inside it.
- Extension capabilities are advertised with descriptions read from the live
  catalog, so a newly loaded extension is visible without restarting anything.
- Computer use is a tool of its own. It was always a full capability - native
  desktop and private-display access, and the approval gate that refuses
  consequential controls until you confirm that exact action - but a model had to
  already know the name and invent a JSON body for it before it could take a
  screenshot. A subagent still gets the private headless display rather than
  yours: the spec is surface-aware, so promotion cannot widen a child's reach.

### Performance

- A fast reasoning model no longer makes the interface lag. Every frame was
  re-rendering the whole expanded block, beginning with a fresh copy of every
  byte of reasoning received so far, so a frame cost the length of the block and
  a stream cost the square of it. At 800 lines that was 17.22ms a frame and
  about seven seconds of CPU for one answer; it is now 0.27ms and 113ms, and a
  frame through a real terminal measures 2.02ms with thousands of deltas
  coalesced into it. The lines that have finished are reused, and the line still
  being written is redone, because that is the only one that can still change.
- `BORG_TUI_FPS` and `BORG_TUI_STREAMING_FPS` are documented. They existed and
  were clamped, but appeared nowhere, so the one knob that changes how streamed
  text feels could not be found.

### Reliability

- A compaction that loses its connection is retried instead of failing your turn.
  One empty upstream response on one fold used to abort the whole sequence and
  fail the turn, costing you the context it was there to save. Retries are
  bounded, and a refusal no repeat can fix -- auth, billing, quota, an
  oversized request -- still fails on the first attempt with its real cause.
- A provider error now names the field it rejected. Only the code and the
  parameter were reported, which say that something is wrong and never what;
  a malformed request was undiagnosable from outside. The provider's own
  wording comes through with the rejected value stripped.

### Reliability

- A history search on a resumed or forked session can no longer report "no
  matches" while having looked at almost none of the history. Those sessions were
  scanned oldest-first under a hard budget, so on a long thread only the oldest
  events were ever reachable and recent work was invisible however the query was
  phrased. The scan now covers the newest window, which is what a resumed thread
  is asking about.
- Search results say whether they are complete. `truncated` already conflated
  "your hit list hit the limit" with "the scan never reached the rest of the
  history", and the two were indistinguishable to a caller. A result now carries
  `search_incomplete` plus the `scanned_from_sequence`..`scanned_to_sequence`
  window it covered, so an empty result reads as "not found in what I looked at"
  rather than "does not exist", and the rest can be paged with `start_sequence`.
  The `query_history` description says so too.

- An upstream that answers with an empty response is retried instead of ending
  the turn. Only a refusal is treated as fatal now; everything else is
  retryable, as it was before mid-stream errors were recognised at all.
- A tool call carrying the presentation field its own schema advertises is
  accepted. The same idea appears as `action` and as `description`, and a call
  formed exactly as documented was being refused as malformed - the cause of most
  of that tool's flakiness.

### Terminal UI

- An action group that live work was holding open now folds when that work
  finishes. A group held open by a running process never collapsed, because
  being the newest group kept it open on its own. Reverted in Unreleased: the
  fold hid the finished run, and a group now ends when a new message ends it.
- Pending input reads as an action group rather than a bordered panel: the
  same disclosure, the same summary, the same grey, no frame.
- A retry no longer re-announces the goal. Every retry re-emits it, and the
  card was taken out and pushed back, so resuming dropped it to the bottom of
  the transcript and reprinted a goal already on screen.

### Terminal UI

- A status line that overflows now ends in a mark. Losing its last column to
  truncation used to drop the tail with nothing to show the text had been cut.
- The effort and billing segments share one colour instead of being graded per
  value, so the same colour no longer means xhigh in one place and a pro/max
  subscription in another.
- Dragging the scrollbar moves the transcript one line per row. A scrollbar maps
  proportionally, so on a long thread one row of drag moved hundreds of lines and
  the closer to the middle of the thumb you grabbed, the less each row was
  worth. Clicking the track still jumps - that gesture means "go there" - and
  only the drag changed.
- The composer's text-entry ground is darker and neutral, so the three rows you
  type in read as a well rather than another band of transcript.
- The splash says what it is. It now reads "BORG" over "agent" over the version
  with the channel beside it, and all three lines keep one width and one centre
  whatever the version turns out to be - the `v` prefix yields to the width
  rather than the layout bending around it.

### Providers

- A tool result carrying an image no longer puts the image inside the tool
  result field, which the API reads as text. A valid screenshot came back as
  `invalid_value` on `input` -- "the image data you provided does not
  represent a valid image" -- and broke every later turn in the session.
- An image attachment is typed by what its bytes are, not by what the file is
  called, on every path.
- A pay-per-use OpenAI key is no longer sent the request shape a ChatGPT
  subscription uses, which the public API rejects outright.

### Providers

- Qwen models that take an effort ladder are sent one. `enable_thinking` is a
  boolean and is right for Qwen3.5/3.6/3.7, but the Qwen3.8 family takes
  `reasoning_effort` instead and converts a level into a thinking budget itself
  - so a laddered model was being offered low/medium/high in the picker and then
  having `enable_thinking` put in the body, and the effort had no effect on the
  request. The choice is now made per model from the catalog entry that already
  exists for it.

### Setup

- The Python library's documented usage is corrected: `borg` is already in the
  namespace, so `import borg` is not part of it.
- `HOME` can be set for the runtime worker as a user setting, defaulting to off.

## 0.12.9 (2026-09-27)

### Updates

- Automatic updates accept larger release archives; release packaging stays within
  the old updater's download limit so existing installations can still upgrade.

### Terminal UI

- Ctrl/Cmd numbered hints no longer recolour the subscription label in the footer.
- Persistent Python workers no longer let child-process terminal prompts overwrite
  the TUI or stall on terminal job control.

## 0.12.8 (2026-09-27)

### Terminal UI

- The splash swaps orange and white between the logo and alpha caption, including glitches.
- Collapsed plan previews show open items before completed items; keyboard hint badges
  no longer overlap popups or repaint the footer background.
- Running command follow-ups use the shorter “Read output” action label.
- Enhanced terminal input requests shifted key characters so uppercase and punctuation
  reach the composer on terminals that report alternate key codes.
- Stopping a background command watcher also stops job-control child processes
  instead of leaving builds running after the watcher exits.

## 0.12.7 (2026-09-27)

### Context

- Native automatic compaction preserves the request prefix needed to restore
  subsequent provider-measured usage after restart. Local context estimates no
  longer count duplicated provider output or opaque reasoning signatures as text.

## 0.12.6 (2026-09-27)

### Terminal UI

- Numbered keyboard hints label the status controls above and below the composer
  instead of transcript rows. Badges sit above their controls; hold Ctrl on
  Linux/Windows or Cmd on macOS, or press F12 in terminals without modifier events.
- Escape sends queued user input into an active turn instead of interrupting it;
  with no queued input, Escape still interrupts.
- Plan updates highlight replaced and removed rows in red and their replacements
  in green. Fresh chats no longer show an empty `0/0 completed` plan card.

### Configuration

- `[prompt] append` in `agent.toml` adds user-scoped instructions to local agent
  turns without replacing Borg's core prompt. It applies when a session or host
  executor starts with the new settings.

## 0.12.5 (2026-09-27)

### Context

- Resumed native sessions restore provider-measured context usage when the
  saved checkpoint matches the replayed conversation and request prefix.
  Compaction explains when it is using a local estimate instead.
- Codex compaction uses low reasoning effort instead of inheriting the working
  turn's high or extra-high effort, while preserving the cached request prefix.

### Work coordination

- Agent plans and shared work use one durable todo model. Assigned work appears
  in ordered per-agent plans; omitting an assignee creates unassigned backlog.
  Migration preserves existing work, dependencies, subtasks and review metadata.
- Plans retain blocked and awaiting-review states, and selected-agent views can
  show and edit stopped agents' work. Removing an item from a plan unassigns it
  rather than deleting it. Concurrent claims and edits use revision checks.

### Providers and tools

- Claude subscription cache warming uses the same pinned account and connector,
  with capped output and no API-billing fallback. Adaptive thinking settings are
  preserved; budget-based thinking remains ineligible. Subscription cost
  comparisons are labelled as API-equivalent estimates. Automatic Codex warming
  remains disabled without established cache-retention semantics.
- MCP discovery follows paginated tool lists within a bounded discovery timeout.
  MCP tool errors propagate as failures, including through code mode, rather
  than appearing as successful raw responses.

### Terminal UI

- Fast streamed replies avoid rescanning earlier styled spans when wrapping,
  and code blocks reuse highlighting for completed lines instead of restarting
  from the top on every repaint.
- Modifier-held numbered hints activate visible clickable targets with 1–9 and
  0, with an F12 fallback for terminals that cannot report modifier holds.
- Received agent messages use an incoming arrow.
- The status row's scroll-back and return buttons no longer cover the status
  line. The line gives up their columns and ends in an ellipsis in front of
  them, and a click on a button no longer reaches the control it used to hide.

## 0.12.4 (2026-09-26)

### Providers

- Vercel AI Gateway is a provider. `borg login vercel` stores a gateway key, or
  set `VERCEL_AI_GATEWAY_API_KEY`, and `/model` lists every language model the
  gateway serves — 264 today, including `stealth/pixel-canary` — from a session
  on any provider. Embedding, reranking, image, video, realtime, speech and
  transcription models are left out: they cannot answer a chat completion.

### Terminal UI

- A completed reasoning row is marked `◦` instead of `✦`.

### Sessions

- Sending a message no longer resumes a goal that a stop or a block left
  parked. The message is still answered and an Escape stop is still released,
  but the goal waits for an explicit `/goal resume`. Set
  `[capabilities] resume_paused_goal_on_message = true` for the previous
  behavior.

## 0.12.3 (2026-09-26)

### Providers

- When a Claude subscription's 5-hour or weekly window enters Anthropic's
  server-reported grace allowance, the running turn is told to finish only the
  work already in progress, leave a recoverable checkpoint, and report what
  remains instead of starting new tasks or agents. Turns on an active overage
  allowance keep their normal behavior.

### Terminal UI

- A completed reasoning row is marked `✦` instead of `∴`.
- A completed reasoning summary steps through its summary lines once and then
  rests on the last one, rather than cycling back to the first.
- Assistant message headers show `fast`, or the effort level followed by
  `fast`, for a turn that ran in fast mode.
- The composer background is darker.

## 0.12.2 (2026-09-26)

### Sessions

- Resuming a fork after its inherited history now reads only new events, rather
  than recomposing the whole parent history for every update.

### Models

- Codex requests reasoning summaries when a model supports them, even when its
  catalog default is `none`, so the terminal can show its reasoning actions.
- The Codex model picker lists `gpt-5.6-sol` instead of `gpt-5.6-terra`.
  An “Other [provider] model ID…” choice lets you enter an unlisted model for
  a specific provider without routing it through the current provider.

### Terminal UI

- A Plan tooltip closing over an inline image forces a repaint instead of
  leaving its outline or text over the image.
- Goal rows and the status line show `▶` for active goals and the original
  `▮▮` mark for paused or blocked goals. The subagent count says “inactive”
  instead of “stopped” when no agents are working.
- Subagent accents use purple; goal rows, status and menus use the todo orange.
  Watchers keep their purple accent.
- Action groups stay open while work continues and fold once the next assistant
  message completes, rather than staying open throughout a long-running turn.
- Action text truncates before the right-aligned result and timer, and keeps
  that column clear even when a sub-0.1-second action has no visible timer.

## 0.12.1 (2026-09-26)

### Context

- An interrupted or resumed turn no longer compacts early. When a request
  reuses the provider's cached prefix, Borg now measures context from the
  provider's reported usage plus the new message, instead of its own replay
  estimate (which could read 223k of 258k while the provider reported 95k).

### Providers

- A Codex backend `invalid_api_key` 401 is reported as a provider outage
  instead of triggering a reconnect.

### Terminal UI

- Replies stream token by token by default. `/streaming paragraph` (or
  Settings → Response streaming) holds unfinished paragraphs, list items and
  code blocks; in that mode the reply header waits for the first finished
  block, and reloading settings no longer reveals the unfinished tail.
- `/team` broadcasts appear in the timeline immediately and show how many
  live addressed teammates durably acknowledged the message; the count keeps
  updating while the session is active and survives reconnects.
- With an empty composer, press Down to focus the status line; arrows and Tab
  navigate its menus, Enter or Space activates a control, and Escape returns
  to the composer. Ctrl+1–9/0 (Cmd+1–9/0 on macOS) reaches the visible
  controls on both composer lines in reading order. Focus is highlighted.
- Action groups show `▾` when expanded and `▸` when collapsed, with their
  timestamps in the same column, and collapse again when clicked. The header
  no longer carries a failed count; failed rows stay red.
- The composer prompt is `›` with a blinking underscore cursor; `/cursor
  underline`, `/cursor bar` and `/cursor block` select a persisted style.
  Finished command rows use the same slim chevron, edits use `◈`, and pending
  or agent actions keep diamond markers.
- Transcript rows drop their inline "click to expand", "click to collapse"
  and "click to open full screen" labels; hovering a row shows its click
  action in the bottom-left hint, as messages already did.
- The inactive agent roster is collapsed by default. Goal and watcher accents
  are rose-purple; todos use orange.
- A waiting command row names its command, and its poll rows stop showing
  "Waiting on…" once the command exits. `git show` rows say "Show commit(s)".
- Opus 5.5 and Fable 5.1 effort changes no longer warn of a cold cache just
  because a resumed transcript has not loaded provider capabilities yet.
- The TUI logs bounded timing summaries for stream, input, frame and
  interrupt bottlenecks.

## 0.12.0 (2026-09-25)

### Agents

- An agent that sent a `followup_task` and went idle wakes on the next team
  message instead of filing it as a queued report, and `wait_agent` with no
  working children blocks until that reply arrives rather than returning
  `no_active_children`.
- Project guidance loads from `CLAUDE.md` as well as `AGENTS.md` (an identical
  pair is read once), for the main thread and every subagent. As an agent
  works in or names a subdirectory, that subtree's `AGENTS.md`/`CLAUDE.md`
  arrive once with the command's result.
- An image pasted into the chat comes with its file path, so the agent can
  forward it to a subagent with `send_message`/`followup_task` `attachments`.
- Commands can call Borg from code: `import borg` in Python and
  `import borg from "borg"` in Bun (Node: `require("borg")`), with results as
  data and failures as `BorgError`. Calls from code and from `borg call` show in
  the transcript as steps of the command that made them.
- `borg call … | head` no longer panics when the reader closes the pipe.
- The system prompt lists every Borg capability as a compact signature, and
  `borg tools --search QUERY`, `borg.tools("query")` (Python and Bun) rank
  capabilities with one-line descriptions.
- A call with a wrong field or name says what was expected: the capability's
  signature, or the closest capability names.
- `runtime_exec` is back beside `exec`: a persistent Python or Bun namespace
  whose variables survive between calls, with `borg` preloaded (the same
  capability calls as `import borg`, plus `borg.checkpoint`/`borg.restore`).
- The `harness` capability lets an agent improve how it works in a project:
  prompt, memory, skill and subagent entries added to its later turns, with
  `refine` recording the evidence and `rollback` undoing recent changes.

### Models

- New sessions start on the model you last used, and a session that fell back
  after a usage limit stays on its new model instead of switching back at a
  turn boundary.

### Terminal

- The action list is regrouped: every batch of actions sits under one header
  with its start time, action count and working directory (`17:41 · 7 actions ·
  ~/project`, plus `1 failed` in red when something failed); finished batches
  fold to that header until clicked.
  Rows drop their clock time and leading `cd …`, name what a command did
  (`Read src/main.rs:1-40`, `Search “pattern”`, `Write build.py`, `Run Python`),
  show its result beside the duration (`6 matches`, `exit 1`, `12 passed`,
  `+14 -3`) and carry a marker for the kind of work; failures show in red.
- The shell keeps its working directory between commands, so agents no longer
  repeat `cd DIR &&` on every call.
- "Back to thread" (was "Back to actions") sits beside Jump to bottom on the
  status row, and one row of terminal background always separates the
  transcript from the status line.
- Reasoning rows show the latest summary title or a whole sentence from its
  start, instead of a fragment cut at both ends.
- The running timer starts from zero for each new turn, including one a
  message starts after the session was waiting, instead of adding to the last.
- Replies stream a finished paragraph at a time by default: a paragraph once a
  blank line ends it, lists item by item, code blocks once their fence closes,
  and titles together with the text under them. `/streaming token` (or
  Settings → Response streaming) shows every token as it arrives instead.
- Settings menu choices after "Auto-expand tools" opened the setting below
  them; each now opens its own.
- Watch and other action rows sit in the action list with no extra spacing,
  uniform with Ran and Reasoned rows.
- Truncated diff previews end with "click to expand".

## 0.11.6 (2026-09-25)

### Models

- Ordered model fallback chains: set `[models].fallback` (for example
  `["claude-opus-5-5@max", "gpt-6-sol@xhigh", "opencode-go/deepseek-v4.1"]`)
  and a usage limit moves the same turn to the next model with quota, then back
  once the limit resets. Named chains compose with `chain:<name>`, and a route
  never spends API credit unless it opts in. See docs/model-fallback.md.
- A new session started without `--provider` or `--model` begins on the first
  route of the chain when that subscription is signed in.

### Terminal

- Footer hover hints say click and right-click, and they and the Pending Input
  controls use the footer's style: keys in white, the rest in dark grey.
- Stopped and failed subagents stay on the team roster, after the working ones,
  marked "click to resume"; the status line shows "N stopped" when none are
  working so the roster stays reachable.
- "Jump to bottom" sits on the status row instead of covering the newest
  transcript line.

### History

- Filtering history by actor works without search text.

## 0.11.5 (2026-09-25)

### Terminal

- Tool and action rows are single-line by default, with the tool name in a
  fixed column and the duration right-aligned, so rows line up like a table.
  Settings › Wrap action rows (or `/wrap-actions on`) restores wrapping.
- Pending Input puts its controls in the title (click to collapse, Esc to send,
  ↑ to recall) in grey with white keys, leaving a blank row above the status line.
- The status and footer strips inherit the terminal background, and action
  rows keep the same gap before the composer as every other entry.

### Agents and teams

- New human input reopens a blocked goal.
- A session takeover or owner restart no longer kills the team: children are
  parked with their parent instead of stopped, and those that were mid-task
  resume on their own when the session restarts.
- ↑ now recalls every pending steer, including one typed while another was
  still pending (it was misfiled as team input and could not be recalled).
- Escape on a long turn no longer resurfaces its opening messages as failed
  when the model had already acted on them.
- Fixed the release build's Clippy failure in the provider image-tile test.

## 0.11.4 (2026-09-25)

### Terminal

- Pending Input is quieter and gives queued text more room. In the tool
  inspector, Back to actions no longer overlaps the compaction status row.
- Inline diffs show a shorter preview with a hint to inspect the full diff.
  Wait-for-agents shows the maximum timeout as “up to 15m”, not an elapsed timer.
- The Running timer now keeps cumulative active time across action handoffs,
  including for subagents, without adding a second `run` timer.
- The Running status highlight sweeps more slowly and narrowly, without
  changing tool-row sweep speed.
- The full-width status strips above and below the composer are black, and the
  redundant send/Enter footer hint is gone.

### Agents and teams

- Yielding on watchers is enabled by default for new sessions. Set
  `capabilities.watcher_yield = false` to disable it.
- Queued human follow-ups are batched into one turn at the boundary, even
  without an interrupt; each message keeps its own durable identity.
- Esc on owned and attached local sessions bypasses the general command
  backlog so a busy actor can stop the active turn promptly.
- Flushing Pending Input from an attached viewer delivers recovered messages
  together before the flush, preserving the same batch as the session owner.
- Messaging an idle child now reports `queued_idle` and explains how to wake it,
  rather than implying the message was already read.
- `spawn_agent` supports `fresh: true` to start a new child instead of reusing
  an idle session with an old conversation.

### Providers and dictation

- Large images arrive as a scaled overview plus full-resolution tiles. Long
  sessions keep at most 60 image blocks per request (counting tiles), retaining
  the newest images and identifying older omissions by path when needed;
  durable conversation history is unchanged.
- The shared Claude subscription connector selects the pinned, checksummed
  runtime for macOS, Linux ARM, and Windows, not just Linux x86-64.
- Managed dictation retries a recording on an isolated default-accelerator
  server when the existing local server returns a 5xx error; it does not stop
  another process's server.

## 0.11.3 (2026-09-24)

### Terminal

- **Image previews render once, at native size.** Resumed and child
  transcripts forgot the terminal's graphics support, so a preview drew a
  blocky text fallback under a stretched copy of the image and was captioned
  "text unreadable here". Every transcript now uses the graphics protocol.
- Message backgrounds and diff highlight bars reach both edges of the screen;
  the scrollbar is drawn over them.
- The footer's `↓N` behind-count shows a "git pull" tooltip on hover, like the
  `↑N` push count, so it is clear that clicking it pulls.

### Agents and teams

- **Messages sent while a steer is in flight are delivered together.** A
  second message was held until the next model call, which could be a long
  tool call away. Held messages now go to the model, each separately, as soon
  as the earlier one is accepted.

## 0.11.2 (2026-09-24)

### Terminal

- The Running status sweep moves 25% slower.
- Tool-call sweeps keep their speed but start half as often, resting between
  passes.

### Agents and teams

- **`wait_agent` no longer returns empty updates.** When a child resumed work
  or its message was delivered as input during the brief coalescing window,
  the wait returned `child_update` with nothing in it, which looked like a
  dropped message. It now keeps waiting instead.

### Providers and models

- **Large screenshots no longer fail Claude turns.** Images over 2000 px on a
  side, such as full 2560x1440 screenshots, are downscaled before they are
  sent, so a conversation with many images is no longer rejected with "image
  dimensions exceed max allowed size for many-image requests".

## 0.11.1 (2026-09-24)

### Agents and teams

- **Messages you send during a usage-limit wait run immediately.** Borg no
  longer holds them until the automatic retry, which could be hours away after
  you had already topped up or switched account. If the limit still applies,
  the turn returns to the same wait.
- **Team reports stay out of Pending Input during a usage-limit wait.** A
  subagent report that arrived while the session waited on a usage limit was
  queued as if you had typed it; it is now kept as a team update.

## 0.11.0 (2026-09-24)

### Development lanes and engine integration

- **Host-local build lanes and supervised services.** `borg lane` and the
  model-facing lane tools queue resource-bounded jobs across independent Borg
  processes, recover detached jobs, and track their results. Shared services
  have health checks, client leases, restart policies, scoped process cleanup,
  and safe handoff to exclusive build jobs. Reservations cover memory, CPU,
  filesystem space and service dependencies; fairness, coalescing and retry
  behavior are observable rather than hidden.
- **Workspace budgets and safe cleanup.** `borg worktree` reports owned
  worktrees and build targets, enforces per-agent and disk budgets, and previews
  eligible cleanup. Garbage collection requires human confirmation and never
  treats unknown ownership as permission to delete.
- **Native build and Unreal adapters.** The native lane extension sizes Cargo
  jobs and leases a disposable PostgreSQL database for tests; the guarded
  Unreal adapter coordinates UBT builds with editor start/stop and shared
  services. Both expose their verified workflows without commandeering the
  user's active workspace. See `docs/gamedev/`.

### Computer use

- **Private display for testing apps and games (Linux).** `computer_use`
  `launch` runs an app on a session-owned, GPU-accelerated headless display
  (`borg-display`, shipped beside `borg`). Every op works on its `pd:` windows,
  and its input and screenshots never touch your screen, keyboard focus or
  pointer. It starts on demand and is torn down with the session. X11-only
  apps run through xwayland-satellite. See `docs/computer-use.md`.
- **Games and editors on the private display.** Apps can lock or confine the
  pointer, so SDL relative mouse mode and FPS mouse-look receive exact deltas
  from `pointer_move`. `type_text` types any Unicode, and screenshots can draw
  the pointer (`cursor: true`).
- **Window listing and capture on the desktop (Linux).** `list_windows` also
  lists compositor windows without an accessibility tree (games, Unreal) on
  niri, sway, Hyprland and X11. `screenshot {scope: "window"}` captures one
  window. `pointer_move` sends relative mouse motion, `key` takes `hold_ms`,
  `coordinate_space: "window"` targets window pixels, and `restore_focus`
  hands focus back afterwards.
- **Screenshots reach Claude and Codex agents as images.** Tool results now
  carry images as MCP image content instead of base64 inside JSON text, which
  Claude Code spooled to a file unseen. Images larger than the model accepts
  are downscaled first, and `sent_images` reports the exact scale so points on
  the image map back to display pixels.
- **GTK4 on Wayland works with the accessibility ops.** Its elements are no
  longer refused as disabled, and element bounds are window-relative.
- **Sub-agents are confined to private displays.** Sub-agents previously had
  the same desktop access as the top-level session. They can now use
  `computer_use` only on their own private display, or on one they
  `attach_display` to, such as the parent's. The user's desktop is refused.

### Agents and teams

- **Long waits no longer re-wake on one pending steer.** A queued follow-up
  interrupts `wait_agent` once; further waits block until another event or the
  timeout even if the provider has not folded that follow-up into its input yet.
- **Transient Codex 5xx responses retry without switching billing.** A
  subscription HTTP 5xx error enters Borg's bounded same-subscription retry
  path instead of blocking an active goal. Authentication, usage-limit and
  other 4xx responses still surface without API-key fallback.
- **Team configuration and recovery.** Child agents can be reconfigured live;
  forked teams retain their identity, ownership and transcript order after
  restart. Team membership and queued updates survive replay and retry.

- **Claude sessions run on Borg's tools and context.** Claude Code now only
  provides the subscription model and its loop. Borg runs every command and
  file edit: Claude uses Borg's `exec`, `write_file` and `edit_file`, the same
  shell Codex uses, with one process registry and journal, so `borg call` and
  `borg image` work from Claude's shell. Borg also asks for approval outside
  Full Access. Claude sees Borg's full tool catalog, including `web_search`
  and extension tools, and Borg supplies the AGENTS.md chain and skill
  catalog. Claude Code's own tools, claude.ai connectors, plugins, skills,
  settings files, memory and "the user hasn't heard from you" reminder are
  off. That reminder often pushed Claude to write its progress updates
  inside thinking. Borg requests summarized thinking, so Claude's reasoning
  still streams as Reasoned rows.
- **Reasoned rows show their summary.** A collapsed Reasoned row now shows
  the first line of the thinking summary.
- **Provider switches and compaction keep Borg's context.** Switching between
  Claude and Codex, reconnecting after a failed turn, and resuming an evicted
  Claude process now rebuild from the durable session journal. A failed
  compaction stops without replacing the source history with a degraded
  summary; recovery also restores history behind older degraded boundaries.
- **Claude process use is bounded across sessions.** Active turns stay live,
  while the host retains at most four idle Claude processes for up to one hour.
  An idle session rebuilds its context when needed again.
- **Opus 5.5 and Fable 5.1 effort switches can keep Claude's prompt cache.**
  The pinned Claude Code payload now supports this on direct subscription and
  API-key routes. Borg waits for the next usage report instead of predicting a
  cold cache from the effort change alone.
- **Queued team updates recover and clear correctly.** Replayed sub-agent
  messages keep their team role and remain in the durable inbox until the next
  turn. Older queued messages are corrected on recovery; team updates stay out
  of the human Pending Input panel.
- **`wait_agent` waits for real work.** One call blocks up to 30 minutes
  (default 10) and returns as soon as a child settles, reports, or human/team
  input arrives. It says what ended the wait and includes a status line per
  child, so orchestrators no longer need `sleep` loops. Code-mode `wait()` uses
  the same default, and a native `sleep` while children run gets a hint to use
  `wait_agent`.
- **Batch team-message handling.** `acknowledge_team_message` takes several
  ids, `up_to` or `all`, and `list_unread_team_messages` takes `compact` and
  `ack`. Reports already shown by `wait_agent` are acknowledged automatically.
- **Resume after interrupt.** 0.10.0 lets an explicit follow-up restart a
  stopped or failed child. Now the agent that interrupted a live child with
  `interrupt_agent` can also resume it with `followup_task`, and the child is
  told its parent lifted the stop. Other agents can't lift it, and an
  interrupt or stop the human makes in the UI still holds, even after an
  agent's interrupt.

### Terminal

- Image previews are bounded to the transcript, use filtered downscaling and
  fit within complete terminal rows to avoid an extra stripe below a thumbnail.
  In-flight messages remain selectable and attached terminals receive live
  streaming output. Command edits have durable Edit rows; reasoning and live
  text previews update without flooding the transcript or repainting the
  terminal on every delta. The composer status and footer rows are black
  with blank spacing but no separator borders around the dark input stripe.
  The Ready status uses an open-circle icon. The running sweep now darkens
  white tool text so its moving highlight stays visible, and also sweeps
  across the Running spinner, resting between passes. Jump to bottom and
  Back to actions share one right edge and style, side by side when both show.
- **Scrolling and streaming stay responsive on long sessions.** Wheel motion
  catches up by elapsed time, so a slow frame never leaves scrolling that keeps
  draining after the wheel stops, and streamed text is no longer throttled to
  a few frames per second when drawing gets expensive. Live updates redraw
  only the changed tail of the transcript, so streaming cost no longer grows
  with the length of the session. The transcript also fills the row that
  used to sit empty above the status line.
- **Esc stops a turn on the first press.** Queued follow-ups no longer turn
  the first Esc into "send pending input"; the turn stops and the queued
  input runs next. Interrupts also outrank busy team traffic in the session
  actor, and pressing Esc again resends a stop that has not landed yet.
- Edited rows that span several files show each later file as a path row;
  Git's `diff --git` and `index` headers no longer appear as numbered code.
- Ghostty setup ships with the release archives. Completion notifications
  only fire when work actually stops, and new threads receive durable titles.

- Pending Input can be collapsed and shows only queued human prompts. The
  composer has a lighter text stripe between divider lines, the transcript
  scrollbar uses less space, and the completion chime plays more quietly.
- The sub-agent roster now labels the current model separately from total
  lifetime token use. Costs are marked as estimated,
  subscription-equivalent, mixed, partial, or unavailable as appropriate.
- Codex's Ultra effort selection maps to an accepted provider value.

### Install and update

- Draft releases use curated changelog notes and packaged notices. Linux
  computer use is implemented by the bundled native display helper rather
  than an external Python worker; the native lane extension also runs in Blu.

- `borg update`, Linux release archives and `just cli` install `borg-display`
  beside `borg`. Updates verify its version and install it atomically with
  `borg`, rolling both back together on failure. Older releases without it
  update as before.
