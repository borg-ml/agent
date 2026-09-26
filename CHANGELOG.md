# Changelog

User-visible changes. Release pages use these highlights and link to the full
Git comparison.

## Unreleased (since 0.12.2)

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
