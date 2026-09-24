# Changelog

User-visible changes. Release pages use these highlights and link to the full
Git comparison.

## Unreleased (since 0.10.0)

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
