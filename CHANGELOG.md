# Changelog

User-visible changes. Release pages on GitHub also list every merged change.

## Unreleased (since 0.10.0)

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
- **GTK4 on Wayland works with the accessibility ops.** Its elements are no
  longer refused as disabled, and element bounds are window-relative.
- **Sub-agents are confined to private displays.** Sub-agents previously had
  the same desktop access as the top-level session. They can now use
  `computer_use` only on their own private display, or on one they
  `attach_display` to, such as the parent's. The user's desktop is refused.

### Agents and teams

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

### Install and update

- `borg update`, Linux release archives and `just cli` install `borg-display`
  beside `borg`. Updates verify its version and install it atomically with
  `borg`, rolling both back together on failure. Older releases without it
  update as before.
