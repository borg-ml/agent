# Session lifecycle

Borg separates a durable session from any TUI or GUI displaying it. Each local
interactive session has one detached host process that owns the provider app
server, turn execution, tools, and subagents. Frontends are viewers and command
clients; they are not the lifetime owner.

## Closing and attaching views

Starting `borg` creates or resolves a session, starts its host when necessary,
and attaches the terminal. `borg resume` can attach another terminal to the
same session. The GUI uses the same ownership boundary.

Closing one view, including an older Borg version, only detaches that view. An
active turn and its subagents continue, and another attached view keeps
receiving durable events. `/quit` closes the current view; use the explicit
stop or interrupt controls when the intent is to stop work.

Only one process may own a session journal at a time. Borg uses the session
writer lease and control socket to discover an existing owner instead of
starting a second provider app server. A stale owner can be recovered from the
durable journal.

## Idle shutdown

A detached host remains alive while a turn is starting or running, an approval
or provider response is pending, a prompt is being admitted, or any viewer is
attached. When the session is ready, has no pending prompt, and has no viewers,
the host waits five minutes and then exits. Resuming the session starts a new
host from its journal.

Ephemeral, JSON, print, and other non-interactive executions retain their
bounded command lifetime and do not create this detached interactive host.

## Storage and recovery

Session events are stored under Borg's data directory, separately from
configuration in `$XDG_CONFIG_HOME/borg` (normally `~/.config/borg`). A viewer
can reconnect after a frontend crash. If a host itself crashes, the current
provider call may fail, but the durable session remains available and Borg can
restart ownership from its recorded state.

For project-oriented resume behavior and shared workspaces, see
[`multiplayer-workspaces.md`](multiplayer-workspaces.md).
