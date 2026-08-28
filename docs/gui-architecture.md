# Borg native UI architecture

## Product direction

- **Subject:** a live, durable agent session where messages, tool work, goals,
  plans, and subagents evolve together.
- **Audience:** operators who already value Borg's terminal density but want a
  native workstation surface with better pointing, selection, inspection, and
  visual hierarchy.
- **Primary job:** make the current agent state and the next available action
  obvious without hiding the transcript.
- **Signature:** the transcript remains the spine of the application. Tool runs
  form a compact execution rail, while the orange session edge and status strip
  carry live state from the TUI into the native surface.

## Visual thesis

The GUI is a polished instrument panel, not a generic dashboard. It keeps the
TUI's dark ink, warm Borg orange, semantic blue/pink/green accents, compact
monospace data, and uninterrupted vertical transcript. Native affordances add
space and precision; they do not turn each event into a floating card.

Initial tokens live in `borg_ui::palette`:

- near-black canvas and ink-brown raised surfaces;
- warm gray text with restrained muted metadata;
- orange for Borg identity, peach for active work, blue for the operator,
  pink for subagents, green for healthy goals, and red for failure;
- 4/8 px spacing rhythm, one-pixel rules, two-pixel semantic message edges,
  and small radii only around controls and the composer.

## Dependency boundary

```text
borg-core / borg-remote / borg-provider
                  │
              borg-ui
     domain view data + commands only
          ┌───────┴────────┐
       borg-tui         borg-gui
      ratatui I/O        GPUI I/O
          └───────┬────────┘
             borg CLI shell
```

`borg-ui` may depend on Borg domain crates. It must not depend on `ratatui`,
`crossterm`, GPUI, or operating-system window types. Frontends translate native
input into `FrontendCommand` and render `SessionView`; they do not own session
state transitions or persistence.

This boundary intentionally stops short of a universal widget abstraction.
Terminal cells and a retained GPU scene have different layout and interaction
needs. Sharing widgets would couple both frontends while producing a worse API
for each.

## Performance boundary

Session ingestion and projection are frontend-neutral. `borg-ui` incrementally
reduces durable and live events into shared session state, timeline entries,
goals, plans, and agent snapshots on a worker thread. Timeline entries and
history are shared by `Arc`, so a live update does not clone the full transcript
or perform projection work on the window thread.

Only the final rendering strategy is frontend-specific:

- GPUI uses its variable-height virtual list, retained scene, and background
  executor for file/clipboard work. Long transcripts do not create an
  unbounded element tree.
- The TUI keeps terminal viewport, cell-diff, and refresh-rate optimizations.

This is the intended split: state/model performance improvements benefit every
frontend, while native renderers retain the optimizations appropriate to their
output device.

## Layout

1. A narrow native header identifies the session and workspace.
2. The transcript occupies the flexible center and retains message/tool/event
   ordering.
3. Live goal, model, access, context, and agent state form a single status strip.
4. The composer is anchored at the bottom, with contextual action hints below.
5. Pickers and inspectors overlay the transcript only while active; persistent
   secondary information belongs in an optional side inspector, not nested
   cards.

The baseline reference is `artifacts/gui-port/tui-baseline-focused.png`.

## Screenshot comparison

The checked-in references keep the comparison reproducible:

- `artifacts/gui-port/tui-baseline-focused.png` captures the TUI's message
  edges, muted reasoning, compact action rail, status density, and anchored
  composer.
- `artifacts/gui-port/gui-live-virtualized.png` captures the same durable
  session through GPUI with the transcript virtualized at a narrow workstation
  width.

The comparison drove concrete changes rather than pixel imitation. The native
surface now preserves the TUI's transcript-first hierarchy and semantic
orange/blue/pink/green identities, while replacing terminal-only chrome with a
session switcher, clickable status controls, inspectors, attachment chips, and
native overlays. The narrow capture exposed horizontal clipping and an overly
shallow composer; transcript rows now enforce bounded width and wrapping, and
the composer shapes up to three wrapped or explicit lines with Shift/Alt+Enter
newlines. Tool output remains collapsed by default and bounded when expanded;
the complete output is still available through copy.

Screenshot capture is deliberately not part of automated verification because
GPUI requires a real GPU presentation surface. Build, projection, parser, and
workflow checks remain headless; visual captures are reviewed manually without
driving the user's active desktop.

## Running the native frontend

From an installed release, run `borg gui`; from the workspace, run
`cargo run -p borg-gui`. The application resumes the latest durable local
session and starts a headless Borg owner when that session is not already
running. On a fresh installation it creates and owns a new session. Use
`borg gui --session UUID` (or `borg-gui --session UUID`) to open a specific
session; the native session menu can switch or create sessions after launch.
