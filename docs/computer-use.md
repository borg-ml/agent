# Computer use: Linux preview

This is a **partial implementation**, not cross-platform completion. Borg owns
approval, session lifetime, native-helper transport and code-mode clients.
No external provider agent runtime owns desktop control.

## Available

Discover `computer_use` with `borg tools`, then query `{"op":"capabilities"}`.
Every operation, including observing private screen contents, requires Full
Access or approval. Normal confirmation rules still apply to consequential
application actions; approval to run code is not blanket permission to purchase,
send, delete, or change security settings.

Linux requires the desktop session bus, Python 3, PyGObject and AT-SPI2.

- `list_windows`: window IDs are scoped to the helper lifetime.
- `observe`: `window_id`, optional `max_nodes` (1–1000), optional `since` for
  a diff from the immediately preceding observation. Trees and text are bounded.
- `click`: semantic AT-SPI action, not an inferred coordinate click.
- `set_value`: replace editable text, including Unicode; password controls refused.
- Both actions require `window_id`, `element_id`, and the latest `observation_id`.
  Handles are checked against the window ancestry and observed state. An attempted
  effect consumes its observation. Results contain a new tree and a bounded
  tree-settling indicator, **not** a guarantee of application-level completion.
- `screenshot` requires `scope: "desktop"`. It uses `grim` on supported Wayland
  compositors, returns bounded inline PNG attachments, and explicitly captures the
  entire visible desktop. For tree plus image, use `observe` with `screenshot: true`
  and `screenshot_scope: "desktop"`. There is no silent whole-screen fallback.

Python code mode: `cua.capabilities()`, `cua.list_windows()`,
`cua.observe(window_id)`, `cua.screenshot("desktop")`,
`cua.click(window_id, element_id, observation_id)`, and
`cua.set_value(window_id, element_id, observation_id, text)`.
Bun exposes the same methods as promises; use `await`. Return the screenshot
object as the final expression to send its image to the model.

Helpers are serialized per session, killed on timeout/protocol failure and stopped
with the session. After interruption or timeout an action may already have happened:
re-observe before retrying. Never replay a click merely because its response was lost.

## Verification and limits

A real disposable GTK window on this Linux desktop confirmed Unicode text changes
and button-click callbacks through AT-SPI, stale-observation rejection, and empty
unchanged-tree diffs on both XWayland and native Wayland. Evidence for the current
main-thread run is in `/tmp/borg-cua-main`; this is local evidence, not a portable
release certification. The compiled dispatcher and both Python/Bun clients also
passed capability and screenshot-attachment checks. Ten runtime tests plus the
permission regression pass, including image bounds and no replay of host effects
after a Bun runtime error.

AT-SPI coordinates on Wayland are **not** assumed to be screenshot coordinates.
The local Niri compositor omitted visible-window geometry and rejected isolated
`grim -T` capture. Those experiments are not advertised as supported window capture.

## macOS (unverified)

`computer_use/macos.swift` implements the same contract on AXUIElement: window
enumeration over regular apps, bounded tree observation with diffs, `AXPress`
clicks, settable `AXValue` replacement (secure text fields refused), and
`screencapture` for `scope: "desktop"` or `scope: "window"` (isolated capture by
CGWindowID; ambiguous titles refused; images over 4 MiB are downscaled). The
dispatcher compiles the helper once per source revision with `swiftc` into
`~/.borg/state/computer-use/`, so the Xcode Command Line Tools are required, plus
Accessibility and Screen Recording permission for the terminal running Borg.
`capabilities` reports both permission states. **This has not yet been built or
exercised on a real Mac**; treat it as unverified until this section records the
host, commit and observed effects.

Still missing: Windows driver, macOS real-host verification;
keyboard/pointer injection, scrolling and dragging; isolated window capture;
full cross-provider image/desktop task verification; installation and broadcast
verification. `capabilities` on Windows explicitly returns unavailable.
Installing a binary does not upgrade an already-running Borg process.
