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

Consequential controls are gated in the dispatcher on every platform: after an
observation, a `click` or `set_value` whose target name contains a word such as
send, pay, delete, publish, install, password or permission is refused until the
human has confirmed that exact action and the call repeats with `confirmed: true`
(`cua.click(..., confirmed=True)` / `cua.click(..., {confirmed: true})`). Tool
approval alone never satisfies this gate.

### Input injection contract (all platforms)

`type_text {window_id, text}`, `key {window_id, keys}` (one key plus
cmd/ctrl/alt/shift, e.g. `ctrl+shift+t`), `pointer_click {window_id,
element_id+observation_id | x,y, button?, count?}`, `scroll {window_id,
element_id+observation_id | x,y, dx, dy}` (positive `dy` scrolls content down;
units reported in the result) and `drag {window_id, from_x, from_y, to_x, to_y,
button?}`. Every injection raises/focuses the target window — acceptable only
because each op is approval-gated — consumes that window's observation and
returns the settled tree. Semantic `click`/`set_value` remain the preferred,
name-gated way to act on an element; injected ops exist for coordinate space and
for keys, scrolling and dragging that accessibility actions cannot express.
Raw `x,y` targets are in screenshot/screen pixel space (Linux, Windows) or AX
screen points (macOS) and are flagged `coordinate_click: true`; they bypass
name-based confirmation gating because no element is named.

Linux caveat: AT-SPI extents on GTK Wayland are window-relative and the
compositor may not expose the window origin, so element-targeted
`pointer_click`/`scroll` return a clear error there instead of guessing; use
`click`/`set_value` or a raw coordinate read from a desktop screenshot. The
Linux backend is a Borg-owned evdev uinput device plus `wtype` (no `ydotool`).

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

## macOS (verified on a real host)

`computer_use/macos.swift` implements the same contract on AXUIElement: window
enumeration over regular apps, bounded tree observation with diffs, `AXPress`
clicks, settable `AXValue` replacement (secure text fields refused), and
`screencapture` for `scope: "desktop"` or `scope: "window"` (isolated capture by
CGWindowID; ambiguous titles refused; images over 4 MiB are downscaled). The
dispatcher compiles the helper once per source revision with `swiftc` into
`~/.borg/state/computer-use/`, so the Xcode Command Line Tools are required, plus
Accessibility and Screen Recording permission for the terminal running Borg.
`capabilities` reports both permission states.

Verified by the Mac Borg instance on macOS 26.2 (arm64, Swift 6.3 / Xcode 26.4)
at commit `ecc3ff3`: `swiftc` build; `capabilities` with both permissions
granted; `list_windows`; `observe` of a TextEdit window (45 nodes); `set_value`
of "Borg macOS ✓" into its `AXTextArea`, confirmed both in the returned tree and
visually in the window capture; an empty `since` diff after the action; desktop
capture at 2880×1800 and isolated window capture at 1396×1200. Element ids are
scoped to one helper process, as on Linux.

macOS input injection (CGEvent, **not yet verified on the Mac**): `type_text`
(Unicode via keyboard events), `key` (one key plus cmd/ctrl/alt/shift, e.g.
`cmd+s`), `pointer_click` (centre of an observed element, validated like
`click`, or an explicit `x`,`y` in AX screen points; `button`, `count`),
`scroll` (`dx`,`dy` pixels; positive `dy` scrolls content down) and `drag`
(`from_x`,`from_y`,`to_x`,`to_y`). Every injection raises the target window
first and consumes the window's observation; results carry the settled tree.
Coordinate clicks bypass name-based confirmation gating because no element is
named; element-targeted `pointer_click` is gated like `click`.

Verified on the Mac at `mac-input` `5962d2c`: `type_text` ("Borg typed ✓ héllo"
arrived intact), `key` (`cmd+a`, `delete`), element-targeted `pointer_click`,
and `drag` (text selection visible). Two findings drove follow-up changes:
injected typing goes through the app's text-input pipeline, so autocorrect and
auto-capitalisation apply (verify the resulting `text`, not the input); and an
element's geometric centre can lie outside its scroll area, which made `scroll`
a silent no-op. Pointer ops now target the centre of the element's
`visible_bounds` (clipped by enclosing `AXScrollArea`s and the window) and refuse
elements that are scrolled out of view; text nodes also expose `selected_text`.
A tree that is still changing right after typing can reject the next action as
"changed since observation" — re-observe and retry.

## Windows (unverified)

`computer_use/windows.ps1` implements the contract on UI Automation under
Windows PowerShell 5.1+ or `pwsh` (no compile step; the script is cached by
content hash under `~/.borg/state/computer-use/`). Observations use the control
view; bounds are physical screen pixels (the helper is DPI-aware), so they match
`scope: "desktop"` screenshots taken with `CopyFromScreen` over the virtual
desktop. `scope: "window"` uses `PrintWindow` for isolated capture. `click` uses
Invoke, Toggle or SelectionItem patterns; `set_value` uses ValuePattern and
refuses password and read-only controls. Elevated windows are not observable.
Input injection (`type_text`, `key`, `pointer_click`, `scroll`, `drag`) uses
`SendInput` with absolute coordinates normalised over the virtual desktop and
brings the target window to the foreground first.
The script parses cleanly and its JSONL protocol loop (capabilities, error
envelopes, argument validation) was dry-run under PowerShell 7.6 on Linux with
the Windows-only calls stubbed. **Not yet exercised on a real Windows host**;
treat as unverified.

Still missing: Windows real-host verification;
keyboard/pointer injection, scrolling and dragging; isolated window capture;
full cross-provider image/desktop task verification; installation and broadcast
verification.
Installing a binary does not upgrade an already-running Borg process.
