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

Linux requires the desktop session bus and AT-SPI2; the worker is Borg itself
(`borg __computer-use-helper`). Input injection additionally requires a writable `/dev/uinput`
(the `input` group or a udev rule) and `wtype` on Wayland or `xdotool` on X11.

- `list_windows`: window IDs are scoped to the helper lifetime. On Linux the
  AT-SPI windows are merged with the compositor's own window list (niri IPC,
  sway, Hyprland, X11 EWMH, or `lswt` for other wlroots compositors), so windows
  with no accessibility tree — Unreal Editor, SDL games, XWayland apps — appear
  too, as `accessible: false` with ids like `niri:18`. Entries carry a
  `compositor` object (backend, native id, app_id, pid, workspace, output,
  focused, floating, visible, geometry, size); AT-SPI windows get it when they
  correlate unambiguously by pid/title. `list_windows` reports `window_backend`.
- `observe`: `window_id`, optional `max_nodes` (1–1000), optional `since` for
  a diff from the immediately preceding observation. Trees and text are bounded.
- `click`: semantic AT-SPI action, not an inferred coordinate click.
- `set_value`: replace editable text, including Unicode; password controls refused.
- Both actions require `window_id`, `element_id`, and the latest `observation_id`.
  Handles are checked against the window ancestry and observed state. An attempted
  effect consumes its observation. Results contain a new tree and a bounded
  tree-settling indicator, **not** a guarantee of application-level completion.
- `screenshot` requires an explicit scope. `scope: "desktop"` uses `grim` and
  captures the entire visible desktop. `scope: "window"` with `window_id`
  captures one compositor-listed window (see below). Both return bounded inline
  PNG attachments (window captures over 4 MiB are downscaled and report `scale`).
  For tree plus image, use `observe` with `screenshot: true` and
  `screenshot_scope`. Observing a window without an accessibility tree returns
  an empty tree plus the requested screenshot. There is no silent whole-screen
  fallback.

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

Linux: on Wayland, observed `bounds` are AT-SPI window-relative coordinates
(GTK4 reports its screen extents as 0,0), which match `scope: "window"`
screenshot pixels at scale 1. Element-targeted `pointer_click`/`scroll` map them
through the window's desktop origin (below) and fail with a clear error when it
cannot be determined. Pointer ops and `drag` take `coordinate_space: "window"`
to target pixels of the latest window screenshot. Every injection op (and
`click`/`set_value`) takes `restore_focus: true` to give focus back to the
window the human had before Borg started moving focus.

### Linux compositor windows, window capture and games

- Window capture on niri (verified on niri 26.04): `niri msg action
  screenshot-window` over the niri socket renders only that window's surfaces,
  even on another workspace or scrolled off-screen, without a focus change.
  niri 26.04 has no per-toplevel capture protocol (wlr-screencopy is
  output/region only; no ext-image-copy-capture; upstream main added it for
  outputs and cursors only), so this is the only isolated capture. Its side
  effects are real: niri copies every capture to the clipboard and shows a
  transient "Screenshot captured" notification. The helper snapshots the
  clipboard (one MIME type, via wl-clipboard), waits until niri's image
  selection lands, and restores it — unless it changed again meanwhile.
- Window capture elsewhere: `grim -T` when the compositor exposes an
  ext-foreign-toplevel identifier; sway/Hyprland crop the composited desktop to
  the IPC geometry (overlapping windows show; hidden windows are briefly brought
  into view and prior focus restored); X11 uses ImageMagick `import -window`.
- Window → desktop mapping: sway, Hyprland and X11 give absolute geometry, as
  does niri for floating windows (`tile_pos_in_workspace_view`), which is used
  first and needs no capture. niri exposes no scroll position for tiled windows,
  so the fallback locates the window by matching textured strips of a fresh
  window capture in a desktop capture. It
  requires the same position over 300 ms (focus changes animate the view),
  refuses ties such as two identical-looking windows, and re-checks the
  position right before pressing a button, aborting if the window moved.
- Input to compositor windows focuses them through the compositor IPC and
  verifies the focus before injecting.
- `pointer_move {window_id, dx, dy, steps?, duration_ms?, hold_keys?, x?, y?}`
  emits relative REL_X/REL_Y motion from a second Borg uinput device ("Borg
  virtual mouse", a plain mouse to libinput). Wayland sends motion and grants
  pointer lock only to the surface under the pointer, so the first move into a
  window places the pointer at its centre (or at `x`,`y`). Pointer-locked apps
  (SDL relative mode, games) receive the exact unaccelerated counts; the
  visible cursor follows compositor acceleration. `key` takes `hold_ms` (up to
  10 s) and bare modifiers; `hold_keys` holds keys during a `pointer_move`.

Verified live on niri 26.04 with SDL3 test windows (no AT-SPI tree, native
Wayland and XWayland) and a GTK4 window: listing, isolated capture of a window
on a hidden workspace, a held W key (1503 ms measured by the app), window-space
clicks landing on the requested pixel, element-targeted clicks on GTK4 buttons,
pointer-locked relative motion arriving as 10-count deltas, and focus restored
to the human's window. The X11 EWMH path was verified in Xvfb (listing and
`import -window` capture only; uinput reaches the real seat, not Xvfb).

### Linux private display (preferred for testing apps and games)

`launch {argv, env?, cwd?, x11?, wait?, width?, height?}` runs an app on a
session-owned headless display served by `borg-display`, a small Borg
compositor shipped beside `borg`. The display starts on demand, is reused for
the session, and is torn down with it (also when the helper is killed); apps
launched into it are killed at teardown unless `detached`.

- Rendering is on the GPU: GLES on the boot VGA render node, with dmabuf so
  Vulkan/GL clients render directly. X11-only apps (`x11: true`) run through
  xwayland-satellite with GPU glamor. `capabilities.private_display` reports
  the renderer, whether it is hardware-accelerated, and its limitations.
- `list_windows {display: "private"}` returns `pd:` window ids; every op then
  works on them. `screenshot {display: "private", scope: "desktop"}` captures
  the display, `scope: "window"` one window, and `cursor: true` draws the
  pointer. Pointer x,y are private display pixels.
- All input goes through the compositor's private seat, never uinput or the
  user's focused window. `type_text` types any Unicode (characters outside
  the layout go through a temporary keymap). Apps can lock or confine the
  pointer (`zwp_pointer_constraints_v1`); `pointer_move` dx/dy then arrive as
  exact relative motion while the pointer stays put.
- Sub-agents may use only a private display: desktop windows, desktop
  screenshots and seat input are refused for them. Each child gets its own
  display; `attach_display {display_id}` shares a parent's instead, and
  detaching kills only the child's apps.
- Without `borg-display` (for example an older install), the private display is
  reported unavailable with how to get it: update or reinstall Borg, or set
  `BORG_DISPLAY_BIN`.

Verified live on niri 26.04 with an AMD RX 7900 GRE: vkcube (RADV) and
glxgears over X11 rendering on the GPU, GTK typing and clicks, an SDL3 app in
relative mouse mode receiving exact deltas, ✓ and é typed into SDL3 and GTK,
and the human's focused window, pointer and input devices unchanged.

### Linux input backend (implemented, test-only verification)

`computer_use/linux/input.rs` injects through a Borg-owned evdev uinput device
("Borg virtual input": keys, mouse buttons, wheel and an absolute pointer axis)
created lazily on the first injection and owned by the helper process; Unicode
`type_text` goes through `wtype` on Wayland or `xdotool type` on X11 (no
`ydotool` daemon). `capabilities` lists the five injection ops only when
a writable `/dev/uinput` and the typing tool are present, and
otherwise names what is missing. Pointer coordinates are desktop screenshot
pixels: the helper maps them onto the absolute axis using the size of the last
`scope: "desktop"` screenshot (probed once with `grim`/`xdotool` when none was
taken) so a point read from a screenshot lands on that pixel; on multi-output or
scaled layouts the compositor decides how an absolute device maps, so verify
with a screenshot. `scroll` emits wheel notches (about 120 px each, at least one
for any non-zero request) and reports them. Wayland compositors may refuse
focus stealing, so instead of raising blindly the helper checks the AT-SPI
`ACTIVE` state after a `grab_focus` attempt and refuses with a clear error when
the target window is not active — injected events always reach the focused
window. Element-targeted `pointer_click`/`scroll` use AT-SPI screen extents on X11
and window-relative extents plus the compositor's window origin on Wayland. Key names match the other
platforms (`delete` is backspace, `forwarddelete` deletes forward; modifiers
`ctrl`/`alt`/`shift`/`cmd`|`super`); uinput key codes are physical, so the
compositor's keyboard layout applies. Verified so far without live desktop
actions: key parsing, axis mapping, capability reporting, error paths, and
device creation/udev classification with no events emitted.

Python code mode: `cua.capabilities()`, `cua.list_windows()`,
`cua.observe(window_id)`, `cua.screenshot("desktop")` or `cua.screenshot("window", window_id)`, `cua.pointer_move(window_id, dx, dy)`,
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

AT-SPI coordinates on Wayland are **not** assumed to be desktop coordinates;
they are mapped through the compositor window origin as described above.

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

macOS input injection (CGEvent, verified on the Mac — see below): `type_text`
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

Round 4 on the same host (main `58a4a63`, macOS 26.2 arm64) confirmed the
fixes: `scroll` on a 200-line TextEdit document moved the text area by exactly
the requested 600 px from the centre of its `visible_bounds`; partially clipped
elements report the clipped rect and fully hidden Finder rows report
`visible_bounds: null` with `pointer_click` refused as scrolled out of view;
`selected_text` reflects `cmd+a`, shift-arrow selection and drag selection; and
`type_text`, `key`, element-targeted `pointer_click` and `drag` all still pass.
Observed caveats: one `type_text` returned ok while the focus race dropped the
text (a fresh observe and retry landed it), and a macOS TCC prompt blocks
injection with a clean "could not bring the target application to the front"
error until dismissed.

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
