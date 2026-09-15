# Codex Desktop Computer Use — Teardown & Borg Design

Source: ChatGPT.app (unified Codex desktop) **26.908.61612**, published 2026-09-14, pulled from
`https://persistent.oaistatic.com/codex-app-prod/appcast.xml` (Sparkle feed). Everything below is from
reading the shipped, unobfuscated JS/docs and Mach-O headers under
`Contents/Resources/{plugins/openai-bundled,cua_node}` — plus the open-source `openai/codex` Rust harness.
No proprietary binary code was copied; this is a design/protocol description for a clean-room reimplementation.

## 1. Layered architecture (what actually runs)

```
model (Astra) writes JS
        │  MCP tool call:  cua_repl.js  (or legacy node_repl.js)
        ▼
cua_node/bin/node_repl          ← custom Node 24 binary; injects globalThis rpc bridge
        │  import("@oai/cua")  →  `cua`  (or "@oai/sky" → `sky`)
        ▼
@oai/sky | @oai/cua  (pure-JS client)   ← serializes actions, does AX-tree DIFFING, screenshot caching
        │  o.rpc({type:"execute", method, ...})   ← RPC brokered by the node_repl host
        ▼
per-platform native helper  (thin, dumb, does OS calls only)
   mac:     Codex Computer Use.app / SkyComputerUseService   (pipe IPC "CodexComputerUseIPC-5")
   linux:   bin/linux/sky_linux_{arm64,x64}                  (stdio/RPC)
   windows: *.exe helper  (helper_transport.js)              (WGC + UI Automation + SendInput)
```

Key inversion vs. a naive computer-use tool: **the model does not call `click`/`screenshot` as individual
MCP tools.** It calls one MCP tool (`cua_repl.js` — a `js` eval tool, `output_token_limit: 25000`) and
writes a *script* that batches many observe/act steps. This is "code mode" and it is the single biggest
throughput lever. From `unified-computer-use/.mcp.json`:

```json
"cua_repl": { "command": "node", "enabled": false,
  "enabled_tools": ["js","js_reset","turn_ended"],
  "omit_tools_from": ["code_mode","deferred"],
  "tools": { "js": { "output_token_limit": 25000 } } }
```
`enabled:false` = gated on (Astra) rollout. `node_repl` (legacy, `computer-use` plugin) is the same idea
backed by a native launcher; `cua_repl` is the newer "unified" one that also owns browser tabs.

## 2. The model-facing API (`@oai/cua` "tinysky", and `@oai/sky`)

One `Target` interface, identical across apps/tabs/windows/platforms:

```ts
interface Target {
  getAXState(o?): Promise<string>;                 // accessibility tree as TEXT with element indices
  getScreenshot(o?): Promise<Uint8Array>;
  getAXStateAndScreenshot(o?): Promise<{state,screenshot}>;
  click(target: number | [x,y], o?): Promise<void>; // number = element_index (preferred) OR coords
  drag(from,to); pressKey(key);                       // xdotool/X keysym chords, e.g. "Control_L+a"
  scroll(target, dir, pages?); typeText(text); paste(text,{format});
  setValue(i, value); selectText(i, text, o?); performSecondaryAction(i, action);
}
const cua = { getState, getApp, listApps, getBrowser, createBrowserTab, getTab, listBrowsers, listTabs };
```

The decisive design choices (these are the accuracy + throughput wins, not the model weights):

1. **Accessibility-tree-first, screenshot-fallback.** `getAXState()` returns a *text* serialization of the
   AX tree where every actionable node has an `element_index`. The model acts on indices
   (`click(42)`, `setValue(42,"…")`) — semantic, not pixel coordinates. Screenshots are only fetched when
   the AX tree is insufficient. Coordinates are a fallback within the same call.
2. **AX-tree diffing.** `getAXState()` returns *only removed/added/changed* nodes vs. the previous tree by
   default (`disableDiffing:true` for a full tree). Massive token reduction on repeated observations.
3. **Auto-settle.** After an action the runtime waits ~1s, extended up to ~5s if a loading indicator or
   ongoing mutations are detected (mac uses `AXObserver` change notifications). The model never sleeps.
4. **Batch action+observation in one tool call.** Docs explicitly instruct: "Batch deterministic actions
   and the resulting `getAXState()` into one call." `getApp`/`getTab`/`createBrowserTab` auto-include the
   fresh state in their result. One model round-trip ⇒ several UI steps ⇒ one fresh state.
5. **Per-target routing.** Every action carries its `app`/`window`/`tab`. Input is delivered to that
   target specifically (not "global cursor"), which is what lets it run in the background without stealing
   your pointer, and run many targets in parallel.
6. **paste restores the clipboard** (uses pasteboard then restores prior contents); formatted `md/html`.

## 3. macOS native driver — `SkyComputerUseService`

`Codex Computer Use.app`, bundle id `com.openai.sky.CUAService`, **`LSUIElement=true`** (headless
background agent, no Dock icon). Linked frameworks + undefined symbols (from `otool -L` / `nm -u`):

- **Capture:** `ScreenCaptureKit` (`SCStream`, `SCShareableContent`, `SCStreamConfiguration`) — per-window
  capture, not whole-screen. `CoreMedia` for frame plumbing.
- **Accessibility read + act:** full `AXUIElement*` surface — `AXUIElementCopyAttributeValues`,
  `CopyElementAtPosition`, `PerformAction`, `SetAttributeValue`, `CopyParameterizedAttributeValue`, and
  **`AXObserverCreateWithInfoCallback`** (the settle/mutation signal).
- **Input:** `CoreGraphics` + `Carbon` (CGEvent synthesis / HIToolbox keyboard).
- **Window server:** `CGSMainConnectionID`, `CGShieldingWindowLevel`, `CGSessionCopyCurrentDictionary`
  (used by the lock-screen shield + session state).
- **Transport:** `MacNativePipeTransport`, protocol string `CodexComputerUseIPC-5`. Request enums:
  `ComputerUseIPCListAppsRequest`, `…AppGetSkyshotRequest` (a "Skyshot" = AX snapshot + screenshot),
  `…AppPerformActionRequest`, `…AppStartRequest`, `…AppPolicyRequest`, `…{Start,Stop}AudioRecordingRequest`.
  The JS side is pure serialization; the native service does capture/AX/input and returns the Skyshot.
- App launch is transparent (`get_app_state` starts the app in the background if needed).

There is a `bin/mac/normal` and `bin/mac/relaxed` variant of the whole service (relaxed = looser
hardened-runtime/sandbox for entitlement-constrained contexts).

## 4. Locked-screen use (macOS)

`Codex Computer Use.app/Contents/SharedSupport/` ships:
- **`CUALockScreenGuardian.app`** — watches for local input; there's a `*_Parent.coderequirement` pinning
  which parent may talk to it.
- **`CodexComputerUseAuthorizationPlugin.bundle`** (+ `…InstallerTool`) — a macOS **AuthorizationPlugin**
  installed into the login/authorization stack. This is what allows the service to keep operating a
  locked session during an active trusted turn. It uses `CGShieldingWindowLevel` to cover displays and
  relocks instantly on local keyboard/mouse activity. Narrow scope: it is not a general remote-unlock.

For Borg this is the hardest/most OS-invasive piece and I would **not** replicate it initially — it's a
polish feature, not core throughput/accuracy.

## 5. Cross-platform structure (already three-OS in Codex)

Same `Target` interface, three native backends selected at runtime by `target`:

| Platform | Capture | Accessibility | Input | Helper |
|---|---|---|---|---|
| macOS   | ScreenCaptureKit (per-window) | AXUIElement + AXObserver | CGEvent/Carbon | `SkyComputerUseService` via pipe (`CodexComputerUseIPC-5`) |
| Windows | Windows Graphics Capture (`test:windows-wgc`) | UI Automation (IUIAutomation) | SendInput | `*.exe` via `helper_transport.js` |
| Linux   | `sky_linux_{arm64,x64}` native binary | (AT-SPI/native in binary) | (native in binary) | `sky_linux` over stdio/RPC; JS `ActionSettler`, `drag_handle` via `drag_start/move/end` RPCs |

The **window2 API** (`sky-window2-api.md`, `target:"windows"`) is the newest surface: everything is
addressed by an opaque `Window {app,id,title}`; `get_window_state({include_screenshot,include_text})`;
actions take window-relative coords or `element_index`; `activate_window` is an explicit escape hatch. This
window-scoped model is the cleanest to copy for a portable design.

## 6. Safety / confirmations (from `SKILL.md` + open-source Guardian)

- A detailed **Computer Use Confirmations Policy** ships as a skill doc: a taxonomy of action risk
  (Hand-off / Always-confirm / Pre-approval-ok / No-confirm), distinguishing **user-authored** intent from
  **third-party content** (prompt-injection defense: pasted/site text is never permission). Typing sensitive
  data into a form counts as "transmission." CAPTCHAs, password changes, financial confirms, software
  install = always confirm.
- The open-source harness adds a **Guardian** reviewer: `node_repl_policy.md` recursively evaluates every
  action inside a `node_repl`/`cua_repl` script; `guardian-v2/async_scorer` runs review off the critical
  path with fast-approvals restricted to browser/computer-use tools; `turn_ended` hooks (Stop/Interrupt/
  SubagentStop) clean up per turn (see `unified-computer-use/plugin.json` hooks).

## 6b. Screenshot / action pipeline — verified specifics

From driver strings (Swift symbols in `SkyComputerUseService`) and the API docs:

- **"Skyshot" = one observation** = AX snapshot (+ optional screenshot) captured together. Native types:
  `SkyshotClassifier`, `ComputerUse/RefetchableSkyshotAXTree.swift`, `AccessibilityRole(Kind)`.
- **AX diff is native and line-budgeted.** `AccessibilityDifferenceLineBudgetExceeded`,
  "Child page depth differs", "values differ from index …" — the Swift service computes the removed/
  added/changed diff with a bounded line budget, so a huge tree can't blow the token budget; on overflow
  it falls back to a full/truncated tree. Diff is *not* done in JS.
- **Settle = debounced AX notifications.** `_axNotificationDebounceTasks`,
  `AsyncDebounceSequence`/`DebounceStorage.swift`, plus `SystemLockScreenSettleObservation`. The native
  side listens to `AXObserver` change notifications and a CGWindow resize signal (`AX.windowResized`,
  `CGWindow.didResize`) and waits until the UI quiesces (base ~1s, up to ~5s) before returning the Skyshot.
- **Screenshots are JPEG**, delivered three ways: `{ bytes: Uint8Array, data_url: base64 JPEG, filepath }`.
  In node_repl they arrive as `file://` URLs (read from disk, `emitImage`). `backingScaleFactor` is applied
  (captures at logical resolution, not raw Retina pixels) — a token/latency win for the vision path.
- **Accessibility tree representation differs by surface:** macOS/browser return a **text** serialization
  with integer `element_index`; the Linux full-desktop API returns a **JSON** `AccessibilityNode` tree with
  string `element_id` and `ax_tree_source: "at_spi" | "x11"` (AT-SPI primary, dependency-free X11 fallback),
  plus a `query` string that filters the tree to matching nodes and their ancestors. Design lesson: give the
  model a compact indexed representation and a server-side subtree query to bound output.
- **Screenshot caching for coordinate actions:** `click/drag/scroll` accept an optional `screenshotId` tying
  the coordinate to a specific cached frame, so a coordinate action is validated against the frame the model
  actually saw (guards against acting on a stale view).

## 7. Recommended design for Borg (best-in-class throughput + accuracy, all 3 OSes)

1. **Code-mode REPL tool, not per-action tools.** Expose one sandboxed JS/Python REPL tool; give the model
   a `cua` library with the `Target` interface. Let it batch N actions + a final `getAXState()` per call.
   Cap tool output (~25k tokens) and keep REPL state across calls.
2. **AX-tree-first with diffing.** Serialize the platform accessibility tree to compact text with stable
   `element_index`; return diffs by default, full tree on demand, screenshot only as fallback/confirmation.
   This is where both accuracy (semantic targets) and throughput (few tokens, few round-trips) come from.
3. **Auto-settle** using native change observers (AXObserver / UIA StructureChanged / AT-SPI signals),
   ~1s base, extend on loading/mutation. No model-driven sleeps.
4. **Thin native helper per OS, one wire protocol.** Mirror Codex: pure-logic client in the agent runtime;
   a small native helper per platform doing only capture+AX+input over a line-delimited JSON/RPC pipe.
   - macOS: ScreenCaptureKit + AXUIElement + CGEvent, `LSUIElement` background agent.
   - Windows: Windows Graphics Capture + UI Automation + SendInput.
   - Linux: pick per display server — X11 (XTEST + AT-SPI + XShm/XComposite per-window) and Wayland
     (`libei`/`ydotool` for input, xdg-desktop-portal ScreenCast/PipeWire for capture, AT-SPI for a11y).
     Codex ships a single prebuilt `sky_linux` binary that abstracts this; plan for the Wayland/X split.
5. **Per-target (window) addressing** so background + parallel operation works and you never fight the
   user's cursor. Prefer the window2-style opaque `Window` handle.
6. **Layer the confirmations policy + a recursive action reviewer** (adapt `node_repl_policy.md`) and a
   per-turn cleanup hook. Defer locked-screen use.
7. **Bootstrap option:** Codex's `cua_repl` is a standard MCP server; Borg could host the official plugin
   first (as OpenClaw does via `/codex computer-use install`) to get a working baseline, then swap in an
   own clean-room helper stack.

## Artifacts on disk (this machine)
- Mounted image: `/tmp/codex-re/mnt/ChatGPT.app` (dmg at `/tmp/codex-re/Codex.dmg`).
- Plugins: `…/Resources/plugins/openai-bundled/plugins/{computer-use,unified-computer-use,computer-history,browser,chrome}`
- Runtime + client + native driver: `…/Resources/cua_node/` (`bin/node_repl`, `lib/node_modules/@oai/{sky,cua,cua-repl,browser-desktop}`)
- Native driver app: `…/@oai/sky/Codex Computer Use.app` (`SkyComputerUseService`, `CUALockScreenGuardian`, `CodexComputerUseAuthorizationPlugin.bundle`)
- Model-facing docs: `@oai/cua/docs/tinysky-alt-core-cua-repl.md`, `@oai/sky/docs/sky-window2-api.md`,
  `computer-use/.codex-plugin/computer-use-node-repl.md`, `computer-use/skills/computer-use/SKILL.md`
