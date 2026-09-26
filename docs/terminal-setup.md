# Terminal setup

## Ghostty

For an edge-to-edge Borg TUI, add these settings to your Ghostty configuration:

```ini
window-padding-x = 0
window-padding-y = 0
window-padding-balance = false
window-padding-color = extend-always
```

The same settings are available as [`configs/ghostty-borg.conf`](../configs/ghostty-borg.conf).
If you keep a source checkout, Ghostty can include that file with
`config-file = /absolute/path/to/agent/configs/ghostty-borg.conf`. The include
loads after the rest of your Ghostty configuration and overrides those four
settings. Open Ghostty's configuration with `Ctrl+,` and reload it with
`Ctrl+Shift+,`. Open a new pane after changing `window-padding-x` or
`window-padding-y`; Ghostty applies those dimensions to new terminals.

The last terminal row can leave a few pixels when a split pane's height is not
an exact multiple of the font's cell height. Borg cannot draw a partial row.
`extend-always` fills the remaining pixels with the nearest row's background,
so there is no visible bottom strip. The physical remainder can still exist.

## Numbered click hints

Hold Ctrl (or Cmd/Super where the terminal forwards it) to show up to ten
clickable targets. Press the displayed digit to click immediately: **1–9, then
0 for the tenth target**. No Enter is needed. Release the modifier or press Esc
to cancel without clicking. **F12** toggles the same mode when bare modifier
keys are unavailable; press F12 again or Esc to leave it.

Bare modifier press/release needs Kitty keyboard-protocol support. Some
terminals do not report it, and macOS/terminal shortcuts can intercept Cmd;
use F12 in those environments. Borg requests enhanced event reporting but does
not assume the terminal supports it.

Targets are numbered top-to-bottom, then left-to-right. Only the first ten
eligible visible targets receive hints; the rest remain mouse-accessible.
Open pickers expose only their enabled options. A changed target/layout
invalidates the current hints: release/re-hold or toggle F12 to capture again.
Unrelated streaming text does not invalidate unchanged targets. Invalid digits
stay out of the composer. Other shortcuts cancel hints and retain their normal
behavior, including Ctrl+C, Ctrl+Enter and minus/plus/equals zoom shortcuts.

Hints use the existing left-click actions, including message/tool expansion,
links, team navigation and footer controls. Scrollbars and text-selection areas
are not click actions. Approval prompts currently use their existing approval
keyboard shortcuts, not clickable buttons, and are not given numeric targets.
