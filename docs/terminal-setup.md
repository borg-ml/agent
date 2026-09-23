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
