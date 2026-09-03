# macOS notes

## Editor and clipboard shortcuts

Borg uses the native macOS editing conventions in the terminal composer:

- `Option+Left/Right` moves by word.
- `Command+Left/Right` moves to the start or end of the logical line.
- Adding `Shift` extends the composer selection.
- `Command+C` copies the current selection, or the selected/last response.
- `Command+V` accepts clipboard text or an image. Text delivered by the terminal
  uses bracketed paste; a Command+V key event reads the clipboard directly and
  prefers an image when both representations are present.
- `Command+F` opens `/find` for regex search within the current thread.

The `cmd`, `command`, and `super` modifier names are equivalent in custom
keybindings.

## Dictation setup

The first managed dictation setup installs the Parakeet runtime and model. On
macOS it also installs `ffmpeg` with Homebrew when `ffmpeg` is missing. A custom
`BORG_CLI_DICTATION_RECORD_COMMAND` remains fully user-managed and skips this
dependency installation.

## Ghostty bottom padding

Ghostty windows are not always an exact multiple of the terminal cell height.
The remaining pixels appear below the final terminal row and are outside
Borg's drawable grid. To make the final Borg row extend through that padding,
add this to the Ghostty configuration:

```ini
window-padding-color = extend-always
```

Ghostty applies this setting to newly opened windows and tabs. Optionally,
`window-padding-balance = true` distributes leftover pixels across opposite
edges instead of placing all of them at the bottom and right.
