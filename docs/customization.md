# Customizing Borg Agent

Borg has three customization layers:

1. `editor.toml` controls the terminal UI and interaction behavior.
2. `agent.toml` controls capabilities, keybindings, aliases, providers, and extension policy.
3. Blu packages add skills, commands, tools, hooks, workflows, or native code.

The default user configuration directory is `$XDG_CONFIG_HOME/borg`, or
`~/.config/borg` when `XDG_CONFIG_HOME` is unset. Start from
[`configs/editor.example.toml`](../configs/editor.example.toml) and
[`configs/agent.example.toml`](../configs/agent.example.toml). An explicit
`borg agent --config PATH` replaces the default agent configuration path.

## Interactive editor settings

Run `/settings` to open the settings picker. Changes made by editor pickers are
persisted to `editor.toml`.

| Setting | Command | Configuration |
| --- | --- | --- |
| Active-turn messages | `/followups` | `interaction.active_messages = "steer"` or `"queue"` |
| Keep machine awake | `/sleep` | `interaction.prevent_sleep = true` or `false` |
| Desktop completion notification | `/notifications` | `interaction.completion_notifications = "off"`, `"unfocused"`, or `"always"` |
| Completion sound | `/sound` | `interaction.completion_sound = "off"`, `"unfocused"`, or `"always"` |
| Refresh rate | `/refresh` | `presentation.refresh_rate_fps = 15..240` |
| Edit diff display | `/expand-edits` | `presentation.diff_expansion = "expanded"`, `"collapsed"`, or `"until_next_action"` |
| Expand tool details | `/expand-tools` | `presentation.auto_expand_tools = true` or `false` |
| Action descriptors | `/action-descriptors` | `presentation.action_descriptors = true` or `false` |
| Running animations | `/animations` | `presentation.running_sweeps = true` or `false` |
| Dictation icon | `/icons` | `presentation.dictation_icon = "nerd_font"` or `"emoji"` |
| Transcript labels | `/user-label`, `/assistant-label` | `transcript.user_label`, `transcript.assistant_label` |
| Transcript colors | `/colors`, `/color` | the four `transcript.*_color` values in `#RRGGBB` form |

Notification and sound policies are independent. This always shows a desktop
notification but only sounds when the terminal is unfocused:

```toml
[interaction]
completion_notifications = "always"
completion_sound = "unfocused"
```

Borg uses the terminal's desktop-notification and bell protocols. Whether a
bell is audible follows the terminal and operating-system settings. Terminals
that support focus reporting let `unfocused` distinguish foreground from
background windows. Replayed session history never produces alerts.

## Dictation model

Borg downloads Parakeet TDT 0.6B V2 by default. To use a smaller or otherwise
different GGUF supported by `parakeet-server`, point the managed backend at the
local model file before launching Borg:

```sh
export BORG_CLI_DICTATION_MODEL_PATH=/path/to/model.gguf
```

Borg still installs and manages the Parakeet runtime, but skips the bundled
609 MiB model download and starts the server with the selected file. The
existing `BORG_CLI_DICTATION_MODEL` setting remains the API model name for an
externally managed dictation endpoint.

## Keybindings and command aliases

Keybindings live in `agent.toml`. An action accepts one or more chords; omitted
actions retain their defaults. Supported modifiers are `ctrl`, `alt`, and
`shift`.

```toml
[keybindings]
send = ["enter"]
queue = ["tab"]
newline = ["shift+enter", "alt+enter"]
interrupt = ["esc"]
copy = ["ctrl+y"]
```

Aliases prepend a built-in slash command and preserve trailing arguments:

```toml
[commands.aliases]
quick = "/fast on"
deep = "/effort xhigh"
```

## Extension authority

A package declares its required authority in `blu.toml`:

```toml
runtime_access = "sandboxed" # sandboxed | trusted | native
```

- `sandboxed` packages may use skills and embedded Blu/Lua/Luau workflows.
  They cannot launch MCP or external workflow processes.
- `trusted` packages may launch MCP servers and supervised external workflows
  with the user's operating-system authority.
- `native` packages load compiled code into Borg's process. Native code has
  the same authority and failure domain as Borg itself.

The user's `agent.toml` caps package authority:

```toml
[extensions]
default_access = "trusted"
project_access = "sandboxed"
native_access = "prompt" # deny | prompt | allow
```

`default_access` applies to user-installed packages and `project_access` to
packages under `.borg/extensions`. A repository cannot raise these limits.
`native_access = "allow"` is the deliberate, prompt-free setting. The legacy
`allow_project_mcp = true` setting raises project MCP packages to trusted for
compatibility.

Use `borg extensions list`, `borg extensions info ID`, `borg extensions doctor`,
or `/extensions` to see requested access, activation state, and admission
failures. Valid package changes are picked up at the next turn boundary.

### Editor API

An active package can contribute a validated partial `editor.toml` tree plus
keybindings and command aliases. This covers every public editor preference;
unknown fields and invalid chords isolate the package.

```toml
[api.editor.transcript]
assistant_label = "friend"
assistant_label_color = "#c084fc"

[api.editor.presentation]
auto_expand_tools = true
running_sweeps = false

[api.editor.layout]
horizontal_margin = 4
composer_max_height = 12
show_footer = true

[api.keybindings]
send = ["ctrl+enter"]

[api.aliases]
ship = "/fast on"
```

Declarative commands, tools, prompt/context transforms, and lifecycle hooks use
the same `[api]` snapshot. See [`blu-extensions.md`](blu-extensions.md).

## Native extensions

The C header is only the binary contract for `native` packages. Blu, Lua, and
Luau workflows do not include it and continue to use `[api.*]` plus Borg's Blu
host functions. A C ABI is used because it is the portable common boundary for
compiled extensions written in C, C++, Rust, Zig, Swift, or another language;
it does not require the extension itself to be written in C.

Native packages pair `runtime_access = "native"` with a hash-pinned library:

```toml
runtime_access = "native"

[native]
library = "lib/my_extension.so"
sha256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
abi_version = 2
```

The library must stay inside the package. ABI v2 is published as
[`include/borg_extension.h`](../include/borg_extension.h) and provides resolved
configuration, structured event emission, logging, an opaque instance handle,
and an optional shutdown callback:

```c
#include "borg_extension.h"

int32_t borg_extension_init_v2(const borg_extension_host_v2 *host, void **handle) {
    host->log(1, "extension ready");
    *handle = 0;
    return 0;
}
```

ABI v1's `borg_extension_init(uint32_t)` remains supported for compatibility.

Borg verifies the SHA-256 digest before loading. A zero result activates the
extension; a nonzero result isolates it. Loaded native libraries are not hot
unloaded. If their bytes change, restart Borg before activating the new build.

Native extensions are the escape hatch for arbitrary in-process behavior.
Stable, portable integrations should prefer Blu workflows and the versioned
extension API described in [`blu-extensions.md`](blu-extensions.md).

## Package settings and live reload

Extensions can declare typed settings in their manifest. Values live in the
user or project `blu.toml`, keeping installed packages immutable. Blu supports
string, integer, float, boolean, and array values, secret redaction, and
`${config.name}`, `${env.NAME}`, and `${extension_dir}` interpolation.

The complete package, workflow, hook, storage, and persistent-runtime contract
is in [`blu-extensions.md`](blu-extensions.md). Run `borg extensions doctor`
after editing a manifest; invalid packages are isolated without replacing the
last-known-good runtime snapshot.

## Profiles and effective-state inspection

`borg customize inspect` shows the contributing files, effective editor state,
keybindings, aliases, extension access policy, catalog revision, and every
extension admission decision. Add `--json` for tooling.

`borg customize export PROFILE.json` writes user agent/editor settings plus user
and current-project Blu state as one versioned profile. Use `--force` to replace
an existing archive. `borg customize import PROFILE.json` validates the entire
archive before changing anything; `--force` deliberately makes the imported
profile exact, including removing target state files represented as absent.
Profile files can contain configured secrets and are created through a private
temporary file, so treat them as credentials.

## Runtime version freshness

Borg Agent pins Blu to an exact Git revision so builds remain reproducible.
`just blu-check` verifies that the pin equals the current upstream Blu HEAD;
release CI runs this as a hard gate. `just blu-update` advances the manifest and
lockfile together when upstream moves. A release therefore cannot silently ship
an older Blu revision.
