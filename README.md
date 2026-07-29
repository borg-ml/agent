# Borg CLI

Borg CLI is a free and open-source agent harness and orchestrator written entirely
in Rust. It combines a responsive terminal UI, durable sessions, native tools,
major AI subscriptions and APIs, and optional remote control through borg.ml.

```sh
curl -fsSL https://borg.ml/install | sh
```

Windows PowerShell:

```powershell
irm https://borg.ml/install.ps1 | iex
```

The installer detects the operating system and CPU, verifies the release
checksum, installs to a user-local binary directory, and checks the installed
binary with `borg --version`.

Borg CLI checks for verified stable releases in the background and installs them
for the next launch without interrupting active work. Run `borg update`
(`borg install` is an alias) to update immediately, or `borg update --check`
to check without installing. Automatic updates and their check interval are
configurable.

## What Borg CLI provides

- a responsive terminal UI with durable resume, steering, compaction, image
  input, transcript selection, and configurable keybindings;
- Codex, Claude, OpenCode, Kimi, OpenRouter, and OpenAI-compatible providers;
- a native harness with bounded file tools, background
  processes, LSP, goals, plans, subagents, MCP, project guidance, and skills;
- provider-neutral extensions through local/project skills and configurable
  stdio MCP servers, plus configurable slash aliases and keybindings;
- non-blocking, checksum-verified updates that take effect on the next launch;
- **Full Access** (default), **Auto** model-reviewed commands, and **Manual**
  user-reviewed commands;
- Borg Remote hosts using the same durable session and tool runtime.

State, safety boundaries, validation, and tool execution are implemented in
typed Rust.

## Configuration

Copy [`configs/agent.example.toml`](configs/agent.example.toml) to
`$XDG_CONFIG_HOME/borg/agent.toml` (normally
`~/.config/borg/agent.toml`). The typed config supports slash-command aliases,
keybindings, provider-neutral stdio MCP servers, and the model/effort used by
native Auto approval review. Optional multiplayer, subagent, autonomous-team,
shared-work, presence, cloud/web relay, and telemetry capabilities can be
disabled independently; parent capability disablement cascades to dependent
features. Run `borg capabilities` (or `borg capabilities --json`) to inspect
the effective runtime.

Telemetry is disabled by default, and this release does not initialize a
telemetry exporter. Future telemetry support must document the exact emitted
fields and retention rather than treating README disclosure as consent.

Copy [`configs/editor.example.toml`](configs/editor.example.toml) to
`$XDG_CONFIG_HOME/borg/editor.toml` to configure transcript presentation and
active-turn input policy. `active_messages = "steer"` makes Enter inject at the
next Codex boundary; `"queue"` makes Enter start a later turn. The dedicated
queue keybinding remains available in either mode.

Automated CLI/terminal checks should use `borg agent --ephemeral --local-only`.
That creates a temporary session store and removes it when the process exits,
so health checks do not pollute the user's resume history or Remote workspace.

## Extend Borg CLI

Borg CLI uses composable, inspectable extension points instead of a private plugin
runtime:

- put `SKILL.md` packages in `.agents/skills`, `.borg/skills`,
  `~/.agents/skills`, `~/.borg/skills`, or `~/.codex/skills`;
- add local stdio servers under `[mcp.servers.<name>]` in `agent.toml` to expose
  arbitrary tools across Codex, Claude, OpenCode, and Borg's native providers;
- define slash-command aliases and remap every primary TUI action in the same
  typed config; and
- keep project-specific agent instructions beside the code they govern.

MCP extensions can be disabled per server and restricted with
`allowed_tools`. See the checked-in config examples for the complete schema.

## Repository layout

- `borg-provider`: provider-neutral messages, tools, usage and local adapters;
- `borg-remote`: session protocol, journal, actor, host and local tools; and
- `borg`: public CLI commands and terminal UI.

## Development

```sh
cargo check --workspace
cargo test --workspace
```

See [`CONTRIBUTING.md`](CONTRIBUTING.md) before submitting changes.

## Releases

Tags matching `v*` build checksum-paired `borg` archives for Linux, macOS, and
Windows on x86-64 and ARM64. Each release archive contains the self-contained
`borg` CLI, `LICENSE`, `NOTICE.md`, and this README.

---

AGPL-3.0-only OR a separate Borg Commercial Licence.
