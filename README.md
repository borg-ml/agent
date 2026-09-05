# Borg Agent

Borg Agent is a Rust-based agent harness and orchestrator. It provides a
terminal and native GPUI frontends, durable sessions, native tools, provider
adapters, and optional remote hosts. Releases include the `borg` CLI; the
experimental `borg-gui` frontend is developed and built separately.

[Repository](https://github.com/borg-ml/agent) ·
[Blu language](https://github.com/borg-ml/blu) ·
[Documentation](docs/) ·
[简体中文](docs/zh-Hans/README.md) ·
[Español](docs/es/README.md) ·
[Русский](docs/ru/README.md)

## Install

Linux and macOS:

```sh
curl -fsSL https://borg.ml/install | sh
```

Windows PowerShell:

```powershell
irm https://borg.ml/install.ps1 | iex
```

The installer chooses the platform and CPU, verifies the release checksum, and
installs `borg` in a user-local binary directory. Run `borg update` to update
an existing installation. The same release also installs the native GUI
launcher.

## Use Borg

Run `borg` or `borg agent` to start an interactive session. Useful commands
include:

```sh
borg resume
borg gui
borg capabilities
borg extensions list
```

Interactive sessions run in a detached per-session host. Closing one TUI or
GUI view does not stop an active turn, its provider app server, or subagents;
another view can attach to the same durable session. An unattended host exits
after five minutes only when the session is ready, has no pending prompt, and
has no running background processes.
See [`docs/session-lifecycle.md`](docs/session-lifecycle.md).

The default native-provider harness exposes one shell-first `exec` surface.
The model can use shell pipelines or invoke the installed language best suited
to the problem; session-scoped Borg, Blu, plugin, workflow, and collaboration
capabilities are discovered from that shell with `borg tools` and invoked with
`borg call NAME JSON`. Set `capabilities.harness = "native"` only when the
direct one-tool-per-capability fallback is preferred.

Ask Borg to monitor a log or deployment, for example: “Watch the build log and
let me know when it fails.” The `monitor` tool runs a background shell command
and delivers stdout lines automatically, in bounded batches. `list_monitors`
and `stop_monitor` manage the watches. Monitors last for the current session,
up to 24 hours (or the host's command limit), and stop with their process trees
when the session ends. A monitor can wake an idle agent; human messages take
priority, and arriving events are handled at the next turn boundary.

If the provider connection drops, Borg saves the interrupted work and retries
automatically with delays capped at 30 seconds. The terminal shows the retry
countdown; Escape cancels recovery. Completed tool work is included in recovery
context so the agent can check interrupted commands before continuing.

On macOS, Option/Ctrl+Left/Right move by word, Cmd+Left/Right go to line
boundaries, and Cmd+Up/Down go to the start/end of the composer. Shift extends
the selection. Legacy terminal word and line shortcuts are supported too.
Full-screen action inspection wraps long commands so their final arguments
remain visible alongside the output.

Bring existing conversations with you using `/import` or `borg import`.
Threads and Memory are both selected by default, with independent opt-outs.
Codex CLI/Desktop, Claude Code, Claude Desktop exports, and portable JSON are
supported. Imports copy originals, skip duplicates, and appear in `/resume`.
See [importing threads and memory](docs/importing.md).

## What it provides

- Responsive TUI and native GPU-rendered frontends over the same durable
  session runtime, with resumable sessions, compaction, image input, transcript
  navigation, dictation, goals, plans, and subagent controls.
- Codex and Claude subscription adapters, OpenCode, Kimi, OpenRouter, and
  configured OpenAI-compatible providers.
- A provider-neutral `borg-core` crate with no provider SDK, HTTP, MCP, or
  subprocess dependency.
- A shell-first, polyglot execution surface backed by files, processes, LSP,
  MCP, skills, goals, plans, Blu workflows, and subagents.
- Provider-neutral web search with Exa, Firecrawl, Parallel, and Brave
  backends; see [`docs/web-search.md`](docs/web-search.md).
- Full Access, Auto, and Manual command-approval modes.
- Borg Remote hosts that use the same durable session and tool runtime.
  See the [unattended-host runbook](docs/remote-unattended-runbook.md) for
  preflight, fault recovery, and rollback procedures.
- Agent discovery and direct messaging across local projects and machines
  enrolled under the same account. Agents use `list_instances`, then
  `send_message` or `followup_task`; no shared project workspace is required.
  See [multiplayer messaging](docs/multiplayer-workspaces.md).

Extension authority is user-controlled. Packages can request `sandboxed`,
`trusted`, or `native` runtime access; user policy caps project and user
packages independently. Native mode loads hash-pinned, versioned C ABI code
inside Borg's process and is never enabled by repository configuration alone.

## Configuration

Copy the example files to Borg's configuration directory:

```sh
cp configs/agent.example.toml ~/.config/borg/agent.toml
cp configs/editor.example.toml ~/.config/borg/editor.toml
```

Use `$XDG_CONFIG_HOME/borg` instead when `XDG_CONFIG_HOME` is set. The agent
configuration covers providers, capabilities, MCP servers, aliases, and team
settings. The agent interface configuration covers shared language plus TUI
presentation and input behavior; the native frontend uses the same interface
language with platform-native input and rendering. `/ui-language` changes UI
labels, while `/language` independently controls the model's response language.

See [`docs/customization.md`](docs/customization.md) for agent interface settings,
keybindings, alerts, extension authority, and native extension authoring.
Use `borg customize inspect`, `borg customize export`, and
`borg customize import` to inspect or move the complete effective setup.

`borg capabilities --json` shows the effective runtime capabilities. Automated
checks can use `borg agent --ephemeral --local-only` to avoid changing resume
history.

## Privacy-minimal usage count

Release download totals provide the all-time installation proxy. To estimate
active installations, release builds send at most one content-free heartbeat
per day. It contains one random identifier that rotates every 31 days—no
version, OS, model, session, prompt, path, or device data. Set
`usage_count.enabled = false` in `agent.toml` or export
`BORG_DISABLE_USAGE_COUNT=1` to disable it. The receiver must discard network
metadata and raw request logs and retain only aggregate daily/monthly counts.

## Blu extensions

[Blu](https://github.com/borg-ml/blu) is a Lua/Luau superset language and
runtime. Its source and documentation live in the separate
[`borg-ml/blu`](https://github.com/borg-ml/blu) repository. Borg embeds Blu for
bounded workflows and uses it in the live extension package system. A package
can provide skills, namespaced MCP servers, workflows, or an explicitly
admitted native runtime.

```sh
borg extensions install <PATH-or-GIT-URL>
borg extensions new <id> --project
borg extensions doctor
```

See [`docs/blu-extensions.md`](docs/blu-extensions.md) and the
[extension manifest example](configs/extension.example.toml) for the package
contract. Blu workflow files may use `.blu`, `.lua`, or `.luau` entrypoints.

## Documentation

- [`docs/customization.md`](docs/customization.md) — agent interface settings,
  keybindings, alerts, trust policy, and native extensions
- [`docs/session-lifecycle.md`](docs/session-lifecycle.md) — detached hosts,
  attachment, resume, and shutdown behavior
- [`docs/usage-count.md`](docs/usage-count.md) — active-install metric and
  privacy contract
- [`docs/zh-Hans/README.md`](docs/zh-Hans/README.md) — Simplified Chinese guide
- [`docs/es/README.md`](docs/es/README.md) — Spanish guide
- [`docs/ru/README.md`](docs/ru/README.md) — Russian guide
- [`TODO.md`](TODO.md) — remaining customization surface and extension API work
- [`docs/blu-extensions.md`](docs/blu-extensions.md) — extension packages and
  workflows
- [`docs/web-search.md`](docs/web-search.md) — web-search providers
- [`docs/multiplayer-workspaces.md`](docs/multiplayer-workspaces.md) — local
  workspaces and collaboration
- [`docs/agent-runtime-protocol-v1.md`](docs/agent-runtime-protocol-v1.md) —
  runtime protocol
- [`docs/public-release-checklist.md`](docs/public-release-checklist.md) —
  release process

## Repository layout

- `borg-core` — provider-neutral message, tool, usage, and channel contracts
- `borg-provider` — model gateways and provider adapters
- `borg-remote` — durable sessions, hosts, protocols, and native tools
- `borg-search` — bounded web-search contracts and backends
- `borg-ui` — frontend-neutral commands, projections, preferences, and local
  session bridge
- `borg-tui` — Ratatui terminal frontend
- `borg-gui` — experimental GPUI native frontend (opt-in)
- `borg-cli` — public command shell and frontend launchers

## Development

```sh
cargo fmt --all -- --check
cargo check --workspace --exclude borg-gui
cargo test --workspace --exclude borg-gui

# Opt in to the experimental native GUI.
cargo run -p borg-gui
```

See [`CONTRIBUTING.md`](CONTRIBUTING.md) before submitting a change.

## Releases

Release tags matching `v*` build checksum-paired archives for Linux, macOS,
and Windows on x86-64 and ARM64. See the
[public release checklist](docs/public-release-checklist.md) before publishing
a release.

MIT licensed.
