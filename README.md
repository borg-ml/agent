# Borg Agent

Borg Agent is a Rust-based agent harness and orchestrator. It provides a
terminal and native GPUI frontends, durable sessions, native tools, provider
adapters, and optional remote hosts. Releases include `borg` and `borg-gui`.

[Repository](https://github.com/borg-ml/agent) ·
[Blu language](https://github.com/borg-ml/blu) ·
[Documentation](docs/)

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

## What it provides

- Responsive TUI and native GPU-rendered frontends over the same durable
  session runtime, with resumable sessions, compaction, image input, transcript
  navigation, dictation, goals, plans, and subagent controls.
- Codex and Claude subscription adapters, OpenCode, Kimi, OpenRouter, and
  configured OpenAI-compatible providers.
- A provider-neutral `borg-core` crate with no provider SDK, HTTP, MCP, or
  subprocess dependency.
- Native file tools, processes, LSP, MCP, skills, goals, plans, and subagents.
- Provider-neutral web search with Exa, Firecrawl, Parallel, and Brave
  backends; see [`docs/web-search.md`](docs/web-search.md).
- Full Access, Auto, and Manual command-approval modes.
- Borg Remote hosts that use the same durable session and tool runtime.

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
settings. The editor configuration covers TUI presentation and input behavior;
the native frontend uses platform-native input and rendering.

See [`docs/customization.md`](docs/customization.md) for editor settings,
keybindings, alerts, extension authority, and native extension authoring.
Use `borg customize inspect`, `borg customize export`, and
`borg customize import` to inspect or move the complete effective setup.

`borg capabilities --json` shows the effective runtime capabilities. Automated
checks can use `borg agent --ephemeral --local-only` to avoid changing resume
history.

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

- [`docs/customization.md`](docs/customization.md) — editor settings,
  keybindings, alerts, trust policy, and native extensions
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
- `borg-gui` — GPUI native frontend
- `borg-cli` — public command shell and frontend launchers

## Development

```sh
cargo fmt --all -- --check
cargo check --workspace
cargo test --workspace
```

See [`CONTRIBUTING.md`](CONTRIBUTING.md) before submitting a change.

## Releases

Release tags matching `v*` build checksum-paired archives for Linux, macOS,
and Windows on x86-64 and ARM64. See the
[public release checklist](docs/public-release-checklist.md) before publishing
a release.

MIT licensed.
