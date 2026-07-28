# Borg CLI

Borg is the free and open-source agent harness and orchestrator for reliable
local and remote work. It is not limited to coding: use it for research,
operations, document work, analysis, or any task an agent can complete with
local tools. No proprietary Borg runtime runs on your computer.

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

Borg checks for verified stable releases in the background and installs them
for the next launch without interrupting active work. Run `borg update`
(`borg install` is an alias) to update immediately, or `borg update --check`
to check without installing. Automatic updates and their check interval are
configurable.

## What Borg provides

- a responsive terminal UI with durable resume, steering, compaction, image
  input, transcript selection, and configurable keybindings;
- Codex, Claude, OpenCode, Kimi, OpenRouter, and OpenAI-compatible providers;
- a provider-neutral native harness with bounded file tools, background
  processes, LSP, goals, plans, subagents, MCP, project guidance, and skills;
- non-blocking, checksum-verified updates that take effect on the next launch;
- **Full Access** (default), **Auto** model-reviewed commands, and **Manual**
  user-reviewed commands;
- enrolled Borg Remote hosts using the same public session and tool runtime.

The implementation deliberately keeps state, safety boundaries, validation,
and tool execution in typed Rust. It does not include an extension runtime or
product-specific workflow semantics.

## Configuration

Copy [`configs/agent.example.toml`](configs/agent.example.toml) to
`$XDG_CONFIG_HOME/borg/agent.toml` (normally
`~/.config/borg/agent.toml`). The typed config supports slash-command aliases,
keybindings, and the model/effort used by native Auto approval review.

## Boundary

Included:

- `borg-provider`: provider-neutral messages, tools, usage and local adapters;
- `borg-remote`: session protocol, journal, actor, host and local tools; and
- `borg`: public CLI commands and terminal UI.

Excluded: Borg's proprietary server, engine, legal workflows, billing, web
apps, platform MCP/catalog, database/search implementations, internal tooling,
deployment configuration, and generated product assets.

The dependency direction is one-way: the private platform imports versioned
packages from this repository. Public packages never import the private
monorepo.

## Development

```sh
cargo check --workspace
cargo test --workspace
```

See [`CONTRIBUTING.md`](CONTRIBUTING.md) before submitting changes.

## Releases

Tags matching `v*` build checksum-paired `borg` archives for Linux, macOS, and
Windows on x86-64 and ARM64. Each release archive contains the self-contained
`borg` CLI, `LICENSE`, `NOTICE.md`, and this README. Private platform services
and tools are never packaged from this repository.

---

AGPL-3.0-only OR a separate Borg Commercial Licence. Contact Borg.
