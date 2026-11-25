# Borg Agent

Borg Agent is a Rust agent harness and orchestrator. It runs coding agents on
your choice of model provider, with durable resumable sessions, a shell-first
tool surface, subagents and teams, and optional remote hosts. Releases ship
Borg with its terminal UI; the experimental native GUI is built from source.

**Beta.** Borg Agent is pre-1.0: usable for everyday work, with rough edges.
Please report what breaks.

**No telemetry.** Borg collects nothing about you or your work. See
[Privacy](#privacy).

[简体中文](docs/zh-Hans/README.md) ·
[Español](docs/es/README.md) ·
[Русский](docs/ru/README.md) ·
[Documentation](docs/) ·
[Architecture](docs/architecture.md) ·
[Blu language](https://github.com/blulang/blu)

## Install

Linux and macOS:

```sh
curl -fsSL https://borg.ml/install | sh
```

Windows PowerShell:

```powershell
irm https://borg.ml/install.ps1 | iex
```

The installer picks your platform and CPU, verifies the release checksum, and
installs Borg in a user-local binary directory. Borg keeps itself up to date.

## Getting started

Run `borg` in a project directory and type a request. Everything else happens
inside the session: `/help` lists the controls, **Settings** holds the
configuration, and Ctrl-C twice exits (the session stays resumable from
`/resume`). For an edge-to-edge TUI in Ghostty, see the
[terminal setup](docs/terminal-setup.md#ghostty).

## Providers

**Settings › Providers** (or `/login`) shows every provider, which logins it
has and what each one has left, and lets you add, select or remove one.
`/model` switches models.

Supported: ChatGPT/Codex and Claude subscriptions, the Anthropic API,
OpenCode Go, OpenRouter, Vercel AI Gateway, the Kimi, GLM and Qwen coding
plans, any OpenAI-compatible endpoint, and Grok and Muse through their vendors'
own CLIs. Everything else runs on Borg's own agent loop, tools and compaction.

Borg's Claude login is its own, separate from Claude Code's, so the two never
refresh one login. An existing Codex login is reused when found. Keys are
stored in `~/.borg/credentials.json` (mode 0600). Subscription limits never
switch you to API billing automatically, and the footer shows the active
billing mode.

Several Claude or ChatGPT subscriptions can stay connected at once: *Add
subscription* on a provider's page connects another one. By default a turn
moves to the login of the same provider with the most quota left, so an
exhausted subscription no longer stops the work; the provider's page shows each
login's remaining windows and lets you pick Manual, Most headroom or Soonest
reset. Switching only ever picks another login of the same provider and never
moves a turn to API-key billing. Key-billed plans (OpenCode Go, GLM Coding,
Kimi Code, Qwen Coding) keep several named keys and spread turns over them.

Credentials do not own a session: switching accounts or replacing a key keeps
its history, forks and collaborators.

## Working with Borg

### Sessions

Each session runs in a detached host. Closing a TUI or GUI view does not stop
the active turn or its subagents, and another view can attach to the same
session. See [session lifecycle](docs/session-lifecycle.md).

History lives in a durable journal on PostgreSQL, which Borg provisions for
you. See [session store backends](docs/session-store-backends.md).

`/import` brings existing conversations with you: Codex CLI/Desktop, Claude
Code, Claude Desktop exports and portable JSON. Imports copy originals, skip
duplicates and appear in `/resume`. See
[importing threads and memory](docs/importing.md).

### Tools

The agent works through one shell-first tool: shell pipelines, or whichever
installed language fits the problem, with a persistent Python or Bun runtime
between calls. Borg, extension, workflow and collaboration capabilities are
reached from that same surface and keep the host's approval, confirmation and
cancellation checks.

- Commands run in Full Access, Auto or Manual approval mode.
- Your own `~/.borg/AGENTS.md` applies to every session, ahead of the project's
  `AGENTS.md`/`CLAUDE.md`. These, skills, the current goal and the plan stay in
  the model's context, including after compaction.
- The agent can keep evidence-backed prompt, memory and skill refinements for
  one session or the whole project (kept in `~/.borg/projects/`, never in the
  repository), with rollback.
- Agents can show you images in the transcript; they are kept with the session.
  See [image delivery](docs/image-delivery.md).
- Web search works with Exa, Firecrawl, Parallel and Brave; see
  [web search](docs/web-search.md).

### Watchers

Ask Borg to watch something, for example “Watch the build log and tell me when
it fails.” Watchers run in the background, report in bounded batches, last for
the session and can wake an idle agent. Human messages take priority.

### Agents, goals and plans

Agents can spawn subagents, run scouts for bounded exploration, and message
agents in other local projects or, once Borg Remote is enabled, on your other
machines. A durable goal keeps an agent working across turns until it is
complete or blocked; a plan tracks the steps. See
[team harness](docs/team-harness.md) and
[multiplayer messaging](docs/multiplayer-workspaces.md).

### Recovery and limits

If the provider connection drops, Borg saves the interrupted work and retries
with delays capped at 30 seconds; the terminal shows the countdown and Escape
cancels. Usage limits wait until the provider's reported reset time; `/login`
or `/model` lets you continue sooner on another connection.

### Remote hosts

Borg Remote runs the same durable sessions and tools on other machines. It is
off until you enable it with `/remote` and approve the machine with the code it
shows. Enrolling does not mirror your other sessions or open a listener, and
pinned devices are the only ones whose owner commands a host runs. See the
[unattended-host runbook](docs/remote-unattended-runbook.md).

## Configuration and extensions

**Settings** and `/customize` cover providers, capabilities, MCP servers,
aliases, team settings, interface language, keybindings and alerts. The
settings are plain TOML under `~/.config/borg/`; commented examples are in
[`configs/`](configs/). See [customization](docs/customization.md).

[Blu](https://github.com/blulang/blu) is a Lua/Luau superset language and
runtime that Borg embeds for bounded workflows and live extension packages
(`/extensions`). A package can provide skills, namespaced MCP servers,
workflows, or an explicitly admitted native runtime. Packages request
`sandboxed`, `trusted` or `native` access, and your policy caps project and
user packages independently; native code is hash-pinned and never enabled by
repository configuration alone. See [Blu extensions](docs/blu-extensions.md).

## Privacy

Borg has no telemetry: no analytics, usage counts, heartbeats, crash
reporting, or install tracking. It never sends your prompts, code, paths or
session data anywhere you did not choose.

Borg talks to the network only for:

- the model providers and web-search backends you connect;
- Borg Remote, if you enroll a machine;
- checking GitHub releases for updates (turn off with
  `updates.auto_install = false`);
- public model metadata (the models.dev catalog and the current Codex client
  version).

Bug-report diagnostics are built locally and never uploaded. They exclude
credentials, environment variables and configuration contents, include
conversation text only when you ask, and are yours to read before sharing.

## Documentation

- [Model context](docs/model-context.md): inspecting and editing the model's
  context, safety boundaries and recovery
- [Customization](docs/customization.md): interface settings, keybindings,
  alerts, trust policy and native extensions
- [Session lifecycle](docs/session-lifecycle.md): detached hosts, attachment,
  resume and shutdown
- [Blu extensions](docs/blu-extensions.md): extension packages and workflows
- [Web search](docs/web-search.md): search providers
- [Multiplayer messaging](docs/multiplayer-workspaces.md): local workspaces and
  collaboration
- [Agent runtime protocol](docs/agent-runtime-protocol-v1.md): the runtime
  protocol
- [Release checklist](docs/public-release-checklist.md): the release process

## Development

| Crate | Purpose |
| --- | --- |
| `borg-core` | Provider-neutral model, tool, usage and channel contracts |
| `borg-provider` | Model gateways, provider adapters, authentication and MCP |
| `borg-agent-runtime` | Agent loop, tool dispatch, code-mode runtimes, session store and orchestration |
| `borg-remote` | Durable session protocol, hosts and remote tools |
| `borg-search` | Bounded web-search contracts and backends |
| `borg-lanes` | Host-local job queues, services and resource leases |
| `borg-view` | Renderer-independent session view contracts |
| `borg-ui` | Frontend-neutral commands, projections and preferences |
| `borg-tui` | Ratatui terminal frontend |
| `borg-gui` | Experimental GPUI native frontend (opt-in) |
| `borg-dictation` | Local speech-to-text backend |
| `borg-display` | Private headless Wayland display for computer use |
| `borg-cli` | The `borg` binary and frontend launchers |

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for building, testing and submitting a
change, and the [changelog](CHANGELOG.md) for releases.

MIT licensed.
