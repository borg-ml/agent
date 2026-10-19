# Borg Agent

Borg Agent is a Rust agent harness and orchestrator. It runs coding agents on
your choice of model provider, with durable resumable sessions, a shell-first
tool surface, subagents and teams, and optional remote hosts. Releases ship the
`borg` CLI with its terminal UI; the experimental native `borg-gui` frontend is
built from source.

**Beta.** Borg Agent is pre-1.0: usable for everyday work, with rough edges.
Please report what breaks.

[简体中文](docs/zh-Hans/README.md) ·
[Español](docs/es/README.md) ·
[Русский](docs/ru/README.md) ·
[Documentation](docs/) ·
[Architecture](docs/architecture.md) ·
[Blu language](https://github.com/borg-ml/blu)

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
installs `borg` in a user-local binary directory. `borg update` updates an
existing installation.

## Quick start

```sh
borg login codex      # or: claude, opencode, openrouter, ... (see below)
borg                  # start a session in the current directory
borg resume           # pick up the latest session; `borg resume <id>` for one
borg doctor           # check storage and provider readiness
```

Inside a session, type a request; `/help` lists the controls and Ctrl-C twice
exits (the session stays resumable). For an edge-to-edge TUI in Ghostty, see the
[terminal setup](docs/terminal-setup.md#ghostty).

## Connecting providers

`borg login` with no argument lists every provider and whether it is connected.
Inside a session, `/login` switches connections and `/model` switches models.

| Provider | Connect | Notes |
| --- | --- | --- |
| ChatGPT / Codex | `borg login codex` | Device sign-in to `~/.borg/openai-subscription.json`; an existing Codex login in `~/.codex/auth.json` (or `CODEX_HOME`) is reused if found. `--auth-file PATH` uses an existing auth file in place (Borg stores the path, not a copy of its rotating tokens); `BORG_OPENAI_AUTH_FILE` overrides it. `--api-key` uses OpenAI API billing instead. |
| Claude | `borg login claude` | Browser sign-in to Borg's own login in `~/.borg/claude` (`BORG_CLAUDE_CONFIG_DIR` overrides), separate from Claude Code's, so the two never refresh one login. The new login is used only once sign-in succeeds. `--api-key` stores an Anthropic API key. |
| Anthropic API | `borg login anthropic` | API-key lane billed to your Anthropic account. |
| OpenCode | `borg login opencode --api-key` | Go subscription key; Go models appear under **OpenCode Go** in `/model`. Other OpenCode models run through the OpenCode CLI. |
| OpenRouter, Vercel AI Gateway | `borg login openrouter`, `borg login vercel` | API keys. |
| Kimi, GLM, Qwen coding plans | `borg login kimi`, `glm`, `qwen` | Selects the plan and stores its key. |
| OpenAI-compatible | `BORG_OPENAI_COMPATIBLE_BASE_URL` | Plus `BORG_OPENAI_COMPATIBLE_API_KEY` if the endpoint needs one. |
| Grok, Muse | `borg login grok`, `borg login muse` | Reached through the vendor's own CLI. |

Everything else runs on Borg's own agent loop, tools and compaction. Keys
are stored in `~/.borg/credentials.json` (mode 0600); an environment variable
for the same key takes precedence. Subscription limits never switch you to API
billing automatically, and the footer shows the active billing mode.

Credentials do not own a session: switching accounts or replacing a key keeps
its history, forks and collaborators. Provider continuation data is reused only
for the account that created it, and credentials stay fixed within a running
turn.

## Working with Borg

### Sessions

Each interactive session runs in a detached per-session host. Closing a TUI or
GUI view does not stop the active turn or its subagents, and another view can
attach to the same session. An unattended host exits after five minutes once
the session is idle with no pending prompt or background process. See
[session lifecycle](docs/session-lifecycle.md).

History lives in a durable journal on PostgreSQL, which Borg provisions for
you; `BORG_SESSIONS_URL` points it at an existing server instead. See
[session store backends](docs/session-store-backends.md).

Bring existing conversations with you using `/import` or `borg import`: Codex
CLI/Desktop, Claude Code, Claude Desktop exports and portable JSON. Imports
copy originals, skip duplicates and appear in `/resume`. See
[importing threads and memory](docs/importing.md).

### Tools

The agent works through one shell-first `exec` tool: shell pipelines, or
whichever installed language fits the problem. Borg, Blu, plugin, workflow and
collaboration capabilities are discovered from that shell with
`borg tools [--search QUERY]` and invoked with `borg call NAME JSON`, or from
code through the `borg` module in Python and Bun
(`borg.send_message(target=..., message=...)`, `borg.call(name, arguments)`).
Each such call appears in the transcript as a step of the command that made it,
and keeps the host's approval, confirmation and cancellation checks.

- `runtime_exec` keeps a persistent Python or Bun namespace between calls.
- The `harness` capability keeps evidence-backed prompt, memory and skill
  refinements that Borg adds to later turns, for one session (`local`) or every
  session in the project (`project`, kept in `~/.borg/projects/`, never in the
  repository), with rollback.
- Your own `~/.borg/AGENTS.md` applies to every session, ahead of the project's
  `AGENTS.md`/`CLAUDE.md` (including nested ones the session reaches). These,
  skills, the current goal and the plan stay in the model's context, including
  after compaction.
- Commands run in Full Access, Auto or Manual approval mode.
- Web search works with Exa, Firecrawl, Parallel and Brave backends; see
  [web search](docs/web-search.md).

Set `capabilities.harness = "native"` to expose one tool per capability
instead of the shell-first surface.

### Images

`borg image PATH` shows an image to the model. The `show_image` tool shows
images to you: an agent or subagent posts them, with an optional caption, as a
message in its transcript. They are kept with the session, so they still render
after the source file is gone. See [image delivery](docs/image-delivery.md).

### Watchers

Ask Borg to watch something, for example “Watch the build log and tell me when
it fails.” The `watch` tool runs a background command and delivers its output
in bounded batches; `list_watchers` and `stop_watcher` manage them. Watchers
last for the session (up to 24 hours or the host's command limit) and can wake
an idle agent. Human messages take priority.

### Agents, goals and plans

Agents can spawn subagents, run scouts for bounded exploration, and message
agents in other local projects or on other machines enrolled under the same
account (`list_instances`, then `send_message` or `followup_task`). A durable
goal keeps an agent working across turns until it is complete or blocked; a
plan tracks the steps. See [team harness](docs/team-harness.md) and
[multiplayer messaging](docs/multiplayer-workspaces.md).

### Recovery and limits

If the provider connection drops, Borg saves the interrupted work and retries
with delays capped at 30 seconds; the terminal shows the countdown and Escape
cancels. Usage limits wait until the provider's reported reset time (or 30
minutes when none is given); `/login` or `/model` lets you continue sooner on
another connection.

### Remote hosts

Borg Remote hosts run the same durable sessions and tools on other machines.
See the [unattended-host runbook](docs/remote-unattended-runbook.md) for
preflight, fault recovery and rollback.

## Configuration

`borg config init` writes a commented `agent.toml`; `borg config path` shows
where it lives (`~/.config/borg/`, or `$XDG_CONFIG_HOME/borg`). The agent
configuration covers providers, capabilities, MCP servers, aliases and team
settings; `editor.toml` covers interface language, TUI presentation and input.
Examples are in [`configs/`](configs/). `/ui-language` changes interface
labels, and `/language` the model's response language.

See [customization](docs/customization.md) for keybindings, alerts, extension
authority and native extensions. `borg customize inspect|export|import` moves a
complete setup between machines, and `borg capabilities --json` shows the
effective runtime capabilities. Automated checks can use
`borg agent --ephemeral --local-only` to leave resume history untouched.

On macOS, Option/Ctrl+Left/Right move by word, Cmd+Left/Right to line
boundaries and Cmd+Up/Down to the start/end of the composer; Shift extends the
selection.

## Extensions

[Blu](https://github.com/borg-ml/blu) is a Lua/Luau superset language and
runtime that Borg embeds for bounded workflows and live extension packages. A
package can provide skills, namespaced MCP servers, workflows, or an explicitly
admitted native runtime.

```sh
borg extensions install <PATH-or-GIT-URL>
borg extensions new <id> --project
borg extensions doctor
```

Packages request `sandboxed`, `trusted` or `native` access, and your policy caps
project and user packages independently. Native mode loads hash-pinned,
versioned C ABI code and is never enabled by repository configuration alone.
See [Blu extensions](docs/blu-extensions.md) and the
[manifest example](configs/extension.example.toml).

## Privacy and diagnostics

**Usage count.** To estimate active installations, release builds send at most
one content-free heartbeat a day carrying a random identifier that rotates every
31 days: no version, OS, model, session, prompt, path or device data. Disable it
with `usage_count.enabled = false` in `agent.toml` or `BORG_DISABLE_USAGE_COUNT=1`.
See [usage count](docs/usage-count.md).

**Bug reports.** `borg bug` collects a local-only diagnostic bundle: it opens no
socket, calls no model and uploads nothing.

```sh
borg bug                                            # summary only
borg bug --output bundle.json                       # full bundle
borg bug --output bundle.json --include-transcript  # add conversation text
```

The bundle is an allowlist of build identification, storage and provider
readiness, and session metadata. Credentials, environment variables, headers,
configuration contents and the working directory are never included, and
failures are recorded as fixed classes rather than raw messages. Conversation
text is included only with `--include-transcript`, written to a file readable by
you alone. Read the bundle before you share it.

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
- [Usage count](docs/usage-count.md): the active-install metric and its privacy
  contract
- [Release checklist](docs/public-release-checklist.md): the release process
- [`TODO.md`](TODO.md): remaining customization and extension API work

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
| `borg-cli` | The `borg` command and frontend launchers |

```sh
cargo fmt --all -- --check
cargo check --workspace --exclude borg-gui
cargo test --workspace --exclude borg-gui
cargo run -p borg-gui    # opt-in experimental native GUI
```

See [`CONTRIBUTING.md`](CONTRIBUTING.md) before submitting a change. Release
tags matching `v*` build checksummed archives for Linux, macOS and Windows on
x86-64 and ARM64; see the [changelog](CHANGELOG.md) and the
[release checklist](docs/public-release-checklist.md).

MIT licensed.
