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
- Codex and Claude subscription providers, plus OpenRouter and explicitly
  configured OpenAI-compatible providers;
- a native harness with bounded file tools, background
  processes, LSP, goals, plans, subagents, MCP, project guidance, and skills;
- Blu live extensions with dependency-aware skill, MCP, and bounded executable
  workflow packages, typed settings, atomic install/update, and turn-boundary hot reload, plus
  configurable slash aliases and keybindings;
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

Manual and autonomous teams allow 16 concurrently live child agents by
default. Set `[team].worker_concurrency` to a smaller positive integer to fit
local resources or provider budgets; the setting does not require an
autonomous-team preset. Autonomous lifetime assignment, report, escalation,
and specialist limits are separate optional `[team]` settings.

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

Resume paints a bounded recent conversation synchronously, then fetches older
pages only when you scroll. Restored subagents remain dormant metadata until an
explicit child-directed action wakes them, and stopping the owning main thread
stops every live child before releasing the session.

## Extend Borg CLI with Blu

Blu is Borg's inspectable, live extension system:

- put `SKILL.md` packages in `.agents/skills`, `.borg/skills`,
  `~/.agents/skills`, `~/.borg/skills`, or `~/.codex/skills`;
- install a user package with `borg extensions install <PATH-or-GIT-URL>`, or
  add `--project` for a checkout-local package;
- scaffold a package with `borg extensions new <id> --project`;
- enable, disable, configure, update, remove, validate, and inspect packages
  with the corresponding `borg extensions` subcommands;
- add standalone stdio servers under `[mcp.servers.<name>]` in `agent.toml` to
  expose arbitrary tools across Codex, Claude, and Borg's native
  providers;
- define slash-command aliases and remap every primary TUI action in the same
  typed config; and
- keep project-specific agent instructions beside the code they govern.

Blu packages live in project `.borg/extensions/<id>/` or user
`$XDG_CONFIG_HOME/borg/extensions/<id>/` (normally
`~/.config/borg/extensions/<id>/`); legacy flat manifests remain compatible.
Project packages that declare MCP servers remain cataloged but inactive unless
the user explicitly sets [extensions].allow_project_mcp = true; skill-only and
workflow-only project packages do not need MCP trust. Packages may declare
semantic-version dependencies, Borg/capability requirements, typed settings,
skill roots, namespaced stdio MCP servers, and bounded .blu workflows. Invalid
packages are isolated and reported by borg extensions doctor instead of
preventing Borg from starting.

Running local sessions watch the effective catalog, and enrolled Remote hosts
revalidate it at each turn boundary. A validated change swaps skills, MCP
definitions, and workflow source atomically for the next turn; an in-flight
turn keeps the immutable snapshot it started with. Invalid changes retain the
last-known-good runtime and show a TUI notice or host warning. Blu does not
execute install or lifecycle hooks. See [Blu extensions](docs/blu-extensions.md) and the
[manifest example](configs/extension.example.toml) for the full contract.

Agents can maintain this setup through the built-in get_agent_settings,
update_agent_settings, list_plugins, read_plugin, create_plugin,
list_blu_extensions, read_blu_extension, create_blu_extension,
set_blu_extension_enabled, remove_blu_extension, and reload_blu_extensions
tools. These package operations write atomically and append a small
scope-local JSONL audit record.
Settings writes are atomic; slash aliases and keybindings reload in a running
TUI, while Blu skills, MCP servers, and executable workflows reload at the next
turn boundary.
`create_plugin` writes a project
`.borg/skills/<id>/SKILL.md`; the live list/read tools see it immediately, and
the native skill context rescans it at the start of the next native turn
without a restart.

### Durable Blu workflows

The native harness exposes `run_blu_workflow` for ad-hoc bounded workflows and
advertises installed package workflows through `run_blu_extension`. A package
declares an entrypoint such as:

```toml
[workflows.review]
entrypoint = "workflows/review.blu"
description = "Review the current change"
```

The workflow source is loaded from the immutable turn snapshot and identified
by an explicit `workflow_id`. It executes inside Borg's embedded Blu runtime.
Blu supplies control flow; Borg remains authoritative for permissions, tools,
processes, autonomy jobs, checkpoints, and SQLite journaling. Mutating host
calls still require the session's Full Access policy or the normal workflow
approval path.

Guest host calls use stable call ids so a completed effect can be replayed
without running it twice:

```text
borg_emit(call_id, kind, payload_json)
borg_tool(call_id, name, arguments_json)
borg_enqueue(call_id, idempotency_key, kind, payload_json, delay_ms, max_attempts)
borg_job(call_id, job_uuid)
borg_checkpoint(call_id, job_uuid, checkpoint_key, kind, state_json, evidence_json)
borg_exec(call_id, command, workdir, yield_time_ms, timeout_ms, max_output_tokens)
```

Every workflow and host call has durable start/request and terminal records.
`blu_workflow` autonomy jobs are supervised through the same SQLite lease and
retry state machine. Workflow code does not receive raw filesystem, database,
provider, or process handles.

## Cross-model peer consultation

The active GPT or Claude thread remains the primary conversation. `/claude TEXT`
and `/gpt TEXT` ask that primary model to choose the useful context and consult
the matching persistent peer thread; `/ask PROFILE TEXT` remains the explicit
provider form. The peer's answer returns to the primary model for
reconciliation, so the human never has to relay messages between models.
Omitting the provider from `/ask` lets the model use the opposite provider
automatically. The peer cannot invoke another peer. Direct peer maintenance
uses the deliberately explicit `/peer claude|gpt [TEXT|clear]` command, and
shares the same durable `/root/claude` and `/root/gpt` lanes as consultation.

## Local multiplayer workspaces

Run `borg workspaces` to list the current OS user's durable local workspaces.
Start another agent in one of them with `borg agent --workspace <uuid>`.
Selected workspaces are only accepted for new sessions: resuming a transcript
cannot silently move it to another workspace. Agents in the same workspace
share the canonical team messages, delivery state, work items, artifacts,
decisions, reviews, references, and provenance records.

## Repository layout

- `borg-provider`: provider-neutral messages, tools, usage and local adapters;
- `borg-remote`: session protocol, store, actor, host and local tools; and
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
`borg` CLI, the pinned native Claude payload under `providers/claude/`,
the Blu guide and manifest example under `docs/` and `configs/`, `LICENSE`,
`NOTICE.md`, and this README. The Rust protocol runtime is provided
by the standalone MIT-licensed [`claude-agents`](https://github.com/borg-ml/claude-agents)
crate.

---

MIT.
