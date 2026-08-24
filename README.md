# Borg Agent

Borg Agent is a free and open-source agent harness and orchestrator written
entirely in Rust. It combines a responsive terminal UI, durable sessions,
native tools, major AI subscriptions and APIs, and optional remote control
through borg.ml. The executable remains `borg`, and existing configuration
paths and compatibility environment variables remain supported.

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

Borg Agent checks for verified stable releases in the background and installs them
for the next launch without interrupting active work. If an unattended check or
installation fails, Borg keeps a durable notice and prompts you to run the
manual `borg update` command. Run `borg update` (`borg install` is an alias) to
update immediately, or `borg update --check` to check without installing.
Automatic updates and their check interval are configurable.

## What Borg Agent provides

- a responsive terminal UI with durable resume, steering, compaction, image
  input, transcript selection, and configurable keybindings;
- Codex and Claude subscription providers when the optional subscription
  adapters are enabled, OpenCode's JSON subprocess protocol, Kimi's managed
  OpenAI-compatible route, plus OpenRouter and explicitly configured
  OpenAI-compatible providers;
- a standalone `borg-core` contract crate with no provider SDK, HTTP, MCP, or
  subprocess dependency;
- one provider-neutral `web_search` tool with selectable Exa, Firecrawl,
  Parallel, and Brave backends, with parallel federated auto search
  (see [`docs/web-search.md`](docs/web-search.md));
- a native harness with bounded file tools, background
  processes, LSP, goals, plans, subagents, MCP, project guidance, skills, and
  optional Code Mode for programmatic tool dispatch;
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
keybindings, provider-neutral stdio MCP servers, named OpenAI-compatible
providers, and the model/effort used by native Auto approval review. Named
routes are selected with a qualified `provider/model` reference (for example
`groq/openai/gpt-oss-120b`); the provider sends only the model suffix to the
upstream endpoint, and `api_key_env` keeps credentials out of durable session
state. Optional multiplayer, subagent, autonomous-team,
shared-work, presence, cloud/web relay, and telemetry capabilities can be
disabled independently; parent capability disablement cascades to dependent
features. Run `borg capabilities` (or `borg capabilities --json`) to inspect
the effective runtime.

Native tool presentation is configured with `[capabilities].tool_mode`: `compact`
exposes the focused workspace coding tools with a short prompt for headless or
benchmark runs, `native` keeps one-call-per-tool schemas, `code` exposes a
single `run_code` tool with a generated Borg SDK, and `both` (the default)
exposes both surfaces.

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
next provider boundary; `"queue"` makes Enter start a later turn. The dedicated
queue keybinding remains available in either mode. `prevent_sleep = true` keeps
the machine awake during active turns by default; on systemd Linux it also
holds the lid-switch action while a turn is running. Use `/sleep off` or set it
to `false` when a laptop should suspend on lid close.

### Local dictation

The terminal composer has a clickable microphone control on its right edge. On
first use, Borg shows both `󰍬` and `🎤` so you can choose the one that renders
correctly; the choice is saved in `editor.toml`. Use `/icons` to change it later.
`BORG_TUI_NERD_FONT=1` or `0` can override the saved choice for a session. Use
the `dictate` keybinding (`Alt+V` by default) or `/dictate` to start recording;
use it again to stop and insert the transcription into the composer. Audio is
sent only to the local OpenAI-compatible endpoint at
`http://127.0.0.1:5092` by default. Borg does not fall back to a cloud speech
service.

Set `BORG_CLI_DICTATION_BASE_URL` and `BORG_CLI_DICTATION_MODEL` to use another
local model server. Microphone capture uses `ffmpeg`; devices that need custom
input arguments can set `BORG_CLI_DICTATION_RECORD_COMMAND` to a shell-style
argument string containing an `{output}` placeholder. Borg parses that string
into an executable and arguments and never runs it through a shell.

When the default endpoint is unavailable, the first dictation click downloads a
checksum-verified, quantized Parakeet TDT 0.6B V2 runtime and model into Borg's
user cache, starts the local server, and then begins recording. The download is
about 609 MiB and happens only once per cache. Set
`BORG_CLI_DICTATION_AUTO_SETUP=0` to keep the existing externally-managed
service behavior; explicit `BORG_CLI_DICTATION_BASE_URL`, model, or API-key
settings also bypass automatic setup. The downloaded runtime is MIT-licensed;
the converted Parakeet weights are CC-BY-4.0 licensed.

Automated terminal checks should use `borg agent --ephemeral --local-only`.
That creates a temporary session store and removes it when the process exits,
so health checks do not pollute the user's resume history or Remote workspace.

### Local resource limits

On Linux systems with systemd and cgroup v2, one command keeps local Borg
sessions from exhausting the workstation:

```sh
borg limits enable
```

Future `borg` and `borg resume` processes automatically enter their own
terminal-compatible scope; no wrapper or `sudo` is needed. Borg computes a
generous per-session memory boundary and a shared boundary across every active
session, preserving headroom for the desktop. CPU is not capped, but Borg gets
less priority when the machine is busy. Process-count and I/O weights are used
only when the OS actually delegates those controllers.

Run `borg limits` or `borg limits status` to see the effective values, active
protected sessions, and memory-pressure/OOM counters. `borg limits disable`
turns protection off for future launches without stopping existing sessions;
`borg --no-limits` or `BORG_LIMITS=0 borg` bypasses it for one launch. The
managed user unit is validated and probed before enablement, and Borg refuses
to overwrite a unit it does not own.

Important desktop processes can be protected separately when they are managed
as systemd user services:

```sh
borg limits protect add dms
borg limits protect list
borg limits protect remove dms
```

The service name may omit `.service`. Borg configures automatic recovery with
bounded retry delay and gives the service more CPU and I/O priority when those
controllers are available. Changes apply immediately without restarting the
service. Borg refuses to replace a drop-in it does not own; starting or enabling
the service remains under the user's control.

These limits cover Borg and its direct process tree, including builds and
subagents. Containers started by a rootful Docker daemon use Docker's cgroups
and need their own limits. This is a resource-exhaustion guardrail, not a
sandbox or protection from processes running as the same user, kernel, driver,
storage, or hardware failures.

### Durable data and trust boundaries

Local session data is intentionally disposable while Borg is pre-1.0. Borg
accepts only the current SQLite session schema; when an incompatible older
database is found, it moves the file aside as an `*.incompatible-*` archive and
starts a fresh store. Future schemas are rejected rather than overwritten.
There is no historical local-schema migration or downgrade guarantee yet.

**Full Access** is the default permission mode. Native processes, MCP servers,
and Blu executable workflows run with the user's operating-system authority;
Blu runtimes are trusted processes, not sandboxes. Project MCP remains disabled
unless explicitly enabled. Remote host URLs require HTTPS, except for
loopback HTTP development servers, and host configuration files are written
with private permissions.

An enrolled host persists its execution profile in its private host config.
Set `BORG_HOST_EXECUTION_PROFILE=isolated_hosted` only when the host process is
inside an independently enforced container or microVM; otherwise leave it at
the safe `trusted_user` default. Borg Web still requires its own server-owned
host allowlist before using that profile for hosted model-authored code.
At enrollment, the `BORG_HOST_MAX_*` resource-limit variables can lower (never
raise) the persisted ceilings for session duration, runtime/command timeouts,
command output, file transfer, and concurrent sessions.

Resume paints a bounded recent conversation synchronously, then fetches older
pages only when you scroll. Restored subagents remain dormant metadata until an
explicit child-directed action wakes them, and stopping the owning main thread
stops every live child before releasing the session.

## Extend Borg Agent with Blu

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
skill roots, namespaced stdio MCP servers, and bounded Blu/Lua/Luau
workflows. They may also register replay-safe API transforms, turn hooks,
model-facing tools, and commands; those declarations resolve only to the
validated workflow snapshot and are journaled by Borg. Invalid packages are isolated and reported by borg extensions
doctor instead of preventing Borg from starting.

Running local sessions watch the effective catalog, and enrolled Remote hosts
revalidate it at each turn boundary. A validated change swaps skills, MCP
definitions, API registrations, and workflow source atomically for the next turn; an in-flight
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
Persistent `runtime_exec` and `run_code` sessions can call an active package through
`borg.environment(...)`, admit RLM-style child handles with `await borg.rlm(...)`,
and maintain bounded local/global prompt, memory, skill, and subagent harness
state with `borg.harness`; those entries are injected into later turns.
`create_plugin` writes a project
`.borg/skills/<id>/SKILL.md`; the live list/read tools see it immediately, and
the native skill context rescans it at the start of the next native turn
without a restart.

The API manifest is intentionally declarative and versioned:

```toml
[api]
version = 1

[api.transforms.concise]
append_system_prompt = "Prefer concise release notes."
append_context = "Keep the release checklist in view."

[api.hooks.after_turn]
event = "turn_completed"
workflow = "review"
effect = "idempotent"

[api.tools.review]
workflow = "review"
description = "Run the durable review workflow"
input_schema = { type = "object" }
effect = "at_most_once"

[api.commands.review]
workflow = "review"
description = "Run the durable review command"
```

Tool and command invocations expose their validated JSON object to Blu as a
JSON string from `borg_workflow_arguments(call_id)`. Supported lifecycle hooks
cover turn start/completion, tool and command before/after execution, and
pre-compaction. `/ext:<extension-id>:<command>` is available in the TUI
palette and as a direct command. `append_context` is a typed replay-safe
context addition; arbitrary message-list mutation is deliberately excluded so
canonical replay and provider cache identity remain stable. Transforms and
hooks are captured with the turn snapshot; workflow calls use durable IDs and
replay rather than opaque live callbacks.

Local session history can be branched and moved without editing the SQLite
journal in place:

```sh
borg session tree
borg session undo SESSION_ID
borg session fork SESSION_ID --before SEQUENCE
borg session export SESSION_ID --output conversation.json
borg session import conversation.json
```

`borg session export` is a conversation archive, not a provider-process or
tool-side-effect checkpoint. For bounded, opt-in local file rollback use
`borg session snapshot --output workspace.json` and
`borg session restore workspace.json`; snapshots exclude `.git`, `.borg`,
symlinks, and anything beyond their file/byte caps, and restore keeps extra
files unless `--prune` is explicitly supplied.

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
borg_plugin_store(call_id, request_json)
borg_exec(call_id, command, workdir, yield_time_ms, timeout_ms, max_output_tokens)
borg_assert_exec_success(call_id, snapshot_json)
borg_workflow_arguments(call_id)
```

Every workflow and host call has durable start/request and terminal records.
`blu_workflow` autonomy jobs are supervised through the same SQLite lease and
retry state machine. Workflow code does not receive raw filesystem, database,
provider, or process handles.

Extensions may use Borg's host-owned, extension-scoped SQLite storage for
revisioned JSON state and content-addressed artifact receipts. Commits are
idempotent and provenance-bearing; artifact files remain external authorities
and can be rehashed through the same boundary after a crash or local change.

## Cross-model peer consultation

The active GPT or Claude thread remains the primary conversation. `/claude TEXT`
and `/gpt TEXT` ask that primary model to choose the useful context and consult
the matching persistent peer thread; `/ask PROFILE TEXT` remains the explicit
provider form. The peer's answer returns to the primary model for
reconciliation, so the human never has to relay messages between models.
Omitting the provider from `/ask` lets the model use the opposite provider
automatically. The peer cannot invoke another peer. Direct peer maintenance
uses `/peer claude|gpt [TEXT|clear|new MODEL@EFFORT]`; `new` archives the old
lane and starts a fresh durable child, for example
`/peer gpt new gpt-5.6-luna@max`. The same rotation is available to the agent
as `rotate_peer`, with an optional handoff. Once Borg accepts a consultation,
its peer waiter allows up to 30 minutes by default;
`BORG_PEER_CONSULTATION_TIMEOUT_SECS` can tune that between one minute and two
hours. Borg gives its local agent MCP bridge a two-hour-plus tool-call deadline,
so a provider's ordinary five-minute MCP ceiling cannot truncate a consultation
while the peer is still thinking. If the provider is disconnected for another
reason, the peer still continues and delivers a completed result privately at
the next root boundary.

## Local multiplayer workspaces

Run `borg workspaces` to list the current OS user's durable local workspaces.
Start another agent in one of them with `borg agent --workspace <uuid>`.
Selected workspaces are only accepted for new sessions: resuming a transcript
cannot silently move it to another workspace. Agents in the same workspace
share the canonical team messages, delivery state, work items, artifacts,
decisions, reviews, references, and provenance records.

## Repository layout

- `borg-core`: the minimal provider-neutral messages, tools, usage, and
  channel contracts;
- `borg-provider`: model gateways and optional Codex/Claude subscription
  adapters built on those contracts;
- `borg-search`: bounded provider-neutral web search and Exa, Firecrawl,
  Parallel, and Brave adapters;
- `borg-remote`: session protocol, store, actor, host and local tools; and
- `borg`: public commands and terminal UI.

## Development

```sh
cargo check --workspace
cargo test --workspace

# Build the provider-neutral Borg Agent core without the subscription adapters.
cargo check -p borg --no-default-features

# Include Codex/Claude subscription lanes (the release default).
cargo check -p borg --features subscription-adapters
```

See [`CONTRIBUTING.md`](CONTRIBUTING.md) before submitting changes.

## Releases

Tags matching `v*` build checksum-paired `borg` archives for Linux, macOS, and
Windows on x86-64 and ARM64. Each release archive contains the self-contained
`borg` executable, the pinned native Claude payload under `providers/claude/`,
the Blu guide and manifest example under `docs/` and `configs/`, `LICENSE`,
`NOTICE.md`, and this README. The Rust protocol runtime is provided
by the standalone MIT-licensed [`claude-agents`](https://github.com/borg-ml/claude-agents)
crate.

Run `just release` for the next patch release or `just release-minor` for the
next minor release (`0.1.44` → `0.2.0`). The release script repairs an
interrupted publish and recognizes a version bump committed alongside its code,
so rerunning the same command is safe after a transient failure.
Before tagging a public release, follow [`docs/public-release-checklist.md`](docs/public-release-checklist.md).

---

MIT.
