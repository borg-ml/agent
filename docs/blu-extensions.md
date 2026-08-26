# Blu extensions

For the user-facing editor, keybinding, notification, access-policy, and native
extension guide, start with [`customization.md`](customization.md).

Blu is Borg's live extension package system. It combines the discoverability
of an editor package manager with a deliberately small, auditable execution
surface.

## Package layout

```text
.borg/extensions/docs/
├── blu.toml
└── skills/
    └── docs/
        └── SKILL.md
```

User packages use `$XDG_CONFIG_HOME/borg/extensions/<id>/`. A package can also
contain executables or data used by an MCP server. Symlinks are rejected during
installation, and every declared skill root must remain inside the package.

`manifest_version = 1` supports:

- package id, display name, description, and semantic version;
- a semantic-version Borg requirement;
- effective capability requirements;
- semantic-version dependencies on other Blu packages;
- one or more skill roots;
- typed string, integer, float, boolean, and array settings;
- secret-setting redaction;
- namespaced stdio MCP servers with per-server tool allowlists; and
- bounded executable workflows declared with [workflows.<name>] and an
  in-package relative entrypoint; and
- versioned declarative API transforms, turn hooks, workflow-backed tools,
  and commands; and
- `${config.name}`, `${env.NAME}`, and `${extension_dir}` interpolation.
- user-capped `sandboxed`, `trusted`, and in-process `native` access modes.

See [`configs/extension.example.toml`](../configs/extension.example.toml).

## Runtime access

Packages declare `runtime_access = "sandboxed" | "trusted" | "native"`.
The user's `[extensions]` policy in `agent.toml` is authoritative: user packages
default to trusted, project packages default to sandboxed, and native loading
defaults to approval. Setting `native_access = "allow"` is an explicit,
prompt-free choice. Repository configuration cannot raise its own authority
above that policy.

Native packages also declare `[native]` with `library`, `sha256`, and
`abi_version = 2`. Borg verifies that the library remains inside the package
and pins its exact bytes before calling `borg_extension_init_v2` from the
published [`borg_extension.h`](../include/borg_extension.h). ABI v2 provides
resolved settings, logging, bounded JSON event emission, an opaque instance
handle, and optional shutdown. ABI v1 remains supported. Native code runs in
Borg's process and therefore has the user's full authority.

`borg_extension.h` is not part of the Blu/Lua/Luau workflow API. It is a
language-neutral C ABI description for compiled native libraries, including
libraries authored in Rust, C++, Zig, or other languages that can export C ABI
symbols. Ordinary Blu packages use the declarative `[api.*]` surface below.

## Lifecycle

```text
discover → parse → validate → resolve dependencies → activate → snapshot
                                                        ↓
                                  next turn ← atomic live swap
```

Borg hashes manifests, package contents, declared skill roots, state, effective
capabilities, trust, and explicit reload signals. Running local TUI sessions check that revision twice per
second; enrolled Remote hosts revalidate immediately before every turn. A valid
change updates skill roots, MCP server definitions, and executable workflow
source for the next turn without restarting the session. Editor, theme,
keybinding, and alias contributions apply immediately after the validated
catalog swap. The current turn is
never mutated underneath a provider. If validation fails, Borg keeps the
previous runtime and points the user to `borg extensions doctor`.

For enrolled hosts, the host-side catalog is authoritative: serialized skill
paths from a controller are discarded at the host boundary and cannot grant
access to an inactive or untrusted package.

## Extension API v1

An extension can expose declarative registrations alongside its workflows:

```toml
[api]
version = 1

[api.editor.layout]
horizontal_margin = 4
composer_max_height = 12
show_footer = true

[api.editor.transcript]
assistant_label_color = "#c084fc"

[api.keybindings]
send = ["ctrl+enter"]

[api.aliases]
ship = "/fast on"

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

`api.editor` is a partial, typed `editor.toml` tree. It covers layout,
rendering/presentation, theme/transcript, interaction, and future public editor
preferences without requiring a manifest-version change. Unknown or invalid
fields isolate the package. Keybindings replace the named action's chord list;
aliases target slash commands. Active packages merge in dependency-first load
order, so the later package is the visible override.

The supported hook events are `turn_started`, `turn_completed`,
`tool_execute_before`, `tool_execute_after`, `command_execute_before`,
`command_execute_after`, and `before_compaction`. Turn and lifecycle payloads
are bounded JSON objects; the workflow reads them through
`borg_workflow_arguments(call_id)`. `append_context` is a typed, replay-safe
context addition, while arbitrary message-list mutation is intentionally not
part of v1 because it would change canonical replay and provider cache
identity. Tool and command registrations are exposed through the same
provider-neutral dispatcher and execute only workflows in the immutable turn
snapshot. Workflow start, host-call, and terminal records remain in the
session journal, so a retry replays a completed effect instead of invoking an
opaque live callback.

Extension commands are available from the TUI command palette and can also be
typed directly as `/ext:<extension-id>:<command>`. Add a JSON object after the
command for structured arguments, or plain text for `{ "arguments": "..." }`.

## Commands

```sh
borg extensions
borg extensions info docs
borg extensions doctor
borg extensions new docs --project
borg extensions install ./docs-extension --project
borg extensions install https://github.com/example/docs-blu.git
borg extensions enable docs --project
borg extensions disable docs --project
borg extensions config docs index '"stable"' --project
borg extensions config docs index --unset --project
borg extensions update docs
borg extensions remove docs --project
borg extensions reload
```

Every inspection command supports `--json`. Existing `borg extensions --json`
automation remains compatible.

Inside a running Borg TUI, `/extensions` shows the exact last-known-good live
snapshot, activation reasons, scope, and revision. Filesystem changes are
watched automatically; `borg extensions reload` is available when tooling or
an editor needs an explicit reload signal.

## Selectable workflow runtimes

Blu remains the embedded runtime for the whole Lua family. A workflow may use
`.blu`, `.lua`, or `.luau`; `.luau` selects Luau semantics inside the same Blu
engine. Other workflows can select a supervised user runtime without changing
the package lifecycle:

```toml
[workflows.analysis]
runtime = "ipython" # blu | python | ipython | javascript | typescript
entrypoint = "workflows/analysis.py"
description = "Use the project's Python environment for analysis"
command = "ipython" # optional executable override
args = ["--no-banner"] # optional arguments before the entrypoint
```

The default executable profiles are `python3` for Python, `ipython` for IPython,
and `bun` for both JavaScript and TypeScript. Bun is intentional: both source
types use one consistent Node-compatible, npm-aware worker, while
`command = "node"` remains an explicit JavaScript override when a project needs
Node. Set `command` when a project uses
a virtual environment, Deno, a package-manager shim, or another runtime
installation. Runtime processes are supervised, output is bounded and durable,
and the process is killed on workflow cancellation. A worker process is a
lifecycle boundary, **not a sandbox**: Python and JavaScript code runs with the
permissions of the selected user process and must be treated as trusted.

Use the provider-neutral `list_workflows` and `run_workflow` tools for all
runtimes. `list_blu_workflows` and `run_blu_extension` remain compatibility
aliases for existing Blu packages. The model can create a package on the fly
with `create_extension`; the atomic package swap is visible at the next native
turn boundary, just like a Blu edit.

Installs are staged and validated before an atomic directory swap. Git installs
record their source and exact revision in the scope's `blu.toml`; update clones
a fresh copy and uses the same transaction. Local packages intentionally do not
auto-update because Borg cannot infer ownership or a trustworthy upstream.

## State and precedence

Package files stay immutable. Enable overrides, settings, and install sources
live in `.borg/blu.toml` or `$XDG_CONFIG_HOME/borg/blu.toml`. Borg writes these
files atomically and uses mode `0600` on Unix. Secret values are redacted from
catalog output, though environment interpolation is preferred when a secret
already belongs to a process supervisor or secret manager.

Project packages take precedence over user packages with the same id. The
shadowed package is reported as a diagnostic. Dependencies are loaded before
dependents; missing, inactive, incompatible, and cyclic dependency graphs are
isolated rather than partially activated.

## Trust and safety

User packages are eligible to run when enabled. Project packages that declare
MCP commands are untrusted by default; opt in with:

```toml
[extensions]
allow_project_mcp = true
```

Blu extension packages never run package install hooks, update hooks, or
activation scripts. Extension MCP commands are direct argv-based stdio
processes, not shell snippets. Use `allowed_tools` to expose the smallest tool
surface; the allowlist is enforced by Borg's native runtime and translated to
each external provider's MCP policy format.

The native harness advertises each active package workflow through the generic
`run_workflow` tool. Blu workflows additionally retain `run_blu_extension` and
the explicit `run_blu_workflow` request for compatibility. Embedded Blu host
calls are permission-checked and journaled in the canonical SQLite session
store. External runtimes are supervised as trusted processes and their
workflow lifecycle/output is journaled; their normal Python/JavaScript library
calls are intentionally not rewritten into fake Blu handles. Workflow source
is bounded to 256 KiB, must remain inside its package, and is frozen for the
turn that loaded it.

## Extension-scoped durable storage

Extensions can use the host-owned `plugin_store` boundary for correctness-critical
state without receiving a SQLite handle. Persistent Python and Bun runtimes use
`borg.storage("extension-id")`; Blu workflows use `borg_plugin_store(call_id,
request_json)`. The store supports session or project scope, bounded JSON values,
compare-and-swap revisions, content-addressed artifact receipts, provenance, and
idempotent commits.

A commit validates and hashes every declared artifact before one SQLite
`BEGIN IMMEDIATE` transaction applies the state writes, artifact receipts, and
mutation receipt. Reusing the same idempotency key with the same request replays
the stored result; reusing it with different content is rejected. `verify_artifact`
rehashes the current workspace file so a modified external result is visible as
invalid. External files remain the plugin or benchmark's authority and cannot be
rolled back by SQLite; Borg makes their receipt, hash, provenance, and recovery
state durable.

Workflows that need to preserve a failed process attempt should commit its
append-only evidence first, then call `borg_assert_exec_success(call_id,
snapshot_json)`. The assertion is journaled and makes the workflow fail after the
receipt exists, while retries replay both boundaries without repeating the
external effect.

## Persistent programming runtime

`runtime_exec` is a separate model-facing primitive for iterative work. It is
not a workflow and it is not the same as the per-invocation external process
used by `run_workflow`. The first adapter is a session-scoped plain CPython
worker started lazily with `python3 -u` (or `BORG_PYTHON_RUNTIME`), so variables,
imports, helper functions, and parsed data survive multiple calls and native
turns. The worker is owned by the provider-neutral agent-tool dispatcher, which
also serves the local MCP bridge; native, Codex, and Claude paths therefore use
the same session namespace rather than provider-specific copies.

The initial worker intentionally has no `ipykernel` dependency. It supports a
normal persistent Python namespace, final-expression values, top-level await,
captured stdout/stderr, and a small `borg` bridge (`read`, `search`, `exec`,
`history`, `runtime_status`, `checkpoint`, `restore`, `write`, and selected
Borg tool calls). `search` searches workspace files; `history` queries the
session's canonical event journal. It is not yet an IPython kernel: IPython
magics, rich display protocols, kernel extensions, and notebook-style comms
are future adapter work. When `bun` is installed, `runtime = "javascript"`
or `runtime = "typescript"` selects a persistent Bun VM with the same host
bridge; use an explicit `return` for an asynchronous JavaScript/TypeScript
result. The selected environment still determines which packages are
importable; Blu hot reload does not install Python or Bun dependencies.

The bridge also has the three pieces needed for a stateful environment loop:

- `borg.environment("extension-id", "server")` discovers the extension's
  namespaced MCP tools inside the same persistent process. Extension MCP
  definitions are replaced at the next turn boundary; an already-running
  environment is restarted only when its grant actually changes.
- `await borg.rlm("subtask")` admits a child and returns a handle immediately;
  the handle can refresh status, send a follow-up, interrupt, or wait. Child
  results remain explicit agent events rather than being silently injected into
  the parent namespace.
- `borg.harness` provides bounded CRUD for prompt, memory, skill, and subagent
  entries in local session or project-global scope, plus refinement evidence,
  rollback of recent local revisions, and a refinement plan. Persisted entries
  are included in the next turn's prompt, so a successful debugging loop can
  become reusable harness state rather than a note in a lost context window.

The lifecycle is:

```text
first runtime_exec in a session
        ↓ lazy worker start
execute ↔ Borg host-call bridge ↔ filesystem/process/Borg tools
        ↓ next runtime_exec / next native turn
same in-memory namespace
        ↓ error, timeout, cancellation, or session teardown
worker is killed; the durable runtime manifest records the worker boundary
and the last execution hash
        ↓ next Borg process claims the same session
new worker starts; the latest checkpoint is automatically applied to ordinary
public identifiers, and `borg.runtime_status()` reports recovery/checkpoints
        ↓ optional explicit restore
`state = borg.restore("checkpoint-key")["state"]` reads the full JSON data
```

The worker is trusted user-authority execution, not a sandbox. In the
canonical `isolated_hosted` service, model-authored shell commands, external
workflow processes, and native MCP children receive the same fixed non-secret
environment; explicitly declared MCP environment entries are the only
additional variables. Runtime calls
are permission-gated, host mutations use Borg's filesystem/process boundary,
command output is bounded, and the worker starts with a fixed non-secret
environment rather than inheriting provider, deployment, or control-plane
variables. This environment hygiene is a credential-reduction measure, not a
replacement for a container or microVM when hostile multi-tenant code is in
scope. Borg does not replay arbitrary code after a
restart: the session store owns a versioned runtime manifest and named,
content-addressed JSON checkpoints. A skill that wants recovery should call
`borg.checkpoint("name", {"parameters": ..., "cursor": ...})` at a safe
boundary. After a worker restart, only public identifier keys are hydrated
into the namespace; `borg.restore` remains available for the full checkpoint,
and arbitrary Python objects/executable code are never replayed.
The manifest also records worker ownership, execution count, and the last code
hash, making restart and provenance visible without pretending that a Python
heap can be serialized losslessly. Blu remains the small embedded
extension/workflow backend; Python/IPython and Bun remain selectable
supervised workers for cases that need their ecosystems.

## Lossless history retrieval

The durable SQLite journal is the authority for normalized model-visible
inputs, messages, tool calls/results, workflow and child-agent events,
approvals, goals, plans, and external outcomes. Large tool inputs and outputs
remain lossless payload blobs referenced by compact event rows. Streaming UI
deltas and redundant provider wire telemetry may still be coalesced or omitted;
the guarantee is a replayable semantic execution record, not a packet capture.

`query_history` exposes one provider-neutral read contract to native models,
Codex/Claude/OpenCode MCP lanes, Blu workflows, and the persistent Python bridge:

- empty `text`: exact event-id, typed actor/kind, and inclusive sequence-range
  reads over canonical rows;
- `mode=lexical`: tenant-scoped SQLite FTS5 discovery;
- `mode=regex`: a bounded Rust regex scan, optionally narrowed first with a
  literal `prefilter` through FTS;
- `expand_payloads=true`: bounded expansion of deferred payloads under one
  aggregate response byte budget.

The FTS table is a rebuildable projection. It is updated in the same SQLite
transaction as each durable append and backfilled from event rows and payload
blobs when an existing store opens. Every result is rehydrated from
`session_events`; search snippets and scores are discovery aids, never the
source of truth. Forks retain projected child event ids/sequences. Their
inherited prefix currently uses a bounded lineage scan for exact semantics,
while root-session queries use indexed SQL directly.

External semantic search follows the same rule. The store exports a
sequence-cursored `SessionHistoryIndexDocument` feed with stable
`borg-session-event:v1:<session-id>:<event-id>` locators and full searchable
content. BorgSearch/Vespa may index that feed using the workspace as
`owner_id`, but a semantic hit must be resolved back through `query_history`
by event id before the model treats it as evidence. Vespa can therefore be
dropped and rebuilt without losing memory, and it cannot silently replace or
mutate the journal.

Persistent Python/Bun runtimes expose the same feed as
`borg.history_index(after_sequence, limit)`, and Blu exposes it as
`borg_history_index(call_id, query_json)`. An agent can page the complete log
into its durable namespace, build a task-specific lexical/vector/graph
retriever, or submit the records to BorgSearch through an approved adapter;
the returned ids and cursors remain locators, and canonical event resolution
still happens through `borg.history(...)` or `query_history`.

When Web or a host launch supplies a scoped external MCP grant, the persistent
runtime also exposes `borg.mcp_tools()` and `borg.mcp(name, arguments)`. It
provides `borg.semantic_search(query, ...)` as a small convenience for the
scoped `mcp__borg__search_documents` BorgSearch service; this is still
candidate retrieval, not a history or source authority. That one exact search
call is available to a read-only runtime; arbitrary `borg.mcp(...)` calls still
require Full Access because the host cannot infer whether an external MCP tool
mutates state. These calls use the same namespaced MCP tools and credentials as
the provider turn and start clients lazily. This lets an agent build
a matter- or task-specific BorgSearch retriever in code while keeping the MCP
server, workspace scope, and domain records authoritative outside the runtime.
Blu workflows have the equivalent `borg_mcp_tools(call_id)` and
`borg_mcp(call_id, name, arguments_json)` host functions; their calls are
journaled with the workflow id and replayed idempotently.

## Versioned retrieval adapters

Native turns can persist a task-specific retriever under
`.borg/retrievers/<id>`. `create_retrieval_adapter` stores a bounded Python or
JavaScript source file with a `retrieve(query)` entrypoint and optional
`test(retrieve, borg)` source. `list_retrieval_adapters` and
`read_retrieval_adapter` expose the manifest, source, tests, and immutable
revision history; `rollback_retrieval_adapter` atomically restores an earlier
revision while archiving the current one.

The persistent runtime helpers `borg.retrieval_adapter(id, query)` and
`borg.test_retrieval_adapter(id)` load the matching language adapter through
the host boundary. Adapter code can page `history_index`, call a scoped MCP
retrieval service, or combine local ranking logic, but its output is only a
candidate set. A caller must resolve returned event locators with
`borg.history(..., event_id=...)` / `query_history` before using them as
evidence. Adapter revisions therefore improve retrieval ergonomics without
creating a second memory or semantic-state authority.

The ignored `large_session_history_query_p95_gate` regression fixture builds a
25,000-event journal. On the development host on 2026-08-07, an unoptimized
test build measured about 0.7 ms p95 for a rare FTS hit and 39 ms p95 for a
regex narrowed by FTS. These are local regression measurements, not hosted
service latency claims.

## Compatibility

Legacy `.borg/extensions/*.toml` and user `extensions/*.toml` manifests are
still discovered. Moving one into `<id>/blu.toml` opts into the package layout
without changing manifest version 1. The original `borg extensions` and
`borg extensions --json` list forms remain stable.

Self-authored project skills and Blu extension packages carry a human version
and a content revision. Replacing one atomically archives the previous package
under `.versions/` (bounded to 32 revisions); `rollback_plugin` and
`rollback_blu_extension` restore a selected revision while archiving the
current one. A workflow execution remains the test/evidence boundary: its
source revision and idempotent result are recorded before the next turn can
use the package.
