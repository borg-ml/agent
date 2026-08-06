# Blu extensions

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
- `${config.name}`, `${env.NAME}`, and `${extension_dir}` interpolation.

See [`configs/extension.example.toml`](../configs/extension.example.toml).

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
source for the next turn without restarting the session. The current turn is
never mutated underneath a provider. If validation fails, Borg keeps the
previous runtime and points the user to `borg extensions doctor`.

For enrolled hosts, the host-side catalog is authoritative: serialized skill
paths from a controller are discarded at the host boundary and cannot grant
access to an inactive or untrusted package.

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

## Compatibility

Legacy `.borg/extensions/*.toml` and user `extensions/*.toml` manifests are
still discovered. Moving one into `<id>/blu.toml` opts into the package layout
without changing manifest version 1. The original `borg extensions` and
`borg extensions --json` list forms remain stable.
