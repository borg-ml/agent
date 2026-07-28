# Borg CLI

Borg is a free and open-source agent harness and orchestrator for reliable
local and remote work. It provides a terminal interface, provider adapters,
durable sessions, tool execution, and Borg Remote hosting without requiring
proprietary software on the user's computer.

This repository is the public source of truth for that runtime.

## Install

```sh
curl -fsSL https://borg.ml/install | sh
```

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

Licensed under AGPL-3.0-only. For a separate Borg Commercial Licence, contact
Borg.
