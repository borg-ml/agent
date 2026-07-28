# Borg CLI

Borg is a durable coding agent for local and remote work. This repository is
the public source of truth for the terminal client, provider adapters, and Borg
Remote host/session runtime used by the private Borg platform.

## Licence

**AGPL-3.0-only OR a separate Borg Commercial Licence.**

The repository distributes the AGPL terms in [`LICENSE`](LICENSE). Contact
Borg to discuss a separate commercial licence; no commercial contract terms
are created by this repository. See [`NOTICE.md`](NOTICE.md) and
[`THIRD_PARTY.md`](THIRD_PARTY.md).

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
Windows on x86-64 and ARM64. The release archive contains the CLI binary only;
private platform services and tools are never packaged from this repository.
