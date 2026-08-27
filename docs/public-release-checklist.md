# Public release checklist

Use this checklist after a behavior or schema freeze. It is intentionally
focused on release boundaries rather than broad refactors.

## Local verification

- `cargo fmt --all -- --check`
- `cargo test --workspace --locked --no-fail-fast`
- `cargo clippy --workspace --all-targets --locked -- -D warnings`
- `cargo deny check advisories bans licenses sources`
- `cargo audit --ignore RUSTSEC-2024-0320 --ignore RUSTSEC-2025-0141`
- `git diff --check`

Run the workspace test command at least twice with its default parallelism;
serial tests are not a substitute for finding scheduling races.

## Recovery and boundaries

- Start a fresh install and resume a session after a forced process kill.
- Open an intentionally old local database and confirm it is archived as
  `*.incompatible-*`, not silently migrated or overwritten.
- Exercise Remote reconnect, duplicate delivery, late delivery, and expired
  host-token behavior.
- Verify non-loopback Remote endpoints use HTTPS and that host config files do
  not expose bearer tokens through permissions or logs.
- Run permission-mode, project-MCP trust, path-boundary, and symlink tests.
- For hosted-isolation deployments, install the Linux host through `borg
  remote install`, set `BORG_HOST_ALLOWED_NETWORKS` to reviewed Borg/provider/
  DNS addresses or CIDRs, and verify the generated unit retains
  `IPAddressDeny=any`, the expected `IPAddressAllow=` entries, and the
  `ReadWritePaths=` scope. Do not treat a manually exported
  `BORG_HOST_EXECUTION_PROFILE=isolated_hosted` as isolation.

## Package and update verification

- Build and smoke-test every supported platform archive.
- Test fresh install, upgrade, interrupted update, and next-launch recovery.
- Verify the Borg binary and bundled native provider together.
- Run `just release-test`, then `just release-check` from a clean checkout.
- Perform a small beta/canary release before marking the tag stable.

## Rollback

Keep the previous release artifacts and installer instructions available. If a
release is withdrawn, publish the next fixed version rather than retagging a
published version.
