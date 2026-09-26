# Public release checklist

Use this checklist after a behavior or schema freeze. It is intentionally
focused on release boundaries rather than broad refactors.

## Local verification

- Point `BORG_TEST_SESSIONS_URL` and `BORG_SESSIONS_URL` at the same disposable
  PostgreSQL test server, then run `just verify`. The platform CI provisions
  that server automatically and runs the same gate on `main`.
- Run `just release-test`, then `just release-check` from a clean checkout.
- Confirm the six native platform jobs and workspace quality job pass for the
  exact commit to be tagged. Re-run a failed job and investigate any repeated
  failure before publishing.

For changes to scheduling or delivery, additionally run the affected tests
with default parallelism to catch races hidden by the serial workspace gate.

## Public-facing material

- Review the user-facing `CHANGELOG.md` Unreleased section and the release
  notes rendered from it. Include supported platforms, notable fixes and
  known limitations.
- Provide a private security-reporting path and a `SECURITY.md` that names it.
- Confirm the contributor licence signing path described in
  `CONTRIBUTING.md` is available before inviting external contributions.
- Review redistribution rights and notices for the bundled native Claude
  payload before publishing its archives.

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
- Install the candidate archive on a canary host and exercise one real provider
  session before tagging the release.

## Publication

- Create the version tag only after the release candidate and notes are
  approved. The tag workflow attaches every platform archive and its checksum
  to a draft release, then publishes that draft in the same run once the upload
  completes, so a release never appears without its assets. A workflow run that
  fails leaves the release a draft; re-run it for the same tag to finish, and
  the run refuses to touch a release that is already published.

## Rollback

Keep the previous release artifacts and installer instructions available. If a
release is withdrawn, publish the next fixed version rather than retagging a
published version.
