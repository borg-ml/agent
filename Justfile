set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

default:
    @just --list

# Install the pinned Claude Agent SDK adapter used by local and remote sessions.
claude-sdk:
    #!/usr/bin/env bash
    set -euo pipefail
    source_dir="$PWD/packages/borg-claude-sdk"
    install_dir="${BORG_HOME:-$HOME/.borg}/providers/claude-sdk"
    test -f "$source_dir/package-lock.json"
    mkdir -p "$install_dir"
    cp "$source_dir/package.json" "$source_dir/package-lock.json" "$source_dir/tsconfig.json" "$install_dir/"
    mkdir -p "$install_dir/src"
    cp "$source_dir/src/provider.ts" "$install_dir/src/provider.ts"
    npm --prefix "$install_dir" ci
    npm --prefix "$install_dir" run check
    npm --prefix "$install_dir" run build
    npm --prefix "$install_dir" prune --omit=dev

# Validate a release without changing the repository.
release-check version="":
    ./scripts/release.sh --check {{ quote(version) }}

# Exercise release versioning, rollback, tagging, and publication hermetically.
release-test:
    ./scripts/release-test.sh

# Bump, verify, commit, tag, and publish a release. Defaults to the next patch.
release version="":
    ./scripts/release.sh {{ quote(version) }}

# Build and install the optimized public Borg CLI from this checkout.
cli:
    #!/usr/bin/env bash
    set -euo pipefail
    install_root="${BORG_CLI_INSTALL_ROOT:-$HOME/.local}"
    cargo install --root "$install_root" --path crates/borg-cli --locked --force --bin borg
    hash -r
    installed="$install_root/bin/borg"
    resolved="$(command -v borg)"
    if [[ "$resolved" != "$installed" ]]; then
      echo "Installed $installed, but this shell resolves borg to $resolved" >&2
      exit 1
    fi
    remote_config="${BORG_REMOTE_CONFIG:-${BORG_HOME:-$HOME/.borg}/remote/host.json}"
    if [[ "$(uname -s)" == "Linux" && -f "$remote_config" ]]; then
      "$installed" remote install --config "$remote_config"
    fi
    "$installed" --version

# Build and install an unoptimized binary for local debugging.
cli-dev:
    #!/usr/bin/env bash
    set -euo pipefail
    install_root="${BORG_CLI_INSTALL_ROOT:-$HOME/.local}"
    cargo install --root "$install_root" --debug --path crates/borg-cli --locked --force --bin borg
    hash -r
    installed="$install_root/bin/borg"
    resolved="$(command -v borg)"
    if [[ "$resolved" != "$installed" ]]; then
      echo "Installed $installed, but this shell resolves borg to $resolved" >&2
      exit 1
    fi
    remote_config="${BORG_REMOTE_CONFIG:-${BORG_HOME:-$HOME/.borg}/remote/host.json}"
    if [[ "$(uname -s)" == "Linux" && -f "$remote_config" ]]; then
      "$installed" remote install --config "$remote_config"
    fi
    "$installed" --version
