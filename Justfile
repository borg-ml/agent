set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

default:
    @just --list

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
