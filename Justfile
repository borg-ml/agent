set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

default:
    @just --list

# Install only the Claude binary used by the native Rust runtime.
claude-native:
    #!/usr/bin/env bash
    set -euo pipefail
    source_dir="$PWD/packages/claude-native-runtime"
    install_dir="${BORG_HOME:-$HOME/.borg}/providers/claude-native-runtime"
    native_dir="${BORG_HOME:-$HOME/.borg}/providers/claude"
    test -f "$source_dir/package-lock.json"
    mkdir -p "$install_dir" "$native_dir"
    cp "$source_dir/package.json" "$source_dir/package-lock.json" "$install_dir/"
    npm --prefix "$install_dir" ci --omit=dev --ignore-scripts
    platform_dir=""
    for candidate in "$install_dir"/node_modules/@anthropic-ai/claude-agent-sdk-*; do
        if [[ -x "$candidate/claude" ]]; then
            platform_dir="$candidate"
            break
        fi
    done
    test -n "$platform_dir"
    cp "$platform_dir/claude" "$native_dir/claude"
    cp "$platform_dir/manifest.json" "$native_dir/manifest.json"
    cp "$install_dir/node_modules/@anthropic-ai/claude-agent-sdk/package.json" "$native_dir/package.json"
    chmod 700 "$native_dir/claude"

# Validate a release without changing the repository.
release-check version="":
    ./scripts/release.sh --check {{ quote(version) }}

# Exercise release versioning, rollback, tagging, and publication hermetically.
release-test:
    ./scripts/release-test.sh

# Run the repository quality gates used by local development and CI.
verify:
    cargo fmt --all -- --check
    cargo check --workspace --locked
    cargo test --workspace --locked --no-fail-fast
    cargo clippy --workspace --all-targets --locked -- -D warnings
    cargo deny check advisories bans licenses sources
    # Keep the RSA dependency check explicit so a future database feature
    # cannot reintroduce the Marvin-attack edge into the active graph.
    if cargo tree --workspace --target all -e features -i rsa 2>/dev/null | grep -q 'rsa'; then echo 'active rsa dependency detected' >&2; exit 1; fi
    # syntect currently brings bincode 1.x, which is covered by deny.toml's
    # documented unmaintained-dependency exception.
    cargo audit --ignore RUSTSEC-2025-0141
    git diff --check

# Bump, verify, commit, tag, and publish a release. Defaults to the next patch.
release version="":
    ./scripts/release.sh {{ quote(version) }}

# Bump the minor component and reset the patch component, e.g. 0.1.44 -> 0.2.0.
release-minor version="":
    ./scripts/release.sh --minor {{ quote(version) }}

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
