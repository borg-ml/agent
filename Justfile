set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

default:
    @just --list

# Update the pinned Blu runtime to current upstream HEAD and refresh Cargo.lock.
blu-update:
    ./scripts/sync-blu.sh --update

# Release/CI freshness gate for the embedded Blu runtime.
blu-check:
    ./scripts/sync-blu.sh --check

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
    sdk_dir="$install_dir/node_modules/@anthropic-ai/claude-agent-sdk"
    cp "$platform_dir/claude" "$native_dir/claude"
    cp "$sdk_dir/manifest.json" "$native_dir/manifest.json"
    cp "$sdk_dir/package.json" "$native_dir/package.json"
    chmod 700 "$native_dir/claude"

# Validate a release without changing the repository.
release-check version="":
    ./scripts/release.sh --check {{ quote(version) }}

# Exercise release versioning, rollback, tagging, and publication hermetically.
release-test:
    ./scripts/release-test.sh

# Profile frontend input latency with a large session, live output, and bounded disk pressure.
tui-stress:
    ./scripts/stress-terminal-ui.sh

# Run the repository quality gates used by local development and CI.
verify:
    cargo fmt --all -- --check
    cargo check --workspace --exclude borg-gui --locked
    cargo test --workspace --exclude borg-gui --locked --no-fail-fast -- --test-threads=1
    cargo clippy --workspace --exclude borg-gui --all-targets --locked -- -D warnings
    cargo deny check advisories bans licenses sources
    # Keep the RSA dependency check explicit so a future database feature
    # cannot reintroduce the Marvin-attack edge into the active graph.
    if cargo tree --workspace --exclude borg-gui --target all -e features -i rsa 2>/dev/null | grep -q 'rsa'; then echo 'active rsa dependency detected' >&2; exit 1; fi
    # Unmaintained transitive dependencies with no safe upgrade are documented
    # in deny.toml; keep cargo-audit aligned with that reviewed exception list.
    cargo audit --ignore RUSTSEC-2024-0320 --ignore RUSTSEC-2025-0141 --ignore RUSTSEC-2025-0052 --ignore RUSTSEC-2024-0384 --ignore RUSTSEC-2024-0436 --ignore RUSTSEC-2026-0173 --ignore RUSTSEC-2025-0134 --ignore RUSTSEC-2026-0206 --ignore RUSTSEC-2026-0192
    git diff --check

# Check the experimental native GUI explicitly; it is excluded from normal verification and releases.
gui-check:
    cargo check -p borg-gui --locked

# Bump, verify, commit, tag, and publish a release. Defaults to the next patch.
release version="":
    ./scripts/release.sh {{ quote(version) }}

# Bump the minor component and reset the patch component, e.g. 0.1.44 -> 0.2.0.
release-minor version="":
    ./scripts/release.sh --minor {{ quote(version) }}

# Build and install the optimized public Borg Agent from this checkout.
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
