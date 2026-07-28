set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

default:
    @just --list

# Build and install the optimized public Borg CLI from this checkout.
cli:
    cargo install --path crates/borg-cli --locked --force --bin borg
    command -v borg
    borg --version

# Build and install an unoptimized binary for local debugging.
cli-dev:
    cargo install --debug --path crates/borg-cli --locked --force --bin borg
    command -v borg
    borg --version
