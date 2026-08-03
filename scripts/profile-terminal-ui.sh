#!/usr/bin/env bash
set -euo pipefail

profile_test="terminal_ui::tests::large_resume_ingest_and_transcript_scroll_profile"

echo "Running the repeatable release-mode resume/scroll profile"
cargo test -p borg "$profile_test" --release -- --ignored --nocapture

if command -v perf >/dev/null 2>&1; then
    echo
    echo "perf is available; repeat the command below for counters:"
    echo "  perf stat cargo test -p borg $profile_test --release -- --ignored --nocapture"
else
    echo
    echo "perf is not installed; use the printed timings above or install cargo-flamegraph/perf for a sampled profile."
fi
