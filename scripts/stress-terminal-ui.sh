#!/usr/bin/env bash
set -euo pipefail

echo "Running the release-mode large-session frontend profile"
cargo test -p borg terminal_ui::tests::large_resume_ingest_and_transcript_scroll_profile \
    --release -- --ignored --nocapture

echo
echo "Running the real PTY input-latency profile under streaming and storage pressure"
cargo test -p borg --test tui_responsiveness live_tui_input_latency_under_storage_pressure \
    --release --features tui-stress -- --ignored --nocapture
