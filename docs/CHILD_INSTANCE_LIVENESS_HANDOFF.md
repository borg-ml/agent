# Hosted child instance liveness

Source-only repair in `fix/child-owner-liveness-c30c72ed`, based on `62930b99`.
No builds/tests/evaluations/harness probes were run. No running process was
upgraded, restarted or stopped by this repair. Shared `~/agent` edits were not
modified; changes live in an isolated linked worktree.

## Source-backed defect

`run_agent_session_with_store_and_writer_and_team` registers each session's
instance using `std::process::id()`. In-process child actors therefore record
their supervisor's PID. `start_reserved` runs them with a writer lease through
`boxed_agent_store_session`; it does not create a child control server or its
`<child>.control.owner.json` metadata.

Discovery and workspace broadcast treated a missing child owner record as
positive evidence that the owning process had exited and durably retired the
row. Subsequent direct messages then refused that retirement marker even
while the recorded supervisor remained alive. Observed direct ownership
replies to building/habitation were refused, while their parent was reachable.
This establishes a source defect, not proof that every listed child currently
has a running actor or that each replayed startup report is fresh.

## Small repair

- Existing control metadata remains authoritative: PID identity/start-time
  validation stays unchanged. Malformed existing records are not bypassed.
- Only absent metadata permits checking a positive, representable recorded
  PID. Invalid/nonpositive PIDs fail closed.
- Discovery and workspace broadcast use that owner check before retiring rows.
  A child without its own socket remains unreachable for immediate dispatch;
  a live supervisor is not mislabeled as a dead OS process.
- Direct message admission accepts a positively live owner despite a stale
  retirement marker. Recipient routing, explicit stop/wake admission and
  command handling are otherwise unchanged; the patch adds no wake requests.

No bulk roster rewrite or tombstone deletion is performed. Already retired
rows may remain hidden in default discovery until their normal registration
refresh; include-exited can still inspect them. PID-only legacy evidence is
weaker than owner metadata and is used only when that metadata is absent.

## Verification and delivery limit

Static diff/ownership inspection only. Rust LSP was configured explicitly with
Cargo build scripts, proc macros and check-on-save disabled. The first pull
returned before indexing readiness and is not used as clean validation. A
second pass reported quiescent indexing, then returned 70/102 diagnostics for
the two files: inactive-code/macro-disabled hints and 14 existing-site type
errors outside the changed regions. With proc macros disabled, this is limited
analysis, not compilation validation; those unrelated sites were not edited.
The server rejected `workspace/diagnostic` as unsupported. Results were kept
at `/tmp/c30-ra-indexed.json`; no compiler/test substitute was run.

The running Borg binary does not gain this source fix automatically. A human-
authorized normal build/update is required before corrected discovery and
message admission can be verified. Do not bypass the no-build rule, restart
someone else's active agent, claim direct worker delivery repaired already,
or ask the human to relay coordination messages. The reachable coordinator
has the exact completed foundation/furnace clearance in the meantime.
