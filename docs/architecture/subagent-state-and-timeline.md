# Subagent state and timeline authority

## Three different facts

A reusable worker, a control operation, and a report are separate objects:

* **Worker summary** is current state keyed by child session identity. Starting,
  running, waiting on its own watchers, awaiting approval, idle/ready, dormant,
  stopped, and failed are explicit lifecycle states. A dormant interrupted worker
  also carries the existing resume policy; it is not silently interchangeable
  with an idle dormant worker.
* **Operation receipt** is the ordered tool action: spawn, inspect, configure,
  send, follow up, interrupt, wait, or goal control. Its completion settles that
  operation, not the worker. Accepted delivery is not recipient execution;
  accepted interruption is not a confirmed stopped worker.
* **Report/message** has its own message identity and delivery boundary.
  Completing assistant commentary or making a report available never changes
  worker lifecycle. Child report privacy and peer-message visibility remain
  independent of lifecycle.

The timeline retains ordered operation receipts and messages. A worker card is
explicitly a current-state summary, not a historical task-success event. It has
stable `child:<session-id>` identity and opens the child's transcript; inspection
of a tool receipt remains a different action. Updating a worker summary does not
append one card per heartbeat or manufacture a new operation.

## Runtime authority and admission

The child journal owns execution/configuration facts. The coordinator projects
those facts plus parent-controlled dormant/resume state into a versioned
`SubagentSnapshot`. The owning parent actor alone records parent activity.

A new worker is admitted by a typed `SubagentRegistered` manifest in its own
journal, written under the child's writer lease after durable ownership is
registered and before actor startup. Failure before admission releases the
reservation: a failed spawn receipt is not a phantom worker. Failure after
admission is journaled under that same lease. Parent recovery enumerates bounded
owned descendants and verifies exact durable owner identity before accepting a
manifest absent from its mirrored activity. A manifest without a started actor
recovers failed, never as permission to start work automatically.

Child-to-coordinator and coordinator-to-parent live streams are notifications,
not alternate authorities. Durable journal replay repairs missed child events;
periodic bounded reconciliation repairs missed administrative snapshots. The
mirrored child-event watermark is separate from the snapshot watermark, so a
newer summary cannot skip an older undelivered report. Watchers observe only
successfully recorded parent state.

Configuration of stopped/dormant workers is durable and published without
waking them. Recovery refreshes configuration even for terminal workers and
publishes UI-relevant snapshot changes even when the legacy status enum does
not change. Explicit stop, permission, provider, billing, model, effort, and
speed-selection rules are not relaxed by recovery.

## Ordering and compatibility

A snapshot optionally carries an explicit lifecycle and a source cursor:

* `child_sequence`: the child journal watermark represented by the snapshot;
* `revision`: the coordinator projection revision, retained across recovery and
  advanced for accepted execution/administrative changes.

Consumers accept or reject a snapshot once before changing its roster, card,
counts, approvals, or watcher interpretation. Revisions, not delivery clocks,
order versioned snapshots. Legacy snapshots without a cursor retain timestamp
compatibility; legacy English wait/pause details are decoded only at the
compatibility boundary. New producers emit typed lifecycle metadata.

Child view metadata has its own projector cursor and child journal watermark.
Its lifecycle can override a snapshot only when it represents a strictly newer
child journal sequence; the coordinator wins ties, preserving administrative
pause/stop overlays. Metadata must not regress a previously accepted journal
watermark, even across projector epochs. At an equal watermark, newer aggregate
metadata may update descendant approvals without redefining the worker's own
lifecycle. Projection revisions and journal sequences are never compared as if
they were the same clock.

## One semantic reducer

`borg-view::TranscriptCore` owns event interpretation. TUI rendering, remote
wire projection, and native GUI adaptation consume that shared semantic state.
The GUI is an adapter over transcript entries, not a second event reducer.
Canonical lifecycle detail and action identity are shared, rather than inferred
from English labels, report text, or the presence of `final_text`.

Full tool-inspection output can be retained by an opt-in ephemeral presentation
cache keyed by stable row identity. It is not lifecycle authority, is pruned on
row removal/context clear, and is excluded from normalized remote checkpoints.
Canonical journal/payload storage remains the durable inspection evidence.

## Verification contracts

Compiler checks do not detect valid but contradictory projections. Focused
regressions therefore cover report-versus-worker completion, lifecycle parity,
stale snapshot and child-metadata rejection, approval freshness, checkpoint and
journal replay, stopped-worker configuration, manifest-only startup recovery,
dropped-event repair, and accepted-versus-applied operation receipts. Existing
steering/stop/wake behavior remains a regression boundary.

## State machines, not a framework

Use narrow typed transition functions where order has product consequences.
Do not turn rendering, formatting, or every helper into a state machine.

| Machine | Boundary and invariant |
| --- | --- |
| Admission | Reserved → durably registered → actor startup. Pre-registration failure releases the reservation; registered startup failure is durable Failed. Missing parent notification is recoverable from the child manifest. |
| Worker execution | Typed session events drive starting/running/ready/approval/watch waiting; parent shutdown drives dormant states. Report completion is not an execution transition. Legacy status is derived from lifecycle in one transition function. |
| Stop and wake | Stopped/failed/dormant workers do not start merely because a stale event or a UI refresh arrived. Starting again requires an authorized wake or the existing interrupted-restore policy. |
| Delivery/action | The existing `SessionActionState::can_transition` and durable transition log govern admission, delivery, execution and settlement. A notification receipt cannot jump to recipient execution completion. |
| Projection | An accepted version advances state; duplicate/stale versions do not. Live delivery, repaired delivery, replay, and checkpoint restore produce equivalent semantic state. |

These machines communicate through typed events and stable identities. Effects
occur at the actor/lease/transaction boundary, not in renderer transitions.
Guards, replay tests and persistence tests are what make the design robust;
merely naming enum variants is not sufficient.

## Final architecture review

The final pass specifically checked actor ownership versus journal ownership,
repair/live path parity, stop/configuration concurrency, state transition guards,
view-cache authority, and bounded replay cost. It found and corrected additional
integration issues: repaired activity now honors the same nested-parent boundary
as live activity; stopped-worker configuration waits for actor teardown before
acquiring its writer; source-scoped approvals are derived at export rather than
stored in a second summary cache. Unknown completion events still reconstruct a
bounded history row, but known process polls do not create duplicate rows.

Further architectural extraction should follow a concrete need, not file size:

* The coordinator's journal repair/publication boundary is a useful candidate
  for a dedicated typed component if additional consumers appear. Preserve its
  single-writer authority and bounded cursors; do not introduce a parallel
  mutable roster service.
* Legacy status/detail decoding is intentionally isolated compatibility debt.
  Removing it requires a durable journal/protocol migration, not deleting old
  readers or assuming every installed client updates together.
* Worker summaries remain current-state cards at stable timeline identities.
  A future historical assignment/run view needs a separate durable run identity;
  reusing the child-session identity as both worker and assignment would recreate
  the original ambiguity.

No generic state-machine framework, event bus rewrite, or wholesale actor
replacement is required by these findings. Those changes would add a second
migration before demonstrating a stronger invariant.
