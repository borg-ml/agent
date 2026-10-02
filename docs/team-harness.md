# Inspecting and steering a team

These tools use the session/workspace stores; no SQL export or actor restart is
needed. Ownership means canonical child ownership, not shared workspace
membership, fork ancestry, discovery presence or a task-name prefix.

## Inspect without changing state

- `team_snapshot({limit: 50, recent_messages: 2})` returns owned descendants,
  canonical goal IDs/status/accounting, assigned plan and revision, approval and
  decision IDs, explicit gate/wait reasons and bounded actual recent messages.
  Coordinator execution observations have their own source and timestamp.
  No observation means unknown, not dead/finished. Use `next_after` to page;
  `max_bytes` bounds returned agent bodies (default 256 KiB, maximum 1 MiB).
- `inspect_agent({session_id, after_sequence: 0, limit: 50})` reads the actual
  descendant journal, with full assistant text, event/message IDs, timestamps
  and sequence counters. Continue with `next_after_sequence` while `has_more`.
  Limits count scanned canonical events, including excluded events, so an empty
  page can still have more. Text is never silently reduced to a final-answer
  prefix; an oversized event requires increasing `max_bytes` up to 1 MiB.
  Reasoning, provider/tool payloads, nested child events, attachments and private
  interaction payloads are excluded. Exposed text uses Borg's existing
  high-confidence secret scrubber; this is not a guarantee against arbitrary
  bespoke credentials embedded in prose.

Snapshots are non-atomic; execution, goal status and delivery projections are
independent. Active goal ≠ running process ≠ working ≠ waiting ≠ finished.
Read APIs do not acknowledge messages, repair projections or start sessions.

## Recoverable batches

`team_batch` accepts up to 64 existing `send_message`, `followup_task`,
`configure_agent` or `interrupt_agent` operations
for owned descendants. Example:

```json
{"idempotency_key":"handoff-2026-10-02-a","operations":[
  {"session_id":"CHILD_UUID","operation":"followup_task","message":"Continue the approved assignment."}
]}
```

Message operations require `message`; configure operations require a
`configuration` object with the existing provider/model/effort/fast/ultrafast
fields, and remain director-only. Interrupt operations need only `session_id`.
All targets and operation shapes are validated before any effect. Profile
support, billing and recipient gates remain those of the existing single-agent
operation; batching never expands authority.

Keep the key and operations unchanged on retry. The operation ID is stable for
that caller/key. `get_team_operation({operation_id})` reads persistent per-target
progress and results without retrying anything. A durable compare-and-swap
intent fences concurrent attempts before dispatch. Submitted targets are not
replayed. After interruption, a recorded canonical message recovers its ID
without another dispatch; an unproven `sending` target stays uncertain and is
not blindly retried. This intentionally prefers honest uncertainty to duplicate
side effects. A retry can still submit previously unsent targets.

`team_batch({...same request..., cancel:true})` cancels remaining unsent work;
it does not recall admitted messages or stop workers. Caller stop, approval,
decision and goal-budget gates prevent further submissions. Existing recipient
stop/approval/budget gates continue to apply. No goal-clear/reactivation,
session replacement, new permission or automatic handoff is added.

Submission progress is not business completion. `get_message_status` keeps its
existing state/attempt fields and adds timestamped evidence. Pending can already
have been dispatched locally; delivery projections can lag canonical recipient
messages. Attempts count recorded attempt receipts, not every dispatch. Read and
acted-on remain unknown without evidence; ACK is not approval or completed work.

## Explicit worktree context

`spawn_agent` optionally accepts `cwd`, an existing directory relative to the
assignment author workspace (or an absolute directory). It is the new child's
assigned/default directory for exec, initial runtime, read, search and LSP.
An explicit `cwd` prevents reuse of an existing idle worker. External assignment
requires existing Full Access; isolated/manual workspace boundaries still apply.
No existing session, worktree or runtime is rewritten.

`get_tool_context()` reports the assigned/default directory and separately
tracked shell directory. A leading literal shell `cd` persists for shell calls
only; conditional/later/subshell directory changes do not. Explicit tool paths
retain their existing path policy. Runtime code can deliberately change its own
working directory; this report gives its initial default, not an inferred current
runtime directory. There is no heuristic worktree takeover.

LSP unavailable/partial reports are not clean results. Clangd reports missing or
inferred compilation flags separately from diagnostics; source timestamps alone
do not prove the flags are stale. Large diagnostic summaries retain unavailable
and partial caveats. Source tests/commits do not prove an installed binary has
these capabilities.

## Parent goal controls

`get_agent_goal({target})` reads the owned child's canonical goal/accounting and revision.
`control_agent_goal({target, goal_action: {type: "set", objective, token_budget}})`
sets a goal through its owning actor. Use `type: "pause"`, `"resume"`, or `"clear"`
for the other actions. Only the director can mutate descendant goals, and only
under explicit human authorization. Resume/set cannot release a human stop.
Clear detaches the goal while retaining journal history. Pause/clear do not
interrupt a running turn: use `interrupt_agent` separately when stopping work.
A control receipt confirms queuing, not application; read the goal again to
verify the durable transition. Unfinished goal edits retain identity/accounting
under existing host semantics; a new objective after completion starts a new goal.
