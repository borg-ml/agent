# Provider parity contract

Borg exposes one durable session contract across provider-specific model
protocols. Claude and Codex are native HTTP/SSE routes, not CLI/SDK agent runtimes.
See [Provider ownership](provider-ownership.md) for access and billing boundaries.

Parity is checked at meaningful boundaries:

- Borg tools, permissions and approved execution use the same dispatcher.
- Steering, interruption, queue admission and recovery belong to the session actor.
- Text, reasoning, tool-input progress, complete calls and results are normalized
  without inventing progress a transport cannot expose.
- Usage distinguishes uncached input, cache reads, cache writes and output;
  reasoning is part of output, not an additional token charge.
- Compaction preserves the journal, tool pairing, recent evidence and durable
  continuation state. Failure does not justify selective history loss.
- Authentication, entitlement and account-bound replay fail explicitly rather
  than switching models, billing or agent loops without authorization.

Deterministic adapter and session tests protect wire translation and durable
invariants. Credentialed smoke tests additionally check real login, streaming,
tool continuation, cancellation, restart and warm-cache behavior. A successful
small smoke is not proof of subscription-quota savings or long-context parity.

## Runtime supervision

The session actor owns progress supervision for root and child turns across
providers. After one minute without model progress it reports the quiet state;
at five minutes it reports “possibly stalled” without terminating the turn.
A silent model fails after twenty minutes by default. Set
`BORG_PROVIDER_STALL_TIMEOUT_SECS` to change that budget (`0` disables model-stall
failure, not status reporting). Usage counters and provider metadata do not count
as model progress. Startup and drain remain bounded separately.

In-flight tools retain a separate two-hour silence budget. Human approvals and
provider questions pause the watchdog until answered. Both monotonic and wall
clocks are checked so suspend/resume cannot hide an already-expired budget.
After detected suspension or a long watchdog scheduling pause, startup and active
provider waits get a 60-second reconnection grace period before the original
deadline is enforced. Normal polls do not renew this grace; real progress resets
the silence timer. Cancellation and drain deadlines are not extended.
Timeouts publish a durable failed turn; they do not blindly replay a possibly
side-effecting tool. The session remains available for recovery, and cleanup
failure is reported rather than leaving the turn indefinitely running.

Dropping a subscription stream cancels the entire provider invocation, including
handshakes and nested runtime tasks. The CLI sleep-prevention setting also covers
active children when the parent is idle; disabling the setting still wins.
