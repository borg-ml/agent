# Claude and Codex provider parity

Claude and Codex both use Borg's native harness over direct HTTP/SSE model
transports. Neither runs its upstream CLI, SDK agent loop or app server.
Parity means equivalent durable user workflows, not identical wire protocols.

| Concern | Claude | Codex |
| --- | --- | --- |
| Model protocol | Subscription Messages stream | Subscription Responses stream |
| Continuation | Account-bound content blocks and signed thinking | Account-bound Responses items and encrypted reasoning |
| Cache | Explicit breakpoints with one-hour retention | Stable prefix key and turn-scoped routing state |
| Fast mode | Selected account/model capability validation | Subscription priority tier when admitted |
| Agent behavior | Borg tools, permissions, steering, cancellation and compaction | Same Borg-owned contract |
| Recovery | Borg journal and native continuation state | Borg journal and native continuation state |

Borg owns context policy, subagents and durable session state for both routes.
Incomplete tool-input fragments may drive progress feedback, but only validated
complete calls execute. Authentication recovery must preserve the selected
account, model, effort and billing route; failure must not start an upstream
agent loop or silently spend API credit.

## Claude login and recovery

Use `borg login claude`, or `/login` in an idle Claude thread. Borg performs
browser sign-in itself and normally stores its login in `~/.borg/claude`,
separate from Claude Code. `BORG_CLAUDE_CONFIG_DIR` overrides the selected
directory. The selection is saved only after successful sign-in; abandoning
login leaves the working selection unchanged. Existing API credentials are
retained without becoming a subscription fallback.

Sign in on the host executing the session; Borg Remote does not copy credentials.
Replayed native state stays bound to its authenticated account.

## Verification boundary

Adapter tests cover request construction, stream normalization, usage and opaque
state. Session tests cover approved tools, interruption, steering, queueing,
compaction and replay. Credentialed checks additionally exercise login/refresh,
two-turn tool continuation and restart. Cache hits in a bounded probe do not
establish long-context parity or subscription-allowance savings.

See [Provider ownership](provider-ownership.md),
[Provider parity contract](provider-parity.md),
[Model context](model-context.md) and
[Prompt cache posture](prompt-cache-posture.md) for the shared living contracts.
