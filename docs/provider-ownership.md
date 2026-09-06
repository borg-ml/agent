# Provider ownership

## Decision

Borg owns agent behavior. External provider responsibilities must be reduced
to model access and what is necessary to fully support subscriptions with good
UX. Existing upstream agent runtimes are compatibility implementations, not
evidence that their responsibilities must remain external.

This is an ownership boundary, not a requirement to reimplement every library.
Use provider-specific transport and authentication code where it serves this
boundary without importing another agent loop.

## Target boundary

| Responsibility | Owner |
| --- | --- |
| Conversation journal, replay, recovery, and context policy | Borg |
| Agent loop, tool catalog, execution, permissions, and subagents | Borg |
| Steering, cancellation policy, retries of agent work, and UI state | Borg |
| Model request/response translation and model-specific capabilities | Model adapter |
| Network transport, bounded connection recovery, usage, and cache handles | Model adapter |
| Subscription login, secure credentials, refresh, account selection, and entitlement errors | Access adapter |

Adapters expose generation fragments separately from completed tool calls.
The first received nonempty tool fragment or explicit tool-input start makes
generation visible; incomplete arguments never need to parse first. Tool
execution begins only after a complete, validated call. Transport activity,
reasoning completion, and silence do not establish that a new call is generating.

Provider continuation handles and opaque model state may be necessary for
quality and cache reuse. Preserve them as part of Borg's recorded model
exchange; a provider-owned conversation must not silently become authoritative.
Connection retries must not replay completed tool side effects.

Subscription access must not select a different owner for the agent loop.
If a route still requires an external agent runtime, expose its capability
limitations explicitly and retain it only while its replacement is unproven.
Do not silently switch billing routes or execute a second agent loop as fallback.

## Current responsibilities to reduce

| Route | Current implementation | Remaining external agent behavior |
| --- | --- | --- |
| Fresh Codex subscription sessions | Borg native harness and direct model transport | CLI retained for subscription authentication/recovery, version metadata, and quota reads; no provider agent turn |
| Existing Codex compatibility sessions | Pooled `codex app-server` | Model/tool loop, native tools, context/compaction behavior, and parts of approval policy |
| Claude subscription | Claude binary through `claude-agents` | Model/tool loop, native tools, and native session/context behavior |
| OpenCode | Authenticated local server event stream | Model/tool loop, native tools, and native session/context behavior |
| Kimi, GLM, OpenRouter, compatible endpoints | Borg native harness | Model service and provider protocol only |

The OpenCode server integration fixes early tool visibility but does not yet
meet the target ownership boundary. Equivalent rendered events alone do not
establish equivalent ownership.

### Remaining subscription boundaries (checked 2026-09-06)

Claude's [Agent SDK overview](https://code.claude.com/docs/en/agent-sdk/overview)
explicitly includes Claude Code's loop and context management; it directs
callers implementing their own loop to the model API instead. The SDK overview
also requires prior approval for offering subscription login in third-party
products. The newer [subscription usage notice](https://support.claude.com/en/articles/15036540-use-the-claude-agent-sdk-with-your-claude-plan)
says the planned SDK billing change was paused and existing SDK/CLI/third-party
usage still draws on subscription limits. These are not a documented
model-only subscription API contract. Keep the existing SDK-backed access path
while that boundary is unresolved; do not infer that a subscription credential
can simply replace an API key in a new direct adapter.

Borg currently passes Claude's partial-message flag and MCP bridge but leaves
built-in tools enabled. File listing, bounded reads, and search now live in the
central Borg dispatcher: native tools, persistent runtimes, and subscription
MCP clients share the same execution provider and workspace-root checks.
The bridge test verifies discovery, action metadata, bounded line reads, and
refusal to list/read/search outside the session root in Manual mode.

The existing persistent-runtime tool now requests approval directly from the
Borg session actor when called through the bridge in limited permission modes.
The actor journals the prompt and decision and answers the waiting Borg tool,
not the provider control channel. Denial, caller disconnect, interruption, and
turn completion cannot authorize the waiting call; queued requests expire with
their turn. Human approval waits do not trigger the model-stall timeout. Requests
too large to display are rejected instead of presenting a blind approval.
This first consumer uses explicit approval in both Manual and Auto modes when
the caller has not already obtained approval; native Auto review is unchanged.

Native and persistent-runtime file writes/edits now share the dispatcher as
well, including configured transfer limits and exact-match edit validation.
Native trusted-workspace permission policy and the runtime's explicit effects
gate remain at their call sites. The shared mutation handler is not exposed as
a standalone MCP tool.

Merely disabling provider tools is still not a complete migration. Shell
retains its existing execution paths. A narrower SDK
compatibility mode needs Borg's approval, cancellation, and result-journaling
boundary connected before those tools can move into the bridge. Raw execution
methods must not become an MCP shortcut around that boundary. Even then, the
SDK would still own its internal model loop and session context; describe that
limitation explicitly.

The inspected OpenCode source (`bbd72fb`) exposes provider metadata and OAuth
authorization/callback routes, not a model-only streaming endpoint. Its
provider/auth/plugin services are coupled to instance configuration and the
session processor. Retaining the local server preserves those integrations;
calling its session prompt endpoint is still another agent loop, not a native
model adapter. A replacement needs a separately verified access boundary for
each underlying provider, not an assumption that all OpenCode credentials are
interchangeable subscription tokens.

For generation feedback, pending OpenCode tool-input events remain immediate.
An already-running/completed/error snapshot is not evidence of current input
generation, so it must not synthesize a generation event or reopen that phase.

## First migration proof: Codex subscription model access

Investigate a model-level subscription adapter feeding the existing
`NativeModelClient`/`ModelTurnRequest` path. Keep authentication/access separate
from model request construction and Borg's tool dispatcher.

The inspected OpenAI source separates `codex-api` (including raw tool-input
events) and `codex-login` from `codex-core`'s agent loop. These are candidate
reference boundaries, not yet adopted dependencies. Inspect their transitive
dependencies and subscription requirements before deciding what to reuse.

OpenAI documents ChatGPT authentication and app-server integration. Those
documents do not, by themselves, establish a stable public contract for using
the underlying subscription model endpoint directly. An endpoint present in
source is evidence to investigate, not a completed subscription integration.

Before switching a subscription route, verify real account login and refresh,
model/effort availability, first-fragment streaming, Borg-owned tool execution
and permissions, interrupt/steer behavior, multi-turn context and cache reuse,
usage/rate-limit reporting, and recovery without duplicate actions. Keep the
working subscription path available until this proof succeeds. Record exactly
which remaining external components are necessary and why.

### Codex model-only access

`CodexModelProvider` serves fresh Codex sessions through Borg's native harness.
It sends a Responses request directly and returns a `ModelTurnResult`; it
cannot execute tools or start a provider agent turn. The existing Codex binary
is used for `initialize`, `getAuthStatus`, and its version for the model catalog,
retaining credential-store and refresh ownership without copying refresh tokens.
A 401 permits one auth refresh and retry before streaming; interrupted streams
are never retried.
API-key authentication is rejected rather than changing billing routes.

The access boundary remains temporary: app-server is still a large dependency
for this small responsibility. Importing the inspected `codex-login` would
bring 32 local Codex crates (17 for `codex-api` alone), though neither brings
`codex-core`. The upstream SSE decoder also discards function-call argument
deltas. Borg therefore implements the small model wire adapter directly;
extracting/reusing a narrower maintained login component remains open.

Run the bounded, subscription-backed probe explicitly:

```sh
cargo run -p borg-provider --features subscription-adapters --example codex_model_probe
```

The probe verified Astra/low, first tool-generation feedback, a host-owned
read-only tool result, and a second model round after durable-message
serialization. Offline streaming tests additionally hold back complete
arguments until the first `{` generates feedback, and verify opaque reasoning,
assistant phase, and call IDs survive replay. Responses state is recorded in
the optional protocol-tagged `ModelMessage.provider_state`, not hidden in
visible reasoning or a provider-owned conversation. Compatible adapters omit
that field on the wire.

The small transport probe alone does not establish a native-harness migration.
The small live probe reported zero cached tokens; preserving the cache key is
not proof of a cache hit. The following sections record the subsequent
subscription, cache, control, and rollout evidence.

`LocalAgentTurnExecutor` resolves each session's durable route before the actor
loads replay or chooses compaction. Loop ownership is queried
from the executor so replay/compaction use the same choice as model dispatch.
Fresh sessions use Borg's harness; existing subscription history retains its
compatibility route. `with_codex_model_only()` remains an explicit requirement
for isolated probes, not an override of an existing route. Run
`cargo run -p borg-remote --example codex_native_probe` for a temporary session
that approves only `cat probe.txt`, stops, and restarts from its durable journal.
This passed with Astra/low, Borg manual approval and tool execution, preserved
model state across restart, and 1,024 cached input tokens during the first turn.
The strengthened probe also checks opaque output and tool-round boundaries in
the stored journal, not only the live event stream. This caught provider-static
persistence/context filters that discarded native Codex model events. Those
filters now recognize Borg-native event kinds independently of the provider.
The rerun passed journal checks and reported 1,152 cached tokens after restart.

The shared native loop now emits each stateful tool result before dispatching
the next call, stops queued actions after an accepted steer, and polls controls
during parallel read batches. Replay retains completed results and opaque model
state when a round is interrupted, fails, or ends without a terminal event.
Missing results are explicitly unknown, not evidence that a command never ran;
recovery must inspect state before repeating an uncertain action. Accepted
steering input is retained even if interruption precedes its model-message
event. Offline loop tests exercise actual file writes and verify the second
queued write does not run after steering or interruption.

The running-tool wait also continues polling controls after accepting steering.
Further accepted corrections retain their text and attachments in order, while
Stop retains its bounded cleanup wait. A held-open tool test covers multiple
steers followed by completion or interruption; no completed result is discarded
merely because steering was accepted.

Automatic approval review also polls controls while waiting for its model.
Interruption drops the review request, and accepted steering skips the proposed
action. These outcomes are distinct from provider errors, so cancellation does
not open a fallback approval prompt. A held-open reviewer test verifies immediate
control handling and request cancellation. Queued controls are checked again
after approval/status delivery before the tool starts.

`codex_native_probe --controls` verifies real subscription steering and
interruption with one allowlisted command in a disposable directory. The command
records its PID and waits ten seconds. Steering is acknowledged while that
process is still running and changes the final response within the same turn;
interrupt must reap the process before the terminal event, within six seconds.
The live probe exposed a cooperative-interrupt cleanup gap: the model call ended
but its session-owned process survived. The session actor now waits for native
process cleanup before publishing interruption, just as the forced-timeout path
already did. Both live cases passed after the fix. The existing cleanup-barrier
test also covers cooperative native cancellation, so it cannot regress silently
between manual subscription probes. CLI subscription thread reuse is unchanged.

Account admission also polls interruption while authentication/storage is pending.
Steering received during admission remains unacknowledged and recallable until
admission succeeds, then enters Borg's durable model context in arrival order.
Failure or interruption drops the pending acknowledgements without accepting the
messages, so the session actor retains them for a later boundary. A held-admission
test verifies interruption, failure, ordered text/attachments, and recall without
touching live credentials. The existing live native-session probe passed tool
approval/execution, compaction, restart, and isolated consultation after this
change; the stalled-admission cancellation check itself remains deterministic
and offline.

Borg commits an immutable subscription account fingerprint in its SQLite
session authority before the first model request. It is not a credential or
model context. Every model round checks the current account against that
binding before connecting, including authentication recovery. Binding survives
compaction, context clearing, and restart; forks and child sessions inherit it.
The additive table upgrade preserves existing sessions. Old subscription
history without account provenance is refused by model-only admission and requires
a new session. Existing CLI sessions retain their compatibility route.

The additive `session_harness_routes` table fixes the route independently of
context clearing, compaction, restart, or a later executable default. Fresh root
sessions choose native execution; forks and new children inherit the owner's
choice. Existing sessions without a route retain compatibility, except prior
account-bound or native-message sessions, which must enter native admission.
Unbound prototype history therefore fails closed rather than falling back to
the CLI. Attaching a child with a conflicting route is refused. Native startup
always loads Borg's journal, never a provider-thread recovery checkpoint.
The local and enrolled-host factories share this session assembly boundary.

This is not an implicit migration of old history: compatibility sessions still
have the upstream CLI's generic first-argument streaming limitation. Starting a
fresh session selects the first-fragment-capable native route. Live
expired/revoked-auth recovery remains unobserved; the retained upstream
credential manager and Borg's tested recovery policy remain unchanged.

The default-executor live probe passed approval, execution, compaction,
consultation, and restart without a native-only override. Its `--controls`
variant also passed same-turn steering and process cleanup on interruption.
Storage tests cover concurrent selection, both routes across reopen/clear/fork/
child registration, conflicting-route refusal, and the additive schema upgrade.

Turns, compaction, and one-shot consultations now share the same host-owned
`ModelAccessContext` admission step. The context carries the session/store only
inside Borg, never in the model prompt. Native Codex auxiliary calls refuse
missing durable storage before authentication. Compaction no longer checks the
provider's old CLI routing flag; it uses the bound model client, including for
in-turn automatic compaction. The expanded live probe passed manual Borg-owned
compaction, exact-value replay after restart, and a tool-free isolated
consultation under the same account binding. That compaction run reported zero
cached tokens; it does not establish cache reuse across a rewritten summary.

Native usage aggregation preserves subscription-equivalent and provider-reported
cost classifications. Mixed reported/estimated API costs are estimates; missing
prices or incompatible billing bases never produce a misleading partial total.
The live probe verifies subscription classification through turn, compaction,
and restart usage events. This does not yet establish rate-limit reporting.

The model adapter reads context limits and supported effort levels directly
from the subscription model catalog before sending conversation content. The
endpoint requires the access adapter's client version. Only the small metadata
subset is retained, cached in memory for five minutes and scoped to the account;
provider agent instructions and tool policies are not imported. Missing/invalid
context limits and unavailable model/effort selections fail explicitly. Catalog
authentication recovery checks account continuity just like model requests.
The live native probe received a 258,400-token usable context limit for Astra/low,
so Borg's existing context-threshold policy now has a provider-derived limit.

The native model request now carries the session's explicit fast setting through
model rounds, approval reviews, and manual/automatic compaction. Codex validates
catalog support and sends `service_tier: "priority"`; standard requests omit it.
Compatible routes reject unsupported fast requests instead of ignoring the flag.
Isolated consultations use standard routing because consultation profiles do not
select a speed tier. Wire tests verify the outgoing priority field, and loop tests
verify the setting survives a steered follow-up model round.

`codex_model_probe --fast` reports the requested and returned service tiers
separately. The live endpoint reported `service_tier: "default"` despite a
priority request. This is not confirmation of delivered priority processing.
The adapter now also sends the upstream `x-codex-routing-hint` header: model
only for standard requests, model plus `tier=priority` for fast requests. The
existing first-character wire test checks that this matches the fast request
body. A further live fast probe with the header still reported `default`.
The native-session probe with `--fast` passed tool execution, compaction, and
restart; it verifies those workflows with fast requested, not the effective tier.
An ephemeral read-only comparison through installed Codex accepted `priority`
and completed successfully, but its raw-completion event exposed usage metadata
without an effective service tier.

A subsequent bounded localhost relay inspected only model/tier metadata while
forwarding the installed upstream CLI's requests to the fixed subscription
endpoint. Both its HTTP fallback and preferred WebSocket transport sent
`priority`, completed a tool-free Astra/low turn, and received `default` in the
raw completed response. No credentials or raw bodies were logged or persisted, no
saved configuration changed, and the relay was stopped after each ephemeral
turn. Thus this field does not demonstrate a Borg-specific fast-mode regression.
The probe no longer treats it as one; it still reports the discrepancy explicitly,
and wire tests still require the priority request and routing hint. Neither the
upstream comparison nor Borg's probe establishes an effective-priority guarantee.

A real `getAuthStatus` refresh request completed with subscription mode retained
and `includeToken: false`; no credential was returned to the diagnostic process.
The native tool/compaction/restart/consultation probe passed afterwards using the
original credential store. This verifies access after a requested refresh, not
expired-token recovery or re-login after revocation, which still need proof.

Catalog and model HTTP requests share one account-bound recovery boundary.
A controlled loopback test verifies that only HTTP 401 invokes refresh, the
retry preserves the request body and uses the refreshed token on the original
account, and a second 401 returns without another attempt. Changed-account and
failed-refresh results transmit no retry. These checks use synthetic credentials;
they verify Borg's recovery policy, not live credential rotation or re-login.

Native subscription failures now distinguish usage/rate limits, context limits,
and denied account access. HTTP `Retry-After` and structured reset fields provide
retry timing when available. Error bodies are size- and time-bounded; only known
codes and numeric/date fields inform the user-facing message, never raw provider
messages or account identifiers. HTTP quota rejection emits no generation event;
a streamed quota failure after a tool fragment returns no executable result.
Protocol tests cover these boundaries without exhausting live account quota.
No quota retry or billing fallback was added. Existing `/usage` and host provider
admission already read subscription windows independently of the agent loop via
`read_codex_account_rate_limits`. Its access-only app-server exchange initializes
the connection and calls `account/rateLimits/read`; it starts no thread or model
turn. A live host capability read returned the Codex weekly percentage and reset
time. The native route therefore retains the existing on-demand quota view;
there is no need to introduce a second window-event pipeline for parity. Live
exhausted-account recovery remains unverified.

## Evidence

- Current routing: `crates/borg-remote/src/contract.rs` (`uses_native_harness`)
  and `crates/borg-remote/src/agent.rs` (`run_borg_provider_turn`).
- Existing model boundary: `crates/borg-remote/src/native_harness.rs`
  (`NativeModelClient`) and `crates/borg-provider/src/provider/model_turn.rs`.
- [OpenAI authentication documentation](https://learn.chatgpt.com/docs/auth).
- [OpenAI app-server documentation](https://learn.chatgpt.com/docs/app-server).
- OpenAI source inspected at `7dc7c7a7566a970f6d4d09e1384f854aebaf39e0`:
  `codex-rs/codex-api`, `codex-rs/login`, and
  `codex-rs/core/src/session/turn.rs` (`ResponseEvent::ToolCallInputDelta`).
