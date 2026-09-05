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
| Codex subscription | Pooled `codex app-server` | Model/tool loop, native tools, context/compaction behavior, and parts of approval policy |
| Claude subscription | Claude binary through `claude-agents` | Model/tool loop, native tools, and native session/context behavior |
| OpenCode | Authenticated local server event stream | Model/tool loop, native tools, and native session/context behavior |
| Kimi, GLM, OpenRouter, compatible endpoints | Borg native harness | Model service and provider protocol only |

The OpenCode server integration fixes early tool visibility but does not yet
meet the target ownership boundary. Equivalent rendered events alone do not
establish equivalent ownership.

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

### Model-only prototype

`CodexModelProvider` is an explicit prototype, not the production chat route.
It sends a Responses request directly and returns a `ModelTurnResult`; it
cannot execute tools or start a provider agent turn. The existing Codex binary
is used only for `initialize` and `getAuthStatus`, retaining credential-store
and refresh ownership without copying refresh tokens. A 401 permits one
auth refresh and retry before streaming; interrupted streams are never retried.
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
Real refresh and re-login recovery, capabilities/context limits, rate-limit UX,
and end-to-end subscription steering/cancellation still need verification.
The small live probe reported zero cached tokens; preserving the cache key is
not proof of a cache hit. Production routing is deliberately unchanged.

The explicit `LocalAgentTurnExecutor::with_codex_model_only()` probe now routes
through the real native harness and session actor. Loop ownership is queried
from the executor so replay/compaction use the same choice as model dispatch.
Ordinary executors still use the existing subscription route. Run
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

Borg commits an immutable subscription account fingerprint in its SQLite
session authority before the first model request. It is not a credential or
model context. Every model round checks the current account against that
binding before connecting, including authentication recovery. Binding survives
compaction, context clearing, and restart; forks and child sessions inherit it.
The additive table upgrade preserves existing sessions. Old subscription
history without account provenance is refused by the opt-in route and requires
a new session. The production CLI route is unaffected.

Before enabling the route by default, model limits and fast mode still need
to flow through the native contract. Real login/refresh
and in-flight tool control behavior still need end-to-end subscription
verification.

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
