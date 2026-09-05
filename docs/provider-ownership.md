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
