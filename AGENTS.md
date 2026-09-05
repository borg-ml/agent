# Borg Agent instructions

## Provider ownership

- Borg owns the agent loop, context policy, tool execution, permissions,
  steering, cancellation, subagents, and durable state across providers.
- Keep external provider responsibilities to model access and the minimum
  required to fully support subscriptions with good UX. A dependency on an
  upstream agent runtime is a compatibility constraint to reduce, not the
  target architecture. Justify any retained responsibility against a concrete
  subscription requirement.
- Preserve subscription login/recovery, model capabilities, streaming,
  continuation/cache behavior, and usage visibility when moving behavior into
  Borg. Do not silently fall back to API-key billing or another agent loop.

## Change discipline

- Keep simple changes small and direct. Do not add slop: speculative
  abstractions, helper layers, comments, fixtures, scaffolding, or code that
  does not materially support the requested behavior.
- Do not add tests reflexively because a line changed or a bug was mentioned.
  Add one only when it is useful and warranted by a concrete failure mode.
- A warranted test protects a meaningful product contract, public interface,
  durable invariant, security boundary, persistence/replay rule,
  cross-process contract, or high-value user workflow. Before adding one,
  state the failure mode it prevents and why a compiler check, existing test,
  integration smoke test, or simpler code is not the better guard.
- When a test is justified, make it small and high-signal. Test externally
  meaningful behavior rather than incidental strings, private helper shape,
  fixture ordering, or one-off implementation details.

## Long-running work and version control

- Work on `main` unless the user explicitly requests another workflow.
- During long-running agent work, commit coherent units of progress frequently
  and push them to `origin/main` so work stays visible and recoverable.
- Before each commit, inspect the status and diff, preserve unrelated user or
  other-agent changes, and use a clear commit message. Do not bundle unrelated
  work merely to create a commit.
- Keep the working tree recoverable: do not leave a long-running task with a
  large, uncommitted change set when a coherent checkpoint can be committed.
