# Unreal migration safety checklist

The Unreal adapter is a preview, not approval to replace a project's existing
build/editor wrappers. Implementation lives in `extensions/unreal/bin/`,
`editor/`, `workflows/` and `adapter.json`. See the
[adapter README](../../extensions/unreal/README.md) for configuration and CLI.

## Current boundaries

- Borg core owns queueing, budgets, process scopes, exclusive service handoff
  and recovery. The adapter supplies Unreal commands, fingerprints, canonical
  project/worktree keys, UBT startup isolation, symbols and editor hooks.
- `build.ubt_start_lock` / `UE_UBT_START_LOCK` can select a startup lock shared
  with a legacy lane. This merges only startup serialization, not queue,
  memory reservations or project run locks. Do not mix active pathways in
  one project tree.
- The adapter emits run JobSpecs, but non-spec exclusive runs fail closed.
  Core CLI foreign-client waiting is implemented; model-facing exclusive jobs
  and service restart remain disabled.
- Editor service specs set `adapter_enforces_leases=false`. Raw MCP mutation
  remains blocked without an adapter enforcing owner/generation on every call
  and restoring owner-specific PIE/cvars/camera/HUD state.
- Configured project visual/assets adapters are dispatched as literal argv;
  their scene/fixture/asset policy belongs to the project. Dispatch is not a
  bypass of editor ownership or permission checks.

## Before a default cutover

Validate in a private tree, with build/editor owners:

1. All launch paths use one canonical authority: builds, editor, imports,
   commandlets, automation and captures. Drain the old lane before switching.
2. Preserve or explicitly retire wrapper options, exit/log/status/recover,
   queue timeout, abandon, hooks, completed-result reuse and no-join behavior.
   Pending coalescing is not completed-result reuse or running-job joining.
3. Prove UBT startup collision handling, private mappings/logs, RAM-derived
   parallelism, build flags/cache behavior, symbols and generated-output
   isolation. Share native DDC, not writable per-tree intermediates.
4. Exercise real editor foreign/same-owner leases, grace, descendant teardown,
   proxy 503/fencing, post-hook order, restart denial and automatic Healthy
   resume. Public status/recover must converge after slow readiness or crash;
   no manual internal resume command should be required.
5. Verify restoration, mutating MCP authorization, disk/memory admission,
   crash/stall recovery and owned teardown. Fake-service tests do not prove
   live Unreal parity.
6. Obtain owner approval and document rollback that drains Borg before
   restoring the old authority. No silent fallback between queues.

See [interfaces](interfaces.md), [services](services.md) and
[regression gates](benchmark.md). This checklist grants no rollout approval.
