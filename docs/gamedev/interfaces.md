# Game-development coordination interfaces

The source of truth for serialized fields is `crates/borg-lanes/src/lanes.rs`,
`services.rs`, `workspace.rs` and `adapter.rs`. CLI bindings are in
`crates/borg-cli/src/lane_commands.rs`, `lane_service_commands.rs` and
`worktree_commands.rs`; restricted MCP bindings are in
`crates/borg-agent-runtime/src/lane_tools.rs` and `subagents.rs`.

## Resource and job contract

`ResourceScope` is Host, Project(path) or Worktree(path). Project/Worktree
paths must be existing, absolute and canonical at store ingress. The capacity
CLI canonicalizes its path. `ResourceRequest` combines a key with Exclusive
or Shared slots. Multi-key admission is atomic; shared capacities and
conflicting-ticket FIFO are enforced under the lane journal lock.

`Holder` records participant and session. `LeaseRequest` contains resources,
holder and optional queue timeout. `JobSpec` carries the lease, fingerprint,
argv, cwd, env, admission budget, memory limit, timeout, stall timeout,
coalescing flag and pre/post hooks. Additive fields include foreign-client
grace and per-resource overrides, abandonment timeout, unit prefix, swap
limit and detached finish hook. See [lanes](lanes.md) for lifecycle semantics.

Only pending jobs with compatible full specs coalesce. Running jobs are not
joined, even if a caller supplies the same fingerprint. Adapters must include
source/toolchain/flags/output policy in fingerprints. Coalescing is not
completed-result reuse. Hooks cannot authorize a grant outside the resource
transaction or disguise workload exit status.

State defaults to `$XDG_RUNTIME_DIR/borg/lanes`, falling back to
`$TMPDIR/borg-lanes-<uid>` (the platform temporary directory). `BORG_LANE_DIR`,
then `BORG_LANES_ROOT`, override it; CLI `--state-dir` overrides the default.
The store uses `state.json`, stable `state.lock`, job lock files and per-job
directories. Services live under `services/ID`. Do not unlink live lock files,
move a live root between hosts, or assume ephemeral state is archived to
XDG_STATE_HOME automatically. Host RAM admission additionally uses the
shared host-admission directory; changing it isolates those reservations.

## CLI

```text
borg lane [--state-dir DIR] --json job submit --spec FILE_OR_DASH
borg lane --json job wait ID [--until started] [--timeout SECONDS]
borg lane --json job status [ID]
borg lane --json job logs ID
borg lane --json job cancel ID
borg lane --json job recover [--dry-run | --wait SECONDS]
borg lane --json resource list|status|set-capacity ...
borg lane --json service start|status|lease|release|restart|yield|resume|stop|logs ...
borg worktree --project ROOT ...
```

`borg lane recover` aliases job recovery. Submission accepts serialized
`JobSpec`, not `--adapter/--template` shorthand. It returns immediately;
terminal `wait` propagates job outcome (cancel 125), while timeout returns
124 without cancelling. There is no public `borg lane lease` subcommand or
`borg lane workspace` command: workspace operations use `borg worktree`.

## Model-facing tools

- `lane_job`: submit, wait, status, list, cancel. Submit selects a human-curated
  adapter/template JSON policy (`BORG_LANE_TEMPLATES`, otherwise the Borg
  config lane-templates directory); it never takes arbitrary model argv,
  paths or resource declarations. Exclusive templates are rejected.
  `watch=true` requests exit notification; bounded wait is also supported.
- `lane_service`: status, lease, release, read for registered services.
  Identity is derived from the actor session, never a supplied owner/fence.
  Read requires a current lease, exact audited path, loopback GET, no redirects
  and bounded output. Start/stop/yield/resume/restart and raw MCP writes are
  not model tools.
- `lane_workspace`: scoped worktree/budget/freeze helpers when enabled by the
  runtime. Model GC is dry-run; a model-supplied confirmation is not approval.

A job owner can inspect and cancel its job; a coalesced submitter can inspect
or wait but cannot cancel another holder's ticket. Co-submit authorization is
in memory: after runtime restart a non-owner joiner must resubmit or use the
trusted CLI. It fails closed. Other holders' identities are redacted in model
responses; operator CLI output retains full identities.

For long waits prefer `watch=true` or a Borg command watch running
`borg lane job wait ID`. Use `await_watchers` only when no work is actionable;
use `wait_agent` for other agents, not artifact polling.

## Services, workspace and adapters

See [services](services.md) for client modes, health, restart/yield barriers,
restore failures and proxy fencing. Foreign-client grant/renew and Preparing
checks share the lane lock; this is implemented, not a proposed bridge
snapshot check. Shared clients are opt-in and independently restored.

See [workspace hygiene](workspace-hygiene.md) for budgets and destructive-action
protection. Freeze records are a handshake, not an automatic exclusive lane.
Workspace shared-work events remain the durable claim/dependency authority.

`AdapterManifest`, `JobTemplate` and `JobKind` describe engine policy, but Blu
v1 does not automatically interpret `[api.lanes]`. Packages use validated
workflows/data. Native and Unreal adapters supply engine commands, private
output paths, canonical keys, cache policy and health/hooks. They cannot
raise permissions. VCS-native asset locks remain the edit authority.
