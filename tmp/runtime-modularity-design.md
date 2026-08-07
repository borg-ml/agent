# Borg Modular Runtime Design

Status: design discussion capture with an implemented first vertical slice; not a final ADR
Date: 2026-08-06
Repository: `/home/shulgin/borg-cli`

## First vertical slice (2026-08-07)

The first model-facing slice now lives in `crates/borg-remote` without adding
Python dependencies to the Borg binary. `persistent_runtime.rs` supervises one
plain CPython worker per session, using a framed JSON-lines protocol and a
language-neutral `RuntimeHost` bridge. `AgentToolDispatcher` exposes it as the
provider-neutral `runtime_exec` tool, so native turns and the local MCP server
share the same in-memory namespace. The worker supports persistent globals,
final-expression results, top-level await, captured output, and bounded host
calls; it is deliberately not `ipykernel` yet. Blu package hot reload remains a
next-turn catalog operation and is independent of the worker namespace.

The second slice adds a shared lossless-history contract rather than placing
memory inside any language kernel. The SQLite event/payload journal remains
canonical; an atomically maintained, rebuildable FTS5 projection provides
fast discovery. `query_history` is shared by every provider lane and
`borg.history(...)` exposes the same operation in Python. Exact ids, typed
filters, sequence ranges, bounded regex (with optional FTS prefilter), and
bounded payload expansion all resolve back to canonical events. A
sequence-cursored `SessionHistoryIndexDocument` feed gives BorgSearch/Vespa a
stable semantic-index adapter without making a vector store authoritative.
Blu and future Bun/IPython adapters must call this host contract rather than
implementing private memory stores.

The surf project exercises this boundary through the project-local
`.borg/skills/surf-calibration/SKILL.md` skill. Its reference contract is a
deterministic per-tick state/input trace; a Source `.dem` is retained as an
optional end-to-end check rather than the tuning oracle. Kernel state is not
durable across worker loss, so the skill persists traces and metric summaries
as explicit artifacts.

## Executive summary

Borg should be able to execute workflows and, eventually, the model-facing
control environment through more than one language runtime. Blu should remain
the lightweight embedded baseline, while Python/IPython and JavaScript/
TypeScript remain selectable alternatives.

The recommended shape is:

```text
Borg CLI monorepo
├── crates/borg-runtime/
│   └── language-neutral runtime contracts and worker protocol
├── crates/borg-runtime-blu/
│   └── Blu adapter
├── crates/borg-runtime-ipy/
│   └── CPython/IPython worker adapter
├── crates/borg-remote/
│   └── generic durable runner, host bridge, policy, journaling
├── crates/borg-cli/
│   └── CLI, extension catalog, configuration, packaging
└── runtime-workers/
    └── optional Python/IPython or other worker sources and assets
```

This does **not** mean that every runtime should be statically linked into the
default Borg binary. The contract and Blu can be built in; heavier runtimes
can be installed, discovered, or bundled as supervised workers.

The Blu language engine itself should remain in its standalone repository. The
Borg-specific adapter and durable host integration belong in this monorepo.

## 1. What is being made modular?

There are four related but different concepts:

1. **Language engine** — for example the Blu VM, CPython, RustPython,
   JavaScriptCore, QuickJS, Boa, or Bun.
2. **Runtime adapter** — the code that loads source/artifacts, invokes the
   engine, converts values, exposes host calls, and interrupts execution.
3. **Execution placement** — embedded in the Borg process, a supervised local
   worker, or eventually a remote worker.
4. **Agent control runtime** — the environment in which the model performs its
   normal programmatic work. This is not necessarily the same thing as an
   extension/workflow runtime.

The design should not force these axes into one global `runtime` setting.

## 2. Current Borg coupling

The current implementation already has a useful extraction seam:

- `crates/borg-remote/src/blu_workflow.rs::BluWorkflowRunner` owns workflow
  admission, leases, cancellation, durable completion, and replay.
- `execute_blu_source` owns Blu-specific VM construction, compiler limits,
  native function registration, interruption, and `BluValue` conversion.
- `HostBridge` mixes generic Borg policy/journaling with Blu-specific native
  function calling.
- `BluWorkflowDefinition`, `BluWorkflowRequest`, and
  `SessionEventKind::BluWorkflow*` encode Blu into the public durable model.
- Extension discovery requires `.blu` workflow entrypoints.
- Model tools are named `list_blu_workflows`, `run_blu_extension`, and
  `run_blu_workflow`.

The first refactor should extract a generic runner/host boundary around the
existing Blu code. It should not begin by rewriting the VM or adding several
new languages.

## 3. Dependency and repository boundaries

The Borg CLI monorepo is the right place for all Borg runtime integration at
this stage. It gives the host, adapters, worker protocol, manifests, events,
tests, and release packaging one version matrix.

The dependency direction should be:

```text
borg-runtime-blu ───> borg-runtime <─── borg-remote
borg-runtime-ipy ───> borg-runtime

borg-cli ───────────────────────────────> borg-remote
```

`borg-runtime` should not depend on SQLite, `borg-remote`, provider code, or
the CLI. It should contain only stable contract types, runtime metadata,
artifact identity, worker messages, and conformance fixtures.

`borg-remote` should remain the authority for session identity, permissions,
approvals, host effects, leases, durable journals, cancellation, and process
supervision.

The runtime registry/composition layer may depend on the adapter crates, but
the adapters must never depend on `borg-remote`. That keeps the language
implementations reusable and prevents a dependency cycle.

Initially, these can be modules under `borg-remote`. Extracting separate Cargo
crates should wait until the interfaces are exercised by at least Blu and one
external worker.

## 4. Generic runtime contract

The exact Rust trait can evolve, but the conceptual boundary is:

```rust
trait WorkflowRuntime {
    fn identity(&self) -> RuntimeIdentity;
    fn admit(&self, artifact: RuntimeArtifact) -> Result<PreparedArtifact>;
    fn execute(
        &self,
        artifact: PreparedArtifact,
        context: RuntimeContext,
        host: WorkflowHost,
        cancel: CancellationToken,
    ) -> Result<RuntimeResult>;
}
```

The adapter owns language semantics. The generic runner owns effects and
durability.

### Adapter responsibilities

- parse, compile, or load the guest source/artifact;
- apply language-specific limits;
- expose host functions or worker calls;
- map values and exceptions to the common result model;
- report stdout, stderr, structured output, and streaming values;
- interrupt and shut down the guest.

### Borg responsibilities

- capability and permission checks;
- host-call journaling and idempotency;
- SQLite/session state and workflow leases;
- cancellation propagation;
- process lifecycle and crash recovery;
- provider, filesystem, process, tool, autonomy, and checkpoint access;
- audit and event presentation.

The guest must not receive raw SQLite, filesystem, provider, or process
handles. The initial host-call representation may remain JSON because the
current Blu workflow journal already uses bounded JSON requests and results.
A later canonical typed interface can optimize embedded calls without
changing the worker protocol or durable semantics.

## 5. Durable identity and replay

Current Blu events use a source hash. A multi-runtime event must pin more:

```text
workflow_id
runtime_id
runtime_version / engine build
artifact_hash
dependency_lock_hash (when applicable)
placement / isolation mode
state_scope
```

Reusing a workflow ID with a different runtime, source, compiled artifact, or
dependency environment must fail rather than silently replay under different
semantics.

The generic event names should eventually be:

```text
WorkflowStarted
WorkflowCallRequested
WorkflowCallCompleted
WorkflowOutput
WorkflowCompleted
```

Existing `BluWorkflow*` events should remain readable for old SQLite journals,
or be translated at a compatibility boundary. A rename must not make old
workflow history unrecoverable.

## 6. Candidate runtime profiles

There is no single engine that is simultaneously tiny, fully compatible with
the Python/JavaScript ecosystems, embeddable, and easy to distribute. The
best design is to offer lightweight and full-ecosystem profiles explicitly.

| Profile | Candidate | Placement | Assessment |
| --- | --- | --- | --- |
| `blu` | `blu-lang` | Embedded | Current lightweight, bounded workflow baseline. |
| `py` | RustPython | Optional embedded | Actual Python interpreter written in Rust; useful for pure Python, but incomplete ecosystem and not a realistic IPython target. |
| `ipy` | CPython + IPython/ipykernel | Supervised worker | Best compatibility and persistent Python environment; heavier, but installable on demand. |
| `js` | Boa | Optional embedded | Rust-native JavaScript engine; good for small scripts, but experimental and not Node-compatible. |
| `js` | QuickJS-NG via `rquickjs` | Optional embedded | Lightweight, low startup overhead, relatively mature bindings; JavaScript rather than full web/Node runtime. |
| `ts` | Bun | Supervised worker | Full TypeScript and broad JavaScript tooling; Rust/JSC-based, but still a large all-in-one runtime. |
| `ts` | Deno | Supervised worker | Another full JS/TS runtime; useful if Deno permissions and APIs are preferred. |

### Bun

The current Bun upstream README describes Bun as written in Rust, powered by
JavaScriptCore, and supporting TypeScript directly. It remains an all-in-one
runtime, package manager, bundler, and Node-compatible toolkit, so its Rust
implementation should not be confused with a small embeddable crate.

Treat Bun as an optional worker executable first. This keeps Bun, JavaScript-
Core, its compatibility layer, and its package ecosystem out of the default
Borg link and makes upgrades independent of the Borg host.

### Python and IPython

RustPython is attractive for a small `py` profile, but RustPython's own project
currently describes it as development-phase with incomplete standard-library
support. It should not be used as a drop-in substitute for IPython, NumPy,
pandas, or arbitrary native Python packages.

PyO3 embeds the normal CPython interpreter and is the practical route to full
Python compatibility. However, directly linking CPython into Borg increases
binary and distribution complexity. A supervised CPython/ipykernel worker is
the better default for `ipy`.

Rust crates such as `jupyter_protocol` can represent and transport Jupyter
messages, but they are protocol/client libraries, not a Rust implementation of
IPython. The kernel should remain actual IPython/CPython.

## 7. Lightweight distribution strategy

Use a two-tier runtime policy:

```text
Default Borg binary:
  generic runtime protocol + Blu

Optional embedded features:
  RustPython (`py`)
  Boa or QuickJS (`js`)

Optional managed workers:
  CPython/IPython (`ipy`)
  Bun (`ts`)
  Deno (`ts` alternative)
```

Cargo features can prevent optional embedded engines from entering builds that
do not need them. Managed workers can be:

- discovered on `PATH`;
- installed into a Borg-managed runtime cache;
- bundled only in a distribution that opts into them; or
- supplied by a project/user runtime package.

Because Borg normally runs with full access, a worker is primarily a lifecycle
and dependency boundary, not a security boundary. It still provides useful
restart, crash, version, and environment isolation. The host bridge remains
important for durable effects and auditability even when the guest is trusted.

## 8. Agent control runtime versus workflow runtime

Prime Agent is a useful reference for an IPython control environment: its
architecture uses a persistent Python environment, separate worker/kernel
processes, typed host requests, and durable session state. Its documentation
also makes clear that those processes normally share the client's OS
permissions and are not a security sandbox.

That is not identical to Borg's current `run_blu_extension` workflow path.
Therefore configuration should separate the two choices:

```toml
[agent]
control_runtime = "native" # or "ipy"

[workflows]
default_runtime = "blu"
```

Individual workflow definitions can override the default:

```toml
[workflows.analysis]
runtime = "ipy"
entrypoint = "workflows/analysis.py"
state_scope = "session"
```

IPython's persistent state requires an explicit `state_scope` such as:

- `fresh` — new interpreter state per invocation;
- `session` — persistent kernel state;
- `checkpointed` — persistent state only through an explicit checkpoint model.

Durable replay and idempotent retry should be strongest for `fresh` workflows.
Session-state workflows need explicit recovery semantics and must not pretend
that re-running source recreates arbitrary Python heap state.

## 9. Host worker protocol

The worker protocol should be small, versioned, and language-neutral. A framed
stdio transport is enough initially:

```text
start { runtime, artifact, context, limits }
host_call { call_id, operation, arguments }
host_result { call_id, result | error }
output { stream, value }
completed { result }
failed { error }
cancel { reason }
shutdown
```

Required semantics:

- stable call IDs;
- bounded message sizes;
- backpressure for output and host calls;
- cancellation acknowledgement;
- explicit worker crash/timeout status;
- runtime and artifact identity in the start handshake;
- no unversioned assumptions about environment variables or current working
  directory.

Embedded adapters can use the same logical host contract without paying the
serialization cost internally.

## 10. Manifest and tool model

Workflow declarations should select a runtime explicitly while defaulting to
Blu for compatibility:

```toml
[workflows.review]
runtime = "blu"
entrypoint = "workflows/review.blu"
description = "Review the current change"
state_scope = "fresh"
```

The runtime registry, not a filename extension alone, validates the entrypoint
and reports unsupported runtimes during extension discovery.

Generic tools should eventually be added:

```text
list_workflows
run_workflow
```

Keep `list_blu_workflows`, `run_blu_extension`, and `run_blu_workflow` as
compatibility aliases while existing extensions and sessions migrate.

Useful CLI surfaces:

```text
borg runtime list
borg runtime info <id>
borg runtime install <id>
borg runtime doctor
borg runtime use <id>
```

`runtime use` should be scoped and explicit: agent control runtime, workflow
default, or project/user configuration. It should not mutate an in-flight
workflow.

## 11. Recommended implementation order

### Phase 0 — contract tests and ADR

Define the host operations and run the same fixtures against the current Blu
implementation. Include success, structured errors, cancellation, output
limits, host-call replay, permission behavior, and worker crash handling.

### Phase 1 — genericize Blu without changing behavior

- Introduce generic workflow names internally.
- Extract the durable runner/host interface.
- Put the current source executor behind `BluRuntimeAdapter`.
- Add runtime identity and artifact identity to new events.
- Preserve old Blu event/tool compatibility.

### Phase 2 — stabilize the monorepo crate boundary

Extract `borg-runtime` and `borg-runtime-blu` only after the adapter contract
is exercised. Keep the generic crate free of Borg session/database types.

### Phase 3 — add IPython

Implement `borg-runtime-ipy` as a supervised CPython/ipykernel worker. Use a
small Python-side host module for preferred Borg operations, while retaining
full-access escape hatches where the selected policy permits them.

### Phase 4 — add lightweight alternatives

Evaluate RustPython for `py` and Boa/QuickJS for `js` against the conformance
fixtures. Do not advertise them as IPython, Node, or Bun compatibility.

### Phase 5 — add full TypeScript runtime

Add Bun as an optional worker when there is a concrete TypeScript workflow
need. Prefer the Bun executable and a Borg adapter over linking Bun internals.

### Phase 6 — optionally make IPython the agent control runtime

Only after workflow execution is stable should Borg consider making IPython a
first-class model-facing control environment. This is a larger session-loop
change than adding an alternate workflow engine.

## 12. Main risks

1. **Stateful kernels:** source replay does not reconstruct arbitrary Python or
   JavaScript state.
2. **Environment identity:** interpreter versions, package locks, native
   libraries, and transpilers affect results.
3. **Bloat:** optional engines must not become mandatory dependencies of the
   default binary.
4. **Protocol drift:** worker messages need explicit versions even inside one
   monorepo.
5. **Tool semantics:** direct shell/file access can bypass durable Borg host
   journaling unless the runtime exposes preferred host APIs.
6. **Cancellation:** Python and JavaScript workers may require process-level
   interruption rather than cooperative VM interruption.
7. **Packaging:** a full IPython environment is an environment distribution
   problem, not just a Cargo dependency.
8. **Security language:** full access should be described as trusted execution;
   a worker process must not be marketed as a sandbox.

## 13. Current recommendation

The best practical combination is:

```text
Blu       embedded default for small bounded workflows
IPython   optional CPython worker for full Python and Prime-like control
Bun       optional worker for full TypeScript/JavaScript
RustPython/Boa/QuickJS optional lightweight profiles, after conformance work
```

Do not choose between “one huge universal runtime” and “many unrelated
plugins.” Use one small Borg host contract, a monorepo-owned registry and
conformance suite, and a mix of embedded and worker-backed adapters selected by
the workload.

## References checked

- [Blu repository](https://github.com/borg-ml/blu)
- [Prime Agent README](https://github.com/PrimeIntellect-ai/prime-agent)
- [Prime Agent architecture](https://github.com/PrimeIntellect-ai/prime-agent/blob/main/packages/coding-agent/docs/architecture.md)
- [Prime Agent RLM/IPython model](https://github.com/PrimeIntellect-ai/prime-agent/blob/main/packages/coding-agent/docs/rlm.md)
- [Bun README](https://raw.githubusercontent.com/oven-sh/bun/main/README.md)
- [PyO3](https://github.com/PyO3/pyo3)
- [RustPython](https://rustpython.github.io/)
- [Boa](https://github.com/boa-dev/boa)
- [rquickjs](https://github.com/DelSkayn/rquickjs)
- [Deno](https://github.com/denoland/deno)
- [`deno_core` documentation](https://docs.rs/deno_core/latest/deno_core/)
- [`jupyter_protocol` documentation](https://docs.rs/jupyter-protocol/latest/jupyter_protocol/)
- [Jupyter kernel authoring and protocol documentation](https://jupyter-client.readthedocs.io/en/stable/kernels.html)
