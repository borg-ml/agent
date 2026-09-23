# Workspace hygiene (local engine-neutral layer)

Core: `borg-lanes::workspace::hygiene`. CLI: `borg worktree`. MCP:
`lane_workspace` (when shared work and multiplayer are enabled). Existing
`create_shared_work`, `claim_shared_work`, `declare_work_dependency`, and Borg
team messages remain the durable work/communication authority; the local freeze
record in the Git common directory is only a handshake.

## Worktrees and budgets

- `borg worktree --project . new task-name [--shared-cargo] [--root DIR]`
  creates `DIR/task-name-<session-prefix>`, branch
  `agent/task-name-<session-prefix>`, and an owner marker under the new tree's
  Git administrative directory. Default root is the sibling `borg-wt`;
  `BORG_WORKTREE_ROOT` overrides it. A CLI without a session can create a tree
  with a generated owner UUID, but GC cannot prove that synthetic owner exited.
- `list` reports owning session when known, branch, merged state, dirtiness,
  directory modification time and `du -skx` apparent allocated disk usage.
  Directory mtime is an approximate activity signal, not a reliable last-edit
  timestamp. A process whose cwd is elsewhere can edit this tree; absence of
  a matching instance is **not** proof of abandonment.
- `gc` is dry-run by default and lists all checked-out trees with protection
  reasons and size. `--apply` requires a journal-confirmed exited owner;
  `--force` only relaxes the dirty-tree check, never a live owner, unknown
  owner, recent unmerged branch, or primary tree. Unmerged branches with no
  worktree/index/commit activity for 30 days can be proposed as abandoned; Git
  branches are preserved even when a checked-out worktree is removed. MCP GC is always dry-run; direct CLI `--apply` requires a real terminal
  and typing the exact path for every deletion, including `--force`. It does not delete assets or Git branches.
- `borg worktree budget` checks `statvfs` on the output filesystem and Linux
  `MemAvailable`. Default safety reserves: **60 GiB free disk**, **8 GiB
  MemAvailable**, **32 GiB per agent on disk**, **16 GiB per agent RAM**.
  Admission subtracts projected outputs and current reservations; `create`
  counts other Borg-owned worktrees toward the same owner's disk cap. Lanes
  call `assess_budget(&AdmissionBudget, reserved_ram, reserved_disk)` at
  dispatch and queue with its explicit reason; this is an API and **not** a
  substitute for an actual lane supervisor maintaining cross-process
  reservations.
- A session can create a Borg command watch over `borg worktree --project P
  monitor --interval-secs 60` with `notify_on=match` and pattern
  `workspace pressure`. The timer emits only on pressure; it is not an
  agent-side sleep/poll loop. MCP `lane_workspace {"op":"budget",...}`
  broadcasts an actionable team warning when pressure is already present.
  A monitor must be started by a session or service; no always-on daemon is
  automatically installed.

Cache policy is deliberately conservative: Cargo's default `CARGO_HOME`
shares registry/downloads and its native locks; **per-worktree `target`**
remains private, as do CMake build dirs and Unreal `Binaries`/`Intermediate`.
Use installed Unreal engine and its existing shared DDC/Zen through the
adapter; do not copy it per tree. Sharing a writable Cargo target across
branches risks lock contention and stale outputs, and is not enabled. A
reflink copy can seed *immutable* artifacts on CoW filesystems but is not a
live shared writable cache. `sccache` would be worth measuring after it is
installed; it was not installed here. `borg worktree target-status --cap-gib 24` reports per-worktree target
usage and cap breaches (MCP op `target_status` is also read-only). Lanes should
queue further builds with the budget reason until an owner clears outputs.
Per-output target cleaning on a live
worktree is deliberately not automated: wait for all owner jobs to finish,
request the owner's confirmation, then `cargo clean` there. Whole-tree GC
only handles clean merged Borg-created trees with confirmed-dead owners.

## Freeze protocol

1. Create/claim a shared-work item; declare dependencies for blocked work.
2. `borg worktree --project P freeze-preview 'Source/**/*.h'` lists dirty
   matches across *all* Git worktrees, including paths with unknown owners.
3. Request a freeze with shared-work UUID, glob(s), rationale and timeout:
   CLI `freeze --work-id ID --owner SESSION --reason TEXT GLOB` or MCP
   `lane_workspace {op:"freeze",project:P,work_id:ID,globs:[...],reason:...}`.
   MCP verifies its caller owns the shared-work claim at request time and
   broadcasts a request to the team; the record lists the participating
   live sessions and an initial dirty-owner snapshot. The file is updated
   under a stable `flock`, atomic rename and fsync; no running locks are
   unlinked. This handshake does *not* magically lock Git or editors.
4. All affected sessions ack (`freeze-ack ID SESSION` / MCP `op:"ack"`).
   Acquire an **exclusive project/refactor lane** before landing; let existing
   jobs drain and inspect unsaved/unknown-owner edits. `freeze-land` requires
   all acknowledgements before deadline and zero protected dirty files,
   plus a nonempty "where did X move" note. Rebase/commit your own edits;
   never rewrite another owner's dirty tree.
5. `freeze-release` ends the local handshake; MCP `op:"unfreeze"` broadcasts the move note or
   explicitly aborts (MCP `op:"abort"`). The shared-work claim and team
   messages are the durable authority; local state is not a replacement for
   review or an exclusive resource lease. A timeout is not auto-ack or
   auto-rebase.

## Measured read-only demo (2026-09-23, this host)

Commands used a debug `borg` binary built from this branch. **No deletion or
Abundance edit was performed.** Initial `df -h /home`: ~218 GiB free;
`MemAvailable` varied 9–30 GiB with concurrent builds. `/home` is btrfs;
`cp --reflink=always` on a scratch file succeeded. `sccache` and `ccache`
were absent. `~/agent/target` was **42 GiB**, our worktree `target` **3.4 GiB**
during debug build. Sample Abundance per-worktree intermediates:
`build-lane/Intermediate` **1.4 GiB**, `header-split/Intermediate` **6.3 GiB**;
per-tree `Binaries` about **288–290 MiB**. `du` is logical on CoW/reflink
filesystems; real freed blocks may be lower.

- `borg worktree --project ~/agent gc` took **3.403 s**. No tree passed
  the ownership/merge/clean/exited-owner gate: **0 bytes safely reclaimable**.
- `borg worktree --project /home/shulgin/abundance gc` took **2.912 s**:
  likewise **0 bytes safely reclaimable**, because the observed worktrees are
  user/agent-created without a Borg owner marker or are dirty/live. The GC
  report lists protected paths and their sizes rather than offering to remove
  another worker's output. Do not interpret this as zero disk pressure.
- `borg worktree --project /home/shulgin/abundance freeze-preview
  'Source/**/*.h'` took **1.085 s** and found 3 dirty worktrees: main
  (6 matching headers, known live session), `header-split` (15), and
  `header-split-xform` (15). The two split worktree session IDs were unknown
  to local directory matching; the paths and files are still shown. Nothing
  was frozen or altered.

### Final CLI replay after concurrent agent updates

With the final debug binary, `borg worktree --project ~/agent gc` took
**3.482 s** and listed **14** checked-out trees, **0 eligible**, **0 B safely
reclaimable**. `borg worktree --project /home/shulgin/abundance gc` took
**2.742 s**, listed **10** trees, **0 eligible**. The report includes protection
reasons and sizes; the main Borg checkout was ~297 GiB total logical usage,
Abundance main ~101 GiB, `header-split` ~7.6 GiB. These figures are transient
and may count CoW shared extents. `borg worktree --project ~/agent
 target-status --cap-gib 24` took **1.082 s**, reported **14** targets,
**2 over cap** (`~/agent/target` ~42.8 GiB and `window-capture/target` ~39.2
GiB). `freeze-preview 'Source/**/*.h'` took **1.174 s** and now listed **2**
dirty worktrees (main: 6 matching headers, header-split-xform: 23): the
header-split tree changed while other agents worked. `gc --apply` from
non-interactive shell returned `GC deletion requires a human at a terminal...`
and made no modifications. The earlier three-owner snapshot is retained above
as a demonstration of why this report must be live, not cached.

## Limitations / integration decisions

The architect's v0 `WorkspaceCoordinator` records do not include an owner
exit proof, active job leases, freeze deadline/status, or bytes; richer local
reports remain in `workspace::hygiene` until shared wire fields are agreed.
Release is a *protocol acknowledgement* and does not automatically hold or
release an exclusive lane lease; adapters and the lane supervisor must wire
that gate before treating a freeze as an enforced maintenance window. The
MCP tool is scoped to the caller's repository and cannot manage arbitrary
other projects. `borg worktree` as a direct trusted user CLI accepts a
`--project` path for read-only cross-repository inspection.
