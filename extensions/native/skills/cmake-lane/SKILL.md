---
name: cmake-lane
description: "Configure, build and ctest in a CMake worktree with bounded -j, private build dirs and test labels."
---

# CMake/ctest native lane

Use your own Abundance worktree (never edit or build the shared main checkout).
From the worktree root, call the `native` Blu workflow through
`/ext:native:run` (or the `ext__native__run` tool with a fresh `request_id`):

```text
/ext:native:run cmake configure
/ext:native:run cmake build --target worldgen-tests
/ext:native:run ctest test --exclude-label slow
/ext:native:run ctest test --label slow
/ext:native:run ctest test --regex worldgen
/ext:native:run ctest test            # full gate
```

The adapter selects a worktree-private `build/`, Release (for native C++), and
`-j` from RAM (8 GiB reserve plus 2 GiB fixed overhead, 6-process cap).
`BORG_NATIVE_MAX_JOBS=2` can lower parallelism and its lane reservation on a
busy host, never the 8 GiB admission floor. Override `--build-dir` only with
a path **inside** that worktree; others are refused. Pass raw CMake/ctest flags
after `--`; `--dry-run` prints the JobSpec without submitting.
`-LE slow` is safe even if the checkout predates label registration; inspect
`ctest --print-labels` before assuming any tests were excluded (our 306-test
Abundance checkout currently has no labels). CTest `--output-on-failure`
preserves evidence. Core resource keys are worktree-private build output and
host CPU/RAM; use test fixture leases when tests mutate external state. Never
run Unreal via this adapter.

Each call submits one engine-neutral Borg lane job and returns its ID; await
it with `borg lane job wait ID --json`. There is no fallback that runs CMake
or ctest outside the lane.

Disk admission keeps a 60 GiB free-space floor **plus** planned growth
(default 24 GiB for Cargo, 6 GiB for CMake/ctest) and other active reservations.
For a cached job with a justified smaller growth estimate, pass
`--reserve-disk-gib 4` on that invocation (integer 1..1024); this never lowers
the floor or changes defaults. Do not use cached-growth estimates for cold
builds. A queued job's reason is in `borg lane job status ID --json`;
`borg lane job wait ID --timeout 60 --json` returns that state/reason on timeout
(exit 124) without cancelling the job.
