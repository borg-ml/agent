---
name: cmake-lane
description: "Configure, build and ctest in a CMake worktree with bounded -j, private build dirs and test labels."
---

# CMake/ctest native lane

Use your own Abundance worktree (never edit or build the shared main checkout).
Run from the worktree root:

```sh
python3 .borg/extensions/native/native.py cmake configure
python3 .borg/extensions/native/native.py cmake build --target worldgen-tests
python3 .borg/extensions/native/native.py ctest test --exclude-label slow
python3 .borg/extensions/native/native.py ctest test --label slow
python3 .borg/extensions/native/native.py ctest test --regex worldgen
python3 .borg/extensions/native/native.py ctest test # full gate
```

The adapter selects a worktree-private `build/`, Release (for native C++), and
`-j` from RAM (8 GiB reserve plus 2 GiB fixed overhead, 6-process cap).
`BORG_NATIVE_MAX_JOBS=2` can lower parallelism and its lane reservation on a
busy host, never the 8 GiB admission floor. Override `--build-dir` only with
a path **inside** that worktree. Pass raw CMake/ctest flags after `--`.
`cmake_configure`, `cmake_build`, `ctest_fast`, `ctest_all` workflows are also
registered as `/ext:native:<command>` in a project with the Blu
package installed. `-LE slow` is safe even if the checkout predates label
registration; inspect `ctest --print-labels` before assuming any tests were
excluded (our 306-test Abundance checkout currently has no labels). CTest `--output-on-failure` preserves evidence. Core resource keys
are worktree-private build output and host CPU/RAM; use test fixture leases
when tests mutate external state. Never run Unreal via this adapter.

Normal execution requires the engine-neutral Borg job lane; no silent fallback.
Explicit `--probe-direct` is an **uncoordinated** smoke/benchmark only.
