---
name: harvey-lab
description: "Run and evaluate Harvey LAB tasks with durable Borg receipts."
---

# harvey-lab

This extension is a thin Borg adapter around the official Harvey LAB
filesystem-first harness. It does not copy transcripts into Borg's database.
Harvey's result directory remains the benchmark authority; Borg stores the
run or evaluation summary, content-addressed artifact receipt, immutable
provenance, and an idempotent commit record.

Create `.borg/harvey-lab.json` in the project before a real run. The example
configuration beside this skill points at a local Harvey LAB checkout and the
official `harness.run` and `evaluation.run_eval` module entrypoints.

Use the `run_task` workflow to execute the configured task. It creates a
project-scoped state entry under `runs/<workflow-id>` and receipts for
`run.json` and `metrics.json`. A failed official command still produces a
failure receipt before the workflow reports failure; missing official metrics
are explicit fallback data, not an all-pass result. Use `grade_task` after a
successful run; it selects the configured `run_id`, or the latest successful
Borg-recorded run, invokes the official evaluator, and receipts `grade.json`
and `scores.json`.

The workflow UUID is the correlation and idempotency identity. Repeating the
same workflow UUID replays the existing workflow and storage result; a new
UUID represents a new benchmark attempt. Artifact verification must go through
Borg's `plugin_store` boundary so a changed local file is reported as invalid.

The adapter's process timeout is deliberately kept below Borg's outer workflow
timeout so a Harvey timeout is recorded by the adapter instead of being
mistaken for a lost process.
