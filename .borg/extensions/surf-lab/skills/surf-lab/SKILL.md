---
name: surf-lab
description: "Use Borg's persistent runtime to run deterministic surf trajectories, branch counterfactual inputs, and compare traces."
---

# surf-lab

Use this extension through the session's persistent Python runtime. It is a
stateful deterministic environment, not a screenshot or GUI driver.

```python
env = borg.environment("surf-lab", "lab")
started = env.start({"profile": "source"})
session_id = started["session_id"]
```

The environment exposes `start`, `step`, `observe`, `trace`, `branch`,
`compare`, and `config`. Keep the returned session id in the persistent
namespace. Use `step` with bounded batches, save the observations, branch at a
specific tick, apply a counterfactual command to the branch, and compare the
two traces to identify the first divergent tick and position error.

The lab reuses the game's Source-style acceleration, air acceleration, friction,
gravity, and multi-plane slide functions. It is intentionally a deterministic
diagnostic environment; the generated analytic ramp planes are not a claim
that a run has been validated against every imported BSP surface. Use trace
evidence to decide what to inspect in the live game or replay comparator.
