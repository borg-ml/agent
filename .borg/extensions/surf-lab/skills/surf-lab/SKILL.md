---
name: surf-lab
description: "Use Borg's persistent runtime to run the actual headless surf world, inspect lossless tick telemetry, branch counterfactual inputs, and compare traces."
---

# surf-lab

Use this extension through the session's persistent Python runtime. It is a
stateful deterministic environment, not a screenshot or GUI driver.

```python
env = borg.environment("surf-lab", "lab")
started = env.start({"profile": "source"})
session_id = started["session_id"]
```

The environment exposes `start`, `step`, `observe`, `trace`, `log`, `branch`,
`compare`, `map.generate`, `map.inspect`, `map.preview`, `map.route`, `map.export`, and `config`. Keep the returned session id in the persistent
namespace. Use `step` with bounded batches, save the observations, branch at a
specific tick, apply a counterfactual command to the branch, and compare the
two traces to identify the first divergent tick and position error. `log`
returns recorder metadata and a bounded window from the same persistent log as
`trace`.

The lab runs a renderer-free Bevy/Avian world using the game's course setup,
static collision representation, player hull, fixed-step controller, and
spatial queries. With `map` set to a Source BSP path, it uses the same BSP
loader and imported collision geometry as the game; without one it uses the
game's generated course. The process is pinned to one profile and map, so start
a new environment to change either.

Every simulated tick is appended to a per-episode JSONL log and flushed as the
episode advances. Responses intentionally expose bounded observation windows;
the model should use batches and query evidence after the run rather than try
to steer a 66/165 Hz loop interactively. The extension log is the high-rate
authority; Borg's canonical journal records the bounded MCP queries and their
results, not an unbounded copy of every tick.

For autonomous map work, use `map.generate` with a seed and bounded candidate
budget. It returns the winning versioned `MapSpec`, compiled patch/bounds
summary, measured route certificate, and conservative surfability/difficulty/
fun-proxy/robustness metrics. Robustness is the fraction of nominal and small
deterministic yaw-biased replays that retain the route's checkpoint progress.
Use `map.inspect` to validate a supplied `MapSpec` without
running a route, and `map.preview` for bounded sampled surface strips. Use
`map.route` to search a supplied spec in the compact fast
simulator. Use `map.export` to persist a validated spec, then pass its path to
`start` as `map_spec` to run the same geometry in Bevy/Avian. These are chunked policy searches; the LLM is not asked to steer
individual physics ticks. Treat a route certificate as best-known evidence,
not a proof of global optimality, and verify interesting candidates in the
actual Bevy/Avian session afterward.
