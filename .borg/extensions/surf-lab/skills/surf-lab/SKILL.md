---
name: surf-lab
description: "Use Borg's persistent runtime to run the actual headless surf world, train a replay-derived controller probe, and compare traces."
---

# surf-lab

Use this extension through the session's persistent Python runtime. It is a
stateful deterministic environment, not a screenshot or GUI driver.

```python
import json

env = borg.environment("surf-lab", "lab")
started_response = env.start({"profile": "reference", "map": "/absolute/path/to/surf_utopia_njv.bsp"})
started = json.loads(started_response["content"][0]["text"])
session_id = started["session_id"]
```

Environment calls preserve the standard MCP response envelope; Surf Lab puts
its structured JSON payload in the first text content block.

The environment exposes `start`, `step`, `observe`, `trace`, `log`, `branch`,
`compare`, `policy.train`, `policy.inspect`, `policy.compare`, `policy.evolve`,
`map.generate`, `map.inspect`, `map.preview`, `map.route`, `map.export`, and
`config`. Keep the returned session id in the persistent
namespace. Use `step` with bounded batches, save the observations, branch at a
specific tick, apply a counterfactual command to the branch, and compare the
two traces to identify the first divergent tick and position error. `log`
returns recorder metadata and a bounded window from the same persistent log as
`trace`.

The lab launcher uses an already-built `surf_lab` executable (release first,
then debug) so MCP startup does not block on a Cargo build. Set
`SURF_LAB_BIN` to override the executable path or `SURF_LAB_ROOT` to override
the sibling Surf checkout. The lab runs a renderer-free Bevy/Avian world using
the game's course setup, static collision representation, player hull,
fixed-step controller, and spatial queries. With `map` set to a Source BSP
path, it uses the same BSP loader and imported collision geometry as the game;
without one it uses the game's generated course. The process is pinned to one
profile and map, so start a new environment to change either.

For native Source-unit calibration, use the same BSP path that CS:S loads. The
importer includes Source displacement grids and oriented surf-ramp faces in the
headless collision world, in addition to BSP brush and packed-prop collision.

MCP tool names containing punctuation are exposed to the runtime with
runtime-safe underscores (for example, `map.generate` appears in `tools()` as
`map_generate`), while `env.call("map.generate", ...)` remains accepted.

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

## Authentic surf-controller probes

Acquire a public KSF `.rec` from the same map and keep the replay file outside
the repository. KSF replay frames contain the reference buttons, view angles,
positions, and velocities; they are the authority for the comparison, not the
learned model. For example, use the installed Utopia/NJV BSP and a downloaded
`surf_utopia_njv` world-record replay:

```python
trained = env.call("policy.train", {
    "replay": "/tmp/utopia-njv-wr.rec",
    "output": "/tmp/utopia-njv-policy.json",
    "epochs": 240,
    "hidden": 32,
    "seed": 42,
})

comparison = env.call("policy.compare", {
    "replay": "/tmp/utopia-njv-wr.rec",
    "policy": "/tmp/utopia-njv-policy.json",
    "map": "/home/user/.local/share/surf/maps/bsp/surf_utopia_njv.bsp",
    "profile": "reference",
    "ticks": 3880,
})
```

`policy.compare` runs two independent trajectories on the same BSP: exact KSF
commands establish the physics/reference error, while the frozen policy
establishes controller-plus-physics error. Read the action-agreement metrics
before attributing a learned-policy failure to collision or movement physics.
`policy.evolve` performs a bounded deterministic yaw-perturbation search around
the frozen policy and returns a trajectory certificate; it never changes policy
weights or the physics authority. Use `policy.inspect` to validate a saved
artifact before executing it.
