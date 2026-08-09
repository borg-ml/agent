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
`compare`, `native.inspect`, `native.sweep`, `native.audit`, `replay.audit`,
`policy.train`, `policy.inspect`, `policy.compare`, `policy.track`,
`policy.correct`, `policy.evolve`,
`map.generate`, `map.inspect`, `map.preview`, `map.route`, `map.export`, and
`config`. Keep the returned session id in the persistent
namespace. Use `step` with bounded batches, save the observations, branch at a
specific tick, apply a counterfactual command to the branch, and compare the
two traces to identify the first divergent tick and position error. `log`
returns recorder metadata and a bounded window from the same persistent log as
`trace`.

The lab launcher uses an already-built `surf_lab` executable and refuses to
select one older than the Surf source tree, so MCP startup does not block on a
Cargo build or silently run stale physics. It selects the freshest release or
debug binary; set `SURF_LAB_BIN` to explicitly override this check, or set
`SURF_LAB_ROOT` to override the sibling Surf checkout. The lab runs a
renderer-free Bevy/Avian world using
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

Every simulated tick is appended to a lossless per-episode JSONL log. The log
flushes every 64 ticks by default; set `SURF_LAB_FLUSH_INTERVAL=1` when strict
per-tick crash durability is needed. Responses intentionally expose bounded observation windows;
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
The exact-reference result also reports the first divergent movement phase
(`after_acceleration`, `after_move`, or `final`) and source-BSP contacts include
raw brush and plane IDs, so a mismatch can be localized before changing the
physics authority. For physics-only work, `replay.audit` runs the exact KSF
command path without a policy artifact and returns effective movement
parameters, coordinate scale, trajectory metrics, and a durable trace path. Use
`diagnostics: "full"` for contact/phase evidence or `diagnostics: "compact"`
for throughput-sensitive sweeps. Evolution candidates automatically use the
compact state-only recorder because their score does not depend on contacts.
`policy.evolve` performs a bounded deterministic yaw-perturbation search around
the frozen policy and returns a trajectory certificate; it never changes policy
weights or the physics authority. Use `policy.inspect` to validate a saved
artifact before executing it.

Use `policy.track` as a bounded diagnostic when an open-loop schedule appears
to lose replay phase. It sweeps monotonic repeat/forward-skip windows and
velocity matching horizons in one loaded world, then verifies the best result.
It remains replay-guided state feedback, not an independently learned
controller. A failed sweep rules out simple clock alignment before spending a
large evolutionary population.

Use `policy.correct` only after fixed-clock and phase-tracking evidence. It runs
bounded receding-horizon yaw and temporary replay-phase candidates from the
actual controller state in a separate authoritative CPU probe world, applies
the best short correction, and reports both the extra simulated ticks and
strict route reward. Treat it as an MPC diagnostic and a source of
off-trajectory corrective examples, not as a trained policy. If it does not
materially improve strict terminal state, stop instead of widening the
candidate set blindly.

Set `beam_width` only for a bounded multi-block ablation. Beam mode retains
several authoritative states at each control block and commits the complete
selected horizon so receding-horizon replanning cannot defer a necessary
intervention. On the Utopia tick-808 checkpoint, a 16-state, 16-tick beam over
nine yaw/phase controls reduced terminal-state error from 18.63 to 13.51 but
still reset and did not beat the prior joint greedy result of 12.77. Treat that
as evidence against further beam widening on the same state and objective.

`policy.evolve` checkpoint completion is state-valid: the candidate must be
within both the terminal position and velocity bounds. The defaults are 1
world unit and 1 world unit/second, while the wider route corridor still scores
ordered progress. This prevents a curriculum stage from accepting a positional
near miss whose velocity cannot execute the demonstrated continuation. Use
`completion_radius` and `completion_velocity_radius` only for explicit bounded
sensitivity runs; report overrides with the result.

## Native POV calibration

Use `native.inspect` first to recover reset-safe supervision segments and
authentic packet-pose anchors from an old-engine Source POV `.dem`. Use
`native.sweep` for an exhaustive bounded comparison: it loads the demo and BSP
once, re-injects every valid consecutive packet anchor into one headless world,
and returns endpoint-error quantiles plus the largest outliers in one call.
This is substantially faster and produces one log instead of scripting one
`native.audit` request per anchor.

```python
sweep = env.call("native.sweep", {
    "demo": "/absolute/path/to/native-pov.dem",
    "map": "/absolute/path/to/surf_utopia_njv.bsp",
    "profile": "native",
    "command_tick_offset": -3,
    "start_velocity": "ballistic",
    "collision": "source",
    "max_velocity": 4000,
})
```

The ballistic estimator removes half the configured gravity impulse over the
next packet interval. It is an assumption-dependent lower bound, particularly
when a landing occurs exactly at a packet boundary. Rerun a reported outlier
with `native.audit`, its exact `start_tick`/`ticks`, and `diagnostics: "full"`
before treating it as a physics discrepancy. `max_velocity` is a diagnostic
override; do not silently promote it to a production profile without an
authoritative server cvar or a demonstrated lower bound.
