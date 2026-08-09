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
`policy.train`, `policy.train-native`, `policy.inspect`, `policy.compare`,
`policy.compare-native`, `policy.track`, `policy.correct`, `policy.evolve`,
`policy.evolve-native`, `map.generate`, `map.inspect`, `map.preview`,
`map.route`, `map.export`, and `config`. Keep the returned session id in the persistent
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
    "network_trace": "/absolute/path/to/decoded-packet-entities.jsonl",
    "profile": "native",
    "command_tick_offset": 0,
    "start_velocity": "ballistic",
    "collision": "source",
})
```

Decode each sparse `dem_usercmd` payload against a zero command. Omitted fields
do not inherit from the preceding payload. Preserve `tick_count`; when a
decoded PacketEntities trace is available, `policy.compare-native` advances
the server world clock and selects commands on that clock rather than using
the usually sparse demo-packet gap. The trace is authoritative for start
position, velocity, and ground state. Without it, the ballistic estimator
removes half the configured gravity impulse over the next packet interval and
remains assumption-dependent, particularly at a landing boundary. Rerun a
reported outlier with `native.audit`, its exact `start_tick`/`ticks`, and
`diagnostics: "full"` before treating it as a physics discrepancy.

On the captured Utopia demo, the calibrated alignment is offset `0`, native
ground acceleration is `5`, ground speed is `250`, movement commands are
normalized by `250`, and Source-authoritative collision uses zero additional
slide skin because the Source box sweep already applies its 1/32-unit epsilon.
Do not reuse the former `-3` alignment or a `4000` max-velocity diagnostic
override without fresh evidence from the target demo/map.

After calibrating `command_tick_offset` and diagnosing any collision-sensitive
outlier, use `policy.train-native` to train directly from the reset-safe native
POV transitions. It preserves independent forward and side magnitudes
(including full diagonal `[1, 1]` commands), never writes an action schedule,
and can omit only the explicitly diagnosed transitions. Use
`validation_segment_index` to hold out one complete reset-safe attempt; the
held-out examples do not enter gradient updates. Repeat this across the usable
attempts before fitting the all-attempt artifact.

```python
trained = env.call("policy.train-native", {
    "demo": "/absolute/path/to/native-pov.dem",
    "output": "/absolute/path/to/native-policy.json",
    "command_tick_offset": 0,
    "phase_features": True,
    "history_features": True,
    "hidden": 128,
    "epochs": 2000,
})
```

Successful native yaw-correction episode logs can be fed back through
`corrective_traces`. Each logged command is paired with the immediately
preceding visited state. Use `corrective_only: true` to keep only the authentic
initial example plus those corrected labels; otherwise nearly identical
authentic and corrected states can receive conflicting actions. Setting
`state_memory: true` installs the corrective-only labels as a nearest-state
feedback controller with no action schedule and no runtime future-state
lookup. This is a useful route-specific control bound, not a neural model or a
generalization result.

The POV stream itself has no authoritative grounded flag, so native policy
features hold it false even when the paired network trace can seed physics
ground state. Velocity in the training examples is the forward difference of
consecutive interpolated packet poses. Native view control is learned as a
delta from the preceding authentic UserCmd yaw, with previous yaw delta in the
history features. These assumptions and the split kind are returned with every
artifact. Do not call the interleaved training-sample metric a holdout.
Analog-magnitude commands are sparse; increase `analog_weight` only through a
bounded attempt-level holdout sweep and retain ordinary-movement and yaw
metrics alongside analog RMSE. Offline command agreement is controller
evidence, not closed-loop physics parity.

Use `policy.compare-native` for the paired validation. It runs exact UserCmd and
the frozen schedule-free policy in separate native BSP worlds, splitting at
every reset or explicitly excluded transition. It also re-injects every
consecutive packet anchor and reports policy-minus-exact endpoint error. Treat
that packet-interval comparison as the bounded physics authority: packet poses
are sparse and their interpolated in-between ticks are not native physical
states. A long rollout can expose accumulation and reset behavior, but cannot
identify controller error when the exact-command rollout fails first.

Set `native_tracking: true` only as a bounded replay-guided upper-bound probe.
It sweeps monotonic repeat/forward-skip windows against current native position
and velocity, reruns the best setting deterministically, and reports it under
`closed_loop.tracked_native_commands`. It still applies authentic UserCmds and
is not an independently learned controller. On Utopia, max advances through
64, velocity horizons through 16 ticks, and phase penalties 0.1, 1, and 10
produced no full-route completion. The ordinary sweep delayed the exact
open-loop reset from tick 2702 to 3007; a phase-heavy setting reached 3582 only
by lagging 1,095 command ticks and accumulating multi-thousand-unit route
error. Treat simple native phase alignment as exhausted there rather than
widening it again.

Set `native_yaw_correction: true` only for a full-episode bounded corrective
probe after phase tracking is exhausted. `policy.compare-native` evaluates a
small yaw-offset set around the authentic UserCmds in a separate authoritative
CPU world, retains zero yaw as a candidate, and reports the result under
`closed_loop.yaw_corrected_native_commands`. A reset counts as terminal only
when its pre-teleport position and velocity are both within the configured
authentic end-state radii and it occurs within the end slack. The defaults are
64 Source units, 64 Source units/second, and 2 ticks. Every earlier floor or
teleport reset remains a failure.

On Utopia, a 32-tick horizon with 8-tick control blocks and yaw candidates
`[-8,-4,-2,0,2,4,8]` completed at demo tick 4195. Its pre-teleport position
error was `22.52` Source units; 121 of 495 decisions used a nonzero yaw and the
search evaluated 110,404 candidate ticks. This proves a small bounded control
correction can carry the authoritative Surf Lab world through the full route.
It still uses the authentic commands and future position/velocity targets, so
treat it as corrective-label generation—not as schedule-free controller
parity. Distill the corrections and rerun without future-state access.

On the calibrated Utopia user demo, exact commands across 1,183 independent
packet intervals and 3,942 simulated server ticks produced mean endpoint error
`0.00377`, p99 `0.00249`, and max `1.486` Source units, with no reset or error
over 2 units. This is the bounded physics authority. The final schedule-free
v3 controller reached 97.62% teacher-forced movement-direction agreement and
`0.370°` yaw MAE, but its packet endpoint error was mean `0.907`, p99 `5.936`,
and max `31.30`; 129 intervals exceeded 2 units. Two of four long spans reset,
but those checkpointed spans zero controller history at interpolation-only
supervision boundaries. A separate continuous v3 rollout preserved history and
completed all 3,956 usable ticks without a reset, while ending `27,320` Source
units from the authentic pose. Exact commands reset at tick 2702 in the same
continuous mode. This cleanly separates strong local physics parity and weak
no-reset completion from still-incomplete route parity.

Fitting the Utopia MPC corrections into the existing 128-wide neural head did
not materially improve the continuous route, whether the authentic and
corrective examples were combined or the corrective labels were used alone.
The corrective-only nearest-state v6 controller instead reproduced the MPC
trajectory in an independent continuous rollout: it completed at demo tick
4195 with no failure reset and `22.52` Source-unit terminal position error.
Its artifact contains 3,955 state-memory samples and zero scheduled actions;
runtime prediction reads only the current simulated state. This demonstrates
that the corrected trajectory can be distilled out of the future-state MPC,
but the nonparametric memory is heavily specific to this demonstration and
does not establish neural or perturbation-robust controller parity. Packet
reinjection remains mixed: mean endpoint error `0.751`, p99 `6.957`, max
`13.091`, with 143 of 1,183 intervals over 2 Source units.

Do not describe that v6 controller as neural. A genuine neural artifact has
no `state_memory` and no `action_schedule`; version 4 artifacts may use a
second ReLU hidden layer. `tools/train_native_corrective_gpu.py` in the Surf
checkout trains those weights with PyTorch on ROCm/CUDA, then bakes feature
normalization into the exported runtime weights. Keep GPU batches modest and
remember that this accelerates neural fitting, not the authoritative CPU
Avian/Source-brush rollout.

The current Utopia neural results are negative but informative. A 512×512
ReLU policy reached 99.90% movement-direction agreement and `0.086°` yaw MAE
on the 3,955 corrective-memory examples, yet reset at demo tick 683. Two
nearest-memory-labelled DAgger rounds moved the reset to tick 763. A second
near-full corrective trajectory and stricter movement-threshold losses did
not produce independent completion; the best resulting weights-only rollout
reset at tick 786. Treat high offline agreement as insufficient and require a
full neural-only rollout plus bounded spawn/state perturbation runs before
claiming neural controller parity or generalization.

Use `policy.evolve-native` for bounded closed-loop search on a weights-only
analog policy. It rejects any artifact with `state_memory` or an
`action_schedule`, mutates only the neural movement/yaw output head, scores
ordered progress against the authentic native position/velocity route, and
reruns the winning candidate deterministically in a fresh CPU-authoritative
Source-brush world before optional persistence. Floor resets end a candidate;
for equal unfinished progress, longer survival ranks ahead of geometric
tie-breaks. Keep populations modest because the physics rollout is CPU-side;
32 candidates is the established compositor-safe Utopia width.

The first Utopia native-neural evolution probe is also negative evidence. On a
tick-440 curriculum checkpoint, the strict 512-unit corridor moved v18's
furthest authentic checkpoint from tick 344 to 413. A temporary wide-corridor
pass followed by 768-unit annealing reached tick 434 and reduced terminal
position error to `897.64` Source units, but it never met the strict `64`-unit
position and `64`-unit/second velocity gate. More importantly, its independent
full rollout reset at tick 708, worse than v18's tick 786. Do not publish that
temporary branch or treat curriculum progress as controller improvement; the
next approach must improve full-rollout survival as well as route progress.

For training-data generation only, `native_correction_policy_base: true`
runs bounded future-state correction around a frozen neural policy.
`native_correction_authentic_movement_candidates` adds authentic movement
vectors during each committed control block, while
`native_correction_authentic_command_candidates` also adds a full authentic
command continuation to the probe horizon. On Utopia, yaw-only correction
moved the v12 neural reset from tick 763 to 802, movement candidates to 965,
and full-command continuation reached tick 4193 with `83.01` Source-unit
terminal position error. The latter used authentic commands for 443 of 495
decisions and therefore remains expert-label generation, not independent
neural evidence.

## Native policy visualization

The playable `surf` binary can attach a policy whose source starts with
`native-demo:`. It loads the demo named in the artifact, requires
`SURF_NATIVE_NETWORK_TRACE` for an authoritative initial state, uses the same
native-to-policy feature transform as headless validation, and runs the full
artifact episode across interpolation-only supervision boundaries. A floor or
nonterminal teleport-trigger contact is logged as a failure and immediately
restarts from the authoritative initial state. A terminal trigger within 64
Source units and 64 Source units/second of the authentic finish is logged as
completion before restarting. It does not copy a replay action schedule into
the controller. Always select the playable binary explicitly because this
crate also contains `surf_lab`:

```bash
cd /absolute/path/to/surf
SURF_WINDOW_MODE=windowed \
SURF_MOVEMENT_MODE=native \
SURF_NATIVE_BSP_COLLISION=source \
SURF_BSP='/absolute/path/to/surf_utopia_njv.bsp' \
SURF_POLICY='/absolute/path/to/native-policy.json' \
SURF_NATIVE_NETWORK_TRACE='/absolute/path/to/decoded-packet-entities.jsonl' \
cargo run --release --bin surf
```

Use `SURF_WINDOW_MODE=hidden` for a renderer smoke test. A natural loop reload
after the reported full `rollout_ticks` proves the episode completed without
an earlier floor/teleport restart; it does not prove the learned path matches
the authentic route. Read `closed_loop.continuous` from
`policy.compare-native` for the matching trajectory error, while retaining the
independently reinjected packet intervals as the bounded local authority.
