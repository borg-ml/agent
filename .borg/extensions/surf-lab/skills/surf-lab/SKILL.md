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
normalization into the exported runtime weights. Native neural evolution also
has a closed-loop ROCm path: it evaluates neural inference, Source movement,
BSP brush sweeps, route progress, and reset triggers on GPU, then reruns a
bounded shortlist in the CPU Source-brush world before promotion. The CPU
world remains the validation oracle rather than the population hot loop.

The current Utopia neural results are negative but informative. A 512×512
ReLU policy reached 99.90% movement-direction agreement and `0.086°` yaw MAE
on the 3,955 corrective-memory examples, yet reset at demo tick 683. Two
nearest-memory-labelled DAgger rounds moved the reset to tick 763. A second
near-full corrective trajectory and stricter movement-threshold losses did
not produce independent completion; the best resulting weights-only rollout
reset at tick 786. Unconstrained nearest-memory DAgger can select labels
hundreds of route ticks away after autoregressive history drifts. The GPU
trainer can therefore bound labels with `--teacher-phase-window-ticks`, omit
history from matching with `--teacher-ignore-history`, cap rollout prefixes,
freeze the hidden representation, disable autoregressive history inputs with
`--disable-history-features`, and explicitly clip otherwise unrepresentable
teacher yaw deltas. Clipping remains opt-in and is reported.
Treat high offline agreement as insufficient and require a full neural-only
rollout plus bounded spawn/state perturbation runs before claiming neural
controller parity or generalization.

Use `policy.evolve-native` for bounded closed-loop search on a weights-only
analog policy. It rejects any artifact with `state_memory` or an
`action_schedule`, and mixes compact neural movement/yaw output-head mutations
with candidate-specific second-hidden-layer bias and rank-one hidden-matrix
mutations. The latter change ReLU gates or the shared 512×512 representation
without cloning that matrix per candidate. ROCm scores ordered progress against
the authentic native position/velocity route, and GPU-ranked candidates are
baked into ordinary policy weights and rerun in the
CPU-authoritative Source-brush world before they may update or persist the
controller. Floor resets end a candidate; for equal unfinished progress,
longer survival ranks ahead of geometric tie-breaks.
ROCm builds default to `gpu_population: 256` and `gpu_top_k: 16`; use
`gpu_population: 0` only for the serial CPU fallback. Keep the default width
until a larger batch is explicitly measured on the active desktop. A 256-wide
full-route Utopia validation screened in `20.46s`; its four CPU finalists had
furthest-route deltas `[1,0,0,0]` ticks, and the parent terminated after 526
episode ticks in both GPU and CPU worlds. Only CPU-rechecked candidates are
promotion evidence.

The first production GPU search screened 2,048 mutations as 8 generations of
256 and CPU-rechecked 128 candidates. It found no authoritative improvement
over v40. Most generation-level GPU/CPU finalist progress deltas were within
four ticks, but two generations contained GPU false positives up to 102 route
ticks and 158 termination ticks away. This confirms that ROCm is a proposal
engine, not the authority. The shortlist therefore spends half its CPU budget
on top GPU ranks, always includes the parent, and stratifies the remainder
across GPU ranks to retain recall when approximate physics misranks candidates.
A controlled rerun of the same 2,048 mutations with that strategy checked GPU
ranks 0–7 plus 28, 56, 85, 113, 141, 170, 198, and 226 in every generation
and still found no CPU-authoritative improvement. Treat this output-head
mutation set as exhausted; changing only shortlist selection is not a path
beyond v40.

Compact second-layer-bias evolution passed a live 32-wide differential with
18 output candidates, 13 hidden-bias candidates, and the parent. The parent
matched CPU termination exactly; a CPU-checked hidden-bias candidate differed
by four route ticks and one termination tick. A desktop-safe 2,048-proposal
search was then checkpointed as 3+3+2 generations to stay below the MCP call
deadline. Its first stage promoted an output-head candidate from checkpoint
633 to 692. Its second stage promoted a genuine second-hidden-bias candidate
from 692 to 767, `9.2117%` ordered progress, and reset tick 1104. The last 512
proposals found no further gain. V45 is the promoted weights-only neural
baseline; v46 has identical weights and records the final negative stage.
Neither completes Utopia. Some shortlisted proposals differed from CPU by up
to 186 route ticks and 579 termination ticks, so the GPU remains a proposal
engine and CPU reruns remain the sole promotion authority.

A follow-up rank-one hidden-matrix path stored only two 512-value vectors per
candidate, computed their projection in the ROCm kernel, and baked the outer
product exactly into ordinary policy weights for CPU promotion. A live
32-wide smoke CPU-checked a rank-one candidate with zero route and termination
tick delta. A desktop-safe 2,048-proposal search from v45 used a mutation scale
of `0.01` for its first 1,536 proposals and a final 512-proposal `0.0025` local
pass; it produced no CPU-authoritative improvement. Steady-state 256-wide GPU
generations took about 19–28 seconds, with some first generations near 39
seconds. Treat this rank-one mutation family as exhausted at those bounds
rather than widening it.

Candidate-specific suffix reconstruction then exposed mutation scale as the
next constraint. At `0.0001`, most candidates reset before the exact focus and
the wide search did not improve v45. Opening the lower bound and searching at
`0.00001` produced v53: checkpoint 777, `9.4229%` ordered progress, and reset
tick 1369. Every promotion was a complete authentic-start CPU Source-brush
rerun; the GPU remained a proposal engine.

Version 5 policies can carry a phase-gated neural residual over the second
hidden and output biases. Its residual is exactly zero before
`phase_adapter_start_tick`, then ramps for 64 ticks. Focused evolution can
therefore reconstruct one exact CPU parent state, reuse it for the entire GPU
suffix batch, and omit the inaccurate per-candidate prefix screen without
changing any candidate's pre-focus behavior. This remains a weights-only
state-feedback neural policy: it has neither state memory nor an action
schedule. A unit contract verifies the zero-prefix and ramp behavior through
serialization, and full normal/ROCm suites cover both binaries.

On Utopia, a focus-650 phase search raised survival to reset tick 1427 but held
checkpoint 777. Moving the exact focus to episode tick 1000 reduced the parent
GPU/CPU route discrepancy from 70 ticks to 8 in the calibration smoke. A
`0.1` phase-adapter probe then promoted checkpoint 778 with reset tick 1408;
the six-generation 256-wide v58 continuation retained checkpoint 778,
`9.4438%` progress, and raised reset to tick 1415. Its later-generation GPU
finalists were typically 5–8 route ticks from CPU, although termination timing
remained less faithful. The 256-wide suffix generations took roughly 7–30
seconds, one shared exact prefix took 0.3–1.3 seconds, and 16 complete CPU
promotion reruns took 9–28 seconds. V58 is the current genuine-neural
pre-late-adapter baseline; it does not complete Utopia and is not parity
evidence.

The ROCm neural rollout now assigns one 256-thread block per candidate: the
block cooperatively evaluates both 512-wide neural layers while lane zero
alone advances Source-brush physics. An exact repeated seed preserved the GPU
route result and all eight CPU/GPU termination deltas. GPU suffix time fell
from 7.52 to 0.50 seconds at width 32 and from 11.23 to 1.77 seconds at the
desktop-safe width 256. Running the eight independent authentic-start CPU
promotion worlds concurrently then reduced promotion time from 5.25 to 1.41
seconds. A bounded 24-generation continuation consequently screened 6,144
new phase-adapter candidates in about 91 measured seconds, but none advanced
past checkpoint 778. Treat additional
mutation-only searching at focus tick 1000 as exhausted at these bounds.

Version 6 adds one independently gated late residual without altering the
existing controller or primary adapter. CPU and ROCm apply it only from its own
start tick, and a serialization contract covers both exact zero prefixes and
ramps. A focus-1100 smoke matched the CPU route checkpoint for all eight GPU
finalists and found a real one-tick survival gain. The subsequent desktop-safe
24×256 production run screened 6,144 mutations; its persisted v59 controller
is byte-identical to v58 apart from the version, evolution receipt, and new
late-adapter fields, and its authoritative rollout is identical through
episode tick 1099. It retains checkpoint 778 and `9.4438%` progress, then resets
at demo tick 1416 instead of 1415. V59 is now the honest best weights-only
neural controller. The one-tick gain does not establish completion, physics
parity, or spawn/state perturbation robustness.

Authoritative terminal diagnostics localize v59's failure: at demo tick 1416
(episode tick 1176), it enters `trigger_teleport` index 4, whose Source bounds
are `[-3072,8192,-1536.0001]` to `[-1024,10016,1536.0001]`. Its pre-reset
position is `[-2999.752,8317.142,710.779]` with velocity
`[-355.326,-1470.838,314.413]`; neither the terminal tick nor the preceding
eight ticks contains a solid contact. Treat this endpoint as a controller line
loss, not a contact-order failure. Full `policy.compare-native` confirms the
split: v59 is already wrong by more than one Source unit at demo tick 242 and
diverges at tick 246, while 1,183 independent exact-command packet anchors
have mean endpoint error `0.00377`, p99 `0.00249`, max `1.486`, no resets, and
no errors over two units. The frozen neural controller has mean packet error
`20.60` and 1,161 errors over two units. The current measured bottleneck is
closed-loop control robustness, although these local packet bounds do not
prove full-route physics parity.

The GPU trainer can warm-start version 5/6 policies and freeze the entire base
network while fitting only `primary`, `late`, or `both` gated adapters with
`--train-phase-adapter`. `--adapter-update-scale` applies a signed `-1..=1`
trust-region line search to the fitted delta; zero preserves all runtime
weights exactly. Exported artifacts record the selected adapter and scale and
discard stale evolution receipts. A live ROCm fit processed 26,723 weighted
examples for 3,000 full-batch epochs in about 11 seconds on the installed AMD
GPU; ordinary version-4 and dual-adapter version-6 exports both passed runtime
inspection.

Do not promote the current imitation-gradient probes. A global phase-bounded
retrain regressed to checkpoint 614, `5.6518%` progress, and reset tick 714.
Late-adapter positive trust scales `[0.05,0.1,0.25,0.5,1.0]` all retained
checkpoint 778 but reset at `[1411,1408,1408,1408,1402]`; `-0.01` only tied
v59's reset without improving progress or fitness. Primary-adapter positive
scales `[0.001,0.01,0.05]` reset at `[1415,1407,1412]`, while negative scales
`[-0.001,-0.005]` reset at `[1415,1414]`. V59 remains the champion. This
bounded signed sweep rules out adapter-only supervised distillation along this
teacher-gradient direction; the next controller approach must optimize
closed-loop route reward or provide better off-trajectory supervision rather
than widening the same fit.

The first Utopia native-neural evolution probe is also negative evidence. On a
tick-440 curriculum checkpoint, the strict 512-unit corridor moved v18's
furthest authentic checkpoint from tick 344 to 413. A temporary wide-corridor
pass followed by 768-unit annealing reached tick 434 and reduced terminal
position error to `897.64` Source units, but it never met the strict `64`-unit
position and `64`-unit/second velocity gate. More importantly, its independent
full rollout reset at tick 708, worse than v18's tick 786. Do not publish that
temporary branch or treat curriculum progress as controller improvement; the
next approach must improve full-rollout survival as well as route progress.

Phase-gated DAgger produced v21, which independently reset at tick 1387 and
reached authentic checkpoint 459 (`2.08%` ordered route progress). A
full-route 16×32 CPU-authoritative output-head evolution then produced v28 at
checkpoint 498 and `2.9229%`. Removing autoregressive history and fully
converging a fresh 512×512 policy raised this to checkpoint 558 and `4.3701%`;
two full-route evolutionary searches produced v35 at checkpoint 614 and
`5.6518%` (`8,009.71` Source units). V35 reset at tick 706, so it is better on
the primary unfinished-run route objective but not on survival. A final bounded
serial search produced v40 at checkpoint 633 and `6.1152%` (`8,666.56` Source
units), with reset tick 766; v40 was the pre-adapter CPU-authoritative baseline.
These artifacts have neural weights only, no state memory, and no schedule, so
they are genuine closed-loop improvements, but still far from completion and
must not be described as parity.

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

A v21 full-command corrective pass survived 3,069 simulated ticks before
reset, but selected authentic full commands for 352 of 384 decisions. Only 5
of its 10,976 combined teacher labels exceeded the neural yaw-output bound;
explicit clipping made the data representable, but whole-network and
output-only distillation both regressed. Removing full-command candidates
reset at tick 766. This shows that the longer trace depended on authentic
command rescue rather than a directly distillable local correction; reject
offline-fit improvements unless the authoritative full rollout also improves.

Use `native_correction_start_episode_tick` with policy-base correction to run
the frozen neural controller unchanged to an episode-relative boundary, then
start MPC from the state it actually visited. On v28, the first 1,101 logged
prefix records matched the independent baseline exactly. Handoffs at episode
ticks 1100 and 1250 reset earlier than the uncorrected controller; a handoff at
1375 reproduced its original reset. This proved that fixed-clock MPC was
chasing unreachable future targets after v28 fell behind the authentic route;
the boundary is valid diagnostic/data-generation machinery, not evidence of a
successful recovery policy.

Focused ROCm neural rollouts must initialize their view from
`simulation_yaw` and `simulation_pitch`, not the stale sampled `yaw` and
`pitch` fields in `HeadlessPlayerState`. The stale handoff changed the very
first controller action and made focused GPU ranking untrustworthy. The fixed
path records the first GPU and CPU actions and, when an exact CPU suffix screen
is requested, uses the same bounded suffix horizon on both devices. On v85,
the repaired one-tick rollout had identical position and about `1.8e-5`
Source-unit/second velocity error; a 512-tick suffix had identical route and
reset timing with about `0.18` Source-unit terminal position error. Six checked
route/survival pairs ranked identically. Three subsequent 16-generation,
256-wide route-reward searches screened 12,288 candidates at roughly
31,000--36,000 candidate ticks/second without advancing checkpoint 778 or
reset tick 1430. Keep 256 as the desktop-safe width, retain CPU authentic-start
promotion, and treat the late constant-bias adapter family as exhausted; the
next search must add state-dependent recovery capacity or better
off-trajectory supervision.

Version 7 adds that state-dependent capacity as a phase-gated linear recovery
head over the same 19 live neural observation features. Its 6x19 weights and
six biases are exactly zero before `recovery_adapter_start_tick`; afterward
they add a state-dependent residual to the existing neural outputs. It remains
a weights-only controller with no action schedule, state memory, or future
replay access. CPU serialization tests cover the zero prefix, ramp, and
opposite responses to opposite positions. A live focused ROCm check on a
nonzero version-7 policy matched CPU first movement within `3e-8`, matched yaw
exactly, produced identical one-tick position, and differed in velocity by
about `1.9e-5` Source units/second.

The first recovery-head searches used the desktop-safe 256-wide batch at
episode tick 1100. A `0.01` pass learned side-movement feedback from position,
yaw, and phase but retained reset tick 1430. A `0.1` continuation moved the
pre-reset x coordinate about five Source units toward safety but still reset
at 1430. A bounded `0.5` pass cleared that old contact and promoted a complete
authentic-start CPU result that resets at demo tick 1431, with the same ordered
route checkpoint. Its controller is
`utopia-user-native-controller-v103-recovery-controller.json`; it has 17
nonzero recovery weights and neither a schedule nor state memory. A subsequent
16x256 continuation produced no further CPU promotion, so retain v103 as the
current genuine-neural controller and do not describe the one-tick gain as
completion, perturbation robustness, or CS:S parity.

Use `policy.recover-native` to generate bounded off-trajectory supervision
around a frozen weights-only native controller. It rolls that controller to a
visited state, builds its closed-loop baseline horizon, screens short
piecewise yaw and analog-movement residual programs in a 256-wide ROCm batch,
then reruns a bounded shortlist in the exact CPU Source-brush world before
committing a control block. Authentic commands and fixed-clock future poses
are not candidate actions or the local score. Recovery ranking is
lexicographic: authentic ordered route progress first, floor/reset avoidance
second, then route-state error and survival. Short terminal horizons skip
piecewise blocks outside the remaining action schedule.

An early survival-first diagnostic screened 1,068,750 GPU candidate ticks at
about 170,000 candidate ticks/second and extended reset from demo tick 1431 to
1463, but fell from route checkpoint 778 to 774. A longer-horizon run remained
alive through its 1500-tick bound at only checkpoint 772. These runs exposed
the wrong reward ordering; they are useful corrective-data bounds, not
controller promotions. With progress-first selection, starts from episode
ticks 900 through 1160, four random seeds, and 2/4/8/16-tick control sweeps
either regressed or tied v103 at checkpoint 778 and reset tick 1431. Observed
256-wide throughput was roughly 130,000--208,000 GPU candidate ticks/second;
exact CPU shortlist throughput remained roughly 2,400--2,900 ticks/second.

The survival-first trace was converted to the 1,223-example state-memory
teacher `utopia-user-native-controller-v105-gpu-recovery-memory-teacher.json`
for GPU distillation only. A full recovery-head fit regressed to checkpoint
758/reset tick 1315. A zero-initialized 0.25-scale fit survived to tick 2648
but regressed to checkpoint 771, while trust-region deltas fitted from v103 at
scales ±0.01, ±0.05, ±0.1, and ±0.25 all reset no later than tick 1430 at
checkpoint 778. At this recovery-only bound, retain v103. Version-7 recovery
gates may overlap older phase-adapter gates for supervised experiments; the
staged evolution initializer still constructs them in order.

At v103's demo-tick-1431 failure, the exact authentic UserCmd rollout in the
same CPU Source-brush world is only about `1.8` Source units from the decoded
packet position and `0.2` Source units/second from its velocity. The v103
controller is thousands of units behind, and a fresh first-attempt comparison
measured only `44%` movement-direction agreement and `3.70°` yaw MAE against
authentic commands. This isolates the immediate failure to controller
imitation/covariate shift rather than the BSP contact solver. The independent
packet-anchor physics bound remains the authority for general physics claims.

Set `first_segment_only: true` on `policy.train-native` to train only the
first reset-safe native attempt and use that attempt's duration as
`episode_ticks`. This excludes later reset-separated attempts and matches the
native visualizer's authoritative first-attempt start. It is mutually
exclusive with attempt holdout and full-episode corrective traces. With
`state_memory: true`, the resulting authentic first-attempt examples may be
used as a GPU-training teacher; that memory artifact is data, not the promoted
controller.

On Utopia, the 128-wide first-segment clone reset at demo tick 664/checkpoint
374. A 512x512 ReLU clone reached `98.07%` offline movement agreement and
`0.117°` yaw MAE but reset at tick 525/checkpoint 398. Four bounded authentic
DAgger rounds labelled only visited states using a ±64-tick phase window and
advanced the exact CPU rollout through checkpoint/reset pairs `437/533`,
`535/571`, `628/723`, then `980/1024`. A fifth round regressed to `633/707`,
so retain the weights-only artifact
`utopia-user-native-controller-v113-first-segment-dagger4.json`. It is a
512x512 version-4 weights-only controller with `98.86%` offline movement
agreement, `0.141°` yaw MAE, no action schedule, no state memory, and an
independently reproduced checkpoint-980/reset-1024 rollout. This supersedes
v103 for authentic ordered route progress, but it has not completed the first
attempt or passed state/spawn perturbation tests.

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
