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
`policy.evolve-native`, `policy.recover-native`, `policy.collect-native-ppo`,
`map.generate`, `map.inspect`, `map.preview`,
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
but a progress-first recovery teacher from v113 reached checkpoint 1215
without resetting inside its 1000-tick supervision bound. Distilling that
teacher together with the four successful visited-state rounds produced the
512x512 ReLU v116 controller with `98.31%` offline movement agreement and
`0.194°` yaw MAE. A bounded exact-CPU output-head mutation advanced it from
checkpoint/reset `1340/1442` to `1343/1446`; an independent authentic-start
rerun reproduced that result exactly. A sixth from-scratch round that appended
v116's failure trace regressed to checkpoint/reset `726/753`.

Use `--model-update-scale` with `--initialize` in the GPU corrective trainer to
bound full-model DAgger updates around a proven controller. A zero-scale v116
control reproduced checkpoint/reset `1343/1446` exactly. Exact CPU screening
of 1%, 3%, 6%, 8%, 10%, 12%, 14%, 14.5%, 15%, 15.5%, 16%, 20%, and 30%
updates exposed a narrow ReLU promotion basin: the 15% update reached
checkpoint/reset `1404/1468`, while 14.5% and 16% both regressed below
checkpoint 1200. Retain the independently reproduced weights-only artifact
`utopia-user-native-controller-v118-first-segment-trust-dagger6.json`. It has
no action schedule or state memory. V118 supersedes v116, v113, and v103 for
authentic ordered route progress, but it has not completed the first attempt
or passed state/spawn perturbation tests.

A 256-wide progress-first recovery pass from v118 reached checkpoint 1464 and
reset tick 1902 at about 78,000 GPU candidate ticks/second; exact CPU shortlist
throughput was about 2,380 ticks/second. Its 64 committed control decisions
are a supervision bound, not a controller. Full-model trust-region fits of
that trace did not beat v118. Instead, initialize a zero primary phase adapter
with `--phase-adapter-start-tick`, `--phase-adapter-ramp-ticks`, and
`--reset-phase-adapter`, then train only that adapter. The zero adapter exactly
reproduced v118. With a tick-1100 start, 64-tick ramp, and 20% adapter update,
the independently reproduced version-5 weights-only artifact
`utopia-user-native-controller-v120-first-segment-phase-dagger7.json` reached
checkpoint/reset `1450/1550` and 53.16% ordered route progress. It has no
action schedule or state memory. Retain v120 as the current neural route
authority; it remains short of first-attempt completion and perturbation
validation.

Starting recovery at episode tick 1250 from v120 produced an 84-decision
teacher that reached checkpoint 1475 and reset at demo tick 2156, 53 ticks
before the first authentic attempt ends. The 256-wide screen ran at about
86,000 GPU candidate ticks/second and the exact CPU shortlist at about 2,520
ticks/second. A zero second phase adapter at tick 1250 reproduced v120
exactly. Late-adapter fits using either nearest-authentic DAgger labels or the
teacher's aligned next commands did not exceed checkpoint 1450 and reset no
later than v120. `--action-rollout-trace` pairs each recovery command with its
preceding visited state; use it only for bounded recovery teachers, while
ordinary `--rollout-trace` inputs retain nearest-authentic labels. Retain v120
after this non-promotion.

Doubling the recovery horizon to 256 ticks and increasing route lookahead
regressed the teacher slightly to checkpoint/reset `1474/2126` while reducing
exact CPU throughput to about 1,650 ticks/second. A zero version-7
state-dependent recovery head at tick 1250 also reproduced v120 exactly.
Direct-action fits of its 6x19 gated linear weights produced at most the same
checkpoint 1450; the 5% update moved reset from 1550 to 1551 and fitness from
`0.4263106` to `0.4263188`, but did not advance ordered route progress. Do not
replace the displayed route authority for that secondary-only gain; retain
v120 and change the recovery/search evidence before another distillation
sweep.

Trace alignment localizes v120's next failure before its bad brush contact.
Its speed follows the authentic attempt closely through episode tick 1100,
but the first sustained world-space wish-direction error above 25 degrees
begins at tick 1115 when the authentic strafe changes side. The error exceeds
170 degrees around ticks 1149--1152, sustained speed loss begins at tick 1142,
and speed falls to 42% of authentic by tick 1200. The later contact with brush
3581 at tick 1212 is therefore downstream of controller phase/command drift,
not evidence of a BSP collision mismatch.

Match each recovery objective horizon to the ticks the candidate can actually
control. With three four-tick decisions, `start_tick: 1050`, `horizon: 12`, a
desktop-safe 256-wide GPU batch, and an exact 32-candidate CPU shortlist, the
v120 recovery teacher reached checkpoint 2092, 94.1057% ordered route
progress, and did not reset during the requested span. A 128-tick horizon had
judged 104 uncontrollable baseline ticks after only 24 mutated ticks and
selected slow, off-route survival instead. The checkpoint-2092 result is a
stateful action teacher only; it is not an independently executable neural
controller and remains short of the authentic first-attempt finish.

Distilling the matched-horizon teacher into the phase-gated recovery head
produced
`utopia-user-native-controller-v128-first-segment-recovery-dagger8.json`.
An independent authentic-start CPU rerun reproduced checkpoint 1456, 53.5134%
ordered route progress, and no reset over all 1969 requested ticks. The
version-7 512x512 ReLU artifact has no action schedule, state memory, or
autoregressive history features; its 50% recovery-head update supersedes v120
as the current weights-only neural route authority. It still has no terminal
completion and has not passed bounded spawn/state perturbation tests, so it is
not evidence of generalization or CS:S parity.

A second matched-horizon pass from v128 reached checkpoint 2091 with no reset,
essentially tying the v120 teacher rather than replacing it. Aggregating both
teachers exposed the 6x19 linear recovery head as the next capacity limit: its
trust-region fits and new primary-adapter fits did not retain both progress and
full-span survival. Version 8 therefore adds a phase-gated 6x512 residual from
the frozen second hidden layer while preserving every v128 weight. A zero
residual reproduced v128's checkpoint, fitness, and no-reset rollout exactly.
Training only those 3,072 residual weights on ROCm and applying a 22.5% update
produced
`utopia-user-native-controller-v130-hidden-recovery-dagger9.json`. Two
independent authentic-start exact-CPU reruns both reached checkpoint 1464,
54.0371% progress, and no reset over all 1969 ticks. V130 has no action
schedule, state memory, or history features and supersedes v128 as the current
weights-only neural authority. It remains nonterminal and untested under
spawn/state perturbations.

The HIP neural rollout now carries the version-8 6x512 hidden recovery residual
through both single-lane and cooperative inference. A desktop-safe 256-wide
integration screened the full v130 controller at about 20,142 candidate ticks
per second and its leading GPU route had zero checkpoint and termination delta
when rerun in the CPU Source-brush world. The promoted
`utopia-user-native-controller-v131-hidden-recovery-gpu-evolve1.json` then
reproduced checkpoint 1465, 54.0953% progress, and no reset over all 1969 ticks
in two independent CPU-only reruns. V131 supersedes v130 as the current
weights-only neural authority at that checkpoint.

Full and focused ROCm evolution now mutate the hidden recovery field instead
of merely carrying it through inference. An eight-generation, 256-wide search
from v131 devoted 63 candidates per generation to that field and screened
2,048 new candidates at about 19,400--21,400 candidate ticks per second. Its
promoted v132 controller changes 496 hidden recovery weights and one output
bias. Two independent authentic-start CPU-only reruns both reached checkpoint
1469, 54.3287% progress, and no reset over all 1969 ticks. V132 supersedes
v131 as the current weights-only neural authority. It remains nonterminal and
untested under spawn/state perturbations; the exact CPU world remains the sole
promotion authority and no result here establishes completion or CS:S parity.

When multiple adapters share one focus tick, focused evolution must select the
newest valid adapter. V132 has primary and recovery gates at episode tick 1100;
the repaired selector targets recovery there. A 32-wide receipt mutated 15
hidden and 16 linear recovery candidates with zero primary mutations. Two
subsequent 8x256 focused searches at `1e-6` and `1e-5` produced no full-route
promotion beyond v132, exhausting that local no-reset family at those bounds.

The full-scale three-teacher hidden-recovery fit reached checkpoint 1475 but
reset at episode tick 1524. Treating it only as a higher-progress search
lineage, a four-generation 256-wide recovery search with 512 exact CPU suffix
ticks and `0.001` mutations reached checkpoint 1479 and 54.8565% progress. Two
independent authentic-start CPU reruns reproduced both that checkpoint and the
same reset tick. V133 is therefore the route-progress authority, while v132
remains the no-reset full-span authority. Neither is a terminal completion or
perturbation/CS:S parity result.

Once a decoded PacketEntities trace exists, pass the same `network_trace` to
every production `policy.evolve-native`, `policy.recover-native`, and
`policy.compare-native` request. A run without it starts from a demo pose plus
the configured velocity estimator and is assumption-dependent; it must not
promote or supersede a traced controller. Also keep the two tick clocks
explicit: adapter and GPU-focus ticks are episode-relative, while reported
route-reference and reset ticks are absolute demo ticks. For this demo the
first episode starts at demo tick 240.

This provenance rule exposed a false late-search lineage: v201 appeared to
reach reference tick 2084 without resetting from a ballistic inferred start,
but all v179--v201 late variants reached only tick 1510 and reset at absolute
demo tick 1596 (episode tick 1356) from the authoritative network state. The
late adapters never activated in that real run. A traced, staged 256-wide GPU
search then promoted recovery at episode tick 1100, terminal at 1200, final at
1640, and completion at 1700. The resulting weights-only v7002 controller
independently reaches reference tick 2083, runs all 1969 requested ticks with
no reset, and has no action schedule or state memory. Its durable local artifact
is `.borg/surf-lab/policies/utopia-user-native-controller-v7002-traced-completion1700.json`.
It is not a completion: continuous `policy.compare-native` still ends about
5,983 Source units from the authentic finish. The bounded physics authority is
much tighter than the controller: across 590 independently reinjected packet
intervals, exact native commands have maximum endpoint error 0.00432 Source
units and no resets. Retain v7002 as the traced furthest-route neural authority,
not as CS:S parity.

Match the demo header to the BSP before diagnosing a route failure. This demo
names `surf_utopia_njv`; running it on the separate `surf_utopia_v3.bsp` raised
the exact-command packet-anchor maximum from `0.00432` to `19.12` Source units.
That cross-map result is invalid controller and physics evidence even when the
early route looks similar.

Playable policy inference must run before same-tick collider sizing and movement.
The former visual ordering computed v7002's action after collider sizing, so its
duck state first disagreed with headless validation at policy tick 1222, exceeded
100 Source units of position error by tick 1236, and reset at tick 1355. With
`prepare_policy_command` scheduled first in both binaries, a hidden-renderer run
matched all 1,969 headless state bits, did not reset, and accumulated at most
5.67 Source units of floating-point position drift. This fixes the observed
post-ramp-3 pirouette without changing the network or adding geometry features.

After that playback fix, v7002's remaining headless error is ordinary controller
drift rather than another ramp contact failure. Against the exact-command
continuous trajectory, its first sustained yaw error over 10 degrees begins at
episode tick 1052 (demo tick 1292), and its world-space wish direction remains
over 25 degrees wrong from episode tick 1150 (demo tick 1390). It still runs the
full span without resetting, but finishes nearest demo tick 2073 and about 5,983
Source units from the authentic end. Localize any next correction to episode
ticks 1050--1150; do not add geometry features or widen later adapters without
evidence that this measured command drift requires them.

Three network-traced matched-horizon recovery probes show that the existing
local recovery objective is not a safe teacher for v7002. All used episode tick
1050, a 12-tick horizon, three four-tick decisions, the desktop-safe 256-wide
ROCm batch, and a 32-candidate exact CPU shortlist. Seeds 7003 and 7004 both
regressed from the frozen controller's no-reset 91.8227% route result to
58.1809% and reset at episode ticks 1136 and 1135. A third seed with the exact
CPU velocity-error guard set to 0.1 selected more baseline decisions but still
reset at episode tick 1137 with 58.3333% progress. Do not distill these traces,
widen this search, or change the 19-feature observation schema based on them.
Retain v7002 until a different bounded off-trajectory objective beats it in a
complete authentic-start CPU rollout.

Reducing the recovery-program amplitude did not make it a usable teacher for
v7002. With the same traced start, 12-tick horizon, 4-tick controls, 256-wide
ROCm screen, and 32-candidate CPU shortlist, a 4-degree/0.25-movement probe
reached only 68.1402% progress and reset at absolute demo tick 1692; a
2-degree/0.1-movement probe reached 62.7033% and reset at absolute tick 1523.
Both are below the unchanged controller's 91.8227% no-reset result. A separate
one-generation focused `policy.evolve-native` smoke at episode tick 1100
evaluated 257 candidates with a 64-tick exact CPU focus screen and four full
CPU promotion reruns. Its best result exactly tied v7002 (reference tick 2083,
91.8226%, no reset), while GPU-leading candidates terminated substantially
earlier on CPU. Do not promote amplitude variants or widen this local mutation
family; a new teacher objective or off-trajectory data source is required
before another controller change.

The native command microscope explains why a context ablation is now
warranted. Around the measured route gap, the authentic stream holds
`side_move=400` through demo tick 1354, goes neutral at 1355, changes to
`-200/-400` on ticks 1356--1357, goes neutral at 1387, then changes to
`+200/+400` at ticks 1389--1390. The latter reversal is exactly where the
continuous v7002 wish-direction error exceeds 25 degrees. A matched diagnostic
pair trained 128-wide tanh policies on the first reset-safe segment with and
without authentic previous-command features. History raised offline movement
agreement from 54.57% to 97.72% and reduced yaw MAE from 0.288 to 0.267
degrees. It also reduced packet policy-minus-exact error from mean 2.424/p99
11.358 (215 intervals over 2 units) to mean 1.068/p99 5.881 (116 over 2), but
both teacher-forced policies suffered early closed-loop resets at absolute demo
ticks 633 (history) and 655 (no history). Treat this as evidence that command
context carries useful information, not as a production controller result:
naive teacher-forced history training is covariate-shifted. The next bounded
ablation should use visited-state/off-trajectory labels with history-aware
trust-region training and full authentic-start CPU validation; explicit ramp
geometry remains unsupported by the current failure evidence.

The first history-aware off-trajectory probes did not meet that promotion bar.
A 512-wide ReLU history policy fit directly from the state-memory teacher had
excellent teacher-forced agreement (98.78% movement, 0.093 degrees yaw MAE),
but its authentic-start closed-loop run reset at absolute demo tick 1916;
packet policy-minus-exact error was mean 1.164, p99 9.921, with 94 of 590
intervals above 2 Source units. Adding one rollout-trace DAgger pass made the
result worse (reset at tick 597; mean 1.475, p99 12.441, 122 intervals over
2). Trust-region scales of 0.1 and 0.01 also regressed, resetting at ticks 809
and 503 respectively (means 2.139 and 1.233). None of these diagnostic
artifacts was promoted, and v7002 remains the no-reset 91.8227% authority.

The current focused search implementation also rejects this policy class:
`policy.evolve-native` requires `history_features=false` whenever
`gpu_focus_episode_tick` is nonzero, and its exact CPU focus screen has the
same requirement. An attempted history-focused call therefore produced no
artifact and the worker exited before returning a result; do not repeat it as
if it were a valid search. Supporting a useful history-aware objective now
requires an explicit state-dependent/off-trajectory implementation, followed
by the same complete authentic-start CPU and in-game checks. Until that exists,
keep v7002 and the corrected playback ordering unchanged; do not add ramp
geometry inputs or claim parity from these ablations.

A final bounded endpoint-adapter check also failed to move the authority. Three
generations of antithetic CEM at the existing completion-adapter focus
(episode tick 1700) screened eight ROCm candidates per generation and retained
the parent in every exact route comparison. The GPU best stopped at reference
tick 1882 while the parent also stopped at 1882, so the CPU promotion gate
correctly skipped all non-novel candidates; the complete parent rerun remained
reference tick 2083, 91.8226% progress, and no reset. The emitted diagnostic
artifact has byte-identical weights to v7002 after removing only its evolution
receipt. This exhausts the current endpoint-adapter/CEM family at the safe
bound; a future improvement needs a new off-trajectory objective, not a wider
batch or more endpoint mutations.

The first bounded off-trajectory objective was tested before adding any new
observation geometry. Authentic native UserCmds were aligned to the frozen
v7002 controller's actual visited states (episode ticks 0--1968), converted by
the same `native_command` mapping, and used as labels for small ROCm-trained
adapters. A history-input-column-only probe reset at episode ticks 739--789.
State-dependent recovery adapters trained on the full visited-state set or on
the ramp-3 window (trust scales 0.01--0.1, gates starting at ticks 1040--1100)
reset at ticks 1520--1557; phase-bias probes reset at ticks 1524--1557. The
unchanged v7002 parent still runs all 1,969 ticks without a reset. The labels
also required clipping 651 of 1,969 yaw targets to the runtime's +/-0.5-radian
neural delta range, confirming that an off-trajectory replay command is not a
safe one-step correction target once the controller has drifted. None of these
artifacts is a promotion candidate. Do not add ramp normals, nearest-ramp
features, or autoregressive history solely on the basis of teacher-forced
agreement; require a complete authentic-start CPU improvement first.

A four-generation focused route-state search tested the next bounded objective
without changing the observation schema. It resumed the exact v7002 state at
episode tick 1100, screened 256 ROCm candidates per generation, exact-CPU
screened a 256-tick suffix, and retained v7002 as the full-route promotion
anchor. The parent stayed best in the complete 1,969-tick CPU rollout: no
reset, reference tick 2083, and 91.8226% ordered progress. The GPU-leading
resume-only center had a better short suffix score but reset at episode tick
1814 when restarted authentically, reaching only reference tick 1705 and
68.0914% progress. It was therefore not promoted. This exhausts the bounded
phase-gated route-state family at the desktop-safe width; retain v7002 and
require a genuinely new closed-loop objective or supervision source before
adding geometry/history inputs or widening the search.

The follow-up transition microscope narrows the failure further. In the
v7002 and exact CPU logs, episode tick 1050 is already about 8.7 degrees too
positive in yaw and roughly 120 Source units off-line; the exact and learned
worlds still share the same ramp plane immediately before the next contact.
The learned side-input jump at episode tick 1109 is therefore a consequence
of controller state drift, not an unmodeled collision plane. Moving the
existing recovery gate from tick 1100 to 1120--1968 either reset earlier or
ended much farther from the route. Zeroing its movement rows reset at tick
1486; zeroing its yaw row preserved survival but ended about 15,967 units
off-route. A route-relative 128-wide ReLU policy reached 99.95% offline
movement agreement yet reset at tick 505 in independent CPU playback. Finally,
a yaw-only, 2-degree closed-loop recovery teacher from tick 1050 reset at
tick 1321. None is a controller promotion. These bounded ablations support
keeping the current 19-feature policy and collision/order implementation; the
remaining work needs a new closed-loop supervision signal, not more feature
columns or a wider version of these adapters.

Two final covariate-shift probes were run without changing the runtime
features. Authentic labels were augmented with bounded position, velocity,
and yaw perturbations in the 1,030--1,150 window and fitted only into the
existing recovery adapter. Perturbing exact-route states reset at tick 1526;
pairing the same labels with v7002's actual off-trajectory states (including
36 clipped yaw targets) reset at tick 1476. Both were complete CPU-authority
failures, so retain v7002 and do not promote local adapter fits from this
supervision family.

Moving that perturbed off-trajectory objective upstream did not help. An
authentic-command window from ticks 850--1050 with a recovery gate at tick
900 reset at tick 1296 in the full CPU rollout. The apparent late-window
boundary is therefore not the missing signal; retain the v7002 gate timing and
require a different closed-loop target rather than sliding this adapter again.

A fresh route-relative 512x512 ReLU policy combined authentic first-attempt
examples with four-times-weighted labels from v7002's actual visited states.
Despite 99.42% offline movement agreement and 0.066-degree yaw MAE on ROCm, its
full authoritative CPU rollout reset at absolute demo tick 573. This rejects
route geometry as a replacement for v7002 at the current evidence bound.

The playable runtime can extend a frozen native policy past its recorded
episode with `SURF_POLICY_ROLLOUT_TICKS` and apply a diagnostic piecewise clock
using `SURF_POLICY_PHASE_START_TICK` plus `SURF_POLICY_PHASE_RATE`; all three
default to the original behavior. Unmodified v7002 remained active beyond its
old 1,969-tick cutoff but hit a floor reset at policy tick 2119, about 1,572
Source units from the nearest authentic state (episode tick 1908). A uniform
0.93 clock regressed to episode reset tick 516, and a 0.83 clock starting at
tick 1000 reset at tick 1249. Starting a 0.50 clock only at tick 1700 preserved
the strong prefix and cut the final nearest-route miss to about 1,037 units at
episode tick 1943, but it still hit the floor at policy tick 2119; rates 0.58
and 0.40 tied or regressed that reset. Treat these as bounded playback
counterfactuals, not controller promotions. Retain unchanged v7002 and its
corrected same-tick command ordering.

The clean-controller branch now has a reproducible phase-free training path.
Surf commits `9e72063` and `3a50b58` add `--disable-phase-features` and allow
closed-loop-only DAgger data: the offline teacher may use the recorded tick to
label a visited state, but columns 11--12 are zeroed before fitting and the
export is forced to `phase_features=false`, `history_features=false`, a zero
schedule offset, and no adapter payloads. Distilling v7002's two complete
1,969-tick closed-loop traces plus one clean policy's visited states produced
seed 3001 at
`.borg/surf-lab/policies/utopia-user-native-clean-vnext-v7002-dagger2-seed3001.json`.
It is a single 512x512 ReLU version-4 network with no action schedule, state
memory, route table, phase/history input, or adapter weights. Its authentic-
start CPU rollout reset at episode tick 742 and reached reference tick 878
(23.5347% route-distance progress), so it is not a v7002 replacement.

Full-network evolution can improve this clean artifact without reintroducing
phase machinery. Three desktop-safe generations of 256 ROCm candidates with
CPU Source-brush promotion produced
`.borg/surf-lab/policies/utopia-user-native-clean-vnext-evolved1.json`: it
reset at episode tick 992, reached reference tick 1132, and achieved 35.7690%
route-distance progress. A second three-generation pass produced
`utopia-user-native-clean-vnext-evolved2.json` but only reached reference tick
1133, 35.8219% progress, and reset at episode tick 993. Bounded recovery
teachers starting at ticks 640 and 400 tied or regressed the seed, and are not
training authorities. The clean branch is therefore real and materially
improved, but still incomplete and below frozen v7002. A one-generation
256-wide scale ablation found that `0.0001` produced only a GPU false positive,
while `0.00001` advanced the exact CPU route by one tick. Keep
`utopia-user-native-clean-vnext-scale1e5.json` as the clean search parent: it
reaches reference tick 1134, 35.8747% route-distance progress, and resets at
episode tick 993. A subsequent 3x256 continuation at the same scale found no
further gain. Do not claim completion or visualize it as a top run yet.

The first genuinely closed-loop planner experiment around that clean failure
separates useful teacher capacity from distillation failure. Progress-first
GPU recovery from episode tick 900 tied reference tick 1133 and survived only
to tick 1006; moving it to tick 800 with a 15% velocity guard regressed to
reference tick 1112/reset tick 973. A future-state CPU planner starting at
episode tick 800 and allowed authentic analog movement candidates did better:
it reached reconstructed reference tick 1209 and reset at tick 1089, selecting
authentic movement in 6 of 37 decisions. This remains a training-only teacher,
not controller evidence. Full-model phase-free trust updates from 3% through
0.001% all regressed, while a stable output-only 0.001% update reached only
reference tick 1133. Reject that supervised gradient.

Surf commits `684eacd` and `8ac659a` make this ablation reproducible without
adding runtime clock machinery. Phase-free warm starts now require a
phase-free initializer; zero-scale exports preserve every network tensor
exactly, and output-only fitting leaves frozen layers byte-identical. Planner
guidance can screen ordinary version-4 full-network mutations from the
authentic start, trims a terminal reset/spawn observation from the guidance
route, and still requires complete CPU Source-brush promotion. A 32-wide
smoke, 3x256 production pass, and final 1x256 scale/recall check all failed to
advance beyond reference tick 1134. The best guidance artifact changed only
the next-point distance from 522.354 to 521.839 Source units at the same route
and reset ticks. Treat the current planner-guidance objective as exhausted at
these bounds and retain `scale1e5`, not the guidance artifact, as the clean
route authority.

Surf commit `1d3e4a1` also lets the existing antithetic ES/CEM machinery target
the ordinary six-output head during authentic-start planner-guided screening;
the search mean is never persisted unless a complete CPU route rerun promotes
it. An 8-wide smoke had full paired signal and retained a clean version-4
artifact. Two 3x256 antithetic-ES passes at initial sigmas `0.0001` and
`0.00001` then produced no CPU improvement over reference tick 1134/reset tick
993. The smaller pass had 127 pairs per generation and nonzero success rates,
but its search-mean guidance progress oscillated while every CPU promotion
remained tied. Treat planner-guided base-output antithetic search as exhausted
at these bounds too; it does not supersede `scale1e5`.

Surf commit `ac2bafa` adds the clean on-policy path that supersedes further
generic ES around this failure. `policy.collect-native-ppo` defaults to a
desktop-bounded ROCm collector: up to 256 authentic or authentic-start
rollouts run neural inference, censored-Gaussian action sampling, Source-brush
physics, ordered-route reward, and transition capture in one HIP batch. The
trajectory width remains capped at 256, while horizons may reach 2,048 ticks
so a rollout can encounter its own failure. Only initial-state construction,
compact JSONL transfer, bounded differential checks, and controller promotion
remain on CPU. Set `backend: "cpu"` only for the exact reference collector.
`tools/train_native_ppo_gpu.py` performs the clipped policy/value update on
ROCm, uses the correct point masses for actions clipped at their runtime
bounds, and rejects epochs that cross the KL limit. The exported controller is
still one ordinary phase-free route-relative network with recent-action
features and no replay clock, action schedule, state memory, or adapters.

A zero-noise 32-tick differential matched route indices and all terminal flags;
maximum reward drift was `2.6e-5`. A 256x128 production batch collected 32,244
transitions in 1.278 seconds end to end (about 25,227 candidate ticks/second),
roughly ten times the measured CPU collector. A 128-wide authentic-start batch
with a 1,969-tick horizon collected 50,524 transitions in 1.978 seconds without
increasing parallel width. Large PPO steps improved some randomized metrics
but regressed the authoritative start and were rejected. One `1e-7` update is
the current clean candidate at
`.borg/surf-lab/policies/utopia-user-native-ppo-route-gpu-fullstart-v4tiny.json`:
its exact CPU reset moved from absolute demo tick 727 to 856, while the bounded
deterministic GPU route index moved from 233 to 286. It still resets, does not
complete the map, and does not replace v7002. Continue from this candidate only
when both full-start CPU survival/route progress and perturbed validation
improve; keep regressed v3/v4/v5 artifacts as diagnostics only.

Surf commit `2d5ab19` adds explicit PPO step backtracking and recomputes the
actual post-scale KL before export. The rejected v5 direction was screened from
1/1,024 through 3/4: its GPU-leading 1/128 step reached route index 322 over
1,262 ticks, but exact CPU playback reset at tick 612 instead of v4tiny's 856.
A lower-variance direction combined four independent 128-rollout ROCm batches
(210,973 transitions). Its full step improved held-out perturbed GPU reward
from 30.48 to 30.96 and furthest route index from 292 to 304, but reset at CPU
tick 601; backtracked scales from 0.001 through 0.75 did not retain the
deterministic GPU gain. Finally, 64 stochastic full-start rollouts collected in
the exact CPU world produced 26,070 transitions at about 2,503 ticks/second.
The full update reset at CPU tick 636. Scale 0.002 advanced the exact CPU
collector's route index from 267 to 294 but shortened survival from 616 to 554
ticks, and a fine search around it found no dual improvement. None of these is
a promotion. Retain v4tiny and do not repeat these three PPO directions; a next
update needs a different closed-loop objective or a tighter long-horizon GPU
transfer bound, not more line-search scales.

Surf commit `920a90a` tightens the local transfer bound by scaling positions,
velocities, and route points into prototype units before the HIP route-tangent
and dot-product feature math, matching CPU evaluation order. At the identical
first state, maximum feature error fell from `4.77e-6` to `2.39e-7` and maximum
policy-mean error fell from `4.00e-4` to `1.60e-5`; a 32-tick differential kept
route indices and terminal flags equal with reward drift below `4.9e-7`.
Long-horizon trajectories still decorrelate, so CPU promotion remains required.

With that fix, a 256x128 randomized authentic-route batch collected 32,433
transitions at about 25,062 candidate ticks/second. Mixing it with the 64-rollout
exact-CPU full-start batch and backtracking one PPO update produced the new
clean parent at
`.borg/surf-lab/policies/utopia-user-native-ppo-route-aligned-mixed-v8step-0.125.json`
(SHA-256 `ab968c782e77389dc3ef30521ef6fe81fce18a44bd4934a5415f5a77c18faf46`).
Exact authentic-start CPU validation improved reset tick 856 to 950, route
index 267 to 303, and executed ticks 616 to 710. Across 64 matched CPU state
perturbations, mean survival improved from 397.59 to 407.09 ticks, mean episode
reward from 27.97 to 31.01, and furthest route index from 269 to 290. A matched
GPU perturbation batch improved reward but slightly regressed mean survival and
furthest route, reinforcing the CPU authority. The artifact is one phase-free
256x256 ReLU network with recent-action inputs and no clocks, schedules, memory,
or adapters. It still resets, does not complete Utopia, and does not replace
v7002; it does supersede v4tiny as the clean PPO training parent.

A second aligned mixed generation from that v8 parent regressed at every
screened scale from 0.001 through 0.75, so retain v8 for the route-relative PPO
lineage. It still does not supersede the farther phase-free `scale1e5` clean
route authority.

Surf commit `86158e1` removes an artificial PPO admission restriction and lets
the same GPU collector/trainer optimize either supported phase-free weights-only
schema. The existing `scale1e5` controller is a 512x512 pose-policy network with
no clock, history, schedule, memory, or adapters. Its zero-noise 32-tick GPU/CPU
differential had bit-identical first-state features, first-action error
`6.8e-8`, maximum 32-tick feature/mean errors `7.2e-7`/`3.8e-6`, equal route and
terminal flags, and reward drift `5.6e-9`. Full deterministic GPU and CPU
rollouts both reset after 993 ticks and reached route indices 893 and 894,
respectively, making this the tightest long-horizon neural transfer tested.

Two direct ROCm PPO objectives still failed promotion. A broad full-start plus
random-route batch contained 99,715 transitions; scales 0.001--0.0011 tied the
894/993 CPU baseline with slightly worse reward, and larger scales regressed.
A near-policy 80,765-transition batch raised average perturbed survival to 631
ticks, but only scales 0.003--0.004 tied the baseline and none advanced route or
survival. Keep all generated PPO variants as diagnostics and retain unchanged
`utopia-user-native-clean-vnext-scale1e5.json` as the clean route authority.
The pose-policy PPO path is now valid and well aligned, but needs a different
closed-loop objective rather than another scale refinement of these directions.

Surf commit `fdb9865` adds `--train-scope output`, freezing both hidden layers
so PPO can update only the ordinary six-output head. This is still a single
clean network, not an adapter. A 99,715-transition output-only update from
`scale1e5` was screened from scale 0.001 through 0.032. The two smallest steps
kept 993 CPU ticks but lost one route sample (893 instead of 894); larger steps
reset around 733 ticks. This rejects excessive full-network degrees of freedom
as the immediate cause for the failed directions. Do not promote any
output-only variant; retain `scale1e5`.

Surf commits `aa155c6` and `e041651` add a distinct policy-visited PPO
curriculum without changing the deployed controller. Set `start_source` to
`policy-prefix` to run the frozen memoryless policy once from the authentic
start, snapshot every requested episode tick with its actual ordered-route
index, and send only the stochastic suffix batch to ROCm. The one-pass snapshot
implementation reduced an identical 256x192 collection from 58.334 to 6.664
seconds end to end (8.75x), with 5.902 seconds in GPU simulation. All 40,447
transition rows remained byte-identical. A zero-noise 8x64 CPU/GPU differential
from policy tick 850 matched rewards, route indices, and terminal flags; maximum
feature and action/mean errors were `2.09e-7` and `1.37e-6`.

That new distribution is valid and fast but its first bounded PPO direction
did not promote. The 256-rollout batch reached route index 902 stochastically,
yet full-network scales from 0.001 through 0.064 and output-only scales from
0.001 through 0.032 all regressed the exact authentic-start CPU rollout. The
best 0.001 steps reached route 893 over 992 ticks, versus the unchanged
`scale1e5` baseline at route 894 over 993 ticks. A full-parent-trajectory
rehearsal ablation also topped out at route 892 and was removed rather than
retained as another unused training option. Keep `scale1e5` as the clean route
authority and do not repeat this policy-prefix PPO direction or its scale
search unchanged.

Surf commit `6ef6eea` keeps the persistent MCP worker alive after an ordinary
tool error. `tools/call` failures now return a JSON-RPC error instead of
escaping `main` and closing stdout; an invalid PPO call followed by a valid
`tools/list` request succeeded in the same release worker.

Surf commit `528c7f2` extends policy-prefix snapshots to history-enabled
route-relative policies. Exact CPU prefixes now preserve the previous analog
movement, yaw delta, and jump state across both CPU and GPU handoff rather than
silently zeroing them. At v8 episode tick 500, the first route-policy suffix
state had nonzero history (`movement=[1,-1]`, normalized yaw delta `0.2998`,
jump held); GPU/CPU first-state feature and mean errors were `5.96e-8` and
`1.14e-5`. A zero-noise 8x64 differential matched route and terminal results.

A 256x192 v8 policy-prefix batch then collected 40,366 transitions in 1.776
seconds end to end (about 22,734 candidate ticks/second; 1.299 seconds GPU
simulation) and found stochastic continuations to route index 316 versus the
parent's deterministic 303. The resulting PPO direction still failed exact
CPU promotion at every screened scale from 0.001 through 0.032; the best scale
reached route 278 over 663 ticks, below v8's route 303 over 710 ticks. Retain
v8 as the route-relative PPO parent and do not promote these variants.

Surf commit `6fcb383` adds temporally coherent exploration without changing a
deployed policy. `exploration_hold_ticks` holds one sampled continuous-action
residual for up to 64 suffix ticks in both the ROCm and CPU collectors. Values
above one are explicitly rejected by the PPO trainer because the samples are
correlated and their independent-action likelihood would be invalid. CPU and
GPU use the same counter-based normal samples for held exploration, while the
default one-tick CPU sampler retains its prior behavior.

This resolved an important exploration failure around the clean `scale1e5`
controller. From its exact episode-tick-800 state, an otherwise identical
desktop-safe 256x256 one-tick-noise control reached only route index 884. With
32-tick residuals, movement standard deviation 0.1, yaw standard deviation
0.04, and pitch standard deviation 0.004, the top three trajectories instead
reached route indices 960, 955, and 944; all three survived the complete
256-tick suffix and 20 candidates exceeded the frozen parent's route-894
ceiling. An exact CPU rerun reproduced those top route and survival results.
Across all 256
candidates, CPU/GPU reset classification was identical, route disagreement was
at most one sample, and termination timing differed by at most six ticks. A
separate nonzero-noise 8x64 differential matched every route index and terminal
flag, with maximum reward error `4.65e-6`, feature error `2.39e-4`, and policy
mean/action error `0.00224` after stochastic trajectory divergence.

Do not promote a controller from that trajectory result alone. Full-network,
output-only, and frozen-hidden ridge distillation of the three CPU-verified
elites all failed complete authentic-start validation and the experimental
distiller was removed. The closest full-network trust step (`3e-5`) retained
993 ticks but reached route 893 instead of the unchanged parent's 894; other
steps reset earlier. Even prefix-anchored output solves changed the sensitive
early line and reset around ticks 731--755. Retain unchanged `scale1e5` as the
clean authority. The held trajectories prove that useful closed-loop actions
exist at the failure, but the next model needs clean state-localized capacity
that preserves its prefix without a clock, gated adapter, schedule, or memory.

A bounded feature microscope also argues against adding live ramp geometry at
this point. Among the closest five percent of temporally nonlocal authentic
states, route-relative features with previous-action context had no conflicting
movement labels, while the phase-free pose-only representation had conflicts
in about 51.5 percent. At the `scale1e5` failure, however, globally nearest
route position advanced to index 944 while ordered progress remained at 894.
Any next route-conditioned schema must avoid that off-route nearest-point phase
leap; this is route-state indexing evidence, not evidence for ramp normals or
another collision change.

Surf commit `dcfa6fd` records the bounded clean-capacity follow-up. A history-input-
only fit enabled the four existing recent-action columns while freezing every
other `scale1e5` tensor. Its exact zero-scale control reproduced route 894 and
993 ticks, but its best nonzero CPU candidate reached only route 893 over 992
ticks; larger steps reset earlier. A function-preserving expansion then embedded
the complete 512-wide parent in one ordinary 768-wide version-4 ReLU network and
initialized 256 new units at zero output. The deployed artifact still had no
clock, adapter fields, schedule, state memory, or route table.

That added capacity was trained on six CPU-verified coherent recoveries, the
exact 800-tick parent prefix, and a bounded 8,192-state sample from the existing
80,765-transition near-policy and 31,975-transition randomized-authentic ROCm
collections. Full and scaled CPU rollouts did not promote: the mixed model's
best scale (`0.01`) only tied route 894/993, and larger useful-scale candidates
reset earlier. Restricting the teacher to the mutually consistent top three
recoveries (routes 960, 955, and 944) also failed; scales `0.005`--`0.03`
reached only routes 653--654 over 746--751 ticks. Retain unchanged `scale1e5`.
The reproducible trainer is `tools/train_native_capacity_gpu.py`, but this
supervised recovery-distillation objective is exhausted: the next capacity
update needs direct closed-loop rollout optimization of state-dependent weights,
not another imitation fit, scale sweep, or geometry feature.

The subsequent architecture audit rejected one more direct localized-output
search before changing the observation contract. A zero-initialized residual
suffix was active on only 16 of 800 authentic prefix states but on 37.49 units
per sampled recovery state. A 2x64 ROCm direct search changed only that suffix;
the best exact CPU rerun tied the parent at reference tick 1134/reset tick 993
and improved only the secondary next-point distance. Do not treat its generic
`improved` flag as route progress or promote the artifact.

The clean route-state replacement uses feature schema
`native_route_progress_v2`. `PolicyRuntime` owns a monotonic route belief
separate from the permissive legacy route/completion scorer. It can advance at
most eight samples per physical observation, its admitted route arc is capped
at four times actual displacement, it cannot advance without movement, and it
cannot move backward. CPU playback, authoritative evaluation, focused-state
capture, and ROCm rollouts carry this policy index independently; never seed it
from `RouteTracker.furthest_index`. That prior cross-wiring started some focused
paths about one corridor/lookahead ahead and made prefix capture disagree with
playback. PPO progress, tangent, and target shaping for v2 now use the same
policy belief that produced the observation, while the legacy tracker remains
completion/reporting evidence only.

Use short ROCm horizons for this schema. Across 32 independently sampled
authentic starts and 256 zero-noise eight-tick transitions, CPU/GPU route
indices and terminal flags were identical; maximum reward, feature, and action
errors were `1.65e-5`, `0.0041`, and `0.0041`. Long closed loops remain
chaotically decorrelated and are not promotion evidence. Desktop-safe 256x8
batches covered the full authentic route, the ramp-3 region, and genuine
policy-prefix states at roughly 3.7k--9.8k end-to-end candidate ticks/second.

The bounded training results are negative for PPO but informative for
corrective supervision. The migrated v8 parent reaches v2 route index 279 and
resets after 793 episode ticks (absolute demo tick 1033). One output-head PPO
direction trained from 6,144 authentic, ramp-weighted, perturbed, and visited
transitions regressed at every screened scale from `0.005` through `1.0`; even
the smallest first-action change was about `1.7e-6`, yet its route branch
diverged by tick 17. A route-matched warm-start robustness fit also regressed.
Do not repeat that PPO direction or line search.

PPO transition rows now include `observation_route_index`, the pre-action
policy belief. `tools/train_native_corrective_gpu.py --ppo-rollout` uses it to
label perturbed and closed-loop states with the authentic route-aligned command,
including the current-state correction to yaw delta. This remains one
weights-only, phase-free network. Aggregated DAgger improved exact CPU route
progress from 279 to 319 and then 380, but survival fell to 676 and 588 ticks;
the next generation collapsed into a no-reset route-23 stall. None is a
promotion or visualization candidate. This closes blind DAgger iteration at
the current discrete representation. The next architecture should use smooth
continuous route projection plus an explicit local-sensitivity/contractive
objective. Add ramp normals or recurrence only if that smooth controller still
shows observation ambiguity; do not add clocks, schedules, lookup memory, or
per-ramp adapters.

Surf commit `b1e6e40` implements that minimal architecture change as feature
schema `native_route_continuous_v3`. The controller now carries fractional
progress on the demonstrated polyline and interpolates route position,
tangent, and short/long lookahead geometry instead of snapping observations to
integer samples. The tracker retains the v2 safety contract: forward-only,
eight samples maximum per observation, admitted route arc no greater than four
times physical displacement, no advance without movement, and no backward
jump. CPU playback, focused-state capture, the PPO collector, and HIP all carry
the fractional value explicitly. PPO transition rows contain both post-step
`route_progress` and pre-action `observation_route_progress`.

Keep controller geometry separate from scoring geometry on GPU. HIP now has a
dedicated Source-unit copy of the artifact's `route_points` for neural features
and controller progress; the authoritative native route remains the only
reward/completion route. Using the scorer's points for both was a real transfer
bug even when the two routes looked nominally identical. CPU and GPU stochastic
collection also key the initial perturbation by `(seed, rollout)` and action
noise by `(seed, rollout, exploration block)`, so batching order cannot change
the experiment.

The short-horizon transfer proof used 32 authentic starts with eight stochastic
steps each. CPU and ROCm had zero route-index, reset, terminal, or completion
mismatches. Maximum feature, mean, action, reward, and fractional-progress
errors were `0.0003353`, `0.0004596`, `0.0004411`, `0.0001221`, and `0.00183`.
A desktop-safe 256x32 authentic batch ran at about 25,600 candidate ticks per
second; 256-wide policy-prefix batches measured about 22k--30k. Continue using
ROCm for wide short-horizon collection/search, but require an exact full-start
CPU rollout for promotion.

The first bounded v3 sequence produced a real but incomplete gain. A fresh
controller (`r1`) reached route sample 196 and reset after 271 episode ticks.
Authentic plus policy-visited GPU supervision produced `r2`, route 392 and 457
ticks. A 256-wide, 128-tick recovery teacher screened 1.64 million GPU candidate
ticks and 105,216 exact CPU shortlist ticks, reaching route 433 over 471 ticks.
Output-head-only distillation at a 10% trust step produced the current best
artifact,
`utopia-user-native-route-continuous-recovery-r4-output-s010.json`: exact CPU
route sample 485 and reset after 500 episode ticks. It is one 256-wide,
history-enabled, phase-free neural network with no action schedule, state
memory, gated adapter, replay clock, or per-ramp branch. This is only about
24.6% of the 1,969-sample route and is not map completion.

Promote only when both exact-CPU ordered progress and survival improve. A
second local recovery pass from that best artifact regressed to route 324/345
ticks, and a PPO-only output update reset after 292 ticks. These failures show
that the next bottleneck is long-horizon teacher credit and teacher-to-policy
distribution shift: locally optimal receding-horizon commands can destroy the
global line. Do not widen the same MPC/PPO search, add ramp-specific rules, or
blame the ordinary floor reset without new evidence. Add ramp normals or
recurrence only if a state-ambiguity audit of v3 finds conflicting actions;
otherwise keep the single generalized network and build a globally non-myopic
teacher or rollout objective around the verified v3 state contract.

A GPU nearest-neighbor ambiguity audit supports that decision. It compared all
1,969 authentic states with a route-stratified sample of 7,826 stochastic
policy-visited states, excluding neighbors within 64 route samples. Among the
closest one percent of temporally nonlocal v3 observations there were zero
movement, yaw-over-five-degrees, jump, or duck conflicts. In the route-350--550
failure window, the closest five percent also had zero conflicts. Conflicts
appear only at materially larger feature distances. This is evidence against
adding ramp normals or recurrence for the current failure; it does not claim
that such inputs can never help elsewhere.

Surf commit `0b5d0d8` adds globally continued recovery scoring. Set
`cpu_continuation_ticks` on `policy.recover-native` to make ROCm screen the
wide local action-program set, then have exact CPU ranking commit only the
candidate's first control block and run the unchanged neural policy for the
remaining evaluation span. Global ordered progress and survival outrank local
route-state error. The parent and first-block axis probes are always retained
in the exact shortlist before GPU-ranked candidates. Zero keeps the original
local-horizon behavior.

The first global probe started the r4 controller at episode tick 400, used a
256-wide/128-tick ROCm screen, eight-tick commits, and 16 exact CPU
continuations. It screened 5,987,517 GPU candidate ticks in 41.82 seconds
(about 143k/s) and 879,421 authoritative CPU ticks in 360.08 seconds (about
2,442/s). The teacher reached route sample 1,427 (72.51%) and ran the full
1,969-tick span without a reset, compared with the frozen network's route
485/reset around tick 500. This is a strong global control bound, not a trained
controller or completion: progress stalled at 1,427 and the final state was
17,681 Source units from the reference. Clip recovery supervision before the
stall; never promote or visualize the teacher as if it were the network.

Ordinary distillation of that clipped teacher is also bounded and closed. With
r4 as parent, output-head trust scales `0.01`, `0.005`, `0.002`, and `0.001`
reached exact-CPU route/tick pairs 472/496, 380/464, 479/512, and 481/501. A
full-network `0.001` update reached 481/500. None improved both r4's 485/500,
so r4 remains the controller. The global teacher proves useful action sequences
exist; the remaining problem is transferring them without moving the sensitive
prefix. The next method should optimize one generalized network directly
against the global continuation objective with an explicit prefix-action trust
constraint, not repeat behavior cloning, add per-ramp logic, or widen the same
local search.

Surf commits `34677d5`, `66a25b9`, `6c4c1eb`, and `4f73034` add the clean
state-localized-capacity path. With `gpu_focus_episode_tick`,
`gpu_output_hidden_start`, and a positive `cpu_focus_screen_ticks`,
`policy.evolve-native` first proves every mutable output-suffix unit is exactly
inactive on the complete CPU parent prefix, then ranks every candidate suffix
in the exact CPU Source-brush world. ROCm remains a diagnostic proposal path.
Full authentic-start promotion is strict: both ordered route progress and
survival must improve. The ordinary version-4 ReLU artifact retains absolute
pose/motion plus one previous action and has no phase feature, adapter, route
table, action schedule, or state memory.

Staging that capacity produced the strongest clean controller so far. A
576-wide first stage advanced the old 512-wide route/tick pair `894/993` to
`1164/1287` (`50.535%`). A 640-wide second stage, silent through episode tick
999, reached `1214/1306` (`53.397%`). A 704-wide third stage, silent through
tick 1049, reached `1222/1349` (`53.9207%`). Its artifact is
`utopia-user-native-clean-capacity192-stage3-teacher-centered-v1.json`.
Complete CPU rollouts reproduce those results. Global nearest-authentic route
index is still unsafe at the failure: it is 1275 while ordered progress is
1222, a 53-sample phase leap. Keep absolute pose/history and do not add nearest-
route conditioning or ramp geometry from this evidence.

A fourth 32-unit bank was rejected rather than promoted. Exact CPU coherent
exploration from tick 1100 found three genuine joint suffix winners; the best
reached route 1232 over 268 suffix ticks versus the parent's route 1222 over
249. The bank was exactly silent through tick 1099 and active on about 98% of
the relevant parent/recovery tail. Nevertheless, multi-elite and single-elite
supervision, with and without post-focus zero rehearsal, two 256-candidate
exact-CPU output-suffix searches, and a 32-point per-output closed-loop scale
grid produced no complete-start gain on both axes. The three elites also gave
different residuals at their shared start, up to 0.198 apart on one component.
Adding 2, 4, or 8 previous actions did not reduce nonlocal recovery conflicts;
the measured over-0.05 conflict fraction rose from about 46% with one previous
action to 57% with eight. This does not support recurrence or a wider bank.
Retain the 704-wide third stage as clean authority.

`tools/train_native_capacity_gpu.py --output-step-scales MOVE_X MOVE_Y YAW
PITCH` exports independently scaled continuous rows from the same trained
prefix-safe bank. Tensor checks showed that only the requested suffix row
changed. On the rejected fourth stage, a pitch-only 0.01 step survived 76 ticks
longer but lost seven route samples; movement-x/yaw preserved the parent and
side movement regressed it. No bounded combination retained that survival gain
while improving route.

Surf commit `b519160` therefore adds opt-in pitch proposals to
`policy.recover-native` via `pitch_scale_degrees`. The default remains movement
plus yaw only. Globally continued evaluation automatically reserves the parent
and all first-block axis probes in the exact CPU shortlist (raising
`cpu_top_k` to at least nine when pitch is enabled), because fixed-schedule GPU
physics cannot score pitch's later neural-feedback effect. A measured 0.025--
0.1 degree sweep selected one corrective block and advanced route 1222 to 1223,
but reset one tick earlier. Its trace is diagnostic supervision only, not a
controller promotion; the stronger coherent `1232/+19` suffix bound remains
the relevant teacher evidence.

Surf commit `d002024` adds an opt-in common-random-number robustness objective
to the exact CPU focus screen. Set `cpu_focus_perturbations` to an odd value
above one and provide at least one of `cpu_focus_position_noise`,
`cpu_focus_velocity_noise_fraction`, or `cpu_focus_yaw_noise_degrees`. Every
candidate then receives the same nominal state and deterministic antithetic
perturbation pairs, and candidates rank by their worst exact-CPU route progress
and survival. The receipt counts every scenario tick. The prefix-inactivity
proof and strict complete-start CPU promotion gate are unchanged; ROCm remains
diagnostic.

On this desktop, explicitly keep `gpu_population` at or below 32 for subsequent
controller work and do not overlap ROCm search with rendering. A compositor
failure previously coincided with a long 256-wide batch. The robust 8- and
32-candidate checks used 0.60 and 1.07 seconds of GPU time, returned to 1% idle
and 6% VRAM, and left the junction at 58--61 C. The 32-wide exact-CPU screen
used three shared scenarios and found all candidates tied at route 1222; its
search-only center improved a reset-penetration tie-break but reproduced the
same full-start route and reset. It is not promoted. Keep
`utopia-user-native-clean-capacity192-stage3-teacher-centered-v1.json` as the
clean controller authority.

Surf commit `982728f` adds a final constrained localization diagnostic:
`gpu_hidden_bias_suffix_start` selects an ordinary second-hidden suffix, proves
every selected ReLU is inactive over the exact CPU prefix, and permits only
downward bias mutations. Those mutations cannot wake a prefix unit or alter any
other tensor. A positive exact CPU route-survival screen ranks the entire
population, while ROCm remains diagnostic and complete authentic-start CPU
route-plus-survival gains remain the only promotion path.

This hidden-threshold family is also bounded and closed. Exact CPU coherent
action noise from episode tick 1200 produced no route or survival improvement
over the unchanged parent in 32 matched states. Uniform downward shifts of the
11 fourth-bank biases by 0.025, 0.05, 0.1, 0.2, 0.35, and 0.5 reached full-start
route/reset pairs 1216/1425, 1216/1427, 1220/1347, 1213/1305, 1222/1349, and
1222/1349; none improved both clean-authority axes. The compositor-safe
downward-only search then used populations 8 and 32. The 32-wide GPU suffix
took 1.00 seconds, returned to 1% idle and 6% VRAM at 59 C junction, and
generated route variation from 1213 to 1222. Its three-scenario exact CPU rank
disagreed with the ROCm order (Kendall tau about -0.36), did not update the
search center, and retained the unchanged 1222/1349 authority. Do not spend
more rollout budget on output-only or hidden-threshold searches. Revisit the
end-to-end representation, data distribution, and learning objective that made
the earlier controller successful before proposing another optimizer.

Surf commit `ad6cd04` identifies why the earlier clean reproduction was not a
faithful or valid distillation. The Python `policy_teacher_targets` evaluator
omitted v7002's final and completion state-feedback branches. Those branches
materially affect 329 states from episode tick 1639 onward, with a pre-clipping
output difference as large as 2.60. The repaired evaluator matches commands
emitted by the Rust runtime over 1,967 states to maximum movement error
`6.20e-6`, maximum yaw error `5.78e-5` degrees, maximum pitch error `1.20e-7`,
and zero jump or duck disagreements. Corrective exports now also retain analog
movement RMSE/max/over-0.05 and maximum yaw error; the old
`movement_accuracy` measured signs only and hid magnitude error.

Faithful labels alone are not enough because v7002 is not an off-trajectory
expert. On its own trajectory, the old clean seed already differs by over 0.05
movement on 11% of the first 400 states despite its reported 98.94% movement-
sign accuracy. A compositor-safe 250-epoch fine-tune took 0.89 seconds of GPU
time and improved same-dataset movement RMSE from 0.165 to 0.066 and yaw MAE
from 0.458 to 0.316 degrees, yet its exact CPU rollout regressed from episode
reset tick 742 to 432. A new CPU-only `diagnostic_switch_policy` handoff then
proved the teacher mismatch directly: clean seed to v7002 at tick 400 reset at
545, and the stronger stage-3 controller to v7002 at tick 800 reset at 1033,
versus unchanged reset ticks 742 and 1349. The handoff is explicitly clocked,
diagnostic, nonexportable, and nonpromotable.

Do not use v7002 actions as DAgger labels on clean-controller states. Its 91.8%
result depends on its own phase-gated state distribution. The next clean
training method needs a true exact-CPU receding-horizon expert that labels the
student's visited states by ordered progress and survival, followed by ordinary
phase-free network fitting and complete-start CPU validation. This is a data
and closed-loop objective correction, not another width or mutation sweep.

Surf commit `35da440` adds a bounded TrackMania-style off-policy baseline and a
per-tick-consistent exploration path. `tools/train_native_sac_gpu.py` trains
twin critics and one ordinary phase-free actor from replay, holds out complete
trajectories for return-ranking diagnostics, and can restrict actor gradients
to continuous-action rows in a selected ordinary output suffix. The restriction
is structural: all earlier columns, both button rows, every hidden tensor, and
the output bias must remain bit-identical. A function-equivalent prefix policy
is accepted only after exact output equality on every protected feature. The
trainer aborts if a frozen parameter or protected-prefix output changes.

The first calibration closed two unsafe update paths. A full 704-wide actor
update changed protected-prefix outputs by only `1.36e-4` at worst but still
regressed from reset tick 1349 to about 734, so soft behavior penalties are not
a closed-loop trust region. The existing 736-wide zero-control expansion is
bit-identical to the 704-wide parent on all 1,349 parent states; its final 32
units are inactive through tick 1099 and all activate in the tail. On the old
held-noise replay, a critic-only ROCm run took 2.10 seconds and reached held-out
return rank correlations `0.938` over transitions and `0.737` at trajectory
starts. A hard-suffix actor update kept protected-prefix and frozen-parameter
deltas exactly zero, but exact CPU reset at absolute demo tick 1527 versus the
parent's 1589. Reject that candidate.

The high critic rank was confounded by the replay contract. Old exploration
held one sampled residual for 32--64 ticks and also perturbed each initial
position, velocity, and yaw. Its first action therefore identified a future
control program and a different recovery state, while deployment recomputed an
action every tick from the nominal state. Do not use held-noise diagnostics as
ordinary one-step SAC actor data. The ROCm and CPU collectors now accept
`exploration_autocorrelation`: a stationary AR(1) residual updated every tick,
mutually exclusive with hold lengths above one. With identical seed, 256
matched CPU/GPU transitions agreed on unclipped exploration residuals within
`5.96e-8`, rewards within `1.20e-7`, and had zero route, reset, termination, or
truncation disagreements.

Keep smooth collection at compositor-safe width 32. Four sequential exact-state
AR batches (128 total candidates, correlation half-life 32 ticks) reached best
route indices 1213, 1213, 1211, and 1211 versus the parent's 1222; do not train
or tune on that uniformly worse set. A causal replay also separated the old
route-1232 elite: the identical perturbed state with zero action residuals
reached 1217, proving that the held program genuinely recovered that displaced
state, but the same program from the exact nominal parent state reached only
1214. It is recovery evidence, not a nominal route improvement. Any continuing
off-policy loop must collect from the current actor with matching per-tick
semantics and keep nominal advancement separate from perturbation recovery in
both critic validation and promotion. The 704-wide stage-3 policy remains the
clean authority.

Surf commit `2bc35db` turns that probe into a persistent off-policy training
loop. `policy.collect-native-ppo` now has an opt-in `route-time` reward: ordered
route distance earns reward, every simulated tick pays a cost, and
`stall_ticks` ends a no-progress trajectory instead of rewarding it for staying
alive. `exploration_ticks` limits action noise to an initial control window and
then returns control to the frozen actor for continuation scoring. CPU and ROCm
share the same reward, stall, and continuation contract. An 8x64 route-time
differential had identical rewards, route indices, and terminal decisions; a
separate autocorrelated 8x64 continuation check had identical aggregate
results, including three stall terminations.

`tools/train_native_sac_gpu.py` now supports multi-step targets, persistent
actor/critic/optimizer checkpoints, conservative TD3+BC and advantage-weighted
actor objectives, and explicit matched-rollout actor eligibility. Critics may
learn from the complete replay while the actor sees only trajectories proven
better than their deterministic control. Checkpoint resume requires the input
policy to reproduce the saved actor exactly. The deployed artifact remains one
ordinary phase-free network; critics, normalization, replay, and optimizer
state are training-only.

Three compositor-safe 32x256 ROCm batches collected 19,500 route-time
transitions in under one second per batch and covered authentic starts through
route index 1710. Plain SAC and TD3+BC made large off-distribution actor moves
and failed GPU validation. Persistent advantage-weighted regression kept the
maximum prefix delta below `0.0048` and slightly improved matched local GPU
reward, but exact full-start CPU playback reset at absolute tick 979. Do not
promote it.

The prefix-inactive 736-wide bank then held the first 1,100 CPU ticks exactly.
Paired exact-CPU continuation scoring found only two noisy rollouts that
improved both route and survival. Training only on those globally valid
continuations still regressed the full-start GPU route, while broader matched
training produced GPU gains that exact CPU rejected: route 1219 or 1217 versus
the unchanged parent's 1222 at the same reset tick 1589. This closes the fourth
absolute-pose bank under SAC, TD3+BC, unfiltered AWR, matched local AWR, and
globally continued AWR. Retain
`utopia-user-native-clean-capacity192-stage3-teacher-centered-v1.json` as the
clean authority. The next architecture should use the already-verified
`native_route_continuous_v3` path geometry and actor-visited collection rather
than another absolute-pose suffix or scale sweep.

Surf commit `84a9423` applies that correction to the existing continuous-route
actor. The off-policy trainer accepts `native_route_continuous_v3`, can freeze
the hidden representation while fitting the ordinary output layer, can limit
actor labels to matched elite rollouts and their actual exploration window,
and retains all replay for critic fitting. The export is still one ordinary
phase-free, history-enabled network with no schedule, state memory, adapter, or
per-ramp branch.

Use fixed action persistence for exploration rather than per-tick AR noise when
collecting this lineage. With a 16-tick hold, one compositor-safe 32-wide GPU
batch from the authentic start found route 694 over 727 ticks, compared with
the frozen route actor's GPU 380/466. Long-horizon GPU ranking was not a safe
teacher: the matched exact-CPU batch selected a different rollout at route
688/728. Distilling only that exact rollout's first 512 exploratory ticks into
the ordinary output layer produced
`utopia-user-native-route-continuous-r4-elitebc-output-cpufullstart8-r1.json`.
Two independent exact-CPU starts both reproduced route 638 over 759 ticks;
ROCm reached route 639 over 774 ticks. The prior route-v3 controller reached
only about route 481/501 in the same collector contract. Across 16 matched
small CPU state perturbations, the new actor improved mean route by 12.625,
mean survival by 3.1875 ticks, and mean route-time reward by 6.60, although it
regressed several individual cases. It is the route-v3 training parent, not a
completion or robustness claim, and it remains below the 704-wide absolute-
pose clean authority at route 1222.

The next focused collection bound is also explicit. From the new actor's exact
episode-tick-500 state, a 32-wide 16-tick-held GPU screen proposed route 722;
the exact CPU replay reproduced the leading candidate at route 723 over 264
suffix ticks versus the parent at 638/258. Softly anchoring the first 500
actions while fitting that suffix still regressed the complete GPU start to
476/495, so do not promote it or line-search the same fit. Retain the route-638
actor. Future continuation needs exact prefix preservation inside one ordinary
network, or a new globally validated actor update.

Surf commit `38e22bd` removes the underlying host/GPU split for both supported
clean policy schemas. `native_pose_v1` and `native_route_continuous_v3` now use
the same owned C++/HIP-compatible implementation for movement, Source-brush
collision, triggers, ordered-route state, continuous controller-route state,
feature extraction, yaw history, ReLU inference, and action decoding. Route-v3
points remain in their artifact's prototype units instead of being converted
to Source units and back, and route-only length/normalization uses the same
deterministic arithmetic on both targets.

The installed-Utopia regression runs ordinary mutated policies through all
1,969 ticks on both backends and requires exact equality for route progress,
reset timing, first action, final position, and final velocity. Both clean
schemas pass. For these schemas, ROCm is therefore the training and selection
backend rather than a proposal engine awaiting a separate CPU-physics verdict.
Host reruns remain useful as parity audits and playable-game integration tests,
and promotion still requires better complete-start progress and survival,
bounded perturbation robustness, natural completion, and in-game reproduction.
Any future schema or core change must re-establish this full-rollout equality
before production training uses it.

Surf commits `4049b96` and `d434b82` extend the unified route-training loop
without changing the deployed controller contract. An explicitly requested
`policy.evolve-native` rollout may now continue for up to 5,000 ticks beyond
the recorded demonstration episode, using the final route state only for
post-episode trajectory statistics. Policy-prefix collection can start in the
same extended range. Unified route ARS may retain a separate strict promotion
anchor, so a high-progress resetting student can seed search without replacing
a lower-progress no-reset controller. The capacity trainer can relabel a
candidate's actual visited states with a coherent teacher trajectory, verifies
its NumPy parent inference against runtime means, and requires every DAgger
rollout to begin immediately after the protected prefix. The export remains
one ordinary phase-free, history-enabled ReLU network.

This closed-loop DAgger plus anchored-ARS cycle produced the strongest clean
controllers so far. The prior 544-wide authority reached route 1757 and ran
all 2,600 requested ticks. A route-1936 coherent action teacher produced a
route-1858 resetting DAgger student; four 256-wide unified ARS generations
then promoted a no-reset route-1799 controller, and four more reached route
1801 (`89.2595%`). A fresh route-1902 action teacher produced a route-1876
resetting student, whose 672-wide descendant reached route 1802 (`89.3203%`)
without resetting. A playable audit exposed that one-sample result as a false
leader: it reached its last sample 324 ticks later than route 1801, reduced
fitness from `0.82048` to `0.79728`, increased terminal velocity error from
`4475.83` to `6119.84`, and slowed from about 2,477 Source units/second at
episode tick 1700 to 169 at its final progress tick and 61 at tick 2600.
Retain
`utopia-user-native-route-continuous-capacity256-dagger2-anchored-direct-s1e3-g8-p256-v2.json`
as the strict clean authority. The route-1802 artifact is a wider search center
only. Neither controller reaches the terminal trigger, so neither is map
completion or parity evidence.

Surf commits `75cd6da` and `8dc3a2e` correct that objective. Unfinished
promotion now requires strict ordered-route advancement without regressing
2,600-tick survival, route pace (`progress_tick - furthest_index`), or
continuation fitness. Optional common antithetic start perturbations rank by
their worst route, survival, pace, and fitness, and require the same strict
improvement. The default one-scenario path retains the original population
without duplicating every policy tensor; robustness expansion occurs only when
explicitly requested. A 4-candidate/3-scenario smoke rejected route 1802,
retained route 1801, and had exact final GPU/host route, pace, fitness, timing,
and terminal-error parity. The full ROCm suite and installed-Utopia parity test
pass.

For the unified headless route lineage, 256 candidates are productive when no
renderer is active: the 544-wide controller sustained about 31.7k--33.0k
candidate ticks/second over 2,600-tick rollouts, while the 672-wide controller
sustained about 21.6k--22.8k. Do not overlap ROCm training with the playable
renderer. The older 32-candidate cap remains appropriate for legacy focus
screens that run substantial host work per candidate, not for this full GPU
population path.

Surf commits `2698c9a` and `8506e8c` make the next clean-capacity experiment
reproducible. A wider resetting search center now falls back to its own
architecture-compatible seed until that lineage passes the strict promotion
gate; resetting it directly to a narrower authority had sliced prefix samples
at the wrong width. DAgger can also align a student's actual visited states
monotonically to a coherent teacher by continuous route progress plus bounded
route-relative physical-state distance instead of using invalid absolute-tick
labels. Both changes retain the same ordinary phase-free network and shared
GPU/host movement core.

That state-aligned supervision fixes the observed late slowdown but not the
next landing. A 672-wide student reached route 1881 (`94.20%`) with pace lag
142, and movement/yaw row ablations reached route 1887 (`94.58%`), but all
reset around absolute demo ticks 2314--2338. Movement-only and single-axis variants
also reset, ruling out the prior survival-biased slowdown as the remaining
failure. Exact GPU/host selection parity held and the strict anchor retained
route 1801.

From the fast controller's actual episode-tick-1900 state, a 256-wide coherent
action search found a route-1944 trajectory, but it also reset and is a teacher
only. A prefix-silent 128-unit bank fitted to that trajectory produced a 10%
trust variant at route 1904 (`95.75%`) and delayed reset to absolute demo tick
2385. Separating
its learned rows reached at most route 1905 (`95.82%`); forward, lateral, yaw,
and combined movement variants all still reset. A second state-aligned round
stayed in the same resetting basin. Do not promote or visualize these
trajectories. Retain the route-1801 no-reset controller as the strict clean
authority until a fast candidate also survives the full 2,600-tick audit.

The playable route-1905 audit hit teleport trigger 56 at policy tick 2121,
position `[-2668.72,2086.80,7962.08]` and velocity
`[45.27,-1920.24,41.28]`. Adding the first-attempt demo offset of 240 gives
absolute tick 2361, exactly matching the headless reset receipt. This verifies
playable/headless reset parity for the fastest diagnostic candidate; it does
not make that candidate promotable.

The nominal route-1905 count was also physically misleading. At its final
progress sample the controller was free-falling at about 2,398 Source
units/second and merely passed 509 units from a later reference point, just
inside the 512-unit corridor. At 256- and 128-unit corridors both that
candidate and route 1801 fall back to the same earlier route region, so a
narrower corridor is not a usable fix. Surf commit `d664f9a` instead ranks
prefix-constrained optimization and globally continued recovery by full-span
survival before route progress, makes the zero-update ARS center an explicit
backtracking option, removes the small-mutation learning-rate floor, and
preserves the continuous policy runtime's previous route position across
focused suffixes. The latter state was required for exact sequential recovery:
without it, locally selected continuations compounded into route 1683--1734;
with it, the controlled recovery audit reproduced route 1801 exactly with no
reset through 2,600 ticks.

After that parity repair, a bounded 256-program late recovery search starting
at episode tick 1850 produced a genuine route-1803 no-reset teacher trace at
`.borg/surf-lab/episodes/policy-native-gpu-recovery-35511cd9-111d-4b3f-b5b8-f5a39f032e92.jsonl`.
A wider yaw, pitch, and movement search reached only route 1802. Route 1803 is
valid state-aligned supervision, not a learned controller or a new authority;
retain route 1801 until an ordinary NN independently exceeds it and passes the
same full-span host/GPU audit.

Surf commit `09b9f44` fixes the remaining prefix-search lock. The old
`gpu_route_prefix_ticks` path constrained early actions but still scored every
perturbation from spawn, so tiny pre-branch divergence could erase the late
continuation signal before the requested branch. Prefix-constrained ARS now
captures the frozen controller's full physical, route, and action-history
state at the requested tick, learns from paired continuations that share that
exact state, and separately reruns every possible promotion from the authentic
start through the full horizon. The stored controller remains one ordinary
phase-free network; the branch exists only inside training evaluation.

Equal-survival branch scores now use bounded ordered progress, route pace, and
continuation fitness while keeping each extra survival tick lexicographically
dominant. A tick-1400 smoke captured route 1375 and evaluated a 1,200-tick
continuation without altering the promotion gate. Perturbations of `1e-3` and
`1e-5` were too unstable from spawn; at `1e-6`, a bounded 2-generation,
256-candidate validation retained full 2,600-tick survival and exact final
host/GPU selection parity but did not improve route 1801, its 162-tick pace
lag, or fitness. Do not promote the resulting search artifact. Retain
`utopia-user-native-route-continuous-capacity256-dagger2-anchored-direct-s1e3-g8-p256-v2.json`
as the strict authority.

An activation audit of that 544-wide authority found an ordinary second-hidden
bank at indices `352..544` that is exactly inactive through episode tick 1699,
then activates for roughly 240 ticks before the route-1801 stall. This permits
output-suffix ARS to preserve every earlier action exactly without a clock,
gate, adapter, or deployed branch. A calibrated tick-1700 probe at `sigma=1`
produced useful route/survival variation, but it also exposed a continuation
bug: one candidate was scored as surviving the full 900-tick branch while its
complete-start rollout reset only 405 ticks after the same branch point.

Surf commit `38b0cac` fixes that false continuation by carrying the continuous
policy tracker's previous route position and the pending duck transition
through the ROCm neural initial-state ABI. Structurally exact dormant-bank
searches now enforce that every branch candidate reproduces its complete-start
suffix exactly in survival, route progress, and terminal physical state. The
post-fix 64-candidate replay had eight survivors in both views and zero
disagreements; the corrected 256-candidate validation checked all 256 suffixes
exactly and retained GPU/host selection parity.

That corrected wide search still did not produce a promotable network. Its
best full-span survivor reached route 1797, while its fastest candidate reached
route 1907 but reset at episode tick 2122. The emitted artifact is tensor-for-
tensor identical to the route-1801 authority. Keep the route-1907 result as
search evidence only, and retain
`utopia-user-native-route-continuous-capacity256-dagger2-anchored-direct-s1e3-g8-p256-v2.json`
as the strict controller authority.

The second exact commitment point at episode tick 1200 was also tested rather
than assumed away. Its `288..544` suffix remained exactly inactive over the
prefix and all 64 branch suffixes matched their complete-start rollouts. A
`sigma=1` probe was over-scaled by a hidden-activation spike near tick 1550; a
profile-calibrated `sigma=0.25` probe retained 29 full-span survivors, but the
best reached only route 1682 and the fastest resetting candidate only route
1786. This earlier branch does not justify a wide generation.

Surf commits `e7945f0` and `47d84ba` then made promising branch directions
usable without weakening promotion. Direction-specific scale backtracking
retries a bounded set of coherent frontier directions down to a zero-safe
update, and the training center may follow a complete-start candidate beyond
the authority even when that candidate is not yet promotable. Exact searches
from ticks 1700 and 1200 showed that merely shrinking the previously promising
directions did not recover survival, but a three-generation 64-wide search from
tick 1700 produced the first genuine nominal improvement: route 1900 with all
2,600 ticks survived, pace lag 142, and fitness `0.8842321`. GPU and host
shared-core rollouts matched exactly, and a hidden playable audit reached the natural
rollout boundary. Visually it carries the transfer and strikes the front of the
next ramp; this is a geometric near miss, not the earlier free-fall corridor
artifact. The nominal artifact is
`utopia-user-native-route-continuous-capacity256-true-branch1700-suffix352-s1-p64-survival-frontier-center-g3-v1.json`.

Do not call route 1900 robust or complete. Under the bounded five-scenario
position, velocity, and yaw audit, its worst case reached route 213 for 281
ticks versus route 214 for 282 ticks from the old route-1801 authority. A
robust-aware repair search found no acceptable replacement. Route 1900 is the
verified nominal/playable leader; route 1801 remains the strict perturbation
authority until robustness improves.

Surf commit `47e8594` preserves a farther full-survival training center instead
of letting a later resetting endpoint overwrite it, and permits prefix-
conditioned route ARS to emit that center as an explicitly resume-only artifact.
The resulting route-1903 checkpoint survives all 2,600 ticks but regresses to
pace lag 153 and fitness `0.8825033`, so it is not promoted. A bounded local
repair at `sigma=0.1` sampled resetting trajectories as far as route 2180 but
found no candidate that advanced beyond route 1900 while preserving its pace,
fitness, and survival. Retain route 1903 only as a search center and avoid blind
additional sweeps of this branch.

An exact contact microscope confirms why route 1900 looks close in the playable
game. At policy tick 2044 it strikes Source brush 3401, plane 5890, with normal
`[0,0.3420,0.9397]`; speed drops from about 2,533 to 512 Source units/second in
one tick. The authentic run does not touch that brush. Its next surf chain
starts on brush 3398 and continues through brushes 3402, 3405, and 3408 at
roughly 3,190--3,209 units/second. The miss is therefore a real wrong-surface
commitment rather than route-corridor scoring or CPU/GPU collision drift.

Bounded recovery localized the commitment before the visible impact. Starting
at episode tick 1980 could not exceed route 1900. Starting at tick 1900 found an
exact CPU-evaluated 48-tick program at tick 1916 that reached route 1901 and
survived the full 2,600 ticks, but ordinary receding replanning later overwrote
that valid branch and reset. Surf commit `2bd03f2` adds a training-only
single-global-intervention mode: it advances through baseline decisions,
commits the first globally evaluated nonbaseline program, then resumes the
frozen NN. Replaying seed 77131 reproduced route 1901 with one intervention and
no reset. This trace is corrective supervision only, not NN progress or a
deployable controller.

Surf commit `ce7bd4f` lets the corrective GPU trainer update only hidden-output
columns at or after an explicit suffix index while preserving the earlier
columns and output bias bit-for-bit in the exported artifact. Fitting the
route-1901 intervention into the 544-wide controller's `352..544` bank did not
distill the gain. Full, 10%, 3%, 1%, and 0.1% update scales all regressed to
route 1866--1896 and reset between ticks 2302 and 2388. An exact zero-update
export retained every model tensor and reproduced route 1900, all 2,600 ticks,
pace lag 142, and fitness `0.8842321`, proving the failures are genuine
closed-loop sensitivity rather than exporter metadata drift. Do not promote
any suffix-fit artifact; retain route 1900 as the nominal leader and route 1801
as the strict perturbation authority.

A final targeted closed-loop use of the route-1901 trace was also negative. A
one-generation, 256-candidate antithetic output-head search at `sigma=1e-6`
screened against that guidance while retaining complete-start promotion. None
of 258 evaluated policies exceeded route 1900; the emitted artifact is tensor-
for-tensor identical to the input controller. Do not continue this guidance
path as an unbounded scale or generation sweep.

The follow-up audit rejected automatic backward-restart ranking before it was
made into another optimizer. The same 32 ordinary full-network antithetic
directions were evaluated from the authentic start and from four geometric
lookbacks behind the route-1900 center's own last-progress tick. Paired
advantage correlations back to the authentic start were `0.0183`, `0.00049`,
`0.2833`, and `-0.3841`, with sign agreement `53.1%`, `59.4%`, `59.4%`, and
`34.4%`. Local branch gains therefore do not reliably select spawn gains. The
diagnostic source was removed after this decision; do not build multi-anchor
ranking or run a scale sweep from this result.

Two function-preservation alternatives also failed without producing neural
progress. Mixing all parent runtime actions with the 32 genuine correction
actions reached low supervised loss but reset from spawn at tick 699. A
training-only 64-unit ordinary ReLU capacity fit was then forced exactly
inactive through the state preceding the correction. Its first deployable fit
reached only route 1891 and reset at tick 2339; projecting the exact inactivity
constraint after every optimizer step found no unit that could remain inactive
on the complete prefix and activate on the correction/tail. Both experimental
trainer edits were removed. Do not tune weights, margins, widths, or epochs on
these formulations.

The apparent conflict is not a Python/runtime feature mismatch. On 1,969 exact
route-1900 states, the Python continuous-route features match the shared
runtime after the two intentionally incomplete startup-history rows with
maximum error `6.71e-7` and no row above `1e-3`. A map-general representation
probe confirmed high local action aliasing around the corrective states, but
replacing the four demonstration-index yaw-lookahead fields offline with
policy-velocity-time future lateral/elevation channels did not reduce it:
10-neighbor movement disagreement changed from `72.5%` to `74.0%`, and mean
k-neighbor movement error from `0.1492` to `0.1512`. Do not add that route
schema or pay the CPU/ROCm integration cost.

The remaining clean direction is state-feedback data, not another branch
score, parameter trust scale, capacity graft, or Utopia-specific sensor. Build
the next training batch by relabeling a perturbed neighborhood of the policy's
own earlier visited states with bounded receding-horizon teacher actions,
replacing conflicting parent labels in that causal neighborhood rather than
adding one clock-like corrective block. The deployed authority remains one
ordinary phase-free NN, and complete authentic-start plus perturbation
rollouts remain the only promotion gate. Route 1900 is still only the nominal
leader; route 1801 remains the bounded-perturbation authority.

That state-feedback formulation was subsequently tested and rejected rather
than left as speculation. A bounded global-continuation relabeler produced
deterministic state-dependent first-action targets around the route-1900
policy's own visited states: 61 of 75 labels changed over the broad
route-1700--1940 window, and 63 of 80 changed over the dense causal
route-1876--1940 window. Output-suffix students nevertheless reset and
regressed to route 1658--1849. The experimental collector and trainer code was
removed. Keep only the discovered antithetic-state fix: the negative member
must mirror the continuous policy tracker's previous route position instead
of inheriting the positive member's position.

Surf route ARS now has an opt-in, training-only policy archive through
`gpu_route_search_centers`. It retains at most eight ordinary full-policy
lineages that are not Pareto-dominated by the current authority in survival,
ordered progress, and continuation fitness; among those, it preserves the
survival/progress/fitness extremes and fills by novelty in normalized
progress, survival, and pace. The least-visited retained lineage supplies the
next generation. This is optimizer state only: it adds no deployed input,
clock, phase, memory, ramp rule, adapter, or second controller, and strict
complete-start promotion still exports one stateless NN. A no-prefix smoke
confirmed that dominated early-reset lineages are discarded rather than kept
for diversity.

The same audit found and fixed a genuine GPU/host objective mismatch. ROCm
already tracked the best combined terminal position/velocity approach over a
trajectory, but `RolloutResult` exposed only final state, so GPU route fitness
reconstructed terminal error from the last tick while the host used the best
approach. The rollout ABI now carries the route tracker's best position,
velocity, and combined terminal errors directly. The installed-demo ignored
release test verifies shared-core full-rollout parity including those values.

With the corrected objective, a final four-generation, 64-wide, four-center
prefix-constrained run produced a genuine nominal route-1901 controller. It
survived all 2,600 ticks, improved pace lag from 142 to 140 and fitness from
`0.8842321` to `0.8855021`, reproduced every one of 256 branch continuations
from its exact prefix, and matched GPU/host final selection exactly. It is not
promoted: under the fixed five-state position/velocity/yaw audit its worst
case survived 280 ticks to route 226, while the route-1801 perturbation
authority survived 279 ticks to route 227. Do not call route 1901 robust or a
new authority, and do not turn this result into another open-ended seed or
scale sweep. Route 1900 remains the durable nominal leader and route 1801 the
strict perturbation authority.

Robust multi-center search now ranks and filters its training-only archive by
each lineage's worst perturbation outcome and the matching robust authority;
nominal search continues to use nominal outcomes. A four-generation, 64-wide,
four-center run on perturbation seed 77134 improved that seed's worst case from
266 ticks/route 176 to 282 ticks/route 218 while preserving nominal route 1901,
all 2,600 ticks, pace lag 140, fitness `0.8855137`, and exact GPU/host parity.
This was seed-specific rather than a robust promotion. On the held-out seed
77133, the candidate survived 280 ticks to route 226, versus 279 ticks to route
227 for the existing authority, so it failed the strict survival-and-progress
gate and was rejected. Do not promote or visualize it, and do not continue a
seed sweep. Route 1900 remains the nominal leader and route 1801 remains the
bounded-perturbation authority.

Surf commits `a320a0b` and `c2479d5` close a stricter robustness loophole.
Robust route promotion now requires scenario-by-scenario non-regression against
the authority, not merely a better aggregate worst case, and may train on
several fixed common-random-number antithetic batches with the nominal start
deduplicated. Expanded robust rollouts are launched in at most 256 GPU blocks,
rounded down to whole scenario groups; this preserves result order and avoids
the GPU hang seen when 64 policies and nine starts were launched as one
576-block kernel.

The nine-scenario smoke showed that the nominal route-1901 controller regresses
six matched perturbation starts even though its aggregate worst result looks
better. A single chunked four-generation, 64-wide run then screened 256 network
candidates on the same nine starts, retained exact GPU/host selection parity,
and found no strict improvement over route 1801. Keep route 1801 as the robust
authority and do not turn this result into a perturbation-seed or scale sweep.

Surf commit `52bf420` enables the same robust contract for ordinary full-network
ARS without a prefix. Every antithetic direction now starts at the authentic
spawn, mutates all core weight matrices, sees the identical fixed
common-random-number scenario set, and is scored for the full requested rollout
horizon. Promotion still requires nominal improvement, scenario-by-scenario
non-regression, strict worst-case improvement, and exact final GPU/host parity;
the exported controller remains one phase-free feed-forward network. The first
smoke caught an inherited progress-based 2,155-tick curriculum horizon before
the result was used; the corrected receipt reports route-index-zero starts and
the full 2,600-tick horizon.

One corrected eight-candidate smoke and one 32-candidate generation used nine
complete-start scenarios at full-network sigma `0.001`. The larger run had real
paired signal (`1.95247` RMS), but 13 of 16 directions were pinned to the same
worst failure at tick 266; the other three gained only a few worst-case survival
ticks while destroying nominal progress. Every accumulated-update backtrack
through scale `0.0009765625` regressed matched scenarios, so scale zero was
selected. The unchanged route-1801 authority survived all 2,600 nominal ticks,
and its final GPU/host route, progress, timing, and terminal errors matched
exactly. Do not promote either diagnostic artifact or repeat this worst-case
full-network sigma/seed search unchanged; the result identifies a worst-scenario
plateau and globally sensitive trust step, not a controller gain.

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
