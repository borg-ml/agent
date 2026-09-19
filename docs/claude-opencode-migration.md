# Claude and OpenCode ownership migration

## Subscription feasibility

Anthropic documents the authentication boundary at
<https://code.claude.com/docs/en/legal-and-compliance#authentication-and-credential-use>.
The current text prohibits third-party applications from routing user requests
through Free/Pro/Max credentials or collecting/intermediating Claude.ai tokens.
It explicitly permits an end user to sign into the unmodified Claude Code binary
with their own subscription, including on a platform hosting that binary.

Therefore Borg retains its existing unmodified-Claude-Code compatibility lane.
A native Claude OAuth replay adapter is not an acceptable replacement under this
published boundary. A native API-key/cloud route would be a different billing
lane and requires explicit selection; this migration does not enable one or
start a fresh sign-in. The existing `claude-agents` dependency is a Rust wrapper
for the CLI stream-json/control protocol, not a direct model API.

## OpenCode routes are separate services

The local route inventory, without exposing credential contents, contains
`opencode-go` and `groq`, both with API-shaped keys. Go uses its subscription
allowance; Groq is a separate API-key account, not a Go subscription fallback.
No credential files were copied or changed for this audit.

Borg already has a native Go model adapter in `provider/opencode_model.rs`.
It uses the Go endpoint and stable `x-opencode-session` header, with Borg-owned
tools and persistence. Other OpenCode routes retain the external compatibility
path. Go service endpoints are documented at <https://opencode.ai/docs/go/#endpoints>.
The docs list mixed preferred protocols, so the existence of a model in the Go
catalog is not evidence that every streaming/tool/thinking feature is compatible
with Borg. Cross-protocol and cache baselines must remain separate checks.

## First implementation checkpoint

- Go readiness no longer requires an OpenCode executable when the Go key is
  configured. Capability detail explicitly limits native availability to Go.
- Go catalog discovery no longer filters the service list through
  `opencode models`: the native adapter does not use that installed runtime.
- The explicit `opencode_go_access_probe` verifies admission, detailed readiness,
  subscription classification, and native catalog retrieval without model calls
  or credential writes.

With an empty PATH and seccomp denying both `execve` and `execveat`, the probe
passed both readiness modes and retrieved 37 models. The external CLI catalog
contained 27, all present in the service list. `cargo check -p borg-remote`
passed; provider tests passed 127 with 3 ignored, and remote tests passed 77
with 2 ignored. Targeted diagnostics reported no errors.

A rebuilt GLM-5.1 Go session also passed two OS-process runs under the no-exec
guard. The second process returned an exact random marker in a new assistant
event, not replayed output. Its usage reported 10,590 cached input tokens and
184 uncached input tokens. Cost basis was `unavailable`: readiness classifies
the Go subscription lane, but no equivalent-dollar usage price is inferred.
This is native cache/restart evidence, not measured parity against OpenCode.

Re-checked this round against the stored logs rather than taken on trust, and
all four figures hold exactly: provider tests `127 passed; 0 failed; 3 ignored`,
remote tests `77 passed; 0 failed; 2 ignored`, the native Go catalog 37 models
(`/tmp/borg-go-models.json`), and the external CLI catalog 27 entries
(`/tmp/borg-go-catalog-baseline.log`). The GLM-5.1 cache figures in this section
predate the current round's model restriction and are retained as history; the
current receipts are the Flash tables below.

## Flash two-process native cache receipts

Re-read from the durable journals rather than from a prior summary. Both models
ran two separate OS processes against one resumed session, under an empty PATH
with `LD_PRELOAD=/tmp/borg-no-exec.so` denying `execve`/`execveat`. The guard was
in force in every process: each journal carries a `mcp_server_unavailable` event
reporting `failed to start native MCP server unreal with executable bunx:
Operation not permitted (os error 1)`.

| Model | Process | input | cached input | cache creation | output | cost basis |
| --- | --- | --- | --- | --- | --- | --- |
| `opencode-go/glm-5.3-flash` | first | 10588 | 0 | 0 | 36 | unavailable |
| `opencode-go/glm-5.3-flash` | second | 146 | 10496 | 0 | 32 | unavailable |
| `opencode-go/deepseek-v4.1-flash` | first | 10533 | 0 | 0 | 24 | unavailable |
| `opencode-go/deepseek-v4.1-flash` | second | 218 | 10368 | 0 | 24 | unavailable |

In both second processes the exact random nonce came back inside a *new*
assistant message id (GLM `deba1ae9…` after first-process `2f9867eb…`; DeepSeek
`5f6041d0…` after `0bea5b39…`), so this is a fresh generation against reused
cache, not replayed transcript. State files: `/tmp/borg-go-flash-state.json`,
`/tmp/borg-go-deepseek-state.json`.

Limits. `cost_basis` is `unavailable` for every row: readiness classifies the Go
subscription lane but no equivalent-dollar price is inferred. Cached-token counts
are the service's own reported accounting, not an independently measured saving.

### Independently reproduced

The table above was re-read from artifacts produced before this round, so the
whole experiment was then re-run from scratch — fresh sandbox, fresh session,
fresh nonce — by `/tmp/borg-go-cache-repro.py`:

| Model | Process | input | cached input | output |
| --- | --- | --- | --- | --- |
| `opencode-go/glm-5.3-flash` | first | 10566 | 0 | 25 |
| `opencode-go/glm-5.3-flash` | second | 122 | **10496** | 25 |
| `opencode-go/deepseek-v4.1-flash` | first | 10515 | 0 | 26 |
| `opencode-go/deepseek-v4.1-flash` | second | 202 | **10368** | 26 |

Both second processes returned the new run's exact nonce inside a new assistant
message id. The notable result is that **cached input is bit-identical to the
original round** — 10496 for GLM and 10368 for DeepSeek — across independent
sessions with different nonces, while the uncached remainder moved slightly
(146→122, 218→202) with prompt wording. The cached prefix boundary is therefore
deterministic per model rather than incidental to one lucky run.

Guard provenance for these runs: `PATH=/nonexistent` plus
`LD_PRELOAD=/tmp/borg-no-exec.so`, whose constructor installs a seccomp BPF
filter returning `EPERM` for `execve`/`execveat` and calls `_exit(125)` if the
filter cannot be installed. Seccomp filters are inherited across `fork`, so the
whole process tree is covered. Verified in the identical environment:
`env -i PATH=/nonexistent LD_PRELOAD=… /bin/sh -c '/bin/echo …'` returns
`Operation not permitted`, and a preloaded `/bin/true` exits 0, confirming the
filter installed rather than silently failing. These runs used an empty
`agent.toml`, so unlike the earlier round there was no MCP server to fail and
produce an incidental denial event; the guard evidence is the filter itself.

## External server cache baseline and what it does not prove

`/tmp/borg-go-server-baseline.py` drove the installed OpenCode CLI
(`opencode serve`, v1.18.31) over its HTTP route with the same 512-row fixture
prefix and the same two prompts, on `opencode-go/glm-5.3-flash`.

Provenance: these particular figures came from an earlier session's run and
covered `glm-5.3-flash` only. They are retained below as history. The run was
originally deferred because it starts a local `opencode serve` process, which
was left pending an explicit decision. That decision has since been made — the
prohibition is on local *models*, and `opencode serve` is a localhost HTTP proxy
in front of the same hosted Go subscription, not an inference server — so the
baseline was approved and has now been collected first-hand for both models.

Earlier round, `glm-5.3-flash` only:

- round 1: total 10492, input 10379, output 113, cache read 0, cache write 0
- round 2: total 10575, input 10518, output 57, cache read 0, cache write 0

### First-hand run, both approved models

Run by `/tmp/borg-go-server-baseline-owned.py`, an owned adaptation of the
original script (the original and its receipt are left untouched). Each model
got its own `opencode serve` child bound to `127.0.0.1` on a random port with an
ephemeral unprinted password, startup bounded at 20 s and every request at 45 s.
Exact process count: **two** server children, one per model, each stopped by
terminating that one process handle; **four** hosted requests, two per model.
The receipt records a disposition on all four rows, which is per-request, not
per-process. Every round returned the run's marker and no round attempted a tool
call. Receipt: `/tmp/borg-go-server-baseline-owned.log`.

On the sandbox, claiming only what was actually checked: the global OpenCode
config declares no MCP servers at all — `~/.config/opencode/opencode.jsonc` is a
single `$schema` line and no `mcp` key appears in any opencode config — so there
was nothing for the child to launch. Each child was additionally passed
`OPENCODE_CONFIG_CONTENT={"permission":{"*":"deny"}}` and ran in a fresh
`mkdtemp`. Whether that variable replaces the global config or merges into it
was not verified, so no wholesale-takeover claim is made here; the absence of
any configured MCP server is what the no-other-apps assurance rests on.

| Model | Round | total | input | output | reasoning | cache read | cache write |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `glm-5.3-flash` | 1 | 10505 | 10383 | 122 | 0 | 0 | 0 |
| `glm-5.3-flash` | 2 | 10623 | 10530 | 93 | 0 | **0** | 0 |
| `deepseek-v4.1-flash` | 1 | 10301 | 10180 | 27 | 94 | 0 | 0 |
| `deepseek-v4.1-flash` | 2 | 10262 | 251 | 27 | 0 | **9984** | 0 |

**This corrects an inference in the earlier draft.** That draft had only GLM
external figures, saw `cache read 0` on both rounds, and read the contrast with
the native lane as suggestive of native prefix reuse. Adding DeepSeek shows the
behaviour is *model-dependent, not lane-wide*: on the external route DeepSeek
reported 9984 cache-read tokens on its second turn and its input collapsed from
10180 to 251, which is the same shape as the native lane. Second turns side by
side, native figures from the first-hand re-run above:

| Model | Native input / cached | External input / cached |
| --- | --- | --- |
| `glm-5.3-flash` | 122 / 10496 | 10530 / 0 |
| `deepseek-v4.1-flash` | 202 / 10368 | 251 / 9984 |

So the honest reading is narrower than before: for DeepSeek the two lanes are
comparable on this workload, and only GLM shows reuse on the native lane that
the external route did not report.

What this still does not establish. It is a *matched workload* — same prefix,
same prompts, same model, same hosted route — not a byte-identical request body,
because the two loops necessarily wrap the fixture differently; the lanes are
therefore not a controlled comparison. **`cache read 0` remains no proof that no
server-side caching occurred**: it shows only that the route reported no cache
tokens in its own accounting, which is exactly why the GLM row should not be
read as an absolute. `cost_basis` is `unavailable` throughout, so no dollar
figure follows. Nothing here is a parity or savings claim.

## Borg-owned tool execution, approval and cancellation

These runs deliberately do **not** use the blanket no-exec guard: Borg's `exec`
tool needs a subprocess, so a blanket `execve` denial cannot coexist with a tool
workflow. The strict model no-exec proof stays in the separate two-process smoke
runs above; these are a different experiment and are not evidence about the
model's own process behaviour.

Tool execution (`/tmp/borg-go-tool-exec.py`, `--permission full-access`,
`opencode-go/glm-5.3-flash`): the model issued one `exec` call, Borg ran it, and
`marker.txt` in the isolated project directory contained exactly the run's random
nonce. `tool_completed` reported `is_error: false`, final text `DONE`, exit 0.
Usage: input 4396, output 72, cached 0, cost basis `unavailable`.

Manual approval and cancellation (`/tmp/borg-go-tui-drive.py`, PTY-driven
fallback terminal, `--permission manual`):

- Approval: Borg rendered `? Run command` with the exact command and
  `Allow · y   Deny · n ›`. Answering `y` let the call proceed; `marker.txt`
  again matched the run nonce and the model replied `DONE`. Manual approval
  genuinely gates a real Borg-owned tool call on the Go native lane.
- Cancellation: during a "count to 4000" streaming turn, `/interrupt` stopped
  output mid-stream at `124` and returned the session to its ready prompt with
  no further tokens.

The same driver was re-run on `opencode-go/deepseek-v4.1-flash`, so approval and
cancellation are not single-model results:

- Approval: the same `? Run command` / `Allow · y   Deny · n ›` gate appeared,
  `y` released it, and `marker.txt` matched that run's nonce with final text
  `DONE`.
- Cancellation: `/interrupt` ended the turn at `412` of 4000. Unlike the GLM run
  it drained roughly a dozen already-streamed lines after the command before
  stopping, so cancellation is effective but not instantaneous at the output
  boundary. Worth re-checking if exact cancellation latency ever matters.

One trap worth recording: with an interactive TTY, Borg launches a *detached
session host*, whose own stdin is closed. Its EOF branch answers any pending
approval with `ApprovalDecision::Deny`, so an approval prompt is auto-denied
before a driver can answer it. The receipts above use `--ephemeral`, which keeps
the session in one process (`should_use_detached_session_host` requires
`!args.ephemeral`).

## Defects found on Borg-owned control paths

All three were pre-existing in committed `HEAD`, not working-tree edits. They
were reported first and then, on the parent's decision, repaired. Each is
described below as originally diagnosed; the repairs and their receipts follow
in the next section. All three repairs have since been committed in `86ed7db`
("Add watcher yields and harden agent integrations").

**1. Compaction cannot run on the OpenCode Go native lane.** `/compact` failed
immediately with `Error: OpenCode native sessions require an explicit model`,
even when the session was started with `--model opencode-go/glm-5.3-flash`.
It reproduces identically on `opencode-go/deepseek-v4.1-flash`, so the failure is
lane-wide rather than model-specific.
`AgentTurnExecutor::compact_native` (`crates/borg-agent-runtime/src/agent.rs`)
receives `model: &str` and passes it to `.compact(provider, model, …)`, but the
preceding line calls `with_model_access(provider, &access)`, which forwards
`None`. For `CodingProvider::OpenCode` that reaches
`with_opencode_go_access(None, …)` and bails at
`crates/borg-agent-runtime/src/native_harness.rs:203`. The apparent fix is to
call `with_model_access_for(provider, Some(model), &access)`, matching the turn
path; this needs the owning agent's review, so it is reported, not applied.

The asymmetry is visible in the call sites: the turn path uses
`with_model_access_for(turn.provider, turn.model.as_deref(), &access)`
(`native_harness.rs:137`) and works, while the compaction path uses the
model-less wrapper and cannot.

This is not limited to someone typing `/compact`. Automatic, threshold-triggered
compaction takes the same route: when a native-harness session crosses
`AUTO_COMPACT_REMAINING_PERCENT` (5%, i.e. at 95% of the context window) the
prompt loop calls the same `executor.compact_native(...)`, so it fails
identically. The failure is handled gracefully rather than fatally — the error
branch records `context_compaction_failed` plus an `Error` event and explicitly
continues "without discarding history" — but because the threshold check runs on
every prompt, a long Go session past 95% will retry and fail on *every*
subsequent turn, emitting an error each time while context keeps growing toward
the provider's hard limit. It degrades into a repeating error loop rather than
crashing.

Evidence status: the manual path is empirically proven (reproduced on both
models). The automatic path is established by reading the call site, which
invokes the same function already proven to fail; it was not driven to the 95%
threshold, because doing so means pushing roughly a full context window of
subscription tokens through the lane for a predictable result.

*Same pattern, not independently exercised:* `AgentTurnExecutor::consult`
(`agent.rs:1010`) also resolves an explicit model and then calls
`with_model_access(...)` with `None`, so peer consultation should fail the same
way on the Go native lane. It was fixed together with the compaction call site
in the same repair.

**2. `BORG_AGENT_TOOL_PROVIDER` is serialized in the wrong vocabulary.** Borg
spawns `borg __agent-mcp` as an MCP server (`subagents.rs::external_mcp_server`)
and passes `BORG_AGENT_TOOL_PROVIDER = provider.catalog_backend()`.
`catalog_backend()` is kebab-case (`open-code`), while the receiving end parses
`CodingProvider` with `#[serde(rename_all = "snake_case")]` (`open_code`), so
the value sent is one the receiver cannot accept and the server exits at
startup. Reproduced against the real entry point:

| Env value passed | `borg __agent-mcp` |
| --- | --- |
| `open-code` (what OpenCode sent) | fails to start |
| `openrouter` (what OpenRouter sent) | fails to start |
| `openai-compatible` (what OpenAiCompatible sent) | fails to start |
| `open_code`, `open_router`, `open_ai_compatible` | start |
| `codex`, `claude`, `kimi`, `glm` | start |

Three providers were affected; the other four were fine only because their
kebab and snake spellings coincide. Both sides matched in committed `HEAD`, so
this was pre-existing.

**Correction to an earlier draft of this section.** An earlier version of this
document inferred from the Go lane's `exec`-only tool surface that Borg's
capabilities were therefore *unreachable* there. That was an overclaim and is
withdrawn. Shell-first tool access is the intended design on this lane: the
model receives `exec` and reaches Borg capabilities through `borg call`.
Verified directly inside a Go session — the model ran
`borg call get_goal '{}'` and got `{"goal":null,"remaining_tokens":null}` with
exit code 0. So capabilities were reachable the whole time, and the observation
that the model "only had `exec`" was the design working, not the defect.

The serialization bug is nonetheless real, and its actual blast radius is the
**compatibility MCP path** — the routes where a provider consumes Borg's MCP
server rather than shelling out. It was fixed for that reason.

**3. `borg acp` cannot receive any client message while a turn is running.**
Broader than approvals, and the cause is documented by the protocol library
itself. `agent-client-protocol` states that `Builder` "runs all handler
callbacks on a single async task - the event loop. While a handler is running,
**the server cannot receive new messages**", and `block_task()` carries the
explicit warning that "using it directly in a handler callback will deadlock the
connection". Borg's `respond_prompt` is registered through `on_receive_request`
and then awaits the whole turn — including
`connection.send_request(RequestPermissionRequest…).block_task()` — inside that
callback, so the dispatch loop stays occupied for the entire prompt.

Three consequences, each reproduced:

- **Approval-gated tool calls deadlock permanently.** Once
  `session/request_permission` is emitted, a correctly formed
  `RequestPermissionResponse` can never be read; the prompt never returns and
  the journal ends at `approval_requested`. Repro `/tmp/borg-go-acp-probe.py`.
- **Nothing else is answered mid-turn either.** With `--permission full-access`
  and no gate at all, a second `initialize` sent during a plain counting turn
  went unanswered. Repro `/tmp/borg-go-acp-blocking.py`.
- **`session/cancel` cannot interrupt a turn.** It is not read until the handler
  returns. In that same run the turn ended 0.1 s after the cancel was sent with
  `stopReason: "end_turn"` — it had already finished naturally and the queued
  notification was merely drained afterwards. ACP cancellation is inert, not
  just delayed.

The library's prescribed shape is to offload the work with
`ConnectionTo::spawn`, so the turn and any `block_task()` run outside the
dispatch loop. None of this is Go-specific; the stall precedes any provider
handling.

## Repairs and verification

Three source changes, each minimal and confined to the call site at fault.

| File | Change |
| --- | --- |
| `crates/borg-agent-runtime/src/agent.rs` | `compact_native` and `consult` now call `with_model_access_for(provider, Some(model), &access)` instead of the model-less wrapper. Two lines. |
| `crates/borg-agent-runtime/src/subagents.rs` | `external_mcp_server` serializes the provider through serde rather than `catalog_backend()`, so the value matches the deserializer by construction and the two sides cannot drift again. |
| `crates/borg-cli/src/acp.rs` | The `PromptRequest` handler hands the turn to `connection.spawn(...)`, the library's prescribed shape, so the dispatch loop stays free while a turn runs. |

Receipts, all on the hosted OpenCode Go subscription:

**Compaction — now works, both models.** Read from the durable journal, not the
screen. GLM-5.3-Flash and DeepSeek-4.1-Flash each recorded
`context_compaction status=started` followed by `status=completed native=true`
carrying a real continuation summary, with an empty `error` list for the
session. Previously both failed with `OpenCode native sessions require an
explicit model`. Probe: `/tmp/borg-go-compact-probe.py`.

**ACP — responsive, cancellable, and approvals complete.**

| Check | Before | After |
| --- | --- | --- |
| second `initialize` answered mid-turn | no | **yes** |
| `session/cancel` result | ignored; turn ran to completion, `stopReason: end_turn` | **turn ended in 0.0 s, `stopReason: cancelled`** |
| approval-gated tool call | deadlocked forever | **completed; `marker.txt` matched the run nonce** |

Probes: `/tmp/borg-go-acp-blocking.py`, `/tmp/borg-go-acp-tools.py`.

**Shell-first tool access confirmed working**, which is what retired the
overclaim above: inside a Go session the model ran `borg call get_goal '{}'`
and received `{"goal":null,"remaining_tokens":null}`, exit 0.

**Test suite**, after the changes:

- `borg-agent-runtime --lib`: 836 passed, 0 failed, 16 ignored
- `borg --bins`: 210 passed, 0 failed, 3 ignored
- `borg-provider` (all targets): 119 passed, 0 failed, 4 ignored
- `borg-remote --lib`: 77 passed, 0 failed, 2 ignored

The provider figure differs from the 127/3 recorded earlier in this document.
That baseline was taken at an older tree state and other agents have since
changed provider code; none of the three repairs here touch `borg-provider`.

### Regression coverage

The three repairs were committed without tests guarding them, which is worth
naming because defect 2 was itself a silent drift between two sides that no test
compared. Coverage is now uneven, deliberately so:

- **Defect 2 has a regression test.**
  `subagents::tests::agent_tool_provider_environment_parses_back_for_every_provider`
  starts a real `AgentToolServer` for each of the seven `CodingProvider`
  variants, reads `BORG_AGENT_TOOL_PROVIDER` back out of
  `external_mcp_server().env`, and parses it exactly the way `borg-cli`'s
  `agent_tool_provider()` does. It compares the two sides at the boundary where
  they actually disagreed, rather than asserting a spelling.
- **Defect 1 has no unit test, and is instead prevented structurally.** With
  `with_model_access` deleted there is no model-less wrapper to reach for, so a
  call site can only forward `None` by writing it explicitly. An earlier draft
  of this section proposed a test pinning the `require an explicit model` error
  string; it was dropped on review because it exercised the helper's own error
  text rather than what actually broke — that `compact_native` and `consult`
  pass the model they already resolved. The empirical receipts are the
  compaction probe runs above.
- **Defect 3 has no unit test.** Its evidence is the ACP probe receipts above
  (`/tmp/borg-go-acp-blocking.py`, `/tmp/borg-go-acp-tools.py`).

Both follow-ups originally left to their owners are now closed.

*Closed.* Converting both call sites left `native_harness::with_model_access`
unused and raising a `dead_code` warning. The owner removed that footgun wrapper
in `f15af98` ("Remove obsolete model-less native access wrapper"), which is what
turned defect 1 into the structural prevention described above.

*Also closed.* The external baseline has now been collected first-hand for both
approved models — see *First-hand run, both approved models*. The blocker had
been recorded as "it would start a local `opencode serve`"; that framing was
narrowed (the prohibition is on local *models*, and `opencode serve` is a
localhost proxy to the same hosted subscription), the bounded run was approved,
and it executed in four hosted calls. It corrected an inference rather than
confirming one: external caching turned out to be model-dependent. Nothing in
this document asserts parity.

## Scope limits on this round

All model calls used the hosted OpenCode Go subscription. The Go key was read
in place from `~/.local/share/opencode/auth.json`; nothing was copied or
printed, no sign-in was started, and no API-billing route was used. Models were
restricted to `opencode-go/glm-5.3-flash` and `opencode-go/deepseek-v4.1-flash`.

No local model or inference server was started for any of this work. One
qualification, since an earlier version of this line was written before the
external baseline ran: that baseline does start a local `opencode serve`
process. It is a localhost HTTP proxy that forwards to the same hosted
subscription — it performs no inference — and each run spawns exactly one such
child, bound to `127.0.0.1` on a random port, stopped by terminating that one
process handle. Every call in this document therefore still resolves to the
hosted service; the native figures reach it through Borg's own adapter and the
baseline figures reach it through that proxy.

Receipts come from the existing `target/debug/borg` build, which predates other
agents' in-flight uncommitted edits to the CLI and provider crates. It was reused
deliberately rather than rebuilt: rebuilding would have taken the shared target
lock and changed the binary under test mid-workstream. These results therefore
describe that build, not the current working tree.

## Compatibility lanes are still intact

Captured from a real Go-lane session journal (`provider_capabilities_updated`),
so this is an observation of the running system rather than a config reading:

| Provider | installed | authenticated | detail |
| --- | --- | --- | --- |
| `claude` | yes | yes | Claude subscription authenticated |
| `open_code` | yes | yes | OpenCode Go native subscription available; other routes retain the compatibility path |
| `codex` | yes | yes | OpenAI API key configured |
| `open_router` | yes | yes | OpenRouter API key configured |
| `kimi`, `open_ai_compatible` | yes | no | — |

The Claude lane is still the authenticated unmodified-CLI route, unchanged by
this work, and OpenCode still reports Go native alongside a retained
compatibility path for its other routes. Nothing here was re-authenticated,
re-signed-in, or rewritten to produce this table.

Note in passing that this snapshot spells providers `open_code` / `open_router`
/ `open_ai_compatible` — the serde `snake_case` form. That is the spelling
defect 2 expects and `catalog_backend()` failed to supply.

## Remaining verification

A matched external cache baseline now exists for both approved models (see
*First-hand run, both approved models*), and compaction and ACP
approval/cancellation have passing receipts (see *Repairs and verification*).

What remains unverified:

- **A byte-identical request-body comparison.** The collected baseline is a
  matched workload, not a controlled one: the native and server loops
  necessarily wrap the same fixture differently. No parity or savings claim
  should rest on it, and none is made here.
- **A cost basis.** `cost_basis` is `unavailable` on every native row, so no
  dollar comparison is derivable on either lane.
- **External tool-path behaviour.** Only model/cache accounting was measured
  externally; the tool, approval and cancellation receipts are native-lane only.
- **Mixed-protocol models**, below — check these and preserve route-specific
  semantics before claiming full coverage.

Do not remove the generic OpenCode compatibility path while
other routes still use it. Claude direct subscription migration requires a
supported access change or explicit provider authorization, not an API billing
substitution.

A bounded MiniMax M2.7 attempt through the existing native Go Chat Completions
path did not complete within 45 seconds and entered provider/network retries.
It is not a passing compatibility receipt; mixed-protocol behavior remains
unresolved. No credentials or billing routes were changed in response. MiniMax
work is stopped: this round used only `opencode-go/glm-5.3-flash` and
`opencode-go/deepseek-v4.1-flash`.
