# Borg Computer-Use Readiness — code mode & harness gap analysis

Companion to `docs/codex-computer-use-teardown.md` (what SotA looks like). This doc assesses the Borg
codebase (crates/borg-*, at v0.6.6) and ranks the concrete changes needed to do computer use in a
best-in-class (throughput + accuracy) way. File:line anchors are current as of this analysis.

## What Borg already has (good foundations)

- **Three code-mode tiers.** (1) **Blu** bounded guest (`blu_workflow.rs`) — durable/replayable, host calls
  via `borg_tool`/`borg_exec`. (2) **Dedicated workflow runtimes** `WorkflowRuntime::{Python,Ipython,
  Javascript,Typescript}` — real runtimes, fresh process per call. (3) **Persistent runtimes**
  (`persistent_runtime.rs`) — session-scoped, namespace-retaining **Python + Bun (JS/TS)** workers with a
  clean host boundary `RuntimeHost::call(operation, args) -> JSON`. Tier 3 is Borg's direct analog to
  Codex `cua_node`/`cua_repl` and is the right vehicle for a computer-use REPL.
- **Tool loop** (`native_harness.rs`) already: batches multiple tool calls per turn, runs read-only calls
  in parallel (chunks of 4), streams progress, supports steering/interrupt at tool boundaries, auto-compacts.
- **Output caps** matching the doc's ~25k-token guidance: `MAX_TOOL_RESULT_BYTES = 1 MiB`, exec
  `max_output_tokens ≤ 64000`.
- **Provider image encoding on the input side is done:** `codex_model.rs` emits `input_image`,
  `openai_compatible.rs` emits `image_url`, both from `ModelInputAttachment{media_type,data_base64}`.
- **Permission spine** (`execute_tool` → `request_tool_approval` / `review_tool_automatically`) — a reviewer
  LLM with a hardened prompt already exists; the perfect hook for a confirmations taxonomy.
- **MCP client** (`native_mcp.rs`) — local stdio subprocess servers, namespaced `mcp__{server}__{tool}`,
  bundled/local servers fully supported. Lowest-friction bootstrap path (host Codex's own plugin, or a Borg
  driver, as a local MCP server).

## Ranked gaps (what to change)

### 1. [LANDED — was BLOCKER] Tool results are text-only — no inline image channel
**Status: landed on `overhaul/billing-indicator` ("Let tool results carry images to the model").**
`ModelMessage::Tool` now carries `attachments: Vec<ModelInputAttachment>` (serde-default, journals replay
unchanged). Contract for any tool: return a JSON object with `"borg_attachments": [{media_type, data_base64,
filename?}]` (≤4 images, ≤6 MiB base64 each; rejects are noted in `dropped_attachments`) and the native
harness attaches them to the tool message. MCP `type:"image"` blocks are lifted automatically. The Responses
encoder emits `function_call_output` content blocks with `input_image`; chat-completions follows the `tool`
message with a `user` message holding the images labelled by call id. Images are counted as a flat vision
cost (not megabytes of text) for compaction, and compaction pruning drops stale images with their text.
`runtime_exec` results lift `value.borg_attachments` to the top level, so a REPL script can emit a screenshot
inline (#3 below).

Original finding, kept for context:
`ModelMessage::Tool { tool_call_id, content: String }` (`borg-core/src/model.rs:35`). Only `User` messages
carry `attachments: Vec<ModelInputAttachment>`. The harness never promotes tool output to an image
(`record_native_tool_result` pushes a `String`; all `attachments` uses are the user-prompt path). MCP
`type:"image"` content is stringified to base64 text (`native_mcp.rs`), not an image block.

Why it blocks SotA: Codex's observe→act loop returns the screenshot **inline in the same tool result**, so
one model round = call runtime → get AX text + screenshot → decide. Borg can only inject a screenshot as a
*separate synthetic User message*, which desyncs image from tool call, breaks the code-mode contract, and
fights the provider tool-result encoding.

Fix: give `ModelMessage::Tool` a structured, multimodal result — e.g.
`Tool { tool_call_id, content: String, attachments: Vec<ModelInputAttachment> }` (or a `Vec<ContentBlock>`
with Text/Image variants). Thread it through:
- `record_native_tool_result` (`native_harness.rs:~1930`)
- both provider encoders (`codex_model.rs`, `openai_compatible.rs`/`model_turn.rs`/`chat_stream.rs`) — they
  already know how to emit `input_image`/`image_url`; this is mostly moving that call to the tool-result path.
- `native_mcp.rs` result mapping (surface MCP image content blocks as attachments, not base64 text).
- the persistent-runtime host boundary (see #3) so a REPL screenshot flows out as an attachment.
This is the single highest-leverage change and unblocks *all* image-returning tools, not just computer use.

### 2. [BLOCKER] No computer-use driver or host operations
No screen capture / accessibility / input injection anywhere (verified: no ScreenCaptureKit, AXUIElement,
xdotool, CDP, SendInput, UIA). Need a native driver + host ops exposed into the code-mode runtime.

Fix (mirror Codex's split): a thin per-OS native helper behind one line-delimited JSON/RPC boundary, plus a
`cua`-style typed client library injected into the persistent Bun/Python runtime as globals. Ops:
`getAXState`(text+indices, diffed), `getScreenshot`, `click(index|xy)`, `type`, `pressKey`, `scroll`,
`drag`, `setValue`, `perform_secondary_action`, `list_apps`/`list_windows`. Backends: macOS
ScreenCaptureKit+AXUIElement+CGEvent; Windows WGC+UIA+SendInput; Linux AT-SPI(+X11 fallback) / Wayland
portal+libei. Per-window addressing for background + parallel. See teardown §3, §5, §7.

Crate placement: this is product core, not remote transport — put the driver + host-ops in a core/agent
crate (see #7), not under `borg-remote`.

### 3. [LANDED with #1] Persistent-runtime host boundary returns JSON only
**Status: covered by the `borg_attachments` contract — a runtime script returns `{ borg_attachments: [...] }`
as its value and the harness lifts it into the tool message.** Remaining: a convenience host op / helper in
the injected runtime globals (`emitImage(bytes, mediaType)`) once the driver exists.

Original finding:
`RuntimeHost::call(op, args) -> serde_json::Value`, `PersistentRuntimeResult{ value: Value, stdout, stderr }`
(`persistent_runtime.rs`). A screenshot can only come back as base64-in-JSON or a filepath, then something
must convert it — same root cause as #1. Once #1 lands, extend the persistent-runtime result to carry
image attachments (or a `nodeRepl.emitImage`-style host op) so a REPL script can emit a screenshot into the
tool result inline. This is what makes code-mode computer use actually work.

### 4. [HIGH] AX-tree-first representation, diffing, and auto-settle are absent
These are the accuracy + throughput multipliers in Codex, and none exist in Borg. Build into the driver /
host-ops (native side, like Codex's Swift `RefetchableSkyshotAXTree`):
- serialize the a11y tree to compact text with stable `element_index`; return **diffs** by default
  (removed/added/changed), full tree on demand — with a line budget.
- **auto-settle**: after an action wait ~1s, extend to ~5s on loading/mutation via native change observers
  (AXObserver / UIA StructureChanged / AT-SPI signals). No model-driven sleeps.
- prefer semantic index actions over pixel coordinates; screenshot as fallback with a cached `screenshotId`.

### 5. [MED] No image downscaling / resolution control
No `resize`/`downscale`/`backingScaleFactor` handling in the image path; only byte/count caps
(`native_user_message`: ≤4 images, ≤25 MiB). Capture/return screenshots at **logical** resolution as JPEG
(a token + latency win). Add near the driver's screenshot op and/or the tool-result attachment builder.

### 6. [MED] Confirmations taxonomy for UI side effects
Borg has the generic reviewer (`review_tool_automatically`) but not the computer-use risk taxonomy. Port the
teardown's confirmations policy (user-authored vs third-party-content intent; delete/transmit-sensitive/
CAPTCHA/software-install/financial → confirm) + a recursive per-action reviewer over the REPL script, and a
per-turn cleanup hook. Hook into `execute_tool`/`review_tool_automatically`.

### 7. [MED] Crate organization — agent core is filed under `borg-remote`
`borg-remote` (98k LOC) is self-described as "shared semantic kernel… enrollment and transport are
workload-neutral" yet contains the entire agent loop (`native_harness`, `subagents`, `agent`, `session`,
`blu_workflow`, `persistent_runtime`, `orchestration`, `autonomy`). `borg-core` is 300 LOC (message/tool
contract only). Per `AGENTS.md` ("Borg owns the agent loop… a dependency on an upstream agent runtime is a
compatibility constraint to reduce, not the target architecture"), the loop + code-mode runtimes + tool
dispatch are the product core and should live in `borg-core` or a new `borg-agent`/`borg-harness`;
`borg-remote` keeps enrollment/transport/relay. Sequence this refactor alongside the driver work so the new
computer-use subsystem hangs off the core, not the transport crate. Not a blocker for a prototype.

## Recommended sequencing

1. **#1 tool-result image channel** (unblocks everything; independently valuable for any image tool).
2. **#2 + #3 driver + persistent-runtime `emitImage`**, macOS first, behind the `cua` client library.
3. **#4 AX-first + diff + settle** in the driver (the accuracy/throughput core).
4. **#6 confirmations**; **#5 downscaling**; then Windows/Linux backends.
5. **#7 crate refactor** as a parallel cleanup.

Bootstrap option: ship the driver as a local `ExternalMcpServer` first (matches `native_mcp.rs`), validate
the loop end-to-end, then fold it into the persistent runtime as a first-class code-mode library.

## Anchor files
`borg-core/src/model.rs` (`ModelMessage`/`ModelInputAttachment`); `borg-remote/src/native_harness.rs` (loop,
tools, approvals, `record_native_tool_result`, `native_user_message`); `persistent_runtime.rs` +
`blu_workflow.rs` (code-mode); `native_mcp.rs` (MCP client); `subagents.rs` (`AgentToolDispatcher`,
`run_persistent_runtime`); `borg-provider/src/provider/{codex_model,openai_compatible,chat_stream,model_turn}.rs`
(image encoding).
