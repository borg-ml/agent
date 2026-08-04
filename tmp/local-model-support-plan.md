# Local Model Support in Borg — Implementation Plan

Status: proposal, ready to implement
Target repo: `/home/shulgin/borg-cli`
Author context: written after a discovery pass on 2026-08-04; every claim in
"Verified findings" was checked against this tree or this machine.

---

## 0. TL;DR for the implementing agent

Local models **already work** through the existing `OpenAiCompatible` provider.
Nothing needs to be built for basic operation. This plan is about **UX**: making
local models discoverable and selectable in `/model`, and making them fit the
GPU without hand-tuning.

Do the work in this order. Phase 1 is a prerequisite for everything else.

1. Dynamic arm on the model catalog (unblocks all UI work)
2. `ModelSource` trait + `NativeGgufSource` (discovery)
3. VRAM fit estimation + auto `-ngl` / `-ncmoe`
4. Supervised `llama-server` subprocess — **config block already landed**, spawn/health/teardown remain
5. Context-window metadata for the `Generic` profile — **DONE, see §6**
6. `BluModelSource` (later, once the Blu embed lands)

## 0.1 Already implemented (do not redo)

Landed on 2026-08-04 as the minimum needed to configure a local model through
Borg rather than through shell exports:

- **`[local]` block in `agent.toml`** — `crates/borg-cli/src/agent_config.rs`.
  New `LocalProviderConfig` (`base_url`, `model`, `context_window_tokens`,
  `api_key`), validated in `AgentConfig::validate`, plus
  `AgentConfig::apply_local_provider_env` which exports the settings into the
  process environment **without overwriting existing values**, so an explicit
  export or `--model` still wins.
- **Wired at startup** — `remote_commands.rs::run_local_agent`, immediately
  after `AgentConfig::load` and before any worker thread reads provider env.
- **Context window for `Generic`** — `openai_compatible.rs`. New
  `generic_context_window_tokens()` reads
  `BORG_OPENAI_COMPATIBLE_CONTEXT_WINDOW_TOKENS`; the `Generic` arm now
  populates `context_tokens` and `context_window_tokens`, so auto-compaction
  engages. This closes the gap described in §1.4.
- **Tests** — four in `agent_config.rs::tests` covering parse, defaults,
  URL-scheme validation, and the positive-context-window rule.

Consequence: this is now valid and sufficient, with no wrapper script and no
environment exports:

```toml
[local]
base_url = "http://127.0.0.1:11434/v1"
model = "qwen3.6:35b-a3b"
context_window_tokens = 32768
```

```bash
borg agent --provider open-ai-compatible
```

**Verified end to end on 2026-08-04** against `qwen3.6:35b-a3b` on this box:
a full `borg agent --provider open-ai-compatible` turn planned, called a tool,
wrote a file, and reported
`"context_tokens":5510,"context_window_tokens":32768` — confirming the Phase 5
fix is live and auto-compaction can now engage. `reasoning_content` was
journaled through the native harness as `native_model_message`.

**Known remaining wart:** `OLLAMA_CONTEXT_LENGTH` must still be set on the
Ollama *server* to match `context_window_tokens`, because the daemon is a
separate process Borg does not own. Phase 4 removes this by having Borg
supervise `llama-server` directly and pass `--ctx-size` itself — at which point
`context_window_tokens` should be derived from the resolved value rather than
declared twice.

---

## 1. Verified findings

These were confirmed by inspection, not assumed.

### 1.1 The provider already exists and is keyless

`crates/borg-provider/src/provider/openai_compatible.rs`

- `OpenAiCompatibleProfile::Generic` (line 41), wire name `"openai-compatible"` (line 49)
- Line 181: `if api_key.is_none() && profile != OpenAiCompatibleProfile::Generic`
  — the API-key requirement is **explicitly waived** for `Generic`. This exists
  for keyless local servers.
- Line 496: falls back to the literal string `"local"` as the bearer token.
- Default base URL (line ~720): `http://127.0.0.1:8000/v1`, overridable with
  `BORG_OPENAI_COMPATIBLE_BASE_URL`.
- Model name comes from `BORG_OPENAI_COMPATIBLE_MODEL`
  (`crates/borg-cli/src/remote_commands.rs:784`) or `--model`.

Provider is wired through CLI (`cli.rs:565`), remote (`contract.rs:180`),
native harness (`native_harness.rs:631`), and TUI.

### 1.2 The `/model` selector cannot show local models today

`crates/borg-provider/src/runtime.rs`

```rust
pub struct ProviderModelCatalog {
    pub backend: &'static str,
    pub default_model: &'static str,
    pub selectable_models: &'static [(&'static str, &'static str)],
    pub effort_levels: &'static [&'static str],
}

pub const MODEL_CATALOGS: [ProviderModelCatalog; 3] = [
    CODEX_MODEL_CATALOG, CLAUDE_MODEL_CATALOG, KIMI_MODEL_CATALOG,
];
```

Everything is `&'static str`. There is **no** `OpenAiCompatible` catalog — a
repo-wide grep for `OpenAiCompatible` in `runtime.rs` returns nothing.

The picker at `crates/borg-cli/src/terminal_ui.rs:1238`
(`model_picker_options`) iterates `catalog.selectable_models`. `OpenRouter` and
`OpenAiCompatible` fall into a hand-rolled arm that only echoes the *current*
model back. Discovered models are runtime-owned `String`s and cannot enter this
structure without change.

**This is the single blocking issue for local-model UX.**

Catalog consumers that must keep compiling:
- `crates/borg-remote/src/contract.rs:185,210`
- `crates/borg-remote/src/subagents.rs:2805,2840,2864,4742`
- `crates/borg-cli/src/terminal_ui.rs:1238`
- `crates/borg-cli/src/terminal_ui/tests.rs:407`
- `crates/borg-provider/src/provider/mod.rs:1000`

### 1.3 There is no local model registry to read

`llama-server` takes `-m /path/to.gguf`. No install dir, no registry, no index.
Models are loose files. **But GGUF files are self-describing** — the header
holds `general.architecture`, `general.name`, `general.size_label`,
`general.quantized_by`, `general.license`, plus block count and context length.
Reading the first few KB of each file yields a better catalog entry than
Ollama's opaque tag strings.

Three discoverable sources:

1. **User-configured directories** — glob `*.gguf`, parse headers.
2. **Ollama's blob store, without running the daemon.** The manifest at
   `~/.ollama/models/manifests/registry.ollama.ai/library/<name>/<tag>` is JSON;
   the layer with `mediaType: application/vnd.ollama.image.model` has a digest
   mapping to `blobs/sha256-<digest>`. That blob **is** a GGUF and can be passed
   straight to `llama-server -m`. This gives `ollama pull` as a downloader with
   `llama-server` as the runtime — no daemon required.
3. **HF cache** — `~/.cache/huggingface/hub` (present on this machine, 2.7 G).

### 1.4 `Generic` has two capability gaps

- **No reasoning-effort mapping.** Kimi and OpenRouter translate `effort`;
  `Generic` sends only `max_tokens` / `temperature` / `extra_body`.
  `remote_commands.rs:795` sets effort to `None` for `OpenAiCompatible`.
  `--effort` is silently inert.
- **No context-window metadata.** `context_window_tokens` is populated only for
  OpenRouter (from `BORG_OPENROUTER_CONTEXT_WINDOW_TOKENS`, line 636) and Kimi
  (hardcoded, line 1057). `Generic` sets nothing, so **auto-compaction never
  engages**. This is the highest-value small fix in this document.

### 1.5 `llama-server` is a drop-in, and is already on disk

`/usr/lib/ollama/llama-server` (llama.cpp `b4d6c7d8f`, libggml 0.17.0) exposes
`/v1/chat/completions`, `/v1/models`, `/v1/health`, plus `/v1/messages`
(Anthropic) and `/v1/responses`. `--jinja` is on by default, so tool calling
runs through the model's own chat template — which is what Borg's `Generic`
path needs, since it already sends `tools` + `tool_choice: "auto"`.
`--reasoning-format deepseek` emits `message.reasoning_content`, the exact field
the native harness journals and replays for Kimi K3.

Architecture support confirmed in `libllama.so`: `qwen35`, `qwen35moe`,
`qwen3moe`, `qwen3vlmoe`. Note that `qwen35`/`qwen35moe` are the **GGUF
architecture identifiers used by Qwen3.6 models** — the arch string lags the
marketing version. Qwen3.6-27B on this box reports `general.architecture:
qwen35`.

Expert-offload flags are present and are central to Phase 3:
`-ot/--override-tensor`, `-cmoe/--cpu-moe`, `-ncmoe/--n-cpu-moe N`.

### 1.6 Blu is a real runtime, but is not linked into Borg

Blu (`github.com/borg-ml/blu`, sibling checkout at `../blu`) is a Lua/Luau
runtime, ~110k LOC across 8 crates. Relevant primitives:

- `try_set_global` + `NativeFunctionId` — host injects natives
- `load_owned_source_with_limits` — bounded execution
- `vm.rs` gates file read/write/seek and `os.execute` behind **host-granted
  capabilities**, returning structured unavailable-capability errors

That capability model is a **better** trust boundary for this feature than a
native scan, because a script cannot read a directory unless the host grants it.

**However:** Borg's `Cargo.lock` contains **zero** `blu` entries. The runtime is
not a dependency. Borg's "Blu extensions" today are the declarative TOML
manifests described in `docs/blu-extensions.md`, which contribute skill roots
and stdio MCP servers and explicitly **never** opaque lifecycle hooks. Landing
the embed is a much larger piece of work than this feature, and Blu's README
places it at the "safe Luau-bytecode loader and interpreter" milestone with
conformance gates in progress.

Therefore: **design the interface so Blu can supply it later, but do not block
on the embed.**

Note also that MCP is the wrong layer for this regardless. MCP servers
contribute tools the *model* calls; `/model` is host-side TUI state a *human*
scrolls. Letting project-scoped MCP packages inject picker entries would let
untrusted config control which model runs — precisely what
`allow_project_mcp = false` defends against.

---

## 2. Architecture

One interface, three implementations over time.

```rust
/// A source of locally-runnable models.
pub trait ModelSource: Send + Sync {
    fn name(&self) -> &str;
    fn discover(&self) -> Result<Vec<LocalModel>, ModelSourceError>;
}

pub struct LocalModel {
    pub id: String,            // stable, e.g. "gguf:qwen3.6-35b-a3b-q4_k_m"
    pub display_name: String,  // from general.name
    pub path: PathBuf,
    pub architecture: String,  // general.architecture, e.g. "qwen35moe"
    pub quant: String,         // "Q4_K_M"
    pub size_bytes: u64,
    pub block_count: Option<u32>,
    pub train_ctx: Option<u32>,
    pub expert_count: Option<u32>,   // Some(_) => MoE, drives -ncmoe
    pub source: &'static str,        // "dir" | "ollama" | "hf"
}
```

- `NativeGgufSource` — Phase 2. Directory glob + header parse.
- `OllamaBlobSource` — Phase 2. Manifest JSON → blob path. No daemon.
- `BluModelSource` — Phase 6. Same trait, script-supplied, capability-gated.

The trait is the point: Phase 6 becomes additive rather than a rewrite, and the
Blu embed is judged on its own timeline instead of gating a UX fix.

---

## 3. Phases

### Phase 1 — Dynamic catalog arm (prerequisite)

**Problem:** `ProviderModelCatalog` is `&'static str` throughout. Converting it
wholesale to `Cow<'static, str>` touches every catalog and every consumer listed
in §1.2 — large blast radius for no gain to the three fixed providers.

**Approach:** leave the three `const` catalogs untouched. Add a parallel
runtime-owned path used only by open-ended backends.

```rust
pub struct DynamicModelEntry { pub id: String, pub label: String, pub detail: Option<String> }

pub fn dynamic_models_for_backend(backend: &str) -> Vec<DynamicModelEntry>;
```

Then in `model_picker_options` (`terminal_ui.rs:1238`), the arm that currently
handles `OpenAiCompatible | OpenCode | None` merges dynamic entries in addition
to echoing the current model.

**Acceptance:** `/model` under `--provider open-ai-compatible` lists discovered
models; the three fixed catalogs render byte-identically to before;
`terminal_ui/tests.rs:407` still passes unmodified.

### Phase 2 — Discovery (`ModelSource` + GGUF parsing)

New module, suggested `crates/borg-provider/src/local/`.

GGUF header parsing — enough of the spec only:
- magic `GGUF`, version (3), tensor count, KV count
- typed KV reader; pull `general.*`, `<arch>.block_count`,
  `<arch>.context_length`, `<arch>.expert_count`
- **do not** read tensor data; stop after the KV block

Config:

```toml
[local]
model_dirs = ["~/models", "/mnt/big/gguf"]
include_ollama_store = true   # read blobs, do not run the daemon
include_hf_cache = false
```

**Test fixtures already on this machine:**
- `/home/shulgin/twilight/target/qwen36-local-gate/Qwen3.6-27B-Q4_K_M.gguf` (15.66 GiB)
- `/home/shulgin/twilight/target/qwen36-local-gate/Qwen3.6-27B-MTP-Q4_K_M.gguf` (15.93 GiB)
- `/home/shulgin/twilight/target/ternary-bonsai27-gate/Ternary-Bonsai-27B-Q2_g64.gguf` (7.06 GiB)

**Acceptance:** unit tests parse all three headers and return correct
name/arch/quant/size without loading weights. Malformed and truncated files
produce a typed error, never a panic.

### Phase 3 — Fit estimation and auto-offload

> **Corrected 2026-08-04 after observing a real load.** An earlier draft of this
> plan claimed "Ollama guesses; llama.cpp makes you tune by hand" and proposed
> Borg compute `-ngl`/`-ncmoe` itself. **That is now wrong.** llama.cpp
> `b4d6c7d8f` ships `common_params_fit_impl`, which does MoE-aware automatic
> fitting, and it works. Observed on this box loading `qwen3.6:35b-a3b` (22.3 GiB)
> onto a 16 GiB card:
>
> ```
> projected to use 22495 MiB of device memory vs. 16106 MiB of free device memory
> cannot meet free memory target of 1901 MiB, need to reduce device memory by 8291 MiB
> getting device memory data with all MoE tensors moved to system memory:
>   with only dense weights in device memory there is a total surplus of 11741 MiB
> set ngl_per_device[0].(n_layer, n_part, overflow_type)=(41, 17, GATE)
>   - ROCm0 (RX 7900 GRE): 41 layers (17 overflowing), 14011 MiB used, 2094 MiB free
> successfully fit params to free device memory  (took 2.75 seconds)
> load_tensors: offloaded 41/42 layers to GPU
> ```
>
> It independently derived the exact strategy this plan recommended — dense
> weights on GPU, MoE experts overflowing to system RAM — and tuned to partial
> *layer fractions*, which is finer-grained than the whole-layer `-ncmoe N`
> control. Measured result: **27 tok/s generation, 93 tok/s prompt**.
>
> **Therefore: do not build a fit solver.** Building one would mean
> reimplementing upstream, worse, and re-tuning it on every llama.cpp bump. The
> knobs are `LLAMA_ARG_FIT` / `LLAMA_ARG_FIT_TARGET`; leave fitting on and stay
> out of the way.
>
> **What is still worth building** is the *reporting* half — the picker showing
> what will happen before you commit to a load:
>
> ```
>   qwen3.6 35b-a3b   Q4_K_M   22.3G   MoE · experts spill to RAM · ~27 tok/s
>   qwen3.6 27b       Q4_K_M   15.7G   dense · spills · slow
>   ternary-bonsai    Q2_g64    7.1G   fits fully
> ```
>
> That needs GGUF metadata (Phase 2) and VRAM totals, but **not** a solver:
> `expert_count.is_some()` plus file size versus free VRAM is enough to label a
> row. Anything more precise should be read back from the server's own fit log
> after loading, not predicted.
>
> The material below is retained only as background on where the numbers come
> from.

Read total/used VRAM:
- Linux/AMD: `/sys/class/drm/card*/device/mem_info_vram_{total,used}`
  (verified on this box: card1 = 17163091968 total, ~1.27 G used)
- NVIDIA: NVML
- macOS: `sysctl` / Metal

Then in `/model`, show what actually matters:

```
  qwen3.6 35b-a3b   Q4_K_M   22.3G   ⚠ MoE spill — experts on CPU, 3B active
  qwen3.6 27b       Q4_K_M   15.7G   ⚠ spills — 58/65 layers on GPU
  ternary-bonsai    Q2_g64    7.1G   ✓ fits · 32k ctx
```

Offload policy:
- **Dense** models: solve `-ngl` from per-layer size and free VRAM, reserving
  KV cache for the requested context.
- **MoE** models (`expert_count.is_some()`): prefer `-ncmoe N` over reducing
  `-ngl`. Keep attention and shared weights on GPU, push expert tensors to CPU.
  With ~3 B active parameters per token the CPU cost per token is small, so this
  preserves quality at q4 instead of forcing a lower quant.

Keep the policy in one function with a table-driven test — it is the part most
likely to need per-GPU tuning, and the best candidate to migrate to Blu later.

**Acceptance:** for each fixture plus the 35b-a3b MoE, the planner emits flags
that load without OOM on a 16 GiB card.

### Phase 4 — Supervised `llama-server`

**Do not embed an inference engine.** Bindings to llama.cpp / candle / mistral.rs
would drag in the GPU backend matrix (ROCm/CUDA/Vulkan/Metal), GGUF quant
support, model download/caching, and KV cache management — an entire product,
forced onto every user of what is otherwise a provider orchestrator. It also
fights the architecture: the harness is deliberately provider-neutral and
providers are thin wire translators.

**Do supervise a child process.** The shape already exists in this repo:
`codex_app_server.rs` manages a long-lived `Child` with stdin/stdout plumbing,
and `subprocess.rs::terminate_std_process_tree` does process-group kills so an
orphaned server is not leaked.

```toml
[local]
server_bin = "/usr/lib/ollama/llama-server"
port = 8000
auto_start = true
ctx_size = 32768
```

Lifecycle: spawn on session start → poll `/v1/health` → set
`BORG_OPENAI_COMPATIBLE_BASE_URL` → tear down via process-group kill on exit.
Reuse an already-listening server rather than racing it.

### Phase 5 — Context window for `Generic`

Smallest change, largest immediate payoff: without it auto-compaction never
engages and long local sessions fail badly.

Add `BORG_OPENAI_COMPATIBLE_CONTEXT_WINDOW_TOKENS`, mirroring the OpenRouter
handling at `openai_compatible.rs:636`. When Phase 4 owns the server, populate
it automatically from the resolved `--ctx-size` — the host already knows the
real number, so the user should never have to supply it.

Optionally probe `/v1/models`, which llama-server answers with context metadata.

### Phase 6 — `BluModelSource` (deferred)

Once the Blu embed lands, implement `ModelSource` over a Blu script with a
host-granted directory-read capability. Migrate the Phase 3 offload policy to
Blu first — it is policy, it wants per-GPU tuning, and it is miserable to
hardcode but pleasant to script. This gives the embed a real first consumer
instead of a toy, and exercises the capability model meaningfully.

---

## 4. Risks

| Risk | Mitigation |
|---|---|
| Catalog refactor breaks fixed providers | Parallel dynamic path; const catalogs untouched; existing tests unmodified |
| GGUF format drift | Parse defensively, unknown KV types skipped, typed errors |
| Ollama blob layout changes | Feature-flagged source; degrade to "no models found" |
| VRAM estimate wrong → OOM | Reserve headroom; surface the estimate in the picker; never silently pick a spilling config without marking it |
| `llama-server` absent or CPU-only | Detect at startup; report backend clearly; do not promise GPU |

---

## 5. Environment notes for this machine

- GPU: **Radeon RX 7900 GRE, 15.98 GiB VRAM** (gfx1100), ~1.2 GiB used by display.
  There is a stale `nvidia-smi` on PATH with no NVIDIA hardware behind it — ignore it.
- `ollama-rocm` 0.32.5-1 **is installed** (as of 2026-08-04 23:48) alongside
  `ollama` 0.32.5-1. GPU acceleration is live: the backend loads from
  `/usr/lib/ollama/rocm_v7_2/` (`libggml-hip.so`, `libamdhip64.so.7`,
  `libhipblas.so.3`) and the server reports
  `library=ROCm compute=gfx1100 ... libdirs=ollama,rocm_v7_2`.
  Note the directory is `rocm_v7_2`, **not** `rocm` — checking for the latter
  gives a false negative, which is how an earlier pass of this document wrongly
  concluded the install was CPU-only.
- `llama-server` is at `/usr/lib/ollama/llama-server` and **not on PATH**.
- Ollama's server default context is 4096. `OLLAMA_CONTEXT_LENGTH` must be set
  **on the server process** and must match `local.context_window_tokens` in
  `agent.toml`. This duplication is what Phase 4 removes.
- Avoid these tags on this hardware: `nvfp4` (Blackwell FP4), `mxfp8` (no RDNA3
  acceleration), `mlx` (Apple Silicon).
