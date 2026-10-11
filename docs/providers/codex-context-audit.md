# Codex reasoning and context parity audit

Reference: [OpenAI Codex at de8fab6d](https://github.com/openai/codex/tree/de8fab6d7adfcef8b4ce6f02f3b5c8be4092015a), inspected 2026-10-10.
Scope: native ChatGPT subscription requests, reasoning streams, context accounting,
compaction, retained history, cache affinity, and model limits.

## Model window: raw, usable, and maximum are different

The live subscription catalog for `gpt-6.1-sol` advertised:

| Limit | Tokens |
| --- | ---: |
| Default raw context window | 272,000 |
| Usable default window, with Codex's 95% allowance | 258,400 |
| Catalog maximum raw window | 872,000 |
| Usable window at that maximum | 828,400 |

Codex resolves the catalog's default window before the maximum, applies the
95% allowance, and permits an explicit configured window clamped to the maximum.
Borg uses that same advertised default and allowance; 258,400 is not the model's
raw maximum. Request audits expose raw, usable, and maximum limits separately.
This audit does not silently opt a session into a larger window or
change its model, effort, speed, credentials, or billing route.

References: `protocol/src/openai_models.rs` (`ModelInfo::usable_context_window`),
`models-manager/src/model_info.rs` (`apply_overrides`), and Borg's
`crates/borg-provider/src/provider/codex_model.rs` (`ModelCapabilities`).

## Corrected mismatches

- **Encrypted reasoning was free in the local estimate.** Borg removed opaque
  replay state from text estimates to avoid counting duplicated text and signatures,
  but also omitted reasoning's actual context cost. Codex estimates reasoning as
  decoded bytes minus 650 bytes of envelope overhead, then approximately four
  bytes per token. Borg now applies that estimate only to encrypted Responses
  reasoning/compaction content; copied visible output and transport metadata stay
  excluded. Provider-reported context remains authoritative when available.
- **The retained tail used that incomplete estimate.** A 25,840-token tail retained
  54 messages with 103,113 visible serialized characters and 285 encrypted reasoning
  items totaling 1,790,264 characters. The next provider input was 175,052 tokens.
  Tail selection now charges encrypted reasoning too and still preserves tool-call
  pairing. If the newest call/result group cannot fit, the summary carries its
  progress rather than replaying an oversized or broken group.
- **Post-tool pressure omitted the newest generated output.** Codex starts from
  the last response's total tokens and adds unsent local items. Borg now checks
  measured input plus generated output plus new tool results/steering, rather than
  input plus tool results alone. Turn-start measured checkpoints already include
  the previous generated output and are not charged for it again.
- **Responses Lite requests differed from Codex.** Lite now disables
  `parallel_tool_calls`, sends `x-openai-internal-codex-responses-lite: true`, and
  keeps `reasoning.context: all_turns` and the existing additional-tools prefix.
- **Summaries were forced to detailed.** Borg now honors the catalog's default
  reasoning summary; `none` omits the request parameter. The inspected model's
  default is `none`. Missing catalog defaults use `auto`, not forced `detailed`.
- **Late final summary fragments opened another row.** Terminal summary updates
  now use the existing provider-item snapshot route to update the original row.
  When summaries were requested, item rows stay distinct even if their text
  arrives only at completion; genuinely hidden chunks can still share a phase.
- **Thread attribution lacked the standard header.** Borg sends `thread-id` for
  the durable thread identity, separate from cache-affinity `session-id`.

References: Codex `core/src/context_manager/history.rs`, `core/src/client.rs`,
`core/src/session/step_settings.rs`, and Borg's `native_harness.rs` and
`codex_model.rs`.

## Intentional differences

- **Borg owns compaction and durable replay.** Codex has local user-message
  retention and remote opaque-checkpoint compaction with different retention
  budgets. Borg's cross-provider summary and configurable recent evidence tail
  preserve its own tools, steering, system overrides, and journal/replay contract.
  The full journal is not deleted when the model-facing view is compacted.
- **Borg's default reserve is larger.** Codex's default automatic threshold is
  90% of the raw window: 244,800 for this model. Borg reserves 15% of the usable
  window, triggering at 219,640. Its tools and steering can add large results
  between provider measurements, and its checkpoint needs headroom. The reserve
  and retained tail are configurable per provider/model. Changing that safety
  policy is not required to fix the retained-reasoning bug.
- **HTTP full-history replay instead of incremental WebSockets.** Borg replays
  provider-native items losslessly with its own durable request state. It retains
  the subscription cache-affinity header, prompt-cache key, sticky turn-state,
  routing cookies, account-bound replay, and same-account authentication recovery.
  Adopting Codex's agent runtime or pretending to be its process is not required
  for those subscription contracts.
- **Effort transitions remain Borg-owned.** Both clients support durable effort
  updates. Borg uses its explicit controls and account catalog, not Codex's CLI
  feature configuration; the original request effort and subsequent updates stay
  at their original conversation positions.
- **Other opaque protocols are not guessed.** Anthropic thinking signatures and
  duplicated replay text remain excluded from text estimates. Responses reasoning
  has a known upstream sizing rule; unrelated signatures do not.

## Verification

Regression coverage checks encrypted-reasoning tail limits, lossless retention of
valid tool pairs, generated-output pressure, duplicate completion replay, Lite
request fields and headers, default summary behavior, and streamed native output
surviving a replay. Compile and LSP checks complement those behavioral checks;
compilers cannot detect a wrong wire header or a zero-cost encrypted history.

Verified suites: runtime 1,125 passed (12 ignored); provider 187 passed plus 6
integration tests (5 ignored); TUI 523 passed (21 ignored). The final workspace
LSP pass scanned 206 documents with no diagnostics. The TUI suite also checks
that only the compaction marker and “Compacted context” title are orange and its displayed separator has no colon.
