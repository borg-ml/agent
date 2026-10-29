# Private input and provider-key requests

Masking is a display feature, not encryption or a complete secret boundary.

## Asking for a provider key

The root agent in an updated Borg session can call:

```json
{"provider":"work","reason":"Authenticate the configured work model endpoint"}
```

with `request_secret`. `provider` is an existing custom provider ID or a built-in
API-key lane: `openai`, `anthropic`, `openrouter`, `vercel`, `opencode-go`, `kimi`,
`glm`, or `qwen`. The default timeout is 600 seconds (maximum 3600).

Borg shows a masked private prompt naming the destination. Enter an **API key**
there, or press Escape / enter `/cancel` to decline. The value goes directly to
that provider's host-local credential slot; the model receives only a status
receipt, never the key or its length. It is not a normal chat message. The slot
is shared by sessions using that provider on this host. Saving does not select
a model or billing route. If an OpenAI key could implicitly activate API billing,
choose an explicit connection/billing lane first. Environment credentials retain
precedence. Stored custom-provider keys take precedence over legacy inline keys.

Custom keys appear as set in **Settings → Providers**. **Clear authentication**
removes the slot on Save; explicitly replacing the key or choosing an environment
reference also clears the old stored override. Renaming a provider carries its
stored key to the new slot, and removing a provider removes that slot.

## Boundaries and limitations

- Private TUI input is not saved as a resumable draft, command, recall item or
  undo step. The ordinary unfinished prompt is suspended and restored. Older
  unmarked draft files are withheld for local review if a session resumes into
  private input, rather than treating a possible old secret as an ordinary draft.
- Private GUI input is masked and cannot be copied or read back through the
  input-method context. Neither frontend treats it as a slash command.
- Private replies are bound to their source session, request ID and available
  request revision at input capture and checked again on delivery. An expired or
  changed request discards the reply; it cannot fall back to ordinary chat or a
  newer request. Changing private requests clears the previous private buffer.
- Sensitive resolution events contain redaction/status metadata, not answers.
  First-party API-key answers are not passed to a model provider. Generic
  external provider questions can still send answers to that external provider;
  their masking is **not** the first-party secret-consumption guarantee. Those
  requests are labeled provider-owned; providers cannot claim the reserved
  Borg credential-request kind.
- Streaming command journals scrub complete lines, including recognized keys split
  across pipe chunks and multi-line private keys. Overlong lines are omitted
  rather than partially released; following data can also be withheld when a
  private-key boundary is uncertain. This is pattern-based protection, not a
  guarantee for arbitrary passwords or deliberately encoded secrets.
- Credentials use an owner-only directory/file on supported Unix hosts, atomic
  updates and a cross-process lock. They are **plaintext at rest**, not an OS
  keychain. Inline keys in `agent.toml` are also local plaintext. The new request
  tool is unavailable where private-file permissions are not enforced.
- Do not put secrets in chat, tool arguments, URL query strings, request-option
  JSON, screenshots or files you will ask the agent to print. Masked provider
  header JSON can carry credentials; blank input preserves it and `{}` clears it.
- Borg is trusted-user execution: a Full Access agent or same-user process can
  read local files. This flow prevents accidental transcript/model disclosure;
  it is not protection from a malicious agent with local filesystem authority,
  a compromised OS, terminal logging or a clipboard-history service. Remote input
  also trusts the configured host/transport/relay; this change does not add an
  end-to-end encrypted secret transport. Prefer local host entry when that relay
  trust is not appropriate.
- Do **not** request or collect payment-card details, financial PINs or CVVs in
  Borg. Complete payment and password/PIN authentication directly in the original
  application with human confirmation. This is not a payment-data vault.
- Earlier journals, answer receipt digests, draft sidecars, exports and backups are not silently erased
  by this change. Review and rotate credentials previously entered through older
  private prompts if they may have been exposed.

See the [T3 Code source audit](audits/t3-code-2026-10-29.md) for the comparison and
prioritized follow-ups, including write-only settings and OS-backed storage.
