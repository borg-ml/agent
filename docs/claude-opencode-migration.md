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

## Remaining verification

Capture matched external Go model/cache/tool baselines; check mixed-protocol
models and preserve route-specific semantics before claiming full coverage.
Verify tool approval, cancellation and compaction on the Go application lane.
Do not remove the generic OpenCode compatibility path while other routes still
use it. Claude direct subscription migration requires a supported access change
or explicit provider authorization, not an API billing substitution.

A bounded MiniMax M2.7 attempt through the existing native Go Chat Completions
path did not complete within 45 seconds and entered provider/network retries.
It is not a passing compatibility receipt; mixed-protocol behavior remains
unresolved. No credentials or billing routes were changed in response.
