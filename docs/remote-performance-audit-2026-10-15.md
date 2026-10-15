# Remote performance audit — 2026-10-15

## Verdict

The sampled network path is healthy, but the running host's session publisher
is not. Host presence alone currently gives a misleading impression of health.
The source repair below is tested, not deployed by this audit.

## Fresh measurements

Non-disruptive production probes against `https://borg.ml`, from the enrolled
Linux host, at approximately 16:27–16:34 UTC:

| Check | Result |
| --- | --- |
| HTTPS `/api/health`, eight fresh requests | 8/8 HTTP 200; median 199 ms, maximum 442 ms |
| WebSocket upgrades, eight fresh connections | 8/8 successful; 149–182 ms |
| WebSocket ping round trip | Median 48 ms; range 42–52 ms |
| Installed host | Connected; zero automatic restarts in its current two-hour run; about 159 MiB memory |
| Local root-session publication | 11 of 21 owned roots had no remote view checkpoint |
| Root durable backlog | About 35,000 events beyond committed projection watermarks; excludes children and live text revisions |
| Session directory | HTTP 200 for an older registered session; HTTP 404 for both active audit and Abundance sessions |
| `borg remote sync` for the audit session | Failed with directory HTTP 404 |

The WebSocket samples were upgrade/ping diagnostics only, without authenticating
as the existing host, which could replace its live connection. These are not
commit-to-browser-paint measurements or a cross-machine agent round trip.

The retained host log contained 87,118 catalog-recovery errors since October 14:
24,555 checkpoint-budget errors, 11 missing-child-row errors, 62,548
noncontiguous-revision errors, and four pool timeouts. The running host continued
logging noncontiguous-revision errors roughly once per second during the audit.
No production session, enrollment, database record, service, or network setting
was changed to obtain these measurements.

## Confirmed failure and source repair

1. Bounded transcript eviction removes an old settled child's row while retaining
   its ownership and metadata. `ViewReducer::apply_child_meta` nevertheless
   required that row and failed after advancing its cursor.
2. Catalog reconciliation stopped at the first failed scope. Its mutated reducer
   remained in memory ahead of the durable checkpoint, causing repeated
   noncontiguous-revision errors and preventing later sessions from publishing.

The repair accepts metadata-only updates for evicted children without appending
historical cards again. Scope recovery discards uncommitted in-memory projectors
(including lifted parent metadata), continues scanning other scopes, and retries
failed work from durable checkpoints. Retry waiting remains active only while
recovery is needed. Persistent checkpoint-budget or storage faults are isolated,
not silently skipped or considered repaired.

## Verification

- Original `borg-remote` library suite: 68 passed, one profiling test ignored.
- Added a disposable PostgreSQL fault-injection regression: an unwritable first
  scope must not starve a later scope; after storage repair it must resume at
  the committed revision. It failed before the repair and passed afterward.
- Extended the child-summary regression through row eviction and checkpoint
  restore. It reproduced `missing child summary row` before the repair and
  passed afterward, including idempotent metadata replay.
- Repaired `borg-remote` and `borg-view` library/integration suites: 107 passed,
  zero failed, one profiling test ignored. Includes the existing 100,000-event
  bounded-state test.
- Builds and tests used a bounded Cargo lane and disposable fixture databases,
  not the production journal. Raw evidence is retained under
  `~/agent-evidence/remote-audit-d9c568ac/` on the audited host.

## Remaining acceptance

The running host still needs the repaired binary and a controlled restart before
production recovery can be claimed. Then verify new-session registration,
backlog convergence, browser controls, and output catch-up. Do not restart active
user actors merely to obtain acceptance evidence.

Browser commit-to-paint latency, cold/warm large-history loading, real packet-loss
recovery, cross-machine message final delivery, and provider turns were not
established by these probes. The product repository's remote soak harness itself
states that it is not yet a complete acceptance pass. HTTP command-poll sections
in `remote-unattended-runbook.md` describe the legacy transport, not current
protocol-6 WebSocket command delivery; collaboration HTTP routes remain in use.
