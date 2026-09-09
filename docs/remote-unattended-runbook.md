# Borg Remote unattended-host runbook

## Connect another computer

On each computer whose projects should appear in Borg Remote, run:

```sh
borg remote connect --name "My laptop" --root /path/to/project
```

Approve the computer in the browser using the same Borg account as the web
app. Repeat `--root` to include more projects. Opening the web app on a new
computer does not automatically enroll that computer or expose its files.

On macOS, this installs `~/Library/LaunchAgents/ml.borg.remote.plist`, starts
the outbound connection, and restarts it after process exits and subsequent
logins. The Mac must stay logged in and awake; a LaunchAgent does not run
before login or after logout. Check it with:

```sh
launchctl print gui/$(id -u)/ml.borg.remote
```

After updating Borg, run `borg remote install` from the intended executable
to refresh the service. Linux uses the systemd service described below.
Other platforms can enroll with `borg remote enroll` and run
`borg remote host` under their own login service.

Borg's `/sleep` option (enabled by default; `/sleep on` or `/sleep off`)
prevents idle sleep during active terminal turns. It does not keep an idle
remote host awake, and on macOS it does not override lid-close sleep. Configure
the computer's power settings for unattended availability.

## Linux unattended operation

This runbook covers a personal Linux machine enrolled as a Borg Remote
`trusted_user` host. The host makes an outbound connection to Borg; it does
not require an internet-facing inbound port.

## Install once

Run the install command from the exact Borg binary that should own the
service:

```sh
borg remote install --config "$HOME/.borg/remote/host.json"
```

The installed user service is enabled across logout and reboot with systemd
user lingering. It uses readiness notification, a 90-second watchdog,
five-second restart backoff, and unlimited restart attempts. The watchdog
uses `SIGKILL` and core dumps are disabled so a failed host cannot persist
in-memory credentials. Re-running the command is safe and regenerates the
service from the invoking binary.

## Preflight before leaving

```sh
borg remote status --root "$HOME/path/to/enrolled-workspace"
jq '{server, name, roots}' "$HOME/.borg/remote/host.json"
loginctl show-user "$USER" -p Linger
systemctl --user is-enabled borg-remote.service
systemctl --user show borg-remote.service \
  -p ActiveState -p SubState -p MainPID -p NRestarts \
  -p Type -p NotifyAccess -p WatchdogUSec
journalctl --user -u borg-remote.service --since '10 minutes ago' \
  --no-pager -n 100
borg doctor --json
df -h "$HOME"
```

Check all of the following:

- Every provider needed on the trip is authenticated and `can_spawn` is true.
- The intended workspace appears in the host config's `roots`. The `--root`
  argument to `remote status` checks a candidate root; it does not enroll it.
- Linger is `yes`; the service is enabled, active, and running.
- The effective service has `Type=notify`, `NotifyAccess=all`, and a
  90-second watchdog.
- The journal contains a recent `Borg Remote host connected` message.
- The Borg database reports healthy WAL, foreign keys, and synchronous mode.
- There is ample free disk space. Investigate before free space falls below
  the amount a long turn, build, and WAL checkpoint may need.

Never print the complete host config in logs or support messages: it contains
the host bearer token. The config directory should be mode `0700` and the
config and state files mode `0600`.

## Automatic recovery contract

| Fault | Expected automatic behavior |
| --- | --- |
| Borg process exits or is killed | systemd starts a new process after five seconds. |
| Borg process hangs | Missing watchdog notifications cause systemd to replace it. |
| borg.ml or the network is unavailable | The host keeps running, retries with bounded backoff, and reconnects without exhausting the systemd watchdog. |
| User logs out or the machine reboots | User lingering starts the enabled service without an interactive login. |
| Host dies after durably admitting a prompt | The persisted cursor and pending action allow startup recovery without silently skipping the admitted prompt. This does not establish recovery of every launch/bootstrap or host-shell failure. |

After a network interruption, allow up to a few minutes for DNS, network
readiness, backoff, and presence propagation before intervening.

## Web session reliability and upgrades

Host presence, session presence, event delivery, and command delivery are separate
checks. An online host does not prove that a particular terminal session is
mirroring output or accepting controls.

Current terminal mirrors negotiate `command_scope: session_v1` at registration.
Their command polls and acknowledgements are session-scoped; the background
host cannot consume those commands. Output upload and presence renewal continue
independently of command long-polls. Prompts are journaled locally before the
mirror acknowledges delivery.

After a lost command-poll response, delivery can wait for the outstanding
60-second claim lease to expire. Later commands must not bypass that lease: a
claim proves reservation, not receipt or execution. Arbitrary controls are not
exactly-once; prompt retries use durable message identities.

A new hosted launch that exceeds the host session limit, uses an invalid
working directory, exceeds the 512 KiB serialized launch-metadata limit, or has
an invalid same-host workspace attachment is durably marked Failed and published
before acknowledgement.
It does not wait at the head of the command queue, where it could prevent Stop
from freeing a slot. Retrying the same launch replays the failure, even after a
slot opens; stop another session or correct the directory and create a new
session instead. If failure publication cannot reach the relay, the launch
remains unacknowledged until publication succeeds.

Oversized launches and newly rejected same-host attachments keep a bounded
rejection record with a SHA-256 fingerprint of
all decoded launch metadata (request and attachment), atomically owned by the
host UUID and relay origin. The rejected payload is not retained or executed.
An exact retry replays rejection; a different request cannot replace it, even
if the replacement is small enough. This also handles oversized commands already
queued by an older relay. A restart between saving the rejection and creating
its failure journal relies on the still-unacknowledged command being replayed;
once the journal exists, background upload recovery can publish it too.
Later controls for a rejected session settle locally once its terminal journal
exists, rather than restore an actor or require another upload. If only the
rejection metadata exists, controls remain unacknowledged until launch replay
creates the failure journal.
Older host binaries cannot read these new rejection records and may retain
related commands instead of making progress; use an updated host to drain them.
Attachments naming another host still fail the ownership fence without writing
metadata or a failure journal; they are not converted into locally owned
rejections. Ownership conflicts and relay publication failures can still block
the host command queue. There is not yet a relay-side launch-size check, so
oversized admission is a host-visible Failed result,
not an immediate HTTP validation error in the launch form.

Rejection replays preserve the original reason and fingerprint even when a clock
change makes a previously expired lease appear valid. Already-admitted executable
metadata remains immutable when its lease expires. An exact launch replay can
acknowledge the existing actor or settle its terminal state without starting new
work; an unstarted launch replay with an expired lease fails visibly. Restoration of a
nonterminal actor still requires a valid attachment. Stop may settle that inactive
session locally despite lease expiry, but only after host ownership, structural
attachment identity, and any explicit Stop grant have been checked. This neither renews the lease nor starts
an actor. Eligible plain prompts can be durably deferred while the lease is
expired, allowing later Stop commands through. Non-deferable commands can still
block later controls; lease renewal for started sessions and general queue
fairness remain open.

New hosted launches also persist an unfinished-bootstrap record before
acknowledgement. Recovery can therefore find a launch even before its session
or initial prompt exists. The record hands off to normal prompt recovery only
after the initial prompt is durable (or an empty session reports Ready).
Pre-actor startup errors record a generic Failed status; host logs contain the
diagnostic, and a fresh launch is required after correcting the cause. Failed
publication is retried by the independent journal worker, including after restart.
Already-terminal sessions replay their journal rather than execute again.

An acknowledged, unstarted bootstrap whose attachment expires or becomes invalid
is marked Failed and locally settled during recovery, even at full execution
capacity. Terminal bootstrap markers are also settled locally. Recovery holds
the session writer lease through status and action/bootstrap settlement, preserves
launch metadata and terminal states, and does not await a relay probe or upload.
The journal worker retains responsibility for publication after the marker is
removed, so the browser may not observe Failed until the relay recovers.
Command-triggered rejection still publishes successfully before acknowledgement.
Active actors, held writer leases, started nonterminal sessions, foreign owners,
and unverified legacy launches are not failed by this recovery path. Started
sessions with invalid attachments still await authorized recovery; this does not
renew leases or solve all actor-initialization failures.

Actor recovery inspects at most 256 pending launches per command-poll pass,
advancing across pages even when earlier sessions are active, lease-blocked,
unverified, or capacity-blocked. Owned launches retain priority over unverified
legacy launches; creation time and session ID give each page a stable order.
The in-memory offset wraps at the end and resets on host restart. Settlement or
ownership changes can move entries between passes, so a skipped entry is
revisited on a later sweep rather than considered acknowledged. This bounds
returned candidates per pass, not database scan cost or wall-clock recovery
latency. Restoration still requires the existing ownership, lease, capacity,
and writer fences; idle sessions without pending work are not newly restored.

An independent upload-only recovery loop also scans inactive hosted journals
against durable, confirmed relay cursors. Final session events and remaining
live-state snapshots are retried even if the actor already exited, including
after host restart or a lost successful-upload response. Recovery does not
restart the actor or consume an execution slot. It pages past active or failing
sessions and preserves upload backoff, independently of host command polling.
On the first upgraded start, historical hosted journals are checked too; they
are not assumed delivered. Missing relay sessions (404/410) retain their local
journal and retry with a five-minute backoff.

These are additive SQLite changes, not a database reset. They do not establish
full recovery for idle actors, every failure inside actor initialization,
workspace/private-message final delivery, or loss of the relay database itself;
those need separate acceptance.

Upgrade the relay before the agent for full remote controls. Against a relay
that does not confirm session-scoped commands, the new mirror still uploads
output and renews presence, but deliberately does not poll or acknowledge the
shared command queue. Its log reports read-only mirroring and the need for a
relay upgrade. Updating a binary does not upgrade already-running terminal
sessions; do not restart active work just to refresh discovery. `borg remote
sync --session SESSION_UUID` refreshes discovery/inbox state, not the process
version or its event mirror.

Before considering a rollout verified, exercise an isolated test session:

1. Keep two terminal sessions and the background host online. Send distinct
   prompts from the browser and verify each reaches only its intended journal.
2. Leave a command long-poll pending while output is generated. Verify output
   continues appearing, including the terminal status when the session closes.
3. Disconnect/reconnect the browser and briefly interrupt relay connectivity.
   Verify missed output catches up, queued prompts survive, and no prompt or
   old stop command is replayed after reconnection.
4. Leave the session idle beyond the presence window and verify it remains
   controllable. Check both the host and the session, not just the fleet badge.
5. Launch a session while discovery refresh is delayed, then switch sessions
   with text in the composer. Verify selection and drafts stay with the intended
   session, and failed controls produce a visible error.
6. Fill a disposable host to its configured session limit, attempt one more
   launch, then Stop an existing session. Verify the extra launch is visibly
   Failed, Stop reaches its target, and the rejected launch does not start when
   capacity opens. An invalid working directory and a launch over the 512 KiB
   metadata limit must also fail visibly without blocking the later Stop.

Use disposable sessions for stop/restart and connectivity fault tests. Never
kill active user work to prove recovery, and never expose host tokens while
collecting evidence.

## Inactive-session capacity and Stop

Restoring an inactive hosted session from a command now respects the same
process-local execution limit as Launch and restart recovery. At capacity or
with an expired attachment presence lease, a plain prompt (no output schema)
for an already started/configured session can be acknowledged without starting
an actor **only when both its local journal message and action are durable**.
Stored host/relay ownership, structural attachment identity, and any explicit
Prompt grant must still pass before admission. This is durable input retention,
not execution permission or a renewed lease. Queue/Steer delivery and attachment
paths are retained in the journal. The pending-action scan schedules the prompt
only once capacity and a valid attachment allow restoration, without needing
another relay command. Exact retries reuse the same message/action rather than
creating a second turn. If the workspace inbox also contains the same ID, an
existing local action still requires validation against the original journal
admission: changed text, attachment paths, or delivery mode is not an exact
retry. A coalesced action payload does not replace that original baseline.
A later Stop can cancel the deferred action locally;
no actor is created merely to admit or cancel it.

This is deliberately not a general deferred-control queue. Schema-bearing
prompts, sessions without durable startup/configuration, workspace-only or
inherited messages without a local action, and other inactive-session controls
remain unacknowledged when capacity or lease expiry prevents restoration. They
can still block later relay commands.
Do not mistake a workspace message existing for proof of local prompt replay,
or silently drop an output schema to admit a request.

Stop for an inactive session does not start an actor or provider. With the
session writer lease held, the host persists Stopped, cancels unfinished local
actions (retaining transition history and invalidating their leases), and
settles its bootstrap marker. A busy writer retains the command for retry;
an empty local actor map alone is not proof that another process has stopped.
The independent journal worker publishes terminal output, including after
restart or relay outage. Acknowledgement here means durable local settlement,
not immediate browser visibility.

An inactive terminal session is not resurrected by a late command or duplicate
Launch. Startup rechecks terminal state under writer ownership as well, so a
Stop recorded while startup awaited the relay wins over that stale startup.
Historical queue events are retained. These host checks do not establish
explicit local CLI reopening behavior or recipient-side message consumption.
Terminal launch rejection still uploads its failure before acknowledgement.

## Hosted actor ownership and synchronization faults

The hosted session supervisor owns its actor task, and the actor owns its
provider-turn task and action-lease heartbeat. Dropping or aborting the
supervisor cancels these owned tasks rather than detaching execution and
continuing to renew an abandoned action lease. On fatal synchronization
rejection the supervisor explicitly aborts and joins the actor before returning.

During active execution, journal/projection decoding or storage errors in output
synchronization are logged and retried after a two-second backoff without
tearing down the actor or its command channel. Fixing the local projection or
restoring storage availability allows upload to catch up. This is not automatic
repair of malformed durable data; do not delete journal rows to clear an error.
Existing network-failure backoff and missing-session retention still apply.

HTTP 401 from event, payload, live-state, or workspace/private-message uploads
remains fatal rather than becoming a retryable synchronization error. A journal
replay conflict (HTTP 409) is also fatal, with relay conflict details retained.
Rejected output is not acknowledged locally. Inbox/directory/roster refreshes
retain their existing internal retry policies; this is not a blanket new
revocation policy for every relay endpoint.

Disposable localhost tests cover sync fault recovery, Stop after recovery,
upload rejection, supervisor cancellation, provider-future cancellation, and
cessation of action-lease heartbeats. They use a pending fake executor, not a
live model. Owned Rust task cancellation does not prove cleanup of every
external provider process or detached helper. Actor errors can still leave
nonterminal durable work eligible for later recovery. Normal startup and final
synchronization retain their existing behavior; independent recovery workers
publish retained session output and hosted workspace/private messages after the
actor exits. Production acceptance remains required.

The host wraps each hosted supervisor in an abort-owned Tokio task. An unwinding
panic in that supervisor now reaches ordinary failure handling and route cleanup
instead of leaving a closed sender permanently counted as active. Unstarted
launches use the existing durable rejection path; started journals and pending
work remain eligible for normal recovery. Cleanup still waits for that error
handling, and route removal is not proof that every child or external process has
finished cancellation. This is not protection against process aborts or panics
or cancellation in the outer cleanup task itself.

## Hosted duration expiry

The host duration timer covers the actor supervision loop, including awaited
journal/message synchronization: a slow relay cannot defer cancellation until
its HTTP timeout. On expiry the supervisor aborts and joins its actor, retains
the session writer lease, records Failed with a duration-limit reason, and
settles unfinished actions/bootstrap markers. Already-terminal status is not
overwritten. Final publication is left to the independent recovery workers;
expiry does not wait for a relay response before releasing the execution slot.

Recovery subtracts elapsed wall time since the original durable SessionStarted
from the currently configured host limit; restart does not grant a fresh full
budget. An already-expired, verified-owned session settles locally before
runtime-context/provider startup, including with an unavailable relay. The pending
recovery scan performs this settlement before presence-lease and capacity gates,
so expired work does not need an execution slot or a renewed attachment to be
marked Failed locally. It rereads the deadline under the writer lock; a busy
writer defers only that candidate. Active actors remain supervised by their
existing duration timer. Ownership and writer fencing still apply, and legacy
ownership verification can still require relay access. Downtime and idle time
count toward the recovered budget;
clock changes and deliberate host-limit changes affect this calculation.

This is not an idle-session discovery sweep: inactive sessions with no pending
work are checked when restoration is attempted. Storage failures can defer
terminal settlement, and cooperative Rust task cancellation does not prove that
all external provider processes or detached helpers have stopped. Production
acceptance remains required.

## Final hosted workspace and private messages

A separate output recovery worker drains workspace/private messages for
inactive hosted sessions, even when their session journal is already caught up
and all actor slots are occupied. It never starts an actor/provider, imports an
inbox, refreshes presence, or renews a lease. Legacy launches first require the
one-time ownership verification described below. Active hosted sessions retain their
own uploader; both paths persist progress in `host_workspace_cursors`, keyed by
host, session, and workspace. This additive current-v5 table does not rewrite
journal history. Reopening the database preserves acknowledged progress.

Only sessions with durable hosted launch metadata and a workspace binding to
the current enrolled host are scanned. A binding/launch identity mismatch is
retained for diagnosis, not silently reassigned. Stored host identity is also
checked on hosted startup, control, and journal recovery paths. Local CLI mirrors without
hosted launch metadata are outside this worker; their mirror or explicit
`borg remote sync --session SESSION_UUID --send-pending` remains responsible.

Network errors, HTTP 503, or a lost success response retain the exact message
idempotency key for replay. Cursors advance only over handled events; they never
regress on stale checkpoints. A cursor is an upload/disposition boundary, not
proof the recipient actor consumed the message. Existing permanent private
message rejection policy remains: most HTTP 4xx responses record Failed locally;
401 and 429 are not treated as delivered. Shared-workspace 404 retains the
message and schedules a 300-second retry for that workspace, not its sender.
Other transient upload failures retain a two-second per-workspace backoff.
Worker attempts are bounded to ten seconds per session and scan
past deferred/active sessions, independently of journal and command polling.

Successful old-binary uploads have no local durable cursor, so the first new
recovery pass can replay historical messages with their original IDs. Relay
idempotency is required for that replay and concurrent uploaders. Rollback to
an old binary leaves the new cursor table intact but removes inactive message
recovery until a capable binary returns. Never delete workspace events or
cursor rows to force recovery.

A shared-workspace 404 or transient message upload failure no longer ends the
entire upload pass: other workspaces, including private conversations from the
same sender, are still attempted. The failed workspace retains its cursor and
FIFO order; successfully handled workspaces checkpoint their own progress. A 401
still aborts the pass without acknowledging the rejected message.

Message-upload retry timers are scoped to each workspace and are independent
of the session journal retry timer. Inactive recovery retains them across
worker passes and cancelled attempts, so a new private message can upload while
an unrelated shared workspace remains in its five-minute backoff. Recovery
reloads confirmed SQLite cursors on every attempt; a cancelled cursor checkpoint
cannot make uncommitted in-memory progress authoritative. Expired timer entries
are discarded. Timers are process-local: a host restart can retry earlier, with
unchanged durable message identities and cursor/idempotency protection.

This is not fully independent scheduling: authorization/storage errors still
back off the sender session, a slow request can exhaust the ten-second recovery
budget, and a large earlier workspace can delay later messages. Directory and
roster requests are still awaited before outgoing messages. It is not a general
per-recipient outbox or a guarantee that unavailable/deleted recipients accept
output.

For running shared-workspace sessions, a roster 404 disables shared uploads but
no longer permanently disables discovery. Roster probes continue every thirty
seconds even while unavailable. A successful response must decode and persist
its roster before re-enabling a previously disabled route; queued messages then
resume with their original identities. Transient HTTP/network errors, malformed
rosters, and roster projection failures retry after five seconds while retaining
previous availability. A fresh successful roster is refreshed after thirty
seconds, not on every upload tick. This does not change roster membership-pruning
or authorization/revocation policy, and it does not require an actor restart.
Inactive upload-only recovery still does not fetch rosters or renew presence.

## Stored host identity after re-enrollment

Hosted startup, command dispatch, inactive Stop/terminal settlement, launch
rejection cleanup, and journal recovery check the stored workspace binding and
any host identity in the original launch attachment. If either names another
host, they retain the work and reject the operation instead of rewriting its
binding, settling its actions, or uploading with the new host credentials.
Startup checks before fetching runtime context and again under the session
writer lease after that await. Local settlement also rechecks under the writer
lease. The original owner can still Stop its inactive session while the relay
is unavailable.

Pending-launch recovery filters known foreign identities before applying its
bounded result limit, so an old enrollment does not fill that recovery page and
hide eligible work for the current host. Foreign relay commands remain
unacknowledged and can still block later commands on that relay queue; this is
not a command-transfer or remote rejection-result protocol.

New hosted launches atomically persist an immutable `(host_id, relay_origin)`
owner in `host_launch_owners` with their launch metadata, before acknowledgement
or actor startup. The origin is the normalized scheme/host/port: trailing slashes
and default-port spelling do not create a new owner, but a different relay does.
Exact launch retries preserve that owner; another host or relay cannot adopt the
same local launch ID. A failed owner write rolls back launch admission too.
Token rotation for the same host and origin does not change this ownership key.

The additive current-v5 table leaves legacy rows unowned. It never infers their
owner from the currently enrolled host. Background journal recovery scans these
rows even without a session journal and verifies `/sessions/SESSION_UUID/sync`
using the configured host credentials. Only a successful, valid response permits
a one-time owner claim; the SQLite transaction rechecks local binding/attachment
identity and any concurrent owner claim. A 401, 404, outage, or malformed reply
retains the original metadata/bootstrap without constructing an actor, settling
work, or inventing ownership. Known mismatches are rejected before any probe.

The main command/recovery poller does not wait for these legacy network probes.
It retains unverified legacy commands and waits for background verification;
verified owned launches are prioritized over unowned rows in the bounded launch
recovery scan. First recovery after upgrading an unowned legacy launch therefore
requires relay availability, including before an inactive Stop can be accepted.
Once verified, ownership survives restart and the same owner can again Stop
locally while offline. Legacy verification does not prove recipient consumption
or publish output from a session that has not yet been created.

A binding already overwritten by an older binary is not automatically repaired;
conflicting durable evidence remains an error. Local CLI mirrors without hosted
launch metadata remain outside this ownership table. Old binaries do not enforce
the new owner/origin checks: retain the table on rollback and do not use an old
binary to re-enroll or take over shared state. Do not edit host IDs in the database
or restart active actors as an automatic repair. Explicit ownership transfer and
local CLI mirror re-enrollment policy remain separate work.

## Deferred shell and workspace commands

Shell and workspace command acknowledgements mean **durable local admission**,
not execution success. The host stores the immutable command in
`host_operation_queue` in `sessions.sqlite3` before advancing its relay cursor.
An independent, serial worker executes these operations and uploads their
results; a long command or failed result upload does not block host polling,
Stop/Interrupt delivery to session actors, presence, or session journal upload.
Filesystem operations and OpenTerminal still run on the polling path.

The deferred lane preserves FIFO among valid commands, including waiting for a
result upload before starting the next operation. Other command kinds may pass
it. Callers with dependent operations must await the actual result, not just
command admission. Execution timeouts do not include time spent waiting in the
local queue; the web request may time out first and must reconcile its request
ID rather than assume no side effects occurred.

A process-level worker lock prevents two hosts sharing the same session root
from executing the lane concurrently. Completed receipts replay the exact
stored result after restart or a lost upload response. A Started receipt without
a completed result becomes Indeterminate after worker ownership is released;
Borg does not re-execute uncertain side effects. The queue entry is removed only
after result acceptance (or relay 404/410), while its receipt remains. This is
conservative receipt recovery, not a universal exactly-once guarantee.

CancelWorkspaceCommand durably records Cancelled for an unstarted queued
workspace command. Cancellation and execution use the same per-request lock;
cancellation cannot overwrite a live or prior Started/Terminal receipt. If
ownership exists but admission is not yet durable, cancellation remains
unacknowledged for retry. This does **not** stop a command whose execution has
already started. Shell commands have no cancellation command. Cancelling a queued
workspace command prevents execution even if its Cancelled result must wait
behind another operation for upload.

Invalid persisted UUID/JSON, oversized payloads, unsupported command kinds, and
mismatched identities are quarantined: retained with `quarantine_reason` and
logged, but skipped so later valid operations can proceed. They are not marked
successful or automatically retried after an upgrade. No result is uploaded for
a quarantined row; the web request times out and reconciliation remains pending
until diagnosed. Inspect queue metadata without printing command payloads
(which may contain private data):

```sql
SELECT sequence, request_id, host_id, quarantine_reason
FROM host_operation_queue ORDER BY sequence;
```

Do not clear quarantine, delete receipts, or reassign host IDs blindly. A new
host enrollment does not authorize executing an old host queue. Admission
failures (including conflicting request identity, payload bounds, or storage
failure) retain the relay command with a warning rather than acknowledge lost
work; these can still block later relay commands and require diagnosis. Keep
the database and lock files intact during recovery; per-request lock files
accumulate and must not be unlinked while hosts are active (that could split
lock ownership across inodes). Rolling back to a binary without this worker
leaves locally admitted operations pending until a capable binary runs again; they are no longer recoverable from the relay cursor alone.

## Diagnose through an independent connection

If borg.ml still shows the host offline, use the independently tested access
path, then run:

```sh
systemctl --user status borg-remote.service --no-pager
journalctl --user -u borg-remote.service --since '30 minutes ago' \
  --no-pager -n 300
systemctl --user reset-failed borg-remote.service
systemctl --user restart borg-remote.service
```

Confirm both `active (running)` and a new `connected` journal line. If the
unit was edited, moved, or partially updated, regenerate it:

```sh
borg remote install --config "$HOME/.borg/remote/host.json"
```

If authentication was revoked, re-enrol the host from the Borg Remote page.
Do not copy a host token through chat or place it in a shell history entry.

## Binary rollback

Keep the previous executable beside the installed binary. Do not merely copy
an old executable over a new one and restart: an old Borg that predates
readiness notification cannot satisfy a newer `Type=notify` unit.

Instead, invoke the preserved executable's installer directly:

```sh
/path/to/previous-borg remote install \
  --config "$HOME/.borg/remote/host.json"
```

That writes a service compatible with that binary and points `ExecStart` at
the preserved path. Verify the process and connection as above. To roll
forward, invoke `remote install` from the current binary again.

## Disk and database recovery

Start with read-only checks:

```sh
df -h "$HOME"
du -sh "$HOME/.borg"
borg doctor --json
```

Do not delete `sessions.sqlite3`, `workspaces.sqlite3`, or any `-wal`/`-shm`
file while Borg processes are running. Stop the affected Borg services and
take a filesystem-level copy before attempting manual SQLite repair. A full
disk is not fixed by repeatedly restarting the remote service.

## Independent access and physical failures

The Borg connection cannot repair itself if the binary, user service manager,
host token, whole network, or machine is unavailable. Before departure, test
an independent path from the actual travel laptop, preferably SSH over a
private overlay network such as Tailscale:

```sh
tailscale status
tailscale ping HOST
ssh USER@HOST
```

Test a real login and `systemctl --user status borg-remote.service`; a daemon
that merely says `active` is not enough. Keep SSH private to the overlay
network rather than forwarding port 22 from the public internet.

Also verify that the desktop will not suspend, that firmware restores power
after an outage if desired, and that the router and machine have reliable
power. A UPS is the only automatic recovery from many short power cuts.

## Departure acceptance test

The host is ready to leave unattended only after all of these pass:

1. Borg Remote launches a small real turn in every provider needed on the
   trip and can access every intended enrolled root.
2. Killing the service's main process produces a new PID and a new
   `connected` journal line without manual repair.
3. The preserved previous binary can install and connect, and the current
   binary can then restore the hardened unit.
4. A reboot returns the host to `active (running)` and connected without a
   local login.
5. The travel laptop can reach an independent shell after the reboot.
6. Disk space and database health have comfortable margins.
