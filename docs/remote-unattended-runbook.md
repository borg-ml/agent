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

A new hosted launch that exceeds the host session limit or uses an invalid
working directory is durably marked Failed and published before acknowledgement.
It does not wait at the head of the command queue, where it could prevent Stop
from freeing a slot. Retrying the same launch replays the failure, even after a
slot opens; stop another session or correct the directory and create a new
session instead. If failure publication cannot reach the relay, the launch
remains unacknowledged until publication succeeds.

New hosted launches also persist an unfinished-bootstrap record before
acknowledgement. Recovery can therefore find a launch even before its session
or initial prompt exists. The record hands off to normal prompt recovery only
after the initial prompt is durable (or an empty session reports Ready).
Pre-actor startup errors publish a generic Failed status; host logs contain the
diagnostic, and a fresh launch is required after correcting the cause. Failed
publication is retried by the host recovery loop, including after restart.
Already-terminal sessions replay their journal rather than execute again.

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
   capacity opens. An invalid working directory must also fail visibly.

Use disposable sessions for stop/restart and connectivity fault tests. Never
kill active user work to prove recovery, and never expose host tokens while
collecting evidence.

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
