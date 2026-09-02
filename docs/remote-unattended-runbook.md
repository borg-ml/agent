# Borg Remote unattended-host runbook

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
five-second restart backoff, and unlimited restart attempts. Re-running the
command is safe and regenerates the service from the invoking binary.

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
| Host dies while accepting a command | A command is acknowledged only after durable admission; the persisted cursor and pending action allow startup recovery without silently skipping it. |

After a network interruption, allow up to a few minutes for DNS, network
readiness, backoff, and presence propagation before intervening.

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
