# Native remote sessions

The recommended first version of laptop-to-PC attachment is the existing Borg
terminal interface over SSH, not another public Borg control socket and not the
Borg web application.

On the PC, leave the owning Borg session running. From the laptop, discover and
attach to it with:

```sh
ssh pc.example 'borg inspect live --json'
ssh -t pc.example 'borg resume SESSION_UUID'
```

The second command runs the Borg TUI on the PC through an SSH PTY. `borg resume`
detects the existing session owner and attaches through Borg's local Unix
control socket while tailing the durable SQLite journal. Closing the SSH client
detaches that view without stopping the owning session.

This route already covers the important operational properties:

- Discovery uses `borg inspect live`; a future `borg remote attach --ssh HOST`
  command can combine discovery and selection.
- Authentication, host verification, encryption, and optional hardware keys
  remain SSH's responsibility.
- Reconnecting repeats `borg resume SESSION_UUID`; the durable journal restores
  missed events.
- Terminal latency is one SSH stream with no browser rendering layer.
- PTY resize messages keep the remote Borg viewport synchronized with the
  laptop terminal.
- The PC remains the sole session owner and filesystem authority, avoiding a
  split-brain replicated session store.

The main limitation is local clipboard image capture: the Borg process runs on
the PC, so direct clipboard APIs see the PC clipboard. Bracketed text paste and
OSC 52 copy continue to cross SSH. A later convenience client can proxy image
bytes as normal Borg attachments without changing session ownership.

If a dedicated transport is added later, it should reuse the typed local
control commands and durable event stream behind an authenticated relay. The
Unix socket itself should remain local-only; exposing it over TCP would weaken
the current single-owner trust boundary.
