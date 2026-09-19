# Images through the shell-first interface

For an existing PNG or JPEG, run:

```sh
borg image /absolute/path/capture.png
```

Native `exec` provides a private image channel. The command sends image data
through that channel, separately from truncated shell output. The same channel
lifts `borg call computer_use` image results automatically; desktop permissions
and screenshot scope still apply. Do not print base64 and assume the model saw it.

## Running sessions without the new image channel

A newly installed CLI can deliver selected files to an already-running local
session without restarting its owner:

```sh
borg image /absolute/path/capture.png --session SESSION_UUID
```

Use the actual session UUID, not a project name. The host-local team prompt
preserves explicit user-stop behavior. Its receipt says `admitted`: the recipient
must still confirm actual pixel visibility before claiming visual acceptance.
This does not change provider, credentials, or billing. The selected provider
must support images.

The file command accepts one to four PNG/JPEG files, each at most 4.5 MiB, and
checks content signatures before delivery. It reads only the files explicitly
selected. With no image channel and no explicit target, it fails rather than
printing image data into the transcript.

## Verification

The real CLI and native process manager transported a real PNG byte-for-byte
while leaving only a short JSON descriptor in stdout. A running-session check
also delivered the Abundance equipment screenshot to the model as actual pixels:
the selected Equipment tab, central unpublished-model panel, and right-hand
inventory/socket telemetry were visually identified. This verifies transport;
it is not acceptance of that application UI.
