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
uses normal cancellation and goal-state semantics. Its receipt says `admitted`: the recipient
must still confirm actual pixel visibility before claiming visual acceptance.
This does not change provider, credentials, or billing. The selected provider
must support images.

The file command accepts any number of PNG/JPEG files of up to 50 MiB each and
checks content signatures before delivery. A file over 4.5 MiB is re-encoded to
fit (JPEG, scaled only if needed) rather than refused; a tool result carries 20
images, and the model is told to fetch any beyond that in another call. It reads only the files explicitly
selected. With no image channel and no explicit target, it fails rather than
printing image data into the transcript.

## Showing images to the user

`show_image({"paths":["/absolute/path/capture.png"],"caption":"Optional caption"})`
posts a visible assistant message, rather than returning pixels only to the model.
It copies any number of PNG, JPEG, GIF or WebP files (sources up to 50 MiB; one
over 20 MiB is re-encoded to fit) into the
session attachment store, so the message does not depend on temporary source files.
Relative paths resolve against the session shell directory. The tool requires an
active turn and a session attachment store.
