# Unreal build templates

In a private project worktree, inspect `.uproject`, engine root, available RAM
and output disk. First generate a template:

`python3 /path/to/extensions/unreal/bin/unreal.py --project "$PWD/Game.uproject" --engine-root /path/to/UE --participant-id UUID --session-id UUID build --spec GameEditor Linux Development "$PWD/Game.uproject"`

After core lane job CLI integration, omit `--spec` to submit; use `--wait` only
for a deliberate blocking shell call. Borg core owns queue/admission/logs;
inspect with `borg lane job status|logs|wait ID` when available. Do not call
Build.sh concurrently in the same worktree or treat template generation as
admission. `-Clean` disables pending-job coalescing. Never delete another
worktree's outputs. The UBT helper handles only its startup hazard and symbols.
