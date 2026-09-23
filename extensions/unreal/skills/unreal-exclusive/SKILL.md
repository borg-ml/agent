# Exclusive Unreal commandlets and imports

A template can be inspected with
`python3 /path/to/extensions/unreal/bin/unreal.py --project "$PWD/Game.uproject" --engine-root /path/to/UE --participant-id UUID --session-id UUID run commandlet --spec -- /path/to/UnrealEditor-Cmd "$PWD/Game.uproject" -run=ResavePackages`.
Kinds are `commandlet`, `import`, `verify`, `exclusive`. This does **not** run
anything. Non-spec runs fail closed, and v0 model-facing exclusive
templates remain disabled: core's fake-editor handoff cannot atomically delay
preemption for a foreign active client lease. Coordinate with the editor lease
holder before any operator-submitted trusted JobSpec; do not kill or bypass a
shared editor or run against its live checkout. Core yields matching canonical
Project-key services only when they declare that resource; never assume all
services or unmanaged editors are covered. No package-local lock/hook
substitutes for Borg lane admission.
