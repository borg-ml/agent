# Exclusive Unreal commandlets and imports

A template can be inspected with
`python3 /path/to/extensions/unreal/bin/unreal.py --project "$PWD/Game.uproject" --engine-root /path/to/UE --participant-id UUID --session-id UUID run commandlet --spec -- /path/to/UnrealEditor-Cmd "$PWD/Game.uproject" -run=ResavePackages`.
Kinds are `commandlet`, `import`, `verify`, `exclusive`. This does **not** run
anything. Non-spec runs fail closed: the core must first atomically yield the
project's live editor, then admit an exclusive project job, and resume safely.
A service belonging to another package is not automatically yielded. Coordinate
with its owner; never kill or bypass the shared editor or run against its live
checkout. No package-local lock/hook substitutes for Borg lane admission.
