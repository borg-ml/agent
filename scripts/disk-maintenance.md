# macOS disk maintenance

`disk-maintenance.py` reduces duplicate immutable Git-object storage on APFS.
It does **not** remove checkouts, working files, commits, saves, or session history.
Dry-run is the default; `--apply` atomically replaces verified byte-identical
objects with independent copy-on-write clones. Existing ownership, mode, mtime,
extended attributes, and ACLs must be preserved; mismatches or concurrent changes
leave the original untouched. Mutable working files are never hardlinked.

Example (supply your actual source repositories and scratch roots):

```sh
python3 scripts/disk-maintenance.py \
  --root "$HOME/project/.git" \
  --root "$HOME/project/Saved/ParallelWork" \
  --root /private/tmp \
  --state "$HOME/.local/state/disk-maintenance/state.json"
```

Add `--apply --notify` after reviewing the scope. State remembers unchanged
objects, bounds error reporting, and records measured free space. Notifications
warn below 40 GiB and are throttled to once per six hours. This is a warning
threshold, **not** a reserved allocation or hard disk quota.

The host-local installation uses:

- `~/.local/lib/disk-maintenance/maintain.py`: installed script.
- `~/.local/state/disk-maintenance/roots.json`: explicit scanned roots.
- `~/.local/state/disk-maintenance/state.json`: latest report and object cache.
- `~/Library/LaunchAgents/local.disk-maintenance.plist`: hourly, low-priority job.

Inspect the latest report and `launchctl print gui/$(id -u)/local.disk-maintenance`.
Disable scheduling with `launchctl bootout gui/$(id -u)/local.disk-maintenance`.
Disabling it does not require undoing clones: their data remains independent.

## Growth prevention beyond sharing

- Global Cargo dev/test defaults can use `debug = "line-tables-only"` and
  `incremental = false`, retaining useful stack locations without multiplying
  full debug/incremental trees. Rich debugger symbols remain an explicit per-task
  override. `[cache] auto-clean-frequency = "1 day"` controls Cargo cache cleanup
  frequency, not its byte capacity. Preserve existing config and keep a backup.
- Scratch owners should prefer reusable builds and disposable worktrees/local
  clones, retain delivery patches/evidence separately, and retire completed
  checkouts only after verifying no unique changes or pending users depend on them.
- Do not blanket-delete `/tmp`, `Saved`, browser profiles, dependency installations,
  SDKs, or histories. APFS space sharing means summed directory sizes can overstate
  reclaimable space; verify the actual free-space change.

These measures reduce avoidable amplification. Unique project data and retained
history still require capacity planning; no finite disk can retain unlimited
new data forever.
