#!/usr/bin/env python3
"""Unreal-specific UBT startup hazard and changed-symbol snapshot; not a scheduler."""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import resource
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def libraries(tree: Path, platform: str) -> dict[str, list[int]]:
    result = {}
    bases = [tree / 'Binaries' / platform, *(tree / 'Plugins').glob('*/Binaries/' + platform)]
    for base in bases:
        for path in base.glob('*.so'):
            st = path.stat()
            result[str(path)] = [st.st_ino, st.st_mtime_ns]
    return result


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--symbols', type=Path, required=True)
    parser.add_argument('argv', nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = args.argv[1:] if args.argv[:1] == ['--'] else args.argv
    if not command:
        parser.error('expected -- Build.sh ARG...')
    project = next((Path(item) for item in command if item.endswith('.uproject')), None)
    if project is None:
        parser.error('Build.sh command must include the .uproject')
    root = project.resolve().parent
    symbols_enabled = '-NoDumpSyms' in command
    before = libraries(root, 'Linux') if symbols_enabled else {}
    log_arg = next((a for a in command if a.lower().startswith('-log=')), None)
    if not log_arg:
        parser.error('per-build -Log= is required')
    log_path = Path(log_arg.split('=', 1)[1])
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.unlink(missing_ok=True)
    args.symbols.unlink(missing_ok=True)
    start_lock = Path(os.environ.get('UE_UBT_START_LOCK', '/tmp/borg-unreal-ubt-start.lock'))
    start_lock.parent.mkdir(parents=True, exist_ok=True)
    def start_once():
        # UBA's shared-memory GC is unsafe when two builds share its mapping
        # directory. Give each UBT process (including a startup retry) private
        # paths, even if the Borg helper inherited a lane's TMPDIR or UBA env.
        with tempfile.TemporaryDirectory(prefix='borg-unreal-ubt-') as private_tmp:
            mapping = Path(private_tmp) / 'uba-mappings'
            mapping.mkdir(mode=0o700)
            child_env = dict(os.environ, TMPDIR=private_tmp,
                             UBA_FILE_MAPPING_DIR=str(mapping))
            fd = os.open(start_lock, os.O_CREAT | os.O_RDWR | os.O_CLOEXEC, 0o600)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX)
                started = time.time_ns()
                proc = subprocess.Popen(command, close_fds=True, env=child_env)
                # Release startup key when the private UBT log opens, not after compile.
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline and proc.poll() is None:
                    try:
                        if log_path.stat().st_mtime_ns >= started:
                            break
                    except FileNotFoundError:
                        pass
                    time.sleep(0.1)
            finally:
                os.close(fd)
            return proc.wait()

    started_at = time.monotonic()
    rc = start_once()
    if rc and log_path.exists():
        tail = log_path.read_bytes()[-200_000:]
        if b'BackupLogFile' in tail and b'Trace.uba' in tail:
            # Another direct UBT may not take our lock. The retry is narrow
            # and only runs for this known early-startup collision.
            rc = start_once()
    # The job's transient systemd unit may be gone by `job wait`. Capture
    # bounded process/cgroup evidence while this helper still holds its scope.
    child_rss = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    scope_peak = None
    if sys.platform == 'linux':
        try:
            group = next(line.split('::', 1)[1] for line in
                         Path('/proc/self/cgroup').read_text().splitlines()
                         if line.startswith('0::'))
            scope_peak = int((Path('/sys/fs/cgroup') / group.lstrip('/') /
                              'memory.peak').read_text())
        except (OSError, ValueError, StopIteration):
            pass
    metrics = log_path.with_suffix('.metrics.json')
    temporary = metrics.with_suffix('.tmp')
    temporary.write_text(json.dumps({
        'exit_code': rc, 'ubt_wall_seconds': time.monotonic() - started_at,
        # ru_maxrss is KiB on Linux, bytes on macOS; this is one child maximum,
        # not concurrent aggregate RSS. memory.peak includes page cache.
        'ubt_child_max_rss_bytes': child_rss * (1024 if sys.platform == 'linux' else 1),
        'scope_memory_peak_bytes': scope_peak,
    }))
    os.replace(temporary, metrics)
    if rc == 0 and symbols_enabled:
        changed = [name for name, info in libraries(root, 'Linux').items() if before.get(name) != info]
        for name in changed:
            Path(name).with_suffix('.sym').unlink(missing_ok=True)
        args.symbols.parent.mkdir(parents=True, exist_ok=True)
        tmp = args.symbols.with_suffix('.tmp')
        tmp.write_text(json.dumps(changed))
        os.replace(tmp, args.symbols)
    return rc


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
