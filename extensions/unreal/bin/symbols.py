#!/usr/bin/env python3
"""Borg-owned post-build hook: regenerate symbols without stale publication."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


def identity(files: list[Path]) -> list[tuple[int, int]] | None:
    try:
        return [(p.stat().st_ino, p.stat().st_mtime_ns) for p in files]
    except OSError:
        return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--engine', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, required=True)
    args = parser.parse_args()
    if not args.manifest.is_file():
        return 0
    syms = args.engine / 'Engine/Binaries/Linux/dump_syms'
    encoder = args.engine / 'Engine/Binaries/Linux/BreakpadSymbolEncoder'
    for name in json.loads(args.manifest.read_text()):
        library = Path(name)
        debug = library.with_suffix('.debug')
        source = debug if debug.exists() else library
        watched = [library, source] if source != library else [library]
        before = identity(watched)
        if before is None:
            continue
        temp = library.with_name(f'.{library.stem}.{os.getpid()}.sym.tmp')
        psym = temp.with_suffix('.psym')
        try:
            if (subprocess.call([str(syms), '-c', '-o', str(psym), str(source)],
                                stdout=subprocess.DEVNULL) == 0 and
                subprocess.call([str(encoder), str(psym), str(temp)],
                                stdout=subprocess.DEVNULL) == 0 and
                    identity(watched) == before):
                os.replace(temp, library.with_suffix('.sym'))
        finally:
            temp.unlink(missing_ok=True)
            psym.unlink(missing_ok=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
