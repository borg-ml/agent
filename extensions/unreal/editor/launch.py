#!/usr/bin/env python3
"""Record the backend PID, then exec Unreal in that same tracked process."""
import argparse
import json
import os
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--pid-file', type=Path, required=True)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    if not command:
        parser.error('missing editor command')
    args.pid_file.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    start = None
    if sys.platform == 'linux':
        fields = Path('/proc/self/stat').read_text().rsplit(') ', 1)[1].split()
        start = fields[19]  # /proc field 22, stable across exec
    temporary = args.pid_file.with_suffix('.tmp')
    temporary.write_text(json.dumps({'pid': os.getpid(), 'start': start}))
    os.replace(temporary, args.pid_file)
    os.execvpe(command[0], command, os.environ)


if __name__ == '__main__':
    main()
