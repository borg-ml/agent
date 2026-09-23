"""Blu Python workflow entry. Explicit argv only: no project-supplied shell code."""
import json
import os
import subprocess
import sys
from pathlib import Path


def main() -> int:
    # External workflow arguments are provided by Borg's runtime; don't guess
    # unstructured stdin. For executable lane commands use bin/unreal.py.
    command = [sys.executable, str(Path(__file__).resolve().parents[1] / 'bin/unreal.py'), 'discover']
    proc = subprocess.run(command, capture_output=True, text=True, cwd=os.getcwd(), check=False)
    print(json.dumps({'exit_code': proc.returncode, 'stdout': proc.stdout, 'stderr': proc.stderr}))
    return proc.returncode


if __name__ == '__main__':
    sys.exit(main())
