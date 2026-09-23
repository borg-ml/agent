#!/usr/bin/env python3
"""Provision a throwaway local Postgres cluster and register it with Borg services."""
import argparse
import getpass
import json
import os
from pathlib import Path
import subprocess

GIB = 1024**3


def spec(data: Path, port: int) -> dict:
    root = data.resolve()
    return {
        'id': 'test-postgres',
        'argv': ['/usr/bin/postgres', '-D', str(root), '-k', str(root.parent),
                 '-h', '', '-p', str(port)],
        'cwd': str(root.parent), 'env': [], 'resources': [],
        'memory_max_bytes': GIB,
        'admission': {'min_available_ram_bytes': 8 * GIB, 'reserve_ram_bytes': GIB,
                      'min_free_disk_bytes': 60 * GIB, 'reserve_disk_bytes': GIB,
                      'disk_path': str(root)},
        'health': {'argv': ['pg_isready', '-h', str(root.parent), '-p', str(port)],
                   'kind': 'command', 'interval_ms': 1000, 'timeout_ms': 3000},
        'restart': {'max_restarts': 3, 'backoff_ms': 500, 'debounce_ms': 500},
        'endpoint': None, 'restore': None,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-dir', default=f'/tmp/borg-native-test-postgres-{os.getuid()}/data')
    parser.add_argument('--port', type=int, default=55481)
    parser.add_argument('--start', action='store_true', help='initialize owned test cluster and launch via core')
    args = parser.parse_args()
    data = Path(args.data_dir).resolve()
    if not data.is_relative_to(Path('/tmp')):
        parser.error('test cluster must be under /tmp')
    if not 1024 <= args.port <= 65535:
        parser.error('invalid port')
    definition = spec(data, args.port)
    file = data.parent / 'test-postgres.json'
    if args.start:
        if not (data / 'PG_VERSION').is_file():
            data.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            subprocess.run(['initdb', '-D', str(data), '--auth-local=peer',
                            '--auth-host=reject', '--no-instructions'], check=True)
        file.write_text(json.dumps(definition, indent=2) + '\n')
        file.chmod(0o600)
        subprocess.run([os.environ.get('BORG_NATIVE_BORG', 'borg'), 'lane', 'service', 'start', 'test-postgres',
                        '--definition', str(file), '--json'], check=True)
    else:
        print(json.dumps(definition, indent=2))
    print(f'BORG_TEST_POSTGRES_ADMIN_URL=postgresql://{getpass.getuser()}@localhost:{args.port}/postgres?host={data.parent}')


if __name__ == '__main__':
    main()
