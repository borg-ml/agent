#!/usr/bin/env python3
"""Use one supervised test-postgres service, with one database per client.

The lease CLI is owned by services core. --probe-admin-url is an explicitly
unleased bootstrap probe for a throwaway server owned by the caller.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from urllib.parse import quote, urlsplit, urlunsplit
from uuid import NAMESPACE_OID, uuid4, uuid5


def client_url(admin: str, name: str) -> str:
    parsed = urlsplit(admin)
    if parsed.scheme not in ('postgres', 'postgresql') or not parsed.path.strip('/'):
        raise ValueError('admin URL must select a PostgreSQL maintenance database')
    return urlunsplit((parsed.scheme, parsed.netloc, '/' + quote(name), parsed.query, parsed.fragment))


def sql(admin: str, statement: str) -> None:
    subprocess.run(['psql', '-X', '-q', '-v', 'ON_ERROR_STOP=1', '--dbname', admin,
                    '-c', statement], check=True, stdout=subprocess.DEVNULL)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--probe-admin-url', help='unleased private test server; benchmark only')
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    if not command:
        parser.error('command required after --')
    lease_id: str | None = None
    owner: str | None = None
    admin: str | None = None
    name = 'borg_native_' + uuid4().hex
    borg = os.environ.get('BORG_NATIVE_BORG', 'borg')
    created = False
    try:
        if args.probe_admin_url:
            admin = args.probe_admin_url
            print('PROBE DIRECT: PostgreSQL service is not leased', file=sys.stderr)
        else:
            # Service core owns liveness, resource leases and restart. A
            # client receives an ID; it must release even if tests fail.
            owner = name
            response = subprocess.check_output([borg, 'lane', 'service', 'lease', 'test-postgres',
                '--owner', owner, '--ttl-seconds', '3600', '--purpose', 'database', '--json'], text=True)
            status = json.loads(response)
            lease_id = next(client['id'] for client in status['clients']
                            if client['owner']['participant_id'] == str(uuid5(NAMESPACE_OID, owner)))
            admin = os.environ['BORG_TEST_POSTGRES_ADMIN_URL']
        assert admin is not None
        sql(admin, f'CREATE DATABASE {name}')
        created = True
        url = client_url(admin, name)
        result = subprocess.run(command, env={**os.environ, 'BORG_TEST_SESSIONS_URL': url},
                                capture_output=True, text=True)
        print(result.stdout, end='')
        print(result.stderr, end='', file=sys.stderr)
        if result.returncode == 0 and not args.probe_admin_url:
            try:
                job_id = json.loads(result.stdout)['job_id']
            except (ValueError, KeyError, TypeError):
                pass
            else:
                return subprocess.call([borg, 'lane', 'job', 'wait', job_id, '--json'])
        return result.returncode
    finally:
        try:
            if created and admin is not None:
                sql(admin, f'DROP DATABASE IF EXISTS {name} WITH (FORCE)')
        finally:
            if lease_id and owner is not None:
                subprocess.run([borg, 'lane', 'service', 'release', 'test-postgres',
                                '--owner', owner, '--lease-id', lease_id], check=True)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (ValueError, OSError, KeyError, StopIteration, subprocess.CalledProcessError) as exc:
        print(f'native postgres: {exc}', file=sys.stderr)
        sys.exit(2)
