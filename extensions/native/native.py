#!/usr/bin/env python3
"""Cargo/CMake adapter; engine-neutral Borg lanes own admission and execution."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from uuid import uuid4
from urllib.parse import urlsplit

GIB = 1024**3


def available_ram() -> int:
    for line in Path('/proc/meminfo').read_text().splitlines():
        if line.startswith('MemAvailable:'):
            return int(line.split()[1]) * 1024
    raise RuntimeError('MemAvailable unavailable; cannot size native jobs')


def jobs(bytes_per_job: int) -> int:
    ram = max(0, available_ram() - 8 * GIB)
    if ram < bytes_per_job:
        raise RuntimeError('insufficient available RAM for one native job')
    return min(6, os.cpu_count() or 1, ram // bytes_per_job)


def project_root(path: str) -> Path:
    root = Path(path).resolve(strict=True)
    if not root.is_dir():
        raise ValueError(f'project must be a directory: {root}')
    return root


def inside_project(root: Path, path: str) -> Path:
    output = (root / path).resolve()
    if not output.is_relative_to(root):
        raise ValueError(f'output must stay in worktree: {output}')
    return output


def plan(args: argparse.Namespace) -> tuple[Path, str, list[str], dict[str, str]]:
    root = project_root(args.project)
    env: dict[str, str] = {}
    if args.tool == 'cargo':
        if not (root / 'Cargo.toml').is_file():
            raise ValueError('Cargo.toml missing from project root')
        env['CARGO_TARGET_DIR'] = str(inside_project(root, args.target_dir))
        env['CARGO_BUILD_JOBS'] = str(jobs(1536 * 1024**2))
        command = ['cargo', args.operation, '-j', env['CARGO_BUILD_JOBS']]
        if args.package:
            command += ['-p', args.package]
        command += args.extra
        return root, 'cargo-' + args.operation, command, env
    if not (root / 'CMakeLists.txt').is_file():
        raise ValueError('CMakeLists.txt missing from project root')
    build = inside_project(root, args.build_dir)
    if args.tool == 'cmake':
        if args.operation == 'configure':
            command = ['cmake', '-S', str(root), '-B', str(build), '-DCMAKE_BUILD_TYPE=Release', *args.extra]
        else:
            command = ['cmake', '--build', str(build), '-j', str(jobs(2 * GIB))]
            if args.target:
                command += ['--target', args.target]
            command += args.extra
    else:
        command = ['ctest', '--test-dir', str(build), '-j', str(jobs(512 * 1024**2)), '--output-on-failure']
        if args.exclude_label:
            command += ['-LE', args.exclude_label]
        if args.label:
            command += ['-L', args.label]
        if args.regex:
            command += ['-R', args.regex]
        command += args.extra
    return root, f'{args.tool}-{args.operation}', command, env


def fingerprint(root: Path, command: list[str], env: dict[str, str]) -> str:
    digest = hashlib.sha256()
    digest.update(json.dumps([str(root), command, env], sort_keys=True).encode())
    if (root / '.git').exists():
        for git_args in (['rev-parse', 'HEAD'], ['status', '--porcelain', '--untracked-files=normal'],
                         ['diff', 'HEAD', '--binary']):
            digest.update(subprocess.check_output(['git', '-C', str(root), *git_args]))
    return digest.hexdigest()


def job_spec(root: Path, lane: str, command: list[str], env: dict[str, str]) -> dict:
    executable = shutil.which(command[0])
    if not executable:
        raise RuntimeError(f'command is not installed: {command[0]}')
    kind = 'cargo' if lane.startswith('cargo-') else 'ctest' if lane.startswith('ctest-') else 'cmake'
    per_job = {'cargo': 1536 * 1024**2, 'cmake': 2 * GIB, 'ctest': 512 * 1024**2}[kind]
    job_count = int(env['CARGO_BUILD_JOBS']) if kind == 'cargo' else (
        int(command[command.index('-j') + 1]) if '-j' in command else 1)
    memory = job_count * per_job + 2 * GIB
    if 'BORG_TEST_SESSIONS_URL' in os.environ:
        url = os.environ['BORG_TEST_SESSIONS_URL']
        if urlsplit(url).password:
            raise ValueError('do not persist a PostgreSQL password in a lane job spec')
        env['BORG_TEST_SESSIONS_URL'] = url
    identity = os.environ.get('BORG_PARTICIPANT_ID') or str(uuid4())
    # A standalone CLI has no Borg participant/session context: retain unique
    # correlators, but never imply these are the live agent's identities.
    session = os.environ.get('BORG_SESSION_ID') or str(uuid4())
    untracked = bool(subprocess.check_output(
        ['git', '-C', str(root), 'ls-files', '--others', '--exclude-standard'])) if (root / '.git').exists() else False
    return {
        'fingerprint': fingerprint(root, command, env),
        'lease': {
            'resources': [{'key': {'scope': {'Worktree': str(root)}, 'name':
                          'target' if kind == 'cargo' else 'build'}, 'access': 'Exclusive'}],
            'holder': {'participant_id': identity, 'session_id': session,
                       'host_pid': None, 'purpose': lane}, 'queue_timeout_ms': None,
        },
        'argv': [shutil.which('nice') or '/usr/bin/nice', '-n', '10', executable, *command[1:]],
        'cwd': str(root), 'env': sorted(env.items()),
        'memory_max_bytes': memory,
        'admission': {'min_available_ram_bytes': 8 * GIB, 'reserve_ram_bytes': memory,
                      'min_free_disk_bytes': 60 * GIB,
                      'reserve_disk_bytes': 24 * GIB if kind == 'cargo' else 6 * GIB,
                      'disk_path': str(root)},
        'pre_hook': None, 'post_hook': None, 'timeout_ms': 30 * 60 * 1000,
        'stall_timeout_ms': None,
        'coalesce': kind != 'ctest' and 'BORG_TEST_SESSIONS_URL' not in env and not untracked,
    }


def run(args: argparse.Namespace) -> int:
    root = project_root(args.project)
    borg = os.environ.get('BORG_NATIVE_BORG', 'borg')
    if args.tool == 'workspace':
        # The core worktree CLI defaults to dry-run. Never expose --apply here.
        command = [borg, 'worktree', '--project', str(root), 'gc']
        if args.dry_run:
            print(json.dumps(command))
            return 0
        return subprocess.call(command)
    if args.tool == 'wait':
        return subprocess.call([borg, 'lane', 'job', 'wait', args.job_id, '--json'])
    root, lane, command, env = plan(args)
    if args.dry_run:
        print(json.dumps({'project': str(root), 'lane': lane, 'argv': command,
                          'env': env, 'spec': job_spec(root, lane, command, env)}, sort_keys=True))
        return 0
    free = shutil.disk_usage(root).free
    if free < 60 * GIB:
        raise RuntimeError(f'free disk on output filesystem below 60 GiB: {free / GIB:.1f} GiB')
    if args.probe_direct:
        print('PROBE DIRECT: no lane admission', file=sys.stderr)
        return subprocess.call(['nice', '-n', '10', *command], cwd=root, env={**os.environ, **env})
    if lane == 'cargo-test' and args.package == 'borg-agent-runtime' and not os.environ.get('BORG_TEST_SESSIONS_URL'):
        raise RuntimeError('BORG_TEST_SESSIONS_URL required; lease test-postgres via postgres.py')
    spec = job_spec(root, lane, command, env)
    # Immediate job ID; blocking wait belongs in a shell/watch, not a workflow.
    result = subprocess.run([borg, 'lane', 'job', 'submit', '--spec', '-', '--json'],
                            cwd=root, input=json.dumps(spec), text=True, check=True,
                            capture_output=True)
    print(result.stdout, end='')
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--project', default='.')
    p.add_argument('--target-dir', default='target')
    p.add_argument('--build-dir', default='build')
    p.add_argument('--dry-run', action='store_true')
    p.add_argument('--probe-direct', action='store_true', help='uncoordinated verification only')
    sub = p.add_subparsers(dest='tool', required=True)
    cargo = sub.add_parser('cargo')
    cargo.add_argument('operation', choices=['check', 'build', 'test'])
    cargo.add_argument('-p', '--package')
    cmake = sub.add_parser('cmake')
    cmake.add_argument('operation', choices=['configure', 'build'])
    cmake.add_argument('--target')
    ctest = sub.add_parser('ctest')
    ctest.add_argument('operation', choices=['test'])
    ctest.add_argument('--exclude-label')
    ctest.add_argument('--label')
    ctest.add_argument('--regex')
    workspace = sub.add_parser('workspace')
    workspace.add_argument('operation', choices=['gc'])
    wait = sub.add_parser('wait')
    wait.add_argument('job_id')
    try:
        argv = sys.argv[1:]
        split = argv.index('--') if '--' in argv else len(argv)
        args = p.parse_args(argv[:split])
        args.extra = argv[split + 1:] if split < len(argv) else []
        return run(args)
    except (ValueError, RuntimeError, OSError) as exc:
        print(f'native: {exc}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
