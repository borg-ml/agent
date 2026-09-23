#!/usr/bin/env python3
"""Cargo/CMake adapter; engine-neutral Borg lanes own admission and execution."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

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


def run(args: argparse.Namespace) -> int:
    root = project_root(args.project)
    if args.tool == 'workspace':
        # Workspace core owns GC. This adapter never deletes another agent's outputs.
        command = ['borg', 'lane', 'workspace', 'gc', '--project', str(root)]
        if not args.apply:
            command += ['--dry-run']
        if args.dry_run:
            print(json.dumps(command))
            return 0
        return subprocess.call(command)
    root, lane, command, env = plan(args)
    if args.dry_run:
        print(json.dumps({'project': str(root), 'lane': lane, 'argv': command, 'env': env}, sort_keys=True))
        return 0
    free = shutil.disk_usage(root).free
    if free < 60 * GIB:
        raise RuntimeError(f'free disk on output filesystem below 60 GiB: {free / GIB:.1f} GiB')
    if args.probe_direct:
        print('PROBE DIRECT: no lane admission', file=sys.stderr)
        return subprocess.call(['nice', '-n', '10', *command], cwd=root, env={**os.environ, **env})
    # Until core lands this fails closed, not silently uncoordinated.
    submission = ['borg', 'lane', 'job', 'submit', '--wait', '--adapter', 'native',
                  '--template', lane, '--project', str(root), '--', *command]
    return subprocess.call(submission, cwd=root, env={**os.environ, **env})


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
    workspace.add_argument('--apply', action='store_true')
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
