#!/usr/bin/env python3
"""Unreal policy adapter for Borg's host-local job and service coordinators."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tomllib
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PLATFORM = 'Mac' if sys.platform == 'darwin' else 'Linux'
GIB = 1 << 30


def project_path(explicit: str | None, start: Path) -> Path:
    if explicit:
        path = Path(explicit).expanduser().resolve(strict=True)
        if path.suffix != '.uproject' or not path.is_file():
            raise ValueError(f'not a .uproject file: {path}')
        return path
    for directory in (start.resolve(), *start.resolve().parents):
        matches = sorted(directory.glob('*.uproject'))
        if len(matches) == 1:
            return matches[0].resolve()
        if len(matches) > 1:
            raise ValueError(f'multiple .uproject files in {directory}; pass --project')
    raise ValueError('no .uproject in this directory or its parents; pass --project')


def engine_path(project: Path, explicit: str | None) -> Path:
    association = json.loads(project.read_text()).get('EngineAssociation', '')
    candidates = [explicit, os.environ.get('UE_ENGINE_ROOT')]
    if isinstance(association, str) and association.startswith(('/', '~')):
        candidates.append(association)
    candidates += [str(project.parent.parent / 'UnrealEngine'), '/opt/unreal-engine']
    for candidate in candidates:
        if not candidate:
            continue
        path = Path(candidate).expanduser().resolve()
        if (path / 'Engine/Build/BatchFiles' / PLATFORM / 'Build.sh').is_file():
            return path
        if candidate in (explicit, os.environ.get('UE_ENGINE_ROOT')):
            raise ValueError(f'no {PLATFORM}/Build.sh under engine root {path}')
    raise ValueError('engine not found; set --engine-root or UE_ENGINE_ROOT')


def read_config(project: Path, explicit: str | None) -> dict:
    path = Path(explicit).resolve(strict=True) if explicit else project.parent / '.borg-unreal.toml'
    cfg = tomllib.loads(path.read_text()) if path.is_file() else {}
    if not isinstance(cfg, dict):
        raise ValueError('configuration must be TOML')
    return cfg


def sizes(cfg: dict, kind: str, key: str, default: int) -> int:
    value = int(cfg.get(kind, {}).get(key, default))
    if value < 0:
        raise ValueError(f'{kind}.{key} must be >= 0')
    return value


def ports(cfg: dict) -> tuple[int, list[int]]:
    editor = cfg.get('editor', {})
    front = int(editor.get('port', 8240))
    back = list(map(int, editor.get('backend_ports', [front + 3, front + 4])))
    if len(back) != 2 or any(not 1024 <= p <= 65535 for p in [front, *back]) or len(set([front, *back])) != 3:
        raise ValueError('editor port and two distinct backend_ports must be valid non-privileged ports')
    return front, back


def mem_available_bytes() -> int:
    try:
        return next(int(line.split()[1]) * 1024 for line in Path('/proc/meminfo').read_text().splitlines()
                    if line.startswith('MemAvailable:'))
    except (OSError, StopIteration, ValueError):
        return 0


def budget(project: Path, cfg: dict, section: str) -> dict:
    min_ram = sizes(cfg, section, 'min_available_ram_gb', 8)
    disk = sizes(cfg, section, 'min_free_disk_gb', 20)
    return {'min_available_ram_bytes': min_ram * GIB,
            'reserve_ram_bytes': sizes(cfg, section, 'reserve_ram_gb', 4) * GIB,
            'min_free_disk_bytes': disk * GIB,
            'reserve_disk_bytes': sizes(cfg, section, 'reserve_disk_gb', 10) * GIB,
            'disk_path': str(project.parent)}


def fingerprint(project: Path, engine: Path, args: list[str]) -> str:
    """Source revisions and compiler identity; never reuse a stale binary after a source edit."""
    h = hashlib.sha256()
    h.update(json.dumps([str(project), str(engine), args], sort_keys=True).encode())
    for top in (project, engine / 'Engine/Build/BatchFiles' / PLATFORM / 'Build.sh',
                engine / 'Engine/Build/Build.version', ROOT / 'bin/ubt.py',
                ROOT / 'bin/symbols.py', project.parent / 'Source', project.parent / 'Plugins'):
        if not top.exists():
            continue
        paths = [top] if top.is_file() else sorted(p for p in top.rglob('*') if p.is_file()
                 and not any(part in {'Intermediate', 'Binaries', 'Saved', '.git', 'DerivedDataCache'}
                             for part in p.parts))
        for path in paths:
            st = path.stat()
            h.update(f'{path}:{st.st_size}:{st.st_mtime_ns}\n'.encode())
    return h.hexdigest()


def holder(args: argparse.Namespace, purpose: str) -> dict:
    participant = args.participant_id or os.environ.get('BORG_PARTICIPANT_ID')
    session = args.session_id or os.environ.get('BORG_SESSION_ID')
    if not participant or not session:
        raise ValueError('core jobs require --participant-id and --session-id (or corresponding BORG_* env vars)')
    return {'participant_id': str(uuid.UUID(participant)), 'session_id': str(uuid.UUID(session)),
            'host_pid': os.getpid(), 'purpose': purpose}


def key(scope: str, name: str, path: Path | None = None) -> dict:
    return {'scope': scope if path is None else {scope: str(path)}, 'name': name}


def project_id(project: Path) -> str:
    return hashlib.sha256(str(project).encode()).hexdigest()[:16]


def build_spec(args: argparse.Namespace, project: Path, engine: Path, cfg: dict) -> dict:
    positional = args.build_args or [str(cfg.get('editor', {}).get('target', project.stem + 'Editor')),
                                    PLATFORM, 'Development', str(project)]
    if positional[:1] == ['--']:
        positional = positional[1:]
    if len(positional) < 3 or any(a.startswith('--') for a in positional):
        raise ValueError('build needs TARGET PLATFORM CONFIG [UPROJECT] [UBT flags]')
    # The lease and UBT must name the same canonical project, even when the
    # caller supplied a path alias. A second UBT project would bypass that tie.
    if any(a.lower().startswith('-project=') for a in positional):
        raise ValueError('use a single positional UPROJECT; -Project= is ambiguous')
    project_args = [i for i, a in enumerate(positional) if a.lower().endswith('.uproject')]
    if len(project_args) > 1:
        raise ValueError('build accepts only one UPROJECT')
    if project_args:
        i = project_args[0]
        if Path(positional[i]).resolve(strict=True) != project:
            raise ValueError('UBT project and lane project must be identical')
        positional[i] = str(project)
    else:
        positional.insert(3, str(project))
    script = engine / 'Engine/Build/BatchFiles' / PLATFORM / 'Build.sh'
    state = Path(os.environ.get('XDG_RUNTIME_DIR') or '/tmp') / 'borg' / 'unreal' / project_id(project)
    action_gb = float(cfg.get('build', {}).get('gb_per_action', 1.5))
    reserve = sizes(cfg, 'build', 'reserve_ram_gb', 4)
    if action_gb <= 0:
        raise ValueError('build.gb_per_action must be positive')
    available = mem_available_bytes() // GIB
    actions = max(1, min((os.cpu_count() or 2) // 2, int(max(1, available - reserve) / action_gb)))
    ubt_args = [a for a in positional if not a.lower().startswith(('-waitmutex', '-nomutex', '-log=', '-maxparallelactions='))]
    ubt_args += ['-NoMutex', f'-MaxParallelActions={actions}']
    have_syms = (PLATFORM == 'Linux' and
                 all((engine / 'Engine/Binaries/Linux' / n).is_file()
                     for n in ('dump_syms', 'BreakpadSymbolEncoder')) and
                 not any(a.lower().startswith(('-clean', '-mode=', '-nodumpsyms')) for a in positional))
    if have_syms:
        ubt_args.append('-NoDumpSyms')
    # Share a host-wide startup lock with an existing lane if explicitly set.
    start_lock = os.environ.get('UE_UBT_START_LOCK') or cfg.get('build', {}).get('ubt_start_lock')
    if start_lock is None:
        start_lock = state.parent / 'ubt-start.lock'
    if not isinstance(start_lock, (str, Path)) or not Path(start_lock).expanduser().is_absolute():
        raise ValueError('build.ubt_start_lock / UE_UBT_START_LOCK must be an absolute path')
    start_lock = Path(start_lock).expanduser().resolve()
    # Identical pending jobs must have identical argv/env/hooks. Keep log and
    # manifest stable per revision/policy, and clear old artifacts at job start.
    rev = fingerprint(project, engine, [str(script), *ubt_args, sys.executable, str(state),
                                        json.dumps(cfg.get('build', {}), sort_keys=True),
                                        str(int(action_gb * GIB)), str(start_lock)])
    log = state / f'{rev}.ubt.log'
    symbols = state / f'{rev}.symbols.json'
    ubt_args.append(f'-Log={log}')
    cmd = [sys.executable, str(ROOT / 'bin/ubt.py'), '--symbols', str(symbols), '--', str(script), *ubt_args]
    env = [['UnrealBuildTool_ParallelExecutor__MemoryPerActionBytes', str(int(action_gb * GIB))],
           ['UE_UBT_START_LOCK', str(start_lock)]]
    return {'fingerprint': rev, 'lease': {'resources': [
                {'key': key('Worktree', 'unreal-build-output', project.parent), 'access': 'Exclusive'}],
                'holder': holder(args, 'Unreal build'), 'queue_timeout_ms': None},
            'argv': cmd, 'cwd': str(project.parent), 'env': env,
            'memory_max_bytes': sizes(cfg, 'build', 'memory_max_gb', 24) * GIB,
            'admission': budget(project, cfg, 'build'),
            'pre_hook': None,
            'post_hook': {'argv': [sys.executable, str(ROOT / 'bin/symbols.py'),
                                   '--engine', str(engine), '--manifest', str(symbols)],
                          'timeout_ms': 180_000} if have_syms else None,
            'timeout_ms': sizes(cfg, 'build', 'timeout_seconds', 5400) * 1000,
            'stall_timeout_ms': sizes(cfg, 'build', 'stall_seconds', 900) * 1000,
            'coalesce': not any(a.lower().startswith(('-clean', '-rebuild', '-mode=')) for a in positional)}


def service_spec(project: Path, engine: Path, cfg: dict) -> dict:
    front, backend = ports(cfg)
    editor_bin = engine / 'Engine/Binaries' / PLATFORM / ('UnrealEditor-Cmd' if PLATFORM == 'Mac' else 'UnrealEditor')
    if not editor_bin.exists():
        raise ValueError(f'Unreal editor unavailable: {editor_bin}')
    extension_args = cfg.get('editor', {}).get('args', [])
    if not isinstance(extension_args, list) or not all(isinstance(a, str) for a in extension_args):
        raise ValueError('editor.args must be an array of arguments')
    return {'id': f'unreal-{project_id(project)}', 'cwd': str(project.parent),
            'argv': [str(editor_bin), str(project), '-RenderOffscreen', '-unattended', '-nosplash',
                     '-nosound', '-nop4', '-saveddirsuffix=BorgEditorLane',
                     '-ModelContextProtocolStartServer', '-ModelContextProtocolPort={port}',
                     '-ini:Engine:[HTTPServer.Listeners]:DefaultBindAddress=127.0.0.1',
                     '-ini:EditorPerProjectUserSettings:[/Script/UnrealEd.EditorLoadingSavingSettings]:bAutoSaveEnable=False',
                     *extension_args],
            'env': [['UE_PYTHONPATH', str(ROOT / 'editor/python')]],
            'resources': [{'key': key('Project', 'unreal-project-run', project), 'access': 'Exclusive'}],
            'memory_max_bytes': sizes(cfg, 'editor', 'memory_max_gb', 16) * GIB,
            'admission': budget(project, cfg, 'editor'),
            'health': {'argv': ['/mcp'], 'kind': 'mcp_initialize', 'interval_ms': 2000, 'timeout_ms': 5000},
            'restart': {'max_restarts': 5, 'backoff_ms': 15000, 'debounce_ms': 45000},
            'endpoint': {'listen': f'127.0.0.1:{front}', 'backend_ports': backend},
            'restore': None, 'adapter_enforces_leases': False,
            'readiness_timeout_ms': 900_000,
            'graceful_stop': {'argv': [sys.executable, str(ROOT / 'editor/quit.py'), '--port', '{port}'],
                              'timeout_ms': 90_000}}


def run_spec(args: argparse.Namespace, project: Path, cfg: dict) -> dict:
    command = args.run_args[1:] if args.run_args[:1] == ['--'] else args.run_args
    if not command:
        raise ValueError('run KIND -- COMMAND [ARGS...] is required')
    if args.kind not in ('commandlet', 'import', 'verify', 'exclusive'):
        raise ValueError(f'unknown run kind: {args.kind}')
    return {'fingerprint': str(uuid.uuid4()), 'lease': {'resources': [
                {'key': key('Project', 'unreal-project-run', project), 'access': 'Exclusive'},
                {'key': key('Host', 'unreal-run-ram', None), 'access': 'Exclusive'}],
                'holder': holder(args, 'Unreal ' + args.kind), 'queue_timeout_ms': None},
            'argv': command, 'cwd': str(project.parent), 'env': [],
            'memory_max_bytes': sizes(cfg, 'run', 'memory_max_gb', 16) * GIB,
            'admission': budget(project, cfg, 'run'),
            'pre_hook': None, 'post_hook': None,
            'timeout_ms': sizes(cfg, 'run', 'timeout_seconds', 3600) * 1000,
            'stall_timeout_ms': None, 'coalesce': False}


def lane(*parts: str, input_data: dict | None = None) -> subprocess.CompletedProcess:
    borg = os.environ.get('BORG_AGENT_CLI') or shutil.which('borg')
    if not borg:
        raise ValueError('Borg executable not found')
    proc = subprocess.run([borg, 'lane', *parts], input=json.dumps(input_data) if input_data else None,
                          text=True, capture_output=True, check=False)
    if proc.returncode != 0 and ('unrecognized subcommand' in proc.stderr or 'unexpected argument' in proc.stderr):
        raise ValueError('Borg lane CLI is not installed in this Borg binary; use spec output or update Borg')
    return proc


def submit(spec: dict, wait: bool) -> int:
    proc = lane('job', 'submit', '--spec', '-', '--json', input_data=spec)
    print(proc.stdout, end='')
    if proc.returncode:
        print(proc.stderr, file=sys.stderr, end='')
        return proc.returncode
    if not wait:
        return 0
    job_id = json.loads(proc.stdout)['job_id']
    result = lane('job', 'wait', job_id, '--json')
    print(result.stdout, end='')
    if result.stderr:
        print(result.stderr, file=sys.stderr, end='')
    return result.returncode


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--project')
    ap.add_argument('--engine-root')
    ap.add_argument('--config')
    ap.add_argument('--participant-id')
    ap.add_argument('--session-id')
    sub = ap.add_subparsers(dest='command', required=True)
    sub.add_parser('discover')
    build = sub.add_parser('build')
    build.add_argument('--spec', action='store_true', help='print validated core JobSpec only')
    build.add_argument('--wait', action='store_true', help='wait for terminal result (manual shell use)')
    build.add_argument('build_args', nargs=argparse.REMAINDER)
    run = sub.add_parser('run')
    run.add_argument('kind', choices=['commandlet', 'import', 'verify', 'exclusive'])
    run.add_argument('--spec', action='store_true')
    run.add_argument('--wait', action='store_true')
    run.add_argument('run_args', nargs=argparse.REMAINDER)
    editor = sub.add_parser('editor')
    editor.add_argument('operation', choices=['spec', 'start', 'status', 'restart', 'yield', 'resume', 'stop', 'lease', 'release'])
    editor.add_argument('service_args', nargs=argparse.REMAINDER)
    mcp = sub.add_parser('mcp')
    mcp.add_argument('mcp_args', nargs=argparse.REMAINDER)
    opts = ap.parse_args(argv)
    # argparse.REMAINDER preserves UBT/Python flags but also captures adapter
    # flags placed after the target/kind. Only consume flags before the explicit
    # `--` boundary; everything after it belongs to the child command.
    if opts.command in ('build', 'run'):
        attr = 'build_args' if opts.command == 'build' else 'run_args'
        rest = getattr(opts, attr)
        boundary = rest.index('--') if '--' in rest else len(rest)
        prefix, suffix = rest[:boundary], rest[boundary:]
        for flag in ('spec', 'wait'):
            option = '--' + flag
            if option in prefix:
                setattr(opts, flag, True)
                prefix.remove(option)
        setattr(opts, attr, prefix + suffix)
    try:
        project = project_path(opts.project or os.environ.get('UE_UPROJECT'), Path.cwd())
        cfg = read_config(project, opts.config)
        engine = engine_path(project, opts.engine_root or cfg.get('engine_root'))
        if opts.command == 'discover':
            front, back = ports(cfg)
            print(json.dumps({'project': str(project), 'engine': str(engine), 'platform': PLATFORM,
                              'targets': sorted(p.name.removesuffix('.Target.cs') for p in
                                                (project.parent / 'Source').glob('*.Target.cs')),
                              'ports': [front, *back]}, indent=2))
            return 0
        if opts.command in ('build', 'run'):
            spec = (build_spec(opts, project, engine, cfg) if opts.command == 'build'
                    else run_spec(opts, project, cfg))
            if opts.spec:
                print(json.dumps(spec, indent=2))
                return 0
            if opts.command == 'run':
                raise ValueError('exclusive run is not wired to verified editor yield, post-hook resume and owner fencing; use --spec only')
            return submit(spec, opts.wait)
        if opts.command == 'editor':
            definition = service_spec(project, engine, cfg)
            if opts.operation == 'spec':
                print(json.dumps(definition, indent=2))
                return 0
            service = definition['id']
            if opts.operation == 'start':
                definition_path = Path(os.environ.get('XDG_RUNTIME_DIR') or '/tmp') / 'borg' / 'unreal' / project_id(project) / 'service.json'
                definition_path.parent.mkdir(parents=True, exist_ok=True)
                definition_path.write_text(json.dumps(definition))
                proc = lane('service', 'start', service, '--definition', str(definition_path), '--json')
            else:
                proc = lane('service', opts.operation, service, *opts.service_args, '--json')
            print(proc.stdout, end='')
            if proc.stderr:
                print(proc.stderr, file=sys.stderr, end='')
            return proc.returncode
        raise ValueError('raw MCP access is blocked until the core service proxy enforces owner leases; direct backend access is intentionally not exposed')
    except (ValueError, OSError, KeyError, TypeError, json.JSONDecodeError, tomllib.TOMLDecodeError) as error:
        print(f'unreal: {error}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
