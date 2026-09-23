"""Template contract tests against a private fake engine; never launch Unreal."""
import hashlib
import json
import os
import socket
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CLI = ROOT / 'bin/unreal.py'
PARTICIPANT = '00000000-0000-0000-0000-000000000001'
SESSION = '00000000-0000-0000-0000-000000000002'


class AdapterTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='borg-unreal-test-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.engine = self.root / 'UE'
        builder = self.engine / 'Engine/Build/BatchFiles/Linux/Build.sh'
        builder.parent.mkdir(parents=True)
        builder.write_text('#!/bin/sh\nprintf "[1/1] Compile ok.cpp\\n"\n')
        builder.chmod(0o755)
        editor = self.engine / 'Engine/Binaries/Linux/UnrealEditor'
        editor.parent.mkdir(parents=True)
        editor.write_text('')
        self.project = self.root / 'Game' / 'Game.uproject'
        self.project.parent.mkdir()
        self.project.write_text('{"EngineAssociation":"5.8"}')
        (self.project.parent / 'Source').mkdir()
        (self.project.parent / 'Source/GameEditor.Target.cs').write_text('')
        self.env = dict(os.environ, XDG_RUNTIME_DIR=str(self.root / 'runtime'))

    def cli(self, *argv):
        return subprocess.run([sys.executable, str(CLI), '--project', str(self.project),
                               '--engine-root', str(self.engine), '--participant-id', PARTICIPANT,
                               '--session-id', SESSION, *argv], env=self.env,
                              capture_output=True, text=True, timeout=20)

    def test_discovery_and_template_port_validation(self):
        info = self.cli('discover')
        self.assertEqual(info.returncode, 0, info.stderr)
        self.assertEqual(json.loads(info.stdout)['targets'], ['GameEditor'])
        config = self.project.parent / '.borg-unreal.toml'
        config.write_text('[editor]\nport = 49311\nbackend_ports = [49312,49313]\n')
        self.assertEqual(json.loads(self.cli('discover').stdout)['ports'], [49311, 49312, 49313])
        config.write_text('[editor]\nport = 49311\nbackend_ports = [49311,49313]\n')
        self.assertEqual(self.cli('editor', 'spec').returncode, 2)

    def test_build_project_alias_normalized_and_conflicts_rejected(self):
        # Pin -MaxParallelActions despite host MemAvailable changing mid-test.
        (self.project.parent / '.borg-unreal.toml').write_text(
            '[build]\ngb_per_action = 0.01\nreserve_ram_gb = 0\n')
        canonical = json.loads(self.cli('build', '--spec', 'GameEditor', 'Linux',
                                        'Development', str(self.project)).stdout)
        alias = str(self.project.parent / '..' / 'Game' / 'Game.uproject')
        aliased = self.cli('build', '--spec', 'GameEditor', 'Linux', 'Development', alias)
        self.assertEqual(aliased.returncode, 0, aliased.stderr)
        normalized = json.loads(aliased.stdout)
        self.assertEqual(normalized['argv'], canonical['argv'])
        self.assertEqual(normalized['fingerprint'], canonical['fingerprint'])
        self.assertEqual(normalized['lease']['resources'], canonical['lease']['resources'])
        other = self.project.parent / 'Other.uproject'
        other.write_text('{}')
        for args in ((str(other),), (str(self.project), str(other)),
                     ('-Project=' + str(other),)):
            with self.subTest(args=args):
                result = self.cli('build', '--spec', 'GameEditor', 'Linux',
                                  'Development', *args)
                self.assertEqual(result.returncode, 2, result.stdout)

    def test_build_job_core_schema_and_input_revision(self):
        # Keep RAM-derived -MaxParallelActions stable for this fingerprint test.
        (self.project.parent / '.borg-unreal.toml').write_text(
            '[build]\ngb_per_action = 0.01\n')
        one = self.cli('build', '--spec')
        self.assertEqual(one.returncode, 0, one.stderr)
        spec = json.loads(one.stdout)
        self.assertEqual(spec['lease']['resources'][0]['key']['scope'],
                         {'Worktree': str(self.project.parent)})
        self.assertEqual(spec['lease']['holder']['participant_id'], PARTICIPANT)
        self.assertIn('-NoMutex', spec['argv'])
        self.assertTrue(any(a.startswith('-MaxParallelActions=') for a in spec['argv']))
        self.assertTrue(any(a.startswith('-Log=') for a in spec['argv']))
        self.assertEqual(spec['cwd'], str(self.project.parent))
        self.assertGreater(spec['admission']['min_free_disk_bytes'], 0)
        self.assertTrue(spec['coalesce'])
        old = spec['fingerprint']
        same = json.loads(self.cli('build', '--spec').stdout)
        self.assertEqual(same['fingerprint'], old)
        self.assertEqual(same['argv'], spec['argv'])
        self.assertEqual(same['post_hook'], spec['post_hook'])
        (self.project.parent / 'Source/Game.cpp').write_text('int changed;\n')
        again = self.cli('build', '--spec')
        self.assertNotEqual(json.loads(again.stdout)['fingerprint'], old)
        # Missing core CLI must fail closed even on a host with Borg installed.
        self.env['BORG_AGENT_CLI'] = str(self.root / 'missing-borg')
        proc = self.cli('build')
        self.assertNotEqual(proc.returncode, 0)

    def test_symbols_flag_and_core_post_hook(self):
        tools = self.engine / 'Engine/Binaries/Linux'
        for name in ('dump_syms', 'BreakpadSymbolEncoder'):
            (tools / name).write_text('stub')
        spec = json.loads(self.cli('build', '--spec').stdout)
        self.assertIn('-NoDumpSyms', spec['argv'])
        self.assertIsNotNone(spec['post_hook'])
        spec = json.loads(self.cli('build', '--spec', 'GameEditor', 'Linux', 'Development',
                                   str(self.project), '-Clean').stdout)
        self.assertNotIn('-NoDumpSyms', spec['argv'])
        self.assertIsNone(spec['post_hook'])
        self.assertFalse(spec['coalesce'])
        config = self.project.parent / '.borg-unreal.toml'
        config.write_text('[build]\ngb_per_action = 2.0\n')
        new = json.loads(self.cli('build', '--spec').stdout)
        self.assertNotEqual(new['fingerprint'], spec['fingerprint'])

    def test_shared_ubt_start_lock_config_and_environment(self):
        original = json.loads(self.cli('build', '--spec').stdout)
        default = dict(original['env'])['UE_UBT_START_LOCK']
        self.assertTrue(Path(default).is_absolute())
        shared = self.root / 'existing-lane' / 'ubt-start.lock'
        config = self.project.parent / '.borg-unreal.toml'
        config.write_text('[build]\nubt_start_lock = ' + json.dumps(str(shared)) + '\n')
        selected = self.cli('build', '--spec')
        self.assertEqual(selected.returncode, 0, selected.stderr)
        spec = json.loads(selected.stdout)
        self.assertEqual(dict(spec['env'])['UE_UBT_START_LOCK'], str(shared))
        self.assertNotEqual(spec['fingerprint'], original['fingerprint'])
        self.env['UE_UBT_START_LOCK'] = str(self.root / 'override.lock')
        override = json.loads(self.cli('build', '--spec').stdout)
        self.assertEqual(dict(override['env'])['UE_UBT_START_LOCK'], self.env['UE_UBT_START_LOCK'])
        self.assertNotEqual(override['fingerprint'], spec['fingerprint'])
        self.env['UE_UBT_START_LOCK'] = 'relative.lock'
        self.assertIn('absolute path', self.cli('build', '--spec').stderr)
        self.env.pop('UE_UBT_START_LOCK')
        config.write_text('[build]\nubt_start_lock = "relative.lock"\n')
        self.assertEqual(self.cli('build', '--spec').returncode, 2)

    def test_ubt_helper_and_symbols_hook_with_fake_tools(self):
        build = self.engine / 'Engine/Build/BatchFiles/Linux/Build.sh'
        build.write_text('#!/bin/sh\n'
                         'for arg; do case "$arg" in -Log=*) log="${arg#-Log=}";; esac; done\n'
                         'echo "[1/1] Compile" > "$log"\n'
                         'test -d "$TMPDIR" && test -d "$UBA_FILE_MAPPING_DIR" || exit 92\n'
                         'test -z "${UBA_FILE_MAPPING_MEMFD:-}" && '
                         'test -z "${UnrealBuildTool_TMP:-}" || exit 93\n'
                         'printf "%s\n%s\n" "$TMPDIR" "$UBA_FILE_MAPPING_DIR" > "' +
                         str(self.root / 'uba-env.txt') + '"\n'
                         'mkdir -p "' + str(self.project.parent / 'Binaries/Linux') + '"\n'
                         'echo changed > "' + str(self.project.parent / 'Binaries/Linux/libGame.so') + '"\n'
                         'for fd in /proc/$$/fd/*; do\n'
                         '  link=$(readlink "$fd" 2>/dev/null || :)\n'
                         '  case "$link" in *ubt-start.lock*) exit 91;; esac\n'
                         'done\n')
        manifest = self.root / 'changed.json'
        log = self.root / 'build.log'
        ambient_mapping = self.root / 'shared-uba'
        env = dict(self.env, UE_UBT_START_LOCK=str(self.root / 'ubt.lock'),
                   TMPDIR=str(self.root), UBA_FILE_MAPPING_DIR=str(ambient_mapping),
                   UBA_FILE_MAPPING_MEMFD='1', UnrealBuildTool_TMP=str(self.root))
        proc = subprocess.run([sys.executable, str(ROOT / 'bin/ubt.py'),
                               '--symbols', str(manifest), '--', str(build),
                               'GameEditor', 'Linux', 'Development', str(self.project),
                               '-NoDumpSyms', '-Log=' + str(log)], env=env,
                              capture_output=True, text=True, timeout=15)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        private_tmp, mapping = map(Path, (self.root / 'uba-env.txt').read_text().splitlines())
        self.assertNotEqual(mapping, ambient_mapping)
        self.assertEqual(mapping, private_tmp / 'uba-mappings')
        self.assertEqual(private_tmp.parent, self.root)
        self.assertFalse(private_tmp.exists())  # Process-scoped cleanup after UBT exits.
        repeated = subprocess.run(proc.args, env=env, capture_output=True,
                                  text=True, timeout=15)
        self.assertEqual(repeated.returncode, 0, repeated.stderr)
        next_tmp, next_mapping = map(Path, (self.root / 'uba-env.txt').read_text().splitlines())
        self.assertNotEqual(next_tmp, private_tmp)
        self.assertEqual(next_mapping, next_tmp / 'uba-mappings')
        self.assertFalse(next_tmp.exists())
        metrics = json.loads(log.with_suffix('.metrics.json').read_text())
        self.assertEqual(metrics['exit_code'], 0)
        self.assertGreater(metrics['ubt_wall_seconds'], 0)
        self.assertGreater(metrics['ubt_child_max_rss_bytes'], 0)
        self.assertIn('scope_memory_peak_bytes', metrics)
        library = self.project.parent / 'Binaries/Linux/libGame.so'
        self.assertEqual(json.loads(manifest.read_text()), [str(library)])
        debug = library.with_suffix('.debug')
        debug.write_text('full DWARF')
        tools = self.engine / 'Engine/Binaries/Linux'
        (tools / 'dump_syms').write_text('#!/bin/sh\n[ "$1" = "-c" ] && [ "$2" = "-o" ] || exit 1\nprintf "%s\\n" "$4" > "$3"\n')
        (tools / 'BreakpadSymbolEncoder').write_text('#!/bin/sh\ncat "$1" > "$2"\n')
        for name in ('dump_syms', 'BreakpadSymbolEncoder'):
            (tools / name).chmod(0o755)
        result = subprocess.run([sys.executable, str(ROOT / 'bin/symbols.py'),
                                 '--engine', str(self.engine), '--manifest', str(manifest)],
                                capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(library.with_suffix('.sym').read_text(), str(debug) + '\n')

    @unittest.skipUnless(os.environ.get('BORG_UNREAL_TEST_CLI'),
                         'set BORG_UNREAL_TEST_CLI to an integrated Borg lane binary')
    def test_isolated_core_job_with_fake_engine(self):
        """Opt-in isolated fake core job; scoped only with an explicit user bus."""
        binary = Path(os.environ['BORG_UNREAL_TEST_CLI']).resolve(strict=True)
        self.assertTrue(binary.is_file())
        builder = self.engine / 'Engine/Build/BatchFiles/Linux/Build.sh'
        builder.write_text('#!/bin/sh\n'
                           'for arg; do case "$arg" in -Log=*) log="${arg#-Log=}";; esac; done\n'
                           'for fd in /proc/$$/fd/*; do\n'
                           '  link=$(readlink "$fd" 2>/dev/null || :)\n'
                           '  case "$link" in *ubt-start.lock*|*/locks/*) exit 91;; esac\n'
                           'done\n'
                           'printf "[1/1] Fake compile\\n" > "$log"\n'
                           'echo fake-compiler-finished\n')
        self.project.parent.joinpath('.borg-unreal.toml').write_text(
            '[build]\nmin_available_ram_gb=0\nreserve_ram_gb=0\n'
            'min_free_disk_gb=0\nreserve_disk_gb=0\n')
        self.env.update(BORG_AGENT_CLI=str(binary),
                        BORG_LANE_DIR=str(self.root / 'isolated-lane-state'))
        if os.environ.get('BORG_UNREAL_TEST_SCOPED') == '1':
            runtime = os.environ.get('XDG_RUNTIME_DIR')
            bus = os.environ.get('DBUS_SESSION_BUS_ADDRESS')
            if not runtime or not bus or not Path(runtime).is_dir():
                self.fail('scoped smoke needs XDG_RUNTIME_DIR and DBUS_SESSION_BUS_ADDRESS')
            self.env['XDG_RUNTIME_DIR'] = runtime
            self.env['DBUS_SESSION_BUS_ADDRESS'] = bus
            self.env.pop('BORG_LANE_DEGRADED', None)
            self.env.pop('BORG_LANE_SCOPE', None)
            state = Path(runtime) / 'borg/unreal' / hashlib.sha256(
                str(self.project).encode()).hexdigest()[:16]
            self.addCleanup(shutil.rmtree, state, ignore_errors=True)
        else:
            self.env.update(BORG_LANE_SCOPE='0', BORG_LANE_DEGRADED='1')
        result = self.cli('build', '--wait')
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        stages = [json.loads(line) for line in result.stdout.splitlines()]
        self.assertEqual(len(stages), 2)
        self.assertEqual(stages[-1]['state'], {'Finished': {'exit_code': 0}})
        self.assertEqual(stages[0]['job_id'], stages[-1]['job_id'])
        self.assertTrue(Path(stages[-1]['log_path']).is_file())
        if os.environ.get('BORG_UNREAL_TEST_SCOPED') == '1':
            status = subprocess.run([str(binary), 'lane', 'job', 'status', '--json'],
                                    env=self.env, capture_output=True, text=True, timeout=20)
            self.assertEqual(status.returncode, 0, status.stderr)
            record = next(r for r in json.loads(status.stdout)
                          if r['job']['id'] == stages[-1]['job_id'])
            self.assertIn('.scope', record['scope_cgroup'])
            metrics_dir = Path(self.env['XDG_RUNTIME_DIR']) / 'borg/unreal' / hashlib.sha256(
                str(self.project).encode()).hexdigest()[:16]
            metrics_files = list(metrics_dir.glob('*.ubt.metrics.json'))
            self.assertEqual(len(metrics_files), 1)
            metrics = json.loads(metrics_files[0].read_text())
            self.assertEqual(metrics['exit_code'], 0)
            self.assertGreater(metrics['ubt_child_max_rss_bytes'], 0)
            self.assertGreater(metrics['scope_memory_peak_bytes'], 0)

    @unittest.skipUnless(os.environ.get('BORG_UNREAL_TEST_SERVICE_CLI'),
                         'set BORG_UNREAL_TEST_SERVICE_CLI for the fake service smoke')
    def test_isolated_core_service_with_fake_editor(self):
        """Fake MCP health, test-only exclusive handoff and stop; not real UE."""
        binary = Path(os.environ['BORG_UNREAL_TEST_SERVICE_CLI']).resolve(strict=True)
        runtime = os.environ.get('XDG_RUNTIME_DIR')
        bus = os.environ.get('DBUS_SESSION_BUS_ADDRESS')
        if not runtime or not bus or not Path(runtime).is_dir():
            self.fail('service smoke needs a working systemd user bus')
        self.env.update(XDG_RUNTIME_DIR=runtime, DBUS_SESSION_BUS_ADDRESS=bus,
                        BORG_LANE_DIR=str(self.root / 'service-lane'))
        self.env.pop('BORG_LANE_DEGRADED', None)
        self.env.pop('BORG_LANE_SCOPE', None)
        editor = self.engine / 'Engine/Binaries/Linux/UnrealEditor'
        quit_marker = self.root / 'quit.called'
        editor.write_text("""#!/usr/bin/env python3
import json, sys, threading
from pathlib import Path
from http.server import BaseHTTPRequestHandler, HTTPServer
port = int(next(a.split('=', 1)[1] for a in sys.argv
                if a.startswith('-ModelContextProtocolPort=')))
class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get('Content-Length', '0'))
        request = json.loads(self.rfile.read(length) or b'{}')
        result = {'protocolVersion': '2025-11-25', 'capabilities': {},
                  'serverInfo': {'name': 'fake-unreal', 'version': '1'}}
        if request.get('method') == 'tools/call':
            tool = request['params']['arguments'].get('tool_name')
            if tool == 'exec_console':
                command = request['params']['arguments']['arguments']['command']
                Path('FAKE_QUIT_MARKER').write_text(command)
                threading.Timer(0.2, server.shutdown).start()
            result = {'content': [{'type': 'text',
                                   'text': json.dumps({'returnValue': False})}]}
        body = json.dumps({'jsonrpc': '2.0', 'id': request.get('id'),
                           'result': result}).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *args):
        pass
server = HTTPServer(('127.0.0.1', port), Handler)
server.serve_forever()
server.server_close()
""".replace('FAKE_QUIT_MARKER', str(quit_marker)))
        editor.chmod(0o755)
        def free_port():
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', 0))
                return sock.getsockname()[1]
        ports = []
        while len(ports) < 3:
            port = free_port()
            if port not in ports and port != 8231:
                ports.append(port)
        (self.project.parent / '.borg-unreal.toml').write_text(
            '[editor]\nport = {}\nbackend_ports = [{}, {}]\n'
            'memory_max_gb = 1\nmin_available_ram_gb = 0\nreserve_ram_gb = 0\n'
            'min_free_disk_gb = 0\nreserve_disk_gb = 0\n'
            '[run]\nmemory_max_gb = 1\nmin_available_ram_gb = 0\n'
            'reserve_ram_gb = 0\nmin_free_disk_gb = 0\n'
            'reserve_disk_gb = 0\n'.format(*ports))
        generated = self.cli('editor', 'spec')
        self.assertEqual(generated.returncode, 0, generated.stderr)
        definition = json.loads(generated.stdout)
        self.assertFalse(definition['adapter_enforces_leases'])
        service = definition['id']
        self.env['BORG_AGENT_CLI'] = str(binary)
        state = Path(runtime) / 'borg/unreal' / hashlib.sha256(
            str(self.project).encode()).hexdigest()[:16]
        self.addCleanup(shutil.rmtree, state, ignore_errors=True)
        attempted = False
        try:
            attempted = True  # a timed-out start may still have launched service
            start = self.cli('editor', 'start')
            self.assertEqual(start.returncode, 0, start.stderr + start.stdout)
            self.assertIn('Healthy', str(json.loads(start.stdout)['state']))
            status = self.cli('editor', 'status')
            self.assertEqual(status.returncode, 0, status.stderr)
            self.assertIn('Healthy', str(json.loads(status.stdout)['state']))
            request = urllib.request.Request(f'http://127.0.0.1:{ports[0]}/mcp',
                                             data=b'{}', method='POST')
            with self.assertRaises(urllib.error.HTTPError) as denied:
                urllib.request.urlopen(request, timeout=4).read()
            self.assertEqual(denied.exception.code, 403)
            denied.exception.close()
            before = json.loads(status.stdout)['backend_pid']
            marker = self.root / 'exclusive-observed'
            # This test submits a spec directly to core; the public adapter
            # deliberately still refuses non-spec exclusive runs.
            probe = ("import socket, urllib.request, urllib.error\n"
                     "from pathlib import Path\n"
                     f"ports = {ports!r}\n"
                     "for port in ports[1:]:\n"
                     "    with socket.socket() as sock:\n"
                     "        assert sock.connect_ex(('127.0.0.1', port)) != 0\n"
                     "try:\n"
                     "    urllib.request.urlopen(f'http://127.0.0.1:{ports[0]}/mcp', timeout=4)\n"
                     "except urllib.error.HTTPError as error:\n"
                     "    assert error.code == 503, error.code\n"
                     "else:\n"
                     "    raise AssertionError('fenced proxy routed during exclusive job')\n"
                     f"Path({str(marker)!r}).write_text('fenced')\n")
            generated = self.cli('run', 'commandlet', '--spec', '--',
                                 sys.executable, '-c', probe)
            self.assertEqual(generated.returncode, 0, generated.stderr)
            spec = json.loads(generated.stdout)
            self.assertEqual(spec['lease']['resources'][0]['key'],
                             definition['resources'][0]['key'])
            submit = subprocess.run([str(binary), 'lane', 'job', 'submit',
                                     '--spec', '-', '--json'], input=json.dumps(spec),
                                    env=self.env, capture_output=True, text=True, timeout=20)
            self.assertEqual(submit.returncode, 0, submit.stderr)
            job = json.loads(submit.stdout)['job_id']
            done = subprocess.run([str(binary), 'lane', 'job', 'wait', job,
                                   '--json'], env=self.env, capture_output=True,
                                  text=True, timeout=30)
            result = json.loads(done.stdout)
            log_path = Path(result['log_path'])
            log = log_path.read_text() if log_path.is_file() else '(missing job log)'
            self.assertEqual(done.returncode, 0, done.stderr + done.stdout + log)
            self.assertEqual(result['state'], {'Finished': {'exit_code': 0}}, log)
            self.assertEqual(marker.read_text(), 'fenced')
            # The core resumes asynchronously after the job releases its key.
            events = self.root / 'service-lane/services' / service
            record = {}
            for _ in range(4):
                resumed = self.cli('editor', 'status')
                self.assertEqual(resumed.returncode, 0, resumed.stderr)
                record = json.loads(resumed.stdout)
                if record.get('backend_pid') and 'Healthy' in record.get('state', {}):
                    break
                subprocess.run(['inotifywait', '-q', '-t', '2', '-e', 'moved_to',
                                str(events)], stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL, timeout=3)
            self.assertIn('Healthy', str(record['state']))
            self.assertNotEqual(record['backend_pid'], before)
        finally:
            if attempted:
                stopped = self.cli('editor', 'stop')
                self.assertEqual(stopped.returncode, 0, stopped.stderr)
                self.assertEqual(json.loads(stopped.stdout)['state'], 'Stopped')
                self.assertEqual(quit_marker.read_text(), 'QUIT_EDITOR')
                for port in ports:
                    with socket.socket() as sock:
                        sock.settimeout(1)
                        self.assertNotEqual(sock.connect_ex(('127.0.0.1', port)), 0)

    def test_graceful_quit_waits_for_backend_and_times_out_without_killing_it(self):
        """Private fake MCP server: StopPIE -> QUIT -> port close -> process exit."""
        fake = self.root / 'fake_editor.py'
        marker = self.root / 'quit-events.txt'
        fake.write_text("""import json, sys, threading, time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
port, marker, mode = int(sys.argv[1]), Path(sys.argv[2]), sys.argv[3]
class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers.get('Content-Length', '0'))))
        if request.get('method') == 'tools/call':
            name = request['params']['arguments']['tool_name']
            if name in ('StopPIE', 'exec_console'):
                with marker.open('a') as events:
                    events.write(name + '\\n')
            if name == 'exec_console' and mode == 'exit':
                threading.Timer(0.3, server.shutdown).start()
            value = name == 'IsPIERunning'
            result = {'content': [{'type': 'text',
                                   'text': json.dumps({'returnValue': value})}]}
        else:
            result = {'protocolVersion': '2025-11-25'}
        body = json.dumps({'jsonrpc': '2.0', 'id': request.get('id'),
                           'result': result}).encode()
        self.send_response(200)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def do_DELETE(self):
        self.send_response(202)
        self.end_headers()
    def log_message(self, *args):
        pass
server = HTTPServer(('127.0.0.1', port), Handler)
print('ready', flush=True)
server.serve_forever()
server.server_close()
time.sleep(0.3)  # Closing MCP is not enough: the process must exit too.
""")
        for mode in ('exit', 'linger'):
            with self.subTest(mode=mode):
                marker.unlink(missing_ok=True)
                with socket.socket() as sock:
                    sock.bind(('127.0.0.1', 0))
                    port = sock.getsockname()[1]
                pid_file = self.root / f'{mode}-{port}.pid'
                child = subprocess.Popen([sys.executable, str(ROOT / 'editor/launch.py'),
                                          '--pid-file', str(pid_file), '--',
                                          sys.executable, str(fake), str(port), str(marker), mode],
                                         stdout=subprocess.PIPE,
                                         stderr=subprocess.PIPE, text=True)
                try:
                    assert child.stdout is not None
                    self.assertEqual(child.stdout.readline().strip(), 'ready')
                    record = json.loads(pid_file.read_text())
                    self.assertEqual(record['pid'], child.pid)
                    before = time.monotonic()
                    hook = subprocess.run([sys.executable, str(ROOT / 'editor/quit.py'),
                                           '--port', str(port), '--pid-file', str(pid_file),
                                           '--timeout-seconds', '1.5' if mode == 'exit' else '0.6'],
                                          capture_output=True, text=True, timeout=5)
                    if mode == 'exit':
                        self.assertEqual(hook.returncode, 0, hook.stderr)
                        self.assertGreaterEqual(time.monotonic() - before, 0.5)
                        self.assertEqual(child.wait(timeout=2), 0)
                    else:
                        self.assertEqual(hook.returncode, 1, hook.stderr)
                        self.assertIsNone(child.poll())  # Hook never kills the editor.
                    self.assertEqual(marker.read_text().splitlines(),
                                     ['StopPIE', 'exec_console'])
                finally:
                    if child.poll() is None:
                        child.terminate()
                    child.communicate(timeout=3)

    def test_exclusive_template_fails_closed_and_service_spec(self):
        result = self.cli('run', 'commandlet', '--spec', '--', sys.executable, '-c', 'print("ok")')
        self.assertEqual(result.returncode, 0, result.stderr)
        spec = json.loads(result.stdout)
        self.assertFalse(spec['coalesce'])
        self.assertEqual(spec['argv'][:2], [sys.executable, '-c'])
        self.assertEqual([r['key']['name'] for r in spec['lease']['resources']],
                         ['unreal-project-run', 'unreal-run-ram'])
        self.assertEqual(self.cli('run', 'verify', '--', sys.executable, '-c', 'print(1)').returncode, 2)
        service = json.loads(self.cli('editor', 'spec').stdout)
        self.assertIn('editor/launch.py', service['argv'][1])
        self.assertEqual(service['argv'][2:4], ['--pid-file',
                                                service['graceful_stop']['argv'][-1]])
        self.assertIn('backend-{port}.pid', service['argv'][3])
        self.assertIn('-RenderOffscreen', service['argv'])
        self.assertIn('-unattended', service['argv'])
        self.assertIn('-ModelContextProtocolPort={port}', service['argv'])
        self.assertEqual(service['health']['kind'], 'mcp_initialize')
        self.assertFalse(service['adapter_enforces_leases'])
        self.assertEqual(service['resources'][0]['key']['name'], 'unreal-project-run')
        self.assertEqual(service['endpoint']['listen'], '127.0.0.1:8240')
        self.assertEqual(service['memory_max_bytes'], 16 << 30)
        self.assertEqual(self.cli('mcp', 'health').returncode, 2)


if __name__ == '__main__':
    unittest.main()
