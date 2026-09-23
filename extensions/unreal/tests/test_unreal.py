"""Template contract tests against a private fake engine; never launch Unreal."""
import hashlib
import json
import os
import socket
import shutil
import subprocess
import sys
import tempfile
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
        # Incomplete core CLI must fail closed rather than build directly.
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

    def test_ubt_helper_and_symbols_hook_with_fake_tools(self):
        build = self.engine / 'Engine/Build/BatchFiles/Linux/Build.sh'
        build.write_text('#!/bin/sh\n'
                         'for arg; do case "$arg" in -Log=*) log="${arg#-Log=}";; esac; done\n'
                         'echo "[1/1] Compile" > "$log"\n'
                         'mkdir -p "' + str(self.project.parent / 'Binaries/Linux') + '"\n'
                         'echo changed > "' + str(self.project.parent / 'Binaries/Linux/libGame.so') + '"\n'
                         'for fd in /proc/$$/fd/*; do\n'
                         '  link=$(readlink "$fd" 2>/dev/null || :)\n'
                         '  case "$link" in *ubt-start.lock*) exit 91;; esac\n'
                         'done\n')
        manifest = self.root / 'changed.json'
        log = self.root / 'build.log'
        env = dict(self.env, UE_UBT_START_LOCK=str(self.root / 'ubt.lock'))
        proc = subprocess.run([sys.executable, str(ROOT / 'bin/ubt.py'),
                               '--symbols', str(manifest), '--', str(build),
                               'GameEditor', 'Linux', 'Development', str(self.project),
                               '-NoDumpSyms', '-Log=' + str(log)], env=env,
                              capture_output=True, text=True, timeout=15)
        self.assertEqual(proc.returncode, 0, proc.stderr)
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

    @unittest.skipUnless(os.environ.get('BORG_UNREAL_TEST_SERVICE_CLI'),
                         'set BORG_UNREAL_TEST_SERVICE_CLI for the fake service smoke')
    def test_isolated_core_service_with_fake_editor(self):
        """Fake MCP health/proxy/stop, not live Unreal or D11 handoff."""
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
import json, sys
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
HTTPServer(('127.0.0.1', port), Handler).serve_forever()
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
            'min_free_disk_gb = 0\nreserve_disk_gb = 0\n'.format(*ports))
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
