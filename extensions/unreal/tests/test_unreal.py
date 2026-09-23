"""Template contract tests against a private fake engine; never launch Unreal."""
import json
import os
import subprocess
import sys
import tempfile
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

    def test_build_job_core_schema_and_input_revision(self):
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
                         'echo changed > "' + str(self.project.parent / 'Binaries/Linux/libGame.so') + '"\n')
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
        tools = self.engine / 'Engine/Binaries/Linux'
        (tools / 'dump_syms').write_text('#!/bin/sh\n[ "$1" = "-c" ] && [ "$2" = "-o" ] || exit 1\necho raw > "$3"\n')
        (tools / 'BreakpadSymbolEncoder').write_text('#!/bin/sh\ncat "$1" > "$2"\n')
        for name in ('dump_syms', 'BreakpadSymbolEncoder'):
            (tools / name).chmod(0o755)
        result = subprocess.run([sys.executable, str(ROOT / 'bin/symbols.py'),
                                 '--engine', str(self.engine), '--manifest', str(manifest)],
                                capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(library.with_suffix('.sym').read_text(), 'raw\n')

    @unittest.skipUnless(os.environ.get('BORG_UNREAL_TEST_CLI'),
                         'set BORG_UNREAL_TEST_CLI to an integrated Borg lane binary')
    def test_isolated_core_job_with_fake_engine(self):
        """Explicit test-only unscoped core smoke; never run against real UE."""
        binary = Path(os.environ['BORG_UNREAL_TEST_CLI']).resolve(strict=True)
        self.assertTrue(binary.is_file())
        builder = self.engine / 'Engine/Build/BatchFiles/Linux/Build.sh'
        builder.write_text('#!/bin/sh\n'
                           'for arg; do case "$arg" in -Log=*) log="${arg#-Log=}";; esac; done\n'
                           'printf "[1/1] Fake compile\\n" > "$log"\n'
                           'echo fake-compiler-finished\n')
        self.project.parent.joinpath('.borg-unreal.toml').write_text(
            '[build]\nmin_available_ram_gb=0\nreserve_ram_gb=0\n'
            'min_free_disk_gb=0\nreserve_disk_gb=0\n')
        self.env.update(BORG_AGENT_CLI=str(binary),
                        BORG_LANE_DIR=str(self.root / 'isolated-lane-state'),
                        BORG_LANE_SCOPE='0', BORG_LANE_DEGRADED='1')
        result = self.cli('build', '--wait')
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        stages = [json.loads(line) for line in result.stdout.splitlines()]
        self.assertEqual(len(stages), 2)
        self.assertEqual(stages[-1]['state'], {'Finished': {'exit_code': 0}})
        self.assertEqual(stages[0]['job_id'], stages[-1]['job_id'])
        self.assertTrue(Path(stages[-1]['log_path']).is_file())

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
