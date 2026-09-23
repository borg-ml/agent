"""Guard against output escape and accidental uncoordinated execution."""
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

FILE = Path(__file__).resolve().parents[1] / 'native.py'
spec = importlib.util.spec_from_file_location('native_toolchain', FILE)
native = importlib.util.module_from_spec(spec)
spec.loader.exec_module(native)


class NativePlannerTest(unittest.TestCase):
    def test_sizing_preserves_ram_and_caps_jobs(self):
        with patch.object(native, 'available_ram', return_value=14 * native.GIB):
            self.assertEqual(native.jobs(2 * native.GIB), 3)
        with patch.object(native, 'available_ram', return_value=8 * native.GIB):
            with self.assertRaisesRegex(RuntimeError, 'insufficient available RAM'):
                native.jobs(native.GIB)

    def test_output_cannot_escape_worktree(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaisesRegex(ValueError, 'stay in worktree'):
                native.inside_project(root, '../another-tree/target')
            with self.assertRaisesRegex(ValueError, 'stay in worktree'):
                native.inside_project(root, '/tmp/shared-target')

    def test_cargo_args_use_private_target_and_explicit_jobs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'Cargo.toml').write_text('[workspace]\n')
            from argparse import Namespace
            with patch.object(native, 'jobs', return_value=4):
                _, lane, cmd, env = native.plan(Namespace(project=tmp, tool='cargo',
                    operation='test', package='borg-agent-runtime', extra=['--locked'],
                    target_dir='target'))
            self.assertEqual(lane, 'cargo-test')
            self.assertEqual(cmd, ['cargo', 'test', '-j', '4', '-p', 'borg-agent-runtime', '--locked'])
            self.assertEqual(env['CARGO_TARGET_DIR'], str(root / 'target'))


if __name__ == '__main__':
    unittest.main()
