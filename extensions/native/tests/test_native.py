"""Guard against output escape and accidental uncoordinated execution."""
import importlib.util
from pathlib import Path
import tempfile
import unittest

PG_FILE = Path(__file__).resolve().parents[1] / 'postgres.py'
pg_spec = importlib.util.spec_from_file_location('native_pg', PG_FILE)
assert pg_spec is not None and pg_spec.loader is not None
pg = importlib.util.module_from_spec(pg_spec)
pg_spec.loader.exec_module(pg)
from unittest.mock import patch

FILE = Path(__file__).resolve().parents[1] / 'native.py'
spec = importlib.util.spec_from_file_location('native_toolchain', FILE)
assert spec is not None and spec.loader is not None
native = importlib.util.module_from_spec(spec)
spec.loader.exec_module(native)


class NativePlannerTest(unittest.TestCase):
    def test_sizing_preserves_ram_and_caps_jobs(self):
        with patch.object(native, 'available_ram', return_value=14 * native.GIB):
            self.assertEqual(native.jobs(2 * native.GIB), 2)
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

    def test_client_database_url_keeps_admin_endpoint(self):
        self.assertEqual(pg.client_url('postgresql://me@localhost:55451/postgres', 'borg_native_123'),
                         'postgresql://me@localhost:55451/borg_native_123')

    def test_database_credentials_never_enter_job_specs(self):
        with self.assertRaisesRegex(ValueError, 'do not embed passwords'):
            pg.client_url('postgresql://me:secret@localhost/postgres', 'client')

    def test_untracked_source_disables_coalescing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            import subprocess
            subprocess.run(['git', 'init', '-q', str(root)], check=True)
            (root / 'generated.cpp').write_text('source one')
            with patch.object(native, 'fingerprint', return_value='fingerprint'):
                value = native.job_spec(root, 'cmake-build', ['cmake', '--build', str(root), '-j', '2'], {})
            self.assertFalse(value['coalesce'])

    def test_failed_submit_keeps_database_and_lease(self):
        import json
        import subprocess
        from contextlib import redirect_stderr
        from io import StringIO
        from uuid import NAMESPACE_OID, uuid5
        owner = 'borg_native_' + '0' * 32
        status = {'clients': [{'id': '11111111-1111-4111-8111-111111111111',
                    'owner': {'participant_id': str(uuid5(NAMESPACE_OID, owner))}}]}
        with patch.dict('os.environ', {'BORG_TEST_POSTGRES_ADMIN_URL': 'postgresql://me@localhost/postgres'}), \
             patch.object(pg.sys, 'argv', ['postgres.py', '--', 'python3', 'native.py', 'cargo', 'test']), \
             patch.object(pg, 'uuid4', return_value=type('Id', (), {'hex': '0' * 32})()), \
             patch.object(pg, 'sql') as sql, \
             patch.object(pg.subprocess, 'check_output', return_value=json.dumps(status)), \
             patch.object(pg.subprocess, 'run', return_value=subprocess.CompletedProcess([], 3, '', '')), \
             redirect_stderr(StringIO()):
            self.assertEqual(pg.main(), 3)
        sql.assert_called_once()  # CREATE; neither DROP nor release while submission is uncertain

    def test_database_cleanup_only_after_terminal_job(self):
        self.assertFalse(pg.terminal_job({'job': {'state': 'Queued'}}))
        self.assertFalse(pg.terminal_job({'job': {'state': {'Running': {'scope': 'active'}}}}))
        self.assertTrue(pg.terminal_job({'job': {'state': {'Finished': {'exit_code': 1}}}}))

    def test_job_spec_is_immediate_and_keeps_worktree_budget(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with patch.object(native, 'fingerprint', return_value='fingerprint'):
                value = native.job_spec(root, 'cargo-test', ['cargo', 'test', '-j', '2'],
                                        {'CARGO_BUILD_JOBS': '2', 'CARGO_TARGET_DIR': str(root / 'target')})
        self.assertEqual(value['lease']['resources'][0]['key']['scope'], {'Worktree': str(root)})
        self.assertEqual(value['admission']['min_free_disk_bytes'], 60 * native.GIB)
        self.assertEqual(value['admission']['reserve_disk_bytes'], 24 * native.GIB)
        self.assertEqual(Path(value['argv'][-4]).name, 'cargo')
        self.assertEqual(value['argv'][-3:], ['test', '-j', '2'])

    def test_postgres_job_never_coalesces_across_clients(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict('os.environ', {'BORG_TEST_SESSIONS_URL': 'postgresql://me@localhost/db'}):
                value = native.job_spec(Path(tmp), 'cargo-test', ['cargo', 'test', '-j', '2'],
                                        {'CARGO_BUILD_JOBS': '2'})
            self.assertFalse(value['coalesce'])

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
