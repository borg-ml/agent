#!/usr/bin/env python3
"""Fake workloads exercise the actual CLI across processes; never invokes an engine.

Run: python3 crates/borg-cli/tests/test_lane_process.py target/debug/borg
"""
import json
import os
import pathlib
import signal
import subprocess
import sys
import tempfile
import time
import unittest
import uuid

BORG = pathlib.Path(sys.argv.pop() if len(sys.argv) > 1 else "target/debug/borg").resolve()


class LaneProcess(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="gd-lane-process-")
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        self.env = dict(os.environ, BORG_LANE_DIR=str(self.root / "state"))

    def cli(self, *args, spec=None, timeout=25):
        return subprocess.run([str(BORG), "lane", "--json", *args],
                              input=json.dumps(spec) if spec is not None else None,
                              capture_output=True, text=True, env=self.env, timeout=timeout)

    def spec(self, name, script=None, access="Exclusive", scope=None, **extra):
        resource = {"key": {"scope": scope or {"Worktree": str(self.root)}, "name": "build"},
                    "access": access}
        spec = {"fingerprint": name, "lease": {"resources": [resource],
                "holder": {"participant_id": str(uuid.uuid4()), "session_id": str(uuid.uuid4()),
                           "host_pid": None, "purpose": name}, "queue_timeout_ms": None},
                "argv": [sys.executable, "-c", script or "print('ok')"], "cwd": str(self.root),
                "env": [], "memory_max_bytes": 1_073_741_824,
                "admission": {"min_available_ram_bytes": 0, "reserve_ram_bytes": 1_048_576,
                              "min_free_disk_bytes": 0, "reserve_disk_bytes": 1_048_576,
                              "disk_path": str(self.root)},
                "pre_hook": None, "post_hook": None, "timeout_ms": 10000,
                "stall_timeout_ms": None, "coalesce": True}
        spec.update(extra)
        return spec

    def submit(self, spec):
        out = self.cli("job", "submit", "--spec", "-", spec=spec)
        self.assertEqual(out.returncode, 0, (out.stdout, out.stderr))
        return json.loads(out.stdout)["job_id"]

    def wait(self, id, code=0):
        out = self.cli("job", "wait", id)
        self.assertEqual(out.returncode, code, (out.stdout, out.stderr))
        return json.loads(out.stdout)

    def records(self):
        return json.loads(self.cli("job", "status").stdout)

    def until(self, predicate, timeout=5):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = predicate()
            if value:
                return value
            time.sleep(.03)  # Test-only synchronization; production agent wait uses flock.
        self.fail("timed out waiting for fake process state")

    def test_fifo_and_coalesced_pending_job(self):
        trace = self.root / "trace"
        def job(name, delay):
            script = (f"import time; f=open({str(trace)!r},'a'); f.write('start {name}\\n');f.close();"
                      f"time.sleep({delay}); f=open({str(trace)!r},'a');f.write('end {name}\\n');f.close()")
            return self.spec(name, script)
        first = self.submit(job("a", .30))
        self.until(lambda: trace.exists() and "start a" in trace.read_text())
        second = self.submit(job("b", .12))
        joined = self.submit(job("b", .12))
        self.assertEqual(second, joined)
        third = self.submit(job("c", .05))
        for id in (first, second, third): self.wait(id)
        self.assertEqual(trace.read_text().splitlines(),
                         ["start a", "end a", "start b", "end b", "start c", "end c"])

    def test_distinct_worktrees_overlap_and_exclusive_waits_for_shared(self):
        self.cli("resource", "set-capacity", "--name", "build", "--slots", "2", "--scope", str(self.root))
        trace = self.root / "overlap"
        def job(name, access):
            script = (f"import time;f=open({str(trace)!r},'a');f.write('start {name}\\n');f.close();"
                      "time.sleep(.24);" + f"f=open({str(trace)!r},'a');f.write('end {name}\\n');f.close()")
            return self.spec(name, script, access=access)
        a = self.submit(job("a", {"Shared": {"slots": 1}}))
        b = self.submit(job("b", {"Shared": {"slots": 1}}))
        x = self.submit(job("x", "Exclusive"))
        for id in (a, b, x): self.wait(id)
        lines = trace.read_text().splitlines()
        self.assertLess(lines.index("start b"), lines.index("end a"))
        self.assertGreater(lines.index("start x"), lines.index("end a"))
        self.assertGreater(lines.index("start x"), lines.index("end b"))

    def test_rejects_project_and_worktree_path_aliases(self):
        project = self.root / "project"
        project.mkdir()
        link = self.root / "project-link"
        link.symlink_to(project, target_is_directory=True)
        canonical = self.submit(self.spec("canonical", scope={"Project": str(project)}))
        self.wait(canonical)
        for scope_kind in ("Project", "Worktree"):
            for path in (project / ".." / "project", link):
                spec = self.spec("alias", scope={scope_kind: str(path)})
                out = self.cli("job", "submit", "--spec", "-", spec=spec)
                self.assertNotEqual(out.returncode, 0, (out.stdout, out.stderr))
                self.assertIn("not canonical", out.stderr)
                cap = self.cli("resource", "set-capacity", "--name", "build",
                               "--scope", str(path), "--slots", "2")
                # The capacity CLI canonicalizes this path before setting the
                # SAME key; the store rejects raw aliases at its own boundary.
                self.assertEqual(cap.returncode, 0, (cap.stdout, cap.stderr))
        self.assertEqual(len(self.records()), 1, "invalid aliases must never enter the journal")

    def test_ram_and_disk_queue_reasons(self):
        spec = self.spec("ram", admission={"min_available_ram_bytes": 2**60, "reserve_ram_bytes": 1,
                            "min_free_disk_bytes": 0, "reserve_disk_bytes": 1, "disk_path": str(self.root)})
        job = self.submit(spec)
        reason = self.until(lambda: next((r.get("wait_reason") for r in self.records()
                                          if r["ticket"]["id"] == job and r.get("wait_reason")), None))
        self.assertIn("RAM", reason)
        self.assertEqual(self.cli("job", "cancel", job).returncode, 0)
        self.wait(job, 125)
        disk = self.spec("disk", admission={"min_available_ram_bytes": 0, "reserve_ram_bytes": 1,
                                    "min_free_disk_bytes": 2**60, "reserve_disk_bytes": 1,
                                    "disk_path": str(self.root)})
        job = self.submit(disk)
        reason = self.until(lambda: next((r.get("wait_reason") for r in self.records()
                                          if r["ticket"]["id"] == job and r.get("wait_reason")), None))
        self.assertIn("disk", reason)
        self.assertEqual(self.cli("job", "cancel", job).returncode, 0)
        self.wait(job, 125)  # Supervisor exits before TemporaryDirectory cleanup.

    def test_pre_exclusive_prepares_before_grant_and_failing_hook_blocks_workload(self):
        state_file = self.root / "prestate"
        pre = (f"import json,os,subprocess; s=subprocess.check_output([{str(BORG)!r},'lane',"
               "'--json','job','status',os.environ['BORG_LANE_JOB']]);"
               f"open({str(state_file)!r},'w').write(str(json.loads(s)['state']))")
        spec = self.spec("pre", pre_hook={"argv": [sys.executable, "-c", pre], "timeout_ms": 5000})
        job = self.submit(spec)
        self.wait(job)
        self.assertEqual(state_file.read_text(), "Preparing")
        forbidden = self.root / "forbidden"
        spec = self.spec("failed-pre", script=f"open({str(forbidden)!r},'w').write('bad')",
                         pre_hook={"argv": [sys.executable, "-c", "raise SystemExit(7)"], "timeout_ms": 5000})
        self.wait(self.submit(spec), 125)
        self.assertFalse(forbidden.exists())

    def test_supervisor_crash_recovers_only_owned_scope(self):
        job = self.submit(self.spec("orphan", script="import time; time.sleep(60)", timeout_ms=70_000))
        r = self.until(lambda: next((r for r in self.records() if r["ticket"]["id"] == job
                                    and isinstance(r["job"]["state"], dict) and r["job"]["state"].get("Running")
                                    and r.get("scope_cgroup")), None))
        pid = r["supervisor_pid"]
        self.assertIn("borg-lane-", r["scope_cgroup"])
        os.kill(pid, signal.SIGKILL)  # This test started exactly this supervisor.
        out = self.cli("job", "recover", "--dry-run")
        self.assertEqual(out.returncode, 0, out.stderr)
        self.assertTrue(json.loads(out.stdout))
        out = self.cli("job", "recover")
        self.assertEqual(out.returncode, 0, out.stderr)
        result = self.wait(job, 125)
        self.assertEqual(result["state"]["Finished"]["exit_code"], 125)
        self.assertFalse(next(r for r in self.records() if r["ticket"]["id"] == job)["quarantined"])

if __name__ == "__main__":
    unittest.main(verbosity=2)
