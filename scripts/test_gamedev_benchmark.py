"""High-signal invariants: no lost waits, cross-key concurrency, safe admission."""
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("bench", Path(__file__).with_name("gamedev_benchmark.py"))
assert spec is not None and spec.loader is not None
bench = importlib.util.module_from_spec(spec)
import sys
sys.modules[spec.name] = bench
spec.loader.exec_module(bench)


class BenchmarkTests(unittest.TestCase):
    def test_retry_overshoot_is_counted_and_fifo_does_not_poll(self):
        tasks = [[bench.Request(i, 0, "leaf", "one", str(i), 11, 2, 2)] for i in range(3)]
        naive = bench.simulate(tasks, "naive", scale=1)
        fifo = bench.simulate(tasks, "fifo", scale=1)
        self.assertGreater(naive["agent_wait_hours"], 0)
        self.assertGreater(naive["refusals"], 0)
        self.assertEqual(fifo["refusals"], 0)
        self.assertGreater(naive["makespan_seconds"], fifo["makespan_seconds"])

    def test_borg_coalesces_only_same_fingerprint_and_runs_disjoint_keys(self):
        tasks = [[bench.Request(i, 0, "leaf", f"tree-{i // 2}", "same" if i < 2 else f"different-{i}", 10, 2, 2)]
                 for i in range(4)]
        borg = bench.simulate(tasks, "borg", ram_limit=4, scale=1)
        self.assertEqual(borg["coalesced"], 1)
        self.assertEqual(borg["launches"], 3)
        self.assertLess(borg["makespan_seconds"], bench.simulate(tasks, "fifo", scale=1)["makespan_seconds"])
        self.assertEqual(borg["oom"], 0)
        for a in borg["spans"]:
            for b in borg["spans"]:
                if a is not b and a["key"] == b["key"]:
                    self.assertTrue(a["end"] <= b["start"] or b["end"] <= a["start"])

    def test_budget_and_service_yield(self):
        tasks = [[bench.Request(0, 0, "capture", "editor", "a", 2, 2, 1),
                  bench.Request(0, 1, "import", "tree-0", "b", 20, 6, 3),
                  bench.Request(0, 2, "capture", "editor", "c", 2, 2, 1)]]
        result = bench.simulate(tasks, "borg", ram_limit=6, scale=1)
        self.assertEqual(result["service_yields"], 1)
        self.assertEqual(result["oom"], 0)
        self.assertGreaterEqual(result["spans"][-1]["end"] - result["spans"][-1]["start"], 14)
        with self.assertRaises(ValueError):
            bench.simulate(tasks, "borg", ram_limit=5)

    def test_seed_reproducible(self):
        tasks = bench.workloads(8, 9, 23)
        self.assertEqual(bench.simulate(tasks, "borg"), bench.simulate(bench.workloads(8, 9, 23), "borg"))
        self.assertNotEqual(tasks, bench.workloads(8, 9, 24))


if __name__ == "__main__":
    unittest.main()
