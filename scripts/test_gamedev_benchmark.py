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
        tasks = [[bench.Request(i, 0, "leaf", f"tree-{i // 3}", "same" if i < 3 else f"different-{i}", 10, 2, 2)]
                 for i in range(5)]
        borg = bench.simulate(tasks, "borg", ram_limit=4, scale=1)
        self.assertEqual(borg["coalesced"], 1)
        self.assertEqual(borg["launches"], 4)
        self.assertLess(borg["makespan_seconds"], bench.simulate(tasks, "fifo", scale=1)["makespan_seconds"])
        self.assertEqual(borg["oom"], 0)
        for a in borg["spans"]:
            for b in borg["spans"]:
                if a is not b and a["key"] == b["key"]:
                    self.assertTrue(a["end"] <= b["start"] or b["end"] <= a["start"])

    def test_running_join_requires_explicit_revision_witness(self):
        tasks = [[bench.Request(i, 0, "leaf", "tree-0", "same", 10, 2, 2)] for i in range(2)]
        self.assertEqual(bench.simulate(tasks, "borg", scale=1)["coalesced"], 0)
        verified = bench.simulate(tasks, "borg", scale=1, coalesce_running=True)
        self.assertEqual(verified["coalesced"], 1)
        self.assertEqual(verified["launches"], 1)

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

    def test_seeded_metrics_match_documented_scenario(self):
        # Makespan is shown to one decimal in benchmark.md; wait quantiles
        # use the exact precision emitted in the public JSON output.
        expected = {
            "naive": (177.7, 89.5, 162.0),
            "fifo": (174.9, 137.235, 159.213),
            "borg": (57.4, 33.491, 42.902),
        }
        tasks = bench.workloads(6, 8, 23)
        for policy, (makespan, p50, p95) in expected.items():
            with self.subTest(policy=policy):
                result = bench.simulate(tasks, policy, scale=10)
                self.assertEqual(round(result["makespan_seconds"], 1), makespan)
                self.assertEqual(len(result["per_agent_wait_seconds"]), 6)
                self.assertEqual(result["per_agent_wait_seconds_p50"], p50)
                self.assertEqual(result["per_agent_wait_seconds_p95"], p95)
                self.assertAlmostEqual(sum(result["per_agent_wait_seconds"]) / 3600,
                                       result["agent_wait_hours"], places=4)

    def test_seed_reproducible(self):
        tasks = bench.workloads(8, 9, 23)
        self.assertEqual(bench.simulate(tasks, "borg"), bench.simulate(bench.workloads(8, 9, 23), "borg"))
        self.assertNotEqual(tasks, bench.workloads(8, 9, 24))


if __name__ == "__main__":
    unittest.main()
