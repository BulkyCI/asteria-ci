"""The all-to-all probe's run list and its per-iteration timing."""

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import profiles  # noqa: E402
import summarize  # noqa: E402

HEADER = "rank,parallelism_domain,collective_type,training_step,workload_node_id,logical_bytes,start_time_ns,end_time_ns\n"


class Runs(unittest.TestCase):
    def test_run_list(self):
        runs = profiles.a2a_runs({"graded_per_receiver": {"gamma": 0.5}})
        self.assertEqual(len(runs), 16)
        self.assertEqual(len({r["key"] for r in runs}), 16)
        self.assertTrue(all(r["key"].startswith("a2a_probe-") for r in runs))
        sq = [r for r in runs if r["arm"] == "graded_switch_queue"]
        self.assertTrue(all(r["setting"] == {"gamma": 0.5} for r in sq))

    def test_profile(self):
        run = next(r for r in profiles.a2a_runs(None) if r["condition"] == "half_rate" and r["compute_ns"])
        document = profiles.a2a_profile(run, None)
        workload = document["workload"]
        self.assertEqual(document["steps"], workload["iterations"])
        self.assertEqual(workload["message_bytes"] % 4, 0)
        self.assertEqual(workload["compute_ns"] % 1000, 0)
        self.assertEqual(
            document["network"]["link_failures"],
            [{"model": "gray", "rate": "200Gbps", "start_ns": profiles.DEGRADE_AT_NS, "spine": 3, "leaf": 0}],
        )


class Timing(unittest.TestCase):
    def timing(self, rows, iterations=3, group=(0, 1), collectives=1):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "collective_events.csv"
            path.write_text(HEADER + "".join(f"{r},,{t},{i},{i},8,{s},{e}\n" for r, t, i, s, e in rows))
            return summarize.all_to_all_timing(path, iterations, list(group), collectives)

    def test_span_over_ranks(self):
        rows = [
            (0, "all_to_all", 1, 100, 400), (1, "all_to_all", 1, 150, 500),
            (0, "all_to_all", 2, 600, 700), (1, "all_to_all", 2, 600, 900),
            (0, "all_to_all", 3, 1000, 1100), (1, "all_to_all", 3, 1050, 1100),
            (0, "all_reduce", 3, 0, 99999),
        ]
        timing = self.timing(rows)
        self.assertTrue(timing["complete"])
        self.assertEqual(timing["iteration_completion_ns"], {"1": 400, "2": 300, "3": 100})
        self.assertEqual((timing["p50_ns"], timing["p99_ns"], timing["max_ns"]), (300, 400, 400))
        self.assertAlmostEqual(timing["mean_ns"], 800 / 3)

    def test_dispatch_and_combine_share_the_step(self):
        # Each step's dispatch, then its combine; step 2 lacks rank 1's
        # combine, as a run stopped after its last dispatch would.
        rows = [
            (0, "all_to_all", 1, 100, 200), (1, "all_to_all", 1, 100, 210),
            (0, "all_to_all", 1, 330, 500), (1, "all_to_all", 1, 330, 520),
            (0, "all_to_all", 2, 1000, 1100), (1, "all_to_all", 2, 1000, 1110),
            (0, "all_to_all", 2, 1230, 1400),
        ]
        timing = self.timing(rows, 2, collectives=2)
        self.assertFalse(timing["complete"])
        self.assertEqual(timing["iteration_completion_ns"], {"1": 420})
        self.assertTrue(self.timing(rows, 2)["complete"])

    def test_all_reduce_rows(self):
        rows = [(0, "all_reduce", 1, 100, 400), (1, "all_reduce", 1, 120, 450), (0, "all_to_all", 1, 0, 9999)]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "collective_events.csv"
            path.write_text(HEADER + "".join(f"{r},,{t},{i},{i},8,{s},{e}\n" for r, t, i, s, e in rows))
            timing = summarize.all_to_all_timing(path, 1, [0, 1], collective_type="all_reduce")
        self.assertTrue(timing["complete"])
        self.assertEqual(timing["iteration_completion_ns"], {"1": 350})

    def test_missing_rank_is_incomplete(self):
        timing = self.timing([(0, "all_to_all", 1, 0, 10), (1, "all_to_all", 1, 0, 10), (0, "all_to_all", 2, 20, 30)], 2)
        self.assertFalse(timing["complete"])
        self.assertEqual(timing["iterations_completed"], 1)


if __name__ == "__main__":
    unittest.main()
