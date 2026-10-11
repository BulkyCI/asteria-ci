"""Per-transient measures on synthetic offered-window series."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import summarize  # noqa: E402

W = summarize.WINDOW_NS
UNIFORM = 1 / 8
HALF_RATE_MIDPOINT = (1 / 8 + 1 / 15) / 2


def series(shares: dict[int, float], last: int) -> list[tuple[int, float]]:
    """Windows 0..last at share 1/8 except those given, by window index."""
    return [(i * W, shares.get(i, UNIFORM)) for i in range(last + 1)]


class Measures(unittest.TestCase):
    def setUp(self):
        # Four low windows (13 to 16) are not a run; the run of five starts
        # at 18.
        shares = {i: 0.05 for i in range(13, 17)}
        shares.update({i: 0.04 for i in range(18, 30)})
        shares.update({30: 0.06, 31: 0.06, 32: 0.06, 33: 0.06, 34: 0.09, 35: 0.05})
        shares.update({i: 0.10 for i in range(36, 41)})
        shares.update({i: 0.03 for i in range(63, 68)})
        self.series = series(shares, 100)
        self.schedule = [(10 * W, 30 * W), (60 * W, 63 * W)]
        self.measures = summarize.transient_measures(self.series, self.schedule, HALF_RATE_MIDPOINT, UNIFORM)

    def test_detection(self):
        first = self.measures[0]
        self.assertTrue(first["detected"])
        self.assertEqual(first["detection_ns"], 8 * W)
        self.assertAlmostEqual(first["detected_share"], 0.04)

    def test_restore(self):
        # Threshold (0.04 + 1/8) / 2 = 0.0825: window 34 is above it but 35
        # breaks the run; the run of five starts at 36, at a share of 0.10,
        # below the midpoint of 1/8 and the fluid share.
        self.assertEqual(self.measures[0]["restore_ns"], 6 * W)

    def test_run_starting_at_the_end_is_not_a_detection(self):
        second = self.measures[1]
        self.assertFalse(second["detected"])
        self.assertIsNone(second["detection_ns"])
        self.assertIsNone(second["restore_ns"])

    def test_no_midpoint_reports_shares_only(self):
        measures = summarize.transient_measures(self.series, self.schedule, None, UNIFORM)
        self.assertFalse(any(m["detected"] for m in measures))
        self.assertAlmostEqual(measures[1]["mean_share"], UNIFORM)

    def test_false_hold(self):
        low = 0.05
        held = summarize.false_hold(
            {
                "spine0": series({9: low, 10: low, 20: low, 45: low}, 60),
                "spine1": series({45: low, 50: low}, 60),
            },
            [(10 * W + 5, 30 * W)],
            HALF_RATE_MIDPOINT,
        )
        self.assertEqual(held["per_spine_ns"], {"spine0": 2 * W, "spine1": 2 * W})
        self.assertEqual(held["any_ns"], 3 * W)


class FromOfferedWindows(unittest.TestCase):
    """Leaf 0's spine 3 carries nothing from window 10 on; leaf 1 carries
    seven times leaf 0's packets, all spines alike."""

    def offered(self):
        counts = {}
        for window in range(40):
            for spine in range(8):
                counts[(window, 0, spine)] = [0 if spine == 3 and window >= 10 else 10, 0]
                counts[(window, 1, spine)] = [70, 0]
        return counts

    def profile(self, failure):
        return {"network": {"spine_count": 8, "link_rate": "400Gbps", "link_failures": [failure]}}

    def failure(self, **extra):
        return {"model": "gray", "rate": "200Gbps", "start_ns": 10 * W, "spine": 3,
                "duration_ns": 20 * W, "repeat": 1, "period_ns": 0, **extra}

    def test_single_downlink_reads_its_leaf(self):
        result = summarize.transients(self.profile(self.failure(leaf=0)), self.offered(), [0, 1])
        self.assertEqual(result["affected_leaves"], [0])
        self.assertAlmostEqual(result["midpoint"], HALF_RATE_MIDPOINT)
        self.assertEqual(result["occurrences"][0]["detection_ns"], 0)
        self.assertEqual(result["false_hold"]["per_spine_ns"]["spine3_leaves1"], 0)

    def test_spine_wide_reads_every_receiving_leaf(self):
        result = summarize.transients(self.profile(self.failure()), self.offered(), [0, 1])
        self.assertEqual(result["affected_leaves"], [0, 1])
        self.assertFalse(result["occurrences"][0]["detected"])
        self.assertAlmostEqual(result["occurrences"][0]["mean_share"], 70 / 630)

    def test_fluid_shares(self):
        self.assertAlmostEqual(summarize.fluid_share({"model": "gray", "rate": "200Gbps"}, "400Gbps", 8), 1 / 15)
        self.assertEqual(summarize.fluid_share({"model": "silent"}, "400Gbps", 8), 0.0)
        self.assertIsNone(summarize.fluid_share({"model": "gray", "error_rate": 0.05}, "400Gbps", 8))

    def test_untimed_failure_has_no_transients(self):
        failure = {"model": "silent", "start_ns": 0, "spine": 3}
        self.assertIsNone(summarize.transients(self.profile(failure), self.offered(), [0]))


if __name__ == "__main__":
    unittest.main()
