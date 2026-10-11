"""Cost, recovered fraction, verdicts and the tuning selection."""

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import analyze  # noqa: E402


def row(arm, seed, completion, kind="half_rate", t_us=400, setting=None, stage="t_comparison"):
    geometry = "none" if kind == "healthy" else "spinewide"
    return {"stage": stage, "arm": arm, "setting": setting, "k": 32, "kind": kind,
            "geometry": geometry, "t_us": t_us, "seed": seed, "completion_ns": completion}


class Cost(unittest.TestCase):
    def rows(self):
        rows = []
        # Healthy completions at T = 400 us, and decoys at T = 100 us.
        for arm, base in (("uniform", 1000), ("graded_per_receiver", 900), ("graded_per_connection", 950)):
            for seed, offset in ((301, 0), (302, 10), (303, 20)):
                rows.append(row(arm, seed, base + offset, kind="healthy"))
                rows.append(row(arm, seed, 1, kind="healthy", t_us=100))
        # Faulted: uniform costs 400 on every seed; per_receiver 100, 120, 140;
        # per_connection 300, 200, 380.
        for seed, offset in ((301, 0), (302, 10), (303, 20)):
            rows.append(row("uniform", seed, 1400 + offset))
        for seed, cost in ((301, 100), (302, 120), (303, 140)):
            rows.append(row("graded_per_receiver", seed, 900 + (seed - 301) * 10 + cost))
        for seed, cost in ((301, 300), (302, 200), (303, 380)):
            rows.append(row("graded_per_connection", seed, 950 + (seed - 301) * 10 + cost))
        return rows

    def test_cost_and_recovered_fraction(self):
        (cell,) = analyze.cost_cells(self.rows(), reference="graded_per_connection")
        receiver = cell["arms"]["graded_per_receiver"]
        self.assertEqual(receiver["cost_ns"], [100, 120, 140])
        self.assertEqual(receiver["cost_ns_mean"], 120)
        for got, want in zip(receiver["recovered"], [0.75, 0.7, 0.65]):
            self.assertAlmostEqual(got, want)
        self.assertAlmostEqual(receiver["recovered_mean"], 0.7)
        self.assertEqual(cell["arms"]["uniform"]["recovered"], [0, 0, 0])
        connection = cell["arms"]["graded_per_connection"]
        for got, want in zip(connection["recovered"], [0.25, 0.5, 0.05]):
            self.assertAlmostEqual(got, want)

    def test_verdicts(self):
        (cell,) = analyze.cost_cells(self.rows(), reference="graded_per_connection")
        receiver = cell["arms"]["graded_per_receiver"]
        # Gap 173 against ranges 40 and 180: better on every seed, within noise.
        self.assertEqual(receiver["versus_reference_cost"], "within seed noise")
        self.assertEqual(receiver["versus_reference_recovered"], "within seed noise")
        uniform = cell["arms"]["uniform"]
        self.assertEqual(uniform["versus_reference_recovered"], "within seed noise")
        self.assertEqual(
            analyze.verdict([0.9, 0.8, 0.85], [0.1, 0.2, 0.15], "a", "b", higher_is_better=True), "a beats b"
        )
        self.assertEqual(analyze.verdict([10, 11, 12], [30, 31, 32], "a", "b"), "a beats b")

    def test_incomplete_and_unpaired(self):
        rows = [row("uniform", 301, 1000, kind="healthy"), row("uniform", 301, None),
                row("reps", 301, 1200)]
        by_arm = {r["arm"]: r["cost_ns"] for r in analyze.costs(rows)}
        self.assertEqual(by_arm, {"uniform": float("inf"), "reps": None})
        self.assertIsNone(analyze.recovered_fraction(100, float("inf")))
        self.assertIsNone(analyze.recovered_fraction(100, 0))

    def test_arms_with_one_setting_pair_by_name(self):
        setting = {"gamma": 0.5}
        rows = [dict(r, setting=setting) if r["arm"] != "uniform" else r for r in self.rows()]
        (cell,) = analyze.cost_cells(rows, reference="graded_per_connection")
        receiver = cell["arms"]["graded_per_receiver"]
        self.assertEqual(receiver["setting"], setting)
        self.assertEqual(receiver["versus_reference_cost"], "within seed noise")
        for got, want in zip(receiver["recovered"], [0.75, 0.7, 0.65]):
            self.assertAlmostEqual(got, want)

    def test_arms_with_several_settings_stay_apart(self):
        rows = []
        for gamma, faulted in ((0.25, 1500), (1.0, 1300)):
            setting = {"gamma": gamma}
            rows.append(row("graded_per_receiver", 201, faulted, setting=setting, stage="t_tuning"))
            rows.append(row("graded_per_receiver", 201, 1000, kind="healthy", setting=setting, stage="t_tuning"))
        (cell,) = analyze.cost_cells(rows, reference="graded_per_connection")
        self.assertEqual(sorted(e["cost_ns"] for e in cell["arms"].values()), [[300], [500]])

    def test_select_by_cost_not_completion(self):
        rows = []
        # Setting A: completion 1500, healthy 1400, cost 100. Setting B:
        # completion 1300, healthy 1000, cost 300.
        for setting, faulted, healthy in (({"gamma": 0.25}, 1500, 1400), ({"gamma": 1.0}, 1300, 1000)):
            rows.append(row("graded_per_receiver", 201, faulted, setting=setting, stage="t_tuning"))
            rows.append(row("graded_per_receiver", 201, healthy, kind="healthy", setting=setting, stage="t_tuning"))
        self.assertEqual(analyze.select_by_cost(rows)["graded_per_receiver"], {"gamma": 0.25})


class Versus(unittest.TestCase):
    def test_cells_split_by_geometry_and_duration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runs = []
            for geometry, t_us, base in (("spinewide", 400, 1000), ("downlink0", 400, 2000), ("spinewide", 100, 3000)):
                for arm, extra in (("uniform", 100), ("graded_per_receiver", 0)):
                    for seed in (301, 302, 303):
                        key = f"t_comparison-k32-half_rate-{geometry}-t{t_us}-{arm}-s{seed}"
                        runs.append({"key": key, "stage": "t_comparison", "arm": arm, "k": 32,
                                     "condition": "half_rate", "geometry": geometry, "t_us": t_us, "seed": seed})
                        (root / key).mkdir()
                        (root / key / "summary.json").write_text(
                            json.dumps({"collective_completion_ns": base + extra + seed - 301}))
            (root / "runs.json").write_text(json.dumps(runs))
            rows = analyze.versus(root / "runs.json", root, "graded_per_receiver", "uniform")
        self.assertEqual(len(rows), 3)
        by_cell = {(r["geometry"], r["t_us"]): r for r in rows}
        self.assertEqual(by_cell[("downlink0", 400)]["uniform"], [2100, 2101, 2102])
        self.assertEqual(by_cell[("spinewide", 100)]["graded_per_receiver"], [3000, 3001, 3002])
        self.assertTrue(all(r["verdict"] == "graded_per_receiver beats uniform" for r in rows))


if __name__ == "__main__":
    unittest.main()
