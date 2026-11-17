"""Tests for the Kubernetes scaling benchmark helpers (scripts/benchmark-distributed-kubernetes.py and
scripts/analyze-distributed-scaling.py). They run on made-up inputs, no cluster needed."""
import importlib.util
import json
import unittest
import xml.dom.minidom
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


bench = load("bench", "benchmark-distributed-kubernetes.py")
analyze = load("analyze", "analyze-distributed-scaling.py")


def run(workers, rate, cpu):
    return {"workers": workers, "repetition": 1, "pages_per_second": rate, "run_pages_per_second": rate,
            "window_seconds": 60, "task_latency": {"p50_ms": 130, "p95_ms": 200},
            "db_pool": {"mean_wait_ms": 0.01, "p95_wait_ms": 0.1, "connections": 4, "max_in_use_sampled": 2},
            "cpu_cores": {"coordinator": cpu, "postgres": cpu, "fixture": cpu, "workers_total": cpu, "all_roles": 4 * cpu},
            "warnings": []}


class Benchmark(unittest.TestCase):
    def test_self_test(self):
        bench.self_test()

    def test_the_sources_config_has_one_source_per_origin(self):
        class Args:
            pages, origins, pages_per_origin = 100, 3, 50
        text = bench.sources_toml(Args, "scout-fixture")
        self.assertEqual(text.count("[[sources]]"), 3)
        self.assertIn('allowed_origins = ["http://scout-fixture:9002"]', text)
        self.assertIn("page_target = 100", text)


class Analysis(unittest.TestCase):
    results = {"environment": {"node": {"cpus": "4"}, "parameters": {}},
               "runs": [run(1, 10, .2), run(1, 12, .2), run(2, 20, .4), run(4, 24, .8), run(8, 25, 1)]}

    def test_aggregate_computes_speedup_and_efficiency_from_the_one_worker_mean(self):
        rows = analyze.aggregate(self.results)
        self.assertEqual([row["workers"] for row in rows], [1, 2, 4, 8])
        self.assertEqual(rows[0]["pages_per_second"], 11)
        self.assertAlmostEqual(rows[1]["speedup"], 20 / 11)
        self.assertAlmostEqual(rows[1]["efficiency"], 20 / 11 / 2)
        self.assertEqual(rows[0]["min"], 10)
        self.assertEqual(rows[0]["max"], 12)

    def test_flattening_names_the_first_step_that_stops_paying(self):
        flat = analyze.flattening(analyze.aggregate(self.results))
        self.assertEqual((flat["after_workers"], flat["to_workers"]), (2, 4))
        self.assertIsNone(analyze.flattening(analyze.aggregate({"runs": [run(1, 10, 1), run(2, 19, 1), run(4, 36, 1)]})))

    def test_outputs_are_well_formed(self):
        rows = analyze.aggregate(self.results)
        xml.dom.minidom.parseString(analyze.svg(self.results, rows))
        text = analyze.markdown(self.results, rows)
        self.assertIn("| 8 | 1 |", text)
        self.assertIn("flattens after 2 workers", text)


if __name__ == "__main__":
    unittest.main()
