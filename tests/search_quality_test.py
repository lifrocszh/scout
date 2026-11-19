"""Tests for scripts/ci/search_quality.py (the search-quality gate's helper)."""

import contextlib
import importlib.util
import io
import json
import os
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "ci" / "search_quality.py"
spec = importlib.util.spec_from_file_location("search_quality", SCRIPT)
sq = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sq)


def scores(ndcg, recall, mrr):
    return {"ndcg_at_10": ndcg, "recall_at_10": recall, "mrr_at_10": mrr}


def results(**modes):
    return {"schema_version": sq.SCHEMA, "query_count": 64, "modes": modes}


BASELINE = results(
    keyword=scores(0.8000, 0.9000, 0.8000),
    semantic=scores(0.9000, 0.9500, 0.9000),
    hybrid=scores(0.9200, 0.9600, 0.9100),
)


def with_keyword(**overrides):
    current = json.loads(json.dumps(BASELINE))
    current["modes"]["keyword"].update(overrides)
    return current


class CompareTest(unittest.TestCase):
    def test_identical_results_pass(self):
        rows, problems = sq.compare(BASELINE, BASELINE, 0.02)
        self.assertEqual(problems, [])
        self.assertEqual(len(rows), 9)
        self.assertTrue(all(status == "ok" for *_, status in rows))

    def test_a_drop_of_exactly_the_threshold_passes(self):
        _, problems = sq.compare(BASELINE, with_keyword(ndcg_at_10=0.7800), 0.02)
        self.assertEqual(problems, [])

    def test_a_drop_above_the_threshold_fails_and_names_both_values(self):
        rows, problems = sq.compare(BASELINE, with_keyword(ndcg_at_10=0.7799), 0.02)
        self.assertEqual(len(problems), 1)
        self.assertIn("keyword NDCG@10", problems[0])
        self.assertIn("0.8000", problems[0])
        self.assertIn("0.7799", problems[0])
        self.assertIn(("keyword", "ndcg_at_10", 0.8, 0.7799, "REGRESSED"), rows)

    def test_every_metric_is_gated(self):
        for metric in sq.METRICS:
            with self.subTest(metric=metric):
                _, problems = sq.compare(BASELINE, with_keyword(**{metric: 0.5}), 0.02)
                self.assertEqual(len(problems), 1)
                self.assertIn(sq.LABELS[metric], problems[0])

    def test_the_threshold_is_configurable(self):
        current = with_keyword(ndcg_at_10=0.7000)
        self.assertEqual(len(sq.compare(BASELINE, current, 0.02)[1]), 1)
        self.assertEqual(sq.compare(BASELINE, current, 0.15)[1], [])

    def test_an_improvement_is_flagged_but_does_not_fail(self):
        rows, problems = sq.compare(BASELINE, with_keyword(ndcg_at_10=0.9000), 0.02)
        self.assertEqual(problems, [])
        self.assertIn(("keyword", "ndcg_at_10", 0.8, 0.9, "improved"), rows)

    def test_a_baseline_mode_missing_from_the_results_fails(self):
        current = json.loads(json.dumps(BASELINE))
        del current["modes"]["hybrid"]
        _, problems = sq.compare(BASELINE, current, 0.02)
        self.assertEqual(len(problems), 1)
        self.assertIn("hybrid", problems[0])

    def test_a_results_mode_without_a_baseline_fails(self):
        baseline = json.loads(json.dumps(BASELINE))
        del baseline["modes"]["semantic"]
        _, problems = sq.compare(baseline, BASELINE, 0.02)
        self.assertEqual(len(problems), 1)
        self.assertIn("semantic", problems[0])

    def test_the_modes_filter_limits_what_is_gated(self):
        current = json.loads(json.dumps(BASELINE))
        del current["modes"]["semantic"], current["modes"]["hybrid"]
        rows, problems = sq.compare(BASELINE, current, 0.02, modes=["keyword"])
        self.assertEqual(problems, [])
        self.assertEqual({mode for mode, *_ in rows}, {"keyword"})

    def test_a_changed_query_count_fails(self):
        current = json.loads(json.dumps(BASELINE))
        current["query_count"] = 63
        _, problems = sq.compare(BASELINE, current, 0.02)
        self.assertTrue(any("query count" in problem for problem in problems))

    def test_the_table_shows_before_and_after(self):
        rows, _ = sq.compare(BASELINE, with_keyword(ndcg_at_10=0.7000), 0.02)
        text = sq.format_table(rows, 0.02)
        line = next(line for line in text.splitlines() if "keyword" in line and "NDCG@10" in line)
        for expected in ("0.8000", "0.7000", "-0.1000", "REGRESSED"):
            self.assertIn(expected, line)
        self.assertIn("**REGRESSED**", sq.format_table(rows, 0.02, markdown=True))


class CommandTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)

    def run_main(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            try:
                code = sq.main(list(argv))
            except SystemExit as exit_:
                code = exit_.code
        return code, out.getvalue(), err.getvalue()

    def write(self, name, value):
        path = self.dir / name
        path.write_text(json.dumps(value))
        return str(path)

    def test_compare_exit_codes(self):
        baseline = self.write("baseline.json", BASELINE)
        same = self.write("same.json", BASELINE)
        worse = self.write("worse.json", with_keyword(mrr_at_10=0.5))
        code, out, _ = self.run_main("compare", "--baseline", baseline, "--results", same)
        self.assertEqual(code, 0)
        self.assertIn("PASS", out)
        code, out, err = self.run_main("compare", "--baseline", baseline, "--results", worse)
        self.assertEqual(code, 1)
        self.assertIn("REGRESSED", out)
        self.assertIn("update the baseline", err)

    def test_compare_without_a_baseline_says_how_to_create_one(self):
        code, _, err = self.run_main(
            "compare",
            "--baseline",
            str(self.dir / "none.json"),
            "--results",
            self.write("r.json", BASELINE),
        )
        self.assertEqual(code, 1)
        self.assertIn("--update-baseline", err)

    def test_compare_reads_the_threshold_from_the_environment(self):
        baseline = self.write("baseline.json", BASELINE)
        worse = self.write("worse.json", with_keyword(ndcg_at_10=0.7000))
        os.environ["SEARCH_QUALITY_MAX_DROP"] = "0.15"
        self.addCleanup(os.environ.pop, "SEARCH_QUALITY_MAX_DROP", None)
        code, _, _ = self.run_main("compare", "--baseline", baseline, "--results", worse)
        self.assertEqual(code, 0)

    def test_summarize_keeps_the_gated_metrics_per_mode_rounded(self):
        report = {
            "query_count": 64,
            "metrics": {
                "keyword": {
                    "ndcg_at_10": 0.828412345,
                    "mrr_at_10": 0.8,
                    "recall_at_10": 0.95,
                    "recall_at_100": 1.0,
                },
                "other": {"ndcg_at_10": 1.0, "mrr_at_10": 1.0, "recall_at_10": 1.0},
            },
        }
        summary = sq.summarize(report)
        self.assertEqual(summary["query_count"], 64)
        self.assertEqual(list(summary["modes"]), ["keyword"])
        self.assertEqual(summary["modes"]["keyword"], scores(0.8284, 0.95, 0.8))

    def test_package_resolves_paths_to_crawled_page_ids_and_rejects_unknown_pages(self):
        eval_dir = self.dir / "eval"
        eval_dir.mkdir()
        (eval_dir / "queries.jsonl").write_text('{"query_id": "q-001"}\n')
        (eval_dir / "judgments.jsonl").write_text(
            '{"query_id": "q-001", "path": "/rust/a.html", "grade": 3}\n'
            '{"query_id": "q-001", "path": "/rust/b.html", "grade": 1}\n'
        )
        (eval_dir / "metadata.json").write_text('{"evaluation_id": "fixture"}')
        event = lambda path, page_id: json.dumps(  # noqa: E731
            {
                "event": "page.admitted",
                "data": {"final_url": "http://h:1" + path, "page_id": page_id},
            }
        )
        crawl = self.dir / "crawl.jsonl"
        crawl.write_text("noise that is not json\n" + event("/rust/a.html", "id-a") + "\n")
        original = sq.EVAL_DIR
        sq.EVAL_DIR = eval_dir
        self.addCleanup(setattr, sq, "EVAL_DIR", original)
        common = [
            "package",
            "--crawl-log",
            str(crawl),
            "--origin",
            "http://h:1",
            "--snapshot",
            "snap",
            "--generation",
            "gen",
            "--model-digest",
            "digest",
            "--out",
            str(self.dir / "pkg"),
        ]

        code, _, err = self.run_main(*common)
        self.assertEqual(code, 1)
        self.assertIn("/rust/b.html", err)

        crawl.write_text(
            event("/rust/a.html", "id-a") + "\n" + event("/rust/b.html", "id-b") + "\n"
        )
        code, _, _ = self.run_main(*common)
        self.assertEqual(code, 0)
        rows = [
            json.loads(line)
            for line in (self.dir / "pkg" / "judgments.jsonl").read_text().splitlines()
        ]
        self.assertEqual([row["page_id"] for row in rows], ["id-a", "id-b"])
        metadata = json.loads((self.dir / "pkg" / "metadata.json").read_text())
        self.assertEqual(
            (metadata["corpus_snapshot_id"], metadata["build_id"], metadata["model_digest"]),
            ("snap", "gen", "digest"),
        )

    def test_baseline_needs_all_three_modes(self):
        partial = self.write("partial.json", results(keyword=scores(0.8, 0.9, 0.8)))
        code, _, err = self.run_main("baseline", "--results", partial)
        self.assertEqual(code, 1)
        self.assertIn("semantic", err)


if __name__ == "__main__":
    unittest.main()
