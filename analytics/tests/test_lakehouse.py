#!/usr/bin/env python3
"""Local lakehouse tests over the committed fixture crawl (analytics/fixtures).

Run from the repository root: python3 -B analytics/tests/test_lakehouse.py
Needs analytics/requirements.txt.
"""

import contextlib
import hashlib
import io
import json
import shutil
import sys
import tempfile
import unittest
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from analytics.lakehouse import __main__ as cli  # noqa: E402
from analytics.lakehouse import fixture, load, quality, query, sources, transform  # noqa: E402
from analytics.lakehouse import tables as t  # noqa: E402


def counts(lake):
    return {spec.qualified: t.read(lake, spec).num_rows for spec in t.ALL_TABLES}


def versions(lake):
    return {spec.qualified: t.version(lake, spec) for spec in t.ALL_TABLES}


def rows(lake, sql):
    con = query.connect(lake)
    return con.sql(sql).to_arrow_table().to_pylist()


def failed_rules(error):
    return {result.rule for result in error.results if not result.passed}


def write_event_batch(directory, run_id, payloads, consumer_id="lakehouse-test", quarantine=0):
    """A published scout.databricks-crawl-event-batch.v1 directory, as `scout events export` writes."""
    schema = "scout.databricks-crawl-event-batch.v1"
    max_records, max_bytes = 128, 64 * 1024 * 1024
    entries = []
    for payload in payloads:
        event = json.loads(payload)
        entries.append(
            (
                "crawl_event",
                event["data"]["event_id"],
                event["event_type"],
                event["data"].get("source_id"),
                payload,
            )
        )
    for number in range(quarantine):
        entries.append(
            (
                "quarantine",
                f"broken-{number}",
                "capture_committed",
                None,
                json.dumps({"reason": "unsupported or invalid crawl event payload"}),
            )
        )
    record_ids = [entry[1] for entry in entries]
    identity = "\0".join(
        (schema, run_id, consumer_id, str(max_records), str(max_bytes), "\0".join(record_ids))
    )
    batch_id = "batch-" + hashlib.sha256(identity.encode()).hexdigest()
    lines = []
    for record_type, record_id, event_type, source_id, record_json in entries:
        lines.append(
            json.dumps(
                {
                    "schema_version": schema,
                    "batch_id": batch_id,
                    "crawl_run_id": run_id,
                    "record_type": record_type,
                    "record_id": record_id,
                    "source_id": source_id,
                    "event_type": event_type,
                    "event_schema_version": 1,
                    "event_created_at_utc": "2026-11-18T00:00:00Z",
                    "payload_sha256": hashlib.sha256(record_json.encode()).hexdigest()
                    if record_type == "crawl_event"
                    else None,
                    "record_json": record_json,
                },
                separators=(",", ":"),
            )
        )
    records = ("\n".join(lines) + "\n").encode()
    manifest = json.dumps(
        {
            "schema_version": schema,
            "batch_id": batch_id,
            "crawl_run_id": run_id,
            "consumer_id": consumer_id,
            "records_file": "records.jsonl",
            "records_sha256": hashlib.sha256(records).hexdigest(),
            "records_bytes": len(records),
            "record_count": len(entries),
            "quarantine_count": quarantine,
            "max_records": max_records,
            "max_batch_bytes": max_bytes,
        },
        indent=2,
    ).encode()
    batch = Path(directory) / run_id / batch_id
    batch.mkdir(parents=True)
    (batch / "records.jsonl").write_bytes(records)
    (batch / "batch-manifest.json").write_bytes(manifest)
    (batch / "_PUBLISHED").write_text(hashlib.sha256(manifest).hexdigest() + "\n", encoding="ascii")
    return batch


class LakehouseTest(unittest.TestCase):
    def setUp(self):
        self.work = tempfile.TemporaryDirectory(prefix="scout-lakehouse-test-")
        self.lake = str(Path(self.work.name) / "lake")

    def tearDown(self):
        self.work.cleanup()


class FixtureQueries(unittest.TestCase):
    """One fixture load, shared by the read-only assertions."""

    @classmethod
    def setUpClass(cls):
        cls.work = tempfile.TemporaryDirectory(prefix="scout-lakehouse-test-")
        cls.lake = str(Path(cls.work.name) / "lake")
        cls.snapshots = fixture.snapshot_ids()
        cls.reports = fixture.load_all(
            cls.lake, freshness_sla_hours=24, as_of=fixture.fixture_as_of()
        )

    @classmethod
    def tearDownClass(cls):
        cls.work.cleanup()

    def test_every_check_passes_on_the_fixture(self):
        for report in self.reports:
            self.assertTrue(report.checks)
            self.assertTrue(all(check.passed for check in report.checks), report.lines())

    def test_silver_rows_match_the_snapshot_manifests(self):
        found = rows(
            self.lake,
            "SELECT snapshot_id, page_count, passage_count, alias_count FROM silver_snapshots",
        )
        self.assertEqual({row["snapshot_id"] for row in found}, set(self.snapshots))
        for row in found:
            manifest = json.loads(
                (
                    fixture.DATA_DIR / "corpora" / row["snapshot_id"] / "snapshot-manifest.json"
                ).read_text(encoding="utf-8")
            )
            self.assertEqual(
                (row["page_count"], row["passage_count"], row["alias_count"]),
                (manifest["page_count"], manifest["passage_count"], manifest["alias_count"]),
            )

    def test_change_rate_between_the_two_crawls(self):
        found = {
            row["source_id"]: row
            for row in rows(
                self.lake,
                "SELECT * FROM gold_snapshot_changes WHERE previous_snapshot_id IS NOT NULL",
            )
        }
        docs, reference = found["docs"], found["reference"]
        # docs: upgrade removed, configure-legacy added, index and configure edited, install unchanged.
        self.assertEqual(
            (docs["added"], docs["removed"], docs["changed"], docs["unchanged"]), (1, 1, 2, 1)
        )
        self.assertAlmostEqual(docs["change_rate"], 2 / 3)
        self.assertAlmostEqual(docs["churn_rate"], 4 / 5)
        # reference: cli.html became an alias of the identical cli-mirror.html; index edited.
        self.assertEqual(
            (
                reference["added"],
                reference["removed"],
                reference["changed"],
                reference["unchanged"],
            ),
            (1, 1, 1, 1),
        )
        self.assertEqual(docs["previous_snapshot_id"], self.snapshots[0])
        self.assertEqual(docs["snapshot_id"], self.snapshots[1])

    def test_duplicate_rates_per_origin(self):
        found = {
            row["origin"]: row
            for row in rows(
                self.lake,
                f"SELECT * FROM gold_duplicate_rates WHERE snapshot_id = '{self.snapshots[1]}'",
            )
        }
        docs, reference = found["http://127.0.0.1:18111"], found["http://127.0.0.1:18112"]
        self.assertEqual((docs["pages"], docs["near_duplicate_pages"]), (4, 1))
        self.assertAlmostEqual(docs["near_duplicate_rate"], 0.25)
        self.assertEqual(reference["exact_duplicate_aliases"], 1)
        self.assertAlmostEqual(reference["exact_duplicate_rate"], 1 / 4)

    def test_freshness_by_origin(self):
        found = rows(self.lake, "SELECT * FROM gold_origin_freshness ORDER BY origin")
        self.assertEqual(
            [row["origin"] for row in found], ["http://127.0.0.1:18111", "http://127.0.0.1:18112"]
        )
        for row in found:
            self.assertEqual(row["latest_snapshot_id"], self.snapshots[1])
            self.assertEqual(row["snapshot_count"], 2)
            self.assertIsNotNone(row["latest_capture_at_utc"])

    def test_example_queries_run(self):
        con = query.connect(self.lake)
        for path in query.example_files():
            with self.subTest(path.name):
                self.assertGreater(
                    con.sql(path.read_text(encoding="utf-8")).to_arrow_table().num_rows, 0
                )

    def test_lineage_names_run_and_snapshot_of_every_version(self):
        lineage = rows(self.lake, "SELECT * FROM lineage_loads")
        traced = {(row["table_name"], row["table_version"]) for row in lineage}
        for spec in t.ALL_TABLES:
            if spec is t.LINEAGE_LOADS or not t.exists(self.lake, spec):
                continue
            for version in range(t.version(self.lake, spec) + 1):
                self.assertIn((spec.qualified, version), traced)
        snapshot_rows = [row for row in lineage if row["table_name"] == "silver_snapshots"]
        self.assertEqual({row["snapshot_id"] for row in snapshot_rows}, set(self.snapshots))
        for row in snapshot_rows:
            run_id = json.loads(
                (
                    fixture.DATA_DIR / "corpora" / row["snapshot_id"] / "snapshot-manifest.json"
                ).read_text(encoding="utf-8")
            )["run_id"]
            self.assertEqual(row["run_id"], run_id)
            self.assertTrue(row["batch_id"].startswith("batch-"))
        gold = [row for row in lineage if row["table_name"].startswith("gold_")]
        self.assertTrue(gold and all("silver_pages@" in row["input_ref"] for row in gold))

    def test_delta_commits_carry_lineage_metadata(self):
        latest = t.history(self.lake, t.SILVER_PAGES)[0]
        self.assertEqual(latest["scout.snapshot_id"], self.snapshots[1])
        self.assertTrue(latest["scout.run_id"].startswith("run-"))


class Idempotency(LakehouseTest):
    def test_reloading_snapshots_and_events_adds_nothing(self):
        fixture.load_all(self.lake)
        before_counts, before_versions = counts(self.lake), versions(self.lake)
        again = fixture.load_all(self.lake)
        self.assertEqual([report.inserted for report in again], [0, 0, 0])
        self.assertEqual(counts(self.lake), before_counts)
        self.assertEqual(versions(self.lake), before_versions)

    def test_redelivered_events_are_loaded_once(self):
        lines = fixture.EVENTS.read_text(encoding="utf-8").splitlines()
        repeated = Path(self.work.name) / "events.jsonl"
        repeated.write_text("\n".join(lines + lines[:5]) + "\n", encoding="utf-8")
        load.load_events(self.lake, sources.JsonlEventSource(repeated), str(repeated))
        self.assertEqual(t.read(self.lake, t.SILVER_CRAWL_EVENTS).num_rows, len(lines))
        load.load_events(self.lake, sources.JsonlEventSource(fixture.EVENTS), "again")
        self.assertEqual(t.read(self.lake, t.BRONZE_CRAWL_EVENTS).num_rows, len(lines))

    def test_snapshot_batch_and_data_dir_inputs_are_the_same_rows(self):
        snapshot_id = fixture.snapshot_ids()[0]
        exports = Path(self.work.name) / "exports"
        batch = sources.snapshot_batch.export_snapshot(fixture.DATA_DIR, snapshot_id, exports)
        run_id = json.loads(
            (fixture.DATA_DIR / "corpora" / snapshot_id / "snapshot-manifest.json").read_text(
                encoding="utf-8"
            )
        )["run_id"]
        report_path = fixture.DATA_DIR / "crawls" / run_id / "near-duplicates.json"
        first = load.load_snapshot(self.lake, sources.snapshot_from_batch(batch, report_path))
        self.assertGreater(first.inserted, 0)
        second = load.load_snapshot(
            self.lake, sources.snapshot_from_data_dir(fixture.DATA_DIR, snapshot_id)
        )
        self.assertEqual(second.inserted, 0)

    def test_event_batches_and_jsonl_are_the_same_rows(self):
        by_run = {}
        for line in fixture.EVENTS.read_text(encoding="utf-8").splitlines():
            by_run.setdefault(json.loads(line)["data"]["crawl_run_id"], []).append(line)
        batches = Path(self.work.name) / "crawl-event-batches"
        for run_id, payloads in by_run.items():
            write_event_batch(batches, run_id, payloads, quarantine=1)
        report = load.load_events(self.lake, sources.EventBatchSource(batches), str(batches))
        total = sum(len(payloads) for payloads in by_run.values())
        self.assertEqual(
            report.inserted, 2 * total
        )  # Bronze and Silver; the quarantine row is skipped
        self.assertEqual(t.read(self.lake, t.SILVER_CRAWL_EVENTS).num_rows, total)
        transports = set(t.read(self.lake, t.BRONZE_CRAWL_EVENTS).column("transport").to_pylist())
        self.assertEqual(transports, {"event_batch"})
        again = load.load_events(self.lake, sources.JsonlEventSource(fixture.EVENTS), "jsonl")
        self.assertEqual(again.inserted, 0)

    def test_real_outbox_export_and_consume_output_are_the_same_rows(self):
        # analytics/fixtures/outbox: `scout events export` batches and `scout events consume`
        # output of one distributed crawl (coordinator, worker, Postgres) of the v2 site. That
        # run reproduced the fixture's v2 snapshot ID, so its published event must match it.
        fixture.load_all(self.lake)
        outbox = fixture.FIXTURES / "outbox"
        first = load.load_events(
            self.lake, sources.EventBatchSource(outbox / "crawl-event-batches"), "batches"
        )
        self.assertEqual(first.written["silver_crawl_events"][1], 10)
        again = load.load_events(
            self.lake, sources.JsonlEventSource(outbox / "events.jsonl"), "jsonl"
        )
        self.assertEqual(again.inserted, 0)
        published = rows(
            self.lake,
            "SELECT crawl_run_id, snapshot_id FROM silver_crawl_events "
            "WHERE event_type = 'corpus_snapshot_published'",
        )
        self.assertEqual(len({row["crawl_run_id"] for row in published}), 3)
        self.assertEqual({row["snapshot_id"] for row in published}, set(fixture.snapshot_ids()))

    def test_gold_is_rebuilt_after_a_load_that_stopped_before_gold(self):
        fixture.load_all(self.lake)
        expected = t.read(self.lake, t.GOLD_SNAPSHOT_CHANGES).sort_by("source_id").to_pylist()
        for spec in t.GOLD_TABLES:
            shutil.rmtree(spec.path(self.lake))
        reports = fixture.load_all(self.lake)
        self.assertEqual(
            [report.inserted for report in reports if "silver_pages" in report.written], []
        )
        self.assertIn("gold_snapshot_changes", reports[0].written)
        self.assertEqual(
            t.read(self.lake, t.GOLD_SNAPSHOT_CHANGES).sort_by("source_id").to_pylist(), expected
        )

    def test_lineage_is_rebuilt_from_delta_commit_metadata(self):
        fixture.load_all(self.lake)
        expected = sorted(
            (row["table_name"], row["table_version"], row["snapshot_id"] or "")
            for row in t.read(self.lake, t.LINEAGE_LOADS).to_pylist()
        )
        shutil.rmtree(t.LINEAGE_LOADS.path(self.lake))
        load.sync_lineage(self.lake)
        found = sorted(
            (row["table_name"], row["table_version"], row["snapshot_id"] or "")
            for row in t.read(self.lake, t.LINEAGE_LOADS).to_pylist()
        )
        self.assertEqual(found, expected)


class BrokenInputs(LakehouseTest):
    def broken(self, kind):
        return fixture.broken_copy(kind, Path(self.work.name) / kind)

    def test_schema_drift_fails_and_lands_nothing_for_that_snapshot(self):
        data_dir, events, as_of = self.broken("schema-drift")
        with self.assertRaises(quality.QualityError) as caught:
            fixture.load_all(self.lake, data_dir, events, as_of=as_of)
        self.assertEqual(failed_rules(caught.exception), {"schema.drift"})
        self.assertIn("fetched_at_utc", str(caught.exception))
        loaded = set(t.read(self.lake, t.SILVER_SNAPSHOTS).column("snapshot_id").to_pylist())
        self.assertEqual(loaded, {fixture.snapshot_ids()[0]})
        broken_id = fixture.snapshot_ids()[1]
        self.assertNotIn(
            broken_id,
            set(t.read(self.lake, t.BRONZE_SNAPSHOT_RECORDS).column("snapshot_id").to_pylist()),
        )

    def test_conflicting_event_redelivery_fails(self):
        data_dir, events, as_of = self.broken("event-conflict")
        with self.assertRaises(quality.QualityError) as caught:
            fixture.load_all(self.lake, data_dir, events, as_of=as_of)
        self.assertEqual(failed_rules(caught.exception), {"event.identity"})
        self.assertEqual(t.read(self.lake, t.BRONZE_CRAWL_EVENTS).num_rows, 0)

    def test_event_against_an_already_loaded_payload_conflicts(self):
        load.load_events(self.lake, sources.JsonlEventSource(fixture.EVENTS), "first")
        event = json.loads(fixture.EVENTS.read_text(encoding="utf-8").splitlines()[0])
        event["data"]["task_id"] += 1000
        changed = Path(self.work.name) / "changed.jsonl"
        changed.write_text(json.dumps(event) + "\n", encoding="utf-8")
        with self.assertRaises(quality.QualityError) as caught:
            load.load_events(self.lake, sources.JsonlEventSource(changed), "second")
        self.assertEqual(failed_rules(caught.exception), {"event.identity"})

    def test_event_missing_a_required_field_fails(self):
        data_dir, events, as_of = self.broken("event-schema")
        with self.assertRaises(quality.QualityError) as caught:
            fixture.load_all(self.lake, data_dir, events, as_of=as_of)
        self.assertEqual(failed_rules(caught.exception), {"event.schema"})
        self.assertIn("capture_committed.source_id", str(caught.exception))

    def test_unknown_event_type_and_bad_json_fail(self):
        bad = Path(self.work.name) / "bad.jsonl"
        bad.write_text('{"event_type":"page_deleted","data":{}}\nnot json\n', encoding="utf-8")
        with self.assertRaises(quality.QualityError) as caught:
            load.load_events(self.lake, sources.JsonlEventSource(bad), "bad")
        self.assertIn("unknown event_type", str(caught.exception))
        self.assertIn("bad.jsonl:2", str(caught.exception))

    def test_stale_origins_fail_the_freshness_sla(self):
        fixture.load_all(self.lake)
        _, _, as_of = self.broken("stale")
        with self.assertRaises(quality.QualityError) as caught:
            quality.check_lake(query.connect(self.lake), as_of, 24)
        self.assertEqual(failed_rules(caught.exception), {"freshness.sla_24h"})
        self.assertIn("http://127.0.0.1:18111", str(caught.exception))
        # Within the SLA the same lake passes.
        quality.check_lake(query.connect(self.lake), fixture.fixture_as_of(), 24)

    def test_published_event_counts_must_match_the_snapshot(self):
        fixture.load_all(self.lake)
        lines = fixture.EVENTS.read_text(encoding="utf-8").splitlines()
        events = [json.loads(line) for line in lines]
        published = next(
            event for event in events if event["event_type"] == "corpus_snapshot_published"
        )
        published["data"]["page_count"] += 1
        published["data"]["event_id"] = "event-lying-about-counts"
        lying = Path(self.work.name) / "lying.jsonl"
        lying.write_text(json.dumps(published) + "\n", encoding="utf-8")
        with self.assertRaises(quality.QualityError) as caught:
            load.load_events(self.lake, sources.JsonlEventSource(lying), "lying")
        self.assertEqual(failed_rules(caught.exception), {"counts.events_match_snapshot"})

    def test_a_snapshot_id_cannot_change_its_manifest(self):
        snapshot = sources.snapshot_from_data_dir(fixture.DATA_DIR, fixture.snapshot_ids()[0])
        load.load_snapshot(self.lake, snapshot)
        altered = replace(
            snapshot, manifest=dict(snapshot.manifest, source_snapshot_manifest_sha256="f" * 64)
        )
        with self.assertRaises(quality.QualityError) as caught:
            load.load_snapshot(self.lake, altered)
        self.assertIn("immutable.snapshot", failed_rules(caught.exception))

    def test_corrupt_snapshot_is_rejected_by_the_shared_validator(self):
        data_dir = Path(self.work.name) / "data"
        shutil.copytree(fixture.DATA_DIR, data_dir)
        snapshot_id = fixture.snapshot_ids()[0]
        pages = data_dir / "corpora" / snapshot_id / "pages.jsonl"
        pages.write_text(pages.read_text(encoding="utf-8") + "\n", encoding="utf-8")
        with self.assertRaises(sources.SourceError):
            sources.snapshot_from_data_dir(data_dir, snapshot_id)


class InputRules(unittest.TestCase):
    """Rules on staged rows that the shared batch validator cannot produce on its own."""

    @classmethod
    def setUpClass(cls):
        cls.snapshot = sources.snapshot_from_data_dir(fixture.DATA_DIR, fixture.snapshot_ids()[1])

    def staged(self):
        return transform.snapshot_rows(self.snapshot, t.now_utc())

    def test_duplicate_page_per_snapshot(self):
        rows_, _ = self.staged()
        rows_[t.SILVER_PAGES].append(dict(rows_[t.SILVER_PAGES][0]))
        self.assertFalse(quality.rule_unique_keys(rows_).passed)
        self.assertFalse(quality.rule_counts_match_snapshot(self.snapshot, rows_).passed)

    def test_null_key(self):
        rows_, _ = self.staged()
        rows_[t.SILVER_PAGES][0]["page_id"] = None
        result = quality.rule_not_null(rows_)
        self.assertFalse(result.passed)
        self.assertIn("page_id", result.detail)

    def test_wrong_type(self):
        rows_, _ = self.staged()
        rows_[t.SILVER_PASSAGES][0]["ordinal"] = "first"
        self.assertFalse(quality.rule_schema_types(rows_).passed)

    def test_missing_field_is_drift(self):
        _, records = self.staged()
        del records["passage"][0]["heading_path"]
        result = quality.rule_schema_drift(records)
        self.assertFalse(result.passed)
        self.assertIn("heading_path", result.detail)

    def test_dangling_passage(self):
        rows_, _ = self.staged()
        rows_[t.SILVER_PASSAGES][0]["page_id"] = "0" * 64
        self.assertFalse(quality.rule_referential(rows_).passed)

    def test_near_duplicate_report_of_another_snapshot(self):
        rows_, _ = self.staged()
        other = replace(
            self.snapshot,
            near_duplicates=dict(self.snapshot.near_duplicates, snapshot_id="snapshot-x"),
        )
        self.assertFalse(quality.rule_near_duplicate_lineage(other, rows_).passed)

    def test_origin_matches_rust_origin_key(self):
        self.assertEqual(transform.origin_of("https://docs.rs/a/b?q"), "https://docs.rs")
        self.assertEqual(transform.origin_of("http://127.0.0.1:18111/x"), "http://127.0.0.1:18111")
        self.assertEqual(transform.origin_of("http://[::1]:8080/x"), "http://[::1]:8080")


class Cli(unittest.TestCase):
    def run_cli(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli.main(list(argv))
        return code, out.getvalue(), err.getvalue()

    def test_demo_passes(self):
        code, out, err = self.run_cli("demo")
        self.assertEqual(code, 0, err)
        self.assertIn("examples/change_rate.sql", out)
        self.assertIn("no new rows", out)

    def test_broken_demo_exits_1(self):
        for kind in fixture.BREAKAGES:
            with self.subTest(kind):
                code, _, err = self.run_cli("demo", "--broken", kind)
                self.assertEqual(code, 1, err)
                self.assertIn("FAIL", err)

    def test_load_check_and_query_commands(self):
        with tempfile.TemporaryDirectory() as work:
            lake = str(Path(work) / "lake")
            self.assertEqual(
                self.run_cli("load-snapshot", "--lake", lake, "--data-dir", str(fixture.DATA_DIR))[
                    0
                ],
                0,
            )
            self.assertEqual(
                self.run_cli("load-events", "--lake", lake, "--jsonl", str(fixture.EVENTS))[0], 0
            )
            as_of = (fixture.fixture_as_of()).isoformat()
            code, out, _ = self.run_cli(
                "check", "--lake", lake, "--freshness-sla-hours", "24", "--as-of", as_of
            )
            self.assertEqual(code, 0)
            self.assertIn("PASS freshness.sla_24h", out)
            stale = (fixture.fixture_as_of() + timedelta(days=3)).isoformat()
            code, _, _ = self.run_cli(
                "check", "--lake", lake, "--freshness-sla-hours", "24", "--as-of", stale
            )
            self.assertEqual(code, 1)
            code, out, _ = self.run_cli(
                "query", "--lake", lake, "--sql", "SELECT COUNT(*) AS pages FROM silver_pages"
            )
            self.assertEqual(code, 0)
            self.assertIn("14", out)
            missing = str(Path(work) / "missing")
            self.assertEqual(
                self.run_cli("load-snapshot", "--lake", lake, "--data-dir", missing)[0], 2
            )


if __name__ == "__main__":
    unittest.main()
