"""Data-quality rules. Every rule has a stable name; any failure raises QualityError.

Input rules run on a staged snapshot or event set before anything is written,
so a broken input lands nothing. Lake rules run over the loaded tables (after
every load, and with ``check``).
"""

from collections import Counter
from dataclasses import dataclass
from datetime import timedelta

import pyarrow as pa

from . import tables as t
from .transform import (CRAWL_EVENT_SCHEMA_VERSION, EVENT_FIELDS, SNAPSHOT_RECORD_FIELDS,
                        EVENT_TIME_FIELD, parse_event, parse_utc)

MAX_EXAMPLES = 5


@dataclass(frozen=True)
class CheckResult:
    rule: str
    passed: bool
    detail: str = ""

    def line(self):
        return f"{'PASS' if self.passed else 'FAIL'} {self.rule}" + (f": {self.detail}" if self.detail else "")


class QualityError(Exception):
    def __init__(self, results):
        self.results = results
        failed = [result for result in results if not result.passed]
        super().__init__("data-quality check failed:\n" + "\n".join(f"  {r.line()}" for r in failed))


def result(rule, problems):
    problems = list(problems)
    if not problems:
        return CheckResult(rule, True)
    shown = "; ".join(str(problem) for problem in problems[:MAX_EXAMPLES])
    more = f" (+{len(problems) - MAX_EXAMPLES} more)" if len(problems) > MAX_EXAMPLES else ""
    return CheckResult(rule, False, f"{len(problems)} problem(s): {shown}{more}")


def enforce(results):
    if any(not item.passed for item in results):
        raise QualityError(results)
    return results


def _empty(value):
    return value is None or value == ""


def rule_schema_types(rows):
    problems = []
    for spec, items in rows.items():
        try:
            pa.Table.from_pylist(items, schema=spec.schema)
        except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError, ValueError) as error:
            problems.append(f"{spec.qualified}: {error}")
    return result("schema.types", problems)


def rule_not_null(rows):
    problems = []
    for spec, items in rows.items():
        required = [f.name for f in spec.schema if not f.nullable]
        for item in items:
            missing = [name for name in required if _empty(item.get(name))]
            if missing:
                key = ",".join(str(item.get(k)) for k in spec.keys)
                problems.append(f"{spec.qualified}[{key}] null {','.join(missing)}")
    return result("not_null.required_columns", problems)


def rule_unique_keys(rows):
    problems = []
    for spec, items in rows.items():
        counts = Counter(tuple(item.get(key) for key in spec.keys) for item in items)
        problems += [f"{spec.qualified} key {key} appears {n}x" for key, n in counts.items() if n > 1]
    return result("unique.keys", problems)


def rule_schema_drift(records):
    problems = []
    for record_type, expected in SNAPSHOT_RECORD_FIELDS.items():
        seen = set()
        for record in records.get(record_type, []):
            seen |= set(record)
            missing = expected - set(record)
            if missing:
                problems.append(f"{record_type} missing {sorted(missing)}")
                break
        extra = seen - expected
        if extra:
            problems.append(f"{record_type} has new field(s) {sorted(extra)}")
    return result("schema.drift", problems)


def rule_counts_match_snapshot(snapshot, rows):
    snap = rows[t.SILVER_SNAPSHOTS][0]
    manifest = snapshot.manifest
    problems = []
    for spec, field in ((t.SILVER_PAGES, "page_count"), (t.SILVER_PASSAGES, "passage_count"),
                        (t.SILVER_PAGE_ALIASES, "alias_count")):
        actual = len(rows[spec])
        for label, expected in (("snapshot manifest", snap[field]), ("batch manifest", manifest.get(field))):
            if actual != expected:
                problems.append(f"{spec.qualified} has {actual} rows, {label} {field} = {expected}")
    return result("counts.match_snapshot", problems)


def rule_referential(rows):
    pages = {row["page_id"] for row in rows[t.SILVER_PAGES]}
    sources = {row["source_id"] for row in rows[t.SILVER_SOURCES]}
    problems = [f"page {row['page_id']} has unknown source {row['source_id']}"
                for row in rows[t.SILVER_PAGES] if row["source_id"] not in sources]
    problems += [f"passage {row['passage_id']} references missing page {row['page_id']}"
                 for row in rows[t.SILVER_PASSAGES] if row["page_id"] not in pages]
    problems += [f"alias {row['alias_page_id']} references missing page {row['representative_page_id']}"
                 for row in rows[t.SILVER_PAGE_ALIASES] if row["representative_page_id"] not in pages]
    return result("referential.pages", problems)


def rule_near_duplicate_lineage(snapshot, rows):
    report = snapshot.near_duplicates
    if report is None:
        return CheckResult("lineage.near_duplicates", True, "no report (optional)")
    problems = []
    if report.get("schema_version") != "scout.near-duplicates.v1":
        problems.append(f"unsupported schema {report.get('schema_version')!r}")
    if report.get("snapshot_id") != snapshot.snapshot_id or report.get("run_id") != snapshot.run_id:
        problems.append("report belongs to another snapshot or run")
    per_origin = Counter(row["origin"] for row in rows[t.SILVER_PAGES])
    for origin in report.get("origins", []):
        if per_origin.get(origin.get("origin"), 0) != origin.get("page_count"):
            problems.append(f"{origin.get('origin')} page_count {origin.get('page_count')} "
                            f"!= {per_origin.get(origin.get('origin'), 0)} snapshot Pages")
    return result("lineage.near_duplicates", problems)


def rule_immutable_snapshot(snapshot, lake):
    existing = t.read(lake, t.SILVER_SNAPSHOTS).to_pylist()
    problems = [f"{row['snapshot_id']} already loaded with snapshot manifest {row['snapshot_manifest_sha256']}"
                for row in existing
                if row["snapshot_id"] == snapshot.snapshot_id
                and row["snapshot_manifest_sha256"] != snapshot.manifest["source_snapshot_manifest_sha256"]]
    return result("immutable.snapshot", problems)


def check_snapshot(snapshot, rows, records, lake):
    """Input rules for one snapshot. Raises QualityError on any failure."""
    return enforce([
        rule_schema_drift(records),
        rule_schema_types(rows),
        rule_not_null(rows),
        rule_unique_keys(rows),
        rule_counts_match_snapshot(snapshot, rows),
        rule_referential(rows),
        rule_near_duplicate_lineage(snapshot, rows),
        rule_immutable_snapshot(snapshot, lake),
    ])


def _event_problems(record):
    try:
        event_type, data = parse_event(record)
    except ValueError as error:  # json.JSONDecodeError is a ValueError
        return [f"{record.transport_ref}: {error}"]
    fields = EVENT_FIELDS.get(event_type)
    if fields is None:
        return [f"{record.transport_ref}: unknown event_type {event_type!r}"]
    problems = []
    for name, kind in fields.items():
        value = data.get(name)
        if _empty(value) or not isinstance(value, kind) or (kind is int and isinstance(value, bool)):
            problems.append(f"{record.transport_ref}: {event_type}.{name} missing or not {kind.__name__}")
    extra = set(data) - set(fields)
    if extra:
        problems.append(f"{record.transport_ref}: {event_type} has new field(s) {sorted(extra)}")
    if data.get("schema_version") != CRAWL_EVENT_SCHEMA_VERSION:
        problems.append(f"{record.transport_ref}: unsupported schema_version {data.get('schema_version')!r}")
    try:
        parse_utc(data.get(EVENT_TIME_FIELD[event_type]))
    except (TypeError, ValueError, AttributeError):
        problems.append(f"{record.transport_ref}: {EVENT_TIME_FIELD[event_type]} is not an RFC 3339 timestamp")
    declared = record.declared
    for name, actual in (("event_id", data.get("event_id")), ("event_type", event_type),
                         ("crawl_run_id", data.get("crawl_run_id")),
                         ("payload_sha256", record.payload_sha256)):
        if declared.get(name) not in (None, actual):
            problems.append(f"{record.transport_ref}: envelope {name} does not match the payload")
    return problems


def check_events(records, lake):
    """Input rules for crawl events. Exact redeliveries are fine; conflicting ones are not."""
    schema_problems = [problem for record in records for problem in _event_problems(record)]
    results = [result("event.schema", schema_problems)]
    if schema_problems:
        return enforce(results)
    seen = {(row["crawl_run_id"], row["event_id"]): row["payload_sha256"]
            for row in t.read(lake, t.BRONZE_CRAWL_EVENTS)
            .select(["crawl_run_id", "event_id", "payload_sha256"]).to_pylist()}
    conflicts = []
    for record in records:
        _, data = parse_event(record)
        key = (data["crawl_run_id"], data["event_id"])
        if seen.setdefault(key, record.payload_sha256) != record.payload_sha256:
            conflicts.append(f"{record.transport_ref}: event {key[1]} redelivered with a different payload")
    results.append(result("event.identity", conflicts))
    return enforce(results)


LAKE_RULES = {
    "unique.page_per_snapshot": """
        SELECT snapshot_id || ' ' || page_id || ' x' || COUNT(*) FROM silver_pages
        GROUP BY snapshot_id, page_id HAVING COUNT(*) > 1""",
    "unique.passage_per_snapshot": """
        SELECT snapshot_id || ' ' || passage_id || ' x' || COUNT(*) FROM silver_passages
        GROUP BY snapshot_id, passage_id HAVING COUNT(*) > 1""",
    "unique.event": """
        SELECT crawl_run_id || ' ' || event_id || ' x' || COUNT(*) FROM silver_crawl_events
        GROUP BY crawl_run_id, event_id HAVING COUNT(*) > 1""",
    "counts.silver_match_snapshot": """
        SELECT s.snapshot_id || ': ' || kind || ' ' || actual || ' rows, manifest ' || expected
        FROM silver_snapshots AS s,
        LATERAL (
            SELECT 'pages' AS kind, s.page_count AS expected,
                   (SELECT COUNT(*) FROM silver_pages AS p WHERE p.snapshot_id = s.snapshot_id) AS actual
            UNION ALL
            SELECT 'passages', s.passage_count,
                   (SELECT COUNT(*) FROM silver_passages AS p WHERE p.snapshot_id = s.snapshot_id)
            UNION ALL
            SELECT 'aliases', s.alias_count,
                   (SELECT COUNT(*) FROM silver_page_aliases AS a WHERE a.snapshot_id = s.snapshot_id)
        ) AS c
        WHERE c.actual <> c.expected""",
    "counts.events_match_snapshot": """
        SELECT e.snapshot_id || ': published event says ' || e.page_count || '/' || e.passage_count
               || '/' || e.alias_count || ' pages/passages/aliases, snapshot has '
               || s.page_count || '/' || s.passage_count || '/' || s.alias_count
        FROM silver_crawl_events AS e JOIN silver_snapshots AS s USING (snapshot_id)
        -- Snapshot IDs are content-derived, so another run may publish the same snapshot:
        -- compare the counts, not the run.
        WHERE e.event_type = 'corpus_snapshot_published'
          AND (e.page_count <> s.page_count OR e.passage_count <> s.passage_count
               OR e.alias_count <> s.alias_count)""",
    "lineage.every_snapshot_traced": """
        SELECT s.snapshot_id FROM silver_snapshots AS s
        WHERE NOT EXISTS (SELECT 1 FROM lineage_loads AS l
                          WHERE l.table_name = 'silver_snapshots' AND l.snapshot_id = s.snapshot_id)""",
}


def check_lake(con, as_of, freshness_sla_hours):
    """Lake rules over a DuckDB connection from query.connect(). Raises on failure."""
    results = [result(rule, [row[0] for row in con.execute(sql).fetchall()])
               for rule, sql in LAKE_RULES.items()]
    if freshness_sla_hours is not None:
        cutoff = as_of - timedelta(hours=freshness_sla_hours)
        stale = con.execute(
            "SELECT origin || ' last snapshot ' || strftime(latest_snapshot_at_utc, '%Y-%m-%dT%H:%M:%SZ') "
            "FROM gold_origin_freshness WHERE latest_snapshot_at_utc < ? ORDER BY origin",
            [cutoff]).fetchall()
        results.append(result(f"freshness.sla_{freshness_sla_hours:g}h", [row[0] for row in stale]))
    return enforce(results)
