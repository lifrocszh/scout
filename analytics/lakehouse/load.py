"""Load snapshots and crawl events into the lakehouse.

Order of one load:

1. Shape the input into rows and run the input quality rules. A failure raises
   before anything is written.
2. Merge Bronze, then Silver. Merges are insert-only on natural keys, so a
   repeated load inserts nothing and creates no table version.
   ``silver_snapshots`` is written last: once a snapshot is listed there, all
   of its Pages, Passages and aliases are in.
3. Rebuild Gold from the committed SQL when the Silver versions it was built
   from changed (also after a load that stopped between Silver and Gold).
4. Sync ``_lineage/loads`` from the Delta commit metadata and run the lake rules.

Every commit carries ``scout.*`` metadata (load ID, crawl run, snapshot, batch,
input). ``_lineage/loads`` is a projection of that metadata, so a load that
stops after a commit and before its lineage row is repaired by the next sync.
Run one loader per lake at a time.
"""

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

from . import quality, query, tables as t, transform

SNAPSHOT_WRITE_ORDER = (t.BRONZE_SNAPSHOT_RECORDS, t.SILVER_SOURCES, t.SILVER_PAGES,
                        t.SILVER_PASSAGES, t.SILVER_PAGE_ALIASES, t.SILVER_NEAR_DUPLICATE_ORIGINS,
                        t.SILVER_SNAPSHOTS)
GOLD_INPUTS = (t.SILVER_SNAPSHOTS, t.SILVER_PAGES, t.SILVER_PAGE_ALIASES,
               t.SILVER_NEAR_DUPLICATE_ORIGINS, t.SILVER_CRAWL_EVENTS)


@dataclass
class LoadReport:
    input_ref: str
    written: dict = field(default_factory=dict)  # table -> (version, rows inserted)
    checks: list = field(default_factory=list)

    @property
    def inserted(self):
        """Bronze and Silver rows added (Gold is rebuilt, not inserted into)."""
        return sum(rows for name, (_, rows) in self.written.items() if not name.startswith("gold_"))

    def lines(self):
        out = [f"input {self.input_ref}"]
        if not self.written:
            out.append("  no new rows (already loaded)")
        out += [f"  {name}: +{rows} rows -> version {version}"
                for name, (version, rows) in sorted(self.written.items())]
        out += [f"  {check.line()}" for check in self.checks]
        return out


def _merge(lake, spec, rows, metadata, report):
    version, inserted = t.merge_insert(lake, spec, t.to_arrow(spec, rows), metadata)
    if version is not None:
        _, earlier = report.written.get(spec.qualified, (None, 0))
        report.written[spec.qualified] = (version, earlier + inserted)


def load_snapshot(lake, snapshot, freshness_sla_hours=None, as_of=None):
    """Load one SnapshotInput. Idempotent per snapshot batch."""
    loaded_at = t.now_utc()
    rows, records = transform.snapshot_rows(snapshot, loaded_at)
    report = LoadReport(snapshot.input_ref)
    report.checks += quality.check_snapshot(snapshot, rows, records, lake)
    metadata = {"load_id": str(uuid.uuid4()), "input_kind": snapshot.input_kind,
                "input_ref": snapshot.input_ref, "run_id": snapshot.run_id,
                "snapshot_id": snapshot.snapshot_id, "batch_id": snapshot.batch_id}
    for spec in SNAPSHOT_WRITE_ORDER:
        _merge(lake, spec, rows[spec], metadata, report)
    return _finish(lake, report, metadata, freshness_sla_hours, as_of or loaded_at)


def load_events(lake, source, input_ref, freshness_sla_hours=None, as_of=None):
    """Load every event a source yields. Exact redeliveries are dropped."""
    loaded_at = t.now_utc()
    records = list(source.records())
    report = LoadReport(input_ref)
    report.checks += quality.check_events(records, lake)
    unique = {}
    for record in records:
        _, data = transform.parse_event(record)
        unique.setdefault((data["crawl_run_id"], data["event_id"]), record)
    by_run = {}
    for (run_id, _), record in unique.items():
        by_run.setdefault(run_id, []).append(record)
    metadata = {}
    for run_id, run_records in sorted(by_run.items()):
        rows = transform.event_rows(run_records, loaded_at)
        snapshot_ids = sorted({row["snapshot_id"] for row in rows[t.SILVER_CRAWL_EVENTS] if row["snapshot_id"]})
        transports = sorted({record.transport for record in run_records})
        metadata = {"load_id": str(uuid.uuid4()), "input_kind": "events:" + ",".join(transports),
                    "input_ref": input_ref, "run_id": run_id,
                    "snapshot_id": snapshot_ids[0] if len(snapshot_ids) == 1 else None}
        for spec in (t.BRONZE_CRAWL_EVENTS, t.SILVER_CRAWL_EVENTS):
            _merge(lake, spec, rows[spec], metadata, report)
    return _finish(lake, report, metadata, freshness_sla_hours, as_of or loaded_at)


def _finish(lake, report, metadata, freshness_sla_hours, as_of):
    refresh_gold(lake, metadata, report)
    sync_lineage(lake)
    report.checks += quality.check_lake(query.connect(lake), as_of, freshness_sla_hours)
    return report


def _silver_versions(lake):
    return ",".join(f"{spec.qualified}@{t.version(lake, spec)}" for spec in GOLD_INPUTS)


def refresh_gold(lake, metadata, report=None):
    """Rebuild Gold from analytics/sql/gold when its Silver inputs moved."""
    inputs = _silver_versions(lake)
    current = [entry.get("scout.silver_versions") for spec in t.GOLD_TABLES
               for entry in t.history(lake, spec)[:1]]
    if len(current) == len(t.GOLD_TABLES) and all(value == inputs for value in current):
        return False
    con = query.connect(lake)
    # Gold depends on every loaded snapshot and event, not on the load that triggered the
    # refresh, so its lineage names the Silver table versions it was built from.
    gold_metadata = {"load_id": metadata.get("load_id"), "silver_versions": inputs,
                     "input_kind": "gold_refresh", "input_ref": inputs}
    for spec in t.GOLD_TABLES:
        data = con.sql(query.sql_file(f"gold/{spec.name}.sql")).to_arrow_table()
        data = data.select(spec.schema.names).cast(spec.schema)
        version = t.overwrite(lake, spec, data, gold_metadata)
        if report is not None:
            report.written[spec.qualified] = (version, data.num_rows)
    return True


def sync_lineage(lake):
    """Add a lineage row for every scout-tagged table version that lacks one."""
    rows = []
    for spec in t.ALL_TABLES:
        if spec is t.LINEAGE_LOADS:
            continue
        for entry in t.history(lake, spec):
            if "scout.load_id" not in entry:
                continue
            metrics = entry.get("operationMetrics") or {}
            if isinstance(metrics, str):
                metrics = json.loads(metrics)
            inserted = metrics.get("num_target_rows_inserted", metrics.get("num_added_rows", 0))
            rows.append({
                "load_id": entry["scout.load_id"],
                "loaded_at_utc": datetime.fromtimestamp(entry["timestamp"] / 1000, timezone.utc),
                "table_name": spec.qualified, "table_version": entry["version"],
                "rows_inserted": int(inserted or 0),
                "input_kind": entry.get("scout.input_kind", ""),
                "input_ref": entry.get("scout.input_ref", ""),
                "run_id": entry.get("scout.run_id"), "snapshot_id": entry.get("scout.snapshot_id"),
                "batch_id": entry.get("scout.batch_id"),
            })
    if rows:
        t.merge_insert(lake, t.LINEAGE_LOADS, t.to_arrow(t.LINEAGE_LOADS, rows), {})
