"""Delta table layout, schemas and idempotent writes for the local lakehouse.

Every table lives at ``<lake>/<layer>/<name>`` as a Delta table written with
delta-rs (``deltalake``). Bronze and Silver are insert-only: rows are merged on
their natural key and an existing key is never rewritten, so loading the same
snapshot or event twice adds nothing and creates no new table version. Gold is
derived from Silver and is replaced only when Silver changed. ``_lineage/loads``
records which crawl run, snapshot and batch produced each table version.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
from deltalake import CommitProperties, DeltaTable, write_deltalake

UTC_TS = pa.timestamp("us", tz="UTC")


@dataclass(frozen=True)
class TableSpec:
    layer: str
    name: str
    schema: pa.Schema
    keys: tuple

    @property
    def qualified(self):
        """The DuckDB view name, e.g. ``silver_pages``."""
        return f"{self.layer.lstrip('_')}_{self.name}"

    def path(self, lake):
        return Path(lake) / self.layer / self.name


def schema(*fields):
    return pa.schema([pa.field(name, kind, nullable=nullable) for name, kind, nullable in fields])


BRONZE_SNAPSHOT_RECORDS = TableSpec("bronze", "snapshot_records", schema(
    ("batch_id", pa.string(), False),
    ("snapshot_id", pa.string(), False),
    ("run_id", pa.string(), False),
    ("line_number", pa.int64(), False),
    ("record_type", pa.string(), False),
    ("record_id", pa.string(), False),
    ("source_id", pa.string(), True),
    ("snapshot_manifest_sha256", pa.string(), False),
    ("crawl_manifest_sha256", pa.string(), False),
    ("record_json", pa.string(), False),
    ("loaded_at_utc", UTC_TS, False),
), ("batch_id", "record_type", "record_id"))

BRONZE_CRAWL_EVENTS = TableSpec("bronze", "crawl_events", schema(
    ("crawl_run_id", pa.string(), False),
    ("event_id", pa.string(), False),
    ("event_type", pa.string(), False),
    ("schema_version", pa.int64(), False),
    ("source_id", pa.string(), True),
    ("payload_sha256", pa.string(), False),
    ("payload_json", pa.string(), False),
    ("transport", pa.string(), False),
    ("transport_ref", pa.string(), False),
    ("loaded_at_utc", UTC_TS, False),
), ("crawl_run_id", "event_id"))

SILVER_SNAPSHOTS = TableSpec("silver", "snapshots", schema(
    ("snapshot_id", pa.string(), False),
    ("run_id", pa.string(), False),
    ("batch_id", pa.string(), False),
    ("created_at_utc", UTC_TS, False),
    ("crawl_started_at_utc", UTC_TS, False),
    ("crawl_completed_at_utc", UTC_TS, False),
    ("stop_reason", pa.string(), False),
    ("embedding_model", pa.string(), True),
    ("page_count", pa.int64(), False),
    ("passage_count", pa.int64(), False),
    ("alias_count", pa.int64(), False),
    ("captured_page_count", pa.int64(), False),
    ("failed_count", pa.int64(), False),
    ("rejected_count", pa.int64(), False),
    ("source_config_sha256", pa.string(), False),
    ("snapshot_manifest_sha256", pa.string(), False),
    ("crawl_manifest_sha256", pa.string(), False),
), ("snapshot_id",))

SILVER_SOURCES = TableSpec("silver", "sources", schema(
    ("snapshot_id", pa.string(), False),
    ("source_id", pa.string(), False),
    ("allowed_origins", pa.string(), False),
    ("scope_json", pa.string(), False),
), ("snapshot_id", "source_id"))

SILVER_PAGES = TableSpec("silver", "pages", schema(
    ("snapshot_id", pa.string(), False),
    ("run_id", pa.string(), False),
    ("page_id", pa.string(), False),
    ("source_id", pa.string(), False),
    ("origin", pa.string(), False),
    ("normalized_final_url", pa.string(), False),
    ("title", pa.string(), True),
    ("language", pa.string(), True),
    ("extraction_method", pa.string(), True),
    ("content_chars", pa.int64(), False),
    ("content_sha256", pa.string(), False),
    ("body_sha256", pa.string(), False),
), ("snapshot_id", "page_id"))

SILVER_PASSAGES = TableSpec("silver", "passages", schema(
    ("snapshot_id", pa.string(), False),
    ("passage_id", pa.string(), False),
    ("page_id", pa.string(), False),
    ("source_id", pa.string(), False),
    ("ordinal", pa.int64(), False),
    ("heading_path", pa.string(), False),
    ("content_chars", pa.int64(), False),
    ("content_sha256", pa.string(), False),
), ("snapshot_id", "passage_id"))

SILVER_PAGE_ALIASES = TableSpec("silver", "page_aliases", schema(
    ("snapshot_id", pa.string(), False),
    ("alias_page_id", pa.string(), False),
    ("representative_page_id", pa.string(), False),
    ("source_id", pa.string(), False),
    ("origin", pa.string(), False),
    ("alias_url", pa.string(), False),
    ("alias_kind", pa.string(), True),
), ("snapshot_id", "alias_page_id"))

SILVER_NEAR_DUPLICATE_ORIGINS = TableSpec("silver", "near_duplicate_origins", schema(
    ("snapshot_id", pa.string(), False),
    ("run_id", pa.string(), False),
    ("origin", pa.string(), False),
    ("page_count", pa.int64(), False),
    ("near_duplicate_page_count", pa.int64(), False),
    ("near_duplicate_rate", pa.float64(), False),
    ("exact_duplicate_alias_count", pa.int64(), False),
), ("snapshot_id", "origin"))

SILVER_CRAWL_EVENTS = TableSpec("silver", "crawl_events", schema(
    ("crawl_run_id", pa.string(), False),
    ("event_id", pa.string(), False),
    ("event_type", pa.string(), False),
    ("occurred_at_utc", UTC_TS, False),
    ("source_id", pa.string(), True),
    ("snapshot_id", pa.string(), True),
    ("content_sha256", pa.string(), True),
    ("stop_reason", pa.string(), True),
    ("page_count", pa.int64(), True),
    ("passage_count", pa.int64(), True),
    ("alias_count", pa.int64(), True),
), ("crawl_run_id", "event_id"))

GOLD_ORIGIN_FRESHNESS = TableSpec("gold", "origin_freshness", schema(
    ("origin", pa.string(), True),
    ("latest_snapshot_id", pa.string(), True),
    ("latest_run_id", pa.string(), True),
    ("latest_snapshot_at_utc", UTC_TS, True),
    ("latest_capture_at_utc", UTC_TS, True),
    ("page_count", pa.int64(), True),
    ("snapshot_count", pa.int64(), True),
), ("origin",))

GOLD_SNAPSHOT_CHANGES = TableSpec("gold", "snapshot_changes", schema(
    ("snapshot_id", pa.string(), True),
    ("previous_snapshot_id", pa.string(), True),
    ("source_id", pa.string(), True),
    ("pages", pa.int64(), True),
    ("previous_pages", pa.int64(), True),
    ("added", pa.int64(), True),
    ("removed", pa.int64(), True),
    ("changed", pa.int64(), True),
    ("unchanged", pa.int64(), True),
    ("change_rate", pa.float64(), True),
    ("churn_rate", pa.float64(), True),
), ("snapshot_id", "source_id"))

GOLD_DUPLICATE_RATES = TableSpec("gold", "duplicate_rates", schema(
    ("snapshot_id", pa.string(), True),
    ("origin", pa.string(), True),
    ("pages", pa.int64(), True),
    ("exact_duplicate_aliases", pa.int64(), True),
    ("exact_duplicate_rate", pa.float64(), True),
    ("near_duplicate_pages", pa.int64(), True),
    ("near_duplicate_rate", pa.float64(), True),
), ("snapshot_id", "origin"))

LINEAGE_LOADS = TableSpec("_lineage", "loads", schema(
    ("load_id", pa.string(), False),
    ("loaded_at_utc", UTC_TS, False),
    ("table_name", pa.string(), False),
    ("table_version", pa.int64(), False),
    ("rows_inserted", pa.int64(), False),
    ("input_kind", pa.string(), False),
    ("input_ref", pa.string(), False),
    ("run_id", pa.string(), True),
    ("snapshot_id", pa.string(), True),
    ("batch_id", pa.string(), True),
), ("table_name", "table_version"))

SNAPSHOT_TABLES = (SILVER_SNAPSHOTS, SILVER_SOURCES, SILVER_PAGES, SILVER_PASSAGES,
                   SILVER_PAGE_ALIASES, SILVER_NEAR_DUPLICATE_ORIGINS)
GOLD_TABLES = (GOLD_ORIGIN_FRESHNESS, GOLD_SNAPSHOT_CHANGES, GOLD_DUPLICATE_RATES)
ALL_TABLES = ((BRONZE_SNAPSHOT_RECORDS, BRONZE_CRAWL_EVENTS) + SNAPSHOT_TABLES
              + (SILVER_CRAWL_EVENTS,) + GOLD_TABLES + (LINEAGE_LOADS,))


def now_utc():
    return datetime.now(timezone.utc)


def exists(lake, spec):
    return DeltaTable.is_deltatable(str(spec.path(lake)))


def read(lake, spec):
    """The current rows of a table, or an empty table with its schema."""
    if not exists(lake, spec):
        return spec.schema.empty_table()
    return DeltaTable(str(spec.path(lake))).to_pyarrow_table()


def version(lake, spec):
    return DeltaTable(str(spec.path(lake))).version() if exists(lake, spec) else None


def to_arrow(spec, rows):
    return pa.Table.from_pylist(rows, schema=spec.schema)


def _commit(metadata):
    return CommitProperties(custom_metadata={f"scout.{key}": str(value)
                                             for key, value in metadata.items() if value is not None})


def merge_insert(lake, spec, table, metadata):
    """Insert rows whose key is not in the table yet. Returns (new version or None, rows inserted)."""
    if table.num_rows == 0:
        return None, 0
    path = str(spec.path(lake))
    if not exists(lake, spec):
        write_deltalake(path, table, mode="append", schema_mode=None, commit_properties=_commit(metadata))
        return DeltaTable(path).version(), table.num_rows
    target = DeltaTable(path)
    before = target.version()
    predicate = " AND ".join(f"t.{key} = s.{key}" for key in spec.keys)
    result = (target.merge(table, predicate=predicate, source_alias="s", target_alias="t",
                           commit_properties=_commit(metadata))
              .when_not_matched_insert_all().execute())
    inserted = int(result.get("num_target_rows_inserted", 0))
    after = DeltaTable(path).version()
    return (after if after != before else None), inserted


def overwrite(lake, spec, table, metadata):
    path = str(spec.path(lake))
    write_deltalake(path, table, mode="overwrite", commit_properties=_commit(metadata))
    return DeltaTable(path).version()


def history(lake, spec):
    if not exists(lake, spec):
        return []
    return DeltaTable(str(spec.path(lake))).history()
