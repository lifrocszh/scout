# Databricks notebook source
"""Compare logical analytics results from a clean load and a replayed load."""

import json
import re
from datetime import datetime, timezone

from pyspark.sql import functions as F


RUN_ID = re.compile(r"^run-[a-f0-9]{64}$")
SNAPSHOT_ID = re.compile(r"^snapshot-[a-f0-9]{64}$")
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Snapshot batch IDs identify immutable source content, so keep them and their
# checksums. Only ingestion timestamps vary on a clean load versus a replay.
SNAPSHOT_TABLES = (
    ("scout_ingestion_batches", ("published_at_utc",)),
    ("scout_bronze_records", ("ingested_at_utc",)),
    ("scout_silver_records", ("ingested_at_utc",)),
    ("scout_source_observations", ()),
    ("scout_page_observations", ()),
    ("scout_page_alias_observations", ()),
    ("scout_page_history_evidence", ()),
)

SNAPSHOT_GOLD_VIEWS = (
    ("gold_snapshot_count_report", ("published_at_utc",)),
    ("gold_source_scope_comparisons", ()),
    ("gold_page_change_observations", ()),
    ("gold_page_presence_comparisons", ()),
    ("gold_page_alias_observations", ()),
    ("gold_snapshot_history_count_report", ()),
    ("gold_source_coverage_report", ()),
    ("gold_page_freshness_report", ("observation_age_days",)),
    ("gold_source_page_change_frequency", ()),
    ("gold_crawl_operational_event_summary", ()),
    ("gold_crawl_error_rate_report", ()),
    ("gold_crawl_run_report", ("published_at_utc",)),
    ("gold_crawl_effectiveness_report", ("published_at_utc",)),
)

# Event batch IDs include consumer_id. Two delivery projections can retain
# repeated exact deliveries; compare complete substantive rows after checking
# one version per logical event.
# Preserve event identity, source time, payload hash, and original JSON.
EVENT_TABLES = (
    ("scout_crawl_event_records", (
        "batch_id", "line_number", "disposition", "disposition_reason",
        "manifest_sha256", "records_sha256", "raw_line", "ingested_at_utc",
    )),
    ("scout_crawl_event_identity", ("batch_id", "first_seen_at_utc")),
    ("scout_provisional_capture_observations", ("batch_id",)),
    ("scout_reconciliation_complete_events", ("batch_id", "disposition")),
)

DELIVERY_PROJECTIONS = {
    "scout_crawl_event_records": ("crawl_run_id", "record_id"),
    "scout_reconciliation_complete_events": ("crawl_run_id", "event_id"),
}

# These ages/latencies use query or ingestion time. Keep source event times,
# counts, statuses, and provenance.
EVENT_GOLD_VIEWS = (
    ("gold_provisional_crawl_run_status", (
        "latest_event_age_seconds", "max_event_to_ingestion_latency_seconds",
    )),
    ("gold_provisional_source_capture_summary", ("latest_capture_age_seconds",)),
)

RECONCILIATION_VIEWS = (
    "gold_crawl_snapshot_reconciliation",
    "gold_authoritative_page_observations",
    "gold_authoritative_page_alias_observations",
    "gold_crawl_capture_reconciliation",
    "gold_crawl_rejected_task_observations",
)

dbutils.widgets.text("baseline_catalog", "workspace")
dbutils.widgets.text("baseline_schema", "analytics_baseline")
dbutils.widgets.text("replay_catalog", "workspace")
dbutils.widgets.text("replay_schema", "analytics_replay")
dbutils.widgets.text("crawl_run_id", "")
dbutils.widgets.text("snapshot_id", "")
dbutils.widgets.text("report_uri", "")

baseline_catalog = dbutils.widgets.get("baseline_catalog").strip()
baseline_schema = dbutils.widgets.get("baseline_schema").strip()
replay_catalog = dbutils.widgets.get("replay_catalog").strip()
replay_schema = dbutils.widgets.get("replay_schema").strip()
crawl_run_id = dbutils.widgets.get("crawl_run_id").strip()
snapshot_id = dbutils.widgets.get("snapshot_id").strip()
report_uri = dbutils.widgets.get("report_uri").strip()

spark.conf.set("spark.sql.session.timeZone", "UTC")


def require(condition, message):
    if not condition:
        raise ValueError(message)


for label, value in (
    ("baseline_catalog", baseline_catalog),
    ("baseline_schema", baseline_schema),
    ("replay_catalog", replay_catalog),
    ("replay_schema", replay_schema),
):
    require(IDENTIFIER.fullmatch(value) is not None, f"invalid {label}")
require((baseline_catalog, baseline_schema) != (replay_catalog, replay_schema),
        "baseline and replay must use separate schemas")
require(RUN_ID.fullmatch(crawl_run_id) is not None, "invalid crawl_run_id")
require(SNAPSHOT_ID.fullmatch(snapshot_id) is not None, "invalid snapshot_id")
if report_uri:
    require(report_uri.startswith("dbfs:/Volumes/") and report_uri.endswith(".json")
            and ".." not in report_uri and "\n" not in report_uri,
            "report_uri must be a new .json file in a Unity Catalog Volume")


def qualified(catalog, schema, name):
    for value in (catalog, schema, name):
        require(IDENTIFIER.fullmatch(value) is not None, "invalid table identifier")
    return ".".join(f"`{part}`" for part in (catalog, schema, name))


def scoped_frame(catalog, schema, name, run_id=crawl_run_id, snap_id=snapshot_id):
    frame = spark.table(qualified(catalog, schema, name))
    columns = set(frame.columns)
    if "crawl_run_id" in columns:
        frame = frame.where(F.col("crawl_run_id") == run_id)
    if "run_id" in columns:
        frame = frame.where(F.col("run_id") == run_id)
    if "snapshot_id" in columns:
        frame = frame.where(F.col("snapshot_id") == snap_id)
    return frame


def values(frame, column):
    return [row[0] for row in frame.select(column).distinct().collect()]


def check_batch_gates(catalog, schema):
    event_batches = spark.table(qualified(catalog, schema, "scout_crawl_event_batches"))
    event_runs = values(event_batches, "crawl_run_id")
    require(event_runs == [crawl_run_id],
            f"{catalog}.{schema} must contain only crawl run {crawl_run_id}; found {event_runs}")
    event_summary = event_batches.agg(
        F.count(F.lit(1)).alias("batch_count"),
        F.sum(F.when(F.col("status") != "complete", 1).otherwise(0)).alias("incomplete_count"),
        F.sum(F.when(F.col("expected_record_count") != F.col("ingested_record_count"), 1).otherwise(0)).alias("count_mismatch"),
        F.sum(F.coalesce(F.col("quarantine_count"), F.lit(0))).alias("quarantine_count"),
        F.sum(F.coalesce(F.col("conflict_count"), F.lit(0))).alias("conflict_count"),
        F.sum(F.coalesce(F.col("expected_record_count"), F.lit(0))).alias("expected_record_count"),
        F.sum(F.coalesce(F.col("ingested_record_count"), F.lit(0))).alias("ingested_record_count"),
    ).first().asDict()
    event_summary = {key: int(value or 0) for key, value in event_summary.items()}
    require(event_summary["batch_count"] > 0, f"{catalog}.{schema} has no event batches")
    require(event_summary["incomplete_count"] == 0, f"{catalog}.{schema} has incomplete event batches")
    require(event_summary["count_mismatch"] == 0, f"{catalog}.{schema} has event count mismatches")
    require(event_summary["quarantine_count"] == 0, f"{catalog}.{schema} has quarantined event rows")
    require(event_summary["conflict_count"] == 0, f"{catalog}.{schema} has event identity conflicts")

    snapshot_batches = spark.table(qualified(catalog, schema, "scout_ingestion_batches"))
    snapshot_ids = values(snapshot_batches, "snapshot_id")
    require(snapshot_ids == [snapshot_id],
            f"{catalog}.{schema} must contain only snapshot {snapshot_id}; found {snapshot_ids}")
    snapshot_summary = snapshot_batches.agg(
        F.count(F.lit(1)).alias("batch_count"),
        F.sum(F.when(F.col("status") != "complete", 1).otherwise(0)).alias("incomplete_count"),
        F.countDistinct("batch_id").alias("distinct_batch_count"),
        F.countDistinct("manifest_sha256").alias("manifest_variants"),
        F.countDistinct("records_sha256").alias("records_variants"),
        F.countDistinct("source_snapshot_manifest_sha256").alias("source_manifest_variants"),
        F.countDistinct("crawl_manifest_sha256").alias("crawl_manifest_variants"),
    ).first().asDict()
    snapshot_summary = {key: int(value or 0) for key, value in snapshot_summary.items()}
    require(snapshot_summary["batch_count"] > 0, f"{catalog}.{schema} has no snapshot batch")
    require(snapshot_summary["incomplete_count"] == 0, f"{catalog}.{schema} has incomplete snapshot batches")
    require(snapshot_summary["distinct_batch_count"] == 1
            and snapshot_summary["manifest_variants"] == 1
            and snapshot_summary["records_variants"] == 1
            and snapshot_summary["source_manifest_variants"] == 1
            and snapshot_summary["crawl_manifest_variants"] == 1,
            f"{catalog}.{schema} has conflicting snapshot batch provenance")
    return {"event_batches": event_summary, "snapshot_batches": snapshot_summary}


def assert_authoritative(catalog, schema):
    reconciliation = scoped_frame(catalog, schema, "gold_crawl_snapshot_reconciliation")
    rows = reconciliation.limit(2).collect()
    require(len(rows) == 1,
            f"{catalog}.{schema} must have exactly one reconciliation row for the selected run/snapshot")
    row = rows[0].asDict()
    require(row.get("authority_status") == "authoritative",
            f"{catalog}.{schema} reconciliation is not authoritative: {row.get('authority_status')}")
    require(row.get("metadata_variant_count") == 1 and row.get("manifest_valid") == 1,
            f"{catalog}.{schema} snapshot metadata or manifest gate failed: "
            f"metadata_variant_count={row.get('metadata_variant_count')}, "
            f"manifest_valid={row.get('manifest_valid')}")
    require(row.get("matching_publication_event_count", 0) > 0,
            f"{catalog}.{schema} lacks a matching completed snapshot publication event")
    require(row.get("capture_event_count") == row.get("expected_capture_count")
            and row.get("distinct_task_count") == row.get("capture_event_count")
            and row.get("missing_task_count") == 0,
            f"{catalog}.{schema} capture identity gate failed")

    pages = scoped_frame(catalog, schema, "gold_authoritative_page_observations")
    aliases = scoped_frame(catalog, schema, "gold_authoritative_page_alias_observations")
    captures = scoped_frame(catalog, schema, "gold_crawl_capture_reconciliation")
    rejected = scoped_frame(catalog, schema, "gold_crawl_rejected_task_observations")
    page_count = pages.count()
    alias_count = aliases.count()
    capture_statuses = {item["reconciliation_status"]: int(item["count"])
                        for item in captures.groupBy("reconciliation_status").count().collect()}
    rejected_statuses = {item["reconciliation_status"]: int(item["count"])
                         for item in rejected.groupBy("reconciliation_status").count().collect()}
    capture_count = sum(capture_statuses.values())
    require(page_count == row.get("expected_page_count"),
            f"{catalog}.{schema} authoritative Page count differs from its snapshot manifest")
    require(alias_count == row.get("expected_alias_count"),
            f"{catalog}.{schema} authoritative alias count differs from its snapshot manifest")
    require(capture_count == row.get("expected_capture_count"),
            f"{catalog}.{schema} capture reconciliation row count differs from its Crawl Manifest")
    require(set(capture_statuses) <= {
        "authoritative_admitted", "authoritative_alias", "authoritative_superseded"
    }, f"{catalog}.{schema} has provisional or unmatched capture reconciliation rows")
    require(set(rejected_statuses) <= {"authoritative_rejected_task"},
            f"{catalog}.{schema} has provisional rejected-task rows")
    return {
        "authority_status": row["authority_status"],
        "expected_capture_count": int(row["expected_capture_count"]),
        "capture_reconciliation_rows": capture_count,
        "capture_reconciliation_status_counts": capture_statuses,
        "authoritative_page_rows": page_count,
        "authoritative_alias_rows": alias_count,
        "authoritative_rejected_task_rows": sum(rejected_statuses.values()),
    }


def canonical_rows(frame, excluded):
    columns = set(frame.columns)
    missing = set(excluded) - columns
    require(not missing, f"expected normalization columns are missing: {sorted(missing)}")
    logical_columns = sorted(columns - set(excluded))
    require(logical_columns, "no logical columns remain after normalization")
    return frame.select(*logical_columns), logical_columns


def compare_table(name, excluded=(), run_scoped=True):
    if run_scoped:
        left = scoped_frame(baseline_catalog, baseline_schema, name)
        right = scoped_frame(replay_catalog, replay_schema, name)
    else:
        left = spark.table(qualified(baseline_catalog, baseline_schema, name))
        right = spark.table(qualified(replay_catalog, replay_schema, name))
    left, columns = canonical_rows(left, excluded)
    right, right_columns = canonical_rows(right, excluded)
    require(columns == right_columns, f"{name} logical columns differ between schemas")
    left_types = {field.name: field.dataType.json() for field in left.schema.fields}
    right_types = {field.name: field.dataType.json() for field in right.schema.fields}
    require(left_types == right_types, f"{name} logical column types differ between schemas")

    # Full JSON row strings avoid digest-collision assumptions. Most tables
    # compare as row multisets. Delivery projections can repeat logical events
    # across batches. Validate each identity has one substantive row version,
    # then compare distinct full logical rows for those projections.
    left_json = left.select(F.to_json(F.struct(*[F.col(column) for column in columns]),
                                      options={"ignoreNullFields": "false"}).alias("logical_row"))
    right_json = right.select(F.to_json(F.struct(*[F.col(column) for column in columns]),
                                       options={"ignoreNullFields": "false"}).alias("logical_row"))
    physical_delivery_rows = None
    if name in DELIVERY_PROJECTIONS:
        identity_columns = DELIVERY_PROJECTIONS[name]
        require(set(identity_columns).issubset(columns),
                f"{name} delivery comparison is missing its logical identity columns")
        physical_delivery_rows = {"baseline": left.count(), "replay": right.count()}
        left_deliveries = left.withColumn(
            "logical_row", F.to_json(F.struct(*[F.col(column) for column in columns]),
                                      options={"ignoreNullFields": "false"})
        ).select(*identity_columns, "logical_row")
        right_deliveries = right.withColumn(
            "logical_row", F.to_json(F.struct(*[F.col(column) for column in columns]),
                                      options={"ignoreNullFields": "false"})
        ).select(*identity_columns, "logical_row")
        deliveries = left_deliveries.unionByName(right_deliveries)
        null_identity = " OR ".join(f"`{column}` IS NULL" for column in identity_columns)
        require(deliveries.where(F.expr(null_identity)).limit(1).count() == 0,
                f"{name} delivery comparison found a null logical event identity")
        conflicting_versions = deliveries.groupBy(*identity_columns).agg(
            F.countDistinct("logical_row").alias("substantive_row_versions")
        ).where(F.col("substantive_row_versions") != 1)
        version_sample = [row.asDict() for row in conflicting_versions.limit(3).collect()]
        require(not version_sample,
                f"logical event identity has conflicting substantive deliveries: {version_sample}")
        left_json = left_json.dropDuplicates(["logical_row"])
        right_json = right_json.dropDuplicates(["logical_row"])
    paired = left_json.withColumn("_side", F.lit("baseline")).unionByName(
        right_json.withColumn("_side", F.lit("replay"))
    )
    grouped = paired.groupBy("logical_row").agg(
        F.sum(F.when(F.col("_side") == "baseline", 1).otherwise(0)).alias("baseline_count"),
        F.sum(F.when(F.col("_side") == "replay", 1).otherwise(0)).alias("replay_count"),
    )
    summary = grouped.agg(
        F.sum("baseline_count").alias("baseline_rows"),
        F.sum("replay_count").alias("replay_rows"),
        F.sum(F.when(F.col("baseline_count") != F.col("replay_count"), 1).otherwise(0)).alias("differing_row_values"),
    ).first().asDict()
    baseline_rows = int(summary.get("baseline_rows") or 0)
    replay_rows = int(summary.get("replay_rows") or 0)
    differing = int(summary.get("differing_row_values") or 0)
    result = {
        "table": name,
        "baseline_rows": baseline_rows,
        "replay_rows": replay_rows,
        "differing_row_values": differing,
        "excluded_columns": list(excluded),
        "logical_columns": columns,
        "equal": differing == 0 and baseline_rows == replay_rows,
    }
    if physical_delivery_rows is not None:
        result["delivery_audit"] = {
            "physical_delivery_rows": physical_delivery_rows,
            "logical_distinct_rows": {"baseline": baseline_rows, "replay": replay_rows},
            "extra_physical_deliveries": {
                "baseline": physical_delivery_rows["baseline"] - baseline_rows,
                "replay": physical_delivery_rows["replay"] - replay_rows,
            },
            "identity_columns": list(DELIVERY_PROJECTIONS[name]),
            "identity_version_gate": "one full substantive row per logical event identity",
        }
    if not result["equal"]:
        result["mismatch_sample"] = [row.asDict() for row in grouped.where(
            F.col("baseline_count") != F.col("replay_count")
        ).limit(3).collect()]
    require(result["equal"], f"logical replay mismatch in {name}: {result}")
    return result


baseline_gates = check_batch_gates(baseline_catalog, baseline_schema)
replay_gates = check_batch_gates(replay_catalog, replay_schema)
baseline_authority = assert_authoritative(baseline_catalog, baseline_schema)
replay_authority = assert_authoritative(replay_catalog, replay_schema)
require(baseline_authority == replay_authority,
        "baseline and replay reconciliation authority/count summaries differ")

comparisons = []
for table_name, excluded in SNAPSHOT_TABLES + SNAPSHOT_GOLD_VIEWS:
    comparisons.append(compare_table(table_name, excluded))
for table_name, excluded in EVENT_TABLES + EVENT_GOLD_VIEWS:
    comparisons.append(compare_table(table_name, excluded))
for table_name in RECONCILIATION_VIEWS:
    comparisons.append(compare_table(table_name))

report = {
    "schema_version": "scout.analytics-replay-verification.v1",
    "status": "passed",
    "verified_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    "crawl_run_id": crawl_run_id,
    "snapshot_id": snapshot_id,
    "baseline_schema": f"{baseline_catalog}.{baseline_schema}",
    "replay_schema": f"{replay_catalog}.{replay_schema}",
    "batch_gates": {"baseline": baseline_gates, "replay": replay_gates},
    "authority_gates": {"baseline": baseline_authority, "replay": replay_authority},
    "comparison_count": len(comparisons),
    "comparisons": comparisons,
    "operational_tables_excluded": [
        "scout_crawl_event_batches: IDs vary by consumer; completion/count/quarantine/conflict gates are checked",
        "scout_crawl_event_ingestion_attempts: attempt IDs, timestamps, and resource metrics are execution evidence",
        "gold_crawl_event_ingestion_status: batch-level operational status is covered by event batch gates",
        "gold_ingestion_metrics: ingestion attempt and storage measurements are execution evidence",
    ],
}
report_json = json.dumps(report, sort_keys=True, separators=(",", ":"), default=str)
print(report_json)
if report_uri:
    dbutils.fs.put(report_uri, report_json, overwrite=False)
    print(f"Saved replay verification report to {report_uri}")
dbutils.notebook.exit(report_json)
