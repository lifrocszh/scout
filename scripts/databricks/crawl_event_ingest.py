# Databricks notebook source
import hashlib
import json
import os
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import functions as F


SCHEMA = "scout.databricks-crawl-event-batch.v1"
MAX_BATCH_BYTES = 64 * 1024 * 1024
MAX_RECORDS = 100_000
HEX_256 = r"^[a-f0-9]{64}$"
EVENT_TYPES = {"capture_committed", "crawl_completed", "corpus_snapshot_published"}

dbutils.widgets.text("batch_id", "")
dbutils.widgets.text("remote_batch_path", "")
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("volume", "scout_exports")

batch_id = dbutils.widgets.get("batch_id")
remote_batch_path = dbutils.widgets.get("remote_batch_path")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
attempt_id = uuid.uuid4().hex
started_clock = time.perf_counter()
started_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
spark.conf.set("spark.sql.session.timeZone", "UTC")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def quote(value):
    return "`" + value.replace("`", "``") + "`"


def timestamp(value, label):
    require(isinstance(value, str) and value.endswith("Z"), f"{label} must be an RFC 3339 UTC timestamp ending in Z")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        raise ValueError(f"{label} is not a valid RFC 3339 UTC timestamp")
    require(parsed.utcoffset() == timedelta(0), f"{label} must be UTC")
    return value


def artifact(data, label):
    value = data.get(label)
    require(isinstance(value, dict), f"{label} must be an object")
    require(isinstance(value.get("uri"), str) and value["uri"], f"{label}.uri is required")
    require(isinstance(value.get("sha256"), str) and re.fullmatch(HEX_256, value["sha256"]),
            f"{label}.sha256 is malformed")
    size = value.get("byte_size")
    require(isinstance(size, int) and not isinstance(size, bool) and size >= 0,
            f"{label}.byte_size must be a non-negative integer")
    return value


def validate_event(envelope, manifest):
    if envelope.get("record_type") != "crawl_event":
        raise ValueError("unsupported record_type")
    if envelope.get("schema_version") != SCHEMA or envelope.get("batch_id") != manifest["batch_id"]:
        raise ValueError("event envelope schema or batch identity mismatch")
    if envelope.get("crawl_run_id") != manifest["crawl_run_id"]:
        raise ValueError("event envelope crosses crawl_run_id")
    event_type = envelope.get("event_type")
    if event_type not in EVENT_TYPES:
        raise ValueError("unsupported event_type")
    schema_version = envelope.get("event_schema_version")
    if isinstance(schema_version, bool) or not isinstance(schema_version, int) or schema_version != 1:
        raise ValueError("unsupported event schema version")
    if not isinstance(envelope.get("record_id"), str) or not envelope["record_id"]:
        raise ValueError("record_id is required")
    source_id = envelope.get("source_id")
    if source_id is not None and (not isinstance(source_id, str) or not source_id):
        raise ValueError("source_id must be null or a non-empty string")
    raw = envelope.get("record_json")
    if not isinstance(raw, str):
        raise ValueError("record_json must be a string")
    payload_sha = envelope.get("payload_sha256")
    if payload_sha is not None and (not isinstance(payload_sha, str) or not re.fullmatch(HEX_256, payload_sha)):
        raise ValueError("payload_sha256 is malformed")
    if payload_sha is not None and sha256(raw.encode("utf-8")) != payload_sha:
        raise ValueError("event payload checksum mismatch")
    payload = json.loads(raw)
    require(isinstance(payload, dict), "event payload must be an object")
    require(payload.get("event_type") == event_type, "event payload type differs from envelope")
    data = payload.get("data")
    require(isinstance(data, dict), "event payload data must be an object")
    require(data.get("event_id") == envelope["record_id"], "event ID differs from envelope")
    require(data.get("crawl_run_id") == manifest["crawl_run_id"], "event run ID differs from batch")
    require(isinstance(data.get("schema_version"), int) and not isinstance(data.get("schema_version"), bool)
            and data["schema_version"] == 1, "event data schema version is unsupported")
    if event_type == "capture_committed":
        require(isinstance(data.get("source_id"), str) and data["source_id"], "capture source_id is required")
        require(source_id == data["source_id"], "capture Source differs from envelope")
        timestamp(data.get("committed_at_utc"), "committed_at_utc")
        for name in ("result_sha256", "content_sha256"):
            require(isinstance(data.get(name), str) and re.fullmatch(HEX_256, data[name]), f"{name} is malformed")
        body = artifact(data, "body_artifact")
        return {
            "record_id": envelope["record_id"], "source_id": source_id,
            "event_type": event_type, "event_created_at_utc": timestamp(
                envelope.get("event_created_at_utc"), "event_created_at_utc"),
            "committed_at_utc": data["committed_at_utc"],
            "result_sha256": data["result_sha256"], "content_sha256": data["content_sha256"],
            "body_artifact_uri": body["uri"], "body_artifact_sha256": body["sha256"],
            "body_artifact_bytes": body["byte_size"], "payload_sha256": payload_sha, "record_json": raw,
        }
    if source_id is not None:
        raise ValueError("run-level event must not name a Source")
    if event_type == "crawl_completed":
        timestamp(data.get("completed_at_utc"), "completed_at_utc")
        require(isinstance(data.get("stop_reason"), str) and data["stop_reason"], "stop_reason is required")
        require(isinstance(data.get("config_sha256"), str) and re.fullmatch(HEX_256, data["config_sha256"]),
                "config_sha256 is malformed")
    else:
        timestamp(data.get("published_at_utc"), "published_at_utc")
        require(isinstance(data.get("snapshot_id"), str) and data["snapshot_id"], "snapshot_id is required")
        artifact(data, "snapshot_manifest")
        artifact(data, "crawl_manifest")
        for name in ("page_count", "alias_count", "passage_count"):
            value = data.get(name)
            require(isinstance(value, int) and not isinstance(value, bool) and value >= 0,
                    f"{name} must be a non-negative integer")
    timestamp(envelope.get("event_created_at_utc"), "event_created_at_utc")
    return {"record_id": envelope["record_id"], "source_id": None, "event_type": event_type,
            "event_created_at_utc": envelope["event_created_at_utc"],
            "payload_sha256": payload_sha, "record_json": raw}


def raw_record(envelope, line_number, raw_line, disposition, reason=None):
    return {"batch_id": batch_id, "line_number": line_number, "record_id": envelope.get("record_id"),
            "record_type": envelope.get("record_type"), "event_type": envelope.get("event_type"),
            "event_schema_version": envelope.get("event_schema_version"), "source_id": envelope.get("source_id"),
            "event_created_at_utc": envelope.get("event_created_at_utc"),
            "payload_sha256": envelope.get("payload_sha256"), "record_json": envelope.get("record_json"),
            "raw_line": raw_line.decode("utf-8", "replace"), "disposition": disposition,
            "disposition_reason": reason, "manifest_sha256": manifest_sha,
            "records_sha256": manifest["records_sha256"]}


for label, value in (("catalog", catalog), ("schema", schema), ("volume", volume)):
    require(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value) is not None, f"invalid {label} identifier")
require(re.fullmatch(r"batch-[a-f0-9]{64}", batch_id) is not None, "invalid batch_id")
volume_uri = f"dbfs:/Volumes/{catalog}/{schema}/{volume}"
upload_root = f"{volume_uri}/scout/crawl-events/{batch_id}/uploads/"
require(re.fullmatch(re.escape(upload_root) + r"[a-f0-9]{32}", remote_batch_path) is not None,
        "remote_batch_path is outside the expected immutable batch upload directory")
driver_root = "/Volumes/" + remote_batch_path.removeprefix("dbfs:/Volumes/")
manifest_path = os.path.join(driver_root, "batch-manifest.json")
records_path = os.path.join(driver_root, "records.jsonl")
published_path = os.path.join(driver_root, "_PUBLISHED")
require(set(os.listdir(driver_root)) == {"batch-manifest.json", "records.jsonl", "_PUBLISHED"},
        "remote event batch is incomplete or contains unexpected files")
require(os.path.getsize(manifest_path) <= 1024 * 1024, "event batch manifest exceeds 1 MiB")
manifest_bytes = Path(manifest_path).read_bytes()
manifest_sha = sha256(manifest_bytes)
manifest = json.loads(manifest_bytes)
require(isinstance(manifest, dict), "event batch manifest must be an object")
require(Path(published_path).read_text(encoding="ascii") == manifest_sha + "\n",
        "publication marker does not match event batch manifest")
records_bytes = os.path.getsize(records_path)
require(manifest.get("schema_version") == SCHEMA, "unsupported event batch schema version")
require(manifest.get("batch_id") == batch_id, "event batch identity mismatch")
require(isinstance(manifest.get("crawl_run_id"), str)
        and re.fullmatch(r"run-[a-f0-9]{64}", manifest["crawl_run_id"]), "invalid crawl_run_id")
require(manifest.get("records_file") == "records.jsonl", "unsupported event batch payload file")
require(isinstance(manifest.get("consumer_id"), str) and manifest["consumer_id"], "invalid consumer_id")
max_records = manifest.get("max_records")
max_bytes = manifest.get("max_batch_bytes")
record_count = manifest.get("record_count")
require(all(isinstance(value, int) and not isinstance(value, bool) for value in
            (max_records, max_bytes, record_count)), "event batch bounds/count must be integers")
require(1 <= max_records <= MAX_RECORDS and 1 <= max_bytes <= MAX_BATCH_BYTES,
        "unsupported event batch bounds")
require(1 <= record_count <= min(max_records, MAX_RECORDS), "event batch exceeds record limit")
require(0 < records_bytes <= min(max_bytes, MAX_BATCH_BYTES), "event batch exceeds byte limit")
require(manifest.get("records_bytes") == records_bytes, "event batch records byte count mismatch")
require(manifest.get("records_sha256") and re.fullmatch(HEX_256, manifest["records_sha256"]),
        "event batch records checksum is malformed")

raw_rows = []
valid_events = []
quarantined = []
seen = {}
duplicate_count = 0
conflict_count = 0
raw_quarantine_count = 0
with open(records_path, "rb") as records_stream:
    for line_number, raw_line in enumerate(records_stream, start=1):
        if not raw_line.strip():
            quarantined.append({"line_number": line_number, "record_id": None,
                                "reason": "empty JSONL record", "raw_line": raw_line.decode("utf-8", "replace")})
            continue
        envelope = None
        try:
            envelope = json.loads(raw_line)
            if not isinstance(envelope, dict):
                raise ValueError("event envelope must be an object")
            identity = envelope.get("record_id")
            if not isinstance(identity, str) or not identity:
                identity = "invalid-" + sha256(raw_line)[:32]
            signature = sha256(raw_line)
            raw_quarantine_count += envelope.get("record_type") == "quarantine"
            if identity in seen:
                if seen[identity] == signature:
                    duplicate_count += 1
                    raw_rows.append(raw_record(envelope, line_number, raw_line, "duplicate_in_batch",
                                               "exact duplicate identity in batch"))
                    continue
                conflict_count += 1
                quarantined.append({"line_number": line_number, "record_id": identity,
                                    "reason": "conflicting duplicate identity in one batch",
                                    "raw_line": raw_line.decode("utf-8", "replace")})
                raw_rows.append(raw_record(envelope, line_number, raw_line, "identity_conflict",
                                           "conflicting duplicate identity in batch"))
                continue
            seen[identity] = signature
            event = validate_event(envelope, manifest)
            event.update({"crawl_run_id": manifest["crawl_run_id"], "batch_id": batch_id,
                          "manifest_sha256": manifest_sha, "records_sha256": manifest["records_sha256"],
                          "line_number": line_number, "raw_line": raw_line.decode("utf-8", "replace")})
            valid_events.append(event)
            raw_rows.append(raw_record(envelope, line_number, raw_line, "pending"))
        except Exception as error:
            reason = str(error)[:1000]
            if isinstance(envelope, dict) and envelope.get("record_type") == "quarantine":
                try:
                    reason = json.loads(envelope.get("record_json", "{}")).get("reason", "Rust quarantined unsupported event")
                except (TypeError, json.JSONDecodeError):
                    reason = "Rust quarantine record has invalid reason payload"
            quarantined.append({"line_number": line_number, "record_id": envelope.get("record_id") if isinstance(envelope, dict) else None,
                                "reason": reason, "raw_line": raw_line.decode("utf-8", "replace")})

require(line_number == record_count, "event batch record_count does not match records.jsonl")
expected_quarantine_count = manifest.get("quarantine_count")
require(isinstance(expected_quarantine_count, int) and not isinstance(expected_quarantine_count, bool)
        and expected_quarantine_count == raw_quarantine_count,
        "event batch quarantine_count does not match records.jsonl")
require(sha256(Path(records_path).read_bytes()) == manifest["records_sha256"], "event batch records checksum mismatch")
batch_id_from_records = "batch-" + hashlib.sha256(
    "\0".join((SCHEMA, manifest["crawl_run_id"], manifest["consumer_id"], str(max_records), str(max_bytes),
                "\0".join([row["record_id"] for row in raw_rows]))).encode("utf-8")
).hexdigest()
# Invalid rows stay quarantined; validate the deterministic identity when all manifest records parsed.
if not quarantined and not duplicate_count and not conflict_count:
    require(batch_id_from_records == batch_id, "batch ID is not derived from immutable event identities")
coverage_sha = sha256("\0".join(row["record_id"] for row in raw_rows).encode("utf-8"))

batch_table = ".".join(quote(part) for part in (catalog, schema, "scout_crawl_event_batches"))
records_table = ".".join(quote(part) for part in (catalog, schema, "scout_crawl_event_records"))
quarantine_table = ".".join(quote(part) for part in (catalog, schema, "scout_crawl_event_quarantine"))
identity_table = ".".join(quote(part) for part in (catalog, schema, "scout_crawl_event_identity"))
observations_table = ".".join(quote(part) for part in (catalog, schema, "scout_provisional_capture_observations"))
attempts_table = ".".join(quote(part) for part in (catalog, schema, "scout_crawl_event_ingestion_attempts"))

spark.sql(f"""CREATE TABLE IF NOT EXISTS {batch_table} (
 batch_id STRING, crawl_run_id STRING, consumer_id STRING, schema_version STRING,
 manifest_sha256 STRING, records_sha256 STRING, coverage_sha256 STRING, expected_record_count BIGINT,
 ingested_record_count BIGINT, capture_count BIGINT, terminal_event_count BIGINT,
 quarantine_count BIGINT, duplicate_event_count BIGINT, conflict_count BIGINT,
 status STRING, attempt_count BIGINT, retry_count BIGINT, started_at_utc STRING,
 completed_at_utc STRING, last_error STRING) USING DELTA""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {records_table} (
 batch_id STRING, line_number BIGINT, record_id STRING, crawl_run_id STRING,
 record_type STRING, event_type STRING, event_schema_version BIGINT, source_id STRING,
 event_created_at_utc STRING, payload_sha256 STRING, disposition STRING,
 disposition_reason STRING, manifest_sha256 STRING, records_sha256 STRING,
 record_json STRING, raw_line STRING, ingested_at_utc TIMESTAMP)
 USING DELTA""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {quarantine_table} (
 batch_id STRING, line_number BIGINT, crawl_run_id STRING, record_id STRING,
 reason STRING, raw_record_sha256 STRING, raw_record_bytes BIGINT, raw_record_excerpt STRING,
 raw_record_json STRING, quarantined_at_utc TIMESTAMP) USING DELTA""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {identity_table} (
 crawl_run_id STRING, event_id STRING, event_type STRING, source_id STRING,
 batch_id STRING, event_created_at_utc STRING, payload_sha256 STRING, record_json STRING,
 first_seen_at_utc TIMESTAMP)
 USING DELTA""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {observations_table} (
 crawl_run_id STRING, event_id STRING, source_id STRING, batch_id STRING,
 event_created_at_utc STRING, committed_at_utc STRING, result_sha256 STRING,
 content_sha256 STRING, body_artifact_uri STRING, body_artifact_sha256 STRING,
 body_artifact_bytes BIGINT, record_json STRING) USING DELTA""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {attempts_table} (
 attempt_id STRING, batch_id STRING, crawl_run_id STRING, started_at_utc STRING,
 completed_at_utc STRING, execution_millis BIGINT, processed_bytes BIGINT,
 input_record_count BIGINT, ingested_record_count BIGINT, capture_count BIGINT,
 terminal_event_count BIGINT, quarantine_count BIGINT, duplicate_event_count BIGINT,
 conflict_count BIGINT, events_per_second DOUBLE, bytes_per_second DOUBLE,
 serverless_runtime_seconds BIGINT, capacity_wait_seconds BIGINT,
 driver_memory_bytes BIGINT, executor_memory_bytes BIGINT, cpu_seconds DOUBLE,
 resource_metrics_available BOOLEAN, unavailable_metrics_reason STRING) USING DELTA""")
identity_columns = set(spark.table(identity_table).columns)
if "event_created_at_utc" not in identity_columns:
    spark.sql(f"ALTER TABLE {identity_table} ADD COLUMNS (event_created_at_utc STRING)")
batch_columns = set(spark.table(batch_table).columns)
if "coverage_sha256" not in batch_columns:
    spark.sql(f"ALTER TABLE {batch_table} ADD COLUMNS (coverage_sha256 STRING)")
record_columns = set(spark.table(records_table).columns)
for column in ("disposition", "disposition_reason"):
    if column not in record_columns:
        spark.sql(f"ALTER TABLE {records_table} ADD COLUMNS ({quote(column)} STRING)")

existing_batch = spark.table(batch_table).where(F.col("batch_id") == batch_id).limit(2).collect()
require(len(existing_batch) <= 1, "duplicate crawl-event batch audit identities exist")
if existing_batch:
    require(existing_batch[0]["manifest_sha256"] == manifest_sha
            and existing_batch[0]["records_sha256"] == manifest["records_sha256"],
            "batch_id already exists with conflicting immutable content")

retry = bool(existing_batch and existing_batch[0]["status"] in ("failed", "processing"))
batch_values = spark.createDataFrame([(
    batch_id, manifest["crawl_run_id"], manifest["consumer_id"], SCHEMA, manifest_sha,
    manifest["records_sha256"], coverage_sha, record_count, "processing", started_at,
)], "batch_id STRING, crawl_run_id STRING, consumer_id STRING, schema_version STRING, manifest_sha256 STRING, records_sha256 STRING, coverage_sha256 STRING, expected_record_count BIGINT, status STRING, started_at_utc STRING")
batch_values.createOrReplaceTempView("scout_incoming_event_batch")
spark.sql(f"""MERGE INTO {batch_table} AS target USING scout_incoming_event_batch AS source
ON target.batch_id = source.batch_id
WHEN MATCHED AND target.manifest_sha256 = source.manifest_sha256 THEN UPDATE SET
 target.status = 'processing', target.attempt_count = COALESCE(target.attempt_count, 0) + 1,
 target.retry_count = COALESCE(target.retry_count, 0) + {1 if retry else 0},
 target.coverage_sha256 = source.coverage_sha256, target.records_sha256 = source.records_sha256,
 target.started_at_utc = source.started_at_utc, target.last_error = NULL
WHEN NOT MATCHED THEN INSERT (batch_id, crawl_run_id, consumer_id, schema_version, manifest_sha256,
 records_sha256, coverage_sha256, expected_record_count, status, attempt_count, retry_count, started_at_utc)
VALUES (source.batch_id, source.crawl_run_id, source.consumer_id, source.schema_version,
 source.manifest_sha256, source.records_sha256, source.coverage_sha256, source.expected_record_count, source.status, 1, 0, source.started_at_utc)""")


def quarantine_row(item):
    raw = item["raw_line"]
    raw_bytes = raw.encode("utf-8")
    return {"batch_id": batch_id, "line_number": item["line_number"], "crawl_run_id": manifest["crawl_run_id"],
            "record_id": item.get("record_id"), "reason": item["reason"], "raw_record_sha256": sha256(raw_bytes),
            "raw_record_bytes": len(raw_bytes), "raw_record_excerpt": raw[:2048], "raw_record_json": raw}


try:
    # Logical event identity is (crawl_run_id, event_id). Exact replays remain
    # eligible for observation repair; changed payloads are quarantined.
    event_schema = "crawl_run_id STRING, record_id STRING, source_id STRING, event_type STRING, event_created_at_utc STRING, committed_at_utc STRING, result_sha256 STRING, content_sha256 STRING, body_artifact_uri STRING, body_artifact_sha256 STRING, body_artifact_bytes BIGINT, payload_sha256 STRING, record_json STRING, line_number BIGINT"
    event_rows = [{name: event.get(name) for name in (
        "crawl_run_id", "record_id", "source_id", "event_type", "event_created_at_utc", "committed_at_utc",
        "result_sha256", "content_sha256", "body_artifact_uri", "body_artifact_sha256", "body_artifact_bytes",
        "payload_sha256", "record_json", "line_number")}
        for event in valid_events]
    incoming_events = spark.createDataFrame(event_rows, event_schema) if event_rows else None
    matched = {}
    if incoming_events is not None:
        prior = spark.table(identity_table).alias("prior")
        incoming = incoming_events.alias("incoming")
        prior_rows = prior.join(
            incoming,
            (F.col("prior.crawl_run_id") == F.col("incoming.crawl_run_id"))
            & (F.col("prior.event_id") == F.col("incoming.record_id")),
            "inner",
        ).select("incoming.crawl_run_id", "incoming.record_id",
                 F.col("prior.record_json").alias("prior_json"),
                 F.col("incoming.record_json").alias("incoming_json"))
        for row in prior_rows.collect():
            key = (row["crawl_run_id"], row["record_id"])
            matched.setdefault(key, []).append(row["prior_json"])

    accepted = []
    conflicted_ids = set()
    dispositions = {}
    for event in valid_events:
        key = (event["crawl_run_id"], event["record_id"])
        previous = matched.get(key, [])
        if previous and any(value != event["record_json"] for value in previous):
            conflict_count += 1
            conflicted_ids.add(key)
            quarantined.append({"line_number": event["line_number"], "record_id": event["record_id"],
                                "reason": "event identity conflicts with prior immutable event",
                                "raw_line": event["raw_line"]})
            dispositions[event["line_number"]] = ("identity_conflict", "event identity conflicts with prior immutable event")
        elif previous:
            duplicate_count += 1
            accepted.append(event)
            dispositions[event["line_number"]] = ("duplicate", "exact event identity replay")
        else:
            accepted.append(event)
            dispositions[event["line_number"]] = ("accepted", None)

    for raw_row in raw_rows:
        if raw_row["disposition"] == "pending":
            raw_row["disposition"], raw_row["disposition_reason"] = dispositions.get(
                raw_row["line_number"], ("identity_conflict", "identity not accepted"))

    records_schema = "batch_id STRING, line_number BIGINT, record_id STRING, record_type STRING, event_type STRING, event_schema_version BIGINT, source_id STRING, event_created_at_utc STRING, payload_sha256 STRING, record_json STRING, raw_line STRING, disposition STRING, disposition_reason STRING, manifest_sha256 STRING, records_sha256 STRING"
    if raw_rows:
        incoming_records = spark.createDataFrame(raw_rows, records_schema).withColumn(
            "crawl_run_id", F.lit(manifest["crawl_run_id"])
        ).withColumn("ingested_at_utc", F.current_timestamp())
        incoming_records.createOrReplaceTempView("scout_incoming_event_records")
        existing_records = spark.table(records_table).where(F.col("batch_id") == batch_id).alias("existing")
        incoming_records_alias = incoming_records.alias("incoming")
        existing_conflicts = existing_records.join(
            incoming_records_alias,
            (F.col("existing.batch_id") == F.col("incoming.batch_id"))
            & (F.col("existing.line_number") == F.col("incoming.line_number")),
            "inner",
        ).where(~F.col("existing.raw_line").eqNullSafe(F.col("incoming.raw_line")))
        require(existing_conflicts.limit(1).count() == 0,
                "batch record identity conflicts with previously ingested content")
        spark.sql(f"""MERGE INTO {records_table} AS target USING scout_incoming_event_records AS source
ON target.batch_id = source.batch_id AND target.line_number = source.line_number
WHEN MATCHED AND target.raw_line = source.raw_line THEN UPDATE SET
 target.disposition = source.disposition, target.disposition_reason = source.disposition_reason
WHEN NOT MATCHED THEN INSERT (batch_id, line_number, record_id, crawl_run_id, record_type,
 event_type, event_schema_version, source_id, event_created_at_utc, payload_sha256,
 disposition, disposition_reason, manifest_sha256, records_sha256, record_json, raw_line, ingested_at_utc)
VALUES (source.batch_id, source.line_number, source.record_id, source.crawl_run_id, source.record_type,
 source.event_type, source.event_schema_version, source.source_id, source.event_created_at_utc,
 source.payload_sha256, source.disposition, source.disposition_reason, source.manifest_sha256,
 source.records_sha256, source.record_json, source.raw_line, source.ingested_at_utc)""")

    if accepted:
        accepted_rows = [row for row in event_rows if (row["crawl_run_id"], row["record_id"]) not in conflicted_ids]
        incoming_identity = spark.createDataFrame(accepted_rows, event_schema).select(
            "crawl_run_id", F.col("record_id").alias("event_id"), "event_type", "source_id",
            F.lit(batch_id).alias("batch_id"), "event_created_at_utc", "payload_sha256", "record_json",
            F.current_timestamp().alias("first_seen_at_utc"),
        )
        incoming_identity.createOrReplaceTempView("scout_incoming_event_identity")
        spark.sql(f"""MERGE INTO {identity_table} AS target USING scout_incoming_event_identity AS source
ON target.crawl_run_id = source.crawl_run_id AND target.event_id = source.event_id
WHEN NOT MATCHED THEN INSERT (crawl_run_id, event_id, event_type, source_id, batch_id,
 event_created_at_utc, payload_sha256, record_json, first_seen_at_utc)
VALUES (source.crawl_run_id, source.event_id, source.event_type, source.source_id,
 source.batch_id, source.event_created_at_utc, source.payload_sha256, source.record_json,
 source.first_seen_at_utc)""")
        identity_conflicts_after_merge = spark.table(identity_table).alias("target").join(
            incoming_identity.alias("source"),
            (F.col("target.crawl_run_id") == F.col("source.crawl_run_id"))
            & (F.col("target.event_id") == F.col("source.event_id")),
            "inner",
        ).where(~F.col("target.record_json").eqNullSafe(F.col("source.record_json")))
        require(identity_conflicts_after_merge.limit(1).count() == 0,
                "batch cannot complete: accepted identity conflicts with immutable identity ledger")

    if quarantined:
        quarantine_rows = [quarantine_row(item) for item in quarantined]
        quarantine_schema = "batch_id STRING, line_number BIGINT, crawl_run_id STRING, record_id STRING, reason STRING, raw_record_sha256 STRING, raw_record_bytes BIGINT, raw_record_excerpt STRING, raw_record_json STRING"
        incoming_quarantine = spark.createDataFrame(quarantine_rows, quarantine_schema).withColumn(
            "quarantined_at_utc", F.current_timestamp()
        )
        incoming_quarantine.createOrReplaceTempView("scout_incoming_event_quarantine")
        spark.sql(f"""MERGE INTO {quarantine_table} AS target USING scout_incoming_event_quarantine AS source
ON target.batch_id = source.batch_id AND target.line_number = source.line_number
WHEN NOT MATCHED THEN INSERT *""")

    captures = [event for event in accepted if event["event_type"] == "capture_committed"]
    if captures:
        records = [{"crawl_run_id": event["crawl_run_id"], "event_id": event["record_id"],
                    "source_id": event["source_id"], "batch_id": batch_id,
                    "event_created_at_utc": event["event_created_at_utc"],
                    "committed_at_utc": event.get("committed_at_utc"),
                    "result_sha256": event.get("result_sha256"), "content_sha256": event.get("content_sha256"),
                    "body_artifact_uri": event.get("body_artifact_uri"),
                    "body_artifact_sha256": event.get("body_artifact_sha256"),
                    "body_artifact_bytes": event.get("body_artifact_bytes"), "record_json": event["record_json"]}
                   for event in captures]
        capture_schema = "crawl_run_id STRING, event_id STRING, source_id STRING, batch_id STRING, event_created_at_utc STRING, committed_at_utc STRING, result_sha256 STRING, content_sha256 STRING, body_artifact_uri STRING, body_artifact_sha256 STRING, body_artifact_bytes BIGINT, record_json STRING"
        incoming_captures = spark.createDataFrame(records, capture_schema)
        incoming_captures.createOrReplaceTempView("scout_incoming_provisional_captures")
        spark.sql(f"""MERGE INTO {observations_table} AS target USING scout_incoming_provisional_captures AS source
ON target.crawl_run_id = source.crawl_run_id AND target.event_id = source.event_id
WHEN NOT MATCHED THEN INSERT *""")
        capture_conflicts = spark.table(observations_table).alias("target").join(
            incoming_captures.alias("source"),
            (F.col("target.crawl_run_id") == F.col("source.crawl_run_id"))
            & (F.col("target.event_id") == F.col("source.event_id")),
            "inner",
        ).where(~F.col("target.record_json").eqNullSafe(F.col("source.record_json")))
        require(capture_conflicts.limit(1).count() == 0,
                "batch cannot complete: provisional capture conflicts with accepted event identity")

    batch_records = spark.table(records_table).where(F.col("batch_id") == batch_id)
    batch_quarantine = spark.table(quarantine_table).where(F.col("batch_id") == batch_id)
    accounted_lines = batch_records.select("line_number").unionByName(
        batch_quarantine.select("line_number")
    ).distinct().count()
    require(accounted_lines == record_count,
            "batch cannot complete: raw and quarantined records do not account for every input line")
    require(batch_records.where(F.col("disposition") == "pending").limit(1).count() == 0,
            "batch cannot complete: raw record has no final disposition")
    missing_quarantine = batch_records.where(F.col("disposition") == "identity_conflict").select(
        "batch_id", "line_number"
    ).join(batch_quarantine.select("batch_id", "line_number"), ["batch_id", "line_number"], "left_anti")
    require(missing_quarantine.limit(1).count() == 0,
            "batch cannot complete: rejected identity lacks a quarantine record")

    processed_bytes = len(manifest_bytes) + records_bytes + len((manifest_sha + "\n").encode("ascii"))
    elapsed_ms = max(1, int((time.perf_counter() - started_clock) * 1000))
    completed_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    attempt = spark.createDataFrame([(
        attempt_id, batch_id, manifest["crawl_run_id"], started_at, completed_at, elapsed_ms,
        processed_bytes, record_count, len(accepted), len(captures),
        sum(event["event_type"] != "capture_committed" for event in accepted), len(quarantined),
        duplicate_count, conflict_count, len(accepted) / (elapsed_ms / 1000.0),
        processed_bytes / (elapsed_ms / 1000.0), None, None, None, None, None, False,
        "serverless runtime, capacity wait, driver/executor memory, and CPU metrics are not exposed to this notebook",
    )], "attempt_id STRING, batch_id STRING, crawl_run_id STRING, started_at_utc STRING, completed_at_utc STRING, execution_millis BIGINT, processed_bytes BIGINT, input_record_count BIGINT, ingested_record_count BIGINT, capture_count BIGINT, terminal_event_count BIGINT, quarantine_count BIGINT, duplicate_event_count BIGINT, conflict_count BIGINT, events_per_second DOUBLE, bytes_per_second DOUBLE, serverless_runtime_seconds BIGINT, capacity_wait_seconds BIGINT, driver_memory_bytes BIGINT, executor_memory_bytes BIGINT, cpu_seconds DOUBLE, resource_metrics_available BOOLEAN, unavailable_metrics_reason STRING")
    attempt.createOrReplaceTempView("scout_incoming_event_ingestion_attempt")
    spark.sql(f"INSERT INTO {attempts_table} SELECT * FROM scout_incoming_event_ingestion_attempt")
    batch_summary = spark.createDataFrame([(
        batch_id, len(accepted), len(captures),
        sum(event["event_type"] != "capture_committed" for event in accepted),
        len(quarantined), duplicate_count, conflict_count, completed_at,
    )], "batch_id STRING, ingested_record_count BIGINT, capture_count BIGINT, terminal_event_count BIGINT, quarantine_count BIGINT, duplicate_event_count BIGINT, conflict_count BIGINT, completed_at_utc STRING")
    batch_summary.createOrReplaceTempView("scout_completed_event_batch")
    spark.sql(f"""MERGE INTO {batch_table} AS target USING scout_completed_event_batch AS source
ON target.batch_id = source.batch_id
WHEN MATCHED THEN UPDATE SET target.ingested_record_count = source.ingested_record_count,
 target.capture_count = source.capture_count, target.terminal_event_count = source.terminal_event_count,
 target.quarantine_count = source.quarantine_count, target.duplicate_event_count = source.duplicate_event_count,
 target.conflict_count = source.conflict_count, target.status = 'complete',
 target.completed_at_utc = source.completed_at_utc, target.last_error = NULL""")
except Exception as error:
    failed_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    error_text = str(error)[:4000].replace("'", "''")
    spark.sql(f"UPDATE {batch_table} SET status = 'failed', completed_at_utc = '{failed_at}', last_error = '{error_text}' WHERE batch_id = '{batch_id}'")
    raise


gold_run = ".".join(quote(part) for part in (catalog, schema, "gold_provisional_crawl_run_status"))
gold_sources = ".".join(quote(part) for part in (catalog, schema, "gold_provisional_source_capture_summary"))
gold_lag = ".".join(quote(part) for part in (catalog, schema, "gold_crawl_event_ingestion_status"))
spark.sql(f"""CREATE OR REPLACE VIEW {gold_run} AS
WITH committed_event_rows AS (
 SELECT r.crawl_run_id, r.record_id AS event_id, r.event_type, r.event_created_at_utc,
        r.record_json, i.first_seen_at_utc
 FROM {records_table} r
 JOIN {batch_table} b ON b.batch_id = r.batch_id AND b.status = 'complete'
 JOIN {identity_table} i ON i.crawl_run_id = r.crawl_run_id AND i.event_id = r.record_id
 WHERE r.record_type = 'crawl_event' AND r.disposition IN ('accepted', 'duplicate')
), event_summary AS (
 SELECT crawl_run_id,
        MAX(CASE WHEN event_type = 'crawl_completed' THEN GET_JSON_OBJECT(record_json, '$.data.completed_at_utc') END) AS crawl_completed_at_utc,
        MAX(CASE WHEN event_type = 'crawl_completed' THEN GET_JSON_OBJECT(record_json, '$.data.stop_reason') END) AS stop_reason,
        MAX(CASE WHEN event_type = 'corpus_snapshot_published' THEN GET_JSON_OBJECT(record_json, '$.data.snapshot_id') END) AS published_snapshot_id,
        MAX(COALESCE(event_created_at_utc,
            CASE event_type WHEN 'capture_committed' THEN GET_JSON_OBJECT(record_json, '$.data.committed_at_utc')
              WHEN 'crawl_completed' THEN GET_JSON_OBJECT(record_json, '$.data.completed_at_utc')
              ELSE GET_JSON_OBJECT(record_json, '$.data.published_at_utc') END)) AS latest_event_created_at_utc,
        MIN(COALESCE(event_created_at_utc,
            CASE event_type WHEN 'capture_committed' THEN GET_JSON_OBJECT(record_json, '$.data.committed_at_utc')
              WHEN 'crawl_completed' THEN GET_JSON_OBJECT(record_json, '$.data.completed_at_utc')
              ELSE GET_JSON_OBJECT(record_json, '$.data.published_at_utc') END)) AS first_event_created_at_utc,
        MAX(UNIX_TIMESTAMP(first_seen_at_utc) - UNIX_TIMESTAMP(TO_TIMESTAMP(COALESCE(event_created_at_utc,
            CASE event_type WHEN 'capture_committed' THEN GET_JSON_OBJECT(record_json, '$.data.committed_at_utc')
              WHEN 'crawl_completed' THEN GET_JSON_OBJECT(record_json, '$.data.completed_at_utc')
              ELSE GET_JSON_OBJECT(record_json, '$.data.published_at_utc') END)))) AS max_event_to_ingestion_latency_seconds
 FROM committed_event_rows GROUP BY crawl_run_id
), captures AS (
 SELECT o.crawl_run_id, COUNT(DISTINCT o.event_id) AS provisional_capture_count,
        COUNT(DISTINCT o.source_id) AS observed_source_count,
        MIN(o.committed_at_utc) AS first_capture_committed_at_utc,
        MAX(o.committed_at_utc) AS latest_capture_committed_at_utc
 FROM {observations_table} o
 WHERE EXISTS (
   SELECT 1 FROM committed_event_rows r
   WHERE r.crawl_run_id = o.crawl_run_id AND r.event_id = o.event_id
     AND r.event_type = 'capture_committed'
 )
 GROUP BY o.crawl_run_id
), quarantine AS (
 SELECT crawl_run_id, COUNT(*) AS quarantined_record_count FROM {quarantine_table} GROUP BY crawl_run_id
), runs AS (
 SELECT DISTINCT crawl_run_id FROM {batch_table}
)
SELECT r.crawl_run_id, COALESCE(c.provisional_capture_count, 0) AS provisional_capture_count,
       COALESCE(c.observed_source_count, 0) AS observed_source_count,
       c.first_capture_committed_at_utc, c.latest_capture_committed_at_utc,
       e.first_event_created_at_utc, e.latest_event_created_at_utc,
       CASE WHEN e.latest_event_created_at_utc IS NOT NULL
            THEN UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(TO_TIMESTAMP(e.latest_event_created_at_utc)) END AS latest_event_age_seconds,
       e.max_event_to_ingestion_latency_seconds,
       e.crawl_completed_at_utc, e.stop_reason, e.published_snapshot_id,
       CASE WHEN e.published_snapshot_id IS NULL THEN 'provisional'
            ELSE 'snapshot_publication_event_seen' END AS evidence_status,
       COALESCE(q.quarantined_record_count, 0) AS quarantined_record_count,
       CAST(NULL AS BIGINT) AS failed_count, CAST(NULL AS BIGINT) AS rejected_count,
       'durable event v1 does not publish fetch failure or rejection totals' AS unavailable_run_error_metrics_reason
FROM runs r LEFT JOIN event_summary e USING (crawl_run_id)
LEFT JOIN captures c USING (crawl_run_id) LEFT JOIN quarantine q USING (crawl_run_id)""")
spark.sql(f"""CREATE OR REPLACE VIEW {gold_sources} AS
WITH committed_capture_rows AS (
 SELECT o.* FROM {observations_table} o
 WHERE EXISTS (
   SELECT 1 FROM {records_table} r
   JOIN {batch_table} b ON b.batch_id = r.batch_id AND b.status = 'complete'
   WHERE r.crawl_run_id = o.crawl_run_id AND r.record_id = o.event_id
     AND r.record_type = 'crawl_event' AND r.event_type = 'capture_committed'
     AND r.disposition IN ('accepted', 'duplicate')
 )
)
SELECT crawl_run_id, source_id, COUNT(DISTINCT event_id) AS provisional_capture_count,
       MIN(committed_at_utc) AS first_capture_committed_at_utc,
       MAX(committed_at_utc) AS latest_capture_committed_at_utc,
       UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(TO_TIMESTAMP(MAX(committed_at_utc))) AS latest_capture_age_seconds,
       'provisional event observations; no Page admission, extraction, or exact deduplication applied' AS observation_basis
FROM committed_capture_rows GROUP BY crawl_run_id, source_id""")
spark.sql(f"""CREATE OR REPLACE VIEW {gold_lag} AS
WITH latest_attempt AS (
 SELECT *, ROW_NUMBER() OVER (PARTITION BY batch_id ORDER BY started_at_utc DESC, attempt_id DESC) AS attempt_rank
 FROM {attempts_table}
)
SELECT b.batch_id, b.crawl_run_id, b.consumer_id, b.status, b.expected_record_count,
       b.ingested_record_count, b.capture_count, b.terminal_event_count, b.quarantine_count,
       b.duplicate_event_count, b.conflict_count, b.attempt_count, b.retry_count,
       b.started_at_utc, b.completed_at_utc, b.last_error,
       CASE WHEN b.status = 'complete' THEN 0 ELSE 1 END AS pending_remote_batch,
       a.execution_millis, a.processed_bytes, a.events_per_second, a.bytes_per_second,
       a.serverless_runtime_seconds, a.capacity_wait_seconds, a.driver_memory_bytes,
       a.executor_memory_bytes, a.cpu_seconds, a.resource_metrics_available,
       a.unavailable_metrics_reason
FROM {batch_table} b LEFT JOIN latest_attempt a ON a.batch_id = b.batch_id AND a.attempt_rank = 1""")

receipt_table = ".".join(quote(part) for part in (catalog, schema, "scout_crawl_event_ingestion_receipts"))
spark.sql(f"""CREATE TABLE IF NOT EXISTS {receipt_table} (
 schema_version STRING, batch_id STRING, consumer_id STRING,
 manifest_sha256 STRING, records_sha256 STRING, coverage_sha256 STRING,
 input_record_count BIGINT, ingested_record_count BIGINT, quarantine_count BIGINT,
 conflict_count BIGINT, status STRING, completed_at_utc STRING) USING DELTA""")
completed_batch = spark.table(batch_table).where(F.col("batch_id") == batch_id).select(
    "crawl_run_id", "consumer_id", "manifest_sha256", "records_sha256", "coverage_sha256",
    "expected_record_count", "ingested_record_count", "quarantine_count", "conflict_count",
    "status", "completed_at_utc",
).first()
require(completed_batch is not None and completed_batch["status"] == "complete",
        "cannot issue ingestion receipt before downstream batch completion")
receipt = {
    "schema_version": "scout.databricks-crawl-event-ingestion-receipt.v1",
    "batch_id": batch_id,
    "consumer_id": completed_batch["consumer_id"],
    "manifest_sha256": completed_batch["manifest_sha256"],
    "records_sha256": completed_batch["records_sha256"],
    "coverage_sha256": completed_batch["coverage_sha256"],
    "input_record_count": completed_batch["expected_record_count"],
    "ingested_record_count": completed_batch["ingested_record_count"],
    "quarantine_count": completed_batch["quarantine_count"],
    "conflict_count": completed_batch["conflict_count"],
    "status": completed_batch["status"],
    "completed_at_utc": completed_batch["completed_at_utc"],
}
receipt_schema = "schema_version STRING, batch_id STRING, consumer_id STRING, manifest_sha256 STRING, records_sha256 STRING, coverage_sha256 STRING, input_record_count BIGINT, ingested_record_count BIGINT, quarantine_count BIGINT, conflict_count BIGINT, status STRING, completed_at_utc STRING"
spark.createDataFrame([tuple(receipt.values())], receipt_schema).createOrReplaceTempView("scout_incoming_event_receipt")
spark.sql(f"""MERGE INTO {receipt_table} AS target USING scout_incoming_event_receipt AS source
ON target.batch_id = source.batch_id AND target.consumer_id = source.consumer_id
WHEN MATCHED AND target.manifest_sha256 = source.manifest_sha256
 AND target.records_sha256 = source.records_sha256 AND target.coverage_sha256 = source.coverage_sha256
THEN UPDATE SET target.status = source.status, target.completed_at_utc = source.completed_at_utc
WHEN NOT MATCHED THEN INSERT *""")
saved_receipt = spark.table(receipt_table).where(
    (F.col("batch_id") == batch_id) & (F.col("consumer_id") == receipt["consumer_id"])
).limit(2).collect()
require(len(saved_receipt) == 1 and saved_receipt[0]["manifest_sha256"] == manifest_sha
        and saved_receipt[0]["records_sha256"] == manifest["records_sha256"]
        and saved_receipt[0]["coverage_sha256"] == coverage_sha,
        "downstream ingestion receipt conflicts with its immutable batch")
dbutils.notebook.exit(json.dumps(receipt, separators=(",", ":")))
