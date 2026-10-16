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
from pyspark.sql.types import StringType
from pyspark.sql.window import Window


SCHEMA = "scout.databricks-snapshot-batch.v1"
MAX_BATCH_BYTES = 64 * 1024 * 1024
MAX_RECORDS = 100_000
RECORD_TYPES = ("snapshot", "source", "page", "passage", "page_alias", "crawl_manifest", "operational")
HEX_256 = r"^[a-f0-9]{64}$"
HISTORY_EVIDENCE_SCHEMA = "scout.page-history-evidence.v1"
PAGE_TOMBSTONE_SCHEMA = "scout.page-tombstone.v1"
SOURCE_COVERAGE_SCHEMA = "scout.source-coverage-assertion.v1"
SOURCE_SCOPE_FIELDS = (
    "seeds", "allowed_urls", "allowed_origins", "path_prefixes",
    "deny_path_prefixes", "sitemap_url_prefixes",
)

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
spark.conf.set("spark.sql.session.timeZone", "UTC")
ingestion_started_perf = time.perf_counter()
ingestion_started_at_utc = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
ingestion_attempt_id = uuid.uuid4().hex


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def pretty_json(value):
    return json.dumps(value, ensure_ascii=False, indent=2, separators=(",", ": ")).encode("utf-8")


def quote(value):
    return "`" + value.replace("`", "``") + "`"


def canonical_json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def source_scope_sha256(source):
    scope = {"source_id": source.get("source_id")}
    for field in SOURCE_SCOPE_FIELDS:
        value = source.get(field)
        if field == "allowed_urls" and value is None:
            scope[field] = None
            continue
        if field == "sitemap_url_prefixes" and value is None:
            value = []
        if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
            raise ValueError(f"Source.{field} must be a string array")
        scope[field] = sorted(value)
    return hashlib.sha256(canonical_json(scope).encode("utf-8")).hexdigest()


def extraction_contract_sha256(policy):
    fields = (
        "min_content_chars", "max_decoded_body_bytes", "max_extraction_bytes",
        "max_extraction_millis", "extraction_schema",
    )
    contract = {field: policy.get(field) for field in fields}
    return hashlib.sha256(canonical_json(contract).encode("utf-8")).hexdigest()


def require_utc_timestamp(value, label):
    require(isinstance(value, str) and value.endswith("Z"), f"{label} must be an RFC 3339 UTC timestamp ending in Z")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        raise ValueError(f"{label} is not a valid RFC 3339 UTC timestamp")
    require(parsed.utcoffset() == timedelta(0), f"{label} must be UTC")
    return parsed.astimezone(timezone.utc)


for label, value in (("catalog", catalog), ("schema", schema), ("volume", volume)):
    require(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value) is not None, f"invalid {label} identifier")
require(re.fullmatch(r"batch-[a-f0-9]{64}", batch_id) is not None, "invalid batch_id")
volume_uri = f"dbfs:/Volumes/{catalog}/{schema}/{volume}"
upload_root = f"{volume_uri}/scout/snapshots/{batch_id}/uploads/"
require(re.fullmatch(re.escape(upload_root) + r"[a-f0-9]{32}", remote_batch_path) is not None,
        "remote_batch_path is outside the expected immutable batch upload directory")
driver_root = "/Volumes/" + remote_batch_path.removeprefix("dbfs:/Volumes/")
manifest_path = os.path.join(driver_root, "batch-manifest.json")
records_path = os.path.join(driver_root, "records.jsonl")
published_path = os.path.join(driver_root, "_PUBLISHED")
remote_names = set(os.listdir(driver_root))
required_names = {"batch-manifest.json", "records.jsonl", "_PUBLISHED"}
require(required_names.issubset(remote_names) and remote_names - required_names in (set(), {"history-evidence.json"}),
        "remote batch is incomplete or contains unexpected files")

require(os.path.getsize(manifest_path) <= 1024 * 1024, "batch manifest exceeds 1 MiB")
manifest_bytes = Path(manifest_path).read_bytes()
manifest = json.loads(manifest_bytes)
manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
require(Path(published_path).read_text(encoding="ascii") == manifest_sha + "\n",
        "publication marker does not match batch manifest")
records_bytes = os.path.getsize(records_path)
require(manifest.get("schema_version") == SCHEMA, "unsupported batch schema version")
require(manifest.get("batch_id") == batch_id, "batch identity mismatch")
require(manifest.get("records_file") == "records.jsonl", "unsupported payload file")
require(manifest.get("max_batch_bytes") == MAX_BATCH_BYTES and manifest.get("max_records") == MAX_RECORDS,
        "unsupported batch bounds")
snapshot_id = manifest.get("snapshot_id")
snapshot_manifest_sha = manifest.get("source_snapshot_manifest_sha256")
history_evidence_sha = manifest.get("history_evidence_sha256")
require(isinstance(snapshot_id, str) and re.fullmatch(r"snapshot-[a-f0-9]{64}", snapshot_id),
        "invalid source snapshot identity")
require(isinstance(snapshot_manifest_sha, str) and re.fullmatch(HEX_256, snapshot_manifest_sha),
        "invalid source snapshot checksum")
artifacts = manifest.get("source_artifacts")
required_artifacts = {"snapshot-manifest.json", "crawl-manifest.json", "pages.jsonl", "passages.jsonl", "aliases.jsonl"}
allowed_artifacts = required_artifacts | {"operational-log.jsonl", "history-evidence.json"}
require(isinstance(artifacts, dict) and required_artifacts.issubset(artifacts)
        and set(artifacts).issubset(allowed_artifacts)
        and all(isinstance(value, str) and re.fullmatch(HEX_256, value) for value in artifacts.values()),
        "source_artifacts checksums are malformed")
crawl_manifest_file_sha = manifest.get("crawl_manifest_file_sha256", artifacts.get("crawl-manifest.json"))
crawl_manifest_file_bytes = manifest.get("crawl_manifest_file_bytes")
raw_crawl_identity_present = "crawl_manifest_file_sha256" in manifest
require(artifacts["snapshot-manifest.json"] == snapshot_manifest_sha
        and isinstance(crawl_manifest_file_sha, str) and re.fullmatch(HEX_256, crawl_manifest_file_sha)
        and artifacts["crawl-manifest.json"] == crawl_manifest_file_sha
        and (raw_crawl_identity_present == (crawl_manifest_file_bytes is not None))
        and (crawl_manifest_file_bytes is None
             or (isinstance(crawl_manifest_file_bytes, int) and not isinstance(crawl_manifest_file_bytes, bool)
                 and crawl_manifest_file_bytes >= 0)),
        "source artifact checksums conflict with batch provenance")
history_evidence_path = os.path.join(driver_root, "history-evidence.json")
history_evidence = None
if history_evidence_sha:
    require(isinstance(history_evidence_sha, str) and re.fullmatch(HEX_256, history_evidence_sha),
            "history_evidence_sha256 is malformed")
    require("history-evidence.json" in remote_names, "history evidence checksum has no sidecar artifact")
    history_evidence_bytes = Path(history_evidence_path).read_bytes()
    require(hashlib.sha256(history_evidence_bytes).hexdigest() == history_evidence_sha
            and artifacts.get("history-evidence.json") == history_evidence_sha,
            "history evidence sidecar checksum mismatch")
    history_evidence = json.loads(history_evidence_bytes)
else:
    require("history-evidence.json" not in remote_names and "history-evidence.json" not in artifacts,
            "history evidence sidecar is not declared in the manifest")
history_bytes = os.path.getsize(history_evidence_path) if history_evidence is not None else 0
require(records_bytes + history_bytes <= MAX_BATCH_BYTES
        and records_bytes + history_bytes + len(manifest_bytes) + 65 <= MAX_BATCH_BYTES,
        "batch exceeds the 64 MiB processing limit")
expected_batch_id = "batch-" + hashlib.sha256(f"{SCHEMA}\0{snapshot_id}\0{snapshot_manifest_sha}".encode()).hexdigest()
if raw_crawl_identity_present:
    expected_batch_id = "batch-" + hashlib.sha256(
        f"{SCHEMA}\0{snapshot_id}\0{snapshot_manifest_sha}\0{crawl_manifest_file_sha}".encode()
    ).hexdigest()
if history_evidence_sha:
    identity = f"{SCHEMA}\0{snapshot_id}\0{snapshot_manifest_sha}"
    if raw_crawl_identity_present:
        identity += f"\0{crawl_manifest_file_sha}"
    identity += f"\0{history_evidence_sha}"
    expected_batch_id = "batch-" + hashlib.sha256(identity.encode()).hexdigest()
require(expected_batch_id == batch_id, "batch ID is not derived from this immutable snapshot")
require(manifest.get("records_bytes") == records_bytes and manifest.get("records_sha256") == sha256_file(records_path),
        "records payload size or checksum mismatch")
require(manifest.get("record_count", MAX_RECORDS + 1) <= MAX_RECORDS, "batch exceeds the record limit")

frame = spark.read.json(records_path)
required_columns = {
    "schema_version", "batch_id", "snapshot_id", "snapshot_manifest_sha256",
    "crawl_manifest_sha256", "record_type", "record_id", "source_id", "record_json",
}
require(required_columns.issubset(set(frame.columns)), "records payload is missing envelope fields")
require(frame.count() == manifest["record_count"], "records payload count mismatch")
bad_envelope = frame.where(
    (F.col("schema_version") != SCHEMA)
    | (F.col("batch_id") != batch_id)
    | (F.col("snapshot_id") != snapshot_id)
    | (F.col("snapshot_manifest_sha256") != snapshot_manifest_sha)
    | (F.col("crawl_manifest_sha256") != manifest["crawl_manifest_sha256"])
    | (~F.col("record_type").isin(*RECORD_TYPES))
    | F.col("record_id").isNull()
    | F.col("record_json").isNull()
)
require(bad_envelope.limit(1).count() == 0, "record envelope is malformed or crosses batch lineage")
actual_counts = {row["record_type"]: row["count"] for row in frame.groupBy("record_type").count().collect()}
actual_counts = {record_type: int(actual_counts.get(record_type, 0)) for record_type in RECORD_TYPES}
expected_counts = {record_type: int(manifest.get("record_counts", {}).get(record_type, 0)) for record_type in RECORD_TYPES}
require(actual_counts == expected_counts, "record-type counts do not match batch manifest")

for record_type, field in (("source", "source_id"), ("page", "page_id"), ("passage", "passage_id"), ("page_alias", "alias_page_id")):
    wrong_identity = frame.where(
        (F.col("record_type") == record_type)
        & (F.get_json_object("record_json", f"$.{field}") != F.col("record_id"))
    )
    require(wrong_identity.limit(1).count() == 0, f"{record_type} ID differs from its canonical Scout ID")
wrong_source = frame.where(
    F.col("record_type").isin("page", "passage", "page_alias")
    & (F.get_json_object("record_json", "$.source_id") != F.col("source_id"))
)
require(wrong_source.limit(1).count() == 0, "record Source identity mismatch")
bad_hash = frame.where(
    F.col("record_type").isin("page", "passage")
    & (
        ~F.get_json_object("record_json", "$.content_sha256").rlike(HEX_256)
        | (
            (F.col("record_type") == "page")
            & (F.sha2(F.get_json_object("record_json", "$.content_identity"), 256) != F.get_json_object("record_json", "$.content_sha256"))
        )
        | (
            (F.col("record_type") == "passage")
            & (F.sha2(F.get_json_object("record_json", "$.content"), 256) != F.get_json_object("record_json", "$.content_sha256"))
        )
    )
)
require(bad_hash.limit(1).count() == 0, "Page or Passage content hash mismatch")
bad_canonical_id = frame.where(
    F.col("record_type").isin("page", "passage", "page_alias")
    & (~F.col("record_id").rlike(HEX_256))
)
require(bad_canonical_id.limit(1).count() == 0, "Page, Passage, or alias ID is malformed")
identity_count = frame.select("record_type", "record_id").distinct().count()
require(identity_count == frame.count(), "duplicate record identities in batch")

source_ids = frame.where(F.col("record_type") == "source").select(F.col("record_id").alias("ref_id")).distinct()
page_ids = frame.where(F.col("record_type") == "page").select(F.col("record_id").alias("ref_id")).distinct()
source_refs = frame.where(F.col("record_type").isin("page", "passage", "page_alias")).select(
    F.col("source_id").alias("ref_id")
)
require(source_refs.join(source_ids, "ref_id", "left_anti").limit(1).count() == 0,
        "Page/Passage/alias references an unknown Source")
passage_refs = frame.where(F.col("record_type") == "passage").select(
    F.get_json_object("record_json", "$.page_id").alias("ref_id")
)
alias_refs = frame.where(F.col("record_type") == "page_alias").select(
    F.get_json_object("record_json", "$.representative_page_id").alias("ref_id")
)
require(passage_refs.join(page_ids, "ref_id", "left_anti").limit(1).count() == 0,
        "Passage references an unknown Page")
require(alias_refs.join(page_ids, "ref_id", "left_anti").limit(1).count() == 0,
        "Page alias references an unknown representative Page")
page_source_pairs = frame.where(F.col("record_type") == "page").select(
    F.col("record_id").alias("parent_page_id"), "source_id"
).distinct()
passage_source_pairs = frame.where(F.col("record_type") == "passage").select(
    F.get_json_object("record_json", "$.page_id").alias("parent_page_id"), "source_id"
)
alias_source_pairs = frame.where(F.col("record_type") == "page_alias").select(
    F.get_json_object("record_json", "$.representative_page_id").alias("parent_page_id"), "source_id"
)
require(passage_source_pairs.join(page_source_pairs, ["parent_page_id", "source_id"], "left_anti").limit(1).count() == 0,
        "Passage belongs to a different Source than its Page")
require(alias_source_pairs.join(page_source_pairs, ["parent_page_id", "source_id"], "left_anti").limit(1).count() == 0,
        "Page alias belongs to a different Source than its representative Page")

snapshot_rows = frame.where(F.col("record_type") == "snapshot").select("record_id", "record_json").limit(2).collect()
crawl_rows = frame.where(F.col("record_type") == "crawl_manifest").select("record_id", "record_json").limit(2).collect()
require(len(snapshot_rows) == 1 and snapshot_rows[0]["record_id"] == snapshot_id, "missing or duplicate snapshot record")
require(len(crawl_rows) == 1, "missing or duplicate Crawl Manifest record")
snapshot_record = json.loads(snapshot_rows[0]["record_json"])
crawl_record = json.loads(crawl_rows[0]["record_json"])
require(snapshot_record.get("status") == "complete" and snapshot_record.get("snapshot_id") == snapshot_id,
        "snapshot record is not complete")
require(crawl_record.get("status") == "complete" and crawl_record.get("snapshot_id") == snapshot_id
        and crawl_record.get("manifest_sha256") == manifest["crawl_manifest_sha256"],
        "Crawl Manifest provenance mismatch")
# The exporter verified the original Rust bytes before normalizing records.jsonl.
# The logical digest stays bound through this embedded claim and the immutable batch hashes.
require({item.get("source_id") for item in crawl_record.get("sources", [])} ==
        {row["ref_id"] for row in source_ids.collect()}, "Source records do not match Crawl Manifest")
for field in ("page_count", "passage_count", "alias_count"):
    record_type = {"page_count": "page", "passage_count": "passage", "alias_count": "page_alias"}[field]
    require(snapshot_record.get(field) == actual_counts[record_type] == manifest.get(field),
            f"{field} does not match snapshot records")
    require(crawl_record.get("summary", {}).get(field) == manifest.get(field),
            f"Crawl Manifest {field} does not match snapshot")
require(crawl_record.get("run_id") == manifest.get("run_id"), "Crawl Manifest run_id mismatch")
bad_event = frame.where(
    (F.col("record_type") == "operational")
    & (
        (F.get_json_object("record_json", "$.schema_version") != "scout.operational-event.v1")
        | (F.get_json_object("record_json", "$.run_id") != manifest.get("run_id"))
    )
)
require(bad_event.limit(1).count() == 0, "operational record schema or run_id mismatch")

history_rows = frame.where(
    (F.col("record_type") == "operational")
    & F.get_json_object("record_json", "$.event").startswith("history.")
).select("source_id", "record_id", "record_json").collect()
history_events = []
for row in history_rows:
    event = json.loads(row["record_json"])
    evidence = event.get("data")
    require(isinstance(evidence, dict), "history evidence event data must be an object")
    expected_event = "history.page_tombstone" if evidence.get("schema_version") == PAGE_TOMBSTONE_SCHEMA else "history.source_coverage_assertion"
    require(event.get("event") == expected_event, "unsupported history evidence event type")
    require(row["source_id"] == evidence.get("source_id"), "history evidence Source envelope mismatch")
    expected_record_id = "history-evidence-" + hashlib.sha256(canonical_json(evidence).encode("utf-8")).hexdigest()
    require(row["record_id"] == expected_record_id, "history evidence record_id is not content-derived")
    history_events.append(evidence)
history_events.sort(key=canonical_json)
history_evidence_count = manifest.get("history_evidence_count", 0)
require(isinstance(history_evidence_count, int) and not isinstance(history_evidence_count, bool)
        and history_evidence_count >= 0 and history_evidence_count == len(history_events),
        "history evidence count does not match the operational records")
if history_evidence is None:
    require(not history_events and history_evidence_count == 0, "history evidence records require the sidecar artifact")
else:
    require(history_evidence.get("schema_version") == HISTORY_EVIDENCE_SCHEMA,
            "unsupported history evidence schema_version")
    require(history_evidence.get("snapshot_id") == snapshot_id
            and history_evidence.get("run_id") == manifest.get("run_id"),
            "history evidence run/snapshot lineage mismatch")
    require(history_evidence.get("source_snapshot_manifest_sha256") == snapshot_manifest_sha
            and history_evidence.get("crawl_manifest_sha256") == manifest.get("crawl_manifest_sha256"),
            "history evidence source checksums mismatch")
    evidence_records = history_evidence.get("records")
    require(isinstance(evidence_records, list) and evidence_records,
            "history evidence records must be a non-empty array")
    require(sorted(evidence_records, key=canonical_json) == history_events,
            "history evidence sidecar differs from its operational records")
    sources_by_id = {source.get("source_id"): source for source in crawl_record.get("sources", [])}
    source_scopes = {source_id: source_scope_sha256(source) for source_id, source in sources_by_id.items()}
    current_page_pairs = {
        (row["source_id"], row["record_id"])
        for row in frame.where(F.col("record_type") == "page").select("source_id", "record_id").collect()
    }
    current_page_counts = {
        row["source_id"]: int(row["count"])
        for row in frame.where(F.col("record_type") == "page").groupBy("source_id").count().collect()
    }
    seen_evidence = set()
    summary = crawl_record.get("summary", {})
    for number, evidence in enumerate(evidence_records, start=1):
        schema_version = evidence.get("schema_version")
        require(schema_version in (PAGE_TOMBSTONE_SCHEMA, SOURCE_COVERAGE_SCHEMA),
                f"history evidence record {number} has unsupported schema_version")
        source_id = evidence.get("source_id")
        require(isinstance(source_id, str) and source_id in sources_by_id,
                f"history evidence record {number} references an unknown Source")
        require(evidence.get("snapshot_id") == snapshot_id and evidence.get("run_id") == manifest.get("run_id"),
                f"history evidence record {number} run/snapshot lineage mismatch")
        require(evidence.get("scope_sha256") == source_scopes[source_id],
                f"history evidence record {number} Source scope hash mismatch")
        timestamp_field = "tombstoned_at_utc" if schema_version == PAGE_TOMBSTONE_SCHEMA else "asserted_at_utc"
        timestamp = evidence.get(timestamp_field)
        if schema_version == PAGE_TOMBSTONE_SCHEMA:
            page_id = evidence.get("page_id")
            require(isinstance(page_id, str) and re.fullmatch(HEX_256, page_id)
                    and (source_id, page_id) not in current_page_pairs,
                    f"history evidence record {number} Page ID is malformed or still present")
            identity = (schema_version, source_id, page_id)
        else:
            require(evidence.get("complete") is True,
                    f"history evidence record {number} must explicitly assert complete=true")
            expected_page_count = evidence.get("expected_page_count")
            if expected_page_count is not None:
                require(isinstance(expected_page_count, int) and not isinstance(expected_page_count, bool)
                        and expected_page_count >= 0
                        and expected_page_count == current_page_counts.get(source_id, 0),
                        f"history evidence record {number} expected_page_count mismatch")
            require(summary.get("stop_reason") == "frontier_exhausted"
                    and int(summary.get("failed_count", 0)) == 0
                    and int(summary.get("rejected_count", 0)) == 0,
                    f"history evidence record {number} cannot assert complete coverage for a bounded or errored crawl")
            identity = (schema_version, source_id)
        require_utc_timestamp(timestamp, f"history evidence record {number} timestamp")
        require(identity not in seen_evidence, "duplicate history evidence for the same Source/Page")
        seen_evidence.add(identity)

batch_table = ".".join(quote(part) for part in (catalog, schema, "scout_ingestion_batches"))
bronze_table = ".".join(quote(part) for part in (catalog, schema, "scout_bronze_records"))
silver_table = ".".join(quote(part) for part in (catalog, schema, "scout_silver_records"))
gold_view = ".".join(quote(part) for part in (catalog, schema, "gold_snapshot_count_report"))
source_history_table = ".".join(quote(part) for part in (catalog, schema, "scout_source_observations"))
page_history_table = ".".join(quote(part) for part in (catalog, schema, "scout_page_observations"))
alias_history_table = ".".join(quote(part) for part in (catalog, schema, "scout_page_alias_observations"))
evidence_history_table = ".".join(quote(part) for part in (catalog, schema, "scout_page_history_evidence"))
ingestion_metrics_table = ".".join(quote(part) for part in (catalog, schema, "scout_ingestion_metrics"))
gold_source_comparisons = ".".join(quote(part) for part in (catalog, schema, "gold_source_scope_comparisons"))
gold_page_changes = ".".join(quote(part) for part in (catalog, schema, "gold_page_change_observations"))
gold_page_presence = ".".join(quote(part) for part in (catalog, schema, "gold_page_presence_comparisons"))
gold_alias_observations = ".".join(quote(part) for part in (catalog, schema, "gold_page_alias_observations"))
gold_history_counts = ".".join(quote(part) for part in (catalog, schema, "gold_snapshot_history_count_report"))
history_page_requests_view = ".".join(quote(part) for part in (catalog, schema, "scout_history_page_requests"))
gold_source_coverage = ".".join(quote(part) for part in (catalog, schema, "gold_source_coverage_report"))
gold_page_freshness = ".".join(quote(part) for part in (catalog, schema, "gold_page_freshness_report"))
gold_change_frequency = ".".join(quote(part) for part in (catalog, schema, "gold_source_page_change_frequency"))
gold_operational_summary = ".".join(quote(part) for part in (catalog, schema, "gold_crawl_operational_event_summary"))
gold_crawl_error_rate = ".".join(quote(part) for part in (catalog, schema, "gold_crawl_error_rate_report"))
gold_crawl_run = ".".join(quote(part) for part in (catalog, schema, "gold_crawl_run_report"))
gold_crawl_effectiveness = ".".join(quote(part) for part in (catalog, schema, "gold_crawl_effectiveness_report"))
gold_ingestion_metrics = ".".join(quote(part) for part in (catalog, schema, "gold_ingestion_metrics"))

spark.sql(f"CREATE TABLE IF NOT EXISTS {batch_table} (batch_id STRING, snapshot_id STRING, schema_version STRING, manifest_sha256 STRING, records_sha256 STRING, expected_page_count BIGINT, expected_passage_count BIGINT, expected_alias_count BIGINT, record_count BIGINT, record_counts_json STRING, status STRING, published_at_utc TIMESTAMP) USING DELTA")
spark.sql(f"CREATE TABLE IF NOT EXISTS {bronze_table} (batch_id STRING, snapshot_id STRING, schema_version STRING, manifest_sha256 STRING, records_sha256 STRING, record_type STRING, record_id STRING, source_id STRING, record_json STRING, ingested_at_utc TIMESTAMP) USING DELTA")
spark.sql(f"CREATE TABLE IF NOT EXISTS {silver_table} (batch_id STRING, snapshot_id STRING, schema_version STRING, manifest_sha256 STRING, records_sha256 STRING, record_type STRING, record_id STRING, source_id STRING, parent_page_id STRING, url STRING, content_sha256 STRING, record_json STRING, ingested_at_utc TIMESTAMP) USING DELTA")
spark.sql(f"CREATE TABLE IF NOT EXISTS {source_history_table} (snapshot_id STRING, source_id STRING, batch_id STRING, scope_sha256 STRING, extraction_contract_sha256 STRING, observed_at_utc STRING, source_snapshot_manifest_sha256 STRING, crawl_manifest_sha256 STRING, stop_reason STRING, failed_count BIGINT, rejected_count BIGINT, page_count BIGINT, source_json STRING) USING DELTA")
spark.sql(f"CREATE TABLE IF NOT EXISTS {page_history_table} (snapshot_id STRING, source_id STRING, page_id STRING, batch_id STRING, scope_sha256 STRING, extraction_contract_sha256 STRING, observed_at_utc STRING, content_sha256 STRING, url STRING, record_json STRING) USING DELTA")
spark.sql(f"CREATE TABLE IF NOT EXISTS {alias_history_table} (snapshot_id STRING, source_id STRING, alias_page_id STRING, batch_id STRING, scope_sha256 STRING, extraction_contract_sha256 STRING, observed_at_utc STRING, representative_page_id STRING, alias_url STRING, record_json STRING) USING DELTA")
spark.sql(f"CREATE TABLE IF NOT EXISTS {evidence_history_table} (snapshot_id STRING, source_id STRING, evidence_schema STRING, page_id STRING, batch_id STRING, scope_sha256 STRING, evidence_at_utc STRING, expected_page_count BIGINT, complete BOOLEAN, record_json STRING) USING DELTA")
spark.sql(f"CREATE TABLE IF NOT EXISTS {ingestion_metrics_table} (attempt_id STRING, batch_id STRING, snapshot_id STRING, started_at_utc STRING, completed_at_utc STRING, execution_millis BIGINT, processed_bytes BIGINT, input_record_count BIGINT, bronze_candidate_new_rows BIGINT, silver_candidate_new_rows BIGINT, bronze_existing_rows BIGINT, silver_existing_rows BIGINT, record_counts_json STRING, storage_measurements_json STRING) USING DELTA")
batch_columns = set(spark.table(batch_table).columns)
for column, column_type in (
    ("source_snapshot_manifest_sha256", "STRING"),
    ("crawl_manifest_sha256", "STRING"),
    ("crawl_manifest_file_sha256", "STRING"),
    ("crawl_manifest_file_bytes", "BIGINT"),
    ("history_evidence_sha256", "STRING"),
    ("operational_log_present", "BOOLEAN"),
):
    if column not in batch_columns:
        spark.sql(f"ALTER TABLE {batch_table} ADD COLUMNS ({quote(column)} {column_type})")

existing_batches = spark.table(batch_table).where(F.col("batch_id") == batch_id).limit(2).collect()
require(len(existing_batches) <= 1, "conflicting duplicate batch identities already exist")
if existing_batches:
    existing = existing_batches[0]
    require(existing["manifest_sha256"] == manifest_sha,
            "batch_id already exists with a different immutable manifest")
    if existing["status"] == "complete":
        print(f"Already ingested batch {batch_id}; refreshing idempotent history views.")

batch_values = spark.createDataFrame([(
    batch_id, snapshot_id, SCHEMA, manifest_sha, manifest["records_sha256"],
    int(manifest["page_count"]), int(manifest["passage_count"]), int(manifest["alias_count"]),
    int(manifest["record_count"]), json.dumps(expected_counts, sort_keys=True), "processing", None,
    snapshot_manifest_sha, manifest["crawl_manifest_sha256"], crawl_manifest_file_sha,
    crawl_manifest_file_bytes, history_evidence_sha,
    "operational-log.jsonl" in artifacts,
)], "batch_id STRING, snapshot_id STRING, schema_version STRING, manifest_sha256 STRING, records_sha256 STRING, expected_page_count BIGINT, expected_passage_count BIGINT, expected_alias_count BIGINT, record_count BIGINT, record_counts_json STRING, status STRING, published_at_utc TIMESTAMP, source_snapshot_manifest_sha256 STRING, crawl_manifest_sha256 STRING, crawl_manifest_file_sha256 STRING, crawl_manifest_file_bytes BIGINT, history_evidence_sha256 STRING, operational_log_present BOOLEAN")
batch_values.createOrReplaceTempView("scout_incoming_batch")
spark.sql(f"""MERGE INTO {batch_table} AS target USING scout_incoming_batch AS source
ON target.batch_id = source.batch_id
WHEN MATCHED AND target.manifest_sha256 = source.manifest_sha256 AND target.status = 'complete' AND target.operational_log_present IS NULL THEN UPDATE SET target.operational_log_present = source.operational_log_present
WHEN MATCHED AND target.manifest_sha256 = source.manifest_sha256 AND target.status != 'complete' THEN UPDATE SET target.status = 'processing', target.crawl_manifest_file_sha256 = source.crawl_manifest_file_sha256, target.crawl_manifest_file_bytes = source.crawl_manifest_file_bytes, target.operational_log_present = source.operational_log_present
WHEN NOT MATCHED THEN INSERT *""")

bronze = frame.select(
    "batch_id", "snapshot_id", "schema_version", "record_type", "record_id", "source_id", "record_json"
).withColumn("manifest_sha256", F.lit(manifest_sha)) \
 .withColumn("records_sha256", F.lit(manifest["records_sha256"])) \
 .withColumn("ingested_at_utc", F.current_timestamp()) \
 .select("batch_id", "snapshot_id", "schema_version", "manifest_sha256", "records_sha256", "record_type", "record_id", "source_id", "record_json", "ingested_at_utc")
silver = bronze.withColumn(
    "parent_page_id",
    F.when(F.col("record_type") == "passage", F.get_json_object("record_json", "$.page_id"))
     .when(F.col("record_type") == "page_alias", F.get_json_object("record_json", "$.representative_page_id"))
     .otherwise(F.lit(None).cast("string")),
).withColumn(
    "url",
    F.when(F.col("record_type") == "page", F.get_json_object("record_json", "$.normalized_final_url"))
     .when(F.col("record_type") == "passage", F.get_json_object("record_json", "$.normalized_final_url"))
     .when(F.col("record_type") == "page_alias", F.get_json_object("record_json", "$.alias_url"))
     .otherwise(F.lit(None).cast("string")),
).withColumn(
    "content_sha256",
    F.when(F.col("record_type").isin("page", "passage"), F.get_json_object("record_json", "$.content_sha256"))
     .otherwise(F.lit(None).cast("string")),
).select(
    "batch_id", "snapshot_id", "schema_version", "manifest_sha256", "records_sha256", "record_type", "record_id", "source_id", "parent_page_id", "url", "content_sha256", "record_json", "ingested_at_utc"
)

def merge_append_only(table, incoming, view_name):
    target = spark.table(table).alias("target")
    source = incoming.alias("source")
    condition = (
        (F.col("target.batch_id") == F.col("source.batch_id"))
        & (F.col("target.record_type") == F.col("source.record_type"))
        & F.col("target.source_id").eqNullSafe(F.col("source.source_id"))
        & (F.col("target.record_id") == F.col("source.record_id"))
    )
    conflict = target.join(source, condition).where(F.col("target.record_json") != F.col("source.record_json"))
    require(conflict.limit(1).count() == 0, f"conflicting immutable record already exists in {table}")
    candidate_new_rows = source.join(target, condition, "left_anti").count()
    incoming.createOrReplaceTempView(view_name)
    spark.sql(f"""MERGE INTO {table} AS target USING {view_name} AS source
ON target.batch_id = source.batch_id AND target.record_type = source.record_type
AND target.source_id <=> source.source_id AND target.record_id = source.record_id
WHEN NOT MATCHED THEN INSERT *""")
    return candidate_new_rows

bronze_candidate_new_rows = merge_append_only(bronze_table, bronze, "scout_incoming_bronze")
silver_candidate_new_rows = merge_append_only(silver_table, silver, "scout_incoming_silver")
persisted_counts = {
    row["record_type"]: int(row["count"])
    for row in spark.table(silver_table).where(F.col("batch_id") == batch_id).groupBy("record_type").count().collect()
}
persisted_counts = {record_type: persisted_counts.get(record_type, 0) for record_type in RECORD_TYPES}
require(persisted_counts == expected_counts, "Silver record counts differ from the validated batch")

spark.sql(f"""CREATE OR REPLACE VIEW {gold_view} AS
SELECT b.batch_id, b.snapshot_id, b.manifest_sha256, b.published_at_utc,
       b.expected_page_count, SUM(CASE WHEN s.record_type = 'page' THEN 1 ELSE 0 END) AS page_count,
       b.expected_passage_count, SUM(CASE WHEN s.record_type = 'passage' THEN 1 ELSE 0 END) AS passage_count,
       b.expected_alias_count, SUM(CASE WHEN s.record_type = 'page_alias' THEN 1 ELSE 0 END) AS alias_count,
       b.record_count
FROM {batch_table} AS b LEFT JOIN {silver_table} AS s ON b.batch_id = s.batch_id
WHERE b.status = 'complete'
GROUP BY b.batch_id, b.snapshot_id, b.manifest_sha256, b.published_at_utc,
         b.expected_page_count, b.expected_passage_count, b.expected_alias_count, b.record_count""")
spark.sql(f"""UPDATE {batch_table}
SET status = 'complete', published_at_utc = COALESCE(published_at_utc, current_timestamp())
WHERE batch_id = '{batch_id}' AND manifest_sha256 = '{manifest_sha}'""")

# Rebuild logical observations from all complete Silver batches. The merge keys
# use snapshot identities so an evidence-bearing re-export cannot double count
# the same Source, Page, or alias observation.
complete_batches = spark.table(batch_table).where(F.col("status") == "complete").select(
    "batch_id", "snapshot_id", "source_snapshot_manifest_sha256", "crawl_manifest_sha256"
)
history_silver = spark.table(silver_table).join(complete_batches, ["batch_id", "snapshot_id"])
history_crawls = history_silver.where(F.col("record_type") == "crawl_manifest").select(
    "batch_id", "snapshot_id", F.col("record_json").alias("crawl_json"),
    "source_snapshot_manifest_sha256", "crawl_manifest_sha256",
    F.coalesce("crawl_manifest_sha256", F.get_json_object("record_json", "$.manifest_sha256")).alias("crawl_sha"),
    F.get_json_object("record_json", "$.summary.stop_reason").alias("stop_reason"),
    F.get_json_object("record_json", "$.summary.failed_count").cast("bigint").alias("failed_count"),
    F.get_json_object("record_json", "$.summary.rejected_count").cast("bigint").alias("rejected_count"),
)
history_snapshots = history_silver.where(F.col("record_type") == "snapshot").select(
    "batch_id", "snapshot_id", F.get_json_object("record_json", "$.created_at_utc").alias("observed_at_utc")
)
history_meta = history_crawls.join(history_snapshots, ["batch_id", "snapshot_id"]).withColumn("extraction_contract_sha256", F.udf(
    lambda raw: extraction_contract_sha256(json.loads(raw).get("policy", {})), StringType()
)(F.col("crawl_json")))
scope_hash_udf = F.udf(lambda raw: source_scope_sha256(json.loads(raw)), StringType())

source_page_counts = history_silver.where(F.col("record_type") == "page").groupBy(
    "batch_id", "snapshot_id", "source_id"
).count().withColumnRenamed("count", "page_count")
source_observations = history_silver.where(F.col("record_type") == "source").select(
    "batch_id", "snapshot_id", F.col("record_id").alias("source_id"), F.col("record_json").alias("source_json")
).join(history_meta, ["batch_id", "snapshot_id"]).join(
    source_page_counts, ["batch_id", "snapshot_id", "source_id"], "left"
).fillna({"page_count": 0}).withColumn(
    "scope_sha256", scope_hash_udf(F.col("source_json"))
).select(
    "snapshot_id", "source_id", "batch_id", "scope_sha256", "extraction_contract_sha256", "observed_at_utc",
    "source_snapshot_manifest_sha256", F.col("crawl_sha").alias("crawl_manifest_sha256"),
    "stop_reason", "failed_count", "rejected_count", "page_count", "source_json",
)

source_keys = ["snapshot_id", "source_id"]

def merge_history(table, incoming, keys, immutable_columns, view_name, time_column="observed_at_utc"):
    variants = incoming.groupBy(*keys).agg(
        F.countDistinct(F.to_json(F.struct(*[F.col(name) for name in immutable_columns]))).alias("variant_count")
    )
    require(variants.where(F.col("variant_count") > 1).limit(1).count() == 0,
            f"conflicting logical history records in {table}")
    if time_column and time_column in incoming.columns:
        ordering = [F.col(time_column).asc_nulls_first(), F.col("batch_id").asc()]
    else:
        ordering = [F.col("batch_id").asc()]
    incoming = incoming.withColumn("_history_rank", F.row_number().over(Window.partitionBy(*keys).orderBy(*ordering))) \
        .where(F.col("_history_rank") == 1).drop("_history_rank")
    target = spark.table(table).alias("target")
    source = incoming.alias("source")
    match = None
    for key in keys:
        term = F.col(f"target.{key}").eqNullSafe(F.col(f"source.{key}"))
        match = term if match is None else match & term
    difference = None
    for name in immutable_columns:
        term = ~F.col(f"target.{name}").eqNullSafe(F.col(f"source.{name}"))
        difference = term if difference is None else difference | term
    require(target.join(source, match).where(difference).limit(1).count() == 0,
            f"conflicting immutable history observation in {table}")
    incoming.createOrReplaceTempView(view_name)
    updates = ["target.batch_id = least(target.batch_id, source.batch_id)"]
    if time_column and time_column in incoming.columns:
        updates.append(f"target.{time_column} = least(target.{time_column}, source.{time_column})")
    spark.sql(f"""MERGE INTO {table} AS target USING {view_name} AS source
ON {' AND '.join(f'target.{key} <=> source.{key}' for key in keys)}
WHEN MATCHED THEN UPDATE SET {', '.join(updates)}
WHEN NOT MATCHED THEN INSERT *""")

merge_history(source_history_table, source_observations, source_keys,
              ["scope_sha256", "extraction_contract_sha256", "observed_at_utc",
               "source_snapshot_manifest_sha256", "crawl_manifest_sha256", "stop_reason",
               "failed_count", "rejected_count", "page_count", "source_json"],
              "scout_incoming_source_observations")

page_observations = history_silver.where(F.col("record_type") == "page").select(
    "batch_id", "snapshot_id", "source_id", F.col("record_id").alias("page_id"),
    F.col("record_json").alias("page_json"), "content_sha256", F.col("url").alias("page_url"),
).join(source_observations.select(
    "batch_id", "snapshot_id", "source_id", "scope_sha256", "extraction_contract_sha256", "observed_at_utc"
), ["batch_id", "snapshot_id", "source_id"]).select(
    "snapshot_id", "source_id", "page_id", "batch_id", "scope_sha256", "extraction_contract_sha256",
    "observed_at_utc", "content_sha256", F.col("page_url").alias("url"), F.col("page_json").alias("record_json"),
)
merge_history(page_history_table, page_observations, ["snapshot_id", "source_id", "page_id"],
              ["scope_sha256", "extraction_contract_sha256", "content_sha256", "url", "record_json"],
              "scout_incoming_page_observations")

alias_observations = history_silver.where(F.col("record_type") == "page_alias").select(
    "batch_id", "snapshot_id", "source_id", F.col("record_id").alias("alias_page_id"),
    F.get_json_object("record_json", "$.representative_page_id").alias("representative_page_id"),
    F.col("url").alias("alias_url"), F.col("record_json").alias("alias_json"),
).join(source_observations.select(
    "batch_id", "snapshot_id", "source_id", "scope_sha256", "extraction_contract_sha256", "observed_at_utc"
), ["batch_id", "snapshot_id", "source_id"]).select(
    "snapshot_id", "source_id", "alias_page_id", "batch_id", "scope_sha256", "extraction_contract_sha256",
    "observed_at_utc", "representative_page_id", "alias_url", F.col("alias_json").alias("record_json"),
)
merge_history(alias_history_table, alias_observations, ["snapshot_id", "source_id", "alias_page_id"],
              ["scope_sha256", "extraction_contract_sha256", "representative_page_id", "alias_url", "record_json"],
              "scout_incoming_page_alias_observations")

evidence_observations = history_silver.where(
    (F.col("record_type") == "operational")
    & F.get_json_object("record_json", "$.event").isin("history.page_tombstone", "history.source_coverage_assertion")
).select(
    "batch_id", "snapshot_id", "source_id", "record_json",
    F.get_json_object("record_json", "$.data.schema_version").alias("evidence_schema"),
    F.coalesce(F.get_json_object("record_json", "$.data.page_id"), F.lit("")).alias("page_id"),
    F.get_json_object("record_json", "$.data.scope_sha256").alias("scope_sha256"),
    F.coalesce(F.get_json_object("record_json", "$.data.tombstoned_at_utc"),
               F.get_json_object("record_json", "$.data.asserted_at_utc")).alias("evidence_at_utc"),
    F.get_json_object("record_json", "$.data.expected_page_count").cast("bigint").alias("expected_page_count"),
    F.get_json_object("record_json", "$.data.complete").cast("boolean").alias("complete"),
).withColumnRenamed("record_json", "event_json")
evidence_observations = evidence_observations.select(
    "snapshot_id", "source_id", "evidence_schema", "page_id", "batch_id", "scope_sha256",
    "evidence_at_utc", "expected_page_count", "complete", F.col("event_json").alias("record_json"),
)
merge_history(evidence_history_table, evidence_observations,
              ["snapshot_id", "source_id", "evidence_schema", "page_id"],
              ["scope_sha256", "evidence_at_utc", "expected_page_count", "complete", "record_json"],
              "scout_incoming_page_history_evidence", time_column="evidence_at_utc")

spark.sql(f"""CREATE OR REPLACE VIEW {gold_source_comparisons} AS
WITH ordered AS (
  SELECT *,
    LAG(snapshot_id) OVER (PARTITION BY source_id ORDER BY TO_TIMESTAMP(observed_at_utc), snapshot_id) AS previous_snapshot_id,
    LAG(scope_sha256) OVER (PARTITION BY source_id ORDER BY TO_TIMESTAMP(observed_at_utc), snapshot_id) AS previous_scope_sha256,
    LAG(extraction_contract_sha256) OVER (PARTITION BY source_id ORDER BY TO_TIMESTAMP(observed_at_utc), snapshot_id) AS previous_extraction_contract_sha256
  FROM {source_history_table}
)
SELECT *, CASE
  WHEN previous_snapshot_id IS NULL THEN 'first_observation'
  WHEN scope_sha256 = previous_scope_sha256 AND extraction_contract_sha256 = previous_extraction_contract_sha256 THEN 'compatible'
  ELSE 'incompatible_scope_or_extraction_contract'
END AS comparison_status
FROM ordered""")

spark.sql(f"""CREATE OR REPLACE VIEW {gold_page_changes} AS
WITH ordered AS (
  SELECT *,
    LAG(snapshot_id) OVER (PARTITION BY source_id, page_id, scope_sha256, extraction_contract_sha256 ORDER BY TO_TIMESTAMP(observed_at_utc), snapshot_id) AS previous_snapshot_id,
    LAG(content_sha256) OVER (PARTITION BY source_id, page_id, scope_sha256, extraction_contract_sha256 ORDER BY TO_TIMESTAMP(observed_at_utc), snapshot_id) AS previous_content_sha256,
    LAG(observed_at_utc) OVER (PARTITION BY source_id, page_id, scope_sha256, extraction_contract_sha256 ORDER BY TO_TIMESTAMP(observed_at_utc), snapshot_id) AS previous_observed_at_utc
  FROM {page_history_table}
)
SELECT *,
  CASE WHEN previous_snapshot_id IS NULL THEN 'new'
       WHEN content_sha256 = previous_content_sha256 THEN 'unchanged'
       ELSE 'changed' END AS change_status,
  CASE WHEN previous_snapshot_id IS NOT NULL AND content_sha256 <> previous_content_sha256 THEN previous_observed_at_utc END AS observed_change_interval_start_utc,
  CASE WHEN previous_snapshot_id IS NOT NULL AND content_sha256 <> previous_content_sha256 THEN observed_at_utc END AS observed_change_interval_end_utc
FROM ordered""")

spark.sql(f"""CREATE OR REPLACE VIEW {gold_page_presence} AS
WITH candidates AS (
  SELECT current.snapshot_id, current.source_id, prior.snapshot_id AS previous_snapshot_id,
         prior_page.page_id, current.scope_sha256, current.extraction_contract_sha256,
         prior.observed_at_utc AS previous_observed_at_utc, current.observed_at_utc,
         current.stop_reason AS next_stop_reason, current.failed_count AS next_failed_count,
         current.rejected_count AS next_rejected_count, current.page_count AS next_page_count,
         ROW_NUMBER() OVER (
           PARTITION BY current.snapshot_id, current.source_id, prior_page.page_id
           ORDER BY TO_TIMESTAMP(prior.observed_at_utc) DESC, prior.snapshot_id DESC
         ) AS prior_rank
  FROM {source_history_table} current
  JOIN {source_history_table} prior
    ON prior.source_id = current.source_id
   AND (TO_TIMESTAMP(prior.observed_at_utc) < TO_TIMESTAMP(current.observed_at_utc)
        OR (TO_TIMESTAMP(prior.observed_at_utc) = TO_TIMESTAMP(current.observed_at_utc)
            AND prior.snapshot_id < current.snapshot_id))
   AND prior.scope_sha256 = current.scope_sha256
   AND prior.extraction_contract_sha256 = current.extraction_contract_sha256
  JOIN {page_history_table} prior_page
    ON prior_page.snapshot_id = prior.snapshot_id AND prior_page.source_id = prior.source_id
   AND prior_page.scope_sha256 = current.scope_sha256
   AND prior_page.extraction_contract_sha256 = current.extraction_contract_sha256
  LEFT ANTI JOIN {source_history_table} intervening
    ON intervening.source_id = current.source_id
   AND (TO_TIMESTAMP(intervening.observed_at_utc) > TO_TIMESTAMP(prior.observed_at_utc)
        OR (TO_TIMESTAMP(intervening.observed_at_utc) = TO_TIMESTAMP(prior.observed_at_utc)
            AND intervening.snapshot_id > prior.snapshot_id))
   AND (TO_TIMESTAMP(intervening.observed_at_utc) < TO_TIMESTAMP(current.observed_at_utc)
        OR (TO_TIMESTAMP(intervening.observed_at_utc) = TO_TIMESTAMP(current.observed_at_utc)
            AND intervening.snapshot_id < current.snapshot_id))
   AND (intervening.scope_sha256 <> current.scope_sha256
        OR intervening.extraction_contract_sha256 <> current.extraction_contract_sha256)
  LEFT ANTI JOIN {page_history_table} present
    ON present.snapshot_id = current.snapshot_id AND present.source_id = current.source_id
   AND present.page_id = prior_page.page_id
), missing AS (
  SELECT * FROM candidates WHERE prior_rank = 1
), supported AS (
  SELECT m.*,
    tomb.evidence_at_utc AS tombstoned_at_utc,
    coverage.evidence_at_utc AS coverage_asserted_at_utc
  FROM missing m
  LEFT JOIN {evidence_history_table} tomb
    ON tomb.snapshot_id = m.snapshot_id AND tomb.source_id = m.source_id AND tomb.page_id = m.page_id
   AND tomb.evidence_schema = '{PAGE_TOMBSTONE_SCHEMA}' AND tomb.scope_sha256 = m.scope_sha256
  LEFT JOIN {evidence_history_table} coverage
    ON coverage.snapshot_id = m.snapshot_id AND coverage.source_id = m.source_id AND coverage.page_id = ''
   AND coverage.evidence_schema = '{SOURCE_COVERAGE_SCHEMA}' AND coverage.scope_sha256 = m.scope_sha256
   AND coverage.complete = true
   AND (coverage.expected_page_count IS NULL OR coverage.expected_page_count = m.next_page_count)
   AND m.next_stop_reason = 'frontier_exhausted' AND m.next_failed_count = 0 AND m.next_rejected_count = 0
)
SELECT *,
  CASE WHEN tombstoned_at_utc IS NOT NULL OR coverage_asserted_at_utc IS NOT NULL THEN 'removed' ELSE 'unobserved' END AS presence_status,
  CASE WHEN tombstoned_at_utc IS NOT NULL THEN 'explicit_tombstone'
       WHEN coverage_asserted_at_utc IS NOT NULL THEN 'complete_scope_assertion'
       ELSE 'no_removal_evidence' END AS removal_basis,
  COALESCE(tombstoned_at_utc, coverage_asserted_at_utc) AS removal_evidence_at_utc,
  previous_observed_at_utc AS observed_change_interval_start_utc,
  observed_at_utc AS observed_change_interval_end_utc
FROM supported""")

spark.sql(f"""CREATE OR REPLACE VIEW {gold_alias_observations} AS
SELECT snapshot_id, source_id, alias_page_id, representative_page_id, alias_url,
       scope_sha256, extraction_contract_sha256, observed_at_utc, batch_id, record_json
FROM {alias_history_table}""")
spark.sql(f"""CREATE OR REPLACE VIEW {gold_history_counts} AS
WITH sources AS (
  SELECT snapshot_id, MIN(observed_at_utc) AS observed_at_utc, COUNT(DISTINCT source_id) AS source_count
  FROM {source_history_table} GROUP BY snapshot_id
), pages AS (
  SELECT snapshot_id, COUNT(DISTINCT page_id) AS page_count FROM {page_history_table} GROUP BY snapshot_id
), aliases AS (
  SELECT snapshot_id, COUNT(DISTINCT alias_page_id) AS alias_count FROM {alias_history_table} GROUP BY snapshot_id
)
SELECT s.snapshot_id, s.observed_at_utc, s.source_count, COALESCE(p.page_count, 0) AS page_count,
       COALESCE(a.alias_count, 0) AS alias_count
FROM sources s LEFT JOIN pages p ON p.snapshot_id = s.snapshot_id
LEFT JOIN aliases a ON a.snapshot_id = s.snapshot_id""")

# Exact allowlist coverage needs the requested URL from the Crawl Manifest.
# Page history intentionally retains only the final URL, so do not infer URL
# coverage from a final URL or a page count.
manifest_request_schema = "array<struct<source_id:string,page_id:string,requested_url:string>>"
spark.sql(f"""CREATE OR REPLACE VIEW {history_page_requests_view} AS
SELECT DISTINCT c.snapshot_id, page.source_id, page.page_id, page.requested_url
FROM {silver_table} c
LATERAL VIEW EXPLODE(FROM_JSON(GET_JSON_OBJECT(c.record_json, '$.pages'), '{manifest_request_schema}')) expanded AS page
WHERE c.record_type = 'crawl_manifest'
  AND page.source_id IS NOT NULL AND page.requested_url IS NOT NULL""")

spark.sql(f"""CREATE OR REPLACE VIEW {gold_source_coverage} AS
WITH scopes AS (
  SELECT h.*, GET_JSON_OBJECT(c.record_json, '$.source_config_sha256') AS source_config_sha256,
         FROM_JSON(GET_JSON_OBJECT(h.source_json, '$.allowed_urls'), 'ARRAY<STRING>') AS allowed_urls
  FROM {source_history_table} h
  JOIN {silver_table} c ON c.batch_id = h.batch_id AND c.snapshot_id = h.snapshot_id
   AND c.record_type = 'crawl_manifest'
), requested AS (
  SELECT s.snapshot_id, s.source_id, COUNT(DISTINCT r.requested_url) AS captured_allowed_url_count
  FROM scopes s
  JOIN {history_page_requests_view} r
    ON r.snapshot_id = s.snapshot_id AND r.source_id = s.source_id
  WHERE s.allowed_urls IS NOT NULL AND ARRAY_CONTAINS(s.allowed_urls, r.requested_url)
  GROUP BY s.snapshot_id, s.source_id
), assertions AS (
  SELECT snapshot_id, source_id, scope_sha256,
         MAX(CASE WHEN evidence_schema = '{SOURCE_COVERAGE_SCHEMA}' AND complete = true THEN 1 ELSE 0 END) AS attested_complete,
         MAX(CASE WHEN evidence_schema = '{SOURCE_COVERAGE_SCHEMA}' AND complete = true THEN expected_page_count END) AS expected_page_count,
         MAX(CASE WHEN evidence_schema = '{SOURCE_COVERAGE_SCHEMA}' AND complete = true THEN evidence_at_utc END) AS complete_asserted_at_utc
  FROM {evidence_history_table}
  GROUP BY snapshot_id, source_id, scope_sha256
)
SELECT s.snapshot_id, s.source_id, s.batch_id, s.source_snapshot_manifest_sha256,
       s.crawl_manifest_sha256, s.source_config_sha256, s.scope_sha256,
       s.extraction_contract_sha256, s.observed_at_utc,
       s.page_count AS observed_page_count,
       CASE WHEN a.attested_complete = 1 AND a.expected_page_count IS NOT NULL THEN a.expected_page_count
            WHEN a.attested_complete = 1 THEN NULL
            WHEN s.allowed_urls IS NOT NULL THEN SIZE(s.allowed_urls)
            ELSE NULL END AS coverage_denominator,
       CASE WHEN a.attested_complete = 1 AND a.expected_page_count IS NOT NULL THEN s.page_count
            WHEN a.attested_complete = 1 THEN NULL
            WHEN s.allowed_urls IS NOT NULL THEN COALESCE(r.captured_allowed_url_count, 0)
            ELSE NULL END AS coverage_numerator,
       CASE WHEN a.attested_complete = 1 AND a.expected_page_count IS NOT NULL THEN 'measured_complete_scope_assertion'
            WHEN a.attested_complete = 1 THEN 'attested_complete_no_denominator'
            WHEN s.allowed_urls IS NOT NULL THEN 'measured_exact_allowed_urls'
            ELSE 'unknown_open_scope' END AS coverage_basis,
       CASE WHEN a.attested_complete = 1 AND a.expected_page_count IS NULL THEN 'attested_complete'
            WHEN a.attested_complete = 1 THEN 'measured'
            WHEN s.allowed_urls IS NOT NULL THEN 'measured'
            ELSE 'unknown' END AS coverage_status,
       CASE WHEN a.attested_complete = 1 THEN a.complete_asserted_at_utc ELSE NULL END AS complete_asserted_at_utc,
       CASE WHEN a.attested_complete = 1 AND a.expected_page_count IS NOT NULL
                 AND a.expected_page_count > 0
            THEN 100.0 * s.page_count / a.expected_page_count
            WHEN a.attested_complete = 1 AND a.expected_page_count IS NOT NULL THEN NULL
            WHEN a.attested_complete = 1 THEN NULL
            WHEN s.allowed_urls IS NOT NULL AND SIZE(s.allowed_urls) > 0
            THEN 100.0 * COALESCE(r.captured_allowed_url_count, 0) / SIZE(s.allowed_urls)
            ELSE NULL END AS coverage_percent,
       CASE WHEN a.attested_complete = 1 AND a.expected_page_count IS NULL
            THEN 'complete scope is attested, but no denominator was supplied'
            WHEN a.attested_complete = 1 AND a.expected_page_count = 0
            THEN 'complete scope has a zero denominator; percentage is undefined'
            WHEN s.allowed_urls IS NOT NULL AND SIZE(s.allowed_urls) = 0
            THEN 'finite allowlist is empty; percentage is undefined'
            WHEN s.allowed_urls IS NOT NULL
            THEN 'exact requested_url matches against the finite allowed_urls list'
            ELSE 'no finite Source inventory or validated complete-scope denominator is available' END AS coverage_reason
FROM scopes s
LEFT JOIN assertions a ON a.snapshot_id = s.snapshot_id AND a.source_id = s.source_id
 AND a.scope_sha256 = s.scope_sha256
LEFT JOIN requested r ON r.snapshot_id = s.snapshot_id AND r.source_id = s.source_id""")

spark.sql(f"""CREATE OR REPLACE VIEW {gold_page_freshness} AS
WITH latest_pages AS (
  SELECT p.*, GET_JSON_OBJECT(c.record_json, '$.source_config_sha256') AS page_observation_source_config_sha256,
         ROW_NUMBER() OVER (
           PARTITION BY p.source_id, p.page_id
           ORDER BY TO_TIMESTAMP(p.observed_at_utc) DESC, p.snapshot_id DESC
         ) AS freshness_rank
  FROM {page_history_table} p
  JOIN {silver_table} c ON c.batch_id = p.batch_id AND c.snapshot_id = p.snapshot_id
   AND c.record_type = 'crawl_manifest'
), latest_sources AS (
  SELECT h.*, GET_JSON_OBJECT(c.record_json, '$.source_config_sha256') AS source_config_sha256,
         ROW_NUMBER() OVER (
           PARTITION BY h.source_id ORDER BY TO_TIMESTAMP(h.observed_at_utc) DESC, h.snapshot_id DESC
         ) AS source_rank
  FROM {source_history_table} h
  JOIN {silver_table} c ON c.batch_id = h.batch_id AND c.snapshot_id = h.snapshot_id
   AND c.record_type = 'crawl_manifest'
), latest_presence AS (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY snapshot_id, source_id, page_id ORDER BY previous_observed_at_utc DESC
  ) AS presence_rank
  FROM {gold_page_presence}
)
SELECT p.snapshot_id, p.source_id, p.page_id, p.scope_sha256, p.extraction_contract_sha256,
       p.observed_at_utc, p.content_sha256, p.url, p.batch_id,
       p.page_observation_source_config_sha256, s.source_config_sha256 AS latest_source_config_sha256,
       s.snapshot_id AS latest_source_snapshot_id, s.scope_sha256 AS latest_source_scope_sha256,
       CASE WHEN p.scope_sha256 = s.scope_sha256
                  AND p.extraction_contract_sha256 = s.extraction_contract_sha256
            THEN COALESCE(x.presence_status,
                 CASE WHEN p.snapshot_id = s.snapshot_id THEN 'observed_in_latest_source_snapshot'
                      ELSE 'no_compatible_presence_comparison' END)
            ELSE 'scope_or_extraction_contract_changed' END AS current_presence_status,
       DATEDIFF(CURRENT_DATE(), TO_DATE(TO_TIMESTAMP(p.observed_at_utc))) AS observation_age_days,
       'snapshot_seal_time' AS freshness_basis,
       'page fetch timestamp unavailable' AS timestamp_limit
FROM latest_pages p
JOIN latest_sources s ON s.source_id = p.source_id AND s.source_rank = 1
LEFT JOIN latest_presence x ON x.snapshot_id = s.snapshot_id AND x.source_id = p.source_id
 AND x.page_id = p.page_id AND x.presence_rank = 1
WHERE p.freshness_rank = 1""")

spark.sql(f"""CREATE OR REPLACE VIEW {gold_change_frequency} AS
WITH changes AS (
  SELECT h.*, GET_JSON_OBJECT(c.record_json, '$.source_config_sha256') AS source_config_sha256
  FROM {gold_page_changes} h
  JOIN {silver_table} c ON c.batch_id = h.batch_id AND c.snapshot_id = h.snapshot_id
   AND c.record_type = 'crawl_manifest'
)
SELECT source_id, source_config_sha256, scope_sha256, extraction_contract_sha256,
       DATE_TRUNC('MONTH', TO_TIMESTAMP(observed_at_utc)) AS observation_month_utc,
       MIN(observed_at_utc) AS first_observed_at_utc, MAX(observed_at_utc) AS last_observed_at_utc,
       COUNT(*) AS page_observations,
       SUM(CASE WHEN change_status = 'new' THEN 1 ELSE 0 END) AS new_page_observations,
       SUM(CASE WHEN change_status = 'changed' THEN 1 ELSE 0 END) AS changed_page_observations,
       SUM(CASE WHEN change_status = 'unchanged' THEN 1 ELSE 0 END) AS unchanged_page_observations,
       SUM(CASE WHEN previous_snapshot_id IS NOT NULL THEN 1 ELSE 0 END) AS comparable_page_observations,
       CASE WHEN SUM(CASE WHEN previous_snapshot_id IS NOT NULL THEN 1 ELSE 0 END) > 0
            THEN 100.0 * SUM(CASE WHEN change_status = 'changed' THEN 1 ELSE 0 END)
                 / SUM(CASE WHEN previous_snapshot_id IS NOT NULL THEN 1 ELSE 0 END)
            ELSE NULL END AS changed_percent_of_comparable_pages,
       MIN(observed_change_interval_start_utc) AS earliest_change_interval_start_utc,
       MAX(observed_change_interval_end_utc) AS latest_change_interval_end_utc,
       'monthly observations; change time is interval-bounded' AS frequency_basis
FROM changes
GROUP BY source_id, source_config_sha256, scope_sha256, extraction_contract_sha256,
         DATE_TRUNC('MONTH', TO_TIMESTAMP(observed_at_utc))""")

spark.sql(f"""CREATE OR REPLACE VIEW {gold_operational_summary} AS
WITH records AS (
  SELECT e.snapshot_id, e.batch_id, e.record_id, e.record_json, e.source_id,
         GET_JSON_OBJECT(c.record_json, '$.source_config_sha256') AS source_config_sha256,
         ROW_NUMBER() OVER (PARTITION BY e.snapshot_id, e.record_id ORDER BY e.batch_id) AS replay_rank
  FROM {silver_table} e
  JOIN {silver_table} c ON c.batch_id = e.batch_id AND c.snapshot_id = e.snapshot_id
   AND c.record_type = 'crawl_manifest'
  WHERE e.record_type = 'operational'
    AND GET_JSON_OBJECT(e.record_json, '$.event') NOT LIKE 'history.%'
), events AS (
  SELECT snapshot_id, batch_id, record_id, source_config_sha256,
         GET_JSON_OBJECT(record_json, '$.run_id') AS run_id,
         COALESCE(source_id, GET_JSON_OBJECT(record_json, '$.data.source_id')) AS source_id,
         GET_JSON_OBJECT(record_json, '$.event') AS event_name,
         GET_JSON_OBJECT(record_json, '$.outcome') AS outcome,
         GET_JSON_OBJECT(record_json, '$.component') AS component,
         GET_JSON_OBJECT(record_json, '$.timestamp') AS event_at_utc,
         GET_JSON_OBJECT(record_json, '$.data.task_id') AS task_id,
         CAST(GET_JSON_OBJECT(record_json, '$.data.task_attempt') AS BIGINT) AS task_attempt
  FROM records
  WHERE replay_rank = 1
)
SELECT snapshot_id, MIN(batch_id) AS batch_id, source_config_sha256, run_id,
       source_id, event_name, outcome, component,
       COUNT(*) AS event_count, COUNT(DISTINCT task_id) AS distinct_task_count,
       SUM(CASE WHEN task_attempt > 1 THEN 1 ELSE 0 END) AS retry_claim_event_count,
       COUNT(DISTINCT CASE WHEN task_attempt > 1 THEN task_id END) AS retried_task_count,
       MIN(event_at_utc) AS first_event_at_utc, MAX(event_at_utc) AS last_event_at_utc,
       SUM(CASE WHEN outcome IN ('failure', 'error') THEN 1 ELSE 0 END) AS failure_outcome_count,
       'deduplicated by snapshot_id and operational record_id' AS replay_handling
FROM events
GROUP BY snapshot_id, source_config_sha256, run_id, source_id, event_name, outcome, component""")

spark.sql(f"""CREATE OR REPLACE VIEW {gold_crawl_run} AS
SELECT b.batch_id, b.snapshot_id, b.manifest_sha256 AS batch_manifest_sha256,
       b.source_snapshot_manifest_sha256, b.crawl_manifest_sha256, b.published_at_utc,
       b.operational_log_present,
       GET_JSON_OBJECT(s.record_json, '$.run_id') AS run_id,
       GET_JSON_OBJECT(s.record_json, '$.source_config_sha256') AS source_config_sha256,
       GET_JSON_OBJECT(s.record_json, '$.started_at_utc') AS started_at_utc,
       GET_JSON_OBJECT(s.record_json, '$.completed_at_utc') AS completed_at_utc,
       GET_JSON_OBJECT(s.record_json, '$.summary.stop_reason') AS stop_reason,
       CAST(GET_JSON_OBJECT(s.record_json, '$.summary.target_page_count') AS BIGINT) AS target_page_count,
       CAST(GET_JSON_OBJECT(s.record_json, '$.summary.captured_page_count') AS BIGINT) AS captured_page_count,
       CAST(GET_JSON_OBJECT(s.record_json, '$.summary.page_count') AS BIGINT) AS page_count,
       CAST(GET_JSON_OBJECT(s.record_json, '$.summary.alias_count') AS BIGINT) AS alias_count,
       CAST(GET_JSON_OBJECT(s.record_json, '$.summary.passage_count') AS BIGINT) AS passage_count,
       CAST(GET_JSON_OBJECT(s.record_json, '$.summary.rejected_count') AS BIGINT) AS rejected_count,
       CAST(GET_JSON_OBJECT(s.record_json, '$.summary.failed_count') AS BIGINT) AS failed_count,
       SIZE(FROM_JSON(GET_JSON_OBJECT(s.record_json, '$.sources'), 'ARRAY<STRUCT<source_id:STRING>>')) AS source_count,
       (UNIX_TIMESTAMP(TO_TIMESTAMP(GET_JSON_OBJECT(s.record_json, '$.completed_at_utc')))
        - UNIX_TIMESTAMP(TO_TIMESTAMP(GET_JSON_OBJECT(s.record_json, '$.started_at_utc')))) AS crawl_duration_seconds,
       'failed_count counts fetch.failed events; rejected_count counts page.rejected events' AS error_count_basis
FROM {batch_table} b
JOIN {silver_table} s ON s.batch_id = b.batch_id AND s.snapshot_id = b.snapshot_id
WHERE b.status = 'complete' AND s.record_type = 'crawl_manifest'""")

spark.sql(f"""CREATE OR REPLACE VIEW {gold_crawl_effectiveness} AS
WITH event_totals AS (
  SELECT snapshot_id,
         SUM(CASE WHEN event_name = 'task.claimed' THEN event_count ELSE 0 END) AS task_claim_count,
         MAX(CASE WHEN event_name = 'capture.accepted' THEN 1 ELSE 0 END) AS capture_event_type_present,
         SUM(CASE WHEN event_name = 'capture.accepted' THEN event_count ELSE 0 END) AS capture_accept_event_count,
         MAX(CASE WHEN event_name = 'worker.task_failed' THEN 1 ELSE 0 END) AS worker_failure_event_type_present,
         SUM(CASE WHEN event_name = 'worker.task_failed' THEN event_count ELSE 0 END) AS worker_task_failure_event_count,
         MAX(CASE WHEN event_name = 'page.rejected' THEN 1 ELSE 0 END) AS rejection_event_type_present,
         SUM(retry_claim_event_count) AS retry_claim_event_count,
         SUM(retried_task_count) AS retried_task_count,
         SUM(CASE WHEN event_name = 'page.rejected' THEN event_count ELSE 0 END) AS page_rejected_event_count,
         COUNT(*) AS operational_event_groups
  FROM {gold_operational_summary}
  GROUP BY snapshot_id
)
SELECT r.*,
       CASE WHEN e.task_claim_count > 0 THEN e.task_claim_count ELSE NULL END AS task_claim_attempts,
       CASE WHEN e.capture_event_type_present = 1 THEN e.capture_accept_event_count ELSE NULL END AS capture_accept_event_count,
       CASE WHEN e.worker_failure_event_type_present = 1 THEN e.worker_task_failure_event_count ELSE NULL END AS worker_task_failure_event_count,
       CASE WHEN e.task_claim_count > 0 THEN e.retry_claim_event_count ELSE NULL END AS retry_claim_event_count,
       CASE WHEN e.task_claim_count > 0 THEN e.retried_task_count ELSE NULL END AS retried_task_count,
       CASE WHEN e.rejection_event_type_present = 1 THEN e.page_rejected_event_count ELSE NULL END AS page_rejected_event_count,
       CASE WHEN r.operational_log_present = false THEN 'operational log absent'
            WHEN r.operational_log_present IS NULL THEN 'operational log availability unknown'
            WHEN COALESCE(e.task_claim_count, 0) = 0 THEN 'operational log present; no task.claimed events; attempt denominator unknown'
            ELSE 'task.claimed event count' END AS attempt_denominator_status,
       CASE WHEN e.task_claim_count > 0
            THEN CAST(r.captured_page_count AS DOUBLE) / e.task_claim_count ELSE NULL END AS captured_pages_per_task_claim,
       CASE WHEN e.task_claim_count > 0
            THEN CAST(r.failed_count AS DOUBLE) / e.task_claim_count ELSE NULL END AS fetch_failures_per_task_claim,
       CASE WHEN r.crawl_duration_seconds > 0
            THEN 60.0 * CAST(r.page_count AS DOUBLE) / r.crawl_duration_seconds ELSE NULL END AS pages_per_minute,
       'attempts use task.claimed; failures use Crawl Manifest fetch.failed summary; rejected Pages remain separate' AS effectiveness_basis
FROM {gold_crawl_run} r
LEFT JOIN event_totals e ON e.snapshot_id = r.snapshot_id""")

spark.sql(f"""CREATE OR REPLACE VIEW {gold_crawl_error_rate} AS
SELECT snapshot_id, source_config_sha256, source_id,
       SUM(event_count) AS observed_operational_event_count,
       SUM(failure_outcome_count) AS failed_outcome_event_count,
       CASE WHEN SUM(event_count) > 0
            THEN 100.0 * SUM(failure_outcome_count) / SUM(event_count) ELSE NULL END AS failed_outcome_percent,
       SUM(CASE WHEN event_name = 'fetch.failed' THEN event_count ELSE 0 END) AS fetch_failed_event_count,
       SUM(CASE WHEN event_name = 'worker.task_failed' THEN event_count ELSE 0 END) AS worker_task_failed_event_count,
       SUM(CASE WHEN event_name = 'page.rejected' THEN event_count ELSE 0 END) AS page_rejected_event_count,
       'failure percentage uses only observed operational event outcomes; it is not an HTTP request error rate' AS denominator_basis
FROM {gold_operational_summary}
GROUP BY snapshot_id, source_config_sha256, source_id""")

spark.sql(f"""CREATE OR REPLACE VIEW {gold_ingestion_metrics} AS
SELECT m.attempt_id, m.batch_id, m.snapshot_id, r.source_config_sha256,
       m.started_at_utc, m.completed_at_utc,
       execution_millis, processed_bytes, input_record_count,
       bronze_candidate_new_rows, silver_candidate_new_rows,
       bronze_existing_rows, silver_existing_rows, record_counts_json,
       storage_measurements_json,
       'candidate new rows counted before MERGE; concurrent duplicate ingestion can lower committed inserts' AS row_change_limit
FROM {ingestion_metrics_table} m
LEFT JOIN {gold_crawl_run} r ON r.batch_id = m.batch_id""")

report = spark.table(gold_view).where(F.col("batch_id") == batch_id).first().asDict()
require(report["page_count"] == manifest["page_count"]
        and report["passage_count"] == manifest["passage_count"]
        and report["alias_count"] == manifest["alias_count"], "Gold count report does not match source snapshot")

storage_measurements = {}
for storage_table in (bronze_table, silver_table, source_history_table, page_history_table,
                      alias_history_table, evidence_history_table):
    try:
        detail = spark.sql(f"DESCRIBE DETAIL {storage_table}").select("sizeInBytes").first()
        storage_measurements[storage_table.split(".")[-1].replace("`", "")] = int(detail["sizeInBytes"])
    except Exception as error:
        storage_measurements[storage_table.split(".")[-1].replace("`", "")] = {
            "available": False, "reason": f"{type(error).__name__}: {str(error)[:240]}"
        }
ingestion_completed_at_utc = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
ingestion_execution_millis = int((time.perf_counter() - ingestion_started_perf) * 1000)
processed_bytes = int(records_bytes + history_bytes + len(manifest_bytes))
metrics_row = spark.createDataFrame([(
    ingestion_attempt_id, batch_id, snapshot_id, ingestion_started_at_utc,
    ingestion_completed_at_utc, ingestion_execution_millis, processed_bytes,
    int(manifest["record_count"]), int(bronze_candidate_new_rows), int(silver_candidate_new_rows),
    int(manifest["record_count"] - bronze_candidate_new_rows),
    int(manifest["record_count"] - silver_candidate_new_rows),
    json.dumps(expected_counts, sort_keys=True), json.dumps(storage_measurements, sort_keys=True),
)], "attempt_id STRING, batch_id STRING, snapshot_id STRING, started_at_utc STRING, completed_at_utc STRING, execution_millis BIGINT, processed_bytes BIGINT, input_record_count BIGINT, bronze_candidate_new_rows BIGINT, silver_candidate_new_rows BIGINT, bronze_existing_rows BIGINT, silver_existing_rows BIGINT, record_counts_json STRING, storage_measurements_json STRING")
metrics_row.write.mode("append").insertInto(ingestion_metrics_table)
print(json.dumps(report, default=str, sort_keys=True))
