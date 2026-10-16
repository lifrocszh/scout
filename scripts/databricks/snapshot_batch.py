#!/usr/bin/env python3
"""Export, validate, upload, and submit one immutable Scout snapshot batch."""

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from snapshot_remote import submit_batch, upload_batch


SCHEMA = "scout.databricks-snapshot-batch.v1"
MAX_BATCH_BYTES = 64 * 1024 * 1024
MAX_RECORDS = 100_000
RECORD_TYPES = ("snapshot", "source", "page", "passage", "page_alias", "crawl_manifest", "operational")
HEX_256 = re.compile(r"^[a-f0-9]{64}$")
SNAPSHOT_ID = re.compile(r"^snapshot-[a-f0-9]{64}$")
HISTORY_EVIDENCE_SCHEMA = "scout.page-history-evidence.v1"
PAGE_TOMBSTONE_SCHEMA = "scout.page-tombstone.v1"
SOURCE_COVERAGE_SCHEMA = "scout.source-coverage-assertion.v1"
SOURCE_SCOPE_FIELDS = (
    "seeds", "allowed_urls", "allowed_origins", "path_prefixes",
    "deny_path_prefixes", "sitemap_url_prefixes",
)


def fail(message):
    raise ValueError(message)


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def pretty_json(value):
    return json.dumps(value, ensure_ascii=False, indent=2, separators=(",", ": ")).encode("utf-8")


def read_json(path, label):
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        fail(f"cannot read {label} at {path}: {error}")
    if not isinstance(value, dict):
        fail(f"{label} must be a JSON object")
    return value


def require_string(value, field, where):
    value = value.get(field) if isinstance(value, dict) else None
    if not isinstance(value, str) or not value:
        fail(f"{where}.{field} must be a non-empty string")
    return value


def verify_crawl_manifest(crawl, snapshot_id, raw_bytes=None):
    if crawl.get("schema_version") != "scout.crawl-manifest.v1" or crawl.get("status") != "complete":
        fail("Crawl Manifest is not a supported completed crawl")
    if crawl.get("snapshot_id") != snapshot_id:
        fail("Crawl Manifest snapshot_id does not match the Corpus snapshot")
    claimed = require_string(crawl, "manifest_sha256", "crawl_manifest")
    if not HEX_256.fullmatch(claimed):
        fail("Crawl Manifest manifest_sha256 is malformed")
    if raw_bytes is not None:
        target = f'"manifest_sha256": "{claimed}"'.encode("ascii")
        if raw_bytes.count(target) != 1:
            fail("Crawl Manifest digest field is missing or duplicated")
        normalized = raw_bytes.replace(target, b'"manifest_sha256": ""', 1)
        if sha256_bytes(normalized) != claimed:
            fail("Crawl Manifest self-checksum mismatch")
    return claimed


def source_scope_sha256(source):
    scope = {"source_id": require_string(source, "source_id", "Source")}
    for field in SOURCE_SCOPE_FIELDS:
        value = source.get(field)
        if field == "allowed_urls" and value is None:
            scope[field] = None
            continue
        if field == "sitemap_url_prefixes" and value is None:
            value = []
        if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
            fail(f"Source.{field} must be a string array")
        scope[field] = sorted(value)
    return sha256_bytes(canonical_json(scope).encode("utf-8"))


def validate_utc_timestamp(value, label):
    if not isinstance(value, str) or not value.endswith("Z"):
        fail(f"{label} must be an RFC 3339 UTC timestamp ending in Z")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        fail(f"{label} is not a valid RFC 3339 UTC timestamp")
    if parsed.utcoffset() != timedelta(0):
        fail(f"{label} must be UTC")
    return parsed.astimezone(timezone.utc)


def validate_history_evidence(evidence, crawl, snapshot_id, snapshot_sha, crawl_sha,
                              page_source_pairs, source_page_counts):
    if evidence.get("schema_version") != HISTORY_EVIDENCE_SCHEMA:
        fail("unsupported history evidence schema_version")
    if evidence.get("snapshot_id") != snapshot_id or evidence.get("run_id") != crawl.get("run_id"):
        fail("history evidence run/snapshot lineage mismatch")
    if evidence.get("source_snapshot_manifest_sha256") != snapshot_sha or evidence.get("crawl_manifest_sha256") != crawl_sha:
        fail("history evidence is not bound to these immutable snapshot and Crawl Manifest checksums")
    records = evidence.get("records")
    if not isinstance(records, list) or not records:
        fail("history evidence records must be a non-empty array")
    sources = {source["source_id"]: source for source in crawl.get("sources", [])}
    seen = set()
    validated = []
    summary = crawl.get("summary", {})
    for number, record in enumerate(records, start=1):
        label = f"history evidence record {number}"
        if not isinstance(record, dict):
            fail(f"{label} must be an object")
        schema = record.get("schema_version")
        if schema not in (PAGE_TOMBSTONE_SCHEMA, SOURCE_COVERAGE_SCHEMA):
            fail(f"{label} has unsupported schema_version")
        source_id = require_string(record, "source_id", label)
        if source_id not in sources:
            fail(f"{label} references an unknown Source")
        if record.get("snapshot_id") != snapshot_id or record.get("run_id") != crawl.get("run_id"):
            fail(f"{label} run/snapshot lineage mismatch")
        scope_sha = require_string(record, "scope_sha256", label)
        if not HEX_256.fullmatch(scope_sha) or scope_sha != source_scope_sha256(sources[source_id]):
            fail(f"{label} Source scope hash mismatch")
        timestamp_field = "tombstoned_at_utc" if schema == PAGE_TOMBSTONE_SCHEMA else "asserted_at_utc"
        validate_utc_timestamp(require_string(record, timestamp_field, label), f"{label}.{timestamp_field}")
        if schema == PAGE_TOMBSTONE_SCHEMA:
            page_id = require_string(record, "page_id", label)
            if not HEX_256.fullmatch(page_id) or (source_id, page_id) in page_source_pairs:
                fail(f"{label} Page ID is malformed or still present in this snapshot")
            identity = (schema, source_id, page_id)
        else:
            if record.get("complete") is not True:
                fail(f"{label} must explicitly assert complete=true")
            expected_count = record.get("expected_page_count")
            if expected_count is not None:
                if isinstance(expected_count, bool) or not isinstance(expected_count, int) or expected_count < 0:
                    fail(f"{label}.expected_page_count must be a non-negative integer")
                if expected_count != source_page_counts.get(source_id, 0):
                    fail(f"{label}.expected_page_count does not match this snapshot's Source Page count")
            if (summary.get("stop_reason") != "frontier_exhausted"
                    or int(summary.get("failed_count", 0)) != 0
                    or int(summary.get("rejected_count", 0)) != 0):
                fail(f"{label} cannot assert complete coverage for a bounded or errored crawl")
            identity = (schema, source_id)
        if identity in seen:
            fail(f"{label} duplicates evidence for the same Source/Page")
        seen.add(identity)
        validated.append(record)
    return validated


def validate_content(record, where, hashed_field="content"):
    require_string(record, "content", where)
    hashed_value = require_string(record, hashed_field, where)
    content_hash = require_string(record, "content_sha256", where)
    if not HEX_256.fullmatch(content_hash) or sha256_bytes(hashed_value.encode("utf-8")) != content_hash:
        fail(f"{where}.content_sha256 does not match its stored content identity")


def append_envelope(stream, batch_id, snapshot_id, snapshot_manifest_sha, crawl_sha, record_type, record_id, source_id, record):
    envelope = {
        "schema_version": SCHEMA,
        "batch_id": batch_id,
        "snapshot_id": snapshot_id,
        "snapshot_manifest_sha256": snapshot_manifest_sha,
        "crawl_manifest_sha256": crawl_sha,
        "record_type": record_type,
        "record_id": record_id,
        "source_id": source_id,
        "record_json": canonical_json(record),
    }
    stream.write(canonical_json(envelope).encode("utf-8") + b"\n")


def validate_envelopes(records_path, manifest, history_evidence=None):
    source_ids, page_ids, page_sources, seen = set(), set(), {}, set()
    counts = {record_type: 0 for record_type in RECORD_TYPES}
    history_records = []
    page_source_pairs = set()
    source_page_counts = {}
    last_rank = -1
    snapshot_record = crawl_record = None
    snapshot_id = manifest["snapshot_id"]
    batch_id = manifest["batch_id"]
    snapshot_manifest_sha = manifest["source_snapshot_manifest_sha256"]
    crawl_sha = manifest["crawl_manifest_sha256"]
    run_id = manifest["run_id"]
    rank = {record_type: index for index, record_type in enumerate(RECORD_TYPES)}

    with records_path.open("rb") as stream:
        for line_number, raw_line in enumerate(stream, start=1):
            if not raw_line.strip():
                fail(f"records.jsonl line {line_number} is empty")
            try:
                envelope = json.loads(raw_line)
                record = json.loads(envelope["record_json"])
            except (UnicodeError, json.JSONDecodeError, KeyError, TypeError) as error:
                fail(f"records.jsonl line {line_number} is invalid: {error}")
            if not isinstance(envelope, dict) or not isinstance(record, dict):
                fail(f"records.jsonl line {line_number} must contain JSON objects")
            record_type = envelope.get("record_type")
            if record_type not in rank:
                fail(f"records.jsonl line {line_number} has unsupported record_type {record_type!r}")
            if rank[record_type] < last_rank:
                fail("records.jsonl records are not in canonical type order")
            last_rank = rank[record_type]
            if envelope.get("schema_version") != SCHEMA or envelope.get("batch_id") != batch_id:
                fail(f"records.jsonl line {line_number} has an incompatible batch envelope")
            if envelope.get("snapshot_id") != snapshot_id:
                fail(f"records.jsonl line {line_number} references another snapshot")
            if envelope.get("snapshot_manifest_sha256") != snapshot_manifest_sha or envelope.get("crawl_manifest_sha256") != crawl_sha:
                fail(f"records.jsonl line {line_number} has mismatched snapshot provenance")
            record_id = require_string(envelope, "record_id", f"records.jsonl line {line_number}")
            source_id = envelope.get("source_id")
            if source_id is not None and (not isinstance(source_id, str) or not source_id):
                fail(f"records.jsonl line {line_number} has invalid source_id")
            key = (record_type, record_id)
            if key in seen:
                fail(f"records.jsonl repeats {record_type} identity {record_id}")
            seen.add(key)

            if record_type == "snapshot":
                if record.get("schema_version") != "scout.corpus-snapshot.v1" or record_id != snapshot_id or record.get("snapshot_id") != snapshot_id or record.get("status") != "complete":
                    fail("snapshot record identity or status mismatch")
                if record.get("run_id") != run_id:
                    fail("snapshot record run_id mismatch")
                snapshot_record = record
            elif record_type == "source":
                source = require_string(record, "source_id", "source")
                if source != record_id or source_id != source or source in source_ids:
                    fail("Source record identity mismatch or duplicate source_id")
                source_ids.add(source)
            elif record_type == "page":
                page_id = require_string(record, "page_id", "page")
                source = require_string(record, "source_id", "page")
                require_string(record, "normalized_final_url", "page")
                require_string(record, "content_identity", "page")
                body_sha = require_string(record, "body_sha256", "page")
                if not HEX_256.fullmatch(page_id) or not HEX_256.fullmatch(body_sha):
                    fail("Page or body checksum is malformed")
                if page_id != record_id or source != source_id or source not in source_ids:
                    fail("Page identity or Source relationship mismatch")
                validate_content(record, "page", "content_identity")
                page_ids.add(page_id)
                page_sources[page_id] = source
                page_source_pairs.add((source, page_id))
                source_page_counts[source] = source_page_counts.get(source, 0) + 1
            elif record_type == "passage":
                passage_id = require_string(record, "passage_id", "passage")
                source = require_string(record, "source_id", "passage")
                page_id = require_string(record, "page_id", "passage")
                require_string(record, "normalized_final_url", "passage")
                if not HEX_256.fullmatch(passage_id):
                    fail("Passage ID is malformed")
                if passage_id != record_id or source != source_id or source not in source_ids or page_sources.get(page_id) != source:
                    fail("Passage identity or Page/Source relationship mismatch")
                validate_content(record, "passage")
            elif record_type == "page_alias":
                alias_id = require_string(record, "alias_page_id", "page_alias")
                source = require_string(record, "source_id", "page_alias")
                representative = require_string(record, "representative_page_id", "page_alias")
                require_string(record, "alias_url", "page_alias")
                if not HEX_256.fullmatch(alias_id):
                    fail("Page alias ID is malformed")
                if alias_id != record_id or source != source_id or source not in source_ids or page_sources.get(representative) != source:
                    fail("Page alias identity or representative relationship mismatch")
            elif record_type == "crawl_manifest":
                if record_id != run_id or record.get("run_id") != run_id:
                    fail("Crawl Manifest record identity mismatch")
                # The original Rust bytes were checked before export. This
                # record is JSON-normalized with sorted keys, so its bytes no
                # longer match the Rust struct serialization used by the hash.
                if verify_crawl_manifest(record, snapshot_id) != crawl_sha:
                    fail("Crawl Manifest record checksum does not match the batch")
                summary = record.get("summary", {})
                if not isinstance(summary, dict):
                    fail("Crawl Manifest summary must be an object")
                if summary.get("page_count") != manifest["page_count"] or summary.get("passage_count") != manifest["passage_count"] or summary.get("alias_count") != manifest["alias_count"]:
                    fail("Crawl Manifest counts do not match the snapshot")
                crawl_record = record
            elif record_type == "operational":
                if record.get("schema_version") != "scout.operational-event.v1" or record.get("run_id") != run_id:
                    fail("operational event schema or run_id mismatch")
                nested = record.get("data")
                event_schema = nested.get("schema_version") if isinstance(nested, dict) else None
                history_event = record.get("event")
                if event_schema in (PAGE_TOMBSTONE_SCHEMA, SOURCE_COVERAGE_SCHEMA):
                    expected_event = "history.page_tombstone" if event_schema == PAGE_TOMBSTONE_SCHEMA else "history.source_coverage_assertion"
                    if history_event != expected_event or source_id != nested.get("source_id"):
                        fail("history evidence event type or Source envelope mismatch")
                    expected_record_id = "history-evidence-" + sha256_bytes(canonical_json(nested).encode("utf-8"))
                    if record_id != expected_record_id:
                        fail("history evidence record_id is not content-derived")
                    history_records.append(nested)
                elif isinstance(history_event, str) and history_event.startswith("history."):
                    fail("unsupported or malformed history evidence event")

            counts[record_type] += 1
            if sum(counts.values()) > MAX_RECORDS:
                fail(f"batch exceeds the {MAX_RECORDS} record limit")

    if snapshot_record is None or crawl_record is None:
        fail("batch is missing its snapshot or Crawl Manifest provenance record")
    crawl_sources = crawl_record.get("sources")
    if not isinstance(crawl_sources, list):
        fail("Crawl Manifest sources must be a list")
    if {item.get("source_id") for item in crawl_sources if isinstance(item, dict)} != source_ids:
        fail("Source records do not match the Crawl Manifest")
    expected = {key: int(value) for key, value in manifest.get("record_counts", {}).items()}
    if expected != counts or manifest.get("record_count") != sum(counts.values()):
        fail("batch record counts do not match records.jsonl")
    if counts["page"] != manifest["page_count"] or counts["passage"] != manifest["passage_count"] or counts["page_alias"] != manifest["alias_count"]:
        fail("batch Page/Passage/alias counts do not match its snapshot reference")
    for field, record_type in (("page_count", "page"), ("passage_count", "passage"), ("alias_count", "page_alias")):
        if snapshot_record.get(field) != counts[record_type]:
            fail(f"snapshot record {field} does not match its records")
    expected_history_count = int(manifest.get("history_evidence_count", 0))
    if expected_history_count != len(history_records):
        fail("history evidence count does not match operational records")
    if history_evidence is None:
        if history_records or manifest.get("history_evidence_sha256") is not None:
            fail("history evidence records require the immutable history-evidence.json artifact")
    else:
        validated = validate_history_evidence(
            history_evidence, crawl_record, snapshot_id, snapshot_manifest_sha, crawl_sha,
            page_source_pairs, source_page_counts,
        )
        if canonical_json(history_records) != canonical_json(validated):
            fail("history evidence sidecar differs from its operational batch records")
        evidence_sha = require_string(manifest, "history_evidence_sha256", "batch manifest")
        if not HEX_256.fullmatch(evidence_sha) or manifest.get("source_artifacts", {}).get("history-evidence.json") != evidence_sha:
            fail("history evidence checksum is missing or inconsistent in the batch manifest")
        if len(validated) != expected_history_count:
            fail("history evidence sidecar count does not match the batch manifest")
    return counts


def validate_batch(batch_dir):
    batch_dir = Path(batch_dir).resolve()
    if not batch_dir.is_dir() or any(path.is_symlink() for path in batch_dir.iterdir()):
        fail(f"batch directory is missing or contains a symlink: {batch_dir}")
    names = {path.name for path in batch_dir.iterdir()}
    required_names = {"batch-manifest.json", "records.jsonl", "_PUBLISHED"}
    if not required_names.issubset(names) or names - required_names not in (set(), {"history-evidence.json"}):
        fail("batch must contain exactly the required files and optional history-evidence.json")
    manifest_path = batch_dir / "batch-manifest.json"
    records_path = batch_dir / "records.jsonl"
    marker_path = batch_dir / "_PUBLISHED"
    manifest_bytes = manifest_path.read_bytes()
    if len(manifest_bytes) > 1024 * 1024:
        fail("batch manifest exceeds 1 MiB")
    try:
        manifest = json.loads(manifest_bytes)
        marker = marker_path.read_text(encoding="ascii")
    except (UnicodeError, json.JSONDecodeError, OSError) as error:
        fail(f"cannot parse batch publication files: {error}")
    if not isinstance(manifest, dict) or manifest.get("schema_version") != SCHEMA:
        fail("unsupported or malformed batch manifest schema")
    if manifest.get("records_file") != "records.jsonl":
        fail("unsupported records file name")
    snapshot_id = require_string(manifest, "snapshot_id", "batch manifest")
    if not SNAPSHOT_ID.fullmatch(snapshot_id):
        fail("batch manifest snapshot_id is malformed")
    source_manifest_sha = require_string(manifest, "source_snapshot_manifest_sha256", "batch manifest")
    crawl_sha = require_string(manifest, "crawl_manifest_sha256", "batch manifest")
    require_string(manifest, "run_id", "batch manifest")
    if not HEX_256.fullmatch(source_manifest_sha) or not HEX_256.fullmatch(crawl_sha):
        fail("batch manifest contains malformed source checksums")
    artifacts = manifest.get("source_artifacts")
    required_artifacts = {"snapshot-manifest.json", "crawl-manifest.json", "pages.jsonl", "passages.jsonl", "aliases.jsonl"}
    allowed_artifacts = required_artifacts | {"operational-log.jsonl", "history-evidence.json"}
    if (not isinstance(artifacts, dict) or not required_artifacts.issubset(artifacts)
            or not set(artifacts).issubset(allowed_artifacts)
            or any(not isinstance(value, str) or not HEX_256.fullmatch(value) for value in artifacts.values())):
        fail("batch manifest source_artifacts is malformed")
    crawl_file_sha = manifest.get("crawl_manifest_file_sha256", artifacts.get("crawl-manifest.json"))
    crawl_file_bytes = manifest.get("crawl_manifest_file_bytes")
    if (artifacts["snapshot-manifest.json"] != source_manifest_sha
            or not isinstance(crawl_file_sha, str) or not HEX_256.fullmatch(crawl_file_sha)
            or artifacts["crawl-manifest.json"] != crawl_file_sha
            or (crawl_file_bytes is not None
                and (isinstance(crawl_file_bytes, bool) or not isinstance(crawl_file_bytes, int)
                     or crawl_file_bytes < 0))):
        fail("batch manifest source artifact checksums conflict with its provenance")
    history_evidence = None
    evidence_path = batch_dir / "history-evidence.json"
    if evidence_path.exists():
        evidence_bytes = evidence_path.read_bytes()
        evidence_sha = require_string(manifest, "history_evidence_sha256", "batch manifest")
        if (not HEX_256.fullmatch(evidence_sha) or sha256_bytes(evidence_bytes) != evidence_sha
                or artifacts.get("history-evidence.json") != evidence_sha):
            fail("history-evidence.json checksum does not match the batch manifest")
        try:
            history_evidence = json.loads(evidence_bytes)
        except (UnicodeError, json.JSONDecodeError) as error:
            fail(f"cannot parse history-evidence.json: {error}")
        if not isinstance(history_evidence, dict):
            fail("history-evidence.json must contain an object")
    elif "history_evidence_sha256" in manifest or "history-evidence.json" in artifacts:
        fail("batch manifest declares history evidence but the immutable artifact is missing")
    raw_manifest_sha = manifest.get("crawl_manifest_file_sha256")
    if ("crawl_manifest_file_sha256" in manifest) != ("crawl_manifest_file_bytes" in manifest):
        fail("batch manifest has an incomplete raw Crawl Manifest identity")
    batch_identity = f"{SCHEMA}\0{snapshot_id}\0{source_manifest_sha}"
    if "crawl_manifest_file_sha256" in manifest:
        batch_identity += f"\0{raw_manifest_sha}"
    if history_evidence is not None:
        batch_identity += f"\0{manifest['history_evidence_sha256']}"
    expected_batch = "batch-" + sha256_bytes(batch_identity.encode())
    if manifest.get("batch_id") != expected_batch:
        fail("batch_id is not the stable identity of this source snapshot and evidence")
    if not HEX_256.fullmatch(require_string(manifest, "batch_id", "batch manifest")[6:]):
        fail("batch_id is malformed")
    if manifest_bytes.endswith(b"\n"):
        fail("batch manifest has unexpected trailing bytes")
    manifest_sha = sha256_bytes(manifest_bytes)
    if marker != manifest_sha + "\n":
        fail("publication marker does not match batch manifest")
    records_bytes = records_path.stat().st_size
    evidence_bytes = evidence_path.stat().st_size if evidence_path.exists() else 0
    if records_bytes + evidence_bytes > MAX_BATCH_BYTES or records_bytes + evidence_bytes + len(manifest_bytes) + len(marker.encode("ascii")) > MAX_BATCH_BYTES:
        fail(f"batch exceeds the {MAX_BATCH_BYTES}-byte limit")
    if manifest.get("records_bytes") != records_bytes or manifest.get("records_sha256") != sha256_file(records_path):
        fail("records.jsonl size or checksum mismatch")
    if manifest.get("max_batch_bytes") != MAX_BATCH_BYTES or manifest.get("max_records") != MAX_RECORDS:
        fail("batch uses an unsupported size limit")
    if not isinstance(manifest.get("record_counts"), dict) or any(key not in RECORD_TYPES for key in manifest["record_counts"]):
        fail("batch manifest record_counts is malformed")
    validate_envelopes(records_path, manifest, history_evidence)
    return manifest, manifest_sha


def read_jsonl(path, label):
    with path.open("rb") as stream:
        for number, line in enumerate(stream, start=1):
            if not line.strip():
                fail(f"{label} line {number} is empty")
            try:
                record = json.loads(line)
            except (UnicodeError, json.JSONDecodeError) as error:
                fail(f"{label} line {number} is invalid JSON: {error}")
            if not isinstance(record, dict):
                fail(f"{label} line {number} must be a JSON object")
            yield record


def export_snapshot(data_dir, snapshot_id, output_root, history_evidence_path=None):
    if not SNAPSHOT_ID.fullmatch(snapshot_id):
        fail("--snapshot-id must be snapshot- followed by 64 lowercase hex characters")
    data_dir = Path(data_dir).expanduser().resolve()
    snapshot_dir = data_dir / "corpora" / snapshot_id
    snapshot_path = snapshot_dir / "snapshot-manifest.json"
    if not snapshot_path.is_file():
        fail(f"Corpus snapshot does not exist: {snapshot_dir}")
    snapshot = read_json(snapshot_path, "snapshot manifest")
    if snapshot.get("schema_version") != "scout.corpus-snapshot.v1" or snapshot.get("snapshot_id") != snapshot_id or snapshot.get("status") != "complete":
        fail("Corpus snapshot is incomplete, unsupported, or has a mismatched ID")
    run_id = require_string(snapshot, "run_id", "snapshot manifest")
    if not re.fullmatch(r"run-[a-f0-9]{64}", run_id):
        fail("snapshot run_id is malformed")
    crawl_path = data_dir / "crawls" / run_id / "crawl-manifest.json"
    crawl = read_json(crawl_path, "Crawl Manifest")
    crawl_sha = verify_crawl_manifest(crawl, snapshot_id, crawl_path.read_bytes())
    if snapshot.get("crawl_manifest_sha256") != crawl_sha or snapshot.get("source_config_sha256") != crawl.get("source_config_sha256"):
        fail("snapshot and Crawl Manifest provenance mismatch")
    summary = crawl.get("summary")
    if not isinstance(summary, dict):
        fail("Crawl Manifest summary is missing")
    for field in ("page_count", "passage_count", "alias_count"):
        if snapshot.get(field) != summary.get(field):
            fail(f"snapshot and Crawl Manifest {field} mismatch")

    source_files = {
        "snapshot-manifest.json": snapshot_path,
        "crawl-manifest.json": crawl_path,
    }
    for file_name, manifest_key in (("pages.jsonl", "pages_jsonl_sha256"), ("passages.jsonl", "passages_jsonl_sha256"), ("aliases.jsonl", "aliases_jsonl_sha256")):
        path = snapshot_dir / file_name
        if not path.is_file() or sha256_file(path) != snapshot.get(manifest_key):
            fail(f"Corpus snapshot checksum mismatch: {file_name}")
        source_files[file_name] = path
    event_path = data_dir / "logs" / f"{run_id}.jsonl"
    if event_path.is_file():
        source_files["operational-log.jsonl"] = event_path
    history_evidence = None
    history_evidence_bytes = None
    if history_evidence_path is not None:
        evidence_path = Path(history_evidence_path).expanduser().resolve()
        try:
            history_evidence_bytes = evidence_path.read_bytes()
            history_evidence = json.loads(history_evidence_bytes)
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            fail(f"cannot read history evidence at {evidence_path}: {error}")
        if not isinstance(history_evidence, dict):
            fail("history evidence must be a JSON object")
        source_files["history-evidence.json"] = evidence_path
    total_input_bytes = sum(path.stat().st_size for path in source_files.values())
    if total_input_bytes > MAX_BATCH_BYTES:
        fail(f"source files exceed the {MAX_BATCH_BYTES}-byte batch limit")

    source_manifest_sha = sha256_file(snapshot_path)
    history_evidence_sha = sha256_bytes(history_evidence_bytes) if history_evidence_bytes is not None else ""
    crawl_file_sha = sha256_file(crawl_path)
    batch_identity = f"{SCHEMA}\0{snapshot_id}\0{source_manifest_sha}\0{crawl_file_sha}"
    if history_evidence_sha:
        batch_identity += f"\0{history_evidence_sha}"
    batch_id = "batch-" + sha256_bytes(batch_identity.encode())
    output_root = Path(output_root).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    output_dir = output_root / batch_id
    staging = Path(tempfile.mkdtemp(prefix=f".{batch_id}.", dir=output_root))
    records_path = staging / "records.jsonl"
    counts = {record_type: 0 for record_type in RECORD_TYPES}
    source_ids = set()
    page_source_pairs = set()
    source_page_counts = {}

    try:
        with records_path.open("wb") as output:
            append_envelope(output, batch_id, snapshot_id, source_manifest_sha, crawl_sha, "snapshot", snapshot_id, None, snapshot)
            counts["snapshot"] += 1
            for source in crawl.get("sources", []):
                source_id = require_string(source, "source_id", "Crawl Manifest Source")
                if source_id in source_ids:
                    fail(f"duplicate Source ID: {source_id}")
                source_ids.add(source_id)
                append_envelope(output, batch_id, snapshot_id, source_manifest_sha, crawl_sha, "source", source_id, source_id, source)
                counts["source"] += 1

            page_ids = set()
            for page in read_jsonl(source_files["pages.jsonl"], "pages.jsonl"):
                page_id = require_string(page, "page_id", "page")
                source_id = require_string(page, "source_id", "page")
                if page_id in page_ids or source_id not in source_ids:
                    fail("duplicate Page ID or unknown Page Source")
                require_string(page, "normalized_final_url", "page")
                require_string(page, "content_identity", "page")
                require_string(page, "body_file", "page")
                require_string(page, "body_sha256", "page")
                validate_content(page, "page", "content_identity")
                if not HEX_256.fullmatch(page_id) or not HEX_256.fullmatch(page["body_sha256"]):
                    fail("Page or body checksum is malformed")
                page_ids.add(page_id)
                page_source_pairs.add((source_id, page_id))
                source_page_counts[source_id] = source_page_counts.get(source_id, 0) + 1
                append_envelope(output, batch_id, snapshot_id, source_manifest_sha, crawl_sha, "page", page_id, source_id, page)
                counts["page"] += 1

            for passage in read_jsonl(source_files["passages.jsonl"], "passages.jsonl"):
                passage_id = require_string(passage, "passage_id", "passage")
                page_id = require_string(passage, "page_id", "passage")
                source_id = require_string(passage, "source_id", "passage")
                if page_id not in page_ids or source_id not in source_ids:
                    fail("Passage references an unknown Page or Source")
                validate_content(passage, "passage")
                if not HEX_256.fullmatch(passage_id):
                    fail("Passage ID is malformed")
                append_envelope(output, batch_id, snapshot_id, source_manifest_sha, crawl_sha, "passage", passage_id, source_id, passage)
                counts["passage"] += 1

            for alias in read_jsonl(source_files["aliases.jsonl"], "aliases.jsonl"):
                alias_id = require_string(alias, "alias_page_id", "page alias")
                source_id = require_string(alias, "source_id", "page alias")
                representative = require_string(alias, "representative_page_id", "page alias")
                require_string(alias, "alias_url", "page alias")
                if source_id not in source_ids or representative not in page_ids:
                    fail("Page alias references an unknown Source or representative Page")
                append_envelope(output, batch_id, snapshot_id, source_manifest_sha, crawl_sha, "page_alias", alias_id, source_id, alias)
                counts["page_alias"] += 1

            append_envelope(output, batch_id, snapshot_id, source_manifest_sha, crawl_sha, "crawl_manifest", run_id, None, crawl)
            counts["crawl_manifest"] += 1
            if "operational-log.jsonl" in source_files:
                for line_number, event in enumerate(read_jsonl(event_path, "operational log"), start=1):
                    if event.get("schema_version") != "scout.operational-event.v1" or event.get("run_id") != run_id:
                        fail(f"operational event line {line_number} has unsupported schema or run_id")
                    event_id = sha256_bytes(f"{line_number}\0".encode() + canonical_json(event).encode("utf-8"))
                    append_envelope(output, batch_id, snapshot_id, source_manifest_sha, crawl_sha, "operational", event_id, None, event)
                    counts["operational"] += 1
            history_records = []
            if history_evidence is not None:
                history_records = validate_history_evidence(
                    history_evidence, crawl, snapshot_id, source_manifest_sha, crawl_sha,
                    page_source_pairs, source_page_counts,
                )
                for evidence in history_records:
                    event_schema = evidence["schema_version"]
                    timestamp = evidence["tombstoned_at_utc"] if event_schema == PAGE_TOMBSTONE_SCHEMA else evidence["asserted_at_utc"]
                    event = {
                        "schema_version": "scout.operational-event.v1",
                        "timestamp": timestamp,
                        "level": "info",
                        "event": "history.page_tombstone" if event_schema == PAGE_TOMBSTONE_SCHEMA else "history.source_coverage_assertion",
                        "component": "scout-history-evidence",
                        "run_id": run_id,
                        "outcome": "success",
                        "data": evidence,
                    }
                    record_id = "history-evidence-" + sha256_bytes(canonical_json(evidence).encode("utf-8"))
                    append_envelope(output, batch_id, snapshot_id, source_manifest_sha, crawl_sha,
                                    "operational", record_id, evidence["source_id"], event)
                    counts["operational"] += 1
            output.flush()
            os.fsync(output.fileno())

        if counts["page"] != snapshot.get("page_count") or counts["passage"] != snapshot.get("passage_count") or counts["page_alias"] != snapshot.get("alias_count"):
            fail("Corpus JSONL counts do not match snapshot manifest")
        if sum(counts.values()) > MAX_RECORDS:
            fail(f"snapshot exceeds the {MAX_RECORDS} record limit")
        records_bytes = records_path.stat().st_size
        records_sha = sha256_file(records_path)
        if history_evidence_bytes is not None:
            (staging / "history-evidence.json").write_bytes(history_evidence_bytes)
        manifest = {
            "schema_version": SCHEMA,
            "batch_id": batch_id,
            "snapshot_id": snapshot_id,
            "snapshot_schema_version": snapshot["schema_version"],
            "source_snapshot_manifest_sha256": source_manifest_sha,
            "crawl_manifest_sha256": crawl_sha,
            "crawl_manifest_file_sha256": crawl_file_sha,
            "crawl_manifest_file_bytes": crawl_path.stat().st_size,
            "run_id": run_id,
            "created_at_utc": snapshot.get("created_at_utc"),
            "records_file": "records.jsonl",
            "records_sha256": records_sha,
            "records_bytes": records_bytes,
            "record_count": sum(counts.values()),
            "record_counts": counts,
            "page_count": snapshot["page_count"],
            "passage_count": snapshot["passage_count"],
            "alias_count": snapshot["alias_count"],
            "source_artifacts": {name: sha256_file(path) for name, path in source_files.items()},
            "max_batch_bytes": MAX_BATCH_BYTES,
            "max_records": MAX_RECORDS,
        }
        if history_evidence_bytes is not None:
            manifest["history_evidence_sha256"] = sha256_bytes(history_evidence_bytes)
            manifest["history_evidence_count"] = len(history_records)
        manifest_bytes = pretty_json(manifest)
        evidence_bytes = len(history_evidence_bytes) if history_evidence_bytes is not None else 0
        if records_bytes + evidence_bytes + len(manifest_bytes) + 65 > MAX_BATCH_BYTES:
            fail(f"export exceeds the {MAX_BATCH_BYTES}-byte batch limit")
        (staging / "batch-manifest.json").write_bytes(manifest_bytes)
        (staging / "_PUBLISHED").write_text(sha256_bytes(manifest_bytes) + "\n", encoding="ascii")
        validate_batch(staging)
        if output_dir.exists():
            validate_batch(output_dir)
            staged_names = {path.name for path in staging.iterdir()}
            existing_names = {path.name for path in output_dir.iterdir()}
            if staged_names != existing_names or any(
                (staging / name).read_bytes() != (output_dir / name).read_bytes()
                for name in staged_names
            ):
                fail(f"immutable export identity conflicts with existing content: {output_dir}")
            shutil.rmtree(staging)
            return output_dir
        os.rename(staging, output_dir)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise

    return output_dir


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    export = subparsers.add_parser("export", help="validate and export one complete Corpus snapshot")
    export.add_argument("--data-dir", required=True, help="Scout data directory")
    export.add_argument("--snapshot-id", required=True)
    export.add_argument("--output-root", required=True, help="directory in which batch-id folder is created")
    export.add_argument("--history-evidence", help="optional versioned Source/Page tombstone or coverage assertion JSON")
    validate = subparsers.add_parser("validate", help="validate an immutable exported batch")
    validate.add_argument("--batch-dir", required=True)
    upload = subparsers.add_parser("upload", help="upload files to a Unity Catalog Volume; marker is uploaded last")
    upload.add_argument("--batch-dir", required=True)
    upload.add_argument("--volume", required=True)
    upload.add_argument("--profile", help="Databricks CLI profile; defaults to DEFAULT")
    run = subparsers.add_parser("run", help="submit one bounded serverless ingestion run")
    run.add_argument("--batch-dir", required=True)
    run.add_argument("--volume", required=True)
    run.add_argument("--remote-path", required=True, help="published path printed by upload")
    run.add_argument("--catalog", required=True)
    run.add_argument("--schema", required=True)
    run.add_argument("--volume-name", required=True)
    run.add_argument("--workspace-path", required=True, help="absolute path such as /Users/name@example.com/scout-snapshot-ingest")
    run.add_argument("--profile", help="Databricks CLI profile; defaults to DEFAULT")
    args = parser.parse_args()
    try:
        if args.command == "export":
            print(export_snapshot(args.data_dir, args.snapshot_id, args.output_root, args.history_evidence))
        elif args.command == "validate":
            manifest, manifest_sha = validate_batch(args.batch_dir)
            print(f"valid batch={manifest['batch_id']} snapshot={manifest['snapshot_id']} records={manifest['record_count']} manifest_sha256={manifest_sha}")
        elif args.command == "upload":
            manifest, _ = validate_batch(args.batch_dir)
            upload_batch(args.batch_dir, args.volume, args.profile, manifest)
        elif args.command == "run":
            manifest, _ = validate_batch(args.batch_dir)
            submit_batch(args.batch_dir, args.volume, args.remote_path, args.catalog, args.schema, args.volume_name, args.workspace_path, args.profile, manifest)
    except (OSError, ValueError, KeyError, TypeError, subprocess.CalledProcessError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
