"""Shape validated inputs into Bronze and Silver rows. Pure functions, no I/O."""

import json
from datetime import datetime, timezone
from urllib.parse import urlsplit

from . import tables as t

# Fields each snapshot record type carries today. A field outside this set is
# schema drift: the quality checks fail so the lakehouse schema gets updated
# on purpose instead of silently dropping a column.
SNAPSHOT_RECORD_FIELDS = {
    "page": {
        "page_id",
        "source_id",
        "normalized_final_url",
        "title",
        "description",
        "language",
        "canonical_url",
        "extraction_method",
        "content",
        "content_identity",
        "content_sha256",
        "body_sha256",
        "body_file",
    },
    "passage": {
        "page_id",
        "passage_id",
        "source_id",
        "normalized_final_url",
        "title",
        "heading_path",
        "ordinal",
        "content",
        "content_sha256",
    },
    "page_alias": {
        "source_id",
        "alias_url",
        "alias_page_id",
        "representative_page_id",
        "alias_kind",
        "requested_url",
        "redirect_chain",
        "canonical_url",
    },
}

# Required payload fields per crawl event type (src/distributed/events.rs).
EVENT_FIELDS = {
    "capture_committed": {
        "event_id": str,
        "schema_version": int,
        "crawl_run_id": str,
        "source_id": str,
        "task_id": int,
        "committed_at_utc": str,
        "result_sha256": str,
        "content_sha256": str,
        "body_artifact": dict,
    },
    "crawl_completed": {
        "event_id": str,
        "schema_version": int,
        "crawl_run_id": str,
        "completed_at_utc": str,
        "stop_reason": str,
        "config_sha256": str,
    },
    "corpus_snapshot_published": {
        "event_id": str,
        "schema_version": int,
        "crawl_run_id": str,
        "snapshot_id": str,
        "published_at_utc": str,
        "snapshot_manifest": dict,
        "crawl_manifest": dict,
        "page_count": int,
        "alias_count": int,
        "passage_count": int,
    },
}
EVENT_TIME_FIELD = {
    "capture_committed": "committed_at_utc",
    "crawl_completed": "completed_at_utc",
    "corpus_snapshot_published": "published_at_utc",
}
CRAWL_EVENT_SCHEMA_VERSION = 1


def parse_utc(value):
    """An RFC 3339 UTC timestamp as an aware datetime (None when absent)."""
    if value is None:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp without a time zone: {value!r}")
    return parsed.astimezone(timezone.utc)


def origin_of(url):
    """Scheme, host and explicit port, as ``src/url.rs::origin_key`` writes them."""
    parts = urlsplit(url)
    host = parts.hostname or ""
    if ":" in host:
        host = f"[{host}]"
    return f"{parts.scheme}://{host}:{parts.port}" if parts.port else f"{parts.scheme}://{host}"


def snapshot_rows(snapshot, loaded_at):
    """{TableSpec: [row]} for one SnapshotInput, plus the parsed records by type."""
    manifest = snapshot.manifest
    sid, run_id, batch_id = snapshot.snapshot_id, snapshot.run_id, snapshot.batch_id
    bronze, records = [], {}
    for number, envelope in enumerate(snapshot.envelopes, start=1):
        bronze.append(
            {
                "batch_id": batch_id,
                "snapshot_id": sid,
                "run_id": run_id,
                "line_number": number,
                "record_type": envelope["record_type"],
                "record_id": envelope["record_id"],
                "source_id": envelope.get("source_id"),
                "snapshot_manifest_sha256": envelope["snapshot_manifest_sha256"],
                "crawl_manifest_sha256": envelope["crawl_manifest_sha256"],
                "record_json": envelope["record_json"],
                "loaded_at_utc": loaded_at,
            }
        )
        records.setdefault(envelope["record_type"], []).append(json.loads(envelope["record_json"]))

    snap = records["snapshot"][0]
    crawl = records["crawl_manifest"][0]
    summary = crawl.get("summary", {})
    rows = {t.BRONZE_SNAPSHOT_RECORDS: bronze}
    rows[t.SILVER_SNAPSHOTS] = [
        {
            "snapshot_id": sid,
            "run_id": run_id,
            "batch_id": batch_id,
            "created_at_utc": parse_utc(snap.get("created_at_utc")),
            "crawl_started_at_utc": parse_utc(crawl.get("started_at_utc")),
            "crawl_completed_at_utc": parse_utc(crawl.get("completed_at_utc")),
            "stop_reason": summary.get("stop_reason"),
            "embedding_model": snap.get("embedding_model"),
            "page_count": snap.get("page_count"),
            "passage_count": snap.get("passage_count"),
            "alias_count": snap.get("alias_count"),
            "captured_page_count": summary.get("captured_page_count"),
            "failed_count": summary.get("failed_count"),
            "rejected_count": summary.get("rejected_count"),
            "source_config_sha256": snap.get("source_config_sha256"),
            "snapshot_manifest_sha256": manifest["source_snapshot_manifest_sha256"],
            "crawl_manifest_sha256": manifest["crawl_manifest_sha256"],
        }
    ]
    rows[t.SILVER_SOURCES] = [
        {
            "snapshot_id": sid,
            "source_id": source.get("source_id"),
            "allowed_origins": ",".join(sorted(source.get("allowed_origins") or [])),
            "scope_json": json.dumps(source, sort_keys=True, separators=(",", ":")),
        }
        for source in records.get("source", [])
    ]
    rows[t.SILVER_PAGES] = [
        {
            "snapshot_id": sid,
            "run_id": run_id,
            "page_id": page.get("page_id"),
            "source_id": page.get("source_id"),
            "origin": origin_of(page.get("normalized_final_url") or ""),
            "normalized_final_url": page.get("normalized_final_url"),
            "title": page.get("title"),
            "language": page.get("language"),
            "extraction_method": page.get("extraction_method"),
            "content_chars": len(page.get("content") or ""),
            "content_sha256": page.get("content_sha256"),
            "body_sha256": page.get("body_sha256"),
        }
        for page in records.get("page", [])
    ]
    rows[t.SILVER_PASSAGES] = [
        {
            "snapshot_id": sid,
            "passage_id": passage.get("passage_id"),
            "page_id": passage.get("page_id"),
            "source_id": passage.get("source_id"),
            "ordinal": passage.get("ordinal"),
            "heading_path": " > ".join(passage.get("heading_path") or []),
            "content_chars": len(passage.get("content") or ""),
            "content_sha256": passage.get("content_sha256"),
        }
        for passage in records.get("passage", [])
    ]
    rows[t.SILVER_PAGE_ALIASES] = [
        {
            "snapshot_id": sid,
            "alias_page_id": alias.get("alias_page_id"),
            "representative_page_id": alias.get("representative_page_id"),
            "source_id": alias.get("source_id"),
            "origin": origin_of(alias.get("alias_url") or ""),
            "alias_url": alias.get("alias_url"),
            "alias_kind": alias.get("alias_kind"),
        }
        for alias in records.get("page_alias", [])
    ]
    report = snapshot.near_duplicates
    rows[t.SILVER_NEAR_DUPLICATE_ORIGINS] = (
        []
        if report is None
        else [
            {
                "snapshot_id": report.get("snapshot_id"),
                "run_id": report.get("run_id"),
                "origin": origin.get("origin"),
                "page_count": origin.get("page_count"),
                "near_duplicate_page_count": origin.get("near_duplicate_page_count"),
                "near_duplicate_rate": origin.get("near_duplicate_rate"),
                "exact_duplicate_alias_count": origin.get("exact_duplicate_alias_count"),
            }
            for origin in report.get("origins", [])
        ]
    )
    return rows, records


def parse_event(record):
    """(event_type, data) of a CrawlEventRecord, or raise ValueError."""
    event = json.loads(record.payload_json)
    if not isinstance(event, dict) or set(event) != {"event_type", "data"}:
        raise ValueError("payload must be an object with exactly event_type and data")
    if not isinstance(event["data"], dict):
        raise ValueError("payload data must be an object")
    return event["event_type"], event["data"]


def event_rows(records, loaded_at):
    """Bronze and Silver rows for crawl events that already passed the event checks."""
    bronze, silver = [], []
    for record in records:
        event_type, data = parse_event(record)
        bronze.append(
            {
                "crawl_run_id": data["crawl_run_id"],
                "event_id": data["event_id"],
                "event_type": event_type,
                "schema_version": data["schema_version"],
                "source_id": data.get("source_id"),
                "payload_sha256": record.payload_sha256,
                "payload_json": record.payload_json,
                "transport": record.transport,
                "transport_ref": record.transport_ref,
                "loaded_at_utc": loaded_at,
            }
        )
        silver.append(
            {
                "crawl_run_id": data["crawl_run_id"],
                "event_id": data["event_id"],
                "event_type": event_type,
                "occurred_at_utc": parse_utc(data[EVENT_TIME_FIELD[event_type]]),
                "source_id": data.get("source_id"),
                "snapshot_id": data.get("snapshot_id"),
                "content_sha256": data.get("content_sha256"),
                "stop_reason": data.get("stop_reason"),
                "page_count": data.get("page_count"),
                "passage_count": data.get("passage_count"),
                "alias_count": data.get("alias_count"),
            }
        )
    return {t.BRONZE_CRAWL_EVENTS: bronze, t.SILVER_CRAWL_EVENTS: silver}
