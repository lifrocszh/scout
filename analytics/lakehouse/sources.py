"""Inputs to the lakehouse: immutable Corpus snapshots and durable crawl events.

Snapshots are read through the Databricks snapshot batch format
(``scout.databricks-snapshot-batch.v1``) and its validator in
``scripts/databricks/snapshot_batch.py``, so both analytics paths accept and
reject exactly the same input. A snapshot can be given as an exported batch
directory, or as a Scout data directory plus snapshot ID, which is exported to
a temporary batch first.

Crawl events are transport-neutral: every source yields ``CrawlEventRecord``s
holding the same ``{"event_type": ..., "data": {...}}`` payload that the Rust
``CrawlEvent`` type serializes. Two transports exist today:

- ``JsonlEventSource``: the stdout of ``scout events consume`` (one event per line).
- ``EventBatchSource``: published ``scout events export`` batch directories,
  validated with ``scripts/databricks/crawl_event_batch.py``.

A Kafka source (issue #13) only has to implement ``EventSource.records()``
with the topic's message values as payloads; loading, quality checks and the
tables do not change.
"""

import hashlib
import json
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, Optional, Protocol

ROOT = Path(__file__).resolve().parents[2]
DATABRICKS = ROOT / "scripts" / "databricks"
if str(DATABRICKS) not in sys.path:
    sys.path.insert(0, str(DATABRICKS))

import crawl_event_batch  # noqa: E402  (shared validator, stdlib only)
import snapshot_batch  # noqa: E402  (shared exporter and validator, stdlib only)


class SourceError(ValueError):
    """An input is missing, unpublished, corrupt or fails the shared batch validator."""


def sha256(data):
    return hashlib.sha256(data).hexdigest()


@dataclass(frozen=True)
class SnapshotInput:
    """One validated snapshot batch: its manifest, envelopes, and optional sidecars."""

    manifest: dict
    envelopes: list
    near_duplicates: Optional[dict]
    input_kind: str
    input_ref: str

    @property
    def snapshot_id(self):
        return self.manifest["snapshot_id"]

    @property
    def run_id(self):
        return self.manifest["run_id"]

    @property
    def batch_id(self):
        return self.manifest["batch_id"]


def _read_envelopes(batch_dir):
    with (Path(batch_dir) / "records.jsonl").open("rb") as stream:
        return [json.loads(line) for line in stream if line.strip()]


def _read_near_duplicates(path):
    if path is None or not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise SourceError(f"cannot read near-duplicate report {path}: {error}") from error


def snapshot_from_batch(batch_dir, near_duplicates=None):
    """A published snapshot batch directory (``snapshot_batch.py export`` output)."""
    batch_dir = Path(batch_dir).resolve()
    try:
        manifest, _ = snapshot_batch.validate_batch(batch_dir)
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise SourceError(f"snapshot batch {batch_dir} is invalid: {error}") from error
    report = _read_near_duplicates(Path(near_duplicates) if near_duplicates else None)
    return SnapshotInput(
        manifest, _read_envelopes(batch_dir), report, "snapshot_batch", str(batch_dir)
    )


def snapshot_from_data_dir(data_dir, snapshot_id):
    """A sealed snapshot in a Scout data directory, plus its near-duplicate report."""
    data_dir = Path(data_dir).resolve()
    with tempfile.TemporaryDirectory(prefix="scout-lakehouse-export-") as staging:
        try:
            batch_dir = snapshot_batch.export_snapshot(data_dir, snapshot_id, staging)
        except (OSError, ValueError, KeyError, TypeError) as error:
            raise SourceError(
                f"snapshot {snapshot_id} in {data_dir} is invalid: {error}"
            ) from error
        loaded = snapshot_from_batch(batch_dir)
    report_path = data_dir / "crawls" / loaded.run_id / "near-duplicates.json"
    return SnapshotInput(
        loaded.manifest,
        loaded.envelopes,
        _read_near_duplicates(report_path),
        "data_dir",
        f"{data_dir}#{snapshot_id}",
    )


def snapshot_ids(data_dir):
    """Complete snapshot IDs in a data directory, oldest first by creation time."""
    found = []
    for manifest in sorted((Path(data_dir) / "corpora").glob("snapshot-*/snapshot-manifest.json")):
        value = json.loads(manifest.read_text(encoding="utf-8"))
        if value.get("status") == "complete":
            found.append((value.get("created_at_utc", ""), value["snapshot_id"]))
    return [snapshot_id for _, snapshot_id in sorted(found)]


@dataclass(frozen=True)
class CrawlEventRecord:
    """One durable crawl event as delivered by a transport.

    ``payload_json`` is the serialized ``CrawlEvent``; ``transport_ref`` says where
    it came from (file and line, batch and line, or topic/partition/offset).
    """

    payload_json: str
    transport: str
    transport_ref: str
    declared: dict = field(default_factory=dict)

    @property
    def payload_sha256(self):
        return sha256(self.payload_json.encode("utf-8"))


class EventSource(Protocol):
    """Anything that yields crawl events. Delivery may be at least once."""

    def records(self) -> Iterator[CrawlEventRecord]: ...


class JsonlEventSource:
    """``scout events consume`` output: one ``CrawlEvent`` JSON object per line."""

    def __init__(self, path):
        self.path = path

    def records(self):
        stream = sys.stdin if str(self.path) == "-" else open(self.path, encoding="utf-8")
        name = "stdin" if str(self.path) == "-" else str(Path(self.path).resolve())
        try:
            for number, line in enumerate(stream, start=1):
                if line.strip():
                    yield CrawlEventRecord(line.rstrip("\n"), "jsonl", f"{name}:{number}")
        finally:
            if stream is not sys.stdin:
                stream.close()


class EventBatchSource:
    """Published ``scout events export`` batches (``crawl-event-batches/<run>/<batch>/``).

    Accepts one batch directory or a directory of them. Quarantined rows are
    skipped: the exporter already set them aside, and their full payload stays in
    Postgres.
    """

    def __init__(self, path):
        self.path = Path(path).resolve()

    def batch_dirs(self):
        if (self.path / "batch-manifest.json").is_file():
            return [self.path]
        found = sorted(path.parent for path in self.path.rglob("_PUBLISHED"))
        if not found:
            raise SourceError(f"no published crawl-event batches under {self.path}")
        return found

    def records(self):
        for batch_dir in self.batch_dirs():
            try:
                manifest = crawl_event_batch.validate_batch(batch_dir)
            except (OSError, ValueError, KeyError, TypeError) as error:
                raise SourceError(f"crawl-event batch {batch_dir} is invalid: {error}") from error
            lines = (batch_dir / "records.jsonl").read_text(encoding="utf-8").splitlines()
            for number, line in enumerate(lines, start=1):
                envelope = json.loads(line)
                if envelope.get("record_type") != "crawl_event":
                    continue
                yield CrawlEventRecord(
                    envelope["record_json"],
                    "event_batch",
                    f"{manifest['batch_id']}:{number}",
                    declared={
                        "event_id": envelope.get("record_id"),
                        "event_type": envelope.get("event_type"),
                        "crawl_run_id": envelope.get("crawl_run_id"),
                        "payload_sha256": envelope.get("payload_sha256"),
                    },
                )
