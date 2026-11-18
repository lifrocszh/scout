"""The committed fixture crawl (analytics/fixtures) and deliberate breakages of it.

``analytics/fixtures/build_fixture.py`` regenerates the fixture from two real
``scout crawl`` runs. ``broken_copy`` writes a copy with one defect that the
shared batch validator accepts but a data-quality rule must reject.
"""

import hashlib
import json
import shutil
from datetime import timedelta
from pathlib import Path

from . import sources
from .transform import parse_utc

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
DATA_DIR = FIXTURES / "data"
EVENTS = FIXTURES / "events.jsonl"
BREAKAGES = ("schema-drift", "event-conflict", "event-schema", "stale")


def snapshot_ids(data_dir=DATA_DIR):
    return sources.snapshot_ids(data_dir)


def latest_snapshot_at(data_dir=DATA_DIR):
    created = [json.loads(path.read_text(encoding="utf-8"))["created_at_utc"]
               for path in Path(data_dir).glob("corpora/*/snapshot-manifest.json")]
    return max(parse_utc(value) for value in created)


def fixture_as_of(data_dir=DATA_DIR):
    """A clock one hour after the newest fixture snapshot, so freshness is stable in CI."""
    return latest_snapshot_at(data_dir) + timedelta(hours=1)


def _reseal_pages(snapshot_dir, rewrite):
    """Rewrite pages.jsonl and update its checksum in the snapshot manifest."""
    pages = snapshot_dir / "pages.jsonl"
    lines = [json.dumps(rewrite(json.loads(line)), ensure_ascii=False, separators=(",", ":"))
             for line in pages.read_text(encoding="utf-8").splitlines() if line.strip()]
    pages.write_text("\n".join(lines) + "\n", encoding="utf-8")
    manifest_path = snapshot_dir / "snapshot-manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["pages_jsonl_sha256"] = hashlib.sha256(pages.read_bytes()).hexdigest()
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def broken_copy(kind, target):
    """Copy the fixture to ``target`` with one defect. Returns (data_dir, events_path, as_of)."""
    if kind not in BREAKAGES:
        raise ValueError(f"unknown breakage {kind!r}; choose from {', '.join(BREAKAGES)}")
    target = Path(target)
    data_dir = target / "data"
    shutil.copytree(DATA_DIR, data_dir)
    events = target / "events.jsonl"
    lines = EVENTS.read_text(encoding="utf-8").splitlines()
    as_of = fixture_as_of()
    if kind == "schema-drift":
        # A new Page field appears upstream: the batch still validates, the lakehouse must not
        # silently drop it.
        snapshot_dir = data_dir / "corpora" / snapshot_ids(data_dir)[-1]
        _reseal_pages(snapshot_dir, lambda page: dict(page, fetched_at_utc="2026-01-01T00:00:00Z"))
    elif kind == "event-conflict":
        # At-least-once redelivery is fine, but not with a different payload for the same event ID.
        event = json.loads(lines[0])
        event["data"]["content_sha256"] = "0" * 64
        lines.append(json.dumps(event, separators=(",", ":"), sort_keys=True))
    elif kind == "event-schema":
        event = json.loads(lines[0])
        del event["data"]["source_id"]
        lines[0] = json.dumps(event, separators=(",", ":"), sort_keys=True)
    elif kind == "stale":
        as_of = latest_snapshot_at() + timedelta(days=30)
    events.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return data_dir, events, as_of


def load_all(lake, data_dir=DATA_DIR, events=EVENTS, freshness_sla_hours=None, as_of=None):
    """Load every fixture snapshot, then its events. Returns the load reports."""
    from . import load
    reports = []
    for snapshot_id in snapshot_ids(data_dir):
        snapshot = sources.snapshot_from_data_dir(data_dir, snapshot_id)
        reports.append(load.load_snapshot(lake, snapshot, freshness_sla_hours, as_of))
    reports.append(load.load_events(lake, sources.JsonlEventSource(events), str(events),
                                    freshness_sla_hours, as_of))
    return reports

