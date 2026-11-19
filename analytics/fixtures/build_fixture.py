#!/usr/bin/env python3
"""Rebuild the committed lakehouse fixture from two real `scout crawl` runs.

The fixture is two Corpus snapshots of the same two Sources on two Origins:

  v1  docs (4 Pages) and reference (3 Pages)
  v2  one docs Page edited, one removed, one near-duplicate Page added, and one
      exact-duplicate reference Page added (it becomes a Page alias)

Both crawls write into analytics/fixtures/data. Only the files the lakehouse
reads are kept: the snapshot manifest and JSONL, the Crawl Manifest, the
near-duplicate report. Body files and operational logs are dropped.

The durable crawl-event feed needs the distributed stack (Postgres), so this
script derives `events.jsonl`, in the format `scout events consume` prints,
from each crawl's manifests: one capture_committed per captured Page, then
crawl_completed and corpus_snapshot_published. The events are synthetic but use
the real event schema and the crawl's real IDs and checksums.

Usage: analytics/fixtures/build_fixture.py [--scout target/debug/scout]
"""

import argparse
import hashlib
import http.server
import json
import random
import shutil
import subprocess
import sys
import tempfile
import threading
from functools import partial
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = Path(__file__).resolve().parent
DATA = FIXTURES / "data"
PORTS = {"docs": 18111, "reference": 18112}

VOCABULARY = (
    "scout crawler index page passage source origin snapshot manifest corpus generation "
    "query search keyword semantic hybrid ranking relevance judgment frontier lease worker "
    "coordinator postgres outbox event consumer checkpoint replay batch spool capture body "
    "robots politeness spacing throttle retry backoff timeout redirect canonical alias "
    "duplicate fingerprint shingle hamming band candidate threshold report metric gauge "
    "histogram counter latency throughput budget quota deadline schedule cron release "
    "deploy chart helm cluster node pod container image volume secret config profile "
    "model embedding vector dimension cosine tokenizer runtime provider batch thread "
    "memory disk file directory checksum digest hash signature seal verify activate "
    "retire integrity schema version migration column table row partition commit log "
    "lineage freshness quality rule check failure alert threshold window interval"
).split()


def words(seed, count):
    rng = random.Random(seed)
    return [rng.choice(VOCABULARY) for _ in range(count)]


def paragraph_html(tokens, size=60):
    return "\n".join(
        f"<p>{' '.join(tokens[i : i + size])}.</p>" for i in range(0, len(tokens), size)
    )


def page(title, tokens, links=()):
    nav = "".join(f'<li><a href="{href}">{href}</a></li>' for href in links)
    return (
        '<!doctype html>\n<html lang="en"><head><meta charset="utf-8">'
        f"<title>{title}</title></head><body><main>\n<h1>{title}</h1>\n"
        f"{paragraph_html(tokens)}\n<ul>{nav}</ul>\n</main></body></html>\n"
    )


def site(version):
    """Return {origin_name: {path: html}} for one site version."""
    configure = words("docs/configure", 420)
    docs_links = ["/docs/install.html", "/docs/configure.html"]
    if version == 1:
        docs_links.append("/docs/upgrade.html")
    else:
        docs_links.append("/docs/configure-legacy.html")
    docs = {
        "docs/index.html": page("Docs", words("docs/index", 300), docs_links),
        "docs/install.html": page("Install", words("docs/install", 350)),
    }
    if version == 1:
        docs["docs/configure.html"] = page("Configure", configure)
        docs["docs/upgrade.html"] = page("Upgrade", words("docs/upgrade", 320))
    else:
        edited = list(configure) + words("docs/configure/v2", 80)
        docs["docs/configure.html"] = page("Configure", edited)
        legacy = list(edited)
        legacy[100] = "legacy"
        legacy[300] = "deprecated"
        docs["docs/configure-legacy.html"] = page("Configure", legacy)
    cli = page("CLI", words("reference/cli", 380))
    ref_links = ["/reference/cli.html", "/reference/api.html"]
    if version == 2:
        ref_links.append("/reference/cli-mirror.html")
    reference = {
        "reference/index.html": page("Reference", words("reference/index", 300), ref_links),
        "reference/cli.html": cli,
        "reference/api.html": page("API", words("reference/api", 360)),
    }
    if version == 2:
        reference["reference/cli-mirror.html"] = cli
    return {"docs": docs, "reference": reference}


SOURCES_TOML = """schema_version = "scout.sources.v1"
contact_url = "https://github.com/lifrocszh/scout"
user_agent = "ScoutLakehouseFixture/0.1 (+https://github.com/lifrocszh/scout)"
embedding_model = "BAAI/bge-small-en-v1.5"
page_target = 50
max_frontier = 100
max_duration_seconds = 60
global_concurrency = 2
origin_concurrency = 2
min_start_spacing_ms = 5

[[sources]]
source_id = "docs"
seeds = ["http://127.0.0.1:{docs}/docs/index.html"]
allowed_origins = ["http://127.0.0.1:{docs}"]
path_prefixes = ["/docs/"]

[[sources]]
source_id = "reference"
seeds = ["http://127.0.0.1:{reference}/reference/index.html"]
allowed_origins = ["http://127.0.0.1:{reference}"]
path_prefixes = ["/reference/"]
"""


class QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *_args):
        pass


def serve(directory, port):
    handler = partial(QuietHandler, directory=str(directory))
    server = http.server.ThreadingHTTPServer(("127.0.0.1", port), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def write_site(root, version):
    for origin, files in site(version).items():
        base = root / origin
        (base).mkdir(parents=True, exist_ok=True)
        (base / "robots.txt").write_text("User-agent: *\nAllow: /\n", encoding="utf-8")
        for path, html in files.items():
            target = base / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(html, encoding="utf-8")


def crawl(scout, config, data_dir):
    result = subprocess.run(
        [scout, "crawl", "--config", str(config), "--data-dir", str(data_dir)],
        check=True,
        capture_output=True,
        text=True,
    )
    for line in result.stdout.splitlines():
        if line.startswith("crawl complete:"):
            fields = dict(part.split("=", 1) for part in line.split() if "=" in part)
            return fields["snapshot_id"]
    raise SystemExit(f"no snapshot ID in crawl output:\n{result.stdout}\n{result.stderr[-2000:]}")


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def artifact(path, uri):
    data = path.read_bytes()
    return {"uri": uri, "sha256": sha256(data), "byte_size": len(data)}


def synthesize_events(data_dir, snapshot_id):
    """Events in `scout events consume` format, derived from one sealed crawl."""
    snapshot_path = data_dir / "corpora" / snapshot_id / "snapshot-manifest.json"
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    run_id = snapshot["run_id"]
    crawl_path = data_dir / "crawls" / run_id / "crawl-manifest.json"
    crawl_manifest = json.loads(crawl_path.read_text(encoding="utf-8"))

    def event_id(kind, key):
        return "event-" + sha256(f"{run_id}\0{kind}\0{key}".encode())

    events = []
    for task_id, entry in enumerate(crawl_manifest["pages"], start=1):
        events.append(
            {
                "event_type": "capture_committed",
                "data": {
                    "event_id": event_id("capture_committed", entry["capture_id"]),
                    "schema_version": 1,
                    "crawl_run_id": run_id,
                    "source_id": entry["source_id"],
                    "task_id": task_id,
                    "committed_at_utc": crawl_manifest["completed_at_utc"],
                    "result_sha256": sha256(json.dumps(entry, sort_keys=True).encode()),
                    "content_sha256": entry["content_sha256"],
                    "body_artifact": {
                        "uri": f"crawls/{run_id}/{entry['body_file']}",
                        "sha256": entry["body_sha256"],
                        "byte_size": entry["content_bytes"],
                    },
                },
            }
        )
    events.append(
        {
            "event_type": "crawl_completed",
            "data": {
                "event_id": event_id("crawl_completed", run_id),
                "schema_version": 1,
                "crawl_run_id": run_id,
                "completed_at_utc": crawl_manifest["completed_at_utc"],
                "stop_reason": crawl_manifest["summary"]["stop_reason"],
                "config_sha256": crawl_manifest["source_config_sha256"],
            },
        }
    )
    events.append(
        {
            "event_type": "corpus_snapshot_published",
            "data": {
                "event_id": event_id("corpus_snapshot_published", snapshot_id),
                "schema_version": 1,
                "crawl_run_id": run_id,
                "snapshot_id": snapshot_id,
                "published_at_utc": snapshot["created_at_utc"],
                "snapshot_manifest": artifact(
                    snapshot_path, f"corpora/{snapshot_id}/snapshot-manifest.json"
                ),
                "crawl_manifest": artifact(crawl_path, f"crawls/{run_id}/crawl-manifest.json"),
                "page_count": snapshot["page_count"],
                "alias_count": snapshot["alias_count"],
                "passage_count": snapshot["passage_count"],
            },
        }
    )
    return run_id, events


def prune(data_dir):
    keep_snapshot = {"snapshot-manifest.json", "pages.jsonl", "passages.jsonl", "aliases.jsonl"}
    keep_crawl = {"crawl-manifest.json", "near-duplicates.json"}
    for child in list(data_dir.iterdir()):
        if child.name not in ("corpora", "crawls"):
            shutil.rmtree(child) if child.is_dir() else child.unlink()
    for parent, keep in ((data_dir / "corpora", keep_snapshot), (data_dir / "crawls", keep_crawl)):
        for entry in parent.iterdir():
            if not entry.is_dir():
                entry.unlink()
                continue
            for child in list(entry.iterdir()):
                if child.name not in keep:
                    shutil.rmtree(child) if child.is_dir() else child.unlink()


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--scout", default=str(ROOT / "target" / "debug" / "scout"))
    args = parser.parse_args()
    if not Path(args.scout).is_file():
        raise SystemExit(f"scout binary not found at {args.scout}; run cargo build --locked first")

    shutil.rmtree(DATA, ignore_errors=True)
    DATA.mkdir(parents=True)
    snapshots = []
    with tempfile.TemporaryDirectory(prefix="scout-lakehouse-fixture-") as work:
        work = Path(work)
        config = work / "sources.toml"
        config.write_text(SOURCES_TOML.format(**PORTS), encoding="utf-8")
        for version in (1, 2):
            root = work / f"site-v{version}"
            write_site(root, version)
            servers = [serve(root / name, port) for name, port in PORTS.items()]
            try:
                snapshots.append(crawl(args.scout, config, DATA))
            finally:
                for server in servers:
                    server.shutdown()
                    server.server_close()

    prune(DATA)
    with (FIXTURES / "events.jsonl").open("w", encoding="utf-8") as stream:
        for snapshot_id in snapshots:
            _, events = synthesize_events(DATA, snapshot_id)
            for event in events:
                stream.write(json.dumps(event, separators=(",", ":"), sort_keys=True) + "\n")
    (FIXTURES / "snapshots.json").write_text(
        json.dumps({"snapshots": snapshots}, indent=2) + "\n", encoding="utf-8"
    )
    print("\n".join(snapshots))
    return 0


if __name__ == "__main__":
    sys.exit(main())
