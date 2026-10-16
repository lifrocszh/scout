#!/usr/bin/env python3
"""Validate, upload, and submit one immutable Scout crawl-event batch."""

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

from transport_state import TransportState, make_scope


SCHEMA = "scout.databricks-crawl-event-batch.v1"
MAX_BATCH_BYTES = 64 * 1024 * 1024
MAX_RECORDS = 100_000
HEX_256 = re.compile(r"^[a-f0-9]{64}$")
BATCH_ID = re.compile(r"^batch-[a-f0-9]{64}$")
RUN_ID = re.compile(r"^run-[a-f0-9]{64}$")
VOLUME = re.compile(r"^dbfs:/Volumes/([A-Za-z_][A-Za-z0-9_]*)/([A-Za-z_][A-Za-z0-9_]*)/([A-Za-z_][A-Za-z0-9_]*)$")


def fail(message):
    raise ValueError(message)


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def read_object(path, label):
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        fail(f"cannot read {label} at {path}: {error}")
    if not isinstance(value, dict):
        fail(f"{label} must be a JSON object")
    return value


def validate_batch(batch_dir):
    batch_dir = Path(batch_dir).resolve()
    if not batch_dir.is_dir():
        fail("--batch-dir must name a published batch directory")
    if {path.name for path in batch_dir.iterdir()} != {"batch-manifest.json", "records.jsonl", "_PUBLISHED"}:
        fail("event batch must contain exactly batch-manifest.json, records.jsonl, and _PUBLISHED")

    manifest_path = batch_dir / "batch-manifest.json"
    if manifest_path.stat().st_size > 1024 * 1024:
        fail("event batch manifest exceeds 1 MiB")
    manifest_bytes = manifest_path.read_bytes()
    manifest = read_object(manifest_path, "event batch manifest")
    manifest_sha = sha256(manifest_bytes)
    try:
        published = (batch_dir / "_PUBLISHED").read_text(encoding="ascii")
    except (OSError, UnicodeError) as error:
        fail(f"cannot read event batch publication marker: {error}")
    if published != manifest_sha + "\n":
        fail("publication marker does not match event batch manifest")

    batch_id = manifest.get("batch_id")
    run_id = manifest.get("crawl_run_id")
    consumer_id = manifest.get("consumer_id")
    if manifest.get("schema_version") != SCHEMA:
        fail("unsupported event batch schema version")
    if not isinstance(batch_id, str) or not BATCH_ID.fullmatch(batch_id):
        fail("invalid event batch ID")
    if not isinstance(run_id, str) or not RUN_ID.fullmatch(run_id):
        fail("invalid crawl_run_id")
    if not isinstance(consumer_id, str) or not consumer_id or len(consumer_id) > 512:
        fail("invalid consumer_id")
    if manifest.get("records_file") != "records.jsonl":
        fail("unsupported event batch payload file")

    max_records = manifest.get("max_records")
    max_bytes = manifest.get("max_batch_bytes")
    record_count = manifest.get("record_count")
    quarantine_count = manifest.get("quarantine_count")
    if any(isinstance(value, bool) or not isinstance(value, int) for value in
           (max_records, max_bytes, record_count, quarantine_count)):
        fail("event batch bounds and counts must be integers")
    if not 1 <= max_records <= MAX_RECORDS or not 1 <= max_bytes <= MAX_BATCH_BYTES:
        fail("event batch declares unsupported record or byte bounds")
    if not 1 <= record_count <= min(max_records, MAX_RECORDS):
        fail("event batch record count exceeds its declared bounds")
    if not 0 <= quarantine_count <= record_count:
        fail("event batch quarantine_count is out of range")

    records_path = batch_dir / "records.jsonl"
    records_bytes = records_path.read_bytes()
    if not 0 < len(records_bytes) <= min(max_bytes, MAX_BATCH_BYTES):
        fail("event batch records exceed their declared byte bounds")
    if manifest.get("records_bytes") != len(records_bytes) or manifest.get("records_sha256") != sha256(records_bytes):
        fail("event batch records size or checksum mismatch")

    record_ids = []
    seen_ids = set()
    actual_quarantine = 0
    for number, raw_line in enumerate(records_bytes.splitlines(), start=1):
        if not raw_line.strip():
            fail(f"records.jsonl line {number} is empty")
        try:
            envelope = json.loads(raw_line)
        except (UnicodeError, json.JSONDecodeError) as error:
            fail(f"records.jsonl line {number} is invalid JSON: {error}")
        if not isinstance(envelope, dict):
            fail(f"records.jsonl line {number} must be a JSON object")
        if envelope.get("schema_version") != SCHEMA or envelope.get("batch_id") != batch_id:
            fail(f"records.jsonl line {number} has incompatible batch identity")
        if envelope.get("crawl_run_id") != run_id:
            fail(f"records.jsonl line {number} crosses crawl_run_id")
        record_id = envelope.get("record_id")
        if (not isinstance(record_id, str) or len(record_id) > 512
                or (not record_id and envelope.get("record_type") != "quarantine")):
            fail(f"records.jsonl line {number} has invalid record_id")
        if record_id in seen_ids:
            fail(f"records.jsonl repeats event identity {record_id}")
        seen_ids.add(record_id)
        if not isinstance(envelope.get("record_json"), str):
            fail(f"records.jsonl line {number} has invalid record_json")
        record_ids.append(record_id)
        actual_quarantine += envelope.get("record_type") == "quarantine"

    if len(record_ids) != record_count:
        fail("event batch record count does not match records.jsonl")
    if actual_quarantine != quarantine_count:
        fail("event batch quarantine_count does not match records.jsonl")
    identity = "\0".join((SCHEMA, run_id, consumer_id, str(max_records), str(max_bytes), "\0".join(record_ids)))
    if batch_id != "batch-" + sha256(identity.encode("utf-8")):
        fail("event batch ID is not derived from its immutable manifest and record identities")
    return manifest


def databricks_cli(profile):
    executable = shutil.which("databricks")
    if not executable:
        fail("Databricks CLI required; configure OAuth with `databricks auth login --host <workspace-url>`")
    return [executable] + (["--profile", profile] if profile else [])


def upload_batch(batch_dir, volume, profile, manifest):
    if not VOLUME.fullmatch(volume):
        fail("--volume must be dbfs:/Volumes/<catalog>/<schema>/<volume>")
    cli = databricks_cli(profile)
    directory = Path(batch_dir).resolve()
    names = ("batch-manifest.json", "records.jsonl")
    request = {
        "files": {name: sha256((directory / name).read_bytes()) for name in names},
        "marker": sha256((directory / "_PUBLISHED").read_bytes()),
    }
    scope = make_scope("event", "upload", manifest["batch_id"], volume, profile)
    with TransportState(scope, request) as state:
        def publish(current, attempt_id, _token):
            target = f"{volume}/scout/crawl-events/{manifest['batch_id']}/uploads/{attempt_id}"
            current.data["candidate_remote_path"] = target
            current.data["remote_attempt_id"] = attempt_id
            current.save()
            for path in (f"{volume}/scout", f"{volume}/scout/crawl-events",
                         f"{volume}/scout/crawl-events/{manifest['batch_id']}",
                         f"{volume}/scout/crawl-events/{manifest['batch_id']}/uploads", target):
                current.run(cli + ["fs", "mkdir", path])
            for name in names + ("_PUBLISHED",):
                current.run(cli + ["fs", "cp", str(directory / name), f"{target}/{name}"])
            return target

        target = state.execute("upload", "published_path", "published", publish)
    print(f"Published event batch {manifest['batch_id']} at {target}")
    return target


def submit_batch(volume, remote_path, catalog, schema, volume_name, workspace_path, profile, manifest):
    volume_match = VOLUME.fullmatch(volume)
    if not volume_match:
        fail("--volume must be dbfs:/Volumes/<catalog>/<schema>/<volume>")
    if not all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value) for value in (catalog, schema, volume_name)):
        fail("catalog, schema, and volume names must use letters, digits, and underscores")
    if volume_match.groups() != (catalog, schema, volume_name):
        fail("--volume must match --catalog, --schema, and --volume-name")
    upload_root = f"{volume}/scout/crawl-events/{manifest['batch_id']}/uploads/"
    if not re.fullmatch(re.escape(upload_root) + r"[a-f0-9]{32}", remote_path):
        fail("--remote-path must be a published upload path for this batch")
    if not workspace_path.startswith("/") or "\n" in workspace_path:
        fail("--workspace-path must be an absolute Databricks workspace path")

    cli = databricks_cli(profile)
    notebook = Path(__file__).with_name("crawl_event_ingest.py").resolve()
    request = {
        "run_name": f"scout-crawl-events-{manifest['batch_id'][-12:]}",
        "queue": {"enabled": False},
        "performance_target": "STANDARD",
        "tasks": [{
            "task_key": "ingest_crawl_events",
            "timeout_seconds": 3600,
            "max_retries": 0,
            "disable_auto_optimization": True,
            "environment_key": "scout_serverless",
            "notebook_task": {
                "notebook_path": workspace_path,
                "source": "WORKSPACE",
                "base_parameters": {
                    "batch_id": manifest["batch_id"],
                    "remote_batch_path": remote_path,
                    "catalog": catalog,
                    "schema": schema,
                    "volume": volume_name,
                },
            },
        }],
        "environments": [{"environment_key": "scout_serverless", "spec": {"environment_version": "4"}}],
    }
    request_identity = {
        "job_request": request,
        "notebook_sha256": sha256(notebook.read_bytes()),
        "manifest": manifest,
    }
    scope = make_scope("event", "submit", manifest["batch_id"], volume,
                       profile, workspace_path)
    with TransportState(scope, request_identity) as state:
        state.data["task_timeout_seconds"] = 3600
        state.save()

        def submit(current, _attempt_id, token):
            current.run(cli + ["workspace", "import", workspace_path, "--file", str(notebook),
                               "--format", "SOURCE", "--language", "PYTHON", "--overwrite"])
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=".json", delete=False) as request_file:
                json.dump(request, request_file)
                request_path = request_file.name
            try:
                result = current.run(cli + ["jobs", "submit", "--json", "@" + request_path,
                                            "--idempotency-token", token, "--no-wait", "-o", "json"],
                                     capture_output=True)
                try:
                    response = json.loads(result.stdout)
                except (TypeError, json.JSONDecodeError) as error:
                    fail(f"Databricks CLI did not return a JSON job run ID: {error}")
                run_id = response.get("run_id") if isinstance(response, dict) else None
                if isinstance(run_id, bool) or not isinstance(run_id, (str, int)) or not str(run_id):
                    fail("Databricks CLI response did not include run_id; submission token is retained for safe reconciliation")
                return str(run_id)
            finally:
                os.unlink(request_path)

        run_id = state.execute("submit", "run_id", "submitted", submit)
    print(f"Submitted event batch {manifest['batch_id']} as Databricks run {run_id}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    validate = commands.add_parser("validate", help="verify an immutable local event batch")
    validate.add_argument("--batch-dir", required=True)
    upload = commands.add_parser("upload", help="publish one validated batch to a Unity Catalog Volume")
    upload.add_argument("--batch-dir", required=True)
    upload.add_argument("--volume", required=True)
    upload.add_argument("--profile")
    run = commands.add_parser("run", help="submit one serverless ingestion task")
    run.add_argument("--batch-dir", required=True)
    run.add_argument("--volume", required=True)
    run.add_argument("--remote-path", required=True)
    run.add_argument("--catalog", required=True)
    run.add_argument("--schema", required=True)
    run.add_argument("--volume-name", required=True)
    run.add_argument("--workspace-path", required=True)
    run.add_argument("--profile")
    args = parser.parse_args()
    try:
        manifest = validate_batch(args.batch_dir)
        if args.command == "validate":
            print(f"Valid event batch {manifest['batch_id']}: {manifest['record_count']} records, {manifest['records_bytes']} bytes")
        elif args.command == "upload":
            upload_batch(args.batch_dir, args.volume, args.profile, manifest)
        else:
            submit_batch(args.volume, args.remote_path, args.catalog, args.schema,
                         args.volume_name, args.workspace_path, args.profile, manifest)
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        parser.exit(2, f"error: {error}\n")


if __name__ == "__main__":
    main()
