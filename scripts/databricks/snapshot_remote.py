"""Authenticated Databricks CLI transport for validated Scout batches."""

import json
import hashlib
import os
import re
import shutil
import tempfile
from pathlib import Path

from transport_state import TransportState, make_scope


VOLUME = re.compile(r"^dbfs:/Volumes/([A-Za-z_][A-Za-z0-9_]*)/([A-Za-z_][A-Za-z0-9_]*)/([A-Za-z_][A-Za-z0-9_]*)$")


def fail(message):
    raise ValueError(message)


def databricks_cli(profile):
    executable = shutil.which("databricks")
    if not executable:
        fail("Databricks CLI is required; configure OAuth with `databricks auth login --host <workspace-url>`")
    return [executable] + (["--profile", profile] if profile else [])


def file_sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read_run_id(result):
    try:
        response = json.loads(result.stdout)
    except (TypeError, json.JSONDecodeError) as error:
        fail(f"Databricks CLI did not return a JSON job run ID: {error}")
    run_id = response.get("run_id") if isinstance(response, dict) else None
    if isinstance(run_id, bool) or not isinstance(run_id, (str, int)) or not str(run_id):
        fail("Databricks CLI response did not include run_id; submission token is retained for safe reconciliation")
    return str(run_id)


def upload_batch(batch_dir, volume, profile, manifest):
    if not VOLUME.fullmatch(volume):
        fail("--volume must be dbfs:/Volumes/<catalog>/<schema>/<volume>")
    cli = databricks_cli(profile)
    directory = Path(batch_dir).resolve()
    names = ["batch-manifest.json", "records.jsonl"]
    if manifest.get("history_evidence_sha256"):
        names.append("history-evidence.json")
    request = {"files": {name: file_sha256(directory / name) for name in names}, "marker": file_sha256(directory / "_PUBLISHED")}
    scope = make_scope("snapshot", "upload", manifest["batch_id"], volume, profile)
    with TransportState(scope, request) as state:
        def publish(current, attempt_id, _token):
            target = f"{volume}/scout/snapshots/{manifest['batch_id']}/uploads/{attempt_id}"
            current.data["candidate_remote_path"] = target
            current.data["remote_attempt_id"] = attempt_id
            current.save()
            directories = (
                f"{volume}/scout",
                f"{volume}/scout/snapshots",
                f"{volume}/scout/snapshots/{manifest['batch_id']}",
                f"{volume}/scout/snapshots/{manifest['batch_id']}/uploads",
                target,
            )
            for path in directories:
                current.run(cli + ["fs", "mkdir", path])
            for name in names + ["_PUBLISHED"]:
                current.run(cli + ["fs", "cp", str(directory / name), f"{target}/{name}"])
            return target

        target = state.execute("upload", "published_path", "published", publish)
    print(f"Published batch {manifest['batch_id']} at {target}")
    return target


def submit_batch(batch_dir, volume, remote_path, catalog, schema, volume_name, workspace_path, profile, manifest):
    volume_match = VOLUME.fullmatch(volume)
    if not volume_match:
        fail("--volume must be dbfs:/Volumes/<catalog>/<schema>/<volume>")
    if not all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value) for value in (catalog, schema, volume_name)):
        fail("catalog, schema, and volume names must use letters, digits, and underscores")
    if volume_match.groups() != (catalog, schema, volume_name):
        fail("--volume must match --catalog, --schema, and --volume-name")
    batch_root = f"{volume}/scout/snapshots/{manifest['batch_id']}/uploads/"
    if not re.fullmatch(re.escape(batch_root) + r"[a-f0-9]{32}", remote_path):
        fail("--remote-path must be a published upload path for this batch")
    if not workspace_path.startswith("/") or "\n" in workspace_path:
        fail("--workspace-path must be an absolute Databricks workspace path")

    cli = databricks_cli(profile)
    notebook = Path(__file__).with_name("snapshot_ingest.py").resolve()
    request = {
        "run_name": f"scout-snapshot-{manifest['batch_id'][-12:]}",
        "queue": {"enabled": False},
        "performance_target": "STANDARD",
        "tasks": [{
            "task_key": "ingest_snapshot",
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
        "notebook_sha256": file_sha256(notebook),
        "manifest_sha256": file_sha256(Path(batch_dir).resolve() / "batch-manifest.json"),
        "records_sha256": file_sha256(Path(batch_dir).resolve() / "records.jsonl"),
        "history_evidence_sha256": file_sha256(Path(batch_dir).resolve() / "history-evidence.json")
        if manifest.get("history_evidence_sha256") else None,
    }
    scope = make_scope("snapshot", "submit", manifest["batch_id"], volume,
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
                return read_run_id(result)
            finally:
                os.unlink(request_path)

        run_id = state.execute("submit", "run_id", "submitted", submit)
    print(f"Submitted snapshot batch {manifest['batch_id']} as Databricks run {run_id}")
