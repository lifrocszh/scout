#!/usr/bin/env python3
"""Focused local checks for the Databricks CLI transport seam."""

import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parent))
import crawl_event_batch
import snapshot_remote
import transport_state


VOLUME = "dbfs:/Volumes/main/analytics/scout"
CATALOG, SCHEMA, VOLUME_NAME = "main", "analytics", "scout"
WORKSPACE_PATH = "/Users/test@example.com/scout-ingest"


class TransportStateTests(unittest.TestCase):
    def environment(self, **extra):
        root = tempfile.TemporaryDirectory()
        values = {
            "SCOUT_DATABRICKS_STATE_DIR": root.name,
            "SCOUT_DATABRICKS_MAX_ATTEMPTS": "2",
            "SCOUT_DATABRICKS_RETRY_BASE_DELAY_SECONDS": "0",
            "SCOUT_DATABRICKS_MAX_RETRY_DELAY_SECONDS": "0",
            "SCOUT_DATABRICKS_COMMAND_TIMEOUT_SECONDS": "5",
            "SCOUT_DATABRICKS_OPERATION_TIMEOUT_SECONDS": "30",
            "DATABRICKS_HOST": "https://workspace.test",
        }
        values.update(extra)
        return root, patch.dict(os.environ, values)

    def batch_dir(self, root, names):
        batch = Path(root) / "batch"
        batch.mkdir()
        for name in names:
            (batch / name).write_text(name + "\n", encoding="utf-8")
        return batch

    def test_snapshot_upload_is_bounded_and_publishes_marker_last(self):
        state_tmp, env = self.environment()
        self.addCleanup(state_tmp.cleanup)
        env.start()
        self.addCleanup(env.stop)
        batch = self.batch_dir(state_tmp.name, ("batch-manifest.json", "records.jsonl", "_PUBLISHED"))
        manifest = {"batch_id": "snapshot-" + "a" * 64}
        calls = []

        def fake_run(command, **kwargs):
            calls.append((command, kwargs))
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

        patch_cli = patch.object(snapshot_remote, "databricks_cli", return_value=["databricks"])
        with patch_cli, patch.object(transport_state.subprocess, "run", side_effect=fake_run):
            first_path = snapshot_remote.upload_batch(batch, VOLUME, "qa", manifest)
            count = len(calls)
            self.assertEqual(snapshot_remote.upload_batch(batch, VOLUME, "qa", manifest), first_path)
        self.assertEqual(len(calls), count)
        cp_calls = [command for command, _ in calls if command[1:3] == ["fs", "cp"]]
        self.assertEqual(Path(cp_calls[-1][-2]).name, "_PUBLISHED")
        self.assertTrue(all(call_kwargs.get("timeout") <= 5 for _, call_kwargs in calls))
        state_files = list(Path(state_tmp.name).glob("*.json"))
        self.assertEqual(len(state_files), 1)
        state = json.loads(state_files[0].read_text(encoding="utf-8"))
        self.assertEqual(state["status"], "published")
        self.assertEqual(state["remote_attempt_id"], first_path.rsplit("/", 1)[1])

    def test_event_upload_retries_with_new_remote_attempt(self):
        state_tmp, env = self.environment()
        self.addCleanup(state_tmp.cleanup)
        env.start()
        self.addCleanup(env.stop)
        batch = self.batch_dir(state_tmp.name, ("batch-manifest.json", "records.jsonl", "_PUBLISHED"))
        manifest = {"batch_id": "batch-" + "b" * 64}
        attempts = []
        calls = []
        failed_once = False

        def fake_run(command, **kwargs):
            nonlocal failed_once
            calls.append((command, kwargs))
            if command[1:3] == ["fs", "mkdir"] and command[-1].endswith("/uploads") is False:
                target = command[-1].rsplit("/", 1)[-1]
                if len(target) == 32 and target not in attempts:
                    attempts.append(target)
                    if not failed_once:
                        failed_once = True
                        raise subprocess.CalledProcessError(1, command)
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

        with patch.object(crawl_event_batch, "databricks_cli", return_value=["databricks"]), \
             patch.object(transport_state.subprocess, "run", side_effect=fake_run):
            path = crawl_event_batch.upload_batch(batch, VOLUME, "qa", manifest)
        self.assertEqual(len(attempts), 2)
        self.assertEqual(path.rsplit("/", 1)[1], attempts[-1])
        self.assertNotEqual(attempts[0], attempts[1])
        copies = [command for command, _ in calls if command[1:3] == ["fs", "cp"]]
        self.assertEqual(Path(copies[-1][-2]).name, "_PUBLISHED")
        state = json.loads(next(Path(state_tmp.name).glob("*.json")).read_text(encoding="utf-8"))
        self.assertEqual(state["attempt_count"], 2)
        self.assertEqual(state["status"], "published")

    def test_snapshot_submission_reuses_token_after_uncertain_outcome(self):
        state_tmp, env = self.environment(SCOUT_DATABRICKS_MAX_ATTEMPTS="1")
        self.addCleanup(state_tmp.cleanup)
        env.start()
        self.addCleanup(env.stop)
        batch = self.batch_dir(state_tmp.name, ("batch-manifest.json", "records.jsonl"))
        manifest = {"batch_id": "snapshot-" + "c" * 64}
        remote_path = f"{VOLUME}/scout/snapshots/{manifest['batch_id']}/uploads/" + "d" * 32
        calls, tokens, requests = [], [], []

        def fake_run(command, **kwargs):
            calls.append((command, kwargs))
            if "jobs" in command and "submit" in command:
                token = command[command.index("--idempotency-token") + 1]
                tokens.append(token)
                request_arg = command[command.index("--json") + 1]
                requests.append(json.loads(Path(request_arg[1:]).read_text(encoding="utf-8")))
                if len(tokens) == 1:
                    raise subprocess.TimeoutExpired(command, kwargs["timeout"])
                return subprocess.CompletedProcess(command, 0, stdout='{"run_id":12345}', stderr="")
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

        scope = transport_state.make_scope("snapshot", "submit", manifest["batch_id"],
                                           VOLUME, "qa", WORKSPACE_PATH)
        with patch.object(snapshot_remote, "databricks_cli", return_value=["databricks"]), \
             patch.object(transport_state.subprocess, "run", side_effect=fake_run):
            with self.assertRaises(ValueError):
                snapshot_remote.submit_batch(batch, VOLUME, remote_path, CATALOG, SCHEMA, VOLUME_NAME,
                                             WORKSPACE_PATH, "qa", manifest)
            failed_state = json.loads(next(Path(state_tmp.name).glob("*.json")).read_text(encoding="utf-8"))
            token_before_retry = failed_state["idempotency_token"]
            self.assertEqual(failed_state["status"], "exhausted")
            transport_state.retry_state(scope)
            snapshot_remote.submit_batch(batch, VOLUME, remote_path, CATALOG, SCHEMA, VOLUME_NAME,
                                         WORKSPACE_PATH, "qa", manifest)
            call_count = len(calls)
            snapshot_remote.submit_batch(batch, VOLUME, remote_path, CATALOG, SCHEMA, VOLUME_NAME,
                                         WORKSPACE_PATH, "qa", manifest)
        self.assertEqual(len(tokens), 2)
        self.assertEqual(tokens[0], tokens[1])
        self.assertEqual(tokens[0], token_before_retry)
        self.assertEqual(len(calls), call_count)
        self.assertEqual(requests[0]["queue"], {"enabled": False})
        task = requests[0]["tasks"][0]
        self.assertEqual(task["max_retries"], 0)
        self.assertIs(task["disable_auto_optimization"], True)
        self.assertIn("--no-wait", calls[-1][0])
        state = json.loads(next(Path(state_tmp.name).glob("*.json")).read_text(encoding="utf-8"))
        self.assertEqual(state["status"], "submitted")
        self.assertEqual(state["run_id"], "12345")
        self.assertEqual(state["remote_job_id"], "12345")

    def test_snapshot_upload_exhaustion_persists_until_explicit_retry(self):
        state_tmp, env = self.environment(SCOUT_DATABRICKS_MAX_ATTEMPTS="1")
        self.addCleanup(state_tmp.cleanup)
        env.start()
        self.addCleanup(env.stop)
        batch = self.batch_dir(state_tmp.name, ("batch-manifest.json", "records.jsonl", "_PUBLISHED"))
        manifest = {"batch_id": "snapshot-" + "9" * 64}
        calls = []

        def fail_upload(command, **kwargs):
            calls.append(command)
            raise subprocess.CalledProcessError(1, command)

        scope = transport_state.make_scope("snapshot", "upload", manifest["batch_id"], VOLUME, "qa")
        with patch.object(snapshot_remote, "databricks_cli", return_value=["databricks"]), \
             patch.object(transport_state.subprocess, "run", side_effect=fail_upload):
            with self.assertRaisesRegex(ValueError, "attempts exhausted"):
                snapshot_remote.upload_batch(batch, VOLUME, "qa", manifest)
            attempts_after_exhaustion = len(calls)
            with self.assertRaisesRegex(ValueError, "attempts exhausted"):
                snapshot_remote.upload_batch(batch, VOLUME, "qa", manifest)
            self.assertEqual(len(calls), attempts_after_exhaustion)
            transport_state.retry_state(scope)

        def successful_upload(command, **kwargs):
            calls.append(command)
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

        with patch.object(snapshot_remote, "databricks_cli", return_value=["databricks"]), \
             patch.object(transport_state.subprocess, "run", side_effect=successful_upload):
            path = snapshot_remote.upload_batch(batch, VOLUME, "qa", manifest)
        state = json.loads(next(Path(state_tmp.name).glob("*.json")).read_text(encoding="utf-8"))
        self.assertEqual(state["status"], "published")
        self.assertEqual(state["attempt_count"], 1)
        self.assertEqual(state["generation"], 1)
        self.assertEqual(state["published_path"], path)

    def test_operation_timeout_covers_retry_sleep(self):
        state_tmp, env = self.environment(SCOUT_DATABRICKS_MAX_ATTEMPTS="3",
                                          SCOUT_DATABRICKS_RETRY_BASE_DELAY_SECONDS="5",
                                          SCOUT_DATABRICKS_MAX_RETRY_DELAY_SECONDS="5",
                                          SCOUT_DATABRICKS_OPERATION_TIMEOUT_SECONDS="1")
        self.addCleanup(state_tmp.cleanup)
        env.start()
        self.addCleanup(env.stop)
        scope = transport_state.make_scope("event", "upload", "batch-timeout", VOLUME, "qa")
        started = time.monotonic()
        with transport_state.TransportState(scope, {"test": "bounded"}) as state:
            with self.assertRaises(TimeoutError):
                state.execute("upload", "published_path", "published",
                              lambda *_: (_ for _ in ()).throw(subprocess.CalledProcessError(1, ["fake"])))
        self.assertLess(time.monotonic() - started, 2)
        persisted = json.loads(next(Path(state_tmp.name).glob("*.json")).read_text(encoding="utf-8"))
        self.assertEqual(persisted["status"], "retry_wait")
        self.assertEqual(persisted["attempt_count"], 1)
        self.assertIsNotNone(persisted["next_retry_at"])

    def test_state_lock_serializes_same_scope(self):
        state_tmp, env = self.environment()
        self.addCleanup(state_tmp.cleanup)
        env.start()
        self.addCleanup(env.stop)
        scope = transport_state.make_scope("event", "upload", "batch-lock", VOLUME, "qa")
        first = transport_state.TransportState(scope, {"same": "request"})
        second = transport_state.TransportState(scope, {"same": "request"}, acquire_timeout=0.1)
        self.assertEqual(first.path, second.path)
        first.__enter__()
        try:
            with self.assertRaises(TimeoutError):
                second.__enter__()
        finally:
            first.__exit__(None, None, None)

    def test_event_submission_persists_job_identity_and_timeout(self):
        state_tmp, env = self.environment()
        self.addCleanup(state_tmp.cleanup)
        env.start()
        self.addCleanup(env.stop)
        manifest = {
            "batch_id": "batch-" + "e" * 64,
            "crawl_run_id": "run-" + "f" * 64,
            "consumer_id": "consumer-a",
            "records_sha256": "1" * 64,
            "record_count": 1,
        }
        remote_path = f"{VOLUME}/scout/crawl-events/{manifest['batch_id']}/uploads/" + "2" * 32
        calls, requests = [], []

        def fake_run(command, **kwargs):
            calls.append((command, kwargs))
            if "jobs" in command and "submit" in command:
                request_arg = command[command.index("--json") + 1]
                requests.append(json.loads(Path(request_arg[1:]).read_text(encoding="utf-8")))
                return subprocess.CompletedProcess(command, 0, stdout='{"run_id":67890}', stderr="")
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

        with patch.object(crawl_event_batch, "databricks_cli", return_value=["databricks"]), \
             patch.object(transport_state.subprocess, "run", side_effect=fake_run):
            crawl_event_batch.submit_batch(VOLUME, remote_path, CATALOG, SCHEMA, VOLUME_NAME,
                                          WORKSPACE_PATH, "qa", manifest)
        self.assertEqual(requests[0]["queue"], {"enabled": False})
        task = requests[0]["tasks"][0]
        self.assertEqual(task["max_retries"], 0)
        self.assertIs(task["disable_auto_optimization"], True)
        submit = next(command for command, _ in calls if "jobs" in command and "submit" in command)
        self.assertEqual(len(submit[submit.index("--idempotency-token") + 1]), 64)
        self.assertTrue(all(kwargs["timeout"] > 0 for _, kwargs in calls))
        state = json.loads(next(Path(state_tmp.name).glob("*.json")).read_text(encoding="utf-8"))
        self.assertEqual(state["attempt_count"], 1)
        self.assertEqual(state["task_timeout_seconds"], 3600)
        self.assertEqual(state["run_id"], "67890")

    def test_scope_isolated_by_workspace_host(self):
        scope_a = transport_state.make_scope("event", "upload", "batch-x", VOLUME, "qa")
        state_a = transport_state.TransportState(scope_a, {})
        with patch.dict(os.environ, {"DATABRICKS_HOST": "https://other-workspace.test"}):
            scope_b = transport_state.make_scope("event", "upload", "batch-x", VOLUME, "qa")
            state_b = transport_state.TransportState(scope_b, {})
        self.assertNotEqual(state_a.path, state_b.path)


if __name__ == "__main__":
    unittest.main()
