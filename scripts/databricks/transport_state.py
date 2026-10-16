#!/usr/bin/env python3
"""Durable, bounded retry state for Databricks CLI transport operations."""

import argparse
import configparser
import fcntl
import hashlib
import json
import os
import subprocess
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path


STATE_SCHEMA = "scout.databricks-transport-state.v1"


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _scope_sha256(scope):
    # Workspace notebook path belongs to the immutable submission request.
    # Keep it out of the lock key so a path change cannot silently create a
    # second job identity for the same workspace/profile, volume, and batch.
    key = {name: value for name, value in scope.items() if name != "workspace_path"}
    return _sha256(_canonical(key))


def _utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _env_int(name, default, minimum, maximum):
    raw = os.environ.get(name)
    try:
        value = default if raw is None else int(raw)
    except ValueError as error:
        raise ValueError(f"{name} must be an integer") from error
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


def limits():
    return {
        "max_attempts": _env_int("SCOUT_DATABRICKS_MAX_ATTEMPTS", 3, 1, 10),
        "retry_base_delay_seconds": _env_int("SCOUT_DATABRICKS_RETRY_BASE_DELAY_SECONDS", 2, 0, 300),
        "max_retry_delay_seconds": _env_int("SCOUT_DATABRICKS_MAX_RETRY_DELAY_SECONDS", 30, 0, 300),
        "command_timeout_seconds": _env_int("SCOUT_DATABRICKS_COMMAND_TIMEOUT_SECONDS", 60, 1, 3600),
        "operation_timeout_seconds": _env_int("SCOUT_DATABRICKS_OPERATION_TIMEOUT_SECONDS", 1800, 1, 86400),
    }


def _configured_host(profile):
    host = os.environ.get("DATABRICKS_HOST")
    if host:
        return host.strip().rstrip("/")
    config_path = Path(os.environ.get("DATABRICKS_CONFIG_FILE", "~/.databrickscfg")).expanduser()
    parser = configparser.ConfigParser(interpolation=None)
    try:
        parser.read(config_path, encoding="utf-8")
        section = profile or "DEFAULT"
        if section == "DEFAULT":
            host = parser.defaults().get("host")
        elif parser.has_section(section):
            host = parser.get(section, "host", fallback=None)
    except (OSError, configparser.Error):
        host = None
    return host.strip().rstrip("/") if host else None


def make_scope(kind, operation, batch_id, volume, profile=None, workspace_path=None):
    """Return state scope bound to batch, workspace profile, and Unity Catalog volume."""
    if kind not in ("snapshot", "event") or operation not in ("upload", "submit"):
        raise ValueError("invalid transport kind or operation")
    profile_name = profile or "DEFAULT"
    host = _configured_host(profile)
    scope = {
        "kind": kind,
        "operation": operation,
        "batch_id": batch_id,
        "profile": profile_name,
        "workspace_host_sha256": _sha256(host) if host else None,
        "volume": volume,
    }
    if workspace_path is not None:
        scope["workspace_path"] = workspace_path
    return scope


def state_root():
    return Path(os.environ.get("SCOUT_DATABRICKS_STATE_DIR", "~/.scout/databricks-transport")).expanduser()


def _read_retry_at(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value[:-1] + "+00:00").timestamp()
    except (ValueError, TypeError, IndexError):
        raise ValueError("transport state has an invalid next_retry_at")


class TransportState:
    """One cross-process locked operation with atomic durable checkpoints."""

    def __init__(self, scope, request, *, acquire_timeout=None):
        self.scope = scope
        self.scope_sha256 = _scope_sha256(scope)
        self.request_sha256 = _sha256(_canonical(request)) if request is not None else None
        self.config = limits()
        self.root = state_root()
        self.path = self.root / f"{self.scope_sha256}.json"
        self.lock_path = self.root / f"{self.scope_sha256}.lock"
        self.deadline = time.monotonic() + (acquire_timeout or self.config["operation_timeout_seconds"])
        self._lock_stream = None
        self.data = None

    def __enter__(self):
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        try:
            os.chmod(self.root, 0o700)
        except OSError:
            pass
        self._lock_stream = self.lock_path.open("a+b")
        locked = False
        try:
            os.chmod(self.lock_path, 0o600)
            while True:
                try:
                    fcntl.flock(self._lock_stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    locked = True
                    break
                except BlockingIOError:
                    remaining = self.remaining()
                    time.sleep(min(0.1, remaining))
            return self._load_locked()
        except BaseException:
            if locked:
                fcntl.flock(self._lock_stream.fileno(), fcntl.LOCK_UN)
            self._lock_stream.close()
            self._lock_stream = None
            raise

    def _load_locked(self):
        if self.path.exists():
            try:
                self.data = json.loads(self.path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError) as error:
                raise ValueError(f"cannot read Databricks transport state {self.path}: {error}") from error
            if not isinstance(self.data, dict) or self.data.get("schema_version") != STATE_SCHEMA:
                raise ValueError(f"unsupported Databricks transport state at {self.path}")
            if self.data.get("scope_sha256") != self.scope_sha256:
                raise ValueError(f"Databricks transport state scope mismatch at {self.path}")
            if self.request_sha256 is not None and self.data.get("request_sha256") != self.request_sha256:
                if self.data.pop("allow_request_change", False):
                    self.data["request_sha256"] = self.request_sha256
                    self.data["status"] = "ready"
                    self.data["run_id"] = None
                    self.data["idempotency_token"] = None
                    self.save()
                else:
                    raise ValueError("transport request differs from durable state; use the explicit retry command")
            else:
                self.data.pop("allow_request_change", None)
            self.data["scope"] = self.scope
            self.data.update(self.config)
            self.save()
        elif self.request_sha256 is None:
            raise ValueError("no Databricks transport state exists for this operation")
        else:
            self.data = {
                "schema_version": STATE_SCHEMA,
                "scope": self.scope,
                "scope_sha256": self.scope_sha256,
                "request_sha256": self.request_sha256,
                "status": "ready",
                "generation": 0,
                "attempt_count": 0,
                "next_retry_at": None,
                "active_attempt_id": None,
                "remote_attempt_id": None,
                "published_path": None,
                "run_id": None,
                "remote_job_id": None,
                "idempotency_token": None,
                "last_error": None,
                "created_at_utc": _utc_now(),
                **self.config,
            }
            self.save()
        return self

    def __exit__(self, exc_type, exc, traceback):
        if self._lock_stream is not None:
            fcntl.flock(self._lock_stream.fileno(), fcntl.LOCK_UN)
            self._lock_stream.close()
        return False

    def remaining(self):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Databricks transport operation exceeded its elapsed-time bound")
        return remaining

    def save(self):
        self.data["updated_at_utc"] = _utc_now()
        fd, temp_name = tempfile.mkstemp(prefix=f".{self.path.name}.", suffix=".tmp", dir=self.root)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(self.data, stream, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp_name, self.path)
            directory_fd = os.open(self.root, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            try:
                os.unlink(temp_name)
            except FileNotFoundError:
                pass

    def run(self, command, *, capture_output=False):
        timeout = min(self.config["command_timeout_seconds"], self.remaining())
        return subprocess.run(command, check=True, timeout=timeout,
                              capture_output=capture_output, text=capture_output)

    def _token(self):
        token = self.data.get("idempotency_token")
        if token is None:
            material = f"{self.scope_sha256}:{self.data['request_sha256']}:{self.data['generation']}"
            token = _sha256(material)
            self.data["idempotency_token"] = token
            self.save()
        return token

    def _retry_delay(self):
        base = self.config["retry_base_delay_seconds"]
        cap = self.config["max_retry_delay_seconds"]
        return min(cap, base * (2 ** max(0, self.data["attempt_count"] - 1)))

    def execute(self, operation, result_field, success_status, attempt):
        """Run one idempotent operation with persisted attempts and an elapsed-time cap."""
        if self.data.get("status") == success_status:
            return self.data[result_field]
        if self.data.get("status") == "exhausted":
            raise ValueError(self.exhausted_message())

        while True:
            self.remaining()
            if self.data.get("status") in ("uploading", "submitting"):
                # A prior process may have stopped after the remote side effect.
                # Resume the same upload attempt ID or submit idempotency token.
                if self.data["attempt_count"] >= self.config["max_attempts"]:
                    self.data.update(status="exhausted", next_retry_at=None)
                    self.save()
                    raise ValueError(self.exhausted_message())
                self.data["attempt_count"] += 1
                self.data["attempt_started_at_utc"] = _utc_now()
                attempt_id = self.data.get("active_attempt_id")
                token = self._token() if operation == "submit" else None
                self.save()
            else:
                retry_at = _read_retry_at(self.data.get("next_retry_at"))
                if retry_at is not None:
                    delay = max(0, retry_at - time.time())
                    if delay:
                        time.sleep(min(delay, self.remaining()))
                        if time.time() < retry_at:
                            raise TimeoutError("Databricks retry is not due within the operation time bound")
                if self.data["attempt_count"] >= self.config["max_attempts"]:
                    self.data.update(status="exhausted", next_retry_at=None)
                    self.save()
                    raise ValueError(self.exhausted_message())
                self.data["attempt_count"] += 1
                attempt_id = uuid.uuid4().hex if operation == "upload" else None
                self.data.update(
                    status="uploading" if operation == "upload" else "submitting",
                    active_attempt_id=attempt_id,
                    attempt_started_at_utc=_utc_now(),
                    next_retry_at=None,
                    last_error=None,
                    **({"idempotency_token": None} if operation == "submit" and not self.data.get("idempotency_token") else {}),
                )
                token = self._token() if operation == "submit" else None
                if attempt_id:
                    self.data["remote_attempt_id"] = attempt_id
                self.save()

            try:
                result = attempt(self, attempt_id, token)
                self.data.update(status=success_status, next_retry_at=None, last_error=None)
                self.data[result_field] = result
                if result_field == "run_id":
                    self.data["remote_job_id"] = result
                self.data["active_attempt_id"] = None
                self.save()
                return result
            except (OSError, subprocess.SubprocessError, TimeoutError, ValueError, RuntimeError) as error:
                message = f"{type(error).__name__}: {error}"[:2000]
                if self.data["attempt_count"] >= self.config["max_attempts"]:
                    self.data.update(status="exhausted", next_retry_at=None, last_error=message)
                else:
                    delay = self._retry_delay()
                    self.data.update(
                        status="retry_wait",
                        next_retry_at=datetime.fromtimestamp(time.time() + delay, timezone.utc)
                        .isoformat(timespec="microseconds").replace("+00:00", "Z"),
                        last_error=message,
                    )
                self.save()
                if self.data["status"] == "exhausted":
                    raise ValueError(self.exhausted_message()) from error

    def exhausted_message(self):
        scope = self.scope
        command = ["python3", "scripts/databricks/transport_state.py", "retry",
                   "--kind", scope["kind"], "--operation", scope["operation"],
                   "--batch-id", scope["batch_id"], "--volume", scope["volume"]]
        if scope["profile"] != "DEFAULT":
            command += ["--profile", scope["profile"]]
        if scope.get("workspace_path"):
            command += ["--workspace-path", scope["workspace_path"]]
        return f"{scope['operation']} attempts exhausted; run: {' '.join(command)}"


def retry_state(scope, *, new_job=False, confirmed_terminal_failure=False):
    config = limits()
    scope_sha = _scope_sha256(scope)
    root = state_root()
    path = root / f"{scope_sha}.json"
    lock_path = root / f"{scope_sha}.lock"
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(root, 0o700)
    with lock_path.open("a+b") as lock_stream:
        os.chmod(lock_path, 0o600)
        deadline = time.monotonic() + config["operation_timeout_seconds"]
        while True:
            try:
                fcntl.flock(lock_stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("timed out waiting for Databricks transport state lock")
                time.sleep(min(0.1, remaining))
        try:
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError) as error:
                raise ValueError(f"cannot read Databricks transport state {path}: {error}") from error
            if data.get("schema_version") != STATE_SCHEMA or data.get("scope_sha256") != scope_sha:
                raise ValueError("Databricks transport state scope/schema mismatch")
            if new_job:
                if scope["operation"] != "submit" or not confirmed_terminal_failure:
                    raise ValueError("--new-job requires submit and --confirmed-terminal-failure")
                if data.get("status") != "submitted" or not data.get("run_id"):
                    raise ValueError("a new job requires a recorded run_id; resume unknown submissions with the same token")
                data.update(
                    generation=data.get("generation", 0) + 1,
                    status="ready",
                    attempt_count=0,
                    next_retry_at=None,
                    active_attempt_id=None,
                    run_id=None,
                    remote_job_id=None,
                    idempotency_token=None,
                    allow_request_change=True,
                    last_error=None,
                )
            else:
                if data.get("status") != "exhausted":
                    raise ValueError("operator retry is available only after bounded attempts are exhausted")
                data.update(status="ready", attempt_count=0, next_retry_at=None,
                            active_attempt_id=None, last_error=None)
                if scope["operation"] == "upload":
                    data.update(generation=data.get("generation", 0) + 1,
                                remote_attempt_id=None, published_path=None)
            data.update(config)
            data["updated_at_utc"] = _utc_now()
            fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=root)
            try:
                os.fchmod(fd, 0o600)
                with os.fdopen(fd, "w", encoding="utf-8") as stream:
                    json.dump(data, stream, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
                    stream.write("\n")
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temp_name, path)
                directory_fd = os.open(root, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            finally:
                try:
                    os.unlink(temp_name)
                except FileNotFoundError:
                    pass
        finally:
            fcntl.flock(lock_stream.fileno(), fcntl.LOCK_UN)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    retry = commands.add_parser("retry", help="resume an exhausted upload or idempotent job submission")
    retry.add_argument("--kind", choices=("snapshot", "event"), required=True)
    retry.add_argument("--operation", choices=("upload", "submit"), required=True)
    retry.add_argument("--batch-id", required=True)
    retry.add_argument("--volume", required=True)
    retry.add_argument("--profile")
    retry.add_argument("--workspace-path")
    retry.add_argument("--new-job", action="store_true", help="start another run with a new idempotency token")
    retry.add_argument("--confirmed-terminal-failure", action="store_true",
                       help="confirm the previous Databricks run is terminal and failed")
    args = parser.parse_args()
    if (args.operation == "submit") != bool(args.workspace_path):
        parser.error("--workspace-path is required for submit and forbidden for upload")
    if args.confirmed_terminal_failure and not args.new_job:
        parser.error("--confirmed-terminal-failure requires --new-job")
    try:
        scope = make_scope(args.kind, args.operation, args.batch_id, args.volume,
                           args.profile, args.workspace_path)
        retry_state(scope, new_job=args.new_job,
                    confirmed_terminal_failure=args.confirmed_terminal_failure)
        print("Databricks transport retry armed; rerun the original upload or run command.")
    except (OSError, ValueError, RuntimeError) as error:
        parser.exit(2, f"error: {error}\n")


if __name__ == "__main__":
    main()
