#!/usr/bin/env python3
"""Scout demo control server: same-origin UI + proxy to `scout serve`.

Stdlib only. The process owns at most three child scout processes (crawl job,
index build job, serve daemon), each in its own session so a group kill can
never reach an unrelated process.
"""

import argparse
import json
import os
import re
import signal
import socket
import subprocess
import sys
import threading
import time
import tomllib
import urllib.error
import urllib.request
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
STATIC_DIR = Path(__file__).resolve().parent / "static"
CONFIGS_DIR = REPO / "data" / "configs"

MAX_BODY = 64 * 1024
MAX_QUERY = 10000
MAX_LIMIT = 100
DEFAULT_LIMIT = 10
LOG_LINES = 400
TAIL_LINES = 200
SCAN_TTL = 3.0
PERSIST_THROTTLE = 2.0
BOOT_TIMEOUT = 15

MODES = ("keyword", "semantic", "hybrid")
# Event names that mean the whole job failed (per-URL *.unavailable/fetch.failed do not).
TERMINAL_EVENTS = ("crawl.failed", "index.failed", "crawl.materialization.failed")
# The only configs this demo will ever launch; anything else in data/configs is ignored.
APPROVED_CONFIGS = ("sources-2k.toml", "sources-10k-rebuild-2026-09-30-v2.toml")
SNAPSHOT_RE = re.compile(r"^snapshot-[0-9a-f]{64}$")
GENERATION_RE = re.compile(r"^generation-[0-9a-f]{64}$")
LOOPBACK = ("127.0.0.1", "localhost", "::1")

STATUS = {
    "invalid_request": 400, "invalid_config": 400, "unsafe_path": 400,
    "not_found": 404, "conflict": 409, "not_ready": 503, "read_only": 503,
    "upstream_error": 503, "internal_error": 500,
}


class ApiError(Exception):
    def __init__(self, code, message, status=None):
        super().__init__(message)
        self.code, self.message, self._status = code, message, status

    @property
    def status(self):
        return self._status or STATUS.get(self.code, 500)


def api_error(code, message, status=None):
    raise ApiError(code, message, status)


# ------------------------------------------------------------------ io

def now_iso():
    t = time.time()
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(t)) + ".%03dZ" % (int(t * 1000) % 1000)


def read_json(path, limit=4 * 1024 * 1024):
    """Small manifest read; None on missing/huge/invalid."""
    try:
        if path.stat().st_size > limit:
            return None
        with path.open("rb") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=1))
    os.replace(tmp, path)


def pid_alive(pid):
    if not isinstance(pid, int) or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def kill_group(pid):
    """SIGTERM the managed session, escalate to SIGKILL after 10s. Only that pgid."""
    try:
        pgid = os.getpgid(pid)
    except (ProcessLookupError, PermissionError):
        return
    for sig, wait in ((signal.SIGTERM, 10), (signal.SIGKILL, 3)):
        try:
            os.killpg(pgid, sig)
        except ProcessLookupError:
            return
        deadline = time.time() + wait
        while time.time() < deadline:
            try:
                os.killpg(pgid, 0)
            except ProcessLookupError:
                return
            time.sleep(0.1)
        if sig == signal.SIGKILL:
            return


def port_free(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


# -------------------------------------------------------------- helpers

def elapsed(start, end):
    if not start:
        return 0.0
    try:
        t0 = time.mktime(time.strptime(start[:19], "%Y-%m-%dT%H:%M:%S")) - time.timezone
        t1 = time.mktime(time.strptime(end[:19], "%Y-%m-%dT%H:%M:%S")) - time.timezone if end else time.time()
        return round(max(0.0, t1 - t0), 1)
    except ValueError:
        return 0.0


def parse_event(line):
    """JSONL operational event, or None for noise/plain lines."""
    line = line.strip()
    if not line.startswith("{"):
        return None
    try:
        ev = json.loads(line)
    except ValueError:
        return None
    return ev if isinstance(ev, dict) and "event" in ev else None


def last_error_line(lines):
    if not lines:
        return None
    for line in reversed(list(lines)):
        ev = parse_event(line)
        if ev and ev.get("level") in ("error", "warn"):
            data = ev.get("data") or {}
            msg = data.get("message") or data.get("error") or data.get("code")
            return "%s: %s" % (ev["event"], msg) if msg else ev["event"]
        if not ev and line.strip():
            return line.strip()[:300]
    return None


def event_error(line):
    """Job-level error text, or None. Only terminal/aggregate failures qualify: a
    per-URL `*.unavailable` / `fetch.failed` event is noise, not the job's outcome."""
    ev = parse_event(line)
    if ev:
        if ev.get("level") != "error" or ev.get("event") not in TERMINAL_EVENTS:
            return None
        data = ev.get("data") or {}
        msg = data.get("message") or data.get("error") or data.get("code")
        return "%s: %s" % (ev["event"], msg) if msg else ev["event"]
    text = line.strip()
    return text[:300] if text.startswith("scout ") and " failed" in text else None


def lines_error(lines):
    for line in reversed(list(lines or ())):
        err = event_error(line)
        if err:
            return err
    return None


def config_for(config_id):
    """Allowlist + containment: only the approved names, inside data/configs/."""
    if not isinstance(config_id, str) or config_id not in APPROVED_CONFIGS:
        api_error("invalid_config", "config_id must be one of: %s" % ", ".join(APPROVED_CONFIGS))
    base = CONFIGS_DIR.resolve()
    path = (base / config_id).resolve()
    if path.parent != base or not path.is_file():
        api_error("invalid_config", "config %s is not available" % config_id)
    return {"id": config_id, "abs_path": path}


def scan_configs():
    out = []
    base = CONFIGS_DIR.resolve()
    for name in APPROVED_CONFIGS:
        path = (base / name).resolve()
        entry = {"id": name, "path": "data/configs/" + name, "page_target": None,
                 "source_count": 0, "origins": [], "embedding_model": None,
                 "valid": False, "error": None}
        try:
            if path.parent != base or not path.is_file():
                raise FileNotFoundError("not present")
            with path.open("rb") as fh:
                doc = tomllib.load(fh)
            entry["page_target"] = doc.get("page_target")
            sources = doc.get("sources") or []
            entry["source_count"] = len(sources)
            origins = []
            for origin in [o for s in sources for o in (s.get("allowed_origins") or [])] + \
                          [o.get("origin") for o in (doc.get("origins") or [])]:
                if origin and origin not in origins:
                    origins.append(origin)
            entry["origins"] = origins
            entry["embedding_model"] = doc.get("embedding_model")
            entry["valid"] = isinstance(entry["page_target"], int) and entry["source_count"] > 0
            if not entry["valid"]:
                entry["error"] = "missing page_target or sources"
        except Exception as exc:  # unreadable/malformed config is reportable, not fatal
            entry["error"] = str(exc)
        out.append(entry)
    return out


def scan_snapshots(cfg):
    """Manifest-only scan of both roots; newest first."""
    snaps = []
    for root_tag, root_dir, buildable in (("work", cfg.work_dir, True),
                                          ("prebuilt", cfg.data_dir, False)):
        base = root_dir / "corpora"
        for child in (base.iterdir() if base.is_dir() else ()):
            if not child.is_dir() or not SNAPSHOT_RE.match(child.name):
                continue
            man = read_json(child / "snapshot-manifest.json")
            if not man:
                continue
            snaps.append({
                "snapshot_id": man.get("snapshot_id", child.name), "root": root_tag,
                "data_dir": str(root_dir), "page_count": man.get("page_count"),
                "passage_count": man.get("passage_count"),
                "alias_count": man.get("alias_count"),
                "created_at_utc": man.get("created_at_utc"),
                "age_seconds": elapsed(man["created_at_utc"], None) if man.get("created_at_utc") else None,
                "run_id": man.get("run_id"), "buildable": buildable, "generation_id": None,
                "status": man.get("status"),
            })
    for root_tag, root_dir in (("work", cfg.work_dir), ("prebuilt", cfg.data_dir)):
        base = root_dir / "generations"
        for child in (base.iterdir() if base.is_dir() else ()):
            if not child.is_dir() or not GENERATION_RE.match(child.name):
                continue
            man = read_json(child / "generation-manifest.json")
            if not man or man.get("status") != "sealed":
                continue
            for snap in snaps:
                if (snap["root"] == root_tag and not snap["generation_id"]
                        and snap["snapshot_id"] == man.get("corpus_snapshot_id")):
                    snap["generation_id"] = man.get("generation_id", child.name)
    snaps.sort(key=lambda s: s.get("created_at_utc") or "", reverse=True)
    return snaps


def write_index_profile(cfg, snapshot_id):
    """The only mutation of a build profile: swap snapshot_id, drop model_name."""
    try:
        text = (cfg.data_dir / "config" / "index.toml").read_text()
    except OSError as exc:
        api_error("internal_error", "cannot read build profile template: %s" % exc)
    out = []
    for line in text.splitlines():
        key = line.split("=", 1)[0].strip()
        if key == "snapshot_id":
            line = 'snapshot_id = "%s"' % snapshot_id
        elif key == "model_name":
            continue
        out.append(line)
    target = cfg.work_dir / "config" / "index.toml"
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("\n".join(out) + "\n")
    except OSError as exc:
        api_error("internal_error", "cannot write build profile: %s" % exc)


def run_scout(cfg, args, timeout=180):
    try:
        proc = subprocess.run([str(cfg.scout_bin)] + args, cwd=str(REPO), capture_output=True,
                              text=True, errors="replace", timeout=timeout)
        return {"exit_code": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr}
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"exit_code": -1, "stdout": "", "stderr": str(exc)}


# ----------------------------------------------------------------- state

def blank_crawl():
    return {"job_id": None, "state": "idle", "config_id": None, "data_dir": None,
            "pid": None, "started_at_utc": None, "ended_at_utc": None, "elapsed_seconds": 0.0,
            "exit_code": None, "run_id": None, "snapshot_id": None,
            "counters": {"captures": 0, "admitted": 0, "rejected": 0, "failed": 0},
            "last_event": None, "error": None, "log_tail": []}


def blank_build():
    return {"job_id": None, "state": "idle", "snapshot_id": None, "data_dir": None,
            "pid": None, "started_at_utc": None, "ended_at_utc": None, "elapsed_seconds": 0.0,
            "phase": None, "phases_seen": [], "page_count": None, "passage_count": None,
            "generation_id": None, "exit_code": None, "error": None, "log_tail": []}


def blank_serve():
    return {"state": "stopped", "root": None, "data_dir": None, "port": None,
            "pid": None, "generation_id": None, "ready": False, "error": None}


BLANKS = {"crawl": blank_crawl, "build": blank_build, "serve": blank_serve}


class Demo:
    """All mutable demo state behind one lock, persisted to demo-state.json."""

    def __init__(self, cfg):
        self.cfg = cfg
        self.lock = threading.Lock()
        self.procs = {}   # name -> Popen
        self.lines = {}   # name -> deque[str]
        self.logs = {}    # name -> Path
        self.crawl, self.build, self.serve = blank_crawl(), blank_build(), blank_serve()
        self.snap_cache = (0.0, [])
        self.last_persist = 0.0

    # -- persistence ----------------------------------------------
    def state_path(self):
        return self.cfg.work_dir / "demo-state.json"

    def persist(self, force=False):
        now = time.time()
        if not force and now - self.last_persist < PERSIST_THROTTLE:
            return
        self.last_persist = now
        with self.lock:
            body = {n: dict(getattr(self, n)) for n in BLANKS}
        try:
            write_json(self.state_path(), body)
        except OSError:
            pass

    def load(self):
        data = read_json(self.state_path())
        if not isinstance(data, dict):
            return
        for name, blank in BLANKS.items():
            got = data.get(name)
            if isinstance(got, dict):
                merged = blank()
                merged.update({k: v for k, v in got.items() if k in merged})
                setattr(self, name, merged)
        # Never trust a persisted pid: a "running" job with no live process is stale.
        for name in ("crawl", "build"):
            job = getattr(self, name)
            if job["state"] == "running" and not pid_alive(job["pid"]):
                job.update(state="stale", pid=None,
                           error=job["error"] or "process vanished; demo restarted")
        if self.serve["state"] in ("running", "starting"):
            if not pid_alive(self.serve["pid"]):
                self.serve = blank_serve()
            else:
                # Alive but not our child (no Popen after a demo restart): it still holds the
                # port, and it is not something this process can wait on or trust.
                self.serve.update(state="stale", ready=False,
                                  error="serve was adopted across a demo restart; "
                                        "stop it and start again")

    # -- children -------------------------------------------------
    def job(self, name):
        return getattr(self, name)

    def view(self, name):
        with self.lock:
            job = self.job(name)
            out = {k: (list(v) if isinstance(v, list) else dict(v) if isinstance(v, dict) else v)
                   for k, v in job.items()}
            out["elapsed_seconds"] = elapsed(job.get("started_at_utc"), job.get("ended_at_utc"))
            out["log_tail"] = list(self.lines.get(name) or [])[-TAIL_LINES:]
        return out

    def spawn(self, name, job_id, argv, on_line):
        logdir = self.cfg.work_dir / "demo-logs"
        logdir.mkdir(parents=True, exist_ok=True)
        log = logdir / (job_id + ".log")
        log.write_text("")
        self.logs[name] = log
        self.lines[name] = deque(maxlen=LOG_LINES)
        proc = subprocess.Popen(argv, cwd=str(REPO), stdin=subprocess.DEVNULL,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                errors="replace", bufsize=1, start_new_session=True)
        self.procs[name] = proc
        for pipe, kind in ((proc.stdout, "stdout"), (proc.stderr, "stderr")):
            threading.Thread(target=self.pump, args=(name, pipe, kind, on_line),
                             daemon=True).start()
        return proc

    def pump(self, name, pipe, kind, on_line):
        try:
            for raw in pipe:
                line = raw.rstrip("\n")
                if not line:
                    continue
                with self.lock:
                    self.lines[name].append(line)
                    log = self.logs.get(name)
                if log is not None:
                    try:
                        with log.open("a") as fh:
                            fh.write(line + "\n")
                    except OSError:
                        pass
                try:
                    on_line(line, kind)
                except Exception:  # a parser bug must not kill the reader thread
                    pass
                self.persist()
        finally:
            try:
                pipe.close()
            except OSError:
                pass

    def start(self, name, argv, on_line, **fields):
        with self.lock:
            cur = self.job(name)
            if cur["state"] in ("running", "starting") and pid_alive(cur["pid"]):
                api_error("conflict", "%s is already running" % name)
            job_id = "%s-%s" % (name, os.urandom(4).hex())
            job = BLANKS[name]()
            job.update(job_id=job_id, state="running", data_dir=str(self.cfg.work_dir),
                       started_at_utc=now_iso())
            job.update(fields)  # an explicit data_dir (prebuilt vs work) wins over the default
            setattr(self, name, job)
        try:
            proc = self.spawn(name, job_id, argv, on_line)
        except OSError as exc:
            with self.lock:
                job.update(state="failed", error=str(exc), ended_at_utc=now_iso())
            api_error("internal_error", "cannot start scout %s: %s" % (name, exc))
        with self.lock:
            job["pid"] = proc.pid
            threading.Thread(target=self.reap, args=(name, job_id, proc), daemon=True).start()
        self.persist(force=True)
        return self.view(name)

    def reap(self, name, job_id, proc):
        code = proc.wait()
        with self.lock:
            job = self.job(name)
            if job.get("job_id") != job_id:
                return  # superseded by a newer job of the same kind
            job.update(exit_code=code, ended_at_utc=now_iso())
            if name == "serve":
                job.update(state="stopped" if code == 0 else "failed", ready=False, pid=None)
            elif job["state"] in ("stopped", "stopping"):
                job["state"] = "stopped"  # we killed it: a negative exit code is expected
            else:
                job["state"] = "completed" if code == 0 else "failed"
            if job["state"] == "stopped":
                job["error"] = None
            elif job["state"] == "failed" and not job.get("error"):
                job["error"] = ("terminated by signal %d" % -code if code < 0
                                else lines_error(self.lines.get(name)))
            self.procs.pop(name, None)
        self.persist(force=True)

    def stop(self, name):
        with self.lock:
            job = self.job(name)
            # "stale" is includable on purpose: an adopted child still holds its port.
            alive = job["state"] in ("running", "starting", "stale") and pid_alive(job.get("pid"))
            pid = job.get("pid")
            if name != "serve" and alive:
                job["state"] = "stopping"  # so reap knows the exit was ours
        if not alive:
            return False
        kill_group(pid)
        proc = self.procs.pop(name, None)
        if proc is not None:
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                pass
        with self.lock:
            if name == "serve":
                self.serve.update(state="stopped", ready=False, pid=None, error=None)
            else:
                job.update(state="stopped", ended_at_utc=now_iso())
        self.persist(force=True)
        return True

    # -- crawl ----------------------------------------------------
    def crawl_line(self, line, kind):
        if kind == "stdout":
            m = re.search(r"crawl complete: run_id=(\S+) snapshot_id=(\S+) "
                          r"pages=(\d+) aliases=(\d+)", line)
            if m:
                with self.lock:
                    self.crawl.update(run_id=m.group(1), snapshot_id=m.group(2))
                    self.crawl["counters"].update(admitted=int(m.group(3)),
                                                  rejected=int(m.group(4)))
            return
        ev = parse_event(line)
        if not ev:
            return
        name, data = ev.get("event") or "", ev.get("data") or {}
        with self.lock:
            self.crawl["last_event"] = {"event": name, "timestamp": ev.get("timestamp"),
                                        "level": ev.get("level")}
            c = self.crawl["counters"]
            if name == "fetch.completed":
                c["captures"] += 1
            elif name == "page.admitted":
                c["admitted"] += 1
            elif name in ("page.rejected", "page.deduplicated"):
                c["rejected"] += 1
            elif name in ("fetch.failed", "fetch.retry"):
                c["failed"] += 1
            elif name == "crawl.completed":
                self.crawl["snapshot_id"] = data.get("snapshot_id") or self.crawl["snapshot_id"]
                c["admitted"] = data.get("page_count", c["admitted"])
                c["rejected"] = data.get("alias_count", c["rejected"])

    def crawl_start(self, config_id):
        conf = config_for(config_id)
        return self.start("crawl",
                          [str(self.cfg.scout_bin), "crawl", "--config", str(conf["abs_path"]),
                           "--data-dir", str(self.cfg.work_dir)],
                          self.crawl_line, config_id=conf["id"])

    def crawl_stop(self):
        if not self.stop("crawl"):
            api_error("conflict", "no crawl is running")
        return self.view("crawl")

    # -- build ----------------------------------------------------
    def build_line(self, line, kind):
        if kind == "stdout":
            m = re.search(r"index build complete: generation_id=(\S+) pages=(\d+) "
                          r"passages=(\d+)", line)
            if m:
                with self.lock:
                    self.build.update(generation_id=m.group(1), page_count=int(m.group(2)),
                                      passage_count=int(m.group(3)))
            return
        ev = parse_event(line)
        if not ev:
            return
        data = ev.get("data") or {}
        with self.lock:
            if ev.get("event") == "index.phase" and data.get("phase"):
                self.build["phase"] = data["phase"]
                if data["phase"] not in self.build["phases_seen"]:
                    self.build["phases_seen"].append(data["phase"])
                if data.get("passage_count") is not None:
                    self.build["passage_count"] = data["passage_count"]
            elif ev.get("event") == "index.completed":
                self.build.update(generation_id=data.get("generation_id"),
                                  page_count=data.get("page_count"),
                                  passage_count=data.get("passage_count"))

    def build_start(self, snapshot_id, root):
        if not isinstance(snapshot_id, str) or not SNAPSHOT_RE.match(snapshot_id):
            api_error("invalid_request", "snapshot_id must be snapshot-<64 lowercase hex>")
        root = root or "work"
        if root not in ("work", "prebuilt"):
            api_error("invalid_request", "root must be work or prebuilt")
        if root == "prebuilt":
            api_error("read_only", "the prebuilt tree is read-only; build into a work snapshot")
        man = read_json(self.cfg.work_dir / "corpora" / snapshot_id / "snapshot-manifest.json")
        if not man or man.get("snapshot_id") != snapshot_id:
            api_error("not_found", "unknown work snapshot %s" % snapshot_id)
        write_index_profile(self.cfg, snapshot_id)
        return self.start("build",
                          [str(self.cfg.scout_bin), "index", "build", "--corpus", snapshot_id,
                           "--data-dir", str(self.cfg.work_dir), "--mode", "keyword-only"],
                          self.build_line, snapshot_id=snapshot_id)

    # -- serve ----------------------------------------------------
    def serve_start(self, root, port):
        root = root or "prebuilt"
        if root not in ("prebuilt", "work"):
            api_error("invalid_request", "root must be prebuilt or work")
        port = self.cfg.serve_port if port is None else port
        if isinstance(port, bool) or not isinstance(port, int) or not (1024 <= port <= 65535):
            api_error("invalid_request", "port must be an integer in 1024..65535")
        data_dir = self.cfg.data_dir if root == "prebuilt" else self.cfg.work_dir
        if not (data_dir / "CURRENT").exists() and not (data_dir / "corpora").is_dir():
            api_error("not_found", "no corpus in %s yet" % data_dir)
        seen = {"listening": False}

        def on_line(line, kind):
            if "scout serve listening on" in line:
                seen["listening"] = True
            elif kind == "stderr":
                ev = parse_event(line)
                if ev and ev.get("event") == "serve.started":
                    seen["listening"] = True

        if not port_free(port):
            cur = self.view("serve")
            if cur.get("port") == port and pid_alive(cur.get("pid")):
                api_error("conflict", "port %d is held by the %s serve; stop it first%s" % (
                    port, cur["state"],
                    " in the UI (it was adopted across a demo restart)"
                    if cur["state"] == "stale" else ""))
            api_error("conflict", "port %d is already in use" % port)
        self.start("serve",
                   [str(self.cfg.scout_bin), "serve", "--data-dir", str(data_dir),
                    "--bind", "127.0.0.1:%d" % port,
                    "--access-log", str(self.cfg.work_dir / "demo-logs" / "scout-access.log")],
                   on_line, root=root, port=port, data_dir=str(data_dir))
        proc = self.procs.get("serve")
        deadline = time.time() + BOOT_TIMEOUT
        while time.time() < deadline and not seen["listening"] and proc and proc.poll() is None:
            time.sleep(0.05)
        # Startup recovery answers 503 until the Generation is active, so a 503 is
        # not terminal: keep polling /v1/readyz until the deadline.
        body = None
        deadline = time.time() + BOOT_TIMEOUT
        while time.time() < deadline:
            try:
                with urllib.request.urlopen("http://127.0.0.1:%d/v1/readyz" % port,
                                            timeout=2) as resp:
                    body = json.loads(resp.read().decode())
                if body.get("status") == "ready":
                    break
            except urllib.error.HTTPError:
                pass
            except (OSError, ValueError):
                pass
            body = None
            time.sleep(0.2)
        with self.lock:
            if body and body.get("status") == "ready":
                self.serve.update(state="running", ready=True, error=None,
                                  generation_id=body.get("generation_id"))
            elif seen["listening"]:
                self.serve.update(state="running", ready=False,
                                  error="scout serve is up but has no ready Generation")
            else:
                self.serve.update(state="failed", ready=False,
                                  error=last_error_line(self.lines.get("serve"))
                                  or "scout serve did not report listening")
        self.persist(force=True)
        return self.view("serve")

    def serve_stop(self):
        if not self.stop("serve"):
            api_error("conflict", "scout serve is not running")
        return self.view("serve")

    # -- proxy ----------------------------------------------------
    def proxy(self, method, path, body=None):
        with self.lock:
            serve = dict(self.serve)
        if serve["state"] not in ("running", "starting") or not serve.get("port"):
            api_error("not_ready", "scout serve is not running")
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            "http://127.0.0.1:%d%s" % (serve["port"], path), data=data, method=method,
            headers={"Content-Type": "application/json"} if data else {})
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.status, json.loads(resp.read().decode() or "{}")
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode(errors="replace")
            try:
                return exc.code, json.loads(raw)  # scout bodies pass through verbatim
            except ValueError:
                return exc.code, {"error": {"code": "upstream_error",
                                            "message": raw[:500] or "upstream error"}}
        except (OSError, ValueError) as exc:
            api_error("not_ready", "scout serve is unreachable: %s" % exc)

    def snapshots(self):
        stamp, cached = self.snap_cache
        if time.time() - stamp < SCAN_TTL:
            return cached
        snaps = scan_snapshots(self.cfg)
        self.snap_cache = (time.time(), snaps)
        return snaps

    def watchdog(self):
        """Jobs adopted from demo-state.json have no reaper thread (no Popen). Poll their
        pid so a vanished child surfaces as stale instead of a permanent running claim."""
        while True:
            time.sleep(1.0)
            with self.lock:
                pending = [(n, self.job(n)) for n in ("crawl", "build", "serve")]
            changed = False
            for name, job in pending:
                if name in self.procs or job["state"] not in ("running", "starting"):
                    continue
                if pid_alive(job.get("pid")):
                    continue
                with self.lock:
                    cur = self.job(name)
                    if cur is not job or name in self.procs:
                        continue
                    if name == "serve":
                        cur.update(state="stopped", ready=False, pid=None)
                    else:
                        cur.update(state="stale", pid=None, ended_at_utc=now_iso(),
                                   error=cur.get("error") or "process vanished; no terminal event")
                changed = True
            with self.lock:
                parked = [self.job(n) for n in ("crawl", "build")
                          if self.job(n)["state"] == "stopping" and not pid_alive(self.job(n).get("pid"))]
                for job in parked:
                    job.update(state="stopped", error=None,
                               ended_at_utc=job.get("ended_at_utc") or now_iso())
            if parked:
                changed = True
            if changed:
                self.persist(force=True)

    def shutdown_children(self):
        for name in list(self.procs):
            pid = getattr(self.procs.get(name), "pid", None)
            if pid:
                kill_group(pid)
        with self.lock:
            for name in ("crawl", "build"):
                job = getattr(self, name)
                if job["state"] == "running":
                    job.update(state="stopped", pid=None, ended_at_utc=now_iso())
            self.serve.update(state="stopped", ready=False, pid=None)
        # Keep completed/failed history (activate depends on it) but drop liveness claims.
        self.persist(force=True)


# --------------------------------------------------------------- handler

class Handler(BaseHTTPRequestHandler):
    server_version = "scout-demo/1.0"
    protocol_version = "HTTP/1.1"
    demo = None  # injected in main()

    def log_message(self, fmt, *args):
        if os.environ.get("DEMO_VERBOSE"):
            sys.stderr.write("demo: " + fmt % args + "\n")

    # -- plumbing -------------------------------------------------
    def send_json(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def send_error_json(self, exc):
        if isinstance(exc, ApiError):
            self.send_json(exc.status, {"error": {"code": exc.code, "message": exc.message}})
        else:
            self.send_json(500, {"error": {"code": "internal_error", "message": str(exc)}})

    def body_json(self):
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            api_error("invalid_request", "invalid Content-Length")
        if length > MAX_BODY:
            self.close_connection = True  # unread body desyncs a keep-alive connection
            api_error("invalid_request", "request body exceeds %d bytes" % MAX_BODY)
        raw = self.rfile.read(length) if length else b""
        if not raw.strip():
            return {}
        if "application/json" not in (self.headers.get("Content-Type") or ""):
            api_error("invalid_request", "Content-Type must be application/json")
        try:
            doc = json.loads(raw.decode())
        except (UnicodeDecodeError, ValueError):
            api_error("invalid_request", "request body must be JSON")
        if not isinstance(doc, dict):
            api_error("invalid_request", "request body must be a JSON object")
        return doc

    # -- verbs ----------------------------------------------------
    def do_GET(self):
        self.dispatch()

    def do_HEAD(self):
        self.dispatch()

    def do_POST(self):
        self.dispatch()

    def do_PUT(self):
        self.send_error_json(ApiError("invalid_request", "method not allowed", 405))

    do_DELETE = do_PATCH = do_OPTIONS = do_PUT

    def dispatch(self):
        path = self.path.split("?", 1)[0]
        try:
            if path.startswith("/api/"):
                self.route_api(path)
            elif self.command in ("GET", "HEAD"):
                self.serve_static(path)
            else:
                api_error("invalid_request", "method not allowed", 405)
        except ApiError as exc:
            self.send_error_json(exc)
        except BrokenPipeError:
            pass
        except Exception as exc:  # never answer with an HTML traceback
            self.send_error_json(ApiError("internal_error", str(exc) or exc.__class__.__name__))

    def method(self, allowed):
        if self.command not in allowed:
            api_error("invalid_request", "method not allowed", 405)

    def route_api(self, path):
        get = {"/api/configs": self.r_configs, "/api/state": self.r_state,
               "/api/snapshots": self.r_snapshots, "/api/generation": self.r_generation,
               "/api/readyz": self.r_readyz}
        post = {"/api/crawl/start": self.r_crawl_start, "/api/crawl/stop": self.r_crawl_stop,
                "/api/build/start": self.r_build_start, "/api/index/activate": self.r_activate,
                "/api/serve/start": self.r_serve_start, "/api/serve/stop": self.r_serve_stop,
                "/api/search": self.r_search}
        for routes, verb in ((get, "GET"), (post, "POST")):
            if path in routes:
                self.method((verb, "HEAD") if verb == "GET" else (verb,))
                return routes[path]()
        api_error("not_found", "no such endpoint: %s" % path)

    def relay(self, method, path, body=None):
        status, payload = self.demo.proxy(method, path, body)
        self.send_json(status, payload)

    # -- GET ------------------------------------------------------
    def r_configs(self):
        self.send_json(200, {"configs": scan_configs()})

    def r_state(self):
        cfg, demo = self.demo.cfg, self.demo
        self.send_json(200, {
            "paths": {"prebuilt_data_dir": str(cfg.data_dir), "work_dir": str(cfg.work_dir),
                      "configs_dir": str(CONFIGS_DIR), "scout_bin": str(cfg.scout_bin)},
            "crawl": demo.view("crawl"), "build": demo.view("build"),
            "serve": demo.view("serve"), "snapshots_count": len(demo.snapshots()),
            "modes": list(MODES),
        })

    def r_snapshots(self):
        self.send_json(200, {"snapshots": [
            {k: v for k, v in s.items() if k != "status"} for s in self.demo.snapshots()]})

    def r_generation(self):
        self.relay("GET", "/v1/generation")

    def r_readyz(self):
        self.relay("GET", "/v1/readyz")

    # -- POST -----------------------------------------------------
    def r_crawl_start(self):
        self.send_json(200, {"crawl": self.demo.crawl_start(self.body_json().get("config_id"))})

    def r_crawl_stop(self):
        self.body_json()
        self.send_json(200, {"crawl": self.demo.crawl_stop()})

    def r_build_start(self):
        doc = self.body_json()
        self.send_json(200, {"build": self.demo.build_start(doc.get("snapshot_id"),
                                                            doc.get("root"))})

    def r_serve_start(self):
        doc = self.body_json()
        port = doc.get("port")
        if port is not None and (isinstance(port, bool) or not isinstance(port, int)):
            api_error("invalid_request", "port must be an integer")
        self.send_json(200, {"serve": self.demo.serve_start(doc.get("root"), port)})

    def r_serve_stop(self):
        self.body_json()
        self.send_json(200, {"serve": self.demo.serve_stop()})

    def r_activate(self):
        demo = self.demo
        generation_id = self.body_json().get("generation_id")
        if not isinstance(generation_id, str) or not GENERATION_RE.match(generation_id):
            api_error("invalid_request", "generation_id must be generation-<64 lowercase hex>")
        # Only a generation this demo's own completed build produced may activate.
        build = demo.view("build")
        if build.get("state") != "completed" or build.get("generation_id") != generation_id:
            api_error("invalid_request", "generation %s was not produced by a completed build "
                      "in this demo" % generation_id)
        # No filesystem precheck: a completed build lands in generations/.staging/<run-id>/ and
        # only `scout index activate` seals it, so `scout` decides what is resolvable.
        act = run_scout(demo.cfg, ["index", "activate", "--generation", generation_id,
                                   "--data-dir", str(demo.cfg.work_dir)])
        if act["exit_code"] != 0:
            api_error("upstream_error", "index activate failed (exit %s): %s" % (
                act["exit_code"], (act["stderr"] or act["stdout"]).strip()[:500]))
        ver = run_scout(demo.cfg, ["index", "verify", "--data-dir", str(demo.cfg.work_dir)])
        generation = None
        if demo.view("serve")["state"] in ("running", "starting"):
            try:
                generation = demo.proxy("GET", "/v1/generation")[1]
            except ApiError:
                generation = None
        self.send_json(200, {
            "generation_id": generation_id, "activated": True,
            "verify": {"ok": ver["exit_code"] == 0, "stdout": ver["stdout"].strip()[:2000],
                       "exit_code": ver["exit_code"]},
            "generation": generation,
        })

    def r_search(self):
        doc = self.body_json()
        query = doc.get("query")
        if not isinstance(query, str) or not query.strip():
            api_error("invalid_request", "query must be a non-empty string")
        query = query.strip()
        if len(query) > MAX_QUERY:
            api_error("invalid_request", "query exceeds %d characters" % MAX_QUERY)
        mode = doc.get("mode")
        if mode not in MODES:
            # Never forward a mode-less request: scout would default to hybrid.
            api_error("invalid_request", "mode is required: keyword, semantic, or hybrid")
        limit = doc.get("limit")
        if limit is None:
            limit = DEFAULT_LIMIT
        elif isinstance(limit, bool) or not isinstance(limit, int) or not (1 <= limit <= MAX_LIMIT):
            api_error("invalid_request", "limit must be an integer in 1..%d" % MAX_LIMIT)
        source = doc.get("source")
        if source is not None and not isinstance(source, str):
            api_error("invalid_request", "source must be a string or null")
        payload = {"query": query, "mode": mode, "limit": limit}
        if source:
            payload["source"] = source
        self.relay("POST", "/v1/search", payload)

    # -- static ---------------------------------------------------
    def serve_static(self, path):
        name = {"/": "index.html", "/index.html": "index.html",
                "/app.js": "app.js", "/styles.css": "styles.css"}.get(path)
        if name is None:
            api_error("not_found", "no such path: %s" % path)
        target = STATIC_DIR / name
        if not target.is_file():
            api_error("not_ready", "UI files missing: demo/static/%s is not present" % name)
        body = target.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", {"index.html": "text/html; charset=utf-8",
                                          "app.js": "text/javascript; charset=utf-8",
                                          "styles.css": "text/css; charset=utf-8"}[name])
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)


# ------------------------------------------------------------------ main

class Config:
    pass


def parse_args(argv):
    p = argparse.ArgumentParser(prog="demo/server.py",
                                description="Scout demo control server (UI + proxy).")
    p.add_argument("--data-dir", default="data/real-model-10k-fast-v1")
    p.add_argument("--work-dir", default="data/demo-runs/live")
    p.add_argument("--bind", default="127.0.0.1:8765")
    p.add_argument("--scout-bin", default="target/release/scout")
    p.add_argument("--serve-port", type=int, default=8788)
    return p.parse_args(argv)


def build_config(args):
    cfg = Config()
    cfg.data_dir = Path(args.data_dir).expanduser().resolve()
    if not cfg.data_dir.is_dir():
        sys.exit("demo: --data-dir does not exist: %s" % cfg.data_dir)
    if not (cfg.data_dir / "CURRENT").exists() and not (cfg.data_dir / "corpora").is_dir():
        sys.exit("demo: --data-dir has neither CURRENT nor corpora/: %s" % cfg.data_dir)
    cfg.work_dir = Path(args.work_dir).expanduser().resolve()
    cfg.work_dir.mkdir(parents=True, exist_ok=True)
    cfg.scout_bin = Path(args.scout_bin).expanduser().resolve()
    if not cfg.scout_bin.is_file() or not os.access(cfg.scout_bin, os.X_OK):
        sys.exit("demo: --scout-bin is not an executable file: %s" % cfg.scout_bin)
    host, _, port = args.bind.rpartition(":")
    if not host or not port.isdigit():
        sys.exit("demo: --bind must be host:port, got %r" % args.bind)
    if host not in LOOPBACK:
        sys.exit("demo: --bind host must be loopback (%s), got %r" % (", ".join(LOOPBACK), host))
    cfg.host, cfg.port, cfg.serve_port = "127.0.0.1", int(port), args.serve_port
    return cfg


def main(argv=None):
    cfg = build_config(parse_args(argv))
    demo = Demo(cfg)
    demo.load()
    Handler.demo = demo
    try:
        httpd = ThreadingHTTPServer((cfg.host, cfg.port), Handler)
    except OSError as exc:
        sys.exit("demo: cannot bind %s:%d: %s" % (cfg.host, cfg.port, exc))
    httpd.daemon_threads = True
    threading.Thread(target=demo.watchdog, daemon=True).start()

    def stop(signum, frame):
        threading.Thread(target=httpd.shutdown, daemon=True).start()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    print("demo server listening on http://%s:%d" % (cfg.host, cfg.port), flush=True)
    for label, value in (("prebuilt data dir", cfg.data_dir), ("work dir", cfg.work_dir),
                         ("configs dir", CONFIGS_DIR), ("scout binary", cfg.scout_bin),
                         ("serve port", cfg.serve_port)):
        print("  %-17s %s" % (label + ":", value), flush=True)
    try:
        httpd.serve_forever(poll_interval=0.2)
    finally:
        httpd.server_close()
        demo.shutdown_children()
        print("demo server stopped", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
