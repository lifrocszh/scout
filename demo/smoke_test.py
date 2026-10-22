#!/usr/bin/env python3
"""Stdlib smoke test for demo/server.py.

Starts the demo server on a free loopback port, exercises the contract against
the read-only prebuilt tree, and always cleans up the demo server and any
`scout serve` child it started. No crawl and no index build are performed.
"""

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
RESULTS = []


def check(name, fn):
    try:
        fn()
    except AssertionError as exc:
        RESULTS.append((name, False, str(exc)))
        print("FAIL  %s: %s" % (name, exc))
    except Exception as exc:
        RESULTS.append((name, False, "%s: %s" % (exc.__class__.__name__, exc)))
        print("FAIL  %s: %s: %s" % (name, exc.__class__.__name__, exc))
    else:
        RESULTS.append((name, True, ""))
        print("PASS  %s" % name)


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def request(port, path, body=None, method=None):
    url = "http://127.0.0.1:%d%s" % (port, path)
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data, method=method or ("POST" if data is not None else "GET"),
        headers={"Content-Type": "application/json"} if data is not None else {},
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode()
            return resp.status, raw
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()


def as_json(raw):
    try:
        return json.loads(raw)
    except ValueError:
        raise AssertionError("body is not JSON: %r" % raw[:200])


def wait_ready(port, proc, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            raise AssertionError("demo server exited early (code %s)" % proc.returncode)
        try:
            status, raw = request(port, "/api/state")
            if status == 200:
                return as_json(raw)
        except OSError:
            pass
        time.sleep(0.1)
    raise AssertionError("demo server did not become ready in %ss" % timeout)


def main():
    demo_port = free_port()
    serve_port = free_port()
    work_dir = tempfile.mkdtemp(prefix="scout-demo-smoke-")
    proc = subprocess.Popen(
        [
            sys.executable, str(HERE / "server.py"),
            "--bind", "127.0.0.1:%d" % demo_port,
            "--work-dir", work_dir,
            "--scout-bin", str(REPO / "target" / "release" / "scout"),
            "--data-dir", str(REPO / "data" / "real-model-10k-fast-v1"),
        ],
        cwd=str(REPO), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, start_new_session=True,
    )
    state = {"serve_started": False}
    try:
        wait_ready(demo_port, proc)

        def t_ui():
            status, raw = request(demo_port, "/")
            assert status == 200, "GET / status %s" % status
            assert "<html" in raw.lower(), "GET / body has no <html"

        def t_state_cheap():
            t0 = time.time()
            for _ in range(5):
                status, raw = request(demo_port, "/api/state")
                assert status == 200, "GET /api/state status %s" % status
            worst = (time.time() - t0) / 5
            body = as_json(raw)
            for key in ("paths", "crawl", "build", "serve", "snapshots_count", "modes"):
                assert key in body, "/api/state missing %s" % key
            assert body["modes"] == ["keyword", "semantic", "hybrid"], body["modes"]
            assert worst < 0.25, "/api/state too slow: %.3fs avg" % worst

        def t_configs():
            status, raw = request(demo_port, "/api/configs")
            assert status == 200, "status %s" % status
            cfgs = as_json(raw)["configs"]
            ids = sorted(c["id"] for c in cfgs)
            assert ids == ["sources-10k-rebuild-2026-09-30-v2.toml", "sources-2k.toml"], ids
            by_id = {c["id"]: c for c in cfgs}
            assert by_id["sources-2k.toml"]["page_target"] == 2000, by_id["sources-2k.toml"]
            assert by_id["sources-10k-rebuild-2026-09-30-v2.toml"]["page_target"] == 10000
            for c in cfgs:
                assert c["valid"] is True, "%s invalid: %s" % (c["id"], c["error"])
                assert c["source_count"] == 4, c["source_count"]
                assert c["origins"], "no origins for %s" % c["id"]

        def t_serve_start():
            status, raw = request(
                demo_port, "/api/serve/start", {"root": "prebuilt", "port": serve_port}
            )
            assert status == 200, "serve/start status %s: %s" % (status, raw[:300])
            serve = as_json(raw)["serve"]
            state["serve_started"] = True
            assert serve["state"] == "running", "serve state %s (%s)" % (serve["state"], serve["error"])
            assert serve["ready"] is True, "serve not ready: %s" % serve["error"]
            assert serve["port"] == serve_port, serve["port"]
            assert serve["root"] == "prebuilt", serve["root"]

        def t_generation():
            status, raw = request(demo_port, "/api/generation")
            deadline = time.time() + 20
            while status == 503 and "overloaded" in raw and time.time() < deadline:
                time.sleep(0.2)
                status, raw = request(demo_port, "/api/generation")
            assert status == 200, "status %s: %s" % (status, raw[:300])
            gen = as_json(raw)
            assert gen["status"] == "ready", gen
            assert gen["page_count"] == 10002, gen["page_count"]
            assert gen["passage_count"] == 149150, gen["passage_count"]
            assert gen["semantic_count"] == 0, gen["semantic_count"]

        def t_readyz():
            status, raw = request(demo_port, "/api/readyz")
            assert status == 200, "status %s" % status
            assert as_json(raw)["status"] == "ready", raw[:200]

        def t_search_keyword():
            status, raw = request(
                demo_port, "/api/search",
                {"query": "ownership and borrowing", "mode": "keyword", "limit": 3},
            )
            assert status == 200, "status %s: %s" % (status, raw[:300])
            body = as_json(raw)
            results = body.get("results") or []
            assert results, "no results"
            assert body["mode"] == "keyword", body["mode"]
            first = results[0]
            for key in ("rank", "url", "title", "heading_path", "snippet", "score", "page_id"):
                assert key in first, "result missing %s: %s" % (key, sorted(first))
            assert isinstance(first["heading_path"], list), first["heading_path"]

        def t_search_missing_mode():
            status, raw = request(demo_port, "/api/search", {"query": "x"})
            assert status == 400, "status %s: %s" % (status, raw[:200])
            assert as_json(raw)["error"]["code"] == "invalid_request", raw[:200]

        def t_search_semantic():
            status, raw = request(
                demo_port, "/api/search", {"query": "x", "mode": "semantic", "limit": 3}
            )
            assert status == 503, "status %s: %s" % (status, raw[:200])
            assert as_json(raw)["error"]["code"] == "not_ready", raw[:200]

        def t_bad_config():
            status, raw = request(demo_port, "/api/crawl/start", {"config_id": "../etc/passwd"})
            assert status == 400, "status %s: %s" % (status, raw[:200])
            assert as_json(raw)["error"]["code"] == "invalid_config", raw[:200]
            status, raw = request(demo_port, "/api/state")
            crawl = as_json(raw)["crawl"]
            assert crawl["state"] == "idle", "crawl state after bad config: %s" % crawl["state"]
            assert crawl["pid"] in (None, 0), "crawl pid after bad config: %s" % crawl["pid"]
            assert crawl["job_id"] is None, "crawl job created for bad config: %s" % crawl["job_id"]

        def t_bad_snapshot():
            status, raw = request(
                demo_port, "/api/build/start", {"snapshot_id": "../../etc/passwd", "root": "work"}
            )
            assert status == 400, "status %s: %s" % (status, raw[:200])
            assert as_json(raw)["error"]["code"] == "invalid_request", raw[:200]

        def t_build_prebuilt_readonly():
            status, raw = request(demo_port, "/api/snapshots")
            assert status == 200, "status %s" % status
            snaps = as_json(raw)["snapshots"]
            pre = [s for s in snaps if s["root"] == "prebuilt"]
            assert pre, "no prebuilt snapshot listed"
            assert pre[0]["buildable"] is False, pre[0]
            assert pre[0]["generation_id"], "prebuilt snapshot has no generation_id: %s" % pre[0]
            status, raw = request(
                demo_port, "/api/build/start",
                {"snapshot_id": pre[0]["snapshot_id"], "root": "prebuilt"},
            )
            # contract: read_only is a 503
            assert status == 503, "status %s: %s" % (status, raw[:200])
            assert as_json(raw)["error"]["code"] == "read_only", raw[:200]

        def t_activate_refused():
            # no completed build in this demo, so any generation_id must be refused
            bogus = "generation-" + "0" * 64
            status, raw = request(demo_port, "/api/index/activate", {"generation_id": bogus})
            assert status == 400, "status %s: %s" % (status, raw[:200])
            assert as_json(raw)["error"]["code"] == "invalid_request", raw[:200]
            status, raw = request(demo_port, "/api/index/activate", {"generation_id": "x"})
            assert status == 400, "status %s" % status
            assert as_json(raw)["error"]["code"] == "invalid_request", raw[:200]

        def t_unknown_path():
            status, raw = request(demo_port, "/api/nope")
            assert status == 404, "status %s" % status
            assert as_json(raw)["error"]["code"] == "not_found", raw[:200]

        def t_wrong_method():
            status, raw = request(demo_port, "/api/search", method="GET")
            assert status == 405, "status %s: %s" % (status, raw[:200])
            body = as_json(raw)
            assert body["error"]["code"] == "invalid_request", raw[:200]
            assert body["error"]["message"] == "method not allowed", raw[:200]

        def t_traversal_static():
            status, raw = request(demo_port, "/../server.py")
            assert status in (404, 503), "status %s: %s" % (status, raw[:120])

        check("ui: GET / serves html", t_ui)
        check("state: GET /api/state cheap + shaped", t_state_cheap)
        check("configs: exactly the 2 approved configs", t_configs)
        check("serve: start prebuilt is ready", t_serve_start)
        check("generation: 10002 pages / 149150 passages / 0 semantic", t_generation)
        check("readyz: proxied ready", t_readyz)
        check("search: keyword honouring contract", t_search_keyword)
        check("search: missing mode -> 400 invalid_request", t_search_missing_mode)
        check("search: semantic on keyword-only -> 503 not_ready", t_search_semantic)
        check("crawl: bad config_id rejected, nothing spawned", t_bad_config)
        check("build: unsafe snapshot_id rejected", t_bad_snapshot)
        check("build: prebuilt root is read_only", t_build_prebuilt_readonly)
        check("activate: refused without a completed build", t_activate_refused)
        check("routing: unknown api path -> 404", t_unknown_path)
        check("routing: wrong method -> 405", t_wrong_method)
        check("static: path traversal refused", t_traversal_static)

        def t_serve_stop():
            status, raw = request(demo_port, "/api/serve/stop", {})
            assert status == 200, "status %s: %s" % (status, raw[:200])
            serve = as_json(raw)["serve"]
            state["serve_started"] = False
            assert serve["state"] == "stopped", serve["state"]
            assert serve["pid"] is None, serve["pid"]
            status, raw = request(demo_port, "/api/generation")
            assert status == 503, "status after stop %s" % status
            assert as_json(raw)["error"]["code"] == "not_ready", raw[:200]

        check("serve: stop then generation -> 503 not_ready", t_serve_stop)
    finally:
        if state["serve_started"]:
            try:
                request(demo_port, "/api/serve/stop", {})
            except OSError:
                pass
        proc.terminate()
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)
        try:
            os.killpg(os.getpgid(proc.pid), 9)
        except (ProcessLookupError, PermissionError):
            pass
        for path in (Path(work_dir) / "demo-logs", Path(work_dir)):
            try:
                if path.is_dir():
                    for child in sorted(path.iterdir(), reverse=True):
                        child.unlink() if child.is_file() else None
                    path.rmdir()
            except OSError:
                pass

    failed = [r for r in RESULTS if not r[1]]
    print("\n%d/%d checks passed" % (len(RESULTS) - len(failed), len(RESULTS)))
    print("FAIL" if failed else "PASS")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
