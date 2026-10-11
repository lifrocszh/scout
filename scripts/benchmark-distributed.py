#!/usr/bin/env python3
"""Reproducible distributed crawl measurement; Python stdlib and psql only.

Use an isolated database and an already built scout binary. Never builds a Generation.
Raw evidence stays in --output; reruns refuse to overwrite completed run directories.
"""
import argparse
import collections
import hashlib
import http.server
import json
import os
import pathlib
import platform
import signal
import shutil
import socket
import statistics
import subprocess
import threading
import time
import tomllib
import urllib.parse
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[1]


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def sql(args, query):
    result = subprocess.run([args.psql, args.database_url, "-XAt", "-v", "ON_ERROR_STOP=1", "-c", query],
                            text=True, capture_output=True, check=True,
                            env={**os.environ, "PGAPPNAME": "scout-benchmark-query"})
    return json.loads(result.stdout)


def percentile(values):
    values = sorted(values)
    if not values:
        return {"count": 0}
    return {"count": len(values), "p50_ms": statistics.median(values) / 1e6,
            "p95_ms": values[min(len(values) - 1, int(len(values) * .95))] / 1e6,
            "max_ms": max(values) / 1e6, "total_ms": sum(values) / 1e6}


def json_lines(path):
    result = []
    for line in path.read_text().splitlines():
        try:
            value = json.loads(line)
            if isinstance(value, dict):
                result.append(value)
        except ValueError:
            pass  # stdout and psql watch headers accompany JSON records
    return result


def policy(intervals, spacing_ms):
    """Validate observed intervals; end before start on ties respects released permits."""
    active = collections.Counter()
    maximum = collections.Counter()
    events = []
    starts = collections.defaultdict(list)
    for row in intervals:
        start, end, origin = row["start"], row["end"], row["origin"]
        assert end >= start
        events.extend([(start, 1, origin), (end, -1, origin)])
        starts[origin].append(start)
    for _, delta, origin in sorted(events):
        active["global"] += delta
        active[origin] += delta
        maximum["global"] = max(maximum["global"], active["global"])
        maximum[origin] = max(maximum[origin], active[origin])
    gaps = {origin: min((b - a) * 1000 for a, b in zip(sorted(values), sorted(values)[1:]))
            for origin, values in starts.items() if len(values) > 1}
    return {"max_active_global": maximum["global"],
            "max_active_by_origin": {key: value for key, value in maximum.items() if key != "global"},
            "minimum_spacing_ms": gaps,
            "spacing_violations": {key: gap for key, gap in gaps.items() if gap + .001 < spacing_ms}}


class Fixtures:
    def __init__(self):
        self.records = []
        self.lock = threading.Lock()
        self.servers = []
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                start = time.time()
                origin = f"http://127.0.0.1:{self.server.server_port}"
                if self.path == "/robots.txt":
                    body, mime, delay = b"User-agent: *\nAllow: /\n", "text/plain", .12
                else:
                    page = int(self.path.rsplit("/", 1)[1])
                    # A few sub-timeout requests exercise actual lease renewal RPCs.
                    delay = .8 if page == 0 else .12
                    body = (f"<html><head><title>Fixture {self.server.fixture_id} page {page}</title></head>"
                            f"<body><main><h1>Fixture {self.server.fixture_id} page {page}</h1>"
                            f"<p>Distinct deterministic crawl benchmark content from origin {self.server.fixture_id}, "
                            f"page {page}. This documentation explains durable coordination and provides "
                            "enough prose for stable extraction, page admission, passages, and Corpus equivalence."
                            "</p></main></body></html>").encode()
                    mime = "text/html"
                time.sleep(delay)
                self.send_response(200)
                self.send_header("Content-Type", mime)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                with owner.lock:
                    owner.records.append({"origin": origin, "url": origin + self.path,
                                          "start": start, "end": time.time(), "bytes": len(body)})

        for number in range(3):
            server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
            server.fixture_id = number
            threading.Thread(target=server.serve_forever, daemon=True).start()
            self.servers.append(server)

    def config(self, path):
        text = config_header(36, timeout=1)
        for number, server in enumerate(self.servers):
            origin = f"http://127.0.0.1:{server.server_port}"
            pages = [f"{origin}/page/{page}" for page in range(12)]
            text += source(f"fixture-{number}", origin, pages, "/page/", quota=12)
        path.write_text(text)

    def close(self):
        for server in self.servers:
            server.shutdown()
            server.server_close()


def config_header(target, timeout=10):
    return f'''schema_version = "scout.sources.v1"
contact_url = "https://github.com/lifrocszh/scout"
user_agent = "Scout/0.1 (+https://github.com/lifrocszh/scout)"
embedding_model = "BAAI/bge-small-en-v1.5"
page_target = {target}
global_concurrency = 3
origin_concurrency = 1
min_start_spacing_ms = 500
max_frontier = 200
max_decoded_body_bytes = 2097152
max_extraction_bytes = 2097152
max_extraction_millis = 5000
request_timeout_seconds = {timeout}
max_attempts = 2
retry_backoff_ms = 500
retry_max_delay_ms = 2000
max_total_attempts = 100
max_duration_seconds = 120
robots_cache_seconds = 86400
max_passage_tokens = 384
min_content_chars = 100
'''


def source(name, origin, pages, prefix, quota=10):
    return f'''\n[[sources]]
source_id = "{name}"
seeds = {json.dumps(pages)}
allowed_urls = {json.dumps(pages)}
allowed_origins = [{json.dumps(origin)}]
path_prefixes = [{json.dumps(prefix)}]
minimum_page_quota = {quota}
'''


def stop(process):
    if process.poll() is None:
        process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=8)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def pg_watch(args, path):
    query = """SELECT json_build_object('sample_unix',extract(epoch from clock_timestamp()),
      'connections',(SELECT coalesce(json_agg(json_build_object('pid',pid,'application_name',application_name,'backend_type',backend_type,
      'state',state,'wait_type',wait_event_type,'wait_event',wait_event,'query',left(query,500),
      'transaction_age_ms',extract(epoch from clock_timestamp()-xact_start)*1000)), '[]'::json)
      FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND application_name <> 'scout-benchmark-query'),
      'locks',(SELECT coalesce(json_agg(json_build_object('pid',pid,'type',locktype,
      'relation',relation::regclass::text,'mode',mode,'granted',granted,
      'wait_age_ms',extract(epoch from clock_timestamp()-waitstart)*1000)), '[]'::json)
      FROM pg_locks WHERE pid IN (SELECT pid FROM pg_stat_activity WHERE datname=current_database()
      AND pid<>pg_backend_pid() AND application_name <> 'scout-benchmark-query')))"""
    log = path.open("w")
    process = subprocess.Popen([args.psql, args.database_url, "-XAt", "-v", "ON_ERROR_STOP=1"],
                               stdin=subprocess.PIPE, stdout=log, stderr=subprocess.STDOUT, text=True,
                               env={**os.environ, "PGAPPNAME": "scout-benchmark-observer"})
    process.stdin.write(query + ";\n\\watch 0.1\n")
    process.stdin.flush()
    return process, log


def resources(path, processes, done):
    with path.open("w") as log:
        try:
            while not done.is_set():
                text = subprocess.check_output(["ps", "-axo", "pid=,ppid=,%cpu=,rss=,time=,command="], text=True)
                rows = []
                for line in text.splitlines():
                    parts = line.split(None, 5)
                    if len(parts) == 6 and (int(parts[0]) in processes or parts[5].startswith("postgres:") or "/bin/postgres -D " in parts[5]):
                        rows.append({"pid": int(parts[0]), "parent_pid": int(parts[1]),
                                     "cpu_percent_lifetime_average": float(parts[2]), "rss_kib": int(parts[3]),
                                     "cpu_time": parts[4], "role": processes.get(int(parts[0]), "postgres-cluster"),
                                     "command": parts[5]})
                log.write(json.dumps({"sample_unix": time.time(), "processes": rows}) + "\n")
                log.flush()
                done.wait(.25)
        except Exception as error:
            log.write(json.dumps({"observer_error": repr(error)}) + "\n")
            log.flush()


def database_state(args, run):
    # run IDs are generated here, never interpolated from user input.
    return sql(args, f"""SELECT json_build_object(
      'run',(SELECT row_to_json(r) FROM scout_crawl_runs r WHERE crawl_run_id='{run}'),
      'tasks',(SELECT coalesce(json_agg(t),'[]'::json) FROM scout_crawl_tasks t WHERE crawl_run_id='{run}'),
      'results',(SELECT coalesce(json_agg(json_build_object('task_id',task_id,'content_hash',content_hash,
      'rejection_reason',rejection_reason,'body_size',body_size)),'[]'::json)
      FROM scout_crawl_task_results WHERE crawl_run_id='{run}'),
      'groups',(SELECT count(*) FROM scout_crawl_content_groups WHERE crawl_run_id='{run}'),
      'permits',(SELECT coalesce(json_agg(json_build_object('permit_id',permit_id,'request_id',request_id,
      'origin',origin,'kind',request_kind,'status',status,'outcome',outcome,
      'start',extract(epoch from acquired_at),'end',extract(epoch from finished_at),
      'queued',extract(epoch from queued_at))),'[]'::json)
      FROM scout_crawl_request_permits WHERE crawl_run_id='{run}'))""")


def summarize(path, duration, config, state, processes):
    rows = json_lines(path / "coordinator.stderr")
    worker_rows = [row for file in path.glob("worker-*.stderr") for row in json_lines(file)]
    timings = collections.defaultdict(lambda: collections.defaultdict(list))
    for row in rows + worker_rows:
        if row.get("metric") in ("rpc", "database_scope", "worker_rpc"):
            for key, value in row.items():
                if key.endswith("_ns"):
                    timings[row["metric"] + ":" + row["operation"]][key].append(value)
    timings = {name: {key: percentile(values) for key, values in parts.items()} for name, parts in timings.items()}
    http = [row for row in worker_rows if row.get("metric") == "http_attempt"]
    intervals = [{"origin": urllib.parse.urlsplit(row["url"]).scheme + "://" + urllib.parse.urlsplit(row["url"]).netloc,
                  "start": row["start_unix_ns"] / 1e9, "end": row["end_unix_ns"] / 1e9} for row in http]
    acquired = [row for row in state["permits"] if row["start"] is not None and row["end"] is not None]
    claim_keys = [(row["data"]["task_id"], row["data"]["lease_epoch"]) for row in rows if row.get("event") == "task.claimed"]
    completion_keys = [row["data"]["task_id"] for row in worker_rows if row.get("event") == "worker.task_completed"]
    samples = json_lines(path / "postgres-samples.jsonl")
    resource_samples = json_lines(path / "resources.jsonl")
    assert samples, "PostgreSQL observer produced no samples"
    assert resource_samples, "resource observer produced no samples"
    assert not any("observer_error" in r for r in resource_samples), "resource observer failed"
    metadata = json.loads((path / "metadata.json").read_text())
    start, end = metadata["measurement_start_unix"], metadata["measurement_end_unix"]
    for records, tolerance in ((samples, .3), (resource_samples, .5)):
        assert records[0]["sample_unix"] - start < tolerance, "observer started late"
        assert end - records[-1]["sample_unix"] < tolerance, "observer stopped early"
    observed_roles = {r["role"] for sample in resource_samples for r in sample["processes"]}
    assert set(processes.values()) | {"postgres-cluster"} <= observed_roles, "required resource role missing"
    for operation in ("claim_one", "acquire_request_permit", "complete_task"):
        assert "rpc:" + operation in timings and "worker_rpc:" + operation in timings, operation
    if any(source["source_id"].startswith("fixture-") for source in config["sources"]):
        assert "rpc:renew_lease" in timings and "worker_rpc:renew_lease" in timings
    maxima = collections.defaultdict(lambda: {"max_rss_kib": 0, "max_cpu_percent_lifetime_average": 0})
    cpu_by_pid = collections.defaultdict(list)
    for sample in resource_samples:
        totals = collections.defaultdict(lambda: {"rss": 0, "cpu": 0})
        for row in sample["processes"]:
            role = row["role"]
            totals[role]["rss"] += row["rss_kib"]
            totals[role]["cpu"] += row["cpu_percent_lifetime_average"]
            chunks = row["cpu_time"].replace("-", ":").split(":")
            elapsed = sum(float(value) * scale for value, scale in zip(reversed(chunks), [1, 60, 3600, 86400]))
            cpu_by_pid[(role, row["pid"])].append(elapsed)
        for role, row in totals.items():
            maxima[role]["max_rss_kib"] = max(maxima[role]["max_rss_kib"], row["rss"])
            maxima[role]["max_cpu_percent_lifetime_average"] = max(maxima[role]["max_cpu_percent_lifetime_average"], row["cpu"])
    for role in maxima:
        maxima[role]["observed_cpu_seconds"] = sum(max(values) - min(values) for (key, _), values in cpu_by_pid.items() if key == role)
        maxima[role]["observed_average_cpu_percent"] = maxima[role]["observed_cpu_seconds"] / duration * 100
    summary = {"duration_seconds": duration, "unique_successful_pages": state["groups"],
               "pages_per_minute": state["groups"] / duration * 60,
               "rejected_pages": sum(bool(r["rejection_reason"]) for r in state["results"]),
               "failed_tasks": sum(r["status"] == "failed" for r in state["tasks"]),
               "task_failure_events": sum(r.get("event") == "worker.task_failed" for r in worker_rows),
               "duplicate_same_epoch_claims": len(claim_keys) - len(set(claim_keys)),
               "reclaimed_or_retried_tasks": sum(r["task_attempt_count"] > 1 for r in state["tasks"]),
               "duplicate_successful_completion_reports": len(completion_keys) - len(set(completion_keys)),
               "http_attempts": len(http),
               "http_outcomes": dict(collections.Counter(r["outcome"] for r in http)),
               "uncompleted_permits": sum(r["status"] != "completed" for r in state["permits"]),
               "unfinished_tasks": sum(r["status"] in ("pending", "leased") for r in state["tasks"]),
               "decoded_body_bytes": sum(r["decoded_body_bytes"] for r in http),
               "permits_acquired": len(acquired),
               "permit_queue_ms": percentile([(r["start"] - r["queued"]) * 1e9 for r in acquired]),
               "http_policy": policy(intervals, config["min_start_spacing_ms"]),
               "permit_policy": policy(acquired, config["min_start_spacing_ms"]),
               "timings": timings, "process_resources": dict(maxima),
               "postgres_samples": len(samples),
               "max_coordinator_database_connections": max((sum(c["application_name"] == "scout-benchmark-coordinator" for c in r["connections"]) for r in samples), default=0),
               "max_database_client_connections_excluding_observer": max((sum(c["backend_type"] == "client backend" for c in r["connections"]) for r in samples), default=0),
               "postgres_lock_wait_samples": sum(any(c["wait_type"] == "Lock" for c in r["connections"]) for r in samples),
               "postgres_waiting_row_lock_samples": sum(any(not l["granted"] and l["type"] in ("tuple", "transactionid") for l in r["locks"]) for r in samples),
               "max_observed_row_lock_wait_age_ms": max((l["wait_age_ms"] or 0 for r in samples for l in r["locks"] if not l["granted"] and l["type"] in ("tuple", "transactionid")), default=0)}
    for name in ("http_policy", "permit_policy"):
        measure = summary[name]
        assert measure["max_active_global"] <= config["global_concurrency"], (name, measure)
        assert all(v <= config["origin_concurrency"] for v in measure["max_active_by_origin"].values()), (name, measure)
        # Permit starts are authoritative. Worker observation precedes actual socket dispatch;
        # different grant-to-send delays can shorten observed spacing without earlier grants.
        tolerance = 2 if name == "http_policy" else .001
        assert all(gap + tolerance >= config["min_start_spacing_ms"] for gap in measure["minimum_spacing_ms"].values()), (name, measure)
        measure["observation_tolerance_ms"] = tolerance
    summary["requests_per_second"] = len(http) / duration
    summary["rpc_per_second"] = sum(v["server_total_ns"]["count"] for name, v in timings.items() if name.startswith("rpc:")) / duration
    summary["coordinator_service_occupancy_fraction"] = sum(v["service_ns"]["total_ms"] for name, v in timings.items() if name.startswith("rpc:")) / (duration * 1000)
    permits = {r["permit_id"]: r for r in acquired}
    summary["grant_to_observed_send_delay_ms"] = percentile([(r["start_unix_ns"] / 1e9 - permits[r["permit_id"]]["start"]) * 1e9 for r in http if r["permit_id"] in permits])
    assert len(http) <= config["max_total_attempts"]
    assert summary["duplicate_same_epoch_claims"] == 0
    assert summary["duplicate_successful_completion_reports"] == 0
    save(path / "summary.json", summary)
    return summary


def run_once(args, config_path, count, repetition, workload, fixture=None):
    run = "run-" + hashlib.sha256(f"ticket7-{workload}-w{count}-r{repetition}-{uuid.uuid4().hex}".encode()).hexdigest()
    path = args.output / f"{workload}-w{count}-r{repetition}"
    path.mkdir()  # refuse accidental evidence overwrite
    config = tomllib.loads(config_path.read_text())
    port = free_port()
    logs, children, watcher, resource_thread = [], [], None, None
    processes, done = {}, threading.Event()
    before = len(fixture.records) if fixture else 0
    try:
        def launch(role, command, env=None):
            out, err = (path / f"{role}.stdout").open("w"), (path / f"{role}.stderr").open("w")
            logs.extend([out, err])
            child = subprocess.Popen(command, stdout=out, stderr=err, env=env)
            children.append(child)
            processes[child.pid] = role
            return child
        coordinator = launch("coordinator", [str(args.binary), "coordinator", "--config", str(config_path),
                            "--data-dir", str(path), "--bind", f"127.0.0.1:{port}", "--run-id", run],
                            {**os.environ, "SCOUT_DATABASE_URL": args.database_url + ("&" if "?" in args.database_url else "?") + "application_name=scout-benchmark-coordinator",
                             "SCOUT_DISTRIBUTED_METRICS": "1", "PGAPPNAME": "scout-benchmark-coordinator"})
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=.2):
                    break
            except OSError:
                assert coordinator.poll() is None, "coordinator exited; inspect stderr"
                time.sleep(.05)
        else:
            raise TimeoutError("coordinator readiness")
        watcher, watch_log = pg_watch(args, path / "postgres-samples.jsonl")
        logs.append(watch_log)
        resource_thread = threading.Thread(target=resources, args=(path / "resources.jsonl", processes, done))
        resource_thread.start()
        start = time.monotonic()
        workers = [launch(f"worker-{number}", [str(args.binary), "worker", "--coordinator", f"http://127.0.0.1:{port}",
                          "--data-dir", str(path), "--worker-id", f"benchmark-worker-{number}"],
                          {**{key: value for key, value in os.environ.items() if key not in ("SCOUT_DATABASE_URL", "SCOUT_TEST_DATABASE_URL")},
                           "SCOUT_DISTRIBUTED_METRICS": "1",
                           "SCOUT_BENCHMARK_WORKER_CONCURRENCY": str(getattr(args, "worker_concurrency", 1))}) for number in range(1, count + 1)]
        deadline = time.monotonic() + min(config["max_duration_seconds"], getattr(args, "max_run_seconds", config["max_duration_seconds"])) + 30
        while any(worker.poll() is None for worker in workers):
            assert coordinator.poll() is None, "coordinator exited; inspect stderr"
            assert watcher.poll() is None, "PostgreSQL observer exited early; inspect samples"
            if time.monotonic() > deadline:
                raise TimeoutError("workers exceeded crawl budget")
            time.sleep(.05)
        duration = time.monotonic() - start
        ended_unix = time.time()
        assert watcher.poll() is None, "PostgreSQL observer exited early; inspect samples"
        done.set()
        resource_thread.join()
        resource_thread = None
        watcher.terminate()
        watcher.wait(timeout=5)
        watcher = None
        watch_log.flush()
        watch_log.close()
        assert all(worker.returncode == 0 for worker in workers), "worker failed; inspect stderr"
        state = database_state(args, run)
        save(path / "database-state.json", state)
        stop(coordinator)
        assert coordinator.returncode == 0, "coordinator failed graceful shutdown"
        materialized = subprocess.run([str(args.binary), "crawl", "--config", str(config_path), "--data-dir", str(path),
                         "--materialize-spool", run, "--json"], text=True, capture_output=True)
        (path / "materialize.stdout").write_text(materialized.stdout)
        (path / "materialize.stderr").write_text(materialized.stderr)
        if workload != "live":
            assert materialized.returncode == 0, "materialization failed; inspect stderr"
        save(path / "metadata.json", {"run_id": run, "workload": workload, "worker_count": count,
             "worker_concurrency": getattr(args, "worker_concurrency", 1), "repetition": repetition, "config_file_sha256": hashlib.sha256(config_path.read_bytes()).hexdigest(),
             "process_ids": processes, "duration_seconds": duration,
             "measurement_end_unix": ended_unix, "measurement_start_unix": ended_unix-duration, "source_config_sha256": state["run"]["config_digest"]})
        if fixture:
            save(path / "fixture-requests.json", fixture.records[before:])
        summary = summarize(path, duration, config, state, processes)
        manifest_path = path / "crawls" / run / "crawl-manifest.json"
        summary["snapshot_id"] = json.loads(manifest_path.read_text())["snapshot_id"] if manifest_path.exists() else None
        summary["materialization_exit_code"] = materialized.returncode
        if workload != "live":
            assert summary["unique_successful_pages"] == config["page_target"]
            assert summary["failed_tasks"] == summary["rejected_pages"] == 0
            summary["artifact_sha256"] = {name: hashlib.sha256((path / "corpora" / summary["snapshot_id"] / name).read_bytes()).hexdigest()
                                          for name in ("pages.jsonl", "aliases.jsonl", "passages.jsonl")}
        summary.update({"run_id": run, "worker_count": count, "repetition": repetition, "workload": workload})
        save(path / "summary.json", summary)
        print(json.dumps({key: summary[key] for key in ("run_id", "pages_per_minute", "unique_successful_pages", "snapshot_id")}), flush=True)
        return summary
    finally:
        done.set()
        if resource_thread:
            resource_thread.join()
        if watcher:
            stop(watcher)
        for child in reversed(children):
            stop(child)
        for log in logs:
            log.close()
        if getattr(args, "cleanup_crawl_data", False):
            for name in ("crawls", "corpora", "logs"):
                shutil.rmtree(path / name, ignore_errors=True)
            (path / "database-state.json").unlink(missing_ok=True)
            remaining = sql(args, f"WITH removed AS (DELETE FROM scout_crawl_runs WHERE crawl_run_id='{run}' RETURNING 1) SELECT count(*) FROM removed")
            assert sql(args, f"SELECT count(*) FROM scout_crawl_tasks WHERE crawl_run_id='{run}'") == 0
            assert not any((path / name).exists() for name in ("crawls", "corpora", "logs"))
            save(path / "cleanup.json", {"crawl_run_rows_deleted": remaining, "remaining_tasks": 0,
                 "bodies_spool_corpus_event_logs_removed": True, "database_state_dump_removed": True,
                 "completed_unix": time.time()})


def self_test():
    assert percentile([1e6, 3e6])["p50_ms"] == 2
    p = policy([{"origin": "a", "start": 1, "end": 1.2}, {"origin": "a", "start": 1.5, "end": 1.7},
                {"origin": "b", "start": 1.1, "end": 1.4}], 500)
    assert p["max_active_global"] == 2 and p["max_active_by_origin"]["a"] == 1
    assert p["minimum_spacing_ms"]["a"] == 500 and not p["spacing_violations"]
    assert policy([{"origin": "a", "start": 1, "end": 2}, {"origin": "a", "start": 1.1, "end": 2.1}], 500)["spacing_violations"]
    print("benchmark measurement self-check passed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=pathlib.Path, default=ROOT / "target/debug/scout")
    parser.add_argument("--database-url", default=os.environ.get("SCOUT_TEST_DATABASE_URL"))
    parser.add_argument("--psql", default="/opt/homebrew/opt/postgresql@17/bin/psql")
    parser.add_argument("--output", type=pathlib.Path, default=ROOT / "docs/runs/ticket7-2026-10-11")
    parser.add_argument("--live-config", type=pathlib.Path, default=ROOT / "data/configs/benchmark-distributed-live-v1.toml")
    parser.add_argument("--workload", choices=("fixtures", "live", "all"), default="all")
    parser.add_argument("--repetitions", type=int, default=2)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    parser.error("--database-url or SCOUT_TEST_DATABASE_URL required") if not args.database_url else None
    args.binary = args.binary.resolve()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    save(args.output / "environment.json", {"platform": platform.platform(), "machine": platform.machine(),
         "cpu_count": os.cpu_count(), "python": platform.python_version(),
         "physical_memory_bytes": os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES"),
         "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
         "source_diff_sha256": hashlib.sha256(subprocess.check_output(["git", "diff", "--", "src/distributed"], cwd=ROOT)).hexdigest(),
         "runner_sha256": hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),
         "postgres": sql(args, "SELECT to_json(version())"),
         "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
         "git_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
         "sampling": {"postgres_seconds": .1, "resources_seconds": .25},
         "started_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())})
    summaries, fixture = [], None
    try:
        workloads = ["fixtures", "live"] if args.workload == "all" else [args.workload]
        for workload in workloads:
            config_path = args.output / f"{workload}-sources-v1.toml"
            if workload == "fixtures":
                fixture = Fixtures()
                fixture.config(config_path)
            else:
                config_path.write_bytes(args.live_config.read_bytes())
            # Reverse the second pass to reduce monotonic warm-cache/order effects.
            for repetition in range(1, args.repetitions + 1):
                for count in ([1, 2, 3] if repetition % 2 else [3, 2, 1]):
                    summaries.append(run_once(args, config_path, count, repetition, workload, fixture if workload == "fixtures" else None))
            if workload == "fixtures":
                snapshots = {s["snapshot_id"] for s in summaries if s["workload"] == "fixtures"}
                assert len(snapshots) == 1, "deterministic snapshots differ"
                assert len({json.dumps(s["artifact_sha256"], sort_keys=True) for s in summaries if s["workload"] == "fixtures"}) == 1, "deterministic JSONL artifacts differ"
                fixture.close()
                fixture = None
        save(args.output / f"{args.workload}-comparison.json", summaries)
    finally:
        if fixture:
            fixture.close()


if __name__ == "__main__":
    main()
