#!/usr/bin/env python3
"""Sequential owned-fixture 500-page coordination capacity comparison."""
import argparse
import asyncio
import collections
import datetime
import hashlib
import importlib.util
import json
import os
import pathlib
import platform
import shutil
import statistics
import subprocess
import threading
import time
import tomllib
import urllib.parse

ROOT = pathlib.Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("sweep", ROOT / "scripts/benchmark-distributed-concurrency.py")
sweep = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sweep)
measurement = sweep.measurement


class CapacityFixtures:
    """One async event loop serves four origins without one OS thread per request."""
    def __init__(self, ports=None, discovery=False):
        self.records, self.loop_lag_ms, self.servers = [], [], []
        self.delay = .12
        self.discovery = discovery
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever)
        self.thread.start()
        self.ports = asyncio.run_coroutine_threadsafe(self.start(ports or [0] * 4), self.loop).result()

    async def start(self, ports):
        for origin_id, port in enumerate(ports):
            async def handle(reader, writer, number=origin_id):
                try:
                    while True:
                        header = await reader.readuntil(b"\r\n\r\n")
                        path = header.split(b" ", 2)[1].decode()
                        started = time.time()
                        origin = f"http://127.0.0.1:{writer.get_extra_info('sockname')[1]}"
                        if path == "/robots.txt":
                            body, mime = b"User-agent: *\nAllow: /\n", "text/plain"
                        else:
                            page = int(path.rsplit("/", 1)[1])
                            assert 0 <= page < 125
                            links = "".join(f'<a href="/page/{(page + offset) % 125}">next</a>'
                                            for offset in range(1, 9)) if self.discovery else ""
                            body = (f"<html><head><title>Capacity origin {number} page {page}</title></head>"
                                    f"<body><main><h1>Capacity origin {number} page {page}</h1>"
                                    f"<p>Unique deterministic capacity benchmark document, origin {number}, page {page}. "
                                    "Durable coordination assigns crawl tasks, observes request policy and preserves "
                                    "accepted content. This prose produces stable extracted passages and a sealed Corpus. "
                                    f"</p><p>{links}</p></main></body></html>").encode()
                            mime = "text/html"
                        await asyncio.sleep(self.delay)
                        writer.write(f"HTTP/1.1 200 OK\r\nContent-Type: {mime}\r\nContent-Length: {len(body)}\r\nConnection: keep-alive\r\n\r\n".encode() + body)
                        await writer.drain()
                        self.records.append({"origin": origin, "url": origin + path, "start": started,
                                             "end": time.time(), "bytes": len(body)})
                except (asyncio.IncompleteReadError, ConnectionError):
                    pass
                finally:
                    writer.close()
                    await writer.wait_closed()
            self.servers.append(await asyncio.start_server(handle, "127.0.0.1", port, backlog=512))
        self.lag_task = asyncio.create_task(self.measure_lag())
        return [s.sockets[0].getsockname()[1] for s in self.servers]

    async def measure_lag(self):
        while True:
            start = self.loop.time()
            await asyncio.sleep(.01)
            self.loop_lag_ms.append(max(0, self.loop.time() - start - .01) * 1000)

    def config(self, path, constrained=False):
        text = measurement.config_header(500).replace("global_concurrency = 3", "global_concurrency = 192")
        text = text.replace("origin_concurrency = 1", "origin_concurrency = 192")
        text = text.replace("min_start_spacing_ms = 500", "min_start_spacing_ms = 0")
        text = text.replace("max_frontier = 200", "max_frontier = 1000")
        text = text.replace("max_total_attempts = 100", "max_total_attempts = 2000")
        text = text.replace("max_duration_seconds = 120", "max_duration_seconds = 300")
        origin_cap = 1 if constrained else 192
        for number, port in enumerate(self.ports):
            origin = f"http://127.0.0.1:{port}"
            text += f'''\n[[origins]]
origin = "{origin}"
origin_concurrency = {origin_cap}
min_start_spacing_ms = 0
'''
            all_pages = [f"{origin}/page/{page}" for page in range(125)]
            source_text = measurement.source(f"capacity-{number}", origin, all_pages, "/page/", quota=125)
            if self.discovery:
                source_text = source_text.replace(f"seeds = {json.dumps(all_pages)}",
                                                  f"seeds = {json.dumps(all_pages[:8])}")
            text += source_text + f'politeness_group = "capacity-{number}"\n'
        if constrained:
            text = text.replace("global_concurrency = 192", "global_concurrency = 4")
            text = text.replace("origin_concurrency = 192", "origin_concurrency = 1")
        if path.exists():
            assert path.read_text() == text, "comparison configuration differs"
        else:
            path.write_text(text)

    async def probe(self, concurrency):
        async def request(number):
            started = time.perf_counter()
            reader, writer = await asyncio.open_connection("127.0.0.1", self.ports[number % 4])
            writer.write(f"GET /page/{number % 125} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode())
            await writer.drain()
            header = await reader.readuntil(b"\r\n\r\n")
            length = int(next(line.split(b":", 1)[1] for line in header.split(b"\r\n") if line.lower().startswith(b"content-length:")))
            await reader.readexactly(length)
            writer.close()
            await writer.wait_closed()
            return (time.perf_counter() - started) * 1e9
        started = time.perf_counter()
        latencies = await asyncio.gather(*(request(i) for i in range(concurrency)))
        duration = time.perf_counter() - started
        observed = measurement.policy(self.records, 0)
        self.records.clear()
        return {"concurrent_clients": concurrency, "completed": len(latencies), "duration_seconds": duration,
                "requests_per_second": len(latencies) / duration, "latency": measurement.percentile(latencies),
                "server_observed_policy": observed, "configured_latency_ms": self.delay * 1000}

    def close(self):
        async def close():
            self.lag_task.cancel()
            for server in self.servers:
                server.close()
                await server.wait_closed()
        asyncio.run_coroutine_threadsafe(close(), self.loop).result()
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join()
        self.loop.close()


def derived(path, summary, config, fixture, lag_before):
    metadata = json.loads((path / "metadata.json").read_text())
    rows = measurement.json_lines(path / "coordinator.stderr")
    rpc = [r for r in rows if r.get("metric") == "rpc"]
    target = min(datetime.datetime.fromisoformat(r["timestamp"]).timestamp() for r in rows
                 if r.get("event") == "capture.accepted" and r["data"].get("target_reached"))
    seconds = target - metadata["measurement_start_unix"]
    before = [r for r in rpc if metadata["measurement_start_unix"] <= r["end_unix_ns"] / 1e9 < target]
    slots, maximum = collections.Counter(), collections.Counter()
    events = []
    for file in path.glob("worker-*.stderr"):
        worker_rows = measurement.json_lines(file)
        starts = [r for r in worker_rows if r.get("event") == "worker.started"]
        assert len(starts) == 1 and starts[0]["data"]["concurrency"] == summary["worker_concurrency"]
        for r in worker_rows:
            if r.get("event") in ("worker.task_started", "worker.task_completed", "worker.task_failed", "worker.task_stale"):
                events.append((r["timestamp"], 1 if r["event"] == "worker.task_started" else -1, r["data"]["worker_id"]))
    for _, delta, worker in sorted(events):
        for key in (worker, "global"):
            slots[key] += delta
            maximum[key] = max(maximum[key], slots[key])
    assert not any(slots.values())
    assert maximum["global"] <= 3 * summary["worker_concurrency"]
    assert all(n <= summary["worker_concurrency"] for worker, n in maximum.items() if worker != "global")
    requests = json.loads((path / "fixture-requests.json").read_text())
    assert len(requests) == summary["http_attempts"] == 504
    assert len({r["url"] for r in requests}) == 504
    assert summary["uncompleted_permits"] == summary["unfinished_tasks"] == 0
    assert summary["failed_tasks"] == summary["task_failure_events"] == summary["rejected_pages"] == 0
    assert summary["source_quotas_met"] and summary["unique_successful_pages"] == 500
    result_ids = [row["data"]["task_id"] for file in path.glob("worker-*.stderr")
                  for row in measurement.json_lines(file) if row.get("event") == "worker.task_completed"]
    claimed = [row["data"]["task_id"] for row in rows if row.get("event") == "task.claimed"]
    assert summary["database_result_rows"] == len(result_ids) == len(set(result_ids))
    assert summary["duplicate_database_result_rows"] == 0
    assert len(claimed) == len(set(claimed)) == 500
    assert summary["reclaimed_or_retried_tasks"] == 0
    source_counts = collections.Counter(urllib.parse.urlsplit(r["url"]).netloc for r in requests if "/page/" in r["url"])
    assert len(source_counts) == 4 and set(source_counts.values()) == {125}
    service = sum(r["service_ns"] for r in before) / 1e9
    operations = {}
    for operation in sorted({r["operation"] for r in before}):
        selected = [r for r in before if r["operation"] == operation]
        operations[operation] = {"calls": len(selected), "service_fraction": sum(r["service_ns"] for r in selected) / 1e9 / seconds,
                                 "mutex_wait": measurement.percentile([r["core_lock_wait_ns"] for r in selected]),
                                 "service": measurement.percentile([r["service_ns"] for r in selected])}
    accepted = [row["data"] for row in rows if row.get("event") == "capture.accepted"]
    admitted = sum(row["accepted_discovered_count"] for row in accepted)
    rejected = sum(row["rejected_discovered_count"] for row in accepted)
    assert admitted == (468 if fixture.discovery else 0) and rejected == 0
    phases = collections.defaultdict(list)
    for row in rows:
        if row.get("metric") == "completion_phase":
            phases[row["phase"]].append(row["elapsed_ns"])
    lane_events = [row for row in rows if row.get("event") == "coordinator.started"]
    expected_lanes = int(os.environ.get("SCOUT_COMPLETION_LANES", "4"))
    assert len(lane_events) == 1 and lane_events[0]["data"]["completion_lanes"] == expected_lanes
    summary.update({"time_to_target_seconds": seconds, "drain_seconds": metadata["measurement_end_unix"] - target,
                    "overshoot_pages": summary["unique_successful_pages"] - 500,
                    "target_pages_per_minute": 500 / seconds * 60,
                    "pre_target_service_occupancy_fraction": service / seconds,
                    "pre_target_rpc_per_second": len(before) / seconds,
                    "pre_target_operations": operations,
                    "dispatch_service_fraction": operations.get("dispatch", {}).get("service_fraction", 0),
                    "complete_task_service_fraction": operations.get("complete_task", {}).get("service_fraction", 0),
                    "completion_phase_timings": {key: measurement.percentile(values) for key, values in phases.items()},
                    "admitted_discoveries": admitted, "rejected_discoveries": rejected,
                    "captures_with_admissions": sum(row["accepted_discovered_count"] > 0 for row in accepted),
                    "reported_completion_lanes": lane_events[0]["data"]["completion_lanes"],
                    "unique_result_rows": len(result_ids), "unique_claimed_tasks": len(claimed),
                    "observed_task_slots": dict(maximum),
                    "fixture_server_policy": measurement.policy(requests, 0),
                    "fixture_loop_lag": measurement.percentile([v * 1e6 for v in fixture.loop_lag_ms[lag_before:]]),
                    "fixture_server_http_duration": measurement.percentile([(r["end"] - r["start"]) * 1e9 for r in requests]),
                    "exact_page_coverage_verified": True, "all_task_leases_drained": True})
    measurement.save(path / "summary.json", summary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--binary", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--variant", choices=("baseline", "candidate"), required=True)
    parser.add_argument("--workload", choices=("page-only", "discovery"), default="page-only")
    parser.add_argument("--policy-constrained", action="store_true")
    parser.add_argument("--repetitions", type=int, default=2)
    parser.add_argument("--concurrency", nargs="+", type=int, default=[1, 4, 8, 16, 32])
    args = parser.parse_args()
    loaded_source_hashes = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in (pathlib.Path(__file__).resolve(), ROOT / "scripts/benchmark-distributed-concurrency.py", ROOT / "scripts/benchmark-distributed.py")}
    args.psql = "/opt/homebrew/opt/postgresql@17/bin/psql"
    args.binary, root = args.binary.resolve(), args.output.resolve()
    root.mkdir(parents=True, exist_ok=True)
    config_path = root / "fixture-sources-500.toml"
    ports = None
    if config_path.exists():
        ports = [urllib.parse.urlsplit(s["allowed_origins"][0]).port for s in tomllib.loads(config_path.read_text())["sources"]]
    fixture = CapacityFixtures(ports, discovery=args.workload == "discovery")
    args.cleanup_crawl_data, args.max_run_seconds = True, 300
    original = measurement.summarize
    measurement.summarize = lambda *values: sweep.analyze(*values, args, original)
    summaries = []
    try:
        fixture.config(config_path, args.policy_constrained)
        probe = asyncio.run_coroutine_threadsafe(fixture.probe(192), fixture.loop).result()
        assert probe["completed"] == 192 and probe["server_observed_policy"]["max_active_global"] == 192
        assert probe["latency"]["p95_ms"] < 250, "fixture server lacks benchmark headroom"
        measurement.save(root / f"{args.variant}-fixture-probe.json", probe)
        environment = {"variant": args.variant, "workload": args.workload, "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
                       "config_sha256": hashlib.sha256(config_path.read_bytes()).hexdigest(), "platform": platform.platform(),
                       "workers": 3, "concurrency_levels": args.concurrency, "repetitions": args.repetitions,
                       "completion_lanes": int(os.environ.get("SCOUT_COMPLETION_LANES", "4")),
                       "fixture_latency_ms": 120, "fixture_origins": 4, "pages_per_origin": 125,
                       "global_http_cap": 4 if args.policy_constrained else 192,
                       "origin_http_cap": 1 if args.policy_constrained else 192, "spacing_ms": 0,
                       "build_profile": "debug", "sequential": True, "cleanup_after_each_run": True,
                       "startup_runner_source_hashes": loaded_source_hashes}
        measurement.save(root / f"{args.variant}-environment.json", environment)
        for repetition in range(1, args.repetitions + 1):
            for concurrency in args.concurrency if repetition % 2 else reversed(args.concurrency):
                args.worker_concurrency = concurrency
                args.output = root / args.variant / f"c{concurrency}"
                args.output.mkdir(parents=True, exist_ok=True)
                print(json.dumps({"variant": args.variant, "workload": args.workload, "concurrency": concurrency, "repetition": repetition, "started_unix": time.time()}), flush=True)
                lag_before = len(fixture.loop_lag_ms)
                result = measurement.run_once(args, config_path, 3, repetition, "capacity", fixture)
                path = args.output / f"capacity-w3-r{repetition}"
                derived(path, result, tomllib.loads(config_path.read_text()), fixture, lag_before)
                if summaries:
                    previous = root / args.variant / f"c{summaries[-1]['worker_concurrency']}" / f"capacity-w3-r{summaries[-1]['repetition']}"
                    assert json.loads((previous / "cleanup.json").read_text())["completed_unix"] < json.loads((path / "metadata.json").read_text())["measurement_start_unix"]
                summaries.append(result)
                measurement.save(root / f"{args.variant}-comparison.json", summaries)
                assert measurement.sql(args, "SELECT count(*) FROM scout_crawl_runs") == 0
                subprocess.run([args.psql, args.database_url, "-Xq", "-v", "ON_ERROR_STOP=1", "-c", "VACUUM ANALYZE"], check=True)
        assert len({s["snapshot_id"] for s in summaries}) == 1
        assert len({json.dumps(s["artifact_sha256"], sort_keys=True) for s in summaries}) == 1
        measurement.save(root / f"{args.variant}-verification.json", {"runs": len(summaries), "exact_pages": 500,
             "identical_corpus_artifacts": True, "sequential_cleanup_verified": True,
             "snapshot_id": summaries[0]["snapshot_id"], "artifact_sha256": summaries[0]["artifact_sha256"],
             "remaining_run_rows": measurement.sql(args, "SELECT count(*) FROM scout_crawl_runs")})
    finally:
        fixture.close()


if __name__ == "__main__":
    main()
