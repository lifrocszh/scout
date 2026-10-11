#!/usr/bin/env python3
"""Sequential 500-page worker-concurrency sweep; remove crawl data after every run."""
import argparse
import collections
import hashlib
import importlib.util
import json
import os
import pathlib
import platform
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("measurement", ROOT / "scripts/benchmark-distributed.py")
measurement = importlib.util.module_from_spec(spec)
spec.loader.exec_module(measurement)


def analyze(path, duration, config, state, processes, args, original):
    result = original(path, duration, config, state, processes)
    metadata = json.loads((path / "metadata.json").read_text())
    start, end = metadata["measurement_start_unix"], metadata["measurement_end_unix"]
    rpc = [r for r in measurement.json_lines(path / "coordinator.stderr") if r.get("metric") == "rpc"]
    http = [r for file in path.glob("worker-*.stderr") for r in measurement.json_lines(file)
            if r.get("metric") == "http_attempt"]
    bins = []
    for left in range(0, int(duration), 10):
        a, b = start + left, min(start + left + 10, end)
        rows = [r for r in rpc if a <= r["end_unix_ns"] / 1e9 < b]
        requests = [r for r in http if a <= r["start_unix_ns"] / 1e9 < b]
        occupied = sum(max(0, min(b, r["end_unix_ns"] / 1e9) -
                           max(a, (r["end_unix_ns"] - r["service_ns"]) / 1e9)) for r in rpc)
        bins.append({"start_seconds": left, "duration_seconds": b - a,
                     "rpc_per_second": len(rows) / (b - a),
                     "http_starts_per_second": len(requests) / (b - a),
                     "service_occupancy_fraction": occupied / (b - a),
                     "mutex_wait": measurement.percentile([r["core_lock_wait_ns"] for r in rows])})
    by_origin = {}
    for origin in sorted({r["origin"] for r in state["permits"]}):
        attempts = [r for r in http if r["url"].startswith(origin + "/")]
        permits = [r for r in state["permits"] if r["origin"] == origin and r["start"] is not None]
        by_origin[origin] = {"http_attempts": len(attempts),
                            "http_duration": measurement.percentile([r["end_unix_ns"] - r["start_unix_ns"] for r in attempts]),
                            "permit_queue": measurement.percentile([(r["start"] - r["queued"]) * 1e9 for r in permits]),
                            "observed_http_active_fraction": sum(r["end_unix_ns"] - r["start_unix_ns"] for r in attempts) / 1e9 / duration}
    counts = collections.Counter(r["operation"] for r in rpc)
    result.update({"worker_concurrency": args.worker_concurrency,
                   "total_task_slots": 3 * args.worker_concurrency,
                   "ten_second_windows": bins, "origins": by_origin,
                   "rpc_counts": dict(counts),
                   "claim_grants_per_second": sum(r["task_attempt_count"] for r in state["tasks"]) / duration,
                   "permit_grants_per_second": result["permits_acquired"] / duration,
                   "task_completions_per_second": len(state["results"]) / duration,
                   "permit_polls_per_grant": counts["acquire_request_permit"] / max(1, result["permits_acquired"]),
                   "task_status_counts": dict(collections.Counter(r["status"] for r in state["tasks"])),
                   "failures": dict(collections.Counter(r["last_error"] for r in state["tasks"] if r["last_error"])),
                   "capture_counts_by_source": measurement.sql(args, f"SELECT coalesce(json_object_agg(source_id,n),'{{}}'::json) FROM (SELECT source_id,count(*) n FROM scout_crawl_content_groups WHERE crawl_run_id='{metadata['run_id']}' GROUP BY source_id) s")})
    for name, timings in result["timings"].items():
        if name.startswith("rpc:"):
            operation = name.split(":", 1)[1]
            for field in timings:
                values = sorted(r[field] for r in rpc if r["operation"] == operation)
                timings[field]["p99_ms"] = values[min(len(values) - 1, int(len(values) * .99))] / 1e6
    result["source_quotas_met"] = all(result["capture_counts_by_source"].get(s["source_id"], 0) >= s["minimum_page_quota"] for s in config["sources"])
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--binary", type=pathlib.Path, default=ROOT / "target/debug/scout")
    parser.add_argument("--config", type=pathlib.Path, default=ROOT / "data/configs/sources-500.toml")
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--repetitions", type=int, default=2)
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 4, 8, 16, 32])
    parser.add_argument("--max-run-seconds", type=int, default=900)
    args = parser.parse_args()
    args.psql = "/opt/homebrew/opt/postgresql@17/bin/psql"
    args.binary = args.binary.resolve()
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=True)
    config = root / "sources-500.toml"
    if config.exists():
        assert config.read_bytes() == args.config.read_bytes()
    else:
        config.write_bytes(args.config.read_bytes())
    args.cleanup_crawl_data = True
    measurement.save(root / "environment.json", {
        "platform": platform.platform(), "cpu_count": os.cpu_count(),
        "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
        "config_file_sha256": hashlib.sha256(config.read_bytes()).hexdigest(),
        "git_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "postgres": measurement.sql(args, "SELECT to_json(version())"),
        "workers": 3, "concurrency_levels": args.concurrency,
        "repetitions": args.repetitions, "started_unix": time.time(),
        "sampling_seconds": {"postgres": .1, "resources": .25},
        "max_run_seconds": args.max_run_seconds, "build_profile": "debug",
        "policy": "unchanged sources-500.toml", "sequential": True,
        "cleanup_after_each_run": True})
    original = measurement.summarize
    measurement.summarize = lambda *values: analyze(*values, args, original)
    summaries = []
    for repetition in range(1, args.repetitions + 1):
        for concurrency in args.concurrency if repetition % 2 else reversed(args.concurrency):
            args.worker_concurrency = concurrency
            args.output = root / f"c{concurrency}"
            args.output.mkdir(exist_ok=True)
            print(json.dumps({"starting_concurrency": concurrency, "repetition": repetition, "time": time.time()}), flush=True)
            result = measurement.run_once(args, config, 3, repetition, "live")
            summaries.append(result)
            measurement.save(root / "all-comparison.json", summaries)
            assert measurement.sql(args, "SELECT count(*) FROM scout_crawl_runs") == 0, "crawl rows remain before next run"
            # Bound between-run table/index bloat without changing the measured crawl.
            subprocess.run([args.psql, args.database_url, "-Xq", "-v", "ON_ERROR_STOP=1", "-c", "VACUUM ANALYZE"], check=True)


if __name__ == "__main__":
    main()
