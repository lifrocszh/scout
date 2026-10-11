#!/usr/bin/env python3
"""Audit retained measurements after the sequential worker-concurrency sweep."""
import collections
import datetime
import hashlib
import importlib.util
import json
import pathlib
import sys

spec = importlib.util.spec_from_file_location("measurement", pathlib.Path(__file__).with_name("benchmark-distributed.py"))
measurement = importlib.util.module_from_spec(spec)
spec.loader.exec_module(measurement)


def run(root):
    summaries = json.loads((root / "all-comparison.json").read_text())
    derived = []
    for summary in summaries:
        path = root / f"c{summary['worker_concurrency']}" / f"live-w3-r{summary['repetition']}"
        metadata = json.loads((path / "metadata.json").read_text())
        cleanup = json.loads((path / "cleanup.json").read_text())
        assert cleanup["remaining_tasks"] == 0 and cleanup["crawl_run_rows_deleted"] == 1
        assert not any((path / name).exists() for name in ("crawls", "corpora", "logs", "database-state.json"))
        assert summary["source_quotas_met"] and summary["unique_successful_pages"] >= 500
        assert summary["duplicate_same_epoch_claims"] == summary["duplicate_successful_completion_reports"] == 0
        assert summary["uncompleted_permits"] == 0
        assert summary["materialization_exit_code"] == 0
        for name in ("permit_policy", "http_policy"):
            policy = summary[name]
            assert policy["max_active_global"] <= 4
            assert all(count <= 1 for count in policy["max_active_by_origin"].values())
            assert all(gap + policy["observation_tolerance_ms"] >= 500 for gap in policy["minimum_spacing_ms"].values())
        rows = measurement.json_lines(path / "coordinator.stderr")
        rpc = [r for r in rows if r.get("metric") == "rpc"]
        target = min(datetime.datetime.fromisoformat(r["timestamp"]).timestamp() for r in rows
                     if r.get("event") == "capture.accepted" and r["data"].get("target_reached"))
        start, end = metadata["measurement_start_unix"], metadata["measurement_end_unix"]
        assert start < target <= end
        before = [r for r in rpc if start <= r["end_unix_ns"] / 1e9 < target]
        seconds = target - start
        service = sum(r["service_ns"] for r in before) / 1e9
        assert service <= seconds * 1.01
        by_operation = {}
        for operation in sorted({r["operation"] for r in before}):
            selected = [r for r in before if r["operation"] == operation]
            by_operation[operation] = {"calls": len(selected), "rpc_per_second": len(selected) / seconds,
                                       "service_fraction": sum(r["service_ns"] for r in selected) / 1e9 / seconds,
                                       "mutex_wait": measurement.percentile([r["core_lock_wait_ns"] for r in selected]),
                                       "service": measurement.percentile([r["service_ns"] for r in selected])}
        active = collections.Counter()
        maximum = collections.Counter()
        task_events = []
        started_workers = []
        for file in path.glob("worker-*.stderr"):
            worker_rows = measurement.json_lines(file)
            started_workers.extend(r for r in worker_rows if r.get("event") == "worker.started")
            for r in worker_rows:
                event = r.get("event")
                if event in ("worker.task_started", "worker.task_completed", "worker.task_failed", "worker.task_stale"):
                    task_events.append((datetime.datetime.fromisoformat(r["timestamp"]).timestamp(),
                                        1 if event == "worker.task_started" else -1, r["data"]["worker_id"]))
        assert len(started_workers) == 3
        assert all(r["data"]["concurrency"] == summary["worker_concurrency"] for r in started_workers)
        for _, delta, worker in sorted(task_events):
            active[worker] += delta
            active["global"] += delta
            maximum[worker] = max(maximum[worker], active[worker])
            maximum["global"] = max(maximum["global"], active["global"])
        assert all(count == 0 for count in active.values()), active
        assert maximum["global"] <= 3 * summary["worker_concurrency"]
        assert all(maximum[r["data"]["worker_id"]] <= summary["worker_concurrency"] for r in started_workers)
        observers = {}
        for filename, tolerance in (("postgres-samples.jsonl", .3), ("resources.jsonl", .5)):
            records = measurement.json_lines(path / filename)
            stamps = [r["sample_unix"] for r in records]
            assert stamps[0] - start < tolerance and end - stamps[-1] < tolerance
            observers[filename] = {"samples": len(stamps), "start_delay_ms": (stamps[0] - start) * 1000,
                                   "final_gap_ms": (end - stamps[-1]) * 1000,
                                   "max_sample_interval_ms": max(b - a for a, b in zip(stamps, stamps[1:])) * 1000}
        windows = [b for b in summary["ten_second_windows"] if b["duration_seconds"] >= 9.99]
        derived.append({"worker_concurrency": summary["worker_concurrency"], "repetition": summary["repetition"],
                        "run_id": summary["run_id"], "time_to_valid_target_seconds": seconds,
                        "target_normalized_pages_per_minute": 500 / seconds * 60,
                        "drain_seconds": end - target, "service_occupancy_before_target": service / seconds,
                        "rpc_per_second_before_target": len(before) / seconds,
                        "rpc_operations_before_target": by_operation,
                        "max_observed_task_slots": dict(maximum), "observer_coverage": observers,
                        "full_ten_second_windows": len(windows),
                        "windows_at_least_90_percent_service": sum(b["service_occupancy_fraction"] >= .9 for b in windows),
                        "max_full_window_service": max(b["service_occupancy_fraction"] for b in windows),
                        "measurement_start_unix": start, "measurement_end_unix": end,
                        "cleanup_completed_unix": cleanup["completed_unix"]})
    for previous, current in zip(derived, derived[1:]):
        assert previous["cleanup_completed_unix"] < current["measurement_start_unix"], "runs overlap or cleanup occurred late"
    hashes = json.loads((root / "execution-source-hashes.json").read_text())
    for name, digest in hashes.items():
        assert hashlib.sha256((root / "execution-sources" / name).read_bytes()).hexdigest() == digest
    measurement.save(root / "measurement-audit.json", {"runs": derived, "sequential_cleanup_verified": True,
                     "source_archive_hashes_verified": True, "duplicate_and_policy_checks_verified": True})
    for r in derived:
        print(r["worker_concurrency"], r["repetition"], "target seconds", round(r["time_to_valid_target_seconds"], 2),
              "pre-target service", round(r["service_occupancy_before_target"] * 100, 2),
              "pre-target RPC/s", round(r["rpc_per_second_before_target"], 2))


if __name__ == "__main__":
    run(pathlib.Path(sys.argv[1]))
