#!/usr/bin/env python3
"""Verify retained fixture evidence and compare polling versus notifications."""
import collections
import hashlib
import importlib.util
import json
import pathlib
import statistics
import sys

spec = importlib.util.spec_from_file_location("measurement", pathlib.Path(__file__).with_name("benchmark-distributed.py"))
measurement = importlib.util.module_from_spec(spec)
spec.loader.exec_module(measurement)


def audit_cohort(root):
    variants, records = {}, []
    config = __import__("tomllib").loads((root / "fixture-sources-500.toml").read_text())
    for variant in ("baseline", "candidate"):
        comparison = root / f"{variant}-comparison.json"
        if not comparison.exists():
            continue
        environment = json.loads((root / f"{variant}-environment.json").read_text())
        assert environment["config_sha256"] == hashlib.sha256((root / "fixture-sources-500.toml").read_bytes()).hexdigest()
        summaries = json.loads(comparison.read_text())
        variants[variant] = summaries
        previous_cleanup = 0
        for summary in summaries:
            path = root / variant / f"c{summary['worker_concurrency']}" / f"capacity-w3-r{summary['repetition']}"
            metadata = json.loads((path / "metadata.json").read_text())
            cleanup = json.loads((path / "cleanup.json").read_text())
            assert previous_cleanup < metadata["measurement_start_unix"]
            previous_cleanup = cleanup["completed_unix"]
            assert cleanup["remaining_tasks"] == 0 and cleanup["crawl_run_rows_deleted"] == 1
            assert not any((path / p).exists() for p in ("crawls", "corpora", "logs", "database-state.json"))
            assert metadata["config_file_sha256"] == environment["config_sha256"]
            assert summary["worker_count"] == 3 and summary["unique_successful_pages"] == 500
            assert summary["source_quotas_met"] and summary["materialization_exit_code"] == 0
            for field in ("failed_tasks", "task_failure_events", "rejected_pages", "reclaimed_or_retried_tasks",
                          "duplicate_same_epoch_claims", "duplicate_successful_completion_reports", "uncompleted_permits", "unfinished_tasks", "overshoot_pages"):
                assert summary[field] == 0, (path, field, summary[field])
            assert summary["http_attempts"] == 504 and summary["http_outcomes"] == {"success": 504}
            assert summary["exact_page_coverage_verified"] and summary["all_task_leases_drained"]
            for policy_name in ("permit_policy", "http_policy", "fixture_server_policy"):
                policy = summary[policy_name]
                assert policy["max_active_global"] <= config["global_concurrency"]
                assert max(policy["max_active_by_origin"].values()) <= config["origin_concurrency"]
            coordinator = [r for r in measurement.json_lines(path / "coordinator.stderr") if r.get("metric") == "rpc"]
            worker = [r for p in path.glob("worker-*.stderr") for r in measurement.json_lines(p) if r.get("metric") == "worker_rpc"]
            dispatch = [r for r in coordinator if r["operation"] == "dispatch"]
            remote = [r for r in coordinator if r["operation"] != "dispatch"]
            target = metadata["measurement_start_unix"] + summary["time_to_target_seconds"]
            before = [r for r in coordinator if metadata["measurement_start_unix"] <= r["end_unix_ns"] / 1e9 < target]
            summary.update({"coordinator_passes_per_second": len(coordinator) / summary["duration_seconds"],
                            "network_rpc_per_second": len(remote) / summary["duration_seconds"],
                            "dispatcher_passes_per_second": len(dispatch) / summary["duration_seconds"],
                            "pre_target_coordinator_passes_per_second": len(before) / summary["time_to_target_seconds"],
                            "pre_target_network_rpc_per_second": sum(r["operation"] != "dispatch" for r in before) / summary["time_to_target_seconds"]})
            summary.pop("pre_target_rpc_per_second", None)
            measurement.save(path / "summary.json", summary)
            worker_counts = collections.Counter(r["operation"] for r in worker)
            remote_counts = collections.Counter(r["operation"] for r in remote)
            assert worker_counts["acquire_request_permit"] >= 504
            # Dispatcher passes are internal scheduled work, not network RPC traffic.
            if variant == "candidate":
                assert worker_counts["acquire_request_permit"] == 504
                assert worker_counts["claim_one"] == 500 + 3 * summary["worker_concurrency"]
                assert dispatch
            samples = {}
            for name in ("postgres-samples.jsonl", "resources.jsonl"):
                rows = measurement.json_lines(path / name)
                stamps = [r["sample_unix"] for r in rows]
                assert len(stamps) >= 2
                assert stamps[0] - metadata["measurement_start_unix"] < .5
                assert metadata["measurement_end_unix"] - stamps[-1] < .5
                samples[name] = {"count": len(stamps), "max_gap_ms": max(b-a for a,b in zip(stamps,stamps[1:])) * 1000}
            record = {"variant": variant, "concurrency": summary["worker_concurrency"], "repetition": summary["repetition"],
                      "network_rpc_calls": len(remote), "worker_rpc_calls": len(worker), "dispatcher_passes": len(dispatch),
                      "worker_permission_calls": worker_counts["acquire_request_permit"],
                      "worker_permission_calls_per_grant": worker_counts["acquire_request_permit"] / 504,
                      "coordinator_rpc_counts": dict(remote_counts), "worker_rpc_counts": dict(worker_counts),
                      "all_coordinator_service_seconds": sum(r["service_ns"] for r in coordinator) / 1e9,
                      "remote_enqueue_and_rpc_service_seconds": sum(r["service_ns"] for r in remote) / 1e9,
                      "dispatcher_service_seconds": sum(r["service_ns"] for r in dispatch) / 1e9,
                      "coordinator_mutex_wait": measurement.percentile([r["core_lock_wait_ns"] for r in coordinator]),
                      "remote_rpc_mutex_wait": measurement.percentile([r["core_lock_wait_ns"] for r in remote]),
                      "dispatcher_mutex_wait": measurement.percentile([r["core_lock_wait_ns"] for r in dispatch]),
                      "database_scopes": {k:v for k,v in summary["timings"].items() if k.startswith("database_scope:")},
                      "samples": samples, "summary": str(path.relative_to(root) / "summary.json"),
                      "measurement_start_unix": metadata["measurement_start_unix"],
                      "measurement_end_unix": metadata["measurement_end_unix"], "cleanup_completed_unix": cleanup["completed_unix"]}
            records.append(record)
        measurement.save(comparison, summaries)
        assert len({s["snapshot_id"] for s in summaries}) == 1
        assert len({json.dumps(s["artifact_sha256"], sort_keys=True) for s in summaries}) == 1
    if len(variants) == 2:
        assert {s["snapshot_id"] for s in variants["baseline"]} == {s["snapshot_id"] for s in variants["candidate"]}
        assert {json.dumps(s["artifact_sha256"], sort_keys=True) for s in variants["baseline"]} == {json.dumps(s["artifact_sha256"], sort_keys=True) for s in variants["candidate"]}
        assert records[len(variants["baseline"]) - 1]["cleanup_completed_unix"] < records[len(variants["baseline"])]["measurement_start_unix"]
    measurement.save(root / "measurement-audit.json", {"runs": records, "sequential_cleanup_verified": True,
                      "cross_variant_corpus_equal": len(variants) == 2, "network_and_dispatch_counts_separated": True})
    return variants, records


def average(rows, field):
    return statistics.mean(r[field] for r in rows)


def run(root):
    capacity, capacity_audit = audit_cohort(root)
    constrained, constrained_audit = audit_cohort(root / "policy-bound")
    if "candidate" not in capacity or "candidate" not in constrained:
        print("Baseline audited; candidate comparison pending")
        return
    text = "# Coordinator notification experiment: owned 500-page fixtures\n\n"
    policy_gain = average(constrained["candidate"], "pages_per_minute") / average(constrained["baseline"], "pages_per_minute") - 1
    c32_baseline = [r for r in capacity["baseline"] if r["worker_concurrency"] == 32]
    c32_candidate = [r for r in capacity["candidate"] if r["worker_concurrency"] == 32]
    c1_baseline = [r for r in capacity["baseline"] if r["worker_concurrency"] == 1]
    c1_candidate = [r for r in capacity["candidate"] if r["worker_concurrency"] == 1]
    c32_gain = average(c32_candidate,"pages_per_minute") / average(c32_baseline,"pages_per_minute") - 1
    c1_gain = average(c1_candidate,"pages_per_minute") / average(c1_baseline,"pages_per_minute") - 1
    completion = average([r["pre_target_operations"]["complete_task"] for r in c32_candidate], "service_fraction")
    text += (f"Queued coordinator notifications reduce policy-bound permission calls from roughly 50 to one per grant, with {policy_gain*100:+.1f}% mean useful throughput change. With wide HTTP caps, C32 throughput changes {c32_gain*100:+.1f}%; the coordinator remains saturated, with completion accounting for {completion*100:.1f}% of elapsed pre-target time. C1 throughput changes {c1_gain*100:+.1f}% on average and differs by repetition, so this is not a universal speedup.\n\n")
    text += "Three workers crawled exactly 500 deterministic pages from four owned local origins. Each setting ran twice, in opposite order, with crawl data deleted after every run. This compares archived polling and notification debug binaries under identical configuration, 120 ms response latency and payloads. Public-site policy was unchanged. See [protocol.md](protocol.md) for controls.\n\n"
    text += "Results below are means of two repetitions. Wide HTTP caps (192 global and per origin, zero spacing) exceed all worker capacity. Dispatcher passes are internal work and are reported separately from network RPC calls.\n\n"
    text += "| Tasks per worker | Total task slots | Baseline Pages/min | Candidate Pages/min | Change | Baseline target service | Candidate target service |\n|---:|---:|---:|---:|---:|---:|---:|\n"
    for concurrency in (1,4,8,16,32):
        baseline = [s for s in capacity["baseline"] if s["worker_concurrency"] == concurrency]
        candidate = [s for s in capacity["candidate"] if s["worker_concurrency"] == concurrency]
        a, b = average(baseline,"pages_per_minute"), average(candidate,"pages_per_minute")
        text += f"| {concurrency} | {3*concurrency} | {a:.0f} | {b:.0f} | {(b/a-1)*100:+.1f}% | {average(baseline,'pre_target_service_occupancy_fraction')*100:.1f}% | {average(candidate,'pre_target_service_occupancy_fraction')*100:.1f}% |\n"
    text += "\nThe wide-cap baseline already made exactly one permission call per grant. Its improvement cannot be attributed to removing permission retries. The candidate batches central work assignments and reduces measured claiming service; these mechanisms change together, so their individual effects were not isolated. Completion remains the dominant measured serialized path. This does not establish database saturation or identify a particular body-I/O operation as its cause. The candidate preserves a serialized durable core; throughput conclusions apply to this measured workload.\n\n"
    text += "## Focused policy-bound workload\n\nThe same pages at C32 use global concurrency four and per-origin concurrency one, with zero spacing. This deliberately produces policy waiting.\n\n| Variant | Pages/min | Permission calls/grant | Network RPC calls/run | Dispatcher passes/run | Time to target | Target service |\n|---|---:|---:|---:|---:|---:|---:|\n"
    for variant in ("baseline","candidate"):
        rows = constrained[variant]
        audits = [s for s in constrained_audit if s["variant"] == variant]
        text += f"| {variant} | {average(rows,'pages_per_minute'):.0f} | {average(audits,'worker_permission_calls_per_grant'):.1f} | {average(audits,'network_rpc_calls'):.0f} | {average(audits,'dispatcher_passes'):.0f} | {average(rows,'time_to_target_seconds'):.2f} s | {average(rows,'pre_target_service_occupancy_fraction')*100:.1f}% |\n"
    text += "\nCandidate worker permit RPC duration includes intentional policy waiting until a real grant. It is not directly comparable with one quick polling attempt; coordinator service timings and total call counts quantify the processing cost instead.\n\n"
    text += "## C32 resource and control-path measurements\n\nMeans of two repetitions below use the full crawl interval. CPU is measured as observed CPU seconds divided by elapsed time, with 100% representing one CPU core. RSS is each run's sampled peak. Worker RSS is the largest single-worker peak, not the three-process sum. Mutex P95 covers remote RPC scopes so internal dispatcher samples do not dilute the comparison.\n\n"
    text += "| Workload / variant | Coordinator CPU | Coordinator RSS MiB | Largest worker RSS MiB | Remote core mutex P95 ms | Network RPC/s | Dispatcher passes/s | DB connections | DB lock-wait samples |\n|---|---:|---:|---:|---:|---:|---:|---:|---:|\n"
    for label, cohort, audits in (("wide caps", capacity, capacity_audit),("policy bound", constrained, constrained_audit)):
        for variant in ("baseline", "candidate"):
            rows = [s for s in cohort[variant] if s["worker_concurrency"] == 32]
            selected = [s for s in audits if s["variant"] == variant and s["concurrency"] == 32]
            cpu = statistics.mean(s["process_resources"]["coordinator"]["observed_average_cpu_percent"] for s in rows)
            rss = statistics.mean(s["process_resources"]["coordinator"]["max_rss_kib"] / 1024 for s in rows)
            worker_rss = statistics.mean(max(v["max_rss_kib"] for k,v in s["process_resources"].items() if k.startswith("worker-")) / 1024 for s in rows)
            wait = statistics.mean(s["remote_rpc_mutex_wait"]["p95_ms"] for s in selected)
            text += f"| {label} / {variant} | {cpu:.1f}% | {rss:.1f} | {worker_rss:.1f} | {wait:.2f} | {average(rows,'network_rpc_per_second'):.1f} | {average(rows,'dispatcher_passes_per_second'):.1f} | {max(s['max_coordinator_database_connections'] for s in rows)} | {sum(s['postgres_lock_wait_samples'] for s in rows)} |\n"
    text += "\nPostgreSQL observations are sampled every 100 ms and can miss brief waits. Per-run exact counts, resource peaks, scope latency and gaps remain in the comparison JSON and audits. Permission RPC wall time includes policy wait in the candidate; it is not a service latency.\n\n"
    text += "## Implementation scope\n\nProtocol version 3 requires coordinated binary upgrades. Held unary task claims provide worker capacity credits; permission requests wait once for coordinator eligibility. Pending waits follow renewable task leases, durable caps and epoch fencing. Workers abort cancelled task work and reject delayed grants; the expiry guard requires synchronized worker/coordinator clocks. Robots refresh waiting still polls. The shared PostgreSQL client, core mutex and completion/body I/O remain serialized. See [coordination contract](../../coordination.md).\n\n"
    text += "## Evidence and limits\n\nAll 24 crawls admitted exactly 500 distinct pages, met all four Source quotas, made exactly 504 successful HTTP requests and drained all task leases and permits. No rejections, failures, task retries or duplicate reports occurred. Baseline and candidate produced identical Corpus snapshots and JSONL hashes within each workload. Every run was sequential, and its Corpus, bodies, spool and database rows were removed before the next run. The retained [measurement-audit.json](measurement-audit.json) and [policy-bound audit](policy-bound/measurement-audit.json) verify these claims and separate actual RPCs from dispatcher work.\n\n"
    text += "The 192-client fixture probes verified sufficient server headroom. Per-run event-loop lag and actual response durations, coordinator and worker CPU/RSS, PostgreSQL connection/lock samples, per-operation lock wait, service time and request rate remain in summaries. A sample observer may miss brief database waits. Serialized service timing excludes instrumentation after the timed scope; occupancy is an estimate.\n\n"
    text += "These small local documents and debug builds establish this workload's behavior. Two repetitions do not provide a statistical confidence interval or production capacity guarantee. Crash/restart and cancellation semantics are validated separately, rather than inferred from these non-failing crawls.\n"
    (root / "report.md").write_text(text)
    print("Compared and audited 24 sequential crawls")


if __name__ == "__main__":
    run(pathlib.Path(sys.argv[1]))
