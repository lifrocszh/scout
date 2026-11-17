#!/usr/bin/env python3
"""Crawl throughput against worker count, on the Helm release (kind). Python stdlib, kubectl, helm.

Every run is one crawl of the same synthetic origin set (many origins, so per-origin politeness does
not cap the result) with the same page budget, on a fresh run ID and a fresh coordinator. A run
records pages per second, task latency, coordinator DB-pool wait and the CPU each role used.
Raw evidence (event logs, /metrics scrapes, CPU samples) stays in --output; results.json holds one
summary per run. scripts/analyze-distributed-scaling.py turns it into the chart and report.

Shares its helpers with benchmark-distributed.py. `--self-test` checks the parsing and arithmetic
without a cluster.
"""
import argparse
import collections
import datetime
import importlib.util
import json
import pathlib
import platform
import secrets
import subprocess
import sys
import threading
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("measurement", ROOT / "scripts/benchmark-distributed.py")
measurement = importlib.util.module_from_spec(spec)
spec.loader.exec_module(measurement)

CHART = ROOT / "deploy/helm/scout"
FIRST_PORT = 9000


# ---- parsing and arithmetic (no cluster needed) -------------------------------------------------
def parse_time(text):
    return datetime.datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()


def parse_metrics(text):
    """Prometheus text format -> {name: [(labels dict, value)]}."""
    series = collections.defaultdict(list)
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        name, _, rest = line.partition("{")
        labels = {}
        if rest:
            body, _, value = rest.rpartition("}")
            for pair in filter(None, body.split(",")):
                key, _, raw = pair.partition("=")
                labels[key] = raw.strip('"')
        else:
            name, _, value = line.partition(" ")
        try:
            series[name.strip()].append((labels, float(value.split()[0])))
        except (ValueError, IndexError):
            pass
    return series


def histogram(series, name):
    """(count, sum, [(upper bound, cumulative count)]) of an unlabelled histogram."""
    buckets = sorted((float("inf") if labels["le"] == "+Inf" else float(labels["le"]), value)
                     for labels, value in series.get(name + "_bucket", []))
    total = sum(value for _, value in series.get(name + "_sum", []))
    count = sum(value for _, value in series.get(name + "_count", []))
    return count, total, buckets


def histogram_delta(after, before):
    """The histogram of what happened between two scrapes."""
    count = after[0] - before[0]
    earlier = dict(before[2])
    return count, after[1] - before[1], [(bound, value - earlier.get(bound, 0)) for bound, value in after[2]]


def quantile(buckets, q):
    """Prometheus-style estimate: linear inside the bucket the quantile falls in."""
    if not buckets or buckets[-1][1] <= 0:
        return None
    rank = q * buckets[-1][1]
    lower, below = 0.0, 0.0
    for bound, cumulative in buckets:
        if cumulative >= rank:
            if bound == float("inf"):
                return lower
            span = cumulative - below
            return bound if span <= 0 else lower + (bound - lower) * (rank - below) / span
        lower, below = bound, cumulative
    return lower


def events_named(rows, name):
    return [row for row in rows if row.get("event") == name]


def task_latencies(worker_rows):
    """Seconds from worker.task_started to the matching finish, keyed by (worker, task)."""
    started, result = {}, []
    for row in worker_rows:
        key = (row["data"].get("worker_id"), row["data"].get("task_id"))
        if row["event"] == "worker.task_started":
            started[key] = parse_time(row["timestamp"])
        elif row["event"] in ("worker.task_completed", "worker.task_failed", "worker.task_stale") and key in started:
            result.append((parse_time(row["timestamp"]), parse_time(row["timestamp"]) - started.pop(key)))
    return result


def crawl_window(coordinator_rows, workers):
    """When did the crawl run with every worker busy, and how many pages did it accept then?

    The window opens when the last of the `workers` first claims a task and closes when the page
    target is reached. Workers that never claimed anything are reported, not hidden.
    """
    claims = events_named(coordinator_rows, "task.claimed")
    first_claim = {}
    for row in claims:
        first_claim.setdefault(row["data"]["worker_id"], parse_time(row["timestamp"]))
    accepted = sorted(parse_time(row["timestamp"]) for row in events_named(coordinator_rows, "capture.accepted"))
    completed = events_named(coordinator_rows, "crawl.completed")
    if not claims or not accepted or not completed:
        raise ValueError("the coordinator log has no claims, accepted captures or crawl.completed")
    begin = min(parse_time(row["timestamp"]) for row in claims)
    end = parse_time(completed[0]["timestamp"])
    full = max(first_claim.values())
    # With fewer than `workers` workers claiming, the whole run is all there is to measure.
    window_start = full if len(first_claim) >= workers and full < end else begin
    pages_in_window = sum(1 for t in accepted if window_start < t <= end)
    return {"first_claim_unix": begin, "window_start_unix": window_start, "end_unix": end,
            "stop_reason": completed[0]["data"].get("stop_reason"),
            "workers_that_claimed": len(first_claim),
            "pages_accepted": len(accepted), "pages_in_window": pages_in_window,
            "window_seconds": end - window_start, "run_seconds": end - begin,
            "pages_per_second": pages_in_window / (end - window_start) if end > window_start else None,
            "run_pages_per_second": len(accepted) / (end - begin) if end > begin else None}


def cpu_cores(samples, start, end):
    """Average cores used between two instants, from (unix time, cumulative CPU microseconds)."""
    inside = [(t, usec) for t, usec in samples if t >= start - 0.001 and t <= end + 5]
    before = [(t, usec) for t, usec in samples if t <= start]
    first = before[-1] if before else (inside[0] if inside else None)
    after = [(t, usec) for t, usec in samples if t >= end]
    last = after[0] if after else (inside[-1] if inside else None)
    if first is None or last is None or last[0] <= first[0]:
        return None
    return (last[1] - first[1]) / 1e6 / (last[0] - first[0])


def nearest(samples, t):
    return min(samples, key=lambda sample: abs(sample["t"] - t)) if samples else None


def summarize_run(workers, repetition, coordinator_rows, worker_rows, metric_samples, cpu_samples, fixture_stats,
                  budget, node_cpus):
    window = crawl_window(coordinator_rows, workers)
    start, end = window["window_start_unix"], window["end_unix"]
    latencies = [seconds * 1e9 for t, seconds in task_latencies(worker_rows) if start < t <= end]
    summary = {"workers": workers, "repetition": repetition, "page_budget": budget, **window,
               "task_latency": measurement.percentile(latencies),
               "failed_tasks": len(events_named(coordinator_rows, "fetch.failed")),
               "rejected_pages": len(events_named(coordinator_rows, "page.rejected")),
               "registered_workers": len({row["data"]["worker_id"] for row in events_named(coordinator_rows, "worker.registered")}),
               "fixture_requests": fixture_stats}
    scrape_before, scrape_after = nearest(metric_samples, start), nearest(metric_samples, end)
    if scrape_before and scrape_after and scrape_after["t"] > scrape_before["t"]:
        wait = histogram_delta(histogram(scrape_after["series"], "scout_db_pool_wait_seconds"),
                               histogram(scrape_before["series"], "scout_db_pool_wait_seconds"))
        in_use = [value for sample in metric_samples if start <= sample["t"] <= end
                  for _, value in sample["series"].get("scout_db_pool_connections_in_use", [])]
        pool = [value for _, value in scrape_after["series"].get("scout_db_pool_connections", [])]
        summary["db_pool"] = {
            "acquisitions": wait[0], "mean_wait_ms": wait[1] / wait[0] * 1e3 if wait[0] else None,
            "p95_wait_ms": (quantile(wait[2], .95) or 0) * 1e3 if wait[0] else None,
            "total_wait_seconds": wait[1], "connections": pool[0] if pool else None,
            "max_in_use_sampled": max(in_use) if in_use else None,
            "sample_gap_seconds": [round(scrape_before["t"] - start, 2), round(scrape_after["t"] - end, 2)]}
    cores = {role: cpu_cores(samples, start, end) for role, samples in cpu_samples.items()}
    worker_cores = [value for role, value in cores.items() if role.startswith("worker") and value is not None]
    summary["cpu_cores"] = {"coordinator": cores.get("coordinator"), "postgres": cores.get("postgres"),
                            "fixture": cores.get("fixture"), "workers_total": sum(worker_cores) if worker_cores else None}
    parts = [v for v in (summary["cpu_cores"]["coordinator"], summary["cpu_cores"]["postgres"],
                         summary["cpu_cores"]["fixture"], summary["cpu_cores"]["workers_total"]) if v is not None]
    summary["cpu_cores"]["all_roles"] = sum(parts) if parts else None
    summary["node_cpus"] = node_cpus
    warnings = []
    if window["pages_accepted"] < budget:
        warnings.append(f"only {window['pages_accepted']} pages accepted for a budget of {budget}")
    if window["workers_that_claimed"] < workers:
        warnings.append(f"{workers - window['workers_that_claimed']} worker(s) never claimed a task")
    if summary["registered_workers"] != workers:
        warnings.append(f"{summary['registered_workers']} workers registered, expected {workers}")
    if summary["failed_tasks"]:
        warnings.append(f"{summary['failed_tasks']} failed tasks")
    if window["window_seconds"] < 20:
        warnings.append(f"the measured window is only {window['window_seconds']:.1f}s; raise --pages")
    summary["warnings"] = warnings
    return summary


# ---- the cluster ----------------------------------------------------------------------------------
class Cluster:
    def __init__(self, args):
        self.context, self.namespace, self.release = args.context, args.namespace, args.release
        self.args = args

    def run(self, *command, check=True, timeout=300):
        result = subprocess.run(command, text=True, capture_output=True, timeout=timeout)
        if check and result.returncode:
            raise RuntimeError(f"{' '.join(command[:6])} failed ({result.returncode}): {result.stderr.strip()[-600:]}")
        return result.stdout

    def kubectl(self, *args, **kwargs):
        return self.run("kubectl", "--context", self.context, "-n", self.namespace, *args, **kwargs)

    def helm(self, *args, **kwargs):
        return self.run("helm", "--kube-context", self.context, "-n", self.namespace, *args, **kwargs)

    def pods(self, component):
        out = self.kubectl("get", "pods", "-l", f"app.kubernetes.io/component={component}",
                           "--field-selector=status.phase=Running", "-o", "jsonpath={.items[*].metadata.name}")
        return out.split()

    def proxy(self, pod, port, path):
        return self.kubectl("get", "--raw", f"/api/v1/namespaces/{self.namespace}/pods/{pod}:{port}/proxy/{path}")

    def cpu_usec(self, pod, container=None):
        """Cumulative CPU of one container's cgroup (cgroup v2, else v1), in microseconds."""
        command = ["exec", pod] + (["-c", container] if container else []) + [
            "--", "sh", "-c", "cat /sys/fs/cgroup/cpu.stat 2>/dev/null || cat /sys/fs/cgroup/cpuacct/cpuacct.usage"]
        out = self.kubectl(*command, check=False)
        for line in out.splitlines():
            if line.startswith("usage_usec"):
                return int(line.split()[1])
        return int(out.strip()) // 1000 if out.strip().isdigit() else None

    def values(self, workers, run_id, sources_path):
        a = self.args
        values = ["--set", f"image.repository={a.image_repo}", "--set", f"image.tag={a.image_tag}",
                  "--set", f"crawl.runId={run_id}", "--set-file", f"crawl.sources={sources_path}",
                  "--set", "index.enabled=false", "--set", "serve.replicas=0",
                  "--set", "fixture.mode=synthetic", "--set", f"fixture.synthetic.origins={a.origins}",
                  "--set", f"fixture.synthetic.pagesPerOrigin={a.pages_per_origin}",
                  "--set", f"fixture.synthetic.latencyMs={a.latency_ms}",
                  "--set", f"worker.replicas={workers}",
                  # The coordinator keeps serving /metrics this long after the crawl, for a last scrape.
                  "--set", "coordinator.metricsLingerSeconds=20", "--set", "coordinator.metricsSampleSeconds=1"]
        if a.db_lanes:
            values += ["--set", f"coordinator.dbLanes={a.db_lanes}"]
        if a.completion_lanes:
            values += ["--set", f"coordinator.completionLanes={a.completion_lanes}"]
        return values

    def deploy(self, workers, run_id, sources_path):
        self.helm("upgrade", "--install", self.release, str(CHART), "--create-namespace", *self.values(workers, run_id, sources_path),
                  timeout=300)

    def crawl_finished(self):
        """The coordinator stops itself once the crawl completes and leaves this marker."""
        return subprocess.run(["kubectl", "--context", self.context, "-n", self.namespace, "exec",
                               f"deploy/{self.release}-coordinator", "--", "test", "-e", "/tmp/crawl-finished"],
                              capture_output=True, timeout=30).returncode == 0

    def workers_ready(self, workers):
        status = json.loads(self.kubectl("get", f"deploy/{self.release}-worker", "-o", "json"))["status"]
        return all(status.get(key, 0) == workers for key in ("replicas", "updatedReplicas", "readyReplicas"))

    def wait(self, what, condition, timeout, interval=2):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                if condition():
                    return
            except (RuntimeError, subprocess.TimeoutExpired, ValueError):
                pass
            time.sleep(interval)
        raise TimeoutError(f"timed out after {timeout}s waiting for {what}")


def sources_toml(args, host):
    text = f'''schema_version = "scout.sources.v1"
contact_url = "https://github.com/lifrocszh/scout"
user_agent = "ScoutBenchmark/0.1 (+https://github.com/lifrocszh/scout)"
embedding_model = "BAAI/bge-small-en-v1.5"
page_target = {args.pages}
max_frontier = {args.origins * args.pages_per_origin}
max_total_attempts = {args.origins * args.pages_per_origin * 5}
max_duration_seconds = 3000
request_timeout_seconds = 10
# Politeness is deliberately out of the way: the origins are our own pods.
global_concurrency = 128
origin_concurrency = 4
min_start_spacing_ms = 0
'''
    for origin in range(args.origins):
        base = f"http://{host}:{FIRST_PORT + origin}"
        text += f'''
[[sources]]
source_id = "origin-{origin}"
seeds = ["{base}/p/0"]
allowed_origins = ["{base}"]
path_prefixes = ["/p/"]
'''
    return text


def run_once(cluster, args, workers, repetition, output):
    run_dir = output / f"w{workers}-r{repetition}"
    run_dir.mkdir(parents=True, exist_ok=True)
    run_id = "run-" + secrets.token_hex(32)
    sources = run_dir / "sources.toml"
    sources.write_text(sources_toml(args, f"{cluster.release}-fixture"))
    print(json.dumps({"event": "run.start", "workers": workers, "repetition": repetition, "run_id": run_id,
                      "unix": time.time()}), flush=True)
    cluster.deploy(workers, run_id, sources)
    release = cluster.release
    cluster.wait("the coordinator", lambda: json.loads(cluster.kubectl("get", f"deploy/{release}-coordinator", "-o", "json"))
                 ["status"].get("readyReplicas") == 1 and cluster.pods("coordinator"), 300)
    cluster.wait(f"{workers} ready workers", lambda: cluster.workers_ready(workers), 300)
    coordinator = cluster.pods("coordinator")[0]
    postgres = f"{release}-postgres-0"
    fixture = cluster.pods("fixture")[0]
    node_cpus = int(cluster.kubectl("get", "nodes", "-o", "jsonpath={.items[0].status.capacity.cpu}"))
    stats_before = json.loads(cluster.proxy(fixture, FIRST_PORT, "__stats"))

    metric_samples, cpu_samples = [], collections.defaultdict(list)
    roles = {"coordinator": coordinator, "postgres": postgres, "fixture": fixture}
    roles.update({f"worker-{pod}": pod for pod in cluster.pods("worker")})
    done = threading.Event()

    def sample_metrics():
        # Fast, so the pool-wait counters bracket the measured window tightly.
        while not done.is_set():
            try:
                text = cluster.proxy(coordinator, 9101, "metrics")
                metric_samples.append({"t": time.time(), "series": parse_metrics(text)})
            except (RuntimeError, subprocess.TimeoutExpired):
                pass
            done.wait(1)

    def sample_cpu():
        while not done.is_set():
            for role, pod in roles.items():
                try:
                    usec = cluster.cpu_usec(pod, "worker" if role.startswith("worker") else None)
                    if usec is not None:
                        cpu_samples[role].append((time.time(), usec))
                except (RuntimeError, subprocess.TimeoutExpired):
                    pass
            done.wait(1)

    threads = [threading.Thread(target=sample_metrics), threading.Thread(target=sample_cpu)]
    for thread in threads:
        thread.start()
    try:
        cluster.wait("the crawl to finish", cluster.crawl_finished, args.run_timeout, interval=3)
    finally:
        done.set()
        for thread in threads:
            thread.join()

    log = cluster.kubectl("exec", f"deploy/{release}-coordinator", "--", "cat", f"/data/logs/{run_id}.jsonl")
    (run_dir / "coordinator-events.jsonl").write_text(log)
    worker_log = cluster.kubectl("logs", "-l", "app.kubernetes.io/component=worker", "-c", "worker", "--prefix",
                                 "--tail=-1", "--ignore-errors", check=False)
    (run_dir / "worker-events.jsonl").write_text(worker_log)
    stats_after = json.loads(cluster.proxy(fixture, FIRST_PORT, "__stats"))
    fixture_stats = {key: stats_after[key] - stats_before[key] for key in stats_after}
    with (run_dir / "metrics-samples.jsonl").open("w") as handle:
        for sample in metric_samples:
            handle.write(json.dumps({"t": sample["t"], "series": {k: v for k, v in sample["series"].items()
                                                                   if k.startswith("scout_db_pool")}}) + "\n")
    measurement.save(run_dir / "cpu-samples.json", cpu_samples)

    coordinator_rows = measurement.json_lines(run_dir / "coordinator-events.jsonl")
    # `kubectl logs --prefix` puts "[pod/name/container] " before each JSON line.
    worker_rows = []
    for line in worker_log.splitlines():
        _, _, body = line.partition("] ")
        try:
            row = json.loads(body)
        except ValueError:
            continue
        if isinstance(row, dict) and str(row.get("event", "")).startswith("worker."):
            worker_rows.append(row)
    summary = summarize_run(workers, repetition, coordinator_rows, worker_rows, [
        {"t": s["t"], "series": s["series"]} for s in metric_samples], cpu_samples, fixture_stats, args.pages, node_cpus)
    summary["run_id"] = run_id
    measurement.save(run_dir / "summary.json", summary)
    print(json.dumps({"event": "run.done", "workers": workers, "repetition": repetition,
                      "pages_per_second": summary["pages_per_second"], "warnings": summary["warnings"]}), flush=True)
    return summary


def environment(cluster, args):
    versions = {}
    for name, command in {"kubectl": ["kubectl", "version", "--client", "-o", "json"], "helm": ["helm", "version", "--short"],
                          "kind": ["kind", "version"]}.items():
        try:
            versions[name] = cluster.run(*command).strip()[:300]
        except (RuntimeError, OSError):
            versions[name] = "unavailable"
    node = json.loads(cluster.kubectl("get", "nodes", "-o", "json"))["items"][0]["status"]
    return {"platform": platform.platform(), "python": sys.version.split()[0], "versions": versions,
            "node": {"cpus": node["capacity"]["cpu"], "memory": node["capacity"]["memory"],
                     "kubelet": node["nodeInfo"]["kubeletVersion"], "kernel": node["nodeInfo"]["kernelVersion"]},
            "parameters": {key: getattr(args, key) for key in
                           ("workers", "repetitions", "pages", "origins", "pages_per_origin", "latency_ms",
                            "db_lanes", "completion_lanes", "image_repo", "image_tag")}}


def self_test():
    metrics = parse_metrics('# HELP x y\nscout_db_pool_wait_seconds_bucket{le="0.001"} 8\n'
                            'scout_db_pool_wait_seconds_bucket{le="0.01"} 18\n'
                            'scout_db_pool_wait_seconds_bucket{le="+Inf"} 20\n'
                            'scout_db_pool_wait_seconds_sum 0.5\nscout_db_pool_wait_seconds_count 20\n')
    hist = histogram(metrics, "scout_db_pool_wait_seconds")
    assert hist[0] == 20 and hist[1] == 0.5, hist
    assert abs(quantile(hist[2], .5) - (0.001 + 0.009 * 0.2)) < 1e-9, quantile(hist[2], .5)
    assert quantile(hist[2], .95) == 0.01  # falls in the open bucket: the last finite bound
    earlier = (10, 0.2, [(0.001, 5), (0.01, 9), (float("inf"), 10)])
    assert histogram_delta(hist, earlier)[0] == 10
    assert cpu_cores([(0, 0), (10, 5_000_000), (20, 15_000_000)], 10, 20) == 1.0
    rows = []
    stamp = lambda s: datetime.datetime.fromtimestamp(1_000_000 + s, datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    for worker, at in (("a", 1), ("b", 3)):
        rows.append({"event": "task.claimed", "timestamp": stamp(at), "data": {"worker_id": worker}})
        rows.append({"event": "worker.registered", "timestamp": stamp(at - 1), "data": {"worker_id": worker}})
    rows += [{"event": "capture.accepted", "timestamp": stamp(t), "data": {}} for t in range(2, 12)]
    rows.append({"event": "crawl.completed", "timestamp": stamp(11), "data": {"stop_reason": "page_target"}})
    window = crawl_window(rows, 2)
    assert window["window_start_unix"] == 1_000_003 and window["pages_in_window"] == 8, window
    assert abs(window["pages_per_second"] - 1.0) < 1e-9, window
    workers = [{"event": "worker.task_started", "timestamp": stamp(5), "data": {"worker_id": "a", "task_id": 1}},
               {"event": "worker.task_completed", "timestamp": stamp(5.25), "data": {"worker_id": "a", "task_id": 1}}]
    assert abs(task_latencies(workers)[0][1] - 0.25) < 1e-9
    print("self-test ok")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=pathlib.Path)
    parser.add_argument("--workers", nargs="+", type=int, default=[1, 2, 4, 8])
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--pages", type=int, default=2000, help="page budget of every run")
    parser.add_argument("--origins", type=int, default=32)
    parser.add_argument("--pages-per-origin", type=int, default=125)
    parser.add_argument("--latency-ms", type=int, default=100, help="how long each origin takes to answer a page")
    parser.add_argument("--db-lanes", default="", help="SCOUT_COORDINATOR_DB_LANES; empty keeps the default")
    parser.add_argument("--completion-lanes", default="", help="SCOUT_COMPLETION_LANES; empty keeps the default")
    parser.add_argument("--context", default="kind-scout")
    parser.add_argument("--namespace", default="scout")
    parser.add_argument("--release", default="scout")
    parser.add_argument("--image-repo", default="scout")
    parser.add_argument("--image-tag", default="kind")
    parser.add_argument("--run-timeout", type=int, default=1500, help="seconds one crawl may take")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    if not args.output:
        parser.error("--output is required")
    if args.pages > args.origins * args.pages_per_origin:
        parser.error("the budget exceeds the origin set: raise --origins or --pages-per-origin")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    cluster = Cluster(args)
    summaries = []
    results = {"environment": None, "runs": summaries}
    for repetition in range(1, args.repetitions + 1):
        # Alternate the order so drift (a warming node, a growing database) does not favour one count.
        for workers in args.workers if repetition % 2 else reversed(args.workers):
            summaries.append(run_once(cluster, args, workers, repetition, output))
            if results["environment"] is None:
                results["environment"] = environment(cluster, args)
            measurement.save(output / "results.json", results)
    print("RESULTS-JSON-BEGIN")
    print(json.dumps(results, sort_keys=True))
    print("RESULTS-JSON-END")


if __name__ == "__main__":
    main()
