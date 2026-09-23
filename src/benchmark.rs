use super::app::{report_error, run};
use super::evaluation::{
    load_evaluation_metadata, metadata_string, ranked_page_ids, validate_benchmark_queries,
};
use super::generation::{
    build_generation, collect_files, directory_bytes, directory_digest, validate_generation,
};
use super::http::read_http_request;
use super::observability::Events;
use super::search::load_active_generation;
use super::storage::{jsonl, new_run_id, parse_jsonl, sha256_hex, write_if_absent, write_json};
use super::{
    Arc, AtomicBool, AtomicOrdering, BENCHMARK_DEFAULT_COLD_OPEN_RUNS,
    BENCHMARK_DEFAULT_CONCURRENCY, BENCHMARK_DEFAULT_INDEX_WORKERS,
    BENCHMARK_DEFAULT_MEASURED_PASSES, BENCHMARK_DEFAULT_REPETITIONS,
    BENCHMARK_DEFAULT_WARMUP_PASSES, BENCHMARK_INSTABILITY_LIMIT, BENCHMARK_SCHEMA, BTreeMap,
    BTreeSet, BenchmarkConfig, BenchmarkDistribution, BenchmarkPlan, BenchmarkSample,
    BenchmarkSystemEvidence, Cli, Command, Duration, Error, EvaluationQuery, GenerationManifest,
    HashMap, Instant, LoadedGeneration, Mutex, Operation, Ordering, OsString, Path, PathBuf,
    RawBenchmarkConfig, SNAPSHOT_SCHEMA, StoredSnapshotManifest, TcpListener, TcpStream, Value,
    Write, fs, io, json, thread,
};

pub(crate) fn run_benchmark(cli: &Cli) -> i32 {
    let Operation::Benchmark { config } = &cli.operation else {
        unreachable!("benchmark operation required")
    };
    let run_id = new_run_id();
    let mut events = match Events::open(&cli.data_dir, &run_id, "benchmark") {
        Ok(events) => events,
        Err(error) => {
            report_error(cli, &error);
            return error.exit_code;
        }
    };
    match execute_benchmark(config, &cli.data_dir, &run_id) {
        Ok(report) => {
            events.emit("benchmark.completed", "success", report.clone());
            println!(
                "benchmark complete: run_id={run_id} report={}",
                report["report_path"].as_str().unwrap_or_default()
            );
            0
        }
        Err(error) => {
            events.emit(
                "benchmark.failed",
                "failure",
                json!({"code": error.code, "message": error.message}),
            );
            report_error(cli, &error);
            error.exit_code
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
struct ResourceSample {
    schema_version: &'static str,
    timestamp_utc: String,
    elapsed_ms: u64,
    benchmark_run_id: String,
    phase: String,
    root_pid: u32,
    sampled_pids: Vec<u32>,
    missing_process_ids: Vec<u32>,
    configured_interval_ms: u64,
    actual_interval_ms: Option<f64>,
    process_cpu_time_ms: Option<f64>,
    process_cpu_time_scope: &'static str,
    cpu_utilization_percent: Option<f64>,
    cpu_utilization_scope: &'static str,
    rss_bytes: Option<u64>,
    rss_scope: &'static str,
    os_high_water_rss_bytes: Option<u64>,
    os_high_water_scope: &'static str,
    host_swap_in_counter: Option<u64>,
    host_swap_out_counter: Option<u64>,
    host_swap_counter_unit: &'static str,
    host_swap_in_delta: Option<u64>,
    host_swap_out_delta: Option<u64>,
    host_swap_total_bytes: Option<u64>,
    host_swap_used_bytes: Option<u64>,
    host_observation_scope: &'static str,
    filesystem_used_bytes: Option<u64>,
    data_dir_bytes: Option<u64>,
    data_dir_growth_bytes: Option<i64>,
    missing_fields: Vec<String>,
    status: &'static str,
}

#[derive(Debug, Clone, serde::Serialize)]
struct ResourceBoundary {
    schema_version: &'static str,
    timestamp_utc: String,
    elapsed_ms: u64,
    benchmark_run_id: String,
    kind: &'static str,
    phase: String,
    sequence: usize,
    status: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct ResourcePhaseSummary {
    sample_count: usize,
    measured_cpu_samples: usize,
    mean_cpu_utilization_percent: Option<f64>,
    cumulative_cpu_time_ms: Option<f64>,
    peak_rss_bytes_sampled: Option<u64>,
    peak_rss_bytes_os_high_water: Option<u64>,
    missing_sample_count: usize,
    disk_growth_bytes: Option<i64>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct ResourceSummary {
    schema_version: &'static str,
    status: String,
    benchmark_run_id: String,
    root_pid: u32,
    sampling_interval_ms: u64,
    configuration_path: String,
    samples_path: String,
    boundaries_path: String,
    sample_count: usize,
    measured_cpu_sample_count: usize,
    timestamp_start_utc: Option<String>,
    timestamp_end_utc: Option<String>,
    process_tree_coverage: String,
    peak_rss_bytes_sampled: Option<u64>,
    peak_rss_scope: &'static str,
    peak_rss_bytes_os_high_water: Option<u64>,
    os_high_water_rss_scope: &'static str,
    mean_cpu_utilization_percent: Option<f64>,
    cumulative_cpu_time_ms: Option<f64>,
    cumulative_cpu_time_scope: &'static str,
    host_swap_in_delta: Option<u64>,
    host_swap_out_delta: Option<u64>,
    host_swap_in_scope: &'static str,
    host_swap_out_scope: &'static str,
    host_swap_counter_unit: &'static str,
    host_swap_usage_start_bytes: Option<u64>,
    host_swap_usage_end_bytes: Option<u64>,
    disk_growth_bytes: Option<i64>,
    disk_growth_scope: &'static str,
    missing_sample_count: usize,
    missing_fields: Vec<String>,
    by_phase: BTreeMap<String, ResourcePhaseSummary>,
    phase_boundaries: Vec<ResourceBoundary>,
    runlens_metadata_path: Option<String>,
    runlens_metadata_sha256: Option<String>,
    runlens_trace_id: Option<String>,
}

#[derive(Debug, Clone)]
struct ProcessSnapshot {
    cpu_ticks_by_pid: BTreeMap<u32, u64>,
    rss_bytes: u64,
    hwm_bytes: u64,
    sampled_pids: Vec<u32>,
    missing_pids: Vec<u32>,
}

struct ResourceSampler {
    stop: Arc<AtomicBool>,
    phase: Arc<Mutex<Option<(String, usize)>>>,
    boundaries: Arc<Mutex<Vec<ResourceBoundary>>>,
    samples: Arc<Mutex<Vec<ResourceSample>>>,
    join: Option<thread::JoinHandle<()>>,
    output_dir: PathBuf,
    data_dir: PathBuf,
    run_id: String,
    root_pid: u32,
    interval_ms: u64,
    logical_cpus: usize,
    ticks_per_second: u64,
    runlens_metadata: Option<PathBuf>,
    started: Instant,
}

impl ResourceSampler {
    fn start(
        output_dir: &Path,
        data_dir: &Path,
        run_id: &str,
        interval_ms: u64,
        runlens_metadata: Option<&Path>,
    ) -> Result<Self, Error> {
        if interval_ms == 0 {
            return Err(Error::invalid(
                "resource_sampling_interval_ms must be positive",
            ));
        }
        let root_pid = std::process::id();
        let logical_cpus = std::thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or(1);
        let ticks_per_second = proc_ticks_per_second();
        let stop = Arc::new(AtomicBool::new(false));
        let phase = Arc::new(Mutex::new(Some(("setup".into(), 0))));
        let boundaries = Arc::new(Mutex::new(vec![ResourceBoundary {
            schema_version: "scout.resource-boundary.v1",
            timestamp_utc: resource_timestamp(),
            elapsed_ms: 0,
            benchmark_run_id: run_id.into(),
            kind: "start",
            phase: "setup".into(),
            sequence: 0,
            status: None,
        }]));
        let samples = Arc::new(Mutex::new(Vec::<ResourceSample>::new()));
        let thread_stop = stop.clone();
        let thread_phase = phase.clone();
        let thread_samples = samples.clone();
        let thread_data_dir = data_dir.to_owned();
        let thread_run_id = run_id.to_owned();
        let thread_started = Instant::now();
        let join = thread::spawn(move || {
            let mut previous: Option<(Instant, BTreeMap<u32, u64>)> = None;
            let mut observed_cpu_ticks = BTreeMap::<u32, u64>::new();
            while !thread_stop.load(AtomicOrdering::Relaxed) {
                let sampled_at = Instant::now();
                let process = read_process_snapshot(root_pid);
                let swap = read_host_swap();
                let filesystem_used_bytes = read_filesystem_used_bytes(&thread_data_dir);
                let data_dir_bytes = directory_bytes(&thread_data_dir).ok();
                let (phase_name, _) = thread_phase
                    .lock()
                    .ok()
                    .and_then(|phase| phase.clone())
                    .unwrap_or_else(|| ("unassigned".into(), 0));
                let (actual_interval_ms, cpu_utilization_percent) = process
                    .as_ref()
                    .zip(previous.as_ref())
                    .map(|(current, (last_at, last_ticks))| {
                        let elapsed = sampled_at.duration_since(*last_at).as_secs_f64();
                        let delta = current
                            .cpu_ticks_by_pid
                            .iter()
                            .map(|(pid, ticks)| {
                                ticks.saturating_sub(last_ticks.get(pid).copied().unwrap_or(*ticks))
                            })
                            .sum::<u64>();
                        let utilization = (delta as f64
                            / ticks_per_second as f64
                            / elapsed.max(f64::EPSILON)
                            / logical_cpus as f64)
                            * 100.0;
                        (Some(elapsed * 1_000.0), Some(utilization))
                    })
                    .unwrap_or((None, None));
                if let Some(process) = process.as_ref() {
                    for (pid, ticks) in &process.cpu_ticks_by_pid {
                        observed_cpu_ticks
                            .entry(*pid)
                            .and_modify(|observed| *observed = (*observed).max(*ticks))
                            .or_insert(*ticks);
                    }
                }
                let process_cpu_time_ms = (!observed_cpu_ticks.is_empty()).then(|| {
                    observed_cpu_ticks.values().sum::<u64>() as f64 / ticks_per_second as f64
                        * 1_000.0
                });
                let previous_disk = thread_samples
                    .lock()
                    .ok()
                    .and_then(|samples| samples.last().and_then(|sample| sample.data_dir_bytes));
                let data_dir_growth_bytes = data_dir_bytes
                    .zip(previous_disk)
                    .map(|(current, previous)| current as i64 - previous as i64);
                let mut missing_fields = Vec::new();
                if process.is_none() {
                    missing_fields.push("process_metrics".into());
                } else if process
                    .as_ref()
                    .is_some_and(|value| !value.missing_pids.is_empty())
                {
                    missing_fields.push("process_tree_child_metrics".into());
                }
                if actual_interval_ms.is_none() {
                    missing_fields.push("cpu_interval_baseline".into());
                }
                if swap.0.is_none() {
                    missing_fields.push("host_swap_in_counter".into());
                }
                if swap.1.is_none() {
                    missing_fields.push("host_swap_out_counter".into());
                }
                if swap.2.is_none() {
                    missing_fields.push("host_swap_total_bytes".into());
                }
                if filesystem_used_bytes.is_none() {
                    missing_fields.push("filesystem_used_bytes".into());
                }
                if data_dir_bytes.is_none() {
                    missing_fields.push("data_dir_bytes".into());
                }
                let (host_swap_in_delta, host_swap_out_delta) = thread_samples
                    .lock()
                    .ok()
                    .and_then(|samples| {
                        samples.last().map(|sample| {
                            (
                                swap.0
                                    .zip(sample.host_swap_in_counter)
                                    .map(|(current, previous)| current.saturating_sub(previous)),
                                swap.1
                                    .zip(sample.host_swap_out_counter)
                                    .map(|(current, previous)| current.saturating_sub(previous)),
                            )
                        })
                    })
                    .unwrap_or((None, None));
                let sample_status = if missing_fields.is_empty() {
                    "complete"
                } else {
                    "partial"
                };
                let sample = ResourceSample {
                    schema_version: "scout.resource-sample.v1",
                    timestamp_utc: resource_timestamp(),
                    elapsed_ms: thread_started.elapsed().as_millis() as u64,
                    benchmark_run_id: thread_run_id.clone(),
                    phase: phase_name,
                    root_pid,
                    sampled_pids: process
                        .as_ref()
                        .map(|value| value.sampled_pids.clone())
                        .unwrap_or_default(),
                    missing_process_ids: process
                        .as_ref()
                        .map(|value| value.missing_pids.clone())
                        .unwrap_or_else(|| vec![root_pid]),
                    configured_interval_ms: interval_ms,
                    actual_interval_ms,
                    process_cpu_time_ms,
                    process_cpu_time_scope: "root-process-tree",
                    cpu_utilization_percent,
                    cpu_utilization_scope: "root-process-tree-over-host-logical-cpus",
                    rss_bytes: process.as_ref().map(|value| value.rss_bytes),
                    rss_scope: "root-process-tree-sampled-current-rss",
                    os_high_water_rss_bytes: process.as_ref().map(|value| value.hwm_bytes),
                    os_high_water_scope: "root-process-tree-summed-os-vmhwm",
                    host_swap_in_counter: swap.0,
                    host_swap_out_counter: swap.1,
                    host_swap_counter_unit: "pages",
                    host_swap_in_delta,
                    host_swap_out_delta,
                    host_swap_total_bytes: swap.2,
                    host_swap_used_bytes: swap
                        .2
                        .zip(swap.3)
                        .map(|(total, free)| total.saturating_sub(free)),
                    host_observation_scope: "host-wide-proc-vmstat-and-meminfo",
                    filesystem_used_bytes,
                    data_dir_bytes,
                    data_dir_growth_bytes,
                    missing_fields,
                    status: sample_status,
                };
                if let Ok(mut values) = thread_samples.lock() {
                    if let Some(process) = process.as_ref() {
                        previous = Some((sampled_at, process.cpu_ticks_by_pid.clone()));
                    }
                    values.push(sample);
                }
                thread::sleep(Duration::from_millis(interval_ms));
            }
        });
        let sampler = Self {
            stop,
            phase,
            boundaries,
            samples,
            join: Some(join),
            output_dir: output_dir.to_owned(),
            data_dir: data_dir.to_owned(),
            run_id: run_id.into(),
            root_pid,
            interval_ms,
            logical_cpus,
            ticks_per_second,
            runlens_metadata: runlens_metadata.map(Path::to_owned),
            started: Instant::now(),
        };
        sampler.write_config()?;
        Ok(sampler)
    }

    fn set_phase(&self, name: &str) {
        let mut phase = self.phase.lock().expect("resource sampler phase lock");
        if phase.as_ref().is_some_and(|(current, _)| current == name) {
            return;
        }
        let sequence = phase.as_ref().map_or(0, |(_, sequence)| sequence + 1);
        if let Some((current, current_sequence)) = phase.take() {
            self.boundaries
                .lock()
                .expect("resource sampler boundary lock")
                .push(ResourceBoundary {
                    schema_version: "scout.resource-boundary.v1",
                    timestamp_utc: resource_timestamp(),
                    elapsed_ms: self.started.elapsed().as_millis() as u64,
                    benchmark_run_id: self.run_id.clone(),
                    kind: "end",
                    phase: current,
                    sequence: current_sequence,
                    status: None,
                });
        }
        self.boundaries
            .lock()
            .expect("resource sampler boundary lock")
            .push(ResourceBoundary {
                schema_version: "scout.resource-boundary.v1",
                timestamp_utc: resource_timestamp(),
                elapsed_ms: self.started.elapsed().as_millis() as u64,
                benchmark_run_id: self.run_id.clone(),
                kind: "start",
                phase: name.into(),
                sequence,
                status: None,
            });
        *phase = Some((name.into(), sequence));
    }

    fn end_phase(&self, status: &str) {
        let mut phase = self.phase.lock().expect("resource sampler phase lock");
        if let Some((current, sequence)) = phase.take() {
            self.boundaries
                .lock()
                .expect("resource sampler boundary lock")
                .push(ResourceBoundary {
                    schema_version: "scout.resource-boundary.v1",
                    timestamp_utc: resource_timestamp(),
                    elapsed_ms: self.started.elapsed().as_millis() as u64,
                    benchmark_run_id: self.run_id.clone(),
                    kind: "end",
                    phase: current,
                    sequence,
                    status: Some(status.into()),
                });
        }
    }

    fn write_config(&self) -> Result<(), Error> {
        write_json(
            &self.output_dir.join("resource-config.json"),
            &json!({
                "schema_version": "scout.resource-config.v1",
                "benchmark_run_id": self.run_id,
                "root_pid": self.root_pid,
                "sampling_interval_ms": self.interval_ms,
                "logical_cpus": self.logical_cpus,
                "cpu_ticks_per_second": self.ticks_per_second,
                "data_dir": self.data_dir,
                "monitoring_commands": [
                    "/proc/<pid>/stat (utime/stime)",
                    "/proc/<pid>/status (VmRSS/VmHWM)",
                    "/proc/<pid>/task/<pid>/children",
                    "/proc/vmstat (pswpin/pswpout)",
                    "/proc/meminfo (SwapTotal/SwapFree)",
                    "df -Pk <data_dir>",
                    "directory byte walk <data_dir>",
                    "getconf CLK_TCK",
                ],
                "units": {
                    "interval": "milliseconds",
                    "cpu_time": "milliseconds",
                    "cpu_utilization": "percent of host logical CPU capacity",
                    "rss": "bytes",
                    "swap_counters": "vmstat pages",
                    "swap_usage": "bytes",
                    "disk": "bytes",
                },
                "scope": {
                    "process": "root process and descendants observed at each sample",
                    "rss": "sampled current RSS plus operating-system VmHWM",
                    "swap": "host-wide counters and usage",
                    "disk": "host filesystem used bytes and data-directory bytes",
                },
                "runlens_metadata_path": self.runlens_metadata,
            }),
        )?;
        Ok(())
    }

    fn finish(mut self, status: &str) -> Result<ResourceSummary, Error> {
        self.stop.store(true, AtomicOrdering::Relaxed);
        if let Some(join) = self.join.take() {
            join.join()
                .map_err(|_| Error::storage("resource sampler panicked"))?;
        }
        self.end_phase(status);
        let samples = self
            .samples
            .lock()
            .expect("resource sampler sample lock")
            .clone();
        let boundaries = self
            .boundaries
            .lock()
            .expect("resource sampler boundary lock")
            .clone();
        self.write_artifacts(&samples, &boundaries, status)?;
        Ok(resource_summary(
            &self.run_id,
            self.root_pid,
            self.interval_ms,
            samples,
            boundaries,
            status,
            self.runlens_metadata.as_deref(),
        ))
    }

    fn write_artifacts(
        &self,
        samples: &[ResourceSample],
        boundaries: &[ResourceBoundary],
        status: &str,
    ) -> Result<(), Error> {
        let samples_jsonl = samples
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| Error::storage(format!("serialize resource samples: {error}")))?
            .join("\n");
        let samples_jsonl = if samples_jsonl.is_empty() {
            Vec::new()
        } else {
            format!("{samples_jsonl}\n").into_bytes()
        };
        let boundaries_jsonl = boundaries
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| Error::storage(format!("serialize resource boundaries: {error}")))?
            .join("\n");
        let boundaries_jsonl = if boundaries_jsonl.is_empty() {
            Vec::new()
        } else {
            format!("{boundaries_jsonl}\n").into_bytes()
        };
        write_if_absent(
            &self.output_dir.join("resource-samples.jsonl"),
            &samples_jsonl,
        )?;
        write_if_absent(
            &self.output_dir.join("resource-boundaries.jsonl"),
            &boundaries_jsonl,
        )?;
        let summary = resource_summary(
            &self.run_id,
            self.root_pid,
            self.interval_ms,
            samples.to_vec(),
            boundaries.to_vec(),
            status,
            self.runlens_metadata.as_deref(),
        );
        write_json(&self.output_dir.join("resource-summary.json"), &summary)?;
        Ok(())
    }
}

impl Drop for ResourceSampler {
    fn drop(&mut self) {
        if self.join.is_none() {
            return;
        }
        self.stop.store(true, AtomicOrdering::Relaxed);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
        self.end_phase("aborted");
        let samples = self
            .samples
            .lock()
            .map(|values| values.clone())
            .unwrap_or_default();
        let boundaries = self
            .boundaries
            .lock()
            .map(|values| values.clone())
            .unwrap_or_default();
        let _ = self.write_artifacts(&samples, &boundaries, "aborted");
    }
}

fn resource_timestamp() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn proc_ticks_per_second() -> u64 {
    std::process::Command::new("getconf")
        .arg("CLK_TCK")
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .and_then(|value| value.trim().parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(100)
}

fn process_tree(root_pid: u32) -> Vec<u32> {
    let mut pending = vec![root_pid];
    let mut seen = BTreeSet::new();
    while let Some(pid) = pending.pop() {
        if !seen.insert(pid) {
            continue;
        }
        let path = format!("/proc/{pid}/task/{pid}/children");
        if let Ok(children) = fs::read_to_string(path) {
            pending.extend(
                children
                    .split_whitespace()
                    .filter_map(|value| value.parse::<u32>().ok()),
            );
        }
    }
    seen.into_iter().collect()
}

fn read_process_metrics(pid: u32) -> Option<(u64, u64, u64)> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let fields = stat
        .rsplit_once(") ")?
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    let ticks = fields
        .get(11)
        .and_then(|value| value.parse::<u64>().ok())?
        .saturating_add(fields.get(12)?.parse::<u64>().ok()?);
    let status = fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let mut rss = None;
    let mut hwm = None;
    for line in status.lines() {
        if let Some(value) = line.strip_prefix("VmRSS:") {
            rss = value
                .split_whitespace()
                .next()
                .and_then(|v| v.parse::<u64>().ok());
        }
        if let Some(value) = line.strip_prefix("VmHWM:") {
            hwm = value
                .split_whitespace()
                .next()
                .and_then(|v| v.parse::<u64>().ok());
        }
    }
    Some((ticks, rss?.saturating_mul(1024), hwm?.saturating_mul(1024)))
}

fn read_process_snapshot(root_pid: u32) -> Option<ProcessSnapshot> {
    let pids = process_tree(root_pid);
    let mut cpu_ticks_by_pid = BTreeMap::new();
    let mut rss_bytes: u64 = 0;
    let mut hwm_bytes: u64 = 0;
    let mut sampled_pids = Vec::new();
    let mut missing_pids = Vec::new();
    for pid in pids {
        if let Some((process_ticks, process_rss, process_hwm)) = read_process_metrics(pid) {
            cpu_ticks_by_pid.insert(pid, process_ticks);
            rss_bytes = rss_bytes.saturating_add(process_rss);
            hwm_bytes = hwm_bytes.saturating_add(process_hwm);
            sampled_pids.push(pid);
        } else {
            missing_pids.push(pid);
        }
    }
    (!sampled_pids.is_empty()).then_some(ProcessSnapshot {
        cpu_ticks_by_pid,
        rss_bytes,
        hwm_bytes,
        sampled_pids,
        missing_pids,
    })
}

fn read_host_swap() -> (Option<u64>, Option<u64>, Option<u64>, Option<u64>) {
    let vmstat = fs::read_to_string("/proc/vmstat").ok();
    let counter = |name: &str| {
        vmstat.as_deref()?.lines().find_map(|line| {
            let mut fields = line.split_whitespace();
            (fields.next() == Some(name)).then(|| fields.next()?.parse().ok())?
        })
    };
    let meminfo = fs::read_to_string("/proc/meminfo").ok();
    let memory = |name: &str| {
        meminfo.as_deref()?.lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            (key == name).then(|| value.split_whitespace().next()?.parse::<u64>().ok())?
        })
    };
    (
        counter("pswpin"),
        counter("pswpout"),
        memory("SwapTotal").map(|value| value.saturating_mul(1024)),
        memory("SwapFree").map(|value| value.saturating_mul(1024)),
    )
}

fn read_filesystem_used_bytes(path: &Path) -> Option<u64> {
    let output = std::process::Command::new("df")
        .args(["-Pk", &path.display().to_string()])
        .output()
        .ok()?;
    let output_text = String::from_utf8_lossy(&output.stdout);
    let line = output_text.lines().rfind(|line| !line.trim().is_empty())?;
    line.split_whitespace()
        .nth(2)
        .and_then(|value| value.parse::<u64>().ok())
        .map(|value| value.saturating_mul(1024))
}

fn resource_summary(
    run_id: &str,
    root_pid: u32,
    interval_ms: u64,
    samples: Vec<ResourceSample>,
    boundaries: Vec<ResourceBoundary>,
    status: &str,
    runlens_metadata: Option<&Path>,
) -> ResourceSummary {
    let measured = samples
        .iter()
        .filter_map(|sample| sample.cpu_utilization_percent)
        .collect::<Vec<_>>();
    let peak_rss = samples.iter().filter_map(|sample| sample.rss_bytes).max();
    let peak_hwm = samples
        .iter()
        .filter_map(|sample| sample.os_high_water_rss_bytes)
        .max();
    let first = samples.first();
    let last = samples.last();
    let disk_growth_bytes = first
        .and_then(|sample| sample.data_dir_bytes)
        .zip(last.and_then(|sample| sample.data_dir_bytes))
        .map(|(first, last)| last as i64 - first as i64);
    let missing_fields = samples
        .iter()
        .flat_map(|sample| sample.missing_fields.iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut by_phase = BTreeMap::new();
    for phase in samples
        .iter()
        .map(|sample| sample.phase.clone())
        .collect::<BTreeSet<_>>()
    {
        let phase_samples = samples
            .iter()
            .filter(|sample| sample.phase == phase)
            .collect::<Vec<_>>();
        let cpu = phase_samples
            .iter()
            .filter_map(|sample| sample.cpu_utilization_percent)
            .collect::<Vec<_>>();
        by_phase.insert(
            phase,
            ResourcePhaseSummary {
                sample_count: phase_samples.len(),
                measured_cpu_samples: cpu.len(),
                mean_cpu_utilization_percent: (!cpu.is_empty())
                    .then(|| cpu.iter().sum::<f64>() / cpu.len() as f64),
                cumulative_cpu_time_ms: phase_samples
                    .iter()
                    .rev()
                    .find_map(|sample| sample.process_cpu_time_ms),
                peak_rss_bytes_sampled: phase_samples
                    .iter()
                    .filter_map(|sample| sample.rss_bytes)
                    .max(),
                peak_rss_bytes_os_high_water: phase_samples
                    .iter()
                    .filter_map(|sample| sample.os_high_water_rss_bytes)
                    .max(),
                missing_sample_count: phase_samples
                    .iter()
                    .filter(|sample| sample.status != "complete")
                    .count(),
                disk_growth_bytes: phase_samples
                    .first()
                    .and_then(|sample| sample.data_dir_bytes)
                    .zip(
                        phase_samples
                            .last()
                            .and_then(|sample| sample.data_dir_bytes),
                    )
                    .map(|(first, last)| last as i64 - first as i64),
            },
        );
    }
    let (runlens_metadata_sha256, runlens_trace_id) = runlens_metadata
        .and_then(|path| fs::read(path).ok())
        .map(|bytes| {
            let trace = serde_json::from_slice::<Value>(&bytes)
                .ok()
                .and_then(|value| {
                    value
                        .get("trace_id")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                });
            (Some(sha256_hex(&bytes)), trace)
        })
        .unwrap_or((None, None));
    ResourceSummary {
        schema_version: "scout.resource-summary.v1",
        status: status.into(),
        benchmark_run_id: run_id.into(),
        root_pid,
        sampling_interval_ms: interval_ms,
        configuration_path: "resource-config.json".into(),
        samples_path: "resource-samples.jsonl".into(),
        boundaries_path: "resource-boundaries.jsonl".into(),
        sample_count: samples.len(),
        measured_cpu_sample_count: measured.len(),
        timestamp_start_utc: first.map(|sample| sample.timestamp_utc.clone()),
        timestamp_end_utc: last.map(|sample| sample.timestamp_utc.clone()),
        process_tree_coverage: "root process plus descendants visible at each /proc sample; short-lived children may be absent".into(),
        peak_rss_bytes_sampled: peak_rss,
        peak_rss_scope: "sampled-process-tree-current-rss",
        peak_rss_bytes_os_high_water: peak_hwm,
        os_high_water_rss_scope: "summed-per-process-/proc-status-VmHWM",
        mean_cpu_utilization_percent: (!measured.is_empty())
            .then(|| measured.iter().sum::<f64>() / measured.len() as f64),
        cumulative_cpu_time_ms: last.and_then(|sample| sample.process_cpu_time_ms),
        cumulative_cpu_time_scope: "root-process-tree-summed-utime-stime",
        host_swap_in_delta: first
            .and_then(|sample| sample.host_swap_in_counter)
            .zip(last.and_then(|sample| sample.host_swap_in_counter))
            .map(|(first, last)| last.saturating_sub(first)),
        host_swap_out_delta: first
            .and_then(|sample| sample.host_swap_out_counter)
            .zip(last.and_then(|sample| sample.host_swap_out_counter))
            .map(|(first, last)| last.saturating_sub(first)),
        host_swap_in_scope: "host-wide-/proc-vmstat-pswpin-counter-delta",
        host_swap_out_scope: "host-wide-/proc-vmstat-pswpout-counter-delta",
        host_swap_counter_unit: "pages",
        host_swap_usage_start_bytes: first.and_then(|sample| sample.host_swap_used_bytes),
        host_swap_usage_end_bytes: last.and_then(|sample| sample.host_swap_used_bytes),
        disk_growth_bytes,
        disk_growth_scope: "data-directory-byte-walk-delta; filesystem-used-bytes are raw samples",
        missing_sample_count: samples
            .iter()
            .filter(|sample| sample.status != "complete")
            .count(),
        missing_fields,
        by_phase,
        phase_boundaries: boundaries,
        runlens_metadata_path: runlens_metadata.map(|path| path.display().to_string()),
        runlens_metadata_sha256,
        runlens_trace_id,
    }
}

fn validate_benchmark_snapshot(
    data_dir: &Path,
    snapshot_id: &str,
) -> Result<(PathBuf, StoredSnapshotManifest), Error> {
    let snapshot_dir = data_dir.join("corpora").join(snapshot_id);
    let manifest_path = snapshot_dir.join("snapshot-manifest.json");
    let manifest_bytes = fs::read(&manifest_path)
        .map_err(|e| Error::invalid(format!("read benchmark Corpus snapshot: {e}")))?;
    let manifest: StoredSnapshotManifest = serde_json::from_slice(&manifest_bytes)
        .map_err(|e| Error::invalid(format!("parse benchmark Corpus snapshot: {e}")))?;
    if manifest.schema_version != SNAPSHOT_SCHEMA
        || manifest.status != "complete"
        || manifest.snapshot_id != snapshot_id
    {
        return Err(Error::invalid("benchmark Corpus snapshot is not complete"));
    }
    let files = [
        (
            "pages.jsonl",
            manifest.pages_jsonl_sha256.as_str(),
            manifest.page_count,
        ),
        (
            "aliases.jsonl",
            manifest.aliases_jsonl_sha256.as_str(),
            manifest.alias_count,
        ),
        (
            "passages.jsonl",
            manifest.passages_jsonl_sha256.as_str(),
            manifest.passage_count,
        ),
    ];
    for (name, expected_digest, expected_count) in files {
        let bytes = fs::read(snapshot_dir.join(name))
            .map_err(|e| Error::invalid(format!("read benchmark {name}: {e}")))?;
        if sha256_hex(&bytes) != expected_digest {
            return Err(Error::invalid(format!(
                "benchmark snapshot checksum mismatch: {name}"
            )));
        }
        let count = bytes.iter().filter(|byte| **byte == b'\n').count();
        if count != expected_count {
            return Err(Error::invalid(format!(
                "benchmark snapshot count mismatch: {name}"
            )));
        }
    }
    Ok((snapshot_dir, manifest))
}

fn copy_directory(source: &Path, destination: &Path) -> Result<(), Error> {
    if !source.is_dir() {
        return Err(Error::invalid(format!(
            "benchmark staging source is not a directory: {}",
            source.display()
        )));
    }
    let mut files = Vec::new();
    collect_files(source, source, &mut files)?;
    for (relative, bytes) in files {
        write_if_absent(&destination.join(relative), &bytes)?;
    }
    Ok(())
}

fn safe_replay_path(value: &str) -> Option<&Path> {
    let path = Path::new(value);
    (path.is_relative()
        && !path
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir)))
    .then_some(path)
}

fn inspect_replay_crawl(replay: &Path, expected_snapshot_id: &str) -> Value {
    let mut evidence = json!({
        "configured": true,
        "path": replay,
        "present": replay.is_dir(),
        "valid": false,
        "snapshot_id": Value::Null,
        "page_count": 0,
        "body_count": 0,
    });
    let Ok(manifest_bytes) = fs::read(replay.join("crawl-manifest.json")) else {
        return evidence;
    };
    let Ok(manifest) = serde_json::from_slice::<Value>(&manifest_bytes) else {
        return evidence;
    };
    let snapshot_id = manifest
        .get("snapshot_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let pages = manifest
        .get("pages")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut bodies_valid = true;
    let mut body_count = 0_usize;
    for page in &pages {
        let Some(body_file) = page.get("body_file").and_then(Value::as_str) else {
            bodies_valid = false;
            continue;
        };
        let Some(relative) = safe_replay_path(body_file) else {
            bodies_valid = false;
            continue;
        };
        let Ok(body) = fs::read(replay.join(relative)) else {
            bodies_valid = false;
            continue;
        };
        let expected = page
            .get("body_sha256")
            .and_then(Value::as_str)
            .unwrap_or_default();
        bodies_valid &= !expected.is_empty() && sha256_hex(&body) == expected;
        body_count += 1;
    }
    let digest = directory_digest(replay).ok();
    evidence["digest"] = digest.clone().map_or(Value::Null, Value::String);
    evidence["crawl_manifest_sha256"] = Value::String(sha256_hex(&manifest_bytes));
    evidence["snapshot_id"] = Value::String(snapshot_id.into());
    evidence["page_count"] = json!(pages.len());
    evidence["body_count"] = json!(body_count);
    evidence["valid"] = json!(
        manifest.get("status") == Some(&Value::String("complete".into()))
            && snapshot_id == expected_snapshot_id
            && bodies_valid
            && digest.is_some()
    );
    evidence
}

fn load_replay_page_bodies(replay: &Path) -> Result<Vec<Vec<u8>>, Error> {
    let manifest_path = replay.join("crawl-manifest.json");
    let manifest_bytes = fs::read(&manifest_path)
        .map_err(|e| Error::invalid(format!("read replay Crawl Manifest: {e}")))?;
    let manifest: Value = serde_json::from_slice(&manifest_bytes)
        .map_err(|e| Error::invalid(format!("parse replay Crawl Manifest: {e}")))?;
    if manifest.get("status") != Some(&Value::String("complete".into())) {
        return Err(Error::invalid("replay Crawl Manifest is not complete"));
    }
    let pages = manifest
        .get("pages")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::invalid("replay Crawl Manifest has no Pages"))?;
    let mut bodies = Vec::new();
    for page in pages {
        if page.get("alias_of").is_some_and(|alias| !alias.is_null()) {
            continue;
        }
        let body_file = page
            .get("body_file")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::invalid("replay Page has no body file"))?;
        let relative = safe_replay_path(body_file)
            .ok_or_else(|| Error::invalid("replay body path escapes replay directory"))?;
        let body = fs::read(replay.join(relative))
            .map_err(|e| Error::invalid(format!("read replay Page body: {e}")))?;
        let expected = page
            .get("body_sha256")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if expected.is_empty() || sha256_hex(&body) != expected {
            return Err(Error::invalid("replay Page body checksum mismatch"));
        }
        bodies.push(body);
    }
    if bodies.is_empty() {
        return Err(Error::invalid("replay Crawl has no representative Pages"));
    }
    Ok(bodies)
}

struct ReplayServer {
    origin: String,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl ReplayServer {
    fn start(bodies: Vec<Vec<u8>>) -> Result<Self, Error> {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .map_err(|e| Error::storage(format!("bind benchmark replay server: {e}")))?;
        listener
            .set_nonblocking(true)
            .map_err(|e| Error::storage(format!("configure benchmark replay server: {e}")))?;
        let origin = format!(
            "http://{}",
            listener
                .local_addr()
                .map_err(|e| Error::storage(format!("read replay server address: {e}")))?
        );
        let routes = bodies
            .into_iter()
            .enumerate()
            .map(|(index, body)| (format!("/__scout_replay/{index}"), body))
            .collect::<HashMap<_, _>>();
        let expected_requests = routes.len() + 1;
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = stop.clone();
        let handle = thread::spawn(move || {
            let mut routes = routes;
            let mut served = 0_usize;
            while !thread_stop.load(AtomicOrdering::Relaxed) && served < expected_requests {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1));
                        continue;
                    }
                    Err(_) => break,
                };
                let Ok((method, target, _)) = read_http_request(&mut stream) else {
                    break;
                };
                if method != "GET" {
                    let _ = write_replay_response(&mut stream, 405, "text/plain", b"method");
                    served += 1;
                    continue;
                }
                let path = target.split('?').next().unwrap_or_default();
                if path == "/robots.txt" {
                    let _ = write_replay_response(
                        &mut stream,
                        200,
                        "text/plain",
                        b"User-agent: *\nAllow: /\n",
                    );
                } else if let Some(body) = routes.remove(path) {
                    let _ = write_replay_response(&mut stream, 200, "text/html", &body);
                } else {
                    let _ = write_replay_response(&mut stream, 404, "text/plain", b"missing");
                }
                served += 1;
            }
        });
        Ok(Self {
            origin,
            stop,
            handle: Some(handle),
        })
    }

    fn stop(&mut self) {
        self.stop.store(true, AtomicOrdering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for ReplayServer {
    fn drop(&mut self) {
        self.stop();
    }
}

fn write_replay_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) -> io::Result<()> {
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "Error",
    };
    write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)
}

struct FreshCrawl {
    data_dir: PathBuf,
    snapshot_id: String,
}

fn benchmark_fresh_crawl(
    data_dir: &Path,
    output_dir: &Path,
    replay: &Path,
    run_number: usize,
) -> Result<FreshCrawl, Error> {
    let bodies = load_replay_page_bodies(replay)?;
    let mut server = ReplayServer::start(bodies.clone())?;
    let stage = output_dir
        .join("staging")
        .join(format!("crawl-run-{run_number:02}"));
    fs::create_dir_all(stage.join("config"))
        .map_err(|e| Error::storage(format!("create fresh crawl staging: {e}")))?;
    let profile = fs::read(data_dir.join("config").join("index.toml"))
        .map_err(|e| Error::invalid(format!("read benchmark build profile: {e}")))?;
    write_if_absent(&stage.join("config").join("index.toml"), &profile)?;
    let seeds = (0..bodies.len())
        .map(|index| format!("\"{}/__scout_replay/{index}\"", server.origin))
        .collect::<Vec<_>>()
        .join(",\n");
    let source_config = format!(
        r#"schema_version = "scout.sources.v1"
contact_url = "https://example.invalid/scout-benchmark"
user_agent = "Scout-Benchmark-Replay/0.1"
page_target = {}
max_frontier = {}
global_concurrency = 16
origin_concurrency = 16
max_attempts = 1
max_total_attempts = {}
max_duration_seconds = 86400
max_extraction_millis = 5000

[[sources]]
source_id = "benchmark-replay"
seeds = [{}]
allowed_origins = ["{}"]
path_prefixes = ["/"]
"#,
        bodies.len(),
        bodies.len().saturating_add(1),
        bodies.len().saturating_mul(2).saturating_add(1),
        seeds,
        server.origin,
    );
    let source_config_path = stage.join("sources.toml");
    fs::write(&source_config_path, source_config)
        .map_err(|e| Error::storage(format!("write fresh crawl config: {e}")))?;
    let status = run([
        OsString::from("scout"),
        OsString::from("crawl"),
        OsString::from("--config"),
        source_config_path.as_os_str().to_os_string(),
        OsString::from("--data-dir"),
        stage.as_os_str().to_os_string(),
    ]);
    server.stop();
    if status != 0 {
        return Err(Error::fetch(format!(
            "benchmark replay Crawl failed with exit code {status}"
        )));
    }
    let crawl_root = stage.join("crawls");
    let crawl_dirs = fs::read_dir(&crawl_root)
        .map_err(|e| Error::storage(format!("read fresh Crawl artifacts: {e}")))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect::<Vec<_>>();
    if crawl_dirs.len() != 1 {
        return Err(Error::storage(
            "fresh Crawl did not produce one Crawl artifact",
        ));
    }
    let crawl_dir = crawl_dirs
        .into_iter()
        .next()
        .expect("fresh Crawl artifact exists");
    let manifest: Value = serde_json::from_slice(
        &fs::read(crawl_dir.join("crawl-manifest.json"))
            .map_err(|e| Error::storage(format!("read fresh Crawl Manifest: {e}")))?,
    )
    .map_err(|e| Error::storage(format!("parse fresh Crawl Manifest: {e}")))?;
    let snapshot_id = manifest
        .get("snapshot_id")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::storage("fresh Crawl did not publish a Corpus snapshot"))?
        .to_owned();
    Ok(FreshCrawl {
        data_dir: stage,
        snapshot_id,
    })
}

fn benchmark_fresh_index(
    data_dir: &Path,
    output_dir: &Path,
    snapshot_id: &str,
    run: usize,
    worker_count: usize,
) -> Result<(PathBuf, GenerationManifest), Error> {
    let workspace = output_dir
        .join("staging")
        .join(format!("index-run-{run:02}-workers-{worker_count}"));
    let (corpus_dir, _) = validate_benchmark_snapshot(data_dir, snapshot_id)?;
    copy_directory(&corpus_dir, &workspace.join("corpora").join(snapshot_id))?;
    let profile_path = data_dir.join("config").join("index.toml");
    let profile_text = fs::read_to_string(&profile_path)
        .map_err(|e| Error::invalid(format!("read benchmark build profile: {e}")))?;
    let mut profile: toml::Value = toml::from_str(&profile_text)
        .map_err(|e| Error::invalid(format!("parse benchmark build profile: {e}")))?;
    profile
        .as_table_mut()
        .ok_or_else(|| Error::invalid("benchmark build profile must be a TOML table"))?
        .insert(
            "worker_count".into(),
            toml::Value::Integer(worker_count as i64),
        );
    let profile_text = toml::to_string_pretty(&profile)
        .map_err(|e| Error::storage(format!("serialize benchmark build profile: {e}")))?;
    let profile_path = workspace.join("config").join("index.toml");
    write_if_absent(&profile_path, profile_text.as_bytes())?;
    let summary = build_generation(
        &workspace,
        snapshot_id,
        &format!("benchmark-index-run-{run}-workers-{worker_count}"),
    )?;
    let staging = PathBuf::from(
        summary["staging_path"]
            .as_str()
            .ok_or_else(|| Error::storage("benchmark index did not return staging path"))?,
    );
    let manifest = validate_generation(&staging, "validated")?;
    if manifest.corpus_snapshot_id != snapshot_id {
        return Err(Error::invalid("fresh staging snapshot mismatch"));
    }
    Ok((staging, manifest))
}

fn execute_benchmark(config_path: &Path, data_dir: &Path, run_id: &str) -> Result<Value, Error> {
    let config_bytes =
        fs::read(config_path).map_err(|e| Error::invalid(format!("read benchmark config: {e}")))?;
    let raw: RawBenchmarkConfig = toml::from_str(
        std::str::from_utf8(&config_bytes)
            .map_err(|e| Error::invalid(format!("benchmark config is not UTF-8: {e}")))?,
    )
    .map_err(|e| Error::invalid(format!("parse benchmark config: {e}")))?;
    let config = load_benchmark_config(raw, config_path, data_dir)?;
    for (name, value) in [
        ("reference_profile", config.reference_profile.as_str()),
        ("corpus_snapshot_id", config.corpus_snapshot_id.as_str()),
        ("generation_id", config.generation_id.as_str()),
        ("build_id", config.build_id.as_str()),
        ("evaluation_id", config.evaluation_id.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(Error::invalid(format!("benchmark plan must lock {name}")));
        }
    }
    if config.reference.hardware.is_none()
        || config.reference.logical_cpus.is_none()
        || config.reference.ram_bytes.is_none()
        || config.reference.swap_bytes.is_none()
        || config.reference.os.is_none()
        || config.reference.kernel.is_none()
        || config.reference.rust_toolchain.is_none()
        || config.reference.lockfile_sha256.is_none()
        || config.reference.filesystem.is_none()
        || config.reference.free_disk_bytes.is_none()
        || config.reference.background_load.is_none()
    {
        return Err(Error::invalid(
            "benchmark Reference profile must record hardware, OS, toolchain, filesystem, and load fields",
        ));
    }
    let generation = load_active_generation(data_dir)?;
    if config.generation_id != generation.manifest.generation_id {
        return Err(Error::invalid(
            "benchmark generation_id does not match CURRENT",
        ));
    }
    if config.corpus_snapshot_id != generation.manifest.corpus_snapshot_id {
        return Err(Error::invalid(
            "benchmark corpus_snapshot_id does not match active Generation",
        ));
    }
    let (snapshot_dir, snapshot) =
        validate_benchmark_snapshot(data_dir, &config.corpus_snapshot_id)?;
    let generation_path = data_dir
        .join("generations")
        .join(&generation.manifest.generation_id);
    let generation_manifest_bytes = fs::read(generation_path.join("generation-manifest.json"))
        .map_err(|e| Error::invalid(format!("read benchmark Generation manifest: {e}")))?;
    let build_profile_bytes = fs::read(generation_path.join("build-profile.json"))
        .map_err(|e| Error::invalid(format!("read benchmark build profile: {e}")))?;
    let build_profile_sha256 = sha256_hex(&build_profile_bytes);
    if build_profile_sha256 != generation.manifest.build_config_sha256 {
        return Err(Error::invalid("benchmark build profile checksum mismatch"));
    }
    let evaluation_package_sha256 = directory_digest(&config.evaluation_package)?;
    let metadata_path = config.evaluation_package.join("metadata.json");
    let evaluation_metadata = metadata_path
        .is_file()
        .then(|| load_evaluation_metadata(&config.evaluation_package))
        .transpose()?;
    if let Some(metadata) = &evaluation_metadata
        && (metadata_string(metadata, &["evaluation_id"]).as_deref()
            != Some(config.evaluation_id.as_str())
            || metadata_string(metadata, &["corpus_snapshot_id", "corpus_snapshot"]).as_deref()
                != Some(config.corpus_snapshot_id.as_str()))
    {
        return Err(Error::invalid(
            "benchmark evaluation metadata does not match locked inputs",
        ));
    }
    let evaluation_metadata_sha256 = metadata_path
        .is_file()
        .then(|| fs::read(&metadata_path).ok())
        .flatten()
        .map(|bytes| sha256_hex(&bytes));
    let query_path = config.evaluation_package.join("queries.jsonl");
    let query_bytes = if query_path.is_file() {
        fs::read(&query_path).map_err(|e| Error::invalid(format!("read benchmark queries: {e}")))?
    } else {
        return Err(Error::invalid(format!(
            "benchmark evaluation package missing {}",
            query_path.display()
        )));
    };
    let queries = parse_jsonl::<EvaluationQuery>(&query_bytes, "queries.jsonl")?;
    validate_benchmark_queries(&queries)?;
    let queries = benchmark_query_order(queries, &config.query_order, config.seed)?;
    let output_dir = data_dir.join("benchmarks").join(run_id);
    fs::create_dir_all(&output_dir)
        .map_err(|e| Error::storage(format!("create benchmark output: {e}")))?;
    let resource_sampler = ResourceSampler::start(
        &output_dir,
        data_dir,
        run_id,
        config.resource_sampling_interval_ms,
        config.runlens_metadata.as_deref(),
    )?;
    let judgments_path = config.evaluation_package.join("judgments.jsonl");
    let replay_crawl = config
        .replay_crawl_dir
        .as_ref()
        .map(|path| inspect_replay_crawl(path, &config.corpus_snapshot_id))
        .unwrap_or_else(|| json!({"configured": false, "valid": false}));
    let plan = BenchmarkPlan {
        schema_version: BENCHMARK_SCHEMA,
        status: "locked",
        run_id: run_id.into(),
        benchmark_id: config.benchmark_id.clone(),
        config_sha256: sha256_hex(&config_bytes),
        reference_profile: config.reference_profile.clone(),
        reference: config.reference.clone(),
        corpus_snapshot_id: config.corpus_snapshot_id.clone(),
        corpus_snapshot_sha256: sha256_hex(
            &fs::read(snapshot_dir.join("snapshot-manifest.json"))
                .map_err(|e| Error::storage(format!("read benchmark snapshot manifest: {e}")))?,
        ),
        generation_id: generation.manifest.generation_id.clone(),
        generation_manifest_sha256: sha256_hex(&generation_manifest_bytes),
        build_id: config.build_id.clone(),
        build_profile_sha256,
        evaluation_id: config.evaluation_id.clone(),
        evaluation_package: config.evaluation_package.display().to_string(),
        evaluation_package_sha256,
        evaluation_metadata_sha256,
        evaluation_queries_sha256: sha256_hex(&query_bytes),
        evaluation_judgments_sha256: judgments_path
            .is_file()
            .then(|| fs::read(&judgments_path).ok())
            .flatten()
            .map(|bytes| sha256_hex(&bytes)),
        query_count: queries.len(),
        query_ids: queries.iter().map(|query| query.query_id.clone()).collect(),
        seed: config.seed,
        query_order: config.query_order.clone(),
        cache_state: config.cache_state.clone(),
        repetitions: config.repetitions,
        fresh_staging_runs: config.fresh_staging_runs,
        cold_open_runs: config.cold_open_runs,
        warmup_passes: config.warmup_passes,
        measured_passes: config.measured_passes,
        concurrency: config.concurrency.clone(),
        index_workers: config.index_workers.clone(),
        replay_crawl: replay_crawl.clone(),
        runlens_metadata: config
            .runlens_metadata
            .as_ref()
            .map(|path| path.display().to_string()),
        resource_sampling_interval_ms: config.resource_sampling_interval_ms,
        performance_thresholds: Vec::new(),
    };
    write_json(&output_dir.join("plan.json"), &plan)?;
    let mut samples = Vec::new();
    let (swap_in_before, swap_out_before) = read_swap_counters();
    let oom_before = read_oom_counter();
    let generation_path = data_dir
        .join("generations")
        .join(&generation.manifest.generation_id);
    let replay_valid = replay_crawl["valid"].as_bool().unwrap_or(false);
    if let Some(replay) = config.replay_crawl_dir.as_ref() {
        resource_sampler.set_phase("crawl-replay");
        let started = Instant::now();
        let integrity_verified = replay_valid;
        samples.push(BenchmarkSample {
            phase: "crawl-replay".into(),
            run: 1,
            pass: None,
            mode: None,
            concurrency: None,
            worker_count: None,
            cache_state: "recorded".into(),
            duration_ms: started.elapsed().as_secs_f64() * 1_000.0,
            query_count: 0,
            qps: None,
            latency_ms: None,
            integrity_verified,
            artifact_path: Some(replay.display().to_string()),
            status: if integrity_verified {
                "complete"
            } else {
                "failed"
            }
            .into(),
        });
        resource_sampler.end_phase(if integrity_verified {
            "complete"
        } else {
            "failed"
        });
    }
    for run in 0..config.fresh_staging_runs {
        let run_number = run + 1;
        resource_sampler.set_phase("staging-integrity");
        let started = Instant::now();
        let integrity_verified = validate_generation(&generation_path, "sealed").is_ok();
        let duration_ms = started.elapsed().as_secs_f64() * 1_000.0;
        samples.push(BenchmarkSample {
            phase: "staging-integrity".into(),
            run: run + 1,
            pass: None,
            mode: None,
            concurrency: None,
            worker_count: None,
            cache_state: "recorded".into(),
            duration_ms,
            query_count: 0,
            qps: None,
            latency_ms: None,
            integrity_verified,
            artifact_path: Some(generation_path.display().to_string()),
            status: if integrity_verified {
                "complete"
            } else {
                "failed"
            }
            .into(),
        });
        resource_sampler.end_phase(if integrity_verified {
            "complete"
        } else {
            "failed"
        });
        resource_sampler.set_phase("crawl-fresh-staging");
        let crawl_started = Instant::now();
        let fresh_crawl = config
            .replay_crawl_dir
            .as_ref()
            .filter(|_| replay_valid)
            .and_then(|replay| {
                benchmark_fresh_crawl(data_dir, &output_dir, replay, run_number).ok()
            });
        let crawl_stage = fresh_crawl
            .as_ref()
            .map(|fresh| fresh.data_dir.clone())
            .unwrap_or_else(|| {
                output_dir
                    .join("staging")
                    .join(format!("crawl-run-{run_number:02}"))
            });
        let crawl_ok = fresh_crawl.is_some();
        samples.push(BenchmarkSample {
            phase: "crawl-fresh-staging".into(),
            run: run_number,
            pass: None,
            mode: None,
            concurrency: None,
            worker_count: None,
            cache_state: "recorded".into(),
            duration_ms: crawl_started.elapsed().as_secs_f64() * 1_000.0,
            query_count: 0,
            qps: None,
            latency_ms: None,
            integrity_verified: crawl_ok,
            artifact_path: Some(crawl_stage.display().to_string()),
            status: if crawl_ok { "complete" } else { "failed" }.into(),
        });
        resource_sampler.end_phase(if crawl_ok { "complete" } else { "failed" });

        let chosen_worker = config.index_workers[run % config.index_workers.len()];
        for &worker_count in &config.index_workers {
            let index_phase = if worker_count == chosen_worker {
                "index-fresh-staging"
            } else {
                "index-worker"
            };
            resource_sampler.set_phase(index_phase);
            let started = Instant::now();
            let artifact_path = output_dir
                .join("staging")
                .join(format!("index-run-{run_number:02}-workers-{worker_count}"));
            let result = fresh_crawl.as_ref().map_or_else(
                || Err(Error::storage("fresh Crawl unavailable for index staging")),
                |fresh| {
                    benchmark_fresh_index(
                        &fresh.data_dir,
                        &output_dir,
                        &fresh.snapshot_id,
                        run_number,
                        worker_count,
                    )
                },
            );
            let (artifact_path, integrity_verified) = match result {
                Ok((staging, _manifest)) => (staging, true),
                Err(_) => (artifact_path, false),
            };
            samples.push(BenchmarkSample {
                phase: index_phase.into(),
                run: run_number,
                pass: None,
                mode: None,
                concurrency: None,
                worker_count: Some(worker_count),
                cache_state: "recorded".into(),
                duration_ms: started.elapsed().as_secs_f64() * 1_000.0,
                query_count: 0,
                qps: None,
                latency_ms: None,
                integrity_verified,
                artifact_path: Some(artifact_path.display().to_string()),
                status: if integrity_verified {
                    "complete"
                } else {
                    "failed"
                }
                .into(),
            });
            resource_sampler.end_phase(if integrity_verified {
                "complete"
            } else {
                "failed"
            });
        }
    }
    let generation = Arc::new(generation);
    for run in 0..config.cold_open_runs {
        resource_sampler.set_phase("serving-cold-open");
        let started = Instant::now();
        let executable = std::env::current_exe()
            .map_err(|e| Error::storage(format!("resolve Scout executable: {e}")))?;
        let process = Command::new(executable)
            .args(["index", "verify", "--data-dir"])
            .arg(data_dir)
            .output()
            .map_err(|e| Error::storage(format!("run cold-open process: {e}")))?;
        let integrity_verified = process.status.success();
        let duration_ms = started.elapsed().as_secs_f64() * 1_000.0;
        samples.push(BenchmarkSample {
            phase: "serving-cold-open".into(),
            run: run + 1,
            pass: None,
            mode: None,
            concurrency: None,
            worker_count: None,
            cache_state: "cold".into(),
            duration_ms,
            query_count: 0,
            qps: None,
            latency_ms: None,
            integrity_verified,
            artifact_path: Some(generation_path.display().to_string()),
            status: if integrity_verified {
                "complete"
            } else {
                "failed"
            }
            .into(),
        });
        resource_sampler.end_phase(if integrity_verified {
            "complete"
        } else {
            "failed"
        });
    }
    for repetition in 0..config.repetitions {
        for &concurrency in &config.concurrency {
            for pass in 0..config.warmup_passes {
                resource_sampler.set_phase("serving-warmup");
                let (duration_ms, latencies) =
                    benchmark_search_pass(generation.clone(), &queries, concurrency)?;
                samples.push(BenchmarkSample {
                    phase: "serving-warmup".into(),
                    run: repetition + 1,
                    pass: Some(pass + 1),
                    mode: Some("hybrid".into()),
                    concurrency: Some(concurrency),
                    worker_count: None,
                    cache_state: "warmup".into(),
                    duration_ms,
                    query_count: queries.len(),
                    qps: None,
                    latency_ms: Some(benchmark_distribution(&latencies)),
                    integrity_verified: true,
                    artifact_path: Some(generation_path.display().to_string()),
                    status: "complete".into(),
                });
                resource_sampler.end_phase("complete");
            }
            for pass in 0..config.measured_passes {
                resource_sampler.set_phase("serving-warm");
                let (duration_ms, latencies) =
                    benchmark_search_pass(generation.clone(), &queries, concurrency)?;
                samples.push(BenchmarkSample {
                    phase: "serving-warm".into(),
                    run: repetition + 1,
                    pass: Some(pass + 1),
                    mode: Some("hybrid".into()),
                    concurrency: Some(concurrency),
                    worker_count: None,
                    cache_state: config.cache_state.clone(),
                    duration_ms,
                    query_count: queries.len(),
                    qps: (duration_ms > 0.0)
                        .then(|| queries.len() as f64 / (duration_ms / 1_000.0)),
                    latency_ms: Some(benchmark_distribution(&latencies)),
                    integrity_verified: true,
                    artifact_path: Some(generation_path.display().to_string()),
                    status: "complete".into(),
                });
                resource_sampler.end_phase("complete");
            }
        }
    }
    let mut distributions = BTreeMap::new();
    for phase in [
        "staging-integrity",
        "crawl-fresh-staging",
        "index-fresh-staging",
        "index-worker",
        "crawl-replay",
        "serving-cold-open",
        "serving-warmup",
        "serving-warm",
    ] {
        let values = samples
            .iter()
            .filter(|sample| sample.phase == phase)
            .map(|sample| sample.duration_ms)
            .collect::<Vec<_>>();
        distributions.insert(phase, benchmark_distribution(&values));
    }
    let distribution = distributions
        .get("serving-warm")
        .cloned()
        .unwrap_or_else(|| benchmark_distribution(&[]));
    let (swap_in_after, swap_out_after) = read_swap_counters();
    let oom_after = read_oom_counter();
    let generation_staging_leftover = data_dir
        .join("generations")
        .join(".staging")
        .read_dir()
        .is_ok_and(|mut entries| entries.next().is_some());
    let partial_artifacts =
        generation_staging_leftover || samples.iter().any(|sample| sample.status != "complete");
    let integrity_verified = samples.iter().all(|sample| sample.integrity_verified);
    let swap_activity_detected = swap_in_before
        .zip(swap_in_after)
        .is_some_and(|(before, after)| after > before)
        || swap_out_before
            .zip(swap_out_after)
            .is_some_and(|(before, after)| after > before);
    let oom_observed = oom_before
        .zip(oom_after)
        .map(|(before, after)| after > before);
    let runlens_metadata_path = config
        .runlens_metadata
        .as_ref()
        .map(|path| path.display().to_string());
    let runlens_bytes = config
        .runlens_metadata
        .as_ref()
        .and_then(|path| fs::read(path).ok());
    let runlens_metadata_sha256 = runlens_bytes.as_ref().map(|bytes| sha256_hex(bytes));
    let runlens_metadata = runlens_bytes
        .as_deref()
        .and_then(|bytes| serde_json::from_slice(bytes).ok())
        .unwrap_or(Value::Null);
    resource_sampler.set_phase("reporting");
    let resource_summary = resource_sampler.finish("complete")?;
    let page_cache_state = if config.cache_state == "cold" {
        "uncontrolled"
    } else {
        config.cache_state.as_str()
    };
    let system = BenchmarkSystemEvidence {
        benchmark_run_id: run_id.into(),
        process_id: std::process::id(),
        swap_in_before,
        swap_in_after,
        swap_out_before,
        swap_out_after,
        swap_activity_detected,
        oom_observed,
        partial_artifacts,
        integrity_verified,
        page_cache_state: page_cache_state.into(),
        runlens_metadata_path,
        runlens_metadata_sha256,
        runlens_metadata,
        resource_samples_path: output_dir
            .join("resource-samples.jsonl")
            .display()
            .to_string(),
        resource_boundaries_path: output_dir
            .join("resource-boundaries.jsonl")
            .display()
            .to_string(),
        resource_summary_path: output_dir
            .join("resource-summary.json")
            .display()
            .to_string(),
        resource_sampler: serde_json::to_value(&resource_summary)
            .map_err(|e| Error::storage(format!("serialize resource summary: {e}")))?,
    };
    let system_value = serde_json::to_value(&system)
        .map_err(|e| Error::storage(format!("serialize benchmark system evidence: {e}")))?;
    let unstable_phases = distributions
        .iter()
        .filter(|(_, distribution)| distribution.unstable)
        .map(|(phase, _)| (*phase).to_owned())
        .collect::<Vec<_>>();
    let unstable = !unstable_phases.is_empty()
        || swap_activity_detected
        || oom_observed.unwrap_or(false)
        || partial_artifacts
        || !integrity_verified;
    let domain_metrics = json!({
        "corpus_snapshot": {
            "snapshot_id": config.corpus_snapshot_id,
            "page_count": snapshot.page_count,
            "alias_count": snapshot.alias_count,
            "passage_count": snapshot.passage_count,
        },
        "generation": {
            "generation_id": generation.manifest.generation_id,
            "page_count": generation.manifest.page_count,
            "passage_count": generation.manifest.passage_count,
            "alias_count": generation.manifest.alias_count,
            "keyword_count": generation.manifest.keyword_count,
            "semantic_count": generation.manifest.semantic_count,
            "artifact_bytes": directory_bytes(&generation_path).unwrap_or(0),
        },
        "replay_crawl": replay_crawl,
        "index_worker_levels": config.index_workers,
        "search_concurrency_levels": config.concurrency,
    });
    let integrity_checks = json!({
        "active_generation": integrity_verified,
        "replay_crawl": replay_valid,
        "fresh_staging_runs": samples
            .iter()
            .filter(|sample| {
                matches!(sample.phase.as_str(), "crawl-fresh-staging" | "index-fresh-staging")
            })
            .all(|sample| sample.integrity_verified),
        "system": {
            "swap_activity_detected": swap_activity_detected,
            "oom_observed": oom_observed.unwrap_or(false),
            "partial_artifacts": partial_artifacts,
        },
    });
    write_if_absent(&output_dir.join("samples.jsonl"), &jsonl(&samples)?)?;
    write_json(&output_dir.join("system-evidence.json"), &system)?;
    let report = json!({
        "schema_version": BENCHMARK_SCHEMA,
        "run_id": run_id,
        "benchmark_id": config.benchmark_id,
        "plan": "plan.json",
        "samples": "samples.jsonl",
        "distribution": distribution,
        "distributions": distributions,
        "phase_timings": distributions,
        "domain_metrics": domain_metrics,
        "integrity_checks": integrity_checks,
        "operational_log": data_dir
            .join("logs")
            .join(format!("{run_id}.jsonl")),
        "system_evidence": system_value,
        "resource_evidence": {
            "samples": "resource-samples.jsonl",
            "boundaries": "resource-boundaries.jsonl",
            "configuration": "resource-config.json",
            "summary": "resource-summary.json",
            "status": resource_summary.status,
        },
        "flags": {
            "swap_activity_detected": swap_activity_detected,
            "oom_observed": oom_observed.unwrap_or(false),
            "partial_artifacts": partial_artifacts,
            "integrity_verified": integrity_verified,
            "unstable_phases": unstable_phases,
        },
        "performance_thresholds_claimed": false,
        "status": if unstable { "unstable" } else { "complete" }
    });
    let report_path = output_dir.join("report.json");
    write_json(&report_path, &report)?;
    Ok(json!({"report_path": report_path, "sample_count": samples.len(), "unstable": unstable}))
}

fn load_benchmark_config(
    raw: RawBenchmarkConfig,
    config_path: &Path,
    data_dir: &Path,
) -> Result<BenchmarkConfig, Error> {
    let schema_version = raw
        .schema_version
        .unwrap_or_else(|| BENCHMARK_SCHEMA.into());
    if !schema_version.ends_with(".v1") {
        return Err(Error::invalid("unsupported benchmark schema_version"));
    }
    let repetitions = raw.repetitions.unwrap_or(BENCHMARK_DEFAULT_REPETITIONS);
    if repetitions == 0 {
        return Err(Error::invalid("benchmark repetitions must be positive"));
    }
    let query_order = raw.query_order.unwrap_or_else(|| "fixed".into());
    if !matches!(query_order.as_str(), "fixed" | "seeded") {
        return Err(Error::invalid("query_order must be fixed or seeded"));
    }
    let cache_state = raw.cache_state.unwrap_or_else(|| "warm".into());
    if !matches!(cache_state.as_str(), "cold" | "warm" | "recorded") {
        return Err(Error::invalid(
            "cache_state must be cold, warm, or recorded",
        ));
    }
    let fresh_staging_runs = raw.fresh_staging_runs.unwrap_or(3);
    let cold_open_runs = raw
        .cold_open_runs
        .unwrap_or(BENCHMARK_DEFAULT_COLD_OPEN_RUNS);
    let warmup_passes = raw.warmup_passes.unwrap_or(BENCHMARK_DEFAULT_WARMUP_PASSES);
    let measured_passes = raw
        .measured_passes
        .unwrap_or(BENCHMARK_DEFAULT_MEASURED_PASSES);
    let concurrency = raw
        .concurrency
        .unwrap_or_else(|| BENCHMARK_DEFAULT_CONCURRENCY.to_vec());
    let index_workers = raw
        .index_workers
        .unwrap_or_else(|| BENCHMARK_DEFAULT_INDEX_WORKERS.to_vec());
    let resource_sampling_interval_ms = raw.resource_sampling_interval_ms.unwrap_or(100);
    if fresh_staging_runs == 0
        || cold_open_runs == 0
        || warmup_passes == 0
        || measured_passes == 0
        || concurrency.is_empty()
        || concurrency.contains(&0)
        || index_workers.is_empty()
        || index_workers.contains(&0)
        || resource_sampling_interval_ms == 0
    {
        return Err(Error::invalid(
            "benchmark phase counts, concurrency levels, and resource sampling interval must be positive",
        ));
    }
    let package = raw
        .evaluation_package
        .map(|path| {
            if path.is_absolute() {
                path
            } else {
                config_path.parent().unwrap_or(Path::new(".")).join(path)
            }
        })
        .unwrap_or_else(|| data_dir.join("evaluations"));
    let resolve_path = |path: Option<PathBuf>| {
        path.map(|path| {
            if path.is_absolute() {
                path
            } else {
                config_path.parent().unwrap_or(Path::new(".")).join(path)
            }
        })
    };
    Ok(BenchmarkConfig {
        schema_version,
        benchmark_id: raw.benchmark_id.unwrap_or_else(|| "scout-benchmark".into()),
        reference_profile: raw.reference_profile.unwrap_or_else(|| "local".into()),
        reference: raw.reference.unwrap_or_default(),
        corpus_snapshot_id: raw.corpus_snapshot_id.unwrap_or_default(),
        generation_id: raw.generation_id.unwrap_or_default(),
        build_id: raw.build_id.unwrap_or_default(),
        evaluation_id: raw.evaluation_id.unwrap_or_default(),
        evaluation_package: package,
        seed: raw.seed.unwrap_or(0),
        query_order,
        cache_state,
        repetitions,
        fresh_staging_runs,
        cold_open_runs,
        warmup_passes,
        measured_passes,
        concurrency,
        index_workers,
        replay_crawl_dir: resolve_path(raw.replay_crawl_dir),
        runlens_metadata: resolve_path(raw.runlens_metadata),
        resource_sampling_interval_ms,
    })
}

fn benchmark_search_pass(
    generation: Arc<LoadedGeneration>,
    queries: &[EvaluationQuery],
    concurrency: usize,
) -> Result<(f64, Vec<f64>), Error> {
    if queries.is_empty() {
        return Ok((0.0, Vec::new()));
    }
    let queries = Arc::new(queries.to_vec());
    let cursor = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let latencies = Arc::new(Mutex::new(Vec::with_capacity(queries.len())));
    let failure = Arc::new(Mutex::new(None::<Error>));
    let worker_count = concurrency.max(1).min(queries.len());
    let started = Instant::now();
    let mut workers = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let generation = generation.clone();
        let cursor = cursor.clone();
        let latencies = latencies.clone();
        let failure = failure.clone();
        let queries = queries.clone();
        workers.push(thread::spawn(move || {
            loop {
                let index = cursor.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if index >= queries.len() {
                    break;
                }
                let query = &queries[index];
                let query_started = Instant::now();
                if let Err(error) =
                    ranked_page_ids(&generation, "hybrid", &query.query, Some(&query.source))
                {
                    let mut slot = failure.lock().expect("benchmark failure lock");
                    if slot.is_none() {
                        *slot = Some(error);
                    }
                    break;
                }
                latencies
                    .lock()
                    .expect("benchmark latency lock")
                    .push(query_started.elapsed().as_secs_f64() * 1_000.0);
            }
        }));
    }
    for worker in workers {
        worker
            .join()
            .map_err(|_| Error::storage("benchmark worker panicked"))?;
    }
    if let Some(error) = failure.lock().expect("benchmark failure lock").take() {
        return Err(error);
    }
    Ok((
        started.elapsed().as_secs_f64() * 1_000.0,
        Arc::try_unwrap(latencies)
            .map_err(|_| Error::storage("benchmark latency collection still shared"))?
            .into_inner()
            .map_err(|_| Error::storage("benchmark latency collection poisoned"))?,
    ))
}

fn read_swap_counters() -> (Option<u64>, Option<u64>) {
    let Ok(text) = fs::read_to_string("/proc/vmstat") else {
        return (None, None);
    };
    let mut swap_in = None;
    let mut swap_out = None;
    for line in text.lines() {
        let mut fields = line.split_whitespace();
        match fields.next() {
            Some("pswpin") => swap_in = fields.next().and_then(|value| value.parse().ok()),
            Some("pswpout") => swap_out = fields.next().and_then(|value| value.parse().ok()),
            _ => {}
        }
    }
    (swap_in, swap_out)
}

fn read_oom_counter() -> Option<u64> {
    let text = fs::read_to_string("/sys/fs/cgroup/memory.events").ok()?;
    text.lines().find_map(|line| {
        line.strip_prefix("oom_kill ")
            .and_then(|value| value.parse().ok())
    })
}

pub(crate) fn benchmark_query_order(
    mut queries: Vec<EvaluationQuery>,
    order: &str,
    seed: u64,
) -> Result<Vec<EvaluationQuery>, Error> {
    if order == "fixed" {
        return Ok(queries);
    }
    if order != "seeded" {
        return Err(Error::invalid("query_order must be fixed or seeded"));
    }
    let mut state = seed;
    for index in (1..queries.len()).rev() {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        queries.swap(index, (state as usize) % (index + 1));
    }
    Ok(queries)
}

pub(crate) fn benchmark_distribution(values: &[f64]) -> BenchmarkDistribution {
    if values.is_empty() {
        return BenchmarkDistribution {
            sample_count: 0,
            min_ms: 0.0,
            p50_ms: 0.0,
            p95_ms: 0.0,
            p99_ms: 0.0,
            max_ms: 0.0,
            median_spread: 0.0,
            unstable: false,
        };
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
    let percentile = |fraction: f64| {
        sorted[((sorted.len() as f64 * fraction).ceil() as usize)
            .saturating_sub(1)
            .min(sorted.len() - 1)]
    };
    let min_ms = sorted[0];
    let max_ms = *sorted.last().unwrap_or(&min_ms);
    let p50_ms = percentile(0.50);
    let median_spread = if p50_ms == 0.0 {
        0.0
    } else {
        (max_ms - min_ms) / p50_ms
    };
    BenchmarkDistribution {
        sample_count: sorted.len(),
        min_ms,
        p50_ms,
        p95_ms: percentile(0.95),
        p99_ms: percentile(0.99),
        max_ms,
        median_spread,
        unstable: median_spread > BENCHMARK_INSTABILITY_LIMIT,
    }
}
