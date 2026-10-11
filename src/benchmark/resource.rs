use crate::error::Error;
use crate::storage::{sha256_hex, write_json};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, serde::Serialize)]
struct ResourceSample {
    schema_version: &'static str,
    timestamp_utc: String,
    elapsed_ms: u64,
    benchmark_run_id: String,
    phase: String,
    phase_sequence: usize,
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
    host_swap_in_phase_delta: Option<u64>,
    host_swap_out_phase_delta: Option<u64>,
    host_swap_total_bytes: Option<u64>,
    host_swap_free_bytes: Option<u64>,
    host_swap_used_bytes: Option<u64>,
    host_observation_scope: &'static str,
    host_mem_available_bytes: Option<u64>,
    host_mem_available_scope: &'static str,
    filesystem_used_bytes: Option<u64>,
    filesystem_used_scope: &'static str,
    filesystem_used_bytes_sample_age_ms: Option<u64>,
    filesystem_used_bytes_is_stale: bool,
    filesystem_used_bytes_sampled_this_row: bool,
    data_dir_bytes: Option<u64>,
    data_dir_bytes_scope: &'static str,
    data_dir_bytes_sample_age_ms: Option<u64>,
    data_dir_bytes_is_stale: bool,
    data_dir_bytes_sampled_this_row: bool,
    data_dir_growth_bytes: Option<i64>,
    data_dir_growth_scope: &'static str,
    process_cpu_time_delta_ms: Option<f64>,
    baseline_fields: Vec<String>,
    missing_fields: Vec<String>,
    status: &'static str,
    #[serde(flatten)]
    accelerator: AcceleratorSample,
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

#[derive(Clone, Copy)]
struct HostMemorySnapshot {
    swap_in_counter: Option<u64>,
    swap_out_counter: Option<u64>,
    swap_total_bytes: Option<u64>,
    swap_free_bytes: Option<u64>,
    mem_available_bytes: Option<u64>,
}

struct ResourceSummaryInput<'a> {
    run_id: &'a str,
    root_pid: u32,
    interval_ms: u64,
    samples: &'a [ResourceSample],
    boundaries: &'a [ResourceBoundary],
    status: &'a str,
    accelerator: &'a AcceleratorCapability,
    runlens_metadata: Option<&'a Path>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct ResourcePhaseSummary {
    sample_count: usize,
    measured_cpu_samples: usize,
    mean_cpu_utilization_percent: Option<f64>,
    phase_cpu_time_delta_ms: Option<f64>,
    peak_rss_bytes_sampled: Option<u64>,
    peak_rss_bytes_os_high_water: Option<u64>,
    min_host_mem_available_bytes: Option<u64>,
    mean_accelerator_utilization_percent: Option<f64>,
    peak_accelerator_memory_used_bytes: Option<u64>,
    host_swap_in_delta_pages: Option<u64>,
    host_swap_out_delta_pages: Option<u64>,
    host_swap_delta_scope: &'static str,
    missing_sample_count: usize,
    disk_growth_bytes: Option<i64>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(super) struct ResourceSummary {
    schema_version: &'static str,
    pub(super) run_status: String,
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
    phase_cpu_time_scope: &'static str,
    host_swap_in_delta: Option<u64>,
    host_swap_out_delta: Option<u64>,
    host_swap_in_scope: &'static str,
    host_swap_out_scope: &'static str,
    host_swap_counter_unit: &'static str,
    host_swap_usage_start_bytes: Option<u64>,
    host_swap_usage_end_bytes: Option<u64>,
    host_swap_usage_scope: &'static str,
    host_swap_delta_scope: &'static str,
    min_host_mem_available_bytes: Option<u64>,
    host_mem_available_scope: &'static str,
    disk_growth_bytes: Option<i64>,
    disk_growth_scope: &'static str,
    missing_sample_count: usize,
    pub(super) evidence_status: &'static str,
    baseline_fields: Vec<String>,
    missing_fields: Vec<String>,
    by_phase: BTreeMap<String, ResourcePhaseSummary>,
    phase_boundaries: Vec<ResourceBoundary>,
    accelerator: AcceleratorSummary,
    runlens_metadata_path: Option<String>,
    runlens_metadata_sha256: Option<String>,
    runlens_trace_id: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct AcceleratorSample {
    accelerator_requested_provider: Option<String>,
    accelerator_target: Option<String>,
    accelerator_telemetry_provider: Option<String>,
    accelerator_device: Option<String>,
    accelerator_status: &'static str,
    accelerator_reason: Option<String>,
    accelerator_utilization_percent: Option<f64>,
    accelerator_memory_used_bytes: Option<u64>,
    accelerator_metrics_scope: &'static str,
}

#[derive(Debug, Clone)]
struct AcceleratorCapability {
    requested_provider: Option<String>,
    target: Option<String>,
    telemetry_provider: Option<String>,
    device: Option<String>,
    status: &'static str,
    reason: Option<String>,
    device_path: Option<PathBuf>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct AcceleratorSummary {
    requested_provider: Option<String>,
    target: Option<String>,
    telemetry_provider: Option<String>,
    device: Option<String>,
    status: &'static str,
    reason: Option<String>,
    mean_utilization_percent: Option<f64>,
    peak_memory_used_bytes: Option<u64>,
    metrics_scope: &'static str,
}

#[derive(Debug, Clone)]
struct ProcessSnapshot {
    cpu_ticks_by_pid: BTreeMap<u32, u64>,
    rss_bytes: u64,
    hwm_bytes: u64,
    sampled_pids: Vec<u32>,
    missing_pids: Vec<u32>,
}

pub(super) struct ResourceSampler {
    stop: Arc<AtomicBool>,
    phase: Arc<Mutex<Option<(String, usize)>>>,
    boundaries: Arc<Mutex<Vec<ResourceBoundary>>>,
    samples: Arc<Mutex<Vec<ResourceSample>>>,
    boundary_file: Mutex<fs::File>,
    join: Option<thread::JoinHandle<Result<(), Error>>>,
    output_dir: PathBuf,
    data_dir: PathBuf,
    run_id: String,
    root_pid: u32,
    interval_ms: u64,
    logical_cpus: usize,
    ticks_per_second: u64,
    accelerator: AcceleratorCapability,
    runlens_metadata: Option<PathBuf>,
    started: Instant,
}

impl ResourceSampler {
    pub(super) fn start(
        output_dir: &Path,
        data_dir: &Path,
        run_id: &str,
        interval_ms: u64,
        runlens_metadata: Option<&Path>,
        requested_accelerator_provider: Option<&str>,
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
        let accelerator = probe_accelerator(requested_accelerator_provider);
        let stop = Arc::new(AtomicBool::new(false));
        let phase = Arc::new(Mutex::new(Some(("setup".into(), 0))));
        let initial_boundary = ResourceBoundary {
            schema_version: "scout.resource-boundary.v1",
            timestamp_utc: resource_timestamp(),
            elapsed_ms: 0,
            benchmark_run_id: run_id.into(),
            kind: "start",
            phase: "setup".into(),
            sequence: 0,
            status: None,
        };
        let mut boundary_file = fs::File::create(output_dir.join("resource-boundaries.jsonl"))
            .map_err(|e| Error::storage(format!("create resource boundaries: {e}")))?;
        append_resource_jsonl(&mut boundary_file, &initial_boundary, "resource boundary")?;
        let boundaries = Arc::new(Mutex::new(vec![initial_boundary]));
        let samples = Arc::new(Mutex::new(Vec::<ResourceSample>::new()));
        let mut sample_file = fs::File::create(output_dir.join("resource-samples.jsonl"))
            .map_err(|e| Error::storage(format!("create resource samples: {e}")))?;
        let thread_stop = stop.clone();
        let thread_phase = phase.clone();
        let thread_samples = samples.clone();
        let thread_data_dir = data_dir.to_owned();
        let thread_run_id = run_id.to_owned();
        let thread_accelerator = accelerator.clone();
        let thread_started = Instant::now();
        let join = thread::spawn(move || -> Result<(), Error> {
            let mut previous: Option<(Instant, BTreeMap<u32, u64>)> = None;
            let mut previous_phase_sequence = None;
            let mut previous_process_cpu_time_ms = None;
            let mut observed_cpu_ticks = BTreeMap::<u32, u64>::new();
            let mut disk_probe_at = None;
            let mut filesystem_sampled_at = None;
            let mut data_dir_sampled_at = None;
            let mut filesystem_used_bytes = None;
            let mut data_dir_bytes = None;
            while !thread_stop.load(AtomicOrdering::Relaxed) {
                let sampled_at = Instant::now();
                let (phase_name, phase_sequence) = thread_phase
                    .lock()
                    .ok()
                    .and_then(|phase| phase.clone())
                    .unwrap_or_else(|| ("unassigned".into(), 0));
                let process = read_process_snapshot(root_pid);
                let host = read_host_memory();
                let phase_changed = previous_phase_sequence != Some(phase_sequence);
                if phase_changed {
                    previous = None;
                    previous_process_cpu_time_ms = None;
                }
                previous_phase_sequence = Some(phase_sequence);
                let disk_sample_interval = Duration::from_secs(1);
                let mut filesystem_sampled_this_row = false;
                let mut data_dir_sampled_this_row = false;
                if disk_probe_at
                    .is_none_or(|last| sampled_at.duration_since(last) >= disk_sample_interval)
                {
                    if let Some(value) = read_filesystem_used_bytes(&thread_data_dir) {
                        filesystem_used_bytes = Some(value);
                        filesystem_sampled_at = Some(sampled_at);
                        filesystem_sampled_this_row = true;
                    }
                    if let Some(value) = directory_metadata_bytes(&thread_data_dir) {
                        data_dir_bytes = Some(value);
                        data_dir_sampled_at = Some(sampled_at);
                        data_dir_sampled_this_row = true;
                    }
                    disk_probe_at = Some(sampled_at);
                }
                let filesystem_sample_age_ms = filesystem_sampled_at
                    .map(|at| sampled_at.duration_since(at).as_millis() as u64);
                let data_dir_sample_age_ms =
                    data_dir_sampled_at.map(|at| sampled_at.duration_since(at).as_millis() as u64);
                let accelerator_metrics = read_accelerator_metrics(&thread_accelerator);
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
                let previous_sample = thread_samples.lock().ok().and_then(|samples| {
                    samples.last().map(|sample| {
                        (
                            sample.data_dir_bytes,
                            sample.host_swap_in_counter,
                            sample.host_swap_out_counter,
                            sample.phase_sequence,
                        )
                    })
                });
                let previous_disk = previous_sample.and_then(|sample| sample.0);
                let data_dir_growth_bytes = data_dir_bytes
                    .zip(previous_disk)
                    .map(|(current, previous)| current as i64 - previous as i64);
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
                let process_cpu_time_delta_ms = process
                    .as_ref()
                    .and(process_cpu_time_ms)
                    .zip(previous_process_cpu_time_ms)
                    .map(|(current, previous)| current - previous);
                let mut missing_fields = Vec::new();
                let mut baseline_fields = Vec::new();
                if process.is_none() {
                    missing_fields.push("process_metrics".into());
                } else if process
                    .as_ref()
                    .is_some_and(|value| !value.missing_pids.is_empty())
                {
                    missing_fields.push("process_tree_child_metrics".into());
                }
                if actual_interval_ms.is_none() {
                    baseline_fields.push("cpu_interval_baseline".into());
                }
                if host.swap_in_counter.is_none() {
                    missing_fields.push("host_swap_in_counter".into());
                }
                if host.swap_out_counter.is_none() {
                    missing_fields.push("host_swap_out_counter".into());
                }
                if host.swap_total_bytes.is_none() {
                    missing_fields.push("host_swap_total_bytes".into());
                }
                if host.swap_free_bytes.is_none() {
                    missing_fields.push("host_swap_free_bytes".into());
                }
                if host.mem_available_bytes.is_none() {
                    missing_fields.push("host_mem_available_bytes".into());
                }
                if filesystem_used_bytes.is_none() {
                    missing_fields.push("filesystem_used_bytes".into());
                }
                if data_dir_bytes.is_none() {
                    missing_fields.push("data_dir_bytes".into());
                }
                if data_dir_bytes.is_some() && previous_disk.is_none() {
                    baseline_fields.push("data_dir_growth_baseline".into());
                }
                if filesystem_sample_age_ms.is_some_and(|age| age > 2_000) {
                    missing_fields.push("filesystem_used_bytes_stale".into());
                }
                if data_dir_sample_age_ms.is_some_and(|age| age > 2_000) {
                    missing_fields.push("data_dir_bytes_stale".into());
                }
                let host_swap_in_delta = host
                    .swap_in_counter
                    .zip(previous_sample.and_then(|sample| sample.1))
                    .map(|(current, previous)| current.saturating_sub(previous));
                let host_swap_out_delta = host
                    .swap_out_counter
                    .zip(previous_sample.and_then(|sample| sample.2))
                    .map(|(current, previous)| current.saturating_sub(previous));
                let phase_swap_baseline =
                    previous_sample.filter(|sample| sample.3 == phase_sequence);
                let host_swap_in_phase_delta = host
                    .swap_in_counter
                    .zip(phase_swap_baseline.and_then(|sample| sample.1))
                    .map(|(current, previous)| current.saturating_sub(previous));
                let host_swap_out_phase_delta = host
                    .swap_out_counter
                    .zip(phase_swap_baseline.and_then(|sample| sample.2))
                    .map(|(current, previous)| current.saturating_sub(previous));
                if host.swap_in_counter.is_some() && host_swap_in_delta.is_none() {
                    baseline_fields.push("host_swap_in_delta_baseline".into());
                }
                if host.swap_out_counter.is_some() && host_swap_out_delta.is_none() {
                    baseline_fields.push("host_swap_out_delta_baseline".into());
                }
                if host.swap_in_counter.is_some() && host_swap_in_phase_delta.is_none() {
                    baseline_fields.push("phase_swap_in_delta_baseline".into());
                }
                if host.swap_out_counter.is_some() && host_swap_out_phase_delta.is_none() {
                    baseline_fields.push("phase_swap_out_delta_baseline".into());
                }
                if thread_accelerator.target.as_deref() == Some("GPU") {
                    if accelerator_metrics.0.is_none() {
                        missing_fields.push("accelerator_utilization_percent".into());
                    }
                    if accelerator_metrics.1.is_none() {
                        missing_fields.push("accelerator_memory_used_bytes".into());
                    }
                } else if thread_accelerator.status == "unavailable" {
                    missing_fields.push("accelerator_target".into());
                }
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
                    phase_sequence,
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
                    process_cpu_time_scope: "cumulative-root-process-tree-observed-pids",
                    cpu_utilization_percent,
                    cpu_utilization_scope: "root-process-tree-over-host-logical-cpus-within-phase",
                    rss_bytes: process.as_ref().map(|value| value.rss_bytes),
                    rss_scope: "sum-of-current-rss-for-visible-process-tree",
                    os_high_water_rss_bytes: process.as_ref().map(|value| value.hwm_bytes),
                    os_high_water_scope: "sampled-sum-of-per-process-lifetime-vmhwm",
                    host_swap_in_counter: host.swap_in_counter,
                    host_swap_out_counter: host.swap_out_counter,
                    host_swap_counter_unit: "pages",
                    host_swap_in_delta,
                    host_swap_out_delta,
                    host_swap_in_phase_delta,
                    host_swap_out_phase_delta,
                    host_swap_total_bytes: host.swap_total_bytes,
                    host_swap_free_bytes: host.swap_free_bytes,
                    host_swap_used_bytes: host
                        .swap_total_bytes
                        .zip(host.swap_free_bytes)
                        .map(|(total, free)| total.saturating_sub(free)),
                    host_observation_scope: "host-wide-proc-vmstat-and-meminfo",
                    host_mem_available_bytes: host.mem_available_bytes,
                    host_mem_available_scope: "host-wide-proc-meminfo-MemAvailable",
                    filesystem_used_bytes,
                    filesystem_used_scope: "host filesystem used bytes from df -Pk",
                    filesystem_used_bytes_sample_age_ms: filesystem_sample_age_ms,
                    filesystem_used_bytes_is_stale: filesystem_sample_age_ms
                        .is_some_and(|age| age > 0),
                    filesystem_used_bytes_sampled_this_row: filesystem_sampled_this_row,
                    data_dir_bytes,
                    data_dir_bytes_scope: "sum of file metadata lengths under data directory; symlinks excluded",
                    data_dir_bytes_sample_age_ms: data_dir_sample_age_ms,
                    data_dir_bytes_is_stale: data_dir_sample_age_ms.is_some_and(|age| age > 0),
                    data_dir_bytes_sampled_this_row: data_dir_sampled_this_row,
                    data_dir_growth_bytes,
                    data_dir_growth_scope: "difference from prior sample; carried values can yield zero between one-second probes",
                    process_cpu_time_delta_ms,
                    baseline_fields,
                    missing_fields,
                    status: sample_status,
                    accelerator: AcceleratorSample {
                        accelerator_requested_provider: thread_accelerator
                            .requested_provider
                            .clone(),
                        accelerator_target: thread_accelerator.target.clone(),
                        accelerator_telemetry_provider: thread_accelerator
                            .telemetry_provider
                            .clone(),
                        accelerator_device: thread_accelerator.device.clone(),
                        accelerator_status: thread_accelerator.status,
                        accelerator_reason: thread_accelerator.reason.clone(),
                        accelerator_utilization_percent: accelerator_metrics.0,
                        accelerator_memory_used_bytes: accelerator_metrics.1,
                        accelerator_metrics_scope: "host-wide-drm-device-not-process-attributed",
                    },
                };
                if process.is_some() {
                    previous_process_cpu_time_ms = process_cpu_time_ms;
                }
                if let Ok(mut values) = thread_samples.lock() {
                    if let Some(process) = process.as_ref() {
                        previous = Some((sampled_at, process.cpu_ticks_by_pid.clone()));
                    }
                    values.push(sample.clone());
                }
                append_resource_jsonl(&mut sample_file, &sample, "resource sample")?;
                thread::sleep(Duration::from_millis(interval_ms));
            }
            Ok(())
        });
        let sampler = Self {
            stop,
            phase,
            boundaries,
            samples,
            boundary_file: Mutex::new(boundary_file),
            join: Some(join),
            output_dir: output_dir.to_owned(),
            data_dir: data_dir.to_owned(),
            run_id: run_id.into(),
            root_pid,
            interval_ms,
            logical_cpus,
            ticks_per_second,
            accelerator,
            runlens_metadata: runlens_metadata.map(Path::to_owned),
            started: Instant::now(),
        };
        sampler.write_config()?;
        Ok(sampler)
    }

    pub(super) fn set_phase(&self, name: &str) -> Result<(), Error> {
        let mut phase = self.phase.lock().expect("resource sampler phase lock");
        if phase.as_ref().is_some_and(|(current, _)| current == name) {
            return Ok(());
        }
        let sequence = self
            .boundaries
            .lock()
            .expect("resource sampler boundary lock")
            .last()
            .map_or(0, |boundary| boundary.sequence + 1);
        if let Some((current, current_sequence)) = phase.take() {
            let boundary = ResourceBoundary {
                schema_version: "scout.resource-boundary.v1",
                timestamp_utc: resource_timestamp(),
                elapsed_ms: self.started.elapsed().as_millis() as u64,
                benchmark_run_id: self.run_id.clone(),
                kind: "end",
                phase: current,
                sequence: current_sequence,
                status: None,
            };
            let mut file = self
                .boundary_file
                .lock()
                .map_err(|_| Error::storage("resource boundary file lock poisoned"))?;
            append_resource_jsonl(&mut file, &boundary, "resource boundary")?;
            self.boundaries
                .lock()
                .expect("resource sampler boundary lock")
                .push(boundary);
        }
        let boundary = ResourceBoundary {
            schema_version: "scout.resource-boundary.v1",
            timestamp_utc: resource_timestamp(),
            elapsed_ms: self.started.elapsed().as_millis() as u64,
            benchmark_run_id: self.run_id.clone(),
            kind: "start",
            phase: name.into(),
            sequence,
            status: None,
        };
        let mut file = self
            .boundary_file
            .lock()
            .map_err(|_| Error::storage("resource boundary file lock poisoned"))?;
        append_resource_jsonl(&mut file, &boundary, "resource boundary")?;
        self.boundaries
            .lock()
            .expect("resource sampler boundary lock")
            .push(boundary);
        *phase = Some((name.into(), sequence));
        Ok(())
    }

    pub(super) fn end_phase(&self, status: &str) -> Result<(), Error> {
        let mut phase = self.phase.lock().expect("resource sampler phase lock");
        if let Some((current, sequence)) = phase.take() {
            let boundary = ResourceBoundary {
                schema_version: "scout.resource-boundary.v1",
                timestamp_utc: resource_timestamp(),
                elapsed_ms: self.started.elapsed().as_millis() as u64,
                benchmark_run_id: self.run_id.clone(),
                kind: "end",
                phase: current,
                sequence,
                status: Some(status.into()),
            };
            let mut file = self
                .boundary_file
                .lock()
                .map_err(|_| Error::storage("resource boundary file lock poisoned"))?;
            append_resource_jsonl(&mut file, &boundary, "resource boundary")?;
            self.boundaries
                .lock()
                .expect("resource sampler boundary lock")
                .push(boundary);
        }
        Ok(())
    }

    fn write_config(&self) -> Result<(), Error> {
        write_json(
            &self.output_dir.join("resource-config.json"),
            &json!({
                "schema_version": "scout.resource-config.v1",
                "benchmark_run_id": self.run_id,
                "root_pid": self.root_pid,
                "sampling_interval_ms": self.interval_ms,
                "disk_metrics_sample_interval_ms": 1000,
                "disk_metrics_stale_after_ms": 2000,
                "disk_metrics_sample_policy": "last successful reading carried forward; sample age in each row marks stale values",
                "logical_cpus": self.logical_cpus,
                "cpu_ticks_per_second": self.ticks_per_second,
                "data_dir": self.data_dir,
                "monitoring_commands": [
                    "/proc/<pid>/stat (utime/stime)",
                    "/proc/<pid>/status (VmRSS/VmHWM)",
                    "/proc/<pid>/task/<pid>/children",
                    "/proc/vmstat (pswpin/pswpout)",
                    "/proc/meminfo (MemAvailable/SwapTotal/SwapFree)",
                    "df -Pk <data_dir> (at most once per second)",
                    "filesystem metadata-length walk <data_dir> (at most once per second; file contents not read)",
                    "/sys/class/drm/card*/device (GPU identity/utilization/VRAM where exposed)",
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
                    "host_memory_available": "bytes",
                    "accelerator_memory": "bytes",
                },
                "scope": {
                    "process": "root process and descendants observed at each sample",
                    "rss": "sampled current RSS plus operating-system VmHWM",
                    "swap": "host-wide counters and usage",
                    "disk": "host filesystem used bytes and data-directory file metadata lengths; values are carried forward between one-second probes",
                    "host_memory_available": "host-wide /proc/meminfo MemAvailable",
                    "accelerator": "host-wide DRM device; utilization and memory are not attributed to this process or proof of OpenVINO device selection",
                },
                "accelerator": {
                    "requested_provider": self.accelerator.requested_provider,
                    "target": self.accelerator.target,
                    "telemetry_provider": self.accelerator.telemetry_provider,
                    "device": self.accelerator.device,
                    "status": self.accelerator.status,
                    "reason": self.accelerator.reason,
                },
                "runlens_metadata_path": self.runlens_metadata,
            }),
        )?;
        Ok(())
    }

    pub(super) fn finish(mut self, status: &str) -> Result<ResourceSummary, Error> {
        self.stop.store(true, AtomicOrdering::Relaxed);
        if let Some(join) = self.join.take() {
            join.join()
                .map_err(|_| Error::storage("resource sampler panicked"))??;
        }
        self.end_phase(status)?;
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
        self.write_artifacts(&samples, &boundaries, status, &self.accelerator)?;
        Ok(resource_summary(ResourceSummaryInput {
            run_id: &self.run_id,
            root_pid: self.root_pid,
            interval_ms: self.interval_ms,
            samples: &samples,
            boundaries: &boundaries,
            status,
            accelerator: &self.accelerator,
            runlens_metadata: self.runlens_metadata.as_deref(),
        }))
    }

    fn write_artifacts(
        &self,
        samples: &[ResourceSample],
        boundaries: &[ResourceBoundary],
        status: &str,
        accelerator: &AcceleratorCapability,
    ) -> Result<(), Error> {
        let summary = resource_summary(ResourceSummaryInput {
            run_id: &self.run_id,
            root_pid: self.root_pid,
            interval_ms: self.interval_ms,
            samples,
            boundaries,
            status,
            accelerator,
            runlens_metadata: self.runlens_metadata.as_deref(),
        });
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
        let _ = self.end_phase("aborted");
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
        let _ = self.write_artifacts(&samples, &boundaries, "aborted", &self.accelerator);
    }
}

fn resource_timestamp() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn append_resource_jsonl<T: serde::Serialize>(
    file: &mut fs::File,
    value: &T,
    label: &str,
) -> Result<(), Error> {
    let bytes =
        serde_json::to_vec(value).map_err(|e| Error::storage(format!("serialize {label}: {e}")))?;
    file.write_all(&bytes)
        .and_then(|_| file.write_all(b"\n"))
        .and_then(|_| file.flush())
        .map_err(|e| Error::storage(format!("append {label}: {e}")))
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

fn read_host_memory() -> HostMemorySnapshot {
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
    HostMemorySnapshot {
        swap_in_counter: counter("pswpin"),
        swap_out_counter: counter("pswpout"),
        swap_total_bytes: memory("SwapTotal").map(|value| value.saturating_mul(1024)),
        swap_free_bytes: memory("SwapFree").map(|value| value.saturating_mul(1024)),
        mem_available_bytes: memory("MemAvailable").map(|value| value.saturating_mul(1024)),
    }
}

fn directory_metadata_bytes(path: &Path) -> Option<u64> {
    let mut pending = vec![path.to_owned()];
    let mut bytes = 0_u64;
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory).ok()? {
            let entry = entry.ok()?;
            let file_type = entry.file_type().ok()?;
            if file_type.is_dir() {
                pending.push(entry.path());
            } else if file_type.is_file() {
                bytes = bytes.saturating_add(entry.metadata().ok()?.len());
            }
        }
    }
    Some(bytes)
}

fn probe_accelerator(requested_provider: Option<&str>) -> AcceleratorCapability {
    let requested_provider = requested_provider.map(str::to_owned);
    let target = match requested_provider.as_deref() {
        Some("cpu") => Some("CPU".into()),
        Some("openvino_gpu") => Some("GPU".into()),
        _ => None,
    };
    if target.as_deref() != Some("GPU") {
        let (status, reason) = match requested_provider.as_deref() {
            Some("cpu") => (
                "not_requested",
                "Generation build profile selects CPU execution".into(),
            ),
            Some(provider) => (
                "unavailable",
                format!("No accelerator probe is configured for provider {provider}"),
            ),
            None => (
                "unavailable",
                "Generation build profile does not record an execution provider".into(),
            ),
        };
        return AcceleratorCapability {
            requested_provider,
            target,
            telemetry_provider: None,
            device: None,
            status,
            reason: Some(reason),
            device_path: None,
        };
    }

    let devices = fs::read_dir("/sys/class/drm").ok();
    if let Some(devices) = devices {
        for entry in devices.flatten() {
            let card = entry.file_name().to_string_lossy().into_owned();
            if !card.strip_prefix("card").is_some_and(|suffix| {
                !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit())
            }) {
                continue;
            }
            let device_path = entry.path().join("device");
            let vendor_id = fs::read_to_string(device_path.join("vendor"))
                .ok()
                .and_then(|value| {
                    u16::from_str_radix(value.trim().trim_start_matches("0x"), 16).ok()
                });
            let utilization_exposed = device_path.join("gpu_busy_percent").is_file();
            let memory_exposed = device_path.join("mem_info_vram_used").is_file();
            let recognized_vendor = matches!(vendor_id, Some(0x8086 | 0x1002 | 0x10de));
            if !recognized_vendor && !utilization_exposed && !memory_exposed {
                continue;
            }
            let product = fs::read_to_string(device_path.join("product_name"))
                .ok()
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty());
            let product = product.or_else(|| {
                fs::read_to_string(device_path.join("device"))
                    .ok()
                    .map(|value| format!("PCI device {}", value.trim()))
            });
            let driver = fs::read_link(device_path.join("driver"))
                .ok()
                .and_then(|path| {
                    path.file_name()
                        .map(|name| name.to_string_lossy().into_owned())
                });
            let device = Some(
                [driver, product, Some(card)]
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>()
                    .join(" "),
            );
            let missing = [
                (!utilization_exposed).then_some("utilization"),
                (!memory_exposed).then_some("VRAM usage"),
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
            return AcceleratorCapability {
                requested_provider,
                target,
                telemetry_provider: Some("linux-drm-sysfs".into()),
                device,
                status: match missing.len() {
                    0 => "available",
                    1 => "partial",
                    _ => "unavailable",
                },
                reason: (!missing.is_empty()).then(|| {
                    format!(
                        "DRM device detected; {} not exposed by this device",
                        missing.join(" and ")
                    )
                }),
                device_path: Some(device_path),
            };
        }
    }
    let reason = if Path::new("/dev/dxg").exists() {
        "WSL /dev/dxg is present, but no DRM GPU device exposes supported utilization or VRAM metrics"
    } else {
        "No supported DRM GPU device or utilization/VRAM metrics exposed under /sys/class/drm"
    };
    AcceleratorCapability {
        requested_provider,
        target,
        telemetry_provider: None,
        device: None,
        status: "unavailable",
        reason: Some(reason.into()),
        device_path: None,
    }
}

fn read_accelerator_metrics(capability: &AcceleratorCapability) -> (Option<f64>, Option<u64>) {
    let Some(device_path) = capability.device_path.as_ref() else {
        return (None, None);
    };
    let utilization = fs::read_to_string(device_path.join("gpu_busy_percent"))
        .ok()
        .and_then(|value| value.trim().parse::<f64>().ok())
        .filter(|value| value.is_finite() && (0.0..=100.0).contains(value));
    let memory_used = fs::read_to_string(device_path.join("mem_info_vram_used"))
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok());
    (utilization, memory_used)
}

fn read_filesystem_used_bytes(path: &Path) -> Option<u64> {
    let output = std::process::Command::new("df")
        .args(["-Pk", &path.display().to_string()])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let output_text = String::from_utf8_lossy(&output.stdout);
    let line = output_text.lines().rfind(|line| !line.trim().is_empty())?;
    line.split_whitespace()
        .nth(2)
        .and_then(|value| value.parse::<u64>().ok())
        .map(|value| value.saturating_mul(1024))
}

fn resource_summary(input: ResourceSummaryInput<'_>) -> ResourceSummary {
    let ResourceSummaryInput {
        run_id,
        root_pid,
        interval_ms,
        samples,
        boundaries,
        status,
        accelerator,
        runlens_metadata,
    } = input;
    let measured = samples
        .iter()
        .filter_map(|sample| sample.cpu_utilization_percent)
        .collect::<Vec<_>>();
    let peak_rss = samples.iter().filter_map(|sample| sample.rss_bytes).max();
    let peak_hwm = samples
        .iter()
        .filter_map(|sample| sample.os_high_water_rss_bytes)
        .max();
    let min_host_mem_available = samples
        .iter()
        .filter_map(|sample| sample.host_mem_available_bytes)
        .min();
    let accelerator_utilization = samples
        .iter()
        .filter_map(|sample| sample.accelerator.accelerator_utilization_percent)
        .collect::<Vec<_>>();
    let accelerator_mean_utilization = (!accelerator_utilization.is_empty()).then(|| {
        accelerator_utilization.iter().sum::<f64>() / accelerator_utilization.len() as f64
    });
    let accelerator_peak_memory = samples
        .iter()
        .filter_map(|sample| sample.accelerator.accelerator_memory_used_bytes)
        .max();
    let first = samples.first();
    let last = samples.last();
    let disk_observations = samples
        .iter()
        .filter(|sample| sample.data_dir_bytes_sampled_this_row)
        .collect::<Vec<_>>();
    let disk_growth_bytes = (disk_observations.len() > 1)
        .then(|| {
            disk_observations
                .first()
                .and_then(|sample| sample.data_dir_bytes)
                .zip(
                    disk_observations
                        .last()
                        .and_then(|sample| sample.data_dir_bytes),
                )
                .map(|(first, last)| last as i64 - first as i64)
        })
        .flatten();
    let missing_fields = samples
        .iter()
        .flat_map(|sample| sample.missing_fields.iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let baseline_fields = samples
        .iter()
        .flat_map(|sample| sample.baseline_fields.iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let evidence_status = if samples.is_empty()
        || samples
            .iter()
            .any(|sample| !sample.missing_fields.is_empty())
    {
        "partial"
    } else {
        "complete"
    };
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
        let accelerator_cpu = phase_samples
            .iter()
            .filter_map(|sample| sample.accelerator.accelerator_utilization_percent)
            .collect::<Vec<_>>();
        let phase_disk_observations = phase_samples
            .iter()
            .filter(|sample| sample.data_dir_bytes_sampled_this_row)
            .collect::<Vec<_>>();
        by_phase.insert(
            phase,
            ResourcePhaseSummary {
                sample_count: phase_samples.len(),
                measured_cpu_samples: cpu.len(),
                mean_cpu_utilization_percent: (!cpu.is_empty())
                    .then(|| cpu.iter().sum::<f64>() / cpu.len() as f64),
                phase_cpu_time_delta_ms: phase_samples
                    .iter()
                    .filter_map(|sample| sample.process_cpu_time_delta_ms)
                    .reduce(|total, value| total + value),
                peak_rss_bytes_sampled: phase_samples
                    .iter()
                    .filter_map(|sample| sample.rss_bytes)
                    .max(),
                peak_rss_bytes_os_high_water: phase_samples
                    .iter()
                    .filter_map(|sample| sample.os_high_water_rss_bytes)
                    .max(),
                min_host_mem_available_bytes: phase_samples
                    .iter()
                    .filter_map(|sample| sample.host_mem_available_bytes)
                    .min(),
                mean_accelerator_utilization_percent: (!accelerator_cpu.is_empty())
                    .then(|| accelerator_cpu.iter().sum::<f64>() / accelerator_cpu.len() as f64),
                peak_accelerator_memory_used_bytes: phase_samples
                    .iter()
                    .filter_map(|sample| sample.accelerator.accelerator_memory_used_bytes)
                    .max(),
                host_swap_in_delta_pages: phase_samples
                    .iter()
                    .filter_map(|sample| sample.host_swap_in_phase_delta)
                    .reduce(|total, value| total.saturating_add(value)),
                host_swap_out_delta_pages: phase_samples
                    .iter()
                    .filter_map(|sample| sample.host_swap_out_phase_delta)
                    .reduce(|total, value| total.saturating_add(value)),
                host_swap_delta_scope: "sum of host-wide /proc/vmstat page-counter deltas within phase sequence; first sample establishes baseline",
                missing_sample_count: phase_samples
                    .iter()
                    .filter(|sample| sample.status != "complete")
                    .count(),
                disk_growth_bytes: (phase_disk_observations.len() > 1)
                    .then(|| {
                        phase_disk_observations
                            .first()
                            .and_then(|sample| sample.data_dir_bytes)
                            .zip(
                                phase_disk_observations
                                    .last()
                                    .and_then(|sample| sample.data_dir_bytes),
                            )
                            .map(|(first, last)| last as i64 - first as i64)
                    })
                    .flatten(),
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
        run_status: status.into(),
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
        process_tree_coverage: "root process plus descendants visible at each /proc sample; short-lived children may be absent; exited observed pids remain in cumulative CPU accounting".into(),
        peak_rss_bytes_sampled: peak_rss,
        peak_rss_scope: "sampled-process-tree-current-rss",
        peak_rss_bytes_os_high_water: peak_hwm,
        os_high_water_rss_scope: "maximum sampled sum of per-process lifetime /proc/status VmHWM for visible process tree",
        mean_cpu_utilization_percent: (!measured.is_empty())
            .then(|| measured.iter().sum::<f64>() / measured.len() as f64),
        cumulative_cpu_time_ms: last.and_then(|sample| sample.process_cpu_time_ms),
        cumulative_cpu_time_scope: "sum of maximum utime/stime ticks for each observed process id since sampler start",
        phase_cpu_time_scope: "sum of observed CPU-time deltas between samples within each phase; first sample after each phase boundary establishes baseline",
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
        host_swap_usage_scope: "host-wide SwapTotal minus SwapFree from /proc/meminfo; bytes",
        host_swap_delta_scope: "counter delta between first and last resource samples; excludes activity before first sample",
        min_host_mem_available_bytes: min_host_mem_available,
        host_mem_available_scope: "minimum host-wide /proc/meminfo MemAvailable; bytes",
        disk_growth_bytes,
        disk_growth_scope: "metadata file-length delta between first and last successful observations in scope; no file contents read; probes refresh at most once per second, values carry forward with sample ages, stale values beyond two seconds are marked missing",
        missing_sample_count: samples
            .iter()
            .filter(|sample| sample.status != "complete")
            .count(),
        evidence_status,
        baseline_fields,
        missing_fields,
        by_phase,
        phase_boundaries: boundaries.to_vec(),
        accelerator: AcceleratorSummary {
            requested_provider: accelerator.requested_provider.clone(),
            target: accelerator.target.clone(),
            telemetry_provider: accelerator.telemetry_provider.clone(),
            device: accelerator.device.clone(),
            status: accelerator.status,
            reason: accelerator.reason.clone(),
            mean_utilization_percent: accelerator_mean_utilization,
            peak_memory_used_bytes: accelerator_peak_memory,
            metrics_scope: "host-wide DRM device; not attributed to benchmark process or proof of requested execution device",
        },
        runlens_metadata_path: runlens_metadata.map(|path| path.display().to_string()),
        runlens_metadata_sha256,
        runlens_trace_id,
    }
}
