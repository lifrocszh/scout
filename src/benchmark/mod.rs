use serde::{Deserialize, Serialize};

const BENCHMARK_SCHEMA: &str = "scout.benchmark.v1";
const BENCHMARK_DEFAULT_REPETITIONS: usize = 3;
const BENCHMARK_DEFAULT_COLD_OPEN_RUNS: usize = 5;
const BENCHMARK_DEFAULT_WARMUP_PASSES: usize = 2;
const BENCHMARK_DEFAULT_MEASURED_PASSES: usize = 10;
const BENCHMARK_DEFAULT_CONCURRENCY: [usize; 4] = [1, 4, 8, 16];
const BENCHMARK_DEFAULT_INDEX_WORKERS: [usize; 4] = [1, 4, 8, 18];
const BENCHMARK_INSTABILITY_LIMIT: f64 = 0.15;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct BenchmarkReferenceProfile {
    #[serde(default)]
    pub(crate) hardware: Option<String>,
    #[serde(default)]
    pub(crate) cpu_model: Option<String>,
    #[serde(default, alias = "cpu_cache_sizes")]
    pub(crate) cpu_cache: Option<String>,
    #[serde(default)]
    pub(crate) logical_cpus: Option<usize>,
    #[serde(default)]
    pub(crate) ram_bytes: Option<u64>,
    #[serde(default)]
    pub(crate) swap_bytes: Option<u64>,
    #[serde(default)]
    pub(crate) os: Option<String>,
    #[serde(default)]
    pub(crate) kernel: Option<String>,
    #[serde(default)]
    pub(crate) rust_toolchain: Option<String>,
    #[serde(default)]
    pub(crate) lockfile_sha256: Option<String>,
    #[serde(default)]
    pub(crate) filesystem: Option<String>,
    #[serde(default)]
    pub(crate) free_disk_bytes: Option<u64>,
    #[serde(default)]
    pub(crate) background_load: Option<String>,
    #[serde(default)]
    pub(crate) source_revision: Option<String>,
    #[serde(default)]
    pub(crate) model_name: Option<String>,
    #[serde(default)]
    pub(crate) model_revision: Option<String>,
    #[serde(default)]
    pub(crate) tokenizer_sha256: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct RawBenchmarkConfig {
    pub(crate) schema_version: Option<String>,
    pub(crate) benchmark_id: Option<String>,
    pub(crate) reference_profile: Option<String>,
    #[serde(default)]
    pub(crate) reference: Option<BenchmarkReferenceProfile>,
    pub(crate) corpus_snapshot_id: Option<String>,
    pub(crate) generation_id: Option<String>,
    pub(crate) build_id: Option<String>,
    pub(crate) evaluation_id: Option<String>,
    pub(crate) evaluation_package: Option<PathBuf>,
    pub(crate) seed: Option<u64>,
    pub(crate) query_order: Option<String>,
    pub(crate) cache_state: Option<String>,
    pub(crate) repetitions: Option<usize>,
    pub(crate) fresh_staging_runs: Option<usize>,
    pub(crate) cold_open_runs: Option<usize>,
    pub(crate) warmup_passes: Option<usize>,
    pub(crate) measured_passes: Option<usize>,
    #[serde(alias = "search_concurrency")]
    pub(crate) concurrency: Option<Vec<usize>>,
    pub(crate) index_workers: Option<Vec<usize>>,
    #[serde(alias = "replay_crawl")]
    pub(crate) replay_crawl_dir: Option<PathBuf>,
    pub(crate) runlens_metadata: Option<PathBuf>,
    #[serde(alias = "sampling_interval_ms")]
    pub(crate) resource_sampling_interval_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct BenchmarkConfig {
    pub(crate) schema_version: String,
    pub(crate) benchmark_id: String,
    pub(crate) reference_profile: String,
    pub(crate) reference: BenchmarkReferenceProfile,
    pub(crate) corpus_snapshot_id: String,
    pub(crate) generation_id: String,
    pub(crate) build_id: String,
    pub(crate) evaluation_id: String,
    pub(crate) evaluation_package: PathBuf,
    pub(crate) seed: u64,
    pub(crate) query_order: String,
    pub(crate) cache_state: String,
    pub(crate) repetitions: usize,
    pub(crate) fresh_staging_runs: usize,
    pub(crate) cold_open_runs: usize,
    pub(crate) warmup_passes: usize,
    pub(crate) measured_passes: usize,
    pub(crate) concurrency: Vec<usize>,
    pub(crate) index_workers: Vec<usize>,
    pub(crate) replay_crawl_dir: Option<PathBuf>,
    pub(crate) runlens_metadata: Option<PathBuf>,
    pub(crate) resource_sampling_interval_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct BenchmarkPlan {
    pub(crate) schema_version: &'static str,
    pub(crate) status: &'static str,
    pub(crate) run_id: String,
    pub(crate) benchmark_id: String,
    pub(crate) config_sha256: String,
    pub(crate) reference_profile: String,
    pub(crate) reference: BenchmarkReferenceProfile,
    pub(crate) corpus_snapshot_id: String,
    pub(crate) corpus_snapshot_sha256: String,
    pub(crate) generation_id: String,
    pub(crate) generation_manifest_sha256: String,
    pub(crate) build_id: String,
    pub(crate) build_profile_sha256: String,
    pub(crate) evaluation_id: String,
    pub(crate) evaluation_package: String,
    pub(crate) evaluation_package_sha256: String,
    pub(crate) evaluation_metadata_sha256: Option<String>,
    pub(crate) evaluation_queries_sha256: String,
    pub(crate) evaluation_judgments_sha256: Option<String>,
    pub(crate) query_count: usize,
    pub(crate) query_ids: Vec<String>,
    pub(crate) seed: u64,
    pub(crate) query_order: String,
    pub(crate) cache_state: String,
    pub(crate) repetitions: usize,
    pub(crate) fresh_staging_runs: usize,
    pub(crate) cold_open_runs: usize,
    pub(crate) warmup_passes: usize,
    pub(crate) measured_passes: usize,
    pub(crate) concurrency: Vec<usize>,
    pub(crate) index_workers: Vec<usize>,
    pub(crate) replay_crawl: Value,
    pub(crate) runlens_metadata: Option<String>,
    pub(crate) resource_sampling_interval_ms: u64,
    pub(crate) performance_thresholds: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct BenchmarkDistribution {
    pub(crate) sample_count: usize,
    pub(crate) min_ms: f64,
    pub(crate) p50_ms: f64,
    pub(crate) p95_ms: f64,
    pub(crate) p99_ms: f64,
    pub(crate) max_ms: f64,
    pub(crate) median_spread: f64,
    pub(crate) unstable: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct BenchmarkSample {
    pub(crate) phase: String,
    pub(crate) run: usize,
    pub(crate) pass: Option<usize>,
    pub(crate) mode: Option<String>,
    pub(crate) concurrency: Option<usize>,
    pub(crate) worker_count: Option<usize>,
    pub(crate) cache_state: String,
    pub(crate) duration_ms: f64,
    pub(crate) query_count: usize,
    pub(crate) qps: Option<f64>,
    pub(crate) latency_ms: Option<BenchmarkDistribution>,
    pub(crate) integrity_verified: bool,
    pub(crate) artifact_path: Option<String>,
    pub(crate) status: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct BenchmarkSystemEvidence {
    pub(crate) benchmark_run_id: String,
    pub(crate) process_id: u32,
    pub(crate) swap_in_before: Option<u64>,
    pub(crate) swap_in_after: Option<u64>,
    pub(crate) swap_out_before: Option<u64>,
    pub(crate) swap_out_after: Option<u64>,
    pub(crate) swap_activity_detected: bool,
    pub(crate) oom_observed: Option<bool>,
    pub(crate) partial_artifacts: bool,
    pub(crate) integrity_verified: bool,
    pub(crate) page_cache_state: String,
    pub(crate) runlens_metadata_path: Option<String>,
    pub(crate) runlens_metadata_sha256: Option<String>,
    pub(crate) runlens_metadata: Value,
    pub(crate) resource_samples_path: String,
    pub(crate) resource_boundaries_path: String,
    pub(crate) resource_summary_path: String,
    pub(crate) resource_sampler: Value,
}

use self::resource::ResourceSampler;
use self::staging::{
    benchmark_fresh_crawl, benchmark_fresh_index, inspect_replay_crawl, validate_benchmark_snapshot,
};
use super::app::report_error;
use super::evaluation::{
    EvaluationQuery, corpus_source_id, load_evaluation_metadata, metadata_string,
    validate_benchmark_queries,
};
use super::generation::{
    ActiveGeneration, GenerationManifest, IndexGeneration, SearchMode, directory_bytes,
    directory_digest, validate_generation,
};
use super::observability::Events;
use super::storage::{jsonl, new_run_id, parse_jsonl, sha256_hex, write_if_absent, write_json};
use crate::cli::{Cli, Operation};
use crate::error::Error;
use serde_json::{Value, json};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

mod resource;
mod staging;

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
    let generation = IndexGeneration::at(data_dir).open_active()?;
    let generation_info = generation.info();
    if config.generation_id != generation_info.generation_id {
        return Err(Error::invalid(
            "benchmark generation_id does not match CURRENT",
        ));
    }
    if config.corpus_snapshot_id != generation_info.corpus_snapshot_id {
        return Err(Error::invalid(
            "benchmark corpus_snapshot_id does not match active Generation",
        ));
    }
    let (snapshot_dir, snapshot) =
        validate_benchmark_snapshot(data_dir, &config.corpus_snapshot_id)?;
    let generation_path = data_dir
        .join("generations")
        .join(&generation_info.generation_id);
    let generation_manifest_bytes = fs::read(generation_path.join("generation-manifest.json"))
        .map_err(|e| Error::invalid(format!("read benchmark Generation manifest: {e}")))?;
    let generation_manifest: GenerationManifest =
        serde_json::from_slice(&generation_manifest_bytes)
            .map_err(|e| Error::invalid(format!("parse benchmark Generation manifest: {e}")))?;
    let build_profile_bytes = fs::read(generation_path.join("build-profile.json"))
        .map_err(|e| Error::invalid(format!("read benchmark build profile: {e}")))?;
    let build_profile_sha256 = sha256_hex(&build_profile_bytes);
    if build_profile_sha256 != generation_manifest.build_config_sha256 {
        return Err(Error::invalid("benchmark build profile checksum mismatch"));
    }
    let build_profile: Value = serde_json::from_slice(&build_profile_bytes)
        .map_err(|e| Error::invalid(format!("parse benchmark build profile: {e}")))?;
    let requested_accelerator_provider = build_profile
        .get("embedding_execution_provider")
        .and_then(Value::as_str)
        .map(str::to_owned);
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
        requested_accelerator_provider.as_deref(),
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
        generation_id: generation_info.generation_id.clone(),
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
        .join(&generation_info.generation_id);
    let replay_valid = replay_crawl["valid"].as_bool().unwrap_or(false);
    if let Some(replay) = config.replay_crawl_dir.as_ref() {
        resource_sampler.set_phase("crawl-replay")?;
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
        })?;
    }
    for run in 0..config.fresh_staging_runs {
        let run_number = run + 1;
        resource_sampler.set_phase("staging-integrity")?;
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
        })?;
        resource_sampler.set_phase("crawl-fresh-staging")?;
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
        resource_sampler.end_phase(if crawl_ok { "complete" } else { "failed" })?;

        let chosen_worker = config.index_workers[run % config.index_workers.len()];
        for &worker_count in &config.index_workers {
            let index_phase = if worker_count == chosen_worker {
                "index-fresh-staging"
            } else {
                "index-worker"
            };
            resource_sampler.set_phase(index_phase)?;
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
            })?;
        }
    }
    let generation = Arc::new(generation);
    for run in 0..config.cold_open_runs {
        resource_sampler.set_phase("serving-cold-open")?;
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
        })?;
    }
    let mut measured_latencies_by_concurrency = BTreeMap::<usize, Vec<f64>>::new();
    for repetition in 0..config.repetitions {
        for &concurrency in &config.concurrency {
            for pass in 0..config.warmup_passes {
                resource_sampler.set_phase("serving-warmup")?;
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
                resource_sampler.end_phase("complete")?;
            }
            for pass in 0..config.measured_passes {
                resource_sampler.set_phase("serving-warm")?;
                let (duration_ms, latencies) =
                    benchmark_search_pass(generation.clone(), &queries, concurrency)?;
                measured_latencies_by_concurrency
                    .entry(concurrency)
                    .or_default()
                    .extend_from_slice(&latencies);
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
                resource_sampler.end_phase("complete")?;
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
    ] {
        let values = samples
            .iter()
            .filter(|sample| sample.phase == phase)
            .map(|sample| sample.duration_ms)
            .collect::<Vec<_>>();
        distributions.insert(phase, benchmark_distribution(&values));
    }
    let mut serving_by_concurrency = BTreeMap::new();
    let mut unstable_concurrency_levels = Vec::new();
    for &concurrency in &config.concurrency {
        let warmup = samples
            .iter()
            .filter(|sample| {
                sample.phase == "serving-warmup" && sample.concurrency == Some(concurrency)
            })
            .collect::<Vec<_>>();
        let measured = samples
            .iter()
            .filter(|sample| {
                sample.phase == "serving-warm" && sample.concurrency == Some(concurrency)
            })
            .collect::<Vec<_>>();
        let durations = measured
            .iter()
            .map(|sample| sample.duration_ms)
            .collect::<Vec<_>>();
        let qps = measured
            .iter()
            .filter_map(|sample| sample.qps)
            .collect::<Vec<_>>();
        let duration_distribution = benchmark_distribution(&durations);
        let qps_distribution = benchmark_distribution(&qps);
        let latency_distribution = benchmark_distribution(
            measured_latencies_by_concurrency
                .get(&concurrency)
                .map(Vec::as_slice)
                .unwrap_or_default(),
        );
        if duration_distribution.unstable || qps_distribution.unstable {
            unstable_concurrency_levels.push(concurrency);
        }
        serving_by_concurrency.insert(
            concurrency.to_string(),
            json!({
                "warmup": {
                    "duration_ms": benchmark_distribution(
                        &warmup.iter().map(|sample| sample.duration_ms).collect::<Vec<_>>()
                    ),
                    "passes": warmup.iter().map(|sample| benchmark_pass_evidence(sample)).collect::<Vec<_>>(),
                },
                "measured": {
                    "duration_ms": duration_distribution,
                    "qps": qps_distribution,
                    "latency_ms": latency_distribution,
                    "passes": measured.iter().map(|sample| benchmark_pass_evidence(sample)).collect::<Vec<_>>(),
                },
            }),
        );
    }
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
    resource_sampler.set_phase("reporting")?;
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
        || !unstable_concurrency_levels.is_empty()
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
            "generation_id": generation_info.generation_id,
            "page_count": generation_info.page_count,
            "passage_count": generation_info.passage_count,
            "alias_count": generation_info.alias_count,
            "keyword_count": generation_info.keyword_count,
            "semantic_count": generation_info.semantic_count,
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
        "distribution": serving_by_concurrency,
        "distributions": distributions,
        "phase_timings": distributions,
        "serving_by_concurrency": serving_by_concurrency,
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
            "run_status": resource_summary.run_status,
            "evidence_status": resource_summary.evidence_status,
        },
        "flags": {
            "swap_activity_detected": swap_activity_detected,
            "oom_observed": oom_observed.unwrap_or(false),
            "partial_artifacts": partial_artifacts,
            "integrity_verified": integrity_verified,
            "unstable_phases": unstable_phases,
            "unstable_concurrency_levels": unstable_concurrency_levels,
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
    if cold_open_runs == 0
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
    generation: Arc<ActiveGeneration>,
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
                if let Err(error) = generation.ranked_page_ids(
                    SearchMode::Hybrid,
                    &query.query,
                    corpus_source_id(Some(&query.source)),
                ) {
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

fn benchmark_pass_evidence(sample: &BenchmarkSample) -> Value {
    json!({
        "run": sample.run,
        "pass": sample.pass,
        "query_count": sample.query_count,
        "duration_ms": sample.duration_ms,
        "qps": sample.qps,
        "latency_ms": sample.latency_ms.as_ref(),
    })
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
