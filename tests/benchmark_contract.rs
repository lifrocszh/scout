use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir(PathBuf);

impl TempDir {
    fn new(label: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let sequence = TEMP_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "scout-{label}-{}-{nonce}-{sequence}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create temporary directory");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn run_scout(data_dir: &Path, args: &[String]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_scout"))
        .args(args)
        .arg("--data-dir")
        .arg(data_dir)
        .output()
        .expect("run scout")
}

fn read_request(stream: &mut TcpStream) -> String {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set request timeout");
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        let count = stream.read(&mut chunk).expect("read request");
        if count == 0 {
            break;
        }
        bytes.extend_from_slice(&chunk[..count]);
        if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    String::from_utf8(bytes).expect("request is UTF-8")
}

fn write_response(stream: &mut TcpStream, content_type: &str, body: &[u8]) {
    write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .expect("write response headers");
    stream.write_all(body).expect("write response body");
}

fn fixture_server() -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind fixture server");
    let origin = format!("http://{}", listener.local_addr().expect("fixture address"));
    let handle = thread::spawn(move || {
        let page = br#"<!doctype html><html><head><title>Benchmark fixture</title></head><body><main><h1>Benchmark fixture</h1><p>Scout benchmark fixture provides stable server rendered documentation content for a reproducible local crawl and index generation. This body is deliberately long enough for page admission and remains unchanged across runs so benchmark artifacts can be compared without live network variance.</p></main></body></html>"#;
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().expect("accept fixture request");
            let request = read_request(&mut stream);
            if request.starts_with("GET /robots.txt ") {
                write_response(&mut stream, "text/plain", b"User-agent: *\nAllow: /\n");
            } else {
                assert!(request.starts_with("GET / "), "unexpected fixture request");
                write_response(&mut stream, "text/html; charset=utf-8", page);
            }
        }
    });
    (origin, handle)
}

fn only_child(path: &Path) -> PathBuf {
    let entries = fs::read_dir(path)
        .expect("read artifact directory")
        .map(|entry| entry.expect("read artifact entry").path())
        .filter(|entry| entry.is_dir())
        .collect::<Vec<_>>();
    assert_eq!(
        entries.len(),
        1,
        "expected one child under {}",
        path.display()
    );
    entries.into_iter().next().expect("artifact child")
}

fn write_index_profile(data_dir: &Path, snapshot_id: &str) {
    let config_dir = data_dir.join("config");
    fs::create_dir_all(&config_dir).expect("create index config directory");
    fs::write(
        config_dir.join("index.toml"),
        format!(
            r#"schema_version = "scout.index.v1"
snapshot_id = "{snapshot_id}"
extraction_schema = "scout.extraction.v1"
passage_schema = "scout.passage.v1"
max_passage_tokens = 384
overlap_tokens = 0
no_overlap = true
analyzer_id = "scout.unicode-token-v1"
title_boost = 2.0
heading_boost = 1.5
body_boost = 1.0
candidate_depth = 5
writer_memory_bytes = 16000000
worker_count = 1
model_name = "BAAI/bge-small-en-v1.5"
model_source = "local"
model_revision = "contract-revision"
model_license = "contract-license"
onnx_sha256 = "sha256:contract-onnx"
tokenizer_sha256 = "sha256:contract-tokenizer"
model_config_sha256 = "sha256:contract-config"
embedding_backend = "deterministic"
snippet_limit = 240
model_max_tokens = 512
pooling = "cls"
normalization = "l2"
quantization = "none"
query_instruction = ""
document_representation = "passage"
dimensions = 384
embedding_batch_size = 4
embedding_workers = 1
embedding_intra_op_threads = 1
metric = "cosine"
scalar_kind = "f32"
connectivity = 16
construction_expansion = 64
search_expansion = 32
key_map = "one-based-catalog-order-page-id-passage-id"
fusion_candidate_depth = 5
fusion_algorithm = "rrf"
rrf_k = 60
page_aggregation = "max"
snippet_window = 240
"#
        ),
    )
    .expect("write index profile");
}

fn prepare_generation() -> (TempDir, PathBuf, String, String) {
    let data_dir = TempDir::new("benchmark-contract");
    let (origin, server) = fixture_server();
    let source_config = data_dir.path().join("sources.toml");
    fs::write(
        &source_config,
        format!(
            r#"schema_version = "scout.sources.v1"
contact_url = "https://example.invalid/scout"
user_agent = "BenchmarkFixture/1.0"
page_target = 1
max_attempts = 1
retry_backoff_ms = 0

[[sources]]
source_id = "fixture"
seeds = ["{origin}/"]
allowed_origins = ["{origin}"]
path_prefixes = ["/"]
"#
        ),
    )
    .expect("write source config");
    let crawl = run_scout(
        data_dir.path(),
        &[
            "crawl".into(),
            "--config".into(),
            source_config.display().to_string(),
        ],
    );
    server.join().expect("join fixture server");
    assert!(
        crawl.status.success(),
        "crawl failed: {}",
        String::from_utf8_lossy(&crawl.stderr)
    );

    let crawl_dir = only_child(&data_dir.path().join("crawls"));
    let crawl_manifest: Value = serde_json::from_slice(
        &fs::read(crawl_dir.join("crawl-manifest.json")).expect("read crawl manifest"),
    )
    .expect("parse crawl manifest");
    let snapshot_id = crawl_manifest["snapshot_id"]
        .as_str()
        .expect("snapshot ID")
        .to_string();
    write_index_profile(data_dir.path(), &snapshot_id);
    let build = run_scout(
        data_dir.path(),
        &[
            "index".into(),
            "build".into(),
            "--corpus".into(),
            snapshot_id.clone(),
        ],
    );
    assert!(
        build.status.success(),
        "index build failed: {}",
        String::from_utf8_lossy(&build.stderr)
    );
    let staging = only_child(&data_dir.path().join("generations/.staging"));
    let generation_manifest: Value = serde_json::from_slice(
        &fs::read(staging.join("generation-manifest.json")).expect("read generation manifest"),
    )
    .expect("parse generation manifest");
    let generation_id = generation_manifest["generation_id"]
        .as_str()
        .expect("generation ID")
        .to_string();
    let activate = run_scout(
        data_dir.path(),
        &[
            "index".into(),
            "activate".into(),
            "--generation".into(),
            generation_id.clone(),
        ],
    );
    assert!(
        activate.status.success(),
        "index activation failed: {}",
        String::from_utf8_lossy(&activate.stderr)
    );
    (data_dir, crawl_dir, generation_id, snapshot_id)
}

fn write_queries(root: &Path) -> PathBuf {
    let package = root.join("benchmark-evaluation");
    fs::create_dir_all(&package).expect("create benchmark evaluation package");
    let sources = [
        "rust-docs",
        "python-docs",
        "kubernetes-docs",
        "postgres-docs",
    ];
    let intents = ["exact-api", "conceptual", "how-to", "semantic-paraphrase"];
    let mut rows = Vec::new();
    for source in sources {
        for index in 0..16 {
            rows.push(json!({
                "query_id": format!("benchmark-{source}-{index:02}"),
                "query": format!("benchmark fixture documentation {source} {index}"),
                "source": source,
                "intent": intents[index % intents.len()],
                "split": if index < 4 { "development" } else { "held-out" },
            }));
        }
    }
    let mut content = rows
        .iter()
        .map(|row| serde_json::to_string(row).expect("serialize benchmark query"))
        .collect::<Vec<_>>()
        .join("\n");
    content.push('\n');
    fs::write(package.join("queries.jsonl"), content).expect("write benchmark queries");
    package
}

fn write_benchmark_config(
    data_dir: &Path,
    crawl_dir: &Path,
    generation_id: &str,
    snapshot_id: &str,
    package: &Path,
) -> PathBuf {
    let runlens = data_dir.join("runlens.json");
    fs::write(&runlens, r#"{"trace_id":"contract-trace"}"#).expect("write RunLens metadata");
    let config = data_dir.join("benchmark.toml");
    fs::write(
        &config,
        format!(
            r#"schema_version = "scout.benchmark.v1"
benchmark_id = "benchmark-contract"
reference_profile = "contract-reference"
corpus_snapshot_id = "{snapshot_id}"
generation_id = "{generation_id}"
build_id = "build-contract"
evaluation_id = "eval-contract"
evaluation_package = "{}"
seed = 17
query_order = "seeded"
cache_state = "cold"
repetitions = 1
fresh_staging_runs = 3
cold_open_runs = 5
warmup_passes = 2
measured_passes = 10
concurrency = [1, 4, 8, 16]
index_workers = [1, 4, 8, 18]
replay_crawl_dir = "{}"
runlens_metadata = "{}"

[reference]
hardware = "contract-cpu"
logical_cpus = 4
ram_bytes = 8589934592
swap_bytes = 0
os = "test-os"
kernel = "test-kernel"
rust_toolchain = "stable"
lockfile_sha256 = "sha256:lockfile-contract"
filesystem = "test-fs"
free_disk_bytes = 1073741824
background_load = "idle"
"#,
            package.display(),
            crawl_dir.display(),
            runlens.display()
        ),
    )
    .expect("write benchmark config");
    config
}

fn benchmark_artifacts(data_dir: &Path) -> (Value, Value, Vec<Value>) {
    let run_dir = only_child(&data_dir.join("benchmarks"));
    let plan: Value =
        serde_json::from_slice(&fs::read(run_dir.join("plan.json")).expect("read benchmark plan"))
            .expect("parse benchmark plan");
    let report: Value = serde_json::from_slice(
        &fs::read(run_dir.join("report.json")).expect("read benchmark report"),
    )
    .expect("parse benchmark report");
    let samples = fs::read_to_string(run_dir.join("samples.jsonl"))
        .expect("read benchmark samples")
        .lines()
        .map(|line| serde_json::from_str(line).expect("parse benchmark sample"))
        .collect();
    (plan, report, samples)
}

#[test]
fn benchmark_artifact_contract_locks_protocol_inputs_and_integrity_evidence() {
    let (data_dir, crawl_dir, generation_id, snapshot_id) = prepare_generation();
    let package = write_queries(data_dir.path());
    let config = write_benchmark_config(
        data_dir.path(),
        &crawl_dir,
        &generation_id,
        &snapshot_id,
        &package,
    );
    let output = run_scout(
        data_dir.path(),
        &[
            "benchmark".into(),
            "--config".into(),
            config.display().to_string(),
        ],
    );
    assert!(
        output.status.success(),
        "benchmark failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let (plan, report, _samples) = benchmark_artifacts(data_dir.path());
    let run_dir = only_child(&data_dir.path().join("benchmarks"));
    assert!(run_dir.join("resource-config.json").is_file());
    assert!(run_dir.join("resource-samples.jsonl").is_file());
    assert!(run_dir.join("resource-boundaries.jsonl").is_file());
    assert!(run_dir.join("resource-summary.json").is_file());
    let resource_summary: Value = serde_json::from_slice(
        &fs::read(run_dir.join("resource-summary.json")).expect("read resource summary"),
    )
    .expect("parse resource summary");
    assert_eq!(resource_summary["benchmark_run_id"], plan["run_id"]);
    assert!(resource_summary["sampling_interval_ms"].as_u64().is_some());
    assert!(
        resource_summary["sample_count"]
            .as_u64()
            .is_some_and(|count| count > 0)
    );
    let resource_sample: Value = serde_json::from_str(
        fs::read_to_string(run_dir.join("resource-samples.jsonl"))
            .expect("read resource samples")
            .lines()
            .next()
            .expect("one resource sample"),
    )
    .expect("parse resource sample");
    for field in [
        "timestamp_utc",
        "phase",
        "root_pid",
        "configured_interval_ms",
        "process_cpu_time_ms",
        "rss_bytes",
        "host_swap_in_counter",
        "host_swap_out_counter",
        "host_swap_used_bytes",
        "data_dir_bytes",
        "missing_fields",
    ] {
        assert!(
            resource_sample.get(field).is_some(),
            "missing resource field {field}"
        );
    }
    assert_eq!(plan["schema_version"], "scout.benchmark.v1");
    assert_eq!(plan["status"], "locked");
    assert_eq!(plan["corpus_snapshot_id"], snapshot_id);
    assert_eq!(plan["generation_id"], generation_id);
    assert_eq!(plan["query_count"], 64);
    assert_eq!(plan["seed"], 17);
    assert_eq!(plan["query_order"], "seeded");
    assert_eq!(plan["cache_state"], "cold");
    assert_eq!(plan["fresh_staging_runs"], 3);
    assert_eq!(plan["cold_open_runs"], 5);
    assert_eq!(plan["warmup_passes"], 2);
    assert_eq!(plan["measured_passes"], 10);
    assert_eq!(plan["concurrency"], json!([1, 4, 8, 16]));
    assert_eq!(plan["index_workers"], json!([1, 4, 8, 18]));
    assert_eq!(plan["replay_crawl"]["present"], true);
    assert!(plan["corpus_snapshot_sha256"].is_string());
    assert!(plan["build_profile_sha256"].is_string());
    assert!(plan["evaluation_package_sha256"].is_string());
    assert_eq!(report["performance_thresholds_claimed"], false);
    assert!(report["domain_metrics"].is_object());
    assert!(report["phase_timings"].is_object());

    let evidence = fs::read_to_string(
        only_child(&data_dir.path().join("benchmarks")).join("system-evidence.json"),
    )
    .expect("read system evidence");
    let evidence: Value = serde_json::from_str(&evidence).expect("parse system evidence");
    for field in [
        "swap_activity_detected",
        "oom_observed",
        "partial_artifacts",
        "integrity_verified",
        "runlens_metadata",
    ] {
        assert!(
            evidence.get(field).is_some(),
            "missing evidence field {field}"
        );
    }
}

#[test]
fn benchmark_protocol_contract_records_replay_staging_cold_warm_and_latency_evidence() {
    let (data_dir, crawl_dir, generation_id, snapshot_id) = prepare_generation();
    let package = write_queries(data_dir.path());
    let config = write_benchmark_config(
        data_dir.path(),
        &crawl_dir,
        &generation_id,
        &snapshot_id,
        &package,
    );
    let output = run_scout(
        data_dir.path(),
        &[
            "benchmark".into(),
            "--config".into(),
            config.display().to_string(),
        ],
    );
    assert!(
        output.status.success(),
        "benchmark failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let (_plan, _report, samples) = benchmark_artifacts(data_dir.path());
    let phases = samples
        .iter()
        .filter_map(|sample| sample["phase"].as_str())
        .collect::<std::collections::HashSet<_>>();
    for phase in [
        "crawl-replay",
        "crawl-fresh-staging",
        "index-fresh-staging",
        "serving-cold-open",
        "serving-warmup",
        "serving-warm",
    ] {
        assert!(phases.contains(phase), "missing benchmark phase {phase}");
    }
    assert!(
        samples
            .iter()
            .filter(|sample| sample["phase"] == "serving-warm")
            .all(|sample| sample["latency_ms"].is_object()),
        "measured serving samples must include latency distributions"
    );
    let fresh = samples
        .iter()
        .filter(|sample| sample["phase"] == "index-fresh-staging")
        .collect::<Vec<_>>();
    assert_eq!(fresh.len(), 3);
    assert!(fresh.iter().all(|sample| {
        sample["artifact_path"]
            .as_str()
            .is_some_and(|path| path.contains("benchmarks") && path.contains("staging"))
    }));
    assert!(
        fresh
            .iter()
            .all(|sample| sample["artifact_path"] != json!(""))
    );
    let fresh_crawls = samples
        .iter()
        .filter(|sample| sample["phase"] == "crawl-fresh-staging")
        .collect::<Vec<_>>();
    assert_eq!(fresh_crawls.len(), 3);
    for sample in fresh_crawls {
        let path = PathBuf::from(
            sample["artifact_path"]
                .as_str()
                .expect("fresh crawl artifact path"),
        );
        let crawl_dirs = fs::read_dir(path.join("crawls"))
            .expect("fresh crawl directory")
            .map(|entry| entry.expect("fresh crawl entry").path())
            .filter(|entry| entry.is_dir())
            .collect::<Vec<_>>();
        assert_eq!(crawl_dirs.len(), 1);
        let manifest: Value = serde_json::from_slice(
            &fs::read(crawl_dirs[0].join("crawl-manifest.json")).expect("fresh Crawl Manifest"),
        )
        .expect("parse fresh Crawl Manifest");
        assert_eq!(manifest["status"], "complete");
        assert!(manifest["snapshot_id"].is_string());
    }
}
